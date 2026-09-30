import asyncio
import collections.abc
from collections.abc import AsyncGenerator
from logging import getLogger
from time import time
from typing import TYPE_CHECKING, Any, Generic, TypeVar, get_origin

from taskiq.compat import parse_obj_as
from taskiq.exceptions import (
    ResultGetError,
    ResultIsReadyError,
    StreamExpiredError,
    StreamNotFoundError,
    TaskiqResultTimeoutError,
)
from taskiq.result import StreamChunk, StreamRead, StreamReadStatus

if TYPE_CHECKING:  # pragma: no cover
    from taskiq.abc.result_backend import AsyncResultBackend
    from taskiq.depends.progress_tracker import TaskProgress
    from taskiq.result import TaskiqResult

logger = getLogger("taskiq.task")

_ReturnType = TypeVar("_ReturnType")


def _is_stream_return_type(return_type: Any) -> bool:
    """Check whether a resolved return type denotes an async generator task."""
    origin = get_origin(return_type)
    if origin is None:
        return isinstance(return_type, type) and issubclass(
            return_type,
            collections.abc.AsyncGenerator,
        )
    return origin is collections.abc.AsyncGenerator


class AsyncTaskiqTask(Generic[_ReturnType]):
    """AsyncTask for AsyncResultBackend."""

    def __init__(
        self,
        task_id: str,
        result_backend: "AsyncResultBackend[_ReturnType]",
        return_type: type[_ReturnType] | None = None,
    ) -> None:
        self.task_id = task_id
        self.result_backend = result_backend
        self.return_type = return_type

    async def is_ready(self) -> bool:
        """
        Checks if task is completed.

        :raises ResultIsReadyError: if we can't get info about task readiness.

        :return: True if task is completed.
        """
        try:
            return await self.result_backend.is_result_ready(self.task_id)
        except Exception as exc:
            raise ResultIsReadyError from exc

    async def get_result(self, with_logs: bool = False) -> "TaskiqResult[_ReturnType]":
        """
        Get result of a task from result backend.

        :param with_logs: whether you want to fetch logs from worker.

        :raises ResultGetError: if we can't get result from ResultBackend.

        :return: task's return value.
        """
        try:
            res = await self.result_backend.get_result(
                self.task_id,
                with_logs=with_logs,
            )
            if self.return_type is not None and not _is_stream_return_type(
                self.return_type,
            ):
                try:
                    res.return_value = parse_obj_as(
                        self.return_type,
                        res.return_value,
                    )
                except ValueError:
                    logger.warning("Cannot parse return type into %s", self.return_type)
            return res
        except Exception as exc:
            raise ResultGetError from exc

    async def wait_result(
        self,
        check_interval: float = 0.2,
        timeout: float = -1.0,
        with_logs: bool = False,
    ) -> "TaskiqResult[_ReturnType]":
        """
        Waits until result is ready.

        This method just checks whether the task is
        ready. And if it is it returns the result.

        It may throw TaskiqResultTimeoutError if
        task didn't become ready in provided
        period of time.

        :param check_interval: How often checks are performed.
        :param timeout: timeout for the result.
        :param with_logs: whether you want to fetch logs from worker.
        :raises TaskiqResultTimeoutError: if task didn't
            become ready in provided period of time.
        :return: task's return value.
        """
        start_time = time()
        while not await self.is_ready():
            if 0 < timeout < time() - start_time:
                raise TaskiqResultTimeoutError(timeout=timeout)
            await asyncio.sleep(check_interval)
        return await self.get_result(with_logs=with_logs)

    async def get_progress(self) -> "TaskProgress[Any] | None":
        """
        Get task progress.

        :return: task's progress.
        """
        return await self.result_backend.get_progress(self.task_id)

    async def read_stream(
        self,
        since_seq: int = 0,
    ) -> StreamRead[Any]:
        """
        Read fragments of a streaming task starting from `since_seq`.

        The read is idempotent: reading with the same `since_seq` returns
        the same fragments. Pass the sequence after the last received
        chunk to resume reading after disconnecting.

        :param since_seq: sequence number to read from (inclusive).
        :raises StreamNotFoundError: if the stream was never created.
        :raises StreamExpiredError: if the stream was evicted by retention.
        :return: chunks produced since the given sequence number.
        """
        try:
            read = await self.result_backend.read_stream(
                self.task_id,
                since_seq=since_seq,
            )
        except Exception as exc:
            raise ResultGetError from exc
        if read.status == StreamReadStatus.NOT_FOUND:
            raise StreamNotFoundError(task_id=self.task_id)
        if read.status == StreamReadStatus.EXPIRED:
            raise StreamExpiredError(task_id=self.task_id)
        return read

    async def wait_stream(
        self,
        since_seq: int = 0,
        check_interval: float = 0.2,
        timeout: float = -1.0,
    ) -> StreamRead[Any]:
        """
        Wait until at least one new fragment or the terminal state is available.

        :param since_seq: sequence number to read from (inclusive).
        :param check_interval: how often the backend is polled.
        :param timeout: maximum time to wait.
        :raises TaskiqResultTimeoutError: if nothing new appeared in time.
        :raises StreamNotFoundError: if the stream was never created.
        :raises StreamExpiredError: if the stream was evicted by retention.
        :return: chunks produced since the given sequence number.
        """
        start_time = time()
        while True:
            try:
                read = await self.read_stream(since_seq=since_seq)
            except StreamNotFoundError:
                # The worker may not have opened the stream yet.
                read = None
            if read is None:
                if 0 < timeout < time() - start_time:
                    raise TaskiqResultTimeoutError(timeout=timeout)
                await asyncio.sleep(check_interval)
                continue
            if read.chunks or read.status in (
                StreamReadStatus.COMPLETED,
                StreamReadStatus.FAILED,
            ):
                return read
            if 0 < timeout < time() - start_time:
                raise TaskiqResultTimeoutError(timeout=timeout)
            await asyncio.sleep(check_interval)

    async def iter_stream(
        self,
        since_seq: int = 0,
        check_interval: float = 0.2,
    ) -> AsyncGenerator[StreamChunk[Any], None]:
        """
        Iterate over all fragments of a streaming task until it finishes.

        Resuming after a disconnect is done by passing the sequence
        number after the last seen chunk. On a failed task the carried
        exception is raised after all available chunks were yielded.

        :param since_seq: sequence number to start reading from (inclusive).
        :param check_interval: how often the backend is polled.
        :yield: ordered stream chunks.
        """
        next_seq = since_seq
        while True:
            read = await self.wait_stream(
                since_seq=next_seq,
                check_interval=check_interval,
            )
            for chunk in read.chunks:
                next_seq = chunk.seq + 1
                yield chunk
            if read.status == StreamReadStatus.COMPLETED:
                return
            if read.status == StreamReadStatus.FAILED:
                if read.result is not None and read.result.error is not None:
                    raise read.result.error
                return
