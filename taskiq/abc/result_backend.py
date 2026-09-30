from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from taskiq.result import StreamRead, StreamState, TaskiqResult

if TYPE_CHECKING:  # pragma: no cover
    from taskiq.depends.progress_tracker import TaskProgress


_ReturnType = TypeVar("_ReturnType")


class AsyncResultBackend(ABC, Generic[_ReturnType]):
    """Async result backend."""

    async def startup(self) -> None:
        """Do something when starting broker."""

    async def shutdown(self) -> None:
        """Do something on shutdown."""

    @abstractmethod
    async def set_result(self, task_id: str, result: TaskiqResult[_ReturnType]) -> None:
        """
        Saves result to the result backend.

        Result must be save so it can be accessed later
        by the calling side of the system.

        :param task_id: id of a task to save.
        :param result: result of execution.
        :return: nothing.
        """

    @abstractmethod
    async def is_result_ready(self, task_id: str) -> bool:
        """
        Checks that result of task is ready.

        :param task_id: task's id.
        :return: True if task is completed.
        """

    @abstractmethod
    async def get_result(
        self,
        task_id: str,
        with_logs: bool = False,
    ) -> TaskiqResult[_ReturnType]:
        """
        Gets result from the task.

        :param task_id: task's id.
        :param with_logs: if True it will download task's logs.
        :return: task's return value.
        """

    async def set_progress(
        self,
        task_id: str,
        progress: "TaskProgress[Any]",
    ) -> None:
        """
        Saves progress.

        :param task_id: task's id.
        :param progress: progress of execution.
        """

    async def get_progress(
        self,
        task_id: str,
    ) -> "TaskProgress[Any] | None":
        """
        Gets progress.

        :param task_id: task's id.
        """

    async def open_stream(self, task_id: str) -> StreamState[Any]:
        """
        Creates an empty ordered stream for a streaming task.

        :param task_id: task's id.
        :return: initial state of the stream.
        """
        raise NotImplementedError

    async def add_stream_chunk(
        self,
        task_id: str,
        data: Any,
    ) -> int:
        """
        Appends one chunk to the stream.

        Chunks receive monotonically increasing sequence
        numbers, starting from zero.

        :param task_id: task's id.
        :param data: chunk yielded by the task.
        :return: sequence number assigned to the chunk.
        """
        raise NotImplementedError

    async def finish_stream(
        self,
        task_id: str,
        result: TaskiqResult[Any],
    ) -> None:
        """
        Writes the terminal state of a stream.

        The stream is considered finished only after this call.
        Both successful and failed executions must finish
        their streams.

        :param task_id: task's id.
        :param result: final result of the task execution.
        """
        raise NotImplementedError

    async def read_stream(
        self,
        task_id: str,
        since_seq: int = 0,
    ) -> StreamRead[Any]:
        """
        Reads stream chunks starting from the given sequence number.

        Reads are idempotent: the same `since_seq` value always
        returns the same fragments.

        If the stream was evicted by a retention policy the
        response status is EXPIRED. If it was never created
        the status is NOT_FOUND.

        :param task_id: task's id.
        :param since_seq: sequence number to read from (inclusive).
        :return: chunks and current state of the stream.
        """
        raise NotImplementedError
