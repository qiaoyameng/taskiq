import asyncio
from collections import OrderedDict
from collections.abc import AsyncGenerator
from concurrent.futures import ThreadPoolExecutor
from typing import Any, TypeVar

from taskiq.abc.broker import AsyncBroker
from taskiq.abc.middleware import TaskiqMiddleware
from taskiq.abc.result_backend import AsyncResultBackend, TaskiqResult
from taskiq.depends.progress_tracker import TaskProgress
from taskiq.events import TaskiqEvents
from taskiq.exceptions import UnknownTaskError
from taskiq.message import BrokerMessage
from taskiq.receiver import Receiver
from taskiq.result import (
    StreamChunk,
    StreamRead,
    StreamReadStatus,
    StreamState,
    StreamStatus,
)
from taskiq.utils import maybe_awaitable

_ReturnType = TypeVar("_ReturnType")


class InmemoryResultBackend(AsyncResultBackend[_ReturnType]):
    """
    Inmemory result backend.

    This resultbackend is intended to be used only
    with inmemory broker.

    It stores all results in a dict in memory.
    """

    def __init__(self, max_stored_results: int = 100) -> None:
        self.max_stored_results = max_stored_results
        self.results: OrderedDict[str, TaskiqResult[_ReturnType]] = OrderedDict()
        self.progress: OrderedDict[str, TaskProgress[Any]] = OrderedDict()
        self.streams: OrderedDict[str, StreamState[Any]] = OrderedDict()
        # IDs of streams that were removed by the retention policy.
        # This lets us distinguish "never produced" from "already cleaned".
        self.expired_streams: OrderedDict[str, None] = OrderedDict()

    def _is_full(self, storage: "OrderedDict[str, Any]") -> bool:
        """Check whether a bounded storage reached its capacity."""
        return self.max_stored_results != -1 and len(storage) >= self.max_stored_results

    def _evict_oldest_stream(self) -> str | None:
        """Remove the oldest stream and remember it as expired."""
        if not self.streams or not self._is_full(self.streams):
            return None
        evicted_id, _ = self.streams.popitem(last=False)
        self.expired_streams[evicted_id] = None
        if self.max_stored_results != -1 and (
            len(self.expired_streams) > self.max_stored_results
        ):
            self.expired_streams.popitem(last=False)
        return evicted_id

    async def set_result(self, task_id: str, result: TaskiqResult[_ReturnType]) -> None:
        """
        Sets result.

        This method is used to store result of an execution in a
        results dict. But also it removes previous results
        to keep memory footprint as low as possible.

        :param task_id: id of a task.
        :param result: result of an execution.
        """
        if (
            self.max_stored_results != -1
            and len(self.results) >= self.max_stored_results
        ):
            self.results.popitem(last=False)
        self.results[task_id] = result

    async def is_result_ready(self, task_id: str) -> bool:
        """
        Checks whether result is ready.

        Readiness means that result with this task_id is
        present in results dict.

        :param task_id: id of a task to check.
        :return: True if ready.
        """
        if task_id in self.results:
            return True
        stream = self.streams.get(task_id)
        return stream is not None and stream.result is not None

    async def get_result(
        self,
        task_id: str,
        with_logs: bool = False,
    ) -> TaskiqResult[_ReturnType]:
        """
        Get result of a task.

        This method is used to get result
        from result dict.

        It throws exception in case if
        result dict doesn't have a value
        for task_id.

        :param task_id: id of a task.
        :param with_logs: this option is ignored.
        :return: result of a task execution.
        """
        if task_id in self.results:
            return self.results[task_id]
        stream = self.streams.get(task_id)
        if stream is not None and stream.result is not None:
            return stream.result
        raise KeyError(task_id)

    async def set_progress(
        self,
        task_id: str,
        progress: TaskProgress[Any],
    ) -> None:
        """
        Set progress of task execution.

        :param task_id: task id
        :param progress: task execution progress
        """
        if (
            self.max_stored_results != -1
            and len(self.progress) >= self.max_stored_results
        ):
            self.progress.popitem(last=False)

        self.progress[task_id] = progress

    async def get_progress(
        self,
        task_id: str,
    ) -> TaskProgress[Any] | None:
        """
        Get progress of task execution.

        :param task_id: task id
        :return: progress or None
        """
        return self.progress.get(task_id)

    async def open_stream(self, task_id: str) -> StreamState[Any]:
        """
        Create an empty stream for the task.

        :param task_id: task id.
        :return: created stream state.
        """
        self.expired_streams.pop(task_id, None)
        while self._is_full(self.streams):
            self._evict_oldest_stream()
        stream = StreamState[Any](
            task_id=task_id,
            status=StreamStatus.RUNNING,
        )
        self.streams[task_id] = stream
        return stream

    async def add_stream_chunk(self, task_id: str, data: Any) -> int:
        """
        Append an ordered chunk to the stream.

        :param task_id: task id.
        :param data: chunk data.
        :return: sequence number of the stored chunk.
        """
        stream = self.streams.get(task_id)
        if stream is None:
            # Writing to an expired stream is a no-op.
            return -1
        seq = len(stream.chunks)
        stream.chunks.append(StreamChunk(seq=seq, data=data))
        return seq

    async def finish_stream(
        self,
        task_id: str,
        result: TaskiqResult[Any],
    ) -> None:
        """
        Write terminal status of the stream.

        Chunks written before a failure are preserved,
        but the stream is marked as failed.

        :param task_id: task id.
        :param result: final result of the task.
        """
        stream = self.streams.get(task_id)
        if stream is None:
            while self._is_full(self.streams):
                self._evict_oldest_stream()
            stream = StreamState[Any](
                task_id=task_id,
                status=StreamStatus.RUNNING,
            )
            self.streams[task_id] = stream
        stream.status = (
            StreamStatus.SUCCESS if not result.is_err else StreamStatus.FAILURE
        )
        stream.result = result

    async def read_stream(
        self,
        task_id: str,
        since_seq: int = 0,
    ) -> StreamRead[Any]:
        """
        Read stream chunks from the given sequence number.

        :param task_id: task id.
        :param since_seq: sequence number to start reading from (inclusive).
        :return: stream read response.
        """
        if task_id in self.expired_streams:
            return StreamRead[Any](
                task_id=task_id,
                status=StreamReadStatus.EXPIRED,
                seq=since_seq,
            )
        stream = self.streams.get(task_id)
        if stream is None:
            return StreamRead[Any](
                task_id=task_id,
                status=StreamReadStatus.NOT_FOUND,
                seq=since_seq,
            )

        start = max(since_seq, 0)
        chunks = stream.chunks[start:]
        if stream.result is None:
            status = StreamReadStatus.CHUNK
            result = None
        elif stream.result.is_err:
            status = StreamReadStatus.FAILED
            result = stream.result
        else:
            status = StreamReadStatus.COMPLETED
            result = stream.result
        return StreamRead[Any](
            task_id=task_id,
            status=status,
            seq=start,
            chunks=chunks,
            result=result,
        )


class InMemoryBroker(AsyncBroker):
    """
    This broker is used to execute tasks without sending them elsewhere.

    It's useful for local development, if you don't want to setup real broker.
    """

    def __init__(
        self,
        sync_tasks_pool_size: int = 4,
        max_stored_results: int = 100,
        cast_types: bool = True,
        max_async_tasks: int = 30,
        max_async_tasks_jitter: int = 0,
        propagate_exceptions: bool = True,
        await_inplace: bool = False,
    ) -> None:
        super().__init__()
        self.result_backend: InmemoryResultBackend[Any] = InmemoryResultBackend(
            max_stored_results=max_stored_results,
        )
        self.executor = ThreadPoolExecutor(sync_tasks_pool_size)
        self.receiver = Receiver(
            broker=self,
            executor=self.executor,
            validate_params=cast_types,
            max_async_tasks=max_async_tasks,
            max_async_tasks_jitter=max_async_tasks_jitter,
            propagate_exceptions=propagate_exceptions,
        )
        self.await_inplace = await_inplace
        self._running_tasks: set[asyncio.Task[Any]] = set()

    async def kick(self, message: BrokerMessage) -> None:
        """
        Kicking task.

        This method just executes given task.

        :param message: incoming message.

        :raises TaskiqError: if someone wants to kick unknown task.
        """
        target_task = self.find_task(message.task_name)
        if target_task is None:
            raise UnknownTaskError(task_name=message.task_name)

        receiver_cb = self.receiver.callback(message=message.message)
        if self.await_inplace:
            await receiver_cb
            return

        task = asyncio.create_task(receiver_cb)
        self._running_tasks.add(task)
        task.add_done_callback(self._running_tasks.discard)

    def listen(self) -> AsyncGenerator[bytes, None]:
        """
        Inmemory broker cannot listen.

        This method throws RuntimeError if you call it.
        Because inmemory broker cannot really listen to any of tasks.

        :raises RuntimeError: if this method is called.
        """
        raise RuntimeError("Inmemory brokers cannot listen.")

    async def wait_all(self) -> None:
        """
        Wait for all currently running tasks to complete.

        Useful when used in testing and you need to await all sent tasks
        before asserting results.
        """
        to_await = list(self._running_tasks)
        for task in to_await:
            await task

    async def startup(self) -> None:
        """Runs startup events for client and worker side."""
        for event in (TaskiqEvents.CLIENT_STARTUP, TaskiqEvents.WORKER_STARTUP):
            for handler in self.event_handlers.get(event, []):
                await maybe_awaitable(handler(self.state))

        for middleware in self.middlewares:
            if middleware.__class__.startup != TaskiqMiddleware.startup:
                await maybe_awaitable(middleware.startup())

        await self.result_backend.startup()

    async def shutdown(self) -> None:
        """Runs shutdown events for client and worker side."""
        for event in (TaskiqEvents.CLIENT_SHUTDOWN, TaskiqEvents.WORKER_SHUTDOWN):
            for handler in self.event_handlers.get(event, []):
                await maybe_awaitable(handler(self.state))

        for middleware in self.middlewares:
            if middleware.__class__.shutdown != TaskiqMiddleware.shutdown:
                await maybe_awaitable(middleware.shutdown())

        await self.result_backend.shutdown()
        self.executor.shutdown()
