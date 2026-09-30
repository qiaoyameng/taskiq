from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from taskiq.exceptions import StreamingNotSupportedError
from taskiq.result import TaskiqResult

if TYPE_CHECKING:  # pragma: no cover
    from taskiq.depends.progress_tracker import TaskProgress
    from taskiq.stream import StreamChunk, StreamRead


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

    async def start_stream(self, task_id: str) -> None:
        """
        Initialize storage for a streaming task.

        This method is called before the first fragment is appended.
        It must be idempotent: if the stream already exists it should
        be left untouched.

        :param task_id: task's id.

        :raises StreamingNotSupportedError: if streams are not supported.
        """
        raise StreamingNotSupportedError(backend=type(self).__name__)

    async def append_stream_chunk(
        self,
        task_id: str,
        chunk: "StreamChunk[Any]",
    ) -> None:
        """
        Persist a single ordered fragment of a streaming task.

        :param task_id: task's id.
        :param chunk: fragment with its sequence number.

        :raises StreamingNotSupportedError: if streams are not supported.
        """
        raise StreamingNotSupportedError(backend=type(self).__name__)

    async def finish_stream(
        self,
        task_id: str,
        state: str,
        error: BaseException | None = None,
    ) -> None:
        """
        Write the terminal state of a streaming task.

        Only a stream with a terminal state written by this method
        (``SUCCESS`` or ``FAILURE``) is considered finished. A stream
        that only has fragments stored must never be treated as a
        successful completion.

        :param task_id: task's id.
        :param state: terminal state, either ``SUCCESS`` or ``FAILURE``.
        :param error: exception if the stream failed.

        :raises StreamingNotSupportedError: if streams are not supported.
        """
        raise StreamingNotSupportedError(backend=type(self).__name__)

    async def read_stream(
        self,
        task_id: str,
        after_seq: int = -1,
        timeout: float = 0.0,
    ) -> "StreamRead[Any]":
        """
        Read ordered fragments of a streaming task.

        Every returned fragment has a sequence number greater than
        ``after_seq``, which allows consumers to resume reading from any
        position. If the stream is not finished yet and no new fragment
        is available, the method waits up to ``timeout`` seconds for new
        data; finished streams always return immediately.

        :param task_id: task's id.
        :param after_seq: sequence number of the last consumed fragment.
        :param timeout: maximum time to wait for new fragments.

        :raises StreamNotFoundError: if the stream never existed or was
            removed by the retention policy.
        :raises StreamingNotSupportedError: if streams are not supported.
        :return: fragments produced after ``after_seq`` and stream state.
        """
        raise StreamingNotSupportedError(backend=type(self).__name__)
