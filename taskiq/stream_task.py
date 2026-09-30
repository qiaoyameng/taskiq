"""Consumer-side handle for streaming task results."""

import asyncio
from collections.abc import AsyncGenerator
from logging import getLogger
from typing import TYPE_CHECKING, Generic, TypeVar

from taskiq.compat import parse_obj_as
from taskiq.stream import StreamChunk, StreamRead, StreamState

if TYPE_CHECKING:  # pragma: no cover
    from taskiq.abc.result_backend import AsyncResultBackend

logger = getLogger("taskiq.stream_task")

_StreamType = TypeVar("_StreamType")


class AsyncStreamTaskiqTask(Generic[_StreamType]):
    """Handle used to consume results of a streaming task.

    Streaming tasks produce ordered fragments instead of a single final
    value. Fragments are addressed by a zero-based sequence number, so
    consumers can read them repeatedly or resume from any position after
    a disconnect.
    """

    def __init__(
        self,
        task_id: str,
        result_backend: "AsyncResultBackend[_StreamType]",
        return_type: type[_StreamType] | None = None,
    ) -> None:
        self.task_id = task_id
        self.result_backend = result_backend
        self.return_type = return_type

    async def read(
        self,
        after_seq: int = -1,
        timeout: float = 0.0,
    ) -> StreamRead[_StreamType]:
        """
        Read fragments produced after ``after_seq``.

        The call does not consume anything: reading the same range
        twice returns the same fragments. A pending stream blocks up to
        ``timeout`` seconds while waiting for new fragments; finished
        streams return immediately.

        :param after_seq: sequence number of the last consumed fragment.
        :param timeout: maximum wait time for new fragments.
        :return: available fragments and the current stream state.
        """
        result = await self.result_backend.read_stream(
            self.task_id,
            after_seq=after_seq,
            timeout=timeout,
        )
        if self.return_type is not None:
            for chunk in result.chunks:
                try:
                    chunk.data = parse_obj_as(self.return_type, chunk.data)
                except ValueError:
                    logger.warning(
                        "Cannot parse stream fragment into %s",
                        self.return_type,
                    )
        return result

    async def stream(
        self,
        start_seq: int = -1,
        poll_interval: float = 0.1,
        read_timeout: float = 1.0,
    ) -> AsyncGenerator[StreamChunk[_StreamType], None]:
        """
        Yield fragments as they appear until the stream finishes.

        Every yielded fragment carries its sequence number, so callers
        can stop iteration and later resume from the last seen number.
        All fragments produced before a failure are still yielded; if
        the stream fails, the captured exception is raised after the
        last available fragment has been yielded.

        :param start_seq: yield only fragments with greater seq numbers.
        :param poll_interval: fallback interval between backend polls.
        :param read_timeout: blocking timeout per backend read.
        :yield: ordered stream fragments.
        """
        last_seq = start_seq
        while True:
            read_result = await self.read(
                after_seq=last_seq,
                timeout=read_timeout,
            )
            for chunk in read_result.chunks:
                last_seq = chunk.seq
                yield chunk
            if read_result.state == StreamState.SUCCESS:
                return
            if read_result.state == StreamState.FAILURE:
                read_result.raise_for_error()
                return
            await asyncio.sleep(poll_interval)
