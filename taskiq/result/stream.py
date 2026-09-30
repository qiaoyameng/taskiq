import enum
from typing import Any, Generic, TypeVar

from pydantic import BaseModel, ConfigDict

from taskiq.result import TaskiqResult

_ChunkType = TypeVar("_ChunkType")


class StreamStatus(str, enum.Enum):
    """State of a stream result task."""

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"


class StreamReadStatus(str, enum.Enum):
    """Outcome of a single stream read."""

    CHUNK = "CHUNK"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    NOT_FOUND = "NOT_FOUND"
    EXPIRED = "EXPIRED"


class StreamChunk(BaseModel, Generic[_ChunkType]):
    """A single ordered fragment produced by a streaming task."""

    seq: int
    data: _ChunkType


class StreamState(BaseModel, Generic[_ChunkType]):
    """
    Snapshot of a streaming task stored in a result backend.

    A stream is considered finished only when `result` is set.
    Chunks produced before a failure are kept, but the stream
    itself is marked as failed.
    """

    task_id: str
    status: StreamStatus
    chunks: list[StreamChunk[_ChunkType]] = []
    result: TaskiqResult[Any] | None = None

    model_config = ConfigDict(arbitrary_types_allowed=True)


class StreamRead(BaseModel, Generic[_ChunkType]):
    """
    Result of a single consumer read from a stream.

    `status` tells whether returned chunks are produced by a
    still running task, by a finished task, or whether the
    stream was never created / already expired.
    """

    task_id: str
    status: StreamReadStatus
    seq: int
    chunks: list[StreamChunk[_ChunkType]] = []
    result: TaskiqResult[Any] | None = None

    model_config = ConfigDict(arbitrary_types_allowed=True)
