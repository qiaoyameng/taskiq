"""Models for streaming task results.

Streaming tasks are tasks whose target function is an asynchronous
generator. Instead of waiting for a single final return value, every
value yielded by the generator is persisted by the result backend as an
ordered fragment. Consumers can read fragments starting from any
sequence number and resume reading after disconnects.
"""

import enum
import json
from typing import Any, Generic, TypeVar

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_serializer,
    field_validator,
)

from taskiq.serialization import exception_to_python, prepare_exception

_StreamType = TypeVar("_StreamType")


class StreamState(str, enum.Enum):
    """Terminal or intermediate state of a streaming task."""

    PENDING = "PENDING"
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"


class StreamChunk(BaseModel, Generic[_StreamType]):
    """A single ordered fragment produced by a streaming task."""

    seq: int
    data: _StreamType

    model_config = ConfigDict(arbitrary_types_allowed=True)


class StreamRead(BaseModel, Generic[_StreamType]):
    """Result of reading a stream from a result backend.

    ``chunks`` contains fragments with sequence numbers strictly greater
    than the requested ``after_seq`` value. ``state`` is the state of the
    stream at the moment of reading. When the stream failed, ``error``
    holds the exception raised inside the generator.
    """

    chunks: list[StreamChunk[_StreamType]] = Field(default_factory=list)
    state: StreamState = StreamState.PENDING
    error: BaseException | None = None

    model_config = ConfigDict(arbitrary_types_allowed=True)

    @property
    def is_complete(self) -> bool:
        """Whether the stream reached a terminal state."""
        return self.state in (StreamState.SUCCESS, StreamState.FAILURE)

    @field_serializer("error")
    def serialize_error(self, value: BaseException) -> Any:
        """Serialize error field, mirroring ``TaskiqResult`` behaviour."""
        if value:
            return prepare_exception(value, json)
        return None

    def raise_for_error(self) -> "StreamRead[_StreamType]":
        """Raise the captured exception if the stream failed.

        :raises BaseException: exception captured from the generator.
        :return: self.
        """
        if self.error is not None:
            raise self.error
        return self

    @field_validator("error", mode="before")
    @classmethod
    def _validate_error(cls, value: Any) -> BaseException | None:
        return exception_to_python(value)
