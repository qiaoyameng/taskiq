from datetime import timedelta
from enum import Enum
from typing import Any, Generic, TypeVar

from pydantic import ConfigDict

from taskiq.message import TaskiqMessage

_ReturnType = TypeVar("_ReturnType")


class ExecutionDecisionType(str, Enum):
    """Actions that a pre-execution middleware can decide on."""

    SKIP = "skip"
    DEFER = "defer"
    RETURN_RESULT = "return_result"


class ExecutionDecision(TaskiqMessage, Generic[_ReturnType]):
    """Decision made by a middleware before task execution."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    decision_type: ExecutionDecisionType
    return_value: _ReturnType | None = None
    delay: float | None = None

    @classmethod
    def skip(cls, message: TaskiqMessage) -> "ExecutionDecision[Any]":
        """Create a decision to acknowledge the message without executing it."""
        return cls._from_message(message, ExecutionDecisionType.SKIP)

    @classmethod
    def defer(
        cls,
        message: TaskiqMessage,
        delay: float | timedelta | None = None,
    ) -> "ExecutionDecision[Any]":
        """Create a decision to postpone processing of the message."""
        labels = dict(message.labels)
        delay_seconds = None
        if delay is not None:
            delay_seconds = (
                delay.total_seconds() if isinstance(delay, timedelta) else float(delay)
            )
            labels["delay"] = delay_seconds
        message = message.model_copy(update={"labels": labels})
        return cls._from_message(
            message,
            ExecutionDecisionType.DEFER,
            delay=delay_seconds,
        )

    @classmethod
    def postpone(
        cls,
        message: TaskiqMessage,
        delay: float | timedelta | None = None,
    ) -> "ExecutionDecision[Any]":
        """Create a decision to postpone processing of the message."""
        return cls.defer(message, delay)

    @classmethod
    def return_result(
        cls,
        message: TaskiqMessage,
        return_value: _ReturnType,
    ) -> "ExecutionDecision[_ReturnType]":
        """Create a decision to save the supplied value without executing the task."""
        return cls._from_message(
            message,
            ExecutionDecisionType.RETURN_RESULT,
            return_value=return_value,
        )

    @classmethod
    def _from_message(
        cls,
        message: TaskiqMessage,
        decision_type: ExecutionDecisionType,
        **kwargs: Any,
    ) -> "ExecutionDecision[Any]":
        decision = cls(
            task_id=message.task_id,
            task_name=message.task_name,
            labels=dict(message.labels),
            labels_types=message.labels_types,
            args=list(message.args),
            kwargs=dict(message.kwargs),
            decision_type=decision_type,
            **kwargs,
        )
        private_attributes = set(message.__dict__) - set(type(message).model_fields)
        for attribute_name in private_attributes:
            object.__setattr__(
                decision,
                attribute_name,
                getattr(message, attribute_name),
            )
        return decision

    def to_message(self) -> TaskiqMessage:
        """Return the task message represented by this decision."""
        return TaskiqMessage(
            task_id=self.task_id,
            task_name=self.task_name,
            labels=dict(self.labels),
            labels_types=self.labels_types,
            args=list(self.args),
            kwargs=dict(self.kwargs),
        )
