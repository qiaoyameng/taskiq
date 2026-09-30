from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Generic, TypeVar

from taskiq.result import TaskiqResult

_ReturnType = TypeVar("_ReturnType")


@dataclass(frozen=True, slots=True)
class TaskExecutionDecision:
    """Base class for decisions returned from ``pre_execute`` hooks."""


@dataclass(frozen=True, slots=True)
class SkipTask(TaskExecutionDecision):
    """Skip executing the current task without storing a result."""


@dataclass(frozen=True, slots=True)
class DeferTask(TaskExecutionDecision):
    """Schedule another delivery of the current task without storing a result."""

    delay: float = 0

    def __post_init__(self) -> None:
        if self.delay < 0:
            raise ValueError("Defer delay cannot be negative.")


@dataclass(frozen=True, slots=True)
class ReturnResult(TaskExecutionDecision, Generic[_ReturnType]):
    """Use ``result`` as the task result instead of executing the task."""

    result: _ReturnType


def make_result(value: Any, labels: dict[str, Any]) -> TaskiqResult[Any]:
    """Create a successful task result from a middleware-provided value."""
    if isinstance(value, TaskiqResult):
        return value
    return TaskiqResult(
        is_err=False,
        return_value=value,
        execution_time=0,
        labels=labels,
    )


def reject_execution_decision(value: Any, hook_name: str) -> None:
    """Reject execution decisions from middleware hooks other than pre_execute."""
    if isinstance(value, TaskExecutionDecision):
        raise TypeError(
            f"Execution decisions can only be returned from pre_execute, "
            f"not {hook_name}.",
        )


SkipExecution = SkipTask
DeferExecution = DeferTask
ProvideResult = ReturnResult

__all__ = [
    "DeferExecution",
    "DeferTask",
    "ProvideResult",
    "ReturnResult",
    "SkipExecution",
    "SkipTask",
    "TaskExecutionDecision",
    "make_result",
    "reject_execution_decision",
]
