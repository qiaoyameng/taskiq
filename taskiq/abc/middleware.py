import inspect
from collections.abc import Coroutine
from types import CoroutineType
from typing import TYPE_CHECKING, Any, Union

from taskiq.decisions import TaskExecutionDecision
from taskiq.message import TaskiqMessage
from taskiq.utils import maybe_awaitable

if TYPE_CHECKING:
    from taskiq.abc.broker import AsyncBroker
    from taskiq.result import TaskiqResult

MiddlewarePreExecuteResult = Union[
    "TaskiqMessage",
    "TaskExecutionDecision",
]


class TaskiqMiddleware:  # pragma: no cover
    """Base class for middlewares."""

    def __init__(self) -> None:
        self.broker: AsyncBroker = None  # type: ignore

    def set_broker(self, broker: "AsyncBroker") -> None:
        """
        Sets broker to middleware.

        :param broker: broker to set.
        """
        self.broker = broker

    def startup(
        self,
    ) -> Union[Coroutine[Any, Any, None], "CoroutineType[Any, Any, None]", None]:
        """
        Startup method to perform various action during startup.

        This function can be either sync or async.
        Executed during broker's startup.

        :returns nothing.
        """

    def shutdown(
        self,
    ) -> Union[Coroutine[Any, Any, None], "CoroutineType[Any, Any, None]", None]:
        """
        Shutdown method to perform various action during shutdown.

        This function can be either sync or async.
        Executed during broker's shutdown.

        :returns nothing.
        """

    def pre_send(
        self,
        message: "TaskiqMessage",
    ) -> Union[
        "TaskiqMessage",
        "Coroutine[Any, Any, TaskiqMessage]",
        "CoroutineType[Any, Any, TaskiqMessage]",
    ]:
        """
        Hook that executes before sending the task to worker.

        This is a client-side hook, that executes right before
        the message is sent to broker.

        :param message: message to send.
        :return: modified message.
        """
        return message

    def post_send(
        self,
        message: "TaskiqMessage",
    ) -> Union[Coroutine[Any, Any, None], "CoroutineType[Any, Any, None]", None]:
        """
        This hook is executed right after the task is sent.

        This is a client-side hook. It executes right
        after the messages is kicked in broker.

        :param message: kicked message.
        """

    def pre_execute(
        self,
        message: "TaskiqMessage",
        decision: "TaskExecutionDecision | None" = None,
    ) -> Union[
        "MiddlewarePreExecuteResult",
        "Coroutine[Any, Any, MiddlewarePreExecuteResult]",
        "CoroutineType[Any, Any, MiddlewarePreExecuteResult]",
    ]:
        """
        This hook is called before executing task.

        This is a worker-side hook, which means it
        executes in the worker process.

        :param message: incoming parsed taskiq message.
        :param decision: decision made by an earlier middleware.
        :return: modified message.
        """
        return message

    def post_execute(
        self,
        message: "TaskiqMessage",
        result: "TaskiqResult[Any]",
    ) -> Union[Coroutine[Any, Any, None], "CoroutineType[Any, Any, None]", None]:
        """
        This hook executes after task is complete.

        This is a worker-side hook. It's called
        in worker process.

        :param message: incoming message.
        :param result: result of execution for current task.
        """

    def post_save(
        self,
        message: "TaskiqMessage",
        result: "TaskiqResult[Any]",
    ) -> Union[Coroutine[Any, Any, None], "CoroutineType[Any, Any, None]", None]:
        """
        Post save hook.

        This function is called after result of
        the executions is saved in the result_backend.

        :param message: processed message.
        :param result: returned value.
        """

    def on_error(
        self,
        message: "TaskiqMessage",
        result: "TaskiqResult[Any]",
        exception: BaseException,
    ) -> Union[Coroutine[Any, Any, None], "CoroutineType[Any, Any, None]", None]:
        """
        This function is called when exception is found.

        :param message: incoming message.
        :param result: returned value.
        :param exception: found exception.
        """


async def _call_pre_execute(
    middleware: "TaskiqMiddleware",
    message: "TaskiqMessage",
    decision: "TaskExecutionDecision | None",
) -> tuple["TaskiqMessage", "TaskExecutionDecision | None"]:
    """Call a pre-execute hook while preserving its message and decision."""
    positional_parameters = [
        parameter
        for parameter in inspect.signature(middleware.pre_execute).parameters.values()
        if parameter.kind
        in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)
    ]
    has_var_positional = any(
        parameter.kind == inspect.Parameter.VAR_POSITIONAL
        for parameter in inspect.signature(middleware.pre_execute).parameters.values()
    )
    arguments = (
        (message, decision)
        if len(positional_parameters) >= 2 or has_var_positional
        else (message,)
    )
    returned = await maybe_awaitable(middleware.pre_execute(*arguments))
    if returned is None:
        return message, decision
    if isinstance(returned, TaskExecutionDecision):
        return message, returned
    if isinstance(returned, TaskiqMessage):
        return returned, decision
    raise TypeError(
        "pre_execute must return a TaskiqMessage, TaskExecutionDecision, or None.",
    )
