import asyncio
from collections.abc import AsyncGenerator
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import pytest

from taskiq.abc.broker import AsyncBroker
from taskiq.abc.middleware import TaskiqMiddleware
from taskiq.abc.result_backend import AsyncResultBackend
from taskiq.acks import AckableMessage, AcknowledgeType
from taskiq.execution import ExecutionDecision, ExecutionDecisionType
from taskiq.message import BrokerMessage, TaskiqMessage
from taskiq.receiver import Receiver
from taskiq.result import TaskiqResult


class RecordingResultBackend(AsyncResultBackend[Any]):
    def __init__(self, events: list[str], *, fail_save: bool = False) -> None:
        self.events = events
        self.results: dict[str, TaskiqResult[Any]] = {}
        self.fail_save = fail_save

    async def set_result(self, task_id: str, result: TaskiqResult[Any]) -> None:
        self.events.append("save")
        if self.fail_save:
            raise RuntimeError("cannot save result")
        self.results[task_id] = result

    async def is_result_ready(self, task_id: str) -> bool:
        return task_id in self.results

    async def get_result(
        self,
        task_id: str,
        with_logs: bool = False,
    ) -> TaskiqResult[Any]:
        return self.results[task_id]


class RecordingBroker(AsyncBroker):
    def __init__(
        self,
        events: list[str],
        *,
        fail_defer: bool = False,
    ) -> None:
        super().__init__()
        self.events = events
        self.fail_defer = fail_defer
        self.deferred_messages: list[BrokerMessage] = []

    async def kick(self, message: BrokerMessage) -> None:
        self.events.append("defer")
        if self.fail_defer:
            raise RuntimeError("cannot postpone task")
        self.deferred_messages.append(message)

    async def listen(self) -> AsyncGenerator[bytes | AckableMessage, None]:
        raise NotImplementedError
        yield  # pragma: no cover


class DecisionMiddleware(TaskiqMiddleware):
    def __init__(
        self,
        events: list[str],
        decision_factory: Any,
    ) -> None:
        super().__init__()
        self.events = events
        self.decision_factory = decision_factory

    async def pre_execute(
        self,
        message: TaskiqMessage,
    ) -> TaskiqMessage | ExecutionDecision[Any]:
        self.events.append("pre:first")
        return self.decision_factory(message)

    async def post_execute(
        self,
        message: TaskiqMessage,
        result: TaskiqResult[Any],
    ) -> None:
        self.events.append("post_execute:first")

    async def post_save(
        self,
        message: TaskiqMessage,
        result: TaskiqResult[Any],
    ) -> None:
        self.events.append("post_save:first")


class ObserverMiddleware(TaskiqMiddleware):
    def __init__(self, events: list[str]) -> None:
        super().__init__()
        self.events = events
        self.decisions: list[ExecutionDecision[Any]] = []

    async def pre_execute(
        self,
        message: TaskiqMessage,
    ) -> TaskiqMessage:
        if isinstance(message, ExecutionDecision):
            self.decisions.append(message)
            self.events.append(f"pre:second:{message.decision_type.value}")
        else:
            self.events.append("pre:second")
        return message

    async def post_execute(
        self,
        message: TaskiqMessage,
        result: TaskiqResult[Any],
    ) -> None:
        self.events.append("post_execute:second")

    async def post_save(
        self,
        message: TaskiqMessage,
        result: TaskiqResult[Any],
    ) -> None:
        self.events.append("post_save:second")


class FailingPreExecuteMiddleware(TaskiqMiddleware):
    def __init__(self, error: BaseException) -> None:
        super().__init__()
        self.error = error

    async def pre_execute(self, message: TaskiqMessage) -> TaskiqMessage:
        raise self.error


class FailingPostExecuteMiddleware(TaskiqMiddleware):
    async def post_execute(
        self,
        message: TaskiqMessage,
        result: TaskiqResult[Any],
    ) -> None:
        raise RuntimeError("post execute failed")


def defer_without_delay(message: TaskiqMessage) -> ExecutionDecision[Any]:
    return ExecutionDecision.defer(message)


def build_receiver(broker: RecordingBroker) -> Receiver:
    return Receiver(
        broker,
        executor=ThreadPoolExecutor(max_workers=1),
        ack_type=AcknowledgeType.WHEN_RECEIVED,
    )


async def run_callback(
    broker: RecordingBroker,
    task: Any,
    *,
    raise_err: bool = False,
) -> None:
    broker_message = broker.formatter.dumps(
        TaskiqMessage(
            task_id="task_id",
            task_name=task.task_name,
            labels={},
            args=[],
            kwargs={},
        ),
    )

    def ack() -> None:
        broker.events.append("ack")

    receiver = build_receiver(broker)
    await receiver.callback(
        AckableMessage(data=broker_message.message, ack=ack),
        raise_err=raise_err,
    )


async def test_pre_execute_skip_decision_acks_without_saving_result() -> None:
    events: list[str] = []
    broker = RecordingBroker(events)
    broker.with_result_backend(RecordingResultBackend(events))
    broker.with_middlewares(
        DecisionMiddleware(events, ExecutionDecision.skip),
        ObserverMiddleware(events),
    )

    @broker.task
    async def my_task() -> None:
        events.append("task")

    await run_callback(broker, my_task)

    assert events == [
        "pre:first",
        "pre:second:skip",
        "ack",
    ]
    assert broker.deferred_messages == []
    assert broker.result_backend.results == {}


async def test_pre_execute_defer_decision_kicks_before_ack_without_result() -> None:
    events: list[str] = []
    broker = RecordingBroker(events)
    broker.with_result_backend(RecordingResultBackend(events))
    broker.with_middlewares(
        DecisionMiddleware(
            events,
            lambda message: ExecutionDecision.defer(message, delay=5),
        ),
        ObserverMiddleware(events),
    )

    @broker.task
    async def my_task() -> None:
        events.append("task")

    await run_callback(broker, my_task)

    assert events == [
        "pre:first",
        "pre:second:defer",
        "defer",
        "ack",
    ]
    assert broker.result_backend.results == {}
    assert len(broker.deferred_messages) == 1
    deferred_message = broker.deferred_messages[0]
    deferred_task_message = broker.formatter.loads(deferred_message.message)
    assert deferred_task_message.task_id == "task_id"
    assert deferred_task_message.labels == {"delay": 5.0}
    assert not hasattr(deferred_task_message, "decision_type")


async def test_pre_execute_defer_failure_does_not_ack_or_save_result() -> None:
    events: list[str] = []
    broker = RecordingBroker(events, fail_defer=True)
    broker.with_result_backend(RecordingResultBackend(events))
    broker.with_middlewares(
        DecisionMiddleware(
            events,
            defer_without_delay,
        ),
        ObserverMiddleware(events),
    )

    @broker.task
    async def my_task() -> None:
        events.append("task")

    with pytest.raises(RuntimeError, match="cannot postpone task"):
        await run_callback(broker, my_task)

    assert events == [
        "pre:first",
        "pre:second:defer",
        "defer",
    ]
    assert broker.result_backend.results == {}


async def test_pre_execute_result_decision_saves_result_before_ack() -> None:
    events: list[str] = []
    broker = RecordingBroker(events)
    broker.with_result_backend(RecordingResultBackend(events))
    broker.with_middlewares(
        DecisionMiddleware(
            events,
            lambda message: ExecutionDecision.return_result(message, "provided"),
        ),
        ObserverMiddleware(events),
    )

    @broker.task
    async def my_task() -> None:
        events.append("task")

    await run_callback(broker, my_task)

    assert events == [
        "pre:first",
        "pre:second:return_result",
        "post_execute:second",
        "post_execute:first",
        "save",
        "post_save:second",
        "post_save:first",
        "ack",
    ]
    result = broker.result_backend.results["task_id"]
    assert result.return_value == "provided"
    assert result.is_err is False


async def test_all_pre_execute_middlewares_see_same_decision() -> None:
    events: list[str] = []
    broker = RecordingBroker(events)
    broker.with_result_backend(RecordingResultBackend(events))
    decision_middleware = DecisionMiddleware(events, ExecutionDecision.skip)
    observer_middleware = ObserverMiddleware(events)
    broker.with_middlewares(decision_middleware, observer_middleware)

    @broker.task
    async def my_task() -> None:
        events.append("task")

    decision = None
    original_factory = decision_middleware.decision_factory

    def capture_decision(message: TaskiqMessage) -> ExecutionDecision[Any]:
        nonlocal decision
        decision = original_factory(message)
        return decision

    decision_middleware.decision_factory = capture_decision
    await run_callback(broker, my_task)

    assert decision is not None
    assert observer_middleware.decisions == [decision]
    assert observer_middleware.decisions[0].decision_type == ExecutionDecisionType.SKIP


async def test_pre_execute_middleware_error_does_not_execute_save_or_ack() -> None:
    events: list[str] = []
    broker = RecordingBroker(events)
    broker.with_result_backend(RecordingResultBackend(events))
    broker.with_middlewares(FailingPreExecuteMiddleware(RuntimeError("boom")))

    @broker.task
    async def my_task() -> None:
        events.append("task")

    with pytest.raises(RuntimeError, match="boom"):
        await run_callback(broker, my_task)

    assert events == []
    assert broker.deferred_messages == []
    assert broker.result_backend.results == {}


async def test_pre_execute_error_after_decision_does_not_apply_it() -> None:
    events: list[str] = []
    broker = RecordingBroker(events)
    broker.with_result_backend(RecordingResultBackend(events))
    broker.with_middlewares(
        DecisionMiddleware(
            events,
            lambda message: ExecutionDecision.return_result(message, None),
        ),
        FailingPreExecuteMiddleware(RuntimeError("decision observer failed")),
    )

    @broker.task
    async def my_task() -> None:
        events.append("task")

    with pytest.raises(RuntimeError, match="decision observer failed"):
        await run_callback(broker, my_task)

    assert events == ["pre:first"]
    assert broker.deferred_messages == []
    assert broker.result_backend.results == {}


async def test_pre_execute_cancellation_is_not_turned_into_failed_result() -> None:
    events: list[str] = []
    broker = RecordingBroker(events)
    broker.with_result_backend(RecordingResultBackend(events))
    broker.with_middlewares(FailingPreExecuteMiddleware(asyncio.CancelledError()))

    @broker.task
    async def my_task() -> None:
        events.append("task")

    with pytest.raises(asyncio.CancelledError):
        await run_callback(broker, my_task)

    assert events == []
    assert broker.result_backend.results == {}


async def test_replacement_result_post_execute_error_does_not_save_or_ack() -> None:
    events: list[str] = []
    broker = RecordingBroker(events)
    broker.with_result_backend(RecordingResultBackend(events))
    broker.with_middlewares(
        DecisionMiddleware(
            events,
            lambda message: ExecutionDecision.return_result(message, None),
        ),
        FailingPostExecuteMiddleware(),
    )

    @broker.task
    async def my_task() -> None:
        events.append("task")

    with pytest.raises(RuntimeError, match="post execute failed"):
        await run_callback(broker, my_task)

    assert events == ["pre:first"]


async def test_replacement_result_save_failure_does_not_ack() -> None:
    events: list[str] = []
    broker = RecordingBroker(events)
    broker.with_result_backend(RecordingResultBackend(events, fail_save=True))
    broker.with_middlewares(
        DecisionMiddleware(
            events,
            lambda message: ExecutionDecision.return_result(message, None),
        ),
        ObserverMiddleware(events),
    )

    @broker.task
    async def my_task() -> None:
        events.append("task")

    with pytest.raises(RuntimeError, match="cannot save result"):
        await run_callback(broker, my_task, raise_err=True)

    assert events == [
        "pre:first",
        "pre:second:return_result",
        "post_execute:second",
        "post_execute:first",
        "save",
    ]
    assert broker.result_backend.results == {}
