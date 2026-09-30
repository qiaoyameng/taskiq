import asyncio
from collections.abc import AsyncGenerator
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import pytest

from taskiq.abc.broker import AsyncBroker
from taskiq.abc.middleware import TaskiqMiddleware
from taskiq.abc.result_backend import AsyncResultBackend
from taskiq.acks import AckableMessage, AcknowledgeType
from taskiq.decisions import DeferTask, ReturnResult, SkipTask, TaskExecutionDecision
from taskiq.message import BrokerMessage, TaskiqMessage
from taskiq.receiver import Receiver
from taskiq.result import TaskiqResult


class EventResultBackend(AsyncResultBackend[Any]):
    def __init__(
        self,
        events: list[tuple[Any, ...]],
        set_result_error: BaseException | None = None,
    ) -> None:
        self.events = events
        self.results: dict[str, TaskiqResult[Any]] = {}
        self.set_result_error = set_result_error

    async def set_result(self, task_id: str, result: TaskiqResult[Any]) -> None:
        self.events.append(("save", task_id))
        if self.set_result_error is not None:
            raise self.set_result_error
        self.results[task_id] = result

    async def is_result_ready(self, task_id: str) -> bool:
        return task_id in self.results

    async def get_result(
        self,
        task_id: str,
        with_logs: bool = False,
    ) -> TaskiqResult[Any]:
        return self.results[task_id]


class EventBroker(AsyncBroker):
    def __init__(
        self,
        events: list[tuple[Any, ...]],
        kick_error: BaseException | None = None,
    ) -> None:
        super().__init__(None, None)
        self.events = events
        self.kick_error = kick_error

    async def kick(self, message: BrokerMessage) -> None:
        parsed_message = self.formatter.loads(message.message)
        self.events.append(("kick", parsed_message.labels.get("delay")))
        if self.kick_error is not None:
            raise self.kick_error

    def listen(self) -> AsyncGenerator[AckableMessage, None]:
        raise RuntimeError("EventBroker does not listen.")


class DecisionMiddleware(TaskiqMiddleware):
    def __init__(
        self,
        events: list[tuple[Any, ...]],
        decision: TaskExecutionDecision | None = None,
        error: BaseException | None = None,
    ) -> None:
        super().__init__()
        self.events = events
        self.decision = decision
        self.error = error

    async def pre_execute(  # type: ignore[override]
        self,
        message: TaskiqMessage,
        decision: TaskExecutionDecision | None = None,
    ) -> TaskiqMessage | TaskExecutionDecision | None:
        self.events.append(("pre", "first", decision))
        if self.error is not None:
            raise self.error
        return self.decision


class ObserverMiddleware(TaskiqMiddleware):
    def __init__(self, events: list[tuple[Any, ...]], name: str) -> None:
        super().__init__()
        self.events = events
        self.name = name

    async def pre_execute(
        self,
        message: TaskiqMessage,
        decision: TaskExecutionDecision | None = None,
    ) -> TaskiqMessage:
        self.events.append(("pre", self.name, decision))
        return message

    async def post_execute(
        self,
        message: TaskiqMessage,
        result: TaskiqResult[Any],
    ) -> None:
        self.events.append(("post_execute", self.name))

    async def post_save(
        self,
        message: TaskiqMessage,
        result: TaskiqResult[Any],
    ) -> None:
        self.events.append(("post_save", self.name))


class LegacyObserverMiddleware(TaskiqMiddleware):
    def __init__(self, events: list[tuple[Any, ...]]) -> None:
        super().__init__()
        self.events = events

    async def pre_execute(  # type: ignore[override]
        self,
        message: TaskiqMessage,
    ) -> TaskiqMessage:
        self.events.append(("pre", "legacy"))
        return message


class FailingPostExecuteMiddleware(TaskiqMiddleware):
    def __init__(self, events: list[tuple[Any, ...]]) -> None:
        super().__init__()
        self.events = events

    async def post_execute(
        self,
        message: TaskiqMessage,
        result: TaskiqResult[Any],
    ) -> None:
        self.events.append(("post_execute", "failing"))
        raise ValueError("post middleware failed")


class DecisionFromPostMiddleware(TaskiqMiddleware):
    async def post_execute(  # type: ignore[override]
        self,
        message: TaskiqMessage,
        result: TaskiqResult[Any],
    ) -> TaskExecutionDecision:
        return ReturnResult("too late")


class DecisionFromPreSendMiddleware(TaskiqMiddleware):
    async def pre_send(  # type: ignore[override]
        self,
        message: TaskiqMessage,
    ) -> TaskExecutionDecision:
        return SkipTask()


def make_receiver(
    broker: AsyncBroker,
    ack_type: AcknowledgeType = AcknowledgeType.WHEN_SAVED,
) -> Receiver:
    return Receiver(
        broker,
        executor=ThreadPoolExecutor(max_workers=1),
        ack_type=ack_type,
    )


async def callback(
    broker: AsyncBroker,
    receiver: Receiver,
    events: list[tuple[Any, ...]],
    raise_err: bool = False,
) -> None:
    task = broker.find_task("test-task")
    assert task is not None
    broker_message = broker.formatter.dumps(
        TaskiqMessage(
            task_id="task-id",
            task_name=task.task_name,
            labels={},
            args=[],
            kwargs={},
        ),
    )

    def ack() -> None:
        events.append(("ack",))

    await receiver.callback(
        AckableMessage(data=broker_message.message, ack=ack),
        raise_err=raise_err,
    )


async def test_skip_decision_does_not_save_result_but_acks() -> None:
    events: list[tuple[Any, ...]] = []
    broker = EventBroker(events).with_result_backend(EventResultBackend(events))
    decision = SkipTask()

    @broker.task(task_name="test-task")
    async def task() -> str:
        events.append(("task",))
        return "executed"

    broker.with_middlewares(
        DecisionMiddleware(events, decision=decision),
        LegacyObserverMiddleware(events),
        ObserverMiddleware(events, "second"),
        ObserverMiddleware(events, "third"),
    )

    with ThreadPoolExecutor(max_workers=1):
        receiver = make_receiver(broker)
        await callback(broker, receiver, events)

    assert events == [
        ("pre", "first", None),
        ("pre", "legacy"),
        ("pre", "second", decision),
        ("pre", "third", decision),
        ("ack",),
    ]


async def test_skip_decision_respects_manual_ack_type() -> None:
    events: list[tuple[Any, ...]] = []
    broker = EventBroker(events).with_result_backend(EventResultBackend(events))

    @broker.task(task_name="test-task")
    async def task() -> str:
        events.append(("task",))
        return "executed"

    broker.with_middlewares(DecisionMiddleware(events, decision=SkipTask()))

    with ThreadPoolExecutor(max_workers=1):
        receiver = make_receiver(broker, AcknowledgeType.MANUAL)
        await callback(broker, receiver, events)

    assert events == [("pre", "first", None)]


async def test_defer_decision_requeues_before_acknowledgement() -> None:
    events: list[tuple[Any, ...]] = []
    broker = EventBroker(events).with_result_backend(EventResultBackend(events))
    decision = DeferTask(delay=3)

    @broker.task(task_name="test-task")
    async def task() -> str:
        events.append(("task",))
        return "executed"

    broker.with_middlewares(
        DecisionMiddleware(events, decision=decision),
        ObserverMiddleware(events, "second"),
    )

    with ThreadPoolExecutor(max_workers=1):
        receiver = make_receiver(broker, AcknowledgeType.WHEN_RECEIVED)
        await callback(broker, receiver, events)

    assert events == [
        ("pre", "first", None),
        ("pre", "second", decision),
        ("kick", 3.0),
        ("ack",),
    ]


async def test_defer_failure_does_not_acknowledge_or_save() -> None:
    events: list[tuple[Any, ...]] = []
    broker = EventBroker(
        events,
        kick_error=RuntimeError("broker unavailable"),
    ).with_result_backend(EventResultBackend(events))
    decision = DeferTask(delay=1)

    @broker.task(task_name="test-task")
    async def task() -> str:
        events.append(("task",))
        return "executed"

    broker.with_middlewares(
        DecisionMiddleware(events, decision=decision),
        ObserverMiddleware(events, "second"),
    )

    with ThreadPoolExecutor(max_workers=1):
        receiver = make_receiver(broker, AcknowledgeType.WHEN_RECEIVED)
        with pytest.raises(RuntimeError, match="broker unavailable"):
            await callback(broker, receiver, events)

    assert events == [
        ("pre", "first", None),
        ("pre", "second", decision),
        ("kick", 1.0),
    ]


async def test_return_result_decision_saves_successful_result() -> None:
    events: list[tuple[Any, ...]] = []
    result_backend = EventResultBackend(events)
    broker = EventBroker(events).with_result_backend(result_backend)
    decision = ReturnResult("provided")

    @broker.task(task_name="test-task")
    async def task() -> str:
        events.append(("task",))
        return "executed"

    broker.with_middlewares(
        DecisionMiddleware(events, decision=decision),
        ObserverMiddleware(events, "second"),
        ObserverMiddleware(events, "third"),
    )

    with ThreadPoolExecutor(max_workers=1):
        receiver = make_receiver(broker)
        await callback(broker, receiver, events)

    assert events == [
        ("pre", "first", None),
        ("pre", "second", decision),
        ("pre", "third", decision),
        ("post_execute", "third"),
        ("post_execute", "second"),
        ("save", "task-id"),
        ("post_save", "third"),
        ("post_save", "second"),
        ("ack",),
    ]
    saved_result = result_backend.results["task-id"]
    assert not saved_result.is_err
    assert saved_result.error is None
    assert saved_result.return_value == "provided"


async def test_return_result_ack_after_save_with_when_executed() -> None:
    events: list[tuple[Any, ...]] = []
    broker = EventBroker(events).with_result_backend(EventResultBackend(events))

    @broker.task(task_name="test-task")
    async def task() -> str:
        events.append(("task",))
        return "executed"

    broker.with_middlewares(
        DecisionMiddleware(events, decision=ReturnResult("provided")),
    )

    with ThreadPoolExecutor(max_workers=1):
        receiver = make_receiver(broker, AcknowledgeType.WHEN_EXECUTED)
        await callback(broker, receiver, events)

    assert events == [
        ("pre", "first", None),
        ("save", "task-id"),
        ("ack",),
    ]


async def test_return_result_save_failure_does_not_acknowledge() -> None:
    events: list[tuple[Any, ...]] = []
    broker = EventBroker(events).with_result_backend(
        EventResultBackend(
            events,
            set_result_error=RuntimeError("backend unavailable"),
        ),
    )

    @broker.task(task_name="test-task")
    async def task() -> str:
        events.append(("task",))
        return "executed"

    broker.with_middlewares(
        DecisionMiddleware(events, decision=ReturnResult("provided")),
    )

    with ThreadPoolExecutor(max_workers=1):
        receiver = make_receiver(broker, AcknowledgeType.WHEN_EXECUTED)
        with pytest.raises(RuntimeError, match="backend unavailable"):
            await callback(broker, receiver, events, raise_err=True)

    assert events == [("pre", "first", None), ("save", "task-id")]


async def test_pre_execute_middleware_error_does_not_acknowledge() -> None:
    events: list[tuple[Any, ...]] = []
    broker = EventBroker(events).with_result_backend(EventResultBackend(events))

    @broker.task(task_name="test-task")
    async def task() -> str:
        events.append(("task",))
        return "executed"

    broker.with_middlewares(
        DecisionMiddleware(events, error=ValueError("middleware failed")),
        ObserverMiddleware(events, "second"),
    )

    with ThreadPoolExecutor(max_workers=1):
        receiver = make_receiver(broker, AcknowledgeType.WHEN_RECEIVED)
        with pytest.raises(ValueError, match="middleware failed"):
            await callback(broker, receiver, events)

    assert events == [("pre", "first", None)]


async def test_cancelled_pre_execute_remains_cancelled() -> None:
    events: list[tuple[Any, ...]] = []
    broker = EventBroker(events).with_result_backend(EventResultBackend(events))

    @broker.task(task_name="test-task")
    async def task() -> str:
        events.append(("task",))
        return "executed"

    broker.with_middlewares(
        DecisionMiddleware(events, error=asyncio.CancelledError()),
    )

    with ThreadPoolExecutor(max_workers=1):
        receiver = make_receiver(broker, AcknowledgeType.WHEN_RECEIVED)
        with pytest.raises(asyncio.CancelledError):
            await callback(broker, receiver, events)

    assert events == [("pre", "first", None)]


async def test_post_execute_middleware_error_does_not_save_or_ack() -> None:
    events: list[tuple[Any, ...]] = []
    broker = EventBroker(events).with_result_backend(EventResultBackend(events))

    @broker.task(task_name="test-task")
    async def task() -> str:
        events.append(("task",))
        return "executed"

    broker.with_middlewares(FailingPostExecuteMiddleware(events))

    with ThreadPoolExecutor(max_workers=1):
        receiver = make_receiver(broker)
        with pytest.raises(ValueError, match="post middleware failed"):
            await callback(broker, receiver, events)

    assert events == [("task",), ("post_execute", "failing")]


async def test_decision_returned_from_post_execute_is_rejected() -> None:
    events: list[tuple[Any, ...]] = []
    broker = EventBroker(events).with_result_backend(EventResultBackend(events))

    @broker.task(task_name="test-task")
    async def task() -> str:
        events.append(("task",))
        return "executed"

    broker.with_middlewares(DecisionFromPostMiddleware())

    with ThreadPoolExecutor(max_workers=1):
        receiver = make_receiver(broker)
        with pytest.raises(TypeError, match="can only be returned from pre_execute"):
            await callback(broker, receiver, events)

    assert events == [("task",)]


async def test_decision_returned_from_pre_send_is_rejected() -> None:
    events: list[tuple[Any, ...]] = []
    broker = EventBroker(events).with_result_backend(EventResultBackend(events))

    @broker.task(task_name="test-task")
    async def task() -> str:
        return "executed"

    broker.with_middlewares(DecisionFromPreSendMiddleware())

    with pytest.raises(TypeError, match="can only be returned from pre_execute"):
        await task.kiq()

    assert events == []
