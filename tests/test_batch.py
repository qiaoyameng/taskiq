from collections.abc import AsyncGenerator
from typing import Any

import pytest

from taskiq.abc.broker import AsyncBroker
from taskiq.abc.middleware import TaskiqMiddleware
from taskiq.batch import BatchItemStatus, BatchKicker
from taskiq.exceptions import BatchSendError
from taskiq.message import BrokerMessage, TaskiqMessage


class _RecordingBroker(AsyncBroker):
    """Fallback broker that records kicks and can simulate failures."""

    def __init__(self) -> None:
        super().__init__()
        self.kicked: list[BrokerMessage] = []
        self.kick_calls = 0
        self.fail_on_calls: set[int] = set()
        self.disconnect_on_calls: set[int] = set()

    async def kick(self, message: BrokerMessage) -> None:
        call = self.kick_calls
        self.kick_calls += 1
        if call in self.fail_on_calls:
            raise ConnectionError("cannot connect to broker")
        self.kicked.append(message)
        if call in self.disconnect_on_calls:
            raise ConnectionError("confirmation lost after delivery")

    def listen(self) -> AsyncGenerator[bytes, None]:
        raise RuntimeError("Cannot listen.")


class _NativeBatchBroker(_RecordingBroker):
    """Broker with a native batch implementation."""

    def __init__(self) -> None:
        super().__init__()
        self.batches: list[list[BrokerMessage]] = []
        self.batch_error: Exception | None = None

    async def kick_batch(self, messages: list[BrokerMessage]) -> None:
        if self.batch_error is not None:
            raise self.batch_error
        self.batches.append(list(messages))
        self.kicked.extend(messages)


class _RecordingMiddleware(TaskiqMiddleware):
    def __init__(self) -> None:
        super().__init__()
        self.pre_sent: list[TaskiqMessage] = []
        self.post_sent: list[TaskiqMessage] = []

    def pre_send(self, message: TaskiqMessage) -> TaskiqMessage:
        self.pre_sent.append(message)
        return message

    def post_send(self, message: TaskiqMessage) -> None:
        self.post_sent.append(message)


def _make_tasks(broker: AsyncBroker) -> tuple[Any, Any]:
    @broker.task
    async def add(x: int, y: int) -> int:
        return x + y

    @broker.task
    async def greet(name: str) -> str:
        return f"hello, {name}"

    return add, greet


async def test_batch_receipts_align_with_input() -> None:
    broker = _RecordingBroker()
    add, greet = _make_tasks(broker)

    batch = BatchKicker(broker)
    batch.add(add.kicker(), 1, 2)
    batch.add(greet.kicker(), "world")
    batch.add(add.kicker(), x=3, y=4)

    receipts = await batch.kiq()

    assert len(receipts) == 3
    assert [r.status for r in receipts] == [BatchItemStatus.ACCEPTED] * 3
    assert [r.task_name for r in receipts] == [
        add.task_name,
        greet.task_name,
        add.task_name,
    ]
    assert len({r.task_id for r in receipts}) == 3
    assert [m.task_id for m in broker.kicked] == [r.task_id for r in receipts]
    for receipt in receipts:
        assert receipt.ok
        assert receipt.task is not None
        assert receipt.task.task_id == receipt.task_id


async def test_batch_custom_task_id_is_respected() -> None:
    broker = _RecordingBroker()
    add, _ = _make_tasks(broker)

    batch = BatchKicker(broker)
    batch.add(add.kicker().with_task_id("my-id"), 1, 2)
    receipts = await batch.kiq()

    assert receipts[0].task_id == "my-id"
    assert broker.kicked[0].task_id == "my-id"


async def test_batch_serialization_failure_is_rejected() -> None:
    broker = _RecordingBroker()
    add, greet = _make_tasks(broker)

    batch = BatchKicker(broker)
    batch.add(add.kicker(), 1, 2)
    batch.add(greet.kicker(), object())
    batch.add(add.kicker(), 3, 4)

    receipts = await batch.kiq()

    assert [r.status for r in receipts] == [
        BatchItemStatus.ACCEPTED,
        BatchItemStatus.REJECTED,
        BatchItemStatus.ACCEPTED,
    ]
    rejected = receipts[1]
    assert rejected.error
    assert rejected.task is None
    assert rejected.task_id
    assert [m.task_id for m in broker.kicked] == [
        receipts[0].task_id,
        receipts[2].task_id,
    ]


async def test_batch_partial_acceptance_fallback() -> None:
    broker = _RecordingBroker()
    add, _ = _make_tasks(broker)
    broker.fail_on_calls = {1}

    batch = BatchKicker(broker)
    for num in range(3):
        batch.add(add.kicker(), num, num)

    receipts = await batch.kiq()

    assert [r.status for r in receipts] == [
        BatchItemStatus.ACCEPTED,
        BatchItemStatus.UNKNOWN,
        BatchItemStatus.ACCEPTED,
    ]
    assert "ConnectionError" in (receipts[1].error or "")
    assert [m.task_id for m in broker.kicked] == [
        receipts[0].task_id,
        receipts[2].task_id,
    ]


async def test_batch_lost_confirmation_is_unknown_not_rejected() -> None:
    broker = _RecordingBroker()
    add, _ = _make_tasks(broker)
    broker.disconnect_on_calls = {1}

    batch = BatchKicker(broker)
    for num in range(3):
        batch.add(add.kicker(), num, num)

    receipts = await batch.kiq()

    assert receipts[1].status is BatchItemStatus.UNKNOWN
    assert receipts[1].status is not BatchItemStatus.REJECTED
    # The message was actually delivered before the connection dropped.
    assert receipts[1].task_id in {m.task_id for m in broker.kicked}
    assert receipts[0].status is BatchItemStatus.ACCEPTED
    assert receipts[2].status is BatchItemStatus.ACCEPTED


async def test_batch_native_kick_batch_is_used() -> None:
    broker = _NativeBatchBroker()
    add, greet = _make_tasks(broker)

    batch = BatchKicker(broker)
    batch.add(add.kicker(), 1, 2)
    batch.add(greet.kicker(), "world")

    receipts = await batch.kiq()

    assert [r.status for r in receipts] == [BatchItemStatus.ACCEPTED] * 2
    assert len(broker.batches) == 1
    assert [m.task_id for m in broker.batches[0]] == [r.task_id for r in receipts]


async def test_batch_native_partial_acceptance() -> None:
    broker = _NativeBatchBroker()
    add, _ = _make_tasks(broker)
    broker.batch_error = BatchSendError(
        reason="partially accepted",
        accepted={0},
        rejected={2: "message too large"},
    )

    batch = BatchKicker(broker)
    for num in range(3):
        batch.add(add.kicker(), num, num)

    receipts = await batch.kiq()

    assert [r.status for r in receipts] == [
        BatchItemStatus.ACCEPTED,
        BatchItemStatus.UNKNOWN,
        BatchItemStatus.REJECTED,
    ]
    assert receipts[2].error == "message too large"


async def test_batch_native_total_failure_is_unknown() -> None:
    broker = _NativeBatchBroker()
    add, _ = _make_tasks(broker)
    broker.batch_error = ConnectionError("connection lost")

    batch = BatchKicker(broker)
    for num in range(2):
        batch.add(add.kicker(), num, num)

    receipts = await batch.kiq()

    assert [r.status for r in receipts] == [BatchItemStatus.UNKNOWN] * 2


async def test_batch_retry_reuses_original_task_id() -> None:
    broker = _RecordingBroker()
    add, _ = _make_tasks(broker)
    broker.fail_on_calls = {1}

    batch = BatchKicker(broker)
    for num in range(3):
        batch.add(add.kicker(), num, num)

    receipts = await batch.kiq()
    assert receipts[1].status is BatchItemStatus.UNKNOWN

    retried = await batch.retry([1])

    assert len(retried) == 1
    assert retried[0].status is BatchItemStatus.ACCEPTED
    assert retried[0].task_id == receipts[1].task_id
    assert broker.kicked[-1].task_id == receipts[1].task_id


async def test_batch_retry_rejected_item_still_rejected() -> None:
    broker = _RecordingBroker()
    add, greet = _make_tasks(broker)

    batch = BatchKicker(broker)
    batch.add(add.kicker(), 1, 2)
    batch.add(greet.kicker(), object())

    receipts = await batch.kiq()
    assert receipts[1].status is BatchItemStatus.REJECTED

    retried = await batch.retry([1])

    assert retried[0].status is BatchItemStatus.REJECTED
    assert retried[0].task_id == receipts[1].task_id


async def test_batch_retry_out_of_range() -> None:
    broker = _RecordingBroker()
    add, _ = _make_tasks(broker)

    batch = BatchKicker(broker)
    batch.add(add.kicker(), 1, 2)
    await batch.kiq()

    with pytest.raises(IndexError):
        await batch.retry([5])


async def test_batch_middleware_hooks() -> None:
    broker = _RecordingBroker()
    middleware = _RecordingMiddleware()
    broker.add_middlewares(middleware)
    add, greet = _make_tasks(broker)

    batch = BatchKicker(broker)
    batch.add(add.kicker(), 1, 2)
    batch.add(greet.kicker(), object())
    batch.add(add.kicker(), 3, 4)

    receipts = await batch.kiq()

    assert len(middleware.pre_sent) == 3
    assert len(middleware.post_sent) == 2
    assert [r.status for r in receipts] == [
        BatchItemStatus.ACCEPTED,
        BatchItemStatus.REJECTED,
        BatchItemStatus.ACCEPTED,
    ]
