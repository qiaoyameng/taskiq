"""Tests for the batch kicker."""

from collections.abc import AsyncGenerator

import pytest

from taskiq import (
    BatchItemStatus,
    BatchKicker,
    BrokerBatchReceipt,
    InMemoryBroker,
)
from taskiq.abc.middleware import TaskiqMiddleware
from taskiq.exceptions import TaskRejectedError
from taskiq.message import BrokerMessage, TaskiqMessage


class NullBroker(InMemoryBroker):
    """Broker whose kick does nothing, without worker semantics."""

    async def kick(self, message: BrokerMessage) -> None:
        return None

    def listen(self) -> AsyncGenerator[bytes, None]:  # type: ignore[override]
        raise RuntimeError("not used")  # pragma: no cover


class NativeBatchBroker(NullBroker):
    """Broker with an explicit native kick_batch implementation."""

    def __init__(
        self,
        *,
        rejected: set[int] | None = None,
        unknown: set[int] | None = None,
        raises: Exception | None = None,
    ) -> None:
        super().__init__()
        self.rejected = rejected or set()
        self.unknown = unknown or set()
        self.raises = raises
        self.received: list[BrokerMessage] = []
        self.native_called = False
        self.kick_called = 0
        self.calls = 0

    async def kick(self, message: BrokerMessage) -> None:
        self.kick_called += 1

    async def kick_batch(
        self,
        messages: list[BrokerMessage],
    ) -> list[BrokerBatchReceipt]:
        self.native_called = True
        if self.raises is not None:
            raise self.raises
        self.received.extend(messages)
        self.calls += 1
        receipts: list[BrokerBatchReceipt] = []
        for index, _message in enumerate(messages):
            if index in self.rejected:
                receipts.append(
                    BrokerBatchReceipt(index, BatchItemStatus.REJECTED),
                )
            elif index in self.unknown and self.calls == 1:
                receipts.append(
                    BrokerBatchReceipt(
                        index,
                        BatchItemStatus.UNKNOWN,
                        ConnectionError("confirmation lost"),
                    ),
                )
            else:
                receipts.append(
                    BrokerBatchReceipt(index, BatchItemStatus.ACCEPTED),
                )
        return receipts


class FallbackRejectBroker(NullBroker):
    """Broker whose single kick explicitly rejects one task."""

    def __init__(self, reject_task_suffix: str) -> None:
        super().__init__()
        self.reject_task_suffix = reject_task_suffix
        self.kicked: list[str] = []

    async def kick(self, message: BrokerMessage) -> None:
        self.kicked.append(message.task_id)
        if message.task_name.endswith(self.reject_task_suffix):
            raise TaskRejectedError


class FallbackConnectionDropBroker(NullBroker):
    """Broker whose single kick loses the connection after some accepts."""

    def __init__(self, fail_after: int) -> None:
        super().__init__()
        self.fail_after = fail_after
        self.kicked: list[str] = []

    async def kick(self, message: BrokerMessage) -> None:
        if len(self.kicked) >= self.fail_after:
            raise ConnectionError("network down")
        self.kicked.append(message.task_id)


class Unserializable:
    """Object that the default JSON serializer cannot handle."""


class RecordingMiddleware(TaskiqMiddleware):
    """Tracks pre_send/post_send calls in order."""

    def __init__(self) -> None:
        self.pre: list[str] = []
        self.post: list[str] = []

    def pre_send(self, message: TaskiqMessage) -> TaskiqMessage:
        task_id = message.task_id
        self.pre.append(task_id)
        return message

    def post_send(self, message: TaskiqMessage) -> None:
        task_id = message.task_id
        self.post.append(task_id)


async def test_receipts_match_input_positions() -> None:
    """Receipt i corresponds to the i-th added task, even across tasks."""
    broker = NullBroker()

    @broker.task()
    async def add(a: int, b: int) -> int:
        return a + b

    @broker.task()
    async def greet(name: str) -> str:
        return f"hi {name}"

    kicker = broker.batch_kicker()
    kicker.add(add, 1, 2)
    kicker.add(greet, "x")
    kicker.add(add, 3, 4)

    result = await kicker.kick()

    assert len(result) == 3
    assert [receipt.index for receipt in result] == [0, 1, 2]
    assert all(receipt.status == BatchItemStatus.ACCEPTED for receipt in result)
    assert [receipt.task for receipt in result] != [None, None, None]
    assert result[0].task is not None
    assert result[0].task.task_id == result[0].task_id


async def test_serialization_failure_blocks_no_valid_messages() -> None:
    """A bad item is flagged and never reaches the serializer's wire."""
    broker = NativeBatchBroker()

    @broker.task()
    async def add(a: object, b: int) -> int:
        return 1  # pragma: no cover

    kicker = broker.batch_kicker()
    kicker.add(add, 1, 2)
    kicker.add(add, Unserializable(), 2)
    kicker.add(add, 3, 4)

    result = await kicker.kick()

    statuses = [receipt.status for receipt in result]
    assert statuses == [
        BatchItemStatus.ACCEPTED,
        BatchItemStatus.SERIALIZATION_ERROR,
        BatchItemStatus.ACCEPTED,
    ]
    assert result[1].error is not None
    assert result[1].task is None
    assert len(broker.received) == 2
    assert {message.task_id for message in broker.received} == {
        result[0].task_id,
        result[2].task_id,
    }


async def test_partial_acceptance_keeps_previous_accepted() -> None:
    """Rejected/unknown items must not erase earlier accepted receipts."""
    broker = NativeBatchBroker(rejected={1}, unknown={2})

    @broker.task()
    async def task(value: int) -> int:
        return value  # pragma: no cover

    kicker = broker.batch_kicker()
    for value in range(4):
        kicker.add(task, value)

    result = await kicker.kick()

    assert [receipt.status for receipt in result] == [
        BatchItemStatus.ACCEPTED,
        BatchItemStatus.REJECTED,
        BatchItemStatus.UNKNOWN,
        BatchItemStatus.ACCEPTED,
    ]
    assert result.accepted[0].index == 0
    assert {receipt.index for receipt in result.failed} == {1, 2}
    assert broker.native_called is True
    assert broker.kick_called == 0


async def test_native_batch_failure_marks_attempted_unknown_not_unsent() -> None:
    """A raising kick_batch means outcomes are unknown, never not_sent."""
    broker = NativeBatchBroker(raises=ConnectionError("network down"))

    @broker.task()
    async def task() -> None:
        return None  # pragma: no cover

    kicker = broker.batch_kicker()
    kicker.add(task)
    kicker.add(task)

    result = await kicker.kick()

    assert len(result) == 2
    assert all(receipt.status == BatchItemStatus.UNKNOWN for receipt in result)
    assert all(receipt.error is not None for receipt in result)
    assert not any(receipt.status == BatchItemStatus.NOT_SENT for receipt in result)


async def test_explicit_fallback_used_by_default() -> None:
    """Brokers without native batch support use the marked fallback."""
    broker = FallbackRejectBroker(reject_task_suffix=":rejected")

    @broker.task()
    async def good() -> None:
        return None  # pragma: no cover

    @broker.task()
    async def rejected() -> None:
        return None  # pragma: no cover

    @broker.task()
    async def later() -> None:
        return None  # pragma: no cover

    kicker = broker.batch_kicker()
    kicker.add(good)
    kicker.add(rejected)
    kicker.add(later)

    result = await kicker.kick()

    assert [receipt.status for receipt in result] == [
        BatchItemStatus.ACCEPTED,
        BatchItemStatus.REJECTED,
        BatchItemStatus.NOT_SENT,
    ]
    # The fallback reached only the good and rejected messages.
    assert len(broker.kicked) == 2


async def test_fallback_connection_drop_is_unknown() -> None:
    """Dropped connection marks in-flight item unknown, later items unsent."""
    broker = FallbackConnectionDropBroker(fail_after=1)

    @broker.task()
    async def task() -> None:
        return None  # pragma: no cover

    kicker = broker.batch_kicker()
    kicker.add(task)
    kicker.add(task)
    kicker.add(task)

    result = await kicker.kick()

    assert [receipt.status for receipt in result] == [
        BatchItemStatus.ACCEPTED,
        BatchItemStatus.UNKNOWN,
        BatchItemStatus.NOT_SENT,
    ]
    assert isinstance(result[1].error, ConnectionError)


async def test_retry_uses_original_task_id() -> None:
    """Retried items keep the task_id of the original submission."""
    broker = NativeBatchBroker(unknown={0})

    @broker.task()
    async def task(value: int) -> None:
        return None  # pragma: no cover

    kicker = broker.batch_kicker()
    kicker.add(task, 1)
    result = await kicker.kick()
    original_task_id = result[0].task_id

    retried = await kicker.retry(result, 0)

    assert len(retried) == 1
    assert retried[0].index == 0
    assert retried[0].task_id == original_task_id
    assert retried[0].status == BatchItemStatus.ACCEPTED
    assert broker.received[-1].task_id == original_task_id


async def test_retry_by_task_id_and_default_selection() -> None:
    """Default retries all non-accepted; task_id selection is supported."""
    broker = NativeBatchBroker(rejected={0}, unknown={2})

    @broker.task()
    async def task(value: int) -> None:
        return None  # pragma: no cover

    kicker = broker.batch_kicker()
    for value in range(3):
        kicker.add(task, value)
    result = await kicker.kick()

    by_id = await kicker.retry(result, task_ids=[result[2].task_id])
    assert [receipt.index for receipt in by_id] == [2]

    default_retry = await kicker.retry(result)
    assert [receipt.index for receipt in default_retry] == [0, 2]

    with pytest.raises(ValueError, match="Unknown task_id"):
        await kicker.retry(result, task_ids=["does-not-exist"])


async def test_retry_does_not_promise_deduplication() -> None:
    """Even accepted items can be retried; the broker sees them again."""
    broker = NativeBatchBroker()

    @broker.task()
    async def task() -> None:
        return None  # pragma: no cover

    kicker = broker.batch_kicker()
    kicker.add(task)
    result = await kicker.kick()

    received_before = len(broker.received)
    retried = await kicker.retry(result, 0)

    assert retried[0].status == BatchItemStatus.ACCEPTED
    assert retried[0].task_id == result[0].task_id
    assert len(broker.received) == received_before + 1
    assert broker.received[-1].task_id == result[0].task_id


async def test_empty_batch_returns_empty_result() -> None:
    """Kicking an empty batch is a no-op."""
    broker = NativeBatchBroker()

    result = await BatchKicker(broker=broker).kick()

    assert result.receipts == []
    assert broker.native_called is False


async def test_all_bad_items_never_touch_broker() -> None:
    """If every item fails serialization, kick_batch is not called."""
    broker = NativeBatchBroker()

    @broker.task()
    async def task(value: object) -> None:
        return None  # pragma: no cover

    kicker = broker.batch_kicker()
    kicker.add(task, Unserializable())
    kicker.add(task, Unserializable())

    result = await kicker.kick()

    assert [receipt.status for receipt in result] == [
        BatchItemStatus.SERIALIZATION_ERROR,
        BatchItemStatus.SERIALIZATION_ERROR,
    ]
    assert broker.native_called is False


async def test_middlewares_run_per_item_pre_and_post_accepted() -> None:
    """pre_send covers all sendable items; post_send only accepted ones."""
    middleware = RecordingMiddleware()
    broker = NativeBatchBroker(rejected={1})
    broker.add_middlewares(middleware)

    @broker.task()
    async def task(value: int) -> None:
        return None  # pragma: no cover

    kicker = broker.batch_kicker()
    kicker.add(task, 0)
    kicker.add(task, 1)
    kicker.add(task, 2)

    result = await kicker.kick()

    accepted_ids = [result[0].task_id, result[2].task_id]
    assert set(middleware.pre) == set(accepted_ids) | {result[1].task_id}
    assert middleware.post == accepted_ids


async def test_mixed_kickers_and_custom_task_id() -> None:
    """Kickers with labels/custom task id are accepted as batch items."""
    broker = NullBroker()

    @broker.task()
    async def task(value: int) -> int:
        return value  # pragma: no cover

    kicker = broker.batch_kicker()
    kicker.add(task.kicker().with_labels(priority=1), 10)
    kicker.add(task.kicker().with_task_id("fixed-id"), 20)

    result = await kicker.kick()

    assert [receipt.status for receipt in result] == [
        BatchItemStatus.ACCEPTED,
        BatchItemStatus.ACCEPTED,
    ]
    assert result[1].task_id == "fixed-id"


async def test_batch_rejects_multiple_brokers() -> None:
    """A broker-bound kicker refuses items from another broker."""
    broker_a = NullBroker()
    broker_b = NullBroker()

    @broker_a.task()
    async def task_a() -> None:
        return None  # pragma: no cover

    @broker_b.task()
    async def task_b() -> None:
        return None  # pragma: no cover

    kicker = broker_a.batch_kicker()
    kicker.add(task_a)
    with pytest.raises(ValueError, match="same broker"):
        kicker.add(task_b)
