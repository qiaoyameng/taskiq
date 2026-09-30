from pathlib import Path
from typing import Any

import pytest

from taskiq import InMemoryBroker
from taskiq.message import BrokerMessage, TaskiqMessage
from taskiq.refs import (
    BLOB_MARKER_KEY,
    Blob,
    BlobIntegrityError,
    BlobRefMiddleware,
    BlobUploadError,
    LocalFileBlobStore,
)


class FakeClock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


class UncertainKickBroker(InMemoryBroker):
    """Broker that delivers the message, then reports an ambiguous send."""

    async def kick(self, message: BrokerMessage) -> None:
        await super().kick(message)
        msg = "connection reset after enqueue, delivery unknown"
        raise RuntimeError(msg)


class FailingStore:
    async def put(self, data: bytes) -> Any:
        msg = "blob service unavailable"
        raise BlobUploadError(msg)


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


def make_broker(
    tmp_path: Path,
    clock: FakeClock,
    broker_cls: type[InMemoryBroker] = InMemoryBroker,
) -> tuple[InMemoryBroker, LocalFileBlobStore, list[tuple[Any, ...]]]:
    blob_store = LocalFileBlobStore(tmp_path / "blobs", clock=clock)
    broker = broker_cls()
    broker.add_middlewares(
        BlobRefMiddleware(
            blob_store,
            orphan_grace_period=900,
            consumed_grace_period=3600,
        ),
    )
    received: list[tuple[Any, ...]] = []

    @broker.task("consume")
    async def consume(name: str, payload: Any, meta: Any = None) -> str:
        received.append((name, payload, meta))
        return "ok"

    return broker, blob_store, received


async def test_end_to_end_nested_and_repeated_refs(
    tmp_path: Path,
    clock: FakeClock,
) -> None:
    broker, _store, received = make_broker(tmp_path, clock)
    payload = b"z" * 8192

    kicked = await broker.local_task_registry["consume"].kiq(
        "job",
        Blob(payload),
        meta={"rows": [Blob(payload), Blob(b"other")]},
    )
    result = await kicked.wait_result()

    assert result.is_err is False
    assert received == [("job", payload, {"rows": [payload, b"other"]})]


async def test_upload_failure_never_sends(tmp_path: Path, clock: FakeClock) -> None:
    broker = InMemoryBroker()
    broker.add_middlewares(BlobRefMiddleware(FailingStore()))  # type: ignore[arg-type]
    received: list[bytes] = []

    @broker.task("consume-fail")
    async def consume(payload: bytes) -> None:
        received.append(payload)

    with pytest.raises(BlobUploadError):
        await broker.local_task_registry["consume-fail"].kiq(Blob(b"x"))

    await broker.wait_all()
    assert received == []


async def test_uncertain_send_keeps_blob_and_delivered_copy_consumes(
    tmp_path: Path,
    clock: FakeClock,
) -> None:
    broker, store, received = make_broker(
        tmp_path,
        clock,
        broker_cls=UncertainKickBroker,
    )
    payload = b"delivered-but-ack-lost"

    with pytest.raises(Exception, match="Cannot send task"):
        await broker.local_task_registry["consume"].kiq("job", Blob(payload))

    # The message was in fact delivered; its task completes fine.
    await broker.wait_all()
    assert received == [("job", payload, None)]

    # The blob is not deleted despite the client-side error and survives
    # garbage collection while it might still be redelivered.
    clock.now += 3500
    deleted = await store.reap(orphan_grace_period=900, consumed_grace_period=3600)
    assert deleted == []


async def test_read_failure_does_not_invoke_task(
    tmp_path: Path,
    clock: FakeClock,
) -> None:
    broker, store, received = make_broker(tmp_path, clock)
    message = TaskiqMessage(
        task_id="t2",
        task_name="consume",
        labels={},
        args=["job", Blob(b"payload")],
        kwargs={},
    )
    encoded = await broker.middlewares[0].pre_send(message)
    # Remove the object before the worker gets to it.
    for path in store.objects_dir.glob("*/*"):
        if not path.name.endswith(".meta.json"):
            path.unlink()

    callback = broker.receiver.callback(broker.formatter.dumps(encoded).message)
    with pytest.raises(Exception, match="not found"):
        await callback

    assert received == []


async def test_redelivery_is_served_until_consumed_grace(
    tmp_path: Path,
    clock: FakeClock,
) -> None:
    broker, store, received = make_broker(tmp_path, clock)
    task = broker.local_task_registry["consume"]
    payload = b"redelivered"

    await (await task.kiq("first", Blob(payload))).wait_result()
    assert len(received) == 1

    # Broker redelivers long before the consumed grace window elapses.
    clock.now += 3000
    deleted = await store.reap(orphan_grace_period=900, consumed_grace_period=3600)
    assert deleted == []
    await (await task.kiq("second", Blob(payload))).wait_result()
    assert [item[0] for item in received] == ["first", "second"]

    clock.now += 3000
    deleted = await store.reap(orphan_grace_period=900, consumed_grace_period=3600)
    assert deleted == []

    clock.now += 601
    deleted = await store.reap(orphan_grace_period=900, consumed_grace_period=3600)
    assert len(deleted) == 1


async def test_delayed_consumer_within_orphan_grace_restores(
    tmp_path: Path,
    clock: FakeClock,
) -> None:
    broker, store, received = make_broker(tmp_path, clock)
    middleware = broker.middlewares[0]
    payload = b"sleeping in the queue"

    encoded = await middleware.pre_send(
        TaskiqMessage(
            task_id="t3",
            task_name="consume",
            labels={},
            args=["late", Blob(payload)],
            kwargs={},
        ),
    )
    # The object is never consumed while the message sits in the queue.
    clock.now += 899
    deleted = await store.reap(orphan_grace_period=900, consumed_grace_period=3600)
    assert deleted == []

    # Late delivery reaches a worker and is restored successfully.
    callback = broker.receiver.callback(broker.formatter.dumps(encoded).message)
    await callback
    assert received == [("late", payload, None)]


async def test_message_body_carries_only_summary_not_payload(
    tmp_path: Path,
    clock: FakeClock,
) -> None:
    broker, _store, _received = make_broker(tmp_path, clock)
    payload = b"P" * 4096

    message = TaskiqMessage(
        task_id="t4",
        task_name="consume",
        labels={},
        args=["job", Blob(payload)],
        kwargs={},
    )
    encoded = await broker.middlewares[0].pre_send(message)
    wire = broker.formatter.dumps(encoded).message

    assert payload not in wire
    marker = encoded.args[1]
    assert marker[BLOB_MARKER_KEY] == 1
    assert set(marker) == {BLOB_MARKER_KEY, "ref", "length", "digest"}
    assert marker["length"] == 4096


async def test_conflicting_references_for_same_ref_are_rejected(
    tmp_path: Path,
    clock: FakeClock,
) -> None:
    broker, _store, _received = make_broker(tmp_path, clock)
    middleware = broker.middlewares[0]
    message = TaskiqMessage(
        task_id="t-conflict",
        task_name="consume",
        labels={},
        args=[],
        kwargs={},
    )
    good = {
        BLOB_MARKER_KEY: 1,
        "ref": "file:deadbeef",
        "length": 3,
        "digest": "a" * 64,
    }
    bad = dict(good)
    bad["length"] = 4
    message.args = [good, bad]

    with pytest.raises(BlobIntegrityError, match="conflicting"):
        await middleware.pre_execute(message)
