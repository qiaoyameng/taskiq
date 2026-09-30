"""
Acceptance demo for reference parameters.

Run with the repository environment::

    .venv/bin/python docs/guide/ref-params/acceptance_demo.py

The demo uses an in-memory broker with an explicit queue so delivery can be
delayed and repeated, and a local filesystem blob store with a controllable
clock. It demonstrates:

1. marked large arguments leave the message as digest+length+reference and are
   restored on the worker, including nested positions and repeated references;
2. an upload failure aborts the send (no message is produced);
3. an ambiguous send result never triggers immediate blob deletion, and a
   message that was in fact delivered is still consumed;
4. a read/verification failure blocks task execution;
5. repeated delivery and delayed consumption cannot observe premature garbage
   collection - blobs survive orphan and consumed grace windows and are only
   reaped well after their last access.
"""

from __future__ import annotations

import asyncio
import tempfile
from pathlib import Path
from typing import Any

from taskiq import InMemoryBroker
from taskiq.message import BrokerMessage, TaskiqMessage
from taskiq.refs import (
    Blob,
    BlobRefMiddleware,
    BlobUploadError,
    LocalFileBlobStore,
)


class FakeClock:
    """Deterministic clock shared by the blob store and the demo."""

    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class QueueingInMemoryBroker(InMemoryBroker):
    """In-memory broker that stores messages until they are drained."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.queue: asyncio.Queue[BrokerMessage] = asyncio.Queue()

    async def kick(self, message: BrokerMessage) -> None:
        await self.queue.put(message)

    async def deliver_one(self) -> None:
        message = await self.queue.get()
        await self.receiver.callback(message=message.message)

    def queue_depth(self) -> int:
        return self.queue.qsize()


class UncertainKickBroker(QueueingInMemoryBroker):
    """Delivers the message to the queue, then reports an ambiguous send."""

    async def kick(self, message: BrokerMessage) -> None:
        await self.queue.put(message)
        msg = "connection reset after enqueue, delivery unknown"
        raise RuntimeError(msg)


class FailingStore:
    async def put(self, data: bytes) -> Any:
        msg = "blob service unavailable"
        raise BlobUploadError(msg)


def make_pipeline(
    root: Path,
    clock: FakeClock,
    broker_cls: type[InMemoryBroker] = QueueingInMemoryBroker,
) -> tuple[InMemoryBroker, LocalFileBlobStore, list[tuple[Any, ...]]]:
    store = LocalFileBlobStore(root / "blobs", clock=clock)
    broker = broker_cls()
    broker.add_middlewares(
        BlobRefMiddleware(
            store,
            orphan_grace_period=900,
            consumed_grace_period=3600,
        ),
    )
    consumed: list[tuple[Any, ...]] = []

    @broker.task("process")
    async def process(job_id: str, payload: bytes, meta: Any = None) -> str:
        consumed.append((job_id, payload, meta))
        return "ok"

    return broker, store, consumed


async def scenario_happy_path(root: Path, clock: FakeClock) -> None:
    print("[1] nested + repeated references, compact message")
    broker, _store, consumed = make_pipeline(root / "happy", clock)
    payload = b"Z" * 8192

    task = await broker.local_task_registry["process"].kiq(
        "job-1",
        Blob(payload),
        meta={"rows": [Blob(payload), Blob(b"extra")]},
    )
    assert broker.queue_depth() == 1
    await broker.deliver_one()
    result = await task.wait_result()
    assert result.is_err is False
    assert consumed == [
        ("job-1", payload, {"rows": [payload, b"extra"]}),
    ], consumed
    print("    worker restored payload and nested/repeated references")


async def scenario_upload_failure(root: Path, clock: FakeClock) -> None:
    print("[2] upload failure aborts the send")
    broker = QueueingInMemoryBroker()
    broker.add_middlewares(BlobRefMiddleware(FailingStore()))  # type: ignore[arg-type]

    @broker.task("never")
    async def never(payload: bytes) -> None:
        msg = "task must not run"
        raise AssertionError(msg)

    try:
        await broker.local_task_registry["never"].kiq(Blob(b"x"))
    except BlobUploadError:
        pass
    else:
        msg = "expected upload failure to propagate before the send"
        raise AssertionError(msg)
    assert broker.queue_depth() == 0
    print("    no message was enqueued and the task was not invoked")


async def scenario_uncertain_send(root: Path, clock: FakeClock) -> None:
    print("[3] ambiguous send: blob retained, delivered copy still consumed")
    broker, store, consumed = make_pipeline(
        root / "uncertain",
        clock,
        broker_cls=UncertainKickBroker,
    )
    payload = b"delivered-but-ack-lost"

    try:
        await broker.local_task_registry["process"].kiq("job-2", Blob(payload))
    except Exception as exc:
        print(f"    client saw: {exc}")

    await broker.deliver_one()
    await broker.wait_all()
    assert consumed == [("job-2", payload, None)], consumed

    # Even long after the "failed" send, the object must still exist: the
    # message was possibly delivered and may be redelivered.
    clock.advance(3500)
    deleted = await store.reap(orphan_grace_period=900, consumed_grace_period=3600)
    assert deleted == [], deleted
    print("    delivered copy consumed; blob survived GC despite send error")


async def scenario_read_failure(root: Path, clock: FakeClock) -> None:
    print("[4] read failure prevents task execution")
    broker, store, consumed = make_pipeline(root / "missing", clock)
    middleware = broker.middlewares[0]

    encoded = await middleware.pre_send(
        TaskiqMessage(
            task_id="t-missing",
            task_name="process",
            labels={},
            args=["job-3", Blob(b"payload")],
            kwargs={},
        ),
    )
    for path in store.objects_dir.glob("*/*"):
        if not path.name.endswith(".meta.json"):
            path.unlink()

    try:
        await broker.receiver.callback(
            message=broker.formatter.dumps(encoded).message,
        )
    except Exception as exc:
        print(f"    worker raised before running task: {type(exc).__name__}")
    else:
        msg = "missing blob must block execution"
        raise AssertionError(msg)
    assert consumed == [], consumed
    print("    task function was never called")


async def scenario_redelivery_and_delay(
    root: Path,
    clock: FakeClock,
) -> None:
    print("[5] redelivery + delayed consumption never see premature deletion")
    broker, store, consumed = make_pipeline(root / "redelivery", clock)
    task = broker.local_task_registry["process"]
    payload = b"redelivered-payload"

    # First delivery is consumed.
    first = await task.kiq("first", Blob(payload))
    await broker.deliver_one()
    await first.wait_result()

    # Re-delivery #1 after a long delay but inside the consumed window.
    clock.advance(3000)
    assert await store.reap(orphan_grace_period=900, consumed_grace_period=3600) == []
    redelivery_one = await task.kiq("redelivery-1", Blob(payload))
    await broker.deliver_one()
    await redelivery_one.wait_result()

    # A delayed consumer only wakes up nearly one consumed window later.
    clock.advance(3599)
    assert await store.reap(orphan_grace_period=900, consumed_grace_period=3600) == []
    redelivery_two = await task.kiq("redelivery-2", Blob(payload))
    await broker.deliver_one()
    await redelivery_two.wait_result()
    assert [item[0] for item in consumed] == [
        "first",
        "redelivery-1",
        "redelivery-2",
    ]

    # Only after a full window past the LAST access is the object reaped.
    clock.advance(3601)
    deleted = await store.reap(
        orphan_grace_period=900,
        consumed_grace_period=3600,
    )
    assert len(deleted) == 1, deleted

    # Orphans from a never-consumed message survive the orphan window.
    queued = await task.kiq("queued", Blob(b"stuck in queue"))
    clock.advance(899)
    assert await store.reap(orphan_grace_period=900, consumed_grace_period=3600) == []
    await broker.deliver_one()
    await queued.wait_result()
    assert consumed[-1][0] == "queued"
    print("    all redeliveries/delayed consumers succeeded; GC stayed safe")


async def main() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        clock = FakeClock()
        await scenario_happy_path(root, clock)
        await scenario_upload_failure(root, clock)
        await scenario_uncertain_send(root, clock)
        await scenario_read_failure(root, clock)
        await scenario_redelivery_and_delay(root, clock)
    print("\nALL ACCEPTANCE SCENARIOS PASSED")


if __name__ == "__main__":
    asyncio.run(main())
