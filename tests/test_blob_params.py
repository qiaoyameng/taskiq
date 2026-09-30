import asyncio
import hashlib
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest
from pydantic import BaseModel

from taskiq import Blob, InMemoryBroker, LocalFileBlobStore, SendTaskError
from taskiq.abc.blob_store import AsyncBlobStore
from taskiq.message import BrokerMessage


class QueuedInMemoryBroker(InMemoryBroker):
    """In-memory broker that keeps broker messages for explicit deliveries."""

    def __init__(self) -> None:
        super().__init__()
        self.queue: asyncio.Queue[BrokerMessage] = asyncio.Queue()
        self.kick_count = 0

    async def kick(self, message: BrokerMessage) -> None:
        self.kick_count += 1
        await self.queue.put(message)

    async def deliver_next(self) -> None:
        message = await self.queue.get()
        await self.receiver.callback(message.message)


class FailingBlobStore(AsyncBlobStore):
    def __init__(self, operation: str) -> None:
        self.operation = operation

    async def put(self, digest: str, data: bytes) -> str:
        if self.operation == "put":
            raise OSError("put failed")
        return "file:///unused"

    async def get(self, uri: str, digest: str) -> bytes:
        if self.operation == "get":
            raise OSError("get failed")
        return b""


@dataclass
class NestedDataclass:
    payload: bytes


class NestedModel(BaseModel):
    payload: bytes


def blob_files(root: str) -> list[Path]:
    return [
        path
        for path in Path(root).rglob("*")
        if path.is_file() and len(path.name) == 64
    ]


async def test_local_file_blob_store_roundtrip() -> None:
    with TemporaryDirectory() as root:
        store = LocalFileBlobStore(root)
        await store.startup()
        data = b"abc"
        digest = hashlib.sha256(data).hexdigest()

        uri = await store.put(digest, data)

        assert await store.get(uri, digest) == data


async def test_local_file_blob_store_put_creates_root() -> None:
    with TemporaryDirectory() as root:
        store = LocalFileBlobStore(Path(root) / "nested")
        data = b"auto-create"
        digest = hashlib.sha256(data).hexdigest()

        uri = await store.put(digest, data)

        assert await store.get(uri, digest) == data


async def test_cleanup_ignores_unrelated_files() -> None:
    with TemporaryDirectory() as root:
        store = LocalFileBlobStore(root, retention_period=0)
        await store.startup()
        unrelated = Path(root) / "state.txt"
        unrelated.write_text("keep")

        assert await store.cleanup_expired() == []

        assert unrelated.read_text() == "keep"


async def test_blob_params_nested_and_deduplicated() -> None:
    with TemporaryDirectory() as root:
        broker = InMemoryBroker(await_inplace=True).with_blob_store(
            LocalFileBlobStore(root),
        )
        seen = []
        large = b"x" * 1024

        @broker.task
        async def consume(
            first: bytes,
            second: NestedDataclass,
            third: NestedModel,
        ) -> None:
            seen.append((first, second.payload, third.payload))

        await consume.kiq(
            Blob(large),
            NestedDataclass(payload=Blob(large)),
            NestedModel(payload=Blob(b"different")),
        )

        assert seen == [(large, large, b"different")]
        assert len(blob_files(root)) == 2


async def test_message_contains_references_not_large_payload() -> None:
    with TemporaryDirectory() as root:
        large = b"large-bytes" * 64
        broker = QueuedInMemoryBroker().with_blob_store(LocalFileBlobStore(root))

        @broker.task
        async def consume(
            payload: bytes,
            plain: bytes,
            copied: bytes | None = None,
        ) -> None:
            return None

        await broker.startup()
        await consume.kiq(Blob(large), plain=b"small", copied=Blob(large))
        broker_message = await broker.queue.get()
        await broker.shutdown()

        assert large not in broker_message.message
        assert b"small" in broker_message.message
        assert broker.kick_count == 1


async def test_kwargs_nested_blob_restores() -> None:
    with TemporaryDirectory() as root:
        broker = InMemoryBroker(await_inplace=True).with_blob_store(
            LocalFileBlobStore(root),
        )
        seen = []

        @broker.task
        async def consume(*, wrapped: dict[str, list[bytes]]) -> None:
            seen.append(wrapped)

        await consume.kiq(wrapped={"items": [Blob(b"nested-kwarg")]})

        assert seen == [{"items": [b"nested-kwarg"]}]


async def test_same_blob_object_is_stored_and_restored_twice() -> None:
    with TemporaryDirectory() as root:
        broker = InMemoryBroker(await_inplace=True).with_blob_store(
            LocalFileBlobStore(root),
        )
        seen = []
        blob = Blob(b"same-object")

        @broker.task
        async def consume(first: bytes, second: object) -> None:
            seen.append((first, second))

        await consume.kiq(blob, {"wrapped": blob})

        assert seen == [(b"same-object", {"wrapped": b"same-object"})]
        assert len(blob_files(root)) == 1


async def test_blob_without_store_does_not_send() -> None:
    broker = QueuedInMemoryBroker()

    @broker.task
    async def consume(payload: bytes) -> None:
        return None

    with pytest.raises(SendTaskError):
        await consume.kiq(Blob(b"large"))

    assert broker.kick_count == 0


async def test_upload_failure_does_not_send_message() -> None:
    broker = QueuedInMemoryBroker().with_blob_store(FailingBlobStore("put"))
    called = False

    @broker.task
    async def consume(payload: bytes) -> None:
        nonlocal called
        called = True

    with pytest.raises(SendTaskError):
        await consume.kiq(Blob(b"large"))

    assert called is False
    assert broker.kick_count == 0
    assert broker.queue.empty()


async def test_read_failure_does_not_invoke_task() -> None:
    broker = QueuedInMemoryBroker().with_blob_store(FailingBlobStore("get"))
    called = False

    @broker.task
    async def consume(payload: bytes) -> None:
        nonlocal called
        called = True

    await broker.startup()
    await consume.kiq(Blob(b"large"))
    message = await broker.queue.get()
    await broker.receiver.callback(message.message)
    await broker.shutdown()

    assert called is False


async def test_repeated_and_delayed_delivery_keeps_blob() -> None:
    with TemporaryDirectory() as root:
        store = LocalFileBlobStore(root, retention_period=0.2)
        broker = QueuedInMemoryBroker().with_blob_store(store)
        received = []
        large = b"large payload" * 128

        @broker.task
        async def consume(payload: bytes) -> None:
            received.append(payload)

        await broker.startup()
        kicked = await consume.kiq(Blob(large))
        message = await broker.queue.get()
        assert kicked.task_id

        await asyncio.sleep(0.05)
        assert await store.cleanup_expired() == []

        await broker.receiver.callback(message.message)
        await asyncio.sleep(0.15)
        assert await store.cleanup_expired() == []

        await broker.receiver.callback(message.message)
        await asyncio.sleep(0.15)
        assert await store.cleanup_expired() == []
        await broker.shutdown()

        assert received == [large, large]
        assert len(blob_files(root)) == 1

        await asyncio.sleep(0.25)
        removed = await store.cleanup_expired()

        assert len(removed) == 1
        assert blob_files(root) == []
