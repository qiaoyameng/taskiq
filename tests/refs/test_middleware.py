from pathlib import Path

import pytest

from taskiq.message import TaskiqMessage
from taskiq.refs import (
    BLOB_MARKER_KEY,
    Blob,
    BlobNotFoundError,
    BlobRefMiddleware,
    BlobUploadError,
    LocalFileBlobStore,
    StoredBlob,
    digest_blob,
)
from taskiq.refs.middleware import collect_blobs, replace_markers, resolve_refs


def make_message(*args: object, **kwargs: object) -> TaskiqMessage:
    return TaskiqMessage(
        task_id="t1",
        task_name="task",
        labels={},
        args=list(args),
        kwargs=kwargs,
    )


class FailingStore:
    async def put(self, data: bytes) -> StoredBlob:
        msg = "blob service down"
        raise BlobUploadError(msg)


class SpyStore:
    def __init__(self) -> None:
        self.puts: list[bytes] = []

    async def put(self, data: bytes) -> StoredBlob:
        self.puts.append(data)
        return StoredBlob(
            ref=f"file:{digest_blob(data)}",
            length=len(data),
            digest=digest_blob(data),
        )


def test_collect_blobs_handles_nested_and_duplicate_positions() -> None:
    payload = b"x" * 100
    args = [
        Blob(payload),
        {"rows": [Blob(payload), (Blob(b"y"),)]},
        {"other": Blob(b"z")},
    ]

    found = collect_blobs(args)

    # BFS visits top-level positions before descending into containers.
    assert [blob.data for blob in found] == [payload, b"z", b"y"]
    assert collect_blobs(args[0])[0].data == payload


def test_collect_blobs_skips_ordinary_arguments() -> None:
    assert collect_blobs([1, "two", [3, {"k": None}]]) == []


async def test_replace_and_resolve_roundtrip() -> None:
    payload = b"big"
    spy = SpyStore()
    handle = await spy.put(payload)

    encoded = replace_markers(
        {"a": Blob(payload), "b": [Blob(payload)]},
        {Blob(payload): handle},
    )

    assert encoded["a"][BLOB_MARKER_KEY] == 1
    assert encoded["a"] == encoded["b"][0]

    decoded = resolve_refs(encoded, {handle.ref: payload})
    assert decoded == {"a": payload, "b": [payload]}


@pytest.fixture
def store(tmp_path: Path) -> LocalFileBlobStore:
    return LocalFileBlobStore(tmp_path / "blobs")


async def test_pre_send_externalizes_nested_args(store: LocalFileBlobStore) -> None:
    middleware = BlobRefMiddleware(store)
    payload = b"x" * 4096
    message = make_message(
        "normal",
        Blob(payload),
        {"data": [Blob(payload), Blob(b"second")]},
        kw=Blob(payload),
    )

    encoded = await middleware.pre_send(message)

    assert encoded.args[0] == "normal"
    assert encoded.args[1][BLOB_MARKER_KEY] == 1
    # Repeated content becomes one shared reference everywhere.
    assert encoded.args[1]["ref"] == encoded.kwargs["kw"]["ref"]
    assert encoded.args[2]["data"][0]["ref"] == encoded.args[1]["ref"]
    refs = [
        encoded.args[1]["ref"],
        encoded.args[2]["data"][1]["ref"],
    ]
    assert len(set(refs)) == 2
    # Message body stays small and JSON-safe.
    raw = encoded.model_dump_json()
    assert payload.decode("latin1") not in raw


async def test_pre_send_then_pre_execute_restores_arguments(
    store: LocalFileBlobStore,
) -> None:
    middleware = BlobRefMiddleware(store)
    message = make_message(
        Blob(b"one"),
        {"items": [Blob(b"two"), Blob(b"two")]},
        named=Blob(b"three"),
    )

    encoded = await middleware.pre_send(message)
    restored = await middleware.pre_execute(encoded)

    assert restored.args[0] == b"one"
    assert restored.args[1] == {"items": [b"two", b"two"]}
    assert restored.kwargs == {"named": b"three"}


async def test_plain_arguments_pass_through(store: LocalFileBlobStore) -> None:
    middleware = BlobRefMiddleware(store)
    message = make_message(1, "s", [1, {"k": "plain"}], none=None)

    encoded = await middleware.pre_send(message)
    restored = await middleware.pre_execute(encoded)

    assert restored.args[2][1]["k"] == "plain"
    assert restored.kwargs["none"] is None


async def test_upload_failure_aborts_before_send() -> None:
    middleware = BlobRefMiddleware(FailingStore())  # type: ignore[arg-type]
    message = make_message(Blob(b"never uploaded"))

    with pytest.raises(BlobUploadError):
        await middleware.pre_send(message)


async def test_missing_blob_blocks_execution(
    store: LocalFileBlobStore,
    tmp_path: Path,
) -> None:
    producer = BlobRefMiddleware(store)
    message = make_message(Blob(b"payload"))
    encoded = await producer.pre_send(message)

    other_store = LocalFileBlobStore(tmp_path / "other")
    consumer = BlobRefMiddleware(other_store)

    with pytest.raises(BlobNotFoundError):
        await consumer.pre_execute(encoded.model_copy(deep=True))


async def test_empty_blob_roundtrips(store: LocalFileBlobStore) -> None:
    middleware = BlobRefMiddleware(store)
    message = make_message(Blob(b""))

    encoded = await middleware.pre_send(message)
    assert encoded.args[0]["length"] == 0
    restored = await middleware.pre_execute(encoded)

    assert restored.args[0] == b""


async def test_cyclic_container_is_handled(store: LocalFileBlobStore) -> None:
    middleware = BlobRefMiddleware(store)
    cyclic: list[object] = [Blob(b"inside")]
    cyclic.append(cyclic)
    message = make_message(cyclic)

    encoded = await middleware.pre_send(message)
    restored = await middleware.pre_execute(encoded)

    assert restored.args[0][0] == b"inside"
    assert restored.args[0][1] is restored.args[0]
