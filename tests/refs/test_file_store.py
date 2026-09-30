from pathlib import Path
from typing import Any

import pytest

from taskiq.refs import (
    BlobIntegrityError,
    BlobNotFoundError,
    BlobRefError,
    BlobUploadError,
    LocalFileBlobStore,
    StoredBlob,
)


class FakeClock:
    """Controllable clock for retention tests."""

    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def store(tmp_path: Path, clock: FakeClock) -> LocalFileBlobStore:
    return LocalFileBlobStore(tmp_path / "blobs", clock=clock)


async def test_put_get_roundtrip(store: LocalFileBlobStore) -> None:
    data = b"x" * 1024
    handle = await store.put(data)

    assert handle.length == len(data)
    assert handle.ref == f"file:{handle.digest}"
    assert await store.get(handle) == data


async def test_identical_content_is_deduplicated(
    store: LocalFileBlobStore,
) -> None:
    data = b"same"
    first = await store.put(data)
    second = await store.put(data)

    assert first == second
    data_files = [
        path
        for path in store.objects_dir.glob("*/*")
        if not path.name.endswith(".meta.json")
    ]
    assert len(data_files) == 1


async def test_get_missing_blob_raises(store: LocalFileBlobStore) -> None:
    handle = await store.put(b"data")
    for path in store.objects_dir.glob("*/*"):
        if not path.name.endswith(".meta.json"):
            path.unlink()

    with pytest.raises(BlobNotFoundError):
        await store.get(handle)


async def test_get_detects_length_tampering(store: LocalFileBlobStore) -> None:
    handle = await store.put(b"data")
    tampered = StoredBlob(
        ref=handle.ref,
        length=5,
        digest=handle.digest,
    )

    with pytest.raises(BlobIntegrityError):
        await store.get(tampered)


async def test_get_detects_digest_mismatch(store: LocalFileBlobStore) -> None:
    handle = await store.put(b"data")
    tampered = StoredBlob(
        ref=handle.ref,
        length=4,
        digest="c" * 64,
    )

    with pytest.raises(BlobIntegrityError):
        await store.get(tampered)


async def test_rejects_unknown_scheme(store: LocalFileBlobStore) -> None:
    handle = await store.put(b"data")
    forged = StoredBlob(
        ref="s3:bucket/key",
        length=4,
        digest=handle.digest,
    )

    with pytest.raises(BlobRefError):
        await store.get(forged)


async def test_rejects_path_escaping_reference(store: LocalFileBlobStore) -> None:
    forged = StoredBlob(
        ref="file:../../etc/passwd",
        length=4,
        digest="d" * 64,
    )

    with pytest.raises(BlobRefError):
        await store.get(forged)


async def test_reap_keeps_fresh_orphans(
    store: LocalFileBlobStore,
    clock: FakeClock,
) -> None:
    handle = await store.put(b"orphan")
    clock.now += 100

    deleted = await store.reap(orphan_grace_period=300, consumed_grace_period=600)

    assert deleted == []
    assert await store.get(handle) == b"orphan"


async def test_reap_deletes_old_orphans(
    store: LocalFileBlobStore,
    clock: FakeClock,
) -> None:
    handle = await store.put(b"orphan")
    clock.now += 301

    deleted = await store.reap(orphan_grace_period=300, consumed_grace_period=600)

    assert deleted == [handle.ref]
    with pytest.raises(BlobNotFoundError):
        await store.get(handle)


async def test_reap_keeps_consumed_blobs_until_grace_after_last_access(
    store: LocalFileBlobStore,
    clock: FakeClock,
) -> None:
    handle = await store.put(b"payload")
    clock.now += 400
    await store.get(handle)  # first delivery consumed
    clock.now += 599  # redelivery still within consumed grace window

    deleted = await store.reap(orphan_grace_period=300, consumed_grace_period=600)

    assert deleted == []
    # A redelivery at this point must still find the object; reading it
    # refreshes the last-access timestamp.
    assert await store.get(handle) == b"payload"

    clock.now += 601  # last access is now 601s ago
    deleted = await store.reap(orphan_grace_period=300, consumed_grace_period=600)
    assert deleted == [handle.ref]


async def test_delayed_consumer_refreshes_retention(
    store: LocalFileBlobStore,
    clock: FakeClock,
) -> None:
    handle = await store.put(b"payload")
    clock.now += 250
    await store.get(handle)  # first consumer
    clock.now += 500
    await store.get(handle)  # very late redelivery refreshes last access
    clock.now += 599

    deleted = await store.reap(orphan_grace_period=300, consumed_grace_period=600)

    assert deleted == []


async def test_upload_failure_is_reported(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = LocalFileBlobStore(tmp_path / "blobs")

    def boom(*args: Any, **kwargs: Any) -> None:
        msg = "read only"
        raise PermissionError(msg)

    monkeypatch.setattr(Path, "mkdir", boom)

    with pytest.raises(BlobUploadError):
        await store.put(b"data")
