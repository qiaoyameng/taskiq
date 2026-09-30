import hashlib
from typing import Any

from pydantic import BaseModel

from taskiq.abc.blob_store import AsyncBlobStore

BLOB_PLACEHOLDER_KEY = "__taskiq_blob__"


class BlobReference(BaseModel):
    """Reference to a parameter value stored outside the broker message."""

    uri: str
    digest: str
    length: int


def is_blob_placeholder(value: Any) -> bool:
    """Return whether a JSON-like value is a blob placeholder."""
    return isinstance(value, dict) and len(value) == 1 and BLOB_PLACEHOLDER_KEY in value


def encode_blob_value(
    value: bytes,
) -> tuple[str, bytes]:
    """Calculate the content digest for raw blob bytes."""
    data = bytes(value)
    digest = hashlib.sha256(data).hexdigest()
    return digest, data


async def restore_blobs(
    value: Any,
    references: dict[str, BlobReference],
    blob_store: AsyncBlobStore,
) -> Any:
    """Resolve blob placeholders recursively."""
    if is_blob_placeholder(value):
        digest = value[BLOB_PLACEHOLDER_KEY]
        reference = references.get(digest)
        if reference is None:
            raise ValueError(f"Missing blob reference for digest {digest}.")
        data = await blob_store.get(reference.uri, reference.digest)
        actual_digest = hashlib.sha256(data).hexdigest()
        if actual_digest != reference.digest:
            raise ValueError(
                f"Blob digest mismatch: expected {reference.digest}, "
                f"got {actual_digest}.",
            )
        if len(data) != reference.length:
            raise ValueError(
                f"Blob length mismatch: expected {reference.length}, got {len(data)}.",
            )
        return data
    if isinstance(value, list):
        return [await restore_blobs(item, references, blob_store) for item in value]
    if isinstance(value, tuple):
        restored = [await restore_blobs(item, references, blob_store) for item in value]
        return tuple(restored)
    if isinstance(value, dict):
        return {
            key: await restore_blobs(item, references, blob_store)
            for key, item in value.items()
        }
    return value


async def restore_message_blobs(
    message: Any,
    blob_store: AsyncBlobStore | None,
) -> None:
    """Restore all blob references in a message in place."""
    if not message.blob_refs:
        return
    if blob_store is None:
        raise ValueError(
            "Message contains blob references but no blob store is configured.",
        )
    message.args = [
        await restore_blobs(arg, message.blob_refs, blob_store) for arg in message.args
    ]
    message.kwargs = {
        key: await restore_blobs(value, message.blob_refs, blob_store)
        for key, value in message.kwargs.items()
    }
