"""Reference-parameter protocol.

Large, explicitly marked arguments are not embedded into the task
message.  They are uploaded to an external *blob store* and the
message only carries a reference plus a content summary.  The worker
validates the summary while restoring the argument.

This module defines the wire marker, the in-process marker passed by
callers and the :class:`BlobStore` protocol every backend implements.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from hashlib import sha256
from typing import Any, Protocol, runtime_checkable

from pydantic import BaseModel, ConfigDict

#: Key that marks an argument value as a blob reference on the wire.
BLOB_MARKER_KEY = "__taskiq_blob__"
#: Protocol version of the reference payload.
BLOB_PROTOCOL_VERSION = 1


def digest_blob(data: bytes) -> str:
    """
    Compute the content summary (SHA-256 hex digest) of blob data.

    :param data: raw blob content.
    :return: hex-encoded SHA-256 digest.
    """
    return sha256(data).hexdigest()


class Blob(BaseModel):
    """
    Explicit marker for a large argument stored outside the message.

    Callers wrap arguments in ``Blob`` to request external storage::

        await task.kiq(Blob(huge_payload))

    Only positions containing a marker are externalized; ordinary
    arguments are serialized into the message as usual.

    :param data: raw argument bytes.
    """

    model_config = ConfigDict(frozen=True)

    data: bytes

    def __init__(self, data: bytes = b"") -> None:
        super().__init__(data=data)

    def model_dump(self, *args: Any, **kwargs: Any) -> Blob:  # type: ignore[override]
        """Keep the marker intact while the kicker prepares the message."""
        return self

    @property
    def digest(self) -> str:
        """
        Get the content summary of the wrapped data.

        :return: hex-encoded SHA-256 digest.
        """
        return digest_blob(self.data)

    @property
    def length(self) -> int:
        """
        Get the size of the wrapped data in bytes.

        :return: content length.
        """
        return len(self.data)


class BlobStoreError(Exception):
    """Base error for blob store operations."""


class BlobUploadError(BlobStoreError):
    """Raised when a blob cannot be uploaded; the task message is not sent."""


class BlobNotFoundError(BlobStoreError):
    """Raised when a referenced blob is missing or unreadable."""


class BlobIntegrityError(BlobStoreError):
    """Raised when fetched blob content does not match the reference."""


class BlobRefError(BlobStoreError):
    """Raised when a wire reference is malformed."""


@dataclass(frozen=True)
class StoredBlob:
    """
    Handle returned by a blob store after a successful upload.

    :param ref: opaque store-specific reference used to fetch the blob.
    :param length: stored content length in bytes.
    :param digest: hex-encoded SHA-256 digest of the stored content.
    """

    ref: str
    length: int
    digest: str

    def to_wire(self) -> dict[str, Any]:
        """
        Serialize the handle into a JSON-safe wire marker.

        :return: marker dict carried inside the task message.
        """
        return {
            BLOB_MARKER_KEY: BLOB_PROTOCOL_VERSION,
            "ref": self.ref,
            "length": self.length,
            "digest": self.digest,
        }


def parse_wire_ref(value: Any) -> StoredBlob:
    """
    Parse and validate a wire marker into a :class:`StoredBlob`.

    :param value: value found in a decoded task message.
    :raises BlobRefError: if the value is not a well-formed marker.
    :return: the parsed stored-blob handle.
    """
    if not isinstance(value, dict) or BLOB_MARKER_KEY not in value:
        msg = "not a blob reference"
        raise BlobRefError(msg)
    version = value[BLOB_MARKER_KEY]
    if version != BLOB_PROTOCOL_VERSION:
        msg = f"unsupported blob protocol version: {version!r}"
        raise BlobRefError(msg)
    ref = value.get("ref")
    digest = value.get("digest")
    length = value.get("length")
    if not isinstance(ref, str) or not ref:
        msg = "blob reference must contain a non-empty 'ref'"
        raise BlobRefError(msg)
    if not isinstance(digest, str) or len(digest) != 64:
        msg = "blob reference must contain a 64-char hex 'digest'"
        raise BlobRefError(msg)
    try:
        int(digest, 16)
    except ValueError as exc:
        msg = "blob digest is not hex-encoded"
        raise BlobRefError(msg) from exc
    if not isinstance(length, int) or isinstance(length, bool) or length < 0:
        msg = "blob reference must contain a non-negative integer 'length'"
        raise BlobRefError(msg)
    return StoredBlob(ref=ref, length=length, digest=digest)


def is_wire_ref(value: Any) -> bool:
    """
    Check whether a decoded value is a blob wire marker.

    :param value: value found in a decoded task message.
    :return: True if the value carries the blob marker key.
    """
    return isinstance(value, dict) and BLOB_MARKER_KEY in value


@runtime_checkable
class BlobStore(Protocol):
    """Storage backend for reference parameters."""

    async def put(self, data: bytes) -> StoredBlob:
        """
        Upload blob content and return a fetchable handle.

        Implementations should be idempotent for identical content so
        that repeated references reuse the same object.

        :param data: raw blob content.
        :raises BlobUploadError: if the content cannot be stored.
        :return: handle describing where the blob was stored.
        """

    async def get(self, blob: StoredBlob) -> bytes:
        """
        Fetch and verify blob content.

        :param blob: reference carried by the task message.
        :raises BlobNotFoundError: if the object is missing.
        :raises BlobIntegrityError: if length or digest does not match.
        :return: verified raw content.
        """

    async def reap(
        self,
        *,
        orphan_grace_period: float,
        consumed_grace_period: float,
    ) -> list[str]:
        """
        Delete stale objects after safe grace periods.

        Objects that were never consumed are treated as possible orphans
        from failed/uncertain sends; objects consumed at least once are
        kept until long after their last access to survive redelivery
        and delayed consumption.

        :param orphan_grace_period: seconds an unaccessed object must be
            retained before deletion.
        :param consumed_grace_period: seconds since last access a
            consumed object must be retained before deletion.
        :return: references of deleted objects.
        """


Clock = Callable[[], float]
