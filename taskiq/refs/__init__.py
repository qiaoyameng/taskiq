"""Reference parameters: external blob storage for large task arguments."""

from taskiq.refs.file_store import LocalFileBlobStore
from taskiq.refs.middleware import (
    BlobRefMiddleware,
    collect_blobs,
    replace_markers,
    resolve_refs,
)
from taskiq.refs.protocol import (
    BLOB_MARKER_KEY,
    BLOB_PROTOCOL_VERSION,
    Blob,
    BlobIntegrityError,
    BlobNotFoundError,
    BlobRefError,
    BlobStore,
    BlobStoreError,
    BlobUploadError,
    StoredBlob,
    digest_blob,
    is_wire_ref,
    parse_wire_ref,
)

__all__ = (
    "BLOB_MARKER_KEY",
    "BLOB_PROTOCOL_VERSION",
    "Blob",
    "BlobIntegrityError",
    "BlobNotFoundError",
    "BlobRefError",
    "BlobRefMiddleware",
    "BlobStore",
    "BlobStoreError",
    "BlobUploadError",
    "LocalFileBlobStore",
    "StoredBlob",
    "collect_blobs",
    "digest_blob",
    "is_wire_ref",
    "parse_wire_ref",
    "replace_markers",
    "resolve_refs",
)
