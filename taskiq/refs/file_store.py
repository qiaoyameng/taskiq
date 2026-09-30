"""Local filesystem blob store for reference parameters."""

from __future__ import annotations

import contextlib
import json
import os
import time
from pathlib import Path
from typing import Any

from taskiq.refs.protocol import (
    BlobIntegrityError,
    BlobNotFoundError,
    BlobRefError,
    BlobUploadError,
    Clock,
    StoredBlob,
    digest_blob,
)

REF_SCHEME = "file"
_META_SUFFIX = ".meta.json"


def _is_hex_digest(value: str) -> bool:
    if len(value) != 64:
        return False
    try:
        int(value, 16)
    except ValueError:
        return False
    return True


class LocalFileBlobStore:
    """
    Content-addressed blob store backed by a local directory.

    Objects are stored as immutable files named by their SHA-256 digest,
    so repeated references to identical content share one object.  A JSON
    sidecar keeps creation and last-access timestamps used by delayed
    garbage collection.

    :param root_dir: directory that holds the ``objects`` tree.
    :param clock: callable returning the current time in seconds;
        inject a fake clock in tests.
    """

    def __init__(
        self,
        root_dir: str | os.PathLike[str],
        *,
        clock: Clock | None = None,
    ) -> None:
        self.root_dir = Path(root_dir)
        self.objects_dir = self.root_dir / "objects"
        self._clock = clock or time.time

    def _object_path(self, digest: str) -> Path:
        if not _is_hex_digest(digest):
            msg = f"invalid file blob digest: {digest!r}"
            raise BlobRefError(msg)
        path = (self.objects_dir / digest[:2] / digest).resolve()
        objects_root = self.objects_dir.resolve()
        if objects_root not in path.parents:
            msg = f"blob reference escapes the store directory: {digest!r}"
            raise BlobRefError(msg)
        return path

    @staticmethod
    def _meta_path(object_path: Path) -> Path:
        return object_path.with_name(object_path.name + _META_SUFFIX)

    @staticmethod
    def _ref_for_digest(digest: str) -> str:
        return f"{REF_SCHEME}:{digest}"

    async def put(self, data: bytes) -> StoredBlob:
        """
        Store bytes under their content digest.

        Uploads are idempotent: identical content reuses the existing
        object.  Files are written atomically so partial objects are
        never observed by readers.

        :param data: raw blob content.
        :raises BlobUploadError: if the object cannot be written.
        :return: handle describing the stored object.
        """
        return self._put_sync(data)

    def _put_sync(self, data: bytes) -> StoredBlob:
        digest = digest_blob(data)
        object_path = self._object_path(digest)
        meta_path = self._meta_path(object_path)
        handle = StoredBlob(
            ref=self._ref_for_digest(digest),
            length=len(data),
            digest=digest,
        )
        try:
            object_path.parent.mkdir(parents=True, exist_ok=True)
            if not object_path.exists():
                tmp_path = object_path.with_name(f".{digest}.tmp-{os.getpid()}")
                with tmp_path.open("wb") as tmp_file:
                    tmp_file.write(data)
                    tmp_file.flush()
                    os.fsync(tmp_file.fileno())
                tmp_path.replace(object_path)
            if not meta_path.exists():
                now = self._clock()
                self._write_meta(meta_path, created_at=now, last_accessed_at=None)
        except OSError as exc:
            msg = f"cannot upload blob {digest}: {exc}"
            raise BlobUploadError(msg) from exc
        return handle

    @staticmethod
    def _write_meta(
        meta_path: Path,
        *,
        created_at: float,
        last_accessed_at: float | None,
    ) -> None:
        payload: dict[str, Any] = {
            "created_at": created_at,
            "last_accessed_at": last_accessed_at,
        }
        tmp_path = meta_path.with_name(f".{meta_path.name}.tmp-{os.getpid()}")
        with tmp_path.open("w", encoding="utf-8") as tmp_file:
            json.dump(payload, tmp_file)
        tmp_path.replace(meta_path)

    def _parse_ref(self, blob: StoredBlob) -> str:
        scheme, separator, digest = blob.ref.partition(":")
        if not separator or scheme != REF_SCHEME:
            msg = f"unsupported blob reference scheme: {blob.ref!r}"
            raise BlobRefError(msg)
        return digest

    async def get(self, blob: StoredBlob) -> bytes:
        """
        Read a referenced object and verify length and digest.

        A successful read updates the object's last-access timestamp,
        which keeps it alive across message redelivery and delayed
        consumption.

        :param blob: reference carried by the task message.
        :raises BlobNotFoundError: if the object is missing/unreadable.
        :raises BlobIntegrityError: if length or digest does not match.
        :raises BlobRefError: if the reference is malformed.
        :return: verified raw content.
        """
        return self._get_sync(blob)

    def _get_sync(self, blob: StoredBlob) -> bytes:
        digest = self._parse_ref(blob)
        object_path = self._object_path(digest)
        try:
            data = object_path.read_bytes()
        except FileNotFoundError as exc:
            msg = f"blob not found: {blob.ref}"
            raise BlobNotFoundError(msg) from exc
        except OSError as exc:
            msg = f"blob cannot be read: {blob.ref}: {exc}"
            raise BlobNotFoundError(msg) from exc
        if len(data) != blob.length:
            msg = (
                f"blob length mismatch for {blob.ref}: "
                f"expected {blob.length}, got {len(data)}"
            )
            raise BlobIntegrityError(msg)
        actual_digest = digest_blob(data)
        if actual_digest != blob.digest or actual_digest != digest:
            msg = f"blob digest mismatch for {blob.ref}"
            raise BlobIntegrityError(msg)
        with contextlib.suppress(OSError):
            # Best effort: the verified data is returned even if tracking
            # the access time fails.
            self._touch_meta(object_path)
        return data

    def _touch_meta(self, object_path: Path) -> None:
        meta_path = self._meta_path(object_path)
        created_at = self._clock()
        if meta_path.exists():
            with meta_path.open(encoding="utf-8") as meta_file:
                metadata = json.load(meta_file)
            created_at = float(metadata.get("created_at", created_at))
        self._write_meta(
            meta_path,
            created_at=created_at,
            last_accessed_at=self._clock(),
        )

    async def reap(
        self,
        *,
        orphan_grace_period: float,
        consumed_grace_period: float,
    ) -> list[str]:
        """
        Delete objects only after safe grace periods.

        Never-consumed objects (e.g. blobs whose send result is unknown)
        survive ``orphan_grace_period``.  Consumed objects survive
        ``consumed_grace_period`` since their *last* access, so repeated
        delivery and delayed consumers cannot observe premature deletion.

        :param orphan_grace_period: retention seconds for unaccessed
            objects.
        :param consumed_grace_period: retention seconds after last access
            for consumed objects.
        :return: references of deleted objects.
        """
        return self._reap_sync(orphan_grace_period, consumed_grace_period)

    def _reap_sync(
        self,
        orphan_grace_period: float,
        consumed_grace_period: float,
    ) -> list[str]:
        if not self.objects_dir.exists():
            return []
        now = self._clock()
        deleted: list[str] = []
        for object_path in sorted(self.objects_dir.glob("*/*")):
            if object_path.name.endswith(_META_SUFFIX):
                continue
            created_at, last_accessed_at = self._read_meta(object_path)
            if last_accessed_at is not None:
                expired = now - last_accessed_at >= consumed_grace_period
            else:
                expired = now - created_at >= orphan_grace_period
            if not expired:
                continue
            digest = object_path.name
            object_path.unlink(missing_ok=True)
            self._meta_path(object_path).unlink(missing_ok=True)
            deleted.append(self._ref_for_digest(digest))
        return deleted

    def _read_meta(self, object_path: Path) -> tuple[float, float | None]:
        meta_path = self._meta_path(object_path)
        created_at = object_path.stat().st_mtime
        last_accessed_at: float | None = None
        if meta_path.exists():
            try:
                with meta_path.open(encoding="utf-8") as meta_file:
                    metadata = json.load(meta_file)
                created_at = float(metadata["created_at"])
                raw_access = metadata.get("last_accessed_at")
                last_accessed_at = None if raw_access is None else float(raw_access)
            except (OSError, ValueError, KeyError, json.JSONDecodeError):
                pass
        return created_at, last_accessed_at
