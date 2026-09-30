import hashlib
import os
from datetime import timedelta
from pathlib import Path, PurePosixPath
from time import time
from urllib.parse import unquote, urlparse
from uuid import uuid4

from taskiq.abc.blob_store import AsyncBlobStore
from taskiq.exceptions import BlobStoreError


class LocalFileBlobStore(AsyncBlobStore):
    """Store referenced parameters as content-addressed local files."""

    def __init__(
        self,
        root: str | Path,
        *,
        retention_period: timedelta | float = timedelta(hours=24),
        shard_depth: int = 2,
    ) -> None:
        if shard_depth < 0:
            raise ValueError("shard_depth cannot be negative.")
        self.root = Path(root).expanduser().resolve()
        if isinstance(retention_period, timedelta):
            self.retention_seconds = retention_period.total_seconds()
        else:
            self.retention_seconds = float(retention_period)
        if self.retention_seconds < 0:
            raise ValueError("retention_period cannot be negative.")
        self.shard_depth = shard_depth

    async def startup(self) -> None:
        """Create the storage directory."""
        self.root.mkdir(0o700, True, True)

    async def put(self, digest: str, data: bytes) -> str:
        """Store one object atomically unless an identical object exists."""
        path = self._path_for_digest(digest)
        try:
            self.root.mkdir(0o700, True, True)
            exists = path.exists()
            if exists:
                try:
                    existing = self._read_file(path)
                except FileNotFoundError:
                    pass
                else:
                    self._verify_data(existing, digest, len(data), path)
                    return path.as_uri()

            path.parent.mkdir(0o700, True, True)
            temp_name = f".{digest}.{os.getpid()}.{uuid4().hex}.tmp"
            temp_path = path.with_name(temp_name)
            self._write_file(temp_path, data)
            self._publish_temp_file(temp_path, path, data, digest)
            return path.as_uri()
        except BlobStoreError:
            raise
        except OSError as exc:
            raise BlobStoreError(
                operation="put",
                uri=path.as_uri() if path.is_absolute() else str(path),
            ) from exc

    async def get(self, uri: str, digest: str) -> bytes:
        """Read an object and ensure URI, digest, and file location are valid."""
        path = self._path_from_uri(uri, digest)
        try:
            data = self._read_file(path)
        except (FileNotFoundError, OSError) as exc:
            raise BlobStoreError(operation="get", uri=uri) from exc
        actual_digest = hashlib.sha256(data).hexdigest()
        if actual_digest != digest:
            raise BlobStoreError(operation="get", uri=uri)
        os.utime(path, None)
        return data

    async def cleanup_expired(self) -> list[str]:
        """
        Remove objects older than the retention period.

        Successful reads renew an object's timestamp, so delayed or repeated
        deliveries do not lose an object while it is still being consumed.
        """
        removed: list[str] = []
        cutoff = time() - self.retention_seconds

        def remove_expired() -> None:
            if not self.root.exists():
                return
            for path in self.root.rglob("*"):
                if (
                    not path.is_file()
                    or path.name.startswith(".")
                    or not self._is_digest(path.name)
                ):
                    continue
                try:
                    if path.stat().st_mtime <= cutoff:
                        path.unlink()
                        removed.append(path.as_uri())
                except (OSError, ValueError):
                    continue

        remove_expired()
        return removed

    @staticmethod
    def _write_file(path: Path, data: bytes) -> None:
        with path.open("xb") as file:
            file.write(data)
            file.flush()
            os.fsync(file.fileno())
        path.chmod(0o600)

    @staticmethod
    def _read_file(path: Path) -> bytes:
        return path.read_bytes()

    @staticmethod
    def _publish_temp_file(
        temp_path: Path,
        path: Path,
        data: bytes,
        digest: str,
    ) -> None:
        try:
            os.link(temp_path, path)
        except FileExistsError:
            existing = LocalFileBlobStore._read_file(path)
            LocalFileBlobStore._verify_data(
                existing,
                digest,
                len(data),
                path,
            )
        finally:
            temp_path.unlink(True)

    @staticmethod
    def _verify_data(
        data: bytes,
        digest: str,
        length: int,
        path: Path,
    ) -> None:
        if len(data) != length or hashlib.sha256(data).hexdigest() != digest:
            raise BlobStoreError(operation="put", uri=path.as_uri())

    def _path_from_uri(self, uri: str, digest: str) -> Path:
        parsed = urlparse(uri)
        if parsed.scheme != "file":
            raise BlobStoreError(operation="get", uri=uri)
        raw_path = unquote(parsed.path)
        if PurePosixPath(raw_path).name != digest:
            raise BlobStoreError(operation="get", uri=uri)
        path = Path(raw_path).resolve()
        expected = self._path_for_digest(digest).resolve()
        try:
            path.relative_to(self.root)
        except ValueError as exc:
            raise BlobStoreError(operation="get", uri=uri) from exc
        if path != expected:
            raise BlobStoreError(operation="get", uri=uri)
        return path

    def _is_digest(self, value: str) -> bool:
        """Return whether a file name is a SHA-256 digest."""
        return len(value) == 64 and all(
            character in "0123456789abcdef" for character in value
        )

    def _path_for_digest(self, digest: str) -> Path:
        if not self._is_digest(digest):
            raise BlobStoreError(operation="path", uri=digest)
        parts = [
            digest[index : index + 2] for index in range(0, self.shard_depth * 2, 2)
        ]
        return self.root.joinpath(*parts, digest)
