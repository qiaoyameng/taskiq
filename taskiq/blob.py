from typing_extensions import Self


class Blob(bytes):
    """Explicitly mark a task parameter as blob-backed."""

    def __new__(cls, value: bytes) -> Self:
        """Create a marked bytes value."""
        return super().__new__(cls, value)

    def __repr__(self) -> str:
        return f"Blob({bytes(self)!r})"


BlobParameter = Blob
