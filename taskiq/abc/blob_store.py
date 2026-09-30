from abc import ABC, abstractmethod


class AsyncBlobStore(ABC):
    """Storage for large task parameters referenced from broker messages."""

    async def startup(self) -> None:  # noqa: B027
        """Prepare blob storage for use."""

    async def shutdown(self) -> None:  # noqa: B027
        """Release blob storage resources."""

    @abstractmethod
    async def put(self, digest: str, data: bytes) -> str:
        """
        Store data and return its URI.

        Implementations must be idempotent for identical digests and must not
        delete an existing object after an uncertain write result.
        """

    @abstractmethod
    async def get(self, uri: str, digest: str) -> bytes:
        """
        Fetch and verify data for a URI.

        The digest and length supplied by the caller are checked before data is
        returned to task execution.
        """
