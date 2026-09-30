"""Middleware that externalizes large arguments into a blob store."""

from __future__ import annotations

import asyncio
import contextlib
from collections import deque
from typing import Any

from taskiq.abc.middleware import TaskiqMiddleware
from taskiq.message import TaskiqMessage
from taskiq.refs.protocol import (
    Blob,
    BlobIntegrityError,
    BlobStore,
    StoredBlob,
    is_wire_ref,
    parse_wire_ref,
)


def _iter_containers(value: Any) -> Any:
    if isinstance(value, dict):
        return value.values()
    if isinstance(value, list | tuple | set | frozenset):
        return value
    return None


def collect_blobs(args: Any) -> list[Blob]:
    """
    Collect blob markers from arbitrarily nested arguments.

    Lists, tuples, sets and dict values are traversed recursively.
    Each distinct blob content is returned once, in first-seen order, so
    repeated references are uploaded a single time.

    :param args: root value (typically message args or kwargs).
    :return: de-duplicated blob markers found in the value tree.
    """
    found: list[Blob] = []
    seen: set[bytes] = set()
    queue: deque[Any] = deque([args])
    visited: set[int] = set()
    while queue:
        current = queue.popleft()
        if isinstance(current, Blob):
            if current.data not in seen:
                seen.add(current.data)
                found.append(current)
            continue
        items = _iter_containers(current)
        if items is None:
            continue
        marker = id(current)
        if marker in visited:
            continue
        visited.add(marker)
        queue.extend(items)
    return found


def replace_markers(value: Any, uploaded: dict[Blob, StoredBlob]) -> Any:
    """
    Recursively replace blob markers with wire references.

    :param value: root value to transform.
    :param uploaded: mapping from blob content to its stored handle.
    :return: transformed value with wire markers.
    """
    memo: dict[int, Any] = {}

    def transform(current: Any) -> Any:
        result: Any = current
        if isinstance(current, Blob):
            result = uploaded[current].to_wire()
        elif isinstance(current, dict):
            result = {key: transform(item) for key, item in current.items()}
        elif isinstance(current, list):
            marker = id(current)
            existing = memo.get(marker)
            if existing is not None:
                result = existing
            else:
                rebuilt: list[Any] = []
                memo[marker] = rebuilt
                rebuilt.extend(transform(item) for item in current)
                result = rebuilt
        elif isinstance(current, tuple):
            result = tuple(transform(item) for item in current)
        elif isinstance(current, (set, frozenset)):
            result = {transform(item) for item in current}
        return result

    return transform(value)


def resolve_refs(value: Any, fetched: dict[str, bytes]) -> Any:
    """
    Recursively restore wire references to raw blob bytes on a worker.

    Shared references (same ref appearing in multiple positions, or the
    same object referenced multiple times in one container) are restored
    to the same bytes object.

    :param value: root value decoded from the message.
    :param fetched: mapping from reference to verified content.
    :return: value with reference markers replaced by bytes.
    """
    memo: dict[int, Any] = {}

    def transform(current: Any) -> Any:
        if is_wire_ref(current):
            blob = parse_wire_ref(current)
            return fetched[blob.ref]
        if isinstance(current, dict):
            return {key: transform(item) for key, item in current.items()}
        if isinstance(current, list):
            marker = id(current)
            existing = memo.get(marker)
            if existing is not None:
                return existing
            rebuilt: list[Any] = []
            memo[marker] = rebuilt
            rebuilt.extend(transform(item) for item in current)
            return rebuilt
        if isinstance(current, tuple):
            return tuple(transform(item) for item in current)
        return current

    return transform(value)


class BlobRefMiddleware(TaskiqMiddleware):
    """
    Externalize arguments explicitly marked with :class:`Blob`.

    Client side (``pre_send``): every marker is uploaded before the
    message reaches the broker, then replaced by a compact reference
    containing digest and length.  Any upload failure aborts the send, so
    a message can never reference a missing object.

    Worker side (``pre_execute``): references are fetched and verified
    against the embedded digest and length before the task runs.  A read
    or verification failure raises and the task function is never
    invoked.

    Blobs are never deleted synchronously by send or execute hooks: a
    successful broker ack does not prove consumption, and ambiguous send
    results may still have delivered the message.  Stale objects are
    reclaimed asynchronously through :meth:`taskiq.refs.protocol.BlobStore.reap`.

    :param store: blob store backend used by both producer and consumer.
    :param reaper_interval: if set, run ``reap`` periodically after
        startup; None disables background garbage collection.
    :param orphan_grace_period: retention seconds for never-consumed
        objects (including uncertain sends).
    :param consumed_grace_period: retention seconds since last access for
        consumed objects, covering redelivery and delayed consumers.
    """

    def __init__(
        self,
        store: BlobStore,
        *,
        reaper_interval: float | None = None,
        orphan_grace_period: float = 900,
        consumed_grace_period: float = 3600,
    ) -> None:
        super().__init__()
        self.store = store
        self.reaper_interval = reaper_interval
        self.orphan_grace_period = orphan_grace_period
        self.consumed_grace_period = consumed_grace_period
        self._reaper_task: asyncio.Task[None] | None = None

    async def startup(self) -> None:
        """Start the optional background reaper loop."""
        if self.reaper_interval is not None and self._reaper_task is None:
            self._reaper_task = asyncio.create_task(self._reaper_loop())

    async def shutdown(self) -> None:
        """Stop the background reaper loop without deleting anything."""
        task = self._reaper_task
        if task is not None:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
            self._reaper_task = None

    async def _reaper_loop(self) -> None:
        interval = self.reaper_interval
        if interval is None:
            return
        while True:
            await asyncio.sleep(interval)
            await self.store.reap(
                orphan_grace_period=self.orphan_grace_period,
                consumed_grace_period=self.consumed_grace_period,
            )

    async def pre_send(self, message: TaskiqMessage) -> TaskiqMessage:
        """
        Upload all marked arguments and reference them from the message.

        :param message: outgoing message.
        :raises BlobStoreError: if any upload fails; the message is not
            sent in that case.
        :return: message carrying references instead of payloads.
        """
        blobs: list[Blob] = []
        for arg in message.args:
            blobs.extend(collect_blobs(arg))
        for value in message.kwargs.values():
            blobs.extend(collect_blobs(value))
        if not blobs:
            return message
        uploaded: dict[Blob, StoredBlob] = {}
        for blob in blobs:
            uploaded[blob] = await self.store.put(blob.data)
        message.args = [replace_markers(arg, uploaded) for arg in message.args]
        message.kwargs = {
            key: replace_markers(value, uploaded)
            for key, value in message.kwargs.items()
        }
        return message

    async def post_send(self, message: TaskiqMessage) -> None:
        """
        Observe a confirmed send.

        Blobs are intentionally not deleted here: broker acknowledgement
        only means the broker accepted the message, not that every
        possible copy has been consumed.  Retention is handled by
        ``reap`` after safe grace periods.

        :param message: kicked message.
        """

    async def pre_execute(self, message: TaskiqMessage) -> TaskiqMessage:
        """
        Fetch, verify and restore referenced arguments.

        :param message: incoming parsed message.
        :raises BlobStoreError: if a reference is malformed, missing or
            fails verification; the task does not run in that case.
        :return: message with raw bytes restored at every reference.
        """
        references = self._collect_references(message.args)
        references.update(self._collect_references(message.kwargs))
        if not references:
            return message
        fetched: dict[str, bytes] = {}
        for blob in references.values():
            fetched[blob.ref] = await self.store.get(blob)
        message.args = [resolve_refs(arg, fetched) for arg in message.args]
        message.kwargs = {
            key: resolve_refs(value, fetched) for key, value in message.kwargs.items()
        }
        return message

    @staticmethod
    def _collect_references(value: Any) -> dict[str, StoredBlob]:
        references: dict[str, StoredBlob] = {}
        stack: list[Any] = [value]
        visited: set[int] = set()
        while stack:
            current = stack.pop()
            if is_wire_ref(current):
                blob = parse_wire_ref(current)
                existing = references.get(blob.ref)
                if existing is not None and (
                    existing.digest != blob.digest or existing.length != blob.length
                ):
                    msg = f"conflicting references for the same blob: {blob.ref}"
                    raise BlobIntegrityError(msg)
                references[blob.ref] = blob
                continue
            items = _iter_containers(current)
            if items is None:
                continue
            marker = id(current)
            if marker in visited:
                continue
            visited.add(marker)
            stack.extend(items)
        return references

    async def reap_now(self) -> list[str]:
        """
        Run garbage collection immediately with configured grace periods.

        :return: references of deleted objects.
        """
        return await self.store.reap(
            orphan_grace_period=self.orphan_grace_period,
            consumed_grace_period=self.consumed_grace_period,
        )
