from collections.abc import Iterable
from dataclasses import dataclass
from enum import Enum
from logging import getLogger
from typing import Any

from taskiq.abc.broker import AsyncBroker
from taskiq.abc.middleware import TaskiqMiddleware
from taskiq.exceptions import BatchSendError
from taskiq.kicker import AsyncKicker
from taskiq.message import BrokerMessage, TaskiqMessage
from taskiq.task import AsyncTaskiqTask
from taskiq.utils import maybe_awaitable

logger = getLogger("taskiq.batch")


class BatchItemStatus(str, Enum):
    """Delivery status of a single message in a batch."""

    ACCEPTED = "accepted"
    REJECTED = "rejected"
    UNKNOWN = "unknown"


@dataclass
class BatchReceipt:
    """
    Delivery receipt for a single message of a batch.

    Receipts are position-aligned with the order in which
    tasks were added to the batch (or passed to `retry`).

    Status semantics:

    * ACCEPTED - the broker has confirmed the delivery. The `task`
      attribute can be used to await the result.
    * REJECTED - the message was definitively NOT sent (local
      serialization/validation failure or an explicit broker
      rejection). It is safe to retry it.
    * UNKNOWN - the send attempt was interrupted (e.g. the connection
      was lost while awaiting the confirmation). The broker might have
      received the message, so it must NOT be treated as "not sent".
      Retrying it may produce duplicates; taskiq does NOT guarantee
      deduplication - it depends on the broker.
    """

    task_id: str
    task_name: str
    status: BatchItemStatus
    error: "str | None" = None
    task: "AsyncTaskiqTask[Any] | None" = None

    @property
    def ok(self) -> bool:
        """Whether the message was accepted by the broker."""
        return self.status is BatchItemStatus.ACCEPTED


@dataclass
class _BatchEntry:
    """A single collected call with its prepared state."""

    kicker: AsyncKicker[Any, Any]
    args: tuple[Any, ...]
    kwargs: dict[str, Any]
    task_id: "str | None" = None
    message: "TaskiqMessage | None" = None
    broker_message: "BrokerMessage | None" = None


class BatchKicker:
    """
    Kicker for heterogeneous groups of tasks.

    It collects calls of different tasks and sends them in one batch,
    returning a `BatchReceipt` for every message. Receipts are
    position-aligned with the order in which tasks were added.

    Delivery semantics:

    * Every message is fully serialized and validated BEFORE anything
      is sent. Messages that fail validation get a REJECTED receipt,
      are never sent and do not block other messages.
    * If the broker overrides `AsyncBroker.kick_batch`, the whole batch
      is published with a single native call.
    * Otherwise a PER-MESSAGE FALLBACK is used: messages are kicked one
      by one. A failure of one message preserves previously accepted
      receipts and does not stop the remaining ones.
    * When a send attempt is interrupted (e.g. the connection drops),
      the message is reported as UNKNOWN, never as "not sent".

    Retries: use `retry` to resend selected items. Retried messages
    keep their ORIGINAL task_ids. Note that taskiq does NOT guarantee
    deduplication: whether a redelivered task_id is deduplicated
    depends entirely on the broker.
    """

    def __init__(self, broker: AsyncBroker) -> None:
        self.broker = broker
        self._entries: list[_BatchEntry] = []

    def __len__(self) -> int:
        return len(self._entries)

    def add(
        self,
        kicker: AsyncKicker[Any, Any],
        *args: Any,
        **kwargs: Any,
    ) -> "BatchKicker":
        """
        Add a task call to the batch.

        :param kicker: kicker of the task to call (e.g. `my_task.kicker()`).
        :param args: positional arguments of the call.
        :param kwargs: keyword arguments of the call.
        :return: self, to allow chaining.
        """
        self._entries.append(
            _BatchEntry(kicker=kicker, args=args, kwargs=kwargs),
        )
        return self

    async def kiq(self) -> list[BatchReceipt]:
        """
        Send all collected tasks.

        :return: receipts, position-aligned with the order of `add` calls.
        """
        return await self._send(list(range(len(self._entries))))

    async def retry(self, indices: Iterable[int]) -> list[BatchReceipt]:
        """
        Resend selected items with their ORIGINAL task_ids.

        Only the given positions are resent; previously accepted
        results are kept as is. Beware that redelivery may produce
        duplicates - deduplication is broker-specific and is NOT
        guaranteed by taskiq.

        :param indices: positions of the batch items to resend.
        :raises IndexError: if an index is out of range.
        :return: receipts, position-aligned with the given indices.
        """
        normalized = list(indices)
        for index in normalized:
            if not 0 <= index < len(self._entries):
                raise IndexError(f"Batch index out of range: {index}")
        return await self._send(normalized)

    async def _send(self, indices: list[int]) -> list[BatchReceipt]:
        """
        Prepare, validate and send the selected entries.

        :param indices: positions of entries to send.
        :return: receipts aligned with the given indices.
        """
        receipts: dict[int, BatchReceipt] = {}
        sendable: list[tuple[int, BrokerMessage]] = []

        # Phase 1: serialize and validate EVERY message BEFORE sending.
        seen: set[int] = set()
        for index in indices:
            if index in seen:
                continue
            seen.add(index)
            entry = self._entries[index]
            try:
                broker_message = await self._prepare(entry)
            except Exception as exc:
                logger.warning(
                    "Batch item %d (%s) failed validation: %s",
                    index,
                    entry.kicker.task_name,
                    exc,
                )
                receipts[index] = self._receipt(
                    index,
                    BatchItemStatus.REJECTED,
                    error=f"{type(exc).__name__}: {exc}",
                )
            else:
                sendable.append((index, broker_message))

        # Phase 2: send everything that passed validation.
        if sendable:
            if self._supports_native_batch():
                await self._send_native(sendable, receipts)
            else:
                await self._send_fallback(sendable, receipts)

        return [receipts[index] for index in indices]

    async def _prepare(self, entry: _BatchEntry) -> BrokerMessage:
        """
        Build and serialize the message of an entry.

        The task_id is assigned once and reused on retries,
        so resent messages keep their original task_id.

        :param entry: entry to prepare.
        :return: serialized broker message.
        """
        if entry.broker_message is not None:
            return entry.broker_message
        if entry.task_id is None:
            entry.task_id = entry.kicker.custom_task_id or self.broker.id_generator()
        message: TaskiqMessage = entry.kicker._prepare_message(  # noqa: SLF001
            *entry.args,
            **entry.kwargs,
        )
        message.task_id = entry.task_id
        for middleware in self.broker.middlewares:
            if middleware.__class__.pre_send != TaskiqMiddleware.pre_send:
                message = await maybe_awaitable(middleware.pre_send(message))
        entry.message = message
        entry.broker_message = self.broker.formatter.dumps(message)
        return entry.broker_message

    def _supports_native_batch(self) -> bool:
        """Whether the broker overrides the default per-message fallback."""
        return type(self.broker).kick_batch is not AsyncBroker.kick_batch

    async def _send_native(
        self,
        sendable: list[tuple[int, BrokerMessage]],
        receipts: dict[int, BatchReceipt],
    ) -> None:
        """
        Send messages using the broker's native batch implementation.

        :param sendable: entries and messages to send.
        :param receipts: receipts dict to fill.
        """
        positions = [index for index, _ in sendable]
        messages = [message for _, message in sendable]
        try:
            await self.broker.kick_batch(messages)
        except BatchSendError as exc:
            accepted = set(exc.accepted)
            rejected = dict(exc.rejected)
        except Exception as exc:
            # The connection might have been lost at any point of the
            # batch publish, so the outcome of EVERY item is unknown.
            logger.warning("Native batch send failed: %s", exc)
            for index in positions:
                receipts[index] = self._receipt(
                    index,
                    BatchItemStatus.UNKNOWN,
                    error=f"{type(exc).__name__}: {exc}",
                )
            return
        else:
            accepted = set(range(len(messages)))
            rejected = {}
        for position, index in enumerate(positions):
            if position in accepted:
                receipts[index] = await self._accepted_receipt(index)
            elif position in rejected:
                receipts[index] = self._receipt(
                    index,
                    BatchItemStatus.REJECTED,
                    error=rejected[position],
                )
            else:
                receipts[index] = self._receipt(
                    index,
                    BatchItemStatus.UNKNOWN,
                    error="delivery was not confirmed by the broker",
                )

    async def _send_fallback(
        self,
        sendable: list[tuple[int, BrokerMessage]],
        receipts: dict[int, BatchReceipt],
    ) -> None:
        """
        PER-MESSAGE FALLBACK: kick messages one by one.

        Mirrors the default `AsyncBroker.kick_batch`. A failure of one
        message preserves previously accepted receipts and does not
        stop the remaining ones.

        :param sendable: entries and messages to send.
        :param receipts: receipts dict to fill.
        """
        for index, message in sendable:
            try:
                await self.broker.kick(message)
            except Exception as exc:
                # The broker might have received the message before the
                # failure (e.g. the connection dropped while awaiting
                # the confirmation), so the outcome is UNKNOWN - it
                # must never be reported as "not sent".
                logger.warning(
                    "Batch item %d was not confirmed: %s",
                    index,
                    exc,
                )
                receipts[index] = self._receipt(
                    index,
                    BatchItemStatus.UNKNOWN,
                    error=f"{type(exc).__name__}: {exc}",
                )
                continue
            receipts[index] = await self._accepted_receipt(index)

    async def _accepted_receipt(self, index: int) -> BatchReceipt:
        """
        Build a receipt for an accepted message and run post_send hooks.

        :param index: position of the entry.
        :return: ACCEPTED receipt with an awaitable task.
        """
        entry = self._entries[index]
        if entry.message is not None:
            for middleware in reversed(self.broker.middlewares):
                if middleware.__class__.post_send != TaskiqMiddleware.post_send:
                    await maybe_awaitable(middleware.post_send(entry.message))
        task = AsyncTaskiqTask(
            task_id=entry.task_id or "",
            result_backend=self.broker.result_backend,
            return_type=entry.kicker.return_type,
        )
        return self._receipt(index, BatchItemStatus.ACCEPTED, task=task)

    def _receipt(
        self,
        index: int,
        status: BatchItemStatus,
        error: "str | None" = None,
        task: "AsyncTaskiqTask[Any] | None" = None,
    ) -> BatchReceipt:
        """
        Build a receipt for an entry.

        :param index: position of the entry.
        :param status: delivery status.
        :param error: optional error description.
        :param task: optional task handle for accepted messages.
        :return: receipt for the entry.
        """
        entry = self._entries[index]
        return BatchReceipt(
            task_id=entry.task_id or "",
            task_name=entry.kicker.task_name,
            status=status,
            error=error,
            task=task,
        )
