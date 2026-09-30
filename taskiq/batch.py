"""Batch kicking support.

This module provides the :class:`BatchKicker`, which allows clients to
submit a group of heterogeneous tasks in a single call and get a
per-item receipt for every submitted entry.

Key guarantees:

* Every message is fully prepared and serialized **before** anything is
  sent. If a single argument cannot be serialized, no message of the
  batch touches the network. Such entries are reported as
  :attr:`BatchItemStatus.SERIALIZATION_ERROR`.
* Brokers may implement :meth:`AsyncBroker.kick_batch` to push the whole
  group natively. The default implementation falls back to calling
  :meth:`AsyncBroker.kick` one by one; the fallback is marked explicitly
  so callers always know how the batch was delivered.
* When something goes wrong, entries that were already accepted keep
  their :attr:`BatchItemStatus.ACCEPTED` result.
* If the broker connection drops or the outcome of a send is unknown,
  the entry is reported as :attr:`BatchItemStatus.UNKNOWN` -- never as
  "not sent", because the message may have been accepted by the broker.
* Entries can be retried using their original ``task_id`` via
  :meth:`BatchKicker.retry`. Taskiq does not promise broker-side
  deduplication: retrying an accepted entry may enqueue a duplicate.
"""

import enum
from collections.abc import Sequence
from dataclasses import dataclass
from logging import getLogger
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from taskiq.abc.middleware import TaskiqMiddleware
from taskiq.decor import AsyncTaskiqDecoratedTask
from taskiq.kicker import AsyncKicker
from taskiq.message import BrokerMessage, TaskiqMessage
from taskiq.task import AsyncTaskiqTask
from taskiq.utils import maybe_awaitable

if TYPE_CHECKING:  # pragma: no cover
    from taskiq.abc.broker import AsyncBroker

_T = TypeVar("_T")
logger = getLogger("taskiq")


class BatchItemStatus(str, enum.Enum):
    """Status of a single item within a batch."""

    ACCEPTED = "accepted"
    REJECTED = "rejected"
    UNKNOWN = "unknown"
    NOT_SENT = "not_sent"
    SERIALIZATION_ERROR = "serialization_error"


@dataclass(frozen=True)
class BrokerBatchReceipt:
    """Per-item outcome produced by a broker's batch implementation."""

    index: int
    status: BatchItemStatus
    error: Exception | None = None


@dataclass
class BatchItemReceipt(Generic[_T]):
    """Per-item receipt returned to batch callers."""

    index: int
    task_id: str
    status: BatchItemStatus
    error: Exception | None = None
    task: AsyncTaskiqTask[_T] | None = None

    @property
    def accepted(self) -> bool:
        """Whether the broker confirmed that it accepted the message."""
        return self.status == BatchItemStatus.ACCEPTED


@dataclass
class BatchKickResult:
    """Result of kicking a batch of tasks."""

    receipts: list[BatchItemReceipt[Any]]

    def __iter__(self) -> Any:
        return iter(self.receipts)

    def __len__(self) -> int:
        return len(self.receipts)

    def __getitem__(self, index: int) -> BatchItemReceipt[Any]:
        return self.receipts[index]

    @property
    def accepted(self) -> list[BatchItemReceipt[Any]]:
        """Receipts for messages that were accepted by the broker."""
        return [
            receipt
            for receipt in self.receipts
            if receipt.status == BatchItemStatus.ACCEPTED
        ]

    @property
    def failed(self) -> list[BatchItemReceipt[Any]]:
        """Receipts for messages that were not accepted."""
        return [
            receipt
            for receipt in self.receipts
            if receipt.status != BatchItemStatus.ACCEPTED
        ]


@dataclass
class _PendingItem:
    kicker: "AsyncKicker[Any, Any]"
    args: tuple[Any, ...]
    kwargs: dict[str, Any]


@dataclass
class _PreparedItem:
    original_index: int
    message: TaskiqMessage
    broker_message: BrokerMessage
    kicker: "AsyncKicker[Any, Any]"


class _SerializationError(Exception):
    """Internal signal carrying the receipt of a non-serializable item."""

    def __init__(self, receipt: BatchItemReceipt[Any]) -> None:
        super().__init__("Batch item failed serialization")
        self.receipt = receipt


class BatchKicker:
    """Collects heterogeneous tasks and kicks them as one batch."""

    def __init__(self, broker: "AsyncBroker | None" = None) -> None:
        self.broker = broker
        self._items: list[_PendingItem] = []

    def add(
        self,
        task: AsyncTaskiqDecoratedTask[Any, Any] | AsyncKicker[Any, Any],
        *args: Any,
        **kwargs: Any,
    ) -> "BatchKicker":
        """
        Add a task invocation to the batch.

        Heterogeneous tasks (different functions, args, labels, brokers)
        are supported. If this batch kicker was created with a broker,
        every added task must use that broker.

        :param task: decorated task or kicker to invoke.
        :param args: positional arguments of the task.
        :param kwargs: keyword arguments of the task.
        :return: self, for chaining.
        """
        kicker = self._get_kicker(task)
        if self.broker is not None and kicker.broker is not self.broker:
            raise ValueError("All tasks in the batch must use the same broker.")
        self._items.append(_PendingItem(kicker=kicker, args=args, kwargs=kwargs))
        return self

    async def kick(self) -> BatchKickResult:
        """
        Kick all collected tasks as a single batch.

        All messages are prepared and serialized before any of them is
        sent. The result contains one receipt per submitted item, placed
        at the same index as the corresponding :meth:`add` call.

        :return: per-item receipts in input order.
        """
        if not self._items:
            return BatchKickResult(receipts=[])

        broker = self.broker or self._items[0].kicker.broker
        receipts = self._blank_receipts()
        # Phase 1: build and serialize every message. Nothing is sent yet.
        prepared = await self._prepare_messages(broker, receipts)

        if not prepared:
            return BatchKickResult(receipts=receipts)

        # Phase 2: send the serialized group and map outcomes back.
        broker_receipts = await self._send_messages(broker, prepared)
        await self._apply_receipts(broker, prepared, broker_receipts, receipts)
        return BatchKickResult(receipts=receipts)

    def _blank_receipts(self) -> list[BatchItemReceipt[Any]]:
        return [
            BatchItemReceipt(
                index=index,
                task_id="",
                status=BatchItemStatus.NOT_SENT,
            )
            for index in range(len(self._items))
        ]

    async def _prepare_messages(
        self,
        broker: "AsyncBroker",
        receipts: list[BatchItemReceipt[Any]],
    ) -> list[_PreparedItem]:
        prepared: list[_PreparedItem] = []
        for item_index, item in enumerate(self._items):
            if item.kicker.broker is not broker:
                raise ValueError("All tasks in the batch must use the same broker.")
            try:
                prepared_item = await self._prepare_one(broker, item, item_index)
            except _SerializationError as exc:
                receipts[item_index] = exc.receipt
                continue
            prepared.append(prepared_item)
            receipts[item_index] = BatchItemReceipt(
                index=item_index,
                task_id=prepared_item.message.task_id,
                status=BatchItemStatus.NOT_SENT,
            )
        return prepared

    async def _prepare_one(
        self,
        broker: "AsyncBroker",
        item: _PendingItem,
        item_index: int,
    ) -> _PreparedItem:
        message: TaskiqMessage | None = None
        try:
            message = item.kicker._prepare_message(  # noqa: SLF001
                *item.args,
                **item.kwargs,
            )
            for middleware in broker.middlewares:
                if middleware.__class__.pre_send != TaskiqMiddleware.pre_send:
                    message = await maybe_awaitable(middleware.pre_send(message))
            broker_message = broker.formatter.dumps(message)
        except Exception as exc:
            logger.exception(
                "Batch item %s failed serialization and will not be sent.",
                item_index,
            )
            raise _SerializationError(
                BatchItemReceipt(
                    index=item_index,
                    task_id=message.task_id if message is not None else "",
                    status=BatchItemStatus.SERIALIZATION_ERROR,
                    error=exc,
                ),
            ) from exc
        return _PreparedItem(
            original_index=item_index,
            message=message,
            broker_message=broker_message,
            kicker=item.kicker,
        )

    async def _send_messages(
        self,
        broker: "AsyncBroker",
        prepared: list[_PreparedItem],
    ) -> list[BrokerBatchReceipt]:
        try:
            return await broker.kick_batch(
                [item.broker_message for item in prepared],
            )
        except Exception as exc:
            # The batch call itself blew up (e.g. connection lost). Every
            # message may or may not have reached the broker, so none of
            # them can be reported as not sent.
            logger.exception("kick_batch failed; outcomes are unknown.")
            return [
                BrokerBatchReceipt(
                    index=index,
                    status=BatchItemStatus.UNKNOWN,
                    error=exc,
                )
                for index in range(len(prepared))
            ]

    async def _apply_receipts(
        self,
        broker: "AsyncBroker",
        prepared: list[_PreparedItem],
        broker_receipts: list[BrokerBatchReceipt],
        receipts: list[BatchItemReceipt[Any]],
    ) -> None:
        by_position = {receipt.index: receipt for receipt in broker_receipts}
        for position, item in enumerate(prepared):
            broker_receipt = by_position.get(position)
            if broker_receipt is None:
                logger.error(
                    "Broker did not return a receipt for batch item %s; "
                    "treating its outcome as unknown.",
                    item.original_index,
                )
                status = BatchItemStatus.UNKNOWN
                error: Exception | None = None
            else:
                status = broker_receipt.status
                error = broker_receipt.error
            receipt: BatchItemReceipt[Any] = BatchItemReceipt(
                index=item.original_index,
                task_id=item.message.task_id,
                status=status,
                error=error,
            )
            if status == BatchItemStatus.ACCEPTED:
                receipt.task = AsyncTaskiqTask(
                    task_id=item.message.task_id,
                    result_backend=broker.result_backend,
                    return_type=item.kicker.return_type,  # type: ignore
                )
                await self._run_post_send(broker, item)
            receipts[item.original_index] = receipt

    async def _run_post_send(
        self,
        broker: "AsyncBroker",
        item: _PreparedItem,
    ) -> None:
        # post_send mirrors single-message kiq and runs only for messages
        # that were actually accepted.
        for middleware in reversed(broker.middlewares):
            if middleware.__class__.post_send == TaskiqMiddleware.post_send:
                continue
            try:
                await maybe_awaitable(middleware.post_send(item.message))
            except Exception:
                logger.exception(
                    "post_send middleware failed for batch item %s.",
                    item.original_index,
                )

    async def retry(
        self,
        result: BatchKickResult,
        *indices: int,
        task_ids: Sequence[str] | None = None,
    ) -> BatchKickResult:
        """
        Re-send selected items from a previous batch result.

        Selected items are re-serialized and sent again using their
        original ``task_id``. Taskiq does not promise that the broker
        de-duplicates on task_id, so retrying an item that was already
        accepted may produce a duplicate execution.

        With no selection, every non-accepted receipt is retried.

        :param result: result of the previous batch kick.
        :param indices: original input indices to retry.
        :param task_ids: task_ids to retry.
        :return: receipts for the retried items, keyed by original index.
        """
        wanted_indices = self._select_retry_indices(result, indices, task_ids)
        retry_kicker = BatchKicker(broker=self.broker)
        for original_index in wanted_indices:
            item = self._items[original_index]
            kicker = self._get_kicker_of(item.kicker)
            kicker.custom_task_id = result.receipts[original_index].task_id or None
            pending = _PendingItem(
                kicker=kicker,
                args=item.args,
                kwargs=item.kwargs,
            )
            retry_kicker._items.append(pending)
        retried = await retry_kicker.kick()

        retried_by_index = {receipt.index: receipt for receipt in retried.receipts}
        receipts: list[BatchItemReceipt[Any]] = []
        for position, original_index in enumerate(wanted_indices):
            receipt = retried_by_index.get(position)
            if receipt is None:
                continue
            receipts.append(
                BatchItemReceipt(
                    index=original_index,
                    task_id=receipt.task_id,
                    status=receipt.status,
                    error=receipt.error,
                    task=receipt.task,
                ),
            )
        return BatchKickResult(receipts=receipts)

    def _select_retry_indices(
        self,
        result: BatchKickResult,
        indices: tuple[int, ...],
        task_ids: Sequence[str] | None,
    ) -> list[int]:
        if not indices and not task_ids:
            selected = [
                receipt.index
                for receipt in result.receipts
                if receipt.status != BatchItemStatus.ACCEPTED
            ]
        else:
            selected = list(indices)
            if task_ids:
                ids_to_indices = {
                    receipt.task_id: receipt.index for receipt in result.receipts
                }
                for task_id in task_ids:
                    if task_id not in ids_to_indices:
                        raise ValueError(f"Unknown task_id in batch result: {task_id}")
                    selected.append(ids_to_indices[task_id])
            selected = sorted(set(selected))
        for index in selected:
            if not 0 <= index < len(self._items):
                raise ValueError(f"Batch item index out of range: {index}")
        return selected

    @staticmethod
    def _get_kicker(
        task: AsyncTaskiqDecoratedTask[Any, Any] | AsyncKicker[Any, Any],
    ) -> "AsyncKicker[Any, Any]":
        if isinstance(task, AsyncKicker):
            return BatchKicker._clone_kicker(task)
        if isinstance(task, AsyncTaskiqDecoratedTask):
            return task.kicker()
        raise TypeError(
            f"Batch items must be decorated tasks or kickers, got {type(task)!r}.",
        )

    @staticmethod
    def _get_kicker_of(
        kicker: AsyncKicker[Any, Any],
    ) -> "AsyncKicker[Any, Any]":
        return BatchKicker._clone_kicker(kicker)

    @staticmethod
    def _clone_kicker(
        kicker: AsyncKicker[Any, Any],
    ) -> "AsyncKicker[Any, Any]":
        clone: AsyncKicker[Any, Any] = AsyncKicker(
            task_name=kicker.task_name,
            broker=kicker.broker,
            labels=dict(kicker.labels),
            return_type=kicker.return_type,
        )
        clone.custom_task_id = kicker.custom_task_id
        clone.custom_schedule_id = kicker.custom_schedule_id
        return clone
