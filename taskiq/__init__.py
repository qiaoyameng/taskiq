"""Distributed task manager."""

from importlib.metadata import version

from taskiq_dependencies import Depends as TaskiqDepends

from taskiq.abc.blob_store import AsyncBlobStore
from taskiq.abc.broker import AsyncBroker, AsyncTaskiqDecoratedTask
from taskiq.abc.formatter import TaskiqFormatter
from taskiq.abc.middleware import TaskiqMiddleware
from taskiq.abc.result_backend import AsyncResultBackend
from taskiq.abc.schedule_source import ScheduleSource
from taskiq.acks import AckableMessage, AcknowledgeType
from taskiq.blob import Blob, BlobParameter
from taskiq.blob_reference import BlobReference
from taskiq.blob_stores import LocalFileBlobStore
from taskiq.brokers.inmemory_broker import InMemoryBroker
from taskiq.brokers.shared_broker import async_shared_broker
from taskiq.brokers.zmq_broker import ZeroMQBroker
from taskiq.context import Context
from taskiq.events import TaskiqEvents
from taskiq.exceptions import (
    NoResultError,
    ResultGetError,
    ResultIsReadyError,
    SecurityError,
    SendTaskError,
    TaskiqError,
    TaskiqResultTimeoutError,
)
from taskiq.funcs import gather
from taskiq.message import BrokerMessage, TaskiqMessage
from taskiq.middlewares import (
    PrometheusMiddleware,
    SimpleRetryMiddleware,
    SmartRetryMiddleware,
)
from taskiq.result import TaskiqResult
from taskiq.scheduler.scheduled_task import ScheduledTask
from taskiq.scheduler.scheduler import TaskiqScheduler
from taskiq.state import TaskiqState
from taskiq.task import AsyncTaskiqTask

__version__ = version("taskiq")

__all__ = [
    "AckableMessage",
    "AcknowledgeType",
    "AsyncBlobStore",
    "AsyncBroker",
    "AsyncResultBackend",
    "AsyncTaskiqDecoratedTask",
    "AsyncTaskiqTask",
    "Blob",
    "BlobParameter",
    "BlobReference",
    "BrokerMessage",
    "Context",
    "InMemoryBroker",
    "LocalFileBlobStore",
    "NoResultError",
    "PrometheusMiddleware",
    "ResultGetError",
    "ResultIsReadyError",
    "ScheduleSource",
    "ScheduledTask",
    "SecurityError",
    "SendTaskError",
    "SimpleRetryMiddleware",
    "SmartRetryMiddleware",
    "TaskiqDepends",
    "TaskiqError",
    "TaskiqEvents",
    "TaskiqFormatter",
    "TaskiqMessage",
    "TaskiqMiddleware",
    "TaskiqResult",
    "TaskiqResultTimeoutError",
    "TaskiqScheduler",
    "TaskiqState",
    "ZeroMQBroker",
    "__version__",
    "async_shared_broker",
    "gather",
]
