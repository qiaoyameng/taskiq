"""Tests for explicit streaming result tasks with the inmemory backend."""

import asyncio
from collections.abc import AsyncGenerator

import pytest

from taskiq import (
    InMemoryBroker,
    StreamChunk,
    StreamNotFoundError,
    StreamRead,
    StreamState,
)
from taskiq.exceptions import StreamingNotSupportedError
from taskiq.result_backends.dummy import DummyResultBackend
from taskiq.stream import StreamChunk as _StreamChunk


async def test_stream_basic_ordered_fragments() -> None:
    broker = InMemoryBroker()

    @broker.stream_task
    async def numbers(count: int) -> AsyncGenerator[int, None]:
        for value in range(count):
            await asyncio.sleep(0)
            yield value

    handle = await numbers.kiq(4)

    chunks: list[StreamChunk[int]] = []
    async for chunk in handle.stream(read_timeout=0.2):
        chunks.append(chunk)

    assert [chunk.seq for chunk in chunks] == [0, 1, 2, 3]
    assert [chunk.data for chunk in chunks] == [0, 1, 2, 3]

    final = await handle.read()
    assert final.state == StreamState.SUCCESS
    assert final.error is None
    assert final.is_complete


async def test_consumer_disconnect_and_resume() -> None:
    broker = InMemoryBroker()

    @broker.stream_task
    async def slow_numbers() -> AsyncGenerator[int, None]:
        for value in range(6):
            await asyncio.sleep(0.01)
            yield value

    handle = await slow_numbers.kiq()

    first_session: list[int] = []
    async for chunk in handle.stream(read_timeout=0.1):
        first_session.append(chunk.data)
        if chunk.data == 2:
            break
    assert first_session == [0, 1, 2]

    second_session: list[int] = []
    async for chunk in handle.stream(start_seq=2, read_timeout=0.1):
        second_session.append(chunk.data)
    assert second_session == [3, 4, 5]


async def test_task_failure_keeps_early_fragments() -> None:
    broker = InMemoryBroker()

    @broker.stream_task
    async def failing_stream() -> AsyncGenerator[str, None]:
        yield "a"
        await asyncio.sleep(0.01)
        yield "b"
        raise ValueError("kaboom")

    handle = await failing_stream.kiq()

    with pytest.raises(ValueError, match="kaboom"):
        async for _chunk in handle.stream(read_timeout=0.2):
            pass

    read = await handle.read(timeout=0.2)
    assert read.state == StreamState.FAILURE
    assert isinstance(read.error, ValueError)
    assert [chunk.data for chunk in read.chunks] == ["a", "b"]
    # A partially written stream must never be reported as success.
    assert read.state != StreamState.SUCCESS


async def test_repeated_read_same_fragment() -> None:
    broker = InMemoryBroker()

    @broker.stream_task
    async def numbers() -> AsyncGenerator[int, None]:
        yield 10
        yield 20

    handle = await numbers.kiq()
    await handle.read(timeout=0.2)

    first = await handle.read(after_seq=0)
    second = await handle.read(after_seq=0)

    assert [chunk.data for chunk in first.chunks] == [20]
    assert [chunk.seq for chunk in first.chunks] == [1]
    assert first.model_dump() == second.model_dump()


async def test_pending_read_blocks_then_times_out() -> None:
    broker = InMemoryBroker()
    ready = asyncio.Event()

    @broker.stream_task
    async def delayed() -> AsyncGenerator[int, None]:
        ready.set()
        yield 0
        await asyncio.sleep(0.3)
        yield 1

    handle = await delayed.kiq()
    await ready.wait()

    pending = await handle.read(after_seq=0, timeout=0.02)
    assert isinstance(pending, StreamRead)
    assert pending.state == StreamState.PENDING
    assert pending.chunks == []

    unblocked = await handle.read(after_seq=0, timeout=1)
    assert [chunk.data for chunk in unblocked.chunks] == [1]
    assert unblocked.state == StreamState.SUCCESS


async def test_cancellation_runs_generator_cleanup() -> None:
    broker = InMemoryBroker()
    started = asyncio.Event()
    cleaned = asyncio.Event()

    @broker.stream_task
    async def endless() -> AsyncGenerator[int, None]:
        try:
            value = 0
            while True:
                started.set()
                yield value
                value += 1
                await asyncio.sleep(0.01)
        finally:
            cleaned.set()

    handle = await endless.kiq()
    await started.wait()
    await asyncio.sleep(0.02)

    running = list(broker._running_tasks)
    assert running
    for task in running:
        task.cancel()
    await asyncio.gather(*running, return_exceptions=True)

    await asyncio.wait_for(cleaned.wait(), timeout=2)
    read = await handle.read()
    assert read.state == StreamState.FAILURE
    assert isinstance(read.error, asyncio.CancelledError) or read.error is not None


async def test_unknown_stream_is_not_expired() -> None:
    backend = InMemoryBroker().result_backend

    with pytest.raises(StreamNotFoundError) as exc_info:
        await backend.read_stream("missing-task")

    assert exc_info.value.expired is False


async def test_evicted_stream_is_reported_as_expired() -> None:
    broker = InMemoryBroker(max_stored_results=1)

    @broker.stream_task
    async def first() -> AsyncGenerator[int, None]:
        yield 1

    @broker.stream_task
    async def second() -> AsyncGenerator[int, None]:
        yield 2

    first_handle = await first.kiq()
    async for _ in first_handle.stream(read_timeout=0.2):
        pass

    second_handle = await second.kiq()
    async for _ in second_handle.stream(read_timeout=0.2):
        pass

    with pytest.raises(StreamNotFoundError) as exc_info:
        await broker.result_backend.read_stream(first_handle.task_id)
    assert exc_info.value.expired is True

    with pytest.raises(StreamNotFoundError) as exc_info:
        await broker.result_backend.read_stream("never-created")
    assert exc_info.value.expired is False


async def test_expired_tombstone_lifecycle() -> None:
    broker = InMemoryBroker(max_stored_results=1, stream_result_ttl=0.01)

    @broker.stream_task
    async def first() -> AsyncGenerator[int, None]:
        yield 1

    @broker.stream_task
    async def second() -> AsyncGenerator[int, None]:
        yield 2

    first_handle = await first.kiq()
    async for _ in first_handle.stream(read_timeout=0.2):
        pass
    second_handle = await second.kiq()
    async for _ in second_handle.stream(read_timeout=0.2):
        pass

    with pytest.raises(StreamNotFoundError) as exc_info:
        await broker.result_backend.read_stream(first_handle.task_id)
    assert exc_info.value.expired is True

    await asyncio.sleep(0.02)
    with pytest.raises(StreamNotFoundError) as exc_info:
        await broker.result_backend.read_stream(first_handle.task_id)
    # After the tombstone TTL passes, the stream looks like it never existed.
    assert exc_info.value.expired is False


async def test_fragment_retention_trims_old_chunks() -> None:
    broker = InMemoryBroker(max_stored_stream_results=3)

    @broker.stream_task
    async def many() -> AsyncGenerator[int, None]:
        for value in range(10):
            yield value

    handle = await many.kiq()
    read = await handle.read(timeout=0.5)

    assert read.state == StreamState.SUCCESS
    assert [chunk.seq for chunk in read.chunks] == [7, 8, 9]


async def test_dummy_backend_rejects_streams() -> None:
    backend: DummyResultBackend[object] = DummyResultBackend()

    with pytest.raises(StreamingNotSupportedError):
        await backend.start_stream("task-id")
    with pytest.raises(StreamingNotSupportedError):
        await backend.append_stream_chunk("task-id", _StreamChunk(seq=0, data=1))
    with pytest.raises(StreamingNotSupportedError):
        await backend.finish_stream("task-id", StreamState.SUCCESS.value)
    with pytest.raises(StreamingNotSupportedError):
        await backend.read_stream("task-id")


async def test_stream_failure_midway_does_not_set_result() -> None:
    broker = InMemoryBroker()

    @broker.stream_task
    async def broken() -> AsyncGenerator[int, None]:
        yield 1
        raise RuntimeError("boom")

    handle = await broken.kiq()
    with pytest.raises(RuntimeError):
        async for _ in handle.stream(read_timeout=0.2):
            pass

    assert not await broker.result_backend.is_result_ready(handle.task_id)


async def test_finished_stream_is_removed_by_ttl() -> None:
    backend = InMemoryBroker(
        max_stored_results=-1,
        stream_result_ttl=0.01,
    ).result_backend

    await backend.start_stream("ttl-task")
    await backend.finish_stream("ttl-task", StreamState.SUCCESS.value)

    read = await backend.read_stream("ttl-task")
    assert read.state == StreamState.SUCCESS

    await asyncio.sleep(0.02)
    with pytest.raises(StreamNotFoundError) as exc_info:
        await backend.read_stream("ttl-task")
    assert exc_info.value.expired is True
