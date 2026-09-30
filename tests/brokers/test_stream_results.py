import asyncio
import contextlib
from collections.abc import AsyncGenerator

import pytest

from taskiq import (
    InMemoryBroker,
    StreamExpiredError,
    StreamNotFoundError,
    StreamReadStatus,
    TaskiqResult,
    TaskiqResultTimeoutError,
)
from taskiq.task import AsyncTaskiqTask


async def test_streaming_task_happy_path() -> None:
    broker = InMemoryBroker()

    @broker.task
    async def streaming_task(count: int) -> AsyncGenerator[dict[str, int], None]:
        for index in range(count):
            yield {"index": index}

    kicked = await streaming_task.kiq(4)

    received: list[dict[str, int]] = []
    async for chunk in kicked.iter_stream(check_interval=0.01):
        received.append(chunk.data)

    assert received == [{"index": index} for index in range(4)]
    assert await kicked.is_ready()

    result = await kicked.get_result()
    assert not result.is_err
    assert result.return_value is None


async def test_consumer_disconnect_and_resume() -> None:
    broker = InMemoryBroker()
    first_chunk_ready = asyncio.Event()

    @broker.task
    async def slow_stream() -> AsyncGenerator[int, None]:
        for index in range(6):
            yield index
            if index == 0:
                first_chunk_ready.set()
            await asyncio.sleep(0.02)

    kicked = await slow_stream.kiq()

    # First consumer reads only the first chunk, then "disconnects".
    first_read = await kicked.wait_stream(check_interval=0.01)
    await first_chunk_ready.wait()
    assert [chunk.data for chunk in first_read.chunks] == [0]
    last_seq = first_read.chunks[-1].seq

    # A new consumer resumes from the sequence after the last seen chunk.
    resumed = []
    async for chunk in kicked.iter_stream(
        since_seq=last_seq + 1,
        check_interval=0.01,
    ):
        resumed.append(chunk.data)

    assert resumed == [1, 2, 3, 4, 5]
    result = await kicked.get_result()
    assert not result.is_err


async def test_streaming_task_failure_midway() -> None:
    broker = InMemoryBroker()

    @broker.task
    async def failing_stream() -> AsyncGenerator[int, None]:
        yield 1
        yield 2
        await asyncio.sleep(0.01)
        raise ValueError("stream exploded")

    kicked = await failing_stream.kiq()

    received = []
    with pytest.raises(ValueError, match="stream exploded"):
        async for chunk in kicked.iter_stream(check_interval=0.01):
            received.append(chunk.data)

    # Chunks produced before the failure must still be available.
    assert received == [1, 2]

    # But the stream must not look like a successful one.
    read = await kicked.read_stream()
    assert read.status == StreamReadStatus.FAILED
    assert read.result is not None
    assert read.result.is_err
    assert isinstance(read.result.error, ValueError)
    assert await kicked.is_ready()

    final = await kicked.get_result()
    assert final.is_err
    assert isinstance(final.error, ValueError)


async def test_repeated_read_returns_same_fragments() -> None:
    broker = InMemoryBroker()

    @broker.task
    async def streaming_task() -> AsyncGenerator[str, None]:
        yield "a"
        yield "b"
        yield "c"

    kicked = await streaming_task.kiq()
    await kicked.wait_result()

    first = await kicked.read_stream(since_seq=1)
    second = await kicked.read_stream(since_seq=1)
    third = await kicked.read_stream(since_seq=1)

    expected = ["b", "c"]
    for read in (first, second, third):
        assert read.status == StreamReadStatus.COMPLETED
        assert [chunk.data for chunk in read.chunks] == expected
        assert [chunk.seq for chunk in read.chunks] == [1, 2]


async def test_unfinished_stream_is_not_ready() -> None:
    broker = InMemoryBroker()
    release = asyncio.Event()

    @broker.task
    async def hanging_stream() -> AsyncGenerator[int, None]:
        yield 1
        await release.wait()

    kicked = await hanging_stream.kiq()
    read = await kicked.wait_stream(check_interval=0.01)
    assert read.status == StreamReadStatus.CHUNK
    assert not await kicked.is_ready()

    release.set()
    await kicked.wait_result()
    assert await kicked.is_ready()


async def test_never_created_stream_is_not_found() -> None:
    broker = InMemoryBroker()
    ghost = AsyncTaskiqTask("missing-task", broker.result_backend)

    with pytest.raises(StreamNotFoundError):
        await ghost.read_stream()

    with pytest.raises(TaskiqResultTimeoutError):
        await ghost.wait_stream(check_interval=0.01, timeout=0.05)


async def test_expired_stream_distinguished_from_missing() -> None:
    backend = InMemoryBroker(max_stored_results=1).result_backend

    await backend.open_stream("old")
    await backend.add_stream_chunk("old", "payload")
    await backend.finish_stream(
        "old",
        TaskiqResult(is_err=False, return_value=None, execution_time=0),
    )
    await backend.open_stream("new")

    expired_read = await backend.read_stream("old")
    assert expired_read.status == StreamReadStatus.EXPIRED

    missing_read = await backend.read_stream("never-existed")
    assert missing_read.status == StreamReadStatus.NOT_FOUND

    with pytest.raises(StreamExpiredError):
        await AsyncTaskiqTask("old", backend).read_stream()
    with pytest.raises(StreamNotFoundError):
        await AsyncTaskiqTask("never-existed", backend).read_stream()


async def test_cancelled_stream_runs_generator_cleanup() -> None:
    broker = InMemoryBroker()
    entered = asyncio.Event()
    cleaned_up = asyncio.Event()

    @broker.task
    async def cancellable_stream() -> AsyncGenerator[str, None]:
        entered.set()
        try:
            while True:
                yield "tick"
                await asyncio.sleep(10)
        finally:
            cleaned_up.set()

    await cancellable_stream.kicker().with_task_id("cancel-id").kiq()
    await entered.wait()

    running = list(broker._running_tasks)
    assert len(running) == 1
    running[0].cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await running[0]

    assert cleaned_up.is_set()
    read = await broker.result_backend.read_stream("cancel-id")
    assert read.status == StreamReadStatus.FAILED
    assert read.result is not None and read.result.is_err


async def test_failed_stream_without_yields_is_failed_not_pending() -> None:
    broker = InMemoryBroker()

    @broker.task
    async def immediately_failing() -> AsyncGenerator[None, None]:
        raise RuntimeError("no chunks at all")
        yield  # pragma: no cover

    kicked = await immediately_failing.kiq()
    with contextlib.suppress(RuntimeError):
        async for _ in kicked.iter_stream(check_interval=0.01):
            pass

    read = await kicked.read_stream()
    assert read.status == StreamReadStatus.FAILED
    assert read.chunks == []
