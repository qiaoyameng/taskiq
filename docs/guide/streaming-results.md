---
order: 8
---

# Streaming results

Some tasks produce values incrementally instead of returning a single
final value. For such tasks Taskiq supports **streaming result tasks**:
the task is an asynchronous generator and every value it `yield`s is
persisted in the result backend as an ordered fragment. Consumers can
read fragments starting from any sequence number and resume reading
after a disconnect.

## Defining and kicking a streaming task

Use the `stream_task` decorator and `kiq` to get an
`AsyncStreamTaskiqTask` handle:

```python
from taskiq import InMemoryBroker

broker = InMemoryBroker()


@broker.stream_task
async def numbers(count: int):
    for value in range(count):
        yield value


task = await numbers.kiq(5)
```

A regular task whose target is an async generator can also be kicked as
a stream with `kiq_stream()` or `kicker().kiq_stream()`.

## Consuming fragments

`stream()` is an async generator that yields `StreamChunk` objects as
they appear until the task reaches a terminal state. Each chunk carries
its zero-based `seq`; if the task fails, fragments already produced are
still delivered and the captured exception is raised afterwards.

```python
async for chunk in task.stream():
    print(chunk.seq, chunk.data)
```

Fragments are not consumed: use `read()` to inspect a range repeatedly
or to resume after a disconnect by passing the last seen sequence
number.

```python
first = await task.read(after_seq=-1, timeout=1)
# Later, possibly in another process, continue from seq 2.
async for chunk in task.stream(start_seq=2):
    ...
```

`read()` returns a `StreamRead` with `chunks`, the current `state`
(`PENDING`, `SUCCESS` or `FAILURE`) and an optional `error`. Pending
streams block up to `timeout` seconds; finished streams return
immediately.

## Terminal states and retention

A stream is only considered finished after the worker writes a terminal
state: `SUCCESS` when the generator returns normally, or `FAILURE` when
it raises or is cancelled. A stream that has only stored fragments is
never reported as successful.

When a result backend removes a stream through its retention policy,
later reads raise `StreamNotFoundError` with `expired=True`. The same
error with `expired=False` means the stream was never created (its
fragments have not been produced yet). The in-memory backend keeps
tombstones of evicted streams until they expire.

## Generator cancellation

If the worker cancels the task (for example because the `timeout` label
elapses or the worker shuts down), the generator is closed with
`aclose()`, so `try/finally` cleanup inside the task always runs. After
cleanup the backend records the `FAILURE` terminal state.
