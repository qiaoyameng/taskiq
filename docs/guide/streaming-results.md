---
order: 6
---

# Streaming results

Some tasks produce results piece by piece — for example a large export, a
multi-stage report, or output of a long-running subprocess. Waiting for one
final return value is inconvenient for such tasks, so taskiq supports
streaming result tasks.

## Declaring a streaming task

A streaming task is an async generator. Every value it `yield`s is stored in
the result backend as an ordered fragment with a sequence number starting from
zero.

```python
from collections.abc import AsyncGenerator

from taskiq import InMemoryBroker

broker = InMemoryBroker()


@broker.task
async def count_down(seconds: int) -> AsyncGenerator[int, None]:
    for remaining in range(seconds, 0, -1):
        yield remaining
```

## Consuming fragments

`AsyncTaskiqTask` provides three ways to read fragments:

- `read_stream(since_seq=0)` performs one idempotent read.
- `wait_stream(...)` polls until new fragments or the terminal state arrive.
- `iter_stream(...)` asynchronously iterates until the task finishes.

```python
kicked = await count_down.kiq(3)

async for fragment in kicked.iter_stream():
    print(fragment.data)
```

### Resuming after disconnect

Reads are idempotent: reading with the same `since_seq` always returns the same
fragments. After a disconnect, pass the sequence number following the last
fragment you processed:

```python
async for fragment in kicked.iter_stream(since_seq=last_seen_seq + 1):
    ...
```

## Failures and cancellation

If a streaming task fails after producing some fragments, those fragments stay
readable, but the stream is marked as failed — it is never mistaken for a
successful run. When iterating with `iter_stream`, the original exception is
raised after all available fragments are yielded.

Cancelling the worker-side execution closes the generator, so cleanup code in
`try/finally` blocks always runs before the failed terminal state is written.

## Retention

Backends may evict old streams to bound memory. In that case reads return
`StreamReadStatus.EXPIRED` (or raise `StreamExpiredError` through the task
helpers), which is distinct from `StreamReadStatus.NOT_FOUND` for streams that
were never produced.
