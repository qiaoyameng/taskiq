---
order: 3
---

# Result backend

Result backends are used to store information about task execution.
To create new `result_backend` you have to implement `taskiq.abc.result_backend.AsyncResultBackend` class.

Here's a minimal example of a result backend:

@[code python](../examples/extending/result_backend.py)

::: info Cool tip!
It's a good practice to skip fetching logs from the storage unless `with_logs=True` is explicitly specified.
:::


::: caution Important note!
`with_logs` param is now deprecated. It will be removed in future releases.
:::

## Streaming results

Some tasks produce results incrementally instead of returning a single value.
Such tasks are declared as async generators:

```python
@broker.task
async def streaming_task() -> AsyncGenerator[str, None]:
    for line in read_lines():
        yield line
```

Result backends that want to support streaming tasks must implement four
additional methods from `AsyncResultBackend`:

- `open_stream(task_id)` — create an empty ordered stream.
- `add_stream_chunk(task_id, data)` — append one chunk and return its
  monotonically increasing sequence number (starting from zero).
- `finish_stream(task_id, result)` — write the terminal state. A stream is
  considered finished only after this call, so chunks written before a failure
  must not make the stream look successful.
- `read_stream(task_id, since_seq)` — return fragments starting from the given
  inclusive sequence number. Reads must be idempotent.

`read_stream` returns a `StreamRead` object whose `status` distinguishes four
situations:

- `CHUNK` — fragments returned, task is still running.
- `COMPLETED` / `FAILED` — terminal state is available in `result`.
- `NOT_FOUND` — no stream was ever created for this id.
- `EXPIRED` — the stream existed, but was removed by a retention policy.

Implementations that expire old streams should keep tombstones for removed ids,
otherwise consumers cannot tell "never produced" from "already cleaned".

When a streaming task is cancelled, the worker always closes the generator
before writing the terminal state, so cleanup code in `try/finally` blocks is
guaranteed to run.
