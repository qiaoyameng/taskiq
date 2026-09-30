---
order: 5
---

# Batch kicking

Sometimes you need to submit a group of different tasks at once and know
exactly which ones the broker accepted. The `BatchKicker` collects
heterogeneous tasks, validates and serializes every message **before**
sending anything, and returns one receipt per submitted item.

## Building a batch

Create a batch kicker from your broker and add task invocations to it.
Tasks may be completely different functions with different arguments:

```python
broker = InMemoryBroker()


@broker.task()
async def add(a: int, b: int) -> int:
    return a + b


@broker.task()
async def greet(name: str) -> str:
    return f"Hello, {name}"


async def send_batch() -> None:
    batch = broker.batch_kicker()
    batch.add(add, 1, 2)
    batch.add(greet, "Taskiq")
    batch.add(add.kicker().with_labels(priority=10), 3, 4)

    result = await batch.kick()
```

`result.receipts` has exactly one entry per `add` call. Receipt `i`
always corresponds to the `i`-th added task, regardless of individual
failures.

## Receipt statuses

Each `BatchItemReceipt` carries the original `index`, the `task_id`, a
`status`, the optional broker `error`, and (when accepted) an
`AsyncTaskiqTask` handle:

- `accepted` — the broker confirmed acceptance.
- `rejected` — the broker explicitly refused the message.
- `unknown` — the outcome is unknown (e.g. connection dropped after the
  send). The message may already have been enqueued, so it is **never**
  reported as not sent.
- `not_sent` — the message was not attempted (used by the sequential
  fallback after a hard failure).
- `serialization_error` — the message could not be serialized and never
  reached the network.

```python
for receipt in result:
    print(receipt.index, receipt.task_id, receipt.status)

accepted = result.accepted
failed = result.failed
```

## Serialization is validated up front

All messages are built, run through `pre_send` middlewares, and
serialized before any of them is sent. If an argument is not
serializable, that item is reported as `serialization_error` and skipped,
while the other valid messages are still sent. This guarantees a batch
never partially reaches the broker due to serialization problems.

## Native batch sending and the fallback

Brokers that support native batch publishing override
`AsyncBroker.kick_batch(messages)` and return a `BrokerBatchReceipt` per
message. This is where partial acceptance can be reported precisely.

The default `kick_batch` implementation is an **explicitly marked
per-item fallback**: it calls the regular `kick` method sequentially.
When a call raises `TaskRejectedError`, that item is `rejected`; any
other error (connection drop, timeout) is `unknown`, because delivery
cannot be ruled out. In both cases the fallback stops and later messages
are `not_sent`.

## Retrying items

Retry selected items using their original `task_id`. With no selection,
every non-accepted receipt is retried:

```python
# Retry specific input indices.
await batch.retry(result, 0, 2)

# Retry by task_id.
await batch.retry(result, task_ids=[result[0].task_id])

# Retry everything that was not accepted.
await batch.retry(result)
```

Retried receipts keep the original input `index` so they can be matched
back to the first submission.

::: warning
Taskiq does **not** promise broker-side de-duplication. Re-sending with
the same `task_id` may enqueue a duplicate; whether identical ids are
de-duplicated is broker-specific.
:::
