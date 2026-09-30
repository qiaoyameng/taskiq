# Reference parameters (large arguments)

Some task arguments are too large for the broker's message limit. Instead of
embedding such payloads in the message, taskiq can store explicitly marked
arguments in an external **blob store**. The message then only carries a
reference, the content length and a content digest; the worker verifies the
digest while restoring the original argument.

Only arguments you explicitly mark are externalized. Ordinary arguments are
serialized into the message exactly as before.

## Marking a large argument

Wrap bytes in `taskiq.Blob` at the call site:

```python
from taskiq import Blob

await my_task.kiq(Blob(huge_bytes), "normal-arg", nested={"data": Blob(other_bytes)})
```

Markers may appear anywhere in the positional or keyword argument tree
(lists, tuples, sets and nested dicts). The same bytes marked in several
positions are uploaded once and share a single reference.

## Wiring the middleware and the store

`LocalFileBlobStore` is a content-addressed blob store on a local directory.
Both producer and worker configure `BlobRefMiddleware` with a store that can
reach the same objects (for the local store, a shared/networked directory).

```python
from pathlib import Path

from taskiq import BlobRefMiddleware, InMemoryBroker, LocalFileBlobStore

broker = InMemoryBroker()
store = LocalFileBlobStore(Path("/var/lib/taskiq/blobs"))
broker.add_middlewares(
    BlobRefMiddleware(
        store,
        reaper_interval=60,          # optional periodic GC
        orphan_grace_period=900,     # keep never-consumed objects 15 min
        consumed_grace_period=3600,  # keep consumed objects 1 h past last read
    ),
)
```

Any object implementing the `BlobStore` protocol works, so S3, GCS, an HTTP
service, etc. can be added without touching the middleware.

## Wire protocol

A marked value is replaced before serialization with a JSON-safe marker:

```json
{
  "__taskiq_blob__": 1,
  "ref": "file:ba7816...",
  "length": 8192,
  "digest": "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
}
```

* `__taskiq_blob__` is the protocol/version marker.
* `ref` is an opaque, store-specific reference (`file:<sha256>` for the local
  store).
* `length` and `digest` let the worker detect missing, truncated or corrupted
  objects before the task runs.

The worker fetches each distinct reference once, verifies length and SHA-256
digest, and substitutes the raw bytes at every referencing position.

## Failure and delivery guarantees

* **Upload failure aborts the send.** All marked arguments are uploaded in
  `pre_send`, before the broker is contacted. If any upload fails, the
  exception propagates and no message is sent, so workers can never receive a
  message pointing at a missing object.
* **No immediate deletion after sending.** A successful broker acknowledgement
  does not prove that every copy of the message has been consumed. Neither
  `post_send` nor `post_execute` deletes blobs. In particular, when the send
  result is ambiguous (e.g. connection lost after enqueue), the blob is kept so
  a message that was in fact delivered can still be consumed and redelivered.
* **Read/verification failure blocks execution.** If a referenced object is
  missing, unreadable, or its length/digest does not match, `pre_execute`
  raises and the task function is never invoked. With acknowledging brokers
  the message is not acknowledged before execution and can be retried or
  dead-lettered according to broker policy.

## Garbage collection

Blobs are reclaimed only asynchronously by `BlobStore.reap()` (or the optional
`reaper_interval` background loop), using two conservative grace periods:

* `orphan_grace_period` — objects that were **never read** are treated as
  possible orphans (failed or uncertain sends) and retained for this long
  after creation.
* `consumed_grace_period` — objects that were read at least once are retained
  for this long after their **last** access. Every successful read refreshes
  the timestamp, so repeated delivery and delayed consumers never observe
  premature deletion.

Only after the relevant window fully elapses is an object deleted.

## Demo

`acceptance_demo.py` in this directory runs an end-to-end scenario against an
in-memory broker with an explicit queue and a clock-controlled local store,
showing compact messages, upload-failure aborts, ambiguous sends, read-failure
blocking, and blob retention across redelivery and delayed consumption:

```bash
.venv/bin/python docs/guide/ref-params/acceptance_demo.py
```
