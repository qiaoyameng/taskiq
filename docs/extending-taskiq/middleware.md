---
order: 2
---

# Middlewares

Middlewares are super helpful. You can inject some code before or after task's execution.

Middlewares must implement [`taskiq.abc.middleware.TaskiqMiddleware`](https://github.com/taskiq-python/taskiq/blob/master/taskiq/abc/middleware.py) abstract class.
Every method of a middleware can be either sync or async. Taskiq will execute it
as you expect.

For example:

::: tabs

@tab sync

@[code python](../examples/extending/middleware_sync.py)

@tab async

@[code python](../examples/extending/middleware_async.py)

:::

Also, middlewares always have a reference to the current broker in `self.broker` field.
If you want to kick a message during the execution of some middleware hooks, you
may use `self.broker` to do so.

[Taskiq-pipelines](https://github.com/taskiq-python/taskiq-pipelines) uses middlewares to
call next tasks.

## Execution decisions

A worker-side `pre_execute` hook can return an execution decision instead of a message.
Decisions are an explicit way to control execution; they are not errors and are not
stored as failed task results.

```python
from taskiq import DeferTask, ReturnResult, SkipTask, TaskiqMiddleware
from taskiq.message import TaskiqMessage


class RunControlMiddleware(TaskiqMiddleware):
    async def pre_execute(self, message, decision=None):
        if message.labels.get("skip"):
            return SkipTask()
        if message.labels.get("later"):
            return DeferTask(delay=30)
        if message.labels.get("cached"):
            return ReturnResult("cached value")
        return message
```

The available decisions are:

- `SkipTask`: do not execute the task and do not save a result.
- `DeferTask(delay=seconds)`: send the message back to the broker with a `delay`
  label and do not save a result.
- `ReturnResult(value)`: do not execute the task, but save a successful result
  containing `value`. A complete `TaskiqResult` can also be provided.

For acknowledgement modes other than `manual`, skip and successful deferral
acknowledge the original message. A deferred message is acknowledged only after
re-sending it succeeds, so a broker failure leaves the original delivery
unacknowledged. Replacement results acknowledge after the result is saved, even
when the worker uses `when_received` or `when_executed`; a result-backend
failure leaves the original delivery unacknowledged. In `manual` mode all three
decisions leave acknowledgement to application code.

Middleware hooks run in registration order. New-style hooks that accept the
optional second `decision` argument receive the same decision object returned by
an earlier hook. Hooks that only accept `message` remain supported, but cannot
inspect a decision made by an earlier middleware.

Execution decisions can only be returned from `pre_execute`. Returning one from
another hook raises `TypeError`. If `pre_execute` raises an exception, including
`asyncio.CancelledError`, Taskiq treats it as middleware failure or cancellation
instead of converting it into an execution decision.
