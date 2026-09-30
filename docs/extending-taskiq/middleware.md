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

The `pre_execute` hook can return an `ExecutionDecision` instead of a message.
This allows a middleware to control execution without raising an exception, so
decisions are not counted as task failures.

- `ExecutionDecision.skip(message)` acknowledges the current message without
  executing the task or saving a result.
- `ExecutionDecision.defer(message, delay=...)` (alias: `postpone`) sends the
  message again. The optional delay is stored in the conventional `delay`
  label. The current message is acknowledged only after the new message is sent
  successfully.
- `ExecutionDecision.return_result(message, value)` does not execute the task,
  but runs `post_execute`, saves a successful result with the supplied value,
  runs `post_save`, and acknowledges the message.

All `pre_execute` middlewares still run after a decision is returned. Later
middlewares receive the same decision instance and can inspect it like a
`TaskiqMessage`. Decisions are supported only from the `pre_execute` hook.

```python
from taskiq import ExecutionDecision, TaskiqMessage, TaskiqMiddleware


class CacheMiddleware(TaskiqMiddleware):
    async def pre_execute(self, message: TaskiqMessage):
        cached_value = await self.get_cached_value(message.task_id)
        if cached_value is not None:
            return ExecutionDecision.return_result(message, cached_value)
        return message
```
