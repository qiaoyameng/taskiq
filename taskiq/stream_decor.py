"""Decorated task that produces its result as an ordered stream."""

from typing import Generic, ParamSpec, TypeVar

from taskiq.decor import AsyncTaskiqDecoratedTask
from taskiq.kicker import AsyncKicker
from taskiq.stream_task import AsyncStreamTaskiqTask

_StreamType = TypeVar("_StreamType")
_FuncParams = ParamSpec("_FuncParams")


class AsyncStreamTaskiqDecoratedTask(
    AsyncTaskiqDecoratedTask[_FuncParams, _StreamType],
    Generic[_FuncParams, _StreamType],
):
    """A decorated async generator task that yields ordered fragments."""

    is_stream_task: bool = True

    def kicker(self) -> AsyncKicker[_FuncParams, _StreamType]:
        """
        Return a kicker pre-configured for streaming.

        :return: kicker with the ``is_stream`` flag enabled.
        """
        kicker = super().kicker()
        kicker.is_stream = True
        return kicker

    async def kiq(  # type: ignore[override]
        self,
        *args: _FuncParams.args,
        **kwargs: _FuncParams.kwargs,
    ) -> AsyncStreamTaskiqTask[_StreamType]:
        """
        Kick a streaming task.

        :param args: function's arguments.
        :param kwargs: function's keyword arguments.
        :return: handle used to read the produced fragments.
        """
        return await self.kiq_stream(*args, **kwargs)

    async def kiq_stream(
        self,
        *args: _FuncParams.args,
        **kwargs: _FuncParams.kwargs,
    ) -> AsyncStreamTaskiqTask[_StreamType]:
        """
        Kick a streaming task and return a stream consumer handle.

        :param args: function's arguments.
        :param kwargs: function's keyword arguments.
        :return: handle used to read the produced fragments.
        """
        kicker = self.kicker()
        kicker.is_stream = True
        return await kicker.kiq_stream(*args, **kwargs)
