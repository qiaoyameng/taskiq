from typing import Any

import pytest

from taskiq import InMemoryBroker
from taskiq.capabilities import (
    CapabilityMismatch,
    CapabilityMismatchAction,
    compare_versions,
    find_capability_problems,
    normalize_requirements,
    parse_requirements,
    version_satisfies,
)
from taskiq.exceptions import TaskCapabilitiesMismatchError
from taskiq.message import BrokerMessage
from taskiq.receiver import Receiver


class RoundRobinBroker(InMemoryBroker):
    """Broker that distributes messages between several in-memory workers."""

    def __init__(self, workers_capabilities: "list[dict[str, str | None]]") -> None:
        super().__init__()
        self.workers = [
            Receiver(
                broker=self,
                executor=self.executor,
                max_async_tasks=10,
                capabilities=capabilities,
            )
            for capabilities in workers_capabilities
        ]
        self.deliveries = 0

    async def kick(self, message: BrokerMessage) -> None:
        """Route each message to the next worker in line."""
        worker = self.workers[self.deliveries % len(self.workers)]
        self.deliveries += 1
        await worker.callback(message=message.message)


def test_compare_versions_is_predictable() -> None:
    assert compare_versions("1.0", "1.0") == 0
    assert compare_versions("1.0", "1.0.0") < 0
    assert compare_versions("2.0", "10.0") < 0
    assert compare_versions("1.0rc1", "1.0") > 0
    assert compare_versions("1.a", "1.0") > 0
    assert compare_versions("1.0", "1.a") < 0


def test_version_satisfies_operators() -> None:
    assert version_satisfies("2.0", ">=1.0")
    assert version_satisfies("1.0", "1.0")
    assert not version_satisfies("1.0", ">1.0")
    assert version_satisfies("1.0", "==1.0")
    assert version_satisfies("1.0", "!=2.0")
    assert version_satisfies("2.0", ">=1.0,<3.0")
    assert not version_satisfies("3.0", ">=1.0,<3.0")
    assert version_satisfies("1.0", "<=1.0")


def test_version_satisfies_unversioned_capabilities() -> None:
    assert version_satisfies(None, None)
    assert version_satisfies(None, "")
    assert not version_satisfies(None, ">=1.0")
    assert version_satisfies("1.0", None)


def test_normalize_and_parse_requirements() -> None:
    assert normalize_requirements(None) == {}
    assert normalize_requirements({"gpu": ">=1.0", "avx": None}) == {
        "gpu": ">=1.0",
        "avx": None,
    }
    assert normalize_requirements("gpu>=1.0") == {"gpu": ">=1.0"}
    assert normalize_requirements(["gpu", "avx>=2"]) == {"gpu": None, "avx": ">=2"}
    assert parse_requirements('{"gpu": ">=1.0"}') == {"gpu": ">=1.0"}
    assert parse_requirements('["gpu"]') == {"gpu": None}
    with pytest.raises(ValueError, match="Cannot parse capability requirements"):
        parse_requirements("{not json")
    with pytest.raises(ValueError, match="Unsupported capability requirements"):
        normalize_requirements(42)


def test_find_capability_problems() -> None:
    missing, incompatible = find_capability_problems(
        {"gpu": ">=2.0", "avx": None},
        {"gpu": "1.0"},
    )
    assert missing == ["avx"]
    assert "gpu" in incompatible

    missing, incompatible = find_capability_problems(
        {"gpu": ">=1.0"},
        {"gpu": None},
    )
    assert missing == []
    assert "gpu" in incompatible

    missing, incompatible = find_capability_problems(
        {"gpu": ">=1.0"},
        {"gpu": "2.0"},
    )
    assert missing == []
    assert incompatible == {}


def test_capability_mismatch_reason_is_structured() -> None:
    mismatch = CapabilityMismatch(
        task_id="tid",
        task_name="task",
        missing_capabilities=["gpu"],
        incompatible_capabilities={"avx": "worker provides '1.0'"},
        reroute_count=2,
    )
    assert "gpu" in mismatch.reason
    assert "avx" in mismatch.reason
    assert mismatch.reroute_count == 2
    assert mismatch.model_dump()["missing_capabilities"] == ["gpu"]


async def test_unsupported_worker_reroutes_to_capable_worker() -> None:
    broker = RoundRobinBroker([
        {"image_resize": "1.0"},
        {"image_resize": "1.0", "gpu": "2.0"},
    ])
    side_effects: list[int] = []

    @broker.task(requires_capabilities={"gpu": ">=1.0"})
    async def gpu_task(value: int) -> int:
        side_effects.append(value)
        return value * 2

    kicked = await gpu_task.kiq(21)
    result = await kicked.wait_result()

    assert not result.is_err
    assert result.return_value == 42
    # The first (unsupported) worker produced no side effects,
    # the capable worker executed the task exactly once.
    assert side_effects == [21]
    assert broker.deliveries == 2


async def test_reject_when_no_worker_supports_task() -> None:
    broker = RoundRobinBroker([{"image_resize": "1.0"}, {}])
    broker.max_capability_reroutes = 2
    side_effects: list[int] = []

    @broker.task(requires_capabilities={"gpu": ">=1.0"})
    async def gpu_task() -> None:
        side_effects.append(1)

    kicked = await gpu_task.kiq()
    result = await kicked.wait_result()

    assert result.is_err
    assert isinstance(result.error, TaskCapabilitiesMismatchError)
    assert "gpu" in str(result.error)
    # No worker ever executed the task...
    assert side_effects == []
    # ...and rerouting stopped instead of looping forever.
    assert broker.deliveries == 3


async def test_custom_broker_policy_can_reject_immediately() -> None:
    class RejectingBroker(RoundRobinBroker):
        async def on_capability_mismatch(
            self,
            message: BrokerMessage,
            mismatch: CapabilityMismatch,
        ) -> CapabilityMismatchAction:
            return CapabilityMismatchAction.REJECT

    broker = RejectingBroker([{}])
    side_effects: list[int] = []

    @broker.task(requires_capabilities={"gpu": None})
    async def gpu_task() -> None:
        side_effects.append(1)

    kicked = await gpu_task.kiq()
    result = await kicked.wait_result()

    assert result.is_err
    assert isinstance(result.error, TaskCapabilitiesMismatchError)
    assert side_effects == []
    assert broker.deliveries == 1


async def test_task_without_requirements_runs_on_any_worker() -> None:
    broker = RoundRobinBroker([{}])

    @broker.task
    async def simple_task() -> int:
        return 1

    kicked = await simple_task.kiq()
    result = await kicked.wait_result()
    assert not result.is_err
    assert result.return_value == 1
    assert broker.deliveries == 1


async def test_worker_without_capabilities_config_accepts_tasks() -> None:
    broker = InMemoryBroker(await_inplace=True)

    @broker.task(requires_capabilities={"gpu": ">=1.0"})
    async def gpu_task() -> int:
        return 7

    kicked = await gpu_task.kiq()
    result = await kicked.wait_result()
    assert not result.is_err
    assert result.return_value == 7


async def test_inmemory_broker_with_capabilities() -> None:
    broker = InMemoryBroker(await_inplace=True, capabilities={"gpu": "2.0"})

    @broker.task(requires_capabilities={"gpu": ">=1.0"})
    async def gpu_task() -> int:
        return 5

    kicked = await gpu_task.kiq()
    result = await kicked.wait_result()
    assert not result.is_err
    assert result.return_value == 5


async def test_kicker_ships_requirements_as_json_label() -> None:
    broker = InMemoryBroker(await_inplace=True, capabilities={"gpu": "2.0"})
    sent: list[BrokerMessage] = []
    original_kick = broker.kick

    async def spy_kick(message: BrokerMessage) -> None:
        sent.append(message)
        await original_kick(message)

    broker.kick = spy_kick  # type: ignore[method-assign]

    @broker.task(requires_capabilities={"gpu": ">=1.0"})
    async def gpu_task() -> None:
        return None

    await gpu_task.kiq()
    assert sent
    labels: dict[str, Any] = sent[0].labels
    assert labels["requires_capabilities"] == '{"gpu": ">=1.0"}'
