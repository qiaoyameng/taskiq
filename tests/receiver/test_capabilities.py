from typing import Any

import pytest
from taskiq_dependencies import Depends

from taskiq.brokers.inmemory_broker import InMemoryBroker
from taskiq.capabilities import (
    CAPABILITY_MISMATCHES_LABEL,
    CAPABILITY_REQUEUES_LABEL,
    CapabilityMismatchPolicy,
    check_capabilities,
    compare_versions,
    normalize_requirements,
    version_satisfies,
)
from taskiq.exceptions import CapabilityMismatchError


def test_compare_versions_numeric() -> None:
    """Dotted numeric versions are compared numerically with zero padding."""
    assert compare_versions("1.10", "1.9") == 1
    assert compare_versions("1.9", "1.10") == -1
    assert compare_versions("1.2", "1.2.0") == 0
    assert compare_versions("2", "2.0.0") == 0
    assert compare_versions(2, "2.0") == 0


def test_compare_versions_non_numeric_is_predictable() -> None:
    """Non-numeric versions only support exact equality, never ordering."""
    assert compare_versions("beta", "beta") == 0
    assert compare_versions("1.x", "1.x") == 0
    assert compare_versions("beta", "alpha") is None
    assert compare_versions("10a", "9a") is None
    assert compare_versions("beta", "1.0") is None


def test_version_satisfies() -> None:
    """Requirement semantics: presence, numeric minimum, exact fallback."""
    assert version_satisfies("anything", None)
    assert not version_satisfies(None, None)
    assert version_satisfies("2.0", "1.5")
    assert version_satisfies("1.5", "1.5")
    assert not version_satisfies("1.5", "2.0")
    assert version_satisfies("beta", "beta")
    assert not version_satisfies("alpha", "beta")


def test_normalize_requirements() -> None:
    """All declaration forms normalize to a name to min-version mapping."""
    assert normalize_requirements(None) == {}
    assert normalize_requirements("gpu") == {"gpu": None}
    assert normalize_requirements(["gpu", "codec"]) == {"gpu": None, "codec": None}
    assert normalize_requirements({"gpu": True, "codec": "2.0"}) == {
        "gpu": None,
        "codec": "2.0",
    }


def test_check_capabilities_mismatches() -> None:
    """Missing and outdated capabilities produce structured mismatches."""
    check = check_capabilities(
        {"codec": "2.0", "gpu": None},
        {"codec": "1.0"},
    )
    assert not check.satisfied
    by_name = {mismatch.name: mismatch for mismatch in check.mismatches}
    assert by_name["codec"].reason == "version_too_low"
    assert by_name["codec"].required == "2.0"
    assert by_name["codec"].actual == "1.0"
    assert by_name["gpu"].reason == "missing"
    assert by_name["gpu"].actual is None

    satisfied = check_capabilities({"codec": "2.0"}, {"codec": "2.1"})
    assert satisfied.satisfied
    assert satisfied.mismatches == ()


async def test_task_without_requirements_runs_on_any_worker() -> None:
    """Ordinary tasks without capability requirements keep running."""
    broker = InMemoryBroker(await_inplace=True)
    calls = []

    @broker.task
    async def plain_task(value: int) -> int:
        calls.append(value)
        return value

    task = await plain_task.kiq(5)
    result = await task.get_result()

    assert not result.is_err
    assert result.return_value == 5
    assert calls == [5]


async def test_rolling_upgrade_two_workers() -> None:
    """
    Simulate a rolling upgrade with two in-memory workers.

    The old worker lacks the required capability and must not produce
    any task side effects (neither dependency injection nor execution).
    The upgraded worker supports the capability and processes the task.
    """
    side_effects: list[tuple[str, Any]] = []

    old_worker = InMemoryBroker(await_inplace=True)
    new_worker = InMemoryBroker(await_inplace=True).with_capabilities(
        {"image-codec": "2.0"},
    )

    def tracked_dependency() -> int:
        side_effects.append(("dependency_injected", None))
        return 1

    @old_worker.task(task_name="process", capabilities={"image-codec": "2.0"})
    async def process_on_old(
        value: int,
        dep: int = Depends(tracked_dependency),
    ) -> int:
        side_effects.append(("old_executed", value))
        return value + dep

    @new_worker.task(task_name="process", capabilities={"image-codec": "2.0"})
    async def process_on_new(
        value: int,
        dep: int = Depends(tracked_dependency),
    ) -> int:
        side_effects.append(("new_executed", value))
        return value + dep

    # The old worker receives the task but cannot execute it.
    rejected_task = await process_on_old.kiq(1)
    rejected = await rejected_task.get_result()

    assert side_effects == []
    assert rejected.is_err
    assert isinstance(rejected.error, CapabilityMismatchError)
    assert rejected.labels[CAPABILITY_MISMATCHES_LABEL] == [
        {
            "name": "image-codec",
            "required": "2.0",
            "actual": None,
            "reason": "missing",
        },
    ]

    # The upgraded worker picks up the same logical task and runs it.
    accepted_task = await process_on_new.kiq(1)
    accepted = await accepted_task.get_result()

    assert not accepted.is_err
    assert accepted.return_value == 2
    assert side_effects == [("dependency_injected", None), ("new_executed", 1)]


async def test_message_level_requirement_via_kiq_labels() -> None:
    """Requirements can also be attached per-message through labels."""
    broker = InMemoryBroker(await_inplace=True).with_capabilities({"gpu": "1.0"})
    calls = []

    @broker.task
    async def gpu_task() -> None:
        calls.append("ran")

    task = await gpu_task.kicker().with_labels(capabilities={"gpu": "2.0"}).kiq()
    result = await task.get_result()

    assert calls == []
    assert result.is_err
    assert isinstance(result.error, CapabilityMismatchError)
    assert result.labels[CAPABILITY_MISMATCHES_LABEL][0]["reason"] == (
        "version_too_low"
    )


async def test_reroute_policy_is_bounded() -> None:
    """REROUTE re-publishes a bounded number of times, then rejects."""
    broker = InMemoryBroker(await_inplace=True)
    broker.with_capability_mismatch_policy(
        CapabilityMismatchPolicy.REROUTE,
        max_requeues=2,
    )
    calls = []

    @broker.task(capabilities={"newfmt": None})
    async def formatted_task() -> None:
        calls.append("ran")

    task = await formatted_task.kiq()
    result = await task.get_result()

    assert calls == []
    assert result.is_err
    assert isinstance(result.error, CapabilityMismatchError)
    # The message was re-routed exactly max_requeues times before rejection.
    assert result.labels[CAPABILITY_REQUEUES_LABEL] == 2


async def test_reject_policy_does_not_reroute() -> None:
    """REJECT (the default) never requeues the mismatched message."""
    broker = InMemoryBroker(await_inplace=True)

    @broker.task(capabilities={"newfmt": None})
    async def formatted_task() -> None:
        pass

    task = await formatted_task.kiq()
    result = await task.get_result()

    assert result.is_err
    assert isinstance(result.error, CapabilityMismatchError)
    assert CAPABILITY_REQUEUES_LABEL not in result.labels


async def test_unknown_capability_never_satisfied() -> None:
    """A capability the worker does not declare is treated as missing."""
    broker = InMemoryBroker(await_inplace=True).with_capabilities({"other": "9.9"})

    @broker.task(capabilities={"codec": "1.0"})
    async def codec_task() -> None:
        pass

    task = await codec_task.kiq()
    result = await task.get_result()

    assert result.is_err
    assert isinstance(result.error, CapabilityMismatchError)
    mismatch = result.labels[CAPABILITY_MISMATCHES_LABEL][0]
    assert mismatch["reason"] == "missing"
    assert mismatch["actual"] is None


async def test_invalid_max_requeues_rejected() -> None:
    """Negative requeue limits are rejected eagerly."""
    broker = InMemoryBroker()
    with pytest.raises(ValueError, match="max_requeues"):
        broker.with_capability_mismatch_policy(
            CapabilityMismatchPolicy.REROUTE,
            max_requeues=-1,
        )
