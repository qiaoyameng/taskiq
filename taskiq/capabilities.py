"""Task capability requirements and worker capability matching."""

import enum
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

# Label used to declare which capabilities a task requires.
CAPABILITIES_LABEL = "capabilities"
# Label that tracks how many times a message was re-routed
# because the current worker cannot execute it.
CAPABILITY_REQUEUES_LABEL = "capability_requeues"
# Label used in failed results to describe capability mismatches.
CAPABILITY_MISMATCHES_LABEL = "capability_mismatches"


class CapabilityMismatchPolicy(str, enum.Enum):
    """What a worker does with a task it cannot execute."""

    # Save a structured failed result and acknowledge the message.
    REJECT = "reject"
    # Re-publish the message a bounded number of times, then reject.
    REROUTE = "reroute"


def parse_version(version: Any) -> "tuple[int, ...] | None":
    """
    Parse a dotted numeric version.

    Returns None when the value is not a dotted numeric version,
    so callers can fall back to a predictable equality check.

    :param version: version value to parse.
    :return: tuple of numeric segments or None.
    """
    if isinstance(version, bool):
        return None
    if isinstance(version, int):
        return (version,)
    if not isinstance(version, str):
        return None
    parts = version.strip().split(".")
    if not parts or any(not part.isdigit() for part in parts):
        return None
    return tuple(int(part) for part in parts)


def compare_versions(left: Any, right: Any) -> "int | None":
    """
    Compare two versions predictably.

    Dotted numeric versions are compared numerically, padding the
    shorter one with zeros ("1.2" equals "1.2.0", "1.10" is greater
    than "1.9"). Any other values are only comparable by exact string
    equality: the result is 0 when they are equal and None when the
    values have no predictable order.

    :param left: first version.
    :param right: second version.
    :return: -1, 0, 1, or None when not comparable.
    """
    left_version = parse_version(left)
    right_version = parse_version(right)
    if left_version is not None and right_version is not None:
        length = max(len(left_version), len(right_version))
        padded_left = left_version + (0,) * (length - len(left_version))
        padded_right = right_version + (0,) * (length - len(right_version))
        if padded_left == padded_right:
            return 0
        return -1 if padded_left < padded_right else 1
    if str(left) == str(right):
        return 0
    return None


def version_satisfies(actual: Any, required: Any) -> bool:
    """
    Check whether an actual capability version satisfies a requirement.

    A None requirement only demands the capability to be present.
    Numeric versions must be greater than or equal to the requirement.
    Non-numeric versions must match the requirement exactly.

    :param actual: version the worker provides, None when absent.
    :param required: minimum required version, None for presence-only.
    :return: True when the requirement is satisfied.
    """
    if required is None:
        return actual is not None
    if actual is None:
        return False
    comparison = compare_versions(actual, required)
    if comparison is None:
        return False
    return comparison >= 0


@dataclass(frozen=True)
class CapabilityMismatch:
    """A single unsatisfied capability requirement."""

    name: str
    required: "str | None"
    actual: "str | None"
    reason: str

    def as_dict(self) -> dict[str, Any]:
        """
        Represent the mismatch as a plain serializable dict.

        :return: structured mismatch description.
        """
        return {
            "name": self.name,
            "required": self.required,
            "actual": self.actual,
            "reason": self.reason,
        }


@dataclass(frozen=True)
class CapabilityCheck:
    """Result of matching task requirements against worker capabilities."""

    satisfied: bool
    mismatches: tuple[CapabilityMismatch, ...] = ()


def normalize_requirements(raw: Any) -> dict[str, "str | None"]:
    """
    Normalize capability declarations to a name to min-version mapping.

    Accepts a mapping of capability names to minimum versions
    (None or True means any version), a single capability name,
    or an iterable of capability names.

    :param raw: raw capability declaration.
    :return: mapping of capability names to minimum versions.
    """
    if raw is None:
        return {}
    if isinstance(raw, str):
        return {raw: None}
    if isinstance(raw, Mapping):
        normalized: dict[str, str | None] = {}
        for name, requirement in raw.items():
            if requirement is None or requirement is True:
                normalized[str(name)] = None
            else:
                normalized[str(name)] = str(requirement)
        return normalized
    return {str(name): None for name in raw}


def check_capabilities(
    requirements: Any,
    worker_capabilities: Mapping[str, Any],
) -> CapabilityCheck:
    """
    Match task requirements against worker capabilities.

    :param requirements: raw task capability requirements.
    :param worker_capabilities: capabilities the worker provides.
    :return: structured check result with all mismatches.
    """
    mismatches = []
    for name, required in normalize_requirements(requirements).items():
        actual = worker_capabilities.get(name)
        if actual is None:
            mismatches.append(
                CapabilityMismatch(
                    name=name,
                    required=required,
                    actual=None,
                    reason="missing",
                ),
            )
        elif not version_satisfies(actual, required):
            mismatches.append(
                CapabilityMismatch(
                    name=name,
                    required=required,
                    actual=str(actual),
                    reason="version_too_low",
                ),
            )
    return CapabilityCheck(satisfied=not mismatches, mismatches=tuple(mismatches))
