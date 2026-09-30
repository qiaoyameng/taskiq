"""Task capability requirements and worker capability matching.

This module implements an opt-in mechanism for rolling upgrades:

* tasks declare the capabilities they need via the
  ``requires_capabilities`` label (a mapping of capability name to an
  optional version constraint),
* workers (receivers) are configured with the capabilities they provide,
* before a received message reaches parameter injection and execution,
  the receiver verifies that the worker satisfies the requirements.

If requirements are not met, a structured :class:`CapabilityMismatch`
is produced and the broker decides (see
:meth:`taskiq.abc.broker.AsyncBroker.on_capability_mismatch`) whether to
reroute the message to another worker or to reject it. Rerouting is
bounded by a reroute counter stored in message labels, so a message can
never bounce between workers forever.

Version comparison rules are deliberately simple and predictable:

* a version is split into runs of digits and non-digits
  (``"1.0rc1"`` -> ``1``, ``"."``, ``0``, ``"rc"``, ``1``),
* numeric runs compare as integers, non-numeric runs as strings,
  and a numeric run always sorts before a non-numeric one,
* if one version is a prefix of another, the shorter one is smaller
  (``"1.0" < "1.0.0"``),
* constraints support ``==``, ``!=``, ``>=``, ``<=``, ``>``, ``<``
  and comma-separated combinations; a bare version means ``>=``,
* an unversioned worker capability (declared without a version)
  satisfies only unversioned requirements,
* a capability the worker does not declare at all never satisfies
  a requirement.
"""

import enum
import json
import re
from collections.abc import Mapping
from typing import Any

from pydantic import BaseModel

#: Label used to declare task capability requirements.
REQUIRED_CAPABILITIES_LABEL = "requires_capabilities"

#: Label used to count how many times a message was rerouted
#: because no worker with the required capabilities was found.
CAPABILITY_REROUTES_LABEL = "capability_reroutes"

_VERSION_PARTS_RE = re.compile(r"\d+|\D+")
_CAPABILITY_SPEC_RE = re.compile(r"^\s*([A-Za-z0-9_.\-]+)\s*(.*)$")
_CONSTRAINT_RE = re.compile(r"^\s*(==|!=|>=|<=|>|<)?\s*(.+?)\s*$")


class CapabilityMismatchAction(str, enum.Enum):
    """Possible broker decisions for an unsupported task message."""

    #: Re-kick the message so another worker can pick it up.
    REROUTE = "reroute"
    #: Refuse to execute the message and store a failed result.
    REJECT = "reject"


class CapabilityMismatch(BaseModel):
    """Structured reason why a worker cannot execute a task message."""

    task_id: str
    task_name: str
    missing_capabilities: list[str]
    incompatible_capabilities: dict[str, str]
    invalid_requirements: str | None = None
    reroute_count: int = 0

    @property
    def reason(self) -> str:
        """Human-readable description of the mismatch."""
        parts = []
        if self.invalid_requirements is not None:
            parts.append(
                f"invalid capability requirements: {self.invalid_requirements}",
            )
        if self.missing_capabilities:
            parts.append(
                "missing capabilities: " + ", ".join(self.missing_capabilities),
            )
        if self.incompatible_capabilities:
            conflicts = ", ".join(
                f"{name} ({problem})"
                for name, problem in sorted(self.incompatible_capabilities.items())
            )
            parts.append(f"incompatible capabilities: {conflicts}")
        return "; ".join(parts) or "unknown capability mismatch"


def _version_key(version: str) -> tuple[tuple[int, Any], ...]:
    """
    Convert a version string into a totally ordered comparison key.

    Numeric runs map to ``(0, int_value)`` and non-numeric runs to
    ``(1, string_value)``, so keys never compare ints with strings
    and the resulting order is fully deterministic.

    :param version: version string.
    :return: comparison key.
    """
    key: list[tuple[int, Any]] = []
    for part in _VERSION_PARTS_RE.findall(version.strip()):
        if part.isdigit():
            key.append((0, int(part)))
        else:
            key.append((1, part))
    return tuple(key)


def compare_versions(left: str, right: str) -> int:
    """
    Compare two version strings using predictable rules.

    :param left: first version.
    :param right: second version.
    :return: -1 if left < right, 0 if equal, 1 if left > right.
    """
    left_key = _version_key(left)
    right_key = _version_key(right)
    if left_key < right_key:
        return -1
    if left_key > right_key:
        return 1
    return 0


def version_satisfies(available: str | None, constraint: str | None) -> bool:
    """
    Check whether an available capability version satisfies a constraint.

    An empty constraint is satisfied by any declared capability.
    A versioned constraint is never satisfied by an unversioned
    capability (``available is None``).

    :param available: version of the capability on the worker or None
        if the capability is unversioned.
    :param constraint: constraint expression or None.
    :return: True if the constraint is satisfied.
    """
    if constraint is None or not constraint.strip():
        return True
    if available is None:
        return False
    operators = {
        "==": lambda cmp: cmp == 0,
        "!=": lambda cmp: cmp != 0,
        ">=": lambda cmp: cmp >= 0,
        "<=": lambda cmp: cmp <= 0,
        ">": lambda cmp: cmp > 0,
        "<": lambda cmp: cmp < 0,
    }
    for single_constraint in constraint.split(","):
        single = single_constraint.strip()
        if not single:
            continue
        match = _CONSTRAINT_RE.match(single)
        if match is None:
            return False
        operator = match.group(1) or ">="
        required_version = match.group(2)
        if operator not in operators:
            return False
        if not operators[operator](compare_versions(available, required_version)):
            return False
    return True


def normalize_requirements(raw: Any) -> dict[str, str | None]:
    """
    Normalize a capability requirement declaration.

    Accepts a mapping of capability name to constraint (or None),
    a single spec string like ``"gpu>=1.0"`` or ``"gpu"``, or an
    iterable of such spec strings.

    :param raw: raw requirement declaration.
    :raises ValueError: if the declaration cannot be understood.
    :return: mapping of capability name to constraint or None.
    """
    if raw is None:
        return {}
    if isinstance(raw, Mapping):
        return {
            str(name): (None if constraint is None else str(constraint))
            for name, constraint in raw.items()
        }
    if isinstance(raw, str):
        return _normalize_spec_string(raw)
    if isinstance(raw, (list, tuple, set, frozenset)):
        merged: dict[str, str | None] = {}
        for item in raw:
            merged.update(normalize_requirements(item))
        return merged
    raise ValueError(f"Unsupported capability requirements declaration: {raw!r}")


def _normalize_spec_string(spec: str) -> dict[str, str | None]:
    """Parse a single ``name`` or ``name<constraint>`` spec string."""
    match = _CAPABILITY_SPEC_RE.match(spec)
    if match is None or not match.group(1):
        raise ValueError(f"Invalid capability requirement spec: {spec!r}")
    name, constraint = match.group(1), match.group(2).strip()
    return {name: constraint or None}


def parse_requirements(raw: Any) -> dict[str, str | None]:
    """
    Parse capability requirements taken from a message label.

    The label may already be a mapping (set programmatically) or a
    JSON string produced by the kicker. Plain spec strings are also
    accepted for convenience.

    :param raw: raw label value.
    :raises ValueError: if the label value cannot be parsed.
    :return: mapping of capability name to constraint or None.
    """
    if isinstance(raw, str):
        stripped = raw.strip()
        if stripped.startswith(("{", "[")):
            try:
                raw = json.loads(stripped)
            except ValueError as exc:
                raise ValueError(
                    f"Cannot parse capability requirements: {exc}",
                ) from exc
    return normalize_requirements(raw)


def find_capability_problems(
    required: Mapping[str, str | None],
    available: Mapping[str, str | None],
) -> tuple[list[str], dict[str, str]]:
    """
    Compare task requirements with worker capabilities.

    :param required: requirements declared by the task.
    :param available: capabilities provided by the worker.
    :return: tuple of missing capability names and a mapping of
        incompatible capability names to problem descriptions.
    """
    missing: list[str] = []
    incompatible: dict[str, str] = {}
    for name, constraint in sorted(required.items()):
        if name not in available:
            missing.append(name)
            continue
        worker_version = available[name]
        if not version_satisfies(worker_version, constraint):
            if worker_version is None:
                incompatible[name] = (
                    f"worker capability is unversioned, "
                    f"task requires {constraint!r}"
                )
            else:
                incompatible[name] = (
                    f"worker provides {worker_version!r}, "
                    f"task requires {constraint!r}"
                )
    return missing, incompatible
