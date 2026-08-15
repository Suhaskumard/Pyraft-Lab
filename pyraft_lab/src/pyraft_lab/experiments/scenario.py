"""Scenario files: what to run, what to break, and when.

Two dialects have to be read here, because the project has two source documents and
they describe the fault section differently. The original writes a flat list of faults,
each with a ``start`` and a ``duration``; the 2.0 proposal writes a chained timeline of
``at:``/``action:`` steps. They are not rivals: a flat fault with a duration is exactly
a pair of timeline steps - do the thing, then undo it - so the flat form is parsed into
the chained one and everything downstream sees a single timeline. A file may mix them.

Nothing here applies a fault or knows what a cluster is. A scenario is a value: parsed,
comparable, and printable, so a malformed file fails at load with a line to look at
rather than halfway through a thirty-second experiment.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from pyraft_lab import NodeId
from pyraft_lab.client.workload import WorkloadConfig

# --- actions the timeline can name ----------------------------------------------------

CRASH = "crash"
RECOVER = "recover"
LATENCY = "latency"
PACKET_LOSS = "packet_loss"
PARTITION = "partition"
HEAL_PARTITION = "heal_partition"
RESET_FAULTS = "reset_faults"

ACTIONS = frozenset(
    {CRASH, RECOVER, LATENCY, PACKET_LOSS, PARTITION, HEAL_PARTITION, RESET_FAULTS}
)

#: How the original document's ``type:`` names map onto timeline actions, and what
#: undoes each one when a ``duration:`` says it should end.
_FLAT_TYPES = {
    "node_crash": (CRASH, RECOVER),
    "crash": (CRASH, RECOVER),
    "partition": (PARTITION, HEAL_PARTITION),
    "latency": (LATENCY, RESET_FAULTS),
    "packet_loss": (PACKET_LOSS, RESET_FAULTS),
    "link_config": (LATENCY, RESET_FAULTS),
}

_DURATION = re.compile(r"^\s*(?P<value>\d+(?:\.\d+)?)\s*(?P<unit>ms|s|m)?\s*$")


class ScenarioError(Exception):
    """Raised for a scenario file that cannot be read as one."""


def parse_duration(value: Any, *, what: str = "duration") -> float:
    """Read ``30s``, ``500ms``, ``2m`` or a bare number, into seconds."""
    if isinstance(value, int | float):
        return float(value)
    if not isinstance(value, str):
        raise ScenarioError(f"{what} must be a number or a string like '30s', got {value!r}")

    match = _DURATION.match(value)
    if match is None:
        raise ScenarioError(f"{what} {value!r} is not a duration like '30s', '500ms' or '2m'")

    seconds = float(match["value"])
    match match["unit"]:
        case "ms":
            return seconds / 1000.0
        case "m":
            return seconds * 60.0
        case _:
            return seconds


@dataclass(frozen=True)
class FaultStep:
    """One thing that happens to the network or a node, at one moment."""

    at: float
    action: str
    target: str | None = None
    "``leader``, a node id, or a 1-based index. Resolved by the runner, not here."

    params: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.action not in ACTIONS:
            known = ", ".join(sorted(ACTIONS))
            raise ScenarioError(f"unknown fault action {self.action!r}; known actions: {known}")

    def to_dict(self) -> dict[str, Any]:
        return {"at": self.at, "action": self.action, "target": self.target, **self.params}

    def describe(self) -> str:
        detail = " ".join(f"{k}={v}" for k, v in self.params.items())
        target = f" {self.target}" if self.target else ""
        return f"{self.at:6.2f}s  {self.action}{target}{' ' + detail if detail else ''}"


@dataclass(frozen=True)
class Scenario:
    """A whole experiment: the cluster, the timeline, the workload, the expectations."""

    name: str
    duration: float
    nodes: int = 3
    faults: tuple[FaultStep, ...] = ()
    workload: WorkloadConfig = field(default_factory=WorkloadConfig)
    expected: dict[str, Any] = field(default_factory=dict)
    seed: int = 0
    source: Path | None = None

    @property
    def node_ids(self) -> list[NodeId]:
        """The cluster's membership. Generated, not configured - a scenario says how
        many nodes it wants, and the ids follow, so nothing has to keep two lists in
        step."""
        return [f"n{i}" for i in range(1, self.nodes + 1)]

    def resolve_node(self, target: str | int | None) -> NodeId | None:
        """Turn ``2``, ``"2"``, ``"n2"`` or ``"node-2"`` into a member of this cluster.

        The original document's partition groups are written as bare integers, and
        2.0's as ``node-N`` names; both mean the same nodes, so both resolve here rather
        than forcing one dialect on whoever writes a scenario.
        """
        if target is None:
            return None
        text = str(target).strip()
        if text in self.node_ids:
            return text

        digits = re.fullmatch(r"(?:n|node[-_]?)?(\d+)", text, flags=re.IGNORECASE)
        if digits is not None:
            index = int(digits[1])
            if 1 <= index <= self.nodes:
                return self.node_ids[index - 1]
            raise ScenarioError(f"node {target!r} is outside a {self.nodes}-node cluster")
        raise ScenarioError(f"{target!r} does not name a node in a {self.nodes}-node cluster")

    def timeline(self) -> str:
        """The fault schedule, as a reader would want to see it."""
        if not self.faults:
            return "  (no faults)"
        return "\n".join(f"  {step.describe()}" for step in self.faults)

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "duration": self.duration,
            "nodes": self.nodes,
            "seed": self.seed,
            "faults": [step.to_dict() for step in self.faults],
            "workload": self.workload.to_dict(),
            "expected": dict(self.expected),
        }

    # --- loading ------------------------------------------------------------------

    @classmethod
    def from_dict(cls, data: dict[str, Any], *, source: Path | None = None) -> Scenario:
        if not isinstance(data, dict):
            raise ScenarioError(f"a scenario must be a mapping, got {type(data).__name__}")
        if "name" not in data:
            raise ScenarioError("a scenario needs a name")
        if "duration" not in data:
            raise ScenarioError(f"scenario {data['name']!r} needs a duration")

        nodes = int(data.get("nodes", 3))
        if nodes < 1:
            raise ScenarioError(f"a cluster needs at least one node, got {nodes}")

        return cls(
            name=str(data["name"]),
            duration=parse_duration(data["duration"], what="scenario duration"),
            nodes=nodes,
            faults=_parse_faults(data.get("faults")),
            workload=_parse_workload(data.get("workload")),
            expected=dict(data.get("expected") or {}),
            seed=int(data.get("seed", 0)),
            source=source,
        )

    @classmethod
    def from_yaml(cls, text: str, *, source: Path | None = None) -> Scenario:
        try:
            data = yaml.safe_load(text)
        except yaml.YAMLError as exc:
            where = f" in {source}" if source else ""
            raise ScenarioError(f"could not parse YAML{where}: {exc}") from exc
        return cls.from_dict(data, source=source)

    @classmethod
    def load(cls, path: str | Path) -> Scenario:
        path = Path(path)
        if not path.exists():
            raise ScenarioError(f"no scenario file at {path}")
        return cls.from_yaml(path.read_text(encoding="utf-8"), source=path)


def _parse_workload(data: Any) -> WorkloadConfig:
    if not data:
        return WorkloadConfig()
    if not isinstance(data, dict):
        raise ScenarioError(f"workload must be a mapping, got {type(data).__name__}")

    known = {f.name for f in WorkloadConfig.__dataclass_fields__.values()}
    unknown = set(data) - known
    if unknown:
        raise ScenarioError(
            f"unknown workload settings: {', '.join(sorted(unknown))}; "
            f"known settings: {', '.join(sorted(known))}"
        )
    return WorkloadConfig(**data)


def _parse_faults(data: Any) -> tuple[FaultStep, ...]:
    """Read either dialect into one timeline, sorted by time.

    ``faults: {}`` - which is how the original document writes "none" - is accepted
    alongside an absent key and an empty list. A scenario that says it has no faults in
    three different ways still has no faults.
    """
    if not data:
        return ()
    if not isinstance(data, list):
        raise ScenarioError(f"faults must be a list, got {type(data).__name__}")

    steps: list[FaultStep] = []
    for index, entry in enumerate(data):
        if not isinstance(entry, dict):
            raise ScenarioError(f"fault #{index + 1} must be a mapping, got {entry!r}")
        if "action" in entry or "at" in entry:
            steps.extend(_parse_chained(entry, index))
        elif "type" in entry:
            steps.extend(_parse_flat(entry, index))
        else:
            raise ScenarioError(
                f"fault #{index + 1} has neither an 'action' (chained form) nor a "
                f"'type' (flat form): {entry!r}"
            )

    # Stable sort: two steps at the same instant keep the order the file wrote them,
    # which is the only thing that can disambiguate "partition then crash" from
    # "crash then partition" at t=10s.
    return tuple(sorted(steps, key=lambda step: step.at))


def _parse_chained(entry: dict[str, Any], index: int) -> list[FaultStep]:
    if "action" not in entry:
        raise ScenarioError(f"fault #{index + 1} has an 'at' but no 'action': {entry!r}")

    at = parse_duration(entry.get("at", 0), what=f"fault #{index + 1} 'at'")
    action = str(entry["action"])
    params = {k: v for k, v in entry.items() if k not in {"at", "action", "target", "duration"}}
    steps = [FaultStep(at=at, action=action, target=_target_of(entry), params=params)]

    # A chained step may still carry a duration; 2.0's own examples heal explicitly, but
    # allowing it means the two dialects have no capability the other lacks.
    if entry.get("duration") is not None:
        steps.append(_undo(action, entry, at + parse_duration(entry["duration"])))
    return steps


def _parse_flat(entry: dict[str, Any], index: int) -> list[FaultStep]:
    kind = str(entry["type"])
    if kind not in _FLAT_TYPES:
        known = ", ".join(sorted(_FLAT_TYPES))
        raise ScenarioError(f"unknown fault type {kind!r} in fault #{index + 1}; known: {known}")

    action, _ = _FLAT_TYPES[kind]
    at = parse_duration(entry.get("start", 0), what=f"fault #{index + 1} 'start'")
    params = {
        k: v for k, v in entry.items() if k not in {"type", "start", "duration", "target"}
    }

    steps = [FaultStep(at=at, action=action, target=_target_of(entry), params=params)]

    # ``link_config`` is the one flat type that means two things at once.
    if kind == "link_config" and entry.get("drop_rate"):
        steps.append(FaultStep(at=at, action=PACKET_LOSS, params={"rate": entry["drop_rate"]}))

    if entry.get("duration") is not None:
        ends_at = at + parse_duration(entry["duration"], what=f"fault #{index + 1} 'duration'")
        steps.append(_undo(action, entry, ends_at))
    return steps


def _undo(action: str, entry: dict[str, Any], at: float) -> FaultStep:
    """The step that ends a fault with a duration."""
    if action == CRASH:
        return FaultStep(at=at, action=RECOVER, target=_target_of(entry))
    if action == PARTITION:
        return FaultStep(at=at, action=HEAL_PARTITION)
    return FaultStep(at=at, action=RESET_FAULTS)


def _target_of(entry: dict[str, Any]) -> str | None:
    target = entry.get("target")
    return None if target is None else str(target)


def load_all(directory: str | Path) -> list[Scenario]:
    """Every ``*.yaml`` in a directory, in filename order."""
    return [Scenario.load(path) for path in sorted(Path(directory).glob("*.yaml"))]
