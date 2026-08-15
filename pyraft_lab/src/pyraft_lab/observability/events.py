"""The event vocabulary: what a run records, and the shape it records it in.

Twelve kinds, fixed by the specification's list and deliberately not extended - a
vocabulary that grows a kind per debugging session stops being a vocabulary. Anything
finer-grained than these belongs in an event's ``details``, where it costs nothing to
add and nothing downstream has to learn about it.

An event is a fact about a run, not a log line: it carries the clock reading it
happened at, who it happened to, and the term it happened in, so a whole run can be
reconstructed afterwards from nothing but the trace. That reconstruction is what
Phase 5's metrics, Phase 7's replay and Phase 11's dashboard all read.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Any

from pyraft_lab import NodeId


class EventKind(StrEnum):
    """Every event a run can record.

    ``StrEnum`` rather than a bare class of constants: a member compares equal to its
    own name, so a trace read back from JSON needs no conversion step to be filtered
    against these, while ``EventKind("NOPE")`` still fails loudly at the boundary.
    """

    ELECTION_STARTED = "ELECTION_STARTED"
    VOTE_REQUESTED = "VOTE_REQUESTED"
    VOTE_GRANTED = "VOTE_GRANTED"
    LEADER_ELECTED = "LEADER_ELECTED"
    APPEND_ENTRIES_SENT = "APPEND_ENTRIES_SENT"
    APPEND_ENTRIES_REJECTED = "APPEND_ENTRIES_REJECTED"
    LOG_COMMITTED = "LOG_COMMITTED"
    NODE_CRASHED = "NODE_CRASHED"
    NODE_RECOVERED = "NODE_RECOVERED"
    PARTITION_CREATED = "PARTITION_CREATED"
    PARTITION_HEALED = "PARTITION_HEALED"
    TERM_CHANGED = "TERM_CHANGED"


NETWORK_EVENTS = frozenset(
    {
        EventKind.NODE_CRASHED,
        EventKind.NODE_RECOVERED,
        EventKind.PARTITION_CREATED,
        EventKind.PARTITION_HEALED,
    }
)
"""Events the *network* narrates rather than a node.

A crashed node cannot record its own crash, and a partition belongs to no single node,
so these come from ``SimulatedNetwork`` and carry no term - see docs/architecture.md P4.
"""


@dataclass(frozen=True, slots=True)
class Event:
    """One recorded fact.

    Frozen for the same reason messages are (docs/architecture.md D1): a trace that a
    consumer can edit is not a record of anything. ``details`` is a plain dict rather
    than a per-kind schema because its keys differ per kind and every consumer either
    knows which kind it asked for or is displaying the pairs verbatim.
    """

    kind: EventKind
    timestamp: float
    seq: int
    node: NodeId | None = None
    term: int | None = None
    details: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """The specification's JSON shape, plus ``seq``.

        ``seq`` is not decoration: under a ``VirtualClock`` many events share one
        timestamp - time only moves when a test advances it - so timestamp alone is not
        a total order, and a replay that reorders two same-instant events is not a
        replay. See docs/architecture.md P4.
        """
        return {
            "timestamp": self.timestamp,
            "seq": self.seq,
            "node": self.node,
            "term": self.term,
            "event": str(self.kind),
            "details": self.details,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Event:
        return cls(
            kind=EventKind(data["event"]),
            timestamp=data["timestamp"],
            seq=data["seq"],
            node=data.get("node"),
            term=data.get("term"),
            details=data.get("details") or {},
        )

    @property
    def order(self) -> tuple[float, int]:
        """The total order events sort in: clock reading first, then emission order."""
        return (self.timestamp, self.seq)

    def __str__(self) -> str:
        where = self.node if self.node is not None else "network"
        term = f"t{self.term}" if self.term is not None else "-"
        detail = " ".join(f"{k}={v}" for k, v in self.details.items())
        head = f"[{self.timestamp:8.3f}] {where:>8} {term:>4} {self.kind}"
        return f"{head} {detail}" if detail else head


def to_jsonl(events: list[Event]) -> str:
    """Render a trace as JSON Lines - one event per line, appendable, streamable."""
    return "".join(json.dumps(event.to_dict(), sort_keys=True) + "\n" for event in events)


def write_jsonl(path: str | Path, events: list[Event]) -> None:
    Path(path).write_text(to_jsonl(events), encoding="utf-8")


def read_jsonl(path: str | Path) -> list[Event]:
    """Load a trace written by :func:`write_jsonl`.

    The half of the round trip Phase 7's replay is built on: a run that cannot be read
    back from disk can only be inspected while it is still in memory, which is exactly
    when nobody needs to.
    """
    text = Path(path).read_text(encoding="utf-8")
    return [Event.from_dict(json.loads(line)) for line in text.splitlines() if line.strip()]
