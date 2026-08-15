"""``Tracer``: the recorder every node and the network write their events to.

One tracer per *run*, not per node. A node's own view of a run is exactly the view
that cannot explain a distributed failure - the interesting question is always what
some other node was doing at the same moment - so the trace is assembled centrally and
ordered globally as it is written, rather than merged from per-node files afterwards.

Timestamps come from an injected :class:`~pyraft_lab.network.clock.Clock`, the same one
the nodes' timers and the simulator's deliveries run against. Under a ``VirtualClock``
that makes the whole trace reproducible from a seed; under a ``WallClock`` it is simply
the loop's clock.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator
from pathlib import Path
from typing import TYPE_CHECKING, Any

from pyraft_lab import NodeId
from pyraft_lab.observability.events import Event, EventKind, write_jsonl

if TYPE_CHECKING:
    from pyraft_lab.network.clock import Clock

Subscriber = Callable[[Event], None]


class Tracer:
    """Collects :class:`Event` records and hands them to any live subscribers.

    Recording is opt-in per run: a node built without a tracer gets :data:`NULL_TRACER`,
    which drops everything before it touches the clock. That keeps the instrumentation
    genuinely free for the tests and workloads that do not want it, rather than merely
    cheap.
    """

    def __init__(self, clock: Clock | None = None, *, enabled: bool = True) -> None:
        # Left as given - possibly None - and resolved on first use by ``_now``. The
        # default is a WallClock, but importing one here would close a cycle: the
        # simulator imports this module to get NULL_TRACER, and ``network.clock``
        # cannot be reached without initializing ``network``. Deferring the import to
        # the first timestamp costs one branch per event and keeps the two packages
        # independent at import time.
        self._clock: Clock | None = clock
        self.enabled = enabled
        self._events: list[Event] = []
        self._subscribers: list[Subscriber] = []
        self._seq = 0

    # --- recording ------------------------------------------------------------------

    def _now(self) -> float:
        if self._clock is None:
            from pyraft_lab.network.clock import WallClock

            self._clock = WallClock()
        return self._clock.now()

    def emit(
        self,
        kind: EventKind,
        *,
        node: NodeId | None = None,
        term: int | None = None,
        **details: Any,
    ) -> Event | None:
        """Record one event. Returns it, or ``None`` if this tracer is disabled.

        Extra keyword arguments become the event's ``details``, so a caller adds
        context by naming it rather than by constructing anything.
        """
        if not self.enabled:
            return None

        event = Event(
            kind=kind,
            timestamp=self._now(),
            seq=self._seq,
            node=node,
            term=term,
            details=details,
        )
        self._seq += 1
        self._events.append(event)
        for subscriber in self._subscribers:
            subscriber(event)
        return event

    def subscribe(self, subscriber: Subscriber) -> None:
        """Call ``subscriber`` with every event from here on.

        What :class:`~pyraft_lab.observability.metrics.LiveMetrics` attaches itself
        with. Subscribers see events as they happen and are not replayed the backlog:
        anything wanting the whole history can read :attr:`events` for it.
        """
        self._subscribers.append(subscriber)

    def clear(self) -> None:
        """Drop the recorded trace. Sequence numbers keep counting, so events either
        side of a clear can still be ordered against each other."""
        self._events.clear()

    # --- reading --------------------------------------------------------------------

    @property
    def events(self) -> list[Event]:
        """The trace so far, in emission order (a copy - the trace itself is ours)."""
        return list(self._events)

    def of_kind(self, *kinds: EventKind) -> list[Event]:
        wanted = frozenset(kinds)
        return [e for e in self._events if e.kind in wanted]

    def for_node(self, node: NodeId) -> list[Event]:
        return [e for e in self._events if e.node == node]

    def in_term(self, term: int) -> list[Event]:
        return [e for e in self._events if e.term == term]

    def count(self, *kinds: EventKind) -> int:
        return len(self.of_kind(*kinds))

    def first(self, kind: EventKind) -> Event | None:
        return next((e for e in self._events if e.kind is kind), None)

    def last(self, kind: EventKind) -> Event | None:
        return next((e for e in reversed(self._events) if e.kind is kind), None)

    def __len__(self) -> int:
        return len(self._events)

    def __iter__(self) -> Iterator[Event]:
        return iter(self._events)

    # --- persistence ----------------------------------------------------------------

    def write_jsonl(self, path: str | Path) -> None:
        """Persist the trace for Phase 5's metrics and Phase 7's replay."""
        write_jsonl(path, self._events)

    def story(self, events: Iterable[Event] | None = None) -> str:
        """The trace as readable lines. For a failing test, or a `trace` CLI command."""
        return "\n".join(str(e) for e in (self._events if events is None else events))


NULL_TRACER = Tracer(enabled=False)
"""The tracer a node uses when nobody asked for a trace.

Shared, and safe to share: a disabled tracer records nothing, notifies nobody and holds
no state that could leak between runs.
"""
