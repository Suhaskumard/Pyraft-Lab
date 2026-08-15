"""Is this history linearizable - and if not, which operation gave it away?

Linearizable means: every operation appears to take effect instantaneously at some
moment between when it was invoked and when it returned, and the resulting sequence is
a legal run of the state machine. Concurrent operations may be ordered any way at all;
operations that did not overlap may not be reordered.

Two pieces, and they are of different kinds:

- **The verdict is exact.** A backtracking search over every ordering the real-time
  constraints permit, per key, with a step budget. If it finds an ordering, the history
  is linearizable; if it exhausts the space, it is not. When the budget runs out first
  the result says so rather than guessing - ``exhausted`` is a third answer, not a
  quiet PASS.
- **The explanation is a localization.** A failed search proves only that *no* ordering
  works, not which operation is to blame. So the offending operation is identified by
  two rules that are individually sound - each one, on its own, proves the history is
  not linearizable - and if neither fires the report says which key failed without
  pretending to know more (see :class:`Violation`).

Checking each key independently is not an approximation. Herlihy & Wing's locality
theorem says a history is linearizable exactly when its projection onto each object is,
and this KV store has no operation spanning two keys - no multi-key transactions - so
each key is its own object. It is also what keeps the search tractable: the cost is
exponential in operations *per key*, not in the run.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from pyraft_lab import NodeId
from pyraft_lab.consistency.history import History, Operation, OpKind
from pyraft_lab.observability.events import Event, EventKind

DEFAULT_MAX_STEPS = 100_000
"""Search states explored before a key is reported as ``exhausted`` rather than judged."""


class _Absent:
    """The state of a key nobody has written yet, or one that has been deleted.

    A sentinel rather than ``None`` because ``None`` is a value a client can legitimately
    store, and a checker that cannot tell "absent" from "present, holding null" would
    pass a history in which a delete never happened.
    """

    __slots__ = ()

    def __repr__(self) -> str:
        return "<absent>"


ABSENT = _Absent()

WRITES = (OpKind.PUT, OpKind.DELETE, OpKind.CAS)


# --- the sequential specification ----------------------------------------------------
#
# One function per question: "could this operation have returned what it returned, in
# this state?" and "what does the state become?". Kept together and kept small - this
# is the definition of correct that everything else in the file argues about.


def _legal(op: Operation, state: Any) -> bool:
    """Could ``op`` have observed what it reports, applied in ``state``?"""
    present = state is not ABSENT
    current = None if state is ABSENT else state

    match op.kind:
        case OpKind.PUT:
            # A put is legal in any state; it reports back the value it stored.
            return op.result == op.value
        case OpKind.GET:
            return op.ok == present and op.result == current
        case OpKind.DELETE:
            return bool(op.result) == present
        case OpKind.CAS:
            return bool(op.result) == (current == op.expected)


def _apply(op: Operation, state: Any) -> Any:
    current = None if state is ABSENT else state

    match op.kind:
        case OpKind.PUT:
            return op.value
        case OpKind.GET:
            return state
        case OpKind.DELETE:
            return ABSENT
        case OpKind.CAS:
            return op.value if current == op.expected else state


def _writes_value(op: Operation, value: Any) -> bool:
    """Could this operation have left ``value`` behind for a later read to find?"""
    if op.kind is OpKind.PUT:
        return op.value == value
    if op.kind is OpKind.CAS:
        # A CAS that lost changed nothing; one that won left `value` behind. An
        # incomplete CAS might have done either, so it counts as possibly having won.
        return op.value == value and (not op.completed or bool(op.result))
    return False


# --- results -------------------------------------------------------------------------


@dataclass(frozen=True)
class Violation:
    """One localized failure, in the shape the specification asks a report to take."""

    key: str
    operations: list[Operation]
    "The offending operations, earliest first. Usually the read, then what it should have seen."

    cause: str
    "A heuristic, and labelled as one in the report. The verdict above it is not."

    at: float | None = None
    node: NodeId | None = None
    term: int | None = None
    certain: bool = True
    "False when the search proved non-linearizability but no single rule could localize it."

    def explain(self) -> str:
        lines = ["Violation:"]
        lines += [f"  {op.describe()}" for op in self.operations]
        lines.append("")
        if self.node is not None:
            lines.append(f"  Node: {self.node}")
        if self.term is not None:
            lines.append(f"  Term: {self.term}")
        if self.at is not None:
            lines.append(f"  Time: {self.at:.3f}s")
        lines.append("")
        lines.append(f"  Possible cause:\n    {self.cause}")
        return "\n".join(lines)

    def to_dict(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "operations": [op.to_dict() for op in self.operations],
            "cause": self.cause,
            "at": self.at,
            "node": self.node,
            "term": self.term,
            "certain": self.certain,
        }


@dataclass(frozen=True)
class CheckResult:
    """The verdict for a whole history."""

    linearizable: bool
    violations: list[Violation] = field(default_factory=list)
    keys_checked: int = 0
    operations_checked: int = 0
    states_explored: int = 0
    exhausted_keys: list[str] = field(default_factory=list)
    "Keys whose search hit the step budget. Neither passed nor failed - unproven."

    @property
    def conclusive(self) -> bool:
        return not self.exhausted_keys

    @property
    def verdict(self) -> str:
        if self.exhausted_keys and self.linearizable:
            return "UNPROVEN"
        return "PASS" if self.linearizable else "FAIL"

    def explain(self) -> str:
        """The report the specification asks for: a verdict, then why."""
        lines = [f"Linearizability: {self.verdict}"]
        if self.exhausted_keys:
            budget = ", ".join(self.exhausted_keys)
            lines.append(f"  (search budget exhausted on: {budget} - not proven either way)")
        for violation in self.violations:
            lines.append("")
            lines.append(violation.explain())
        return "\n".join(lines)

    def to_dict(self) -> dict[str, Any]:
        return {
            "linearizable": self.linearizable,
            "verdict": self.verdict,
            "conclusive": self.conclusive,
            "keys_checked": self.keys_checked,
            "operations_checked": self.operations_checked,
            "states_explored": self.states_explored,
            "exhausted_keys": list(self.exhausted_keys),
            "violations": [v.to_dict() for v in self.violations],
        }


# --- the search ----------------------------------------------------------------------


class _Budget:
    """A shared step counter, so one pathological key cannot hang a whole run."""

    def __init__(self, max_steps: int) -> None:
        self.max_steps = max_steps
        self.used = 0
        self.exhausted = False

    def spend(self) -> bool:
        self.used += 1
        if self.used > self.max_steps:
            self.exhausted = True
            return False
        return True


def _search(ops: list[Operation], budget: _Budget) -> bool:
    """Is there a legal ordering of ``ops`` consistent with their real-time order?"""
    seen: set[tuple[frozenset[int], str]] = set()

    def step(remaining: frozenset[int], state: Any) -> bool:
        if not remaining:
            return True
        if not budget.spend():
            return False

        memo = (remaining, repr(state))
        if memo in seen:
            return False
        seen.add(memo)

        pending = [op for op in ops if op.id in remaining]
        for op in pending:
            # An operation may go next only if nothing still unlinearized provably
            # finished before it began - that is the one freedom real time does not give.
            if any(other.precedes(op) for other in pending if other.id != op.id):
                continue

            if op.completed:
                if not _legal(op, state):
                    continue
                if step(remaining - {op.id}, _apply(op, state)):
                    return True
            else:
                # No response ever came back, so nothing is claimed about what it
                # returned - but it may still have taken effect, or not at all. Both
                # possibilities are real and both are explored.
                if step(remaining - {op.id}, _apply(op, state)):
                    return True
                if step(remaining - {op.id}, state):
                    return True

        return False

    return step(frozenset(op.id for op in ops), ABSENT)


# --- localizing a failure -------------------------------------------------------------


def _possible_sources(read: Operation, ops: list[Operation]) -> list[Operation]:
    """Writes that could have supplied ``read``'s value.

    A write that begins only after the read has already returned cannot be one of them;
    everything else is fair game, including writes concurrent with the read.
    """
    return [
        op
        for op in ops
        if op.kind in WRITES and not read.precedes(op) and _writes_value(op, read.result)
    ]


def _localize(key: str, ops: list[Operation]) -> tuple[list[Operation], str] | None:
    """Find an operation that is provably wrong on its own, and say what is wrong.

    Both rules below are sound: either one firing is by itself a proof that no
    linearization exists, independent of the search. Returning ``None`` means the
    failure is real but distributed across the ordering rather than pinned to one read.
    """
    reads = [op for op in ops if op.kind is OpKind.GET and op.completed]

    for read in reads:
        observed_absent = not read.ok
        sources = _possible_sources(read, ops)

        # Rule 1 - phantom read. Nothing that could have run before this read ever
        # wrote the value it returned, and it did not read an empty key either.
        if not observed_absent and not sources:
            return [read], (
                f"read a value for {key!r} that no write could have placed there - "
                "a value appeared from nowhere, or was resurrected after deletion"
            )

        # Rule 2 - stale read. Some write definitely finished before this read began,
        # and every write that could explain the value finished before *that* one. The
        # value the read returned had already been replaced by the time it was asked.
        preceding = [op for op in ops if op.kind in WRITES and op.precedes(read) and op.ok]
        if not preceding:
            continue
        latest = max(preceding, key=lambda op: op.completed_at or 0.0)

        if observed_absent:
            if latest.kind in (OpKind.PUT, OpKind.CAS):
                return [read, latest], (
                    f"read {key!r} as missing after a write to it had already completed - "
                    "a stale replica, or a leader that had lost its term serving a read"
                )
            continue

        if sources and all(source.precedes(latest) for source in sources):
            return [latest, read], (
                "stale read: every write that could have produced this value had already "
                f"been overwritten by {latest.describe()} before the read began"
            )

    return None


# --- reading the trace for context -----------------------------------------------------


class _TraceContext:
    """What the event trace can add to an explanation.

    Optional throughout: a history alone is enough to prove a violation, and everything
    here only sharpens the *possible cause* line. Without a trace the report is smaller
    but no less certain.
    """

    def __init__(self, events: list[Event] | None) -> None:
        self.events = sorted(events or [], key=lambda e: e.order)

    def term_at(self, when: float) -> int | None:
        terms = [e.term for e in self.events if e.timestamp <= when and e.term is not None]
        return terms[-1] if terms else None

    def leader_at(self, when: float) -> NodeId | None:
        """Who had most recently been elected as of ``when``.

        Not "who was leader" - nobody in the system knows that globally, which is rather
        the point. It is the last election the trace recorded before this moment.
        """
        elected = [
            e for e in self.events if e.kind is EventKind.LEADER_ELECTED and e.timestamp <= when
        ]
        return elected[-1].node if elected else None

    def partitioned_at(self, when: float) -> bool:
        split = False
        for event in self.events:
            if event.timestamp > when:
                break
            if event.kind is EventKind.PARTITION_CREATED:
                split = True
            elif event.kind is EventKind.PARTITION_HEALED:
                split = False
        return split

    def election_between(self, start: float, end: float) -> bool:
        return any(
            e.kind is EventKind.LEADER_ELECTED and start <= e.timestamp <= end for e in self.events
        )

    def crashed_at(self, when: float) -> set[NodeId]:
        down: set[NodeId] = set()
        for event in self.events:
            if event.timestamp > when:
                break
            if event.kind is EventKind.NODE_CRASHED and event.node is not None:
                down.add(event.node)
            elif event.kind is EventKind.NODE_RECOVERED and event.node is not None:
                down.discard(event.node)
        return down


def _diagnose(operations: list[Operation], base_cause: str, trace: _TraceContext) -> str:
    """Sharpen the cause with whatever the trace says was happening at the time."""
    last = operations[-1]
    when = last.completed_at if last.completed_at is not None else last.invoked_at
    first = operations[0]
    window_start = first.invoked_at

    context: list[str] = []
    if trace.partitioned_at(when):
        served_by = f" by {last.target}" if last.target else ""
        context.append(f"the network was partitioned when this was served{served_by}")
    if trace.election_between(window_start, when):
        context.append("a leader was elected inside this window")
    down = trace.crashed_at(when)
    if down:
        context.append(f"nodes down at the time: {', '.join(sorted(down))}")

    if not context:
        return base_cause
    return f"{base_cause}. At the time: " + "; ".join(context)


# --- the entry point --------------------------------------------------------------------


def check(
    history: History,
    *,
    events: list[Event] | None = None,
    max_steps: int = DEFAULT_MAX_STEPS,
) -> CheckResult:
    """Check ``history`` for linearizability, key by key.

    Pass ``events`` - a Phase 4 trace - to have violations annotated with the term, the
    node that served the offending operation, and what the cluster was doing at the
    time. The verdict does not depend on them.
    """
    trace = _TraceContext(events)
    budget = _Budget(max_steps)

    violations: list[Violation] = []
    exhausted: list[str] = []
    keys = history.keys

    for key in keys:
        ops = sorted(history.for_key(key), key=lambda op: (op.invoked_at, op.id))
        if not ops:
            continue

        before = budget.exhausted
        if _search(ops, budget):
            continue
        if budget.exhausted and not before:
            # Ran out of room rather than out of orderings: say so instead of accusing.
            exhausted.append(key)
            continue

        localized = _localize(key, ops)
        if localized is not None:
            offenders, cause = localized
            certain = True
        else:
            # The search is still conclusive - no ordering exists - but no single rule
            # pins it on one read, so the report shows the completed operations on the
            # key and says plainly that it could not narrow it further.
            offenders = [op for op in ops if op.completed][:6]
            cause = (
                f"no ordering of the operations on {key!r} satisfies both the "
                "real-time order and the state machine; the conflict is in the "
                "combination rather than in any single response"
            )
            certain = False

        last = offenders[-1] if offenders else ops[-1]
        when = last.completed_at if last.completed_at is not None else last.invoked_at
        violations.append(
            Violation(
                key=key,
                operations=offenders,
                cause=_diagnose(offenders or ops, cause, trace),
                at=when,
                node=last.target,
                term=trace.term_at(when),
                certain=certain,
            )
        )

    return CheckResult(
        linearizable=not violations,
        violations=violations,
        keys_checked=len(keys),
        operations_checked=len(history),
        states_explored=budget.used,
        exhausted_keys=exhausted,
    )
