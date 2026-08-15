"""Recording what clients actually asked for, and what they actually got back.

A linearizability check is only as good as the history it reads, and the thing that
makes a history checkable is that every operation is recorded *twice*: once when the
client invoked it and once when it returned. The gap between those two moments is what
makes two operations concurrent, and concurrency is the only reason a history has any
freedom to be linearized at all.

An operation that never returned is not a failure to record - it is a real outcome, and
a meaningful one: a client that times out genuinely does not know whether its write
took effect. Those are kept, with ``completed_at`` left as ``None``, and the checker
treats them as optional rather than pretending either way.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Any

from pyraft_lab import NodeId


class OpKind(StrEnum):
    """The four things a client can ask for - the KV command set, no more."""

    PUT = "put"
    GET = "get"
    DELETE = "delete"
    CAS = "cas"


@dataclass(slots=True)
class Operation:
    """One client operation, from invocation to response.

    Mutable, unlike almost everything else here, and deliberately: an operation is
    recorded at two separate moments, and the interval between them is the datum. A
    frozen record would mean building the invocation and the response separately and
    joining them later, which is the same mutation with extra steps.
    """

    id: int
    client: NodeId
    kind: OpKind
    key: str
    invoked_at: float
    target: NodeId | None = None
    "The node the client was talking to when it got its answer."

    value: Any = None
    "The argument: the new value for PUT, or the proposed value for CAS."

    expected: Any = None
    "CAS only: the value the swap was conditional on."

    completed_at: float | None = None
    result: Any = None
    ok: bool = False
    error: str | None = None

    @property
    def completed(self) -> bool:
        """Did the client ever learn the outcome?

        A `False` here is not the same as ``ok is False``. A rejected CAS completed
        perfectly well and returned False; an operation that timed out returned
        nothing, and may or may not have taken effect.
        """
        return self.completed_at is not None

    @property
    def duration(self) -> float | None:
        return None if self.completed_at is None else self.completed_at - self.invoked_at

    def precedes(self, other: Operation) -> bool:
        """True if this operation provably finished before ``other`` began.

        The real-time order a linearization is not allowed to contradict. An
        incomplete operation precedes nothing - it may still have been in flight.

        Strictly less-than, and that matters more than it looks. A ``VirtualClock``
        only moves when a timer fires, so any number of operations can share one
        timestamp: a response and an invocation reading exactly the same instant is
        the normal case, not a coincidence. Treating equality as precedence would be
        claiming an ordering the clock never measured - and would report a violation
        every time a read was served in the same instant a write returned, purely
        because of the order two coroutines happened to resume in. Equal timestamps
        mean concurrent, which is the honest reading of "no time passed between them".
        """
        return self.completed_at is not None and self.completed_at < other.invoked_at

    def concurrent_with(self, other: Operation) -> bool:
        return not self.precedes(other) and not other.precedes(self)

    def describe(self) -> str:
        """The operation as a reader would say it out loud: ``PUT(x, 10) -> ok``.

        Plain ASCII, deliberately. This ends up on a terminal, and a Windows console
        under its default code page cannot encode an arrow - a report that raises
        ``UnicodeEncodeError`` while explaining a consistency violation is worse than
        one that prints a hyphen.
        """
        match self.kind:
            case OpKind.PUT:
                call = f"PUT({self.key}, {self.value!r})"
            case OpKind.GET:
                call = f"GET({self.key})"
            case OpKind.DELETE:
                call = f"DELETE({self.key})"
            case OpKind.CAS:
                call = f"CAS({self.key}, {self.expected!r} -> {self.value!r})"
        if not self.completed:
            return f"{call} -> (no response)"
        return f"{call} -> {self.result!r}"

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "client": self.client,
            "kind": str(self.kind),
            "key": self.key,
            "invoked_at": self.invoked_at,
            "completed_at": self.completed_at,
            "target": self.target,
            "value": self.value,
            "expected": self.expected,
            "result": self.result,
            "ok": self.ok,
            "error": self.error,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Operation:
        return cls(
            id=data["id"],
            client=data["client"],
            kind=OpKind(data["kind"]),
            key=data["key"],
            invoked_at=data["invoked_at"],
            target=data.get("target"),
            value=data.get("value"),
            expected=data.get("expected"),
            completed_at=data.get("completed_at"),
            result=data.get("result"),
            ok=data.get("ok", False),
            error=data.get("error"),
        )


@dataclass
class History:
    """Every operation a run's clients performed, in invocation order.

    One recorder for the whole run rather than one per client, for the same reason
    there is one tracer: a per-client history cannot show that two clients' operations
    overlapped, and overlap is the entire question.
    """

    operations: list[Operation] = field(default_factory=list)
    _next_id: int = 0

    def invoke(
        self,
        client: NodeId,
        kind: OpKind,
        key: str,
        at: float,
        *,
        value: Any = None,
        expected: Any = None,
    ) -> Operation:
        """Record an operation being issued. Returns it, to be completed later."""
        operation = Operation(
            id=self._next_id,
            client=client,
            kind=kind,
            key=key,
            invoked_at=at,
            value=value,
            expected=expected,
        )
        self._next_id += 1
        self.operations.append(operation)
        return operation

    def complete(
        self,
        operation: Operation,
        at: float,
        *,
        result: Any = None,
        ok: bool = True,
        target: NodeId | None = None,
    ) -> Operation:
        operation.completed_at = at
        operation.result = result
        operation.ok = ok
        operation.target = target
        return operation

    def fail(
        self,
        operation: Operation,
        at: float,
        error: str,
        *,
        target: NodeId | None = None,
    ) -> Operation:
        """Record an operation the client gave up on.

        ``completed_at`` stays ``None``: the client learned nothing, so neither does the
        checker get to assume anything. The time is kept as ``error`` context only.
        """
        operation.error = f"{error} (gave up at {at:.3f}s)"
        operation.ok = False
        operation.target = target
        return operation

    # --- reading -----------------------------------------------------------------

    def __len__(self) -> int:
        return len(self.operations)

    def __iter__(self):
        return iter(self.operations)

    @property
    def keys(self) -> list[str]:
        """Every key touched, in first-touch order."""
        return list(dict.fromkeys(op.key for op in self.operations))

    def for_key(self, key: str) -> list[Operation]:
        return [op for op in self.operations if op.key == key]

    @property
    def completed(self) -> list[Operation]:
        return [op for op in self.operations if op.completed]

    @property
    def incomplete(self) -> list[Operation]:
        return [op for op in self.operations if not op.completed]

    def to_dicts(self) -> list[dict[str, Any]]:
        return [op.to_dict() for op in self.operations]

    def write_json(self, path: str | Path) -> None:
        Path(path).write_text(json.dumps(self.to_dicts(), indent=2), encoding="utf-8")

    @classmethod
    def from_dicts(cls, data: list[dict[str, Any]]) -> History:
        history = cls()
        history.operations = [Operation.from_dict(item) for item in data]
        history._next_id = max((op.id for op in history.operations), default=-1) + 1
        return history

    @classmethod
    def read_json(cls, path: str | Path) -> History:
        return cls.from_dicts(json.loads(Path(path).read_text(encoding="utf-8")))
