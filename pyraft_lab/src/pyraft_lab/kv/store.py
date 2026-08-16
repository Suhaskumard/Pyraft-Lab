"""The replicated key-value state machine.

Two rules govern everything here, and they are what make replicas agree:

1. **Only committed entries are applied.** An entry that is merely replicated can still
   be overwritten; applying it early would let a client observe a value that later
   ceases to exist.
2. **Applied in index order, exactly once.** ``last_applied`` only ever moves forward by
   one, so a re-delivered or replayed commit cannot double-apply. This is what makes
   non-idempotent commands like CAS safe.

Given the same committed log prefix, every replica's ``KVStore`` therefore holds
byte-identical state - the property Phase 5's consistency checks lean on.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from pyraft_lab.kv.commands import (
    SESSION_CLIENT,
    SESSION_SERIAL,
    Cas,
    Command,
    Delete,
    Get,
    Put,
    command_from_wire,
)

if TYPE_CHECKING:  # pragma: no cover
    # Type-only: the state machine reads a log but must not depend on the consensus
    # layer at runtime. Importing it eagerly would also close a cycle, since raft/
    # imports this module to drive it.
    from pyraft_lab.raft.log import RaftLog


def _session_of(command: Any) -> tuple[str | None, int | None]:
    """The (client, serial) a logged command was stamped with, if any.

    Absent for commands submitted straight through ``RaftNode.submit`` - internal
    writes and every test that drives the store directly - which then apply unchanged.
    Deduplication is only meaningful for a request some client might retry.
    """
    if not isinstance(command, dict):
        return None, None
    client = command.get(SESSION_CLIENT)
    serial = command.get(SESSION_SERIAL)
    if not isinstance(client, str) or not isinstance(serial, int):
        return None, None
    return client, serial


@dataclass(frozen=True)
class Result:
    """The outcome of applying one command.

    ``ok`` answers "did the command do what it asked?" - a CAS that lost, or a delete of
    an absent key, is a successful *application* that reports ``ok=False``. Distinguishing
    those from an error matters to the client's success/failure metrics (Day 6).
    """

    ok: bool
    value: Any = None
    index: int = 0


@dataclass
class KVStore:
    """A dict plus the bookkeeping that makes it a deterministic state machine."""

    _data: dict[str, Any] = field(default_factory=dict, repr=False)
    last_applied: int = 0

    _sessions: dict[str, tuple[int, Result]] = field(default_factory=dict, repr=False)
    """Per client: the highest request serial applied, and what it answered.

    Paper §6.3. Rule 2 above makes a *log entry* apply exactly once, but a client whose
    reply is lost retries the same request, and the new leader appends it again - a
    second, distinct entry carrying the same intent. Applying both is not a replay, it
    is the write happening twice, and the damage is not limited to duplicated work: the
    retry lands after whatever was written in between, so an overwritten value comes
    back to life. That is a linearizability violation with no lost data and no diverged
    log, which is exactly how it hid (docs/architecture.md D10).
    """

    # --- reads that do not go through the log ------------------------------------

    def read(self, key: str) -> Any:
        """Read the local value directly.

        Not linearizable on its own: a stale leader can serve a value a newer leader has
        already replaced. ``RaftNode``'s Day 10 leader-lease reads close that gap by
        calling this only once the lease and the applied index both say it is safe; used
        directly like this, it is for tests and for inspecting a specific replica's state.
        """
        return self._data.get(key)

    def __contains__(self, key: str) -> bool:
        return key in self._data

    def __len__(self) -> int:
        return len(self._data)

    def snapshot(self) -> dict[str, Any]:
        """A copy of the whole state. Phase 6 persists this for log compaction."""
        return dict(self._data)

    def restore(self, data: dict[str, Any], last_applied: int) -> None:
        """Replace the state wholesale, as a snapshot install would."""
        self._data = dict(data)
        self.last_applied = last_applied

    # --- the apply path -----------------------------------------------------------

    def apply_committed(self, log: RaftLog, commit_index: int) -> list[Result]:
        """Apply every committed-but-unapplied entry, in order.

        Returns one :class:`Result` per entry applied, so a caller waiting on a specific
        index can pick out its answer. Applying nothing is normal and returns ``[]``.
        """
        if commit_index > log.last_index:
            raise ValueError(
                f"commit_index {commit_index} is past the end of the log ({log.last_index})"
            )

        results: list[Result] = []
        while self.last_applied < commit_index:
            index = self.last_applied + 1
            entry = log.entry_at(index)
            if entry is None:  # pragma: no cover - guarded by the check above
                break
            results.append(self._apply_one(entry.command, index))
            self.last_applied = index
        return results

    def _apply_one(self, command: Any, index: int) -> Result:
        if command is None:
            # A no-op entry. Some Raft implementations append one on election; ours does
            # not yet, but the log format permits it and applying it must be harmless.
            return Result(ok=True, index=index)

        client, serial = _session_of(command)
        if client is not None and serial is not None:
            seen = self._sessions.get(client)
            if seen is not None and serial <= seen[0]:
                # Already applied for this client. Answer with what it answered the
                # first time rather than running the command again: the client is owed
                # the result of its one request, and re-running would overwrite whatever
                # has legitimately been written since.
                return Result(ok=seen[1].ok, value=seen[1].value, index=index)
            result = self._dispatch(command_from_wire(command), index)
            self._sessions[client] = (serial, result)
            return result

        return self._dispatch(command_from_wire(command), index)

    def _dispatch(self, command: Command, index: int) -> Result:
        match command:
            case Put():
                self._data[command.key] = command.value
                return Result(ok=True, value=command.value, index=index)

            case Get():
                return Result(
                    ok=command.key in self._data,
                    value=self._data.get(command.key),
                    index=index,
                )

            case Delete():
                existed = command.key in self._data
                old = self._data.pop(command.key, None)
                return Result(ok=existed, value=old, index=index)

            case Cas():
                current = self._data.get(command.key)
                if current == command.expected:
                    self._data[command.key] = command.new
                    return Result(ok=True, value=command.new, index=index)
                # Losing a CAS is a legitimate outcome; the caller learns the value that
                # beat it, which is what makes a retry loop possible.
                return Result(ok=False, value=current, index=index)

            case _:  # pragma: no cover - unreachable while the registry is exhaustive
                raise TypeError(f"no apply rule for {type(command).__name__}")
