"""The replicated log: 1-indexed, with a term-0 sentinel at index 0.

Matching the Raft paper's indexing exactly (see docs/architecture.md, D1) means index 0
always exists and is always term 0, so ``prevLogIndex == 0`` (an empty log) satisfies the
AppendEntries consistency check without a special case: the sentinel's term is compared
like any other entry's.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class LogEntry(BaseModel):
    """One replicated log entry.

    ``command`` is opaque to Raft - the KV state machine (Phase 2) is the only thing
    that ever interprets it. Raft itself only ever compares ``(index, term)`` pairs.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    term: int = Field(ge=0)
    index: int = Field(ge=0)
    command: Any = None


SENTINEL = LogEntry(term=0, index=0, command=None)


class RaftLog:
    """The log a single node holds.

    ``_entries[i]`` is the entry at index ``i``; ``_entries[0]`` is always the term-0
    sentinel. Not itself a wire type - ``LogEntry`` is what travels inside
    ``AppendEntries.entries``.
    """

    def __init__(self) -> None:
        self._entries: list[LogEntry] = [SENTINEL]

    def __len__(self) -> int:
        return len(self._entries) - 1  # sentinel doesn't count as a real entry

    @property
    def last_index(self) -> int:
        return len(self._entries) - 1

    @property
    def last_term(self) -> int:
        return self._entries[-1].term

    def term_at(self, index: int) -> int | None:
        """The term of the entry at ``index``, or ``None`` if it doesn't exist."""
        if 0 <= index < len(self._entries):
            return self._entries[index].term
        return None

    def entry_at(self, index: int) -> LogEntry | None:
        if 0 <= index < len(self._entries):
            return self._entries[index]
        return None

    def has_entry(self, index: int, term: int) -> bool:
        """Does an entry at ``index`` exist and carry exactly ``term``?

        This is the AppendEntries consistency check (paper §5.3): the receiver
        accepts a new batch of entries only if its log already agrees with the
        leader's on the entry immediately preceding them.
        """
        return self.term_at(index) == term

    def append(self, entry: LogEntry) -> None:
        """Append a new entry produced locally (the leader path, Day 4).

        Enforces contiguity: a log can never have a hole.
        """
        expected = self.last_index + 1
        if entry.index != expected:
            raise ValueError(f"expected next index {expected}, got {entry.index}")
        self._entries.append(entry)

    def truncate_from(self, index: int) -> None:
        """Discard the entry at ``index`` and everything after it (conflict repair)."""
        if index <= 0:
            raise ValueError("cannot truncate the sentinel at index 0")
        del self._entries[index:]

    def entries_from(self, index: int) -> list[LogEntry]:
        """Every entry from ``index`` to the end, in log order."""
        if index <= 0:
            raise ValueError("index must be >= 1")
        return list(self._entries[index:])

    def append_new_entries(self, entries: list[LogEntry]) -> None:
        """Merge a leader's entries into this log per paper §5.3, steps 2-4.

        For each incoming entry: if the log has nothing at that index, append it. If
        it has something that disagrees on term, the log diverged here - truncate from
        this index onward and append the leader's version. If it already has exactly
        this entry, do nothing: replaying a batch the log already holds is a no-op,
        which matters because AppendEntries can be retried after a lost reply.
        """
        for entry in entries:
            existing_term = self.term_at(entry.index)
            if existing_term is None:
                self.append(entry)
            elif existing_term != entry.term:
                self.truncate_from(entry.index)
                self.append(entry)
            # else: identical entry already present - idempotent no-op.

    def candidate_log_is_up_to_date(
        self, candidate_last_term: int, candidate_last_index: int
    ) -> bool:
        """Raft §5.4.1: is a candidate whose log ends at the given (term, index) at
        least as up-to-date as this log?

        Called on the voter's own log. Logs are compared by their last entry: a later
        term wins outright; a tied term falls back to log length.
        """
        if candidate_last_term != self.last_term:
            return candidate_last_term > self.last_term
        return candidate_last_index >= self.last_index
