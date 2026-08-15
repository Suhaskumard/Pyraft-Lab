"""The replicated log: 1-indexed, with a term-0 sentinel at index 0.

Matching the Raft paper's indexing exactly (see docs/architecture.md, D1) means index 0
always exists and is always term 0, so ``prevLogIndex == 0`` (an empty log) satisfies the
AppendEntries consistency check without a special case: the sentinel's term is compared
like any other entry's.

Two things arrive with Phase 6, both of which a log recovered from disk needs:

- a **journal**. Every mutation a log undergoes has to reach the WAL, and this class is
  the single funnel all of them pass through - ``append`` and ``truncate_from``, which
  ``append_new_entries`` is itself built from. Recording here rather than at each caller
  is the same argument D2 makes for ``RaftState.observe_term``: a rule enforced in one
  place cannot be forgotten in another. Unlike Phase 4's tracer this is *not* optional
  instrumentation - it is the durability write, so it is synchronous and ordered with
  the mutation it describes.
- a **base index**. After a snapshot compacts the WAL (Phase 6), the entries at or below
  the snapshot's last included index are no longer on disk, so a log rebuilt from that
  WAL starts partway along. ``_entries[0]`` is then the snapshot's (index, term) standing
  in for the sentinel and playing exactly the same role in the consistency check. With no
  snapshot the base is 0 and this is the original 1-indexed log, unchanged.
"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

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


@runtime_checkable
class LogJournal(Protocol):
    """Where a log's mutations are written down so they survive a crash.

    Two calls, because a log only ever does two things: grow at the end, or lose a
    diverged tail. ``raft/persistence.py``'s ``WriteAheadLog`` is the implementation;
    the protocol exists so this module does not import it, and so a test can watch the
    calls with a list.
    """

    def record_append(self, entry: LogEntry) -> None: ...

    def record_truncate(self, index: int) -> None: ...


class NullJournal:
    """The journal a log without persistence gets: it drops everything.

    Same reasoning as Phase 4's ``NULL_TRACER`` - the feature is opt-in, and a node
    built without a WAL should pay nothing rather than a little.
    """

    def record_append(self, entry: LogEntry) -> None:
        return None

    def record_truncate(self, index: int) -> None:
        return None


NULL_JOURNAL = NullJournal()


class RaftLog:
    """The log a single node holds.

    ``_entries[0]`` is the base - the term-0 sentinel at index 0 on a fresh log, or a
    snapshot's last included ``(index, term)`` on one recovered from a compacted WAL.
    Every real entry follows it contiguously. Not itself a wire type - ``LogEntry`` is
    what travels inside ``AppendEntries.entries``.
    """

    def __init__(self, journal: LogJournal | None = None) -> None:
        self._entries: list[LogEntry] = [SENTINEL]
        self.journal: LogJournal = journal if journal is not None else NULL_JOURNAL

    def __len__(self) -> int:
        return len(self._entries) - 1  # the base doesn't count as a real entry

    @property
    def base_index(self) -> int:
        """The index of the entry before the first one this log still holds.

        0 on a fresh log - the sentinel - and the snapshot's last included index on a
        log recovered from a compacted WAL. Nothing at or below it can be read back,
        and nothing at or below it can be truncated: it is all committed and applied,
        which is the precondition a snapshot is taken under in the first place.
        """
        return self._entries[0].index

    @property
    def base_term(self) -> int:
        return self._entries[0].term

    @property
    def last_index(self) -> int:
        return self.base_index + len(self._entries) - 1

    @property
    def last_term(self) -> int:
        return self._entries[-1].term

    def _slot(self, index: int) -> int | None:
        """Where ``index`` sits in ``_entries``, or ``None`` if this log has no answer."""
        offset = index - self.base_index
        if 0 <= offset < len(self._entries):
            return offset
        return None

    def term_at(self, index: int) -> int | None:
        """The term of the entry at ``index``, or ``None`` if this log cannot say.

        ``None`` covers two different situations that a caller treats the same way: an
        index past the end, and - on a log recovered from a snapshot - an index below the
        base, whose entry existed but has been compacted away.
        """
        slot = self._slot(index)
        return None if slot is None else self._entries[slot].term

    def entry_at(self, index: int) -> LogEntry | None:
        slot = self._slot(index)
        return None if slot is None else self._entries[slot]

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
        self.journal.record_append(entry)

    def truncate_from(self, index: int) -> None:
        """Discard the entry at ``index`` and everything after it (conflict repair)."""
        if index <= self.base_index:
            raise ValueError(f"cannot truncate the base sentinel at index {self.base_index}")
        del self._entries[index - self.base_index :]
        self.journal.record_truncate(index)

    def entries_from(self, index: int) -> list[LogEntry]:
        """Every entry from ``index`` to the end, in log order."""
        if index <= self.base_index:
            raise ValueError(f"index must be >= {self.base_index + 1}")
        return list(self._entries[index - self.base_index :])

    def restore(self, entries: list[LogEntry], *, base_index: int = 0, base_term: int = 0) -> None:
        """Rebuild this log from what a WAL recovery read back (Phase 6).

        Deliberately *not* journalled: this is the read side of the round trip, and
        re-recording entries the WAL just handed us would grow the file on every
        restart. Only valid on an untouched log, so a restore can never interleave with
        live appends.
        """
        if len(self._entries) > 1:
            raise ValueError("cannot restore into a log that already holds entries")

        base = LogEntry(term=base_term, index=base_index, command=None)
        expected = base_index + 1
        for entry in entries:
            if entry.index != expected:
                raise ValueError(f"restored entries are not contiguous at index {entry.index}")
            expected += 1
        self._entries = [base, *entries]

    def append_new_entries(self, entries: list[LogEntry]) -> None:
        """Merge a leader's entries into this log per paper §5.3, steps 2-4.

        For each incoming entry: if the log has nothing at that index, append it. If
        it has something that disagrees on term, the log diverged here - truncate from
        this index onward and append the leader's version. If it already has exactly
        this entry, do nothing: replaying a batch the log already holds is a no-op,
        which matters because AppendEntries can be retried after a lost reply.
        """
        for entry in entries:
            if entry.index <= self.base_index:
                # Below the base this log has no opinion, and it needs none: everything
                # there is committed and applied, so the leader is re-sending what we
                # already ran. Skipping is the same idempotent no-op as the equal-term
                # case below - the alternative, truncating, would try to discard the
                # snapshot the base stands for.
                continue

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
