"""The write-ahead log: Raft's persistent state, and getting it back after a crash.

2.0 item 9's chain, in order:

    WAL -> checksum -> crash recovery -> log validation

Every record is one line: a CRC32 of the payload, a space, then the payload as canonical
JSON. One line per record makes a torn write - the thing a crash actually produces -
detectable and confined: the tail is either a whole valid record or it is not, and the
prefix before it is untouched. Text rather than a binary frame because this file is
evidence in a failure investigation, and `head` beating a hex editor is worth more here
than the bytes it costs.

Five record kinds, which between them are all of Figure 2's persistent state plus the
two things a restart would otherwise have to re-derive:

- ``header``   format version and whose WAL this is
- ``term``     ``currentTerm`` and ``votedFor``, written together because they change
               together (D2: ``observe_term`` clears the vote as it raises the term)
- ``entry``    one log entry, journalled by ``RaftLog.append``
- ``truncate`` a diverged tail being discarded, journalled by ``RaftLog.truncate_from``
- ``commit``   ``commitIndex``. Volatile in Figure 2 and safe to persist: it only ever
               moves forward, so recovering it can never claim more than was committed,
               and it saves a restarted node replaying its whole log before it can answer
               anything. Clamped to the recovered log's length on the way back in.
- ``snapshot`` a reference to a snapshot file, written by compaction (see ``snapshot.py``)

Recovery is a fold over those records, and it is where 2.0's "log validation" lives:
a checksum proves a record was not damaged, not that the *sequence* is a log. A hole in
the indexes, a term record going backward, a commit index past the end of the log - each
is a corrupt WAL even though every individual line verifies, and each is rejected loudly
rather than folded into a state that looks plausible.

Durability is explicit: writes are flushed as they happen, but ``fsync`` only happens in
:meth:`WriteAheadLog.sync`, and ``RaftNode`` calls that before anything leaves the node.
"Nothing this node has told anyone can be forgotten" is the rule that actually matters,
and one fsync per outgoing message is far cheaper than one per record.
"""

from __future__ import annotations

import json
import os
import zlib
from dataclasses import dataclass, field
from pathlib import Path
from typing import IO, Any

from pyraft_lab import NodeId
from pyraft_lab.raft.log import LogEntry
from pyraft_lab.raft.snapshot import SnapshotMeta

WAL_VERSION = 1

KIND_HEADER = "header"
KIND_TERM = "term"
KIND_ENTRY = "entry"
KIND_TRUNCATE = "truncate"
KIND_COMMIT = "commit"
KIND_SNAPSHOT = "snapshot"

RECORD_KINDS = frozenset(
    {KIND_HEADER, KIND_TERM, KIND_ENTRY, KIND_TRUNCATE, KIND_COMMIT, KIND_SNAPSHOT}
)


class WalError(Exception):
    """The WAL could not be used as asked."""


class WalCorruption(WalError):
    """A WAL record is damaged, or the sequence of records is not a valid log."""


# --- the record format ------------------------------------------------------------


def checksum(body: str) -> int:
    """CRC32 of a record's payload.

    CRC32 rather than a cryptographic hash: this defends against a torn write and a
    rotted byte, which are accidents, not against an adversary editing the file - and
    it is fast enough to be affordable on every single record.
    """
    return zlib.crc32(body.encode("utf-8")) & 0xFFFFFFFF


def encode_record(payload: dict[str, Any]) -> str:
    """One record as a line: ``<crc32 hex> <canonical json>``."""
    body = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return f"{checksum(body):08x} {body}\n"


def decode_record(line: str) -> dict[str, Any]:
    """Parse and verify one line. Raises :class:`WalCorruption` if it does not hold up."""
    # Both endings accepted: the writer only ever emits "\n" (see ``_read_lines``), but a
    # WAL that has been through a tool that rewrote its line endings is damaged in a way
    # nothing was lost to, and refusing to read it would be pedantry rather than safety.
    text = line.rstrip("\r\n")
    head, _, body = text.partition(" ")
    if not body:
        raise WalCorruption("record has no payload")

    try:
        recorded = int(head, 16)
    except ValueError as exc:
        raise WalCorruption(f"record checksum {head!r} is not hexadecimal") from exc

    actual = checksum(body)
    if recorded != actual:
        raise WalCorruption(f"checksum mismatch: recorded {recorded:08x}, computed {actual:08x}")

    try:
        payload = json.loads(body)
    except json.JSONDecodeError as exc:
        raise WalCorruption(f"record payload is not JSON: {exc}") from exc

    if not isinstance(payload, dict) or payload.get("kind") not in RECORD_KINDS:
        kind = payload.get("kind") if isinstance(payload, dict) else None
        raise WalCorruption(f"record kind {kind!r} is not one of {sorted(RECORD_KINDS)}")
    return payload


# --- recovery ---------------------------------------------------------------------


@dataclass
class WalRecovery:
    """What a restart learned from the WAL, and what it had to throw away.

    The report 2.0 item 9 asks a restart to print, in a form a test can also assert on.
    ``entries``/``term``/``voted_for``/``commit_index`` are the recovered state;
    ``truncated_tail``/``dropped_records`` are the honest record of what did not survive.
    """

    path: Path
    node_id: NodeId | None = None
    records_read: int = 0
    records_valid: int = 0
    valid_bytes: int = 0
    truncated_tail: bool = False
    dropped_records: int = 0
    term: int = 0
    voted_for: NodeId | None = None
    entries: list[LogEntry] = field(default_factory=list)
    commit_index: int = 0
    snapshot: SnapshotMeta | None = None

    @property
    def base_index(self) -> int:
        """Where the recovered log starts: a snapshot's last included index, or 0."""
        return self.snapshot.last_included_index if self.snapshot else 0

    @property
    def base_term(self) -> int:
        return self.snapshot.last_included_term if self.snapshot else 0

    @property
    def last_index(self) -> int:
        return self.entries[-1].index if self.entries else self.base_index

    @property
    def empty(self) -> bool:
        """Was there nothing to recover? A first boot, or a WAL with only a header."""
        return self.records_valid == 0 or (
            self.term == 0 and self.voted_for is None and not self.entries and self.snapshot is None
        )

    @property
    def discarded(self) -> bool:
        """Did recovery reject part of the file it had read?"""
        return self.truncated_tail or self.dropped_records > 0

    def report_lines(self) -> list[str]:
        """2.0 item 9's restart report."""
        lines = [
            f"Loading WAL... {self.path}",
            f"Validating records... {self.records_valid} of {self.records_read} valid",
        ]
        if self.truncated_tail:
            lines.append("  torn final record discarded (a crash mid-append)")
        if self.dropped_records:
            lines.append(f"  {self.dropped_records} record(s) discarded after corruption")
        if self.snapshot is not None:
            lines.append(f"Loading snapshot... {self.snapshot}")
        lines += [
            "Recovering state...",
            "Replaying committed entries...",
            "Recovery complete.",
            "",
            "Recovered:",
            f"  Node: {self.node_id or '?'}",
            f"  Term: {self.term:,}",
            f"  Voted for: {self.voted_for or '-'}",
            f"  Entries: {self.last_index:,}",
            f"  Commit Index: {self.commit_index:,}",
        ]
        return lines

    def __str__(self) -> str:
        return "\n".join(self.report_lines())


def _read_lines(path: Path) -> list[str]:
    """Read a WAL's lines with the bytes exactly as they are on disk.

    ``newline=""`` disables the line-ending translation Python's text mode would
    otherwise do, for a reason that is not cosmetic: recovery truncates the file at the
    end of the last good record, and it computes that offset by measuring the lines it
    read. Under translation, a Windows ``\\r\\n`` arrives as one character and the
    measurement comes out short - which truncates *into* a good record and turns a torn
    tail into a corrupt file. The write side pins ``newline="\\n"`` for the same reason,
    which also makes a WAL byte-identical across platforms.
    """
    with path.open("r", encoding="utf-8", newline="") as handle:
        return handle.read().splitlines(keepends=True)


def read_wal(path: str | Path, *, repair: bool = False) -> WalRecovery:
    """Read, verify and fold a WAL file without opening it for writing.

    Two failure modes, treated differently on purpose:

    - a damaged **final** record is a torn write. The crash happened during that append,
      so nothing was ever acknowledged on the strength of it: the record is dropped and
      recovery succeeds. This is the ordinary case and needs no operator.
    - a damaged record with valid records *after* it is not a torn write - it is
      corruption in the middle of the file, and the records past it cannot simply be
      applied, because a fold that skips a record is not a replay of what happened
      (skip a ``truncate`` and entries reappear that a leader had already overwritten).
      Recovery refuses, unless ``repair=True`` explicitly accepts losing that record and
      everything after it.
    """
    path = Path(path)
    recovery = WalRecovery(path=path)
    if not path.exists():
        return recovery

    lines = _read_lines(path)
    payloads: list[dict[str, Any]] = []

    for position, line in enumerate(lines, start=1):
        recovery.records_read += 1
        try:
            payload = decode_record(line)
        except WalCorruption as exc:
            remaining = len(lines) - position
            if remaining == 0:
                recovery.truncated_tail = True
                break
            if not repair:
                raise WalCorruption(
                    f"{path}: record {position} is damaged ({exc}) and {remaining} further "
                    f"record(s) follow it, so this is corruption rather than a torn write. "
                    f"Recover with repair enabled to discard record {position} onward."
                ) from exc
            recovery.dropped_records = remaining + 1
            break

        payloads.append(payload)
        recovery.records_valid += 1
        recovery.valid_bytes += len(line.encode("utf-8"))

    _fold(recovery, payloads)
    return recovery


def _fold(recovery: WalRecovery, payloads: list[dict[str, Any]]) -> None:
    """Replay validated records into recovered state, checking the sequence as it goes.

    This is the "log validation" step: every check here is one a checksum cannot make,
    because each is about the relationship between records rather than a record's own
    integrity.
    """
    for position, payload in enumerate(payloads, start=1):
        kind = payload["kind"]
        try:
            if kind == KIND_HEADER:
                version = int(payload["version"])
                if version != WAL_VERSION:
                    raise WalError(f"WAL format version {version}, expected {WAL_VERSION}")
                recovery.node_id = payload.get("node") or None

            elif kind == KIND_TERM:
                term = int(payload["term"])
                if term < recovery.term:
                    raise WalCorruption(
                        f"term went backward at record {position}: {recovery.term} -> {term}"
                    )
                recovery.term = term
                recovery.voted_for = payload.get("voted_for")

            elif kind == KIND_ENTRY:
                entry = LogEntry(
                    term=int(payload["term"]),
                    index=int(payload["index"]),
                    command=payload.get("command"),
                )
                expected = recovery.last_index + 1
                if entry.index != expected:
                    raise WalCorruption(
                        f"log has a hole at record {position}: expected index {expected}, "
                        f"found {entry.index}"
                    )
                # An entry is itself proof this node had reached that term. A follower
                # writes the entries of a new term through its log's journal *before*
                # the driver gets to record the term change, so the term record is not
                # always the first to arrive - taking the max is what makes the order of
                # those two writes something recovery does not have to depend on.
                recovery.term = max(recovery.term, entry.term)
                recovery.entries.append(entry)

            elif kind == KIND_TRUNCATE:
                index = int(payload["index"])
                if index <= recovery.base_index or index > recovery.last_index:
                    raise WalCorruption(
                        f"truncate at record {position} names index {index}, which is not "
                        f"inside the recovered log ({recovery.base_index}..{recovery.last_index})"
                    )
                del recovery.entries[index - recovery.base_index - 1 :]

            elif kind == KIND_COMMIT:
                index = int(payload["index"])
                if index < recovery.commit_index:
                    raise WalCorruption(
                        f"commit index went backward at record {position}: "
                        f"{recovery.commit_index} -> {index}"
                    )
                if index > recovery.last_index:
                    raise WalCorruption(
                        f"commit index {index} at record {position} is past the end of the "
                        f"recovered log ({recovery.last_index})"
                    )
                recovery.commit_index = index

            elif kind == KIND_SNAPSHOT:
                meta = SnapshotMeta.from_dict(payload["meta"])
                if recovery.entries:
                    raise WalCorruption(
                        f"snapshot record at {position} follows log entries; compaction "
                        f"writes it first, so this file was not written by compaction"
                    )
                recovery.snapshot = meta
                # Everything the snapshot absorbed is committed by definition.
                recovery.commit_index = max(recovery.commit_index, meta.last_included_index)
                recovery.term = max(recovery.term, meta.last_included_term)

        except (KeyError, TypeError, ValueError) as exc:
            raise WalCorruption(f"record {position} ({kind}) is malformed: {exc}") from exc


# --- the log itself ---------------------------------------------------------------


class WriteAheadLog:
    """One node's WAL: an append-only file, and the recovery that reads it back.

    Implements ``RaftLog``'s ``LogJournal`` protocol, so attaching it to a log is all it
    takes for every append and truncation to be recorded. Term, vote and commit records
    come from ``RaftNode``, which is the only thing that knows when they change.
    """

    def __init__(self, path: str | Path, *, node_id: NodeId = "", sync: bool = True) -> None:
        self.path = Path(path)
        self.node_id = node_id
        self._sync = sync
        self._file: IO[str] | None = None
        self._recovery: WalRecovery | None = None
        self._dirty = False

        self.records_written = 0
        self.syncs = 0
        self.compactions = 0

    # --- recovery -----------------------------------------------------------------

    @property
    def recovered(self) -> bool:
        return self._recovery is not None

    @property
    def recovery(self) -> WalRecovery:
        """What :meth:`recover` found. Raises if recovery has not run yet."""
        if self._recovery is None:
            raise WalError(f"{self.path} has not been recovered yet")
        return self._recovery

    def recover(self, *, repair: bool = False) -> WalRecovery:
        """Validate and fold this WAL, then open it for appending.

        Idempotent: a second call returns the first call's report rather than re-reading
        a file that is now open for writing. Anything recovery rejected is truncated off
        the file here, so the first new record lands directly after the last good one -
        a torn tail is never left in place to be re-read on the next restart.
        """
        if self._recovery is not None:
            return self._recovery

        recovery = read_wal(self.path, repair=repair)
        if recovery.discarded:
            os.truncate(self.path, recovery.valid_bytes)
        if recovery.node_id and self.node_id and recovery.node_id != self.node_id:
            raise WalError(
                f"{self.path} is {recovery.node_id}'s WAL, but was opened as {self.node_id}'s"
            )

        self._recovery = recovery
        self._open_for_append()
        return recovery

    # --- writing ------------------------------------------------------------------

    def record_term(self, term: int, voted_for: NodeId | None) -> None:
        """Persist ``currentTerm``/``votedFor`` - Figure 2's other two persistent fields."""
        self._write({"kind": KIND_TERM, "term": term, "voted_for": voted_for})

    def record_append(self, entry: LogEntry) -> None:
        """``LogJournal``: one entry was appended."""
        self._write(
            {
                "kind": KIND_ENTRY,
                "term": entry.term,
                "index": entry.index,
                "command": entry.command,
            }
        )

    def record_truncate(self, index: int) -> None:
        """``LogJournal``: the entry at ``index`` and everything after it was discarded."""
        self._write({"kind": KIND_TRUNCATE, "index": index})

    def record_commit(self, index: int) -> None:
        self._write({"kind": KIND_COMMIT, "index": index})

    def sync(self) -> None:
        """Force everything written so far to disk.

        The only ``fsync`` in the system. ``RaftNode`` calls it before any message
        leaves the node, which is what makes "nothing this node has told anyone can be
        forgotten" true. A no-op when nothing has been written since the last call, so a
        heartbeat costs nothing.
        """
        if not self._dirty or self._file is None:
            return
        self._file.flush()
        if self._sync:
            os.fsync(self._file.fileno())
        self._dirty = False
        self.syncs += 1

    def compact(
        self,
        *,
        snapshot: SnapshotMeta | None,
        term: int,
        voted_for: NodeId | None,
        entries: list[LogEntry],
        commit_index: int,
    ) -> None:
        """Rewrite this WAL as snapshot + current state + the entries after it.

        The last step of 2.0 item 10's flow, and the step that makes a snapshot worth
        taking at all: without it the file keeps every entry forever and a restart keeps
        replaying them.

        Written to a temporary file and renamed over the original, so the WAL on disk is
        always one whole valid file - either the old one or the new one. A crash between
        the snapshot being saved and this rename leaves the *uncompacted* WAL, which
        still holds every entry, so nothing is lost either way.
        """
        temp = self.path.with_suffix(self.path.suffix + ".compact")
        records: list[dict[str, Any]] = [
            {"kind": KIND_HEADER, "version": WAL_VERSION, "node": self.node_id}
        ]
        if snapshot is not None:
            records.append({"kind": KIND_SNAPSHOT, "meta": snapshot.to_dict()})
        records.append({"kind": KIND_TERM, "term": term, "voted_for": voted_for})
        records += [
            {"kind": KIND_ENTRY, "term": e.term, "index": e.index, "command": e.command}
            for e in entries
        ]
        records.append({"kind": KIND_COMMIT, "index": commit_index})

        with temp.open("w", encoding="utf-8", newline="\n") as handle:
            handle.write("".join(encode_record(record) for record in records))
            handle.flush()
            if self._sync:
                os.fsync(handle.fileno())

        # Closed before the rename: on Windows a file cannot be replaced while it is
        # open, and reopening afterwards is what a compaction has to do in any case.
        self._close_file()
        os.replace(temp, self.path)
        self._open_for_append()

        self.records_written += len(records)
        self.compactions += 1
        self._dirty = False

    # --- inspection ---------------------------------------------------------------

    @property
    def size(self) -> int:
        """The file's size in bytes - what ``SnapshotPolicy`` weighs against its
        byte threshold.
        """
        if self._file is not None:
            self._file.flush()
        return self.path.stat().st_size if self.path.exists() else 0

    def records(self) -> list[dict[str, Any]]:
        """Every record currently in the file, verified. For the CLI and for tests."""
        if self._file is not None:
            self._file.flush()
        if not self.path.exists():
            return []
        return [decode_record(line) for line in _read_lines(self.path) if line.strip()]

    def close(self) -> None:
        """Flush and let go of the file. Reopened lazily by the next write.

        Deliberately does not ``fsync``: a node stopping is a crash as far as the WAL is
        concerned (see ``RaftNode.stop``), and everything that was ever acknowledged has
        already been synced by the rule above. Closing exists so a stopped node holds no
        handle - not to add a durability point that a real crash would not have.
        """
        self._close_file()

    # --- internals ----------------------------------------------------------------

    def _write(self, payload: dict[str, Any]) -> None:
        handle = self._writer()
        handle.write(encode_record(payload))
        # Flushed but not synced: the bytes are out of this process, so ``records()``
        # and ``size`` see them, while the durability point stays where ``sync`` puts it.
        handle.flush()
        self._dirty = True
        self.records_written += 1

    def _writer(self) -> IO[str]:
        if self._file is None:
            if self._recovery is None:
                # Appending to a file nobody has validated would write good records
                # after a corrupt one - exactly the shape recovery cannot repair.
                self.recover()
            else:
                self._open_for_append()
        assert self._file is not None
        return self._file

    def _open_for_append(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        existed = self.path.exists() and self.path.stat().st_size > 0
        self._file = self.path.open("a", encoding="utf-8", newline="\n")
        if not existed:
            self._write({"kind": KIND_HEADER, "version": WAL_VERSION, "node": self.node_id})

    def _close_file(self) -> None:
        if self._file is not None:
            self._file.flush()
            self._file.close()
            self._file = None
