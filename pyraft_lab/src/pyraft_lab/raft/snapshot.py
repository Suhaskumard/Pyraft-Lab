"""Snapshots: the state machine's state, written down so the log needn't be kept forever.

2.0 item 10 promotes this from the original plan's "deferred" to a real feature, and the
flow it names is the one implemented here:

    log grows -> threshold reached -> create KV snapshot -> persist -> compact WAL

A snapshot is worth exactly one thing: it lets a node throw away log entries it will
never need to replay again. What it does *not* include is network transfer - 2.0 says
explicitly that InstallSnapshot can be skipped, and skipping it has a consequence this
module cannot hide, so it is stated here rather than discovered later:

**Compaction is on-disk only.** ``RaftNode.take_snapshot`` rewrites the WAL down to
snapshot + tail, but leaves the in-memory log whole. That is what keeps a leader able to
repair *any* follower, however far behind - the case a real implementation answers with
InstallSnapshot. The in-memory prefix is therefore dropped only where it genuinely no
longer exists: after a restart, where the WAL is all a node has (see ``RaftLog``'s base
index). A leader in that state can still catch up any follower holding the committed
prefix, and cannot catch up one that has lost it - see docs/architecture.md P6.

Everything is stored as JSON with a checksum over the payload, for the same reason the
WAL checksums its records: a snapshot that has rotted is worse than a missing one,
because it is the thing a restart trusts *instead of* the log.
"""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pyraft_lab import NodeId

SNAPSHOT_VERSION = 1
"""Format version, stored in every file. A future format change fails loudly."""

DEFAULT_ENTRY_THRESHOLD = 500
"""Applied entries since the last snapshot before another one is due."""

DEFAULT_KEEP = 2
"""Snapshot files kept on disk. See :meth:`SnapshotStore.prune` for why not 1."""


class SnapshotError(Exception):
    """A snapshot could not be written, found, or trusted."""


class SnapshotCorruption(SnapshotError):
    """A snapshot file exists but does not match its own checksum, or is unreadable."""


def snapshot_filename(index: int) -> str:
    """The file a snapshot at ``index`` lives in. Zero-padded so a name sort is an
    index sort, which is what lets the store find the newest without reading any file.
    """
    return f"snapshot-{index:012d}.json"


def data_checksum(data: dict[str, Any]) -> str:
    """A checksum over the snapshot payload, canonicalized first.

    Sorted keys and no whitespace, exactly as ``JsonCodec`` does it (D1): two equal
    states must produce one checksum regardless of dict ordering, or a comparison
    between two replicas' snapshots would depend on insertion history.
    """
    canonical = json.dumps(data, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class SnapshotMeta:
    """What a snapshot claims about itself.

    ``last_included_index``/``last_included_term`` are the pair that makes a snapshot
    usable as a log base: they are the (index, term) of the last entry it has already
    absorbed, which is precisely what an AppendEntries consistency check compares
    against when a follower's ``prevLogIndex`` lands there.
    """

    last_included_index: int
    last_included_term: int
    node_id: NodeId = ""
    created_at: float = 0.0
    keys: int = 0
    checksum: str = ""

    @property
    def filename(self) -> str:
        return snapshot_filename(self.last_included_index)

    def to_dict(self) -> dict[str, Any]:
        return {
            "last_included_index": self.last_included_index,
            "last_included_term": self.last_included_term,
            "node": self.node_id,
            "created_at": self.created_at,
            "keys": self.keys,
            "checksum": self.checksum,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SnapshotMeta:
        try:
            return cls(
                last_included_index=int(data["last_included_index"]),
                last_included_term=int(data["last_included_term"]),
                node_id=data.get("node") or "",
                created_at=float(data.get("created_at", 0.0)),
                keys=int(data.get("keys", 0)),
                checksum=str(data.get("checksum", "")),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise SnapshotCorruption(f"unreadable snapshot metadata: {exc}") from exc

    def __str__(self) -> str:
        who = self.node_id or "?"
        return (
            f"{who} @ index {self.last_included_index} term {self.last_included_term} "
            f"({self.keys} keys, t={self.created_at:.3f})"
        )


@dataclass(frozen=True)
class Snapshot:
    """A snapshot and the metadata that describes it."""

    meta: SnapshotMeta
    data: dict[str, Any]

    @classmethod
    def create(
        cls,
        *,
        last_included_index: int,
        last_included_term: int,
        data: dict[str, Any],
        node_id: NodeId = "",
        created_at: float = 0.0,
    ) -> Snapshot:
        """Build a snapshot from live state, computing its checksum and key count."""
        payload = dict(data)
        meta = SnapshotMeta(
            last_included_index=last_included_index,
            last_included_term=last_included_term,
            node_id=node_id,
            created_at=created_at,
            keys=len(payload),
            checksum=data_checksum(payload),
        )
        return cls(meta=meta, data=payload)

    def to_json(self) -> str:
        return json.dumps(
            {"version": SNAPSHOT_VERSION, "meta": self.meta.to_dict(), "data": self.data},
            sort_keys=True,
        )

    @classmethod
    def from_json(cls, text: str) -> Snapshot:
        """Parse and *verify* a snapshot file.

        The checksum is checked here rather than left to the caller, so there is no way
        to load a snapshot without having validated it - the mistake would be silent and
        the consequence would be a replica restarting into state that never existed.
        """
        try:
            payload = json.loads(text)
            version = int(payload["version"])
            meta = SnapshotMeta.from_dict(payload["meta"])
            data = payload["data"]
        except (json.JSONDecodeError, KeyError, TypeError, ValueError) as exc:
            raise SnapshotCorruption(f"unreadable snapshot: {exc}") from exc

        if version != SNAPSHOT_VERSION:
            raise SnapshotError(f"snapshot format version {version}, expected {SNAPSHOT_VERSION}")
        if not isinstance(data, dict):
            raise SnapshotCorruption("snapshot payload is not an object")

        actual = data_checksum(data)
        if meta.checksum and actual != meta.checksum:
            raise SnapshotCorruption(
                f"snapshot at index {meta.last_included_index} fails its checksum "
                f"(recorded {meta.checksum[:12]}, computed {actual[:12]})"
            )
        return cls(meta=meta, data=data)


class SnapshotStore:
    """A directory of snapshot files, newest last.

    One file per snapshot rather than one file overwritten in place: an overwrite has a
    window in which the only copy is half-written, and this is the file a restart trusts
    instead of the log.
    """

    def __init__(
        self, directory: str | Path, *, keep: int = DEFAULT_KEEP, sync: bool = True
    ) -> None:
        self.directory = Path(directory)
        self.keep = max(1, keep)
        self._sync = sync

    # --- writing ------------------------------------------------------------------

    def save(self, snapshot: Snapshot) -> Path:
        """Write ``snapshot`` durably, and return the path it landed at.

        Written to a temporary name and renamed into place, so a reader never sees a
        partial file: on every platform this project runs on, that rename is atomic.
        """
        self.directory.mkdir(parents=True, exist_ok=True)
        path = self.directory / snapshot.meta.filename
        temp = path.with_suffix(path.suffix + ".partial")

        with temp.open("w", encoding="utf-8", newline="\n") as handle:
            handle.write(snapshot.to_json())
            handle.flush()
            if self._sync:
                os.fsync(handle.fileno())
        os.replace(temp, path)
        return path

    def prune(self, keep: int | None = None) -> list[Path]:
        """Delete all but the newest ``keep`` snapshots. Returns what was deleted.

        ``keep`` defaults to 2 rather than 1 because a snapshot is only useful together
        with a WAL that starts where it ends. Between saving a new snapshot and the WAL
        compaction that references it there is a moment where the *older* snapshot plus
        the still-uncompacted WAL is the readable pair, so the previous file survives one
        round.
        """
        limit = self.keep if keep is None else max(1, keep)
        paths = self._paths()
        doomed = paths[: max(0, len(paths) - limit)]
        for path in doomed:
            path.unlink(missing_ok=True)
        return doomed

    # --- reading ------------------------------------------------------------------

    def load(self, index: int) -> Snapshot:
        """Load the snapshot taken at ``index``, verifying it."""
        path = self.directory / snapshot_filename(index)
        if not path.exists():
            raise SnapshotError(f"no snapshot at index {index} in {self.directory}")
        return Snapshot.from_json(path.read_text(encoding="utf-8"))

    def load_latest(self) -> Snapshot | None:
        """The newest snapshot that verifies, or ``None`` if there is none.

        Corrupt files are skipped rather than raised on, because there may be an older
        file that is fine - and one that verifies is always safe to use, since it only
        ever means replaying more of the log.
        """
        for path in reversed(self._paths()):
            try:
                return Snapshot.from_json(path.read_text(encoding="utf-8"))
            except (SnapshotError, OSError):
                continue
        return None

    def metas(self) -> list[SnapshotMeta]:
        """Metadata for every readable snapshot, oldest first."""
        found: list[SnapshotMeta] = []
        for path in self._paths():
            try:
                found.append(Snapshot.from_json(path.read_text(encoding="utf-8")).meta)
            except (SnapshotError, OSError):
                continue
        return found

    def _paths(self) -> list[Path]:
        if not self.directory.exists():
            return []
        return sorted(self.directory.glob("snapshot-*.json"))


@dataclass(frozen=True)
class SnapshotPolicy:
    """When a snapshot is due. The "threshold reached" step of 2.0's flow.

    Two triggers, either of which is enough. ``entry_threshold`` bounds how much of the
    log a restart has to replay; ``wal_bytes_threshold`` bounds the file itself, which is
    the limit that bites first when commands are large.
    """

    entry_threshold: int = DEFAULT_ENTRY_THRESHOLD
    wal_bytes_threshold: int | None = None

    def due(self, *, applied_index: int, last_snapshot_index: int, wal_bytes: int = 0) -> bool:
        """Is a snapshot due, given what has been applied since the last one?

        Deliberately measured against ``applied_index``, not the log's length: a
        snapshot can only cover entries the state machine has actually run, so entries
        replicated but not yet applied are not the log growth this is about.
        """
        pending = applied_index - last_snapshot_index
        if pending <= 0:
            return False
        if self.entry_threshold > 0 and pending >= self.entry_threshold:
            return True
        threshold = self.wal_bytes_threshold
        return threshold is not None and wal_bytes >= threshold
