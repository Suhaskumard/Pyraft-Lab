"""Phase 6, 2.0 item 10: snapshots as a real feature.

Three things are being tested, in order of how much they matter:

1. a snapshot cannot be read back as something other than what was written - the
   checksum is enforced on load, not offered as an option;
2. compaction leaves a WAL that recovers to the same state the long one did, only
   shorter - which is the entire justification for throwing entries away;
3. the log's base index behaves exactly like the sentinel it replaces, because every
   consistency check in Raft compares against it.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from typer.testing import CliRunner

from pyraft_lab.cli.main import app
from pyraft_lab.raft.log import LogEntry, RaftLog
from pyraft_lab.raft.persistence import WriteAheadLog, read_wal
from pyraft_lab.raft.replication import build_append_entries
from pyraft_lab.raft.snapshot import (
    Snapshot,
    SnapshotCorruption,
    SnapshotError,
    SnapshotMeta,
    SnapshotPolicy,
    SnapshotStore,
    data_checksum,
)
from pyraft_lab.raft.state import RaftState, Role

runner = CliRunner()


def entry(index: int, term: int = 1, command: object = None) -> LogEntry:
    return LogEntry(term=term, index=index, command=command)


def snapshot_of(data: dict[str, object], index: int = 10, term: int = 2) -> Snapshot:
    return Snapshot.create(
        last_included_index=index,
        last_included_term=term,
        data=data,
        node_id="n1",
        created_at=1.5,
    )


# --- the snapshot itself ----------------------------------------------------------


def test_creating_a_snapshot_describes_its_own_payload() -> None:
    snapshot = snapshot_of({"a": 1, "b": 2})

    assert snapshot.meta.keys == 2
    assert snapshot.meta.checksum == data_checksum({"b": 2, "a": 1})  # order cannot matter
    assert snapshot.meta.last_included_index == 10
    assert snapshot.meta.filename == "snapshot-000000000010.json"


def test_a_snapshot_round_trips_through_json() -> None:
    snapshot = snapshot_of({"k": [1, 2, {"nested": True}]})

    restored = Snapshot.from_json(snapshot.to_json())

    assert restored == snapshot


def test_a_tampered_payload_fails_its_checksum_on_load() -> None:
    snapshot = snapshot_of({"balance": 100})
    payload = json.loads(snapshot.to_json())
    payload["data"]["balance"] = 0

    with pytest.raises(SnapshotCorruption, match="fails its checksum"):
        Snapshot.from_json(json.dumps(payload))


def test_an_unreadable_or_future_snapshot_is_refused() -> None:
    with pytest.raises(SnapshotCorruption, match="unreadable"):
        Snapshot.from_json("{not json")

    payload = json.loads(snapshot_of({"a": 1}).to_json())
    payload["version"] = 99
    with pytest.raises(SnapshotError, match="version 99"):
        Snapshot.from_json(json.dumps(payload))


# --- the store --------------------------------------------------------------------


def test_the_store_saves_loads_and_finds_the_newest(tmp_path: Path) -> None:
    store = SnapshotStore(tmp_path, sync=False)
    store.save(snapshot_of({"a": 1}, index=5))
    store.save(snapshot_of({"a": 1, "b": 2}, index=12))

    assert store.load(5).meta.keys == 1
    latest = store.load_latest()
    assert latest is not None
    assert latest.meta.last_included_index == 12
    assert [m.last_included_index for m in store.metas()] == [5, 12]


def test_loading_a_snapshot_that_is_not_there_says_so(tmp_path: Path) -> None:
    store = SnapshotStore(tmp_path, sync=False)

    assert store.load_latest() is None
    with pytest.raises(SnapshotError, match="no snapshot at index 7"):
        store.load(7)


def test_the_newest_snapshot_being_corrupt_does_not_hide_an_older_good_one(
    tmp_path: Path,
) -> None:
    store = SnapshotStore(tmp_path, sync=False)
    store.save(snapshot_of({"a": 1}, index=5))
    newest = store.save(snapshot_of({"a": 1, "b": 2}, index=12))
    newest.write_text("half a file", encoding="utf-8")

    latest = store.load_latest()

    assert latest is not None
    assert latest.meta.last_included_index == 5
    assert [m.last_included_index for m in store.metas()] == [5]


def test_a_snapshot_full_of_non_utf8_bytes_is_skipped_not_a_crash(tmp_path: Path) -> None:
    """Same guarantee as the previous test, but for damage bad enough that the file
    can't even be decoded as text - ``load_latest``/``metas`` must still skip it and
    fall back to an older good snapshot, not let ``UnicodeDecodeError`` escape as an
    unhandled exception (it is a ``ValueError``, not an ``OSError``, so it slips past a
    handler that only names the latter).
    """
    store = SnapshotStore(tmp_path, sync=False)
    store.save(snapshot_of({"a": 1}, index=5))
    newest = store.save(snapshot_of({"a": 1, "b": 2}, index=12))
    newest.write_bytes(bytes([0xF6, 0x00, 0x9B, 0xFF]))

    latest = store.load_latest()

    assert latest is not None
    assert latest.meta.last_included_index == 5
    assert [m.last_included_index for m in store.metas()] == [5]


def test_pruning_keeps_the_newest_and_deletes_the_rest(tmp_path: Path) -> None:
    store = SnapshotStore(tmp_path, keep=2, sync=False)
    for index in (1, 2, 3, 4):
        store.save(snapshot_of({"a": index}, index=index))

    deleted = store.prune()

    assert [p.name for p in deleted] == [
        "snapshot-000000000001.json",
        "snapshot-000000000002.json",
    ]
    assert [m.last_included_index for m in store.metas()] == [3, 4]


# --- when a snapshot is due -------------------------------------------------------


def test_the_policy_fires_on_entries_applied_since_the_last_snapshot() -> None:
    policy = SnapshotPolicy(entry_threshold=100)

    assert not policy.due(applied_index=99, last_snapshot_index=0)
    assert policy.due(applied_index=100, last_snapshot_index=0)
    assert not policy.due(applied_index=150, last_snapshot_index=100)
    assert policy.due(applied_index=250, last_snapshot_index=100)


def test_the_policy_never_fires_with_nothing_new_to_capture() -> None:
    policy = SnapshotPolicy(entry_threshold=1)

    assert not policy.due(applied_index=40, last_snapshot_index=40)
    assert not policy.due(applied_index=40, last_snapshot_index=99)  # a stale caller


def test_the_policy_can_be_driven_by_wal_size_instead() -> None:
    policy = SnapshotPolicy(entry_threshold=0, wal_bytes_threshold=4096)

    assert not policy.due(applied_index=5, last_snapshot_index=0, wal_bytes=100)
    assert policy.due(applied_index=5, last_snapshot_index=0, wal_bytes=8192)
    # Still nothing applied, still nothing to snapshot, however big the file is.
    assert not policy.due(applied_index=0, last_snapshot_index=0, wal_bytes=1 << 20)


# --- compaction: the WAL after a snapshot -----------------------------------------


def test_compaction_rewrites_the_wal_as_snapshot_plus_tail(tmp_path: Path) -> None:
    path = tmp_path / "n1.wal"
    wal = WriteAheadLog(path, node_id="n1", sync=False)
    wal.recover()
    wal.record_term(3, "n1")
    for index in range(1, 8):
        wal.record_append(entry(index, term=3, command=index))
    wal.record_commit(7)
    long_size = wal.size

    snapshot = snapshot_of({str(i): i for i in range(1, 6)}, index=5, term=3)
    wal.compact(
        snapshot=snapshot.meta,
        term=3,
        voted_for="n1",
        entries=[entry(6, term=3, command=6), entry(7, term=3, command=7)],
        commit_index=7,
    )
    wal.close()

    assert wal.size < long_size
    assert [record["kind"] for record in wal.records()] == [
        "header",
        "snapshot",
        "term",
        "entry",
        "entry",
        "commit",
    ]

    recovery = read_wal(path)
    assert recovery.snapshot is not None
    assert recovery.base_index == 5
    assert recovery.base_term == 3
    assert [e.index for e in recovery.entries] == [6, 7]
    assert (recovery.term, recovery.voted_for, recovery.commit_index) == (3, "n1", 7)


def test_a_compacted_wal_keeps_accepting_records_after_the_rename(tmp_path: Path) -> None:
    path = tmp_path / "n1.wal"
    wal = WriteAheadLog(path, node_id="n1", sync=False)
    wal.recover()
    for index in range(1, 4):
        wal.record_append(entry(index, command=index))
    wal.compact(
        snapshot=snapshot_of({"a": 1}, index=3, term=1).meta,
        term=1,
        voted_for=None,
        entries=[],
        commit_index=3,
    )
    wal.record_append(entry(4, command="after-compaction"))
    wal.sync()
    wal.close()

    recovery = read_wal(path)
    assert [e.index for e in recovery.entries] == [4]
    assert recovery.base_index == 3
    assert not recovery.discarded


def test_a_snapshot_record_after_entries_is_not_something_compaction_writes(
    tmp_path: Path,
) -> None:
    """A snapshot record is always the first thing in a compacted file. One that turns up
    behind entries means the file was assembled by something else, and the base index it
    implies would silently contradict the entries already folded in.
    """
    from pyraft_lab.raft.persistence import WalCorruption, encode_record

    path = tmp_path / "n1.wal"
    wal = WriteAheadLog(path, node_id="n1", sync=False)
    wal.recover()
    wal.record_append(entry(1, command="a"))
    wal.close()
    with path.open("a", encoding="utf-8", newline="") as handle:
        handle.write(
            encode_record(
                {"kind": "snapshot", "meta": snapshot_of({"a": 1}, index=1).meta.to_dict()}
            )
        )

    with pytest.raises(WalCorruption, match="follows log entries"):
        read_wal(path)


# --- the log's base index ---------------------------------------------------------


def test_a_restored_log_treats_its_base_exactly_as_it_treats_the_sentinel() -> None:
    log = RaftLog()
    log.restore(
        [entry(11, term=4, command="k"), entry(12, term=4, command="l")], base_index=10, base_term=3
    )

    assert log.base_index == 10
    assert log.last_index == 12
    assert len(log) == 2
    # The base answers the consistency check for its own index, and nothing below it.
    assert log.has_entry(10, 3)
    assert not log.has_entry(10, 4)
    assert log.term_at(9) is None
    assert log.term_at(11) == 4


def test_a_restored_log_refuses_to_discard_what_a_snapshot_already_absorbed() -> None:
    log = RaftLog()
    log.restore([entry(11, term=4, command="k")], base_index=10, base_term=3)

    with pytest.raises(ValueError, match="sentinel"):
        log.truncate_from(10)
    with pytest.raises(ValueError, match="index must be >= 11"):
        log.entries_from(10)


def test_a_restored_log_ignores_re_sent_entries_from_below_its_base() -> None:
    """A leader that starts a batch below our base is re-sending applied entries. They
    cannot conflict - a snapshot only covers committed state - so this is the same
    idempotent no-op as a duplicate at the same term.
    """
    log = RaftLog()
    log.restore([entry(11, term=4, command="k")], base_index=10, base_term=3)

    log.append_new_entries(
        [
            entry(9, term=3, command="ancient"),
            entry(10, term=3, command="absorbed"),
            entry(11, term=4, command="k"),
            entry(12, term=4, command="new"),
        ]
    )

    assert log.base_index == 10
    assert [e.index for e in log.entries_from(11)] == [11, 12]


def test_a_log_can_only_be_restored_once() -> None:
    log = RaftLog()
    log.append(entry(1, command="a"))

    with pytest.raises(ValueError, match="already holds entries"):
        log.restore([entry(1, command="a")])


def test_restoring_entries_with_a_hole_is_rejected() -> None:
    log = RaftLog()

    with pytest.raises(ValueError, match="not contiguous"):
        log.restore([entry(1, command="a"), entry(3, command="c")])


def test_a_leader_with_a_compacted_log_never_sends_a_prev_index_below_its_base() -> None:
    """The clamp that keeps a leader recovered from a snapshot able to serve followers.
    Below the base it has nothing to send, so it offers the base itself - which every
    follower holding the committed prefix will match.
    """
    state = RaftState(node_id="n1", role=Role.LEADER, current_term=5)
    state.log.restore(
        [entry(11, term=5, command="k"), entry(12, term=5, command="l")],
        base_index=10,
        base_term=4,
    )
    state.next_index["n2"] = 3  # a follower the leader believes is far behind

    request = build_append_entries(state, "n2")

    assert request.prev_log_index == 10
    assert request.prev_log_term == 4
    assert [e.index for e in request.entries] == [11, 12]


# --- the CLI ---------------------------------------------------------------------


def test_cli_inspects_a_snapshot_file(tmp_path: Path) -> None:
    store = SnapshotStore(tmp_path, sync=False)
    path = store.save(snapshot_of({"a": 1, "b": 2}, index=42, term=7))

    result = runner.invoke(app, ["inspect-snapshot", str(path), "--keys", "2"])

    assert result.exit_code == 0
    assert "Last included index: 42" in result.stdout
    assert "Last included term: 7" in result.stdout
    assert "Keys: 2" in result.stdout
    assert "a = 1" in result.stdout


def test_cli_inspects_a_directory_of_snapshots(tmp_path: Path) -> None:
    store = SnapshotStore(tmp_path, sync=False)
    store.save(snapshot_of({"a": 1}, index=5))
    store.save(snapshot_of({"a": 1}, index=9))

    result = runner.invoke(app, ["inspect-snapshot", str(tmp_path)])

    assert result.exit_code == 0
    assert "snapshot-000000000005.json" in result.stdout
    assert "snapshot-000000000009.json" in result.stdout


def test_cli_refuses_a_corrupt_snapshot(tmp_path: Path) -> None:
    path = tmp_path / "snapshot-000000000001.json"
    payload = json.loads(snapshot_of({"a": 1}, index=1).to_json())
    payload["data"]["a"] = 999
    path.write_text(json.dumps(payload), encoding="utf-8")

    result = runner.invoke(app, ["inspect-snapshot", str(path)])

    assert result.exit_code == 1
    assert "unusable" in result.stdout


def test_cli_reports_non_utf8_snapshot_bytes_cleanly_instead_of_a_traceback(
    tmp_path: Path,
) -> None:
    path = tmp_path / "snapshot-000000000001.json"
    path.write_bytes(bytes([0xF6, 0x00, 0x9B, 0xFF]))

    result = runner.invoke(app, ["inspect-snapshot", str(path)])

    assert result.exit_code == 1
    assert "unusable" in result.stdout


def test_cli_says_so_when_there_is_nothing_to_inspect(tmp_path: Path) -> None:
    assert runner.invoke(app, ["inspect-snapshot", str(tmp_path / "nope.json")]).exit_code == 2
    assert runner.invoke(app, ["inspect-wal", str(tmp_path / "nope.wal")]).exit_code == 2


def test_snapshot_metadata_round_trips_through_its_dict_form() -> None:
    meta = snapshot_of({"a": 1}, index=3, term=2).meta

    assert SnapshotMeta.from_dict(meta.to_dict()) == meta
    with pytest.raises(SnapshotCorruption, match="unreadable snapshot metadata"):
        SnapshotMeta.from_dict({"last_included_term": 2})
