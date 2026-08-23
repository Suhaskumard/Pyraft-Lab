"""Phase 6, 2.0 item 9: the WAL, its checksums, and what recovery does with a file that
has been damaged.

The interesting tests here are the corruption ones. A WAL that recovers a clean file is
table stakes; what decides whether persistence can be trusted is what happens to a file
a crash caught mid-append, and whether a *middle* record going bad is quietly folded away
or refused. Those two cases are deliberately treated differently, and both are asserted.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from typer.testing import CliRunner

from pyraft_lab.cli.main import app
from pyraft_lab.kv.commands import Put
from pyraft_lab.raft.log import LogEntry, RaftLog
from pyraft_lab.raft.persistence import (
    KIND_ENTRY,
    KIND_HEADER,
    KIND_TERM,
    WAL_VERSION,
    WalCorruption,
    WalError,
    WriteAheadLog,
    checksum,
    decode_record,
    encode_record,
    read_wal,
)

runner = CliRunner()


@pytest.fixture
def wal_path(tmp_path: Path) -> Path:
    return tmp_path / "n1.wal"


def open_wal(path: Path, node_id: str = "n1") -> WriteAheadLog:
    """A recovered, ready-to-write WAL. ``sync=False`` keeps the suite off the disk's
    fsync path while still counting the syncs a node performs.
    """
    wal = WriteAheadLog(path, node_id=node_id, sync=False)
    wal.recover()
    return wal


def entry(index: int, term: int = 1, command: object = None) -> LogEntry:
    return LogEntry(term=term, index=index, command=command)


def append_raw(path: Path, line: str) -> None:
    """Append a line exactly as given - the only way to write a record that is wrong.

    ``newline=""`` for the same reason the WAL itself uses it: without it a ``\\n`` would
    become ``\\r\\n`` on Windows and these tests would be measuring line-ending
    translation instead of the corruption they mean to inject.
    """
    with path.open("a", encoding="utf-8", newline="") as handle:
        handle.write(line)


# --- the record format ------------------------------------------------------------


def test_a_record_round_trips_through_its_own_encoding() -> None:
    payload = {
        "kind": KIND_ENTRY,
        "term": 3,
        "index": 7,
        "command": Put(key="a", value=1).to_wire(),
    }
    line = encode_record(payload)

    assert line.endswith("\n")
    assert decode_record(line) == payload


def test_a_flipped_byte_is_caught_by_the_checksum() -> None:
    line = encode_record({"kind": KIND_TERM, "term": 4, "voted_for": "n2"})
    damaged = line.replace('"term":4', '"term":5')

    with pytest.raises(WalCorruption, match="checksum mismatch"):
        decode_record(damaged)


def test_a_line_that_checksums_correctly_can_still_be_rejected() -> None:
    """The checksum only proves the bytes are the bytes that were written. Everything
    past that - is it JSON, is it a record kind this build knows - is a separate check.
    """
    body = "not json at all"
    with pytest.raises(WalCorruption, match="not JSON"):
        decode_record(f"{checksum(body):08x} {body}\n")

    with pytest.raises(WalCorruption, match="record kind"):
        decode_record(encode_record({"kind": "invented", "index": 1}))


def test_a_line_with_no_payload_at_all_is_rejected() -> None:
    with pytest.raises(WalCorruption, match="no payload"):
        decode_record("garbage\n")

    with pytest.raises(WalCorruption, match="not hexadecimal"):
        decode_record('zzzz {"kind":"commit","index":1}\n')


# --- an ordinary round trip -------------------------------------------------------


def test_a_fresh_wal_starts_with_a_header_and_recovers_as_empty(wal_path: Path) -> None:
    wal = open_wal(wal_path)

    assert wal.recovery.empty
    assert wal_path.exists()
    assert wal.records() == [{"kind": KIND_HEADER, "version": WAL_VERSION, "node": "n1"}]


def test_recovering_a_file_that_does_not_exist_yet_is_not_an_error(tmp_path: Path) -> None:
    recovery = read_wal(tmp_path / "nothing-here.wal")

    assert recovery.empty
    assert recovery.records_read == 0
    assert recovery.entries == []


def test_term_vote_entries_commit_all_come_back(wal_path: Path) -> None:
    wal = open_wal(wal_path)
    wal.record_term(4, "n2")
    for index in range(1, 4):
        wal.record_append(entry(index, term=4, command=Put(key=f"k{index}", value=index).to_wire()))
    wal.record_commit(2)
    wal.sync()
    wal.close()

    recovery = read_wal(wal_path)

    assert (recovery.term, recovery.voted_for) == (4, "n2")
    assert [e.index for e in recovery.entries] == [1, 2, 3]
    assert recovery.entries[0].command == Put(key="k1", value=1).to_wire()
    assert recovery.commit_index == 2
    assert recovery.node_id == "n1"
    assert not recovery.discarded


def test_the_last_term_record_wins(wal_path: Path) -> None:
    wal = open_wal(wal_path)
    wal.record_term(1, "n1")
    wal.record_term(2, None)
    wal.record_term(2, "n3")

    assert (read_wal(wal_path).term, read_wal(wal_path).voted_for) == (2, "n3")


def test_a_truncate_record_removes_the_entries_it_named(wal_path: Path) -> None:
    wal = open_wal(wal_path)
    for index in range(1, 5):
        wal.record_append(entry(index, term=1, command=index))
    wal.record_truncate(3)
    wal.record_append(entry(3, term=2, command="replacement"))

    recovery = read_wal(wal_path)

    assert [(e.index, e.term) for e in recovery.entries] == [(1, 1), (2, 1), (3, 2)]
    assert recovery.entries[-1].command == "replacement"


def test_an_entrys_term_is_enough_to_recover_the_term_it_was_written_in(wal_path: Path) -> None:
    """A follower journals a new term's entries before its driver records the term
    change, so recovery must not depend on the term record arriving first.
    """
    wal = open_wal(wal_path)
    wal.record_append(entry(1, term=9, command="from-term-9"))

    assert read_wal(wal_path).term == 9


# --- the journal: a log records itself --------------------------------------------


def test_a_log_journals_every_append_and_truncation(wal_path: Path) -> None:
    wal = open_wal(wal_path)
    log = RaftLog(journal=wal)

    log.append(entry(1, command="a"))
    log.append(entry(2, command="b"))
    log.append_new_entries([entry(2, term=2, command="repaired")])  # a conflict at index 2

    kinds = [record["kind"] for record in wal.records()]
    assert kinds == [KIND_HEADER, KIND_ENTRY, KIND_ENTRY, "truncate", KIND_ENTRY]

    replayed = RaftLog()
    replayed.restore(read_wal(wal_path).entries)
    assert replayed.last_index == log.last_index
    assert replayed.entry_at(2) == log.entry_at(2)


def test_restoring_a_log_does_not_journal_what_it_read(wal_path: Path) -> None:
    wal = open_wal(wal_path)
    log = RaftLog(journal=wal)
    for index in range(1, 4):
        log.append(entry(index, command=index))
    before = len(wal.records())

    revived_wal = open_wal(wal_path)
    revived = RaftLog()
    revived.restore(revived_wal.recovery.entries)
    revived.journal = revived_wal

    assert len(revived_wal.records()) == before  # a restart does not grow the file


# --- durability: fsync is where the node puts it, and nowhere else ----------------


def test_sync_is_a_no_op_until_something_has_been_written(wal_path: Path) -> None:
    wal = open_wal(wal_path)
    wal.sync()
    first = wal.syncs

    wal.sync()
    assert wal.syncs == first  # nothing written in between: nothing to sync

    wal.record_term(2, None)
    wal.sync()
    assert wal.syncs == first + 1


def test_the_real_fsync_path_works_and_round_trips(wal_path: Path) -> None:
    """Every other test here runs with ``sync=False`` to stay off the disk; this one
    exercises the default a node actually uses, so the fsync path is not untested.
    """
    wal = WriteAheadLog(wal_path, node_id="n1")  # sync=True, the default
    wal.recover()
    wal.record_term(2, "n1")
    wal.record_append(entry(1, term=2, command="synced"))
    wal.record_commit(1)
    wal.sync()
    wal.close()

    recovery = read_wal(wal_path)
    assert wal.syncs == 1
    assert (recovery.term, recovery.commit_index) == (2, 1)
    assert [e.command for e in recovery.entries] == ["synced"]


# --- crash recovery: a torn tail ---------------------------------------------------


def test_a_torn_final_record_is_discarded_and_the_prefix_survives(wal_path: Path) -> None:
    """The ordinary crash: the process died partway through writing one record."""
    wal = open_wal(wal_path)
    wal.record_term(2, "n1")
    wal.record_append(entry(1, term=2, command="safe"))
    wal.record_commit(1)
    wal.close()

    line = encode_record({"kind": KIND_ENTRY, "term": 2, "index": 2, "command": "half-written"})
    append_raw(wal_path, line[: len(line) // 2])  # no newline, half a payload

    recovery = read_wal(wal_path)

    assert recovery.truncated_tail
    assert recovery.records_valid == recovery.records_read - 1
    assert [e.index for e in recovery.entries] == [1]
    assert recovery.commit_index == 1


def test_recovery_truncates_the_torn_tail_off_the_file(wal_path: Path) -> None:
    """Otherwise the next restart re-reads the damage, and the append after it would be
    a good record sitting behind a bad one - the shape recovery cannot repair.
    """
    wal = open_wal(wal_path)
    wal.record_append(entry(1, command="safe"))
    wal.close()
    append_raw(wal_path, '{"kind": "entry"')  # a torn write, again

    revived = WriteAheadLog(wal_path, node_id="n1", sync=False)
    assert revived.recover().truncated_tail
    revived.record_append(entry(2, command="after-recovery"))
    revived.close()

    second = read_wal(wal_path)
    assert not second.discarded
    assert [e.index for e in second.entries] == [1, 2]


# --- crash recovery: corruption in the middle -------------------------------------


def test_corruption_with_valid_records_after_it_is_refused(wal_path: Path) -> None:
    """This is not a torn write, so it must not be treated as one: skipping a record
    and folding the rest produces a state the node never actually had.
    """
    wal = open_wal(wal_path)
    for index in range(1, 4):
        wal.record_append(entry(index, command=index))
    wal.close()

    lines = wal_path.read_text(encoding="utf-8").splitlines(keepends=True)
    # Record 2 (the entry at index 1), rotted in place: same length, wrong checksum.
    lines[1] = lines[1].replace('"index":1', '"index":9')
    wal_path.write_text("".join(lines), encoding="utf-8")

    with pytest.raises(WalCorruption, match="corruption rather than a torn write"):
        read_wal(wal_path)


def test_non_utf8_bytes_are_corruption_not_a_crash(wal_path: Path) -> None:
    """A WAL file that isn't even valid UTF-8 (torn write mid-byte-sequence, or garbage
    landing on the path) must surface as ``WalCorruption`` like any other damage - not
    an unhandled ``UnicodeDecodeError`` escaping ``read_wal``.
    """
    wal_path.write_bytes(bytes([0xF6, 0x00, 0x9B, 0xFF]))

    with pytest.raises(WalCorruption, match="not valid UTF-8"):
        read_wal(wal_path)


def test_repair_discards_from_the_damaged_record_onward_and_says_so(wal_path: Path) -> None:
    wal = open_wal(wal_path)
    wal.record_term(3, "n1")
    for index in range(1, 5):
        wal.record_append(entry(index, term=3, command=index))
    wal.record_commit(4)
    wal.close()

    lines = wal_path.read_text(encoding="utf-8").splitlines(keepends=True)
    lines[4] = "00000000 " + lines[4].split(" ", 1)[1]  # record 5: entry at index 3
    wal_path.write_text("".join(lines), encoding="utf-8")

    recovery = read_wal(wal_path, repair=True)

    assert recovery.dropped_records == 3  # the damaged entry, its successor, the commit
    assert [e.index for e in recovery.entries] == [1, 2]
    assert recovery.term == 3
    # The commit record went with them, so nothing claims those entries were committed.
    assert recovery.commit_index == 0


def test_a_repaired_wal_is_left_clean_on_disk(wal_path: Path) -> None:
    wal = open_wal(wal_path)
    for index in range(1, 4):
        wal.record_append(entry(index, command=index))
    wal.close()
    lines = wal_path.read_text(encoding="utf-8").splitlines(keepends=True)
    lines[2] = "deadbeef " + lines[2].split(" ", 1)[1]
    wal_path.write_text("".join(lines), encoding="utf-8")

    revived = WriteAheadLog(wal_path, node_id="n1", sync=False)
    recovery = revived.recover(repair=True)
    revived.close()

    assert recovery.dropped_records == 2
    assert not read_wal(wal_path).discarded


# --- log validation: checks a checksum cannot make --------------------------------


def test_a_hole_in_the_indexes_is_corruption_even_with_valid_checksums(
    wal_path: Path,
) -> None:
    wal = open_wal(wal_path)
    wal.record_append(entry(1, command="a"))
    wal.close()
    append_raw(wal_path, encode_record({"kind": KIND_ENTRY, "term": 1, "index": 5, "command": "e"}))

    with pytest.raises(WalCorruption, match="hole"):
        read_wal(wal_path)


def test_a_term_that_goes_backward_is_corruption(wal_path: Path) -> None:
    wal = open_wal(wal_path)
    wal.record_term(7, None)
    wal.close()
    append_raw(wal_path, encode_record({"kind": KIND_TERM, "term": 6, "voted_for": None}))

    with pytest.raises(WalCorruption, match="term went backward"):
        read_wal(wal_path)


def test_a_commit_index_past_the_end_of_the_log_is_corruption(wal_path: Path) -> None:
    wal = open_wal(wal_path)
    wal.record_append(entry(1, command="a"))
    wal.record_commit(1)
    wal.close()
    append_raw(wal_path, encode_record({"kind": "commit", "index": 9}))

    with pytest.raises(WalCorruption, match="past the end"):
        read_wal(wal_path)


def test_a_commit_index_that_goes_backward_is_corruption(wal_path: Path) -> None:
    wal = open_wal(wal_path)
    wal.record_append(entry(1, command="a"))
    wal.record_append(entry(2, command="b"))
    wal.record_commit(2)
    wal.close()
    append_raw(wal_path, encode_record({"kind": "commit", "index": 1}))

    with pytest.raises(WalCorruption, match="commit index went backward"):
        read_wal(wal_path)


def test_a_truncate_outside_the_log_is_corruption(wal_path: Path) -> None:
    wal = open_wal(wal_path)
    wal.record_append(entry(1, command="a"))
    wal.close()
    append_raw(wal_path, encode_record({"kind": "truncate", "index": 4}))

    with pytest.raises(WalCorruption, match="not inside the recovered log"):
        read_wal(wal_path)


def test_a_future_format_version_is_refused_rather_than_guessed_at(wal_path: Path) -> None:
    append_raw(wal_path, encode_record({"kind": KIND_HEADER, "version": 99, "node": "n1"}))

    with pytest.raises(WalError, match="format version 99"):
        read_wal(wal_path)


def test_a_malformed_record_body_names_the_record_that_was_wrong(wal_path: Path) -> None:
    append_raw(wal_path, encode_record({"kind": KIND_ENTRY, "term": "not-a-number", "index": 1}))

    with pytest.raises(WalCorruption, match="record 1 .entry. is malformed"):
        read_wal(wal_path)


def test_opening_another_nodes_wal_is_refused(wal_path: Path) -> None:
    open_wal(wal_path, node_id="n1").close()

    with pytest.raises(WalError, match="is n1's WAL"):
        WriteAheadLog(wal_path, node_id="n2").recover()


# --- the restart report 2.0 item 9 asks for --------------------------------------


def test_the_recovery_report_states_what_came_back(wal_path: Path) -> None:
    wal = open_wal(wal_path)
    wal.record_term(18, "n1")
    for index in range(1, 12):
        wal.record_append(entry(index, term=18, command=index))
    wal.record_commit(10)
    wal.close()

    report = str(read_wal(wal_path))

    assert "Loading WAL..." in report
    assert "Validating records..." in report
    assert "Replaying committed entries..." in report
    assert "Term: 18" in report
    assert "Entries: 11" in report
    assert "Commit Index: 10" in report


# --- the CLI ---------------------------------------------------------------------


def test_cli_inspects_a_wal(wal_path: Path) -> None:
    wal = open_wal(wal_path)
    wal.record_term(2, "n1")
    wal.record_append(entry(1, term=2, command=Put(key="a", value=1).to_wire()))
    wal.record_commit(1)
    wal.close()

    result = runner.invoke(app, ["inspect-wal", str(wal_path)])

    assert result.exit_code == 0
    assert "Term: 2" in result.stdout
    assert "Commit Index: 1" in result.stdout


def test_cli_reports_a_corrupt_wal_without_pretending_to_recover_it(wal_path: Path) -> None:
    wal = open_wal(wal_path)
    for index in range(1, 4):
        wal.record_append(entry(index, command=index))
    wal.close()
    lines = wal_path.read_text(encoding="utf-8").splitlines(keepends=True)
    lines[1] = "00000000 " + lines[1].split(" ", 1)[1]
    wal_path.write_text("".join(lines), encoding="utf-8")

    result = runner.invoke(app, ["inspect-wal", str(wal_path)])
    assert result.exit_code == 1
    assert "corrupt" in result.stdout.lower()

    repaired = runner.invoke(app, ["inspect-wal", str(wal_path), "--repair"])
    assert repaired.exit_code == 0
    assert "discarded" in repaired.stdout

    # The repair has to reach the file: a second read must find it clean, or --repair
    # only ever described a truncation it never performed.
    again = runner.invoke(app, ["inspect-wal", str(wal_path)])
    assert again.exit_code == 0
    assert "WAL corrupt:" not in again.stdout


def test_cli_reports_non_utf8_wal_bytes_cleanly_instead_of_a_traceback(
    wal_path: Path,
) -> None:
    wal_path.write_bytes(bytes([0xF6, 0x00, 0x9B, 0xFF]))

    result = runner.invoke(app, ["inspect-wal", str(wal_path)])

    assert result.exit_code == 1
    assert "corrupt" in result.stdout.lower()


def test_cli_lists_the_records_when_asked(wal_path: Path) -> None:
    wal = open_wal(wal_path)
    wal.record_term(1, "n1")
    wal.close()

    result = runner.invoke(app, ["inspect-wal", str(wal_path), "--records"])

    assert result.exit_code == 0
    assert KIND_HEADER in result.stdout
    assert KIND_TERM in result.stdout
