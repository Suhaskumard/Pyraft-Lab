"""A retried client write must not apply twice (paper §6.3).

Rule 2 in ``kv/store.py`` makes a *log entry* apply exactly once, which is enough for a
replayed commit. It is not enough for a client whose reply was lost: that client retries
the same request, the leader appends it again, and the log now holds two distinct
entries carrying one intent. Applying both is not a replay - it is the write happening
twice, and the second copy lands *after* whatever was written in between, so an
overwritten value comes back to life.

That is a linearizability violation with no lost data and no diverged log, which is why
it survived every safety check the laboratory had: `scenarios/packet_loss.yaml` failed
its own `expected: linearizable` block on unmodified HEAD for 2 of seeds 33-80.
"""

from __future__ import annotations

from typing import Any

from pyraft_lab.kv.commands import SESSION_CLIENT, SESSION_SERIAL, Put
from pyraft_lab.kv.store import KVStore
from pyraft_lab.raft.log import LogEntry, RaftLog
from pyraft_lab.raft.node import _serial_of


def write(key: str, value: Any, client: str | None = None, serial: int | None = None) -> dict:
    wire = Put(key=key, value=value).to_wire()
    if client is not None:
        wire = {**wire, SESSION_CLIENT: client, SESSION_SERIAL: serial}
    return wire


def log_of(*commands: Any) -> RaftLog:
    log = RaftLog()
    for index, command in enumerate(commands, start=1):
        log.append(LogEntry(term=1, index=index, command=command))
    return log


def test_a_retried_write_does_not_resurrect_an_overwritten_value() -> None:
    """The exact shape that broke packet_loss: A, then B, then A's retry."""
    store = KVStore()
    log = log_of(
        write("k", "A", client="client1", serial=7),
        write("k", "B", client="client2", serial=3),
        # client1 never heard back, so it retried request 7 and the leader logged it
        # again - after B.
        write("k", "A", client="client1", serial=7),
    )

    store.apply_committed(log, log.last_index)

    assert store.read("k") == "B", (
        "client1's retry of request 7 was applied a second time and overwrote B, "
        "a value written after it"
    )


def test_the_retry_still_gets_the_answer_its_first_attempt_earned() -> None:
    """Deduplicating must not mean refusing: the client is owed one result."""
    store = KVStore()
    log = log_of(
        write("k", "A", client="client1", serial=7),
        write("k", "A", client="client1", serial=7),
    )

    results = store.apply_committed(log, log.last_index)

    assert [r.ok for r in results] == [True, True]
    assert results[1].index == 2  # answered for *its* entry, not the first one


def test_a_later_request_from_the_same_client_is_not_mistaken_for_a_retry() -> None:
    store = KVStore()
    log = log_of(
        write("k", "A", client="client1", serial=7),
        write("k", "B", client="client1", serial=8),
    )

    store.apply_committed(log, log.last_index)

    assert store.read("k") == "B"


def test_two_clients_do_not_share_a_session() -> None:
    """Same serial, different clients - unrelated requests, both must apply."""
    store = KVStore()
    log = log_of(
        write("a", "from-1", client="client1", serial=4),
        write("b", "from-2", client="client2", serial=4),
    )

    store.apply_committed(log, log.last_index)

    assert store.read("a") == "from-1"
    assert store.read("b") == "from-2"


def test_an_unstamped_command_still_applies() -> None:
    """Internal writes and direct-drive tests carry no session and are not deduplicated."""
    store = KVStore()
    log = log_of(write("k", "A"), write("k", "B"), write("k", "A"))

    store.apply_committed(log, log.last_index)

    assert store.read("k") == "A"


def test_the_serial_is_read_off_the_client_s_own_request_id() -> None:
    assert _serial_of("client1-117") == 117
    assert _serial_of("repl-client-3") == 3  # ids with hyphens inside the client name
    assert _serial_of("no-number-here") is None
    assert _serial_of("") is None
