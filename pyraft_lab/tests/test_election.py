"""Election decision logic: timeouts, tallies, candidacy and promotion."""

from __future__ import annotations

import random

import pytest

from pyraft_lab.raft.election import (
    ELECTION_TIMEOUT_RANGE,
    HEARTBEAT_INTERVAL,
    VoteTally,
    become_leader,
    begin_election,
    majority,
    random_election_timeout,
)
from pyraft_lab.raft.log import LogEntry
from pyraft_lab.raft.state import RaftState, Role


def test_election_timeout_falls_inside_the_configured_span() -> None:
    rng = random.Random(0)
    low, high = ELECTION_TIMEOUT_RANGE

    for _ in range(200):
        assert low <= random_election_timeout(rng) <= high


def test_election_timeout_is_reproducible_from_a_seed() -> None:
    """Phase 7's replay depends on this holding everywhere randomness is used."""
    a = [random_election_timeout(random.Random(7)) for _ in range(3)]
    b = [random_election_timeout(random.Random(7)) for _ in range(3)]
    assert a == b


def test_election_timeouts_differ_across_nodes() -> None:
    """Identical timeouts would split the vote forever (paper §5.2)."""
    draws = {random_election_timeout(random.Random(seed)) for seed in range(20)}
    assert len(draws) > 1


def test_heartbeat_is_well_under_the_minimum_election_timeout() -> None:
    """A live leader must always refresh a follower before its timer can expire."""
    assert ELECTION_TIMEOUT_RANGE[0] > HEARTBEAT_INTERVAL * 2


@pytest.mark.parametrize(
    ("cluster_size", "needed"),
    [(1, 1), (2, 2), (3, 2), (4, 3), (5, 3), (6, 4), (7, 4)],
)
def test_majority_is_a_strict_majority(cluster_size: int, needed: int) -> None:
    assert majority(cluster_size) == needed


# --- VoteTally --------------------------------------------------------------------


def test_tally_starts_with_the_self_vote() -> None:
    tally = VoteTally(term=1, cluster_size=3, candidate="n1")
    assert tally.votes == 1
    assert tally.won is False


def test_tally_wins_on_a_majority() -> None:
    tally = VoteTally(term=1, cluster_size=3, candidate="n1")
    tally.record("n2", granted=True)
    assert tally.won is True


def test_tally_ignores_refusals() -> None:
    tally = VoteTally(term=1, cluster_size=5, candidate="n1")
    tally.record("n2", granted=False)
    tally.record("n3", granted=False)
    assert tally.votes == 1
    assert tally.won is False


def test_duplicate_grants_cannot_inflate_the_tally() -> None:
    """A retried or duplicated reply must not manufacture a majority."""
    tally = VoteTally(term=1, cluster_size=5, candidate="n1")
    for _ in range(5):
        tally.record("n2", granted=True)

    assert tally.votes == 2
    assert tally.won is False


def test_a_single_node_cluster_wins_immediately() -> None:
    assert VoteTally(term=1, cluster_size=1, candidate="solo").won is True


# --- begin_election / become_leader -----------------------------------------------


def test_begin_election_increments_term_and_self_votes() -> None:
    state = RaftState(node_id="n1", current_term=4)
    request, tally = begin_election(state, random.Random(0), cluster_size=3)

    assert state.current_term == 5
    assert state.role is Role.CANDIDATE
    assert state.voted_for == "n1"
    assert request.term == 5
    assert request.candidate_id == "n1"
    assert tally.votes == 1


def test_begin_election_advertises_the_candidates_last_log_position() -> None:
    state = RaftState(node_id="n1", current_term=2)
    state.log.append(LogEntry(term=2, index=1, command="a"))
    state.log.append(LogEntry(term=2, index=2, command="b"))

    request, _ = begin_election(state, random.Random(0), cluster_size=3)

    assert (request.last_log_index, request.last_log_term) == (2, 2)


def test_become_leader_initializes_follower_tracking() -> None:
    state = RaftState(node_id="n1", current_term=3, role=Role.CANDIDATE)
    state.log.append(LogEntry(term=3, index=1, command="a"))

    become_leader(state, ["n2", "n3"])

    assert state.role is Role.LEADER
    # nextIndex is optimistic (last + 1); matchIndex knows nothing yet.
    assert state.next_index == {"n2": 2, "n3": 2}
    assert state.match_index == {"n2": 0, "n3": 0}
