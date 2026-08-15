"""Leader election: the decisions, with no timers and no I/O.

Everything here is a pure function of state plus a seeded RNG, for the same reason the
Day 2 RPC handlers are (see docs/architecture.md, D2): the rules are testable without
an event loop, and a seeded ``Random`` keeps a run reproducible once Phase 7's replay
needs it. ``raft/node.py`` owns the *when* - the timers, the broadcast, the waiting.
"""

from __future__ import annotations

import random

from pyraft_lab import NodeId
from pyraft_lab.raft.rpc import RequestVote
from pyraft_lab.raft.state import RaftState, Role

ELECTION_TIMEOUT_RANGE = (0.150, 0.300)
"""Seconds. The paper's recommended 150-300 ms spread."""

HEARTBEAT_INTERVAL = 0.050
"""Seconds. Comfortably under the minimum election timeout, so a live leader always
refreshes a follower's timer before it can expire."""


def random_election_timeout(
    rng: random.Random, span: tuple[float, float] = ELECTION_TIMEOUT_RANGE
) -> float:
    """Draw a randomized election timeout.

    Randomization is what breaks split votes (paper §5.2): if every node waited the
    same interval they would campaign in lockstep and keep splitting the vote forever.
    """
    low, high = span
    return rng.uniform(low, high)


def majority(cluster_size: int) -> int:
    """Votes needed to win, i.e. a strict majority of the whole cluster."""
    return cluster_size // 2 + 1


class VoteTally:
    """Counts votes for one candidacy, in one term.

    Votes are tracked per voter rather than as a bare counter so a duplicate reply -
    a retried RequestVote, or a message the simulator delivers twice from Day 8 -
    cannot inflate the count into a false majority.
    """

    def __init__(self, term: int, cluster_size: int, candidate: NodeId) -> None:
        self.term = term
        self.cluster_size = cluster_size
        self.candidate = candidate
        self._granted: set[NodeId] = {candidate}  # a candidate always votes for itself

    def record(self, voter: NodeId, granted: bool) -> None:
        if granted:
            self._granted.add(voter)

    @property
    def votes(self) -> int:
        return len(self._granted)

    @property
    def won(self) -> bool:
        return self.votes >= majority(self.cluster_size)


def begin_election(
    state: RaftState, rng: random.Random, cluster_size: int
) -> tuple[RequestVote, VoteTally]:
    """Transition to candidate and produce the RequestVote to broadcast.

    Paper §5.2: increment the term, vote for yourself, then solicit votes. Returns the
    request together with a tally already crediting the self-vote, so the caller cannot
    forget to count it.
    """
    state.current_term += 1
    state.role = Role.CANDIDATE
    state.voted_for = state.node_id

    request = RequestVote(
        term=state.current_term,
        candidate_id=state.node_id,
        last_log_index=state.log.last_index,
        last_log_term=state.log.last_term,
    )
    tally = VoteTally(state.current_term, cluster_size, state.node_id)
    return request, tally


def become_leader(state: RaftState, peers: list[NodeId]) -> None:
    """Promote a winning candidate and initialize its per-follower tracking.

    Paper Figure 2: ``nextIndex`` starts optimistically at the leader's own last index
    + 1 and walks backward when a follower rejects (Day 4); ``matchIndex`` starts at 0
    because nothing is known to be replicated yet.
    """
    state.role = Role.LEADER
    next_index = state.log.last_index + 1
    state.next_index = {peer: next_index for peer in peers}
    state.match_index = dict.fromkeys(peers, 0)
