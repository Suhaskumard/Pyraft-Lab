"""Raft's per-node state: the fields in Figure 2 of the paper, nothing else.

Persistent state (current_term, voted_for, log) must survive a crash - it is written
to the WAL before a node acts on it, from Day 7. Volatile state resets on restart.
Leader-only state exists only while ``role is Role.LEADER``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum

from pyraft_lab import NodeId
from pyraft_lab.raft.log import RaftLog


class Role(Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


@dataclass
class RaftState:
    """One node's view of Raft's Figure 2 state.

    Term transitions are funneled through :meth:`observe_term`: it is the only place
    allowed to raise ``current_term`` after startup, so "step down on a higher term"
    (paper §5.1, Rules for Servers) can never be forgotten at a call site.
    """

    node_id: NodeId

    # Persistent
    current_term: int = 0
    voted_for: NodeId | None = None
    log: RaftLog = field(default_factory=RaftLog)

    # Volatile
    commit_index: int = 0
    last_applied: int = 0
    role: Role = Role.FOLLOWER

    # Leader-only - populated by election.py on winning an election (Day 3), cleared
    # on stepping down. Empty for followers and candidates.
    next_index: dict[NodeId, int] = field(default_factory=dict)
    match_index: dict[NodeId, int] = field(default_factory=dict)

    def observe_term(self, term: int) -> bool:
        """Apply Raft's universal rule: any RPC carrying a higher term wins.

        Steps this node down to FOLLOWER, clears its vote for the new term, and drops
        any leader-only state. Returns True if ``term`` was in fact higher, so callers
        that need to react further (e.g. cancelling a leader's heartbeat timer) know
        whether a step-down just happened.
        """
        if term <= self.current_term:
            return False
        self.current_term = term
        self.voted_for = None
        self.role = Role.FOLLOWER
        self.next_index.clear()
        self.match_index.clear()
        return True
