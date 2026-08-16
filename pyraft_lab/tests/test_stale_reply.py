"""A delayed AppendEntries reply must not be credited to a later, larger request.

Figure 2's reply carries only ``term`` and ``success``, so a leader that has two
requests outstanding to one peer cannot tell which of them a reply answers. ``node.py``
keeps at most one in flight per peer to avoid exactly that - but the in-flight slot is
released on a *timeout* as well as on a reply, and a request whose reply is merely slow
(rather than lost) leaves the leader holding a reply it cannot attribute. Crediting it
to the newer request advances ``matchIndex`` past what the follower actually stored,
which lets ``advance_commit_index`` see a majority that does not exist.

That is the docs/architecture.md P8 "fifth bug": committed data reported to a client
and then lost. This is its regression test.
"""

from __future__ import annotations

import random

from pyraft_lab.network.clock import VirtualClock
from pyraft_lab.raft.log import LogEntry
from pyraft_lab.raft.node import RaftNode
from pyraft_lab.raft.rpc import AppendEntries, handle_append_entries
from pyraft_lab.raft.state import RaftState, Role


class RecordingTransport:
    """Records what the leader sends and delivers nothing back on its own."""

    def __init__(self) -> None:
        self.sent: list[tuple[str, object]] = []

    def register(self, node_id: str) -> object:
        return _NullInbox()

    def unregister(self, node_id: str, *, crashed: bool = True) -> None:
        return None

    def send(self, src: str, dst: str, message: object) -> None:
        self.sent.append((dst, message))


class _NullInbox:
    pass


def _leader(transport: RecordingTransport, clock: VirtualClock) -> RaftNode:
    node = RaftNode(
        node_id="n1",
        peers=["n1", "n2", "n3"],
        transport=transport,
        rng=random.Random(0),
        clock=clock,
    )
    node.state.role = Role.LEADER
    node.state.current_term = 1
    node.state.next_index = {"n2": 1, "n3": 1}
    node.state.match_index = {"n2": 0, "n3": 0}
    return node


def _appends(transport: RecordingTransport, peer: str) -> list[AppendEntries]:
    return [m for dst, m in transport.sent if dst == peer and isinstance(m, AppendEntries)]


async def test_a_slow_reply_is_not_credited_to_a_later_larger_request() -> None:
    clock = VirtualClock()
    transport = RecordingTransport()
    leader = _leader(transport, clock)

    # A follower that really will apply what it is sent, so its replies are authentic
    # rather than hand-built to suit the assertion.
    follower = RaftState(node_id="n2")
    follower.current_term = 1

    for index in range(1, 4):
        leader.state.log.append(LogEntry(term=1, index=index, command={"i": index}))

    # Round 1: the leader sends entries up to index 3.
    leader._replicate_to("n2")
    first = _appends(transport, "n2")[-1]
    assert first.prev_log_index + len(first.entries) == 3

    # The follower really does store them, and really does reply - but the reply is
    # still in flight when the leader gives up waiting for it.
    slow_reply = handle_append_entries(follower, first)
    assert slow_reply.success
    assert follower.log.last_index == 3

    # The log grows and the in-flight slot times out, so the leader sends a second,
    # larger request. The follower never receives this one.
    for index in range(4, 28):
        leader.state.log.append(LogEntry(term=1, index=index, command={"i": index}))
    await clock.advance(leader._reply_timeout * 2)
    leader._replicate_to("n2")
    second = _appends(transport, "n2")[-1]
    assert second.prev_log_index + len(second.entries) == 27

    # Only now does round 1's reply arrive. It confirms index 3 - nothing more.
    leader._on_append_reply("n2", slow_reply)

    assert leader.state.match_index["n2"] == 3, (
        "a reply to the 3-entry request was credited as if it answered the 27-entry "
        "one; the follower is now believed to hold entries it has never seen"
    )


async def test_the_credited_index_never_exceeds_what_the_follower_stored() -> None:
    """The same property stated the way the safety argument needs it."""
    clock = VirtualClock()
    transport = RecordingTransport()
    leader = _leader(transport, clock)

    follower = RaftState(node_id="n2")
    follower.current_term = 1

    for index in range(1, 6):
        leader.state.log.append(LogEntry(term=1, index=index, command={"i": index}))

    leader._replicate_to("n2")
    reply = handle_append_entries(follower, _appends(transport, "n2")[-1])

    for index in range(6, 41):
        leader.state.log.append(LogEntry(term=1, index=index, command={"i": index}))
    await clock.advance(leader._reply_timeout * 2)
    leader._replicate_to("n2")

    leader._on_append_reply("n2", reply)

    assert leader.state.match_index["n2"] <= follower.log.last_index
