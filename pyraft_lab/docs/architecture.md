# Architecture

A living document. Each phase appends the decisions it made and why, so the Day 14
technical report is assembled rather than reconstructed from memory.

## Layers

```
CLIENTS            put / get / delete / cas, leader discovery, retry with backoff
   |
RAFT CLUSTER       3-5 nodes, each with a log, a KV state machine and a WAL
   |
TRANSPORT          InMemoryBus (honest) or SimulatedNetwork (latency/loss/partitions)
   |
EXPERIMENT LAYER   scenario runner, fault injector, metrics, analytics
```

The transport sits between the cluster and everything below it deliberately: a Raft
node has no idea whether its messages are crossing an honest bus or a simulated
network dropping 30 % of them.

## Decisions

### D1 — asyncio streams over gRPC

**Decision:** plain asyncio and an in-process message bus. No gRPC, no protobuf.

**Why:** the whole point of the project is a *deterministic* network. gRPC would put a
real kernel socket between two nodes, which is exactly the source of nondeterminism the
lab exists to eliminate — and it would cost a day of the fifteen to set up. Nothing in
the requirements needs cross-process transport. If it is ever wanted, `Transport` is a
three-method protocol and a gRPC implementation slots in behind it without the Raft
code noticing.

### D1 — messages are frozen, tagged, and round-trip on every send

**Decision:** every message is an immutable Pydantic model declaring a unique `KIND`,
registered at class-definition time. The bus serializes and deserializes every message
even though sender and receiver share a process.

**Why:** three problems disappear at once.

- *Type loss.* A field typed as the `Message` base would serialize to `{}` and drop
  every subclass field. Tagging the body with `kind` and rebuilding the concrete class
  from a registry makes the round trip lossless.
- *Shared mutable state.* Handing a peer a reference to a live dict is not something a
  real network permits. Round-tripping guarantees the receiver holds a copy, so a bug
  where a node mutates a message another node already "received" cannot exist.
- *Late failures.* An unserializable message fails on the send that created it, not on
  Day 11 when the experiment runner first tries to persist it.

The cost is real CPU per send, and it is worth paying — the simulation is virtual-clock
driven, so serialization cost never distorts a measured latency.

### D1 — an unknown destination is dropped, an unknown sender raises

**Decision:** `send()` to an unregistered destination increments a counter and returns.
`send()` *from* an unregistered node raises `UnknownNode`.

**Why:** on a real network a sender cannot distinguish a crashed peer from a slow one,
so the transport must not offer that information — a node that could detect delivery
failure synchronously would be able to cheat at consensus. An unregistered *sender*,
by contrast, is never a network condition; it is a programming error, and should fail
loudly.

### D1 — JSON now, msgpack behind the same protocol

**Decision:** `JsonCodec` with sorted keys and no whitespace, behind a `Codec` protocol.

**Why:** readable in test failures and dependency-free. Sorted keys make encoding
deterministic, which matters once runs must be byte-for-byte reproducible from a seed.
Swapping in msgpack later is one new class implementing two methods.

### D1 — the log is 1-indexed (recorded now, implemented Day 2)

The Raft paper indexes the log from 1. Matching it exactly avoids a class of off-by-one
bugs in `prevLogIndex` handling that are very hard to see in a partition trace. Index 0
holds a sentinel entry at term 0.

### D2 — RPC handlers are pure functions of (state, message)

**Decision:** `handle_request_vote` and `handle_append_entries` take a `RaftState` and
an RPC message and return a reply. They touch no clock, no transport, and no other
node's state.

**Why:** Figure 2's receiver rules are themselves pure - "given my state and this
request, what do I become and what do I reply?" Keeping the handlers that way makes
every rule (stale-term rejection, log-matching check, conflict truncation, commit-index
advance) unit-testable with a bare `RaftState`, no asyncio and no second node. Day 3's
election loop and Day 4's replication loop are what call these handlers in response to
timers and RPC arrivals; they own the *when*, the handlers own the *what*.

### D2 — term advancement is centralized in `RaftState.observe_term`

**Decision:** the only way `current_term` increases after startup is
`RaftState.observe_term(term)`, which also resets `voted_for`, drops to `FOLLOWER`, and
clears `next_index`/`match_index`.

**Why:** paper §5.1's "if RPC request or response contains term T > currentTerm: set
currentTerm = T, convert to follower" is easy to apply in one handler and forget in
another. Funneling it through one method means every RPC path gets the step-down for
free by calling it first, rather than by each handler re-deriving the rule.

### D4 — replication and heartbeats are one code path

**Decision:** there is no separate heartbeat message builder. `build_append_entries`
sends whatever a follower is missing from its `nextIndex`; a caught-up follower yields
an empty `entries` list, which *is* the heartbeat.

**Why:** two code paths that must stay in lockstep on `prevLogIndex`, `prevLogTerm` and
`leaderCommit` is a standing invitation for them to drift — the classic bug being a
heartbeat that carries a stale `leaderCommit` and quietly stalls follower commits. One
builder cannot drift from itself.

### D4 — the leader remembers what it sent

**Decision:** `RaftNode` keeps a per-peer `_inflight` index; `handle_append_reply` takes
that `last_index_sent` as an argument rather than reading an index off the reply.

**Why:** Figure 2's `AppendEntriesReply` carries only `term` and `success`. Adding a
match index to the reply is a common optimization, but it changes the wire format away
from the paper, and Day 8's simulator will reorder and duplicate these replies — at
which point a reply that self-reports its position is *more* dangerous, not less, since
a stale one looks authoritative. Keeping the record on the leader means a late reply is
folded in with `max()` and can never drag `matchIndex` backward.

### D5 — commands are tagged dicts, not typed objects, inside a log entry

**Decision:** `LogEntry.command` stays `Any`. Commands define `to_wire()` /
`command_from_wire()` and travel as `{"kind": ..., ...}` dicts.

**Why:** the same type-loss problem as D1, arriving through a different door. Raft must
treat a command as opaque — a consensus layer that has to understand a payload to
replicate it is not a consensus layer — but opacity means the codec serializes it as
plain JSON and the class is gone on arrival. Tagging restores the type at the one place
that needs it (`kv/store.py`) without giving `raft/` any knowledge of what a command is.

### D6 — a write is answered after it applies, never when it is appended

**Decision:** `RaftNode` holds a `_pending` map from log index to the waiting client and
replies from `_apply_committed`.

**Why:** an appended entry is not yet safe — it can still be truncated by a future
leader. Acknowledging on append would let a client observe a write that later vanishes,
which is precisely the anomaly Phase 5's linearizability checker exists to catch. On
losing leadership the map is dropped and the client's timeout drives a retry, rather
than the node answering for a term it no longer owns.

### D8 — time is an injected `Clock`, not `asyncio`

**Decision:** `RaftNode` and `SimulatedNetwork` both take a `Clock`. `WallClock` wraps
the event loop's own time; `VirtualClock` only moves when a test calls `advance()`.

**Why:** a deterministic network is the point of the project, and a network whose
delays are measured against wall time is not deterministic no matter how well seeded
its RNG is — the same run on a loaded machine interleaves differently. Making time an
interface means Phase 7's replay can reconstruct a run exactly, and a test can assert
"this message arrives at t=0.1 and not before" rather than polling and hoping.

`Clock` carries `call_at` alongside `sleep_until` because the two callers need
different things. A node's timer has a coroutine to suspend, so `sleep_until` suits it.
A delayed *delivery* has no coroutine of its own — spawning a task per message just to
hold a timer would mean a message is not registered as in-flight until the scheduler
gets round to that task, so a `VirtualClock.advance()` immediately after a `send()`
could miss it. `call_at` registers synchronously, inside `send` itself.

### D8 — `_wait_for_message` keeps the `wait_for` fast path

**Decision:** on a `WallClock`, the receive loop still uses `asyncio.wait_for` exactly
as it did before Day 8. Only a `VirtualClock` takes the two-task race.

**Why:** the general path costs two extra tasks per loop iteration, and the loop
iterates every heartbeat — 12 ms in the test suite. That overhead was enough to shift
scheduling under load and make a replication test flaky. The virtual clock cannot use
`wait_for` (its timeout would be real seconds against virtual time), but the wall clock
has no reason to pay for a mechanism it does not need.

The race itself has one real obligation: whichever task loses, and the case where the
whole call is cancelled mid-wait by `stop()`, must both leave no task still registered
as a waiter on the inbox queue. An orphaned `recv_envelope` there is invisible and
permanent — nothing will ever await it again.

### D10 — reads are served under a leader lease, not ReadIndex

**Decision:** a `Get` never enters the log. The leader answers it from its local
`KVStore` once (a) a majority has acknowledged an AppendEntries in the current term
within the last `election_timeout_min` seconds, and (b) `last_applied` has caught up to
the `commit_index` that was current when the read arrived.

**Why:** paper §8's ReadIndex wants the leader to confirm it still leads by exchanging a
heartbeat round *for that read*. Doing that correctly needs the reply to identify which
round it answers — and `AppendEntriesReply` deliberately carries only `term` and
`success` (D4). Adding a tag would be a second departure from Figure 2's wire format, in
the exact place where Day 8's simulator reordering makes a self-describing reply most
dangerous.

The lease gets the same guarantee from timing instead. A follower resets its election
timer whenever it accepts an AppendEntries, and its next timeout is drawn from
`[min, max]` — so within `min` seconds of an acknowledgement, that follower provably has
not voted for anyone. A majority of such acknowledgements means no challenger can have
won, so this node is still leader and its applied state is authoritative.

The cost is an honest one, and worth naming: the lease trusts clocks not to drift
between nodes. That is a real assumption a ReadIndex implementation would not need. In a
simulation where every node reads the same `Clock` object it holds exactly; on real
hardware it would need bounding.

### D10 — a new leader commits a no-op immediately

**Decision:** `_win_election` appends an entry with a `None` command before doing
anything else.

**Why:** §5.4.2 forbids a leader from committing an entry from an earlier term by
counting replicas. A leader elected into a cluster whose last entries are all from
older terms therefore cannot advance `commitIndex` at all until it gets a write of its
own — even though Leader Completeness guarantees it already holds every committed
entry. On a busy cluster the next client write hides this. On an idle one that only
serves reads, `commit_index` would sit frozen behind the true committed state
indefinitely, and D10's read index would hold reads that should have been servable.
One no-op from the current term unfreezes the whole prefix the first time it commits.

`KVStore._apply_one` already treats a `None` command as a harmless no-op, which is why
this needed no state-machine change.

## Deferred

- **InstallSnapshot.** Listed in the specification's RPC table but assigned no day, and
  genuinely out of scope for 15 days. Declared deferred rather than half-built.
- **A leader that self-demotes without a quorum.** A partitioned leader keeps believing
  it leads until it hears a higher term — correct per §5.1, and safe, since D10's lease
  already stops it serving reads and the majority rule already stops it committing. A
  step-down-on-lease-expiry rule would only change what `role` reports, so it is left
  out rather than added for cosmetics.
