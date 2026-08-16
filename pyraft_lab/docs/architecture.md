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

Labels are the day the decision was made — `D8` is Day 8. Phases the 2.0 upgrade added
have no day of their own, so their decisions are labelled by phase instead: `P4`.

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

### P4 — instrumentation reports, it never decides

**Decision:** the tracer is injected into `RaftNode` and `SimulatedNetwork` only. The
pure modules — `election.py`, `rpc.py`, `replication.py` — never learn that it exists,
and every event is emitted by the driver that already owned the *when*.

**Why:** D2 made the receiver rules pure so they could be tested without an event loop.
Threading a tracer through them would undo exactly that, and worse: a decision function
holding a recorder is a decision function that can be changed by recording. Emitting
from `node.py` costs nothing in fidelity, since the driver observes every state change
the pure functions make, and it keeps the property that turning tracing on cannot alter
what the algorithm does — the one property a debugging tool must not cost you.

The same reasoning makes recording opt-in. A node built without a tracer gets
`NULL_TRACER`, which drops an event before reading a clock, so the 250 tests that
predate this phase pay nothing for it rather than paying a little.

**Consequence:** two values move inside pure functions that then have nobody to report
them — the term (`RaftState.observe_term`, D2) and `commitIndex` (both ends of
replication, D4). Rather than have every call site remember to announce them, `node.py`
watches them: `_check_term_change` and `_check_commit_advance` compare against what was
last traced and emit only on a change. `_trace` runs the term check first, so a message
that both raised the term and produced an event yields `TERM_CHANGED` before that event
rather than a trace in which a node answers an RPC from a term it had not yet reached.

### P4 — the trace is one ordered record, not per-node logs

**Decision:** one `Tracer` per run, shared by every node and by the network. Events
carry `(timestamp, seq)` and are ordered by both.

**Why:** the view that cannot explain a distributed failure is a single node's — the
interesting question is always what someone else was doing at the same moment. Merging
per-node files afterwards would mean reconstructing an order that was known for free at
write time.

`seq` is not decoration. Under a `VirtualClock` time only moves when the run advances
it, so a burst of events all share one timestamp; a replay that reorders two
same-instant events is not a replay. The tracer's own monotonic counter is what makes
the order total, and it keeps counting across `clear()` so events either side of one
remain comparable.

### P4 — the network narrates what a node cannot, and the follower narrates why

**Decision:** `NODE_CRASHED`, `NODE_RECOVERED`, `PARTITION_CREATED` and
`PARTITION_HEALED` are emitted by `SimulatedNetwork` and carry no term.
`APPEND_ENTRIES_REJECTED` is emitted by the *follower* that rejected, not the leader
that saw the rejection.

**Why:** a process that has crashed cannot write down that it crashed, and a partition
belongs to no single node. From outside, a clean `stop()` and a crash are the same
observation — precisely as `send` cannot distinguish a crashed peer from a slow one —
so the network is the honest narrator, and it genuinely does not know what term anyone
was in. A first registration is a join, not a recovery; only a node the network has
seen before produces `NODE_RECOVERED`.

The rejection is the mirror image. *Why* a follower rejected — stale term or log
mismatch — exists only on the follower: Figure 2's reply carries `term` and `success`
and nothing else, and D4 keeps it that way deliberately. Recording at the leader would
lose the reason permanently. The leader's half of the exchange is not lost by this: its
`nextIndex` walking back is already visible as the next `APPEND_ENTRIES_SENT` to that
peer with a lower `prevLogIndex`.

### P4 — live counters and post-hoc metrics are different modules

**Decision:** `observability/metrics.py` holds `LiveMetrics`, a running fold of the
event stream. Percentiles, replication lag and per-scenario summaries stay in
`experiments/metrics.py` (Phase 5). `LiveMetrics` reads nothing but events — never a
`RaftNode`'s state.

**Why:** plan.md's conflict 3 resolved that these are two different things; this is the
line that keeps them from merging back. A dashboard needs a value that is correct *now*
and cheap to update per event; a report needs a distribution over a finished run and can
afford to re-read the whole trace. Building one thing that does both would make the
dashboard pay for percentile maintenance at every heartbeat.

Deriving it purely from events costs a little duplication and buys the property that
matters: a metric cannot look right while the trace is wrong. If the fold shows the
wrong leader, the trace is wrong — and the trace is what Phase 7 will replay from.

### P6 — one line per record, checksummed, and text

**Decision:** the WAL is a text file, one record per line: a CRC32 of the payload, a
space, then the payload as canonical JSON. Six record kinds — `header`, `term`, `entry`,
`truncate`, `commit`, `snapshot`.

**Why:** the failure a crash actually produces is a half-written record at the end of the
file, and a line-delimited format makes that failure both detectable and *contained* — the
tail is either a whole valid record or it is not, and the prefix before it is untouched.
A checksum per record rather than per file is what makes that judgement per-record: a
whole-file digest can only say "something is wrong somewhere", which is exactly the answer
that forces you to throw away good data.

Text, at some cost in bytes, because this file is evidence in a failure investigation.
When a partition trace and a WAL disagree, being able to read the WAL with `head` is worth
more than the space a binary frame would save. CRC32 rather than a cryptographic hash for
the same kind of reason inverted: the threat is a torn write and a rotted byte, both
accidents, and defending against those has to be cheap enough to afford on every record.

### P6 — the log journals itself; the node persists the two scalars

**Decision:** `RaftLog` takes a `LogJournal` and records every `append` and
`truncate_from` through it. `currentTerm`/`votedFor` and `commitIndex` are written by
`RaftNode`, which watches them for changes rather than being told.

**Why:** the same split, and the same reasoning, as P4's tracer — with one deliberate
difference. Log mutations pass through exactly two methods, so recording them there means
no call site can forget (D2's argument for `observe_term`). But the term and the commit
index both move *inside* pure functions that have nothing to persist them and should not
grow a WAL to do it, so the node watches those two values the way it already watches them
for tracing.

The difference from the tracer is that this is not instrumentation. A trace that lags
reality is a worse trace; a WAL that lags reality is a lost write. So the journal call is
synchronous and ordered with the mutation, and it is not opt-in per event — only the whole
WAL is optional (`NULL_JOURNAL`, so a node without one pays nothing).

### P6 — fsync lives in `_send`, not at each call site

**Decision:** `RaftNode._send` persists and syncs before handing anything to the
transport. `WriteAheadLog.sync` is the only `fsync` in the system, and is a no-op when
nothing has been written since the last one.

**Why:** the rule that actually matters is not "write everything down" but *nothing this
node has told anyone can be forgotten*. Every claim a node makes about its own state
leaves through one door, so putting the sync in that door makes the rule structural
instead of a checklist item at half a dozen call sites — and a future call site gets it
for free rather than being a silent durability hole.

The cost looks alarming (an fsync per message) and is not: the sync is skipped unless a
record was actually written, so heartbeats and read replies cost nothing, and a batch of
entries appended together is one sync rather than one per entry. `submit` syncs explicitly
as well, because a single-node cluster commits without sending anything.

### P6 — `commitIndex` is persisted, though Figure 2 calls it volatile

**Decision:** a `commit` record goes in the WAL, and recovery restores `commitIndex` from
it, clamped to the length of the log that actually came back.

**Why:** it is safe and it is useful. Safe, because `commitIndex` only ever moves forward
and a recorded value was true when it was written — Raft's guarantee that a committed
entry is on a majority does not expire on a restart. Useful, because the alternative is a
restarted node that knows nothing is committed until a leader tells it again, which means
its state machine is empty until then and 2.0 item 9's "Replaying committed entries..."
has nothing to replay. The clamp is what keeps it honest: if a torn tail took an entry
that a commit record already counted, the recovered commit index follows the log down.

### P6 — compaction is on-disk; the in-memory log stays whole

**Decision:** `take_snapshot` writes the snapshot, rewrites the WAL as snapshot + tail,
and leaves the in-memory log intact. A log with a base index above 0 therefore only ever
appears after a *restart* from a compacted WAL.

**Why:** 2.0 allows skipping network snapshot transfer, and InstallSnapshot stays deferred
(below) — which means a follower that is further behind than a snapshot's boundary can
only be repaired from the leader's log. Dropping the in-memory prefix would trade a bounded
disk cost for an unbounded correctness one: a leader would become unable to catch up a
follower it can currently repair from any point. Compaction on disk buys what a snapshot is
actually for here — a bounded WAL and a bounded restart — without buying that.

Where the prefix genuinely no longer exists, after a restart, `RaftLog`'s base index stands
in for the sentinel and plays exactly the same role in the AppendEntries consistency check,
and `build_append_entries` clamps `prevLogIndex` to it. That clamp is right rather than
merely safe for every follower that still holds the committed prefix — which is all of
them, since a snapshot only ever covers *applied* entries, and applied entries cannot
diverge. The one case it cannot serve is a follower that has lost committed entries below
that base: it rejects, `nextIndex` walks back, the clamp holds, and that follower stops
making progress while the rest of the cluster is unaffected. That is precisely the hole
InstallSnapshot fills, and it is named here rather than hidden.

### P6 — a torn tail recovers; corruption in the middle refuses

**Decision:** a damaged *final* record is discarded and recovery succeeds. A damaged
record with valid records after it raises `WalCorruption` unless recovery is explicitly
asked to repair, in which case that record and everything after it is dropped and the
report says how much.

**Why:** these are different events and treating them alike loses either data or trust.
A damaged last record is a crash during that append — nothing was ever acknowledged on the
strength of it, so dropping it costs nothing and needs no operator. Damage with valid
records behind it is not a torn write, and the records past it cannot simply be applied:
recovery is a *fold*, and a fold that skips a record is not a replay of what happened —
skip a `truncate` and entries reappear that a leader had already overwritten. Silently
folding past it would produce a state the node never had, which is worse than refusing.

Recovery also validates the *sequence*, not just each record: a hole in the indexes, a
term record going backward, a commit index past the end of the log. A checksum proves a
record was not damaged; only the fold can prove the file is a log. Those checks raise even
with repair enabled, because they indicate a bug rather than a bad byte.

### P6 — the experiment runner still builds nodes without WALs

**Decision:** Phase 5's `ExperimentRunner` is unchanged; scenario runs stay in memory.

**Why:** a scenario's reproducibility comes from the seed and the virtual clock, not from
disk, and giving every run an fsync per message would slow the laboratory down without
making a single measurement more accurate. The case where persistence changes the *answer*
is a restart that keeps its state — which is Phase 8's chaos campaign, whose safety
invariant is "committed data is never silently lost". Wiring it in there is one constructor
argument, and it belongs to that phase rather than being pre-emptively bolted on here.

### P7 — a manifest is the scenario plus three knobs, nothing else

**Decision:** `RunManifest` (`experiments/manifest.py`) carries `run_id`, `seed`,
`Scenario.to_dict()`, and `election_timeout_range`/`heartbeat_interval`/`client_timeout`
- the only three `ExperimentRunner` constructor arguments a scenario file does not
already encode.

**Why:** 2.0 item 5 asks for "run_id, random_seed, scenario, network configuration,
fault timeline, workload, event trace" as if these were seven independent things to
capture. Three of them already collapse: a scenario's `faults:` timeline *is* the fault
timeline, and every network setting that ever changes during a run (latency, loss)
arrives as a timeline step (`LATENCY`/`PACKET_LOSS` actions) rather than as separate
static configuration - so "network configuration" has no content a scenario doesn't
already carry, and `workload:` is a field of the same scenario. Writing a manifest that
duplicated these would create two sources of truth for one run's inputs, with no way to
tell which one a bug should be blamed on.

### P7 — replay proves determinism, it does not assume it

**Decision:** `replay_run` reconstructs an `ExperimentRunner` from a manifest, runs it
again, and diffs the new event trace against the persisted one field-for-field -
including `seq`, since a `VirtualClock` run can have many events share one timestamp
(P4) and `seq` is the only thing that orders them. A mismatch is returned as a
`ReplayReport`, not raised.

**Why:** Phase 5's determinism (one seed feeds every random source; time only moves
because the runner advances a `VirtualClock`) was a design *claim* until something
actually exercised it by running the same inputs twice and comparing. Building that
proof is what turns "the runner is deterministic" from an architectural argument into a
falsifiable, per-run check - and per 2.0's own framing, a mismatch is itself a genuine
finding (something about the run stopped reproducing), not a tooling failure, so it is
reported like a linearizability violation rather than thrown as an exception.

### P7 — a virtual timer must be cancelled, not merely orphaned

**Decision:** `VirtualClock.call_at` returns a `TimerHandle`; `sleep_until` cancels its
own handle when the coroutine awaiting it is cancelled from outside.

**Why:** found while building the first replay tests, not designed in from the start -
a real bug, and worth recording as one. `race_deadline`'s general path (`network/
clock.py`, D8) races a deadline against real work and cancels whichever side loses.
On a busy cluster the *deadline* side loses almost every time, because a message
arrives before an election timeout or a client's retry backoff ever comes due - which
means it is the ordinary case, not an edge case, and it happens on every wait a node or
client performs. Before this fix, cancelling the *task* left the timer it had registered
sitting in `VirtualClock`'s heap forever: nothing had ever cancelled the heap entry
itself, only the coroutine that no longer cared about it. A long or busy run accumulated
these by the tens of thousands, and `next_deadline()` kept reporting the oldest one as
the next thing to happen - so the driver spent a whole outer iteration retiring each
dead timer one at a time, and a scenario that should finish in milliseconds instead
crawled forward in sub-millisecond steps, never finishing in practice. Fixed by handing
`sleep_until` a cancellable handle and having it cancel that handle in its own
`except CancelledError` - lazy deletion at the front of the heap, checked by
`next_deadline`/`pending`/`advance_to` alike, so a cancelled timer can never again be
reported as pending or advanced through as if it mattered.

The bug predates Phase 7 - it lived in Day 8's `race_deadline`/`VirtualClock` and could
have bitten any sufficiently long or busy scenario run - but nothing surfaced it until
Phase 7 needed a scenario to run start-to-finish reliably enough to replay, twice, in a
test. Recorded here because "replay was the thing that finally noticed" is itself worth
knowing the next time a laboratory run behaves strangely for no visible reason.

### P8 — four election/replication bugs, found by chaos testing and fixed here

Not designed in - found. The first chaos sweep (base seed 8, 15 trials, 5 nodes)
reported real data loss on trial 9: a client-acknowledged write vanished from every
node's log by the end of the run, with the trace bit-identical on replay. Chasing it
down turned up four separate, pre-existing bugs in `raft/node.py`'s timer and reply
handling - none of them specific to WAL persistence, crashes, or partitions. The
minimal repro for the worst of the first three needed no fault at all: `pyraft-lab
stress`'s own `baseline.yaml` scenario (30s, 3 nodes, zero faults, `expected:
availability: 1.0`) was failing it before this fix, at 120 term changes and 0.996
availability - nothing before Phase 8 had ever actually *run* that scenario
end-to-end and checked its `expected:` block against a real execution, since the
CLI's `run`/`stress`/`chaos` commands did not exist until this phase. All seven
shipped scenarios in `scenarios/*.yaml` now pass their `expected:` block for the
first time as a direct result of these fixes.

**1. A stepped-down leader kept acting like one.** `_on_append_reply` called
`handle_append_reply` (which already correctly no-ops when `state.role is not
Role.LEADER`, `replication.py:100`) but then unconditionally acted on `reply.success`
regardless of that check - retrying via `_replicate_to` even after this node had
stepped down (via `observe_term`, triggered by processing this very reply). A former
leader would keep sending `AppendEntries` carrying its now-current term, which peers
have no way to distinguish from a genuine leader's traffic. Fixed by adding the same
`role is not LEADER` guard `handle_append_entries` already had, right after the
`observe_term` early-return in `_on_append_reply`.

**2. A demoted leader inherited an unfairly short deadline.** `_run`'s loop only
resets `deadline` to a fresh, properly-randomized election timeout when `_dispatch`
returns `True` - and neither reply case (`RequestVoteReply`, `AppendEntriesReply`)
ever does, by the paper's own rule that a reply is not proof of a current leader.
That rule is right for an ordinary follower, but wrong for the one case a reply
handler can also cause: *this node was leader a moment ago and just stepped down*
inside handling this reply. Its `deadline` was still the leader's `now() +
heartbeat_interval` recompute from the top of this same loop iteration - roughly 50 ms
out, not a real 150-300 ms election timeout. A freshly-demoted leader would then race
back into candidacy well ahead of every other node's honest, randomized wait, winning
disproportionately often and producing exactly the "one node keeps winning, gets
deposed, wins again" oscillation the first chaos trace showed. Fixed: `_run` now also
treats *stepping down from leader as a side effect of this dispatch* as earning a
fresh election deadline, the same as `reset_timer` does.

**3. The leader's own heartbeat could starve under real traffic.** The worst of the
three, and the one that broke `baseline.yaml` with zero faults involved. `_run`
recomputed a leader's heartbeat deadline as `now() + heartbeat_interval` at the *top
of every loop iteration* - not anchored to when a heartbeat last actually went out.
Under any sustained traffic (client requests, replication acks flowing back from
followers - exactly what an active workload produces), some message arrives well
inside every 50 ms window, so the timeout branch that calls `_broadcast_heartbeat`
could go unreached for arbitrarily long. A follower that happened to already be caught
up - nothing new to reactively push to it via `submit`'s or `_on_append_reply`'s
"still behind" paths - could then go without hearing from its leader indefinitely,
time out, and start an election nobody else expected. Fixed with a `_next_heartbeat_at`
instance field that only ever advances when a heartbeat is actually broadcast (in
`_win_election` and in `_run`'s timeout branch), never by the mere arrival of
unrelated traffic - turning it into a real deadline instead of one perpetually pushed
out of reach.

**Why this survived seven phases of hand-written scenarios.** Every hand-written
scenario's fault timeline (`churn.yaml` and friends) is authored to "never [take] enough
[nodes] at once to lose quorum" and to heal or recover promptly - see their own
comments. That discipline is exactly what kept a demoted leader's unfair re-election
(#2) and reply-driven retries (#1) from mattering: with faults spaced out and quorum
never seriously contested, elections were rare enough that a slightly-unfair timeout
essentially never got exercised. Bug #3 needed sustained traffic and enough real time
for the "every message arrives inside 50 ms" condition to hold continuously, which a
30-second scenario with a steady `puts_per_sec` workload provides but nothing in
Phases 1-7's unit-level election/replication tests does - those construct a handful of
messages by hand and never run a real timer loop under load for long enough. Chaos's
open-ended random walk of faults, run for many trials, was the first thing to combine
"long enough," "busy enough," and "adversarial enough" all at once - which is the
whole reason a chaos campaign exists rather than only ever running curated scenarios.

### P8 — open issue: a possible 5th replication-safety bug, found but not yet fixed

Flagged rather than fixed, on a deliberate call to stop chasing an increasingly rare
case rather than open-endedly widen this phase's scope. **Exact repro:** `ChaosConfig
(nodes=5, trial_duration=12.0, trials=15, base_seed=8)`, trial index 10 (`chaos.
trial_seed(8, 10) == 16000034`) - reconstructible any time via `chaos._run_trial
(config, 10)`, or replayed straight from a persisted run once one exists (`pyraft-lab
chaos --nodes 5 --duration 12s --trials 15 --seed 8`, then `pyraft-lab replay
<the trial-10 run_id>`).

**What was confirmed before stopping:** the trial's client is told a write (`k2 =
"client1-24"`) succeeded; by the trial's end that entry is gone not just from every
*alive* node (the gap the broadened `_surviving_log_all_nodes`/`_divergent_nodes_all`
below exists to close) but from a node that was crashed-then-still-down at teardown
too - genuinely absent everywhere except one straggler whose divergent copy is itself
proof something went wrong, not evidence of safety. Instrumenting `advance_commit_index`
directly showed the leader (n3) recording `match_index = {n1: 27, n2: 27, n4: 26, n5:
3}` at the moment it committed index 27 - a textbook-valid 3-of-5 majority *by its own
bookkeeping*. But n2's log does not hold that entry by the run's end. Two explanations
remain open, and telling them apart needs tracing n2's actual `RaftLog` mutations
across the whole trial (not just its start/end state, the way the first four bugs
were each pinned down):

1. `match_index[n2] = 27` was itself wrong - a residual case of the same
   mis-crediting family as the fourth bug above, not fully closed by the single-
   in-flight guard (`_replicate_to`'s `_awaiting_reply`/`_reply_timeout`). Plausible:
   this trial stacks nine faults, including packet loss and repeated crash/recover
   cycles, on a five-node cluster within twelve seconds - considerably more
   adversarial than anything the fourth bug's own repro needed.
2. `match_index[n2] = 27` was accurate at the time, and a *later* leader won an
   election it should not have been able to win with a log that did not include
   index 27 - which would point at `raft/rpc.py`'s `handle_request_vote` or
   `raft/log.py`'s `candidate_log_is_up_to_date` instead, a different and more
   fundamental component than anything touched by this phase's other four fixes.

Whichever it is, it is real and worth fixing - just not something to chase further
within Phase 8's scope tonight. `tests/test_chaos.py`'s slow fuller-sweep test uses a
different base seed for exactly this reason (documented at that test), so the default
regression suite stays green while this stays open and reproducible on demand.

### P8 — chaos gets real WAL-backed crash/recovery; stress does not

**Decision:** `ExperimentRunner` gains one opt-in constructor argument, `wal_dir`.
When set, every node is built with a `WriteAheadLog` under that directory, and a
`recover` step replaces the crashed node's Python object outright with a brand-new
`RaftNode` built against a freshly reopened `WriteAheadLog` on the same path — so its
post-recovery state comes from nothing but what the WAL actually persisted. `chaos.py`
always sets it; `stress.py` never does; every existing caller (Phase 5's scenario
runner, Phase 7's replay of a non-persistent run, all pre-Phase-8 tests) passes
neither, and is byte-for-byte unaffected.

**Why:** this is the fulfillment of the forward reference P6 left standing — "the case
where persistence changes the answer is a restart that keeps its state... it belongs
to [Phase 8] rather than being pre-emptively bolted on here." Before this, a scenario's
`crash` step was a pause: `RaftNode.stop()` then `.start()` on the *same* object, so
in-memory log/term survived a "crash" untouched, and a chaos campaign built on that
would only be testing Raft's in-memory majority-commit guarantee — never the on-disk
recovery path Phase 6 built, which is exactly what a campaign whose invariant is
"committed data is never silently lost" ought to be exercising. Stress stays in-memory
deliberately: its job is coverage across many cheap trials, not durability, and disk
I/O on every trial would buy it nothing it is chartered to test.

### P8 — a trial's fault timeline is seeded from `(base_seed, trial_index)` alone

**Decision:** `stress.trial_seed`/`chaos.trial_seed` each derive one integer from the
campaign's base seed and a trial's index (different multipliers between the two
modules, so a shared base seed can never accidentally line up the same trial for
both), and every random choice a fault-kind builder or the chaos random-walk makes
comes from the one `random.Random` seeded that way — nothing in either module calls
the `random` module unseeded.

**Why:** the same discipline Phase 5 already relies on (one seed feeds every random
source in `ExperimentRunner`) extended one level up, to the campaign that decides
*which* scenario each trial is. Without it, `--seed` on `stress`/`chaos` would control
the run but not the fault selection, and a "failing trial" would not be a fixed,
reproducible scenario at all — defeating the "seed becomes a replay-able regression
case" property plan.md asks for.

### P8 — a recovered WAL-backed node reseeds by formula, not by continuing a stream

**Decision:** the rebuilt node's `rng` in `ExperimentRunner._new_node` is
`random.Random(self.seed + index)` — the exact expression `_build_cluster` used when
the cluster was first built — rather than whatever state the crashed node's `Random`
instance happened to be in.

**Why:** makes a rebuild a pure function of `(seed, node_id)`, independent of how many
draws happened before the crash. That is what lets Phase 7 replay reproduce a
persistent run exactly: replay reconstructs the runner from nothing but the manifest
and re-triggers the same fault timeline at the same simulated times, so reseeding by
formula on every rebuild — replay's included — always lands on the identical sequence.
Continuing a stream would make that depend on exactly how much randomness the crashed
incarnation had consumed, which is not itself part of the manifest.

### P8 — the safety invariant is lost writes and log divergence, computed explicitly

**Decision:** both campaigns compute `passed = not data_loss and not divergent_logs`
by hand from `metrics.report()`'s `summary.data_loss`/`summary.divergent_logs` fields
— never from `metrics.report()`'s own `document["passed"]`. Linearizability's verdict
is still recorded per trial, but informationally only.

**Why:** a real gap, found while wiring this up: `metrics.report()`'s `passed` folds in
a scenario's `expected:` block and `data_loss`, but never looks at `divergent_logs` —
so a run with diverged logs and no `expected:` block would report `passed: true`. It
is backward-compatible to leave as is (no scenario written before Phase 8 has ever
triggered log divergence), but it is exactly the kind of gap a chaos campaign exists to
find, so both `stress._run_trial` and `chaos._run_trial` fold divergence in themselves
rather than depend on the fix landing in `metrics.py`. Plan.md's invariant is also
specifically about lost writes — a narrower, always-decidable question next to
linearizability's occasional `UNPROVEN` — so gating a campaign's pass/fail on full
linearizability would fail runs where nothing was actually lost.

### P8 — a trial's WAL directory does not outlive the trial

**Decision:** `chaos._run_trial` opens its `wal_dir` as a `tempfile.TemporaryDirectory`
and lets it go when the trial ends, win or lose. Nothing about the raw WAL bytes is
kept in `results/`, even for a failing trial.

**Why:** replay already regenerates the same WAL content deterministically (the P8
entry above), so keeping it on disk would grow `results/` for a reproducibility
benefit replay already provides for free. A reviewer wanting the raw bytes for manual
post-mortem debugging is a real, reasonable want that this does not serve — a
`--keep-wal` escape hatch is a natural follow-up, not built here.

## Deferred

- **InstallSnapshot.** Listed in the specification's RPC table but assigned no day, and
  2.0 explicitly allows skipping network snapshot transfer. Declared deferred rather than
  half-built. P6 above names exactly what it would fix: a follower missing committed
  entries below a restarted leader's snapshot boundary.
- **A leader that self-demotes without a quorum.** A partitioned leader keeps believing
  it leads until it hears a higher term — correct per §5.1, and safe, since D10's lease
  already stops it serving reads and the majority rule already stops it committing. A
  step-down-on-lease-expiry rule would only change what `role` reports, so it is left
  out rather than added for cosmetics.
