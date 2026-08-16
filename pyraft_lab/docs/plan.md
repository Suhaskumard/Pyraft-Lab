# PyRaft Lab — Combined Implementation Plan (v2)

Merges the original [`PyRaft_Lab_15_Day_Implementation_Plan.pdf`](../PyRaft_Lab_15_Day_Implementation_Plan.pdf)
with the upgrade proposal [`PyRaft Lab 2.0.pdf`](../PyRaft%20Lab%202.0.pdf). This file is
the plan of record going forward — `README.md`'s status table points here. The two PDFs
remain as historical record of the design intent behind each phase.

## How the two documents were reconciled

- The original plan is the spine: Raft correctness first (Phases 0–3), then the
  laboratory built around it (Phases 4–5), then ship. 2.0 says explicitly to *keep the
  existing architecture* — it doesn't touch that spine, so Phases 0–5 below are
  unchanged in substance from the original.
- 2.0's own priority tiers (Must Have / Strong Upgrade / If Time Remains) answer "in
  what order do the twenty upgrade items get built?" They're folded in as Phases 6–12,
  gated behind the original's completion — a feature never gets built ahead of the
  correctness it depends on.
- 2.0 item 16 ("What NOT to Add") carries forward unchanged as this project's scope
  boundary.
- A few places where the two documents genuinely disagree are resolved explicitly below
  rather than left for whoever implements a phase to guess at.

## Resolved conflicts between the two source documents

1. **CLI list mismatch.** The original names `run / show-log / status / compare /
   serve`; 2.0 names a longer, different set (`init`, `cluster start`, `cluster
   status`, `run`, `stress`, `chaos`, `node inspect`, `node logs`, `trace`, `replay`,
   `benchmark`, `compare`, `snapshot`). 2.0 is additive to the original's
   `start/put/get/status/show-log/run/results`, not a replacement — **the CLI target is
   the union of both**, and each command is introduced only once the feature behind it
   is real (Phase 0's rule: a command that can't yet be answered honestly stays out of
   the CLI, it doesn't get stubbed).
2. **`lab/` vs `experiments/`.** The original names the laboratory package `lab/`;
   2.0's target tree renames it `experiments/` and splits `consistency/`
   (history + linearizability) and `observability/` (events + tracer + live metrics)
   out of it. `lab/` is currently an empty stub nothing imports by name, so **the rename
   happens for free, now** — see "Target directory structure" below.
3. **Two `metrics.py` files.** 2.0's tree implies both a laboratory `metrics.py` (the
   original's Day 12 scope) and `observability/metrics.py`. Both are kept, and they are
   different things: `experiments/metrics.py` computes **post-hoc** summaries from a
   finished run's event trace (p50/p95/p99, election count, replication lag — Day 12).
   `observability/metrics.py` holds **live** counters that the dashboard and tracer read
   while a run is in progress. Recorded once here so it isn't re-derived per phase.
4. **`network/link.py` vs `network/channel.py`.** Same concept, different name across
   the two documents. The original's `link.py` wins, since it's already referenced in
   `docs/architecture.md`'s decision log. 2.0's `faults.py` is added alongside it as a
   genuinely new file — fault *definitions* (`faults.py`) versus fault *application*
   (`simulator.py`, using `link.py`) is a real split the original didn't have. Built
   this way in Phase 3: `link.py` holds one link's characteristics and knows how to
   sample them, `simulator.py` applies them to live traffic, `faults.py` names
   composable fault descriptions that apply themselves to a simulator.

## Target directory structure

```
src/pyraft_lab/
  raft/            node.py state.py log.py rpc.py election.py replication.py
                   persistence.py snapshot.py
  kv/              store.py commands.py
  network/         bus.py codec.py messages.py simulator.py link.py faults.py clock.py
  cluster/         manager.py config.py lifecycle.py          (new, Phase 9)
  client/          client.py workload.py
  experiments/     runner.py scenario.py metrics.py visualization.py stress.py chaos.py
                   (renamed from lab/; stress.py/chaos.py new, Phase 8)
  observability/   events.py tracer.py metrics.py             (new, Phase 4)
  consistency/     history.py linearizability.py              (new, split from lab/, Phase 5)
  dashboard/       app.py                                     (new, Phase 11, if time remains)
  cli/             main.py + subcommand modules as they accrete
scenarios/
results/
docs/
tests/
```

## Phases

### Phase 0 — Foundation (Day 1) — done

Package skeleton, `Message`/`Envelope`, `JsonCodec`, `InMemoryBus`, CLI shell
(`version`, `kinds`), CI. 40 tests.

### Phase 1 — Raft Core (Days 2–4) — done

- **Day 2 — State & RPC** (done): `RaftState`, `Role`, `RaftLog`/`LogEntry`,
  `RequestVote(Reply)`/`AppendEntries(Reply)`, pure receiver handlers. 38 tests.
- **Day 3 — Leader Election** (done): randomized election timeout, candidate
  transition, self-vote, RequestVote broadcast, majority tally, split-vote retry,
  higher-term step-down, leader transition, heartbeat scheduling. `raft/election.py`
  (pure logic) + `raft/node.py` (the async driver that owns the timers and the
  message reactor loop).
- **Day 4 — Log Replication** (done): `raft/replication.py` — `append_command`,
  `build_append_entries` (batched, and a caught-up follower yields the empty
  AppendEntries that *is* the heartbeat), `handle_append_reply` (`nextIndex`
  decrement-and-retry), `advance_commit_index` (majority commit, with §5.4.2's
  current-term restriction). Driven from `node.py` via `RaftNode.submit`.
- Phase acceptance (from the original plan): 3-node cluster elects exactly one leader
  per term; leader crash triggers a new election; 100 commands replicate consistently;
  conflicting follower logs are repaired; no committed entry is ever double-applied or
  replaced at the same index.

### Phase 2 — KV Store & Client (Days 5–6) — done

- **Day 5 — State machine** (done): `kv/commands.py` (`Put`/`Get`/`Delete`/`Cas` as
  `kind`-tagged wire dicts, since Raft types `command` as opaque `Any` and would
  otherwise lose the class on the round trip) and `kv/store.py` (`KVStore`: applies only
  committed entries, in index order, exactly once, tracking `last_applied`). Driven from
  `RaftNode._apply_committed`.
- **Day 6 — Client** (done): `client/client.py` — `KVClient` with leader discovery,
  `NOT_LEADER` redirects, exponential backoff (`RetryPolicy`), and `ClientMetrics`
  counting attempts/successes/failures/redirects/timeouts plus availability. Nodes
  answer a write only once its entry has committed *and* applied.

**Known limitation, recorded rather than hidden:** a client retry after a leader change
can re-submit a command the old leader had already replicated, so a non-idempotent
command (CAS) could apply twice. Raft's answer is client session IDs with a dedup table;
that is not in either source document's scope, and exactly-once client semantics are not
among the Definition-of-Done items. Phase 5's linearizability checker will surface it if
it ever bites.

### Phase 3 — Failure Handling (Days 7–10) — done

- **Day 7 — Failure detection & recovery** (done): heartbeats and stale-leader
  step-down were already built into Day 3's `node.py`; this day exercises them under
  hard crash/rejoin — a *replacement* `RaftNode` under the same id with no log, term or
  vote, not just a clean stop — plus majority-loss unavailability and recovery.
- **Day 8 — Simulated network & virtual clock** (done): `network/clock.py`
  (`Clock` protocol, `WallClock`, `VirtualClock`), `network/link.py` (`LinkConfig`:
  latency range + independent loss), `network/simulator.py` (`SimulatedNetwork`,
  a drop-in `Transport`), `network/faults.py` (named, composable fault descriptions
  ready for Phase 5's YAML timeline). `RaftNode` now takes an injected `Clock`.
- **Day 9 — Partitions** (done): `SimulatedNetwork.partition`/`heal`, with groups
  assigned per node and unlisted nodes reaching everyone. Verified end to end: a
  minority makes no progress, the majority keeps committing, a minority leader may
  append but never commit, and healing reconciles both sides without losing anything
  the majority committed.
- **Day 10 — Linearizable reads** (done): a leader lease rather than ReadIndex, plus a
  no-op entry committed on election. See `docs/architecture.md` D10 for why the lease
  won over ReadIndex, and what it costs.
- Phase acceptance: reads never serve a value a newer leader has already replaced;
  a partitioned leader refuses to answer rather than answering stale; the cluster is
  unavailable-but-safe on majority loss and recovers when a quorum returns.

### Phase 4 — Observability Foundation (new — pulled forward from 2.0 item 4, plus the live half of item 2) — done

Pulled ahead of the laboratory phase because Phase 7 (replay), Phase 8 (stress/chaos
reports), and Phase 11 (dashboard) all consume an event trace that has to already exist.

- `observability/events.py` (done): `EventKind` — exactly 2.0 item 4's twelve kinds,
  fixed and not extended; anything finer-grained goes in an event's `details`. `Event`
  is frozen and carries `(timestamp, seq, node, term, details)`, serializing to the
  specification's JSON shape, with JSONL read/write for Phase 5 and Phase 7.
- `observability/tracer.py` (done): one `Tracer` per run, shared by every node and the
  network, stamping events from the same injected `Clock` the timers run against — so a
  trace taken under a `VirtualClock` is as reproducible as the run. Live `subscribe`,
  plus `of_kind`/`for_node`/`in_term`/`first`/`last` for reading a finished run.
  Recording is opt-in: `NULL_TRACER` drops an event before touching a clock.
- `observability/metrics.py` (done): `LiveMetrics`, a running fold of the stream —
  term, leader, per-node commit index, who is down, partition state, vote and append
  counts, election durations, and a one-word cluster `status`. Derived from events
  alone, never from a `RaftNode`. Post-hoc percentiles remain Phase 5's job.
- Instrumented: `RaftNode` emits the eight node-level kinds, `SimulatedNetwork` the four
  it alone can observe. See `docs/architecture.md` P4 for why the split falls there,
  and why the pure modules stay untouched.
- No dashboard yet — that's Phase 11, which reads `LiveMetrics.snapshot()`.

### Phase 5 — Laboratory Core (Days 11–13, plus 2.0 items 6 and 8) — done

YAML scenario runner (`experiments/runner.py`, `scenario.py`) upgraded with 2.0's
chained fault-timeline DSL (item 8: `at:`/`action:` sequences — a superset of the
original's flat `faults:` list, since a single-fault list is just a length-1 chain).
Metrics/visualization (Day 12, unchanged). `consistency/history.py` +
`consistency/linearizability.py` (Day 13) upgraded per 2.0 item 6 to explain *why* a
check failed — offending operations, node, term, timestamp, a heuristic cause — not
just PASS/FAIL. Seven required scenarios: `baseline`, `leader_crash`,
`minority_partition`, `majority_partition`, `network_latency`, `packet_loss`, `churn`.

### Phase 6 — Persistence Upgrade (2.0 items 9–10) — done

WAL (`raft/persistence.py`) with per-record checksums, crash recovery, and log
validation on restart (the original Day 7 persistence scope, upgraded per 2.0 item 9).
A corrupted-WAL-recovery test as an explicit edge case. Snapshots (`raft/snapshot.py`)
promoted from the original's "deferred" to a real feature per 2.0 item 10:
log-growth threshold → KV snapshot → persist → compact WAL. No network snapshot
transfer (2.0 explicitly allows skipping that under time pressure).

As built:

- `raft/persistence.py` (done): `WriteAheadLog` — one record per line, CRC32 over a
  canonical-JSON payload, six kinds (`header`/`term`/`entry`/`truncate`/`commit`/
  `snapshot`). `RaftLog` records its own appends and truncations through a `LogJournal`;
  `RaftNode` writes the term/vote pair and the commit index, and `_send` syncs before
  anything leaves the node — "nothing this node has told anyone can be forgotten".
  `read_wal` validates and folds: a torn final record recovers, corruption with valid
  records behind it refuses unless explicitly repaired, and the fold enforces what a
  checksum cannot (no holes, no term going backward, no commit past the end).
- `raft/snapshot.py` (done): `Snapshot`/`SnapshotMeta` (checksummed payload, verified on
  load), `SnapshotStore` (one file per snapshot, atomic write, prune), `SnapshotPolicy`
  (applied-entries threshold, or WAL bytes). `RaftNode.take_snapshot` runs 2.0's flow in
  crash-safe order: save and sync the snapshot, *then* compact the WAL against it.
- `RaftLog` gains a base index, because a log recovered from a compacted WAL genuinely
  starts partway along; it plays the sentinel's exact role in the consistency check, and
  `build_append_entries` clamps `prevLogIndex` to it. Compaction itself is on-disk only —
  see `docs/architecture.md` P6 for why, and for the one follower InstallSnapshot would
  otherwise be needed for.
- CLI: `pyraft-lab inspect-wal <path>` (prints 2.0 item 9's restart report, `--repair`,
  `--records`) and `pyraft-lab inspect-snapshot <path>`. 2.0's `--node` forms of these,
  and `snapshot --node`, need a cluster to address and arrive with Phase 9.
- Persistence is opt-in throughout: a node built without a WAL behaves exactly as it did
  before this phase, which is why Phase 5's runner is untouched (architecture P6).

### Phase 7 — Deterministic Replay (2.0 item 5) — done

Every run persists a manifest (`run_id`, `random_seed`, `scenario`, network config,
fault timeline, workload, event trace). `pyraft-lab replay --run <id>` reconstructs the
same run from that manifest and the Phase 4 event trace. Depends on Phase 4 (events)
and Phase 5 (runner) both existing.

As built:

- `experiments/manifest.py` (done): `RunManifest` - a scenario's `to_dict()` plus the
  three `ExperimentRunner` constructor knobs a scenario file does not itself carry
  (`election_timeout_range`, `heartbeat_interval`, `client_timeout`). Network
  configuration and the fault timeline need no separate fields - a scenario's
  `faults:` list already is the fault timeline, and every network setting that changes
  during a run arrives as a timeline step. See `docs/architecture.md` P7.
- `experiments/replay.py` (done): `persist_run` writes one directory per run
  (`manifest.json`, `events.jsonl`, `history.json`, `report.json` - the last three
  already Phase 4/5 formats, reused rather than reinvented). `replay_run` rebuilds an
  `ExperimentRunner` from the manifest, runs it again, and diffs the new event trace
  against the persisted one field-for-field (including `seq`), returning a
  `ReplayReport` rather than raising on a mismatch - a run that stops reproducing is a
  finding, not a tooling error.
- CLI: `pyraft-lab replay <run_id> [--results-dir results]`. There is deliberately no
  `pyraft-lab run` yet to produce persisted runs with - the full CLI surface plan.md
  assigns to Phase 9, and `persist_run` is fully usable from the Python API in the
  meantime, exactly as Phase 5's scenario runner has been all along.
- A genuine pre-existing bug surfaced while building the first replay tests: cancelling
  a `race_deadline` loser on a `VirtualClock` left its timer leaking in the clock's
  heap forever, and a busy scenario run could accumulate tens of thousands of them,
  crawling forward in sub-millisecond steps instead of finishing. Fixed in
  `network/clock.py` - see `docs/architecture.md` P7 - with a regression suite in
  `tests/test_clock.py`. Predates Phase 7 (it lived in Day 8's code); nothing surfaced
  it until replay needed a run to finish twice, reliably, in a test.
- `experiments/scenario.py` and `experiments/runner.py` had no direct tests before this
  phase despite being Phase 5 deliverables; `tests/test_scenario.py` and the
  replay-focused `tests/test_replay.py` are the first coverage either has had.

### Phase 8 — Stress & Chaos Campaigns (2.0 items 7, 13)

`pyraft-lab stress --nodes N --duration Ns`: auto-generated fault combinations across N
trials, `stress-report.json` (trials/passed/failed/data-loss/linearizability/recovery
percentiles). `pyraft-lab chaos --nodes N --duration Ns`: randomized fault sequence
checking the same safety invariant every trial — **committed data is never silently
lost** — reported as `CHAOS TEST REPORT`. Both are thin drivers over Phase 5's runner
plus Phase 7's replay (a failing trial's seed becomes a `replay`-able regression case
for free).

### Phase 9 — Cluster Controller & CLI Upgrade (2.0 items 2, 14)

`cluster/manager.py` (`ClusterManager`, `NodeManager`), `config.py`
(`ClusterConfig`), `lifecycle.py` (`LifecycleManager`). CLI: `cluster start --nodes N`,
`cluster stop`, `cluster restart <node>`, `cluster status`, `cluster topology`, plus the
rest of 2.0 item 14's surface (`init`, `node inspect`, `node logs`, `trace`) layered
onto the original's `put/get/status/show-log/run/results`.

### Phase 10 — Benchmarking & Consistency/Availability Demo (2.0 items 11–12) — done

`pyraft-lab benchmark`: throughput/latency percentiles/election/recovery/CPU/memory
across {3,5,7} nodes × {0,5,10,20}% loss, with comparison graphs generated
automatically (extends Phase 5's existing result schema rather than being a new
subsystem, per 2.0's own note). The "Consistency vs Availability" partition demo (item
12) is a named scenario built entirely from Phase 3 + Phase 5 primitives — no new code,
just a scenario file and a presentation-oriented plot.

As built:

- `experiments/benchmark.py` (done): `BenchmarkConfig`/`BenchmarkCell`/`BenchmarkReport`
  and `run_benchmark` sweep the grid as twelve ordinary `ExperimentRunner` runs, each
  read back through the same `metrics.report()` every other command uses — no new
  report schema, per 2.0's own note. Every cell carries a constant packet-loss rate for
  its duration (skipped at 0%) plus one leader crash/recovery, so `election_time_ms`
  and `recovery_time_ms` are never empty. CPU/memory are sampled with stdlib
  `time.process_time()`/`tracemalloc` around the whole cell, not per simulated node —
  see docs/architecture.md P10 for why per-node has no honest referent here and why
  that avoids a new dependency (`psutil`).
- `experiments/visualization.py` gains `plot_benchmark` (four comparison graphs —
  throughput, latency P50/95/99, election/recovery time, CPU/memory — each vs packet
  loss with one line per cluster size) and `plot_consistency_availability` (item 12's
  picture: every node's commit index over time plus every operation's outcome). The
  latter is generic, not scenario-specific, and is now the fourth plot `plot_run`
  always writes.
- CLI: `pyraft-lab benchmark [--nodes ...] [--loss ...] [--duration ...]` writes
  `benchmark-report.json` and the four graphs. `pyraft-lab run` gains `--plot <dir>`,
  which is also what finally wires `visualization.py` into the CLI at all — it shipped
  in Phase 5's Day 12 scope but nothing before this phase ever called it.
- `scenarios/consistency_availability.yaml` (new): a 5-node, 2-vs-3 partition, item
  12's own diagram. Its partition window is 3 seconds, not the 12–15s the other
  partition scenarios use — found while building this scenario's plot: a longer
  isolation window can livelock the majority after healing, a real, general Raft
  liveness gap (Figure 2's disruptive-server exposure) rather than anything specific to
  this scenario. Flagged rather than fixed, in docs/architecture.md P10 — the fix is
  protocol-level work (pre-vote), out of scope for a benchmarking phase.

### Phase 11 — Local Dashboard (2.0 item 3) — if time remains

Terminal-based live view (not a web app, per item 16's boundary) reading Phase 4's live
metrics: cluster/term/leader/health header, per-node role row, commit/applied/log
length, election time, p99 latency, availability. `dashboard/app.py`.

### Phase 12 — Documentation, Final Architecture Pass, Ship (Days 14–15 + 2.0 item 15)

Directory reorganization to the "Target directory structure" above is finalized — done
incrementally per-phase rather than as a big-bang Day 14 change, since each phase above
already creates its files in the target location. README, architecture doc, technical
report, final 7+-scenario benchmark suite, full integration suite, demo, `v0.1.0` tag.

## Explicitly out of scope (2.0 item 16, unchanged)

Multi-Raft sharding, TLS/authentication, real network deployment, Byzantine fault
tolerance, cluster membership changes, web UI as a core requirement.

## Testing rule (unchanged from the original)

Every phase ends with automated tests; do not proceed with known-failing core tests.
Determinism — seeded randomness, and the virtual clock from Phase 3 onward — is what
keeps Phase 7's replay and Phase 8's campaigns meaningful. A flaky test here is a design
bug, not bad luck.
