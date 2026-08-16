# PyRaft Lab

A from-scratch Raft consensus implementation in Python, wrapped around a replicated
key-value store, with a deterministic fault-injection network simulator and an
experiment runner that measures election time, commit latency, replication lag,
recovery time and linearizability under failure.

Two layers:

1. **The system** — Raft + KV + WAL + snapshots
2. **The laboratory** — fault injection + experiments + analytics

## Status

Built phase by phase against [docs/plan.md](docs/plan.md) — the combined plan of record,
merging the original 15-day plan with the PyRaft Lab 2.0 upgrades. Current state:

| Phase | Status |
| --- | --- |
| 0 — Foundation | **done** |
| 1 — Raft core | **done** — state, log, RPCs, leader election, log replication |
| 2 — KV & client | **done** — PUT/GET/DELETE/CAS state machine, leader-discovering client |
| 3 — Failure handling | **done** — crash recovery, simulated network, partitions, linearizable reads |
| 4 — Observability foundation | **done** — event vocabulary, run tracer, live metrics |
| 5 — Laboratory core | **done** — scenario runner, fault timelines, metrics, linearizability |
| 6 — Persistence & snapshots | **done** — checksummed WAL, crash recovery, snapshots + compaction |
| 7 — Deterministic replay | **done** — run manifests, `pyraft-lab replay`, bit-identical trace diffing |
| 8 — Stress & chaos | **done** — auto-generated stress campaigns, randomized chaos campaigns, both replay-able |
| 9 — Cluster controller & CLI | **done** — live in-process cluster with an interactive session, `init`/`run`/`results` |
| 10 — Benchmarking | **done** — `pyraft-lab benchmark` across node counts × packet loss, comparison graphs, `--plot` for any run |
| 11 — Dashboard | **done** — `dashboard` command inside `cluster start`: live term/leader/health, per-node role, commit/applied/log length, election time, p99 latency, availability |
| 12 — Docs & ship | **done** — directory layout finalized, full integration suite, [technical report](docs/technical_report.md), narrated demo, `v0.1.0` |

Nothing in this repo mocks a Raft mechanism or hard-codes a metric. Commands that
cannot yet be answered honestly are absent from the CLI rather than stubbed.

See [docs/technical_report.md](docs/technical_report.md) for the full write-up:
benchmark numbers, every bug the laboratory actually found while building this, and
what is knowingly left open.

## Install

Requires Python 3.11+.

```bash
pip install -e ".[dev]"
```

## Run the tests

```bash
pytest
```

`tests/test_integration.py` is the full end-to-end suite: every file in `scenarios/`
run for real against its own `expected:` block, and the CLI pipeline (`init` -> `run
--plot` -> `results` -> `replay`, plus a WAL-backed cluster surviving across two
independent `cluster start` invocations) chained the way a user actually would.

## Demo

```bash
python scripts/demo.py
```

Runs the whole system end to end through the real CLI - scaffold a project, run three
scenarios with plots, drive a live 3-node cluster interactively (put/get/status/
topology/dashboard/restart), then compare cluster sizes under packet loss - and prints
every command and its output as it goes. Output lands under `demo-output/` (not
committed).

## CLI

```bash
pyraft-lab version                    # installed version
pyraft-lab kinds                      # message kinds this build can put on the wire
pyraft-lab inspect-wal <path>          # validate a WAL, print what a restart recovers
pyraft-lab inspect-wal <path> --repair # discard from the first corrupt record onward
pyraft-lab inspect-snapshot <path>     # verify and print a snapshot's metadata
pyraft-lab replay <run_id>             # re-run a persisted run and diff its trace
pyraft-lab stress --nodes N --duration Ns  # N auto-generated fault combinations, stress-report.json
pyraft-lab chaos --nodes N --duration Ns   # randomized fault sequences, a CHAOS TEST REPORT
pyraft-lab init                        # write a default cluster.yaml; ensure scenarios/, results/
pyraft-lab cluster start --nodes N     # build a real cluster; enter an interactive session
pyraft-lab run --scenario <path>       # run one scenario file, persist it, print a summary
pyraft-lab run --scenario <path> --plot <dir>  # also write its latency/leadership/replication/consistency plots
pyraft-lab results                     # list persisted runs, newest first
pyraft-lab benchmark                   # compare {3,5,7} nodes x {0,5,10,20}% loss, graphs + benchmark-report.json
```

## Deterministic replay

Every scenario run is reproducible from its seed: one seed feeds every random source
in `ExperimentRunner`, and time only moves because the runner advances a
`VirtualClock`. `persist_run` writes what `replay_run` needs to prove it:

```python
from pyraft_lab.experiments.runner import ExperimentRunner
from pyraft_lab.experiments.replay import persist_run, replay_run

result = await ExperimentRunner(scenario).run()
persist_run(result, "results")             # results/<run_id>/{manifest,events,history,report}

comparison, replayed = await replay_run("results", result.run_id)
print(comparison.explain())                # "MATCH (N events, bit-identical)", or a diff
```

```bash
pyraft-lab replay <run_id> --results-dir results
```

A mismatch is reported, not raised - a run that stops reproducing is itself a finding.

## Persistence

Opt-in, the same way tracing is: a `RaftNode` built without a `WriteAheadLog` behaves
exactly as it did before Phase 6.

```python
wal = WriteAheadLog("data/n1.wal", node_id="n1")
snapshots = SnapshotStore("data/n1-snapshots")
node = RaftNode("n1", ids, transport, wal=wal, snapshots=snapshots)
# __init__ recovers term, vote, log and commit index from the WAL before the node runs.
node.take_snapshot()  # by hand, or automatically once SnapshotPolicy's threshold is due
```

A crash is recovered by building the same `RaftNode` again against the same files.
`pyraft-lab inspect-wal` and `inspect-snapshot` read either one without a node at all.

## Stress & chaos campaigns

Both are many-trial drivers over the same `ExperimentRunner`, and both persist a
failing trial so its `run_id` is directly usable with `pyraft-lab replay`.

`stress` picks one entry per trial from a bounded catalog of fault archetypes (crash
the leader, crash a follower, isolate a minority or the leader alone, a latency spike,
a packet-loss burst, churn, or a combination), cycled by trial index so N trials sweep
every archetype. Trials run in memory — coverage across many cheap trials is the point.

```bash
pyraft-lab stress --nodes 5 --duration 20s --trials 40
# stress-report.json: trials/passed/failed/data-loss/linearizability/recovery percentiles
```

`chaos` runs one open-ended randomized fault sequence per trial — no catalog, no
attempt to keep a quorum alive — and checks a single invariant every time: **committed
data is never silently lost**. Every trial runs with a real per-node WAL, so a crash
truly discards a node's in-memory state and recovery rebuilds it from nothing but disk.

```bash
pyraft-lab chaos --nodes 5 --duration 20s --trials 40
# prints a CHAOS TEST REPORT and writes chaos-report.json
```

## Cluster controller

`cluster start` builds a real, live cluster — real `RaftNode`s, real election timers,
a `WallClock` — in this one process, and hands control to an interactive session.
There is no daemon behind it: `put`/`get`/`status`/`show-log`/`node inspect`/
`node logs`/`trace`/`restart`/`topology`/`dashboard` are commands typed *inside* that
session, not separate `pyraft-lab` invocations, because there is nothing else running
for a later command to address (see [docs/architecture.md](docs/architecture.md) P9).

```bash
pyraft-lab init
pyraft-lab cluster start --nodes 3
```
```
pyraft-lab> put color blue
ok: color = blue
pyraft-lab> get color
color = blue
pyraft-lab> status
status: HEALTHY
term: 1   leader: n2
pyraft-lab> topology
node    alive  role       term  leader
n1      True   follower   1     n2
n2      True   leader     1     n2
n3      True   follower   1     n2
pyraft-lab> restart n2
restarted n2
pyraft-lab> exit
```

Pass `--data-dir` to persist every node's state to a WAL, so a `restart <id>` rebuilds
that node purely from disk rather than reusing its in-memory object. `--config` reads
a hand-edited `cluster.yaml` (written by `init`) for the settings that aren't CLI
flags — election timeout range, heartbeat interval, client timeout.

## Live dashboard

`dashboard` (typed inside a `cluster start` session, same as `status`/`topology`) is
a live, terminal-based view of that same cluster — not a separate command, since
there is nothing outside the session for one to address. Runs until Ctrl+C, or for a
fixed number of frames with `--refreshes`.

```
pyraft-lab> dashboard --refreshes 1
-- refresh 1 --
PyRaft Lab Dashboard
+---------------------------------+
| Cluster: 5 nodes        Term: 1 |
| Leader: n2      Status: HEALTHY |
+---------------------------------+

n1       n2       n3       n4       n5
Follower LEADER   Follower Follower Follower

Commit Index:   2
Applied:        2
Log Entries:    2

Election Time:  1ms
P99 Latency:    2ms
Availability:   100.00%

Elections:      1
Dropped pkts:   0  (loss 0, partition 0)
```

Plain ASCII throughout and no cursor-control escapes — each frame prints in full and
the terminal scrolls, rather than risking a Windows console that can't render 2.0's
own box-drawing mock-up or doesn't support ANSI clear-screen (see
[docs/architecture.md](docs/architecture.md) P11). P99 latency and availability come
from whatever traffic has actually gone through the session's own client — nothing
synthetic is injected to fill them in, so they read `n/a` until something has.

## Running a scenario

`run` is `stress`/`chaos`'s single-scenario sibling: it runs one YAML file through the
same `ExperimentRunner` and persists it through the same Phase 7 machinery, so its
`run_id` works with `replay` immediately. `results` lists what has accumulated.

```bash
pyraft-lab run --scenario scenarios/leader_crash.yaml
pyraft-lab results
```

Pass `--plot <dir>` to also write that run's plots — latency, leadership, replication,
and a "consistency vs availability" picture (every node's commit index over time, and
every operation's outcome) that's generic to any run, not only a partitioned one.

```bash
pyraft-lab run --scenario scenarios/consistency_availability.yaml --plot results/demo
```

That scenario is the "Consistency vs Availability" demo (2.0 item 12): a 5-node
cluster split 2-vs-3. The minority side holds no majority and cannot commit while cut
off; the majority side keeps serving; healing reconciles both without losing a write —
exactly what the `-consistency.png` plot shows happening.

## Benchmarking

`pyraft-lab benchmark` compares throughput, latency (P50/P95/P99), election time,
recovery time, CPU and memory across cluster sizes and packet-loss rates (2.0 item 11).
Each `(nodes, loss%)` cell is one ordinary scenario run over the same `ExperimentRunner`
every other command uses, read back through the same `metrics.report()` — see
[docs/architecture.md](docs/architecture.md) P10 for how CPU/memory are sampled and why
every cell includes a leader crash.

```bash
pyraft-lab benchmark --nodes 3,5,7 --loss 0,5,10,20 --duration 15s
# benchmark-report.json, plus four comparison graphs in results/benchmark/
```

## Tracing a run

Recording is opt-in — a node built without a tracer records nothing. Hand one to the
nodes and the network and the run narrates itself:

```python
tracer = Tracer()                      # or Tracer(clock) to share a VirtualClock
metrics = LiveMetrics(tracer)          # a running fold, readable at any moment
net = SimulatedNetwork(tracer=tracer)  # partitions and node loss
node = RaftNode("n1", ids, net, tracer=tracer)
...
metrics.snapshot()                     # term, leader, status, commit indexes, timings
tracer.write_jsonl("results/run.jsonl")
print(tracer.story())                  # the same trace, one line per event
```

## Layout

```
src/pyraft_lab/
  raft/           consensus: state, RPCs, election, replication, persistence
  kv/             replicated key-value state machine
  network/        transports: the in-memory bus and the fault simulator, plus the
                  virtual clock, link characteristics and fault definitions
  cluster/        cluster manager, config and lifecycle
  client/         leader-discovering client and workload generators
  experiments/    scenario runner, metrics, plots, stress/chaos/benchmark campaigns
  observability/  event vocabulary, tracer, live metrics
  dashboard/      the live terminal view a `cluster start` session can open
  consistency/    history recording and linearizability checking
  cli/            the pyraft-lab command line
scenarios/   YAML experiment definitions
results/     generated experiment output (not committed)
docs/        architecture notes and the technical report
tests/       unit, integration and property tests
```

See [docs/architecture.md](docs/architecture.md) for the design decisions and the
reasoning behind them.

## License

MIT — see [LICENSE](LICENSE).
