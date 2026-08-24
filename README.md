# PyRaft Lab

PyRaft Lab is a from-scratch Raft implementation and a deterministic laboratory for
studying distributed systems. It combines a replicated key-value store with a
discrete-event network simulator, fault injection, persistence, experiment reporting,
and a live React workbench.

The project has two closely related layers:

1. **The system:** Raft state, leader election, log replication, a KV state machine,
   write-ahead logging, snapshots, and a leader-discovering client.
2. **The laboratory:** virtual time, simulated latency and packet loss, partitions,
   crash/recovery, YAML scenarios, metrics, linearizability checks, deterministic
   replay, stress campaigns, chaos campaigns, and benchmarking.

Experiments use a seeded `VirtualClock`, so time advances without waiting for wall
clock time and a persisted run can be reproduced later. The live cluster controller
uses the same Raft implementation with a `WallClock`.

## Features

- Raft leader election, heartbeats, log replication, conflict repair, and majority
  commit tracking.
- Replicated `PUT`, `GET`, `DELETE`, and compare-and-set operations.
- Linearizable reads through a leader lease and a committed no-op on election.
- Optional per-node WAL persistence with checksums, crash recovery, validation, repair,
  and snapshot creation with WAL compaction.
- Deterministic in-memory transport with configurable latency, loss, partitions, and
  fault timelines.
- Event tracing and live metrics for terms, leaders, elections, commits, replication,
  dropped packets, latency, availability, and recovery.
- CLI commands for live clusters, scenarios, replay, results, benchmarking, stress,
  chaos, WAL inspection, snapshot inspection, and the HTTP API.
- FastAPI server with a WebSocket event stream and a React + TypeScript workbench.

## Requirements

- Python 3.11 or newer
- Node.js and npm for the web frontend
- Windows, macOS, or Linux

The backend package is developed and tested against Python 3.11, 3.12, and 3.13.

## Installation

The Python package lives in `pyraft_lab/`, which is also the working directory for
the CLI, scenarios, generated results, documentation, and tests.

### Backend

From the repository root:

```bash
cd pyraft_lab
python -m venv .venv
```

Activate the environment before installing dependencies.

Windows PowerShell:

```powershell
.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -e ".[dev]"
```

macOS or Linux:

```bash
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e ".[dev]"
```

The `dev` extra includes the API dependencies, pytest, pytest-asyncio, Ruff, and
HTTP test utilities. For a runtime-only API installation, use `python -m pip install
-e ".[api]"` instead.

### Frontend

In another terminal, from the repository root:

```bash
cd frontend
npm install
```

The Vite development server proxies `/api` and `/api/stream` to
`http://127.0.0.1:8000`.

## Quick Start

### Run a live CLI cluster

From `pyraft_lab/`:

```bash
pyraft-lab init
pyraft-lab cluster start --nodes 3
```

The command opens an interactive session. Useful commands include:

```text
put color blue
get color
status

show-log
dashboard --refreshes 1
restart n2
exit
```

`cluster start` is an in-process controller. It owns the nodes and client for the
life of that command; there is no separate daemon that later CLI invocations address.
Use `--data-dir` to give each node a WAL-backed data directory and make restarts
rebuild state from disk.

### Run a scenario

```bash
pyraft-lab run --scenario scenarios/baseline.yaml
pyraft-lab run --scenario scenarios/leader_crash.yaml --plot results/leader-crash
pyraft-lab results
```

Each run is persisted under `results/<run_id>/` with a manifest, event trace, history,
and report. Plot output contains commit latency, leadership, replication, and
consistency/availability visualizations.

### Start the API and web workbench

Terminal 1, from `pyraft_lab/`:

```bash
pyraft-lab init
pyraft-lab serve
```

The API is available at `http://127.0.0.1:8000`; its interactive OpenAPI page is at
`http://127.0.0.1:8000/docs`.

Terminal 2, from `frontend/`:

```bash
npm run dev
```

Open `http://localhost:5173` and select **Start cluster**. The frontend is a live
workbench for the cluster, KV store, nodes, logs, trace, WAL and snapshots, fault
injection, scenarios, replay, results, stress, chaos, and benchmark campaigns.

The API process owns the cluster. Keep it running while using the frontend. Campaigns
run in worker threads so a virtual-clock experiment does not freeze the live cluster.

## CLI Reference

Run these commands from `pyraft_lab/` after installing the package:

```bash
pyraft-lab version
pyraft-lab kinds
pyraft-lab init
pyraft-lab cluster start --nodes 3
pyraft-lab run --scenario scenarios/<name>.yaml
pyraft-lab results
pyraft-lab replay <run_id> --results-dir results
pyraft-lab stress --nodes 5 --duration 20s --trials 40
pyraft-lab chaos --nodes 5 --duration 20s --trials 40
pyraft-lab benchmark --nodes 3,5,7 --loss 0,5,10,20 --duration 15s
pyraft-lab inspect-wal data/n1.wal
pyraft-lab inspect-wal data/n1.wal --repair
pyraft-lab inspect-snapshot data/n1-snapshots/snapshot-000000000002.json
pyraft-lab serve
```

Use `pyraft-lab <command> --help` for command-specific options. Generated results,
plots, benchmark reports, and demo output are intentionally not part of the source
package.

## Scenarios and Fault Injection

Scenario definitions are YAML files in `pyraft_lab/scenarios/`. The checked-in suite
covers:

| Scenario | Purpose |
| --- | --- |
| `baseline.yaml` | Fault-free control run |
| `leader_crash.yaml` | Leader failure, re-election, and recovery |
| `minority_partition.yaml` | A partitioned minority cannot commit |
| `majority_partition.yaml` | A surviving majority continues after isolating a leader |
| `network_latency.yaml` | Replication under configurable latency |
| `packet_loss.yaml` | Replication and retries under packet loss |
| `churn.yaml` | Repeated crash and recovery cycles |
| `consistency_availability.yaml` | A 2-vs-3 partition and its consistency/availability trade-off |

The scenario runner applies a virtual-clock timeline to real `RaftNode` instances. It
records events and evaluates the scenario's `expected:` block, including safety,
availability, recovery, and linearizability assertions. Faults can describe crashes,
partitions, healing, latency changes, loss bursts, and node churn.

## Replay, Stress, Chaos, and Benchmarking

### Deterministic replay

Every persisted scenario includes the seed, scenario configuration, runner settings,
and event trace needed for reproduction:

```bash
pyraft-lab replay <run_id> --results-dir results
```

Replay compares the new trace with the original field by field, including event
sequence numbers. A mismatch is reported as a result because loss of reproducibility
is itself useful diagnostic information.

### Stress and chaos

`stress` cycles through a bounded catalog of fault archetypes. `chaos` generates
randomized fault sequences and checks that committed data is never silently lost.
Both are multi-trial drivers over the same experiment runner, and failing trials are
persisted so their run IDs can be replayed.

### Benchmark

`benchmark` sweeps cluster sizes and packet-loss rates, then reports throughput,
latency percentiles, election time, recovery time, CPU, and memory. It writes a JSON
report and comparison plots. These measurements are laboratory observations from
single-process simulated clusters, not production capacity claims.

## Persistence Model

Persistence is opt-in. A node without a WAL remains in-memory; a WAL-backed node
records terms, votes, log entries, truncations, commits, and snapshots using
checksummed records. On restart, the node reconstructs its state from disk.

Snapshots are written and synced before the WAL is compacted. The standalone
inspection commands validate WAL and snapshot files without starting a cluster.
Network `InstallSnapshot` transfer is intentionally not implemented, so a follower
that falls behind a compacted snapshot boundary must be restored through an external
data operation.

## Development

Run the backend test suite from `pyraft_lab/`:

```bash
pytest
```

The suite includes unit, async integration, persistence, replay, linearizability,
scenario, API, and stress-oriented tests. The end-to-end integration test runs every
checked-in scenario against its expected results and exercises the CLI pipeline.

Useful quality checks:

```bash
ruff check src tests
python scripts/demo.py
```

The narrated demo runs the real CLI end to end, writes its artifacts under
`demo-output/`, and exits non-zero if any step fails. For the frontend:

```bash
cd ../frontend
npm run lint
npm run build
npm run preview
```

## Repository Layout

```text
Pyraft-Lab/
├── pyraft_lab/
│   ├── src/pyraft_lab/
│   │   ├── raft/             Raft state, RPCs, election, replication, WAL, snapshots
│   │   ├── kv/               Replicated key-value state machine and commands
│   │   ├── network/          Buses, simulated links, faults, and clocks
│   │   ├── cluster/          Live cluster manager, configuration, and lifecycle
│   │   ├── client/           Leader discovery, retries, workloads, and metrics
│   │   ├── experiments/      Scenarios, reports, plots, replay, stress, chaos, benchmark
│   │   ├── observability/    Events, tracer, and live metrics
│   │   ├── consistency/      Operation history and linearizability checking
│   │   ├── dashboard/        Terminal dashboard for a live cluster session
│   │   ├── api/              FastAPI routes, jobs, and WebSocket stream
│   │   └── cli/              `pyraft-lab` command entry point
│   ├── scenarios/            Checked-in YAML experiments
│   ├── results/              Persisted run output
│   ├── data/                 Example WAL and snapshot data
│   ├── docs/                 Architecture, plan, and technical report
│   ├── scripts/              Narrated end-to-end demo
│   └── tests/                Backend test suite
└── frontend/                 React + TypeScript Vite workbench
```

## Design Boundaries and Known Limitations

The project is intended for correctness experiments and education, not deployment as
a production distributed database. In particular:

- Transport is in-process; real network deployment, TLS, and authentication are out
  of scope.
- Multi-Raft sharding, dynamic membership changes, and Byzantine fault tolerance are
  out of scope.
- Pre-vote is not implemented. A long-lived isolated server can increase its term and
  may cause a liveness problem after healing.
- The live cluster is single-process and has no daemon or external service manager.
- Benchmark results depend on the workload, seed, Python version, and host machine.

Read the technical report for measured results, discovered bugs, and the current open
correctness question. Read the architecture document for the reasoning behind the
transport, clock, persistence, read-lease, replay, and dashboard decisions.

## Documentation

- [Architecture and design decisions](pyraft_lab/docs/architecture.md)
- [Combined implementation plan](pyraft_lab/docs/plan.md)
- [Technical report and measured results](pyraft_lab/docs/technical_report.md)
- [Backend usage notes](pyraft_lab/README.md)
- [Frontend usage and page guide](frontend/README.md)

## License

MIT. See [pyraft_lab/LICENSE](pyraft_lab/LICENSE).
