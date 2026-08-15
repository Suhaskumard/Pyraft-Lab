# PyRaft Lab

A from-scratch Raft consensus implementation in Python, wrapped around a replicated
key-value store, with a deterministic fault-injection network simulator and an
experiment runner that measures election time, commit latency, replication lag,
recovery time and linearizability under failure.

Two layers:

1. **The system** — Raft + KV + WAL
2. **The laboratory** — fault injection + experiments + analytics

## Status

Built phase by phase against [docs/plan.md](docs/plan.md) — the combined plan of record,
merging the original 15-day plan with the PyRaft Lab 2.0 upgrades. Current state:

| Phase | Status |
| --- | --- |
| 0 — Foundation | **done** |
| 1 — Raft core | **done** — state, log, RPCs, leader election, log replication |
| 2 — KV & client | **done** — PUT/GET/DELETE/CAS state machine, leader-discovering client |
| 3 — Failure handling | next |
| 4 — Observability foundation | not started |
| 5 — Laboratory core | not started |
| 6 — Persistence & snapshots | not started |
| 7 — Deterministic replay | not started |
| 8 — Stress & chaos | not started |
| 9 — Cluster controller & CLI | not started |
| 10 — Benchmarking | not started |
| 11 — Dashboard (if time remains) | not started |
| 12 — Docs & ship | not started |

Nothing in this repo mocks a Raft mechanism or hard-codes a metric. Commands that
cannot yet be answered honestly are absent from the CLI rather than stubbed.

## Install

Requires Python 3.11+.

```bash
pip install -e ".[dev]"
```

## Run the tests

```bash
pytest
```

## CLI

```bash
pyraft-lab version    # installed version
pyraft-lab kinds      # message kinds this build can put on the wire
```

The cluster and experiment commands (`start`, `put`, `get`, `status`, `show-log`,
`run --scenario ...`, `results`) arrive with their phases.

## Layout

```
src/pyraft_lab/
  raft/           consensus: state, RPCs, election, replication, persistence
  kv/             replicated key-value state machine
  network/        transports: the in-memory bus, later the fault simulator
  cluster/        cluster manager, config and lifecycle
  client/         leader-discovering client and workload generators
  experiments/    scenario runner, metrics, plots, stress and chaos campaigns
  observability/  event vocabulary, tracer, live metrics
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
