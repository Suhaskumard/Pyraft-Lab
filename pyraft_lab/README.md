# PyRaft Lab

A from-scratch Raft consensus implementation in Python, wrapped around a replicated
key-value store, with a deterministic fault-injection network simulator and an
experiment runner that measures election time, commit latency, replication lag,
recovery time and linearizability under failure.

Two layers:

1. **The system** — Raft + KV + WAL
2. **The laboratory** — fault injection + experiments + analytics

## Status

Built phase by phase against the 15-day plan. Current state:

| Phase | Days | Status |
| --- | --- | --- |
| 0 — Foundation | 1 | **done** |
| 1 — Raft core | 2–4 | not started |
| 2 — KV & client | 5–6 | not started |
| 3 — Failure handling | 7–10 | not started |
| 4 — Laboratory | 11–13 | not started |
| 5 — Ship | 14–15 | not started |

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
  raft/      consensus: state, RPCs, election, replication, persistence
  kv/        replicated key-value state machine
  network/   transports: the in-memory bus, later the fault simulator
  client/    leader-discovering client and workload generators
  lab/       scenario runner, metrics, plots, linearizability checker
  cli/       the pyraft-lab command line
scenarios/   YAML experiment definitions
results/     generated experiment output (not committed)
docs/        architecture notes and the technical report
tests/       unit, integration and property tests
```

See [docs/architecture.md](docs/architecture.md) for the design decisions and the
reasoning behind them.

## License

MIT — see [LICENSE](LICENSE).
