# PyRaft Lab

PyRaft Lab is a from-scratch Raft consensus implementation in Python, wrapped around a replicated key-value store. It features a deterministic fault-injection network simulator, an experiment runner, an interactive CLI, an HTTP API, and a React-based web frontend.

This project is divided into two main components:
1. **[Backend (pyraft_lab)](./pyraft_lab/README.md)**: The core Raft consensus implementation, key-value store, WAL, snapshots, fault injection laboratory, and HTTP API server.
2. **[Frontend](./frontend/README.md)**: A React-based workbench that provides a visual interface for interacting with the live cluster, inspecting the WAL/snapshots, and managing experiment campaigns.

## Directory Layout

```text
Pyraft-Lab/
├── pyraft_lab/             # Backend: Python Raft core, lab, and HTTP API
│   ├── src/pyraft_lab/
│   │   ├── raft/           # Consensus: state, RPCs, election, replication, persistence
│   │   ├── kv/             # Replicated key-value state machine
│   │   ├── network/        # Transports, virtual clock, and fault simulator
│   │   ├── cluster/        # Cluster manager, config and lifecycle
│   │   ├── client/         # Leader-discovering client and workload generators
│   │   ├── experiments/    # Scenario runner, stress/chaos/benchmark campaigns
│   │   ├── dashboard/      # Live terminal dashboard for interactive sessions
│   │   ├── cli/            # pyraft-lab command line tool
│   │   └── observability/  # Event tracing and metrics
│   ├── scenarios/          # YAML experiment definitions
│   ├── docs/               # Architecture notes and the technical report
│   └── tests/              # Unit, integration, and property tests
│
└── frontend/               # Frontend: React web interface
    ├── src/
    │   ├── api/            # API client for the Python backend
    │   ├── components/     # React UI components (cluster view, dashboard, campaigns)
    │   └── types/          # TypeScript definitions
    └── package.json        # Frontend dependencies and scripts
```

## Quick Start

### 1. Backend Setup

The backend requires Python 3.11+.

```bash
cd pyraft_lab
pip install -e ".[api]"
```

Initialize the cluster and start the API server:
```bash
pyraft-lab init
pyraft-lab serve
```
The API will be available at `http://127.0.0.1:8000` (Visit `/docs` for the OpenAPI interactive documentation).

### 2. Frontend Setup

In a new terminal window, start the React development server:

```bash
cd frontend
npm install
npm run dev
```
The frontend application will be available at `http://localhost:5173`.

## Features

- **Raft Consensus & KV Store**: Complete implementation of leader election, log replication, persistence (WAL + Snapshots), and crash recovery.
- **Laboratory & Simulator**: Deterministic network simulator with partitions, packet loss, and latency injection.
- **Stress & Chaos Testing**: Auto-generated stress campaigns and randomized chaos testing to ensure linearizability and zero data loss.
- **Benchmarking**: Compare throughput, latency, and recovery time across different cluster sizes and network conditions.
- **Web Dashboard & CLI**: Full interactive control over the cluster via the terminal CLI (`pyraft-lab cluster start`) or the React frontend.

## Documentation

For a deep dive into the architecture, technical decisions, and benchmark reports, please refer to the documentation within the `pyraft_lab/docs/` directory:
- [Architecture & Design Notes](./pyraft_lab/docs/architecture.md)
- [Development Plan](./pyraft_lab/docs/plan.md)
- [Technical Report](./pyraft_lab/docs/technical_report.md)

## License
MIT
