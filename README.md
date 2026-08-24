<div align="center">
  <h1>🚢 PyRaft Lab</h1>
  <p><strong>A deterministic Raft consensus implementation and interactive laboratory in Python.</strong></p>
  <p>
    <a href="#about-the-project">About</a> •
    <a href="#key-components">Components</a> •
    <a href="#quick-start">Quick Start</a> •
    <a href="#the-laboratory">The Laboratory</a> •
    <a href="#documentation">Docs</a>
  </p>
</div>

---

## 📖 About the Project

Building a distributed consensus algorithm is notoriously difficult; verifying it is even harder. **PyRaft Lab** was built to solve both. It's not just a from-scratch implementation of the [Raft Consensus Algorithm](https://raft.github.io/) in Python—it's a complete, deterministic testing laboratory. 

At its core, it features a replicated Key-Value (KV) store built on top of a Raft state machine. Surrounding the core is a discrete-event network simulator that gives you "God mode" over the cluster. You can pause time, drop packets, sever network links, and trigger chaos monkeys—all with perfectly reproducible results.

Whether you're studying distributed systems, verifying linearizability under extreme failure scenarios, or just want a visual way to watch leader elections happen, PyRaft Lab provides the tooling to do it interactively.

---

## 🏗️ Key Components

The repository is structured into two main ecosystems: the Python backend (consensus and simulation) and the React frontend (visualization).

### 1. The Core (Backend)
- **Consensus Engine**: Implements the full Raft paper. Leader election with randomized timeouts, log replication, safety property enforcement, and commit tracking.
- **Persistence Layer**: Custom Write-Ahead Log (WAL) and state machine snapshots. Nodes can violently crash and perfectly recover their state from disk.
- **Replicated KV Store**: A practical state machine sitting on top of Raft. Supports standard `GET`, `PUT`, and `DELETE` operations with strong consistency guarantees.
- **Discrete-Event Simulator**: Instead of relying on system clocks and real sockets, time is virtualized. Network transports are simulated, allowing for instant "time travel" during tests and deterministic playback of edge cases.

### 2. The Tools
- **Campaign Runner**: Define experiments in YAML (e.g., "start 5 nodes, kill the leader after 10s, partition node 3"). The runner executes these campaigns, injecting faults and validating cluster health.
- **Interactive CLI**: A rich terminal interface (`pyraft-lab`) for managing the cluster, inspecting logs, and triggering manual failovers.
- **REST API**: A FastAPI backend that exposes cluster topology, metrics, and state machine queries.

### 3. The Dashboard (Frontend)
A React-based web interface for the API. It provides visual observability into the cluster:
- Watch log replication happen in real-time.
- View leader/follower states and term numbers.
- Monitor active campaigns and chaos experiments.

---

## 🚀 Quick Start

### Prerequisites
- **Python 3.11+**
- **Node.js 18+**

### Backend Setup

Navigate into the backend directory and install the project along with its API dependencies. We recommend using a virtual environment.

```bash
cd pyraft_lab
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -e ".[api]"
```

Initialize your first cluster and boot up the API server:

```bash
# Generates default configuration and genesis state
pyraft-lab init

# Starts the local API server on port 8000
pyraft-lab serve
```

*Head over to `http://127.0.0.1:8000/docs` to see the interactive OpenAPI endpoints.*

### Frontend Setup

In a **separate terminal window**, spin up the React development server:

```bash
cd frontend
npm install
npm run dev
```

*The frontend UI will be running at `http://localhost:5173`.*

---

## 🧪 The Laboratory: Chaos & Fault Injection

The real power of PyRaft Lab lies in its testing capabilities. Because the network and system clock are virtualized, you can write deterministic tests that simulate years of cluster uptime in seconds.

### Defining a Scenario
Scenarios are defined in YAML. Here's an example of a simple partition test:

```yaml
name: Network Partition Tolerance
cluster_size: 5
steps:
  - wait: 2000  # Let leader election finish
  - action: partition
    nodes: [node-1, node-2]
    duration: 5000
  - wait: 6000
  - assert: has_leader
```

You can run these campaigns directly from the CLI:

```bash
pyraft-lab run-campaign scenarios/partition_test.yaml
```

During execution, the CLI will output a detailed timeline of events, dropped messages, and state transitions.

---

## 📂 Directory Layout

```text
Pyraft-Lab/
├── pyraft_lab/             # Core Python package
│   ├── src/pyraft_lab/
│   │   ├── raft/           # The consensus algorithm (RPCs, election, replication)
│   │   ├── kv/             # Key-Value state machine
│   │   ├── network/        # Virtual clock & deterministic simulator
│   │   ├── cluster/        # Node orchestration and lifecycle
│   │   ├── experiments/    # Scenario engine and chaos campaigns
│   │   ├── cli/            # `pyraft-lab` entrypoints
│   │   └── api/            # FastAPI server
│   ├── scenarios/          # Pre-built chaos experiments
│   ├── docs/               # Technical write-ups and design notes
│   └── tests/              # Pytest suite (unit, integration, property tests)
│
└── frontend/               # React Dashboard
    ├── src/
    │   ├── components/     # UI widgets for nodes, logs, and graphs
    │   └── api/            # Client for the Python backend
    └── package.json
```

---

## 📖 Deep Dive Documentation

If you're interested in the implementation details, how the discrete-event simulator works, or our performance benchmarks, check out the docs:

- 🏗️ **[Architecture & Design Notes](./pyraft_lab/docs/architecture.md)**
- 🗺️ **[Development Plan](./pyraft_lab/docs/plan.md)**
- 📄 **[Technical Report](./pyraft_lab/docs/technical_report.md)**

---

## 📜 License

This project is open-sourced under the **MIT License**. See `LICENSE` for details.
