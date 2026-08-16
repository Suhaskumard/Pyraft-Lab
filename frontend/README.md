# PyRaft Lab — web front end

A React + TypeScript workbench for the Raft cluster and the fault-injection laboratory
in [`../pyraft_lab`](../pyraft_lab). Every number it draws comes from the backend over
HTTP or the live event stream — there is no mock data anywhere in this app.

## Running it

Two processes. The API owns the cluster; Vite serves the UI and proxies `/api` to it,
so there is no host to configure and no CORS to negotiate.

```bash
# 1. the API (from a workspace directory — where cluster.yaml, scenarios/ and results/ live)
cd ../pyraft_lab && pip install -e ".[api]"
pyraft-lab init                      # scaffolds scenarios/ and results/
pyraft-lab serve                     # http://127.0.0.1:8000

# 2. the UI
npm install
npm run dev                          # http://localhost:5173
```

Then press **Start cluster** in the header. Tick *persist each node to a WAL* if you
want the WAL Inspector and Snapshot Store pages to have anything to show — only a
WAL-backed node is genuinely rebuilt from disk on restart.

`npm run build` produces a static bundle in `dist/`; serve it behind anything that
proxies `/api` and `/api/stream` to the API process.

## How it is wired

```
src/api/client.ts       every HTTP call, one typed surface, no fetch anywhere else
src/api/LabContext.tsx  one WebSocket; cluster status, nodes, trace events, jobs
src/api/useResource.ts  fetch + loading + error + optional poll, for per-screen data
src/api/useJob.ts       start a campaign and follow it to completion
src/components/ui.tsx   the shared Obsidian vocabulary (panels, metrics, pills, states)
src/components/screens/ one file per page
```

**One socket, not one poll per panel.** The server pushes a status snapshot twice a
second and batches trace events at 10 Hz, so the header, the topology table and the
event feed all move from the same stream and cannot disagree the way three independent
polls would. The socket is a notification channel, not the source of truth: on connect
and on every reconnect the provider re-reads `GET /api/cluster`, so a client that
missed messages recovers a correct view rather than a patched-up one.

**`n/a` is a real value.** The backend returns `null` for anything it has not measured —
P99 latency before any request has completed, election time before any election. Those
render as `n/a`, never as `0`. Fields the backend cannot measure honestly (per-node CPU
and memory, which are meaningless when every node is a task in one process) do not
exist in the API and are not drawn.

**"No cluster" and "an idle cluster" are different.** Live-cluster routes answer `409`
when nothing is running, and the UI shows that as its own state instead of an empty
table. Laboratory pages — scenarios, campaigns, results, replay — work with no cluster
at all, and the sidebar dims only the pages that need one.

## Pages

| Group | Page | What it reads |
| --- | --- | --- |
| Live cluster | Cluster Overview | live status, topology, the leader's log, a REPL console |
| | Node Inspector | one member's Figure 2 state, its log, its trace events |
| | Key-Value Store | the applied state machine, through a real leader-discovering client |
| | Consensus & RPC | wire-crossing events as directed messages, election counters |
| Storage | WAL Inspector | `read_wal` over the real file — CRC-verified records, what a restart recovers |
| | Snapshot Store | the snapshot directory; takes new ones |
| | Architecture Map | the four layers, annotated with what each is doing now |
| Laboratory | Fault Injection | partitions, latency, loss and crashes on the live network |
| | Experiment Runner | run a scenario file; its full report and `expected:` block |
| | Deterministic Replay | reproduce a persisted run and diff its trace |
| Campaigns | Benchmark / Stress / Chaos | streamed per-trial progress, then the report |
| Results | Explorer / Linearizability / Comparison | persisted runs from `results/` |
| System | Jobs & Diagnostics | every background job, its progress and its failures |
| | Cluster Settings | edits the `cluster.yaml` the CLI reads too |

## Styling

Tailwind v4 plus a small hand-written component layer in `src/index.css` (`.obsidian-card`,
`.status-pill`, `.btn-obsidian`, `.terminal-block`, …). Those live inside
`@layer components` so a utility written alongside one of them still wins —
`class="obsidian-card p-4"` has to mean `p-4`, and unlayered CSS would beat it.
