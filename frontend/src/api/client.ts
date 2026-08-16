/**
 * The only place this app talks to the backend.
 *
 * Every call goes to same-origin `/api` — the Vite dev server proxies it, so there is
 * no host to configure and no CORS to negotiate in development. A non-2xx response
 * becomes an `ApiError` carrying the backend's own `detail` string, because those
 * strings are written to be read by a person ("n2 is already down") and re-wording
 * them here would only make the UI less specific than the server.
 */

import type {
  ApiConfig,
  ClusterConfigForm,
  ClusterPayload,
  FaultState,
  Job,
  KeyValueRecord,
  NodeId,
  NodeLog,
  NodeState,
  ResultRow,
  RpcMessage,
  RunDetail,
  ScenarioSummary,
  SnapshotListing,
  SnapshotMeta,
  TraceEvent,
  WalReport,
} from '../types/pyraft';

export class ApiError extends Error {
  readonly status: number;

  constructor(message: string, status: number) {
    super(message);
    this.name = 'ApiError';
    this.status = status;
  }
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  let response: Response;
  try {
    response = await fetch(`/api${path}`, {
      ...init,
      headers: init?.body ? { 'Content-Type': 'application/json', ...init?.headers } : init?.headers,
    });
  } catch {
    throw new ApiError('cannot reach the API — is `pyraft-lab serve` running?', 0);
  }

  if (!response.ok) {
    let detail = `${response.status} ${response.statusText}`;
    try {
      const body = await response.json();
      if (typeof body?.detail === 'string') detail = body.detail;
      else if (Array.isArray(body?.detail)) detail = body.detail.map((d: any) => d.msg).join('; ');
    } catch {
      /* a non-JSON error body is rare and the status line already says enough */
    }
    throw new ApiError(detail, response.status);
  }

  if (response.status === 204) return undefined as T;
  return response.json() as Promise<T>;
}

const post = <T>(path: string, body?: unknown) =>
  request<T>(path, { method: 'POST', body: body === undefined ? undefined : JSON.stringify(body) });

/**
 * One path segment, escaped.
 *
 * Applied to every interpolated identifier rather than only the ones that look risky:
 * a KV key is arbitrary user text, and the day one arrives holding a `?`, a `#` or a
 * space is not the day to discover the URL was being built by concatenation. Node and
 * job ids are server-generated and safe today, but they cost nothing to escape and
 * stop the pattern from being "escape it if you remember to".
 */
const seg = (value: string) => encodeURIComponent(value);

export interface StartClusterOptions {
  nodes?: number;
  seed?: number;
  persistent?: boolean;
  electionTimeoutMinMs?: number;
  electionTimeoutMaxMs?: number;
  heartbeatIntervalMs?: number;
  clientTimeoutMs?: number;
}

export const api = {
  health: () => request<{ ok: boolean; version: string; workspace: string }>('/health'),

  // --- configuration ---------------------------------------------------------------
  getConfig: () => request<ApiConfig>('/config'),
  saveConfig: (body: ClusterConfigForm) =>
    request<ApiConfig>('/config', { method: 'PUT', body: JSON.stringify(body) }),

  // --- the live cluster ------------------------------------------------------------
  cluster: () => request<ClusterPayload>('/cluster'),
  startCluster: (options: StartClusterOptions) => post<ClusterPayload>('/cluster/start', options),
  stopCluster: () => post<ClusterPayload>('/cluster/stop'),
  exec: (line: string) => post<{ ok: boolean; output: string }>('/cluster/exec', { line }),

  nodes: () => request<NodeState[]>('/cluster/nodes'),
  node: (id: NodeId) => request<NodeState>(`/cluster/nodes/${seg(id)}`),
  restartNode: (id: NodeId) => post<NodeState>(`/cluster/nodes/${seg(id)}/restart`),
  crashNode: (id: NodeId) => post<NodeState>(`/cluster/nodes/${seg(id)}/crash`),
  recoverNode: (id: NodeId) => post<NodeState>(`/cluster/nodes/${seg(id)}/recover`),
  nodeEvents: (id: NodeId, last = 60) =>
    request<TraceEvent[]>(`/cluster/nodes/${seg(id)}/events?last=${last}`),
  nodeWal: (id: NodeId) => request<WalReport>(`/cluster/nodes/${seg(id)}/wal`),
  nodeSnapshots: (id: NodeId) => request<SnapshotListing>(`/cluster/nodes/${seg(id)}/snapshots`),
  takeSnapshot: (id: NodeId) => post<SnapshotMeta>(`/cluster/nodes/${seg(id)}/snapshots`),

  log: (node?: NodeId, limit?: number) => {
    const query = new URLSearchParams();
    if (node) query.set('node', node);
    if (limit) query.set('limit', String(limit));
    return request<NodeLog>(`/cluster/log${query.size ? `?${query}` : ''}`);
  },
  trace: (last = 200) => request<TraceEvent[]>(`/cluster/trace?last=${last}`),
  rpc: (last = 60) => request<RpcMessage[]>(`/cluster/rpc?last=${last}`),

  // --- the key-value store ----------------------------------------------------------
  listKv: () => request<{ nodeId: NodeId; records: KeyValueRecord[] }>('/kv'),
  put: (key: string, value: string) => post<{ ok: boolean }>('/kv', { key, value }),
  get: (key: string) => request<{ key: string; value: unknown; found: boolean }>(`/kv/${seg(key)}`),
  del: (key: string) =>
    request<{ ok: boolean; existed: boolean }>(`/kv/${seg(key)}`, { method: 'DELETE' }),
  cas: (key: string, expected: string, next: string) =>
    post<{ ok: boolean }>('/kv/cas', { key, expected, new: next }),

  // --- faults --------------------------------------------------------------------------
  faults: () => request<FaultState>('/faults'),
  partition: (groups: NodeId[][]) => post<FaultState>('/faults/partition', { groups }),
  heal: () => post<FaultState>('/faults/heal'),
  setLatency: (minMs: number, maxMs: number) => post<FaultState>('/faults/latency', { minMs, maxMs }),
  setLoss: (probability: number) => post<FaultState>('/faults/loss', { probability }),
  resetFaults: () => post<FaultState>('/faults/reset'),

  // --- the laboratory --------------------------------------------------------------------
  scenarios: () => request<ScenarioSummary[]>('/scenarios'),
  scenario: (name: string) =>
    request<{ file: string; yaml: string; scenario: Record<string, unknown> }>(
      `/scenarios/${seg(name)}`
    ),
  runScenario: (scenario: string, seed?: number, plot = false) =>
    post<Job>('/experiments/run', { scenario, seed, plot }),

  runStress: (body: { nodes: number; duration: string; trials: number; seed: number }) =>
    post<Job>('/campaigns/stress', body),
  runChaos: (body: { nodes: number; duration: string; trials: number; seed: number }) =>
    post<Job>('/campaigns/chaos', body),
  runBenchmark: (body: {
    nodeCounts: number[];
    lossPercents: number[];
    duration: string;
    seed: number;
  }) => post<Job>('/campaigns/benchmark', body),

  // --- persisted runs -----------------------------------------------------------------
  results: () => request<ResultRow[]>('/results'),
  result: (runId: string) => request<RunDetail>(`/results/${seg(runId)}`),
  resultEvents: (runId: string, last = 500) =>
    request<TraceEvent[]>(`/results/${seg(runId)}/events?last=${last}`),
  replay: (runId: string) => post<Job>(`/results/${seg(runId)}/replay`),

  // --- jobs ---------------------------------------------------------------------------
  jobs: (kind?: string) => request<Job[]>(`/jobs${kind ? `?kind=${kind}` : ''}`),
  job: (id: string) => request<Job>(`/jobs/${seg(id)}`),
  cancelJob: (id: string) => post<Job>(`/jobs/${seg(id)}/cancel`),
};

/** A message off the live stream. `hello` arrives once, on connect. */
export type StreamMessage =
  | { channel: 'hello'; payload: ClusterPayload }
  | { channel: 'cluster'; payload: { running: boolean } }
  | { channel: 'status'; payload: import('../types/pyraft').ClusterStatus }
  | { channel: 'events'; payload: TraceEvent[] }
  | { channel: 'job'; payload: Job };

export function streamUrl(): string {
  const scheme = window.location.protocol === 'https:' ? 'wss' : 'ws';
  return `${scheme}://${window.location.host}/api/stream`;
}
