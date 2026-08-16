/**
 * The shapes the API actually returns.
 *
 * Where the backend cannot measure something honestly the field is absent rather than
 * estimated — per-node CPU and memory, for instance, are meaningless when every node
 * is a task inside one process, so they are not here and the UI does not draw them.
 * A `number | null` means "not measured yet", never zero.
 */

export type NodeId = string;

export type NodeRole = 'leader' | 'follower' | 'candidate' | 'down';

export type ClusterHealth = 'HEALTHY' | 'DEGRADED' | 'PARTITIONED' | 'NO LEADER';

export interface NodeState {
  id: NodeId;
  alive: boolean;
  role: NodeRole;
  currentTerm: number | null;
  votedFor: NodeId | null;
  leaderId: NodeId | null;
  commitIndex: number;
  lastApplied: number;
  logLength: number;
  logBaseIndex?: number;
  storeKeys: number;
  peers: NodeId[];
  matchIndex?: Record<NodeId, number>;
  nextIndex?: Record<NodeId, number>;
  walPath: string | null;
  walBytes: number | null;
  snapshotCount: number;
}

export interface LogEntry {
  index: number;
  term: number;
  command: string;
  committed: boolean;
  applied: boolean;
  kind: string | null;
  key: string | null;
  value: unknown;
  client: string | null;
  serial: number | null;
}

export interface NodeLog {
  nodeId: NodeId;
  commitIndex: number;
  lastApplied: number;
  entries: LogEntry[];
}

export interface ClientMetrics {
  attempts: number;
  successes: number;
  failures: number;
  redirects: number;
  timeouts: number;
  availability: number;
}

export interface ClusterStatus {
  nodeCount: number;
  term: number;
  leader: NodeId | null;
  status: ClusterHealth;
  partitioned: boolean;
  partitionGroups: NodeId[][];
  down: NodeId[];
  electionsStarted: number;
  leadersElected: number;
  termChanges: number;
  appendEntriesSent: number;
  appendEntriesRejected: number;
  commitIndex: number;
  appliedIndex: number;
  logEntriesCount: number;
  electionTimeMs: number | null;
  meanElectionTimeMs: number | null;
  p50LatencyMs: number | null;
  p99LatencyMs: number | null;
  availabilityPct: number | null;
  client: ClientMetrics | null;
  messagesSent: number;
  messagesDelivered: number;
  droppedPackets: number;
  droppedLoss: number;
  droppedPartition: number;
  droppedUnknown: number;
  delayed: number;
  eventCount: number;
}

export interface ClusterConfig {
  nodes: number;
  data_dir: string | null;
  election_timeout_range: [number, number];
  heartbeat_interval: number;
  client_timeout: number;
  seed: number;
}

export interface ClusterPayload {
  running: boolean;
  config: ClusterConfig;
  status?: ClusterStatus;
}

export interface KeyValueRecord {
  key: string;
  value: unknown;
  version: number;
  modifiedTerm: number | null;
  modifiedIndex: number | null;
}

export interface FaultState {
  minLatencyMs: number;
  maxLatencyMs: number;
  lossProbability: number;
  partitioned: boolean;
  partitionGroups: NodeId[][];
  downNodes: NodeId[];
}

export type EventKind =
  | 'ELECTION_STARTED'
  | 'VOTE_REQUESTED'
  | 'VOTE_GRANTED'
  | 'LEADER_ELECTED'
  | 'APPEND_ENTRIES_SENT'
  | 'APPEND_ENTRIES_REJECTED'
  | 'LOG_COMMITTED'
  | 'NODE_CRASHED'
  | 'NODE_RECOVERED'
  | 'PARTITION_CREATED'
  | 'PARTITION_HEALED'
  | 'TERM_CHANGED';

export interface TraceEvent {
  seq: number;
  timestamp: number;
  node: NodeId | null;
  term: number | null;
  kind: EventKind;
  details: Record<string, unknown>;
  text: string;
}

export interface RpcMessage {
  id: string;
  from: NodeId | null;
  to: NodeId | null;
  type: 'RequestVote' | 'RequestVoteResponse' | 'AppendEntries' | 'AppendEntriesResponse';
  term: number | null;
  timestamp: number;
  details: Record<string, unknown>;
}

export interface WalRecord {
  position: number;
  kind: string;
  index: number | null;
  term: number | null;
  detail: string;
  status: 'VALID';
}

export interface WalReport {
  path: string | null;
  exists: boolean;
  corrupt?: boolean;
  reason?: string;
  nodeId?: string | null;
  recordsRead?: number;
  recordsValid?: number;
  validBytes?: number;
  truncatedTail?: boolean;
  droppedRecords?: number;
  term?: number;
  votedFor?: string | null;
  commitIndex?: number;
  baseIndex?: number;
  lastIndex?: number;
  reportLines?: string[];
  records?: WalRecord[];
}

export interface SnapshotMeta {
  snapshotId: string;
  nodeId: string | null;
  lastIncludedIndex: number;
  lastIncludedTerm: number;
  keys: number;
  createdAt: number;
  checksum: string;
}

export interface SnapshotListing {
  nodeId: NodeId;
  supported: boolean;
  reason?: string;
  directory?: string;
  snapshots: SnapshotMeta[];
}

export interface ScenarioSummary {
  file: string;
  name?: string;
  nodes?: number;
  durationSec?: number;
  seed?: number;
  faultCount?: number;
  expected?: Record<string, unknown>;
  error?: string;
}

export interface ResultRow {
  runId: string;
  scenarioName?: string;
  seed?: number;
  createdAt?: string;
  passed?: boolean;
  availability?: number;
  throughputPerSec?: number;
  dataLoss?: boolean;
  totalRequests?: number;
  p99CommitLatencyMs?: number | null;
  totalElections?: number;
  linearizable?: boolean | null;
  error?: string;
}

/** The full `metrics.report()` document. Deeply nested and stable, so it is typed
 *  where the UI reads it and left as `unknown` elsewhere rather than mirrored whole. */
export interface RunReport {
  scenario: string;
  run_id: string;
  seed: number;
  duration_sec: number;
  passed: boolean;
  summary: {
    total_requests: number;
    successful: number;
    incomplete: number;
    failed_redirects: number;
    timeouts: number;
    availability: number;
    throughput_per_sec: number;
    data_loss: boolean;
    lost_writes: unknown[];
    divergent_logs: unknown[];
  };
  timing: {
    avg_commit_latency_ms: number | null;
    p50_commit_latency_ms: number | null;
    p95_commit_latency_ms: number | null;
    p99_commit_latency_ms: number | null;
  };
  leadership: {
    total_elections: number;
    leader_changes: number;
    election_time_ms: number | null;
    recovery_time_ms: number | null;
    final_leader: string | null;
    final_term: number;
    leadership_timeline: { leader: string; term: number; from: number; to: number }[];
  };
  replication: Record<string, unknown>;
  fault_impact: {
    messages_sent: number;
    messages_delivered: number;
    messages_dropped: number;
    messages_delayed: number;
    dropped_by_partition: number;
    dropped_by_loss: number;
    faults_applied: { at: number; action: string; target?: string | null }[];
  };
  consistency?: {
    linearizable: boolean | null;
    verdict: string;
    explanation: string;
    [key: string]: unknown;
  };
  expectations: { key: string; checked: boolean; met: boolean; expected: unknown; actual: unknown }[];
}

export interface RunDetail {
  runId: string;
  manifest?: Record<string, unknown>;
  report?: RunReport;
}

export type JobStatus = 'running' | 'succeeded' | 'failed' | 'cancelled';

export interface Job<T = unknown> {
  id: string;
  kind: 'scenario' | 'stress' | 'chaos' | 'benchmark' | 'replay';
  params: Record<string, unknown>;
  status: JobStatus;
  completed: number;
  total: number;
  label: string;
  lines: string[];
  result: T | null;
  error: string | null;
  created_at: string;
  finished_at: string | null;
  cancelling: boolean;
}

export interface StressTrial {
  trial_index: number;
  run_id: string;
  seed: number;
  fault_kind: string;
  passed: boolean;
  data_loss: boolean;
  divergent: boolean;
  linearizable_verdict: string;
  recovery_time_ms: number | null;
  duration_sec: number;
}

export interface ChaosTrial {
  trial_index: number;
  run_id: string;
  seed: number;
  passed: boolean;
  data_loss: boolean;
  divergent: boolean;
  lost_write_count: number;
  linearizable_verdict: string;
  recovery_time_ms: number | null;
  fault_count: number;
  duration_sec: number;
}

/**
 * What `/api/campaigns/stress` and `/api/campaigns/chaos` return.
 *
 * The trial list is `trial_results`, matching `StressReport.to_dict` and
 * `ChaosReport.to_dict` — the same key the campaign JSON is persisted under, and the
 * one `tests/test_stress.py` asserts on. It is deliberately not `results`, which is
 * `BenchmarkReport`'s own differently-shaped field.
 *
 * No index signature: one here let a wrong field name typecheck, so both campaign
 * screens read `results`, got `undefined`, and threw on first render of a finished
 * campaign — which unmounted the whole app.
 */
export interface CampaignReport<T> {
  nodes: number;
  trial_duration_sec: number;
  base_seed: number;
  trials: number;
  passed: number;
  failed: number;
  data_loss_trials: number;
  divergent_trials: number;
  linearizability: { PASS: number; FAIL: number; UNPROVEN: number };
  recovery_time_ms: Percentiles;
  trial_results: T[];
}

export interface Percentiles {
  p50: number | null;
  p95: number | null;
  p99: number | null;
  max: number | null;
}

export interface BenchmarkCell {
  nodes: number;
  loss_rate: number;
  seed: number;
  run_id: string;
  throughput_per_sec: number;
  latency_p50_ms: number | null;
  latency_p95_ms: number | null;
  latency_p99_ms: number | null;
  election_time_ms: number | null;
  recovery_time_ms: number | null;
  cpu_seconds: number;
  peak_memory_mb: number;
  data_loss: boolean;
  duration_sec: number;
}

export interface BenchmarkReport {
  node_counts: number[];
  loss_rates: number[];
  trial_duration_sec: number;
  base_seed: number;
  cells: number;
  data_loss_cells: number;
  results: BenchmarkCell[];
  graphs: string[];
}

export interface ReplayResult {
  runId: string;
  matched: boolean;
  explanation: string;
}

export interface ApiConfig {
  path: string;
  saved: boolean;
  nodes: number;
  persistent: boolean;
  electionTimeoutMinMs: number;
  electionTimeoutMaxMs: number;
  heartbeatIntervalMs: number;
  clientTimeoutMs: number;
  seed: number;
  /** Server-side verdict on the saved timers: the heartbeat is not comfortably inside
   *  the shortest election timeout, so followers will campaign against a healthy
   *  leader. Reported, not refused — watching that happen is a valid experiment. */
  heartbeatTooSlow: boolean;
}

/** The fields `PUT /api/config` actually accepts — the rest are server-derived. */
export type ClusterConfigForm = Omit<ApiConfig, 'path' | 'saved' | 'heartbeatTooSlow'>;
