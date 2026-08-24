import { useEffect, useRef, useState } from 'react';
import { CornerDownLeft, RotateCcw, Zap } from 'lucide-react';
import { api } from '../../api/client';
import { useLab } from '../../api/LabContext';
import { useResource } from '../../api/useResource';
import {
  Empty,
  Failure,
  HealthPill,
  Metric,
  Panel,
  RolePill,
  ScreenHeader,
  Skeleton,
  fmtMs,
  fmtNum,
  fmtPct,
} from '../ui';

/**
 * Everything true about the cluster right now, on one page.
 *
 * The numbers come from two different places on purpose. Term, leader, election counts
 * and health are folded from the *trace* by `LiveMetrics` — a metric that could look
 * right while the trace was wrong would be worse than useless. Latency and availability
 * come from the session's own client and read `n/a` until real traffic has gone through,
 * rather than being filled in with something synthetic.
 */
export function ClusterOverviewScreen({ onInspectNode }: { onInspectNode: (id: string) => void }) {
  const { status, nodes, events, act } = useLab();
  const log = useResource(() => api.log(undefined, 40), [], { pollMs: 1500 });

  if (!status) return <Skeleton rows={6} />;

  const healthy = status.nodeCount - status.down.length;

  return (
    <div className="flex flex-col gap-4">
      <ScreenHeader
        title="Cluster Overview"
        description="A live, in-process Raft cluster: real nodes, real election timers, a wall clock. Everything below is folded from this cluster's own event trace."
        actions={<HealthPill status={status.status} />}
      />

      <div className="grid grid-cols-2 md:grid-cols-4 xl:grid-cols-6 gap-3">
        <Metric label="Term" value={status.term} tone="accent" />
        <Metric label="Leader" value={status.leader ?? '(none)'} tone={status.leader ? 'good' : 'bad'} />
        <Metric label="Nodes up" value={`${healthy}/${status.nodeCount}`} tone={healthy === status.nodeCount ? 'good' : 'warn'} />
        <Metric label="Commit index" value={status.commitIndex} tone="good" />
        <Metric label="Applied" value={status.appliedIndex} />
        <Metric label="Log entries" value={status.logEntriesCount} />
        <Metric
          label="Last election"
          value={fmtMs(status.electionTimeMs)}
          hint="Time from the first candidate campaigning in a term to a leader being elected in it."
        />
        <Metric
          label="P99 latency"
          value={fmtMs(status.p99LatencyMs)}
          hint="From this session's own client. Reads n/a until a request has completed."
        />
        <Metric
          label="Availability"
          value={fmtPct(status.availabilityPct)}
          tone={status.availabilityPct === null ? 'default' : status.availabilityPct >= 99 ? 'good' : 'warn'}
          hint="Fraction of client requests that eventually succeeded, counted per request."
        />
        <Metric label="Elections" value={status.electionsStarted} />
        <Metric
          label="Dropped"
          value={fmtNum(status.droppedPackets)}
          tone={status.droppedPackets ? 'warn' : 'default'}
          hint={`${status.droppedLoss} to packet loss, ${status.droppedPartition} to a partition`}
        />
        <Metric label="Trace events" value={fmtNum(status.eventCount)} />
      </div>

      {status.partitioned && (
        <Failure
          message={`Network partitioned into ${status.partitionGroups
            .map((group) => `{${group.join(', ')}}`)
            .join(' | ')} — nodes in different groups cannot reach each other.`}
        />
      )}

      <div className="grid grid-cols-1 xl:grid-cols-3 gap-4">
        <Panel
          title="Topology"
          subtitle="Each member's own view of the term and who leads it"
          className="xl:col-span-2"
        >
          <div className="obsidian-table-container">
            <table className="obsidian-table">
              <thead>
                <tr>
                  <th>Node</th>
                  <th>Role</th>
                  <th>Term</th>
                  <th>Leader</th>
                  <th className="text-right">Commit</th>
                  <th className="text-right">Applied</th>
                  <th className="text-right">Log</th>
                  <th className="text-right">Keys</th>
                  <th />
                </tr>
              </thead>
              <tbody>
                {nodes.map((node) => (
                  <tr key={node.id}>
                    <td className="font-mono font-medium">{node.id}</td>
                    <td><RolePill role={node.role} /></td>
                    <td className="font-mono">{node.currentTerm ?? '—'}</td>
                    <td className="font-mono text-[#8b919d]">{node.leaderId ?? '—'}</td>
                    <td className="font-mono text-right">{node.commitIndex}</td>
                    <td className="font-mono text-right">{node.lastApplied}</td>
                    <td className="font-mono text-right">{node.logLength}</td>
                    <td className="font-mono text-right">{node.storeKeys}</td>
                    <td className="text-right whitespace-nowrap">
                      <button
                        className="btn-obsidian btn-ghost btn-sm"
                        onClick={() => onInspectNode(node.id)}
                      >
                        Inspect
                      </button>
                      {node.alive ? (
                        <button
                          className="btn-obsidian btn-ghost btn-sm"
                          title="Crash this node and leave it down"
                          onClick={() => void act(`crash ${node.id}`, () => api.crashNode(node.id))}
                        >
                          <Zap className="w-3 h-3" />
                        </button>
                      ) : (
                        <button
                          className="btn-obsidian btn-ghost btn-sm"
                          title="Bring this node back"
                          onClick={() => void act(`recover ${node.id}`, () => api.recoverNode(node.id))}
                        >
                          <RotateCcw className="w-3 h-3" />
                        </button>
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Panel>

        <Console />
      </div>

      <div className="grid grid-cols-1 xl:grid-cols-2 gap-4">
        <Panel
          title="Replicated log"
          subtitle={
            log.data
              ? `${log.data.nodeId} — committed through ${log.data.commitIndex}, applied through ${log.data.lastApplied}`
              : 'the leader’s log'
          }
        >
          {log.error ? (
            <Failure message={log.error} />
          ) : !log.data?.entries.length ? (
            <Empty title="The log is empty" hint="Write a key to append the first entry." />
          ) : (
            <div className="obsidian-table-container max-h-80 overflow-y-auto">
              <table className="obsidian-table">
                <thead>
                  <tr>
                    <th className="text-right">Idx</th>
                    <th className="text-right">Term</th>
                    <th>Command</th>
                    <th>State</th>
                  </tr>
                </thead>
                <tbody>
                  {[...log.data.entries].reverse().map((entry) => (
                    <tr key={entry.index}>
                      <td className="font-mono text-right">{entry.index}</td>
                      <td className="font-mono text-right text-[#58a6ff]">{entry.term}</td>
                      <td className="font-mono">{entry.command}</td>
                      <td>
                        <span
                          className={`status-pill ${
                            entry.applied
                              ? 'status-pill-healthy'
                              : entry.committed
                                ? 'status-pill-leader'
                                : 'status-pill-candidate'
                          }`}
                        >
                          <span className="status-dot" />
                          {entry.applied ? 'APPLIED' : entry.committed ? 'COMMITTED' : 'PENDING'}
                        </span>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </Panel>

        <Panel title="Live event trace" subtitle="Streamed from the cluster's one shared tracer">
          <EventFeed events={events} />
        </Panel>
      </div>
    </div>
  );
}

export function EventFeed({ events, height = 'max-h-80' }: { events: { seq: number; text: string; kind: string }[]; height?: string }) {
  const container = useRef<HTMLDivElement>(null);
  const [follow, setFollow] = useState(true);

  // Scroll only this panel's own container, not `scrollIntoView` on the bottom
  // sentinel — that walks every scrollable ancestor, so the whole page would jump
  // to keep this in view instead of just the feed itself.
  useEffect(() => {
    if (follow && container.current) {
      container.current.scrollTop = container.current.scrollHeight;
    }
  }, [events, follow]);

  if (!events.length) {
    return <Empty title="No events yet" hint="They will appear here as the cluster runs." />;
  }

  const colour = (kind: string) =>
    kind.startsWith('NODE_') || kind.startsWith('PARTITION_')
      ? 'text-[#ffba42]'
      : kind === 'LEADER_ELECTED' || kind === 'LOG_COMMITTED'
        ? 'text-[#4ade80]'
        : kind === 'APPEND_ENTRIES_REJECTED'
          ? 'text-[#ffb4ab]'
          : 'text-[#c0c7d4]';

  return (
    <div className="flex flex-col gap-2">
      <label className="flex items-center gap-2 text-[13px] text-[#8b919d] cursor-pointer self-end">
        <input
          type="checkbox"
          checked={follow}
          onChange={(event) => setFollow(event.target.checked)}
          className="accent-[#58a6ff]"
        />
        Follow tail
      </label>
      <div ref={container} className={`terminal-block ${height} overflow-y-auto flex flex-col`}>
        {events.map((event) => (
          <span key={event.seq} className={colour(event.kind)}>
            {event.text}
          </span>
        ))}
      </div>
    </div>
  );
}

/** The REPL, in a panel. Runs against `POST /api/cluster/exec`. */
function Console() {
  const { notify } = useLab();
  const [line, setLine] = useState('');
  const [history, setHistory] = useState<string[]>([]);
  const [busy, setBusy] = useState(false);

  const run = async () => {
    const command = line.trim();
    if (!command) return;
    setBusy(true);
    setLine('');
    try {
      const result = await api.exec(command);
      setHistory((prev) => [...prev.slice(-40), `pyraft-lab> ${command}`, result.output || '(no output)']);
    } catch (error) {
      notify('error', error instanceof Error ? error.message : String(error));
    } finally {
      setBusy(false);
    }
  };

  return (
    <Panel
      title="Cluster console"
      subtitle="The same commands `pyraft-lab cluster start` accepts"
    >
      <div className="flex flex-col gap-2">
        <div className="terminal-block h-56 overflow-y-auto flex flex-col">
          {history.length ? (
            history.map((entry, index) => (
              <span key={index} className={entry.startsWith('pyraft-lab>') ? 'text-[#58a6ff]' : ''}>
                {entry}
              </span>
            ))
          ) : (
            <span className="text-[#8b919d]">Try: help, topology, put color blue, trace --last 10</span>
          )}
        </div>
        <div className="flex gap-2">
          <input
            value={line}
            onChange={(event) => setLine(event.target.value)}
            onKeyDown={(event) => event.key === 'Enter' && void run()}
            placeholder="put color blue"
            className="flex-1 bg-[#0b0e14] border border-[#30363d] rounded px-2.5 py-1.5 font-mono text-sm text-[#e0e2ea] placeholder:text-[#8b919d] focus:border-[#58a6ff] outline-none"
          />
          <button className="btn-obsidian btn-primary btn-sm" onClick={() => void run()} disabled={busy}>
            <CornerDownLeft className="w-3 h-3" />
            Run
          </button>
        </div>
      </div>
    </Panel>
  );
}
