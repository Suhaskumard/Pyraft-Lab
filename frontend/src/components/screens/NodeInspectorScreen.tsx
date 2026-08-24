import { Power, RotateCcw, Zap } from 'lucide-react';
import { api } from '../../api/client';
import { useLab } from '../../api/LabContext';
import { useResource } from '../../api/useResource';
import {
  Empty,
  Failure,
  Field,
  Panel,
  RolePill,
  ScreenHeader,
  Skeleton,
  fmtBytes,
} from '../ui';
import { EventFeed } from './ClusterOverviewScreen';

/**
 * One member's full Raft state, and only what that member itself knows.
 *
 * `matchIndex` and `nextIndex` are shown only for a leader, because Figure 2 says they
 * are leader-only state — a follower's copies are cleared on every step-down, and
 * rendering them anyway would suggest a follower tracks its peers, which it does not.
 */
export function NodeInspectorScreen({
  selectedNodeId,
  onSelectNode,
}: {
  selectedNodeId: string | null;
  onSelectNode: (id: string) => void;
}) {
  const { nodes, act } = useLab();
  const nodeId = selectedNodeId ?? nodes[0]?.id ?? null;
  const node = nodes.find((n) => n.id === nodeId) ?? null;

  const log = useResource(() => api.log(nodeId!, 60), [nodeId], {
    pollMs: 1500,
    enabled: Boolean(nodeId) && Boolean(node?.alive),
  });
  const events = useResource(() => api.nodeEvents(nodeId!, 80), [nodeId], {
    pollMs: 2000,
    enabled: Boolean(nodeId),
  });

  if (!nodes.length) return <Skeleton rows={5} />;

  return (
    <div className="flex flex-col gap-4">
      <ScreenHeader
        title="Node Inspector"
        description="One member at a time: its persistent state, its volatile state, its log, and the trace events it emitted."
      />

      <div className="flex flex-wrap gap-2">
        {nodes.map((candidate) => (
          <button
            key={candidate.id}
            onClick={() => onSelectNode(candidate.id)}
            className={`flex items-center gap-2 px-3 py-1.5 rounded border text-sm transition-all ${
              candidate.id === nodeId
                ? 'border-[#58a6ff] bg-[#1c2025] text-[#58a6ff]'
                : 'border-[#30363d] bg-[#181c21] text-[#c0c7d4] hover:border-[#414752]'
            }`}
          >
            <span className="font-mono font-medium">{candidate.id}</span>
            <RolePill role={candidate.role} />
          </button>
        ))}
      </div>

      {!node ? (
        <Panel>
          <Empty title="Select a node" />
        </Panel>
      ) : (
        <>
          <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
            <Panel
              title={`${node.id} — state`}
              actions={
                <>
                  {node.alive ? (
                    <button
                      className="btn-obsidian btn-danger btn-sm"
                      onClick={() => void act(`crash ${node.id}`, () => api.crashNode(node.id))}
                    >
                      <Zap className="w-3 h-3" />
                      Crash
                    </button>
                  ) : (
                    <button
                      className="btn-obsidian btn-primary btn-sm"
                      onClick={() => void act(`recover ${node.id}`, () => api.recoverNode(node.id))}
                    >
                      <Power className="w-3 h-3" />
                      Recover
                    </button>
                  )}
                  <button
                    className="btn-obsidian btn-secondary btn-sm"
                    disabled={!node.alive}
                    onClick={() => void act(`restart ${node.id}`, () => api.restartNode(node.id))}
                  >
                    <RotateCcw className="w-3 h-3" />
                    Restart
                  </button>
                </>
              }
            >
              <div className="flex flex-col">
                <Field label="Alive" value={node.alive ? 'yes' : 'no'} />
                <Field label="Role" value={<RolePill role={node.role} />} mono={false} />
                <Field label="Current term" value={node.currentTerm ?? '—'} />
                <Field label="Voted for" value={node.votedFor ?? '(nobody)'} />
                <Field label="Believes leader is" value={node.leaderId ?? '(none)'} />
                <Field label="Commit index" value={node.commitIndex} />
                <Field label="Last applied" value={node.lastApplied} />
                <Field label="Log length" value={node.logLength} />
                {node.logBaseIndex ? <Field label="Log base index" value={node.logBaseIndex} /> : null}
                <Field label="Keys in store" value={node.storeKeys} />
                <Field label="Peers" value={node.peers.join(', ') || '—'} />
              </div>
            </Panel>

            <Panel title="Persistence" subtitle="What survives this node being killed">
              {node.walPath ? (
                <div className="flex flex-col">
                  <Field label="WAL" value={node.walPath} />
                  <Field label="WAL size" value={fmtBytes(node.walBytes)} />
                  <Field label="Snapshots" value={node.snapshotCount} />
                </div>
              ) : (
                <Empty
                  title="In-memory node"
                  hint="This cluster was started without persistence, so a restart resumes the same object rather than rebuilding from disk. Restart the cluster with the WAL option to change that."
                />
              )}
            </Panel>

            <Panel
              title="Replication state"
              subtitle={
                node.role === 'leader'
                  ? 'What this leader believes each follower holds'
                  : 'Leader-only state — Figure 2 clears it on step-down'
              }
            >
              {node.role === 'leader' && node.matchIndex && Object.keys(node.matchIndex).length ? (
                <div className="obsidian-table-container">
                  <table className="obsidian-table">
                    <thead>
                      <tr>
                        <th>Peer</th>
                        <th className="text-right">matchIndex</th>
                        <th className="text-right">nextIndex</th>
                      </tr>
                    </thead>
                    <tbody>
                      {Object.entries(node.matchIndex).map(([peer, match]) => (
                        <tr key={peer}>
                          <td className="font-mono">{peer}</td>
                          <td className="font-mono text-right text-[#4ade80]">{match}</td>
                          <td className="font-mono text-right">{node.nextIndex?.[peer] ?? '—'}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              ) : (
                <Empty title={node.role === 'leader' ? 'No peers tracked yet' : `${node.id} is not the leader`} />
              )}
            </Panel>
          </div>

          <div className="grid grid-cols-1 xl:grid-cols-2 gap-4">
            <Panel title={`${node.id} — replicated log`}>
              {!node.alive ? (
                <Empty title={`${node.id} is down`} hint="A crashed node serves no reads." />
              ) : log.error ? (
                <Failure message={log.error} />
              ) : !log.data?.entries.length ? (
                <Empty title="The log is empty" />
              ) : (
                <div className="obsidian-table-container max-h-96 overflow-y-auto">
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
                          <td className="text-[13px] font-mono text-[#8b919d]">
                            {entry.applied ? 'applied' : entry.committed ? 'committed' : 'pending'}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </Panel>

            <Panel title={`${node.id} — trace events`}>
              {events.error ? (
                <Failure message={events.error} />
              ) : (
                <EventFeed events={events.data ?? []} height="max-h-96" />
              )}
            </Panel>
          </div>
        </>
      )}
    </div>
  );
}
