import { useMemo } from 'react';
import { api } from '../../api/client';
import { useLab } from '../../api/LabContext';
import { useResource } from '../../api/useResource';
import { Empty, Failure, Metric, Panel, RolePill, ScreenHeader, Skeleton, fmtNum } from '../ui';

/**
 * Consensus as traffic: who is talking to whom, and what the cluster is doing about it.
 *
 * Only four of the twelve event kinds describe a message crossing the wire, and the
 * direction of each is read off the event itself rather than guessed. A `VOTE_GRANTED`
 * is emitted by the *voter* and names the candidate it answered; an
 * `APPEND_ENTRIES_REJECTED` is emitted by the *follower* and names the leader it
 * rejected — because why a follower rejected is knowable only on the follower, and
 * Figure 2's reply carries no room to say so.
 */
export function ConsensusDemoScreen() {
  const { status, nodes } = useLab();
  const rpc = useResource(() => api.rpc(80), [], { pollMs: 1000 });

  const trafficByPair = useMemo(() => {
    const counts = new Map<string, number>();
    for (const message of rpc.data ?? []) {
      if (!message.from || !message.to) continue;
      const pair = `${message.from}→${message.to}`;
      counts.set(pair, (counts.get(pair) ?? 0) + 1);
    }
    return counts;
  }, [rpc.data]);

  if (!status) return <Skeleton rows={5} />;

  const rejectRate = status.appendEntriesSent
    ? (status.appendEntriesRejected / status.appendEntriesSent) * 100
    : null;

  return (
    <div className="flex flex-col gap-4">
      <ScreenHeader
        title="Consensus & RPC"
        description="Leader election and log replication, as they actually happen on the wire. Replication and heartbeats are one code path — a caught-up follower simply receives an empty entries list."
      />

      <div className="grid grid-cols-2 md:grid-cols-3 xl:grid-cols-6 gap-3">
        <Metric label="Term" value={status.term} tone="accent" />
        <Metric label="Elections started" value={status.electionsStarted} />
        <Metric label="Leaders elected" value={status.leadersElected} />
        <Metric label="Term changes" value={status.termChanges} />
        <Metric label="AppendEntries sent" value={fmtNum(status.appendEntriesSent)} />
        <Metric
          label="Rejected"
          value={rejectRate === null ? String(status.appendEntriesRejected) : `${status.appendEntriesRejected} (${rejectRate.toFixed(1)}%)`}
          tone={status.appendEntriesRejected ? 'warn' : 'good'}
          hint="A rejection is a log-matching failure or a stale term — the leader walks nextIndex back and retries."
        />
      </div>

      <div className="grid grid-cols-1 xl:grid-cols-3 gap-4">
        <Panel title="Cluster ring" subtitle="Roles as each node sees itself" className="xl:col-span-1">
          <div className="flex flex-col gap-2">
            {nodes.map((node) => (
              <div
                key={node.id}
                className={`flex items-center justify-between gap-3 px-3 py-2 rounded border ${
                  node.role === 'leader'
                    ? 'border-[#58a6ff] bg-[#58a6ff]/5'
                    : node.alive
                      ? 'border-[#30363d] bg-[#181c21]'
                      : 'border-[#93000a]/50 bg-[#93000a]/10'
                }`}
              >
                <span className="font-mono text-sm font-medium">{node.id}</span>
                <RolePill role={node.role} />
                <span className="font-mono text-[13px] text-[#8b919d]">t{node.currentTerm ?? '—'}</span>
              </div>
            ))}
          </div>
        </Panel>

        <Panel
          title="Message flow"
          subtitle="Wire-crossing events in the recent trace, counted per directed pair"
          className="xl:col-span-2"
        >
          {!trafficByPair.size ? (
            <Empty title="No traffic recorded yet" />
          ) : (
            <div className="grid grid-cols-2 md:grid-cols-3 gap-2">
              {[...trafficByPair.entries()]
                .sort((a, b) => b[1] - a[1])
                .map(([pair, count]) => (
                  <div
                    key={pair}
                    className="flex items-center justify-between gap-2 px-2.5 py-1.5 rounded bg-[#181c21] border border-[#30363d]"
                  >
                    <span className="font-mono text-[13px] text-[#c0c7d4]">{pair}</span>
                    <span className="font-mono text-[13px] text-[#58a6ff] font-semibold">{count}</span>
                  </div>
                ))}
            </div>
          )}
        </Panel>
      </div>

      <Panel title="Recent RPCs" subtitle="Newest first">
        {rpc.error ? (
          <Failure message={rpc.error} />
        ) : !rpc.data?.length ? (
          <Empty title="No RPCs in the trace yet" />
        ) : (
          <div className="obsidian-table-container max-h-[26rem] overflow-y-auto">
            <table className="obsidian-table">
              <thead>
                <tr>
                  <th className="text-right">t (s)</th>
                  <th>From</th>
                  <th>To</th>
                  <th>Type</th>
                  <th className="text-right">Term</th>
                  <th>Detail</th>
                </tr>
              </thead>
              <tbody>
                {[...rpc.data].reverse().map((message) => (
                  <tr key={message.id}>
                    <td className="font-mono text-right text-[#8b919d]">
                      {message.timestamp.toFixed(3)}
                    </td>
                    <td className="font-mono">{message.from}</td>
                    <td className="font-mono">{message.to ?? '—'}</td>
                    <td>
                      <span
                        className={`status-pill ${
                          message.type.startsWith('AppendEntries')
                            ? message.type.endsWith('Response')
                              ? 'status-pill-down'
                              : 'status-pill-leader'
                            : 'status-pill-follower'
                        }`}
                      >
                        <span className="status-dot" />
                        {message.type}
                      </span>
                    </td>
                    <td className="font-mono text-right">{message.term ?? '—'}</td>
                    <td className="font-mono text-[13px] text-[#8b919d] truncate max-w-md">
                      {Object.entries(message.details)
                        .filter(([k]) => k !== 'peer' && k !== 'candidate' && k !== 'leader')
                        .map(([k, v]) => `${k}=${v}`)
                        .join(' ')}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </Panel>
    </div>
  );
}
