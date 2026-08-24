import { useState } from 'react';
import { RefreshCw } from 'lucide-react';
import { api } from '../../api/client';
import { useLab } from '../../api/LabContext';
import { useResource } from '../../api/useResource';
import { Empty, Failure, Field, Metric, Panel, ScreenHeader, fmtBytes } from '../ui';

/**
 * What a restart would actually recover, read straight off disk.
 *
 * This is the real `pyraft-lab inspect-wal`, not a rendering of the node's memory: the
 * API re-reads the file, verifies every record's CRC32, and folds the result the same
 * way recovery does. A record only appears here if its checksum verified, and the
 * listing stops at the first one that did not — which is exactly how a torn tail shows
 * itself.
 */
export function WalInspectorScreen() {
  const { nodes } = useLab();
  const [nodeId, setNodeId] = useState<string | null>(null);
  const selected = nodeId ?? nodes[0]?.id ?? null;

  const wal = useResource(() => api.nodeWal(selected!), [selected], {
    enabled: Boolean(selected),
    pollMs: 3000,
  });

  const report = wal.data;

  return (
    <div className="flex flex-col gap-4">
      <ScreenHeader
        title="WAL Inspector"
        description="One line per record: a CRC32, then the payload as canonical JSON. Text on purpose — this file is evidence in a failure investigation, and being able to read it beats the bytes a binary frame would save."
        actions={
          <button className="btn-obsidian btn-secondary btn-sm" onClick={() => void wal.reload()}>
            <RefreshCw className="w-3.5 h-3.5" />
            Re-read
          </button>
        }
      />

      <div className="flex flex-wrap gap-2">
        {nodes.map((node) => (
          <button
            key={node.id}
            onClick={() => setNodeId(node.id)}
            className={`px-3 py-1.5 rounded border font-mono text-sm transition-all ${
              node.id === selected
                ? 'border-[#58a6ff] bg-[#1c2025] text-[#58a6ff]'
                : 'border-[#30363d] bg-[#181c21] text-[#c0c7d4] hover:border-[#414752]'
            }`}
          >
            {node.id}
          </button>
        ))}
      </div>

      {wal.error ? (
        <Panel><Failure message={wal.error} /></Panel>
      ) : !report ? (
        <Panel><Empty title="Select a node" /></Panel>
      ) : !report.exists ? (
        <Panel>
          <Empty
            title="This node has no WAL"
            hint={
              report.reason ??
              'Start the cluster with persistence enabled to give each node a write-ahead log.'
            }
          />
        </Panel>
      ) : report.corrupt ? (
        <Panel title="WAL refused">
          <Failure message={report.reason ?? 'unreadable'} />
          <p className="text-[13px] text-[#8b919d] mt-3 leading-relaxed">
            A damaged <em>final</em> record is a torn write and recovers cleanly. Damage with
            valid records after it is refused instead: recovery is a fold, and folding past a
            missing record would produce state this node never had — skip a truncation and
            entries reappear that a leader had already overwritten.
          </p>
        </Panel>
      ) : (
        <>
          <div className="grid grid-cols-2 md:grid-cols-4 xl:grid-cols-6 gap-3">
            <Metric label="Records valid" value={report.recordsValid ?? 0} tone="good" />
            <Metric
              label="Records read"
              value={report.recordsRead ?? 0}
              tone={report.droppedRecords ? 'warn' : 'default'}
            />
            <Metric label="Valid bytes" value={fmtBytes(report.validBytes)} />
            <Metric label="Recovers term" value={report.term ?? 0} tone="accent" />
            <Metric label="Commit index" value={report.commitIndex ?? 0} />
            <Metric label="Last index" value={report.lastIndex ?? 0} />
          </div>

          {report.truncatedTail && (
            <Failure
              message={`A torn final record was discarded (${report.droppedRecords} record(s)). Nothing was ever acknowledged on the strength of it, so this recovers cleanly.`}
            />
          )}

          <div className="grid grid-cols-1 xl:grid-cols-3 gap-4">
            <Panel title="What a restart recovers">
              <div className="flex flex-col">
                <Field label="Path" value={report.path ?? '—'} />
                <Field label="Node id" value={report.nodeId || '—'} />
                <Field label="Term" value={report.term ?? 0} />
                <Field label="Voted for" value={report.votedFor ?? '(nobody)'} />
                <Field label="Commit index" value={report.commitIndex ?? 0} />
                <Field label="Base index" value={report.baseIndex ?? 0} />
                <Field label="Last index" value={report.lastIndex ?? 0} />
              </div>
              {report.reportLines?.length ? (
                <pre className="terminal-block mt-3 max-h-48 overflow-y-auto">
                  {report.reportLines.join('\n')}
                </pre>
              ) : null}
            </Panel>

            <Panel
              title="Records"
              subtitle={`${report.records?.length ?? 0} verified record${report.records?.length === 1 ? '' : 's'}, in file order`}
              className="xl:col-span-2"
            >
              {!report.records?.length ? (
                <Empty title="Nothing written yet" />
              ) : (
                <div className="obsidian-table-container max-h-[28rem] overflow-y-auto">
                  <table className="obsidian-table">
                    <thead>
                      <tr>
                        <th className="text-right">#</th>
                        <th>Kind</th>
                        <th className="text-right">Index</th>
                        <th className="text-right">Term</th>
                        <th>Payload</th>
                        <th>CRC</th>
                      </tr>
                    </thead>
                    <tbody>
                      {report.records.map((record) => (
                        <tr key={record.position}>
                          <td className="font-mono text-right text-[#8b919d]">{record.position}</td>
                          <td>
                            <span className="status-pill status-pill-follower">
                              <span className="status-dot" />
                              {record.kind?.toUpperCase()}
                            </span>
                          </td>
                          <td className="font-mono text-right">{record.index ?? '—'}</td>
                          <td className="font-mono text-right text-[#58a6ff]">{record.term ?? '—'}</td>
                          <td className="font-mono text-[13px] truncate max-w-lg">{record.detail}</td>
                          <td>
                            <span className="status-pill status-pill-healthy">
                              <span className="status-dot" />
                              VALID
                            </span>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </Panel>
          </div>
        </>
      )}
    </div>
  );
}
