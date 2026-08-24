import { useState } from 'react';
import { Camera, RefreshCw } from 'lucide-react';
import { api } from '../../api/client';
import { useLab } from '../../api/LabContext';
import { useResource } from '../../api/useResource';
import { Empty, Failure, Panel, ScreenHeader } from '../ui';

/**
 * Log compaction, on disk only.
 *
 * Taking a snapshot writes it, rewrites the WAL as snapshot-plus-tail, and leaves the
 * in-memory log whole. Dropping the in-memory prefix would trade a bounded disk cost
 * for an unbounded correctness one: with no InstallSnapshot RPC, a follower further
 * behind than the snapshot boundary can only be repaired from the leader's log, and a
 * leader that had discarded that prefix could no longer repair it.
 */
export function SnapshotsScreen() {
  const { nodes, notify } = useLab();
  const [nodeId, setNodeId] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const selected = nodeId ?? nodes[0]?.id ?? null;

  const listing = useResource(() => api.nodeSnapshots(selected!), [selected], {
    enabled: Boolean(selected),
    pollMs: 4000,
  });

  const take = async () => {
    if (!selected) return;
    setBusy(true);
    try {
      const meta = await api.takeSnapshot(selected);
      notify('success', `${selected}: snapshot at index ${meta.lastIncludedIndex} (${meta.keys} keys)`);
      await listing.reload();
    } catch (error) {
      notify('error', error instanceof Error ? error.message : String(error));
    } finally {
      setBusy(false);
    }
  };

  const data = listing.data;

  return (
    <div className="flex flex-col gap-4">
      <ScreenHeader
        title="Snapshot Store"
        description="One file per snapshot, never one file overwritten in place — an overwrite has a window in which the only copy is half-written, and this is the file a restart trusts instead of the log."
        actions={
          <>
            <button className="btn-obsidian btn-secondary btn-sm" onClick={() => void listing.reload()}>
              <RefreshCw className="w-3.5 h-3.5" />
              Refresh
            </button>
            <button
              className="btn-obsidian btn-primary btn-sm"
              onClick={() => void take()}
              disabled={busy || !data?.supported}
            >
              <Camera className="w-3.5 h-3.5" />
              Take snapshot
            </button>
          </>
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
            {node.snapshotCount > 0 && (
              <span className="ml-2 text-[12px] text-[#4ade80]">{node.snapshotCount}</span>
            )}
          </button>
        ))}
      </div>

      <Panel
        title={selected ? `${selected} — snapshots` : 'Snapshots'}
        subtitle={data?.directory}
      >
        {listing.error ? (
          <Failure message={listing.error} />
        ) : !data ? (
          <Empty title="Select a node" />
        ) : !data.supported ? (
          <Empty
            title="Snapshots need a persistent cluster"
            hint={data.reason}
          />
        ) : !data.snapshots.length ? (
          <Empty
            title="No snapshots yet"
            hint="Write some keys, then take one. A snapshot only ever covers applied entries, which is why the prefix it replaces cannot have diverged."
          />
        ) : (
          <div className="obsidian-table-container">
            <table className="obsidian-table">
              <thead>
                <tr>
                  <th>File</th>
                  <th className="text-right">Last included index</th>
                  <th className="text-right">Term</th>
                  <th className="text-right">Keys</th>
                  <th>Checksum</th>
                </tr>
              </thead>
              <tbody>
                {data.snapshots.map((snapshot) => (
                  <tr key={snapshot.snapshotId}>
                    <td className="font-mono text-[#58a6ff]">{snapshot.snapshotId}</td>
                    <td className="font-mono text-right">{snapshot.lastIncludedIndex}</td>
                    <td className="font-mono text-right">{snapshot.lastIncludedTerm}</td>
                    <td className="font-mono text-right">{snapshot.keys}</td>
                    <td className="font-mono text-[13px] text-[#8b919d]">
                      {snapshot.checksum}
                      <span className="ml-2 text-[#4ade80]">verified</span>
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
