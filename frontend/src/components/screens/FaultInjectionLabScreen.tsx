import { useEffect, useState } from 'react';
import { Heart, RotateCcw, Scissors, Zap } from 'lucide-react';
import { api } from '../../api/client';
import { useLab } from '../../api/LabContext';
import { useResource } from '../../api/useResource';
import { Empty, Failure, Metric, Panel, RolePill, ScreenHeader, fmtNum } from '../ui';
import { EventFeed } from './ClusterOverviewScreen';

/**
 * Do something to the network and watch what Raft does about it.
 *
 * The faults here are the real ones a scenario file can name, applied to the live
 * cluster's own `SimulatedNetwork`. They compose the way they do in the simulator: a
 * message dropped for a partition never even reaches the packet-loss roll, and a node
 * named in no partition group can still reach everyone.
 */
export function FaultInjectionLabScreen() {
  const { nodes, status, events, act } = useLab();
  const faults = useResource(() => api.faults(), [], { pollMs: 2000 });

  const [group, setGroup] = useState<Set<string>>(new Set());
  const [minMs, setMinMs] = useState(0);
  const [maxMs, setMaxMs] = useState(0);
  const [loss, setLoss] = useState(0);

  // Seed the sliders from the network's actual configuration, once it is known.
  useEffect(() => {
    if (!faults.data) return;
    setMinMs((prev) => (prev === 0 ? Math.round(faults.data!.minLatencyMs) : prev));
    setMaxMs((prev) => (prev === 0 ? Math.round(faults.data!.maxLatencyMs) : prev));
    setLoss((prev) => (prev === 0 ? Math.round(faults.data!.lossProbability * 100) : prev));
  }, [faults.data]);

  const toggle = (nodeId: string) =>
    setGroup((prev) => {
      const next = new Set(prev);
      if (next.has(nodeId)) next.delete(nodeId);
      else next.add(nodeId);
      return next;
    });

  const others = nodes.map((n) => n.id).filter((id) => !group.has(id));
  const canSplit = group.size > 0 && others.length > 0;

  const after = async (label: string, work: () => Promise<unknown>) => {
    await act(label, work);
    await faults.reload();
  };

  return (
    <div className="flex flex-col gap-4">
      <ScreenHeader
        title="Fault Injection Lab"
        description="Latency, packet loss, partitions and crashes, applied to the live cluster. A node has no way to tell any of these apart from a slow peer — which is exactly the point."
        actions={
          <button
            className="btn-obsidian btn-secondary btn-sm"
            onClick={() => void after('reset faults', () => api.resetFaults())}
          >
            <RotateCcw className="w-3.5 h-3.5" />
            Reset network
          </button>
        }
      />

      {faults.error && <Failure message={faults.error} />}

      <div className="grid grid-cols-2 md:grid-cols-3 xl:grid-cols-6 gap-3">
        <Metric
          label="Health"
          value={status?.status ?? '—'}
          tone={status?.status === 'HEALTHY' ? 'good' : status?.status === 'DEGRADED' ? 'warn' : 'bad'}
        />
        <Metric label="Nodes down" value={status?.down.length ?? 0} tone={status?.down.length ? 'bad' : 'good'} />
        <Metric
          label="Partitioned"
          value={faults.data?.partitioned ? 'yes' : 'no'}
          tone={faults.data?.partitioned ? 'bad' : 'good'}
        />
        <Metric
          label="Link latency"
          value={
            faults.data && faults.data.maxLatencyMs > 0
              ? `${faults.data.minLatencyMs.toFixed(0)}–${faults.data.maxLatencyMs.toFixed(0)} ms`
              : 'ideal'
          }
        />
        <Metric
          label="Packet loss"
          value={faults.data ? `${(faults.data.lossProbability * 100).toFixed(0)}%` : '—'}
          tone={faults.data?.lossProbability ? 'warn' : 'good'}
        />
        <Metric
          label="Dropped"
          value={fmtNum(status?.droppedPackets)}
          hint={`${status?.droppedLoss ?? 0} to loss, ${status?.droppedPartition ?? 0} to partition`}
        />
      </div>

      <div className="grid grid-cols-1 xl:grid-cols-3 gap-4">
        <Panel
          title="Partition the network"
          subtitle="Pick one side; everyone else forms the other"
        >
          <div className="flex flex-col gap-3">
            <div className="flex flex-wrap gap-2">
              {nodes.map((node) => (
                <button
                  key={node.id}
                  onClick={() => toggle(node.id)}
                  className={`flex items-center gap-2 px-2.5 py-1.5 rounded border text-xs transition-all ${
                    group.has(node.id)
                      ? 'border-[#8957e5] bg-[#8957e5]/10 text-[#d3bbff]'
                      : 'border-[#30363d] bg-[#181c21] text-[#c0c7d4] hover:border-[#414752]'
                  }`}
                >
                  <span className="font-mono">{node.id}</span>
                  <RolePill role={node.role} />
                </button>
              ))}
            </div>

            <p className="text-[11px] text-[#8b919d] font-mono">
              {canSplit
                ? `{${[...group].join(', ')}}  ✂  {${others.join(', ')}}`
                : 'select at least one node, and leave at least one out'}
            </p>

            {canSplit && group.size * 2 === nodes.length && (
              <p className="text-[11px] text-[#ffba42]">
                An even split leaves neither side with a majority — expect no leader anywhere.
              </p>
            )}

            <div className="flex gap-2">
              <button
                className="btn-obsidian btn-danger btn-sm flex-1"
                disabled={!canSplit}
                onClick={() =>
                  void after('partition', () => api.partition([[...group], others])).then(() =>
                    setGroup(new Set())
                  )
                }
              >
                <Scissors className="w-3 h-3" />
                Split
              </button>
              <button
                className="btn-obsidian btn-primary btn-sm flex-1"
                disabled={!faults.data?.partitioned}
                onClick={() => void after('heal', () => api.heal())}
              >
                <Heart className="w-3 h-3" />
                Heal
              </button>
            </div>

            {faults.data?.partitioned && (
              <div className="text-[11px] font-mono text-[#ffb4ab] border border-[#93000a] rounded p-2">
                current split:{' '}
                {faults.data.partitionGroups.map((g) => `{${g.join(', ')}}`).join(' | ')}
              </div>
            )}
          </div>
        </Panel>

        <Panel title="Degrade every link" subtitle="Applied network-wide">
          <div className="flex flex-col gap-4">
            <div className="flex flex-col gap-2">
              <div className="flex items-center justify-between">
                <span className="label-caps">Latency</span>
                <span className="font-mono text-xs text-[#58a6ff]">
                  {minMs}–{maxMs} ms
                </span>
              </div>
              <Slider label="min" value={minMs} max={500} onChange={setMinMs} />
              <Slider label="max" value={maxMs} max={500} onChange={setMaxMs} />
              <button
                className="btn-obsidian btn-secondary btn-sm"
                disabled={minMs > maxMs}
                onClick={() => void after('set latency', () => api.setLatency(minMs, maxMs))}
              >
                Apply latency
              </button>
              {minMs > maxMs && (
                <p className="text-[11px] text-[#ffb4ab]">min must not exceed max</p>
              )}
            </div>

            <div className="flex flex-col gap-2 pt-3 border-t border-[#30363d]">
              <div className="flex items-center justify-between">
                <span className="label-caps">Packet loss</span>
                <span className="font-mono text-xs text-[#ffba42]">{loss}%</span>
              </div>
              <Slider label="drop" value={loss} max={100} onChange={setLoss} />
              <button
                className="btn-obsidian btn-secondary btn-sm"
                onClick={() => void after('set loss', () => api.setLoss(loss / 100))}
              >
                Apply loss
              </button>
              {loss >= 50 && (
                <p className="text-[11px] text-[#ffba42]">
                  Above roughly half, a cluster spends most of its time electing rather than
                  replicating — which is itself worth watching.
                </p>
              )}
            </div>
          </div>
        </Panel>

        <Panel title="Crash a node" subtitle="A crash discards the running task and leaves the transport">
          {!nodes.length ? (
            <Empty title="No nodes" />
          ) : (
            <div className="flex flex-col gap-2">
              {nodes.map((node) => (
                <div
                  key={node.id}
                  className="flex items-center justify-between gap-2 px-2.5 py-1.5 rounded bg-[#181c21] border border-[#30363d]"
                >
                  <span className="font-mono text-xs">{node.id}</span>
                  <RolePill role={node.role} />
                  {node.alive ? (
                    <button
                      className="btn-obsidian btn-danger btn-sm"
                      onClick={() => void after(`crash ${node.id}`, () => api.crashNode(node.id))}
                    >
                      <Zap className="w-3 h-3" />
                      Crash
                    </button>
                  ) : (
                    <button
                      className="btn-obsidian btn-primary btn-sm"
                      onClick={() => void after(`recover ${node.id}`, () => api.recoverNode(node.id))}
                    >
                      <RotateCcw className="w-3 h-3" />
                      Recover
                    </button>
                  )}
                </div>
              ))}
            </div>
          )}
        </Panel>
      </div>

      <Panel title="What the cluster did about it" subtitle="Live trace">
        <EventFeed events={events} />
      </Panel>
    </div>
  );
}

function Slider({
  label,
  value,
  max,
  onChange,
}: {
  label: string;
  value: number;
  max: number;
  onChange: (value: number) => void;
}) {
  return (
    <label className="flex items-center gap-3 text-[11px] text-[#8b919d]">
      <span className="w-8 shrink-0">{label}</span>
      <input
        type="range"
        min={0}
        max={max}
        value={value}
        onChange={(event) => onChange(Number(event.target.value))}
        className="flex-1 accent-[#58a6ff]"
      />
      <input
        type="number"
        min={0}
        max={max}
        value={value}
        onChange={(event) => onChange(Number(event.target.value))}
        className="w-16 bg-[#0b0e14] border border-[#30363d] rounded px-1.5 py-0.5 font-mono text-xs text-[#e0e2ea] focus:border-[#58a6ff] outline-none"
      />
    </label>
  );
}
