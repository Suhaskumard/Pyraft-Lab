import { useState } from 'react';
import { Command, Play, Power, Shield, Wifi, WifiOff } from 'lucide-react';
import { api } from '../api/client';
import { useLab } from '../api/LabContext';
import { HealthPill } from './ui';

/**
 * The one place a cluster is started and stopped.
 *
 * The start form is here rather than on a settings page because starting is not a
 * setting — it builds real nodes with real timers, and the knobs it takes (size, seed,
 * persistence) only mean anything at that moment.
 */
export function Header({ onOpenCommandPalette }: { onOpenCommandPalette: () => void }) {
  const { connected, cluster, status, act } = useLab();
  const [showStart, setShowStart] = useState(false);
  const [nodes, setNodes] = useState(3);
  const [seed, setSeed] = useState(0);
  const [persistent, setPersistent] = useState(false);
  const [busy, setBusy] = useState(false);

  const running = cluster?.running ?? false;

  const start = async () => {
    setBusy(true);
    const ok = await act('start cluster', () => api.startCluster({ nodes, seed, persistent }));
    setBusy(false);
    if (ok) setShowStart(false);
  };

  const stop = async () => {
    setBusy(true);
    await act('stop cluster', () => api.stopCluster());
    setBusy(false);
  };

  return (
    <header className="bg-[#101419] border-b border-[#30363d] px-4 py-2.5 flex items-center justify-between gap-4 sticky top-0 z-50">
      <div className="flex items-center gap-3 shrink-0">
        <div className="w-8 h-8 rounded bg-[#181c21] border border-[#58a6ff] flex items-center justify-center text-[#58a6ff]">
          <Shield className="w-4 h-4" />
        </div>
        <div>
          <div className="flex items-center gap-2">
            <h1 className="font-sans font-bold text-base text-[#e0e2ea] tracking-tight">
              PyRaft Lab
            </h1>
            <span
              className="text-[10px] font-mono px-1.5 py-0.5 rounded bg-[#1c2025] text-[#8b919d] border border-[#30363d] flex items-center gap-1"
              title={connected ? 'Live stream connected' : 'Reconnecting to the API…'}
            >
              {connected ? (
                <Wifi className="w-3 h-3 text-[#4ade80]" />
              ) : (
                <WifiOff className="w-3 h-3 text-[#ffb4ab]" />
              )}
              {connected ? 'live' : 'offline'}
            </span>
          </div>
          <p className="text-[11px] text-[#c0c7d4]">
            Deterministic consensus &amp; fault-injection laboratory
          </p>
        </div>
      </div>

      {running && status ? (
        <div className="hidden lg:flex items-center gap-5 bg-[#181c21] border border-[#30363d] px-3.5 py-1.5 rounded">
          <HealthPill status={status.status} />
          <Divider />
          <Reading label="Term" value={status.term} tone="text-[#58a6ff]" />
          <Divider />
          <Reading label="Leader" value={status.leader ?? '(none)'} />
          <Divider />
          <Reading label="Commit" value={status.commitIndex} tone="text-[#4ade80]" />
          <Divider />
          <Reading label="Nodes" value={`${status.nodeCount - status.down.length}/${status.nodeCount}`} />
        </div>
      ) : (
        <div className="hidden lg:block text-xs text-[#8b919d] font-mono">no cluster running</div>
      )}

      <div className="flex items-center gap-2 shrink-0">
        {running ? (
          <button className="btn-obsidian btn-danger btn-sm" onClick={stop} disabled={busy}>
            <Power className="w-3.5 h-3.5" />
            <span className="hidden sm:inline">Stop</span>
          </button>
        ) : (
          <button
            className="btn-obsidian btn-primary btn-sm"
            onClick={() => setShowStart((open) => !open)}
          >
            <Play className="w-3.5 h-3.5" />
            <span className="hidden sm:inline">Start cluster</span>
          </button>
        )}

        <button
          onClick={onOpenCommandPalette}
          className="btn-obsidian btn-secondary btn-sm font-mono"
          title="Command palette (Ctrl+K)"
        >
          <Command className="w-3.5 h-3.5 text-[#58a6ff]" />
          <span className="hidden sm:inline">Ctrl+K</span>
        </button>
      </div>

      {showStart && !running && (
        <div className="absolute right-4 top-14 w-72 obsidian-card z-50 animate-fade-in">
          <h3 className="label-caps mb-3">Start a cluster</h3>
          <div className="flex flex-col gap-3">
            <NumberField label="Nodes" value={nodes} min={1} max={15} onChange={setNodes} />
            <NumberField label="Seed" value={seed} min={0} max={999999} onChange={setSeed} />
            <label className="flex items-center gap-2 text-xs text-[#c0c7d4] cursor-pointer">
              <input
                type="checkbox"
                checked={persistent}
                onChange={(e) => setPersistent(e.target.checked)}
                className="accent-[#58a6ff]"
              />
              Persist each node to a WAL
            </label>
            <p className="text-[10px] text-[#8b919d] leading-relaxed">
              Persistence is what makes a restart real: a WAL-backed node is rebuilt from disk
              rather than resumed in memory, and it is the only way the WAL and snapshot pages
              have anything to show.
            </p>
            <div className="flex gap-2 pt-1">
              <button className="btn-obsidian btn-primary btn-sm flex-1" onClick={start} disabled={busy}>
                {busy ? 'Starting…' : 'Start'}
              </button>
              <button className="btn-obsidian btn-ghost btn-sm" onClick={() => setShowStart(false)}>
                Cancel
              </button>
            </div>
          </div>
        </div>
      )}
    </header>
  );
}

const Divider = () => <div className="h-4 w-px bg-[#30363d]" />;

function Reading({ label, value, tone = 'text-[#e0e2ea]' }: { label: string; value: React.ReactNode; tone?: string }) {
  return (
    <div className="flex items-center gap-2 font-mono text-xs">
      <span className="text-[#8b919d]">{label}:</span>
      <span className={`${tone} font-medium`}>{value}</span>
    </div>
  );
}

function NumberField({
  label,
  value,
  min,
  max,
  onChange,
}: {
  label: string;
  value: number;
  min: number;
  max: number;
  onChange: (value: number) => void;
}) {
  return (
    <label className="flex items-center justify-between gap-3 text-xs text-[#c0c7d4]">
      {label}
      <input
        type="number"
        min={min}
        max={max}
        value={value}
        onChange={(e) => onChange(Number(e.target.value))}
        className="w-24 bg-[#0b0e14] border border-[#30363d] rounded px-2 py-1 font-mono text-xs text-[#e0e2ea] focus:border-[#58a6ff] outline-none"
      />
    </label>
  );
}
