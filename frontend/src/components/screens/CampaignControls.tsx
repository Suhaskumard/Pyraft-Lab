import type { ReactNode } from 'react';
import { Panel } from '../ui';

/** The four knobs stress and chaos share. Same names as the CLI flags they map to. */
export function CampaignControls({
  nodes,
  setNodes,
  duration,
  setDuration,
  trials,
  setTrials,
  seed,
  setSeed,
  running,
  onRun,
  icon,
  label,
  note,
}: {
  nodes: number;
  setNodes: (value: number) => void;
  duration: string;
  setDuration: (value: string) => void;
  trials: number;
  setTrials: (value: number) => void;
  seed: number;
  setSeed: (value: number) => void;
  running: boolean;
  onRun: () => void;
  icon: ReactNode;
  label: string;
  note?: string;
}) {
  return (
    <Panel
      title="Campaign"
      subtitle={note ?? 'Every trial derives its faults from (seed, trial index) alone, so a failure is a fixed, reproducible case.'}
    >
      <div className="flex flex-wrap items-end gap-3">
        <Knob label="Nodes" value={nodes} min={1} max={15} onChange={setNodes} />
        <TextKnob label="Duration" value={duration} onChange={setDuration} placeholder="10s" />
        <Knob label="Trials" value={trials} min={1} max={200} onChange={setTrials} />
        <Knob label="Base seed" value={seed} min={0} max={999999} onChange={setSeed} />
        <button className="btn-obsidian btn-primary btn-sm" onClick={onRun} disabled={running}>
          {icon}
          {running ? 'Running…' : label}
        </button>
      </div>
      <p className="text-[11px] text-[#8b919d] mt-3">
        Trials run on a virtual clock in a worker thread of their own, so a long campaign
        cannot starve a live cluster's wall-clock timers.
      </p>
    </Panel>
  );
}

function Knob({
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
    <label className="flex flex-col gap-1">
      <span className="label-caps">{label}</span>
      <input
        type="number"
        min={min}
        max={max}
        value={value}
        onChange={(event) => onChange(Number(event.target.value))}
        className="w-24 bg-[#0b0e14] border border-[#30363d] rounded px-2 py-1.5 font-mono text-xs text-[#e0e2ea] focus:border-[#58a6ff] outline-none"
      />
    </label>
  );
}

function TextKnob({
  label,
  value,
  onChange,
  placeholder,
}: {
  label: string;
  value: string;
  onChange: (value: string) => void;
  placeholder?: string;
}) {
  return (
    <label className="flex flex-col gap-1">
      <span className="label-caps">{label}</span>
      <input
        value={value}
        placeholder={placeholder}
        onChange={(event) => onChange(event.target.value)}
        className="w-24 bg-[#0b0e14] border border-[#30363d] rounded px-2 py-1.5 font-mono text-xs text-[#e0e2ea] focus:border-[#58a6ff] outline-none"
      />
    </label>
  );
}
