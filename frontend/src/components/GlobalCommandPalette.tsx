import { useEffect, useMemo, useRef, useState } from 'react';
import { CornerDownLeft, Search, TerminalSquare } from 'lucide-react';
import { api } from '../api/client';
import { useLab } from '../api/LabContext';
import type { ScreenId } from './Navigation';

/**
 * Navigate, or type a cluster command.
 *
 * Anything matching a REPL verb is sent to `POST /api/cluster/exec`, which runs the
 * *same* dispatcher `pyraft-lab cluster start` uses — so `put k v` typed here and
 * typed at the terminal are one implementation, not two that can drift.
 */

const PAGES: { id: ScreenId; label: string; keywords: string }[] = [
  { id: 'cluster_overview', label: 'Cluster Overview', keywords: 'live status topology health' },
  { id: 'node_inspector', label: 'Node Inspector', keywords: 'node raft state log match index' },
  { id: 'kv_store', label: 'Key-Value Store', keywords: 'kv keys put get delete cas' },
  { id: 'consensus_demo', label: 'Consensus & RPC', keywords: 'rpc append entries vote messages' },
  { id: 'wal_inspector', label: 'WAL Inspector', keywords: 'wal write ahead log durability crc' },
  { id: 'snapshots', label: 'Snapshot Store', keywords: 'snapshot compaction' },
  { id: 'architecture', label: 'Architecture Map', keywords: 'layers design transport' },
  { id: 'fault_injection', label: 'Fault Injection', keywords: 'partition latency loss crash heal' },
  { id: 'experiment_runner', label: 'Experiment Runner', keywords: 'scenario yaml run' },
  { id: 'deterministic_replay', label: 'Deterministic Replay', keywords: 'replay reproduce seed' },
  { id: 'benchmark_lab', label: 'Benchmark Lab', keywords: 'benchmark throughput latency grid' },
  { id: 'stress_campaign', label: 'Stress Campaign', keywords: 'stress trials archetype' },
  { id: 'chaos_campaign', label: 'Chaos Campaign', keywords: 'chaos data loss invariant' },
  { id: 'results_explorer', label: 'Results Explorer', keywords: 'results runs persisted' },
  { id: 'linearizability', label: 'Linearizability', keywords: 'consistency history checker' },
  { id: 'run_comparison', label: 'Run Comparison', keywords: 'compare diff runs' },
  { id: 'diagnostics', label: 'Jobs & Diagnostics', keywords: 'jobs errors campaign progress' },
  { id: 'settings', label: 'Cluster Settings', keywords: 'config yaml timeouts seed' },
];

/** Verbs `POST /api/cluster/exec` accepts. Typing one switches the palette to console
 *  mode so Enter runs it rather than navigating somewhere. */
const VERBS = [
  'put',
  'get',
  'delete',
  'cas',
  'status',
  'topology',
  'show-log',
  'node',
  'restart',
  'trace',
  'help',
];

interface Props {
  isOpen: boolean;
  onClose: () => void;
  onSelectScreen: (screen: ScreenId) => void;
}

export function GlobalCommandPalette({ isOpen, onClose, onSelectScreen }: Props) {
  const { cluster, notify } = useLab();
  const [query, setQuery] = useState('');
  const [output, setOutput] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const inputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    if (isOpen) {
      setQuery('');
      setOutput(null);
      window.setTimeout(() => inputRef.current?.focus(), 0);
    }
  }, [isOpen]);

  const verb = query.trim().split(/\s+/)[0]?.toLowerCase() ?? '';
  const isCommand = VERBS.includes(verb);

  const matches = useMemo(() => {
    const needle = query.trim().toLowerCase();
    if (!needle) return PAGES;
    return PAGES.filter(
      (page) =>
        page.label.toLowerCase().includes(needle) || page.keywords.includes(needle)
    );
  }, [query]);

  if (!isOpen) return null;

  const submit = async () => {
    if (isCommand) {
      if (!cluster?.running) {
        notify('error', 'no cluster is running — start one before typing cluster commands');
        return;
      }
      setBusy(true);
      try {
        const result = await api.exec(query.trim());
        setOutput(result.output || '(no output)');
      } catch (error) {
        setOutput(error instanceof Error ? error.message : String(error));
      } finally {
        setBusy(false);
      }
      return;
    }
    if (matches.length) {
      onSelectScreen(matches[0].id);
      onClose();
    }
  };

  return (
    <div className="modal-overlay" onClick={onClose}>
      <div
        className="modal-content max-w-2xl w-full"
        onClick={(event) => event.stopPropagation()}
        role="dialog"
        aria-modal="true"
      >
        <div className="flex items-center gap-2.5 pb-3 border-b border-[#30363d]">
          {isCommand ? (
            <TerminalSquare className="w-4 h-4 text-[#4ade80] shrink-0" />
          ) : (
            <Search className="w-4 h-4 text-[#58a6ff] shrink-0" />
          )}
          <input
            ref={inputRef}
            value={query}
            onChange={(event) => setQuery(event.target.value)}
            onKeyDown={(event) => {
              if (event.key === 'Enter') void submit();
              if (event.key === 'Escape') onClose();
            }}
            placeholder="Jump to a page, or type a cluster command (put k v, topology, trace)…"
            className="flex-1 bg-transparent outline-none text-base text-[#e0e2ea] placeholder:text-[#8b919d] font-mono"
          />
          <kbd className="text-[12px] font-mono text-[#8b919d] border border-[#30363d] rounded px-1.5 py-0.5">
            Esc
          </kbd>
        </div>

        {isCommand ? (
          <div className="pt-3 flex flex-col gap-2">
            <div className="flex items-center justify-between">
              <span className="label-caps">Cluster console</span>
              <button
                className="btn-obsidian btn-primary btn-sm"
                onClick={() => void submit()}
                disabled={busy}
              >
                <CornerDownLeft className="w-3 h-3" />
                {busy ? 'Running…' : 'Run'}
              </button>
            </div>
            {output !== null && <pre className="terminal-block max-h-72 overflow-auto">{output}</pre>}
          </div>
        ) : (
          <ul className="pt-2 max-h-80 overflow-y-auto">
            {matches.length === 0 && (
              <li className="text-sm text-[#8b919d] py-4 text-center">
                Nothing matches “{query}”.
              </li>
            )}
            {matches.map((page, index) => (
              <li key={page.id}>
                <button
                  onClick={() => {
                    onSelectScreen(page.id);
                    onClose();
                  }}
                  className={`w-full text-left px-2.5 py-2 rounded text-sm flex items-center justify-between gap-3 hover:bg-[#1c2025] ${
                    index === 0 ? 'bg-[#1c2025] text-[#58a6ff]' : 'text-[#c0c7d4]'
                  }`}
                >
                  <span>{page.label}</span>
                  {index === 0 && (
                    <span className="text-[12px] font-mono text-[#8b919d] flex items-center gap-1">
                      <CornerDownLeft className="w-3 h-3" /> open
                    </span>
                  )}
                </button>
              </li>
            ))}
          </ul>
        )}
      </div>
    </div>
  );
}
