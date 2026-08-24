import type { FC } from 'react';
import { useState } from 'react';
import {
  Activity,
  AlertTriangle,
  BarChart3,
  BoxSelect,
  ChevronsLeft,
  ChevronsRight,
  Database,
  FileCode,
  Flame,
  FolderKanban,
  GitCompare,
  HardDrive,
  Layers,
  Network,
  RotateCcw,
  Server,
  Sliders,
  Terminal,
  Zap,
} from 'lucide-react';

export type ScreenId =
  | 'cluster_overview'
  | 'node_inspector'
  | 'kv_store'
  | 'consensus_demo'
  | 'wal_inspector'
  | 'snapshots'
  | 'architecture'
  | 'fault_injection'
  | 'experiment_runner'
  | 'deterministic_replay'
  | 'benchmark_lab'
  | 'stress_campaign'
  | 'chaos_campaign'
  | 'linearizability'
  | 'run_comparison'
  | 'diagnostics'
  | 'results_explorer'
  | 'settings';

interface NavigationProps {
  activeScreen: ScreenId;
  onSelectScreen: (screen: ScreenId) => void;
  clusterRunning: boolean;
  liveScreens: ReadonlySet<ScreenId>;
}

const GROUPS: { label: string; items: { id: ScreenId; label: string; icon: FC<{ className?: string }> }[] }[] = [
  {
    label: 'Live Cluster',
    items: [
      { id: 'cluster_overview', label: 'Cluster Overview', icon: Activity },
      { id: 'node_inspector', label: 'Node Inspector', icon: Server },
      { id: 'kv_store', label: 'Key-Value Store', icon: Database },
      { id: 'consensus_demo', label: 'Consensus & RPC', icon: Network },
    ],
  },
  {
    label: 'Storage & Engine',
    items: [
      { id: 'wal_inspector', label: 'WAL Inspector', icon: HardDrive },
      { id: 'snapshots', label: 'Snapshot Store', icon: FolderKanban },
      { id: 'architecture', label: 'Architecture Map', icon: Layers },
    ],
  },
  {
    label: 'Laboratory',
    items: [
      { id: 'fault_injection', label: 'Fault Injection', icon: Zap },
      { id: 'experiment_runner', label: 'Experiment Runner', icon: FileCode },
      { id: 'deterministic_replay', label: 'Deterministic Replay', icon: RotateCcw },
    ],
  },
  {
    label: 'Campaigns',
    items: [
      { id: 'benchmark_lab', label: 'Benchmark Lab', icon: BarChart3 },
      { id: 'stress_campaign', label: 'Stress Campaign', icon: Flame },
      { id: 'chaos_campaign', label: 'Chaos Campaign', icon: AlertTriangle },
    ],
  },
  {
    label: 'Results',
    items: [
      { id: 'results_explorer', label: 'Results Explorer', icon: BoxSelect },
      { id: 'linearizability', label: 'Linearizability', icon: GitCompare },
      { id: 'run_comparison', label: 'Run Comparison', icon: GitCompare },
    ],
  },
  {
    label: 'System',
    items: [
      { id: 'diagnostics', label: 'Jobs & Diagnostics', icon: Terminal },
      { id: 'settings', label: 'Cluster Settings', icon: Sliders },
    ],
  },
];

const COLLAPSE_KEY = 'pyraft-lab:nav-collapsed';

export function Navigation({
  activeScreen,
  onSelectScreen,
  clusterRunning,
  liveScreens,
}: NavigationProps) {
  const [collapsed, setCollapsed] = useState(() => {
    try {
      return localStorage.getItem(COLLAPSE_KEY) === '1';
    } catch {
      return false;
    }
  });

  const toggle = () => {
    setCollapsed((value) => {
      const next = !value;
      try {
        localStorage.setItem(COLLAPSE_KEY, next ? '1' : '0');
      } catch {
        // localStorage can be unavailable (private mode, disabled storage) — the
        // toggle still works for this session, it just won't be remembered.
      }
      return next;
    });
  };

  return (
    <aside
      aria-label="Screens"
      className={`bg-[#101419] border-r border-[#30363d] flex flex-col h-full shrink-0 select-none transition-[width] duration-200 ease-out ${
        collapsed ? 'w-[68px]' : 'w-72'
      }`}
    >
      <div className="flex-1 overflow-y-auto overflow-x-hidden p-3 flex flex-col gap-5">
        {GROUPS.map((group) => (
          <div key={group.label} className="flex flex-col gap-1">
            {!collapsed && <div className="label-caps px-2.5 py-1.5">{group.label}</div>}
            {group.items.map((item) => {
              const Icon = item.icon;
              const isActive = activeScreen === item.id;
              // Dimmed, never hidden: a page you cannot use right now and a page that
              // does not exist are different things, and the reason is one hover away.
              const unavailable = liveScreens.has(item.id) && !clusterRunning;
              return (
                <button
                  key={item.id}
                  onClick={() => onSelectScreen(item.id)}
                  // Which screen you are on is otherwise carried by colour and a border
                  // alone, neither of which a screen reader announces.
                  aria-current={isActive ? 'page' : undefined}
                  aria-disabled={unavailable || undefined}
                  title={collapsed ? item.label : unavailable ? 'Needs a running cluster' : undefined}
                  className={`w-full flex items-center gap-3 px-2.5 py-2.5 rounded-md text-base text-left transition-all ${
                    collapsed ? 'justify-center' : ''
                  } ${
                    isActive
                      ? 'bg-[#1c2025] text-[#58a6ff] border-l-2 border-[#58a6ff] font-semibold pl-2'
                      : unavailable
                        ? 'text-[#8b919d]/60 hover:bg-[#181c21]'
                        : 'text-[#c0c7d4] hover:bg-[#181c21] hover:text-[#e0e2ea]'
                  }`}
                >
                  <Icon className={`w-[18px] h-[18px] shrink-0 ${isActive ? 'text-[#58a6ff]' : 'text-[#8b919d]'}`} />
                  {!collapsed && <span className="truncate">{item.label}</span>}
                </button>
              );
            })}
          </div>
        ))}
      </div>

      <div className="p-3 border-t border-[#30363d]">
        <button
          onClick={toggle}
          className={`w-full flex items-center gap-2 px-2.5 py-2 rounded-md text-sm text-[#8b919d] hover:bg-[#181c21] hover:text-[#e0e2ea] transition-colors ${
            collapsed ? 'justify-center' : ''
          }`}
          title={collapsed ? 'Expand sidebar' : 'Collapse sidebar'}
        >
          {collapsed ? <ChevronsRight className="w-[18px] h-[18px]" /> : <ChevronsLeft className="w-[18px] h-[18px]" />}
          {!collapsed && <span>Collapse</span>}
        </button>
      </div>
    </aside>
  );
}
