import { useCallback, useEffect, useState } from 'react';
import { api } from './api/client';
import { LabProvider, useLab } from './api/LabContext';
import { Header } from './components/Header';
import { Navigation } from './components/Navigation';
import type { ScreenId } from './components/Navigation';
import { GlobalCommandPalette } from './components/GlobalCommandPalette';
import { NoticeTray } from './components/NoticeTray';

import { ClusterOverviewScreen } from './components/screens/ClusterOverviewScreen';
import { NodeInspectorScreen } from './components/screens/NodeInspectorScreen';
import { KeyValueStoreScreen } from './components/screens/KeyValueStoreScreen';
import { ConsensusDemoScreen } from './components/screens/ConsensusDemoScreen';
import { WalInspectorScreen } from './components/screens/WalInspectorScreen';
import { SnapshotsScreen } from './components/screens/SnapshotsScreen';
import { ArchitectureScreen } from './components/screens/ArchitectureScreen';
import { FaultInjectionLabScreen } from './components/screens/FaultInjectionLabScreen';
import { ExperimentRunnerScreen } from './components/screens/ExperimentRunnerScreen';
import { DeterministicReplayScreen } from './components/screens/DeterministicReplayScreen';
import { BenchmarkLabScreen } from './components/screens/BenchmarkLabScreen';
import { StressCampaignScreen } from './components/screens/StressCampaignScreen';
import { ChaosCampaignScreen } from './components/screens/ChaosCampaignScreen';
import { LinearizabilityAnalysisScreen } from './components/screens/LinearizabilityAnalysisScreen';
import { RunComparisonScreen } from './components/screens/RunComparisonScreen';
import { SystemErrorDiagnosticsScreen } from './components/screens/SystemErrorDiagnosticsScreen';
import { ResultsExplorerScreen } from './components/screens/ResultsExplorerScreen';
import { SettingsScreen } from './components/screens/SettingsScreen';

/** Screens that address a live cluster. The rest read the workspace's scenarios and
 *  persisted runs, and work perfectly well with nothing running. */
const LIVE_SCREENS: ReadonlySet<ScreenId> = new Set<ScreenId>([
  'cluster_overview',
  'node_inspector',
  'kv_store',
  'consensus_demo',
  'wal_inspector',
  'snapshots',
  'fault_injection',
]);

function Workbench() {
  const lab = useLab();
  const [activeScreen, setActiveScreen] = useState<ScreenId>('cluster_overview');
  const [paletteOpen, setPaletteOpen] = useState(false);
  const [selectedNodeId, setSelectedNodeId] = useState<string | null>(null);

  const running = lab.cluster?.running ?? false;

  // Keep the inspector pointed at a node that exists: a cluster restarted with fewer
  // members would otherwise leave it addressing an id the backend now 404s on.
  useEffect(() => {
    if (!lab.nodes.length) return;
    if (!selectedNodeId || !lab.nodes.some((n) => n.id === selectedNodeId)) {
      setSelectedNodeId(lab.nodes[0].id);
    }
  }, [lab.nodes, selectedNodeId]);

  useEffect(() => {
    const onKey = (event: KeyboardEvent) => {
      if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === 'k') {
        event.preventDefault();
        setPaletteOpen((open) => !open);
      }
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, []);

  const inspectNode = useCallback((nodeId: string) => {
    setSelectedNodeId(nodeId);
    setActiveScreen('node_inspector');
  }, []);

  const screen = () => {
    switch (activeScreen) {
      case 'cluster_overview':
        return <ClusterOverviewScreen onInspectNode={inspectNode} />;
      case 'node_inspector':
        return (
          <NodeInspectorScreen selectedNodeId={selectedNodeId} onSelectNode={setSelectedNodeId} />
        );
      case 'kv_store':
        return <KeyValueStoreScreen />;
      case 'consensus_demo':
        return <ConsensusDemoScreen />;
      case 'wal_inspector':
        return <WalInspectorScreen />;
      case 'snapshots':
        return <SnapshotsScreen />;
      case 'architecture':
        return <ArchitectureScreen />;
      case 'fault_injection':
        return <FaultInjectionLabScreen />;
      case 'experiment_runner':
        return <ExperimentRunnerScreen onOpenResults={() => setActiveScreen('results_explorer')} />;
      case 'deterministic_replay':
        return <DeterministicReplayScreen />;
      case 'benchmark_lab':
        return <BenchmarkLabScreen />;
      case 'stress_campaign':
        return <StressCampaignScreen />;
      case 'chaos_campaign':
        return <ChaosCampaignScreen />;
      case 'linearizability':
        return <LinearizabilityAnalysisScreen />;
      case 'run_comparison':
        return <RunComparisonScreen />;
      case 'diagnostics':
        return <SystemErrorDiagnosticsScreen />;
      case 'results_explorer':
        return <ResultsExplorerScreen />;
      case 'settings':
        return <SettingsScreen />;
    }
  };

  const needsCluster = LIVE_SCREENS.has(activeScreen) && !running;

  return (
    <div className="min-h-screen bg-[#101419] text-[#e0e2ea] flex flex-col font-sans">
      <Header onOpenCommandPalette={() => setPaletteOpen(true)} />

      <div className="flex flex-1 overflow-hidden">
        <Navigation
          activeScreen={activeScreen}
          onSelectScreen={setActiveScreen}
          clusterRunning={running}
          liveScreens={LIVE_SCREENS}
        />

        <main className="flex-1 p-5 overflow-y-auto h-[calc(100vh-57px)] bg-[#0b0e14]">
          {needsCluster ? <ClusterRequired /> : screen()}
        </main>
      </div>

      <NoticeTray />
      <GlobalCommandPalette
        isOpen={paletteOpen}
        onClose={() => setPaletteOpen(false)}
        onSelectScreen={setActiveScreen}
      />
    </div>
  );
}

function ClusterRequired() {
  const lab = useLab();
  return (
    <div className="max-w-xl mx-auto mt-16">
      <div className="obsidian-card text-center py-10 px-6">
        <h2 className="text-base font-semibold text-[#e0e2ea]">This page needs a live cluster</h2>
        <p className="text-xs text-[#8b919d] mt-2 leading-relaxed">
          There is no daemon behind PyRaft Lab — a cluster lives inside the API process for
          exactly as long as it is running. Start one to bring the live pages up. The
          laboratory pages (scenarios, campaigns, results, replay) work without one.
        </p>
        <button
          className="btn-obsidian btn-primary btn-sm mt-4"
          onClick={() => void lab.act('start cluster', () => api.startCluster({}))}
        >
          Start cluster
        </button>
      </div>
    </div>
  );
}

export function App() {
  return (
    <LabProvider>
      <Workbench />
    </LabProvider>
  );
}

export default App;
