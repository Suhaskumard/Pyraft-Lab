import { useCallback, useEffect, useState } from 'react';
import { Shield } from 'lucide-react';
import { api } from './api/client';
import { LabProvider, useLab } from './api/LabContext';
import { Header } from './components/Header';
import { Navigation } from './components/Navigation';
import type { ScreenId } from './components/Navigation';
import { GlobalCommandPalette } from './components/GlobalCommandPalette';
import { NoticeTray } from './components/NoticeTray';
import { ScreenErrorBoundary } from './components/ScreenErrorBoundary';

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
    <div className="h-screen bg-[#101419] text-[#e0e2ea] flex flex-col font-sans overflow-hidden">
      <Header onOpenCommandPalette={() => setPaletteOpen(true)} />

      <div className="flex flex-1 min-h-0">
        <Navigation
          activeScreen={activeScreen}
          onSelectScreen={setActiveScreen}
          clusterRunning={running}
          liveScreens={LIVE_SCREENS}
        />

        <main className="flex-1 p-6 lg:p-8 xl:p-10 overflow-y-auto h-full bg-[#0b0e14]">
          <div className="max-w-[2400px] mx-auto">
            <ScreenErrorBoundary resetKey={activeScreen}>
              <div key={activeScreen} className="animate-screen-in">
                {needsCluster ? <ClusterRequired /> : screen()}
              </div>
            </ScreenErrorBoundary>
          </div>
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
    <div className="h-full flex items-center justify-center">
      <div className="obsidian-card text-center py-16 px-10 max-w-xl">
        <div className="w-14 h-14 rounded-full bg-[#181c21] border border-[#30363d] flex items-center justify-center mx-auto mb-5">
          <Shield className="w-6 h-6 text-[#58a6ff]" />
        </div>
        <h2 className="text-xl font-semibold text-[#e0e2ea]">This page needs a live cluster</h2>
        <p className="text-base text-[#8b919d] mt-3 leading-relaxed">
          There is no daemon behind PyRaft Lab — a cluster lives inside the API process for
          exactly as long as it is running. Start one to bring the live pages up. The
          laboratory pages (scenarios, campaigns, results, replay) work without one.
        </p>
        <button
          className="btn-obsidian btn-primary mt-6"
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
