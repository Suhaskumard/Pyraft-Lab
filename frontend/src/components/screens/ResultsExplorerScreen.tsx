import { useState } from 'react';
import { RefreshCw } from 'lucide-react';
import { api } from '../../api/client';
import { useResource } from '../../api/useResource';
import { Empty, Failure, Panel, ScreenHeader, Skeleton, Verdict, fmtMs, fmtPct } from '../ui';
import { ReportView } from './ExperimentRunnerScreen';

/**
 * Every run this workspace has persisted, newest first.
 *
 * A run directory is self-contained: the manifest that reproduces it, the event trace,
 * the client history, and the report. Selecting one reads its report back — the same
 * document `pyraft-lab run` printed when it finished.
 */
export function ResultsExplorerScreen() {
  const results = useResource(() => api.results(), []);
  const [selected, setSelected] = useState<string | null>(null);
  const detail = useResource(() => api.result(selected!), [selected], {
    enabled: Boolean(selected),
  });

  return (
    <div className="flex flex-col gap-4">
      <ScreenHeader
        title="Results Explorer"
        description="Persisted runs from the workspace's results/ directory. Each one carries everything needed to reproduce it from its seed alone."
        actions={
          <button className="btn-obsidian btn-secondary btn-sm" onClick={() => void results.reload()}>
            <RefreshCw className="w-3.5 h-3.5" />
            Rescan
          </button>
        }
      />

      <Panel title="Runs" subtitle={results.data ? `${results.data.length} persisted` : undefined}>
        {results.loading ? (
          <Skeleton rows={5} />
        ) : results.error ? (
          <Failure message={results.error} />
        ) : !results.data?.length ? (
          <Empty
            title="No runs yet"
            hint="Run a scenario from the Experiment Runner, or a stress or chaos campaign — a failing trial is persisted automatically."
          />
        ) : (
          <div className="obsidian-table-container max-h-96 overflow-y-auto">
            <table className="obsidian-table">
              <thead>
                <tr>
                  <th>Run id</th>
                  <th>Scenario</th>
                  <th className="text-right">Seed</th>
                  <th className="text-right">Availability</th>
                  <th className="text-right">Throughput</th>
                  <th className="text-right">P99</th>
                  <th className="text-right">Elections</th>
                  <th>Linearizable</th>
                  <th>Result</th>
                  <th>Created</th>
                </tr>
              </thead>
              <tbody>
                {results.data.map((row) => (
                  <tr
                    key={row.runId}
                    onClick={() => setSelected(row.runId)}
                    className={`cursor-pointer ${row.runId === selected ? 'bg-[#1c2025]' : ''}`}
                  >
                    <td className="font-mono text-[#58a6ff]">{row.runId}</td>
                    <td className="font-mono">{row.scenarioName ?? '—'}</td>
                    <td className="font-mono text-right text-[#8b919d]">{row.seed ?? '—'}</td>
                    <td className="font-mono text-right">
                      {row.availability === undefined ? '—' : fmtPct(row.availability * 100)}
                    </td>
                    <td className="font-mono text-right">
                      {row.throughputPerSec === undefined ? '—' : `${row.throughputPerSec.toFixed(1)} /s`}
                    </td>
                    <td className="font-mono text-right">{fmtMs(row.p99CommitLatencyMs)}</td>
                    <td className="font-mono text-right">{row.totalElections ?? '—'}</td>
                    <td>
                      {row.linearizable === undefined ? (
                        <span className="text-[#8b919d]">—</span>
                      ) : (
                        <Verdict ok={row.linearizable} yes="YES" no="NO" />
                      )}
                    </td>
                    <td>{row.passed === undefined ? <span className="text-[#8b919d]">—</span> : <Verdict ok={row.passed} />}</td>
                    <td className="font-mono text-[11px] text-[#8b919d]">{row.createdAt ?? '—'}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </Panel>

      {selected && (
        <>
          <h2 className="text-sm font-semibold text-[#e0e2ea] font-mono">{selected}</h2>
          {detail.loading ? (
            <Skeleton rows={4} />
          ) : detail.error ? (
            <Panel><Failure message={detail.error} /></Panel>
          ) : detail.data?.report ? (
            <ReportView report={detail.data.report} />
          ) : (
            <Panel>
              <Empty
                title="This run has a manifest but no report"
                hint="It can still be replayed — the manifest is all replay needs."
              />
            </Panel>
          )}
        </>
      )}
    </div>
  );
}
