import { useState } from 'react';
import { api } from '../../api/client';
import { useResource } from '../../api/useResource';
import { Empty, Failure, Panel, ScreenHeader, Skeleton, Verdict, fmtMs, fmtPct } from '../ui';
import type { RunReport } from '../../types/pyraft';

/**
 * Two runs, side by side.
 *
 * The delta column is only drawn for measurements that are comparable across runs. A
 * value one side did not measure shows as `n/a` on that side and no delta at all,
 * rather than being treated as zero — the usual way a comparison table starts lying.
 */
export function RunComparisonScreen() {
  const results = useResource(() => api.results(), []);
  const [left, setLeft] = useState<string | null>(null);
  const [right, setRight] = useState<string | null>(null);

  const a = useResource(() => api.result(left!), [left], { enabled: Boolean(left) });
  const b = useResource(() => api.result(right!), [right], { enabled: Boolean(right) });

  const rows: { label: string; get: (r: RunReport) => number | null; format: (v: number | null) => string; lowerIsBetter?: boolean }[] = [
    { label: 'Availability', get: (r) => r.summary.availability * 100, format: (v) => fmtPct(v) },
    { label: 'Throughput (ops/s)', get: (r) => r.summary.throughput_per_sec, format: (v) => (v === null ? 'n/a' : v.toFixed(2)) },
    { label: 'Total requests', get: (r) => r.summary.total_requests, format: (v) => (v === null ? 'n/a' : String(v)) },
    { label: 'P50 commit', get: (r) => r.timing.p50_commit_latency_ms, format: fmtMs, lowerIsBetter: true },
    { label: 'P95 commit', get: (r) => r.timing.p95_commit_latency_ms, format: fmtMs, lowerIsBetter: true },
    { label: 'P99 commit', get: (r) => r.timing.p99_commit_latency_ms, format: fmtMs, lowerIsBetter: true },
    { label: 'Elections', get: (r) => r.leadership.total_elections, format: (v) => (v === null ? 'n/a' : String(v)), lowerIsBetter: true },
    { label: 'Leader changes', get: (r) => r.leadership.leader_changes, format: (v) => (v === null ? 'n/a' : String(v)), lowerIsBetter: true },
    { label: 'Election time', get: (r) => r.leadership.election_time_ms, format: fmtMs, lowerIsBetter: true },
    { label: 'Recovery time', get: (r) => r.leadership.recovery_time_ms, format: fmtMs, lowerIsBetter: true },
    { label: 'Messages dropped', get: (r) => r.fault_impact.messages_dropped, format: (v) => (v === null ? 'n/a' : String(v)), lowerIsBetter: true },
  ];

  return (
    <div className="flex flex-col gap-4">
      <ScreenHeader
        title="Run Comparison"
        description="Pick two persisted runs and read their reports against each other — what a different seed, a different cluster size or a different fault timeline actually changed."
      />

      {results.loading ? (
        <Skeleton rows={3} />
      ) : results.error ? (
        <Panel><Failure message={results.error} /></Panel>
      ) : !results.data?.length ? (
        <Panel><Empty title="No runs to compare" hint="Run a scenario first." /></Panel>
      ) : (
        <>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <Picker label="Run A" runs={results.data} value={left} onChange={setLeft} />
            <Picker label="Run B" runs={results.data} value={right} onChange={setRight} />
          </div>

          {a.data?.report && b.data?.report ? (
            <Panel title="Comparison">
              <div className="obsidian-table-container">
                <table className="obsidian-table">
                  <thead>
                    <tr>
                      <th>Measurement</th>
                      <th className="text-right">{left}</th>
                      <th className="text-right">{right}</th>
                      <th className="text-right">Delta</th>
                    </tr>
                  </thead>
                  <tbody>
                    <tr>
                      <td>Verdict</td>
                      <td className="text-right"><Verdict ok={a.data.report.passed} /></td>
                      <td className="text-right"><Verdict ok={b.data.report.passed} /></td>
                      <td className="text-right text-[#8b919d]">—</td>
                    </tr>
                    {rows.map((row) => {
                      const valueA = row.get(a.data!.report!);
                      const valueB = row.get(b.data!.report!);
                      const comparable = valueA !== null && valueB !== null;
                      const delta = comparable ? valueB! - valueA! : null;
                      const better =
                        delta === null || delta === 0
                          ? null
                          : row.lowerIsBetter
                            ? delta < 0
                            : delta > 0;
                      return (
                        <tr key={row.label}>
                          <td>{row.label}</td>
                          <td className="font-mono text-right">{row.format(valueA)}</td>
                          <td className="font-mono text-right">{row.format(valueB)}</td>
                          <td
                            className={`font-mono text-right ${
                              better === null
                                ? 'text-[#8b919d]'
                                : better
                                  ? 'text-[#4ade80]'
                                  : 'text-[#ffb4ab]'
                            }`}
                          >
                            {delta === null ? '—' : `${delta > 0 ? '+' : ''}${delta.toFixed(2)}`}
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            </Panel>
          ) : (
            <Panel>
              <Empty title="Select two runs" hint="Both need a persisted report to compare." />
            </Panel>
          )}
        </>
      )}
    </div>
  );
}

function Picker({
  label,
  runs,
  value,
  onChange,
}: {
  label: string;
  runs: { runId: string; scenarioName?: string; seed?: number }[];
  value: string | null;
  onChange: (value: string) => void;
}) {
  return (
    <Panel title={label}>
      <select
        value={value ?? ''}
        onChange={(event) => onChange(event.target.value)}
        className="w-full bg-[#0b0e14] border border-[#30363d] rounded px-2.5 py-2 font-mono text-sm text-[#e0e2ea] focus:border-[#58a6ff] outline-none"
      >
        <option value="">— select a run —</option>
        {runs.map((run) => (
          <option key={run.runId} value={run.runId}>
            {run.runId} · {run.scenarioName ?? '?'} · seed {run.seed ?? '?'}
          </option>
        ))}
      </select>
    </Panel>
  );
}
