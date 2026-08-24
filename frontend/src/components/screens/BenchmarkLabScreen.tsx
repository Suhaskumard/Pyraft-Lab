import { useMemo, useState } from 'react';
import { BarChart3 } from 'lucide-react';
import { api } from '../../api/client';
import { useJobRunner } from '../../api/useJob';
import { Empty, JobCard, Metric, Panel, ScreenHeader, fmtMs } from '../ui';
import type { BenchmarkReport } from '../../types/pyraft';

/**
 * Throughput and latency across cluster size and packet loss.
 *
 * Each cell is one ordinary scenario run over the same runner and read back through the
 * same report as everything else — no separate benchmarking subsystem, and no
 * statistical repeats per cell, because the percentiles already come from the many
 * client operations inside one run's duration.
 */
export function BenchmarkLabScreen() {
  const [nodeCounts, setNodeCounts] = useState('3,5');
  const [lossPercents, setLossPercents] = useState('0,5,10');
  const [duration, setDuration] = useState('10s');
  const [seed, setSeed] = useState(0);
  const { job, running, starting, start, cancel } = useJobRunner<BenchmarkReport>('benchmark');

  const report = job?.status === 'succeeded' ? job.result : null;

  const parse = (text: string) =>
    text
      .split(',')
      .map((part) => Number(part.trim()))
      .filter((value) => Number.isFinite(value));

  const grid = useMemo(() => {
    if (!report) return null;
    const byNodes = new Map<number, Map<number, (typeof report.results)[number]>>();
    for (const cell of report.results) {
      if (!byNodes.has(cell.nodes)) byNodes.set(cell.nodes, new Map());
      byNodes.get(cell.nodes)!.set(cell.loss_rate, cell);
    }
    return { byNodes, losses: report.loss_rates };
  }, [report]);

  const peakThroughput = report ? Math.max(...report.results.map((c) => c.throughput_per_sec)) : 0;

  return (
    <div className="flex flex-col gap-4">
      <ScreenHeader
        title="Benchmark Lab"
        description="A grid of cluster sizes against packet-loss rates. Every cell includes a leader crash, so the recovery number is measured rather than assumed."
      />

      <Panel title="Grid">
        <div className="flex flex-wrap items-end gap-3">
          <Field label="Node counts" value={nodeCounts} onChange={setNodeCounts} width="w-32" />
          <Field label="Loss %" value={lossPercents} onChange={setLossPercents} width="w-32" />
          <Field label="Duration / cell" value={duration} onChange={setDuration} width="w-28" />
          <label className="flex flex-col gap-1">
            <span className="label-caps">Seed</span>
            <input
              type="number"
              value={seed}
              onChange={(event) => setSeed(Number(event.target.value))}
              className="w-24 bg-[#0b0e14] border border-[#30363d] rounded px-2 py-1.5 font-mono text-sm text-[#e0e2ea] focus:border-[#58a6ff] outline-none"
            />
          </label>
          <button
            className="btn-obsidian btn-primary btn-sm"
            disabled={running || starting}
            onClick={() =>
              void start(() =>
                api.runBenchmark({
                  nodeCounts: parse(nodeCounts),
                  lossPercents: parse(lossPercents),
                  duration,
                  seed,
                })
              )
            }
          >
            <BarChart3 className="w-3 h-3" />
            {running ? 'Running…' : `Run ${parse(nodeCounts).length * parse(lossPercents).length} cells`}
          </button>
        </div>
      </Panel>

      {job && (
        <Panel title="Campaign">
          <JobCard job={job} onCancel={cancel} />
        </Panel>
      )}

      {report && grid && (
        <>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
            <Metric label="Cells" value={report.cells} />
            <Metric label="Peak throughput" value={`${peakThroughput.toFixed(1)} /s`} tone="good" />
            <Metric
              label="Cells with data loss"
              value={report.data_loss_cells}
              tone={report.data_loss_cells ? 'bad' : 'good'}
            />
            <Metric label="Duration / cell" value={`${report.trial_duration_sec}s`} />
          </div>

          <Panel
            title="Throughput"
            subtitle="operations per second — bar length is relative to the best cell in this run"
          >
            <div className="flex flex-col gap-3">
              {[...grid.byNodes.entries()].map(([nodes, byLoss]) => (
                <div key={nodes} className="flex flex-col gap-1.5">
                  <span className="label-caps">{nodes} nodes</span>
                  {grid.losses.map((loss) => {
                    const cell = byLoss.get(loss);
                    const width = cell ? (cell.throughput_per_sec / peakThroughput) * 100 : 0;
                    return (
                      <div key={loss} className="flex items-center gap-2">
                        <span className="font-mono text-[13px] text-[#8b919d] w-12 shrink-0 text-right">
                          {(loss * 100).toFixed(0)}%
                        </span>
                        <div className="flex-1 h-4 bg-[#181c21] rounded-sm overflow-hidden border border-[#30363d]">
                          <div
                            className="h-full bg-[#58a6ff]/70 transition-all"
                            style={{ width: `${width}%` }}
                          />
                        </div>
                        <span className="font-mono text-[13px] text-[#e0e2ea] w-20 shrink-0 text-right">
                          {cell ? `${cell.throughput_per_sec.toFixed(1)} /s` : '—'}
                        </span>
                      </div>
                    );
                  })}
                </div>
              ))}
            </div>
          </Panel>

          <Panel title="Cells">
            {!report.results.length ? (
              <Empty title="No cells" />
            ) : (
              <div className="obsidian-table-container">
                <table className="obsidian-table">
                  <thead>
                    <tr>
                      <th className="text-right">Nodes</th>
                      <th className="text-right">Loss</th>
                      <th className="text-right">Throughput</th>
                      <th className="text-right">P50</th>
                      <th className="text-right">P95</th>
                      <th className="text-right">P99</th>
                      <th className="text-right">Election</th>
                      <th className="text-right">Recovery</th>
                      <th className="text-right">CPU</th>
                      <th className="text-right">Peak mem</th>
                      <th>Data loss</th>
                    </tr>
                  </thead>
                  <tbody>
                    {report.results.map((cell) => (
                      <tr key={`${cell.nodes}-${cell.loss_rate}`}>
                        <td className="font-mono text-right">{cell.nodes}</td>
                        <td className="font-mono text-right">{(cell.loss_rate * 100).toFixed(0)}%</td>
                        <td className="font-mono text-right text-[#4ade80]">
                          {cell.throughput_per_sec.toFixed(1)}
                        </td>
                        <td className="font-mono text-right">{fmtMs(cell.latency_p50_ms)}</td>
                        <td className="font-mono text-right">{fmtMs(cell.latency_p95_ms)}</td>
                        <td className="font-mono text-right text-[#ffba42]">
                          {fmtMs(cell.latency_p99_ms)}
                        </td>
                        <td className="font-mono text-right">{fmtMs(cell.election_time_ms)}</td>
                        <td className="font-mono text-right">{fmtMs(cell.recovery_time_ms)}</td>
                        <td className="font-mono text-right text-[#8b919d]">
                          {cell.cpu_seconds.toFixed(2)}s
                        </td>
                        <td className="font-mono text-right text-[#8b919d]">
                          {cell.peak_memory_mb.toFixed(1)} MB
                        </td>
                        <td className={cell.data_loss ? 'text-[#ffb4ab]' : 'text-[#8b919d]'}>
                          {cell.data_loss ? 'YES' : 'no'}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </Panel>

          {report.graphs?.length > 0 && (
            <Panel title="Comparison graphs" subtitle="written to the workspace">
              <ul className="flex flex-col gap-1">
                {report.graphs.map((path) => (
                  <li key={path} className="font-mono text-[13px] text-[#8b919d]">
                    {path}
                  </li>
                ))}
              </ul>
            </Panel>
          )}
        </>
      )}
    </div>
  );
}

function Field({
  label,
  value,
  onChange,
  width,
}: {
  label: string;
  value: string;
  onChange: (value: string) => void;
  width: string;
}) {
  return (
    <label className="flex flex-col gap-1">
      <span className="label-caps">{label}</span>
      <input
        value={value}
        onChange={(event) => onChange(event.target.value)}
        className={`${width} bg-[#0b0e14] border border-[#30363d] rounded px-2 py-1.5 font-mono text-sm text-[#e0e2ea] focus:border-[#58a6ff] outline-none`}
      />
    </label>
  );
}
