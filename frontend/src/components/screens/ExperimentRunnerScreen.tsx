import { useState } from 'react';
import { FileCode, Play, RefreshCw } from 'lucide-react';
import { api } from '../../api/client';
import { useJobRunner } from '../../api/useJob';
import { useResource } from '../../api/useResource';
import {
  Empty,
  Failure,
  Field,
  JobCard,
  Metric,
  Panel,
  ScreenHeader,
  Skeleton,
  Verdict,
  fmtMs,
  fmtNum,
  fmtPct,
} from '../ui';
import type { RunReport } from '../../types/pyraft';

/**
 * Run one scenario file and read its report.
 *
 * This is `pyraft-lab run` over HTTP: the same runner, the same persistence, the same
 * report document. A scenario's `expected:` block is checked against what actually
 * happened, and a key the build cannot measure is reported as *unchecked* rather than
 * quietly passing.
 */
export function ExperimentRunnerScreen({ onOpenResults }: { onOpenResults: () => void }) {
  const scenarios = useResource(() => api.scenarios(), []);
  const [selected, setSelected] = useState<string | null>(null);
  const [seed, setSeed] = useState('');
  const [plot, setPlot] = useState(false);
  const { job, running, starting, start } = useJobRunner<{ runId: string; report: RunReport; plots: string[] }>(
    'scenario'
  );

  const file = selected ?? scenarios.data?.[0]?.file ?? null;
  const detail = useResource(() => api.scenario(file!), [file], { enabled: Boolean(file) });
  const chosen = scenarios.data?.find((s) => s.file === file);

  const launch = () =>
    void start(() => api.runScenario(file!, seed.trim() ? Number(seed) : undefined, plot));

  const report = job?.status === 'succeeded' ? job.result?.report : null;

  return (
    <div className="flex flex-col gap-4">
      <ScreenHeader
        title="Experiment Runner"
        description="One scenario file, run over a virtual clock so thirty simulated seconds finish in milliseconds — and reproducibly, because one seed feeds every random source in the run."
        actions={
          <button className="btn-obsidian btn-secondary btn-sm" onClick={() => void scenarios.reload()}>
            <RefreshCw className="w-3.5 h-3.5" />
            Rescan
          </button>
        }
      />

      <div className="grid grid-cols-1 xl:grid-cols-3 gap-4">
        <Panel title="Scenarios" subtitle="from the workspace's scenarios/ directory">
          {scenarios.loading ? (
            <Skeleton rows={5} />
          ) : scenarios.error ? (
            <Failure message={scenarios.error} />
          ) : !scenarios.data?.length ? (
            <Empty
              title="No scenario files"
              hint="Drop a .yaml file into the workspace's scenarios/ directory, or run `pyraft-lab init` there."
            />
          ) : (
            <div className="flex flex-col gap-1.5">
              {scenarios.data.map((scenario) => (
                <button
                  key={scenario.file}
                  onClick={() => setSelected(scenario.file)}
                  className={`text-left px-2.5 py-2 rounded border transition-all ${
                    scenario.file === file
                      ? 'border-[#58a6ff] bg-[#1c2025]'
                      : 'border-[#30363d] bg-[#181c21] hover:border-[#414752]'
                  }`}
                >
                  <div className="flex items-center gap-2">
                    <FileCode className="w-3.5 h-3.5 text-[#8b919d] shrink-0" />
                    <span className="font-mono text-xs text-[#e0e2ea] truncate">{scenario.file}</span>
                  </div>
                  {scenario.error ? (
                    <p className="text-[10px] text-[#ffb4ab] mt-1">{scenario.error}</p>
                  ) : (
                    <p className="text-[10px] text-[#8b919d] mt-1 font-mono">
                      {scenario.nodes} nodes · {scenario.durationSec}s · {scenario.faultCount} fault
                      {scenario.faultCount === 1 ? '' : 's'} · seed {scenario.seed}
                    </p>
                  )}
                </button>
              ))}
            </div>
          )}
        </Panel>

        <Panel
          title={file ?? 'Scenario'}
          subtitle="the file, verbatim"
          className="xl:col-span-2"
          actions={
            <>
              <input
                value={seed}
                onChange={(event) => setSeed(event.target.value)}
                placeholder={`seed (${chosen?.seed ?? 0})`}
                className="w-28 bg-[#0b0e14] border border-[#30363d] rounded px-2 py-1 font-mono text-xs text-[#e0e2ea] placeholder:text-[#8b919d] focus:border-[#58a6ff] outline-none"
              />
              <label className="flex items-center gap-1.5 text-[11px] text-[#c0c7d4] cursor-pointer">
                <input
                  type="checkbox"
                  checked={plot}
                  onChange={(event) => setPlot(event.target.checked)}
                  className="accent-[#58a6ff]"
                />
                plots
              </label>
              <button
                className="btn-obsidian btn-primary btn-sm"
                disabled={!file || running || starting}
                onClick={launch}
              >
                <Play className="w-3 h-3" />
                {running ? 'Running…' : 'Run'}
              </button>
            </>
          }
        >
          {detail.error ? (
            <Failure message={detail.error} />
          ) : detail.data ? (
            <pre className="terminal-block max-h-72 overflow-y-auto">{detail.data.yaml}</pre>
          ) : (
            <Empty title="Select a scenario" />
          )}
        </Panel>
      </div>

      {job && (
        <Panel
          title="Run"
          actions={
            job.status === 'succeeded' && (
              <button className="btn-obsidian btn-ghost btn-sm" onClick={onOpenResults}>
                Open in Results Explorer
              </button>
            )
          }
        >
          <JobCard job={job} />
        </Panel>
      )}

      {report && <ReportView report={report} plots={job?.result?.plots ?? []} />}
    </div>
  );
}

export function ReportView({ report, plots = [] }: { report: RunReport; plots?: string[] }) {
  const checked = report.expectations.filter((item) => item.checked);
  const unchecked = report.expectations.filter((item) => !item.checked);

  return (
    <div className="flex flex-col gap-4">
      <div className="grid grid-cols-2 md:grid-cols-4 xl:grid-cols-7 gap-3">
        <Metric
          label="Verdict"
          value={report.passed ? 'PASS' : 'FAIL'}
          tone={report.passed ? 'good' : 'bad'}
        />
        <Metric label="Availability" value={fmtPct(report.summary.availability * 100)} />
        <Metric label="Throughput" value={`${report.summary.throughput_per_sec.toFixed(1)} /s`} />
        <Metric label="P99 commit" value={fmtMs(report.timing.p99_commit_latency_ms)} />
        <Metric label="Elections" value={report.leadership.total_elections} />
        <Metric label="Election time" value={fmtMs(report.leadership.election_time_ms)} />
        <Metric
          label="Data loss"
          value={report.summary.data_loss ? 'YES' : 'none'}
          tone={report.summary.data_loss ? 'bad' : 'good'}
        />
      </div>

      <div className="grid grid-cols-1 xl:grid-cols-3 gap-4">
        <Panel title="Requests">
          <div className="flex flex-col">
            <Field label="Total" value={fmtNum(report.summary.total_requests)} />
            <Field label="Successful" value={fmtNum(report.summary.successful)} />
            <Field label="Incomplete" value={fmtNum(report.summary.incomplete)} />
            <Field label="Redirects" value={fmtNum(report.summary.failed_redirects)} />
            <Field label="Timeouts" value={fmtNum(report.summary.timeouts)} />
            <Field label="P50 commit" value={fmtMs(report.timing.p50_commit_latency_ms)} />
            <Field label="P95 commit" value={fmtMs(report.timing.p95_commit_latency_ms)} />
          </div>
        </Panel>

        <Panel title="Leadership & faults">
          <div className="flex flex-col">
            <Field label="Leader changes" value={report.leadership.leader_changes} />
            <Field label="Final leader" value={report.leadership.final_leader ?? '(none)'} />
            <Field label="Final term" value={report.leadership.final_term} />
            <Field label="Recovery time" value={fmtMs(report.leadership.recovery_time_ms)} />
            <Field label="Messages sent" value={fmtNum(report.fault_impact.messages_sent)} />
            <Field label="Dropped (loss)" value={fmtNum(report.fault_impact.dropped_by_loss)} />
            <Field label="Dropped (partition)" value={fmtNum(report.fault_impact.dropped_by_partition)} />
          </div>
        </Panel>

        <Panel title="Consistency">
          {report.consistency ? (
            <div className="flex flex-col gap-2">
              <div className="flex items-center gap-2">
                <Verdict ok={report.consistency.linearizable} yes="LINEARIZABLE" no="VIOLATION" />
              </div>
              <p className="text-[11px] text-[#c0c7d4] leading-relaxed">
                {report.consistency.explanation}
              </p>
            </div>
          ) : (
            <Empty title="Consistency was not checked for this run" />
          )}
        </Panel>
      </div>

      {(checked.length > 0 || unchecked.length > 0) && (
        <Panel
          title="Expectations"
          subtitle="the scenario's own expected: block, checked against what happened"
        >
          <div className="obsidian-table-container">
            <table className="obsidian-table">
              <thead>
                <tr>
                  <th>Key</th>
                  <th>Expected</th>
                  <th>Actual</th>
                  <th>Result</th>
                </tr>
              </thead>
              <tbody>
                {checked.map((item) => (
                  <tr key={item.key}>
                    <td className="font-mono">{item.key}</td>
                    <td className="font-mono text-[#8b919d]">{String(item.expected)}</td>
                    <td className="font-mono">{formatActual(item.actual)}</td>
                    <td><Verdict ok={item.met} yes="MET" no="MISSED" /></td>
                  </tr>
                ))}
                {unchecked.map((item) => (
                  <tr key={item.key}>
                    <td className="font-mono text-[#8b919d]">{item.key}</td>
                    <td className="font-mono text-[#8b919d]">{String(item.expected)}</td>
                    <td className="font-mono text-[#8b919d]">—</td>
                    <td>
                      <span className="status-pill status-pill-candidate">
                        <span className="status-dot" />
                        NOT MEASURED
                      </span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Panel>
      )}

      {report.fault_impact.faults_applied.length > 0 && (
        <Panel title="Fault timeline" subtitle="what was injected, and when">
          <div className="obsidian-table-container max-h-64 overflow-y-auto">
            <table className="obsidian-table">
              <thead>
                <tr>
                  <th className="text-right">t (s)</th>
                  <th>Action</th>
                  <th>Target</th>
                </tr>
              </thead>
              <tbody>
                {report.fault_impact.faults_applied.map((fault, index) => (
                  <tr key={index}>
                    <td className="font-mono text-right">{fault.at.toFixed(2)}</td>
                    <td className="font-mono text-[#ffba42]">{fault.action}</td>
                    <td className="font-mono text-[#8b919d]">{fault.target ?? '—'}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Panel>
      )}

      {plots.length > 0 && (
        <Panel title="Plots" subtitle="written to the workspace">
          <ul className="flex flex-col gap-1">
            {plots.map((path) => (
              <li key={path} className="font-mono text-[11px] text-[#8b919d]">
                {path}
              </li>
            ))}
          </ul>
        </Panel>
      )}
    </div>
  );
}

const formatActual = (value: unknown) =>
  typeof value === 'number' ? value.toFixed(3).replace(/\.?0+$/, '') : String(value);
