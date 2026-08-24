import { useState } from 'react';
import { api } from '../../api/client';
import { useResource } from '../../api/useResource';
import {
  Empty,
  Failure,
  Field,
  Metric,
  Panel,
  ScreenHeader,
  Skeleton,
  Verdict,
  fmtNum,
  fmtPct,
} from '../ui';

/**
 * Did the cluster ever answer in a way no single-threaded store could have?
 *
 * The checker consumes the recorded client history — every operation's invocation and
 * response instant — and looks for a sequential ordering consistent with real time. A
 * verdict of UNPROVEN is a real third answer, not a failure: the search is bounded, and
 * saying "I could not decide" is more honest than picking a side.
 */
export function LinearizabilityAnalysisScreen() {
  const results = useResource(() => api.results(), []);
  const [selected, setSelected] = useState<string | null>(null);

  const runId = selected ?? results.data?.[0]?.runId ?? null;
  const detail = useResource(() => api.result(runId!), [runId], { enabled: Boolean(runId) });

  const report = detail.data?.report;
  const consistency = report?.consistency;

  return (
    <div className="flex flex-col gap-4">
      <ScreenHeader
        title="Linearizability Analysis"
        description="Every run records a client history — what was asked, when, and what came back. The checker looks for a sequential ordering of those operations consistent with the real time each one occupied."
      />

      <div className="grid grid-cols-1 xl:grid-cols-4 gap-4">
        <Panel title="Runs">
          {results.loading ? (
            <Skeleton rows={5} />
          ) : results.error ? (
            <Failure message={results.error} />
          ) : !results.data?.length ? (
            <Empty title="No runs to analyse" hint="Run a scenario first." />
          ) : (
            <div className="flex flex-col gap-1.5 max-h-96 overflow-y-auto">
              {results.data.map((row) => (
                <button
                  key={row.runId}
                  onClick={() => setSelected(row.runId)}
                  className={`text-left px-2.5 py-2 rounded border transition-all ${
                    row.runId === runId
                      ? 'border-[#58a6ff] bg-[#1c2025]'
                      : 'border-[#30363d] bg-[#181c21] hover:border-[#414752]'
                  }`}
                >
                  <div className="font-mono text-sm truncate">{row.runId}</div>
                  <div className="font-mono text-[12px] text-[#8b919d] mt-0.5">
                    {row.scenarioName ?? '—'}
                  </div>
                </button>
              ))}
            </div>
          )}
        </Panel>

        <div className="xl:col-span-3 flex flex-col gap-4">
          {detail.loading ? (
            <Skeleton rows={4} />
          ) : detail.error ? (
            <Panel><Failure message={detail.error} /></Panel>
          ) : !report ? (
            <Panel><Empty title="Select a run" /></Panel>
          ) : !consistency ? (
            <Panel>
              <Empty
                title="Consistency was not checked for this run"
                hint="The checker runs by default; a run persisted with it disabled has no verdict to show."
              />
            </Panel>
          ) : (
            <>
              <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
                <Metric
                  label="Verdict"
                  value={consistency.verdict}
                  tone={
                    consistency.linearizable === true
                      ? 'good'
                      : consistency.linearizable === false
                        ? 'bad'
                        : 'warn'
                  }
                />
                <Metric label="Operations" value={fmtNum(report.summary.total_requests)} />
                <Metric label="Completed" value={fmtNum(report.summary.successful)} />
                <Metric
                  label="Incomplete"
                  value={fmtNum(report.summary.incomplete)}
                  tone={report.summary.incomplete ? 'warn' : 'default'}
                  hint="An operation whose reply never arrived. It may or may not have taken effect, so the checker has to consider both."
                />
              </div>

              <Panel title="What the checker found">
                <div className="flex flex-col gap-3">
                  <Verdict ok={consistency.linearizable} yes="LINEARIZABLE" no="VIOLATION FOUND" />
                  <pre className="terminal-block whitespace-pre-wrap">{consistency.explanation}</pre>
                </div>
              </Panel>

              <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
                <Panel title="History">
                  <div className="flex flex-col">
                    <Field label="Total requests" value={fmtNum(report.summary.total_requests)} />
                    <Field label="Successful" value={fmtNum(report.summary.successful)} />
                    <Field label="Incomplete" value={fmtNum(report.summary.incomplete)} />
                    <Field label="Redirects" value={fmtNum(report.summary.failed_redirects)} />
                    <Field label="Timeouts" value={fmtNum(report.summary.timeouts)} />
                    <Field label="Availability" value={fmtPct(report.summary.availability * 100)} />
                  </div>
                </Panel>

                <Panel title="Safety">
                  <div className="flex flex-col">
                    <Field
                      label="Data loss"
                      value={
                        report.summary.data_loss ? (
                          <span className="text-[#ffb4ab]">
                            {report.summary.lost_writes.length} lost write(s)
                          </span>
                        ) : (
                          'none'
                        )
                      }
                      mono={false}
                    />
                    <Field
                      label="Diverged logs"
                      value={
                        report.summary.divergent_logs.length ? (
                          <span className="text-[#ffb4ab]">
                            {report.summary.divergent_logs.length} node(s)
                          </span>
                        ) : (
                          'none'
                        )
                      }
                      mono={false}
                    />
                    <Field label="Elections" value={report.leadership.total_elections} />
                    <Field label="Leader changes" value={report.leadership.leader_changes} />
                  </div>
                  <p className="text-[13px] text-[#8b919d] mt-3 leading-relaxed">
                    Lost writes and log divergence are decidable every time; linearizability is
                    not. That is why a campaign's pass/fail gates on the first two and records
                    the third informationally.
                  </p>
                </Panel>
              </div>
            </>
          )}
        </div>
      </div>
    </div>
  );
}
