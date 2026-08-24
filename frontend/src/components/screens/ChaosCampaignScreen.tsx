import { useState } from 'react';
import { AlertTriangle } from 'lucide-react';
import { api } from '../../api/client';
import { useJobRunner } from '../../api/useJob';
import { CampaignControls } from './CampaignControls';
import { Empty, Failure, JobCard, Metric, Panel, ScreenHeader, Verdict, fmtMs } from '../ui';
import type { ChaosTrial, TrialCampaignReport } from '../../types/pyraft';

/**
 * One invariant, checked every time: committed data is never silently lost.
 *
 * Unlike stress, a chaos trial's timeline is an open-ended random walk with no attempt
 * to keep a quorum alive, and every trial runs against a real per-node WAL — so a crash
 * genuinely discards in-memory state and recovery rebuilds from nothing but disk. This
 * is the campaign that found the four election and replication bugs in `raft/node.py`.
 */
export function ChaosCampaignScreen() {
  const [nodes, setNodes] = useState(5);
  const [duration, setDuration] = useState('10s');
  const [trials, setTrials] = useState(10);
  const [seed, setSeed] = useState(0);
  const { job, running, starting, start, cancel } =
    useJobRunner<TrialCampaignReport<ChaosTrial>>('chaos');

  const report = job?.status === 'succeeded' ? job.result : null;
  const lost = report?.trial_results.filter((trial) => trial.data_loss) ?? [];

  return (
    <div className="flex flex-col gap-4">
      <ScreenHeader
        title="Chaos Campaign"
        description="Randomized fault sequences with no attempt to preserve a quorum, each against a real WAL. A trial fails if a write the client was told succeeded is gone by the end of it."
      />

      <CampaignControls
        nodes={nodes}
        setNodes={setNodes}
        duration={duration}
        setDuration={setDuration}
        trials={trials}
        setTrials={setTrials}
        seed={seed}
        setSeed={setSeed}
        running={running || starting}
        onRun={() => void start(() => api.runChaos({ nodes, duration, trials, seed }))}
        icon={<AlertTriangle className="w-3 h-3" />}
        label="Unleash chaos"
        note="Every trial's fault walk derives from (seed, trial index) alone — a failing trial is a fixed case you can replay verbatim."
      />

      {job && (
        <Panel title="Campaign">
          <JobCard job={job} onCancel={cancel} />
        </Panel>
      )}

      {report && (
        <>
          {lost.length > 0 && (
            <Failure
              message={`${lost.length} trial(s) lost committed data. Each is reproducible from its seed — replay run ${lost[0].run_id} to see it happen again, bit for bit.`}
            />
          )}

          <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
            <Metric label="Trials" value={report.trials} />
            <Metric label="Passed" value={report.passed} tone="good" />
            <Metric label="Failed" value={report.failed} tone={report.failed ? 'bad' : 'good'} />
            <Metric
              label="Lost writes"
              value={report.trial_results.reduce((total, trial) => total + trial.lost_write_count, 0)}
              tone={lost.length ? 'bad' : 'good'}
            />
          </div>

          <Panel title="Trials">
            {!report.trial_results.length ? (
              <Empty title="No trials recorded" />
            ) : (
              <div className="obsidian-table-container max-h-[30rem] overflow-y-auto">
                <table className="obsidian-table">
                  <thead>
                    <tr>
                      <th className="text-right">#</th>
                      <th className="text-right">Faults</th>
                      <th className="text-right">Seed</th>
                      <th className="text-right">Lost writes</th>
                      <th>Diverged</th>
                      <th>Linearizability</th>
                      <th className="text-right">Recovery</th>
                      <th>Result</th>
                      <th>Run id</th>
                    </tr>
                  </thead>
                  <tbody>
                    {report.trial_results.map((trial) => (
                      <tr key={trial.trial_index}>
                        <td className="font-mono text-right text-[#8b919d]">{trial.trial_index}</td>
                        <td className="font-mono text-right">{trial.fault_count}</td>
                        <td className="font-mono text-right text-[#8b919d]">{trial.seed}</td>
                        <td
                          className={`font-mono text-right ${
                            trial.lost_write_count ? 'text-[#ffb4ab] font-semibold' : 'text-[#8b919d]'
                          }`}
                        >
                          {trial.lost_write_count}
                        </td>
                        <td className={trial.divergent ? 'text-[#ffb4ab]' : 'text-[#8b919d]'}>
                          {trial.divergent ? 'yes' : 'no'}
                        </td>
                        <td className="font-mono text-[13px] text-[#8b919d]">
                          {trial.linearizable_verdict}
                        </td>
                        <td className="font-mono text-right">{fmtMs(trial.recovery_time_ms)}</td>
                        <td><Verdict ok={trial.passed} /></td>
                        <td className="font-mono text-[13px] text-[#8b919d]">{trial.run_id}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </Panel>
        </>
      )}
    </div>
  );
}
