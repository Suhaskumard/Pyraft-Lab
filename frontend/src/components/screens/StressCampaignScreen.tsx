import { useState } from 'react';
import { Flame } from 'lucide-react';
import { api } from '../../api/client';
import { useJobRunner } from '../../api/useJob';
import { CampaignControls } from './CampaignControls';
import { Empty, JobCard, Metric, Panel, ScreenHeader, Verdict, fmtMs } from '../ui';
import type { StressTrial, TrialCampaignReport } from '../../types/pyraft';

/**
 * Coverage across many cheap trials.
 *
 * Each trial takes one entry from a bounded catalog of fault archetypes — crash the
 * leader, crash a follower, isolate a minority, a latency spike, a loss burst, churn —
 * cycled by trial index, so N trials sweep every archetype rather than sampling
 * randomly. Trials run in memory: coverage is the point here, durability is chaos's.
 */
export function StressCampaignScreen() {
  const [nodes, setNodes] = useState(3);
  const [duration, setDuration] = useState('10s');
  const [trials, setTrials] = useState(10);
  const [seed, setSeed] = useState(0);
  const { job, running, starting, start, cancel } =
    useJobRunner<TrialCampaignReport<StressTrial>>('stress');

  const report = job?.status === 'succeeded' ? job.result : null;

  return (
    <div className="flex flex-col gap-4">
      <ScreenHeader
        title="Stress Campaign"
        description="A trial per fault archetype, over the same runner every other command uses. A failing trial is persisted through the ordinary machinery, so its run_id is directly replayable."
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
        onRun={() => void start(() => api.runStress({ nodes, duration, trials, seed }))}
        icon={<Flame className="w-3 h-3" />}
        label="Run campaign"
      />

      {job && (
        <Panel title="Campaign">
          <JobCard job={job} onCancel={cancel} />
        </Panel>
      )}

      {report && (
        <>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
            <Metric label="Trials" value={report.trials} />
            <Metric label="Passed" value={report.passed} tone="good" />
            <Metric label="Failed" value={report.failed} tone={report.failed ? 'bad' : 'good'} />
            <Metric
              label="Data loss"
              value={report.trial_results.filter((t) => t.data_loss).length}
              tone={report.trial_results.some((t) => t.data_loss) ? 'bad' : 'good'}
            />
          </div>

          <Panel
            title="Trials"
            subtitle="A trial passes when no committed write was lost and no two logs diverged"
          >
            {!report.trial_results.length ? (
              <Empty title="No trials recorded" />
            ) : (
              <div className="obsidian-table-container max-h-[30rem] overflow-y-auto">
                <table className="obsidian-table">
                  <thead>
                    <tr>
                      <th className="text-right">#</th>
                      <th>Fault archetype</th>
                      <th className="text-right">Seed</th>
                      <th>Data loss</th>
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
                        <td className="font-mono">{trial.fault_kind}</td>
                        <td className="font-mono text-right text-[#8b919d]">{trial.seed}</td>
                        <td className={trial.data_loss ? 'text-[#ffb4ab]' : 'text-[#8b919d]'}>
                          {trial.data_loss ? 'yes' : 'no'}
                        </td>
                        <td className={trial.divergent ? 'text-[#ffb4ab]' : 'text-[#8b919d]'}>
                          {trial.divergent ? 'yes' : 'no'}
                        </td>
                        <td className="font-mono text-[11px] text-[#8b919d]">
                          {trial.linearizable_verdict}
                        </td>
                        <td className="font-mono text-right">{fmtMs(trial.recovery_time_ms)}</td>
                        <td><Verdict ok={trial.passed} /></td>
                        <td className="font-mono text-[11px] text-[#8b919d]">{trial.run_id}</td>
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
