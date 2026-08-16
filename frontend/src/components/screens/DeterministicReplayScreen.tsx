import { useState } from 'react';
import { CheckCircle2, RotateCcw, XCircle } from 'lucide-react';
import { api } from '../../api/client';
import { useJobRunner } from '../../api/useJob';
import { useResource } from '../../api/useResource';
import { Empty, Failure, JobCard, Panel, ScreenHeader, Skeleton } from '../ui';
import type { ReplayResult } from '../../types/pyraft';

/**
 * Reproduce a persisted run and diff its trace against the original.
 *
 * Replay does not *add* determinism, it goes looking for proof the determinism holds:
 * a fresh runner is reconstructed from nothing but the manifest, run again, and the new
 * trace is compared event for event — including the sequence number, because under a
 * virtual clock many events share one timestamp and a replay that reorders two
 * same-instant events is not a replay.
 *
 * A mismatch is reported, not raised. A run that has stopped reproducing is itself a
 * finding.
 */
export function DeterministicReplayScreen() {
  const results = useResource(() => api.results(), []);
  const [selected, setSelected] = useState<string | null>(null);
  const { job, running, starting, start } = useJobRunner<ReplayResult>('replay');

  const runId = selected ?? results.data?.[0]?.runId ?? null;
  const outcome = job?.status === 'succeeded' ? job.result : null;

  return (
    <div className="flex flex-col gap-4">
      <ScreenHeader
        title="Deterministic Replay"
        description="Every run is reproducible from its seed: one seed feeds every random source, and time only moves because the runner advances a virtual clock. This is the check that proves it."
      />

      <div className="grid grid-cols-1 xl:grid-cols-3 gap-4">
        <Panel title="Persisted runs" subtitle="pick one to reproduce">
          {results.loading ? (
            <Skeleton rows={5} />
          ) : results.error ? (
            <Failure message={results.error} />
          ) : !results.data?.length ? (
            <Empty
              title="Nothing to replay"
              hint="Run a scenario first — every run is persisted with the manifest replay needs."
            />
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
                  <div className="font-mono text-xs text-[#e0e2ea] truncate">{row.runId}</div>
                  <div className="font-mono text-[10px] text-[#8b919d] mt-0.5">
                    {row.scenarioName ?? '—'} · seed {row.seed ?? '—'}
                  </div>
                </button>
              ))}
            </div>
          )}
        </Panel>

        <Panel
          title="Replay"
          className="xl:col-span-2"
          subtitle={runId ? `will reconstruct ${runId} from its manifest alone` : undefined}
          actions={
            <button
              className="btn-obsidian btn-primary btn-sm"
              disabled={!runId || running || starting}
              onClick={() => void start(() => api.replay(runId!))}
            >
              <RotateCcw className="w-3 h-3" />
              {running ? 'Replaying…' : 'Replay'}
            </button>
          }
        >
          {!runId ? (
            <Empty title="Select a run" />
          ) : (
            <div className="flex flex-col gap-3">
              {outcome && (
                <div
                  className={`flex items-start gap-3 p-3 rounded border ${
                    outcome.matched
                      ? 'border-[#003919] bg-[#003919]/20 text-[#4ade80]'
                      : 'border-[#93000a] bg-[#93000a]/15 text-[#ffb4ab]'
                  }`}
                >
                  {outcome.matched ? (
                    <CheckCircle2 className="w-5 h-5 shrink-0" />
                  ) : (
                    <XCircle className="w-5 h-5 shrink-0" />
                  )}
                  <div className="min-w-0">
                    <p className="text-sm font-semibold">
                      {outcome.matched ? 'Bit-identical' : 'The trace diverged'}
                    </p>
                    <pre className="text-[11px] font-mono mt-1 whitespace-pre-wrap break-words">
                      {outcome.explanation}
                    </pre>
                  </div>
                </div>
              )}

              {job && <JobCard job={job} />}

              {!job && (
                <p className="text-xs text-[#8b919d] leading-relaxed">
                  Replay reconstructs an experiment runner from the manifest — the scenario, the
                  seed, and the three constructor knobs a scenario file does not itself encode —
                  runs it, and compares the new trace against the recorded one field for field.
                  A match means every election, every message drop and every commit happened at
                  the same simulated instant, in the same order.
                </p>
              )}
            </div>
          )}
        </Panel>
      </div>
    </div>
  );
}
