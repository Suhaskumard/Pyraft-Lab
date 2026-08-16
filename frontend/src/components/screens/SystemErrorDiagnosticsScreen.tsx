import { api } from '../../api/client';
import { useLab } from '../../api/LabContext';
import { useResource } from '../../api/useResource';
import { Empty, Failure, Field, JobCard, Metric, Panel, ScreenHeader, fmtNum } from '../ui';
import { EventFeed } from './ClusterOverviewScreen';

/**
 * Where anything that went wrong ends up.
 *
 * Jobs are listed whatever their outcome, including cancelled ones, because a campaign
 * that was stopped halfway is a different fact from one that failed. A cancellation
 * lands *between* trials — a trial holds an in-flight run whose teardown is not
 * interruptible, and abandoning one would leave a half-written run directory behind.
 */
export function SystemErrorDiagnosticsScreen() {
  const { jobs, connected, cluster, status, events, notices } = useLab();
  const health = useResource(() => api.health(), [], { pollMs: 5000 });

  const failed = jobs.filter((job) => job.status === 'failed');
  const running = jobs.filter((job) => job.status === 'running');

  return (
    <div className="flex flex-col gap-4">
      <ScreenHeader
        title="Jobs & Diagnostics"
        description="Background campaigns, their progress and their failures, plus the state of the connection this page is watching them through."
      />

      <div className="grid grid-cols-2 md:grid-cols-4 xl:grid-cols-6 gap-3">
        <Metric
          label="API"
          value={health.data ? 'reachable' : 'unreachable'}
          tone={health.data ? 'good' : 'bad'}
        />
        <Metric label="Live stream" value={connected ? 'connected' : 'reconnecting'} tone={connected ? 'good' : 'warn'} />
        <Metric
          label="Cluster"
          value={cluster?.running ? (status?.status ?? 'running') : 'stopped'}
          tone={cluster?.running ? (status?.status === 'HEALTHY' ? 'good' : 'warn') : 'default'}
        />
        <Metric label="Jobs running" value={running.length} tone={running.length ? 'accent' : 'default'} />
        <Metric label="Jobs failed" value={failed.length} tone={failed.length ? 'bad' : 'good'} />
        <Metric label="Trace events" value={fmtNum(status?.eventCount)} />
      </div>

      {!connected && (
        <Failure message="The live stream is disconnected — reconnecting every two seconds. Readings on other pages may be stale until it comes back." />
      )}

      <div className="grid grid-cols-1 xl:grid-cols-3 gap-4">
        <Panel title="Environment">
          {health.error ? (
            <Failure message={health.error} />
          ) : health.data ? (
            <div className="flex flex-col">
              <Field label="Version" value={health.data.version} />
              <Field label="Workspace" value={health.data.workspace} />
              <Field label="Cluster running" value={cluster?.running ? 'yes' : 'no'} />
              {cluster?.config && (
                <>
                  <Field label="Nodes" value={cluster.config.nodes} />
                  <Field label="Persistence" value={cluster.config.data_dir ?? 'in-memory'} />
                  <Field
                    label="Election timeout"
                    value={`${(cluster.config.election_timeout_range[0] * 1000).toFixed(0)}–${(cluster.config.election_timeout_range[1] * 1000).toFixed(0)} ms`}
                  />
                  <Field
                    label="Heartbeat"
                    value={`${(cluster.config.heartbeat_interval * 1000).toFixed(0)} ms`}
                  />
                  <Field label="Seed" value={cluster.config.seed} />
                </>
              )}
            </div>
          ) : (
            <Empty title="Checking…" />
          )}
        </Panel>

        <Panel title="Recent errors" className="xl:col-span-2">
          {!notices.length && !failed.length ? (
            <Empty title="Nothing has failed" hint="Failed actions and jobs collect here." />
          ) : (
            <div className="flex flex-col gap-2">
              {notices
                .filter((notice) => notice.tone === 'error')
                .map((notice) => (
                  <Failure key={notice.id} message={notice.message} />
                ))}
              {failed.map((job) => (
                <Failure key={job.id} message={`${job.kind} ${job.id}: ${job.error}`} />
              ))}
            </div>
          )}
        </Panel>
      </div>

      <Panel title="Jobs" subtitle="newest first — campaigns, scenario runs and replays">
        {!jobs.length ? (
          <Empty
            title="No jobs yet"
            hint="Run a scenario or a campaign; it will appear here with live progress."
          />
        ) : (
          <div className="flex flex-col gap-2 max-h-[32rem] overflow-y-auto">
            {jobs.map((job) => (
              <JobCard key={job.id} job={job} />
            ))}
          </div>
        )}
      </Panel>

      {cluster?.running && (
        <Panel title="Live trace" subtitle="everything the cluster has narrated recently">
          <EventFeed events={events} height="max-h-96" />
        </Panel>
      )}
    </div>
  );
}
