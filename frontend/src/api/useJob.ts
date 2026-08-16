/**
 * Start a background job and follow it.
 *
 * The server pushes every job change over the live stream, so the common case needs no
 * polling at all. The one-second poll below is a fallback for a dropped socket: a
 * campaign that finishes while the stream is down must still stop showing a spinner.
 */

import { useCallback, useEffect, useState } from 'react';
import { api } from './client';
import { useLab } from './LabContext';
import type { Job } from '../types/pyraft';

export function useJobRunner<T = unknown>(kind: Job['kind']) {
  const { jobs, trackJob, notify } = useLab();
  const [activeId, setActiveId] = useState<string | null>(null);
  const [starting, setStarting] = useState(false);

  // Fall back to the most recent job of this kind so a screen reopened mid-campaign,
  // or after a reload, picks up what is already running instead of looking idle.
  const job = (jobs.find((j) => j.id === activeId) ??
    jobs.find((j) => j.kind === kind) ??
    null) as Job<T> | null;

  const running = job?.status === 'running';

  useEffect(() => {
    if (!running || !job) return;
    const timer = window.setInterval(() => {
      void api
        .job(job.id)
        .then(trackJob)
        .catch(() => undefined);
    }, 1000);
    return () => window.clearInterval(timer);
  }, [running, job, trackJob]);

  const start = useCallback(
    async (launch: () => Promise<Job>) => {
      setStarting(true);
      try {
        const started = await launch();
        trackJob(started);
        setActiveId(started.id);
        return started;
      } catch (error) {
        notify('error', error instanceof Error ? error.message : String(error));
        return null;
      } finally {
        setStarting(false);
      }
    },
    [notify, trackJob]
  );

  const cancel = useCallback(
    async (id: string) => {
      try {
        trackJob(await api.cancelJob(id));
        notify('info', 'cancelling after the current trial finishes');
      } catch (error) {
        notify('error', error instanceof Error ? error.message : String(error));
      }
    },
    [notify, trackJob]
  );

  return { job, running, starting, start, cancel };
}
