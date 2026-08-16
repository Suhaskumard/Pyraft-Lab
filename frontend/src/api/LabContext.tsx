/**
 * The live view of the backend, shared by every screen.
 *
 * One WebSocket, not one poll per panel. The server pushes a status snapshot twice a
 * second and batches trace events at 10 Hz, so the header, the topology and the event
 * feed all move from the same stream and can never disagree with each other the way
 * three independent polls would. Screens that need something the stream does not carry
 * (a WAL listing, a persisted run) fetch it themselves through `useResource`.
 *
 * The socket is a *notification* channel, not the source of truth: on connect, and on
 * every reconnect, the provider re-reads `GET /api/cluster` so a client that missed
 * messages while disconnected recovers a correct view rather than a patched-up one.
 */

import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
} from 'react';
import type { ReactNode } from 'react';
import { ApiError, api, streamUrl } from './client';
import type { StreamMessage } from './client';
import type { ClusterPayload, ClusterStatus, Job, NodeState, TraceEvent } from '../types/pyraft';

const EVENT_BUFFER = 500;
const RECONNECT_MS = 2000;

export interface Notice {
  id: number;
  tone: 'error' | 'success' | 'info';
  message: string;
}

interface LabValue {
  connected: boolean;
  cluster: ClusterPayload | null;
  status: ClusterStatus | null;
  nodes: NodeState[];
  events: TraceEvent[];
  jobs: Job[];
  notices: Notice[];
  dismiss: (id: number) => void;
  notify: (tone: Notice['tone'], message: string) => void;
  /** Run an action, surface any failure as a notice, and refresh. Returns whether it
   *  succeeded, so a caller can avoid a follow-up step after a failure. */
  act: (label: string, work: () => Promise<unknown>) => Promise<boolean>;
  refresh: () => Promise<void>;
  refreshNodes: () => Promise<void>;
  trackJob: (job: Job) => void;
}

const LabContext = createContext<LabValue | null>(null);

export function LabProvider({ children }: { children: ReactNode }) {
  const [connected, setConnected] = useState(false);
  const [cluster, setCluster] = useState<ClusterPayload | null>(null);
  const [nodes, setNodes] = useState<NodeState[]>([]);
  const [events, setEvents] = useState<TraceEvent[]>([]);
  const [jobs, setJobs] = useState<Job[]>([]);
  const [notices, setNotices] = useState<Notice[]>([]);
  const noticeId = useRef(0);

  const notify = useCallback((tone: Notice['tone'], message: string) => {
    const id = ++noticeId.current;
    setNotices((prev) => [...prev.slice(-4), { id, tone, message }]);
    if (tone !== 'error') {
      window.setTimeout(() => setNotices((prev) => prev.filter((n) => n.id !== id)), 4000);
    }
  }, []);

  const dismiss = useCallback((id: number) => {
    setNotices((prev) => prev.filter((n) => n.id !== id));
  }, []);

  const refreshNodes = useCallback(async () => {
    try {
      setNodes(await api.nodes());
    } catch (error) {
      // A 409 here just means no cluster is running, which the rest of the UI already
      // knows from `cluster.running` — reporting it as a failure would be noise.
      if (!(error instanceof ApiError) || error.status !== 409) throw error;
      setNodes([]);
    }
  }, []);

  const refresh = useCallback(async () => {
    try {
      const payload = await api.cluster();
      setCluster(payload);
      if (payload.running) await refreshNodes();
      else setNodes([]);
    } catch (error) {
      setCluster(null);
      if (error instanceof ApiError && error.status === 0) setConnected(false);
    }
  }, [refreshNodes]);

  const act = useCallback(
    async (label: string, work: () => Promise<unknown>) => {
      try {
        await work();
        await refresh();
        return true;
      } catch (error) {
        notify('error', `${label}: ${error instanceof Error ? error.message : String(error)}`);
        return false;
      }
    },
    [notify, refresh]
  );

  const trackJob = useCallback((job: Job) => {
    setJobs((prev) => [job, ...prev.filter((j) => j.id !== job.id)].slice(0, 40));
  }, []);

  // --- the socket -------------------------------------------------------------------

  useEffect(() => {
    let socket: WebSocket | null = null;
    let retry: number | undefined;
    let closed = false;

    const connect = () => {
      socket = new WebSocket(streamUrl());

      socket.onopen = () => {
        setConnected(true);
        void refresh();
        void api.jobs().then(setJobs).catch(() => undefined);
      };

      socket.onmessage = (raw) => {
        const message: StreamMessage = JSON.parse(raw.data);
        switch (message.channel) {
          case 'hello':
            setCluster(message.payload);
            break;
          case 'status':
            setCluster((prev) => (prev ? { ...prev, status: message.payload } : prev));
            break;
          case 'cluster':
            void refresh();
            break;
          case 'events':
            setEvents((prev) => [...prev, ...message.payload].slice(-EVENT_BUFFER));
            break;
          case 'job':
            setJobs((prev) => {
              const next = prev.filter((j) => j.id !== message.payload.id);
              return [message.payload, ...next].slice(0, 40);
            });
            break;
        }
      };

      socket.onclose = () => {
        setConnected(false);
        if (!closed) retry = window.setTimeout(connect, RECONNECT_MS);
      };
      // `onclose` always follows `onerror`, so reconnecting is handled in one place.
      socket.onerror = () => socket?.close();
    };

    connect();
    return () => {
      closed = true;
      window.clearTimeout(retry);
      socket?.close();
    };
  }, [refresh]);

  // Node state is not on the stream — it is a full read of every node — so it is
  // refreshed on a slow timer while a cluster is up, and eagerly after any action.
  useEffect(() => {
    if (!cluster?.running) return;
    const timer = window.setInterval(() => void refreshNodes(), 1000);
    return () => window.clearInterval(timer);
  }, [cluster?.running, refreshNodes]);

  const value = useMemo<LabValue>(
    () => ({
      connected,
      cluster,
      status: cluster?.status ?? null,
      nodes,
      events,
      jobs,
      notices,
      dismiss,
      notify,
      act,
      refresh,
      refreshNodes,
      trackJob,
    }),
    [connected, cluster, nodes, events, jobs, notices, dismiss, notify, act, refresh, refreshNodes, trackJob]
  );

  return <LabContext.Provider value={value}>{children}</LabContext.Provider>;
}

export function useLab(): LabValue {
  const value = useContext(LabContext);
  if (value === null) throw new Error('useLab must be used inside a <LabProvider>');
  return value;
}
