/**
 * `useResource` — one fetch, its loading and error state, and an optional poll.
 *
 * Screens that read something the live stream does not carry (a WAL listing, a
 * scenario file, a persisted report) use this instead of hand-rolling three pieces of
 * state each. A 409 is kept as an error rather than swallowed: "no cluster is running"
 * is a real answer a panel should show, not an empty table.
 */

import { useCallback, useEffect, useRef, useState } from 'react';
import { ApiError } from './client';

export interface Resource<T> {
  data: T | null;
  error: string | null;
  status: number | null;
  loading: boolean;
  reload: () => Promise<void>;
  setData: (value: T | null) => void;
}

export function useResource<T>(
  fetcher: () => Promise<T>,
  deps: readonly unknown[],
  options: { pollMs?: number; enabled?: boolean } = {}
): Resource<T> {
  const { pollMs, enabled = true } = options;
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [status, setStatus] = useState<number | null>(null);
  const [loading, setLoading] = useState(enabled);

  // Kept in a ref so the effect below depends on `deps` alone; an inline arrow
  // fetcher would otherwise be a new value every render and re-fetch forever.
  const fetcherRef = useRef(fetcher);
  fetcherRef.current = fetcher;

  const load = useCallback(async () => {
    if (!enabled) return;
    try {
      const value = await fetcherRef.current();
      setData(value);
      setError(null);
      setStatus(null);
    } catch (err) {
      setData(null);
      setError(err instanceof Error ? err.message : String(err));
      setStatus(err instanceof ApiError ? err.status : null);
    } finally {
      setLoading(false);
    }
  }, [enabled]);

  useEffect(() => {
    if (!enabled) {
      setLoading(false);
      return;
    }
    setLoading(true);
    void load();
    if (!pollMs) return;
    const timer = window.setInterval(() => void load(), pollMs);
    return () => window.clearInterval(timer);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [load, pollMs, enabled, ...deps]);

  return { data, error, status, loading, reload: load, setData };
}
