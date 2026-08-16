/**
 * The shared vocabulary every screen is built from.
 *
 * `Metric` is the important one: it takes `number | null` and renders `n/a` for null,
 * because the backend returns null for anything it has not measured yet and a
 * dashboard that shows 0 ms for "no request has completed" is lying.
 */

import type { ReactNode } from 'react';
import { AlertTriangle, Inbox, Loader2, Server, X } from 'lucide-react';
import type { Job, NodeRole } from '../types/pyraft';

// --- formatting ------------------------------------------------------------------------

export const fmtMs = (value: number | null | undefined, digits = 1) =>
  value === null || value === undefined ? 'n/a' : `${value.toFixed(digits)} ms`;

export const fmtNum = (value: number | null | undefined, digits = 0) =>
  value === null || value === undefined ? 'n/a' : value.toLocaleString(undefined, {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  });

export const fmtPct = (value: number | null | undefined, digits = 2) =>
  value === null || value === undefined ? 'n/a' : `${value.toFixed(digits)}%`;

export const fmtBytes = (value: number | null | undefined) => {
  if (value === null || value === undefined) return 'n/a';
  if (value < 1024) return `${value} B`;
  if (value < 1024 * 1024) return `${(value / 1024).toFixed(1)} KB`;
  return `${(value / 1024 / 1024).toFixed(2)} MB`;
};

export const fmtValue = (value: unknown) =>
  typeof value === 'string' ? value : JSON.stringify(value);

// --- containers ------------------------------------------------------------------------

export function Panel({
  title,
  subtitle,
  actions,
  children,
  className = '',
  bodyClassName = '',
}: {
  title?: string;
  subtitle?: string;
  actions?: ReactNode;
  children: ReactNode;
  className?: string;
  bodyClassName?: string;
}) {
  return (
    <section className={`obsidian-card flex flex-col ${className}`}>
      {(title || actions) && (
        <header className="flex items-start justify-between gap-3 mb-3">
          <div className="min-w-0">
            {title && <h2 className="label-caps text-[#e0e2ea]">{title}</h2>}
            {subtitle && <p className="text-[11px] text-[#8b919d] mt-0.5">{subtitle}</p>}
          </div>
          {actions && <div className="flex items-center gap-1.5 shrink-0">{actions}</div>}
        </header>
      )}
      <div className={`min-w-0 ${bodyClassName}`}>{children}</div>
    </section>
  );
}

export function ScreenHeader({
  title,
  description,
  actions,
}: {
  title: string;
  description: string;
  actions?: ReactNode;
}) {
  return (
    <div className="flex items-start justify-between gap-4 mb-4">
      <div>
        <h1 className="text-lg font-semibold text-[#e0e2ea] tracking-tight">{title}</h1>
        <p className="text-xs text-[#8b919d] mt-1 max-w-3xl">{description}</p>
      </div>
      {actions && <div className="flex items-center gap-2 shrink-0">{actions}</div>}
    </div>
  );
}

// --- readings ---------------------------------------------------------------------------

export function Metric({
  label,
  value,
  tone = 'default',
  hint,
}: {
  label: string;
  value: ReactNode;
  tone?: 'default' | 'good' | 'warn' | 'bad' | 'accent';
  hint?: string;
}) {
  const colors = {
    default: 'text-[#e0e2ea]',
    good: 'text-[#4ade80]',
    warn: 'text-[#ffba42]',
    bad: 'text-[#ffb4ab]',
    accent: 'text-[#58a6ff]',
  };
  return (
    <div className="metric-box" title={hint}>
      <div className="label-caps">{label}</div>
      <div className={`metric-value ${colors[tone]}`}>{value}</div>
    </div>
  );
}

export function Field({ label, value, mono = true }: { label: string; value: ReactNode; mono?: boolean }) {
  return (
    <div className="flex items-baseline justify-between gap-3 py-1 border-b border-[#30363d] last:border-0">
      <span className="text-[11px] text-[#8b919d] shrink-0">{label}</span>
      <span className={`text-xs text-[#e0e2ea] text-right truncate ${mono ? 'font-mono' : ''}`}>
        {value}
      </span>
    </div>
  );
}

export function RolePill({ role }: { role: NodeRole | string }) {
  const cls =
    role === 'leader'
      ? 'status-pill-leader'
      : role === 'candidate'
        ? 'status-pill-candidate'
        : role === 'down'
          ? 'status-pill-down'
          : 'status-pill-follower';
  return (
    <span className={`status-pill ${cls}`}>
      <span className="status-dot" />
      {String(role).toUpperCase()}
    </span>
  );
}

export function HealthPill({ status }: { status: string }) {
  const cls =
    status === 'HEALTHY'
      ? 'status-pill-healthy'
      : status === 'DEGRADED'
        ? 'status-pill-candidate'
        : 'status-pill-down';
  return (
    <span className={`status-pill ${cls}`}>
      <span className="status-dot status-dot-pulse" />
      {status}
    </span>
  );
}

export function Verdict({ ok, yes = 'PASS', no = 'FAIL' }: { ok: boolean | null | undefined; yes?: string; no?: string }) {
  if (ok === null || ok === undefined) {
    return <span className="status-pill status-pill-candidate"><span className="status-dot" />UNPROVEN</span>;
  }
  return (
    <span className={`status-pill ${ok ? 'status-pill-healthy' : 'status-pill-down'}`}>
      <span className="status-dot" />
      {ok ? yes : no}
    </span>
  );
}

// --- states ------------------------------------------------------------------------------

export function Empty({ title, hint, icon }: { title: string; hint?: string; icon?: ReactNode }) {
  return (
    <div className="flex flex-col items-center justify-center text-center py-10 px-4 gap-2">
      <div className="text-[#8b919d]">{icon ?? <Inbox className="w-6 h-6" />}</div>
      <p className="text-sm text-[#c0c7d4]">{title}</p>
      {hint && <p className="text-[11px] text-[#8b919d] max-w-md">{hint}</p>}
    </div>
  );
}

export function Failure({ message }: { message: string }) {
  return (
    <div className="flex items-start gap-2.5 p-3 rounded border border-[#93000a] bg-[#93000a]/15 text-[#ffb4ab]">
      <AlertTriangle className="w-4 h-4 shrink-0 mt-0.5" />
      <p className="text-xs leading-relaxed font-mono">{message}</p>
    </div>
  );
}

export function Loading({ label = 'Loading' }: { label?: string }) {
  return (
    <div className="flex items-center justify-center gap-2 py-8 text-[#8b919d] text-xs">
      <Loader2 className="w-4 h-4 animate-spin" />
      {label}…
    </div>
  );
}

export function Skeleton({ rows = 4 }: { rows?: number }) {
  return (
    <div className="flex flex-col gap-2">
      {Array.from({ length: rows }, (_, i) => (
        <div key={i} className="skeleton h-6 w-full" />
      ))}
    </div>
  );
}

/**
 * The gate every live-cluster screen sits behind.
 *
 * Distinguishing "no cluster" from "an idle cluster" is the whole point: the backend
 * answers 409 for the first, and a screen that rendered an empty table instead would
 * be showing a cluster that is fine but empty, which is a different system state.
 */
export function RequireCluster({
  running,
  children,
  onStart,
}: {
  running: boolean;
  children: ReactNode;
  onStart?: () => void;
}) {
  if (running) return <>{children}</>;
  return (
    <Panel>
      <Empty
        icon={<Server className="w-6 h-6" />}
        title="No cluster is running"
        hint="Start one from the header to bring the live pages up. Laboratory pages — scenarios, campaigns, results — work without a running cluster."
      />
      {onStart && (
        <div className="flex justify-center pb-2">
          <button className="btn-obsidian btn-primary btn-sm" onClick={onStart}>
            Start cluster
          </button>
        </div>
      )}
    </Panel>
  );
}

// --- terminals and jobs ---------------------------------------------------------------------

export function Terminal({ lines, empty = '(nothing yet)' }: { lines: string[]; empty?: string }) {
  return (
    <pre className="terminal-block whitespace-pre-wrap break-words">
      {lines.length ? lines.join('\n') : empty}
    </pre>
  );
}

export function JobCard({ job, onCancel }: { job: Job; onCancel?: (id: string) => void }) {
  const pct = job.total ? Math.round((job.completed / job.total) * 100) : 0;
  const tone =
    job.status === 'succeeded'
      ? 'text-[#4ade80]'
      : job.status === 'running'
        ? 'text-[#58a6ff]'
        : job.status === 'cancelled'
          ? 'text-[#ffba42]'
          : 'text-[#ffb4ab]';

  return (
    <div className="flex flex-col gap-2 p-3 rounded border border-[#30363d] bg-[#181c21]">
      <div className="flex items-center justify-between gap-3">
        <div className="flex items-center gap-2 min-w-0">
          {job.status === 'running' && <Loader2 className="w-3.5 h-3.5 animate-spin text-[#58a6ff]" />}
          <span className="font-mono text-xs text-[#e0e2ea] truncate">{job.id}</span>
          <span className={`text-[10px] font-semibold uppercase tracking-wide ${tone}`}>
            {job.cancelling ? 'cancelling' : job.status}
          </span>
        </div>
        {job.status === 'running' && onCancel && (
          <button className="btn-obsidian btn-ghost btn-sm" onClick={() => onCancel(job.id)}>
            <X className="w-3 h-3" />
            Cancel
          </button>
        )}
      </div>

      {job.total > 0 && (
        <div className="flex items-center gap-2">
          <div className="h-1.5 flex-1 rounded-full bg-[#272a30] overflow-hidden">
            <div
              className="h-full bg-[#58a6ff] transition-all duration-300"
              style={{ width: `${pct}%` }}
            />
          </div>
          <span className="font-mono text-[10px] text-[#8b919d] shrink-0">
            {job.completed}/{job.total}
          </span>
        </div>
      )}

      {job.error && <Failure message={job.error} />}
      {job.lines.length > 0 && (
        <pre className="terminal-block max-h-40 overflow-y-auto text-[11px]">
          {job.lines.join('\n')}
        </pre>
      )}
    </div>
  );
}
