import { AlertTriangle, CheckCircle2, Info, X } from 'lucide-react';
import { useLab } from '../api/LabContext';

/**
 * Where a failed action ends up.
 *
 * Errors stay until dismissed; successes and notes fade. The message is the backend's
 * own `detail` string verbatim — "n2 is already down", "the cluster has no leader to
 * read from" — because those already say precisely what went wrong.
 */
export function NoticeTray() {
  const { notices, dismiss } = useLab();
  if (!notices.length) return null;

  return (
    <div className="fixed bottom-4 right-4 z-[100] flex flex-col gap-2 max-w-md">
      {notices.map((notice) => {
        const tone =
          notice.tone === 'error'
            ? 'border-[#93000a] bg-[#1c1214] text-[#ffb4ab]'
            : notice.tone === 'success'
              ? 'border-[#003919] bg-[#0e1a13] text-[#4ade80]'
              : 'border-[#30363d] bg-[#181c21] text-[#c0c7d4]';
        const Icon =
          notice.tone === 'error' ? AlertTriangle : notice.tone === 'success' ? CheckCircle2 : Info;

        return (
          <div
            key={notice.id}
            className={`flex items-start gap-2.5 px-3 py-2.5 rounded border animate-fade-in ${tone}`}
          >
            <Icon className="w-4 h-4 shrink-0 mt-0.5" />
            <p className="text-xs leading-relaxed flex-1 break-words">{notice.message}</p>
            <button
              onClick={() => dismiss(notice.id)}
              className="shrink-0 opacity-60 hover:opacity-100"
              aria-label="Dismiss"
            >
              <X className="w-3.5 h-3.5" />
            </button>
          </div>
        );
      })}
    </div>
  );
}
