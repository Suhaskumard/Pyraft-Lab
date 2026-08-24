import { useEffect, useState } from 'react';
import { Save } from 'lucide-react';
import { api } from '../../api/client';
import { useLab } from '../../api/LabContext';
import { useResource } from '../../api/useResource';
import { Failure, Panel, ScreenHeader, Skeleton } from '../ui';
import type { ClusterConfigForm } from '../../types/pyraft';

/**
 * The saved `cluster.yaml`, edited in place.
 *
 * This writes the file `pyraft-lab cluster start --config` reads, so a setting saved
 * here applies to a cluster started from the terminal too. It does *not* change a
 * cluster that is already running: a member's election timeout is fixed when the node
 * is built, and pretending otherwise would show a value the cluster is not using.
 */
export function SettingsScreen() {
  const { cluster, notify } = useLab();
  const config = useResource(() => api.getConfig(), []);

  const [form, setForm] = useState<ClusterConfigForm | null>(null);
  const [busy, setBusy] = useState(false);

  useEffect(() => {
    if (!config.data || form) return;
    // `heartbeatTooSlow` is a verdict the server derives from the timers, not a field
    // it accepts back — sending it would be asking the server to agree with itself.
    const {
      path: _path,
      saved: _saved,
      heartbeatTooSlow: _tooSlow,
      ...rest
    } = config.data;
    setForm(rest);
  }, [config.data, form]);

  const save = async () => {
    if (!form) return;
    setBusy(true);
    try {
      config.setData(await api.saveConfig(form));
      notify('success', 'cluster.yaml saved');
    } catch (error) {
      notify('error', error instanceof Error ? error.message : String(error));
    } finally {
      setBusy(false);
    }
  };

  const invalid = form ? form.electionTimeoutMinMs >= form.electionTimeoutMaxMs : false;

  return (
    <div className="flex flex-col gap-4">
      <ScreenHeader
        title="Cluster Settings"
        description="The defaults a new cluster is built from. Written to cluster.yaml in the workspace, so the CLI and this app read the same file."
        actions={
          <button
            className="btn-obsidian btn-primary btn-sm"
            onClick={() => void save()}
            disabled={busy || !form || invalid}
          >
            <Save className="w-3.5 h-3.5" />
            {busy ? 'Saving…' : 'Save'}
          </button>
        }
      />

      {config.loading || !form ? (
        <Skeleton rows={6} />
      ) : config.error ? (
        <Panel><Failure message={config.error} /></Panel>
      ) : (
        <>
          {cluster?.running && (
            <div className="flex items-start gap-2.5 p-3 rounded border border-[#4f3400] bg-[#4f3400]/20 text-[#ffba42]">
              <p className="text-sm leading-relaxed">
                A cluster is running. Saving here changes what the <em>next</em> cluster is built
                from — a node's timers are fixed when it is constructed, so nothing below affects
                the one currently up.
              </p>
            </div>
          )}

          <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
            <Panel title="Cluster" subtitle="size, persistence, and the seed every node's RNG derives from">
              <div className="flex flex-col gap-3">
                <Row
                  label="Nodes"
                  hint="Ids are generated, not configured: n1 … nN."
                  value={form.nodes}
                  min={1}
                  max={15}
                  onChange={(nodes) => setForm({ ...form, nodes })}
                />
                <Row
                  label="Seed"
                  hint="One seed feeds every node's RNG, so a cluster's election order is reproducible."
                  value={form.seed}
                  min={0}
                  max={999999}
                  onChange={(seed) => setForm({ ...form, seed })}
                />
                <label className="flex items-start gap-2.5 cursor-pointer pt-1">
                  <input
                    type="checkbox"
                    checked={form.persistent}
                    onChange={(event) => setForm({ ...form, persistent: event.target.checked })}
                    className="accent-[#58a6ff] mt-0.5"
                  />
                  <span>
                    <span className="text-sm text-[#e0e2ea]">Persist each node to a WAL</span>
                    <span className="block text-[13px] text-[#8b919d] mt-0.5">
                      Only a WAL-backed node is genuinely rebuilt from disk on restart — stopping a
                      node never clears its in-memory state on its own. Required for the WAL and
                      snapshot pages.
                    </span>
                  </span>
                </label>
              </div>
            </Panel>

            <Panel title="Timers" subtitle="real, not test-fast — this is a live cluster a person interacts with">
              <div className="flex flex-col gap-3">
                <Row
                  label="Election timeout min (ms)"
                  value={form.electionTimeoutMinMs}
                  min={10}
                  max={5000}
                  onChange={(v) => setForm({ ...form, electionTimeoutMinMs: v })}
                />
                <Row
                  label="Election timeout max (ms)"
                  hint="Randomized within this range per node, which is what stops every follower campaigning at once."
                  value={form.electionTimeoutMaxMs}
                  min={10}
                  max={5000}
                  onChange={(v) => setForm({ ...form, electionTimeoutMaxMs: v })}
                />
                <Row
                  label="Heartbeat interval (ms)"
                  hint="Must stay comfortably under the minimum election timeout, or followers will campaign against a healthy leader."
                  value={form.heartbeatIntervalMs}
                  min={5}
                  max={2000}
                  onChange={(v) => setForm({ ...form, heartbeatIntervalMs: v })}
                />
                <Row
                  label="Client timeout (ms)"
                  hint="How long the client waits for a reply before trying a different node."
                  value={form.clientTimeoutMs}
                  min={10}
                  max={5000}
                  onChange={(v) => setForm({ ...form, clientTimeoutMs: v })}
                />
                {invalid && <Failure message="The minimum election timeout must be below the maximum." />}
                {!invalid && form.heartbeatIntervalMs >= form.electionTimeoutMinMs && (
                  <div className="text-[13px] text-[#ffba42] leading-relaxed">
                    <strong>This cluster will not hold a leader.</strong> A heartbeat of{' '}
                    {form.heartbeatIntervalMs} ms cannot reach a follower whose election timeout
                    starts at {form.electionTimeoutMinMs} ms, so followers will campaign against a
                    leader that is perfectly healthy — the term climbs without limit and writes
                    rarely commit. Saving is still allowed: watching that happen is a valid
                    experiment, but it is not a configuration to leave in place.
                  </div>
                )}
              </div>
            </Panel>
          </div>

          <Panel title="File">
            <p className="font-mono text-[13px] text-[#8b919d]">
              {config.data?.path} {config.data?.saved ? '(saved)' : '(not written yet — showing built-in defaults)'}
            </p>
          </Panel>
        </>
      )}
    </div>
  );
}

function Row({
  label,
  hint,
  value,
  min,
  max,
  onChange,
}: {
  label: string;
  hint?: string;
  value: number;
  min: number;
  max: number;
  onChange: (value: number) => void;
}) {
  return (
    <div className="flex flex-col gap-1">
      <div className="flex items-center justify-between gap-3">
        <span className="text-sm text-[#c0c7d4]">{label}</span>
        <input
          type="number"
          min={min}
          max={max}
          value={value}
          onChange={(event) => onChange(Number(event.target.value))}
          className="w-28 bg-[#0b0e14] border border-[#30363d] rounded px-2 py-1 font-mono text-sm text-[#e0e2ea] focus:border-[#58a6ff] outline-none"
        />
      </div>
      {hint && <p className="text-[13px] text-[#8b919d] leading-relaxed pr-32">{hint}</p>}
    </div>
  );
}
