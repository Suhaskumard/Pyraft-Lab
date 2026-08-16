import { useState } from 'react';
import { Plus, RefreshCw, Search, Trash2 } from 'lucide-react';
import { api } from '../../api/client';
import { useLab } from '../../api/LabContext';
import { useResource } from '../../api/useResource';
import { Empty, Failure, Panel, ScreenHeader, fmtValue } from '../ui';

/**
 * The replicated state machine, read and written through a real Raft client.
 *
 * Every write here goes through `KVClient`: it guesses a leader, gets redirected if it
 * guessed wrong, retries with backoff, and is answered only once the entry has
 * committed *and* applied. So a `put` that returns has genuinely been replicated to a
 * majority — this table is not a cache in front of the cluster, it is the cluster.
 */
export function KeyValueStoreScreen() {
  const { notify, refresh } = useLab();
  const store = useResource(() => api.listKv(), [], { pollMs: 1500 });

  const [key, setKey] = useState('');
  const [value, setValue] = useState('');
  const [casKey, setCasKey] = useState('');
  const [casExpected, setCasExpected] = useState('');
  const [casNew, setCasNew] = useState('');
  const [lookup, setLookup] = useState('');
  const [lookupResult, setLookupResult] = useState<string | null>(null);
  const [filter, setFilter] = useState('');
  const [busy, setBusy] = useState(false);

  const run = async (label: string, work: () => Promise<unknown>) => {
    setBusy(true);
    try {
      await work();
      await store.reload();
      await refresh();
    } catch (error) {
      notify('error', `${label}: ${error instanceof Error ? error.message : String(error)}`);
    } finally {
      setBusy(false);
    }
  };

  const records = (store.data?.records ?? []).filter((record) =>
    filter ? record.key.toLowerCase().includes(filter.toLowerCase()) : true
  );

  return (
    <div className="flex flex-col gap-4">
      <ScreenHeader
        title="Key-Value Store"
        description="The replicated state machine every node applies its committed log into. Reads are served under ReadIndex — the leader confirms it still leads before answering."
        actions={
          <button className="btn-obsidian btn-secondary btn-sm" onClick={() => void store.reload()}>
            <RefreshCw className="w-3.5 h-3.5" />
            Refresh
          </button>
        }
      />

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
        <Panel title="Write" subtitle="PUT — appended, replicated, then answered">
          <div className="flex flex-col gap-2">
            <Input placeholder="key" value={key} onChange={setKey} />
            <Input placeholder="value" value={value} onChange={setValue} />
            <button
              className="btn-obsidian btn-primary btn-sm"
              disabled={busy || !key}
              onClick={() =>
                void run('put', async () => {
                  await api.put(key, value);
                  setKey('');
                  setValue('');
                })
              }
            >
              <Plus className="w-3 h-3" />
              Put
            </button>
          </div>
        </Panel>

        <Panel title="Compare-and-swap" subtitle="Loses cleanly when the value has moved">
          <div className="flex flex-col gap-2">
            <Input placeholder="key" value={casKey} onChange={setCasKey} />
            <Input placeholder="expected value" value={casExpected} onChange={setCasExpected} />
            <Input placeholder="new value" value={casNew} onChange={setCasNew} />
            <button
              className="btn-obsidian btn-secondary btn-sm"
              disabled={busy || !casKey}
              onClick={() =>
                void run('cas', async () => {
                  const result = await api.cas(casKey, casExpected, casNew);
                  notify(
                    result.ok ? 'success' : 'info',
                    result.ok
                      ? `cas ${casKey}: swapped`
                      : `cas ${casKey}: lost — the value did not match "${casExpected}"`
                  );
                })
              }
            >
              Compare and swap
            </button>
          </div>
        </Panel>

        <Panel title="Read" subtitle="GET — never enters the log">
          <div className="flex flex-col gap-2">
            <Input placeholder="key" value={lookup} onChange={setLookup} />
            <button
              className="btn-obsidian btn-secondary btn-sm"
              disabled={busy || !lookup}
              onClick={() =>
                void run('get', async () => {
                  const result = await api.get(lookup);
                  setLookupResult(
                    result.found ? `${result.key} = ${fmtValue(result.value)}` : `no such key: ${result.key}`
                  );
                })
              }
            >
              <Search className="w-3 h-3" />
              Get
            </button>
            {lookupResult && <pre className="terminal-block">{lookupResult}</pre>}
          </div>
        </Panel>
      </div>

      <Panel
        title="Applied state"
        subtitle={
          store.data
            ? `read from ${store.data.nodeId} — ${records.length} key${records.length === 1 ? '' : 's'}`
            : undefined
        }
        actions={
          <input
            value={filter}
            onChange={(event) => setFilter(event.target.value)}
            placeholder="filter keys…"
            className="bg-[#0b0e14] border border-[#30363d] rounded px-2 py-1 font-mono text-xs text-[#e0e2ea] placeholder:text-[#8b919d] focus:border-[#58a6ff] outline-none w-40"
          />
        }
      >
        {store.error ? (
          <Failure message={store.error} />
        ) : !records.length ? (
          <Empty
            title={filter ? `No key matches “${filter}”` : 'The store is empty'}
            hint={filter ? undefined : 'Write a key above — it will be replicated before it appears here.'}
          />
        ) : (
          <div className="obsidian-table-container">
            <table className="obsidian-table">
              <thead>
                <tr>
                  <th>Key</th>
                  <th>Value</th>
                  <th className="text-right">Writes</th>
                  <th className="text-right">Term</th>
                  <th className="text-right">Log index</th>
                  <th />
                </tr>
              </thead>
              <tbody>
                {records.map((record) => (
                  <tr key={record.key}>
                    <td className="font-mono font-medium text-[#58a6ff]">{record.key}</td>
                    <td className="font-mono">{fmtValue(record.value)}</td>
                    <td className="font-mono text-right text-[#8b919d]">{record.version}</td>
                    <td className="font-mono text-right">{record.modifiedTerm ?? '—'}</td>
                    <td className="font-mono text-right">{record.modifiedIndex ?? '—'}</td>
                    <td className="text-right">
                      <button
                        className="btn-obsidian btn-ghost btn-sm"
                        title={`Delete ${record.key}`}
                        onClick={() => void run('delete', () => api.del(record.key))}
                      >
                        <Trash2 className="w-3 h-3" />
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </Panel>
    </div>
  );
}

function Input({
  placeholder,
  value,
  onChange,
}: {
  placeholder: string;
  value: string;
  onChange: (value: string) => void;
}) {
  return (
    <input
      value={value}
      placeholder={placeholder}
      onChange={(event) => onChange(event.target.value)}
      className="bg-[#0b0e14] border border-[#30363d] rounded px-2.5 py-1.5 font-mono text-xs text-[#e0e2ea] placeholder:text-[#8b919d] focus:border-[#58a6ff] outline-none"
    />
  );
}
