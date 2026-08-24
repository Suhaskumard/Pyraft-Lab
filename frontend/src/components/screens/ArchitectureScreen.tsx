import { useLab } from '../../api/LabContext';
import { Panel, ScreenHeader } from '../ui';

/**
 * The four layers, and what is running in each of them right now.
 *
 * The transport sits between the cluster and the experiment layer deliberately: a Raft
 * node has no idea whether its messages are crossing an honest bus or a simulated
 * network dropping a third of them. That is what makes the same node code testable
 * under faults without a single branch for "am I being tested".
 */
export function ArchitectureScreen() {
  const { cluster, status } = useLab();

  const layers = [
    {
      name: 'Clients',
      accent: '#58a6ff',
      detail: 'put / get / delete / cas, leader discovery, retry with backoff',
      live: status?.client
        ? `${status.client.successes} succeeded · ${status.client.redirects} redirects · ${status.client.timeouts} timeouts`
        : 'no client traffic yet',
      note: 'A redirect is normal traffic, not an error: a follower that knows the leader names it, so the next attempt is informed rather than another guess.',
    },
    {
      name: 'Raft cluster',
      accent: '#8957e5',
      detail: 'each node: a log, a KV state machine, and optionally a WAL',
      live: cluster?.running
        ? `${status?.nodeCount ?? 0} nodes · term ${status?.term ?? 0} · leader ${status?.leader ?? '(none)'}`
        : 'not running',
      note: 'A write is answered after it applies, never when it is appended — an appended entry can still be truncated by a future leader.',
    },
    {
      name: 'Transport',
      accent: '#ffba42',
      detail: 'SimulatedNetwork: latency, jitter, loss, partitions',
      live: status
        ? `${status.messagesSent.toLocaleString()} sent · ${status.droppedPackets} dropped · ${status.delayed} delayed`
        : 'not running',
      note: 'Every message round-trips through the codec even though sender and receiver share a process, so a node can never hold a reference to a message another node already received.',
    },
    {
      name: 'Experiment layer',
      accent: '#4ade80',
      detail: 'scenario runner, fault injector, metrics, linearizability, replay',
      live: 'runs on a virtual clock, in worker threads of its own',
      note: 'Time only moves because the runner advances a VirtualClock, which is what lets thirty simulated seconds finish in milliseconds and still reproduce bit-for-bit.',
    },
  ];

  return (
    <div className="flex flex-col gap-4">
      <ScreenHeader
        title="Architecture Map"
        description="How the pieces stack, and what each layer is doing at this moment."
      />

      <div className="flex flex-col gap-3">
        {layers.map((layer, index) => (
          <div key={layer.name} className="flex flex-col items-center">
            <div
              className="w-full obsidian-card"
              style={{ borderLeft: `3px solid ${layer.accent}` }}
            >
              <div className="flex flex-wrap items-baseline justify-between gap-3">
                <h3 className="text-base font-semibold" style={{ color: layer.accent }}>
                  {layer.name}
                </h3>
                <span className="font-mono text-[13px] text-[#e0e2ea]">{layer.live}</span>
              </div>
              <p className="text-sm text-[#c0c7d4] mt-1">{layer.detail}</p>
              <p className="text-[13px] text-[#8b919d] mt-2 leading-relaxed">{layer.note}</p>
            </div>
            {index < layers.length - 1 && <div className="h-4 w-px bg-[#30363d]" />}
          </div>
        ))}
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <Panel title="Why there is no daemon">
          <p className="text-sm text-[#c0c7d4] leading-relaxed">
            A cluster lives inside the process serving this API and lives for exactly as long as
            it. No PID file, no socket, no background service. The project already rejects real
            kernel sockets for node-to-node traffic — a real socket is precisely the source of
            nondeterminism the laboratory exists to eliminate — so inventing an IPC layer just so
            a second command could address a running cluster would be new architecture rather
            than implementation.
          </p>
        </Panel>

        <Panel title="Why campaigns get their own threads">
          <p className="text-sm text-[#c0c7d4] leading-relaxed">
            An experiment runs on a virtual clock that advances as fast as the CPU allows and
            never yields to real time. Awaiting one on this API's event loop would freeze the
            live cluster sharing that loop — its heartbeat and election timers run on a wall
            clock and need the loop back every few tens of milliseconds — so a thirty-second
            benchmark would look to it like a thirty-second freeze and depose its leader several
            times over. Each campaign therefore gets a worker thread with an event loop of its
            own, and the two share nothing but a progress record.
          </p>
        </Panel>
      </div>
    </div>
  );
}
