# Architecture

A living document. Each phase appends the decisions it made and why, so the Day 14
technical report is assembled rather than reconstructed from memory.

## Layers

```
CLIENTS            put / get / delete / cas, leader discovery, retry with backoff
   |
RAFT CLUSTER       3-5 nodes, each with a log, a KV state machine and a WAL
   |
TRANSPORT          InMemoryBus today; SimulatedNetwork (faults) from Day 8
   |
EXPERIMENT LAYER   scenario runner, fault injector, metrics, analytics
```

The transport sits between the cluster and everything below it deliberately: a Raft
node has no idea whether its messages are crossing an honest bus or a simulated
network dropping 30 % of them.

## Decisions

### D1 — asyncio streams over gRPC

**Decision:** plain asyncio and an in-process message bus. No gRPC, no protobuf.

**Why:** the whole point of the project is a *deterministic* network. gRPC would put a
real kernel socket between two nodes, which is exactly the source of nondeterminism the
lab exists to eliminate — and it would cost a day of the fifteen to set up. Nothing in
the requirements needs cross-process transport. If it is ever wanted, `Transport` is a
three-method protocol and a gRPC implementation slots in behind it without the Raft
code noticing.

### D1 — messages are frozen, tagged, and round-trip on every send

**Decision:** every message is an immutable Pydantic model declaring a unique `KIND`,
registered at class-definition time. The bus serializes and deserializes every message
even though sender and receiver share a process.

**Why:** three problems disappear at once.

- *Type loss.* A field typed as the `Message` base would serialize to `{}` and drop
  every subclass field. Tagging the body with `kind` and rebuilding the concrete class
  from a registry makes the round trip lossless.
- *Shared mutable state.* Handing a peer a reference to a live dict is not something a
  real network permits. Round-tripping guarantees the receiver holds a copy, so a bug
  where a node mutates a message another node already "received" cannot exist.
- *Late failures.* An unserializable message fails on the send that created it, not on
  Day 11 when the experiment runner first tries to persist it.

The cost is real CPU per send, and it is worth paying — the simulation is virtual-clock
driven, so serialization cost never distorts a measured latency.

### D1 — an unknown destination is dropped, an unknown sender raises

**Decision:** `send()` to an unregistered destination increments a counter and returns.
`send()` *from* an unregistered node raises `UnknownNode`.

**Why:** on a real network a sender cannot distinguish a crashed peer from a slow one,
so the transport must not offer that information — a node that could detect delivery
failure synchronously would be able to cheat at consensus. An unregistered *sender*,
by contrast, is never a network condition; it is a programming error, and should fail
loudly.

### D1 — JSON now, msgpack behind the same protocol

**Decision:** `JsonCodec` with sorted keys and no whitespace, behind a `Codec` protocol.

**Why:** readable in test failures and dependency-free. Sorted keys make encoding
deterministic, which matters once runs must be byte-for-byte reproducible from a seed.
Swapping in msgpack later is one new class implementing two methods.

### D1 — the log is 1-indexed (recorded now, implemented Day 2)

The Raft paper indexes the log from 1. Matching it exactly avoids a class of off-by-one
bugs in `prevLogIndex` handling that are very hard to see in a partition trace. Index 0
holds a sentinel entry at term 0.

## Deferred

- **InstallSnapshot.** Listed in the specification's RPC table but assigned no day, and
  genuinely out of scope for 15 days. Declared deferred rather than half-built.
