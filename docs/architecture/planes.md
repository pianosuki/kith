# Runtime Plane Contracts

The runtime is organized into five planes. Each plane owns a specific category
of data and is forbidden from owning other categories. The plane contracts are
the framework's most important invariants: they are what let the runtime scale
horizontally instead of hitting the per-connection N² ceiling that a
reactor-and-connection-centric area-of-interest model runs into. One stage
vocabulary spans the planes: frames **publish** into a plane's stream (ingress),
the gateway **delivers** the composed view per session (per-session egress), and
a **dispatch** is the worker-pool handoff of one decoded frame to its handler.

The contracts are enforced by
`tools/check_runtime_planes.py` (include-based rules in
`tools/plane_rules.yaml`) and by review. A change that violates a plane
invariant is rejected regardless of whether it makes a local test pass.

## Two independent axes

Every module has both a **dependency layer** and a **runtime plane**. They are
independent:

- The dependency layer governs `#include` direction (layer 0 may be included by
  anyone; a higher layer may not be included by a lower one). See
  `docs/architecture/layers.md`. Enforced by `tools/check_module_layers.py`.
- The runtime plane governs data-flow ownership (who writes what state). Enforced
  by `tools/check_runtime_planes.py`.

A "shared" module (foundation or infrastructure) has no plane and may be
included by any plane. A plane module is bound to exactly one plane.

## The five planes

| # | Plane | Library | Role |
|---|---|---|---|
| 1 | Simulation (`sim`) | `libkith_sim.so` | Authoritative actor state, per-cell publish artifacts |
| 2 | World Stream Fabric (`fabric`) | `libkith_fabric.so` | Append-only cell streams, tiered products, subscription fanout |
| 3 | Gateway (`gateway`) | `libkith_gateway.so` | Sockets, session, shared cell cache, relevance composer, delivery |
| 4 | Coordination (`coord`) | `libkith_coord.so` | Cell ownership map, split/merge, inter-node bus |
| 5 | Control (`control`, optional) | `libkith_control.so` | HTTP/WS introspection, command, event/log stream |

The wire codec (`proto`), reactor, state, db, and foundation modules are
shared: they serve the planes and own no plane-specific data. The spatial
query library (`aoi`) is shared the same way — a pure query tool any plane
borrows. The transport (`net`) abstracts the client sockets the Gateway
plane owns, so it belongs to the Gateway plane (see Plane 3).

### Plane 1 — Simulation (`sim`)

**Owns:** actor authority by microcell; movement, combat, world rules; emission
of immutable per-cell publish artifacts each tick.

**Must not:** decide which socket receives a fact; write to connection queues;
learn about gateways.

**Invariants:**

- Simulation is the only writer of authoritative actor state.
- Publish products are immutable once published and keyed by
  `(zone, cell_x, cell_y, cell_z, lod, authority_epoch, publish_seq)`.
- The cell key is 3D-native from inception (ADR-0016). 2D simulation models
  project onto it by setting `cell_z = 0`. The `lod` field is a subcell
  level-of-detail refinement index, not a vertical axis.

### Plane 2 — World Stream Fabric (`fabric`)

**Owns:** append-only cell stream storage; tiered cell products (full / reduced
/ crowd); supersession of stale cell products; subscription-indexed fanout to
gateways.

**Must not:** mutate authoritative state; talk directly to sockets.

**Invariants:**

- The unit of publication is the cell stream, never the actor and never the
  connection.
- The fabric fans out only to subscribed gateways, never to all gateways.
- Supersession happens here, not in per-connection queues.

### Plane 3 — Gateway (`gateway`)

**Owns:** client sockets; auth/session lifecycle; the per-gateway **shared cell
cache** (one subscription per cell for all local players, refcounted); the
per-player **relevance composer** (bounded scored view set from the shared
cache); final socket flush of the already-bounded send set.

**Must not:** hold authoritative actor state; rescan raw zone state each tick;
recompute AOI from scratch per connection.

**Subsystems:**

- `gateway/session` — auth, session, actor bind, keepalive,
  bootstrap. Bootstrap runs on an isolated path separate from world replication
  bulk so it does not contend with live publish.
- `gateway/cache` — `cell_store` (shared, refcounted per-cell snapshots) and
  `subscription_union` (the union of all local players' cell subscriptions).
- `gateway/view` — `relevance_composer`: per-player scored stable view set,
  ring budgets by entity type, representation-tier selection, sticky
  continuity-preserving membership.
- `gateway/delivery` — replication framing and enqueue: serialization of the
  composed view set onto session connections. Delivery semantics are pluggable
  presets by contract (ADR-0021).

**Invariants:**

1. A gateway only receives world state for subscribed cells.
2. A gateway subscribes once per cell for all local players, not once per
   player.
3. A player's send set is always budgeted, even under no load (ADR-0005).
4. Full-fidelity replication is a privilege of relevance, not the default right
   of every visible actor.

### Plane 4 — Coordination (`coord`)

**Owns:** the cell ownership map; split/merge that moves **simulation authority
AND fabric publish authority together** as one coordinated ownership update
(ADR-0003); authority epoch management; gateway reroute metadata; the
inter-node bus.

**Must not:** become a second actor-by-actor relay engine; move payload bulk.

**Invariants:**

- Split/merge moves publish ownership together with simulation ownership, using
  an overlapping cell-stream handoff (the outgoing authority continues
  publishing the affected cell streams for a bounded overlap window after the
  new authority begins, both epoch-guarded). Cross-instance traffic is
  cell-stream-oriented, not actor-by-actor.
- No despawn or reset event is emitted to clients during a normal same-zone
  cell migration; gateways retain the same subscription identity across the
  handoff.

**Subsystems:**

- `coord/ownership` — cell table, authority epochs, split/merge.
- `coord/bus` — inter-node pub/sub fabric (zone subscribe/publish, partition,
  snapshot, input relay).

The coordination plane's ownership map, split/merge coordinator, and
inter-node bus are designed to be shardable and leader-electable for very large
clusters (100+ instances, 10k+ concurrent players). The initial implementation
may use a single backing store and a single coordinator; the contract (coord
owns the cell ownership map) does not change when the backing store is sharded
or the coordinator is leader-elected. That is a deployment concern, not an
architectural one.

### Plane 5 — Control (`control`, optional, zero-cost when off)

**Owns:** the HTTP/WebSocket introspection/control API for agentic testing and
operations (zone/actor/cluster queries, command injection, event/log
streaming).

**Must not:** share the critical path with world replication bulk; block the
reactor.

**Invariants:**

- The control plane is disabled by the compile flag
  `CONTROL_PLANE_ENABLED=OFF` and contributes zero code when off.
- When on, it uses non-blocking I/O on the reactor and never allocates per
  request.

## Pluggability within planes

Within a topology, individual planes have pluggable implementations via the
registry pattern. Every pluggable implementation must respect its plane's
invariants. An implementation that violates its plane's invariants (for
example, a "publish" implementation that pushes per-connection) is not a valid
implementation — the plane checker rejects it. This guardrail is what makes
pluggability safe. See ADR-0013.

| Plane | Default implementation | Alternatives |
|---|---|---|
| `sim` | `tile2d`, `free2d` | custom C model registered via `kith_sim_register_model(name, vtable)` from the composition root and linked into the owning binary; see `docs/guides/writing_c_sim_models.md`. 3D models use the 3D-native cell key (ADR-0016) with `cell_z ≠ 0`; the spatial query library ships sphere and box queries |
| `fabric` storage | in-memory append-only ring | none shipped |
| `gateway` relevance composer | bounded-budget scored rings | none shipped |
| `coord` split/merge | threshold-based split/merge | none shipped |
| `control` | embedded HTTP/WS | off (zero-cost) |
| `proto` types | framework-owned ids below `KITH_PROTO_TYPE_USER_BASE` (1000) | game types registered at startup at or above 1000 |
| `db` queries | generic query registry | game-specific queries registered at startup |
| `gateway` delivery | full resend of the budgeted view set | `tiered` preset shipped (ADR-0021); `delta` remains contract-only |

An alternatives entry names a shipped implementation unless it cites an ADR as
a design contract.

## Enforcement

The include-based rules in `tools/plane_rules.yaml` are the first guardrail:

- `sim` must not include `gateway/*` or `fabric/*` (sim must not know about
  sockets or delivery).
- `fabric` must not include `gateway/*` or `net/*` (fabric must not mutate
  authoritative state or talk directly to sockets).

The data-flow (writes) rules — `gateway/view` must not mutate authoritative
actor state; `coord` must not emit per-actor state frames; `control` must not
be on the reactor's world-replication critical path — are documented in
`tools/plane_rules.yaml` under `forbidden_writes` and enforced by the plane
checker's libclang AST walk over `src/` translation units.

## References

- ADR-0001 — five-plane world stream fabric architecture.
- ADR-0002 — publish products are immutable and cell-scoped.
- ADR-0003 — split/merge with overlapping cell-stream handoff.
- ADR-0005 — bounded relevance budgets and representation tiers are first-class.
- ADR-0013 — pluggability is within planes, not across invariant-breaking
  topologies.
- ADR-0016 — cell identity is 3D-native from inception.
- `docs/architecture/layers.md` — the orthogonal dependency-layer axis.
- `docs/architecture/topologies.md` — embedded versus distributed wiring.
- `AGENTS.md` §1.6 — plane invariants are non-negotiable.
