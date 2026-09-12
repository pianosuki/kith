# Publish Choreography

This page is the per-tick publish contract for a server that replicates:
the three calls game code drives every tick so a connected subscriber
receives fresh state, the pipeline those calls feed, and the flush pattern
that keeps the cadence at scale. The per-session startup half of delivery —
binding the session and seeding its subscription window — is the separate
contract documented in `docs/guides/getting_started.md` ("From input to
delivery"); `Session.populate` performs it as one atomic step at the
join. The wire layout of what subscribers receive lives in
`docs/guides/wire_protocol.md`.

## The pipeline

Replication flows through four stages, each consuming the previous stage's
output:

1. **The sim artifact store holds the content truth.** `publish_artifact`
   writes one actor's state into the store keyed by cell (zone, cell
   coordinates, lod). An actor that already holds an artifact in the cell
   is updated in place; one that crossed a cell boundary is relocated
   atomically — a snapshot never observes the actor absent from the store
   mid-move.
2. **A cell product carries the cell into the fabric stream.**
   `publish_cell_product` renders the cell's artifacts from the store into
   a product header and appends it to the fabric's append-only cell
   stream. The fan-out marks the cell pending in every subscription
   tracking it.
3. **The gateway cache refresh consumes pending cells.** On its refresh
   interval the gateway drains the pending product headers from its fabric
   subscription and re-snapshots each drained cell's artifacts from the
   sim store, advancing the cache entry's product metadata (authority
   epoch, publish sequence, product level, actor count). Cells no publish
   marked pending never refresh.
4. **The composer and delivery shape what each subscriber receives.** On
   the view refresh interval the relevance composer builds a per-subscriber
   scored view set from the shared cache — candidates from the cells near
   the subscriber, scored by distance, classified and tiered, bounded by
   `view_max_subjects` — and delivery encodes the set as replication
   frames on the session's connection.

The pipeline is one tick deep: publishes a tick handler makes are observed
by the next tick's gateway refresh. A subscriber therefore sees last
tick's truth, which is the intended coalescing tradeoff.

`publish_artifact` alone changes the store and delivers nothing. A new
subscriber receives the cell's current truth once, at the subscribe-time
reconcile when its window expands; after that, only a product bump moves
the cell. A server that publishes artifacts but never bumps products runs
a world that reads as frozen: the sim advances, handlers run, and every
subscriber's view stays at its subscribe-time snapshot, with no error and
no metric to say why.

## The cell-scoped broadcast

Free-form cell-local traffic skips this pipeline entirely.
`Server.broadcast_cell` takes a cell key, a game type id, and the
game's own bytes: nothing is written to the artifact store, no product
is bumped, and the composer builds nothing. The request queue is
internally synchronized, so any handler submits the way it publishes;
on the next tick the gateway's drain fans one encoded frame to every
session whose subscription window covers the cell, ahead of that pass's
composed replication. Delivery is best-effort — a full queue refuses
the submit, and recipients the drain cannot reach are counted
(`kith_gateway_broadcast_refused_total`,
`kith_gateway_broadcast_dropped_total`; their operator reading lives in
`docs/guides/operations.md`). Pair the broadcast with a product bump
when the social event should also refresh presence: the spatial
example's `on_chat` submits the chat event on the sender's cell and
marks the cell dirty in the same handler.

## The per-tick choreography

Each tick, game code drives three calls in order:

1. **Step the sim.** `SimModel.step` advances every actor by the tick's
   interval. The model's pending-input map persists an input until it is
   replaced, so an actor keeps integrating its last input across ticks —
   motion is the tick's product, not the input handler's.
2. **Publish each actor's state as an artifact.** `publish_artifact` per
   changed actor places the fresh state where the cache re-snapshot will
   read it.
3. **Bump one cell product per dirty cell.** `publish_cell_product` with a
   monotonic per-cell authority epoch marks the cells whose contents
   changed, and the next refresh delivers them.

The epoch is the bump's ordering contract: it must not regress below the
cell's current epoch. A stale epoch raises `KithStateError`
(`KITH_EPERM`), and that failure is safe to drop: the higher-epoch publish
already marked the cell pending, and the next refresh still reads the
store's latest state. An equal epoch is accepted, so a re-publish of
unchanged state needs no new epoch.

## The dirty-cell flush

Publishing a product per mutation wastes bumps under any real input rate:
the per-mutation cadence coalesces to a per-tick cadence through a
dirty-cell set. Mutators — wire handlers, control routes, anything that
changes a cell's contents — add the cell to the set instead of publishing.
The server's per-tick handler (`register_tick_handler`) drains the set
with one `publish_cell_product` per dirty cell. A cell dirtied again while
the flush runs is bumped on the next tick.

The flush's bookkeeping takes a narrow lock: the dirty-set snapshot and
the epoch read-modify-write hold the game's lock, and the fabric publish
runs outside it. The set and the epoch table are game state, and game
state shared across the worker pool is the game's to synchronize — the
tick handler, message handlers, and control routes all dispatch onto one
shared worker pool and run concurrently with more than one worker
(`docs/guides/operations.md`, "Handler hygiene").

Exceptions raised inside a tick handler are caught at the dispatch
trampoline and counted (`kith_python_handler_exceptions_total`; the
first exception per registration prints its traceback to stderr — see
`docs/guides/operations.md`, "Handler hygiene"). The flush is the wrong
place to learn that a publish started failing; log failures inside the
handler.

## Shipped references

- `examples/spatial/handlers.py` — the full reference: login binds and
  seeds windows, per-input artifact publishes stay on the handlers (the
  freshest state the next tick delivers), a per-tick flush bumps the
  dirty cells, and cell crossings diff the subscriber's window.
- `examples/free_movement/server.py` — the minimal flush: one cell, the
  tick handler owns step, artifact publish, and bump.
- `examples/embedded/server.py` — the flush composed with the replay
  recorder's tick boundary in the composition root.

## Configuration the cadence interacts with

The gateway's `cache_refresh_interval_ms` gates stage 3;
`view_refresh_interval_ms` and the per-tier intervals gate stage 4;
`view_max_subjects` bounds the per-subscriber set. The composition root
forwards all three through its own creation parameters — zero keeps the
shipped derivation: the gateway defaults, and the tick interval for the
two cadences — while the standalone gateway accepts them directly; the
owning headers document both surfaces. The publish cadence above is the
game-side half of the same pipeline.
