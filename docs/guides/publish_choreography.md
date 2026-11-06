# Publish Choreography

A replicating server makes three calls each tick to deliver fresh state to
connected subscribers. These calls feed the replication pipeline; a dirty-cell
flush bounds their cadence at scale. The per-session startup half of delivery
(binding the session and seeding its subscription window) is the separate
contract documented in `docs/guides/getting_started.md` ("From input to
delivery"); `Session.populate` performs it as one atomic step at the join. The
wire layout of what subscribers receive lives in `docs/guides/wire_protocol.md`.

## The pipeline

Replication flows through four stages, each consuming the previous stage's
output:

1. **The sim artifact store holds actor state.** `publish_artifact` writes one
   actor's state into the store keyed by cell (zone, cell coordinates, lod). An
   actor that already holds an artifact in the cell is updated in place; one
   that crossed a cell boundary is relocated atomically; a snapshot never
   observes the actor absent from the store mid-move.
1. **A cell product carries the cell into the fabric stream.**
   `publish_cell_product` renders the cell's artifacts from the store into a
   product header and appends it to the fabric's append-only cell stream. The
   fan-out marks the cell pending in every subscription tracking it.
1. **The gateway cache refresh consumes pending cells.** On its refresh interval
   the gateway drains the pending product headers from its fabric subscription
   and re-snapshots each drained cell's artifacts from the sim store, advancing
   the cache entry's product metadata (authority epoch, publish sequence,
   product level, actor count). Cells no publish marked pending never refresh.
1. **The composer and delivery shape what each subscriber receives.** On the
   view refresh interval the relevance composer builds a per-subscriber scored
   view set from the shared cache. It scores candidates from cells near the
   subscriber by distance, classifies and tiers them, and bounds the set by
   `view_max_subjects`. Delivery encodes the set as replication frames on the
   session's connection.

The gateway refresh precedes tick-handler dispatch. A publish completed by that
handler is therefore eligible at the next gateway pass, giving the usual
pipeline one tick of coalescing latency. This is an ordering rule, not a
wall-clock delivery deadline: a queued or running worker may finish after that
next pass. The cache refresh interval, view refresh interval, per-tier delivery
intervals, connection output queue and network add delay. For example, at a 50
ms tick with both refresh intervals set to 50 ms, a publish completed between
consecutive gateway passes can enter the next pass's view; completion after that
pass waits for a later eligible pass. Multiple artifact updates before
consumption coalesce to the latest store state. A product bump marks work
pending rather than freezing an exact artifact-state version for that pass.

`publish_artifact` alone changes the store and delivers nothing. A new
subscriber receives the cell's current truth once, at the subscribe-time
reconcile when its window expands; after that, only a product bump moves the
cell. A server that publishes artifacts but never bumps products runs a world
that reads as frozen: the sim advances, handlers run, and every subscriber's
view stays at its subscribe-time snapshot, with no error and no metric to say
why.

## The cell-scoped broadcast

Free-form cell-local traffic skips this pipeline entirely.
`Server.broadcast_cell` takes a cell key, a game type id, and the game's own
bytes: nothing is written to the artifact store, no product is bumped, and the
composer builds nothing. The request queue is internally synchronized, so any
handler submits the way it publishes; on the next tick the gateway's drain fans
one encoded frame to every session whose subscription window covers the cell,
ahead of that pass's composed replication. Delivery is best-effort: a full queue
refuses the submit, and recipients the drain cannot reach are counted
(`kith_gateway_broadcast_refused_total`, `kith_gateway_broadcast_dropped_total`;
their operator reading lives in `docs/guides/operations.md`). Pair the broadcast
with a product bump when the social event should also refresh presence: the
spatial example's `on_chat` submits the chat event on the sender's cell and
marks the cell dirty in the same handler.

## The per-tick choreography

Each tick, game code drives three calls in order:

1. **Step the sim.** `SimModel.step` advances every actor by the tick's
   interval. The model's pending-input map persists an input until it is
   replaced, so an actor keeps integrating its last input across ticks; motion
   is the tick's product, not the input handler's.
1. **Publish each actor's state as an artifact.** `publish_artifact` per changed
   actor places the fresh state where the cache re-snapshot will read it.
1. **Bump one cell product per dirty cell.** `publish_cell_product` with a
   monotonic per-cell authority epoch marks the cells whose contents changed,
   and the next refresh delivers them.

The epoch is the bump's ordering contract: it must not regress below the cell's
current epoch. A stale epoch raises `KithStateError` (`KITH_EPERM`), and that
failure is safe to drop: the higher-epoch publish already marked the cell
pending, and the next refresh still reads the store's latest state. An equal
epoch is accepted, so a re-publish of unchanged state needs no new epoch.

## The dirty-cell flush

Publishing a product per mutation wastes bumps under any real input rate: the
per-mutation cadence coalesces to a per-tick cadence through a dirty-cell set.
Mutators (wire handlers, control routes, anything that changes a cell's
contents) add the cell to the set instead of publishing. The server's per-tick
handler (`register_tick_handler`) drains the set with one `publish_cell_product`
per dirty cell. A cell dirtied again while the flush runs is bumped on the next
tick.

The flush's bookkeeping takes a narrow lock: the dirty-set snapshot and the
epoch read-modify-write hold the game's lock, and the fabric publish runs
outside it. The set and the epoch table are game state, and game state shared
across the worker pool is the game's to synchronize: the tick handler, message
handlers, and control routes all dispatch onto one shared worker pool and run
concurrently with more than one worker (`docs/guides/operations.md`, "Handler
hygiene").

Exceptions raised inside a tick handler are caught at the dispatch trampoline
and counted (`kith_python_handler_exceptions_total`; the first exception per
registration prints its traceback to stderr, see `docs/guides/operations.md`,
"Handler hygiene"). Log publish failures inside the handler so they can be
diagnosed during the flush.

## Shipped references

- `examples/spatial/handlers.py`: the full reference: login binds and seeds
  windows, per-input artifact publishes stay on the handlers (the freshest state
  an eligible gateway refresh consumes), a per-tick flush bumps the dirty cells,
  and cell crossings diff the subscriber's window.
- `examples/free_movement/server.py`: the minimal flush: one cell, the tick
  handler owns step, artifact publish, and bump.
- `examples/embedded/server.py`: the flush composed with the replay recorder's
  tick boundary in the composition root.

## Configuration the cadence interacts with

The gateway's `cache_refresh_interval_ms` gates stage 3;
`view_refresh_interval_ms` and the per-tier intervals gate stage 4;
`view_max_subjects` bounds the per-subscriber set. The composition root forwards
all three through its own creation parameters. Zero keeps the shipped
derivation: the gateway defaults, and the tick interval for the two cadences.
The standalone gateway accepts them directly; the owning headers document both
surfaces. The publish cadence above is the game-side half of the same pipeline.
