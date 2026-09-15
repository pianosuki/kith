# Getting Started

This guide walks five of the six shipped example servers and the spatial
game library they build on, from the thinnest end-to-end slice through a
full spatial reference game to the two-instance cluster story, and maps
multiplayer genres to the capabilities each genre wires. The sixth
example, the Postgres-backed wiring, is covered under Persistence. Every
example is a shipped module under `examples/`, not throwaway scaffolding:
each earns its directory by proving a distinct capability. The one
exception ships inside the installed package itself: the visual example
(stop 7) rides the wheel so a plain install boots a living world with no
written code.

## Build

The framework is a C core with a Python facade that loads the built shared
libraries through `ctypes`. Build the C libraries once, then run the
examples as Python modules from the repository root.

```
cmake --preset debug
cmake --build build/debug
```

The Python package lives under `python/kith`. Create a virtual environment,
install the package editable so the `kith` import resolves and the
`examples.*` modules import alongside it, and activate the environment so
`python` resolves the installed package:

```
uv venv
source .venv/bin/activate
uv pip install -e .
```

The loader discovers the shared libraries through `$KITH_LIB`,
`$KITH_BUILD_DIR`, or the `build/<preset>` directory under the repository
root, in that order. The default `debug` preset above writes to
`build/debug`, so no environment variable is required for a from-source
run.

## The skeleton

Five servers and the spatial game library, on a complexity gradient.
Read them in order; each adds one capability the previous did not have.

### 1. `minimal` — the bare composition root

The thinnest server: `Server(...).serve()` with no game-owned wiring. No
wire types, no handlers, no simulation model, no zone, no store, no
control routes. The composition root wires every plane with its defaults,
the gateway listener comes up on an OS-assigned ephemeral port, and a
peer that connects completes the TCP handshake but the gateway decodes
no frames.

The facade has two lifecycle entries. `serve()` starts the run loop on a
worker thread and parks the calling thread until `SIGINT`/`SIGTERM`
request a graceful shutdown — what a standalone server process wants.
`run()` drives the loop on the calling thread and blocks until
`shutdown()` completes — what an embedder or a test that drives shutdown
itself wants. The standalone server examples are processes and call
`serve()`; the distributed launcher drives `run()` on worker threads
itself.

```
python -m examples.minimal.server
```

This is the baseline a game boot is compared against: if a game's server
fails to start, the minimal server isolates whether the failure is in the
framework wiring or in the game's own registration surface.

### 2. `free_movement` — the thinnest spatial server

Adds one built-in sim model (`free2d`), one wire input (`actor_input`),
one replication type (`actor_state`), and four control-plane commands
(`spawn`/`move`/`teleport`/`query_state`). No persistence, no chat, no
login.

```
python -m examples.free_movement.server
```

The wire handler translates a decoded `actor_input` frame into a sim
`apply_input` call, and a per-tick handler runs the
[publish choreography](publish_choreography.md):
it steps the model, publishes updated positions as cell artifacts, and
bumps the changed cell's product so the replication stream carries fresh
state. The example stops there: it ships no login, so a wire session still
needs the bind and window-seed steps of the delivery contract — "From
input to delivery" below states the full contract, and the spatial
example's login handler is the working reference. The control plane is
the harness surface — the agentic
scenarios and integration tests drive actors through HTTP, not through the
wire protocol. Positions are Q16.16 fixed-point (the sim's native
representation); a client that renders in world units divides by `2**16`.
Move inputs are a separate scale: the `SimInput` components are normalized
to [-32767, 32767], not Q16.16.

### 3. `lobby` — the non-spatial slice

Presence, rooms, and chat on Gateway + Control + DB only. No simulation
model, no fabric cell product, no AOI subscription. This proves the planes
are a pickable toolkit, not a monolithic spatial engine: a lobby,
turn-based game, or any non-spatial multiplayer game uses the planes it
needs and leaves the spatial planes out.

```
python -m examples.lobby.server
```

The single S2C type (`presence`) carries join/leave/chat/status
notifications in its payload, not as separate wire types — the same
one-replication-type principle as the spatial game's `actor_state`. The
`GameStateStore` interface (from `examples/_common/store.py`) persists
member display names; the in-memory default ships with the examples (not in
the `kith` package) and a Postgres backing is the opt-in via `register_query`.

### 4. `spatial` — the spatial game library

The game-owned surface a spatial game ships: a wire catalog, login /
actor_input / chat handlers, and a generic persistence interface. The
catalog is the irreducible surface a spatial game exchanges on the wire: a
principal exchange to bind a session to an actor, the hot-path movement
input that maps to `kith_sim_input_t`, a generic chat message, and the
single replication type. The `tile_rpg/` directory adds the tile2d
extension (physics wiring and a TMX behavior-grid converter) on top of
this library.

`spatial/` is a library, not a server: it has no entry point. The modules
are the wiring a topology composes onto — `examples/spatial/` holds the
catalog (`messages.py`), the handlers (`handlers.py`), and the persistence
interface (`db.py`); `examples/tile_rpg/` holds the tile2d physics
(`physics.py`, `zone.py`). The `embedded/` example is its first wiring.

### 5. `embedded` — the first wiring, on one process

The spatial game library and tile2d extension wired onto the embedded
topology: the coordinator owns every cell, the fabric uses in-memory
storage, and no coordination bus, no Postgres pool, and no second instance
are configured. This is the "you do not need a cluster" story. A game that
ships to a cluster wires the same game code onto the distributed topology
and supplies the shared coordination bus and Postgres pool as explicit
handles; the game code is unchanged.

```
python -m examples.embedded.server
```

This is the runnable entry point for the reference game. The wiring proves
topology is a configuration knob of the composition root, not a property of
the game: the same `spatial` + `tile_rpg` modules run on the embedded
topology here and on a distributed topology unchanged — the topology the
facade is constructed with is the only wiring difference.

The delivery preset is a run-time choice, not a wiring constant. Booting
the same wiring under the relevance-tiered preset — the preset the
certified scaling gates measure — is one flag:

```
python -m examples.embedded.server --delivery-strategy tiered
```

The full-versus-tiered trade is described in `examples/embedded/README.md`
under Delivery presets.

### 6. `distributed` — the two-instance coordination story

The embedded wiring twice over plus the coordination layer: two
`EmbeddedServer` instances on one shared `CoordBus`, running the same
spatial and tile2d modules unchanged on both. A split contract published
on the bus moves a cell's authority from instance A to instance B, the
receiving coord applies the rebalance, and both authority tables converge
at the new epoch; a merge contract moves the authority back. This is the
bus-carried ownership contract the distributed topology rests on — a
cell's authority moves as a rebalance on the bus, not an actor-by-actor
state transfer (ADR-0003) — exercised end-to-end on real C handles in
one process.

```
python -m examples.distributed.server
```

The launcher prints both gateways' and both control planes' ports plus
one line showing the split/merge authority transition, then blocks until
interrupted. The launcher forwards the same delivery flags as the embedded
server (`--delivery-strategy`, `--tiered-max-gap-ms`) to both instances,
so a two-instance cluster runs one delivery preset cluster-wide. The bus
and the two coords live alongside the servers, driven by the launcher, so
the ownership handoff is observable end-to-end on one machine — the
contract the cluster story in the `embedded` stop points at, exercised
directly. The certified scaling claim is one server process per instance
with per-instance loopback coordination buses; cross-machine coordination
transport is not part of the certified claim; the `KITH_COORD_BUS_TRANSPORT`
enum's additive extension point is the seam for it.

### 7. visual — the out-of-box example

The shipped package carries one example the guide cannot run for you from a
checkout alone: `kith.examples.visual`, an out-of-box playable world that
installs with the framework and boots with no written code. One embedded
server, ambient actors, cell-scoped chat, a metrics route, and — with the
opt-in visual extra — a graphical client whose network core is the same
harness engine the scaling gates drive. Install the extra and play:

```
pip install "kith-fw[visual]"
kith-visual play crowd-in
```

The launcher's other commands run the scenarios headless (`kith-visual run
soak --minutes 10`) or open just the window against a running server
(`kith-visual client --port <gw>`). The scenario catalog, the controls, and
the HUD's measurement core are documented in the example's own
[README](../../python/kith/examples/visual/README.md); the guide's stops above are
the surfaces that example composes — the composition root, the publish
choreography, the control routes, and the harness client engine.

### Vocabulary

Three word families recur through the examples and the API docs. Spatial
layering: a **zone** is the partition key the coordinator splits and merges, a
**cell** is the stream unit the fabric publishes and the gateway caches, and a
**shard** is the store stripe a sim model keeps its actors in. Time layering: a
**tick** is one clock advance of the server loop, and a **step** is one model
advance of the simulation inside it. Delivery stages: frames **publish** into a
plane's stream, the gateway **delivers** a composed view per session, and a
**dispatch** is the worker-pool handoff of one decoded frame to its handler.

## Planes → genres

The framework's runtime is five planes (see `docs/architecture/planes.md`).
A game uses the subset its genre needs; the unused planes are simply not
wired.

| Capability | Library | Owns |
|---|---|---|
| Gateway | `libkith_gateway.so` | Sockets, session table, shared cell cache, relevance composer, delivery |
| Control | `libkith_control.so` | HTTP introspection and command surface (zero-cost when off) |
| Coord | `libkith_coord.so` | Cell ownership map, split/merge, inter-node bus |
| DB (shared infrastructure) | `libkith_db.so` | Named-query registry and async pool; serves the planes, owns no plane-specific data |
| Sim | `libkith_sim.so` | Authoritative actor state, per-cell publish artifacts |
| Fabric | `libkith_fabric.so` | Append-only cell streams, tiered products, subscription fanout |
| AOI (shared infrastructure) | `libkith_aoi.so` | Spatial-index queries backing the gateway's view composition |

Spatial capabilities (Sim, Fabric, and the AOI query library) carry the
cell-stream model that breaks the per-connection N² ceiling; a non-spatial
game has no cells and so does not wire them.

| Genre | Gateway | Control | Coord | DB | Sim | Fabric | AOI |
|---|:-:|:-:|:-:|:-:|:-:|:-:|:-:|
| Lobby / turn-based | x | x | x | x | | | |
| Real-time spatial (single process) | x | x | | x | x | x | x |
| Real-time spatial (cluster) | x | x | x | x | x | x | x |

The columns are the capabilities a genre wires, not the five planes
themselves. DB and AOI are shared infrastructure: both serve the planes
and own no plane-specific data (`docs/architecture/planes.md`) — the DB
pool serves every wired plane, and AOI is the spatial-query library the
gateway composes views from.

A lobby or turn-based game uses Gateway + Control + Coord + DB; Sim, Fabric,
and AOI are spatial only. On the embedded topology the coordination plane is
a no-op stub (one process, one cell owner, no inter-node bus), so a
single-process lobby wires Gateway + Control + DB and the facade still
constructs the coordinator as a stub. On the distributed topology the
coordination bus carries cross-instance room and presence fanout for the same
non-spatial game, so Coord is part of the genre's plane set even though no
spatial split/merge runs.

A real-time spatial game adds Sim + Fabric + AOI on top. The single-process
variant (the `embedded` topology) leaves Coord as a no-op stub; the cluster
variant (the `distributed` topology) wires Coord so split/merge moves
simulation and publish authority together across instances.

## Anatomy of a server

Every example builds the same shape: construct the `Server` facade, register
the wire catalog on the borrowed proto, register the sim model and zone
(spatial games only), register the wire handlers and the control routes, then
call `run()`. The free-movement server is the smallest example that shows
every part:

```python
from kith import Actor, ArtifactKey, Server, SimInput, SimModelConfig

ACTOR_INPUT_TYPE = 1000
ACTOR_STATE_TYPE = 1001

server = Server(
    topology="embedded",
    listen_port=0,
    tick_hz=20,
    handler_table_size=4096,
    replication_type_id=ACTOR_STATE_TYPE,
)

# Register wire types on the borrowed proto before run so the gateway
# decoder accepts frames of these types during the run loop.
server.register_proto_type("actor_input", ACTOR_INPUT_TYPE)
server.register_proto_type("actor_state", ACTOR_STATE_TYPE)

# Reserve a zone and instantiate the built-in free2d model.
zone_id = server.register_zone("world")
model = server.register_sim_model("free2d", SimModelConfig())

# The wire handler is invoked when a decoded actor_input frame arrives.
server.register_message_handler(ACTOR_INPUT_TYPE, on_actor_input)

# The control plane is the harness surface.
server.register_control_route("POST", "/spawn", route_spawn)
server.register_control_route("POST", "/move", route_move)
server.register_control_route("POST", "/teleport", route_teleport)
server.register_control_route("GET", "/query_state", route_query_state)

# run() blocks the calling thread until shutdown() completes; a standalone
# process calls serve() instead and parks the thread on SIGINT/SIGTERM.
server.run()
```

Key invariants this shape teaches:

- **Register before run.** Wire types register on the borrowed proto before
  `run()` so the gateway decoder accepts frames of every game type during the
  run loop. The `replication_type_id` passed to `Server` carries the id the
  gateway encodes the replication stream as.
- **Borrowed handles, not owning wrappers.** The facade exposes the planes the
  composition root owns as borrowed handles; `register_message_handler`,
  `register_sim_model`, `register_zone`, `register_query`, and
  `register_control_route` call the C plane functions on those handles
  directly. The caller never destroys a borrowed handle — ownership stays with
  the `Server`, released on `close()`.
- **The wire and the control plane are two distinct surfaces.** The wire
  carries player input and replication; the control plane carries the harness
  command surface (`spawn`/`move`/`teleport`/`query_state`). The agentic
  scenarios drive the server through HTTP, not the wire protocol.
- **The composition root does not step the sim.** Stepping is game code's
  concern per the server tick contract; a wire handler applies an input and
  steps the model itself (`apply_movement` in `spatial/handlers.py`), it does
  not rely on the root to advance the sim.
- **Sessions are game-owned, and death is an event.** The gateway fires the
  destroyed-session callback when it destroys a session — a client
  disconnect, a full connection, or the composition root's own destroy —
  handing the game the session's identity (`SessionInfo`: session id,
  principal, bound actor). A facade game registers it with
  `Server.on_session_destroyed(fn)` and reclaims its per-session state
  there; the callback runs on the worker pool like every other callback,
  so game state it touches is synchronized by the game. A handler that
  must act on a session later pins it (`Session.pin()`): the pinned
  reference outlives the disconnect, `send` follows the discrete-send
  contract, and `close()` releases the pin. The live-session count reads
  from any thread (`Server.session_count`). A C composition root that
  creates its own sessions keeps its roster with the same callback
  (`kith_gateway_register_session_destroyed_handler`) and snapshots live
  sessions on the reactor thread (`kith_gateway_session_snapshot`).
  `Session.close` is reactor-thread-only.

## From input to delivery

Decoding a C2S frame and stepping the sim is the input half of the wire.
The delivery half has its own contract, and a connected session receives
replication only when game code completes all three of its steps:

- **Bind the session to an actor** (`session.bind_actor(actor_id)`) — the
  composer needs the subscriber's identity.
- **Seed the session's subscription window** (`window_clear()` plus one
  `window_add(CellKey)` per neighborhood cell) — the window decides which
  cells the composer may deliver to the session.
- **Carry refreshed cells into the fabric stream** (a
  `publish_cell_product` bump per changed cell) — artifact publishes
  refresh the gateway's shared cache; the fabric product is what the
  cache refresh consumes.

Missing any of the three fails silently: frames decode, handlers run, the
sim steps — and no S2C frame arrives, with no error and no metric. The
first two steps have a one-call form:
`session.populate(actor_id, cells)` binds the actor and seeds the window
in one atomic step — the session is either unpopulated or populated, with
no bound-but-windowless intermediate for a game-side sweep or dashboard
to misread. A capacity failure from populate raises but is not fatal: the
bind stays, landed cells stay, and the tick pass retries the failed adds
(the window-capacity section below); the per-cell calls stay the tool
when a step needs per-cell precision, and an empty cell list is a legal
bind-only populate. The shipped `free_movement` example completes the
product-bump half — its per-tick flush bumps each changed cell — but
ships no login, so its wire clients still receive nothing until the game
completes the bind and window-seed steps; the spatial game library's
login handler (`examples/spatial/handlers.py`) is the working reference —
one populate call joins the session, and the dirty-cell flush bumps each
changed cell on the next tick.

### Window capacity

The cells a server tracks live in two fixed-size tables created with the
gateway: the shared cache — striped (16 stripes in the current
implementation), sized by `cache_bucket_count`, 4096 cells by default at
256 per stripe — and the gateway's fabric subscription, a fixed interest
set of `KITH_FABRIC_DEFAULT_BUCKET_COUNT` (4096) cells with no
configuration knob. Every session's window draws from the same tables, so
the tracked-cell total is the union of all sessions' windows, not the
per-session count. A `window_add` needing a new slot fails with
`KITH_ENOMEM` when the cell's cache stripe has no free slot or the fabric
interest set is full — and the failed add is retained: the gateway's tick
pass retries it (bounded work per pass) until a slot frees, so a one-shot
seed heals without a caller-side retry loop. The add still reports the
honest `KITH_ENOMEM`; `window_remove` and `window_clear` cancel a
retained add, so a rescinded intent never lands. Retention is bounded
per session and deduplicated, so a handler that re-tries its own failed
adds cannot fill the ring with duplicates. Size cell geometry and
neighborhood radius so the peak tracked-cell union fits the tables — the
union-sizing rule holds for any static population that fits.

Recurring `KITH_ENOMEM` under sustained cell-crossing churn is not by
itself evidence that the tables are full: the failure persists with both
totals far below their caps, because the cache fills per stripe and the
status gauges report totals, not per-stripe fill. Read
`Gateway.cache_stats()` and the subscribed-cell count before concluding
saturation. When the gauges do reach their caps, widen
`cache_bucket_count`; the fabric interest set has no knob — cell geometry
is the lever there. The heal itself is observable: 
`kith_gateway_window_add_failures_total` counts capacity-failed adds,
`kith_gateway_window_retry_adds_total` counts the tick pass's landings,
and the `kith_gateway_window_retries_pending` and
`kith_gateway_sessions_without_cells` gauges report the retry queue's
depth and the count of sessions bound to an actor while tracking no
cells. Failures climbing with landings landing and the queue draining is
a flood that is healing; failures with no landings and a pegged queue is
capacity that never frees — widen the tables or shrink the geometry.
Saturation, when it happens, degrades delivery
through exactly the cells it leaves untracked: a session whose bound
actor's cell is untracked composes no view set, recorded as
`kith_gateway_view_locate_failures_total` in the metrics surface.

### View capacity and freshness

Each subscriber's composed view set is bounded by `view_max_subjects`
(512 by default): the set holds the subscribing subject plus the closest
candidates up to the budget, candidates past the budget drop from the
delivered individual set, and a candidate count deep enough to cross the
crowd regime's entry point replaces the overflow with the crowd
aggregate. The `kith_gateway_view_candidate_high_watermark` and
`kith_gateway_view_selected_high_watermark` gauges are the
approaching-the-cap evidence; a dense deployment that needs more than the
budget raises the capacity — `Server(view_max_subjects=...)` on the
embedded and distributed shapes, the same-named argument on a standalone
`Gateway`. Compose and deliver run at most once per
`view_refresh_interval_ms`, and the cell cache refreshes at most once per
`cache_refresh_interval_ms`; the server derives both from its tick
interval by default — a refresh interval wider than the tick leaves
inputs published inside the gap undelivered until the next refresh — and
an explicit value passes through unclamped.

## Default network surface

The gateway binds the wildcard address (all interfaces) on its listen port;
an explicit `listen_host` on the server pins that bind to the given address
(a numeric IPv4/IPv6 address or a resolvable host name). The control plane
binds `127.0.0.1` and serves unauthenticated HTTP — it is reachable only from
the local host unless a deployer opts out by binding it to another address
explicitly. Keep the control plane off untrusted interfaces. The wildcard
wire bind exposes the server to every network interface it can reach;
`docs/architecture/threat_model.md` describes the trust boundary that
default creates and what the decoder chain does and does not warrant about
arriving traffic.

## Wire type ids

Game-owned wire types live at or above `KITH_PROTO_TYPE_USER_BASE` (1000),
the boundary the framework reserves for game-owned types. Both peers register
the same name at the same id before exchanging frames; there is no central
enum. The default `handler_table_size` (256) covers framework types but not
the user base, so a game that registers types at 1000+ passes
`handler_table_size=4096` (or larger) so the gateway's direct-indexed table
exceeds the largest user type id.

The replication stream is one type, not four. The gateway's
`replication_type_id` selects a single S2C type; the relevance composer builds
one bounded view set per subscriber per tick, tiers it (full / reduced / crowd)
within the payload, and delivers it as frames of that one type. An actor
leaving the view set is simply absent from the next delivery — there is no
explicit despawn message. Defining separate `state` / `state_snapshot` /
`state_crowd` / `despawn` wire types re-introduces the per-connection-push
model the framework was built to break. The full wire format — the frame
header, the replication record layout, and the delivery cadence — is
documented in `docs/guides/wire_protocol.md`.

### Discrete S2C frames

What a session receives is its composed view set; targeting is a window
question — a record published into a cell reaches the sessions whose windows
track that cell. A game that needs a one-off frame to one client (a login
ack carrying an error code, a private event) sends it directly: the session
a handler was handed carries `send(msg_type, payload)`, and the facade and
gateway wrappers mirror it (`Server.send` / `Gateway.send`). The send
encodes the frame through the gateway's proto and enqueues it on the
session's connection; the reactor flushes the queue on the tick's arm
pass. The call is legal while the session is alive by reference — the
dispatch reference holds for the handler that received the view, so
replying inside a message handler is the intended shape — and it reports
its failures at the call site: backpressure raises `KithNetworkError` (the
frame was not queued), a closed connection raises `KithStateError`, an
oversize payload raises `KithProtocolError`. The send appends no
correlation trailer (request/reply correlation is a payload convention)
and does not consult the type registry — the receiving client's decoder
does — so both peers register the type on their protos.

Two other routes remain for a game that cannot act from a handler: serve
the exchange over the control plane (the shipped examples' login pattern
— the server assigns the actor id and returns it in the route response),
or make the actor id predictable so the first replication record naming
it serves as the ack. The C support surface the send calls,
`kith_gateway_deliver_frame` (`include/kith/gateway/gateway.h`), stays
available to C servers: callable from reactor-thread code (a tick
callback, a C handler, or a custom delivery strategy) or from any context
holding a live session or connection reference.

The thread boundary is the handler-kind boundary: the facade's
`register_message_handler` registers the pool-dispatched
(`KITH_GATEWAY_HANDLER_PYTHON`) kind — the handler runs on a worker under
the dispatch reference, which is exactly the context `send` requires. An
unflagged C handler runs inline on the reactor thread and may call the C
support surface directly. The borrowed gateway handle that the C route
needs is reachable through the private `Server` accessors the shipped
spatial example uses for its composition-root wiring (`_borrowed_gateway`,
used there for the native movement handler).

Zone-wide fanout has a shipped public route, and the recipient set is
the fabric's, not a game-built roster: `publish_artifact` and
`publish_cell_product` (`python/kith`) append to the fabric's cell
stream, fanning out to every subscription tracking the cell —
`publish_cell_product` names the chat-event re-broadcast as its
canonical trigger. Free-form cell-local traffic has its own ride:
`Server.broadcast_cell` submits a chat line, an announcement, or any
game-composed payload on the gateway's broadcast queue, and the next
tick pass delivers one frame to every session whose subscription
window covers the cell. The recipient set stays the framework's
(window membership, not a game-built roster), latency is one tick, and
delivery is best-effort — a saturated queue refuses the submit, and
recipients the drain cannot reach are counted as drops. The
replication record itself remains the fixed-layout kinematic artifact
(actor id, position, velocity, input tick, update sequence — no
free-form payload blob).

## Persistence

Game-state persistence is optional and dependency-injected. The generic
`GameStateStore` interface (`examples/_common/store.py`) is an async
byte-key / byte-value surface; the in-memory default ships with the examples
(not in the `kith` package) so a reference game boots with no external
dependency. A game that wants
durable persistence registers a Postgres-backed implementation alongside it;
`register_query` registers named parameterized SQL on the borrowed db pool and
raises `KithStateError` when no pool is configured. The two backings are
interchangeable behind the interface, mirroring the framework's own
config-struct pattern of one contract with switchable backings.

On the embedded topology the handler set's in-memory actor table is the whole
game state — one process has no restart to survive. The same handler set on
the distributed topology is backed by the generic store with a Postgres pool.

Relational persistence — real rows, multi-statement transactions, and the
commit-before-acknowledge ordering — is the `kith.db` module. `kith.Database`
owns the pool lifecycle (its own reactor thread and the recurring keepalive a
pool-only reactor needs, so a game wires no persistence composition of its
own), `query` runs raw SQL and returns row snapshots, and
`async with database.transaction()` commits before the block exits: code
after the block, including a facade send, observes the committed state. An
exception inside the block rolls back and re-raises. The transaction context
drives the db module's sessions — statements ride one pinned connection, one
at a time. Facade handlers are synchronous; drive the surface from the
game's own event loop the way the postgres example does, and pair an
acknowledged send with the send's live-reference contract.

The byte-key path stays pool-owned, not facade-owned. Game code that
persists opaque state blobs creates its db pool out of band: the pool borrows
a standalone reactor (with a keepalive timer — a pool-only reactor idles out
once the libpq sockets deregister, which would strand every reply callback),
registers named queries on the pool handle, and issues them through
`kith_db_exec` from its own store wrapper; the reply callback fires on the
pool's reactor thread and resolves the caller's future via
`call_soon_threadsafe`. `examples/postgres/server.py` wires this end to end
(`register_queries_on_db` and `PostgresStore` in `examples/spatial/db.py`
are the reference wrapper). Two transport caveats: parameter values are
NUL-terminated text (binary blobs hex-encode for transport), and the
server's own borrowed db handle stays NULL under this pattern — the
facade's `register_query` targets a server-attached pool.

To exercise those persistence paths locally — including
`tests/integration/test_postgres_store.py`,
`tests/integration/test_db_transactions.py`, and the `postgres` example —
run `scripts/dev-postgres.sh` (or reopen the dev container, whose compose
stack includes the same service shaped like the CI one) and paste the
`KITH_PG_*` variables it prints into the shell; see CONTRIBUTING.md for
details. Without a usable Postgres those tests skip cleanly.

## Where to go next

- `docs/guides/writing_extensions.md` — add a new wire message type and a
  Python handler with no C.
- `docs/guides/wire_protocol.md` — the v1 wire format for a hand-written
  client or peer.
- `docs/guides/operations.md` — the operator's runbook for a running
  server.
- `docs/guides/agentic_headless_client.md` — drive a running server
  through the closed-loop agentic harness.
- `docs/architecture/planes.md` — the plane contracts every example honors.
- `docs/architecture/topologies.md` — embedded versus distributed wiring.
- `docs/architecture/performance_budgets.md` — the per-operation budgets,
  the scaling-gate reconciliation, and the measured capacity table.
- `examples/` — the example servers and the game library this guide
  walks.
- `python/kith/examples/visual/README.md` — the out-of-box visual example
  the installed package ships.
