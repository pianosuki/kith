# embedded

The spatial game library and tile2d extension on the embedded topology: one
process, no coordination bus, no Postgres pool. The same wire catalog, handlers,
and tile2d physics as the spatial reference game, wired onto the composition
root's single-process topology. This "you do not need a cluster" setup
configures topology independently of the game.

## What it proves

- The `Server` facade accepts `topology="embedded"` and the composition root
  wires the coordinator with no bus and the fabric with in-memory storage, so
  every plane runs in one process.
- The spatial library (`examples/spatial/messages.py`), handlers
  (`examples/spatial/handlers.py`), and the tile2d extension
  (`examples/tile_rpg/physics.py`) run unchanged: the same modules a distributed
  wiring would load on a cluster load here on one process.
- The wire handlers and the control-plane routes share one actor table through
  the handler set's public mutation methods (`spawn_actor`, `apply_movement`,
  `teleport_actor`, `actor_state`, `actor_states`), so a wire frame and an HTTP
  command mutate the same state.
- The actor table is in-memory and scoped to the process. Its state is lost when
  the process stops or restarts. A game that needs cross-restart durability can
  use the generic `GameStateStore` interface with durable backing and
  application-owned save/load handlers. This example leaves out that persistence
  wiring.

## What it does not wire

- **Coordination bus**: the embedded topology's coordinator owns every cell;
  there is no bus to drain.
- **Postgres pool**: no `register_query` call; the in-memory actor table is the
  whole game state.
- **Second instance**: one process is the whole deployment.

## Run

```
python -m examples.embedded.server
```

Prints `embedded: gateway=<port> control=<port>` and blocks until interrupted. A
wire client connects to the gateway port; a harness drives the control plane at
the control port.

The same wiring runs under the tiered delivery preset:

```
python -m examples.embedded.server --delivery-strategy tiered
```

## Delivery presets

The gateway resolves a delivery preset per session: the rule that decides when
each visible subject's record goes out on its client's connection.

`full` (the factory default) sends every composed record for every visible
subject on every pass. Nothing is skipped, so client logic is simple and
per-client bandwidth is a constant cost (view budget times tick rate) regardless
of what changed. Use it while population is small and simplicity matters most.

`tiered` keeps the same record encoding but schedules by relevance: self and
near actors send at full cadence, far actors at a reduced cadence, and dense far
cells collapse into crowd aggregates: one synthetic record instead of hundreds.
Self records go out on every pass. A non-self record goes out on first delivery,
when its state changes and its cadence is due, or when the maximum-silence
backstop (one second) requires a refresh regardless of change. A client can
therefore always recover from stale state and never silently holds a stale
world. Bandwidth scales with change rate and relevance instead of world size
times tick rate. The certified scaling gates measure this preset. Use it when
the per-client bandwidth cost of `full` is too high at the target population.

A third preset, `delta`, is contract-only: the encoding axis is specified in
`docs/architecture/planes.md`, but it does not ship as a runtime preset.

The preset is configured in the gateway plane independently of topology: both
the embedded and the distributed example select it the same way. `--help` lists
the tuning flags (`--delivery-workers` sizes the delivery executor,
`--tiered-max-gap-ms` overrides the backstop).

## Control plane

| Method | Path                      | Body                               | Returns                                                                                                    |
| ------ | ------------------------- | ---------------------------------- | ---------------------------------------------------------------------------------------------------------- |
| POST   | `/login`                  | `{"principal_id": 100}`            | `{"actor_id": 1, "pos_x": 0, ...}`                                                                         |
| POST   | `/move`                   | `{"actor_id": 1, "move_x": 32767}` | `{"actor_id": 1, "pos_x": ..., "input_tick": 1, ...}`                                                      |
| POST   | `/teleport`               | `{"actor_id": 1, "pos_x": 65536}`  | `{"actor_id": 1, "pos_x": 65536, ...}`                                                                     |
| GET    | `/query_state`            | None                               | `{"actors": [...]}`                                                                                        |
| GET    | `/query_state?actor_id=1` | None                               | `{"actor_id": 1, ...}`                                                                                     |
| GET    | `/bindings`               | None                               | `{"bindings": [{"principal_id": 100, "actor_id": 1}], "bind_conflicts": 0, "identity_gate_drops": 0, ...}` |

Positions are Q16.16 fixed-point (the sim's native representation). A client
that renders in world units divides by `2**16`. The behavior grid is an 8x8 map
with one solid obstacle tile (see `world.tmx`); the tile2d model resolves
axis-separated circle-vs-tile collision against it each step.

## Switching to the distributed topology

A game that outgrows one process constructs the facade with
`topology="distributed"` and supplies a config that points at a coordination bus
and a Postgres pool. The wire catalog, the handlers, the physics, and the
control routes are unchanged. The facade's topology is the only wiring
difference. The `embedded/` wiring is stripped down and reuses the same game
modules, so the same code runs in both.
