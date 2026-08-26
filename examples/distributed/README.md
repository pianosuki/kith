# distributed

Two embedded servers on one shared coordination bus. The spatial game
library and tile2d extension from `examples/embedded` run on two
`EmbeddedServer` instances; a real `kith_coord_bus_t` carries cell
authority rebalance contracts between them, and the receiving coord
applies each contract via `Coord.on_rebalance`, transferring cell
ownership across the instance boundary. This is the split/merge transfer
contract — the overlapping cell-stream handoff is a bus-carried rebalance,
not an actor-by-actor state transfer — exercised end-to-end on real C
handles in one process.

## What it proves

- A `CoordBus` is constructed once and borrowed by two `Coord` handles,
  so a rebalance contract published by one instance is drainable by the
  other. The bus is the inter-instance transport; the coords are the
  per-instance authority tables.
- A split cycle: instance A (the source authority) records the override
  locally, publishes a `RebalanceContract` on the bus, and instance B
  drains the event and applies it. Both coords converge on instance B as
  the new authority at the new epoch. Querying instance A's coord shows
  the authority moving from instance 1 (hash-fallback) to instance 2
  (override).
- A merge cycle: instance A clears its override and publishes a merge
  contract (target 0); instance B applies it, reverting to hash-fallback
  distribution. The authority returns to instance 1.
- The wire catalog (`examples/spatial/messages.py`), the handlers
  (`examples/spatial/handlers.py`), and the tile2d physics
  (`examples/tile_rpg/physics.py`) run unchanged on both servers — the
  same game modules the embedded example loads, on two instances.

## What it does not wire

- **Borrowed coord injection** — the `Server` facade owns every plane
  handle it creates and does not expose a borrowed coord handle for an
  externally-built bus. The servers here run on the embedded topology
  (no internal bus); the coordination bus and the two coords live
  alongside the servers, driven by the launcher.
- **Density-driven automatic split** — the coordinator's threshold
  evaluator fires when the bus has more than one member. The loopback
  transport the bus constructs with carries one member (the local
  instance), so the evaluator short-circuits. The ownership-transfer
  contract — the bus carrying the rebalance and the receiving coord
  applying it — is what this example exercises directly. Automatic
  density-triggered split requires a multi-member bus transport.
- **Cross-instance fabric replication** — each server's fabric is
  process-local (in-memory storage). A cell-stream subscription that
  crosses the instance boundary requires a multi-instance fabric
  transport, the same transport gap as the bus. The authority transfer
  is the foundation; the replication is a separate transport layer.

## Run

```
python -m examples.distributed.server
```

Prints `distributed: gateway_a=<port> control_a=<port> gateway_b=<port>
control_b=<port>` and one line showing the authority transition, then
blocks until interrupted. A wire client connects to either gateway; a
harness drives the coordination surface at `control_a`.

Both instances always run the same delivery preset — the preset is a
cluster-level choice, forwarded to every server at construction:

```
python -m examples.distributed.server --delivery-strategy tiered
```

`tiered` is the relevance-tiered preset the certified scaling gates
measure; the full-versus-tiered trade is described in
`examples/embedded/README.md` under Delivery presets.

## Control plane (instance A)

| Method | Path | Body / Query | Returns |
|--------|------|-------------|---------|
| GET | `/authority` | `?zone=1&cell_x=1&cell_y=1&owner=A` | `{"instance_id": 1, "authority_epoch": 0, "owner": "A"}` |
| POST | `/split` | `{"zone": 1, "cell_x": 1, "cell_y": 1}` | `{"instance_id": 2, "authority_epoch": 1, "owner": "A"}` |
| POST | `/merge` | `{"zone": 1, "cell_x": 1, "cell_y": 1}` | `{"instance_id": 1, "authority_epoch": 0, "owner": "A"}` |
| GET | `/members` | — | `{"members": [{"instance_id": 1, ...}]}` |

`owner` selects which coord to query (`A` or `B`); the default is `B`.
Positions are cell grid coordinates (`int32` zone, cell_x, cell_y,
cell_z, and a `lod` tier).

## Relationship to the embedded example

`examples/embedded` is the single-process topology: one server, no bus,
no second instance. This example is the two-instance layer on top: it
reuses `EmbeddedServer` unchanged for the wire surface and adds the
coordination bus + two coords that demonstrate the distributed contract.
The same game code runs in both wirings — the topology is a property of
the composition root, not of the game.
