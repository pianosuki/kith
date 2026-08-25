# free_movement

The thinnest end-to-end kith server: one `free2d` model, one wire input
(`actor_input`), one replication type (`actor_state`), and four
control-plane commands (`spawn`/`move`/`teleport`/`query_state`). No
persistence, no chat, no login. This is a shipped example, not throwaway
scaffolding.

## What it proves

- The `Server` facade wires the planes and exposes the borrowed handles.
- A built-in sim model (`free2d`) is instantiated, stepped per tick, and
  its actors' state is published as cell artifacts.
- A wire handler translates a decoded frame into a sim `apply_input` call.
- The gateway encodes `actor_state` as the replication type.
- The per-tick publish choreography: step, `publish_artifact` per actor,
  and one `publish_cell_product` bump per dirty cell with a monotonic
  per-cell authority epoch — the chain a wire subscriber's replication
  rides.
- The control plane carries the harness command surface (the agentic
  scenarios drive actors through HTTP, not the wire protocol).
- One actor spawns, moves via a control-plane command, and queries its
  state back.

## Run

```
python -m examples.free_movement.server
```

Prints `free_movement: gateway=<port> control=<port>` and blocks until
interrupted. A wire client connects to the gateway port; a harness drives
the control plane at the control port. A wire session additionally needs
the delivery startup contract — bind the session and seed its subscription
window — documented in docs/guides/getting_started.md ("From input to
delivery"); this login-less skeleton leaves that contract to the caller.

## Control plane

| Method | Path                          | Body                                 | Returns |
|--------|-------------------------------|--------------------------------------|--------|
| POST   | `/spawn`                      | `{}`                                 | `{"actor_id": 1}` |
| POST   | `/move`                       | `{"actor_id": 1, "move_x": 32767}`  | `{"actor_id": 1, "pos_x": ..., "input_tick": 1, ...}` |
| POST   | `/teleport`                   | `{"actor_id": 1, "pos_x": 65536}`   | `{"actor_id": 1, "pos_x": 65536, ...}` |
| GET    | `/query_state`                | —                                    | `{"actors": [...]}` |
| GET    | `/query_state?actor_id=1`     | —                                    | `{"actor_id": 1, ...}` |

Positions are Q16.16 fixed-point (the sim's native representation). A client
that renders in world units divides by `2**16`.
