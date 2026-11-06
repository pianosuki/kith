# lobby

A non-spatial kith server: presence, rooms, and chat on Gateway + Control + DB
only. No simulation model, no fabric cell product, no AOI subscription. This
shows how a lobby, turn-based game, or other non-spatial multiplayer game can
select the planes it needs and leave the spatial planes out.

## What it proves

- The `Server` facade wires Gateway + Control + DB and leaves Sim, Fabric, and
  AOI unwired (no model registered, no zone reserved, no cell product published,
  no AOI subscription).
- A game-owned wire catalog is separate from `spatial`'s: six types (`login`,
  `presence`, `room_join`, `room_leave`, `chat_room`, `presence_update`) at ids
  \>= `KITH_PROTO_TYPE_USER_BASE`, registered on the borrowed proto. Message
  types are game-owned, not framework-owned.
- The single S2C type (`presence`) carries join/leave/chat/status notifications
  in its payload, not as separate wire types: the same one-replication-type
  principle as the spatial game's `actor_state`.
- The `GameStateStore` interface (from `examples/_common/store.py`) is injected
  into `LobbyState` as an extension point; this wiring reads and writes nothing
  through it. Display names are in-memory only; a game can wire a
  Postgres-backed store for durability.
- The control plane is the harness surface: presence, rooms, chat, and the
  composed `presence` outbox are introspected over HTTP.

## Spatial games add Sim + Fabric + AOI on top

A spatial game (see `free_movement/` and `spatial/`) adds:

- **Sim**: a movement model (`free2d`, `tile2d`) that steps actor positions each
  tick.
- **Fabric**: the cell-stream append-only log that fans cell products out to
  subscribers.
- **AOI**: the area-of-interest index that selects which cells a subscriber
  sees.

The lobby needs none of these. It uses only Gateway (wire framing + session
table), Control (HTTP harness surface), and DB (member persistence), the three
planes every multiplayer game uses.

## Run

```
python -m examples.lobby.server
```

Prints `lobby: gateway=<port> control=<port>` and blocks until interrupted. A
wire client connects to the gateway port; a harness drives the control plane at
the control port.

## Control plane

| Method | Path                  | Body                                             | Returns                                                      |
| ------ | --------------------- | ------------------------------------------------ | ------------------------------------------------------------ |
| POST   | `/login`              | `{"principal_id": 1}`                            | `{"member_id": 1, "display_name": "member-1", ...}`          |
| POST   | `/join`               | `{"member_id": 1, "room": "town"}`               | `{"joined": true, "room": "town", "members": [1]}`           |
| POST   | `/leave`              | `{"member_id": 1, "room": "town"}`               | `{"left": true, "room": "town", "members": []}`              |
| POST   | `/chat`               | `{"member_id": 1, "room": "town", "text": "hi"}` | `{"sent": true, "room": "town"}`                             |
| POST   | `/status`             | `{"member_id": 1, "status": 1}`                  | `{"updated": true}`                                          |
| GET    | `/members`            | None                                             | `{"members": [...]}`                                         |
| GET    | `/rooms`              | None                                             | `{"rooms": {"town": [1, 2]}}`                                |
| GET    | `/presence?room=town` | None                                             | `{"room": "town", "presence": [...]}`                        |
| GET    | `/chat_log?room=town` | None                                             | `{"log": [{"room": "town", "member_id": 1, "text": "hi"}]}`  |
| GET    | `/outbox`             | None                                             | `{"outbox": [{"session_ids": [...], "payload_hex": "..."}]}` |

Status flags: 0 = online, 1 = away, 2 = do-not-disturb.
