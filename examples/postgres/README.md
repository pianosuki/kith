# postgres

The spatial game library and tile2d extension on the embedded topology,
with a real Postgres persistence pool created out of band and wired
alongside the server. Named queries are registered on the pool through
`register_queries_on_db`; a `PostgresStore` issues them through the
borrowed db handle via `_BorrowedDbPlane`. Control routes demonstrate
save/load: an actor's state is persisted to Postgres and restored on
demand within a running server (the load path teleports a live actor;
see the key caveat under the persistence cycle below). The relational
tier — transaction contexts and row results over a pool the game owns —
is the `kith.db` module; this example demonstrates the byte-key store
path.

## The relational tier

This example's save/load path rides the borrowed db handle and the
byte-key store; the package-tier relational surface is the `kith.db`
module. A `Database` owns its pool and its reactor thread, raw SQL and
named queries return `Reply` row snapshots, and `transaction()` pins one
session for the block: the commit resolves before the block exits, an
exception rolls back. The snippet runs against the same instance
Prerequisites provisions:

```python
import asyncio

from kith.db import Database


async def main() -> None:
    db = Database(
        host="127.0.0.1",
        port=5432,
        db_name="kith_example",
        user="kith",
        password="kith",
    )
    try:
        async with db.transaction() as txn:
            await txn.query("INSERT INTO game_state (key, value) VALUES ($1, $2)", ["a", "b"])
            reply = await txn.query("SELECT value FROM game_state WHERE key = $1", ["a"])
            print(reply.rows)  # (('b',),)
    finally:
        db.close()


asyncio.run(main())
```

## What it proves

- A real `kith_db_t` pool is created via `kith_db_create` borrowing a
  standalone reactor, and named queries (`state_get`, `state_put`,
  `state_delete`) are registered on it through `register_queries_on_db`
  — the out-of-band path the `kith_server_db` header documents.
- A `PostgresStore` issues the registered queries through
  `_BorrowedDbPlane` over `kith_db_exec`, and the reply callback fires
  on the pool's reactor thread, correlating by `user_data` id and
  resolving the caller's future on the asyncio loop via
  `call_soon_threadsafe`. This is the real libpq async path, not the
  fake `_DbPlane` from tests.
- The `/save` route serializes an actor through `encode_actor_blob`,
  hex-encodes the blob for text-safe transport (the db module's
  parameter values are NUL-terminated text strings), and puts it under
  the actor id as key. The `/load` route retrieves it, decodes it
  through `decode_actor_blob`, and teleports the actor to the saved
  position. A full save/move/load cycle is drivable over HTTP.
- The same spatial game library (`messages.py`, `handlers.py`,
  `db.py`) and tile2d physics (`tile_rpg/physics.py`) the embedded
  example loads run unchanged here — the persistence layer is a
  composition-root concern, not a game-code change.

## What it does not wire

- **Pool injection into the server** — the `Server` facade does not
  expose its reactor for an external pool to borrow, and the server's
  `kith_server_db` accessor returns NULL (the server's wiring never
  creates a db pool, so `register_query` on the facade has no pool to
  reach). The pool here owns a standalone reactor on a
  background thread; the server's wire surface and the persistence
  pool run side by side.
- **Binary-safe parameter transport** — the db module's parameter
  values are NUL-terminated text strings (`const char *const *params`
  in `kith_db_exec`). Binary actor blobs that contain NUL bytes would
  be truncated, so the example hex-encodes them. Binary-safe parameter
  transport (length-prefixed values) requires a C API change.

## Prerequisites

A running Postgres with the `game_state` table. `scripts/dev-postgres.sh`
provisions a CI-shaped instance (role `kith`, password `kith`, database
`kith_example`), creates the table, waits for readiness, and prints the
exact `KITH_PG_*` exports to paste into the shell:

```
./scripts/dev-postgres.sh
# paste the printed exports, e.g.:
# export KITH_PG_HOST=127.0.0.1
# export KITH_PG_PORT=5432
# export KITH_PG_USER=kith
# export KITH_PG_PASSWORD=kith
# export KITH_PG_DB=kith_example
```

The exports are required before starting the server: the built-in
defaults use an empty password, which the provisioned role rejects. If
port 5432 is already taken, start the dev instance
elsewhere (`KITH_DEV_PG_PORT=55432 ./scripts/dev-postgres.sh`).

Using an existing Postgres instead: create the role, a `kith_example`
database, and the table:

```sql
CREATE TABLE IF NOT EXISTS game_state (
    key   text PRIMARY KEY,
    value text NOT NULL
);
```

The key and value are `text` columns because the db module's parameter
values are NUL-terminated text strings (see "What it does not wire"
above).

## Run

```
python -m examples.postgres.server
```

Connection parameters are read from environment variables with defaults
matching the provisioned instance:

| Var | Default | Description |
|-----|---------|-------------|
| `KITH_PG_HOST` | `localhost` | Postgres server hostname |
| `KITH_PG_PORT` | `5432` | Postgres TCP port |
| `KITH_PG_DB` | `kith_example` | Database name |
| `KITH_PG_USER` | `kith` | Postgres role |
| `KITH_PG_PASSWORD` | (empty) | Postgres password |

The server boots even when Postgres is not reachable: `kith_db_create`
initiates async connections and returns before they complete, so the
wire surface starts. Save/load routes report the connection error
(`KITH_ECONNRESET` or a Postgres error status) as a JSON error response
rather than crashing the server.

Prints `postgres: gateway=<port> control=<port> pg=<host>:<port>` and
blocks until interrupted.

## Control plane

| Method | Path | Body / Query | Returns |
|--------|------|-------------|---------|
| POST | `/login` | `{"principal_id": 1}` | `{"actor_id": 1, ...}` |
| POST | `/move` | `{"actor_id": 1, "move_x": 1}` | `{"actor_id": 1, ...}` |
| POST | `/save` | `{"actor_id": 1}` | `{"actor_id": 1, "saved": true}` |
| GET | `/load?actor_id=1` | — | `{"actor_id": 1, ...}` |
| GET | `/query_state` | — | `{"actors": [...]}` |

A full persistence cycle: `/login` spawns an actor, `/move` changes its
position, `/save` persists the state to Postgres, `/load` restores it on
demand (the actor must be live in this server's table), `/query_state`
returns the live state.

The persistence key is the actor id (a server-assigned monotonic id). A
production server keys by the stable principal id instead, so saved
state survives across restarts with different actor-id assignments.

## Relationship to the embedded example

`examples/embedded` is the single-process topology with no persistence:
the in-memory actor table is the whole game state. This example is the
persistence layer on top: it reuses `EmbeddedServer`'s game library and
adds a Postgres pool + `PostgresStore` alongside it. The same game code
runs in both wirings — persistence is a composition-root concern, not
a property of the game.
