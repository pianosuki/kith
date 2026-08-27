"""Postgres-persisted embedded server: the spatial game library with durable state.

The spatial game library and tile2d extension wired onto the embedded
topology, with a real Postgres persistence pool created out of band and
wired alongside the server. Named queries are registered on the pool
through register_queries_on_db; a PostgresStore issues
them through the borrowed db handle via _BorrowedDbPlane. Control
routes demonstrate save/load: an actor's state is persisted to Postgres
and restored on demand within a running server (the load path teleports a
live actor; state does not rehydrate into a fresh process, whose actor
ids differ — the README documents the principal-key caveat).

The persistence pool is created out of band: the server does not expose
its reactor for an external pool to borrow, so the pool owns a standalone
reactor on a background thread. A recurring keepalive timer keeps that
reactor's run loop parked between query bursts — a pool-only reactor
drops to zero registered fds once the idle libpq sockets deregister, and
the run loop exits after one idle iteration when nothing is registered,
which would leave every reply callback undelivered. The server's wire
surface (gateway, sim, fabric) and the persistence pool run side by
side; the PostgresStore's reply callback fires on the pool's
reactor thread and resolves the caller's future on the asyncio loop via
``call_soon_threadsafe``. A dedicated event loop on another background
thread drives the store's async methods; the control-plane routes bridge
sync to async through ``run_coroutine_threadsafe``.

Connection parameters are read from environment variables
(``KITH_PG_HOST``, ``KITH_PG_PORT``, ``KITH_PG_DB``, ``KITH_PG_USER``,
``KITH_PG_PASSWORD``) with defaults pointing at a local Postgres. The
``game_state(key, value)`` table must exist in the target database; the
README documents the schema and the docker command to start a local
Postgres.

What this demonstrates: the Postgres persistence path (the
register_queries_on_db helper, the borrowed db plane, and the
PostgresStore) wired against a real libpq connection, not the
fake ``_DbPlane`` from tests. The same ``QUERIES`` catalog and
PostgresStore the spatial library defines, exercised end-to-end
against a running Postgres.

What it does not wire: the pool is not injected into the server (the
server's borrowed db handle stays NULL). This example demonstrates the
out-of-band path the server header documents.

The db module's parameter values are NUL-terminated text strings, so
binary actor blobs are hex-encoded for text-safe transport. The key is
the actor id as a decimal string; a production server keys by the stable
principal id instead (the README documents this).
"""

from __future__ import annotations

import asyncio
import ctypes
import json
import os
import threading
from collections.abc import Coroutine
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from examples._common.query_state import paginate_query_state
from examples.spatial import handlers as spatial_handlers
from examples.spatial import messages
from examples.spatial.db import (
    PostgresStore,
    decode_actor_blob,
    encode_actor_blob,
    register_queries_on_db,
    store_from_db_handle,
)
from examples.spatial.handlers import SpatialHandlers
from examples.tile_rpg import physics

from kith import Actor, Server, SimInput
from kith._bridge import Bridge
from kith._bridge import load as load_bridge
from kith._generated import configure as configure_all
from kith._generated import db as gen_db
from kith._generated import reactor as gen_reactor
from kith._generated import version as gen_version
from kith.control import Request, Response
from kith.exceptions import KithError, check_error


# Postgres connection-parameter env vars (defaults point at a local Postgres).
_PG_HOST_ENV: str = "KITH_PG_HOST"
_PG_PORT_ENV: str = "KITH_PG_PORT"
_PG_DB_ENV: str = "KITH_PG_DB"
_PG_USER_ENV: str = "KITH_PG_USER"
_PG_PASSWORD_ENV: str = "KITH_PG_PASSWORD"

_DEFAULT_HOST: str = "localhost"
_DEFAULT_PORT: int = 5432
_DEFAULT_DB: str = "kith_example"
_DEFAULT_USER: str = "kith"
_DEFAULT_PASSWORD: str = ""

# The single authoritative zone (same as the embedded example).
_ZONE_NAME: str = "world"

# The TMX behavior grid ships with the embedded example; resolve it
# relative to this module so the example boots regardless of cwd.
_WORLD_TMX: Path = Path(__file__).resolve().parent.parent / "embedded" / "world.tmx"

# Timeout for awaiting a Postgres query reply from a control-plane route.
_DB_TIMEOUT_S: float = 10.0

# Keepalive cadence for the pool-only reactor: the interval only has to be
# short enough that a burst arriving right after a tick waits one interval
# instead of the run loop idling out (which it cannot, while a timer is
# pending).
_KEEPALIVE_INTERVAL_MS: int = 1000


@dataclass(frozen=True)
class ActorState:
    """One actor's published state, exposed to the control plane.

    Positions are Q16.16 fixed-point (the sim's native representation);
    the control plane returns them as integers so a client that wants to
    render in world units divides by 2**16.
    """

    actor_id: int
    pos_x: int
    pos_y: int
    pos_z: int
    vel_x: int
    vel_y: int
    vel_z: int
    input_tick: int


class PostgresServer:
    """The spatial game library on the embedded topology with a Postgres pool.

    Owns the Server facade (the wire/control surface), the
    tile2d model, and the spatial handler set --- the same game library the
    embedded example loads --- alongside a standalone reactor, a db pool
    borrowing that reactor, and a PostgresStore over the pool. The
    control-plane routes drive the handler set for login/move/query and the
    store for save/load, so a harness can exercise the full persistence
    cycle over HTTP.
    """

    __slots__ = (
        "_bridge",
        "_db",
        "_db_lib",
        "_handlers",
        "_keepalive_cb",
        "_loop",
        "_loop_thread",
        "_model",
        "_reactor",
        "_reactor_lib",
        "_reactor_thread",
        "_server",
        "_store",
        "_zone_id",
    )

    def __init__(self) -> None:
        self._server: Server | None = None
        self._model: object | None = None
        self._handlers: SpatialHandlers | None = None
        self._zone_id: int = 0
        self._bridge: Bridge | None = None
        self._reactor_lib: ctypes.CDLL | None = None
        self._db_lib: ctypes.CDLL | None = None
        self._reactor: object | None = None
        self._reactor_thread: threading.Thread | None = None
        self._keepalive_cb: object | None = None
        self._db: object | None = None
        self._store: PostgresStore | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._loop_thread: threading.Thread | None = None

    # -----------------------------------------------------------------------
    # lifecycle
    # -----------------------------------------------------------------------

    def start(self) -> tuple[Server, int, int]:
        """Build the server, the db pool, and the store; return ready to run.

        Returns the facade, the gateway port, and the control-plane port.
        The caller drives run on its own thread and calls
        stop to shut down.
        """
        bridge = load_bridge()
        configure_all(bridge)
        self._bridge = bridge
        self._reactor_lib = bridge.lib("reactor")
        self._db_lib = bridge.lib("db")

        self._start_reactor()
        self._create_db_pool()
        self._start_event_loop()

        assert self._db is not None and self._db_lib is not None
        assert self._loop is not None
        register_queries_on_db(self._db, self._db_lib)
        self._store = store_from_db_handle(self._db, self._db_lib, loop=self._loop)

        server = Server(
            topology="embedded",
            listen_port=0,
            tick_hz=20,
            handler_table_size=4096,
            replication_type_id=messages.ACTOR_STATE_TYPE,
        )
        self._server = server

        messages.register(server)
        self._zone_id, self._model = physics.register(
            server,
            zone_name=_ZONE_NAME,
            tmx_path=_WORLD_TMX,
        )
        self._handlers = spatial_handlers.register(
            server,
            model=self._model,
            zone_id=self._zone_id,
        )

        # The spatial handlers defer cell-product bumps to a per-tick
        # dirty-cell set; register the flush as the server's tick callback so
        # every cell dirtied since the last flush is bumped once per tick.
        # The composition root owns this slot, matching the embedded wiring.
        server.register_tick_handler(self._handlers.flush_dirty_cells)

        server.register_control_route("POST", "/login", self._route_login)
        server.register_control_route("POST", "/move", self._route_move)
        server.register_control_route("POST", "/save", self._route_save)
        server.register_control_route("GET", "/load", self._route_load)
        server.register_control_route("GET", "/query_state", self._route_query_state)

        return server, server.listen_port, server.control_port

    def stop(self) -> None:
        """Release the server, the store, the db pool, and the reactor."""
        if self._server is not None:
            self._server.shutdown()
            self._server.close()
            self._server = None
        self._handlers = None
        self._model = None
        self._store = None

        if self._loop is not None:
            self._loop.call_soon_threadsafe(self._loop.stop)
        if self._loop_thread is not None:
            self._loop_thread.join(timeout=5.0)
            self._loop_thread = None
        # The loop thread has exited, so nothing races the close; an
        # unclosed loop trips a resource warning when it is collected.
        if self._loop is not None:
            self._loop.close()
            self._loop = None

        if self._reactor_lib is not None and self._reactor is not None:
            self._reactor_lib.kith_reactor_stop(self._reactor)
        if self._reactor_thread is not None:
            self._reactor_thread.join(timeout=5.0)
            self._reactor_thread = None

        if self._db_lib is not None and self._db is not None:
            self._db_lib.kith_db_destroy(self._db)
        self._db = None

        if self._reactor_lib is not None and self._reactor is not None:
            self._reactor_lib.kith_reactor_destroy(self._reactor)
        # The timer node died with the wheel; the callback trampoline can go
        # now that no tick can fire.
        self._keepalive_cb = None
        self._reactor = None
        self._reactor_lib = None
        self._db_lib = None
        self._bridge = None

    # -----------------------------------------------------------------------
    # db pool / reactor / event loop
    # -----------------------------------------------------------------------

    def _start_reactor(self) -> None:
        assert self._reactor_lib is not None
        params = gen_reactor.kith_reactor_params_t(
            size=ctypes.sizeof(gen_reactor.kith_reactor_params_t),
            abi_version=gen_version.KITH_ABI_VERSION,
            max_fds=64,
            task_capacity=64,
        )
        out = ctypes.POINTER(gen_reactor.kith_reactor_t)()
        rc = int(
            self._reactor_lib.kith_reactor_create(
                ctypes.byref(params),
                None,
                ctypes.byref(out),
            )
        )
        check_error(rc, "kith_reactor_create")
        self._reactor = out
        # The recurring timer keeps timer_count above zero, so the run loop
        # blocks between query bursts instead of returning after one idle
        # iteration (nothing else is registered once the idle libpq sockets
        # deregister) and stranding the pool's reply callbacks.
        self._keepalive_cb = gen_reactor.kith_reactor_task_cb(self._keepalive_tick)
        now = int(self._reactor_lib.kith_reactor_now_ms(out))
        rc = int(
            self._reactor_lib.kith_reactor_schedule(
                out,
                ctypes.c_uint64(now + _KEEPALIVE_INTERVAL_MS),
                self._keepalive_cb,
                ctypes.c_void_p(0),
            )
        )
        check_error(rc, "kith_reactor_schedule (keepalive)")
        self._reactor_thread = threading.Thread(
            target=self._run_reactor,
            args=(self._reactor_lib, out),
            daemon=True,
        )
        self._reactor_thread.start()

    def _keepalive_tick(self, _ctx: object) -> None:
        assert self._reactor_lib is not None and self._reactor is not None
        now = int(self._reactor_lib.kith_reactor_now_ms(self._reactor))
        rc = int(
            self._reactor_lib.kith_reactor_schedule(
                self._reactor,
                ctypes.c_uint64(now + _KEEPALIVE_INTERVAL_MS),
                self._keepalive_cb,
                ctypes.c_void_p(0),
            )
        )
        # A failed re-arm leaves the reactor to idle out and the next db
        # exec to time out; surface the failure instead of swallowing it.
        check_error(rc, "kith_reactor_schedule (keepalive re-arm)")

    def _create_db_pool(self) -> None:
        assert self._db_lib is not None and self._reactor is not None
        host = os.environ.get(_PG_HOST_ENV, _DEFAULT_HOST)
        port = int(os.environ.get(_PG_PORT_ENV, str(_DEFAULT_PORT)))
        db_name = os.environ.get(_PG_DB_ENV, _DEFAULT_DB)
        user = os.environ.get(_PG_USER_ENV, _DEFAULT_USER)
        password = os.environ.get(_PG_PASSWORD_ENV, _DEFAULT_PASSWORD)

        params = gen_db.kith_db_params_t(
            size=ctypes.sizeof(gen_db.kith_db_params_t),
            abi_version=gen_version.KITH_ABI_VERSION,
            host=host.encode("utf-8"),
            port=port,
            db_name=db_name.encode("utf-8"),
            user=user.encode("utf-8"),
            password=password.encode("utf-8") if password else None,
        )
        out = ctypes.POINTER(gen_db.kith_db_t)()
        rc = int(
            self._db_lib.kith_db_create(
                ctypes.byref(params),
                self._reactor,
                None,
                ctypes.byref(out),
            )
        )
        check_error(rc, "kith_db_create")
        self._db = out

    def _start_event_loop(self) -> None:
        loop = asyncio.new_event_loop()
        self._loop = loop
        self._loop_thread = threading.Thread(
            target=self._run_event_loop,
            args=(loop,),
            daemon=True,
        )
        self._loop_thread.start()

    @staticmethod
    def _run_reactor(lib: ctypes.CDLL, handle: object) -> None:
        lib.kith_reactor_run(handle)

    @staticmethod
    def _run_event_loop(loop: asyncio.AbstractEventLoop) -> None:
        asyncio.set_event_loop(loop)
        loop.run_forever()

    # -----------------------------------------------------------------------
    # control-plane routes
    # -----------------------------------------------------------------------

    def _route_login(self, req: Request, resp: Response) -> None:
        body = _read_json(req)
        principal_id = int(body["principal_id"])
        assert self._handlers is not None
        actor = self._handlers.spawn_actor(principal_id)
        _write_json(resp, 200, _actor_dict(actor))

    def _route_move(self, req: Request, resp: Response) -> None:
        body = _read_json(req)
        actor_id = int(body["actor_id"])
        move_x = int(body.get("move_x", 0))
        move_y = int(body.get("move_y", 0))
        assert self._handlers is not None
        actor = self._handlers.actor_state(actor_id)
        if actor is None:
            _write_json(resp, 404, {"error": "actor not found"})
            return
        updated = self._handlers.apply_movement(
            actor_id,
            SimInput(input_tick=actor.input_tick + 1, move_x=move_x, move_y=move_y),
        )
        assert updated is not None
        _write_json(resp, 200, _actor_dict(updated))

    def _route_save(self, req: Request, resp: Response) -> None:
        body = _read_json(req)
        actor_id = int(body["actor_id"])
        assert self._handlers is not None
        actor = self._handlers.actor_state(actor_id)
        if actor is None:
            _write_json(resp, 404, {"error": "actor not found"})
            return
        blob = encode_actor_blob(actor)
        key = str(actor_id).encode("utf-8")
        value = blob.hex().encode("utf-8")
        try:
            self._await(self._store_put(key, value))
        except KithError as exc:
            _write_json(resp, 502, {"error": str(exc)})
            return
        except TimeoutError:
            _write_json(resp, 504, {"error": "db query timed out"})
            return
        _write_json(resp, 200, {"actor_id": actor_id, "saved": True})

    def _route_load(self, req: Request, resp: Response) -> None:
        actor_id_str = req.path.rpartition("=")[2] if "actor_id=" in req.path else ""
        if not actor_id_str:
            _write_json(resp, 400, {"error": "actor_id required"})
            return
        actor_id = int(actor_id_str)
        key = str(actor_id).encode("utf-8")
        try:
            value = self._await(self._store_get(key))
        except KithError as exc:
            _write_json(resp, 502, {"error": str(exc)})
            return
        except TimeoutError:
            _write_json(resp, 504, {"error": "db query timed out"})
            return
        if value is None:
            _write_json(resp, 404, {"error": "no saved state"})
            return
        blob = bytes.fromhex(value.decode("utf-8"))
        saved = decode_actor_blob(blob)
        assert self._handlers is not None
        updated = self._handlers.teleport_actor(actor_id, saved.pos_x, saved.pos_y)
        if updated is None:
            _write_json(resp, 404, {"error": "actor not found"})
            return
        _write_json(resp, 200, _actor_dict(updated))

    def _route_query_state(self, req: Request, resp: Response) -> None:
        assert self._handlers is not None
        actors = [_actor_dict(a) for a in self._handlers.actor_states()]
        _write_json(resp, 200, paginate_query_state(req.path, actors))

    # -----------------------------------------------------------------------
    # helpers
    # -----------------------------------------------------------------------

    def _store_put(self, key: bytes, value: bytes) -> Coroutine[Any, Any, None]:
        assert self._store is not None
        return self._store.put(key, value)

    def _store_get(self, key: bytes) -> Coroutine[Any, Any, bytes | None]:
        assert self._store is not None
        return self._store.get(key)

    def _await(self, coro: Coroutine[Any, Any, Any]) -> Any:
        assert self._loop is not None
        future = asyncio.run_coroutine_threadsafe(coro, self._loop)
        return future.result(timeout=_DB_TIMEOUT_S)


# ---------------------------------------------------------------------------
# module-level helpers
# ---------------------------------------------------------------------------


def _read_json(req: Request) -> dict[str, Any]:
    return json.loads(req.body.decode("utf-8")) if req.body else {}


def _actor_dict(actor: Actor) -> dict[str, Any]:
    return asdict(
        ActorState(
            actor_id=actor.id,
            pos_x=actor.pos_x,
            pos_y=actor.pos_y,
            pos_z=actor.pos_z,
            vel_x=actor.vel_x,
            vel_y=actor.vel_y,
            vel_z=actor.vel_z,
            input_tick=actor.input_tick,
        )
    )


def _write_json(resp: Response, status: int, body: dict[str, Any]) -> None:
    resp.status(status, "application/json")
    resp.body(json.dumps(body).encode("utf-8"))


# ---------------------------------------------------------------------------
# launcher
# ---------------------------------------------------------------------------


def main() -> None:
    """Boot the server with its Postgres pool and block until interrupted.

    Starts the db pool's reactor and the store's event loop on background
    threads, builds the embedded server with the spatial game library,
    prints the ports, and blocks until interrupted. A harness drives the
    persistence surface (save/load) and the game surface (login/move) over
    the control plane.
    """
    server = PostgresServer()
    facade, gateway_port, control_port = server.start()
    print(
        f"postgres: gateway={gateway_port} control={control_port} "
        f"pg={os.environ.get(_PG_HOST_ENV, _DEFAULT_HOST)}:"
        f"{os.environ.get(_PG_PORT_ENV, str(_DEFAULT_PORT))}",
        flush=True,
    )
    try:
        facade.serve()
    finally:
        server.stop()


if __name__ == "__main__":
    main()
