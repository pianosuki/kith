"""Integration test: the Postgres backing against a running Postgres.

Exercises the real libpq async path end to end. A standalone reactor on
a background thread pumps a db pool created via ``kith_db_create``; the
spatial library's named queries are registered on the pool through
``register_queries_on_db``; a ``PostgresStore`` issues them through
``kith_db_exec`` via ``_BorrowedDbPlane``. Reply callbacks fire on the
pool's reactor thread and resolve the caller's asyncio future on the
test thread via ``call_soon_threadsafe``, correlating by the
``user_data`` id.

The fake-plane unit tests in ``tests/python/test_spatial_db.py`` cover
the correlation edge cases (unknown id, submission failure, status
failure, manual ordering) that a live Postgres cannot reach; this file
covers the path against a live Postgres that the fakes cannot. The
actor blob is hex-encoded for transport because the db module's
parameter values are NUL-terminated text (binary blobs with NUL bytes
would truncate), mirroring the postgres example's save/load routes.

The acquisition/release sequence lives in ``_pg_stack_resources`` so it
can also be driven without a server: the teardown-contract class feeds
it configurations that fail at different points and asserts the stack
still releases everything it acquired, in the order the C lifecycle
contracts require.

Skips locally unless the configured Postgres accepts a real login (set
``KITH_PG_HOST`` etc. to point at one; ``scripts/dev-postgres.sh``
provisions a matching instance). A server that answers but rejects the
configured role or database also skips, with the server's own error as
the skip reason. In CI the skip is disabled so a broken service fails
the build rather than hiding behind a skip.
"""

from __future__ import annotations

import asyncio
import contextlib
import ctypes
import os
import sys
import threading
import time
import warnings
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import pytest
from _helpers import needs_build
from examples.spatial.db import (
    decode_actor_blob,
    encode_actor_blob,
    register_queries_on_db,
    store_from_db_handle,
)

from kith import Actor, KithError
from kith._bridge import Bridge
from kith._generated import db as gen_db
from kith._generated import reactor as gen_reactor
from kith._generated import types as gen_types
from kith._generated import version as gen_version
from kith.exceptions import check_error, code_for


# The store tests share one game_state table with fixed keys and truncate
# it per test, so the whole postgres cohort must run inside one xdist
# worker (--dist loadgroup; the integration conftest enforces that).
pytestmark = pytest.mark.xdist_group("postgres")

_REPO_ROOT = Path(__file__).resolve().parents[2]

_PG_HOST_ENV = "KITH_PG_HOST"
_PG_PORT_ENV = "KITH_PG_PORT"
_PG_DB_ENV = "KITH_PG_DB"
_PG_USER_ENV = "KITH_PG_USER"
_PG_PASSWORD_ENV = "KITH_PG_PASSWORD"

_DEFAULT_HOST = "localhost"
_DEFAULT_PORT = 5432
_DEFAULT_DB = "kith_example"
_DEFAULT_USER = "kith"
_DEFAULT_PASSWORD = ""

_DB_TIMEOUT_S = 10.0
_CONNECT_WAIT_S = 8.0
_KEEPALIVE_INTERVAL_MS = 1000
_REACTOR_THREAD_NAME = "pg-stack-reactor"
_TEARDOWN_JOIN_S = 5.0


# DDL registered alongside the game-state catalog and issued through the
# same kith_db_exec path: the test exercises the framework's own query
# transport for schema setup too, with no external psql dependency.
_SETUP_NAME = "test_setup_schema"
_TRUNCATE_NAME = "test_truncate_state"
_SETUP_SQL = "CREATE TABLE IF NOT EXISTS game_state (key text PRIMARY KEY, value text NOT NULL)"
_TRUNCATE_SQL = "TRUNCATE game_state"


def _pg_config() -> dict[str, str | int]:
    return {
        "host": os.environ.get(_PG_HOST_ENV, _DEFAULT_HOST),
        "port": int(os.environ.get(_PG_PORT_ENV, str(_DEFAULT_PORT))),
        "db": os.environ.get(_PG_DB_ENV, _DEFAULT_DB),
        "user": os.environ.get(_PG_USER_ENV, _DEFAULT_USER),
        "password": os.environ.get(_PG_PASSWORD_ENV, _DEFAULT_PASSWORD),
    }


# libpq is already a link-time dependency of libkith_db.so; loading it a
# second time by soname returns a handle to the same mapping. The
# platform branch mirrors the bridge's shared-object suffix handling.
_PG_SONAMES = (
    ("libpq.dylib", "libpq.5.dylib") if sys.platform == "darwin" else ("libpq.so.5", "libpq.so")
)


def _libpq_login_error() -> str | None:
    """Log in through libpq with the configured credentials.

    Returns the server's error text when the login fails (unknown role,
    unknown database, bad password) or ``None`` when the connection is
    usable. The blocking connect is bounded by libpq's own
    ``connect_timeout``.
    """
    lib = None
    for soname in _PG_SONAMES:
        try:
            lib = ctypes.CDLL(soname)
            break
        except OSError:
            continue
    if lib is None:
        return (
            "libpq not found; cannot verify the configured Postgres "
            f"(tried {', '.join(_PG_SONAMES)})"
        )

    lib.PQconnectdbParams.argtypes = [
        ctypes.POINTER(ctypes.c_char_p),
        ctypes.POINTER(ctypes.c_char_p),
        ctypes.c_int,
    ]
    lib.PQconnectdbParams.restype = ctypes.c_void_p
    lib.PQstatus.argtypes = [ctypes.c_void_p]
    lib.PQstatus.restype = ctypes.c_int
    lib.PQerrorMessage.argtypes = [ctypes.c_void_p]
    lib.PQerrorMessage.restype = ctypes.c_char_p
    lib.PQfinish.argtypes = [ctypes.c_void_p]
    lib.PQfinish.restype = None

    cfg = _pg_config()
    keywords = ["host", "port", "dbname", "user", "connect_timeout"]
    values: list[str] = [
        str(cfg["host"]),
        str(cfg["port"]),
        str(cfg["db"]),
        str(cfg["user"]),
        "2",
    ]
    if cfg["password"]:
        keywords.insert(4, "password")
        values.insert(4, str(cfg["password"]))

    kw_arr = (ctypes.c_char_p * (len(keywords) + 1))(
        *[kw.encode("utf-8") for kw in keywords],
        None,
    )
    val_arr = (ctypes.c_char_p * (len(values) + 1))(
        *[val.encode("utf-8") for val in values],
        None,
    )
    conn: int | None = lib.PQconnectdbParams(kw_arr, val_arr, 0)
    if conn is None:
        return "libpq returned no connection object for the configured server"
    try:
        if int(lib.PQstatus(conn)) == 0:  # CONNECTION_OK
            return None
        message = lib.PQerrorMessage(conn) or b""
        return (
            "postgres rejected the configured role/database: "
            + bytes(message).decode("utf-8", errors="replace").strip()
        )
    finally:
        lib.PQfinish(conn)


def _pg_skip_reason() -> str | None:
    """Return why the live tests cannot run, or ``None`` to run them.

    In CI the probe never runs: the workflow provisions the service
    container and a broken one must fail the build loudly instead of
    skipping. Locally the check is a real login so that a reachable but
    misconfigured server produces an actionable skip reason at
    collection time rather than mid-fixture connection errors.
    """
    if os.environ.get("CI"):
        return None
    try:
        return _libpq_login_error()
    except (OSError, AttributeError) as exc:
        # A missing client library, or one lacking the symbols the probe
        # binds, is an environment gap; anything else propagates.
        return f"postgres client library unusable ({type(exc).__name__}: {exc})"


_PG_SKIP_REASON: str | None = _pg_skip_reason()


needs_pg = pytest.mark.skipif(
    _PG_SKIP_REASON is not None,
    reason=_PG_SKIP_REASON or "no usable Postgres",
)


@dataclass
class _PgStack:
    db_lib: ctypes.CDLL
    db_handle: object


class _OneShotExec:
    """Issue a registered 0-param query and wait for its reply.

    A one-shot plane separate from ``_BorrowedDbPlane``: the store's
    plane pins its trampoline to the store's correlation handler, so DDL
    rides a dedicated trampoline that signals completion via a
    ``threading.Event``. The reply fires on the reactor thread; the test
    thread blocks on the event.
    """

    __slots__ = ("_cb", "_db", "_event", "_lib", "_status")

    def __init__(self, db_lib: ctypes.CDLL, db_handle: object) -> None:
        self._lib = db_lib
        self._db = db_handle
        self._event = threading.Event()
        self._status = 0
        self._cb = gen_db.kith_db_reply_fn(self._on_reply)

    def run(self, name: str, *, timeout_s: float = _DB_TIMEOUT_S) -> None:
        self._event.clear()
        self._status = 0
        rc = int(
            self._lib.kith_db_exec(
                self._db,
                name.encode("utf-8"),
                None,
                ctypes.c_uint32(0),
                self._cb,
                ctypes.c_void_p(0),
            )
        )
        check_error(rc, f"kith_db_exec: {name}")
        if not self._event.wait(timeout=timeout_s):
            raise TimeoutError(f"db exec timed out: {name}")
        if self._status != 0:
            raise KithError(code_for(self._status), f"db query failed: {name}")

    def run_awaiting_connect(self, name: str) -> None:
        # kith_db_create returns before the async connects complete; the
        # first exec races the connection readiness and returns
        # -KITH_ECONNRESET until a connection is live. Retry until the
        # window lapses, surfacing any other error at once.
        deadline = time.monotonic() + _CONNECT_WAIT_S
        while True:
            try:
                self.run(name)
                return
            except KithError as exc:
                if exc.code != gen_types.kith_error.KITH_ECONNRESET:
                    raise
                if time.monotonic() >= deadline:
                    raise TimeoutError(f"postgres never became ready: {name}") from exc
                time.sleep(0.05)

    def _on_reply(self, reply_ptr: object) -> None:
        self._status = int(self._lib.kith_db_reply_status(reply_ptr))
        self._event.set()


class _KeepAlive:
    """Recurring timer that keeps a db-pool-only reactor from exiting.

    ``kith_reactor_run`` returns after one idle iteration when no fds,
    timers, or tasks are registered (reactor.h documents this). A db
    connection deregisters its socket when it goes idle between queries
    (conn.c: ``conn_on_connect_ok`` calls ``conn_register(c, 0u)``), so
    a reactor backing only a db pool drops to zero fds between queries
    and ``run`` exits. This timer re-schedules itself on every fire,
    keeping ``timer_count > 0`` so the reactor blocks indefinitely
    between query bursts and delivers every reply callback. The timer
    is freed when ``kith_reactor_destroy`` drains the timer wheel.
    """

    __slots__ = ("_cb", "_lib", "_r")

    def __init__(self, reactor_lib: ctypes.CDLL, reactor: object) -> None:
        self._lib = reactor_lib
        self._r = reactor
        self._cb = gen_reactor.kith_reactor_task_cb(self._tick)

    def arm(self) -> None:
        now = int(self._lib.kith_reactor_now_ms(self._r))
        rc = int(
            self._lib.kith_reactor_schedule(
                self._r,
                ctypes.c_uint64(now + _KEEPALIVE_INTERVAL_MS),
                self._cb,
                ctypes.c_void_p(0),
            )
        )
        check_error(rc, "kith_reactor_schedule (keepalive)")

    def _tick(self, _ctx: object) -> None:
        now = int(self._lib.kith_reactor_now_ms(self._r))
        rc = int(
            self._lib.kith_reactor_schedule(
                self._r,
                ctypes.c_uint64(now + _KEEPALIVE_INTERVAL_MS),
                self._cb,
                ctypes.c_void_p(0),
            )
        )
        # A scheduling failure here leaves the reactor to go idle and
        # the next db exec to time out; surface it rather than swallow.
        check_error(rc, "kith_reactor_schedule (keepalive re-arm)")


def _register_ddl_queries(db_handle: object, db_lib: ctypes.CDLL) -> None:
    for name, sql in ((_SETUP_NAME, _SETUP_SQL), (_TRUNCATE_NAME, _TRUNCATE_SQL)):
        rc = int(
            db_lib.kith_db_register_query(
                db_handle,
                name.encode("utf-8"),
                sql.encode("utf-8"),
                ctypes.c_uint32(0),
            )
        )
        check_error(rc, f"kith_db_register_query: name={name!r}")


@contextlib.contextmanager
def _pg_stack_resources(
    bridge: Bridge,
    cfg: dict[str, str | int] | None = None,
    *,
    provision: bool = True,
) -> Iterator[_PgStack]:
    """Acquire a reactor, db pool, and pump thread; release in contract order.

    Everything after ``kith_reactor_create`` runs inside one ``try``, so an
    exception at any acquisition step — pool creation, thread start, query
    registration, the schema DDL — still releases what was acquired before
    propagating.

    Release order follows the C lifecycle contracts. ``kith_reactor_stop``
    is the only teardown entry that is safe against a running loop
    (atomic flag + wakeup); the loop's reply callbacks and its socket
    deregistrations through ``kith_reactor_del`` all execute on the reactor
    thread, so joining that thread is the barrier that leaves no callback
    in flight. Only then may ``kith_db_destroy`` walk the connections
    (it calls ``kith_reactor_del`` from the calling thread) and
    ``kith_reactor_destroy`` free the loop itself. Destroying either
    handle earlier races a live poller.

    A thread that ignores stop keeps both handles allocated — a bounded
    leak, reported as a :class:`ResourceWarning`, instead of destroying
    handles the loop is still using.

    Args:
        bridge: Loaded library bridge supplying the reactor and db libs.
        cfg: Connection settings as produced by :func:`_pg_config`, or
            ``None`` to read them from the environment.
        provision: Register the named queries and issue the schema DDL
            after the pump starts; release-only callers pass ``False``.

    Yields:
        The acquired stack.

    Raises:
        KithError: When handle creation or query registration fails.
        Exception: Whatever raised during acquisition or in the caller's
            body, after release completes.
    """
    reactor_lib = bridge.lib("reactor")
    db_lib = bridge.lib("db")

    r_params = gen_reactor.kith_reactor_params_t(
        size=ctypes.sizeof(gen_reactor.kith_reactor_params_t),
        abi_version=gen_version.KITH_ABI_VERSION,
        max_fds=64,
        task_capacity=64,
    )
    reactor = ctypes.POINTER(gen_reactor.kith_reactor_t)()
    rc = int(
        reactor_lib.kith_reactor_create(
            ctypes.byref(r_params),
            None,
            ctypes.byref(reactor),
        )
    )
    check_error(rc, "kith_reactor_create")

    conn_cfg = _pg_config() if cfg is None else cfg
    db_handle: object = None
    reactor_thread: threading.Thread | None = None
    try:
        # Create the db pool before starting the reactor thread so the
        # connection sockets register with the backend before run polls.
        # conn_start (called inside kith_db_create) drives PQconnectPoll and
        # calls kith_reactor_add; the readiness callbacks fire once run
        # starts pumping.
        d_params = gen_db.kith_db_params_t(
            size=ctypes.sizeof(gen_db.kith_db_params_t),
            abi_version=gen_version.KITH_ABI_VERSION,
            host=str(conn_cfg["host"]).encode("utf-8"),
            port=int(conn_cfg["port"]),
            db_name=str(conn_cfg["db"]).encode("utf-8"),
            user=str(conn_cfg["user"]).encode("utf-8"),
            password=str(conn_cfg["password"]).encode("utf-8") if conn_cfg["password"] else None,
        )
        created = ctypes.POINTER(gen_db.kith_db_t)()
        rc = int(
            db_lib.kith_db_create(
                ctypes.byref(d_params),
                reactor,
                None,
                ctypes.byref(created),
            )
        )
        check_error(rc, "kith_db_create")
        db_handle = created

        keepalive = _KeepAlive(reactor_lib, reactor)
        keepalive.arm()

        pump = threading.Thread(
            target=reactor_lib.kith_reactor_run,
            args=(reactor,),
            name=_REACTOR_THREAD_NAME,
            daemon=True,
        )
        pump.start()
        reactor_thread = pump

        if provision:
            register_queries_on_db(db_handle, db_lib)
            _register_ddl_queries(db_handle, db_lib)

            ddl = _OneShotExec(db_lib, db_handle)
            ddl.run_awaiting_connect(_SETUP_NAME)
            ddl.run(_TRUNCATE_NAME)

        yield _PgStack(db_lib=db_lib, db_handle=db_handle)
    finally:
        reactor_lib.kith_reactor_stop(reactor)
        stuck = False
        if reactor_thread is not None:
            reactor_thread.join(timeout=_TEARDOWN_JOIN_S)
            stuck = reactor_thread.is_alive()
        if stuck:
            warnings.warn(
                f"{_REACTOR_THREAD_NAME} ignored stop; leaving the reactor "
                "and pool allocated rather than destroying live handles",
                ResourceWarning,
                stacklevel=2,
            )
        else:
            if db_handle is not None:
                db_lib.kith_db_destroy(db_handle)
            reactor_lib.kith_reactor_destroy(reactor)


@pytest.fixture
def pg_stack(bridge: Bridge) -> Iterator[_PgStack]:
    """Live Postgres stack wired to the environment's server."""
    with _pg_stack_resources(bridge) as stack:
        yield stack


@needs_build
class TestPgStackTeardownContract:
    """Acquisition failure still releases every C resource the stack took."""

    @staticmethod
    def _install_spies(
        bridge: Bridge,
        monkeypatch: pytest.MonkeyPatch,
    ) -> list[str]:
        """Record the order of lifecycle calls on the bridge's libraries."""
        events: list[str] = []

        reactor_lib = bridge.lib("reactor")
        db_lib = bridge.lib("db")
        real_stop = reactor_lib.kith_reactor_stop
        real_reactor_destroy = reactor_lib.kith_reactor_destroy
        real_db_destroy = db_lib.kith_db_destroy

        def spy_stop(handle: object) -> None:
            events.append("reactor_stop")
            real_stop(handle)

        def spy_reactor_destroy(handle: object) -> None:
            events.append("reactor_destroy")
            real_reactor_destroy(handle)

        def spy_db_destroy(handle: object) -> None:
            events.append("db_destroy")
            real_db_destroy(handle)

        monkeypatch.setattr(reactor_lib, "kith_reactor_stop", spy_stop)
        monkeypatch.setattr(reactor_lib, "kith_reactor_destroy", spy_reactor_destroy)
        monkeypatch.setattr(db_lib, "kith_db_destroy", spy_db_destroy)
        return events

    def _assert_thread_gone(self) -> None:
        assert not any(t.name == _REACTOR_THREAD_NAME for t in threading.enumerate())

    def test_teardown_runs_when_body_raises(
        self,
        bridge: Bridge,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        events = self._install_spies(bridge, monkeypatch)
        closed_port: dict[str, str | int] = dict(_pg_config()) | {"port": 1}
        threads_before = threading.active_count()
        with (
            pytest.raises(RuntimeError, match="acquisition aborted"),
            _pg_stack_resources(bridge, cfg=closed_port, provision=False),
        ):
            raise RuntimeError("acquisition aborted")
        assert threading.active_count() == threads_before
        self._assert_thread_gone()
        assert events == ["reactor_stop", "db_destroy", "reactor_destroy"]

    def test_teardown_runs_when_pool_creation_fails(
        self,
        bridge: Bridge,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        events = self._install_spies(bridge, monkeypatch)
        bad_port: dict[str, str | int] = {
            "host": "127.0.0.1",
            "port": "not-a-number",
            "db": "kith_example",
            "user": "kith",
            "password": "",
        }
        with pytest.raises(ValueError), _pg_stack_resources(bridge, cfg=bad_port):
            pass
        self._assert_thread_gone()
        assert events == ["reactor_stop", "reactor_destroy"]


@needs_build
@needs_pg
class TestPostgresStoreLive:
    def test_get_miss_returns_none(self, pg_stack: _PgStack) -> None:
        async def run() -> bytes | None:
            s = store_from_db_handle(pg_stack.db_handle, pg_stack.db_lib)
            return await s.get(b"absent")

        assert asyncio.run(run()) is None

    def test_put_then_get_round_trips(self, pg_stack: _PgStack) -> None:
        async def run() -> bytes | None:
            s = store_from_db_handle(pg_stack.db_handle, pg_stack.db_lib)
            await s.put(b"k", b"V")
            return await s.get(b"k")

        assert asyncio.run(run()) == b"V"

    def test_put_overwrites_via_on_conflict(self, pg_stack: _PgStack) -> None:
        async def run() -> bytes | None:
            s = store_from_db_handle(pg_stack.db_handle, pg_stack.db_lib)
            await s.put(b"k", b"V1")
            await s.put(b"k", b"V2")
            return await s.get(b"k")

        assert asyncio.run(run()) == b"V2"

    def test_delete_removes_key(self, pg_stack: _PgStack) -> None:
        async def run() -> bytes | None:
            s = store_from_db_handle(pg_stack.db_handle, pg_stack.db_lib)
            await s.put(b"k", b"V")
            await s.delete(b"k")
            return await s.get(b"k")

        assert asyncio.run(run()) is None

    def test_actor_blob_hex_round_trip(self, pg_stack: _PgStack) -> None:
        actor = Actor(
            id=42,
            pos_x=1 << 16,
            pos_y=-(2 << 16),
            pos_z=0,
            vel_x=3,
            vel_y=-3,
            vel_z=7,
            input_tick=99,
            flags=1,
        )

        async def run() -> Actor:
            s = store_from_db_handle(pg_stack.db_handle, pg_stack.db_lib)
            blob = encode_actor_blob(actor)
            key = b"actor:42"
            value = blob.hex().encode("utf-8")
            await s.put(key, value)
            fetched = await s.get(key)
            assert fetched is not None
            return decode_actor_blob(bytes.fromhex(fetched.decode("utf-8")))

        assert asyncio.run(run()) == actor
