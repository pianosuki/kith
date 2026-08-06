"""The persistence plane's Python surface: async queries and transactions.

The module wraps the db plane's pool (a libpq-backed Postgres connection
pool driven by a dedicated reactor) with the ergonomics game code wants
for relational state: a :class:`Database` that owns the pool lifecycle,
an async :meth:`Database.query` returning result rows as values, and
:class:`Transaction` — an async context manager whose block commits
before it exits (the commit-before-acknowledge ordering: code after the
block, including a facade send, observes the committed state).

The byte-key/value game-state store lives with the examples; this module
is the durable relational tier over the framework's own db module.

Replies arrive on the pool's reactor thread; each submission carries a
monotonic correlation id, and the callback resolves the awaiting
coroutine's future on the asyncio loop via ``call_soon_threadsafe``.
Coroutines are driven by a running loop — the game's own loop thread,
the pattern the postgres example wires — not by the facade's synchronous
handlers.
"""

from __future__ import annotations

import asyncio
import contextlib
import ctypes
import threading
from collections.abc import Sequence
from types import TracebackType
from typing import cast

from kith._bridge import load as load_bridge
from kith._generated import configure as configure_all
from kith._generated import db as gen_db
from kith._generated import reactor as gen_reactor
from kith._generated import types as gen_types
from kith._generated import version as gen_version
from kith.exceptions import (
    KithError,
    KithNetworkError,
    KithStateError,
    check_error,
    code_for,
    exception_for,
)


__all__ = ["Database", "Reply", "Transaction"]


_KEEPALIVE_INTERVAL_MS = 1000
_JOIN_TIMEOUT_S = 5.0

# The message every run-loop-death failure carries, so a caller sees one
# failure identity whether the submission was awaiting at the death or
# arrived after it.
_RUN_LOOP_FAILURE = "db reactor run loop failed"


def _resolve(fut: asyncio.Future[object], value: object) -> None:
    if not fut.done():
        fut.set_result(value)


def _fail(fut: asyncio.Future[object], exc: KithError) -> None:
    if not fut.done():
        fut.set_exception(exc)


def _encode_params(params: Sequence[str | int | None]) -> list[bytes | None]:
    """Encode caller parameters into the C layer's text transport.

    ``None`` becomes SQL NULL; integers become their decimal text;
    booleans their SQL literal; strings ride as UTF-8. Anything else is a
    type error at the boundary — binary transport is not part of the
    surface.
    """
    encoded: list[bytes | None] = []
    for param in params:
        if param is None:
            encoded.append(None)
        elif isinstance(param, str):
            encoded.append(param.encode("utf-8"))
        elif isinstance(param, bool):
            encoded.append(b"true" if param else b"false")
        elif isinstance(param, int):
            encoded.append(str(param).encode("utf-8"))
        else:
            raise TypeError(
                f"unsupported parameter type {type(param).__name__}: use str, int, bool, or None"
            )
    return encoded


class Reply:
    """A snapshot of one query's result set.

    All cells are copied from the reactor thread's reply storage before
    the snapshot exists, so the object outlives the callback freely.

    Cell values are text (the db module's parameter and result transport
    is text): ``None`` denotes SQL NULL.
    """

    __slots__ = ("_rows",)

    def __init__(self, rows: tuple[tuple[str | None, ...], ...]) -> None:
        self._rows = rows

    @property
    def rows(self) -> tuple[tuple[str | None, ...], ...]:
        """The result set as row tuples of ``str | None`` cells."""
        return self._rows

    @property
    def n_rows(self) -> int:
        """The number of rows in the result set."""
        return len(self._rows)

    @property
    def n_cols(self) -> int:
        """The number of columns per row (0 for an empty result)."""
        return len(self._rows[0]) if self._rows else 0

    def value(self, row: int, col: int) -> str | None:
        """Return the cell at ``(row, col)``, or ``None`` for SQL NULL.

        Args:
            row: The row index into the result set.
            col: The column index within the row.
        """
        return self._rows[row][col]


def _snapshot(lib: ctypes.CDLL, reply_ptr: object) -> Reply:
    """Copy the whole result set out of the reply (callback scope only)."""
    n_rows = int(lib.kith_db_reply_n_rows(reply_ptr))
    n_cols = int(lib.kith_db_reply_n_cols(reply_ptr))
    rows: list[tuple[str | None, ...]] = []
    for r in range(n_rows):
        row: list[str | None] = []
        for c in range(n_cols):
            length = ctypes.c_size_t(0)
            cell = lib.kith_db_reply_value(reply_ptr, r, c, ctypes.byref(length))
            row.append(
                None if not cell else ctypes.string_at(cell, int(length.value)).decode("utf-8")
            )
        rows.append(tuple(row))
    return Reply(tuple(rows))


class Database:
    """An async relational surface over one db pool the object owns.

    The database creates its own reactor on a background thread with the
    recurring keepalive the pool needs (a pool-only reactor idles out once
    the libpq sockets deregister, stranding every reply callback), so a
    game wires no persistence composition of its own. Call ``close``
    before interpreter exit; an unclosed database's daemon reactor thread
    dies with the process.

    Args:
        host: Postgres host. ``None`` lets libpq choose its default.
        port: Postgres TCP port. 0 selects the db module's default
            (5432).
        db_name: Database name. ``None`` selects the libpq default (the
            role name).
        user: Role name. ``None`` selects the libpq default (the process
            user).
        password: Role password. ``None`` relies on ``.pgpass`` or peer
            auth. The handle copies it and scrubs it at close.
        min_connections: Connections established eagerly at construction.
            0 selects the db module's default (1).
        max_connections: Pool capacity. 0 selects the db module's default
            (4).
        connect_timeout_ms: Per-attempt connect timeout. 0 selects the db
            module's default (5000).
        loop: The asyncio loop the async methods run on. ``None`` captures
            the running loop on the first await.

    Raises:
        KithError: When the bridge, the reactor, or the pool cannot be
            created.

    Thread safety:
        @thread_safety safe — multiple tasks may await :meth:`query`,
        :meth:`call`, and :meth:`transaction` concurrently. Each
        submission registers its own future under a lock; the db
        reactor thread resolves futures via
        ``loop.call_soon_threadsafe``. All futures bind to one asyncio
        loop — the ``loop`` argument when given, else the first
        awaiting task's running loop — so calling the async methods
        from other threads is the caller's routing problem, solved
        with that loop's cross-thread submission protocol
        (``asyncio.run_coroutine_threadsafe``).
    """

    __slots__ = (
        "_bridge",
        "_close_cb",
        "_closed",
        "_db",
        "_db_lib",
        "_keepalive_cb",
        "_lock",
        "_loop",
        "_next_id",
        "_open_cb",
        "_pending",
        "_reactor",
        "_reactor_lib",
        "_reactor_thread",
        "_reply_cb",
        "_run_dead",
    )

    def __init__(
        self,
        *,
        host: str | None = None,
        port: int = 0,
        db_name: str | None = None,
        user: str | None = None,
        password: str | None = None,
        min_connections: int = 0,
        max_connections: int = 0,
        connect_timeout_ms: int = 0,
        loop: asyncio.AbstractEventLoop | None = None,
    ) -> None:
        self._bridge = load_bridge()
        configure_all(self._bridge)
        self._db_lib = self._bridge.lib("db")
        self._reactor_lib = self._bridge.lib("reactor")
        self._loop = loop
        self._lock = threading.Lock()
        self._pending: dict[int, asyncio.Future[object]] = {}
        self._next_id = 1
        self._closed = False
        # Set when the reactor run loop exits on a C failure; submissions
        # then refuse instead of queueing into a pump that never drains.
        self._run_dead = False
        self._reactor: object | None = None
        self._reactor_thread: threading.Thread | None = None
        self._db: object | None = None
        # ctypes callback keep-alive slots: the C side holds raw function
        # pointers for the registration's lifetime.
        self._reply_cb = gen_db.kith_db_reply_fn(self._on_reply)
        self._open_cb = gen_db.kith_db_session_open_fn(self._on_open)
        self._close_cb = gen_db.kith_db_session_close_fn(self._on_close)

        self._start_reactor()
        try:
            self._create_pool(
                host=host,
                port=port,
                db_name=db_name,
                user=user,
                password=password,
                min_connections=min_connections,
                max_connections=max_connections,
                connect_timeout_ms=connect_timeout_ms,
            )
        except BaseException:
            self._halt_reactor()
            self._shutdown_reactor()
            raise

    # -----------------------------------------------------------------------
    # lifecycle
    # -----------------------------------------------------------------------

    def _start_reactor(self) -> None:
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
        # iteration (nothing else stays registered once the idle libpq
        # sockets deregister) and stranding the pool's reply callbacks.
        self._keepalive_cb: object = gen_reactor.kith_reactor_task_cb(self._keepalive_tick)
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
        # A failed re-arm leaves the reactor to idle out and the next query
        # to strand; surface the failure instead of swallowing it.
        check_error(rc, "kith_reactor_schedule (keepalive re-arm)")

    def _create_pool(
        self,
        *,
        host: str | None,
        port: int,
        db_name: str | None,
        user: str | None,
        password: str | None,
        min_connections: int,
        max_connections: int,
        connect_timeout_ms: int,
    ) -> None:
        params = gen_db.kith_db_params_t(
            size=ctypes.sizeof(gen_db.kith_db_params_t),
            abi_version=gen_version.KITH_ABI_VERSION,
            host=host.encode("utf-8") if host is not None else None,
            port=port,
            db_name=db_name.encode("utf-8") if db_name is not None else None,
            user=user.encode("utf-8") if user is not None else None,
            password=password.encode("utf-8") if password is not None else None,
            min_connections=min_connections,
            max_connections=max_connections,
            connect_timeout_ms=connect_timeout_ms,
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

    def close(self) -> None:
        """Release the pool, its reactor, and the reactor's thread.

        Fails every awaited-but-unresolved submission with
        :class:`KithStateError` first, then halts the reactor, then
        destroys the pool. The order is the db module's own teardown
        discipline: the reactor stops dispatching before the pool
        storage goes away (a queued establishment task would otherwise
        dispatch into freed memory), and the pool's destroy contract —
        no operation in flight, no reply callback running — holds
        because the caller awaited its last queries before closing. An
        open transaction context must be exited before the close.

        Raises:
            KithError: When the pool destroy fails.
        """
        if self._closed or self._db is None:
            return
        self._closed = True
        with self._lock:
            pending = list(self._pending.values())
            self._pending.clear()
        loop = self._loop
        for fut in pending:
            if loop is not None:
                loop.call_soon_threadsafe(
                    _fail,
                    fut,
                    KithStateError(gen_types.kith_error.KITH_ESTATE, "database closed"),
                )
        self._halt_reactor()
        self._db_lib.kith_db_destroy(self._db)
        self._db = None
        self._shutdown_reactor()

    def _halt_reactor(self) -> None:
        if self._reactor_lib is not None and self._reactor is not None:
            self._reactor_lib.kith_reactor_stop(self._reactor)
        if self._reactor_thread is not None:
            self._reactor_thread.join(timeout=_JOIN_TIMEOUT_S)
            self._reactor_thread = None

    def _shutdown_reactor(self) -> None:
        if self._reactor_lib is not None and self._reactor is not None:
            self._reactor_lib.kith_reactor_destroy(self._reactor)
        # The timer node died with the wheel; the callback trampolines can
        # go now that no tick can fire.
        self._keepalive_cb = None
        self._reactor = None

    def _run_reactor(self, lib: ctypes.CDLL, handle: object) -> None:
        rc = int(lib.kith_reactor_run(handle))
        if rc >= 0:
            return
        # A polling or syscall failure killed the run loop; submissions
        # awaiting at the death resolve with the transport family and
        # submissions arriving after it refuse, instead of hanging on a
        # pool that can no longer dispatch.
        with self._lock:
            pending = list(self._pending.values())
            self._pending.clear()
            self._run_dead = True
        loop = self._loop
        for fut in pending:
            if loop is not None:
                loop.call_soon_threadsafe(
                    _fail,
                    fut,
                    KithNetworkError(gen_types.kith_error.KITH_EIO, _RUN_LOOP_FAILURE),
                )

    # -----------------------------------------------------------------------
    # query surface
    # -----------------------------------------------------------------------

    def register_query(self, name: str, sql: str, n_params: int = 0) -> None:
        """Register a named parameterized query on the pool's registry.

        Args:
            name: Query name, unique across registrations.
            sql: PostgreSQL parameterized command using ``$1, $2, ...``
                positional placeholders.
            n_params: Number of positional placeholders in ``sql``.

        Raises:
            KithStateError: When the name is already registered or the
                database is closed.
            KithError: On a registration failure.

        Thread safety:
            @thread_safety unsafe — concurrent ``call`` submissions and
            other registrations race the registry; register before the
            first await.
        """
        if self._closed or self._db is None:
            raise KithStateError(gen_types.kith_error.KITH_ESTATE, "database closed")
        rc = int(
            self._db_lib.kith_db_register_query(
                self._db,
                name.encode("utf-8"),
                sql.encode("utf-8"),
                ctypes.c_uint32(n_params),
            )
        )
        check_error(rc, f"kith_db_register_query: name={name!r}")

    def transaction(self) -> Transaction:
        """Open a transaction context: one pinned connection, one bracket.

        The context opens a session, issues BEGIN, and on a clean exit
        resolves COMMIT before the block exits; an exception rolls back
        and re-raises. Statements ride ``Transaction.query``.
        """
        return Transaction(self)

    async def query(self, sql: str, params: Sequence[str | int | None] = ()) -> Reply:
        """Run one raw SQL statement on a pooled connection (autocommit).

        Args:
            sql: A single SQL statement. Multi-statement strings are
                rejected by the wire protocol; parameterize with
                ``$1, $2, ...`` and pass ``params``.
            params: Text-transport parameters: ``str``, ``int``,
                ``bool``, or ``None`` (SQL NULL). Anything else is a
                ``TypeError`` at the boundary.

        Returns:
            The result snapshot.

        Raises:
            KithNetworkError: On a connection loss.
            KithStateError: On a saturated pool or a closed database.
            KithError: On any other failure (a rejected statement, a
                submission that could not be queued).
        """
        if self._closed or self._db is None:
            raise KithStateError(gen_types.kith_error.KITH_ESTATE, "database closed")
        encoded = _encode_params(params)
        arr = (ctypes.c_char_p * len(encoded))(*encoded) if encoded else None
        fut = self._new_future()
        rid = self._register(fut)
        rc = int(
            self._db_lib.kith_db_exec_sql(
                self._db,
                sql.encode("utf-8"),
                arr,
                ctypes.c_uint32(len(encoded)),
                self._reply_cb,
                ctypes.c_void_p(rid),
            )
        )
        if rc != 0:
            self._deregister(rid)
            raise self._error_for(rc, "db exec sql")
        return cast(Reply, await self._await(fut))

    async def call(self, name: str, params: Sequence[str | int | None] = ()) -> Reply:
        """Run a registered query by name (the registry path).

        Args:
            name: A name registered via :meth:`register_query`.
            params: The same text-transport parameters as :meth:`query`.

        Returns:
            The result snapshot.

        Raises:
            KithNotFoundError: When the name is not registered.
            KithStateError: When the parameter count mismatches, the pool
                is saturated, or the database is closed.
            KithNetworkError: On a connection loss.
            KithError: On any other failure.
        """
        if self._closed or self._db is None:
            raise KithStateError(gen_types.kith_error.KITH_ESTATE, "database closed")
        encoded = _encode_params(params)
        arr = (ctypes.c_char_p * len(encoded))(*encoded) if encoded else None
        fut = self._new_future()
        rid = self._register(fut)
        rc = int(
            self._db_lib.kith_db_exec(
                self._db,
                name.encode("utf-8"),
                arr,
                ctypes.c_uint32(len(encoded)),
                self._reply_cb,
                ctypes.c_void_p(rid),
            )
        )
        if rc != 0:
            self._deregister(rid)
            raise self._error_for(rc, f"db exec: {name}")
        return cast(Reply, await self._await(fut))

    # -----------------------------------------------------------------------
    # session plumbing (Transaction's executor side)
    # -----------------------------------------------------------------------

    async def _session_open(self) -> object:
        """Open a session (pin a connection); returns the session handle."""
        if self._closed or self._db is None:
            raise KithStateError(gen_types.kith_error.KITH_ESTATE, "database closed")
        fut = self._new_future()
        rid = self._register(fut)
        rc = int(self._db_lib.kith_db_session_open(self._db, self._open_cb, ctypes.c_void_p(rid)))
        if rc != 0:
            self._deregister(rid)
            raise self._error_for(rc, "db session open")
        handle = await self._await(fut)
        return handle

    async def _session_query(
        self, session: object, sql: str, params: Sequence[str | int | None]
    ) -> Reply:
        if self._closed or self._db is None:
            raise KithStateError(gen_types.kith_error.KITH_ESTATE, "database closed")
        encoded = _encode_params(params)
        arr = (ctypes.c_char_p * len(encoded))(*encoded) if encoded else None
        fut = self._new_future()
        rid = self._register(fut)
        rc = int(
            self._db_lib.kith_db_session_exec(
                session,
                sql.encode("utf-8"),
                arr,
                ctypes.c_uint32(len(encoded)),
                self._reply_cb,
                ctypes.c_void_p(rid),
            )
        )
        if rc != 0:
            self._deregister(rid)
            raise self._error_for(rc, "db session exec")
        return cast(Reply, await self._await(fut))

    async def _session_close(self, session: object) -> None:
        if self._closed or self._db is None:
            raise KithStateError(gen_types.kith_error.KITH_ESTATE, "database closed")
        fut = self._new_future()
        rid = self._register(fut)
        rc = int(self._db_lib.kith_db_session_close(session, self._close_cb, ctypes.c_void_p(rid)))
        if rc != 0:
            self._deregister(rid)
            raise self._error_for(rc, "db session close")
        await self._await(fut)

    # -----------------------------------------------------------------------
    # correlation machinery
    # -----------------------------------------------------------------------

    def _new_future(self) -> asyncio.Future[object]:
        loop = self._loop
        if loop is None:
            loop = asyncio.get_running_loop()
            self._loop = loop
        return loop.create_future()

    def _register(self, fut: asyncio.Future[object]) -> int:
        with self._lock:
            if self._run_dead:
                raise KithNetworkError(gen_types.kith_error.KITH_EIO, _RUN_LOOP_FAILURE)
            rid = self._next_id
            self._next_id += 1
            self._pending[rid] = fut
        return rid

    def _deregister(self, rid: int) -> None:
        with self._lock:
            self._pending.pop(rid, None)

    async def _await(self, fut: asyncio.Future[object]) -> object:
        return await fut

    def _error_for(self, rc: int, context: str) -> KithError:
        code = code_for(rc)
        return exception_for(code)(code, context)

    def _on_reply(self, reply_ptr: object) -> None:
        raw_rid = self._db_lib.kith_db_reply_user_data(reply_ptr)
        rid = int(raw_rid) if raw_rid else 0
        with self._lock:
            fut = self._pending.pop(rid, None)
        if fut is None:
            return
        status = int(self._db_lib.kith_db_reply_status(reply_ptr))
        loop = self._loop
        assert loop is not None
        if status != 0:
            err = self._db_lib.kith_db_reply_err_str(reply_ptr)
            code = code_for(status)
            text = err.decode("utf-8", "replace").strip() if err else "query failed"
            exc = exception_for(code)(code, f"db query failed: {text}")
            loop.call_soon_threadsafe(_fail, fut, exc)
            return
        loop.call_soon_threadsafe(_resolve, fut, _snapshot(self._db_lib, reply_ptr))

    def _on_open(self, session_ptr: object, status: int, user_data: int | None) -> None:
        rid = user_data or 0
        with self._lock:
            fut = self._pending.pop(rid, None)
        if fut is None:
            return
        loop = self._loop
        assert loop is not None
        if status != 0 or not session_ptr:
            rc = status if status < 0 else -int(gen_types.kith_error.KITH_ESTATE)
            loop.call_soon_threadsafe(_fail, fut, self._error_for(rc, "db session open"))
            return
        loop.call_soon_threadsafe(_resolve, fut, session_ptr)

    def _on_close(self, status: int, user_data: int | None) -> None:
        rid = user_data or 0
        with self._lock:
            fut = self._pending.pop(rid, None)
        if fut is None:
            return
        loop = self._loop
        assert loop is not None
        if status != 0:
            loop.call_soon_threadsafe(_fail, fut, self._error_for(status, "db session close"))
            return
        loop.call_soon_threadsafe(_resolve, fut, None)


class Transaction:
    """One pinned connection running a single transaction bracket.

    The async context manager opens a session, issues BEGIN, and hands
    itself to the block. On a clean exit it resolves COMMIT before the
    context exits — code after the block observes the committed state,
    which is the commit-before-acknowledge ordering when the block's
    result feeds a facade send. On an exception it issues ROLLBACK and
    re-raises; a rollback failure cannot mask the original error and is
    swallowed after the session release.

    Statements ride :meth:`query`, one at a time — the pool serializes a
    session's operations and a second submit while one is in flight
    answers with the saturation error, so awaiting each statement is the
    contract, not an optimization.

    Abandoning the context (a coroutine cancelled while suspended inside
    the block) leaves the session open; its connection stays pinned and
    exclusive, harming capacity but not correctness, until
    :meth:`Database.close` sweeps it.
    """

    __slots__ = ("_database", "_session")

    def __init__(self, database: Database) -> None:
        """Create an unentered transaction for ``database``.

        Args:
            database: The database whose pool the transaction's session
                pins on entry.
        """
        self._database = database
        self._session: object | None = None

    async def query(self, sql: str, params: Sequence[str | int | None] = ()) -> Reply:
        """Run one statement inside the transaction bracket.

        Args:
            sql: A single SQL statement.
            params: The same text-transport parameters as
                :meth:`Database.query`.

        Returns:
            The result snapshot.

        Raises:
            KithStateError: When the context is not active, the session
                is saturated, or the database is closed.
            KithNetworkError: On a connection loss.
            KithError: On any other failure.
        """
        session = self._session
        if session is None:
            raise KithStateError(
                gen_types.kith_error.KITH_ESTATE,
                "transaction is not active (not entered, or exited)",
            )
        return await self._database._session_query(session, sql, params)

    async def __aenter__(self) -> Transaction:
        self._session = await self._database._session_open()
        try:
            await self._database._session_query(self._session, "BEGIN", ())
        except BaseException:
            session = self._session
            self._session = None
            with contextlib.suppress(KithError):
                await self._database._session_close(session)
            raise
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> bool:
        session = self._session
        self._session = None
        if session is None:
            return False
        if exc_type is None:
            try:
                await self._database._session_query(session, "COMMIT", ())
            finally:
                with contextlib.suppress(KithError):
                    await self._database._session_close(session)
            return False
        with contextlib.suppress(KithError):
            await self._database._session_query(session, "ROLLBACK", ())
        with contextlib.suppress(KithError):
            await self._database._session_close(session)
        return False
