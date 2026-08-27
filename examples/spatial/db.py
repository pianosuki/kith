"""Game-state persistence for the spatial game library.

Two backings sit behind the generic
GameStateStore interface so the same game logic runs with or without a
running Postgres: an in-memory store (the default, zero-dependency
startup) and a Postgres backing registered on the server's borrowed
persistence pool. The spatial library is one-actor-per-account, so the
typed view over the store keys actor state by principal id; a game that
owns a roster of characters would key it by character id instead. The
store itself is generic game state and carries no game vocabulary.

The Postgres backing splits query text from submission: named queries
are registered up front through the facade's
~kith.Server.register_query (which targets the db plane's
``kith_db_register_query`` registry), and async helpers issue a
registered query by name through ``kith_db_exec``. The reply callback
fires on the reactor thread; each in-flight query carries a monotonic id
in the reply's ``user_data`` field, and the store correlates the reply
back to the issuing caller's future by that id. The id is the
session-correlation handle: a handler that issues a query on behalf of a
session associates the id with that session, and the awaited future
resumes the handler's continuation on that session when the reply lands.

The Postgres backing is opt-in: when no persistence pool is configured on
the server, register_queries raises ``KithStateError`` and the
game boots against the in-memory default instead. The backing reaches the
borrowed db handle and the framework's generated ctypes db bindings on its
exec path: the byte-key store is the examples-tier interface, while the
package-tier relational surface — raw queries, result rows, transaction
contexts — is the ``kith.db`` module. The registration path uses the
public facade method, the exec path uses the raw bindings on the borrowed
handle.

An out-of-band path is available for a pool the composition root creates
independently of the server (the server does not expose its reactor for an
external pool to borrow): register_queries_on_db registers the
catalog directly on a db handle, and store_from_db_handle wraps the
handle in a PostgresStore. The two paths share the same
QUERIES catalog and the same _BorrowedDbPlane exec
surface; the difference is whether the pool is wired into the server
(facade path) or created alongside it (out-of-band path).
"""

from __future__ import annotations

import asyncio
import ctypes
import struct
import threading
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Protocol

from examples._common.store import GameStateStore

from kith import Actor, KithError
from kith._generated import db as gen_db
from kith.exceptions import check_error, code_for


if TYPE_CHECKING:
    from kith import Server


__all__ = [
    "ACTOR_BLOB_LEN",
    "ActorStore",
    "PostgresStore",
    "decode_actor_blob",
    "encode_actor_blob",
    "register_queries",
    "register_queries_on_db",
    "store_from_db_handle",
]


# ---------------------------------------------------------------------------
# named queries (generic game_state(key, value) table)
# ---------------------------------------------------------------------------

# A single generic key/value table holds every game-state blob the store
# persists. The table is game-owned (the game creates it out of band); the
# queries here are the named entries the server registers on its db pool.
STATE_GET_NAME: str = "state_get"
STATE_PUT_NAME: str = "state_put"
STATE_DELETE_NAME: str = "state_delete"

_STATE_GET_SQL: str = "SELECT value FROM game_state WHERE key = $1"
_STATE_PUT_SQL: str = (
    "INSERT INTO game_state (key, value) VALUES ($1, $2) "
    "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value"
)
_STATE_DELETE_SQL: str = "DELETE FROM game_state WHERE key = $1"

# Ordered (name, sql, n_params) registry: the single source of the backing's
# named-query surface. register_queries iterates it so the registry and the
# constants cannot drift apart.
QUERIES: tuple[tuple[str, str, int], ...] = (
    (STATE_GET_NAME, _STATE_GET_SQL, 1),
    (STATE_PUT_NAME, _STATE_PUT_SQL, 2),
    (STATE_DELETE_NAME, _STATE_DELETE_SQL, 1),
)


def register_queries(server: Server) -> None:
    """Register the named game-state queries on the server's db pool.

    Targets the db plane's query registry through the facade's
    ~kith.Server.register_query (which calls
    ``kith_db_register_query`` on the borrowed db handle). Call this before
    run so the async helpers can issue the queries by name
    during the run loop.

    Raises:
        KithStateError: When no persistence pool is configured on this
            server (the borrowed db handle is NULL). This is the opt-in
            signal: a server without a pool boots against the in-memory
            default instead of calling this.
        KithError: On a registration failure.
    """
    for name, sql, n_params in QUERIES:
        server.register_query(name, sql, n_params)


def register_queries_on_db(db_handle: object, db_lib: ctypes.CDLL) -> None:
    """Register the named game-state queries directly on a db pool handle.

    Targets the db plane's query registry through ``kith_db_register_query``
    on the given handle, bypassing the facade. Use this for the out-of-band
    persistence path: a pool created independently of the server (the server
    does not expose its reactor for an external pool to borrow) is registered
    directly, and a store_from_db_handle store issues queries through
    it. Call this before run so the async helpers can issue the
    queries by name during the run loop.

    Args:
        db_handle: The db pool handle (a ``kith_db_t*`` pointer wrapper).
        db_lib: The framework's db shared library (the ctypes CDLL the bridge
            returns for ``"db"``); carries ``kith_db_register_query``.

    Raises:
        KithError: On a registration failure.
    """
    for name, sql, n_params in QUERIES:
        rc = int(
            db_lib.kith_db_register_query(
                db_handle,
                name.encode("utf-8"),
                sql.encode("utf-8"),
                ctypes.c_uint32(n_params),
            )
        )
        check_error(rc, f"kith_db_register_query: name={name!r}")


# ---------------------------------------------------------------------------
# actor blob codec
# ---------------------------------------------------------------------------

# Actor state is a fixed-layout 68-byte blob: 7 signed 64-bit fields (id +
# 3 position + 3 velocity, Q16.16 fixed-point) followed by three unsigned
# 32-bit fields (input_tick, flags, update_seq). The codec is the game's
# concern, not the framework's; the store treats it as opaque bytes.
_ACTOR_FMT: str = "<qqqqqqqIII"
ACTOR_BLOB_LEN: int = struct.calcsize(_ACTOR_FMT)


def encode_actor_blob(actor: Actor) -> bytes:
    """Serialize an ~kith.Actor into a fixed-layout byte blob."""
    return struct.pack(
        _ACTOR_FMT,
        actor.id,
        actor.pos_x,
        actor.pos_y,
        actor.pos_z,
        actor.vel_x,
        actor.vel_y,
        actor.vel_z,
        actor.input_tick,
        actor.flags,
        actor.update_seq,
    )


def decode_actor_blob(blob: bytes) -> Actor:
    """Deserialize an ~kith.Actor from a byte blob.

    Raises:
        ValueError: When the blob is shorter than the fixed layout.
    """
    if len(blob) < ACTOR_BLOB_LEN:
        raise ValueError(f"actor blob too short: {len(blob)} < {ACTOR_BLOB_LEN}")
    (
        actor_id,
        pos_x,
        pos_y,
        pos_z,
        vel_x,
        vel_y,
        vel_z,
        input_tick,
        flags,
        update_seq,
    ) = struct.unpack(_ACTOR_FMT, blob[:ACTOR_BLOB_LEN])
    return Actor(
        id=actor_id,
        pos_x=pos_x,
        pos_y=pos_y,
        pos_z=pos_z,
        vel_x=vel_x,
        vel_y=vel_y,
        vel_z=vel_z,
        input_tick=input_tick,
        flags=flags,
        update_seq=update_seq,
    )


# ---------------------------------------------------------------------------
# db plane surface (Protocol satisfied by the ctypes adapter and fakes)
# ---------------------------------------------------------------------------


class _Reply(Protocol):
    """A query reply delivered to the completion callback.

    The ``user_data()`` value is the correlation handle the caller supplied
    to ``exec``; the store uses it to route the reply to the issuing
    future. ``value()`` is valid only during the callback (the underlying
    storage is freed after the callback returns), so the store copies the
    bytes out before the callback returns.
    """

    def status(self) -> int: ...

    def n_rows(self) -> int: ...

    def n_cols(self) -> int: ...

    def user_data(self) -> int: ...

    def value(self, row: int, col: int) -> bytes | None: ...


class _DbPlane(Protocol):
    """Async exec surface over a registered-query db pool.

    ``exec`` submits a registered query by name and returns the C error
    code (0 = submitted, negative = submission failed and no callback
    fires). The completion callback fires once, on the reactor thread,
    with the reply whose ``user_data()`` echoes the caller-supplied handle.
    """

    def exec(
        self,
        name: str,
        params: list[bytes | None],
        on_reply: Callable[[_Reply], None],
        user_data: int,
    ) -> int: ...


class _BorrowedReply:
    """Adapter exposing a raw ``kith_db_reply_t`` pointer as a _Reply."""

    __slots__ = ("_lib", "_ptr")

    def __init__(self, ptr: Any, lib: ctypes.CDLL) -> None:
        self._ptr = ptr
        self._lib = lib

    def status(self) -> int:
        return int(self._lib.kith_db_reply_status(self._ptr))

    def n_rows(self) -> int:
        return int(self._lib.kith_db_reply_n_rows(self._ptr))

    def n_cols(self) -> int:
        return int(self._lib.kith_db_reply_n_cols(self._ptr))

    def user_data(self) -> int:
        ud = self._lib.kith_db_reply_user_data(self._ptr)
        return int(ud) if ud else 0

    def value(self, row: int, col: int) -> bytes | None:
        length = ctypes.c_size_t(0)
        cval = self._lib.kith_db_reply_value(
            self._ptr,
            ctypes.c_uint32(row),
            ctypes.c_uint32(col),
            ctypes.byref(length),
        )
        if not cval:
            return None
        return ctypes.string_at(cval, int(length.value))


class _BorrowedDbPlane:
    """Real _DbPlane over the borrowed db handle and the db library.

    Wraps ``kith_db_exec`` on the borrowed handle. The completion callback
    is a single ``kith_db_reply_fn`` trampoline kept alive for the plane's
    lifetime; it builds a _BorrowedReply adapter around the raw
    reply pointer and forwards it to the store's correlation handler.

    Args:
        db_lib: The framework's db shared library (the ctypes CDLL the
            bridge returns for ``"db"``); carries ``kith_db_exec`` and the
            reply accessors.
        db_handle: The borrowed db pool handle from
            ~kith.Server._borrowed_db. Must be non-NULL.
    """

    __slots__ = ("_c_cb", "_db", "_lib", "_on_reply")

    def __init__(self, db_lib: ctypes.CDLL, db_handle: object) -> None:
        self._db = db_handle
        self._lib = db_lib
        self._on_reply: Callable[[_Reply], None] | None = None
        self._c_cb: object = None

    def exec(
        self,
        name: str,
        params: list[bytes | None],
        on_reply: Callable[[_Reply], None],
        user_data: int,
    ) -> int:
        if self._on_reply is None:
            self._on_reply = on_reply
            self._c_cb = gen_db.kith_db_reply_fn(self._trampoline)
        name_b = name.encode("utf-8")
        n = len(params)
        arr = (ctypes.c_char_p * n)(*params) if n else None
        rc = int(
            self._lib.kith_db_exec(
                self._db,
                name_b,
                arr,
                ctypes.c_uint32(n),
                self._c_cb,
                ctypes.c_void_p(user_data),
            )
        )
        return rc

    def _trampoline(self, reply_ptr: Any) -> None:
        reply = _BorrowedReply(reply_ptr, self._lib)
        assert self._on_reply is not None
        self._on_reply(reply)


# ---------------------------------------------------------------------------
# postgres backing
# ---------------------------------------------------------------------------


def _safe_set_result(fut: asyncio.Future[bytes | None], value: bytes | None) -> None:
    if not fut.done():
        fut.set_result(value)


def _safe_set_exception(fut: asyncio.Future[bytes | None], exc: BaseException) -> None:
    if not fut.done():
        fut.set_exception(exc)


class PostgresStore:
    """Postgres-backed GameStateStore over a registered-query pool.

    Each async method allocates a monotonic correlation id, creates a
    future on the running loop, submits the registered query through the
    db plane with the id as ``user_data``, and awaits the future. The
    plane's completion callback fires on the reactor thread, copies the
    reply's value bytes out during the callback (the storage is freed
    after the callback returns), and resolves the future on the loop
    thread via call_soon_threadsafe.

    The correlation id is the session handle: a caller that issues a
    query on behalf of a session associates the id with that session, and
    the awaited future resumes the caller's continuation on that session
    when the reply lands. The store itself holds no session state; the
    association lives in the caller.

    Args:
        plane: The db plane (real _BorrowedDbPlane or a fake).
        loop: The event loop the async methods run on. When ``None`` the
            store captures the running loop on the first issue.
    """

    __slots__ = ("_lock", "_loop", "_next_id", "_pending", "_plane")

    def __init__(self, plane: _DbPlane, *, loop: asyncio.AbstractEventLoop | None = None) -> None:
        self._plane = plane
        self._loop: asyncio.AbstractEventLoop | None = loop
        # correlation id -> (future, expect_value). expect_value is True for
        # state_get (resolve the row value) and False for state_put/delete
        # (resolve None on success). Guarded by _lock because the reactor
        # thread resolves entries the worker thread registered.
        self._pending: dict[int, tuple[asyncio.Future[bytes | None], bool]] = {}
        self._next_id = 1
        self._lock = threading.Lock()

    async def get(self, key: bytes) -> bytes | None:
        return await self._issue(STATE_GET_NAME, [key], expect_value=True)

    async def put(self, key: bytes, value: bytes) -> None:
        await self._issue(STATE_PUT_NAME, [key, value], expect_value=False)

    async def delete(self, key: bytes) -> None:
        await self._issue(STATE_DELETE_NAME, [key], expect_value=False)

    def _issue(
        self,
        name: str,
        params: list[bytes | None],
        *,
        expect_value: bool,
    ) -> asyncio.Future[bytes | None]:
        loop = self._loop
        if loop is None:
            loop = asyncio.get_running_loop()
            self._loop = loop
        fut: asyncio.Future[bytes | None] = loop.create_future()
        with self._lock:
            rid = self._next_id
            self._next_id += 1
            self._pending[rid] = (fut, expect_value)
        rc = self._plane.exec(name, params, self._on_reply, rid)
        if rc != 0:
            with self._lock:
                self._pending.pop(rid, None)
            code = code_for(rc)
            _safe_set_exception(fut, KithError(code, f"db exec: {name}"))
        return fut

    def _on_reply(self, reply: _Reply) -> None:
        rid = reply.user_data()
        with self._lock:
            entry = self._pending.pop(rid, None)
        if entry is None:
            return
        fut, expect_value = entry
        if fut.done():
            return
        loop = self._loop
        assert loop is not None
        status = reply.status()
        if status != 0:
            code = code_for(status)
            exc = KithError(code, f"db query failed: status={status}")
            loop.call_soon_threadsafe(_safe_set_exception, fut, exc)
            return
        if expect_value and reply.n_rows() > 0:
            value = reply.value(0, 0)
            loop.call_soon_threadsafe(_safe_set_result, fut, value)
        else:
            loop.call_soon_threadsafe(_safe_set_result, fut, None)


def store_from_db_handle(
    db_handle: object,
    db_lib: ctypes.CDLL,
    *,
    loop: asyncio.AbstractEventLoop | None = None,
) -> PostgresStore:
    """Build a PostgresStore over a borrowed db pool handle.

    Wraps the handle in a _BorrowedDbPlane (the real db plane over
    ``kith_db_exec``) and constructs a PostgresStore on it. Use this
    for the out-of-band persistence path: a pool created independently of the
    server is wrapped here, and the resulting store issues registered queries
    through it. Pair with register_queries_on_db to register the
    named queries on the same handle before issuing them.

    Args:
        db_handle: The db pool handle (a ``kith_db_t*`` pointer wrapper).
        db_lib: The framework's db shared library (the ctypes CDLL the bridge
            returns for ``"db"``); carries ``kith_db_exec`` and the reply
            accessors.
        loop: The event loop the async methods run on. When ``None`` the
            store captures the running loop on the first issue.

    Returns:
        The PostgresStore backed by the borrowed handle.
    """
    plane = _BorrowedDbPlane(db_lib, db_handle)
    return PostgresStore(plane, loop=loop)


# ---------------------------------------------------------------------------
# typed actor-state view
# ---------------------------------------------------------------------------


class ActorStore:
    """Typed actor-state view over any GameStateStore.

    Keys actor state by principal id (one actor per account) and
    serializes an ~kith.Actor through the fixed-layout blob codec
    above. The same interface backs the in-memory default and the Postgres
    opt-in, so a caller swaps backings without touching the typed surface
    (dependency injection): the in-memory store boots with no external
    service, the Postgres store survives restarts.

    Args:
        store: The backing game-state store (in-memory or Postgres).
    """

    __slots__ = ("_store",)

    def __init__(self, store: GameStateStore) -> None:
        self._store = store

    async def load_actor(self, principal_id: int) -> Actor | None:
        """Return the saved actor for ``principal_id``, or ``None`` if absent."""
        blob = await self._store.get(self._key(principal_id))
        if blob is None:
            return None
        return decode_actor_blob(blob)

    async def save_actor(self, principal_id: int, actor: Actor) -> None:
        """Persist ``actor`` under ``principal_id``, replacing any prior state."""
        await self._store.put(self._key(principal_id), encode_actor_blob(actor))

    async def delete_actor(self, principal_id: int) -> None:
        """Remove the saved actor for ``principal_id``; absence is not an error."""
        await self._store.delete(self._key(principal_id))

    @staticmethod
    def _key(principal_id: int) -> bytes:
        return struct.pack("<Q", principal_id)
