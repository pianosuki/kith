"""Tests for the spatial persistence layer and the shared game-state store.

The pure-Python cases cover the in-memory store, the actor blob codec, the
typed :class:`ActorStore` over an in-memory backing, and the Postgres
backing's async exec + reply-correlation logic against a fake db plane
(no build, no Postgres). The build-gated case registers the named queries
on a real :class:`kith.Server` facade and confirms the opt-in signal: with
no persistence pool configured, registration raises ``KithStateError``.
"""

from __future__ import annotations

import asyncio
import threading
from collections.abc import Callable, Iterator

import pytest
from _build_gate import _BUILD_DEBUG, needs_build
from examples._common import store as common_store
from examples.spatial import db

from kith import Actor, KithError, KithStateError, Server
from kith._bridge import reset
from kith._generated import types as gen_types


@pytest.fixture(autouse=True)
def _isolate_bridge(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Pin KITH_LIB to the debug build dir and reset the singleton per test."""
    monkeypatch.setenv("KITH_LIB", str(_BUILD_DEBUG))
    reset()
    yield
    reset()


# ---------------------------------------------------------------------------
# in-memory store (the default backing)
# ---------------------------------------------------------------------------


class TestInMemoryStore:
    def test_put_get_round_trips(self) -> None:
        s = common_store.InMemoryStore()
        assert asyncio.run(s.get(b"k")) is None
        asyncio.run(s.put(b"k", b"v"))
        assert asyncio.run(s.get(b"k")) == b"v"

    def test_put_overwrites(self) -> None:
        s = common_store.InMemoryStore()
        asyncio.run(s.put(b"k", b"v1"))
        asyncio.run(s.put(b"k", b"v2"))
        assert asyncio.run(s.get(b"k")) == b"v2"

    def test_empty_value_is_distinct_from_missing(self) -> None:
        s = common_store.InMemoryStore()
        asyncio.run(s.put(b"k", b""))
        assert asyncio.run(s.get(b"k")) == b""
        assert asyncio.run(s.get(b"absent")) is None

    def test_delete_removes_key(self) -> None:
        s = common_store.InMemoryStore()
        asyncio.run(s.put(b"k", b"v"))
        asyncio.run(s.delete(b"k"))
        assert asyncio.run(s.get(b"k")) is None
        assert len(s) == 0

    def test_delete_absent_is_not_an_error(self) -> None:
        s = common_store.InMemoryStore()
        asyncio.run(s.delete(b"never-set"))  # no raise
        assert len(s) == 0

    def test_concurrent_puts_under_threads(self) -> None:
        s = common_store.InMemoryStore()

        def _writer(start: int) -> None:
            for i in range(start, start + 200):
                asyncio.run(s.put(struct_key(i), struct_key(i)))

        threads = [threading.Thread(target=_writer, args=(b * 200,)) for b in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        # 8 writers * 200 keys, each writer owns a disjoint id range.
        assert len(s) == 8 * 200
        assert asyncio.run(s.get(struct_key(0))) == struct_key(0)
        assert asyncio.run(s.get(struct_key(1599))) == struct_key(1599)


def struct_key(i: int) -> bytes:
    return b"k" + i.to_bytes(4, "little")


# ---------------------------------------------------------------------------
# actor blob codec
# ---------------------------------------------------------------------------


class TestActorBlobCodec:
    def test_round_trips(self) -> None:
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
            update_seq=17,
        )
        out = db.decode_actor_blob(db.encode_actor_blob(actor))
        assert out == actor

    def test_blob_length_is_sixty_eight(self) -> None:
        assert db.ACTOR_BLOB_LEN == 68
        assert len(db.encode_actor_blob(Actor(id=1))) == 68

    def test_decode_truncated_raises(self) -> None:
        with pytest.raises(ValueError):
            db.decode_actor_blob(b"\x00" * (db.ACTOR_BLOB_LEN - 1))

    def test_decode_ignores_trailing_bytes(self) -> None:
        actor = Actor(id=7, pos_x=5)
        blob = db.encode_actor_blob(actor) + b"trailing"
        assert db.decode_actor_blob(blob) == actor


# ---------------------------------------------------------------------------
# typed view over the in-memory backing
# ---------------------------------------------------------------------------


class TestActorStoreInMemory:
    def test_load_unknown_principal_returns_none(self) -> None:
        ts = db.ActorStore(common_store.InMemoryStore())
        assert asyncio.run(ts.load_actor(100)) is None

    def test_save_then_load_round_trips(self) -> None:
        ts = db.ActorStore(common_store.InMemoryStore())
        actor = Actor(id=1, pos_x=1 << 16, pos_y=2 << 16, input_tick=3, flags=1)
        asyncio.run(ts.save_actor(100, actor))
        assert asyncio.run(ts.load_actor(100)) == actor

    def test_save_overwrites(self) -> None:
        ts = db.ActorStore(common_store.InMemoryStore())
        first = Actor(id=1, pos_x=1)
        second = Actor(id=1, pos_x=2 << 16, input_tick=4)
        asyncio.run(ts.save_actor(100, first))
        asyncio.run(ts.save_actor(100, second))
        assert asyncio.run(ts.load_actor(100)) == second

    def test_delete_then_load_returns_none(self) -> None:
        ts = db.ActorStore(common_store.InMemoryStore())
        asyncio.run(ts.save_actor(100, Actor(id=1)))
        asyncio.run(ts.delete_actor(100))
        assert asyncio.run(ts.load_actor(100)) is None

    def test_distinct_principals_are_distinct_keys(self) -> None:
        ts = db.ActorStore(common_store.InMemoryStore())
        asyncio.run(ts.save_actor(1, Actor(id=1, pos_x=10)))
        asyncio.run(ts.save_actor(2, Actor(id=2, pos_x=20)))
        assert asyncio.run(ts.load_actor(1)) == Actor(id=1, pos_x=10)
        assert asyncio.run(ts.load_actor(2)) == Actor(id=2, pos_x=20)


# ---------------------------------------------------------------------------
# postgres backing (async exec + reply correlation; fake db plane)
# ---------------------------------------------------------------------------


class _FakeReply:
    """A reply the fake plane hands to the store's correlation handler."""

    def __init__(
        self,
        *,
        status: int = 0,
        n_rows: int = 0,
        n_cols: int = 1,
        user_data: int = 0,
        value_bytes: bytes | None = None,
    ) -> None:
        self._status = status
        self._n_rows = n_rows
        self._n_cols = n_cols
        self._user_data = user_data
        self._value = value_bytes

    def status(self) -> int:
        return self._status

    def n_rows(self) -> int:
        return self._n_rows

    def n_cols(self) -> int:
        return self._n_cols

    def user_data(self) -> int:
        return self._user_data

    def value(self, row: int, col: int) -> bytes | None:
        del row, col
        return self._value


class _SyncDbPlane:
    """A db plane that fires the completion callback synchronously during exec.

    Backed by an in-memory dict so the store's async get/put/delete resolve
    immediately. Mirrors the real plane's contract: exec returns 0 on
    submission and invokes on_reply exactly once.
    """

    def __init__(self) -> None:
        self.table: dict[bytes, bytes] = {}
        self.exec_calls: list[tuple[str, list[bytes | None], int]] = []

    def exec(
        self,
        name: str,
        params: list[bytes | None],
        on_reply: Callable[[_FakeReply], None],
        user_data: int,
    ) -> int:
        self.exec_calls.append((name, params, user_data))
        if name == db.STATE_GET_NAME:
            key = params[0]
            assert key is not None
            val = self.table.get(key)
            reply = (
                _FakeReply(n_cols=0, user_data=user_data)
                if val is None
                else _FakeReply(n_rows=1, user_data=user_data, value_bytes=val)
            )
        elif name == db.STATE_PUT_NAME:
            key = params[0]
            value = params[1]
            assert key is not None and value is not None
            self.table[key] = value
            reply = _FakeReply(n_cols=0, user_data=user_data)
        elif name == db.STATE_DELETE_NAME:
            key = params[0]
            assert key is not None
            self.table.pop(key, None)
            reply = _FakeReply(n_cols=0, user_data=user_data)
        else:
            raise AssertionError(f"unexpected query name: {name}")
        on_reply(reply)
        return 0


class _ManualDbPlane:
    """A db plane that submits without firing; the test fires replies by id.

    Proves the store correlates replies by the user_data id rather
    than by call order: the test fires the second call's reply slot with
    the first call's id and observes the first future resolve.
    """

    def __init__(self) -> None:
        self.calls: list[tuple[str, list[bytes | None], int, Callable[[_FakeReply], None]]] = []

    def exec(
        self,
        name: str,
        params: list[bytes | None],
        on_reply: Callable[[_FakeReply], None],
        user_data: int,
    ) -> int:
        self.calls.append((name, params, user_data, on_reply))
        return 0

    def fire_with_user_data(
        self,
        user_data: int,
        *,
        value: bytes | None = None,
        status: int = 0,
        n_rows: int = 1,
    ) -> None:
        # Pick the most recently registered handler for the requested id;
        # the store uses one bound _on_reply for every exec, so any slot works.
        on_reply = self.calls[-1][3]
        reply = _FakeReply(status=status, n_rows=n_rows, user_data=user_data, value_bytes=value)
        on_reply(reply)


class _SubmitFailPlane:
    """A db plane whose exec always fails submission (returns a C error)."""

    def __init__(self, rc: int) -> None:
        self.rc = rc
        self.exec_calls = 0

    def exec(
        self,
        name: str,
        params: list[bytes | None],
        on_reply: Callable[[_FakeReply], None],
        user_data: int,
    ) -> int:
        del name, params, on_reply, user_data
        self.exec_calls += 1
        return self.rc


class _StatusFailPlane:
    """A db plane whose replies carry a non-zero status (a query failure)."""

    def __init__(self, status: int) -> None:
        self.status = status
        self.fired = 0

    def exec(
        self,
        name: str,
        params: list[bytes | None],
        on_reply: Callable[[_FakeReply], None],
        user_data: int,
    ) -> int:
        del name, params
        reply = _FakeReply(status=self.status, user_data=user_data)
        on_reply(reply)
        self.fired += 1
        return 0


class TestPostgresStore:
    def test_get_hit_returns_value(self) -> None:
        plane = _SyncDbPlane()
        plane.table[b"k"] = b"V"
        s = db.PostgresStore(plane)
        assert asyncio.run(s.get(b"k")) == b"V"
        assert plane.exec_calls[-1][0] == db.STATE_GET_NAME

    def test_get_miss_returns_none(self) -> None:
        plane = _SyncDbPlane()
        s = db.PostgresStore(plane)
        assert asyncio.run(s.get(b"absent")) is None

    def test_put_then_get_round_trips(self) -> None:
        plane = _SyncDbPlane()

        async def run() -> bytes | None:
            s = db.PostgresStore(plane)
            await s.put(b"k", b"V")
            return await s.get(b"k")

        assert asyncio.run(run()) == b"V"
        assert plane.exec_calls[0][0] == db.STATE_PUT_NAME

    def test_delete_removes_key(self) -> None:
        plane = _SyncDbPlane()

        async def run() -> bytes | None:
            s = db.PostgresStore(plane)
            await s.put(b"k", b"V")
            await s.delete(b"k")
            return await s.get(b"k")

        assert asyncio.run(run()) is None

    def test_correlation_routes_by_user_data_not_call_order(self) -> None:
        async def run() -> tuple[bytes | None, bytes | None]:
            plane = _ManualDbPlane()
            s = db.PostgresStore(plane)
            f1 = asyncio.ensure_future(s.get(b"k1"))
            f2 = asyncio.ensure_future(s.get(b"k2"))
            await asyncio.sleep(0)
            assert len(plane.calls) == 2
            ud1 = plane.calls[0][2]
            ud2 = plane.calls[1][2]
            assert ud1 != ud2
            # Fire the second slot's reply with the first call's id: the
            # store must resolve f1 (the first future), leaving f2 pending.
            plane.fire_with_user_data(ud1, value=b"V1")
            v1 = await f1
            assert not f2.done()
            plane.fire_with_user_data(ud2, value=b"V2")
            v2 = await f2
            return v1, v2

        v1, v2 = asyncio.run(run())
        assert (v1, v2) == (b"V1", b"V2")

    def test_correlation_id_starts_at_one_and_increments(self) -> None:
        plane = _ManualDbPlane()
        s = db.PostgresStore(plane)

        async def run() -> None:
            f1 = asyncio.ensure_future(s.get(b"k1"))
            f2 = asyncio.ensure_future(s.get(b"k2"))
            await asyncio.sleep(0)
            assert plane.calls[0][2] == 1
            assert plane.calls[1][2] == 2
            plane.fire_with_user_data(1, value=b"A")
            plane.fire_with_user_data(2, value=b"B")
            assert await f1 == b"A"
            assert await f2 == b"B"

        asyncio.run(run())

    def test_unknown_user_data_reply_is_dropped(self) -> None:
        async def run() -> None:
            plane = _ManualDbPlane()
            s = db.PostgresStore(plane)
            fut = asyncio.ensure_future(s.get(b"k"))
            await asyncio.sleep(0)
            assert len(plane.calls) == 1
            # A reply for an id the store never issued must not resolve fut.
            plane.fire_with_user_data(9999, value=b"nope")
            await asyncio.sleep(0)
            assert not fut.done()
            plane.fire_with_user_data(plane.calls[0][2], value=b"yes")
            assert await fut == b"yes"

        asyncio.run(run())

    def test_submission_failure_raises_kith_error(self) -> None:
        plane = _SubmitFailPlane(rc=-int(gen_types.kith_error.KITH_ECONNRESET))
        s = db.PostgresStore(plane)
        with pytest.raises(KithError):
            asyncio.run(s.get(b"k"))

    def test_reply_status_failure_raises_kith_error(self) -> None:
        plane = _StatusFailPlane(status=-int(gen_types.kith_error.KITH_ESTATE))
        s = db.PostgresStore(plane)
        with pytest.raises(KithError) as exc_info:
            asyncio.run(s.get(b"k"))
        assert exc_info.value.code == gen_types.kith_error.KITH_ESTATE

    def test_postgres_store_satisfies_game_state_store(self) -> None:
        # Structural check: PostgresStore is a valid GameStateStore backing.
        plane = _SyncDbPlane()
        s: common_store.GameStateStore = db.PostgresStore(plane)
        assert s is not None


# ---------------------------------------------------------------------------
# named-query registration on the real facade (build-gated)
# ---------------------------------------------------------------------------


@needs_build
class TestRegisterQueries:
    def test_register_raises_without_a_pool(self) -> None:
        # A server built with no persistence pool has a NULL borrowed db;
        # register_query raises KITH_ESTATE, so register_queries surfaces
        # it as the opt-in signal.
        server = Server(topology="embedded")
        try:
            with pytest.raises(KithStateError) as exc_info:
                db.register_queries(server)
            assert exc_info.value.code == gen_types.kith_error.KITH_ESTATE
        finally:
            server.close()

    def test_queries_catalog_has_three_entries(self) -> None:
        assert len(db.QUERIES) == 3
        names = [name for name, _sql, _n in db.QUERIES]
        assert names == [db.STATE_GET_NAME, db.STATE_PUT_NAME, db.STATE_DELETE_NAME]
        for _name, _sql, n_params in db.QUERIES:
            assert n_params >= 1
