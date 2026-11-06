"""Integration test: the relational transaction surface against a live Postgres.

Exercises :class:`kith.Database` end to end: the owned pool (its own
reactor thread and keepalive), raw-SQL queries returning row snapshots,
the named-query registry path, and the transaction context — BEGIN,
per-statement execution, and COMMIT resolved before the block exits;
ROLLBACK on an exception. The concurrency case runs two transaction
contexts and a pool query at once, which needs the pool's session and
round-robin arms to coexist on real connections.

Skips unless the configured Postgres accepts a login (set ``KITH_PG_*``
to point at one; ``scripts/dev-postgres.sh`` provisions a matching
instance). In CI the skip is disabled so a broken service fails the
build rather than hiding behind a skip.
"""

from __future__ import annotations

import asyncio
import contextlib
import gc
import os
import socket
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

import pytest
from _helpers import needs_build

import kith
import kith.db as db_module
from kith._generated import types as gen_types
from kith.exceptions import KithError, KithStateError


pytestmark = [pytest.mark.xdist_group("postgres"), needs_build()]

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

_TEST_TIMEOUT_S = 30.0

_TABLE = "db_api_txn_test"


def _pg_env() -> dict[str, Any]:
    return {
        "host": os.environ.get(_PG_HOST_ENV, _DEFAULT_HOST),
        "port": int(os.environ.get(_PG_PORT_ENV, str(_DEFAULT_PORT))),
        "db_name": os.environ.get(_PG_DB_ENV, _DEFAULT_DB),
        "user": os.environ.get(_PG_USER_ENV, _DEFAULT_USER),
        "password": os.environ.get(_PG_PASSWORD_ENV, _DEFAULT_PASSWORD),
    }


def _postgres_available(env: dict[str, Any]) -> bool:
    try:
        with socket.create_connection((env["host"], env["port"]), timeout=2.0):
            return True
    except OSError:
        return False


_LOGIN_PROBE_OK: bool | None = None


def _login_probe(env: dict[str, Any]) -> bool:
    """True when the configured role and database accept a real login.

    Cached per test session: one throwaway database boot answers for
    every test in the module, and an answering socket with rejected
    credentials still skips (the shipped postgres tests' discipline).
    """
    global _LOGIN_PROBE_OK
    if _LOGIN_PROBE_OK is None:
        try:
            db = kith.Database(**env)
            asyncio.run(db.query("SELECT 1"))
            db.close()
            _LOGIN_PROBE_OK = True
        except KithError:
            _LOGIN_PROBE_OK = False
    return _LOGIN_PROBE_OK


def _make_database() -> kith.Database:
    return kith.Database(**_pg_env())


async def _reset_table(db: kith.Database) -> None:
    await db.query(f"DROP TABLE IF EXISTS {_TABLE}")
    await db.query(f"CREATE TABLE {_TABLE} (k int PRIMARY KEY, v int)")


def _run(coro: Any) -> Any:
    async def _body() -> Any:
        async with asyncio.timeout(_TEST_TIMEOUT_S):
            return await coro

    return asyncio.run(_body())


@pytest.fixture()
def pg_env() -> dict[str, Any]:
    env = _pg_env()
    if os.environ.get("KITH_SKIP_POSTGRES") == "1" or not _postgres_available(env):
        pytest.skip("Postgres not reachable; set KITH_PG_* to enable")
    if not _login_probe(env):
        pytest.skip("Postgres rejected the configured role or database")
    return env


def test_create_owns_param_string_copies(pg_env: dict[str, Any]) -> None:
    """The handle owns images of the connection strings, so the caller may
    release them once the create returns and the next connect still
    authenticates with the configured role and database."""

    async def _body() -> None:
        host = f"{pg_env['host']}"
        user = f"{pg_env['user']}"
        db_name = f"{pg_env['db_name']}"
        password = f"{pg_env['password']}"
        db = kith.Database(
            host=host, port=pg_env["port"], db_name=db_name, user=user, password=password
        )
        # Release the source strings and recycle their allocator blocks so
        # a stale reference cannot still read the old bytes.
        del host, user, db_name, password
        gc.collect()
        junk = [bytes(len(pg_env["user"])) for _ in range(100_000)]
        del junk
        gc.collect()
        try:
            reply = await db.query("SELECT current_user AS u")
            assert reply.rows == ((pg_env["user"],),)
        finally:
            db.close()

    _run(_body())


def test_query_rows_and_nulls(pg_env: dict[str, Any]) -> None:
    async def _body() -> None:
        db = _make_database()
        try:
            reply = await db.query("SELECT 41 + 1 AS v")
            assert reply.n_rows == 1
            assert reply.rows == (("42",),)
            assert reply.value(0, 0) == "42"

            reply = await db.query("SELECT NULL::int AS n, 7::int AS v")
            assert reply.rows == ((None, "7"),)
            assert reply.value(0, 0) is None
        finally:
            db.close()

    _run(_body())


def test_query_param_types(pg_env: dict[str, Any]) -> None:
    async def _body() -> None:
        db = _make_database()
        try:
            reply = await db.query("SELECT $1::int AS v", [7])
            assert reply.rows == (("7",),)

            reply = await db.query("SELECT $1::text AS v", ["héllo"])
            assert reply.rows == (("héllo",),)

            reply = await db.query("SELECT $1::bool AS v", [True])
            assert reply.rows == (("t",),)

            reply = await db.query("SELECT $1::int IS NULL AS v", [None])
            assert reply.rows == (("t",),)

            # The rejection probe carries a deliberately out-of-contract
            # parameter type: the boundary's TypeError is the tooth.
            with pytest.raises(TypeError, match="unsupported parameter type"):
                await db.query("SELECT $1", [1.5])  # type: ignore[list-item]
        finally:
            db.close()

    _run(_body())


def test_register_query_and_call(pg_env: dict[str, Any]) -> None:
    async def _body() -> None:
        db = _make_database()
        try:
            db.register_query("db_api_one", "SELECT $1::int + 1 AS one", 1)
            reply = await db.call("db_api_one", [41])
            assert reply.rows == (("42",),)

            with pytest.raises(KithError, match="db exec: db_api_missing"):
                await db.call("db_api_missing")

            with pytest.raises(KithStateError, match="name='db_api_one'"):
                db.register_query("db_api_one", "SELECT 1", 0)
        finally:
            db.close()

    _run(_body())


def test_transaction_commit_and_visibility(pg_env: dict[str, Any]) -> None:
    async def _body() -> None:
        db = _make_database()
        try:
            await _reset_table(db)
            async with db.transaction() as txn:
                await txn.query("INSERT INTO " + _TABLE + " (k, v) VALUES (1, 10)")
                await txn.query("INSERT INTO " + _TABLE + " (k, v) VALUES (2, 20)")
                pending = await txn.query("SELECT sum(v) FROM " + _TABLE)
                assert pending.rows == (("30",),)

            # The commit resolved before the block exited: a plain pool
            # query (a different connection) already observes both rows.
            after = await db.query(f"SELECT count(*) FROM {_TABLE}")
            assert after.rows == (("2",),)
        finally:
            db.close()

    _run(_body())


def test_transaction_rollback_on_exception(pg_env: dict[str, Any]) -> None:
    async def _body() -> None:
        db = _make_database()
        try:
            await _reset_table(db)
            with pytest.raises(RuntimeError, match="aborted"):
                async with db.transaction() as txn:
                    await txn.query("INSERT INTO " + _TABLE + " (k, v) VALUES (1, 10)")
                    raise RuntimeError("aborted")

            after = await db.query(f"SELECT count(*) FROM {_TABLE}")
            assert after.rows == (("0",),)

            # The database serves after the rolled-back context.
            healthy = await db.query("SELECT 1")
            assert healthy.rows == (("1",),)
        finally:
            db.close()

    _run(_body())


def test_concurrent_transactions_and_queries(pg_env: dict[str, Any]) -> None:
    async def _body() -> None:
        db = _make_database()
        try:
            await _reset_table(db)

            async def _txn_insert(k: int, v: int) -> None:
                async with db.transaction() as txn:
                    await txn.query(f"INSERT INTO {_TABLE} (k, v) VALUES ($1, $2)", [k, v])

            # Two pinned sessions and a round-robin pool query at once.
            await asyncio.gather(_txn_insert(1, 10), _txn_insert(2, 20), db.query("SELECT 1"))

            after = await db.query(f"SELECT count(*) FROM {_TABLE}")
            assert after.rows == (("2",),)
        finally:
            db.close()

    _run(_body())


def test_close_rejects_and_is_idempotent(pg_env: dict[str, Any]) -> None:
    async def _body() -> None:
        db = _make_database()
        db.close()
        with pytest.raises(KithStateError, match="database closed"):
            db.register_query("late", "SELECT 1", 0)
        with pytest.raises(KithStateError, match="database closed"):
            await db.query("SELECT 1")
        db.close()

    _run(_body())


def test_transaction_rejects_unentered_context(pg_env: dict[str, Any]) -> None:
    async def _body() -> None:
        db = _make_database()
        try:
            txn = db.transaction()
            with pytest.raises(KithStateError, match="not active"):
                await txn.query("SELECT 1")
        finally:
            db.close()

    _run(_body())


class _CompletionGate:
    """Hold copied callback values on the real loop at an ownership boundary."""

    def __init__(self, boundary: str) -> None:
        self.loop = asyncio.get_running_loop()
        self.ready = asyncio.Event()
        self.repeat_ready = asyncio.Event()
        self.operation: asyncio.Task[None] | None = None
        self.delivered_before_cancel = False
        self.phase = ""
        self._boundary = boundary
        self._captured = False
        self._repeat_captured = False
        self._disabled = False
        self._held: tuple[Callable[..., None], tuple[object, ...]] | None = None
        self._resolve = db_module._resolve

    def open_callback(self, callback: Callable[..., None], *args: object) -> None:
        if not self._disabled and self._boundary == "open-before-callback" and not self._captured:
            self._captured = True
            self._held = callback, args
            self.ready.set()
            return
        callback(*args)

    def resolve(self, future: asyncio.Future[object], value: object) -> None:
        if not self._disabled:
            if (
                self._boundary == "open-after-delivery"
                and self._captured
                and not self._repeat_captured
                and self.phase == "BEGIN"
            ):
                self._repeat_captured = True
                self._held = self._resolve, (future, value)
                self.repeat_ready.set()
                return
            target = {
                "open-before-delivery": "OPEN",
                "open-after-delivery": "OPEN",
                "begin-pending": "BEGIN",
                "query-pending": "SELECT 7",
                "close-pending": "CLOSE",
            }.get(self._boundary)
            if not self._captured and self.phase == target:
                self._captured = True
                if self._boundary == "open-after-delivery":
                    self._resolve(future, value)
                    self.delivered_before_cancel = future.done() and not future.cancelled()
                    assert self.operation is not None
                    self.operation.cancel()
                else:
                    self._held = self._resolve, (future, value)
                self.ready.set()
                return
        self._resolve(future, value)

    def release(self) -> None:
        self._disabled = True
        held, self._held = self._held, None
        if held is not None:
            callback, args = held
            self.loop.call_soon(callback, *args)


class _GatedDatabase(kith.Database):
    """Observe real session commands while the gate owns callback delivery."""

    def __init__(self, gate: _CompletionGate, env: dict[str, Any]) -> None:
        self.gate = gate
        self.statements: list[str] = []
        self.close_calls = 0
        super().__init__(
            **env,
            min_connections=1,
            max_connections=1,
            loop=gate.loop,
        )

    def _on_open(self, session: object, status: int, user_data: int | None) -> None:
        self.gate.loop.call_soon_threadsafe(
            self.gate.open_callback, super()._on_open, session, status, user_data
        )

    async def _session_open(self) -> object:
        self.gate.phase = "OPEN"
        return await super()._session_open()

    async def _session_query(
        self, session: object, sql: str, params: Sequence[str | int | None]
    ) -> db_module.Reply:
        self.statements.append(sql)
        self.gate.phase = sql
        return await super()._session_query(session, sql, params)

    async def _session_close(self, session: object) -> None:
        self.close_calls += 1
        self.gate.phase = "CLOSE"
        await super()._session_close(session)


async def _ready_turn() -> None:
    """Run callbacks already queued before this explicit loop barrier."""
    loop = asyncio.get_running_loop()
    ready: asyncio.Future[None] = loop.create_future()
    loop.call_soon(ready.set_result, None)
    await ready


@pytest.mark.parametrize(
    "boundary",
    [
        "open-before-callback",
        "open-before-delivery",
        "open-after-delivery",
        "begin-pending",
        "query-pending",
        "close-pending",
    ],
)
def test_transaction_cancellation_retains_native_ownership(
    pg_env: dict[str, Any], boundary: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def _body() -> None:
        gate = _CompletionGate(boundary)
        monkeypatch.setattr(db_module, "_resolve", gate.resolve)
        db = _GatedDatabase(gate, pg_env)
        operation: asyncio.Task[None] | None = None
        try:
            assert (await db.query("SELECT 1")).rows == (("1",),)
            transaction = db.transaction()

            async def _transaction() -> None:
                async with transaction:
                    assert (await transaction.query("SELECT 7")).rows == (("7",),)

            operation = asyncio.create_task(_transaction())
            gate.operation = operation
            async with asyncio.timeout(5):
                await gate.ready.wait()
                if boundary == "open-after-delivery":
                    assert gate.delivered_before_cancel
                else:
                    operation.cancel()
                await _ready_turn()
                assert not operation.done(), "cancellation abandoned an accepted native phase"
                if boundary == "open-after-delivery":
                    await gate.repeat_ready.wait()
                operation.cancel()
                await _ready_turn()
                assert not operation.done(), "repeated cancellation interrupted owned cleanup"
                gate.release()
                with pytest.raises(asyncio.CancelledError):
                    await operation

                assert transaction._session is None
                assert db.close_calls == 1
                assert not db._pending
                expected = ["BEGIN"]
                if boundary in {"query-pending", "close-pending"}:
                    expected.append("SELECT 7")
                expected.append("COMMIT" if boundary == "close-pending" else "ROLLBACK")
                assert db.statements == expected

                # One slot proves release; distinct transaction ids prove that
                # release did not return an open SQL bracket to that slot.
                first = await db.query("SELECT txid_current()")
                second = await db.query("SELECT txid_current()")
                assert first.n_rows == second.n_rows == 1
                assert first.value(0, 0) != second.value(0, 0)
        finally:
            gate.release()
            try:
                if operation is not None:
                    if not operation.done():
                        operation.cancel()
                    with contextlib.suppress(asyncio.CancelledError, KithError):
                        async with asyncio.timeout(5):
                            await operation
            finally:
                db.close()

    _run(_body())


class _ScriptedSessionDatabase(kith.Database):
    """Inject completed phase failures without a server or native timing."""

    def __init__(self, failures: dict[str, KithError]) -> None:
        self.failures = failures
        self.statements: list[str] = []
        self.close_calls = 0
        self.native_owned = False
        self.handle = object()

    async def _session_open(self) -> object:
        self.native_owned = True
        return self.handle

    async def _session_query(
        self, session: object, sql: str, params: Sequence[str | int | None]
    ) -> db_module.Reply:
        assert session is self.handle
        assert not params
        self.statements.append(sql)
        if sql in self.failures:
            raise self.failures[sql]
        return db_module.Reply(())

    async def _session_close(self, session: object) -> None:
        assert session is self.handle
        self.close_calls += 1
        if "CLOSE" in self.failures:
            raise self.failures["CLOSE"]
        self.native_owned = False

    def close(self) -> None:
        self.native_owned = False


def test_transaction_commit_failure_rolls_back_before_release() -> None:
    async def _body() -> None:
        primary = KithError(gen_types.kith_error.KITH_EIO, "commit rejected")
        db = _ScriptedSessionDatabase({"COMMIT": primary})
        with pytest.raises(KithError) as failure:
            async with db.transaction():
                pass
        assert failure.value is primary
        assert db.statements == ["BEGIN", "COMMIT", "ROLLBACK"]
        assert db.close_calls == 1
        assert not db.native_owned

    _run(_body())


def test_transaction_rollback_failure_retains_session_and_primary_error() -> None:
    async def _body() -> None:
        primary = RuntimeError("body rejected")
        rollback = KithError(gen_types.kith_error.KITH_EIO, "rollback rejected")
        db = _ScriptedSessionDatabase({"ROLLBACK": rollback})
        try:
            with pytest.raises(RuntimeError) as failure:
                async with db.transaction():
                    raise primary
            assert failure.value is primary
            assert any(str(rollback) in note for note in getattr(primary, "__notes__", ()))
            assert db.statements == ["BEGIN", "ROLLBACK"]
            assert db.close_calls == 0
            assert db.native_owned
        finally:
            db.close()

    _run(_body())


def test_transaction_close_submission_failure_is_reported_and_retains_session() -> None:
    async def _body() -> None:
        primary = KithStateError(gen_types.kith_error.KITH_EBUSY, "release submission rejected")
        db = _ScriptedSessionDatabase({"CLOSE": primary})
        try:
            with pytest.raises(KithStateError) as failure:
                async with db.transaction():
                    pass
            assert failure.value is primary
            assert any("Database.close" in note for note in getattr(primary, "__notes__", ()))
            assert db.statements == ["BEGIN", "COMMIT"]
            assert db.close_calls == 1
            assert db.native_owned
        finally:
            db.close()

    _run(_body())
