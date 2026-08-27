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
import gc
import os
import socket
from pathlib import Path
from typing import Any

import pytest
from _helpers import needs_build

import kith
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
