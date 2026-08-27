"""Integration test: the postgres example's save/load cycle end-to-end.

Boots the example's :class:`~examples.postgres.server.PostgresServer` —
the embedded topology plus the out-of-band db pool on its standalone
reactor — against the live dev Postgres and drives the control-plane
persistence cycle: login, move, save, move away, load. The load must
restore the saved position, which proves the pool-only reactor delivered
the reply callbacks: a dead pump times the save out and every subsequent
db exec returns ``KITH_EBUSY`` (the keepalive timer in the example exists
to keep that reactor's run loop alive between query bursts).
"""

from __future__ import annotations

import ctypes
import json
import os
import sys
import threading
import time
import urllib.request
from pathlib import Path
from typing import Any, cast

import pytest
from _helpers import _BUILD_DEBUG, needs_build
from examples.postgres.server import PostgresServer


# The example round-trip writes the same live game_state table the store
# tests truncate, so the whole postgres cohort runs inside one xdist
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

# The example persists actor blobs into the game_state table the README
# documents as operator-provisioned; issuing the same idempotent DDL here
# keeps the test self-sufficient on any live-Postgres environment.
_SCHEMA_SQL = "CREATE TABLE IF NOT EXISTS game_state (key text PRIMARY KEY, value text NOT NULL)"

# libpq is already a link-time dependency of libkith_db.so; loading it a
# second time by soname returns a handle to the same mapping.
_PG_SONAMES = (
    ("libpq.dylib", "libpq.5.dylib") if sys.platform == "darwin" else ("libpq.so.5", "libpq.so")
)


def _load_libpq() -> ctypes.CDLL | None:
    """Load libpq and bind the procedures the probe and provisioning use."""
    lib = None
    for soname in _PG_SONAMES:
        try:
            lib = ctypes.CDLL(soname)
            break
        except OSError:
            continue
    if lib is None:
        return None
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
    lib.PQexec.argtypes = [ctypes.c_void_p, ctypes.c_char_p]
    lib.PQexec.restype = ctypes.c_void_p
    lib.PQresultStatus.argtypes = [ctypes.c_void_p]
    lib.PQresultStatus.restype = ctypes.c_int
    lib.PQresultErrorMessage.argtypes = [ctypes.c_void_p]
    lib.PQresultErrorMessage.restype = ctypes.c_char_p
    lib.PQclear.argtypes = [ctypes.c_void_p]
    lib.PQclear.restype = None
    lib.PQfinish.argtypes = [ctypes.c_void_p]
    lib.PQfinish.restype = None
    return lib


def _connect(lib: ctypes.CDLL) -> int | None:
    """Open one connection to the configured server, or return ``None``."""
    keywords = ["host", "port", "dbname", "user", "connect_timeout"]
    values: list[str] = [
        os.environ.get(_PG_HOST_ENV, _DEFAULT_HOST),
        os.environ.get(_PG_PORT_ENV, str(_DEFAULT_PORT)),
        os.environ.get(_PG_DB_ENV, _DEFAULT_DB),
        os.environ.get(_PG_USER_ENV, _DEFAULT_USER),
        "2",
    ]
    password = os.environ.get(_PG_PASSWORD_ENV, "")
    if password:
        keywords.insert(4, "password")
        values.insert(4, password)

    kw_arr = (ctypes.c_char_p * (len(keywords) + 1))(
        *[kw.encode("utf-8") for kw in keywords],
        None,
    )
    val_arr = (ctypes.c_char_p * (len(values) + 1))(
        *[val.encode("utf-8") for val in values],
        None,
    )
    conn: int | None = lib.PQconnectdbParams(kw_arr, val_arr, 0)
    return conn


def _pg_login_error() -> str | None:
    """Probe the configured server with a read-only login.

    Returns the failure text, or ``None`` when the configured server is
    usable. Runs at import so a misconfigured Postgres produces an
    actionable skip reason instead of a mid-test connection error.
    """
    lib = _load_libpq()
    if lib is None:
        return "libpq not found; cannot reach the configured Postgres"
    conn = _connect(lib)
    if conn is None:
        return "libpq returned no connection object for the configured server"
    try:
        if int(lib.PQstatus(conn)) != 0:  # CONNECTION_OK
            message = lib.PQerrorMessage(conn) or b""
            return (
                "postgres rejected the configured role/database: "
                + bytes(message).decode("utf-8", errors="replace").strip()
            )
        return None
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
        return _pg_login_error()
    except (OSError, AttributeError) as exc:
        # A client library lacking the symbols the probe binds is an
        # environment gap; anything else propagates.
        return f"postgres client library unusable ({type(exc).__name__}: {exc})"


_PG_SKIP_REASON: str | None = _pg_skip_reason()

needs_pg = pytest.mark.skipif(
    _PG_SKIP_REASON is not None,
    reason=_PG_SKIP_REASON or "no usable Postgres",
)


def _provision_schema() -> str | None:
    """Create the game_state table through libpq.

    Returns the failure text, or ``None`` when the configured server is
    usable and the table exists.
    """
    lib = _load_libpq()
    if lib is None:
        return "libpq not found; cannot reach the configured Postgres"
    conn = _connect(lib)
    if conn is None:
        return "libpq returned no connection object for the configured server"
    try:
        if int(lib.PQstatus(conn)) != 0:  # CONNECTION_OK
            message = lib.PQerrorMessage(conn) or b""
            return (
                "postgres rejected the configured role/database: "
                + bytes(message).decode("utf-8", errors="replace").strip()
            )
        result = lib.PQexec(conn, _SCHEMA_SQL.encode("utf-8"))
        if result is None:
            return "schema DDL returned no result object"
        try:
            # PGRES_COMMAND_OK == 1 for a statement returning no rows.
            if int(lib.PQresultStatus(result)) != 1:
                message = lib.PQresultErrorMessage(result) or b""
                return (
                    "schema DDL failed: " + bytes(message).decode("utf-8", errors="replace").strip()
                )
            return None
        finally:
            lib.PQclear(result)
    finally:
        lib.PQfinish(conn)


def _post(base: str, path: str, body: dict[str, object]) -> tuple[int, dict[str, Any]]:
    req = urllib.request.Request(
        base + path,
        data=json.dumps(body).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=15.0) as resp:
        return resp.status, cast("dict[str, Any]", json.loads(resp.read().decode("utf-8")))


def _get(base: str, path: str) -> tuple[int, dict[str, Any]]:
    with urllib.request.urlopen(base + path, timeout=15.0) as resp:
        return resp.status, cast("dict[str, Any]", json.loads(resp.read().decode("utf-8")))


@pytest.fixture()
def _isolate_bridge(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KITH_LIB", str(_BUILD_DEBUG))


@pytest.fixture(scope="session")
def _provisioned_schema() -> None:
    """Provision the game_state table once per session.

    Under CI a provisioning failure fails the test: the workflow
    provisions the service, so a broken one must be loud. Locally it
    skips with the probe's reason.
    """
    error = _provision_schema()
    if error is None:
        return
    if os.environ.get("CI"):
        pytest.fail(f"postgres schema provisioning failed: {error}")
    pytest.skip(error)


@needs_build
@needs_pg
class TestPostgresExamplePersistence:
    def test_save_and_load_round_trip(
        self, _isolate_bridge: None, _provisioned_schema: None
    ) -> None:
        server = PostgresServer()
        facade, _, control_port = server.start()
        run_thread = threading.Thread(target=facade.run, daemon=True)
        run_thread.start()
        try:
            base = f"http://127.0.0.1:{control_port}"
            # The control plane answering proves the server reached RUNNING
            # before the persistence cycle starts.
            deadline = time.monotonic() + 10.0
            while True:
                try:
                    _get(base, "/query_state")
                    break
                except OSError:
                    if time.monotonic() > deadline:
                        raise
                    time.sleep(0.05)

            status, body = _post(base, "/login", {"principal_id": 9042})
            assert status == 200
            actor_id = int(body["actor_id"])

            _, saved = _post(
                base,
                "/move",
                {"actor_id": actor_id, "move_x": 32767, "move_y": 16383},
            )
            saved_position = (saved["pos_x"], saved["pos_y"])

            status, body = _post(base, "/save", {"actor_id": actor_id})
            assert status == 200, f"save failed: {body}"
            assert body["saved"] is True

            # Move away from the saved state so the load's teleport is
            # observable rather than a no-op on an unmoved actor.
            _, drifted = _post(
                base,
                "/move",
                {"actor_id": actor_id, "move_x": -65534, "move_y": -32766},
            )
            assert (drifted["pos_x"], drifted["pos_y"]) != saved_position

            status, body = _get(base, f"/load?actor_id={actor_id}")
            assert status == 200, f"load failed: {body}"
            assert (body["pos_x"], body["pos_y"]) == saved_position
        finally:
            facade.shutdown()
            run_thread.join(timeout=5.0)
            server.stop()
