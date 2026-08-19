"""Shared fixtures for integration tests.

The shared helpers (ClientEngine, data types, constants) live in
``_helpers.py`` so both the test files and this conftest can import them
through the ``pythonpath`` entry that includes ``tests/integration``.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator

import pytest
from _helpers import ClientEngine

from kith._bridge import Bridge, load, reset
from kith._generated import configure as configure_all


@pytest.fixture(autouse=True)
def _isolate_bridge(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Pin the framework and AHC library search at the debug build dir.

    Resets the bridge singleton around every test so one test's loaded
    bridge never leaks into the next.
    """
    from _helpers import _BUILD_DEBUG

    monkeypatch.setenv("KITH_LIB", str(_BUILD_DEBUG))
    monkeypatch.setenv("KITH_PROTO_LIB", str(_BUILD_DEBUG / "libkith_proto.so.1"))
    monkeypatch.setenv("KITH_CLIENT_LIB", str(_BUILD_DEBUG / "libkith_client.so.1"))
    reset()
    yield
    reset()


@pytest.fixture
def bridge() -> Iterator[Bridge]:
    """Load and configure the bridge for the current test."""
    loaded = load()
    configure_all(loaded)
    yield loaded


@pytest.fixture
def make_client(bridge: Bridge) -> Iterator[Callable[..., ClientEngine]]:
    """Factory for :class:`ClientEngine` instances; closes unclosed engines after the test.

    Every engine is closed even when one engine's ``close`` raises; the
    first failure is re-raised after the loop so the error still fails
    the test.
    """
    instances: list[ClientEngine] = []

    def _make(
        proto_handle: object,
        *,
        ping_type_id: int = 0,
        pong_type_id: int = 0,
        ping_interval_ms: int = 0,
    ) -> ClientEngine:
        engine = ClientEngine(
            bridge,
            proto_handle,
            ping_type_id=ping_type_id,
            pong_type_id=pong_type_id,
            ping_interval_ms=ping_interval_ms,
        )
        instances.append(engine)
        return engine

    yield _make
    first_error: Exception | None = None
    for engine in instances:
        try:
            engine.close()
        except Exception as exc:
            first_error = first_error or exc
    if first_error is not None:
        raise first_error


def pytest_configure(config: pytest.Config) -> None:
    """Refuse parallel distribution that would reopen the shared-table race.

    The postgres integration modules share one live table with fixed keys
    and a per-test ``TRUNCATE``, and their ``xdist_group`` marks take
    effect only under ``--dist loadgroup``; any other parallel
    distribution spreads the cohort across workers and interleaves the
    writers.
    """
    if config.pluginmanager.hasplugin("xdist") and hasattr(config, "workerinput"):
        return
    numprocesses = config.getoption("numprocesses", None)
    dist = config.getoption("dist", None)
    if numprocesses not in (None, 0, 1) and dist != "loadgroup":
        raise pytest.UsageError(
            "parallel pytest distribution requires --dist loadgroup: the "
            "postgres integration modules serialize through xdist_group "
            "marks under it and race a shared table without it"
        )
