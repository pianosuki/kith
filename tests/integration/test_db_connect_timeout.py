"""Integration test: the per-attempt connect deadline against a silent peer.

A peer that completes the TCP handshake but never answers the startup
packet leaves libpq's async connect waiting on readability with no kernel
ceiling — the kernel ACKs keepalive probes, and the ``connect_timeout``
keyword does not apply to ``PQconnectPoll``. The db pool's per-attempt
deadline is what bounds the attempt, so this module binds a loopback
listener, never accepts from it, and asserts the query answers the
deadline failure instead of hanging.

Backend-independent: no Postgres server participates, so the module runs
outside the postgres xdist group (a live-database skip would gate the
scenario out).
"""

from __future__ import annotations

import asyncio
import socket
import time
from collections.abc import Iterator

import pytest
from _helpers import needs_build

import kith
from kith.exceptions import KithError


pytestmark = needs_build()

_CONNECT_TIMEOUT_MS = 400
# The window covers the eager attempt's deadline (armed at construction)
# plus the query's own attempt, with margin for xdist contention.
_RUN_WINDOW_S = 30.0
# The deadline arms per attempt, so the reply lands within the deadline of
# whichever attempt carries the query — bounded, never unbounded.
_ELAPSED_MAX_S = 3.0


@pytest.fixture()
def silent_listener_port() -> Iterator[int]:
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    try:
        yield listener.getsockname()[1]
    finally:
        listener.close()


async def _query_with_deadline(port: int) -> float:
    database = kith.Database(
        host="127.0.0.1",
        port=port,
        db_name="kith",
        user="kith",
        connect_timeout_ms=_CONNECT_TIMEOUT_MS,
    )
    start = time.monotonic()
    with pytest.raises(KithError):
        await database.query("SELECT 1")
    elapsed = time.monotonic() - start
    database.close()
    return elapsed


def test_connect_deadline_bounds_silent_listener(silent_listener_port: int) -> None:
    """A silent peer answers the connect deadline, not a hang.

    The query raises (the connection attempt died) within a bounded
    window, and the database closes cleanly afterwards.
    """
    elapsed = asyncio.run(
        asyncio.wait_for(_query_with_deadline(silent_listener_port), _RUN_WINDOW_S)
    )
    assert elapsed < _ELAPSED_MAX_S
