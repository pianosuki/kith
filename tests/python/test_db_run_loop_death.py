"""Unit tests for the db pool's reactor run-loop death surface.

Drives :meth:`kith.db.Database._run_reactor` against a stub run function
whose return reports the C polling/syscall failure, so the death path is
exercised without a PostgreSQL server: the submission awaiting at the
death resolves with the transport family, and a submission arriving
after the death refuses at registration with the same failure identity.
"""

from __future__ import annotations

import asyncio
import ctypes
import threading
from types import SimpleNamespace
from typing import cast

import pytest

from kith._generated import types as gen_types
from kith.db import _RUN_LOOP_FAILURE, Database
from kith.exceptions import KithNetworkError


def test_run_loop_death_fails_pending_and_refuses_later_submissions() -> None:
    db = Database.__new__(Database)
    db._lock = threading.Lock()
    db._pending = {}
    db._next_id = 1
    db._run_dead = False
    loop = asyncio.new_event_loop()
    try:
        db._loop = loop
        fut: asyncio.Future[object] = loop.create_future()
        assert db._register(fut) == 1

        lib = cast(
            ctypes.CDLL,
            SimpleNamespace(kith_reactor_run=lambda _handle: -int(gen_types.kith_error.KITH_EIO)),
        )
        db._run_reactor(lib, object())

        assert db._run_dead
        assert db._pending == {}
        # The submission awaiting at the death resolves with the transport
        # family once the loop drains the threadsafe failure.
        with pytest.raises(KithNetworkError) as exc_info:
            loop.run_until_complete(fut)
        assert exc_info.value.code == gen_types.kith_error.KITH_EIO
        assert _RUN_LOOP_FAILURE in str(exc_info.value)

        # A submission arriving after the death refuses at registration
        # instead of queueing into a pump that never drains.
        with pytest.raises(KithNetworkError) as exc_info:
            db._register(loop.create_future())
        assert exc_info.value.code == gen_types.kith_error.KITH_EIO
    finally:
        loop.close()
