"""Build-gated unit tests for the :class:`~kith.worker.Worker` pool wrapper.

Constructs a :class:`~kith.worker.Worker` directly against the debug build so
the Python-side lifecycle — creation parameters, borrowed-handle exposure,
close idempotence, context-manager release, and finalizer behavior on both
open and closed pools — is exercised through the public Python boundary
without a running reactor and without submitting any tasks.
"""

from __future__ import annotations

import gc
from collections.abc import Iterator

import pytest
from _build_gate import _BUILD_DEBUG, needs_build

from kith._bridge import load, reset
from kith.worker import Worker


@pytest.fixture(autouse=True)
def _isolate_bridge(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Pin KITH_LIB to the debug build dir and reset the singleton per test."""
    monkeypatch.setenv("KITH_LIB", str(_BUILD_DEBUG))
    reset()
    yield
    reset()


@needs_build
class TestWorkerPool:
    def test_explicit_worker_count_and_capacity(self) -> None:
        worker = Worker(worker_count=3, task_capacity=64)
        try:
            assert worker.worker_count() == 3
            # A ctypes null pointer is falsy, so truthiness pins the handle
            # to a live pool rather than a failed out-parameter.
            assert worker.handle is not None
            assert worker.handle
        finally:
            worker.close()
        assert worker.handle is None

    def test_default_construction_selects_a_pool(self) -> None:
        # The C layer resolves the default size: one thread under the GIL,
        # N under a free-threaded interpreter, so only the floor is pinned.
        worker = Worker(bridge=None)
        try:
            assert worker.worker_count() >= 1
            assert worker.handle is not None
            assert worker.handle
        finally:
            worker.close()

    def test_explicit_bridge_skips_singleton_load(self) -> None:
        bridge = load()
        worker = Worker(worker_count=1, bridge=bridge)
        try:
            assert worker.worker_count() == 1
        finally:
            worker.close()

    def test_context_manager_exit_closes(self) -> None:
        with Worker(worker_count=2) as worker:
            assert worker.worker_count() == 2
            assert worker.handle is not None
        assert worker.handle is None

    def test_close_is_idempotent(self) -> None:
        worker = Worker(worker_count=1)
        assert worker.handle is not None
        worker.close()
        assert worker.handle is None
        worker.close()
        assert worker.handle is None

    def test_finalizer_releases_open_pool(self) -> None:
        ran: list[int] = []
        worker = Worker(worker_count=1)
        assert worker.handle is not None
        worker._register_close_hook(lambda: ran.append(1))
        del worker
        gc.collect()
        assert ran == [1]

    def test_finalizer_tolerates_closed_pool(self) -> None:
        ran: list[int] = []
        worker = Worker(worker_count=1)
        worker.close()
        assert worker.handle is None
        worker._register_close_hook(lambda: ran.append(1))
        # Registering on a closed pool runs the hook immediately (the drain
        # has completed), and the finalizer must not close or run it again.
        assert ran == [1]
        del worker
        gc.collect()
        assert ran == [1]

    def test_close_hook_runs_at_close(self) -> None:
        worker = Worker(worker_count=1)
        ran: list[int] = []
        worker._register_close_hook(lambda: ran.append(1))
        worker.close()
        assert ran == [1]
        # The idempotent second close does not re-run the hooks.
        worker.close()
        assert ran == [1]

    def test_close_hook_on_closed_pool_runs_immediately(self) -> None:
        worker = Worker(worker_count=1)
        worker.close()
        ran: list[int] = []
        worker._register_close_hook(lambda: ran.append(1))
        assert ran == [1]
