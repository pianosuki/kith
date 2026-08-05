"""Build-gated unit tests for the :class:`~kith.reactor.Reactor` lifecycle.

Exercises the ``close``/``run`` coordination contract against the debug
build: a run loop parked on a registered descriptor stays in flight until
``close`` requests shutdown, and ``close`` destroys the handle only after
the C run loop has returned — the ordering the C-side runtime guard enforces
with an abort when violated.
"""

from __future__ import annotations

import socket
import threading
import time
import weakref
from collections.abc import Iterator

import pytest
from _build_gate import _BUILD_DEBUG, needs_build

from kith._bridge import reset
from kith.exceptions import KithError, KithStateError
from kith.reactor import Event, Reactor
from kith.worker import Worker


def _new_reactor_with_retry(attempts: int = 10) -> Reactor:
    # Ring pages are memcg-accounted kernel memory that reclaims
    # asynchronously after queue_exit, so a process that cycles reactors
    # back-to-back can transiently pressure the cgroup memory budget; a
    # bounded retry rides out the reclaim window.
    for attempt in range(attempts):
        try:
            return Reactor()
        except KithError as exc:
            if attempt == attempts - 1 or exc.code.name not in ("KITH_EIO", "KITH_ENOMEM"):
                raise
            time.sleep(0.05)
    raise AssertionError("unreachable")


@pytest.fixture(autouse=True)
def _isolate_bridge(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Pin KITH_LIB to the debug build dir and reset the singleton per test."""
    monkeypatch.setenv("KITH_LIB", str(_BUILD_DEBUG))
    reset()
    yield
    reset()


@needs_build
class TestReactorLifecycle:
    def test_close_blocks_until_run_exits(self) -> None:
        reactor = Reactor()
        left, right = socket.socketpair()
        try:
            entered = threading.Event()
            reactor.add(left.fileno(), int(Event.IN), lambda fd, events: None)
            # A zero-delay timer fires only when the run loop advances the
            # timer wheel, so the event proves the loop is executing rather
            # than merely spawned (submit() does not: it dispatches straight
            # onto the worker pool without involving the loop).
            reactor.schedule(0, entered.set)

            run_thread = threading.Thread(target=reactor.run, daemon=True)
            run_thread.start()
            assert entered.wait(timeout=5.0), "run loop did not start"

            # Close from outside the run thread requests shutdown, waits for
            # kith_reactor_run to return, and only then destroys the handle.
            # Destroying under a live run aborts the process, so completing
            # with the loop parked proves the destroy ordering holds.
            reactor.close()
            run_thread.join(timeout=10.0)
            assert not run_thread.is_alive()
            reactor.close()
        finally:
            left.close()
            right.close()

    def test_close_without_run_destroys_cleanly(self) -> None:
        reactor = Reactor()
        assert reactor.handle is not None
        reactor.close()
        assert reactor.handle is None
        reactor.close()

    def test_run_after_close_raises_state_error(self) -> None:
        reactor = Reactor()
        reactor.close()
        with pytest.raises(KithStateError):
            reactor.run()

    def test_close_racing_run_startup_never_orphans_the_loop(self) -> None:
        # Hammers the startup window where close() can decide "no run in
        # flight" while a run thread is between announcing itself and
        # entering the run call: an unserialized interleaving there destroys
        # the handle under a not-yet-entered run (KITH_EINVAL surfacing as a
        # thread exception, or a use-after-free segfault in the io_uring
        # wait). The lifecycle lock makes every
        # interleaving either wait for the loop or refuse it; unannounced
        # runs that lose the race surface here as KithStateError, and any
        # other thread exception is a failure.
        stray: list[BaseException] = []

        def _hook(args: threading.ExceptHookArgs) -> None:
            if args.exc_value is not None:
                stray.append(args.exc_value)

        previous = threading.excepthook
        threading.excepthook = _hook
        try:
            for i in range(32):
                reactor = _new_reactor_with_retry()
                parked = socket.socketpair()
                try:
                    if i % 2 == 0:
                        # Parked variant: the loop blocks on a descriptor, so
                        # close() exercises the shutdown-and-wait path.
                        reactor.add(parked[0].fileno(), int(Event.IN), lambda fd, events: None)
                    run_thread = threading.Thread(target=reactor.run, daemon=True)
                    run_thread.start()
                    reactor.close()
                    run_thread.join(timeout=10.0)
                    assert not run_thread.is_alive(), f"run outlived close (iteration {i})"
                    reactor.close()
                finally:
                    parked[0].close()
                    parked[1].close()
        finally:
            threading.excepthook = previous
        unexpected = [exc for exc in stray if not isinstance(exc, KithStateError)]
        assert not unexpected, f"unexpected thread exceptions: {unexpected!r}"

    def test_close_stops_a_run_that_has_not_entered(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # A run announced but not yet inside the C loop honors a stop
        # delivered in that window: the request takes effect at the loop's
        # next boundary, and the loop's first check returns the run before
        # its poll. The helper mirrors run()'s announcement and exit-event
        # contract but enters the C loop only after close's stop, pinning
        # the pre-entry-request interleaving deterministically instead of
        # leaving it to scheduler luck under full-suite load.
        reactor = _new_reactor_with_retry()
        parked = socket.socketpair()
        try:
            reactor.add(parked[0].fileno(), int(Event.IN), lambda fd, events: None)

            announced = threading.Event()
            first_stop_done = threading.Event()
            stops: list[int] = []
            run_results: list[int] = []
            real_stop = Reactor.stop

            def _counting_stop(target: Reactor) -> None:
                real_stop(target)
                stops.append(1)
                first_stop_done.set()

            def _announce_then_enter_after_first_stop() -> None:
                with reactor._lifecycle_lock:
                    reactor._run_thread = threading.current_thread()
                    reactor._run_exited.clear()
                announced.set()
                assert first_stop_done.wait(timeout=10.0)
                try:
                    rc = int(reactor._bridge.lib("reactor").kith_reactor_run(reactor._handle))
                    run_results.append(rc)
                finally:
                    # run()'s finally sets the exit event; the direct C call
                    # bypasses it, so the helper replicates it here.
                    reactor._run_exited.set()

            monkeypatch.setattr(Reactor, "stop", _counting_stop)
            run_thread = threading.Thread(target=_announce_then_enter_after_first_stop, daemon=True)
            run_thread.start()
            assert announced.wait(timeout=10.0)
            reactor.close()
            run_thread.join(timeout=10.0)
            assert not run_thread.is_alive(), "run outlived close"
            assert run_results == [0]
            assert len(stops) == 1, "close issued more than one stop request"
        finally:
            parked[0].close()
            parked[1].close()


@needs_build
class TestReactorTeardownOrdering:
    """Registration release follows the dispatching pool's drain.

    Queued tasks hold raw trampoline pointers until a worker executes them,
    so a registration freed before the drain (and before the pool's threads
    are joined) hands the C side a collected trampoline. The observable is
    the trampoline's liveness through a weakref: blockers parked on an event
    hold every pool worker, so the drain — and any release that follows it —
    cannot complete until the test releases them.
    """

    @staticmethod
    def _queue_blocker_then_probe(
        reactor: Reactor,
        order: list[str],
        unblock: threading.Event,
    ) -> weakref.ref[object]:
        # One blocker per worker pins every pool thread, so the probe stays
        # queued regardless of the pool's size (the C default is 1 under the
        # standard interpreter, N under a free-threaded one).
        def _block() -> None:
            unblock.wait(timeout=5.0)

        def _probe() -> None:
            order.append("executed")

        for _ in range(reactor.worker.worker_count()):
            reactor.submit(_block)
        before = set(reactor._cbs)
        reactor.submit(_probe)
        probe_trampoline = reactor._cbs[(set(reactor._cbs) - before).pop()]
        ref = weakref.ref(probe_trampoline)
        del probe_trampoline
        return ref

    def test_owned_close_executes_queued_tasks_before_release(self) -> None:
        reactor = _new_reactor_with_retry()
        order: list[str] = []
        unblock = threading.Event()
        ref = self._queue_blocker_then_probe(reactor, order, unblock)

        closer = threading.Thread(target=reactor.close, daemon=True)
        closer.start()
        # The release must not happen while the probe is still queued. The
        # blockers hold the drain, so a trampoline that dies here is freed
        # ahead of it.
        deadline = time.monotonic() + 0.5
        while time.monotonic() < deadline and ref() is not None:
            time.sleep(0.01)
        assert ref() is not None, "registration released before the queued probe executed"
        unblock.set()
        closer.join(timeout=10.0)
        assert not closer.is_alive()
        # The drain executes the queued probe, and only then releases
        # the trampoline.
        assert order == ["executed"]
        assert ref() is None

    def test_borrowed_pool_releases_registrations_at_its_own_close(self) -> None:
        pool = Worker(worker_count=1)
        reactor: Reactor | None = None
        for attempt in range(10):
            try:
                reactor = Reactor(worker=pool)
                break
            except KithError as exc:
                if attempt == 9 or exc.code.name not in ("KITH_EIO", "KITH_ENOMEM"):
                    pool.close()
                    raise
                time.sleep(0.05)
        assert reactor is not None
        order: list[str] = []
        unblock = threading.Event()
        ref = self._queue_blocker_then_probe(reactor, order, unblock)

        reactor.close()
        # The borrowed pool outlives the reactor: the probe's trampoline is
        # still referenced after the reactor is gone, and its release
        # belongs to the pool's close.
        assert ref() is not None
        unblock.set()
        pool.close()
        assert ref() is None
        assert order == ["executed"]

    def test_remove_defers_trampoline_release_to_pool_close(self) -> None:
        reactor = _new_reactor_with_retry()
        left, right = socket.socketpair()
        try:
            reactor.add(left.fileno(), int(Event.IN), lambda fd, events: None)
            entry = reactor._cbs[left.fileno()]
            assert isinstance(entry, tuple)
            trampoline = entry[0]
            ref = weakref.ref(trampoline)
            del trampoline, entry
            reactor.remove(left.fileno())
            # Queued readiness work can still hold the raw pointer, so the
            # deregistration does not release the trampoline.
            assert ref() is not None
            reactor.close()
            assert ref() is None
        finally:
            left.close()
            right.close()

    def test_registration_methods_after_close_raise_state_error(self) -> None:
        reactor = _new_reactor_with_retry()
        left, right = socket.socketpair()
        try:
            reactor.close()
            with pytest.raises(KithStateError):
                reactor.add(left.fileno(), int(Event.IN), lambda fd, events: None)
            with pytest.raises(KithStateError):
                reactor.modify(left.fileno(), int(Event.IN))
            with pytest.raises(KithStateError):
                reactor.remove(left.fileno())
            with pytest.raises(KithStateError):
                reactor.submit(lambda: None)
            with pytest.raises(KithStateError):
                reactor.schedule(1, lambda: None)
        finally:
            left.close()
            right.close()
