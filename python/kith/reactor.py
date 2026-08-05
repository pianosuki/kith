"""Reactor plane wrapper.

The reactor is the async event loop: it polls file descriptors for readiness
and dispatches callbacks, submits inter-thread tasks through a lock-free
queue, and schedules deferred work on a timer wheel. The C surface targets
Linux io_uring; this module wraps it into a Python type whose callbacks are
ordinary Python callables.

Python callbacks run on a worker pool, never on the reactor thread.
The reactor registers C hop functions (from the worker module's
bridge API) as its callbacks; the hops submit the user's Python callables to
the pool from C, and worker threads run them. This matches the gateway
plane's handler-dispatch discipline (``kith_gateway_dispatch`` ->
``kith_worker_submit`` from C): the standalone reactor and the gateway
enforce the same thread boundary.

A :class:`Reactor` owns its C reactor handle and (by default) a
:class:`~kith.worker.Worker` pool; both are released through :meth:`close`
(also invoked by the context manager and ``__del__``). The ctypes-wrapped
callback trampolines and C bridge handles stay referenced until the
dispatching pool's drain completes — released inside :meth:`close` for an
owned pool, and at the borrowed pool's own close when a pool was passed in —
so the C side never invokes a collected trampoline or dereferences a freed
bridge.
"""

from __future__ import annotations

import contextlib
import ctypes
import threading
from collections.abc import Callable
from enum import IntEnum
from types import TracebackType

from kith import _bridge
from kith._generated import configure as configure_all
from kith._generated import reactor as gen_reactor
from kith._generated import types as gen_types
from kith._generated import version as gen_version
from kith._generated import worker as gen_worker
from kith.exceptions import KithError, KithStateError, check_error
from kith.worker import Worker


__all__ = ["Event", "Reactor"]


class Event(IntEnum):
    """Readiness event bits for :meth:`Reactor.add` and :meth:`Reactor.modify`."""

    IN = int(gen_reactor.kith_reactor_event.KITH_REACTOR_IN)
    OUT = int(gen_reactor.kith_reactor_event.KITH_REACTOR_OUT)
    HUP = int(gen_reactor.kith_reactor_event.KITH_REACTOR_HUP)
    ERR = int(gen_reactor.kith_reactor_event.KITH_REACTOR_ERR)


_ReadyCallback = Callable[[int, int], None]
_TaskCallback = Callable[[], None]

# close() waits this long for an in-flight run loop to exit before
# declaring the drain failed.
_DRAIN_TIMEOUT_S: float = 10.0


class Reactor:
    """Async event loop wrapping a ``kith_reactor_t`` handle.

    Python callbacks registered through :meth:`add`, :meth:`submit`, and
    :meth:`schedule` run on a worker pool, not on the reactor thread.
    The reactor thread calls C hop functions that submit the
    user's callables to the pool from C; worker threads run them.

    Args:
        max_fds: Maximum tracked file descriptors; 0 selects the default.
        task_capacity: Pre-allocated task node free list size; 0 selects the
            default.
        worker: A :class:`~kith.worker.Worker` pool to dispatch Python
            callbacks on, or ``None`` to create and own one with default
            settings. When passed in, the caller owns the pool's lifetime,
            and the callback registrations are released at that pool's
            close rather than at this reactor's.
        bridge: A loaded :class:`kith._bridge.Bridge`, or ``None`` to load the
            process-wide singleton.

    Raises:
        KithError: When the reactor handle or internal worker pool cannot be
            built.
        BridgeError: When the shared libraries cannot be loaded.

    Thread safety:
        @thread_safety safe — construction shares no mutable state across
        callers.

    Ownership:
        Owns its ``kith_reactor_t`` handle and, when no pool was passed,
        the internally-created :class:`~kith.worker.Worker`; :meth:`close`
        releases both. A passed-in pool stays the caller's to close.
    """

    __slots__ = (
        "_bridge",
        "_cbs",
        "_closed",
        "_deferred",
        "_handle",
        "_lifecycle_lock",
        "_release_hook_armed",
        "_run_exited",
        "_run_thread",
        "_worker",
        "_worker_owned",
    )

    def __init__(
        self,
        *,
        max_fds: int = 0,
        task_capacity: int = 0,
        worker: Worker | None = None,
        bridge: _bridge.Bridge | None = None,
    ) -> None:
        loaded = bridge if bridge is not None else _bridge.load()
        configure_all(loaded)
        self._bridge = loaded
        self._handle: object = None
        self._cbs: dict[int, object] = {}
        # Registrations whose release must wait for the dispatching pool's
        # drain: ``(trampoline, bridge_or_None)`` pairs. Queued tasks hold
        # raw trampoline pointers, so the pairs are freed by the pool's
        # close hook (or by close() itself for an owned pool, after its
        # drain).
        self._deferred: list[tuple[object, object | None]] = []
        # One-element cell so the release hook can reset the armed flag
        # without capturing the reactor itself: a hook that captures the
        # reactor pins it (and every reactor ever attached) to a shared
        # long-lived borrowed pool.
        self._release_hook_armed = [False]
        self._closed = True
        # Set whenever no run loop is in flight, mirroring the C-side
        # run_in_flight guard: close() blocks on this event so the handle
        # is never destroyed under a live kith_reactor_run.
        self._run_exited = threading.Event()
        self._run_exited.set()
        self._run_thread: threading.Thread | None = None
        # Serializes run() startup against close(): without it a close that
        # observes "no run in flight" can destroy the handle while a run
        # thread is between announcing itself and entering the C call.
        self._lifecycle_lock = threading.Lock()

        if worker is None:
            self._worker = Worker(bridge=loaded)
            self._worker_owned = True
        else:
            self._worker = worker
            self._worker_owned = False

        params = gen_reactor.kith_reactor_params_t(
            size=ctypes.sizeof(gen_reactor.kith_reactor_params_t),
            abi_version=gen_version.KITH_ABI_VERSION,
            max_fds=max_fds,
            task_capacity=task_capacity,
        )
        out = ctypes.POINTER(gen_reactor.kith_reactor_t)()
        rc = int(
            loaded.lib("reactor").kith_reactor_create(
                ctypes.byref(params),
                None,
                ctypes.byref(out),
            )
        )
        if rc != 0 and self._worker_owned:
            with contextlib.suppress(Exception):
                self._worker.close()
        check_error(rc, "kith_reactor_create")
        self._handle = out
        self._closed = False

    @property
    def handle(self) -> object:
        """The borrowed ``kith_reactor_t*`` for passing to a C function.

        Thread safety:
            @thread_safety safe — the handle is fixed after creation.
        """
        return self._handle

    @property
    def worker(self) -> Worker:
        """The worker pool dispatching this reactor's Python callbacks.

        Thread safety:
            @thread_safety safe — the pool is fixed after creation.
        """
        return self._worker

    # -----------------------------------------------------------------------
    # registration
    # -----------------------------------------------------------------------

    def add(self, fd: int, events: int, callback: _ReadyCallback) -> None:
        """Register interest in ``events`` on ``fd`` with a Python callback.

        The callback receives ``(fd, events)`` when readiness is detected,
        running on a worker thread (not the reactor thread). The caller
        retains ownership of ``fd``; the reactor does not close it.

        Args:
            fd: The file descriptor to watch.
            events: The readiness event mask, a bitwise combination of
                ``Event.IN``, ``Event.OUT``, ``Event.HUP``, and
                ``Event.ERR``.
            callback: Invoked with ``(fd, events)`` when readiness is
                detected.

        Raises:
            KithStateError: When ``fd`` is already registered (``KITH_EEXIST``),
                or when the reactor handle is closed.
            KithError: On an invalid argument (``KITH_EINVAL``).

        Thread safety:
            @thread_safety unsafe — registration races ``kith_reactor_run``;
            call before ``run()`` or from the reactor thread. The callback
            dispatches on the worker pool, not the reactor thread.
        """
        self._require_open()
        trampoline = gen_worker.kith_worker_ready_fn(lambda f, e, _ctx: callback(int(f), int(e)))
        bridge_out = ctypes.POINTER(gen_worker.kith_worker_bridge_t)()
        rc = int(
            self._bridge.lib("worker").kith_worker_bridge_create_ready(
                self._worker.handle,
                trampoline,
                None,
                ctypes.byref(bridge_out),
            )
        )
        check_error(rc, "kith_worker_bridge_create_ready")
        hop = self._ready_hop()
        rc = int(
            self._bridge.lib("reactor").kith_reactor_add(
                self._handle,
                ctypes.c_int(fd),
                ctypes.c_uint(events),
                hop,
                bridge_out,
            )
        )
        if rc != 0:
            self._bridge.lib("worker").kith_worker_bridge_destroy(bridge_out)
        check_error(rc, f"kith_reactor_add: fd={fd}")
        self._cbs[fd] = (trampoline, bridge_out)

    def modify(self, fd: int, events: int) -> None:
        """Change the events of interest on an already-registered ``fd``.

        Args:
            fd: The registered file descriptor to update.
            events: The replacement readiness event mask.

        Raises:
            KithStateError: When the reactor handle is closed.

        Thread safety:
            @thread_safety unsafe — call from the reactor thread or before
            ``run()``; registration races ``kith_reactor_run``.
        """
        self._require_open()
        rc = int(
            self._bridge.lib("reactor").kith_reactor_mod(
                self._handle,
                ctypes.c_int(fd),
                ctypes.c_uint(events),
            )
        )
        check_error(rc, f"kith_reactor_mod: fd={fd}")

    def remove(self, fd: int) -> None:
        """Deregister ``fd``. The caller still owns and must close ``fd``.

        The registration's bridge object is destroyed here — no readiness
        dispatch can read it once the deregistration returns — while the
        callback trampoline is released by the dispatching pool's close:
        queued readiness work holds the raw trampoline pointer until a
        worker executes it.

        Args:
            fd: The registered file descriptor to deregister.

        Raises:
            KithStateError: When the reactor handle is closed.

        Thread safety:
            @thread_safety unsafe — call from the reactor thread (or from
            within a readiness callback on the same reactor) or before ``run()``.
        """
        self._require_open()
        rc = int(
            self._bridge.lib("reactor").kith_reactor_del(
                self._handle,
                ctypes.c_int(fd),
            )
        )
        check_error(rc, f"kith_reactor_del: fd={fd}")
        entry = self._cbs.pop(fd, None)
        if isinstance(entry, tuple):
            bridge = entry[1]
            with contextlib.suppress(Exception):
                self._bridge.lib("worker").kith_worker_bridge_destroy(bridge)
            self._defer_trampoline(entry[0])

    # -----------------------------------------------------------------------
    # tasks and timers
    # -----------------------------------------------------------------------

    def submit(self, callback: _TaskCallback) -> None:
        """Enqueue ``callback`` to run on a worker thread.

        The callback runs on the worker pool, not the reactor thread.
        The caller's thread briefly enters Python to issue the
        submit (it is already in Python); the reactor thread is not involved.

        Args:
            callback: The no-argument callable enqueued to the worker pool.

        Raises:
            KithStateError: When the reactor handle is closed.

        Thread safety:
            @thread_safety safe — submits directly to the worker pool's
            mutex-protected task queue; callable from any thread.
        """
        self._require_open()
        trampoline = gen_worker.kith_worker_task_cb(lambda _ctx: callback())
        rc = int(
            self._bridge.lib("worker").kith_worker_submit(
                self._worker.handle,
                trampoline,
                None,
            )
        )
        check_error(rc, "kith_worker_submit")
        self._cbs[id(trampoline)] = trampoline

    def schedule(self, delay_ms: int, callback: _TaskCallback) -> None:
        """Schedule ``callback`` to run after ``delay_ms`` milliseconds.

        The timer fires on the reactor thread; the reactor's C hop submits
        the callback to the worker pool, and a worker thread runs it. The
        callback never runs on the reactor thread.

        ``delay_ms`` is a relative delay from the reactor's current monotonic
        clock; the absolute deadline is computed here before entering the C
        schedule call, whose contract is an absolute monotonic deadline.

        Args:
            delay_ms: The relative delay in milliseconds, measured on
                the reactor's monotonic clock.
            callback: The no-argument callable to run when the timer
                fires.

        Raises:
            KithStateError: When the reactor handle is closed.

        Thread safety:
            @thread_safety unsafe — timer registration races
            ``kith_reactor_run``; call before ``run()`` or from the reactor
            thread. The timer callback dispatches on the worker pool.
        """
        self._require_open()
        trampoline = gen_worker.kith_worker_task_cb(lambda _ctx: callback())
        bridge_out = ctypes.POINTER(gen_worker.kith_worker_bridge_t)()
        rc = int(
            self._bridge.lib("worker").kith_worker_bridge_create_task(
                self._worker.handle,
                trampoline,
                None,
                ctypes.byref(bridge_out),
            )
        )
        check_error(rc, "kith_worker_bridge_create_task")
        hop = self._task_hop()
        deadline_ms = self.now_ms() + delay_ms
        rc = int(
            self._bridge.lib("reactor").kith_reactor_schedule(
                self._handle,
                ctypes.c_uint64(deadline_ms),
                hop,
                bridge_out,
            )
        )
        if rc != 0:
            self._bridge.lib("worker").kith_worker_bridge_destroy(bridge_out)
        check_error(rc, "kith_reactor_schedule")
        self._cbs[id(trampoline)] = (trampoline, bridge_out)

    # -----------------------------------------------------------------------
    # loop
    # -----------------------------------------------------------------------

    def run(self) -> None:
        """Enter the event loop on the calling thread; returns after :meth:`stop`.

        While a run is in flight, :meth:`close` called from another thread
        requests shutdown and waits for this method to return before
        destroying the handle.

        Raises:
            KithError: On a reactor backend failure that aborts the loop, or
                when the handle is already closed (``KITH_ESTATE``).

        Thread safety:
            @thread_safety unsafe — only one thread may call ``run()`` on a
            given reactor.
        """
        with self._lifecycle_lock:
            if self._closed:
                raise KithStateError(
                    gen_types.kith_error.KITH_ESTATE,
                    "reactor handle is closed",
                )
            self._run_thread = threading.current_thread()
            self._run_exited.clear()
        try:
            rc = int(self._bridge.lib("reactor").kith_reactor_run(self._handle))
        finally:
            self._run_exited.set()
        check_error(rc, "kith_reactor_run")

    def stop(self) -> None:
        """Request the event loop to exit at its next run boundary.

        An in-flight run drains the current poll batch and returns; a
        request delivered before the next run returns that run on its first
        check. The run that returns consumes the request.

        Thread safety:
            @thread_safety safe — callable from any thread. Uses an atomic
            flag + platform wakeup.
        """
        self._bridge.lib("reactor").kith_reactor_stop(self._handle)

    def now_ms(self) -> int:
        """Return the reactor's monotonic clock in milliseconds.

        Thread safety:
            @thread_safety safe — returns an atomic snapshot.
        """
        return int(self._bridge.lib("reactor").kith_reactor_now_ms(self._handle))

    # -----------------------------------------------------------------------
    # internal
    # -----------------------------------------------------------------------

    def _ready_hop(self) -> object:
        """Return the C readiness hop as a ``kith_reactor_cb`` callable.

        The hop (``kith_worker_bridge_ready_cb``) is a C function in
        libkith_worker whose signature is structurally identical to
        ``kith_reactor_cb``. The function pointer is fetched from the worker
        library and wrapped in the reactor's callback type so ctypes passes
        it to ``kith_reactor_add`` without entering Python.
        """
        addr = ctypes.cast(
            self._bridge.lib("worker").kith_worker_bridge_ready_cb,
            ctypes.c_void_p,
        ).value
        assert addr is not None
        return gen_reactor.kith_reactor_cb(addr)

    def _task_hop(self) -> object:
        """Return the C task hop as a ``kith_reactor_task_cb`` callable.

        The hop (``kith_worker_bridge_task_cb``) is a C function in
        libkith_worker whose signature is structurally identical to
        ``kith_reactor_task_cb``.
        """
        addr = ctypes.cast(
            self._bridge.lib("worker").kith_worker_bridge_task_cb,
            ctypes.c_void_p,
        ).value
        assert addr is not None
        return gen_reactor.kith_reactor_task_cb(addr)

    def _require_open(self) -> None:
        """Raise ``KithStateError`` when the reactor handle is closed.

        The C functions validate a NULL handle, but the mapped error for a
        closed reactor is a state error, not a call error — and the wrapper's
        own contract is the one callers see.
        """
        if self._closed:
            raise KithStateError(
                gen_types.kith_error.KITH_ESTATE,
                "reactor handle is closed",
            )

    def _defer_trampoline(self, trampoline: object) -> None:
        """Move a registration's trampoline to the pool-close release.

        Queued tasks hold raw trampoline pointers until the dispatching
        pool's drain executes them, so the last Python reference must
        outlive this call.
        """
        self._deferred.append((trampoline, None))
        self._arm_release_hook()

    def _arm_release_hook(self) -> None:
        """Arm the deferred-release hook on the dispatching pool, once.

        The hook runs after the pool's drain and join (immediately when the
        pool is already closed), destroys the bag's bridge objects, and lets
        the bag drop the trampoline references. It captures the bag, the
        worker library, and the armed flag — never the reactor, which a
        shared long-lived borrowed pool would otherwise pin forever.
        """
        if self._release_hook_armed[0]:
            return
        self._release_hook_armed[0] = True
        deferred = self._deferred
        armed = self._release_hook_armed
        worker_lib = self._bridge.lib("worker")

        def _release() -> None:
            armed[0] = False
            for entry in deferred:
                bridge = entry[1]
                if bridge is not None:
                    with contextlib.suppress(Exception):
                        worker_lib.kith_worker_bridge_destroy(bridge)
            deferred.clear()

        self._worker._register_close_hook(_release)

    # -----------------------------------------------------------------------
    # teardown
    # -----------------------------------------------------------------------

    def close(self) -> None:
        """Release the reactor handle, bridge registrations, and owned worker.

        Idempotent. If a run loop is in flight on another thread, requests
        shutdown and blocks until ``kith_reactor_run`` has returned before
        destroying the handle, so ``kith_reactor_destroy`` is never called
        while the run loop is in flight (its ``@thread_safety unsafe``
        contract, enforced by a runtime abort in the C layer). Calling
        :meth:`close` from the run-loop thread itself re-enters during a
        blocking run and is not supported; in that case the method does not
        wait (it cannot join its own thread) and proceeds to destroy, which
        trips the C runtime assertion.

        The C reactor handle is destroyed first: no timer or readiness
        dispatch can fire afterward, so the C bridge objects stop being read
        and the pool stops receiving hop submissions. The registrations are
        released only after every queued callback has executed — an owned
        pool drains inside this call (``kith_worker_destroy`` executes
        queued tasks, then joins the threads, so the drain runs against
        live trampolines), while a borrowed pool is the caller's to close
        and performs the release at its own close, after its drain.

        Raises:
            KithError: When the run loop does not exit within the drain
                window (``KITH_ETIMEDOUT``).

        Thread safety:
            @thread_safety safe-if no run is in flight, or the run is in
            flight on another thread (``close`` requests shutdown and blocks
            until the run loop exits before destroying the handle). Unsafe
            when called from the run-loop thread itself while a run is in
            flight: the wait is skipped (a thread cannot join itself) and
            the C runtime assertion catches the contract violation.
        """
        # The lifecycle lock makes the in-flight decision atomic against
        # run() startup: either this close observes the announcement and
        # waits below, or a concurrent run() has not started yet and its
        # lock acquisition after destroy refuses via the closed flag.
        with self._lifecycle_lock:
            if self._closed:
                return
            # Block until the run loop has exited so kith_reactor_destroy is
            # never called while kith_reactor_run is in flight. The event is
            # cleared on run() entry and set on every return path; a set event
            # means no run is in flight.
            if (
                not self._run_exited.is_set()
                and self._run_thread is not None
                and threading.current_thread() is not self._run_thread
            ):
                # One request covers the announcement window: the stop takes
                # effect at the run loop's next boundary, and a loop that has
                # not entered yet returns on its first check.
                self.stop()
                if not self._run_exited.wait(timeout=_DRAIN_TIMEOUT_S):
                    raise KithError(
                        gen_types.kith_error.KITH_ETIMEDOUT,
                        "reactor run loop did not exit within the drain window",
                    )
            self._bridge.lib("reactor").kith_reactor_destroy(self._handle)
            self._handle = None
            if self._worker_owned:
                # The drain executes queued tasks against the still-referenced
                # trampolines and joins every worker; the pool's close hooks
                # release the deferred bag once the join completes. The
                # registrations _cbs still holds are released here.
                self._worker.close()
                for entry in self._cbs.values():
                    if isinstance(entry, tuple):
                        bridge = entry[1]
                        with contextlib.suppress(Exception):
                            self._bridge.lib("worker").kith_worker_bridge_destroy(bridge)
                self._cbs.clear()
            else:
                # The caller owns the pool's lifetime: queued tasks hold raw
                # trampoline pointers until the pool's own drain executes
                # them, so the registrations move to the pool-close release
                # instead of being freed here.
                for entry in self._cbs.values():
                    if isinstance(entry, tuple):
                        self._deferred.append((entry[0], entry[1]))
                    else:
                        self._deferred.append((entry, None))
                self._cbs.clear()
                self._arm_release_hook()
            self._closed = True

    def __enter__(self) -> Reactor:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()

    def __del__(self) -> None:
        if getattr(self, "_closed", True):
            return
        with contextlib.suppress(Exception):
            self.close()
