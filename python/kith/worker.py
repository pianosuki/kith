"""Worker pool wrapper.

A :class:`Worker` owns a C ``kith_worker_t`` handle: a fixed-size pool of
native threads consuming a bounded task queue. The reactor thread submits
Python-bound callbacks to the pool through the C bridge (the
``kith_worker_bridge_*`` family in libkith_worker) so the reactor thread
never enters the Python interpreter. Worker threads run the
callbacks and post reactor-side completion work back via
``kith_reactor_submit``.

The pool is sized 1 under standard Python (the GIL serializes Python
execution) and N under free-threaded Python (real parallelism). The size is
a creation parameter, not a runtime tunable.
"""

from __future__ import annotations

import contextlib
import ctypes
from collections.abc import Callable
from types import TracebackType

from kith import _bridge
from kith._generated import configure as configure_all
from kith._generated import version as gen_version
from kith._generated import worker as gen_worker
from kith.exceptions import check_error


__all__ = ["Worker"]


class Worker:
    """GIL-aware worker pool wrapping a ``kith_worker_t`` handle.

    Args:
        worker_count: Number of worker threads; 0 selects the C default
            (1 worker). Free-threaded deployments size the pool explicitly
            through the server's ``python_worker_count`` config field.
            Values above 64 are clamped to 64 by the C layer.
        task_capacity: Pre-allocated task node count; 0 selects the
            default (4096). Each in-flight task consumes one node; a submit
            beyond this raises ``KithStateError`` (``KITH_EBUSY``).
        bridge: A loaded :class:`kith._bridge.Bridge`, or ``None`` to load
            the process-wide singleton.

    Raises:
        KithError: When the pool handle cannot be built.
        BridgeError: When the shared libraries cannot be loaded.

    Thread safety:
        @thread_safety safe — construction shares no mutable state across
        callers.

    Ownership:
        Owns its ``kith_worker_t`` handle; :meth:`close` releases it.
    """

    __slots__ = ("_bridge", "_close_hooks", "_closed", "_handle")

    def __init__(
        self,
        *,
        worker_count: int = 0,
        task_capacity: int = 0,
        bridge: _bridge.Bridge | None = None,
    ) -> None:
        loaded = bridge if bridge is not None else _bridge.load()
        configure_all(loaded)
        self._bridge = loaded
        self._handle: object = None
        self._closed = True
        self._close_hooks: list[Callable[[], None]] = []

        params = gen_worker.kith_worker_params_t(
            size=ctypes.sizeof(gen_worker.kith_worker_params_t),
            abi_version=gen_version.KITH_ABI_VERSION,
            worker_count=worker_count,
            task_capacity=task_capacity,
        )
        out = ctypes.POINTER(gen_worker.kith_worker_t)()
        rc = int(
            loaded.lib("worker").kith_worker_create(
                ctypes.byref(params),
                None,
                ctypes.byref(out),
            )
        )
        check_error(rc, "kith_worker_create")
        self._handle = out
        self._closed = False

    @property
    def handle(self) -> object:
        """The borrowed ``kith_worker_t*`` for passing to a C function.

        Thread safety:
            @thread_safety safe — the handle is fixed after creation.
        """
        return self._handle

    def worker_count(self) -> int:
        """Return the number of worker threads in the pool (immutable).

        Thread safety:
            @thread_safety safe — the count is immutable.
        """
        return int(self._bridge.lib("worker").kith_worker_count(self._handle))

    def close(self) -> None:
        """Release the pool handle. Idempotent.

        Signals every worker thread to drain remaining queued tasks and exit,
        joins each thread, and frees the pool. No ``kith_worker_submit`` may
        be in flight on another thread when this is called. Release hooks
        registered through :meth:`_register_close_hook` run after the join,
        so a registration held only through raw C pointers is freed once no
        queued task can still reach it.

        Thread safety:
            @thread_safety unsafe — no ``kith_worker_submit`` may be in
            flight on another thread when this is called.
        """
        if self._closed:
            return
        self._bridge.lib("worker").kith_worker_destroy(self._handle)
        self._handle = None
        self._closed = True
        hooks = self._close_hooks
        self._close_hooks = []
        for hook in hooks:
            with contextlib.suppress(Exception):
                hook()

    def _register_close_hook(self, hook: Callable[[], None]) -> None:
        """Register a callback to run after this pool's drain and join.

        Reactor registrations release their C bridge objects here: queued
        tasks hold raw trampoline pointers until the drain executes them, so
        the release must follow the join rather than precede it. Registering
        on an already-closed pool runs the hook immediately — the drain has
        completed, so the release is safe now.
        """
        if self._closed:
            hook()
            return
        self._close_hooks.append(hook)

    def __enter__(self) -> Worker:
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
