"""Public API for the kith framework.

:mod:`kith` is the user-facing entry point. The :class:`Server` facade wraps
the C composition-root lifecycle (create, run, shutdown, destroy, status): it
loads the framework shared libraries through the ctypes bridge, attaches the
generated bindings, builds a size-versioned creation struct, and drives the C
run loop. C error codes are translated to :class:`KithError` at the boundary
so user code never sees a raw integer outcome.

The exception hierarchy lives in :mod:`kith.exceptions` and the typed
configuration source wrapper lives in :mod:`kith.config`. The per-plane
submodules (reactor, proto, sim, fabric, gateway, coord, control, aoi)
and the persistence and dispatch surfaces (:mod:`kith.db`,
:mod:`kith.worker`) wrap their generated bindings' C surfaces into
Python types at the boundary; this module carries the facade and
re-exports the public names.
"""

from __future__ import annotations

import contextlib
import ctypes
import importlib.metadata
import signal
import threading
from collections.abc import Callable
from enum import IntEnum
from pathlib import Path
from types import FrameType, TracebackType
from typing import Final

from kith import _bridge
from kith._generated import configure as configure_all
from kith._generated import server as gen_server
from kith._generated import types as gen_types
from kith._generated import version as gen_version
from kith._generated.server import kith_server_params_t, kith_server_t
from kith._server.control import ServerControlRoutes as ServerControlRoutes
from kith._server.publication import ServerPublication as ServerPublication
from kith._server.registration import ServerRegistration as ServerRegistration
from kith._server.state import ServerState as ServerState
from kith.aoi import Aoi
from kith.config import Config
from kith.control import (
    Control,
    Method,
    Request,
    Response,
)
from kith.coord import Coord, CoordBus
from kith.db import Database, Reply, Transaction
from kith.exceptions import (
    ABIVersionError,
    BridgeError,
    BridgeLoadError,
    KithConfigError,
    KithError,
    KithNetworkError,
    KithNotFoundError,
    KithProtocolError,
    KithResponseOverflowError,
    KithStateError,
    check_error,
)
from kith.fabric import CellKey, Fabric, Subscription
from kith.gateway import (
    DeliveryTotals,
    Gateway,
    PinnedSession,
    Session,
    SessionInfo,
    SessionType,
)
from kith.proto import Proto
from kith.reactor import Reactor
from kith.sim import (
    Actor,
    ArtifactKey,
    Sim,
    SimInput,
    SimModel,
    SimModelConfig,
)
from kith.worker import Worker


_ServerControlRoutes = ServerControlRoutes
_ServerPublication = ServerPublication
_ServerRegistration = ServerRegistration
_ServerState = ServerState


__all__ = [
    "ABIVersionError",
    "Actor",
    "Aoi",
    "ArtifactKey",
    "BridgeError",
    "BridgeLoadError",
    "CellKey",
    "Config",
    "Control",
    "Coord",
    "CoordBus",
    "Database",
    "DeliveryTotals",
    "Fabric",
    "Gateway",
    "KithConfigError",
    "KithError",
    "KithNetworkError",
    "KithNotFoundError",
    "KithProtocolError",
    "KithResponseOverflowError",
    "KithStateError",
    "Method",
    "PinnedSession",
    "Proto",
    "Reactor",
    "Reply",
    "Request",
    "Response",
    "Server",
    "ServerStatus",
    "Session",
    "SessionInfo",
    "SessionType",
    "Sim",
    "SimInput",
    "SimModel",
    "SimModelConfig",
    "Subscription",
    "Topology",
    "Transaction",
    "Worker",
]


# The installed distribution's version when the package was installed; a
# from-source checkout (no installed metadata) falls back to the generated
# constant, which tracks the C header the bindings were generated from.
try:
    __version__ = importlib.metadata.version("kith-fw")
except importlib.metadata.PackageNotFoundError:
    __version__ = gen_version.KITH_VERSION_STRING


# ---------------------------------------------------------------------------
# public enums (mirror the C enum values, sourced from the generated bindings
# so the Python side tracks the headers through one definition).
# ---------------------------------------------------------------------------


class Topology(IntEnum):
    """Runtime topology the composition root wires.

    Attributes:
        DISTRIBUTED: Planes scale independently across instances, connected
            through the coordination bus.
        EMBEDDED: Every plane runs in one process; the coordinator owns every
            cell and the fabric uses in-memory storage.
    """

    DISTRIBUTED = int(gen_server.kith_server_topology.KITH_SERVER_TOPOLOGY_DISTRIBUTED)
    EMBEDDED = int(gen_server.kith_server_topology.KITH_SERVER_TOPOLOGY_EMBEDDED)


class ServerStatus(IntEnum):
    """Lifecycle state observed through :attr:`Server.status`."""

    CREATED = int(gen_server.kith_server_status.KITH_SERVER_STATUS_CREATED)
    RUNNING = int(gen_server.kith_server_status.KITH_SERVER_STATUS_RUNNING)
    DRAINING = int(gen_server.kith_server_status.KITH_SERVER_STATUS_DRAINING)
    STOPPED = int(gen_server.kith_server_status.KITH_SERVER_STATUS_STOPPED)


_TOPOLOGY_BY_NAME: Final[dict[str, Topology]] = {
    "distributed": Topology.DISTRIBUTED,
    "embedded": Topology.EMBEDDED,
}

# The shutdown signals serve() coordinates: blocked in the calling thread
# and waited on there while a worker drives the run loop. Framework-created
# threads block both from entry, so delivery lands on the waiter.
_SHUTDOWN_SIGNALS: Final[frozenset[int]] = frozenset({signal.SIGINT, signal.SIGTERM})
_SERVE_POLL_SECONDS: Final[float] = 0.5

# PyErr_CheckSignals runs the installed Python signal handlers on the calling
# thread; a -1 return means a handler raised and the error indicator holds
# the exception.
_PYTHONAPI: Final = ctypes.pythonapi
_PYTHONAPI.PyErr_CheckSignals.restype = ctypes.c_int
_PYTHONAPI.PyErr_CheckSignals.argtypes = []
_PYTHONAPI.PyErr_Clear.restype = None
_PYTHONAPI.PyErr_Clear.argtypes = []


class _SignalPump:
    """Deliver pending signals to a run loop on the interpreter main thread.

    CPython raises ``KeyboardInterrupt`` and runs signal handlers only in
    the main thread's eval loop; while that thread sits inside the blocking
    ``kith_server_run`` call, the registered poll observer is the only thing
    that executes it. The observer pumps :c:func:`PyErr_CheckSignals` once
    per tick. A handler that raised leaves the exception in the error
    indicator, where it cannot cross the C run loop, so it is dropped and
    the graceful drain requested instead.

    Signals still at their default disposition get a graceful-shutdown shim:
    ``SIGINT``'s default raises KeyboardInterrupt (unreachable here) and
    ``SIGTERM``'s default hard-kills the process, so neither drains on its
    own. Dispositions the embedder set are left untouched for the pump to
    invoke. Every replaced disposition is restored when the run returns.
    """

    __slots__ = ("_observer", "_previous", "_server")

    def __init__(self, server: Server) -> None:
        self._server = server
        self._previous: dict[int, Callable[[int, FrameType | None], None] | int] = {}

        def _observe(_user_data: object) -> int:
            if _PYTHONAPI.PyErr_CheckSignals() != 0:
                _PYTHONAPI.PyErr_Clear()
                return 1
            return 0

        self._observer: Final = gen_server.kith_server_poll_fn(_observe)

    @property
    def observer(self) -> object:
        """The CFUNCTYPE trampoline registered as the C poll observer.

        The instance must stay referenced for the whole run; the C side
        stores the raw pointer.
        """
        return self._observer

    def install(self) -> None:
        """Fit the default-disposition shims for the shutdown signals."""
        for sig in sorted(_SHUTDOWN_SIGNALS):
            current = signal.getsignal(sig)
            if current is signal.SIG_IGN:
                continue
            if (
                current is None
                or current is signal.SIG_DFL
                or current is signal.default_int_handler
            ):
                # A None read means the handler was never set from Python;
                # SIG_DFL is the restorable equivalent.
                self._previous[sig] = signal.SIG_DFL if current is None else current
                signal.signal(sig, self._shutdown_shim)

    def restore(self) -> None:
        """Restore the dispositions replaced at install time."""
        while self._previous:
            sig, handler = self._previous.popitem()
            signal.signal(sig, handler)

    def _shutdown_shim(self, _signum: int, _frame: FrameType | None) -> None:
        self._server.shutdown()


# ---------------------------------------------------------------------------
# server facade
# ---------------------------------------------------------------------------


class Server(_ServerControlRoutes, _ServerPublication):
    """Composition-root facade over the kith C server lifecycle.

    Construction loads the framework shared libraries through the ctypes
    bridge, attaches the generated bindings, optionally builds a typed
    configuration source from a ``KEY=VALUE`` file, and creates the C server
    handle. The handle starts in the CREATED state; :meth:`run` transitions it
    to RUNNING and blocks until :meth:`shutdown` is requested (from another
    thread or a signal handler) and the drain completes.

    The C server handle owns every plane handle (gateway, sim, fabric,
    coordination, control, and the persistence pool) and exposes them as
    borrowed references through its plane accessors. The registration
    methods (:meth:`register_message_handler`, :meth:`register_tick_handler`,
    :meth:`register_sim_model`, :meth:`register_zone`, :meth:`register_query`,
    :meth:`register_control_route`) call the plane functions on those
    borrowed handles directly; they never wrap the handles in owning
    Gateway/Sim objects (whose close would destroy the server's plane), and
    they never destroy the borrowed handle. Wire types the gateway decodes
    are registered on the borrowed proto before :meth:`run` so the decoder
    accepts frames of those types during the run loop.

    The handle is released by :meth:`close` (also invoked by the context
    manager and ``__del__``); the server handle is destroyed first — joining
    the handler worker pool so no dispatch can be inside game code — then any
    model instances the facade created on the borrowed sim handle, then the
    borrowed configuration source. If :meth:`run` is in flight on another
    thread when :meth:`close` is called, :meth:`close` requests shutdown and
    blocks until the run loop has exited before destroying the handle, so the
    C ``kith_server_destroy`` ``@thread_safety unsafe`` contract is honored
    without a manual shutdown-and-join by the caller.

    Example::

        with kith.Server(config="game.env", topology="distributed") as server:
            server.run()

    Args:
        config: Path to a ``KEY=VALUE`` file (optional), or an existing
            :class:`kith.config.Config`. The server reads the ``tick_hz``,
            ``listen_port``, and ``listen_host`` keys for arguments left at
            their defaults; an explicit argument wins. Every other key in
            the source is the caller's own to read.
        topology: ``"distributed"`` (the default) or ``"embedded"``, or a
            :class:`Topology` member.
        instance_id: Local instance identifier (0 selects the embedded
            single-instance identity).
        listen_host: Gateway listen host, or ``None`` to bind all interfaces.
        listen_port: Gateway listen port; 0 (the default) selects an
            OS-assigned ephemeral port, readable via :attr:`listen_port`.
        tick_hz: Target server tick rate in Hz; 0 uses the configuration
            source or the 20 Hz default. Rates 1..1000 use a
            millisecond-resolution rational schedule and skip
            missed deadlines without catch-up bursts. Larger rates retain
            legacy best-effort scheduling. Applications choose model-step
            durations separately; see ``docs/guides/server_timing.md``.
        python_workers: Python handler worker pool size; 0 selects the default
            (1 under the GIL).
        replication_type_id: Message type id the gateway uses to encode
            per-subject actor state replication frames. Register this type on
            the borrowed proto before :meth:`run` (via
            :meth:`register_proto_type`, or by registering all wire types up
            front). 0 disables per-subject
            delivery encoding; the caller composes and enframes replication
            itself.
        replication_batch_type_id: Message type id for multi-subject batch
            replication frames. When non-zero, the gateway packs the full
            view set into one frame per refresh instead of one frame per
            subject; ``replication_type_id`` is ignored. Register this type
            on the borrowed proto before ``run``. 0 selects per-subject
            delivery.
        delivery_strategy: Registered delivery strategy name the gateway
            resolves for every session (``"full"``, ``"tiered"``, or a
            game-registered name); ``None`` selects the factory default.
            Unknown names fail session creation, not construction.
        delivery_config: Strategy configuration as bytes (a size-versioned
            struct image whose leading ``uint32`` declares its length), or
            ``None`` for the strategy's documented defaults. The schema is
            strategy-specific; :func:`kith.gateway.tiered_delivery_config`
            builds the ``"tiered"`` image.
        delivery_workers: Delivery executor thread count forwarded to the
            gateway. 0 keeps gateway delivery inline on the
            reactor thread; a non-zero count moves the tick's deliver pass
            onto executor threads.
        delivery_wait_budget_us: Per-pass compose-wait budget in
            microseconds forwarded to the gateway. 0 selects
            the gateway default; consulted only when ``delivery_workers``
            is non-zero.
        handler_table_size: Gateway handler-table capacity (number of
            message-type slots). The gateway dispatches by direct index, so
            this must exceed the largest message type id the game registers.
            0 selects the gateway default (256), which covers framework
            types but not user types (ids >= 1000); a game using user types
            sets this to cover its highest type id.
        control_write_buffer_cap: Control-plane per-connection response
            write buffer capacity in bytes. 0 selects the control plane
            default (262144); a route response larger than the capacity
            answers the canonical counted rejection (see
            ``docs/guides/operations.md``), so size the capacity for the
            largest listing a route returns.
        view_max_subjects: Per-subscriber view-set subject capacity
            forwarded to the gateway. 0 selects the gateway default (512);
            the subscribing subject occupies one slot of the set, and
            candidates past the budget drop from the delivered individual
            set — the view candidate and selected high-watermark gauges
            report the approaching-the-cap evidence. Size the capacity for
            the densest view a subscriber's window is expected to cover.
        view_refresh_interval_ms: View compose-and-deliver interval in
            milliseconds forwarded to the gateway. 0 selects the tick
            interval (the composition root's derivation — a refresh
            interval wider than the tick leaves inputs published inside
            the gap undelivered until the next refresh); an explicit value
            passes through unclamped and owns that trade.
        cache_refresh_interval_ms: Cell-cache refresh interval in
            milliseconds forwarded to the gateway. 0 selects the tick
            interval; an explicit value passes through unclamped.

    Raises:
        KithError: When the C server or configuration source cannot be built.
        BridgeError: When the framework shared libraries cannot be
            located or their ABI generation differs from the package's.

    Thread safety:
        @thread_safety safe — construction shares no mutable state across
        callers.
    """

    __slots__ = ()

    def __init__(
        self,
        *,
        config: str | Path | Config | None = None,
        topology: str | Topology = "distributed",
        instance_id: int = 0,
        listen_host: str | None = None,
        listen_port: int = 0,
        tick_hz: int = 0,
        python_workers: int = 0,
        replication_type_id: int = 0,
        replication_batch_type_id: int = 0,
        delivery_strategy: str | None = None,
        delivery_config: bytes | None = None,
        delivery_workers: int = 0,
        delivery_wait_budget_us: int = 0,
        handler_table_size: int = 0,
        control_write_buffer_cap: int = 0,
        view_max_subjects: int = 0,
        view_refresh_interval_ms: int = 0,
        cache_refresh_interval_ms: int = 0,
    ) -> None:
        bridge = _bridge.load()
        # configure() attaches argtypes/restype to every loaded module's
        # KITH_API surface; it is idempotent across Server instances.
        configure_all(bridge)

        self._bridge = bridge
        self._handle: ctypes._Pointer[kith_server_t] | None = None
        self._config: Config | None = None
        self._closed: bool = True
        # Set when no run loop is in flight (the default state). Cleared on
        # entry to run() and re-set when run() returns, so close() can block
        # until the run loop has exited before calling kith_server_destroy.
        self._run_exited: threading.Event = threading.Event()
        self._run_exited.set()
        # The thread currently inside run(), or None when no run is in flight.
        # Used by close() to avoid joining its own thread.
        self._run_thread: threading.Thread | None = None
        # Serializes run() startup against close(): without it a close that
        # observes "no run in flight" can destroy the handle while a run
        # thread is between announcing itself and entering the C call.
        self._lifecycle_lock = threading.Lock()
        self._callback_context = threading.local()
        # Borrowed-view model instances the facade created on the server's
        # sim handle; destroyed on close before the server is released.
        self._models: list[SimModel] = []
        # ctypes callback keep-alive slots: the C side holds a raw function pointer,
        # so the Python callable must stay referenced for the registration's
        # lifetime.
        self._handlers: dict[int, tuple[object, object]] = {}
        self._destroyed_trampoline: object | None = None
        self._destroyed_handler: Callable[[SessionInfo], None] | None = None
        # The per-tick callback is a single slot (the C side holds the raw
        # trampoline pointer for the registration's lifetime).
        self._tick_handler: tuple[object, object] | None = None
        self._route_handlers: dict[int, object] = {}
        # zone name -> assigned numeric zone id, set by register_zone.
        self._zones: dict[str, int] = {}

        owned_config: Config | None = None
        borrowed_handle: object = None
        if config is None:
            borrowed_handle = None
        elif isinstance(config, Config):
            borrowed_handle = config.handle
        else:
            owned_config = Config(file_path=config, bridge=bridge)
            borrowed_handle = owned_config.handle
        self._config = owned_config

        topo = self._topology_value(topology)
        # The config image is copied by the C server during create, so the
        # buffer only has to stay referenced through the call below; a local
        # holds it because ctypes does not keep a c_void_p field's source
        # object alive on its own.
        config_buffer: ctypes.Array[ctypes.c_char] | None = (
            ctypes.create_string_buffer(delivery_config, len(delivery_config))
            if delivery_config is not None
            else None
        )
        cfg = kith_server_params_t(
            size=ctypes.sizeof(kith_server_params_t),
            abi_version=gen_version.KITH_ABI_VERSION,
            config=borrowed_handle,
            topology=topo,
            instance_id=instance_id,
            listen_host=listen_host.encode("utf-8") if listen_host else None,
            listen_port=listen_port,
            tick_hz=tick_hz,
            python_worker_count=python_workers,
            handler_table_size=ctypes.c_uint32(handler_table_size),
            replication_type_id=ctypes.c_uint16(replication_type_id),
            replication_batch_type_id=ctypes.c_uint16(replication_batch_type_id),
            delivery_strategy=(
                ctypes.c_char_p(delivery_strategy.encode("utf-8"))
                if delivery_strategy is not None
                else None
            ),
            delivery_config=(
                ctypes.cast(config_buffer, ctypes.c_void_p) if config_buffer is not None else None
            ),
            delivery_worker_count=delivery_workers,
            delivery_wait_budget_us=delivery_wait_budget_us,
            control_write_buffer_cap=ctypes.c_uint32(control_write_buffer_cap),
            view_max_subjects=ctypes.c_uint32(view_max_subjects),
            view_refresh_interval_ms=ctypes.c_uint32(view_refresh_interval_ms),
            cache_refresh_interval_ms=ctypes.c_uint32(cache_refresh_interval_ms),
        )
        out = ctypes.POINTER(kith_server_t)()
        rc = int(bridge.server().kith_server_create(ctypes.byref(cfg), None, ctypes.byref(out)))
        if rc != 0:
            if owned_config is not None:
                owned_config.close()
            check_error(rc, "kith_server_create")
        self._handle = out
        self._closed = False

    # -----------------------------------------------------------------------
    # construction helpers
    # -----------------------------------------------------------------------

    @staticmethod
    def _topology_value(topology: str | Topology) -> int:
        if isinstance(topology, Topology):
            return int(topology)
        name = topology.lower()
        if name not in _TOPOLOGY_BY_NAME:
            raise KithError(
                gen_types.kith_error.KITH_EINVAL,
                f"unknown topology {topology!r}; expected 'distributed' or 'embedded'",
            )
        return int(_TOPOLOGY_BY_NAME[name])

    # -----------------------------------------------------------------------
    # lifecycle
    # -----------------------------------------------------------------------

    def run(self) -> None:
        """Enter the server run loop and block until shutdown completes.

        Drives the reactor and the tick-maintenance cadence at the configured
        tick rate (coordination-bus drain, coordination tick, gateway refresh
        and delivery); the simulation step itself belongs to the registered
        tick callback. Returns once :meth:`shutdown` has been requested and the
        drain window has elapsed.

        Tracks the in-flight run so :meth:`close` can block until the loop has
        exited before calling ``kith_server_destroy``. The C destroy contract
        is ``@thread_safety unsafe`` while a run is in flight; the facade
        enforces it from Python by joining the run before teardown.

        When :meth:`run` is called on the interpreter's main thread, the
        facade registers a poll observer that pumps pending signals once per
        tick (the bounded exception to the tick path's no-Python rule):
        CPython runs signal handlers
        and raises ``KeyboardInterrupt`` only in the main thread's eval loop,
        which sits inside the blocking C call here. Signals at their default
        disposition are fitted with a graceful-shutdown shim for the duration
        of the run — Ctrl-C and SIGTERM drain instead of doing nothing or
        killing the process — and dispositions an embedder set are invoked by
        the pump as installed. A handler that raises ends the run with a
        graceful drain rather than an exception unwind across live C frames;
        every replaced disposition is restored when the run returns. Runs on
        any other thread are unchanged: signal delivery there is the
        embedder's arrangement (:meth:`serve` is the standalone pattern).

        Raises:
            KithError: When the handle is NULL, a run is already in flight,
                or a reactor or plane failure aborts the loop. Raises
                ``KithStateError`` when the handle is already closed.

        Thread safety:
            @thread_safety unsafe — exactly one run loop may be active on a
            given server.
        """
        with self._lifecycle_lock:
            if self._closed:
                raise KithStateError(
                    gen_types.kith_error.KITH_ESTATE,
                    "server handle is closed",
                )
            self._run_exited.clear()
            self._run_thread = threading.current_thread()
        pump: _SignalPump | None = None
        if threading.current_thread() is threading.main_thread():
            pump = _SignalPump(self)
            rc = int(
                self._bridge.server().kith_server_register_poll_observer(
                    self._handle, pump.observer, None
                )
            )
            check_error(rc, "kith_server_register_poll_observer")
            pump.install()
        try:
            rc = int(self._bridge.server().kith_server_run(self._handle))
            check_error(rc, "kith_server_run")
        finally:
            if pump is not None:
                pump.restore()
                self._bridge.server().kith_server_register_poll_observer(
                    self._handle, gen_server.kith_server_poll_fn(), None
                )
            self._run_thread = None
            self._run_exited.set()

    def shutdown(self) -> None:
        """Request graceful shutdown of the run loop.

        Safe to call from a thread other than the run-loop thread or from a
        signal handler. Idempotent: calling on a handle that is already
        draining or stopped, or that never ran, returns without error.

        Thread safety:
            @thread_safety safe — may be called concurrently with
            :meth:`run` and from a signal handler.
        """
        rc = int(self._bridge.server().kith_server_shutdown(self._handle))
        check_error(rc, "kith_server_shutdown")

    def serve(self) -> None:
        """Run the server on a worker thread until SIGINT or SIGTERM.

        Starts :meth:`run` on a non-daemon worker thread and blocks the
        calling thread waiting for SIGINT or SIGTERM; on delivery it calls
        :meth:`shutdown` and returns while the drain completes on the
        worker (joining stays the job of :meth:`close`, which waits for
        the run loop before destroying the handle). This is how a
        standalone process hosts a server: Python signal handlers and
        ``KeyboardInterrupt`` execute only in the main thread's eval loop,
        which sits inside the C call for the whole run, so neither reaches
        user code while :meth:`run` occupies that thread.

        The calling thread blocks both signals before the worker is created
        — threads inherit the creating thread's mask, and every
        framework-created thread blocks them from entry anyway — then waits
        for one of them, so delivery lands deterministically on this wait.
        POSIX-only mechanics (:func:`signal.pthread_sigmask` and
        :func:`signal.sigtimedwait`). The blocked mask is left in place
        after return: teardown, including any replay-artifact flush in
        :meth:`close`, proceeds undisturbed by a second Ctrl-C.

        The wait also ends when the worker exits without a signal; a failed
        run loop surfaces here as the raised exception instead of a hang.

        Raises:
            KithError: When the handle is closed, a run loop is already in
                flight, or the worker thread's run loop failed.
            BaseException: Whatever else the run loop raised, re-raised
                from the calling thread.

        Thread safety:
            @thread_safety unsafe — exactly one serve/run loop per server;
            call from one thread (the intended caller is the process main
            thread).
        """
        if self._closed or self._handle is None:
            raise KithError(
                gen_types.kith_error.KITH_EINVAL,
                "server handle is closed",
            )
        if self._run_thread is not None:
            raise KithError(
                gen_types.kith_error.KITH_EINVAL,
                "a run loop is already in flight",
            )
        signal.pthread_sigmask(signal.SIG_BLOCK, _SHUTDOWN_SIGNALS)
        failures: list[BaseException] = []

        def _run_on_worker() -> None:
            try:
                self.run()
            except BaseException as exc:
                failures.append(exc)
                raise

        worker = threading.Thread(target=_run_on_worker, name="kith-server-run")
        worker.start()
        while True:
            try:
                delivered = signal.sigtimedwait(_SHUTDOWN_SIGNALS, _SERVE_POLL_SECONDS)
            except InterruptedError:
                continue
            if delivered is not None or not worker.is_alive():
                break
        self.shutdown()
        if failures:
            raise failures[0]

    @property
    def status(self) -> ServerStatus:
        """Current lifecycle state of the server handle.

        After :meth:`close`, reports :attr:`ServerStatus.STOPPED`: the handle
        has been released and the loop has stopped. While the handle
        is live, the value is read from the C composition root.

        Thread safety:
            @thread_safety safe.
        """
        if self._closed:
            return ServerStatus.STOPPED
        raw = int(self._bridge.server().kith_server_status(self._handle))
        return ServerStatus(raw)

    @property
    def listen_port(self) -> int:
        """Actual TCP port the gateway listener is bound to.

        When constructed with ``listen_port=0`` (the default), the OS assigns
        an ephemeral port at bind time; this reads it back from the C handle
        so a caller that bound ephemerally can connect to or advertise the real
        endpoint. Returns 0 after :meth:`close` or when the listener is not
        bound.

        Thread safety:
            @thread_safety safe — the listener fd and bound port are fixed
            after server creation.
        """
        if self._closed:
            return 0
        return int(self._bridge.server().kith_server_listen_port(self._handle))

    @property
    def control_port(self) -> int:
        """Actual TCP port the control-plane listener is bound to.

        The composition root starts the control plane (when the library is
        linked into the build) on an OS-assigned ephemeral port; this reads
        it back so a caller can reach the HTTP routes registered via
        :meth:`register_control_route`. Returns 0 after :meth:`close`, when
        the control plane library is not linked into the build, or when no
        control plane is configured.

        Thread safety:
            @thread_safety safe — the listener fd and bound port are fixed
            after server creation.
        """
        if self._closed:
            return 0
        return int(self._bridge.server().kith_server_control_port(self._handle))

    @property
    def native_handle(self) -> int:
        """Borrow the server's native address for an application-owned C library.

        Pass this address as ``kith_server_t *`` to native code linked against
        the same Kith runtime. Public C server accessors provide borrowed plane
        handles. Keep this Server, the native library and callback state alive
        until :meth:`close` has drained all callbacks. The address and derived
        plane handles become invalid when close destroys the server. Never
        destroy them from native code. A native destructor called after close
        must release only application-owned resources without dereferencing
        any borrowed Kith handle.

        Returns:
            Nonzero borrowed server address; no ownership is transferred.

        Raises:
            KithStateError: When the server is closed.

        Thread safety:
            @thread_safety safe-if the caller serializes all uses of the
            returned address against close. Retrieving it does not pin it.
        """
        with self._lifecycle_lock:
            handle = self._handle
            if self._closed or handle is None:
                raise KithStateError(gen_types.kith_error.KITH_ESTATE, "server handle is closed")
            address = ctypes.cast(handle, ctypes.c_void_p).value
            assert address is not None
            return address

    # -----------------------------------------------------------------------
    # teardown
    # -----------------------------------------------------------------------

    def close(self) -> None:
        """Release the server handle, model instances, and any owned config.

        Idempotent. If a run loop is in flight on another thread, requests
        graceful shutdown and blocks until ``kith_server_run`` has returned
        before destroying the handle, so ``kith_server_destroy`` is never
        called while the run loop is in flight (its ``@thread_safety unsafe``
        contract, enforced by a runtime abort in the C layer). Calling
        :meth:`close` from the run-loop thread itself re-enters during a
        blocking run and is not supported; in that case the method does not
        wait (it cannot join its own thread) and proceeds to destroy, which
        trips the C runtime assertion.

        The server handle is destroyed first so the worker pool is joined
        (``kith_server_destroy`` drains it before releasing any plane) before
        facade-owned model instances are released: a model instance is
        independent of the sim handle after creation, but an in-flight worker
        dispatch may be inside a Python handler that steps or publishes on a
        model, so freeing the model before the workers drain is a
        use-after-free. Releasing the server first guarantees no handler is
        running when the models are destroyed. A configuration source built
        from a path follows; a :class:`Config` passed in is not closed here
        (the caller owns it).

        Raises:
            KithStateError: When called from this server's own handler callback.
                Request shutdown and close from an owning thread instead.
            KithError: When shutdown is requested and the run loop does not
                exit within the drain window (``KITH_ETIMEDOUT``).

        Thread safety:
            @thread_safety safe-if called outside this server's callbacks and
            any active run is on another thread (``close`` requests shutdown
            and blocks until the run loop exits before destroying the handle). Unsafe
            when called from the run-loop thread itself while a run is in
            flight: the wait is skipped (a thread cannot join itself) and
            the C runtime assertion catches the contract violation.
        """
        # Check before taking the lifecycle lock: another owner may hold it
        # while draining this callback. Waiting for that owner deadlocks.
        if getattr(self._callback_context, "depth", 0):
            raise KithStateError(
                gen_types.kith_error.KITH_ESTATE,
                "cannot close a server from its own callback; request shutdown instead",
            )
        # The lifecycle lock makes the in-flight decision atomic against
        # run() startup: either this close observes the announcement and
        # waits below, or a concurrent run() has not started yet and its
        # lock acquisition after destroy refuses via the closed flag.
        with self._lifecycle_lock:
            if self._closed:
                return
            # Block until the run loop has exited so kith_server_destroy is
            # never called while kith_server_run is in flight. The event is
            # cleared on run() entry and set on every return path; a set event
            # means no run is in flight. When the run is on another thread,
            # request shutdown and wait for it to drain; when close() is
            # called from the run-loop thread itself the wait is skipped (a
            # thread cannot join itself) and the C runtime assertion catches
            # the contract violation.
            if (
                not self._run_exited.is_set()
                and self._run_thread is not None
                and threading.current_thread() is not self._run_thread
            ):
                self.shutdown()
                if not self._run_exited.wait(timeout=10.0):
                    raise KithError(
                        gen_types.kith_error.KITH_ETIMEDOUT,
                        "server run loop did not exit within the drain window",
                    )
            # Destroy the server handle before releasing facade-owned model
            # instances. kith_server_destroy joins the worker pool first (the
            # reactor run loop has already exited above, so no new dispatches
            # are submitted, but in-flight handler tasks may still be
            # running), so returning from it guarantees no worker is inside a
            # Python handler that steps a model or publishes a cell product.
            # Releasing the models before that join raced an in-flight
            # dispatch reading the model the facade was tearing down. A model
            # instance is independent of the sim handle after creation
            # (kith_sim_model_destroy touches only the model pointer and its
            # vtable copy, not the sim), so it is safe to free the server —
            # and with it the sim — first.
            self._bridge.server().kith_server_destroy(self._handle)
            # Release facade-owned model instances now that every worker has
            # drained. The views were handed out as borrowed (close is a no-op
            # for the holder); flip to owning here so the underlying model is
            # destroyed.
            for model in self._models:
                model._owned = True
                with contextlib.suppress(Exception):
                    model.close()
            self._models.clear()
            self._handlers.clear()
            self._destroyed_trampoline = None
            self._destroyed_handler = None
            self._tick_handler = None
            self._route_handlers.clear()
            self._zones.clear()
            if self._config is not None:
                self._config.close()
                self._config = None
            self._handle = None
            self._closed = True

    def __enter__(self) -> Server:
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
