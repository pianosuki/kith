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
from kith._generated import control as gen_control
from kith._generated import gateway as gen_gateway
from kith._generated import server as gen_server
from kith._generated import sim as gen_sim
from kith._generated import types as gen_types
from kith._generated import version as gen_version
from kith._generated.server import kith_server_params_t, kith_server_t
from kith.aoi import Aoi
from kith.config import Config
from kith.control import (
    Control,
    Method,
    Request,
    Response,
    _internal_error_body,
    _request_from_c,
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
from kith.fabric import _key_to_c as _cell_key_to_c
from kith.gateway import (
    DeliveryTotals,
    Gateway,
    PinnedSession,
    Session,
    SessionInfo,
    SessionType,
    _broadcast_cell,
    _deliver_frame,
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
    _actor_to_c,
    _key_to_c,
    _model_config_to_c,
)
from kith.worker import Worker


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


class Server:
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
        tick_hz: Simulation tick rate in Hz; 0 selects the default.
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

    __slots__ = (
        "_bridge",
        "_closed",
        "_config",
        "_destroyed_handler",
        "_destroyed_trampoline",
        "_handle",
        "_handlers",
        "_lifecycle_lock",
        "_models",
        "_route_handlers",
        "_run_exited",
        "_run_thread",
        "_tick_handler",
        "_zones",
    )

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
        self._handle: object = None
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

    def register_proto_type(self, name: str, type_id: int) -> None:
        """Register a wire message type on the server's borrowed proto handle.

        Both peers register the same name at the same id before exchanging
        frames of that type; the gateway decodes frames against the proto the
        composition root owns, so a type registered here before :meth:`run` is
        visible to the decoder during the run loop. Game-specific types use
        ids at or above ``KITH_PROTO_TYPE_USER_BASE`` (1000). The
        replication type id (:meth:`Server` ``replication_type_id`` argument)
        is itself a wire type the caller registers here.

        Args:
            name: Message type name; both peers use the same name.
            type_id: Numeric message type id.

        Raises:
            KithStateError: When ``type_id`` is already registered to a
                different name (``KITH_EEXIST``).
            KithError: On a registration failure.

        Thread safety:
            @thread_safety unsafe — must not be called concurrently with
            :meth:`run` (the gateway decoder reads the proto during the run
            loop). Register before :meth:`run`.
        """
        proto = self._borrowed_proto()
        rc = int(
            self._bridge.lib("proto").kith_proto_register_type_id(
                proto,
                name.encode("utf-8"),
                ctypes.c_uint16(type_id),
            )
        )
        check_error(rc, f"kith_proto_register_type_id: {name}")

    # -----------------------------------------------------------------------
    # borrowed plane accessors
    # -----------------------------------------------------------------------
    #
    # The C server owns every plane handle; these return borrowed references
    # the registration methods call into directly. The facade never wraps a
    # borrowed handle in an owning Gateway/Sim/Control object (that close
    # frees the server's plane) and never destroys a borrowed handle.

    def _borrowed_gateway(self) -> object:
        return self._bridge.server().kith_server_gateway(self._handle)

    def _borrowed_sim(self) -> object:
        return self._bridge.server().kith_server_sim(self._handle)

    def _borrowed_fabric(self) -> object:
        return self._bridge.server().kith_server_fabric(self._handle)

    def _borrowed_proto(self) -> object:
        return self._bridge.server().kith_server_proto(self._handle)

    def _borrowed_control(self) -> object:
        return self._bridge.server().kith_server_control(self._handle)

    def _borrowed_db(self) -> object:
        return self._bridge.server().kith_server_db(self._handle)

    # -----------------------------------------------------------------------
    # registration surface
    # -----------------------------------------------------------------------
    #
    # Each method calls the plane function on the borrowed handle directly.
    # ctypes callback trampolines are kept alive on the facade for the
    # registration's lifetime (the C side holds a raw function pointer).

    def register_message_handler(
        self,
        msg_type: int,
        handler: Callable[..., None],
    ) -> None:
        """Register a Python handler for an inbound message type.

        Targets the gateway plane's handler table on the server's borrowed
        gateway handle. The handler is invoked as
        ``handler(msg_type, payload_bytes, session)`` when a decoded frame of
        this type is dispatched; ``session`` is a borrowed view the handler
        must not destroy. Replaces any prior registration for ``msg_type``.

        An exception the handler raises is caught at the dispatch boundary
        and counted in ``kith_python_handler_exceptions_total``; the first
        exception per registration prints its traceback to stderr, subsequent
        ones count silently, and the dispatch continues.

        Args:
            msg_type: Wire message type id (must match a type registered on
                the borrowed proto before :meth:`run`).
            handler: Callable invoked on dispatch.

        Raises:
            KithError: When the gateway rejects the registration.

        Thread safety:
            @thread_safety unsafe — must not be called concurrently with
            :meth:`run` (dispatch reads the handler table during the run
            loop). Register before :meth:`run`. Handlers share the worker
            pool with tick handlers and control routes; with more than one
            worker the callbacks run concurrently, so game state shared
            between them is synchronized by the game.
        """
        gateway = self._borrowed_gateway()
        trampoline = self._build_handler_trampoline(handler)
        rc = int(
            self._bridge.lib("gateway").kith_gateway_register_handler_flags(
                gateway,
                ctypes.c_uint16(msg_type),
                trampoline,
                None,
                ctypes.c_uint32(
                    int(gen_gateway.kith_gateway_handler_flag.KITH_GATEWAY_HANDLER_PYTHON)
                ),
            )
        )
        check_error(rc, f"kith_gateway_register_handler: msg_type={msg_type}")
        self._handlers[msg_type] = (trampoline, handler)

    def _build_handler_trampoline(self, handler: Callable[..., None]) -> object:
        guarded = _bridge.guarded_handler(handler, "message handler")

        def _trampoline(
            msg_type: int,
            payload: int,
            payload_len: int,
            session_ptr: object,
            _user_data: object,
        ) -> None:
            data = bytes(ctypes.string_at(payload, payload_len)) if payload_len and payload else b""
            session = Session(self._bridge, self._borrowed_gateway(), session_ptr, borrowed=True)
            guarded(int(msg_type), data, session)

        return gen_gateway.kith_gateway_msg_handler_fn(_trampoline)

    def register_tick_handler(self, handler: Callable[[int], None]) -> None:
        """Register a Python callback invoked once per tick on a worker thread.

        The server advances a monotonic tick counter (the tick-boundary
        time base) once per normal tick and dispatches the callback to the
        worker pool as a single task, so the reactor thread never enters the
        Python interpreter on the tick path. The callback receives
        the 1-based tick index; the shutdown drain tick does not dispatch it.

        Per-tick game logic (flushing a dirty-cell set, reseeding the PRNG,
        recording the tick's input batch for replay) belongs here rather than
        in a per-input handler, collapsing a per-input publish cadence to a
        per-tick cadence. The callback runs asynchronously: its publishes are
        observed by the next tick's gateway refresh.

        An exception the callback raises is caught at the dispatch boundary
        and counted in ``kith_python_handler_exceptions_total``; the first
        exception per registration prints its traceback to stderr, subsequent
        ones count silently, and the tick cadence continues.

        Args:
            handler: Callable invoked as ``handler(tick)`` once per normal
                tick on a worker thread.

        Raises:
            KithError: When the server rejects the registration.

        Thread safety:
            @thread_safety unsafe — must not be called concurrently with
            :meth:`run` (the reactor reads the registration once per tick).
            Register before :meth:`run`. The handler shares the worker pool
            with message handlers and control routes; with more than one
            worker the callbacks run concurrently, so game state shared
            between them is synchronized by the game.
        """
        trampoline = self._build_tick_trampoline(handler)
        rc = int(
            self._bridge.server().kith_server_register_tick_handler(
                self._handle,
                trampoline,
                None,
                ctypes.c_uint32(
                    int(gen_server.kith_server_handler_flag.KITH_SERVER_HANDLER_PYTHON)
                ),
            )
        )
        check_error(rc, "kith_server_register_tick_handler")
        self._tick_handler = (trampoline, handler)

    def _build_tick_trampoline(self, handler: Callable[[int], None]) -> object:
        guarded = _bridge.guarded_handler(handler, "tick handler")

        def _trampoline(tick: int, _user_data: object) -> None:
            guarded(int(tick))

        return gen_server.kith_server_tick_fn(_trampoline)

    def register_sim_model(self, name: str, config: SimModelConfig) -> SimModel:
        """Instantiate a named simulation model with ``config``.

        The built-in ``free2d`` and ``tile2d`` models are registered on the
        server's sim handle at creation time; this instantiates one by name
        and returns a borrowed :class:`SimModel` view the game steps each
        tick.

        Args:
            name: Registered model name (e.g. ``"free2d"``, ``"tile2d"``).
            config: Model creation parameters.

        Returns:
            A borrowed :class:`SimModel` view over the new instance.

        Raises:
            KithNotFoundError: When ``name`` is not registered.
            KithError: On a creation failure.

        Thread safety:
            @thread_safety unsafe — must not be called concurrently with
            :meth:`run` (the model registry is not synchronized). Register
            before :meth:`run`.

        Ownership:
            Returns a borrowed view: the facade owns the model instance
            and releases it on :meth:`close`; the view's
            :meth:`~SimModel.close` is a no-op.
        """
        sim = self._borrowed_sim()
        cfg = _model_config_to_c(config)
        out = ctypes.POINTER(gen_sim.kith_sim_model_t)()
        rc = int(
            self._bridge.lib("sim").kith_sim_create_model(
                sim,
                name.encode("utf-8"),
                ctypes.byref(cfg),
                None,
                ctypes.byref(out),
            )
        )
        check_error(rc, f"kith_sim_create_model: name={name!r}")
        model = SimModel(self._bridge, out, owned=False)
        self._models.append(model)
        return model

    def publish_artifact(
        self,
        key: ArtifactKey,
        actor: Actor,
    ) -> int:
        """Publish an actor's state as a cell artifact on the borrowed sim.

        After stepping a model instance each tick, game code publishes the
        updated actor positions into the sim's artifact store; the gateway's
        shared cache drains these each tick. A publish refreshes the cache
        and delivers nothing on its own: a session receives the cell only
        through the session-to-delivery contract — the session is bound to
        an actor, its subscription window covers the cell, and a
        ``publish_cell_product`` bump carries the refreshed cell into the
        fabric stream (see docs/guides/getting_started.md, "From input to
        delivery"). The store holds one artifact per actor: if the actor
        already holds an artifact in the same cell, the publish updates it
        in place and the cell's publish sequence keeps climbing; if the
        actor holds an artifact in another cell, the publish relocates it
        atomically under the store lock — a snapshot never observes the
        actor absent from the store while it moves between cells. Returns
        the ``publish_seq`` assigned by the store.

        Args:
            key: Cell locator (zone, cell, lod) to publish into.
            actor: The actor whose state to publish.

        Returns:
            The assigned publish sequence number.

        Raises:
            KithError: On a publish failure.

        Thread safety:
            @thread_safety safe — the sim artifact store is internally
            synchronized and may be called from a worker handler
            concurrently with snapshot reads on the reactor thread.
        """
        sim = self._borrowed_sim()
        c_key = _key_to_c(key)
        c_actor = _actor_to_c(actor)
        out_seq = ctypes.c_uint64(0)
        rc = int(
            self._bridge.lib("sim").kith_sim_publish_artifact(
                sim,
                ctypes.byref(c_key),
                ctypes.byref(c_actor),
                ctypes.byref(out_seq),
            )
        )
        check_error(rc, "kith_sim_publish_artifact")
        return int(out_seq.value)

    def remove_artifact(self, actor_id: int) -> None:
        """Remove every published artifact for one actor across all cells.

        Used when an actor leaves the simulated world entirely (despawn):
        the store drops the actor's artifact and its cell membership in one
        locked operation. Moving an actor between cells does not need a
        remove — :meth:`publish_artifact` relocates the artifact atomically,
        so pairing a remove with a follow-up publish would only open a
        window where the actor is absent from the store while a concurrent
        compose can observe it.

        Args:
            actor_id: The actor whose artifacts to remove.

        Raises:
            KithError: On a failure.

        Thread safety:
            @thread_safety safe — the sim artifact store is internally
            synchronized and may be called from a worker handler
            concurrently with snapshot reads on the reactor thread.
        """
        sim = self._borrowed_sim()
        rc = int(self._bridge.lib("sim").kith_sim_remove_artifact(sim, ctypes.c_uint64(actor_id)))
        check_error(rc, "kith_sim_remove_artifact")

    def publish_cell_product(self, key: CellKey, authority_epoch: int) -> int:
        """Publish a cell product into the fabric stream on the borrowed fabric.

        Renders the cell's artifacts from the borrowed sim into a cell product
        header keyed by ``key`` and appends it to the fabric's append-only cell
        stream, fanning the update out to every subscription tracking the cell.
        Game code calls this to re-broadcast a cell when its contents change
        for reasons other than the per-tick sim publish (for example, a chat
        event that triggers a re-delivery of the sender's cell to nearby
        subscribers). Returns the ``publish_seq`` assigned by the store.

        Args:
            key: Cell locator (zone, cell, lod) to publish.
            authority_epoch: Authority epoch the publishing instance claims.
                A stale epoch (below the cell's current) raises
                ``KithStateError`` (``KITH_EPERM``); the cell's epoch is
                monotonic across publishes.

        Returns:
            The assigned publish sequence number.

        Raises:
            KithStateError: When ``authority_epoch`` is stale.
            KithError: On a publish failure.

        Thread safety:
            @thread_safety safe — the fabric cell store and subscription
            fanout are internally synchronized.
        """
        fabric = self._borrowed_fabric()
        c_key = _cell_key_to_c(key)
        out_seq = ctypes.c_uint64(0)
        rc = int(
            self._bridge.lib("fabric").kith_fabric_publish(
                fabric,
                ctypes.byref(c_key),
                ctypes.c_uint32(authority_epoch),
                ctypes.byref(out_seq),
            )
        )
        check_error(rc, "kith_fabric_publish")
        return int(out_seq.value)

    def send(self, session: Session, msg_type: int, payload: bytes = b"") -> None:
        """Send one discrete message frame to a session's connection.

        The one-off send surface outside the composed replication stream:
        a login ack, a targeted event, a reply. The frame is encoded
        through the server's borrowed proto and enqueued on the session's
        connection; the reactor flushes the connection's output queue.
        The replication cadence is untouched — a sent frame rides the
        connection ahead of (or between) the tick's composed frames.

        The call is legal while ``session`` is alive by reference — from
        the message handler that received it (the dispatch reference
        holds for the handler's duration), or from the reactor thread. A
        view held past its dispatch has no reference and no guarantee.
        The send appends no correlation trailer (request/reply
        correlation is a payload convention) and does not consult the
        type registry — the receiving client's decoder does — so both
        peers register ``msg_type`` on their protos (see
        :meth:`register_proto_type`).

        Args:
            session: The session to send to.
            msg_type: Wire message type id.
            payload: Frame payload bytes; an empty payload sends a
                header-only frame.

        Raises:
            KithNetworkError: When the connection's output queue is at
                its high watermark (backpressure). The frame was not
                queued; the caller decides retry semantics.
            KithStateError: When the server handle is closed, the session
                is closed, or the session's connection has closed.
            KithProtocolError: When the payload exceeds the negotiated
                maximum frame payload.
            KithError: On other failures.

        Thread safety:
            @thread_safety safe-if ``session`` is alive by reference (a
            dispatch view inside its handler, or a session the caller
            owns); unsafe from contexts holding only a stale view.
        """
        if self._closed:
            raise KithStateError(
                gen_types.kith_error.KITH_ESTATE,
                "server handle is closed",
            )
        _deliver_frame(self._bridge, self._borrowed_gateway(), session.handle, msg_type, payload)

    def broadcast_cell(self, key: CellKey, msg_type: int, payload: bytes = b"") -> None:
        """Submit a cell-scoped broadcast.

        Free-form traffic for one cell — chat text, an announcement, a
        per-cell event — rides the gateway's request queue: on the next
        tick pass the reactor delivers one frame of ``msg_type`` carrying
        ``payload`` to every session whose subscription window covers
        ``key``. The recipient set is derived, never submitted — window
        membership is the gateway-side image of the cell's subscription
        set — so the broadcast reaches tier-suppressed subscribers too and
        no caller-side recipient roster forms.

        The submit is legal from any context: the request queue is
        internally synchronized, so a pool-dispatched handler broadcasts
        the way it publishes. Delivery is one tick deep and best-effort —
        the frame is encoded once and fanned per recipient; a recipient
        whose connection queue is full or closed is counted
        (``kith_gateway_broadcast_dropped_total``), a request with zero
        covering recipients completes silently, and requests pending at
        shutdown are counted. The frame is an ordinary message frame —
        both peers register ``msg_type`` (see :meth:`register_proto_type`)
        and the payload format is the game's own; no correlation trailer
        is appended. The composed replication stream, its presets, and the
        replication record are untouched.

        Args:
            key: Cell locator (zone, cell, lod) the broadcast is scoped to.
            msg_type: Wire message type id.
            payload: Frame payload bytes; an empty payload sends a
                header-only frame.

        Raises:
            KithNetworkError: When the request queue is full (saturation;
                counted in ``kith_gateway_broadcast_refused_total``). The
                request was not queued; the caller decides retry
                semantics.
            KithStateError: When the server handle is closed, or the
                gateway is shutting down.
            KithProtocolError: When the payload exceeds the negotiated
                maximum frame payload.
            KithError: On other failures.

        Thread safety:
            @thread_safety safe — the request queue is internally
            synchronized; submission from a pool-dispatched handler is
            expected use.
        """
        if self._closed:
            raise KithStateError(
                gen_types.kith_error.KITH_ESTATE,
                "server handle is closed",
            )
        _broadcast_cell(self._bridge, self._borrowed_gateway(), key, msg_type, payload)

    def on_session_destroyed(self, handler: Callable[[SessionInfo], None]) -> None:
        """Register the destroyed-session callback.

        Invoked as ``handler(info)`` on a pool worker when the gateway
        destroys a session — after the session detaches from the table and
        its subscriptions, before the owner reference drops — with the
        identity record of the destroyed session. This is the session
        death signal: a game that keeps per-session state (rosters, seats,
        per-session caches) reclaims it here. The registration is
        pool-dispatched and shares the worker pool with tick handlers,
        message handlers, and control routes, so game state it touches is
        synchronized by the game. Replaces any prior registration;
        :meth:`off_session_destroyed` detaches. Register before
        :meth:`run` alongside the other handlers.

        An exception the handler raises is caught at the dispatch boundary
        and counted in ``kith_python_handler_exceptions_total``; the first
        exception per registration prints its traceback to stderr, subsequent
        ones count silently, and the session teardown continues.

        Args:
            handler: Callable invoked with the destroyed session's
                :class:`SessionInfo`.

        Raises:
            KithError: When the gateway rejects the registration.

        Thread safety:
            @thread_safety unsafe — must not be called concurrently with
            :meth:`run` (the fire reads the registration slot during the
            run loop), though the registration call itself is mutex-
            serialized on the C side.
        """
        gateway = self._borrowed_gateway()
        trampoline = self._build_destroyed_trampoline(handler)
        rc = int(
            self._bridge.lib("gateway").kith_gateway_register_session_destroyed_handler_flags(
                gateway,
                trampoline,
                None,
                ctypes.c_uint32(
                    int(gen_gateway.kith_gateway_handler_flag.KITH_GATEWAY_HANDLER_PYTHON)
                ),
            )
        )
        check_error(rc, "kith_gateway_register_session_destroyed_handler")
        self._destroyed_trampoline = trampoline
        self._destroyed_handler = handler

    def off_session_destroyed(self) -> None:
        """Unregister the destroyed-session callback. Idempotent.

        Thread safety:
            @thread_safety unsafe — must not be called concurrently with
            :meth:`run`.
        """
        rc = int(
            self._bridge.lib("gateway").kith_gateway_unregister_session_destroyed_handler(
                self._borrowed_gateway()
            )
        )
        check_error(rc, "kith_gateway_unregister_session_destroyed_handler")
        self._destroyed_trampoline = None
        self._destroyed_handler = None

    @property
    def session_count(self) -> int:
        """The number of sessions currently in the server's session table.

        A mirror gauge of the gateway's table, readable from any thread.

        Thread safety:
            @thread_safety safe — the mirror gauge is an atomic read.
        """
        if self._closed:
            raise KithStateError(gen_types.kith_error.KITH_ESTATE, "server handle is closed")
        return int(self._bridge.lib("gateway").kith_gateway_session_count(self._borrowed_gateway()))

    def delivery_totals(self) -> DeliveryTotals:
        """Return the gateway's cumulative delivery totals (the same record
        as :meth:`kith.gateway.Gateway.delivery_totals`).

        Thread safety:
            @thread_safety safe — the counters are atomic; readable from
            any thread.
        """
        if self._closed:
            raise KithStateError(gen_types.kith_error.KITH_ESTATE, "server handle is closed")
        out = gen_gateway.kith_gateway_delivery_totals_t()
        rc = int(
            self._bridge.lib("gateway").kith_gateway_delivery_totals(
                self._borrowed_gateway(),
                ctypes.byref(out),
            )
        )
        check_error(rc, "kith_gateway_delivery_totals")
        return DeliveryTotals(
            enqueued=int(out.enqueued),
            dropped=int(out.dropped),
            events_enqueued=int(out.events_enqueued),
            suppressed=int(out.suppressed),
        )

    def _build_destroyed_trampoline(self, handler: Callable[[SessionInfo], None]) -> object:
        guarded = _bridge.guarded_handler(handler, "session-destroyed handler")

        def _trampoline(
            info_ptr: ctypes._Pointer[gen_gateway.kith_gateway_session_info],
            _user_data: object,
        ) -> None:
            raw = info_ptr.contents
            info = SessionInfo(
                session_id=int(raw.session_id),
                principal_id=int(raw.principal_id),
                actor_id=int(raw.actor_id),
                type=int(raw.type),
            )
            guarded(info)

        return gen_gateway.kith_gateway_session_destroyed_fn(_trampoline)

    def register_zone(self, name: str, *, map_path: str | None = None) -> int:
        """Reserve a named zone id, optionally loading a behavior grid.

        Zones are an implicit uint32 namespace in the cell-stream and artifact
        store (there is no C ``register_zone`` call); this facade method
        reserves a monotonic id for a name so game code refers to zones
        consistently. When ``map_path`` is given, a ``tile2d`` model instance
        is created and its behavior grid loaded from the file, kept alive on
        the facade for the server's lifetime.

        Args:
            name: Zone name; must be unique across registrations.
            map_path: Optional path to a tile2d behavior grid file.

        Returns:
            The reserved zone id (a positive int).

        Raises:
            KithError: When ``name`` is already registered or the behavior
                grid cannot be loaded.

        Thread safety:
            @thread_safety unsafe — mutates the facade's zone registry and
            may instantiate a model; must not race :meth:`register_zone` or
            :meth:`run`.
        """
        if name in self._zones:
            raise KithError(
                gen_types.kith_error.KITH_EEXIST,
                f"register_zone: zone {name!r} is already registered",
            )
        zone_id = len(self._zones) + 1
        self._zones[name] = zone_id
        if map_path is not None:
            model = self.register_sim_model("tile2d", SimModelConfig())
            model.load_behavior(map_path)
        return zone_id

    def register_query(self, name: str, sql: str, n_params: int = 0) -> None:
        """Register a named parameterized SQL query on the persistence pool.

        Targets the db plane's query registry on the server's borrowed db
        handle. Execution resolves the query by name through the db pool's
        async execute path; the ``kith.db`` module's ``Database`` carries
        the async query and transaction surface over a pool the game owns.

        Args:
            name: Query name, unique across registrations.
            sql: PostgreSQL parameterized command using ``$1, $2, ...``
                positional placeholders.
            n_params: Number of positional placeholders in ``sql``.

        Raises:
            KithStateError: When no persistence pool is configured on this
                server (the borrowed db handle is NULL).
            KithError: On a registration failure.

        Thread safety:
            @thread_safety unsafe — must not race
            ``kith_db_exec`` or another ``register_query`` on the same
            handle. Register before :meth:`run`.
        """
        db = self._borrowed_db()
        if not db:
            raise KithStateError(
                gen_types.kith_error.KITH_ESTATE,
                "register_query: no persistence pool configured on this server",
            )
        rc = int(
            self._bridge.lib("db").kith_db_register_query(
                db,
                name.encode("utf-8"),
                sql.encode("utf-8"),
                ctypes.c_uint32(n_params),
            )
        )
        check_error(rc, f"kith_db_register_query: name={name!r}")

    def register_control_route(
        self,
        method: str | Method,
        path: str,
        handler: Callable[[Request, Response], None],
    ) -> None:
        """Register an HTTP route on the control plane.

        Targets the control plane's route table on the server's borrowed
        control handle. The handler receives a :class:`Request` view and
        builds a :class:`Response`.

        An exception the handler raises is answered with a 500 whose body
        names the exception, and counted in
        ``kith_python_handler_exceptions_total``; the first exception per
        registration also prints its traceback to stderr, subsequent ones count
        silently. An oversize response — a builder raising
        :class:`KithResponseOverflowError` — is not a handler exception: it
        answers the canonical counted rejection instead.

        Args:
            method: HTTP method string (``"GET"``, ``"POST"``, ...) or a
                :class:`Method` member.
            path: URL path pattern (supports ``:param`` segments).
            handler: Callable invoked on each matching request.

        Raises:
            KithStateError: When the control plane library is not linked
                into the build (the borrowed control handle is NULL).
            KithError: On a registration failure.

        Thread safety:
            @thread_safety unsafe — must not race request dispatch on the
            same handle. Register before :meth:`run`. The handler itself
            executes on the server's worker pool — the same pool that runs
            tick and message handlers. With more than one worker
            (free-threaded Python, or an explicitly raised pool size)
            callbacks run concurrently; game state a route handler shares
            with other callbacks must be synchronized by the game.
        """
        ctrl = self._borrowed_control()
        if not ctrl:
            raise KithStateError(
                gen_types.kith_error.KITH_ESTATE,
                "register_control_route: the control plane is not linked into this build",
            )
        method_str = method.name if isinstance(method, Method) else method
        trampoline = self._build_route_trampoline(handler, ctrl)
        route = gen_control.kith_control_route_t(
            method=method_str.encode("utf-8"),
            path=path.encode("utf-8"),
            handler=trampoline,
            ctx=None,
            flags=ctypes.c_uint32(
                int(gen_control.kith_control_route_flag.KITH_CONTROL_ROUTE_PYTHON)
            ),
        )
        rc = int(
            self._bridge.lib("control").kith_control_register_route(
                ctrl,
                ctypes.byref(route),
            )
        )
        check_error(rc, f"kith_control_register_route: {method_str} {path}")
        self._route_handlers[id(trampoline)] = trampoline

    def _build_route_trampoline(
        self, handler: Callable[[Request, Response], None], ctrl: object | None
    ) -> object:
        report = _bridge.handler_exception_reporter("control route handler")
        overflow_note = _bridge.response_overflow_reporter()

        def _trampoline(
            req_ptr: ctypes._Pointer[gen_control.kith_control_request_t],
            resp_ptr: ctypes._Pointer[gen_control.kith_control_response_t],
            _ctx: object,
        ) -> int:
            req = _request_from_c(req_ptr.contents)
            response = Response(self._bridge, resp_ptr, ctrl)
            try:
                handler(req, response)
            except KithResponseOverflowError as exc:
                # An oversize response is a sizing condition, not a handler
                # failure: answer the canonical rejection instead of the
                # handler-exception path. A rejection that itself cannot fit
                # leaves a zero-length response, which the flush treats as
                # close-without-response.
                attempted = exc.attempted if exc.attempted is not None else 0
                try:
                    response.reject_overflow(attempted, req.path)
                    overflow_note(req.path, attempted, int(resp_ptr.contents.cap))
                except Exception:
                    pass
            except Exception as exc:
                report(exc)
                # The handler raised after partially writing the response.
                # Reset and emit a 500 so the connection flushes a valid
                # response rather than a half-written one carrying a stale
                # Content-Length for a body that was never written.
                try:
                    response.status(500, "application/json")
                    response.body(_internal_error_body(exc))
                except Exception:
                    pass
            return 0

        return gen_control.kith_control_handler_fn(_trampoline)

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
            KithError: When shutdown is requested and the run loop does not
                exit within the drain window (``KITH_ETIMEDOUT``).

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
