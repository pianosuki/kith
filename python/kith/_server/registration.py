"""Application callback and simulation-model registration on a server."""

from __future__ import annotations

import ctypes
from collections.abc import Callable

from kith import _bridge
from kith._generated import gateway as gen_gateway
from kith._generated import server as gen_server
from kith._generated import sim as gen_sim
from kith.exceptions import (
    check_error,
)
from kith.gateway import (
    Session,
    SessionInfo,
)
from kith.sim import (
    SimModel,
    SimModelConfig,
    _model_config_to_c,
)

from .state import ServerState


class ServerRegistration(ServerState):
    __slots__ = ()

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

        return gen_gateway.kith_gateway_msg_handler_fn(self._guard_callback(_trampoline))

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
        eligible at a gateway refresh after publication products are
        updated. Worker completion, refresh policy and delivery determine
        observation time; there is no fixed one-tick delivery guarantee.

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

        return gen_server.kith_server_tick_fn(self._guard_callback(_trampoline))

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

        return gen_gateway.kith_gateway_session_destroyed_fn(self._guard_callback(_trampoline))


ServerRegistration.register_proto_type.__module__ = "kith"
ServerRegistration.register_proto_type.__qualname__ = "Server.register_proto_type"

ServerRegistration.register_message_handler.__module__ = "kith"
ServerRegistration.register_message_handler.__qualname__ = "Server.register_message_handler"

ServerRegistration._build_handler_trampoline.__module__ = "kith"
ServerRegistration._build_handler_trampoline.__qualname__ = "Server._build_handler_trampoline"

ServerRegistration.register_tick_handler.__module__ = "kith"
ServerRegistration.register_tick_handler.__qualname__ = "Server.register_tick_handler"

ServerRegistration._build_tick_trampoline.__module__ = "kith"
ServerRegistration._build_tick_trampoline.__qualname__ = "Server._build_tick_trampoline"

ServerRegistration.register_sim_model.__module__ = "kith"
ServerRegistration.register_sim_model.__qualname__ = "Server.register_sim_model"

ServerRegistration.on_session_destroyed.__module__ = "kith"
ServerRegistration.on_session_destroyed.__qualname__ = "Server.on_session_destroyed"

ServerRegistration.off_session_destroyed.__module__ = "kith"
ServerRegistration.off_session_destroyed.__qualname__ = "Server.off_session_destroyed"

ServerRegistration._build_destroyed_trampoline.__module__ = "kith"
ServerRegistration._build_destroyed_trampoline.__qualname__ = "Server._build_destroyed_trampoline"
