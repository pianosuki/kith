"""Shared storage and borrowed-plane access for the server facade."""

from __future__ import annotations

import ctypes
import threading
from collections.abc import Callable

from kith import _bridge
from kith._generated.server import kith_server_t
from kith.config import Config
from kith.gateway import (
    SessionInfo,
)
from kith.sim import (
    SimModel,
)


class ServerState:
    __slots__ = (
        "_bridge",
        "_callback_context",
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

    _bridge: _bridge.Bridge
    _callback_context: threading.local
    _closed: bool
    _config: Config | None
    _destroyed_handler: Callable[[SessionInfo], None] | None
    _destroyed_trampoline: object | None
    _handle: ctypes._Pointer[kith_server_t] | None
    _handlers: dict[int, tuple[object, object]]
    _lifecycle_lock: threading.Lock
    _models: list[SimModel]
    _route_handlers: dict[int, object]
    _run_exited: threading.Event
    _run_thread: threading.Thread | None
    _tick_handler: tuple[object, object] | None
    _zones: dict[str, int]

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

    def _guard_callback[**P, R](self, handler: Callable[P, R]) -> Callable[P, R]:
        context = self._callback_context

        def _wrapped(*args: P.args, **kwargs: P.kwargs) -> R:
            depth = getattr(context, "depth", 0)
            context.depth = depth + 1
            try:
                return handler(*args, **kwargs)
            finally:
                context.depth = depth

        return _wrapped


ServerState._borrowed_gateway.__module__ = "kith"
ServerState._borrowed_gateway.__qualname__ = "Server._borrowed_gateway"

ServerState._borrowed_sim.__module__ = "kith"
ServerState._borrowed_sim.__qualname__ = "Server._borrowed_sim"

ServerState._borrowed_fabric.__module__ = "kith"
ServerState._borrowed_fabric.__qualname__ = "Server._borrowed_fabric"

ServerState._borrowed_proto.__module__ = "kith"
ServerState._borrowed_proto.__qualname__ = "Server._borrowed_proto"

ServerState._borrowed_control.__module__ = "kith"
ServerState._borrowed_control.__qualname__ = "Server._borrowed_control"

ServerState._borrowed_db.__module__ = "kith"
ServerState._borrowed_db.__qualname__ = "Server._borrowed_db"

ServerState._guard_callback.__module__ = "kith"
ServerState._guard_callback.__qualname__ = "Server._guard_callback"
