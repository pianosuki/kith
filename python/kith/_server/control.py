"""Server zone, query and HTTP control-route registration."""

from __future__ import annotations

import ctypes
from collections.abc import Callable

from kith import _bridge
from kith._generated import control as gen_control
from kith._generated import types as gen_types
from kith.control import (
    Method,
    Request,
    Response,
    _internal_error_body,
    _request_from_c,
)
from kith.exceptions import (
    KithError,
    KithResponseOverflowError,
    KithStateError,
    check_error,
)
from kith.sim import (
    SimModelConfig,
)

from .registration import ServerRegistration


class ServerControlRoutes(ServerRegistration):
    __slots__ = ()

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

        return gen_control.kith_control_handler_fn(self._guard_callback(_trampoline))


ServerControlRoutes.register_zone.__module__ = "kith"
ServerControlRoutes.register_zone.__qualname__ = "Server.register_zone"

ServerControlRoutes.register_query.__module__ = "kith"
ServerControlRoutes.register_query.__qualname__ = "Server.register_query"

ServerControlRoutes.register_control_route.__module__ = "kith"
ServerControlRoutes.register_control_route.__qualname__ = "Server.register_control_route"

ServerControlRoutes._build_route_trampoline.__module__ = "kith"
ServerControlRoutes._build_route_trampoline.__qualname__ = "Server._build_route_trampoline"
