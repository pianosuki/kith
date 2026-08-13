"""Control plane wrapper.

The control plane exposes an HTTP control API and an SSE event bus for
runtime introspection. It borrows a reactor (for connection polling), a
logger, and a metrics handle. This module wraps that surface into Python
types: routes are registered with ordinary Python callables that receive a
:class:`Request` view and build a :class:`Response`.

A :class:`Control` owns its C handle and releases it through :meth:`close`.
The borrowed reactor, logger, and metrics handles must outlive the control
handle.
"""

from __future__ import annotations

import contextlib
import ctypes
import json
from collections.abc import Callable
from dataclasses import dataclass
from enum import IntEnum
from types import TracebackType

from kith import _bridge
from kith._generated import configure as configure_all
from kith._generated import control as gen_control
from kith._generated import types as gen_types
from kith._generated import version as gen_version
from kith.exceptions import KithResponseOverflowError, check_error
from kith.worker import Worker


__all__ = ["Control", "Method", "Request", "Response"]


class Method(IntEnum):
    """HTTP request method."""

    GET = int(gen_control.kith_control_method.KITH_CONTROL_METHOD_GET)
    POST = int(gen_control.kith_control_method.KITH_CONTROL_METHOD_POST)
    PUT = int(gen_control.kith_control_method.KITH_CONTROL_METHOD_PUT)
    DELETE = int(gen_control.kith_control_method.KITH_CONTROL_METHOD_DELETE)
    UNKNOWN = int(gen_control.kith_control_method.KITH_CONTROL_METHOD_UNKNOWN)


@dataclass(frozen=True)
class Request:
    """An inbound control-plane request view.

    Attributes:
        method: The :class:`Method` value.
        path: The request path.
        headers: A list of ``(name, value)`` header pairs.
        body: The request body bytes.
    """

    method: Method
    path: str
    headers: list[tuple[str, str]]
    body: bytes


class Response:
    """An HTTP response builder handed to a route handler.

    Calls :meth:`status` first, then :meth:`header` zero or more times, then
    :meth:`body` once to finalize.

    Ownership:
        Borrowed: the control plane owns the underlying response and
        finalizes it after the handler returns; the builder is valid
        only inside the handler callback.
    """

    __slots__ = ("_bridge", "_ctrl", "_handle")

    def __init__(self, bridge: _bridge.Bridge, handle: object, ctrl: object | None = None) -> None:
        """Hold the response builder's borrowed handles.

        Args:
            bridge: The loaded bridge the control library calls go
                through.
            handle: The borrowed ``kith_control_response_t`` the builder
                writes through.
            ctrl: The owning ``kith_control_t`` handle, when the builder
                was constructed with one; the canonical overflow
                rejection is counted only through it.
        """
        self._bridge = bridge
        self._handle = handle
        self._ctrl = ctrl

    def status(self, status_code: int, content_type: str | None = None) -> None:
        """Write the status line and Content-Type header.

        Args:
            status_code: The HTTP status code written on the status
                line.
            content_type: The Content-Type media type, or ``None`` to
                omit the header.

        Raises:
            KithResponseOverflowError: When the response cannot fit the
                connection's write buffer.

        Thread safety:
            @thread_safety unsafe — call only from within a route handler
            callback.
        """
        rc = int(
            self._bridge.lib("control").kith_control_response_status(
                self._handle,
                ctypes.c_int(status_code),
                content_type.encode("utf-8") if content_type else None,
            )
        )
        check_error(rc, "kith_control_response_status", exc=KithResponseOverflowError)

    def header(self, name: str, value: str) -> None:
        """Append a response header.

        Args:
            name: The header field name.
            value: The header field value.

        Raises:
            KithResponseOverflowError: When the response cannot fit the
                connection's write buffer.

        Thread safety:
            @thread_safety unsafe — call only from within a route handler
            callback.
        """
        rc = int(
            self._bridge.lib("control").kith_control_response_header(
                self._handle,
                name.encode("utf-8"),
                value.encode("utf-8"),
            )
        )
        check_error(rc, "kith_control_response_header", exc=KithResponseOverflowError)

    def body(self, data: bytes) -> None:
        """Write the response body and finalize Content-Length.

        Args:
            data: The response body bytes.

        Raises:
            KithResponseOverflowError: When the response cannot fit the
                connection's write buffer. ``attempted`` carries
                ``len(data)``.

        Thread safety:
            @thread_safety unsafe — call only from within a route handler
            callback.
        """
        buf = ctypes.create_string_buffer(data, len(data)) if data else None
        rc = int(
            self._bridge.lib("control").kith_control_response_body(
                self._handle,
                buf,
                ctypes.c_uint32(len(data)),
            )
        )
        if rc == -int(gen_types.kith_error.KITH_EOVERFLOW):
            raise KithResponseOverflowError(
                gen_types.kith_error.KITH_EOVERFLOW, "kith_control_response_body", len(data)
            )
        check_error(rc, "kith_control_response_body")

    def reject_overflow(self, attempted: int = 0, route: str | None = None) -> None:
        """Replace a failed response with the canonical counted rejection.

        Called after a builder raised :class:`KithResponseOverflowError`: the
        response is reset and the canonical 500 — naming the route, the
        attempted size, and the capacity, counted in
        ``kith_control_response_overflow_total`` — renders in its place.
        Without a control handle the body renders without the route key and
        the count is skipped.

        Args:
            attempted: The attempted body size in bytes; 0 omits the key.
            route: The request path the oversize response belonged to, or
                ``None`` to omit the route key.

        Raises:
            KithError: When even the rejection cannot fit the buffer; the
                connection then closes with no response.

        Thread safety:
            @thread_safety unsafe — call only from within a route handler
            callback.
        """
        rc = int(
            self._bridge.lib("control").kith_control_reject_overflow(
                self._ctrl,
                self._handle,
                route.encode("utf-8") if route else None,
                ctypes.c_uint32(attempted),
            )
        )
        check_error(rc, "kith_control_reject_overflow")


_RouteHandler = Callable[[Request, Response], None]


def _request_from_c(req: gen_control.kith_control_request_t) -> Request:
    method = Method(int(req.method))
    raw_path = req.path
    path = raw_path.decode("utf-8") if isinstance(raw_path, bytes) else str(raw_path)
    headers: list[tuple[str, str]] = []
    count = int(req.header_count)
    if count and req.headers:
        for i in range(count):
            h = req.headers[i]
            name = h.name.decode("utf-8") if isinstance(h.name, bytes) else str(h.name)
            value = h.value.decode("utf-8") if isinstance(h.value, bytes) else str(h.value)
            headers.append((name, value))
    body_len = int(req.body_len)
    body = bytes(ctypes.string_at(req.body, body_len)) if body_len and req.body else b""
    return Request(method=method, path=path, headers=headers, body=body)


def _internal_error_body(exc: Exception) -> bytes:
    """Build the JSON body a failed route handler surfaces as its 500.

    The detail names the failing cause so the operator reading the response
    can see which field or step broke. The control plane serves the operator
    on loopback by default, and the text is capped so a pathological message
    cannot flood the connection buffer.
    """
    detail = f"{type(exc).__name__}: {exc}"[:200]
    return json.dumps({"error": "internal", "detail": detail}).encode("utf-8")


class Control:
    """Control plane wrapping a ``kith_control_t``.

    Args:
        reactor: Borrowed reactor handle (object with ``.handle`` or a raw
            ctypes pointer). Must outlive the control handle.
        logger: Borrowed logger handle, or ``None`` to drop logging.
        metrics: Borrowed metrics handle, or ``None`` for an empty
            ``/metrics`` payload.
        port: TCP listen port; 0 selects an OS-assigned ephemeral port.
        host: Bind address for the control listener, or ``None`` for the
            loopback default (the plane serves unauthenticated HTTP and
            stays local unless bound out explicitly).
        worker_count: Size of the worker pool this wrapper creates and
            owns for Python route dispatch; ``0`` creates no pool, so
            Python routes answer 503 until one is attached. The owned
            default is one worker on both interpreter flavors; a
            free-threaded deployment wanting handler parallelism attaches
            an explicit pool via :meth:`attach_worker_pool`.
        max_connections: Max simultaneous connections; 0 selects the
            default (64).
        read_buffer_cap: Per-connection read buffer capacity; 0 selects
            the default (4096).
        write_buffer_cap: Per-connection write buffer capacity; 0 selects
            the default (262144). A response body larger than this capacity
            answers the canonical counted rejection (a 500 naming the
            route, the attempted size, and the capacity); size it for the
            largest listing a route returns.
        event_bus_cap: Event bus ring capacity; 0 selects the default
            (4096).
        sse_flush_ms: SSE flush interval in milliseconds; 0 selects the
            default (100).
        bridge: A loaded :class:`kith._bridge.Bridge`, or ``None`` to load the
            process-wide singleton.

    Raises:
        KithError: When the control handle cannot be built.
        BridgeError: When the shared libraries cannot be loaded.

    Thread safety:
        @thread_safety safe — construction shares no mutable state across
        callers.

    Ownership:
        Owns its ``kith_control_t`` handle and the route-dispatch pool it
        created internally; :meth:`close` releases both. A pool attached
        through :meth:`attach_worker_pool` stays the caller's to close.
    """

    __slots__ = ("_bridge", "_closed", "_handle", "_owned_pool", "_routes")

    def __init__(
        self,
        reactor: object,
        logger: object | None = None,
        metrics: object | None = None,
        *,
        host: str | None = None,
        port: int = 0,
        worker_count: int = 1,
        max_connections: int = 0,
        read_buffer_cap: int = 0,
        write_buffer_cap: int = 0,
        event_bus_cap: int = 0,
        sse_flush_ms: int = 0,
        bridge: _bridge.Bridge | None = None,
    ) -> None:
        loaded = bridge if bridge is not None else _bridge.load()
        configure_all(loaded)
        self._bridge = loaded
        self._handle: object = None
        self._routes: dict[int, object] = {}
        self._owned_pool: Worker | None = None
        self._closed = True

        reactor_h = reactor.handle if hasattr(reactor, "handle") else reactor
        logger_h = self._borrow(logger)
        metrics_h = self._borrow(metrics)
        params = gen_control.kith_control_params_t(
            size=ctypes.sizeof(gen_control.kith_control_params_t),
            abi_version=gen_version.KITH_ABI_VERSION,
            host=host.encode("utf-8") if host else None,
            port=ctypes.c_uint16(port),
            max_connections=max_connections,
            read_buffer_cap=read_buffer_cap,
            write_buffer_cap=write_buffer_cap,
            event_bus_cap=event_bus_cap,
            sse_flush_ms=sse_flush_ms,
        )
        out = ctypes.POINTER(gen_control.kith_control_t)()
        rc = int(
            loaded.lib("control").kith_control_create(
                ctypes.byref(params),
                reactor_h,
                logger_h,
                metrics_h,
                None,
                ctypes.byref(out),
            )
        )
        check_error(rc, "kith_control_create")
        self._handle = out
        self._closed = False

        if worker_count > 0:
            pool = Worker(worker_count=worker_count, bridge=loaded)
            rc = int(
                loaded.lib("control").kith_control_attach_worker_pool(self._handle, pool.handle)
            )
            if rc != 0:
                pool.close()
                self.close()
                check_error(rc, "kith_control_attach_worker_pool")
            self._owned_pool = pool

    @property
    def handle(self) -> object:
        """The borrowed ``kith_control_t*`` for passing to a C function.

        Thread safety:
            @thread_safety safe — the handle is fixed after creation.
        """
        return self._handle

    @staticmethod
    def _borrow(handle: object) -> object:
        return handle.handle if hasattr(handle, "handle") and handle is not None else handle

    # -----------------------------------------------------------------------
    # lifecycle
    # -----------------------------------------------------------------------

    def start(self) -> None:
        """Bind the listener and register it with the reactor.

        Raises:
            KithStateError: When already started (``KITH_ESTATE``).
            KithError: On a socket/bind/listen failure (``KITH_EIO``).

        Thread safety:
            @thread_safety unsafe — call from the composition root before
            the reactor starts or from the reactor thread.
        """
        rc = int(self._bridge.lib("control").kith_control_start(self._handle))
        check_error(rc, "kith_control_start")

    def stop(self) -> None:
        """Stop accepting new connections. Idempotent.

        Thread safety:
            @thread_safety unsafe — call from the reactor thread or before
            the reactor starts.
        """
        self._bridge.lib("control").kith_control_stop(self._handle)

    def listen_port(self) -> int:
        """Return the TCP port the listener is bound to.

        With a port of 0 the OS assigns an ephemeral port at bind time;
        this reads the real endpoint back so a caller can advertise it.
        Returns 0 while unbound.

        Thread safety:
            @thread_safety safe — the bound port is fixed after start.
        """
        return int(self._bridge.lib("control").kith_control_listen_port(self._handle))

    # -----------------------------------------------------------------------
    # routes
    # -----------------------------------------------------------------------

    def register_route(self, method: str, path: str, handler: _RouteHandler) -> None:
        """Register a route matched in registration order on each request.

        Routes registered here are Python-bound: each request dispatches to
        the attached worker pool, never to the reactor thread. Without a
        pool attached the route answers 503. The handler executes on a pool
        thread; with more than one worker, route handlers run concurrently
        with each other, and game state shared between them is the game's
        responsibility to synchronize.

        An exception the handler raises is answered with a 500 whose body
        names the exception, and counted in
        ``kith_python_handler_exceptions_total``; the first exception per
        registration also prints its traceback to stderr, subsequent ones count
        silently. An oversize response — a builder raising
        :class:`KithResponseOverflowError` — is not a handler exception: it
        answers the canonical counted rejection instead.

        Args:
            method: HTTP method string (``"GET"``, ``"POST"``, ...).
            path: URL path pattern (supports ``:param`` segments).
            handler: Callable invoked on each matching request.

        Raises:
            KithStateError: When the route table is full (``KITH_EBUSY``).
            KithError: On invalid arguments (``KITH_EINVAL``) or allocation
                failure (``KITH_ENOMEM``).

        Thread safety:
            @thread_safety unsafe — must not race with request dispatch on
            the same handle.
        """
        trampoline = self._build_route_trampoline(handler, self._handle)
        route = gen_control.kith_control_route_t(
            method=method.encode("utf-8"),
            path=path.encode("utf-8"),
            handler=trampoline,
            ctx=None,
            flags=int(gen_control.kith_control_route_flag.KITH_CONTROL_ROUTE_PYTHON),
        )
        rc = int(
            self._bridge.lib("control").kith_control_register_route(
                self._handle,
                ctypes.byref(route),
            )
        )
        check_error(rc, f"kith_control_register_route: {method} {path}")
        self._routes[id(trampoline)] = trampoline

    def _build_route_trampoline(self, handler: _RouteHandler, ctrl: object | None) -> object:
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

    def attach_worker_pool(self, pool: Worker | None) -> None:
        """Attach the worker pool used for Python route dispatch.

        Passing ``None`` detaches: Python-bound routes answer 503 while no
        pool is attached, and the reactor never runs them inline. Attaching
        a pool replaces the wrapper-owned default pool (destroyed on the
        call); a caller-owned pool stays the caller's to close, before
        :meth:`close` destroys the control handle.

        Args:
            pool: The :class:`kith.worker.Worker` to attach, or ``None`` to
                detach.

        Raises:
            KithError: When the attach call fails.

        Thread safety:
            @thread_safety unsafe — call from the composition root before
            the reactor starts (the dispatch path reads the pool without a
            lock).
        """
        rc = int(
            self._bridge.lib("control").kith_control_attach_worker_pool(
                self._handle,
                pool.handle if pool is not None else None,
            )
        )
        check_error(rc, "kith_control_attach_worker_pool")
        # A replaced wrapper-owned pool has no other referee: release it.
        # Caller-owned pools stay the caller's.
        if self._owned_pool is not None and self._owned_pool is not pool:
            owned = self._owned_pool
            self._owned_pool = None
            owned.close()

    # -----------------------------------------------------------------------
    # event bus
    # -----------------------------------------------------------------------

    def publish_event(
        self,
        event_type: str,
        payload: bytes = b"",
        *,
        correlation_id: bytes | None = None,
    ) -> None:
        """Publish an event to the SSE event bus.

        Args:
            event_type: Event type string copied into the ring record.
            payload: Payload bytes copied into the ring record; truncated to
                the per-record capacity when longer.
            correlation_id: Exactly 8 bytes (the optional correlation
                trailer), or ``None``.

        Raises:
            ValueError: When ``correlation_id`` is not exactly 8 bytes.
            KithStateError: When the event bus ring is full (``KITH_EBUSY``).

        Thread safety:
            @thread_safety unsafe — call from the reactor thread or submit
            as a reactor task via ``kith_reactor_submit``.
        """
        if correlation_id is not None and len(correlation_id) != 8:
            raise ValueError(f"correlation_id must be exactly 8 bytes, got {len(correlation_id)}")
        payload_buf = ctypes.create_string_buffer(payload, len(payload)) if payload else None
        corr_buf = (ctypes.c_uint8 * 8).from_buffer_copy(correlation_id) if correlation_id else None
        event = gen_control.kith_control_event_t(
            ts_mono_ns=ctypes.c_uint64(0),
            type=event_type.encode("utf-8"),
            correlation_id=corr_buf,
            payload=payload_buf,
            payload_len=ctypes.c_uint32(len(payload)),
        )
        rc = int(
            self._bridge.lib("control").kith_control_publish_event(
                self._handle,
                ctypes.byref(event),
            )
        )
        check_error(rc, "kith_control_publish_event")

    # -----------------------------------------------------------------------
    # teardown
    # -----------------------------------------------------------------------

    def close(self) -> None:
        """Release the control handle. Idempotent.

        Destroys the wrapper-owned worker pool first (joining its threads
        so no in-flight dispatch aliases the buffers the C destroy frees),
        then the control handle. The reactor must be stopped before this
        call: the C handle's SSE flush timer reschedules itself on the
        reactor, so a pending timer always exists once the listener is
        started. A caller-owned pool attached via :meth:`attach_worker_pool`
        stays the caller's to close.

        Thread safety:
            @thread_safety unsafe — call after the reactor has stopped.
        """
        if self._closed:
            return
        if self._owned_pool is not None:
            owned = self._owned_pool
            self._owned_pool = None
            owned.close()
        self._bridge.lib("control").kith_control_destroy(self._handle)
        self._handle = None
        self._routes.clear()
        self._closed = True

    def __enter__(self) -> Control:
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
