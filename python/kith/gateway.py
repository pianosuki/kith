"""Gateway plane wrapper.

The gateway plane owns the session table, the shared cell cache, and the
per-subscriber view composer. It borrows a net handle (for connections and
frame enqueue), a fabric handle (for cell subscribe and drain), and a proto
handle (for replication-frame encoding). This module wraps that surface into
Python types: message handlers are ordinary Python callables that receive a
:class:`Session` and the decoded payload, and the view/cache introspection
calls return dataclasses.

A :class:`Gateway` owns its C handle; a :class:`Session` owns its own C
session handle and must be closed before the gateway is released. Both
release through :meth:`close`.
"""

from __future__ import annotations

import contextlib
import ctypes
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from enum import IntEnum
from types import TracebackType

from kith import _bridge
from kith._generated import configure as configure_all
from kith._generated import fabric as gen_fabric
from kith._generated import gateway as gen_gateway
from kith._generated import types as gen_types
from kith._generated import version as gen_version
from kith.exceptions import KithNetworkError, KithStateError, check_error, code_for
from kith.fabric import CellKey


__all__ = [
    "CacheStats",
    "DeliveryStats",
    "DeliveryTotals",
    "Gateway",
    "PinnedSession",
    "Session",
    "SessionInfo",
    "SessionType",
    "ViewSnapshot",
    "ViewSubject",
    "ViewTotals",
    "tiered_delivery_config",
]


class SessionType(IntEnum):
    """Session authentication type."""

    SUBSCRIBER = int(gen_gateway.kith_gateway_session_type.KITH_GATEWAY_SESSION_SUBSCRIBER)
    APP = int(gen_gateway.kith_gateway_session_type.KITH_GATEWAY_SESSION_APP)
    SERVICE = int(gen_gateway.kith_gateway_session_type.KITH_GATEWAY_SESSION_SERVICE)


@dataclass(frozen=True)
class SessionInfo:
    """Session metadata."""

    session_id: int
    principal_id: int
    actor_id: int
    type: int


@dataclass(frozen=True)
class CacheStats:
    """Shared cell cache statistics.

    ``cell_count`` counts cells tracked in the shared cache;
    ``subscribed_cell_count`` counts cells subscribed on the gateway's
    fabric subscription. Both tables are fixed-size: recurring
    ``KITH_ENOMEM`` from ``window_add`` is a read-the-gauges-first
    signal, not by itself proof either table is full — a failed add is
    retained and healed by the tick pass's retry (the
    ``kith_gateway_window_retries_pending`` metric reads the queue's
    depth), and the getting-started guide's window capacity section
    carries the sizing contract.
    """

    cell_count: int
    subscribed_cell_count: int
    last_refresh_ms: int


@dataclass(frozen=True)
class ViewSnapshot:
    """A subscriber's composed view set metadata."""

    subscriber_actor_id: int
    built_at_ms: int
    candidate_count: int
    selected_count: int
    tier_selected_count: tuple[int, ...]
    class_selected_count: tuple[int, ...]
    sticky_selected_count: int
    demoted_selected_count: int


@dataclass(frozen=True)
class ViewSubject:
    """One subject in a composed view set."""

    actor_id: int
    pos_x: int
    pos_y: int
    pos_z: int
    vel_x: int
    vel_y: int
    vel_z: int
    input_tick: int
    level: int
    subject_class: int
    sticky: bool
    demoted: bool


@dataclass(frozen=True)
class DeliveryStats:
    """Per-session delivery result."""

    enqueued: int
    dropped: int


@dataclass(frozen=True)
class DeliveryTotals:
    """Cumulative delivery totals folded across every deliver call (both
    the inline tick path and the executor's workers)."""

    enqueued: int
    dropped: int
    events_enqueued: int
    suppressed: int


@dataclass(frozen=True)
class ViewTotals:
    """Cumulative view-composition population totals aggregated at each
    delivery-pass visit of a view-holding session."""

    visits: int
    candidate_total: int
    selected_total: int
    candidate_high_watermark: int
    selected_high_watermark: int


def _cell_key_to_c(key: CellKey) -> gen_fabric.kith_fabric_cell_key_t:
    return gen_fabric.kith_fabric_cell_key_t(
        zone=ctypes.c_uint32(key.zone),
        cell_x=ctypes.c_int32(key.cell_x),
        cell_y=ctypes.c_int32(key.cell_y),
        cell_z=ctypes.c_int32(key.cell_z),
        lod=ctypes.c_uint8(key.lod),
        pad=(ctypes.c_uint8 * 3)(),
    )


def _deliver_frame(
    bridge: _bridge.Bridge,
    gateway: object,
    session: object,
    msg_type: int,
    payload: bytes,
) -> None:
    """Encode and enqueue one discrete frame on a session's connection.

    The shared body of the send surfaces (:meth:`Session.send` and its
    composition-root mirrors). The C side copies the payload into the
    enqueued frame, so the bytes only have to stay referenced through the
    call. Backpressure raises the transport family; the remaining codes
    map through their fixed families (a closed connection to the
    lifecycle family, an oversize payload to the protocol family).
    """
    if not session:
        raise KithStateError(
            gen_types.kith_error.KITH_ESTATE,
            "send: the session is closed",
        )
    conn = bridge.lib("gateway").kith_gateway_session_conn(session)
    rc = int(
        bridge.lib("gateway").kith_gateway_deliver_frame(
            gateway,
            conn,
            ctypes.c_uint16(msg_type),
            payload if payload else None,
            len(payload),
        )
    )
    if rc != 0:
        code = code_for(rc)
        if code is gen_types.kith_error.KITH_EAGAIN:
            raise KithNetworkError(
                code,
                "kith_gateway_deliver_frame: connection output queue is full",
            )
        check_error(rc, "kith_gateway_deliver_frame")


def _broadcast_cell(
    bridge: _bridge.Bridge,
    gateway: object,
    key: CellKey,
    msg_type: int,
    payload: bytes,
) -> None:
    """Submit a cell-scoped broadcast on the gateway's request queue.

    The shared body of the broadcast surfaces (:meth:`Server.broadcast_cell`
    and its composition-root mirror). The C side copies the cell key and
    the payload at submit, so the bytes only have to stay referenced
    through the call; the reactor's next tick pass fans one encoded frame
    to every session whose subscription window covers the cell. Queue
    saturation raises the transport family; the remaining codes map
    through their fixed families (an oversize payload to the protocol
    family).
    """
    c_key = _cell_key_to_c(key)
    rc = int(
        bridge.lib("gateway").kith_gateway_broadcast_cell(
            gateway,
            ctypes.byref(c_key),
            ctypes.c_uint16(msg_type),
            payload if payload else None,
            len(payload),
        )
    )
    if rc != 0:
        code = code_for(rc)
        if code is gen_types.kith_error.KITH_EAGAIN:
            raise KithNetworkError(
                code,
                "kith_gateway_broadcast_cell: the broadcast request queue is full",
            )
        check_error(rc, "kith_gateway_broadcast_cell")


class Session:
    """A gateway session wrapping a ``kith_gateway_session_t``.

    The bound connection is not closed by this wrapper; the caller or the
    reactor closes it via the net surface.

    Ownership:
        Owned when created through :meth:`Gateway.create_session` —
        :meth:`close` releases the C session. A dispatch view handed to a
        handler is borrowed: :meth:`close` is a no-op and the handler must
        not release the server's session.
    """

    __slots__ = ("_borrowed", "_bridge", "_closed", "_gateway", "_handle")

    def __init__(
        self,
        bridge: _bridge.Bridge,
        gateway: object,
        handle: object,
        *,
        borrowed: bool = False,
    ) -> None:
        self._bridge = bridge
        self._gateway = gateway
        self._handle = handle
        self._borrowed = borrowed
        self._closed = False

    @property
    def handle(self) -> object:
        """The borrowed ``kith_gateway_session_t*``.

        Thread safety:
            @thread_safety safe — the handle is fixed after creation.
        """
        return self._handle

    def info(self) -> SessionInfo:
        """Return the session's metadata.

        The metadata fields (session id, principal id, and type) are
        immutable after creation; the bound actor id is written atomically
        by ``kith_gateway_session_bind_actor`` and may change between
        successive calls while a handler rebinds the session. The session
        is refcounted, so a caller holding a reference (the reactor's owner
        reference, or a dispatch reference acquired across the
        reactor→worker handoff) may read them concurrently from a worker
        thread.

        Thread safety:
            @thread_safety safe — the session is refcounted and every field
            read here is either immutable after creation or updated only
            through atomics (the bound actor id). A caller holding a
            reference may read concurrently; the session stays alive until
            the last reference drops.
        """
        out = gen_gateway.kith_gateway_session_info_t()
        rc = int(
            self._bridge.lib("gateway").kith_gateway_session_info(
                self._handle,
                ctypes.byref(out),
            )
        )
        check_error(rc, "kith_gateway_session_info")
        return SessionInfo(
            session_id=int(out.session_id),
            principal_id=int(out.principal_id),
            actor_id=int(out.actor_id),
            type=int(out.type),
        )

    def delivery_totals(self) -> DeliveryTotals:
        """Return this session's cumulative delivery totals: the frames
        enqueued, dropped by output backpressure, membership-event frames
        enqueued, and subjects suppressed across every deliver call of the
        session's lifetime. Per-session sums partition the gateway totals
        (:meth:`Gateway.delivery_totals`): every frame counted on the
        gateway appears on exactly one session.

        Thread safety:
            @thread_safety safe — the counters are atomic; readable from
            any thread while a reference to the session lives (a dispatch
            view inside its handler, a pinned session, or the owner).
        """
        out = gen_gateway.kith_gateway_delivery_totals_t()
        rc = int(
            self._bridge.lib("gateway").kith_gateway_session_delivery_totals(
                self._handle,
                ctypes.byref(out),
            )
        )
        check_error(rc, "kith_gateway_session_delivery_totals")
        return DeliveryTotals(
            enqueued=int(out.enqueued),
            dropped=int(out.dropped),
            events_enqueued=int(out.events_enqueued),
            suppressed=int(out.suppressed),
        )

    def bind_actor(self, actor_id: int) -> None:
        """Bind the session to a subscriber actor identifier.

        Rebinding publishes atomically: refreshes and info reads running
        concurrently with this call are race-free. A rebind landing while a
        refresh composes forces that session's view to be rebuilt at the
        next refresh, so at most one composed frame mixes the previous and
        the new identity.

        Args:
            actor_id: The subscriber actor identifier to bind the
                session to.

        Thread safety:
            @thread_safety safe.
        """
        rc = int(
            self._bridge.lib("gateway").kith_gateway_session_bind_actor(
                self._handle,
                ctypes.c_uint64(actor_id),
            )
        )
        check_error(rc, "kith_gateway_session_bind_actor")

    def window_add(self, key: CellKey) -> None:
        """Add a cell to this subscriber's subscription window.

        The gateway subscribes the cell on the fabric via the shared cell
        cache (incrementing the cache's refcount if another local subscriber
        already tracks it). The relevance composer considers only the cells
        in a subscriber's window when building its view set, so a cell no
        subscriber tracks locally is invisible to that subscriber's view.
        Idempotent: adding a cell already in the window is a no-op. A
        capacity failure (``KITH_ENOMEM``: the cell's cache stripe or the
        gateway's fabric interest set has no free slot) raises, and the
        failed add is retained: the tick pass retries it until a slot
        frees, so a one-shot seed heals without a caller-side retry loop;
        ``window_remove`` and ``window_clear`` cancel a retained add. Other
        failure codes pass uncounted and unretained (``KITH_EINVAL`` is a
        caller error; ``KITH_ESTATE`` names a stripe mutex that failed at
        gateway create). On a detached session (one the reactor already tore
        down while a dispatch held a reference) every cell operation is an
        inert no-op: the detach drained all subscriptions and the session is
        unreachable by composition.

        Args:
            key: Cell locator to add.

        Raises:
            KithError: On a registration or allocation failure.

        Thread safety:
            @thread_safety safe — the window is serialized by a per-session
            mutex. May be called from a worker thread while the reactor
            composes the view set.
        """
        rc = int(
            self._bridge.lib("gateway").kith_gateway_session_window_add(
                self._handle,
                ctypes.byref(_cell_key_to_c(key)),
            )
        )
        check_error(rc, "kith_gateway_session_window_add")

    def window_remove(self, key: CellKey) -> None:
        """Remove a cell from this subscriber's subscription window.

        The shared cache's refcount for the cell is decremented and, when no
        local subscriber tracks it, the cell is unsubscribed on the fabric
        and evicted from the cache. Idempotent: removing a cell not in the
        window is a no-op. On a detached session (one the reactor already
        tore down while a dispatch held a reference) every cell operation is
        an inert no-op: the detach drained all subscriptions and the session
        is unreachable by composition.

        Args:
            key: Cell locator to remove.

        Raises:
            KithError: On a failure.

        Thread safety:
            @thread_safety safe — the window is serialized by a per-session
            mutex.
        """
        rc = int(
            self._bridge.lib("gateway").kith_gateway_session_window_remove(
                self._handle,
                ctypes.byref(_cell_key_to_c(key)),
            )
        )
        check_error(rc, "kith_gateway_session_window_remove")

    def window_clear(self) -> None:
        """Remove every cell from this subscriber's subscription window.

        On a detached session (one the reactor already tore down while a
        dispatch held a reference) this clears the inert window without
        touching fabric subscriptions: detach already drained them.

        Thread safety:
            @thread_safety safe — the window is serialized by a per-session
            mutex.
        """
        self._bridge.lib("gateway").kith_gateway_session_window_clear(self._handle)

    def window_count(self) -> int:
        """Return the number of cells in this subscriber's window.

        Raises:
            KithError: On a failure.

        Thread safety:
            @thread_safety safe — the window is serialized by a per-session
            mutex.
        """
        out = ctypes.c_size_t(0)
        rc = int(
            self._bridge.lib("gateway").kith_gateway_session_window_count(
                self._handle,
                ctypes.byref(out),
            )
        )
        check_error(rc, "kith_gateway_session_window_count")
        return int(out.value)

    def populate(self, actor_id: int, cells: Sequence[CellKey] = ()) -> None:
        """Bind the session to a subscriber actor and seed its window in one
        atomic step.

        The delivery startup contract's identity and subscription halves
        land together: under one hold of the session's window mutex the
        actor id publishes, then each cell joins the window with the
        per-cell add semantics of :meth:`window_add` (idempotent
        membership, subscription via the shared cache). No observer reads
        the gap between the halves: the seed-failure census and the
        relevance composer both snapshot the session under the same mutex,
        so the session is either unpopulated or populated — a game-side
        sweep can gate on the census gauge.

        A capacity failure (``KITH_ENOMEM``: a cell's cache stripe or the
        gateway's fabric interest set has no free slot) raises, and the
        call is not fatal: the bind stays, landed cells stay, and every
        failed add is retained and retried by the tick pass until a slot
        frees — the window converges to the requested cells, and
        ``window_remove`` or ``window_clear`` cancels a retained add.
        Per-cell precision is the per-cell :meth:`window_add` call's job.
        An empty ``cells`` is a legal bind-only populate. On a detached
        session (one the reactor already tore down while a dispatch held
        a reference) the bind publishes and the seed is an inert no-op,
        mirroring the window operations.

        Args:
            actor_id: Subscriber actor identifier to bind.
            cells: Cell locators to seed; duplicates are idempotent. An
                empty sequence binds without seeding.

        Raises:
            KithError: On a caller error (``KITH_EINVAL``) or a capacity
                failure (``KITH_ENOMEM`` — retention-backed; a one-shot
                seed heals without a caller-side retry loop).

        Thread safety:
            @thread_safety safe — the bind publishes the actor id
            atomically and the seed is serialized by the per-session
            window mutex, both under one critical section. May be called
            from a worker thread while the reactor composes the view set.
            The call acquires no reference: the caller must hold one (the
            dispatch reference a handler runs under, or an owned or
            pinned session).
        """
        count = len(cells)
        lib = self._bridge.lib("gateway")
        if count == 0:
            rc = int(
                lib.kith_gateway_session_populate(
                    self._handle,
                    ctypes.c_uint64(actor_id),
                    None,
                    0,
                )
            )
        else:
            keys = (gen_fabric.kith_fabric_cell_key_t * count)(
                *(_cell_key_to_c(cell) for cell in cells)
            )
            rc = int(
                lib.kith_gateway_session_populate(
                    self._handle,
                    ctypes.c_uint64(actor_id),
                    keys,
                    count,
                )
            )
        check_error(rc, "kith_gateway_session_populate")

    def send(self, msg_type: int, payload: bytes = b"") -> None:
        """Send one discrete message frame to this session's connection.

        The one-off send surface outside the composed replication stream:
        a login ack, a targeted event, a reply. The frame is encoded
        through the gateway's proto and enqueued on the session's
        connection; the reactor flushes the connection's output queue.
        The gateway's replication cadence is untouched — a sent frame
        rides the connection ahead of (or between) the tick's composed
        frames, and the receiving client decodes it by its header.

        The call is legal while the session is alive by reference — from
        the message handler that received this view (the dispatch
        reference holds for the handler's duration), or from the reactor
        thread. A view held past its dispatch has no reference and no
        guarantee. The owning gateway must be alive: for a facade
        dispatch view that is the server's lifetime. The send appends no
        correlation trailer (request/reply correlation is a payload
        convention) and does not consult the type registry — the
        receiving client's decoder does — so both peers register
        ``msg_type`` on their protos.

        Args:
            msg_type: Wire message type id.
            payload: Frame payload bytes; an empty payload sends a
                header-only frame.

        Raises:
            KithNetworkError: When the connection's output queue is at
                its high watermark (backpressure). The frame was not
                queued; the caller decides retry semantics.
            KithStateError: When the session is closed or its connection
                has closed.
            KithProtocolError: When the payload exceeds the negotiated
                maximum frame payload.
            KithError: On other failures.

        Thread safety:
            @thread_safety safe-if the session is alive by reference (a
            dispatch view inside its handler, or a session the caller
            owns); unsafe from contexts holding only a stale view.
        """
        _deliver_frame(self._bridge, self._gateway, self._handle, msg_type, payload)

    def pin(self) -> PinnedSession:
        """Pin the session past its dispatch with a counted reference.

        Returns a :class:`PinnedSession` whose :meth:`PinnedSession.close`
        releases the pin. The call is legal only where a reference is
        already held: inside a handler under the dispatch reference — which
        dies when the handler returns, so a pin from a view stored across
        handler returns is undefined behavior on a potentially freed
        session — or on the reactor thread. A pinned session outlives the
        reactor's destroy: ``send`` follows the discrete send contract (a
        closed connection raises ``KithStateError``), and the identity
        record stays readable until the release.

        Raises:
            KithStateError: When the session was already closed by its
                owner.

        Thread safety:
            @thread_safety safe-if the caller already holds a reference to
            the session (a dispatch reference inside a handler, or the
            owner reference on the reactor thread) — the increment is
            atomic, but a stale pointer is undefined.

        Ownership:
            Returns a pinned session: the caller releases it through
            :meth:`PinnedSession.close`.
        """
        if self._closed:
            raise KithStateError(gen_types.kith_error.KITH_ESTATE, "pin: the session is closed")
        self._bridge.lib("gateway").kith_gateway_session_acquire(self._handle)
        return PinnedSession(self._bridge, self._gateway, self._handle)

    def close(self) -> None:
        """Release the session handle when owned; a no-op for a borrowed view.

        A session the composition root owns (looked up during dispatch) is
        handed to a handler as a borrowed view so the handler never frees the
        server's session; only a session the caller created (borrowed=False)
        is destroyed here. A borrowed close leaves the view untouched so the
        handler can keep using it after closing; marking it closed without
        releasing would break every subsequent call on the view.

        The destroy path is serialized with the window operations: the
        gateway pointer detaches under the window mutex and every remaining
        cell subscription is drained there, so a dispatch racing teardown
        completes its geometry bookkeeping as inert no-ops instead of
        failing.

        Thread safety:
            @thread_safety safe-if called from the reactor thread — the
            session table is not synchronized (reactor-thread-only). Safe to
            call while a worker dispatch is in flight on the session: the
            session is freed when the last reference drops.
        """
        if self._closed:
            return
        if not self._borrowed:
            self._bridge.lib("gateway").kith_gateway_session_destroy(self._handle)
            self._handle = None
            self._closed = True

    def __enter__(self) -> Session:
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


class PinnedSession:
    """A session pinned past its dispatch with a counted reference.

    Created by :meth:`Session.pin` inside the context that already holds a
    reference (a handler under its dispatch reference). The underlying
    session survives the reactor's destroy — a closed connection raises the
    lifecycle family from :meth:`send`, per the send contract — and the
    session's memory frees when the last reference releases.
    """

    def __init__(self, bridge: _bridge.Bridge, gateway: object, handle: object) -> None:
        """Create a pinned-session view from a counted session reference.

        Args:
            bridge: The loaded bridge the session's library calls go
                through.
            gateway: The owning gateway, as a raw handle or an object
                exposing ``.handle``.
            handle: The pinned ``kith_gateway_session_t`` pointer.
        """
        self._bridge = bridge
        self._gateway = gateway
        self._handle = handle
        self._released = False

    def send(self, msg_type: int, payload: bytes = b"") -> None:
        """Send one discrete message frame to the pinned session.

        The same contract as :meth:`Session.send`, legal wherever the pin
        keeps the session alive: backpressure raises ``KithNetworkError``,
        a closed connection raises ``KithStateError`` (the connection
        closes when the peer disconnects, which is the ordinary fate of a
        pin held across a disconnect), an oversize payload raises
        ``KithProtocolError``.

        Args:
            msg_type: The message type id to send the frame as.
            payload: The frame's payload bytes.

        Thread safety:
            @thread_safety safe-if the session is alive by reference — the
            pin is that reference, from any thread; the send's internal
            queue is mutex-serialized.
        """
        _deliver_frame(self._bridge, self._gateway, self._handle, msg_type, payload)

    def info(self) -> SessionInfo:
        """Return the session's metadata (the same record as
        :meth:`Session.info`).

        Thread safety:
            @thread_safety safe — the pin keeps the session alive; every
            field read is immutable after creation or atomic (the bound
            actor id).
        """
        out = gen_gateway.kith_gateway_session_info_t()
        rc = int(
            self._bridge.lib("gateway").kith_gateway_session_info(
                self._handle,
                ctypes.byref(out),
            )
        )
        check_error(rc, "kith_gateway_session_info")
        return SessionInfo(
            session_id=int(out.session_id),
            principal_id=int(out.principal_id),
            actor_id=int(out.actor_id),
            type=int(out.type),
        )

    def delivery_totals(self) -> DeliveryTotals:
        """Return the pinned session's cumulative delivery totals (the
        same record as :meth:`Session.delivery_totals`).

        Thread safety:
            @thread_safety safe — the pin keeps the session alive; the
            counters are atomic and readable from any thread.
        """
        out = gen_gateway.kith_gateway_delivery_totals_t()
        rc = int(
            self._bridge.lib("gateway").kith_gateway_session_delivery_totals(
                self._handle,
                ctypes.byref(out),
            )
        )
        check_error(rc, "kith_gateway_session_delivery_totals")
        return DeliveryTotals(
            enqueued=int(out.enqueued),
            dropped=int(out.dropped),
            events_enqueued=int(out.events_enqueued),
            suppressed=int(out.suppressed),
        )

    def close(self) -> None:
        """Release the pinned reference. Idempotent.

        The teardown touches no gateway or session-table state, so this is
        safe on any thread — including from a game thread that finished
        with a pinned session asynchronously.

        Thread safety:
            @thread_safety safe — atomic refcount decrement; the last
            release frees the session.
        """
        if self._released:
            return
        self._bridge.lib("gateway").kith_gateway_session_release(self._handle)
        self._handle = None
        self._released = True

    def __del__(self) -> None:
        if getattr(self, "_released", True):
            return
        with contextlib.suppress(Exception):
            self.close()


_Handler = Callable[[int, bytes, Session], None]
_DestroyedHandler = Callable[[SessionInfo], None]


class Gateway:
    """Gateway plane wrapping a ``kith_gateway_t``.

    Args:
        net: Borrowed net handle (object exposing ``.handle``, or a raw
            ctypes pointer).
        fabric: Borrowed :class:`kith.fabric.Fabric` (or raw handle).
        proto: Borrowed :class:`kith.proto.Proto` (or raw handle).
        max_sessions: Maximum simultaneous sessions; 0 selects the default.
        cache_bucket_count: Shared cell-cache hash bucket count; 0 selects
            the default (4096).
        view_bucket_count: Per-subscriber view-store hash bucket count; 0
            selects the default (4096).
        view_max_subjects: Per-subscriber view-set subject capacity; 0
            selects the default (512). The subscribing subject occupies one
            slot of the set, and candidates past the budget drop from the
            delivered individual set — the view candidate and selected
            high-watermark gauges report the approaching-the-cap evidence.
        view_refresh_interval_ms: View compose-and-deliver interval in
            milliseconds; 0 selects the default (100).
        cache_refresh_interval_ms: Cell-cache refresh interval in
            milliseconds; 0 selects the default (100).
        handler_table_size: Handler-table capacity (number of message-type
            slots). The gateway dispatches by direct index, so this must
            exceed the largest message type id the game registers. 0
            selects the gateway default (256), which covers framework
            types but not user types (ids >= 1000); a game using user
            types sets this to cover its highest type id.
        replication_type_id: Message type id for per-subject replication
            frames; 0 disables per-subject delivery.
        replication_batch_type_id: Message type id for multi-subject batch
            replication frames. When non-zero, the gateway packs the full
            view set into one frame per refresh instead of one frame per
            subject; ``replication_type_id`` is ignored. 0 selects
            per-subject delivery.
        crowd_exit_margin: Crowd-regime exit margin in candidates; 0 selects
            the composer default (one eighth of the view budget, floor 2).
        delivery_strategy: Registered delivery strategy name (``"full"``,
            ``"tiered"``, or a game-registered name); ``None`` selects the
            factory default. Unknown names fail session creation.
        delivery_config: Strategy configuration as bytes (a size-versioned
            struct image whose leading ``uint32`` declares its length), or
            ``None``. The gateway copies it at creation.
        delivery_worker_count: Delivery executor thread count. 0 keeps
            delivery inline on the reactor thread; a non-zero count moves
            the tick's deliver pass onto executor threads (values above
            64 clamp to 64).
        delivery_wait_budget_us: Per-pass compose-wait budget in
            microseconds. 0 selects the gateway default; consulted only
            when ``delivery_worker_count`` is non-zero.
        bridge: A loaded :class:`kith._bridge.Bridge`, or ``None`` to load the
            process-wide singleton.

    Raises:
        KithError: When the gateway handle cannot be built.
        BridgeError: When the shared libraries cannot be loaded.

    Thread safety:
        @thread_safety safe — construction shares no mutable state across
        callers.

    Ownership:
        Owns its ``kith_gateway_t`` handle; :meth:`close` releases it.
    """

    __slots__ = (
        "_bridge",
        "_closed",
        "_destroyed_handler",
        "_destroyed_trampoline",
        "_handle",
        "_handlers",
    )

    def __init__(
        self,
        net: object,
        fabric: object,
        proto: object,
        *,
        max_sessions: int = 0,
        cache_bucket_count: int = 0,
        view_bucket_count: int = 0,
        view_max_subjects: int = 0,
        view_refresh_interval_ms: int = 0,
        cache_refresh_interval_ms: int = 0,
        handler_table_size: int = 0,
        replication_type_id: int = 0,
        replication_batch_type_id: int = 0,
        crowd_exit_margin: int = 0,
        delivery_strategy: str | None = None,
        delivery_config: bytes | None = None,
        delivery_worker_count: int = 0,
        delivery_wait_budget_us: int = 0,
        bridge: _bridge.Bridge | None = None,
    ) -> None:
        loaded = bridge if bridge is not None else _bridge.load()
        configure_all(loaded)
        self._bridge = loaded
        self._handle: object = None
        self._handlers: dict[int, tuple[object, object]] = {}
        self._destroyed_trampoline: object | None = None
        self._destroyed_handler: _DestroyedHandler | None = None
        self._closed = True

        net_h = net.handle if hasattr(net, "handle") else net
        fabric_h = fabric.handle if hasattr(fabric, "handle") else fabric
        proto_h = proto.handle if hasattr(proto, "handle") else proto
        # The config image is copied by the gateway during create, so the
        # buffer only has to stay referenced through the call below; a local
        # holds it because ctypes does not keep a c_void_p field's source
        # object alive on its own.
        config_buffer: ctypes.Array[ctypes.c_char] | None = (
            ctypes.create_string_buffer(delivery_config, len(delivery_config))
            if delivery_config is not None
            else None
        )
        params = gen_gateway.kith_gateway_params_t(
            size=ctypes.sizeof(gen_gateway.kith_gateway_params_t),
            abi_version=gen_version.KITH_ABI_VERSION,
            max_sessions=max_sessions,
            cache_bucket_count=cache_bucket_count,
            view_bucket_count=view_bucket_count,
            view_max_subjects=view_max_subjects,
            view_refresh_interval_ms=view_refresh_interval_ms,
            cache_refresh_interval_ms=cache_refresh_interval_ms,
            handler_table_size=handler_table_size,
            replication_type_id=ctypes.c_uint16(replication_type_id),
            replication_batch_type_id=ctypes.c_uint16(replication_batch_type_id),
            crowd_exit_margin=crowd_exit_margin,
            delivery_strategy=(
                ctypes.c_char_p(delivery_strategy.encode("utf-8"))
                if delivery_strategy is not None
                else None
            ),
            delivery_config=(
                ctypes.cast(config_buffer, ctypes.c_void_p) if config_buffer is not None else None
            ),
            delivery_worker_count=delivery_worker_count,
            delivery_wait_budget_us=delivery_wait_budget_us,
        )
        out = ctypes.POINTER(gen_gateway.kith_gateway_t)()
        rc = int(
            loaded.lib("gateway").kith_gateway_create(
                ctypes.byref(params),
                net_h,
                fabric_h,
                proto_h,
                None,
                ctypes.byref(out),
            )
        )
        check_error(rc, "kith_gateway_create")
        self._handle = out
        self._closed = False

    @property
    def handle(self) -> object:
        """The borrowed ``kith_gateway_t*`` for passing to a C function.

        Thread safety:
            @thread_safety safe — the handle is fixed after creation.
        """
        return self._handle

    # -----------------------------------------------------------------------
    # sessions
    # -----------------------------------------------------------------------

    def create_session(
        self,
        conn: object,
        session_type: SessionType,
        principal_id: int,
    ) -> Session:
        """Create a session bound to ``conn``.

        Args:
            conn: The connection to bind, as an object exposing
                ``.handle`` or a raw ctypes pointer.
            session_type: The session's authentication type (a
                :class:`SessionType`).
            principal_id: The authenticated principal the session
                belongs to.

        Raises:
            KithStateError: When the session table is full (``KITH_EBUSY``).
            KithError: On invalid arguments (``KITH_EINVAL``).

        Thread safety:
            @thread_safety unsafe — the session table is not synchronized.

        Ownership:
            Returns an owned session: the caller closes it through
            :meth:`Session.close`.
        """
        conn_h = conn.handle if hasattr(conn, "handle") else conn
        out = ctypes.POINTER(gen_gateway.kith_gateway_session_t)()
        rc = int(
            self._bridge.lib("gateway").kith_gateway_session_create(
                self._handle,
                conn_h,
                ctypes.c_uint32(int(session_type)),
                ctypes.c_uint64(principal_id),
                None,
                ctypes.byref(out),
            )
        )
        check_error(rc, "kith_gateway_session_create")
        return Session(self._bridge, self._handle, out)

    # -----------------------------------------------------------------------
    # handlers
    # -----------------------------------------------------------------------

    def register_handler(self, msg_type: int, handler: _Handler) -> None:
        """Register a Python handler for ``msg_type``.

        The handler receives ``(msg_type, payload_bytes, session)`` when a
        decoded frame of this type is dispatched. Replaces any prior
        registration for ``msg_type``.

        An exception the handler raises is caught at the dispatch boundary
        and counted in ``kith_python_handler_exceptions_total``; the first
        exception per registration prints its traceback to stderr, subsequent
        ones count silently, and the dispatch continues.

        Args:
            msg_type: The message type id dispatched to ``handler``.
            handler: The Python handler invoked for frames of this type.

        Thread safety:
            @thread_safety safe — the handler table is serialized by a
            per-handle mutex.
        """
        trampoline = self._build_handler_trampoline(handler)
        rc = int(
            self._bridge.lib("gateway").kith_gateway_register_handler(
                self._handle,
                ctypes.c_uint16(msg_type),
                trampoline,
                None,
            )
        )
        check_error(rc, f"kith_gateway_register_handler: msg_type={msg_type}")
        self._handlers[msg_type] = (trampoline, handler)

    def unregister_handler(self, msg_type: int) -> None:
        """Unregister the handler for ``msg_type``. Idempotent.

        Args:
            msg_type: The message type id whose handler to remove.

        Thread safety:
            @thread_safety safe — the handler table is serialized by a
            per-handle mutex.
        """
        rc = int(
            self._bridge.lib("gateway").kith_gateway_unregister_handler(
                self._handle,
                ctypes.c_uint16(msg_type),
            )
        )
        check_error(rc, f"kith_gateway_unregister_handler: msg_type={msg_type}")
        self._handlers.pop(msg_type, None)

    def _build_handler_trampoline(self, handler: _Handler) -> object:
        guarded = _bridge.guarded_handler(handler, "message handler")

        def _trampoline(
            msg_type: int,
            payload: int,
            payload_len: int,
            session_ptr: object,
            _user_data: object,
        ) -> None:
            data = bytes(ctypes.string_at(payload, payload_len)) if payload_len and payload else b""
            # The session is borrowed for the dispatch call: the gateway (via
            # the server wire) owns it and frees it on connection close. The
            # dispatch reference acquired in kith_gateway_dispatch keeps it
            # alive for the handler's duration. Marking the wrapper borrowed
            # keeps __del__/close() from destroying it a second time, freeing
            # the session before the worker task releases its dispatch
            # reference and double-freeing it.
            session = Session(self._bridge, self._handle, session_ptr, borrowed=True)
            guarded(int(msg_type), data, session)

        return gen_gateway.kith_gateway_msg_handler_fn(_trampoline)

    def on_session_destroyed(self, handler: _DestroyedHandler) -> None:
        """Register the destroyed-session callback.

        Invoked as ``handler(info)`` on a pool worker when the gateway
        destroys a session — after the session detaches from the table and
        its subscriptions, before the owner reference drops — with the
        identity record of the destroyed session. This is the session
        death signal: a game that keeps per-session state (rosters, seats,
        per-session caches) reclaims it here. The callback must not rely
        on notification ordering across sessions, and a session destroyed
        by its owner after the gateway is gone delivers nothing. The
        registration is pool-dispatched (``KITH_GATEWAY_HANDLER_PYTHON``):
        like every pool callback, it shares the worker pool with tick
        handlers and message handlers, so game state it touches is
        synchronized by the game. Replaces any prior registration;
        :meth:`off_session_destroyed` detaches.

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
            @thread_safety safe — the registration slot is serialized by
            a mutex on the C side.
        """
        trampoline = self._build_destroyed_trampoline(handler)
        rc = int(
            self._bridge.lib("gateway").kith_gateway_register_session_destroyed_handler_flags(
                self._handle,
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
            @thread_safety safe — the registration slot is serialized by
            a mutex on the C side.
        """
        rc = int(
            self._bridge.lib("gateway").kith_gateway_unregister_session_destroyed_handler(
                self._handle
            )
        )
        check_error(rc, "kith_gateway_unregister_session_destroyed_handler")
        self._destroyed_trampoline = None
        self._destroyed_handler = None

    @property
    def session_count(self) -> int:
        """The number of sessions currently in the gateway's session table.

        A mirror gauge of the table, readable from any thread — the count
        moves with the same reactor-thread create and destroy sites that
        mutate the table.

        Thread safety:
            @thread_safety safe — the mirror gauge is an atomic read.
        """
        return int(self._bridge.lib("gateway").kith_gateway_session_count(self._handle))

    def session_snapshot(self) -> list[SessionInfo]:
        """Copy the identity record of every live session.

        A point-in-time roster of the gateway's session table: entries in
        no particular order, each carrying the same fields the
        session-destroyed callback delivers. The copy is sized from the
        :attr:`session_count` mirror gauge; a table that grows between
        the sizing read and the copy leaves the excess uncopied.

        Thread safety:
            @thread_safety unsafe — the session table is not synchronized
            (reactor-thread-only), like session creation itself.
        """
        count = self.session_count
        if count == 0:
            return []
        out = (gen_gateway.kith_gateway_session_info_t * count)()
        out_count = ctypes.c_size_t(0)
        rc = int(
            self._bridge.lib("gateway").kith_gateway_session_snapshot(
                self._handle,
                out,
                ctypes.c_size_t(count),
                ctypes.byref(out_count),
            )
        )
        check_error(rc, "kith_gateway_session_snapshot")
        return [
            SessionInfo(
                session_id=int(record.session_id),
                principal_id=int(record.principal_id),
                actor_id=int(record.actor_id),
                type=int(record.type),
            )
            for record in out[: out_count.value]
        ]

    def _build_destroyed_trampoline(self, handler: _DestroyedHandler) -> object:
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

    # -----------------------------------------------------------------------
    # subscribe / cache / view
    # -----------------------------------------------------------------------

    def subscribe(self, key: CellKey) -> None:
        """Add a cell key to the shared cache's subscription set.

        Args:
            key: The cell key to add to the subscription set.

        Thread safety:
            @thread_safety safe — the target cell's stripe lock serializes
            subscribe against the shared cache.
        """
        rc = int(
            self._bridge.lib("gateway").kith_gateway_subscribe(
                self._handle,
                ctypes.byref(_cell_key_to_c(key)),
            )
        )
        check_error(rc, "kith_gateway_subscribe")

    def unsubscribe(self, key: CellKey) -> None:
        """Remove a cell key from the shared cache's subscription set.

        Args:
            key: The cell key to remove from the subscription set.

        Thread safety:
            @thread_safety safe — the target cell's stripe lock serializes
            unsubscribe against the shared cache.
        """
        rc = int(
            self._bridge.lib("gateway").kith_gateway_unsubscribe(
                self._handle,
                ctypes.byref(_cell_key_to_c(key)),
            )
        )
        check_error(rc, "kith_gateway_unsubscribe")

    def cache_refresh(self, now_ms: int) -> None:
        """Refresh the shared cache against the fabric.

        Args:
            now_ms: The current monotonic time in milliseconds, driving
                the refresh interval gate; calls within the interval are
                no-ops.

        Thread safety:
            @thread_safety unsafe — the cache is not synchronized.
        """
        rc = int(
            self._bridge.lib("gateway").kith_gateway_cache_refresh(
                self._handle,
                ctypes.c_uint64(now_ms),
            )
        )
        check_error(rc, "kith_gateway_cache_refresh")

    def cache_stats(self) -> CacheStats:
        """Return shared cell cache statistics.

        Thread safety:
            @thread_safety unsafe.
        """
        out = gen_gateway.kith_gateway_cache_stats_t()
        rc = int(
            self._bridge.lib("gateway").kith_gateway_cache_stats(
                self._handle,
                ctypes.byref(out),
            )
        )
        check_error(rc, "kith_gateway_cache_stats")
        return CacheStats(
            cell_count=int(out.cell_count),
            subscribed_cell_count=int(out.subscribed_cell_count),
            last_refresh_ms=int(out.last_refresh_ms),
        )

    def view_refresh(self, session: Session, now_ms: int) -> None:
        """Recompose ``session``'s view set against the shared cache.

        Args:
            session: The session whose view set to recompose.
            now_ms: The current monotonic time in milliseconds, driving
                the refresh interval gate.

        Thread safety:
            @thread_safety unsafe — call from the reactor thread.
        """
        rc = int(
            self._bridge.lib("gateway").kith_gateway_view_refresh(
                self._handle,
                session.handle,
                ctypes.c_uint64(now_ms),
            )
        )
        check_error(rc, "kith_gateway_view_refresh")

    def view_snapshot(
        self, session: Session, *, capacity: int = 512
    ) -> tuple[ViewSnapshot, list[ViewSubject]]:
        """Return ``session``'s composed view set metadata and subjects.

        Args:
            session: The session whose composed view set is read.
            capacity: Upper bound on the returned subject list. When the
                view set holds more subjects than ``capacity`` (possible
                when ``view_max_subjects`` was raised above the default),
                the surplus is silently absent from the returned list;
                ``selected_count`` on the metadata reports the true size,
                so raise ``capacity`` to match it before reading.

        Returns:
            The snapshot metadata and the copied subject list.

        Thread safety:
            @thread_safety unsafe — call from the reactor thread.
        """
        snap = gen_gateway.kith_gateway_view_snapshot_t()
        buf = (gen_gateway.kith_gateway_view_subject_t * capacity)()
        out_count = ctypes.c_size_t(0)
        rc = int(
            self._bridge.lib("gateway").kith_gateway_view_snapshot(
                self._handle,
                session.handle,
                ctypes.byref(snap),
                buf,
                ctypes.c_size_t(capacity),
                ctypes.byref(out_count),
            )
        )
        check_error(rc, "kith_gateway_view_snapshot")
        subjects = [self._subject_from_c(buf[i]) for i in range(int(out_count.value))]
        view = ViewSnapshot(
            subscriber_actor_id=int(snap.subscriber_actor_id),
            built_at_ms=int(snap.built_at_ms),
            candidate_count=int(snap.candidate_count),
            selected_count=int(snap.selected_count),
            tier_selected_count=tuple(int(snap.tier_selected_count[i]) for i in range(3)),
            class_selected_count=tuple(int(snap.class_selected_count[i]) for i in range(3)),
            sticky_selected_count=int(snap.sticky_selected_count),
            demoted_selected_count=int(snap.demoted_selected_count),
        )
        return view, subjects

    def deliver(self, session: Session, now_ms: int) -> DeliveryStats:
        """Encode and enqueue the session's view set as replication frames.

        Args:
            session: The session whose composed view set to encode and
                enqueue.
            now_ms: The current monotonic time in milliseconds.

        Thread safety:
            @thread_safety unsafe — call from the reactor thread.
        """
        out = gen_gateway.kith_gateway_delivery_stats_t()
        rc = int(
            self._bridge.lib("gateway").kith_gateway_deliver(
                self._handle,
                session.handle,
                ctypes.c_uint64(now_ms),
                ctypes.byref(out),
            )
        )
        check_error(rc, "kith_gateway_deliver")
        return DeliveryStats(enqueued=int(out.enqueued), dropped=int(out.dropped))

    def send(self, session: Session, msg_type: int, payload: bytes = b"") -> None:
        """Send one discrete message frame to ``session``'s connection.

        The composition-root mirror of :meth:`Session.send` for a gateway
        driven by hand: the same encode-and-enqueue through this
        gateway's proto, reported at the call site (backpressure raises
        the transport family, a closed connection the lifecycle family,
        an oversize payload the protocol family). Legal from the same
        contexts — the caller holds ``session`` by reference (an owned
        session from :meth:`create_session`, or a dispatch view inside
        its handler).

        Args:
            session: The session to send to.
            msg_type: Wire message type id.
            payload: Frame payload bytes; an empty payload sends a
                header-only frame.

        Raises:
            KithNetworkError: When the connection's output queue is at
                its high watermark (backpressure).
            KithStateError: When the session is closed or its connection
                has closed.
            KithProtocolError: When the payload exceeds the negotiated
                maximum frame payload.
            KithError: On other failures.

        Thread safety:
            @thread_safety safe-if ``session`` is alive by reference;
            unsafe from contexts holding only a stale view.
        """
        _deliver_frame(self._bridge, self._handle, session.handle, msg_type, payload)

    def broadcast_cell(self, key: CellKey, msg_type: int, payload: bytes = b"") -> None:
        """Submit a cell-scoped broadcast on this gateway's request queue.

        The composition-root mirror of :meth:`Server.broadcast_cell` for a
        gateway driven by hand: the same request-queue submit, delivered on
        this gateway's next tick pass. Queue saturation raises the
        transport family (the request was not queued), an oversize payload
        the protocol family, a shutting-down gateway the lifecycle family.

        Args:
            key: Cell locator (zone, cell, lod) the broadcast is scoped to.
            msg_type: Wire message type id.
            payload: Frame payload bytes; an empty payload sends a
                header-only frame.

        Raises:
            KithNetworkError: When the request queue is full (saturation).
            KithStateError: When the gateway is shutting down.
            KithProtocolError: When the payload exceeds the negotiated
                maximum frame payload.
            KithError: On other failures.

        Thread safety:
            @thread_safety safe — the request queue is internally
            synchronized.
        """
        _broadcast_cell(self._bridge, self._handle, key, msg_type, payload)

    def delivery_totals(self) -> DeliveryTotals:
        """Return the gateway's cumulative delivery totals.

        Thread safety:
            @thread_safety safe — the counters are atomic; readable from
            any thread.
        """
        out = gen_gateway.kith_gateway_delivery_totals_t()
        rc = int(
            self._bridge.lib("gateway").kith_gateway_delivery_totals(
                self._handle,
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

    def view_totals(self) -> ViewTotals:
        """Return the gateway's cumulative view-composition population
        totals.

        Thread safety:
            @thread_safety safe — the counters are atomic; readable from
            any thread.
        """
        out = gen_gateway.kith_gateway_view_totals_t()
        rc = int(
            self._bridge.lib("gateway").kith_gateway_view_totals(
                self._handle,
                ctypes.byref(out),
            )
        )
        check_error(rc, "kith_gateway_view_totals")
        return ViewTotals(
            visits=int(out.visits),
            candidate_total=int(out.candidate_total),
            selected_total=int(out.selected_total),
            candidate_high_watermark=int(out.candidate_high_watermark),
            selected_high_watermark=int(out.selected_high_watermark),
        )

    def tick(self, now_ms: int) -> None:
        """Drive one gateway tick: refresh the cache, then refresh and deliver
        every bound session's view set on the reactor thread.

        This folds cache maintenance, view composition, and delivery into one
        reactor-thread pass; a composition root driving the server run loop
        does not call the three separately.

        Args:
            now_ms: The current monotonic time in milliseconds driving
                the pass.

        Thread safety:
            @thread_safety unsafe — call from the reactor thread.
        """
        rc = int(
            self._bridge.lib("gateway").kith_gateway_tick(
                self._handle,
                ctypes.c_uint64(now_ms),
            )
        )
        check_error(rc, "kith_gateway_tick")

    @staticmethod
    def _subject_from_c(s: gen_gateway.kith_gateway_view_subject_t) -> ViewSubject:
        return ViewSubject(
            actor_id=int(s.actor_id),
            pos_x=int(s.pos_x),
            pos_y=int(s.pos_y),
            pos_z=int(s.pos_z),
            vel_x=int(s.vel_x),
            vel_y=int(s.vel_y),
            vel_z=int(s.vel_z),
            input_tick=int(s.input_tick),
            level=int(s.level),
            subject_class=int(s.subject_class),
            sticky=bool(s.sticky),
            demoted=bool(s.demoted),
        )

    # -----------------------------------------------------------------------
    # teardown
    # -----------------------------------------------------------------------

    def close(self) -> None:
        """Release the gateway handle. Idempotent.

        Thread safety:
            @thread_safety unsafe — no session/cache/view/deliver/dispatch may
            be in flight on the gateway when this is called.
        """
        if self._closed:
            return
        self._bridge.lib("gateway").kith_gateway_destroy(self._handle)
        self._handle = None
        self._handlers.clear()
        self._destroyed_trampoline = None
        self._destroyed_handler = None
        self._closed = True

    def __enter__(self) -> Gateway:
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


def tiered_delivery_config(
    *,
    full_interval_ms: int = 0,
    reduced_interval_ms: int = 200,
    crowd_interval_ms: int = 500,
    max_gap_ms: int = 1000,
) -> bytes:
    """Return a ``"tiered"`` strategy configuration image.

    The image is a size-versioned struct whose leading ``uint32`` declares
    its length, assembled from the generated ctypes binding so its layout
    tracks the ABI exactly. Pass it as ``delivery_config`` alongside
    ``delivery_strategy="tiered"`` on :class:`kith.Server` or
    :class:`Gateway`; the gateway copies it at creation. The defaults match
    the documented C defaults, pinned explicitly so a run's cadence table is
    recorded at the call site.

    Args:
        full_interval_ms: Minimum milliseconds between sends of a FULL-tier
            subject; 0 sends every pass.
        reduced_interval_ms: Minimum milliseconds between sends of a
            REDUCED-tier subject.
        crowd_interval_ms: Minimum milliseconds between sends of a CROWD-tier
            subject.
        max_gap_ms: Maximum silence before any subject is refreshed
            regardless of change.

    Returns:
        The encoded ``kith_gateway_tiered_config_t`` image.
    """
    config = gen_gateway.kith_gateway_tiered_config_t(
        size=ctypes.sizeof(gen_gateway.kith_gateway_tiered_config_t),
        abi_version=gen_version.KITH_ABI_VERSION,
        full_interval_ms=full_interval_ms,
        reduced_interval_ms=reduced_interval_ms,
        crowd_interval_ms=crowd_interval_ms,
        max_gap_ms=max_gap_ms,
    )
    return bytes(config)
