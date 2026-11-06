"""Gateway session implementation."""

from __future__ import annotations

import contextlib
import ctypes
from collections.abc import Sequence
from types import TracebackType

from kith import _bridge
from kith._generated import fabric as gen_fabric
from kith._generated import gateway as gen_gateway
from kith._generated import types as gen_types
from kith.exceptions import KithStateError, check_error
from kith.fabric import CellKey

from .delivery import cell_key_to_c, deliver_frame
from .types import DeliveryTotals, SessionInfo


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
                ctypes.byref(cell_key_to_c(key)),
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
                ctypes.byref(cell_key_to_c(key)),
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
                *(cell_key_to_c(cell) for cell in cells)
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
        deliver_frame(self._bridge, self._gateway, self._handle, msg_type, payload)

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
        deliver_frame(self._bridge, self._gateway, self._handle, msg_type, payload)

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


Session.__module__ = "kith.gateway"

PinnedSession.__module__ = "kith.gateway"
