"""Server artifact publication, session delivery and delivery counters."""

from __future__ import annotations

import ctypes

from kith._gateway.delivery import broadcast_cell, deliver_frame
from kith._generated import gateway as gen_gateway
from kith._generated import types as gen_types
from kith.exceptions import (
    KithStateError,
    check_error,
)
from kith.fabric import CellKey
from kith.fabric import _key_to_c as _cell_key_to_c
from kith.gateway import (
    DeliveryTotals,
    Session,
)
from kith.sim import (
    Actor,
    ArtifactKey,
    _actor_to_c,
    _key_to_c,
)

from .state import ServerState


class ServerPublication(ServerState):
    __slots__ = ()

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
        deliver_frame(self._bridge, self._borrowed_gateway(), session.handle, msg_type, payload)

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
        broadcast_cell(self._bridge, self._borrowed_gateway(), key, msg_type, payload)

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


ServerPublication.publish_artifact.__module__ = "kith"
ServerPublication.publish_artifact.__qualname__ = "Server.publish_artifact"

ServerPublication.remove_artifact.__module__ = "kith"
ServerPublication.remove_artifact.__qualname__ = "Server.remove_artifact"

ServerPublication.publish_cell_product.__module__ = "kith"
ServerPublication.publish_cell_product.__qualname__ = "Server.publish_cell_product"

ServerPublication.send.__module__ = "kith"
ServerPublication.send.__qualname__ = "Server.send"

ServerPublication.broadcast_cell.__module__ = "kith"
ServerPublication.broadcast_cell.__qualname__ = "Server.broadcast_cell"

_session_count_property = ServerPublication.__dict__["session_count"]
assert isinstance(_session_count_property, property)
assert _session_count_property.fget is not None
_session_count_property.fget.__module__ = "kith"
_session_count_property.fget.__qualname__ = "Server.session_count"

ServerPublication.delivery_totals.__module__ = "kith"
ServerPublication.delivery_totals.__qualname__ = "Server.delivery_totals"
