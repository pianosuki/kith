"""Gateway delivery implementation."""

from __future__ import annotations

import ctypes

from kith import _bridge
from kith._generated import fabric as gen_fabric
from kith._generated import gateway as gen_gateway
from kith._generated import types as gen_types
from kith._generated import version as gen_version
from kith.exceptions import KithNetworkError, KithStateError, check_error, code_for
from kith.fabric import CellKey


def cell_key_to_c(key: CellKey) -> gen_fabric.kith_fabric_cell_key_t:
    return gen_fabric.kith_fabric_cell_key_t(
        zone=ctypes.c_uint32(key.zone),
        cell_x=ctypes.c_int32(key.cell_x),
        cell_y=ctypes.c_int32(key.cell_y),
        cell_z=ctypes.c_int32(key.cell_z),
        lod=ctypes.c_uint8(key.lod),
        pad=(ctypes.c_uint8 * 3)(),
    )


def deliver_frame(
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


def broadcast_cell(
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
    c_key = cell_key_to_c(key)
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
