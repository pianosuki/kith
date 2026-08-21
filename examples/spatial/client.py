"""Game-aware headless client for the spatial game library.

A factory that builds an ~tools.agent.ahc.AgenticHeadlessClient
pre-configured with the spatial wire catalog (the six types at ids
1000-1005 registered on the client's proto codec) and the login bootstrap
step (send ``login`` with a principal id, await the first ``actor_state``
replication frame). The client is the wire-side half of the agentic
harness: scenarios drive the server through this client (login,
actor_input, chat) and observe the replication stream (``actor_state``
events) it records.

The ``actor_state`` payload codec (the gateway's 68-byte replication
format, defined in ``src/gateway/delivery/delivery.h``) is decoded here
so both the scenarios and the unit-test fakes read it one way. The codec
is the client's concern, not the framework's, matching the catalog
principle that payload (de)serialization is game-owned Python.

The bootstrap awaits ``actor_state`` (not ``login_reply``) because the
spatial ``on_login`` handler does not enqueue a discrete reply frame:
the server's response IS the replication stream, so the first
``actor_state`` frame proves the full publish -> cache -> compose ->
deliver path fired end-to-end.
"""

from __future__ import annotations

import struct
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from typing import TYPE_CHECKING

from examples.spatial import handlers, messages
from tools.agent.ahc import AgenticHeadlessClient, BootstrapStep, ClientEvent, DecodedFrame


if TYPE_CHECKING:
    pass


__all__ = [
    "ACTOR_STATE_BATCH_HEADER_SIZE",
    "ACTOR_STATE_PAYLOAD_SIZE",
    "ActorStateView",
    "actor_state_for",
    "decode_actor_state",
    "decode_actor_state_batch",
    "encode_actor_state",
    "login_bootstrap_step",
    "make_ahc",
]


# actor_state frame payload: 68 bytes, big-endian (network byte order),
# matching the proto frame header.
#   0-7   actor_id      (uint64)
#   8-15  pos_x         (int64, Q16.16)
#   16-23 pos_y         (int64, Q16.16)
#   24-31 pos_z         (int64, Q16.16)
#   32-39 vel_x         (int64, Q16.16)
#   40-47 vel_y         (int64, Q16.16)
#   48-55 vel_z         (int64, Q16.16)
#   56-59 input_tick    (uint32)
#   60-63 update_seq    (uint32)
#   64-67 product_level (uint32; membership-event marker word)
ACTOR_STATE_PAYLOAD_SIZE: int = 68
_ACTOR_STATE_FMT: str = ">QqqqqqqIII"

# actor_state_batch frame payload header: 4 bytes, big-endian (network byte
# order), matching the proto frame header.
#   0-1 subject_count (uint16)
#   2-3 reserved      (uint16)
# Followed by subject_count * 68-byte records (the same layout as a single
# actor_state payload), so one record parses identically whether it arrived
# alone or inside a batch.
ACTOR_STATE_BATCH_HEADER_SIZE: int = 4
_BATCH_HEADER_FMT: str = ">HH"
_BATCH_RECORD_SIZE: int = ACTOR_STATE_PAYLOAD_SIZE


@dataclass(frozen=True, slots=True)
class ActorStateView:
    """The fields of a decoded ``actor_state`` frame payload.

    Positions and velocities are Q16.16 fixed-point (the sim's native
    representation); a caller that wants world units divides by ``1 << 16``.
    """

    actor_id: int
    pos_x: int
    pos_y: int
    pos_z: int
    vel_x: int
    vel_y: int
    vel_z: int
    input_tick: int
    update_seq: int = 0
    product_level: int = 0


def decode_actor_state(payload: bytes) -> ActorStateView:
    """Decode a 68-byte ``actor_state`` frame payload into a view.

    Raises ``ValueError`` when the payload is shorter than the 68-byte
    layout; trailing bytes beyond 68 are ignored.
    """
    if len(payload) < ACTOR_STATE_PAYLOAD_SIZE:
        raise ValueError(
            f"actor_state payload too short: {len(payload)} < {ACTOR_STATE_PAYLOAD_SIZE}"
        )
    actor_id, pos_x, pos_y, pos_z, vel_x, vel_y, vel_z, input_tick, update_seq, product_level = (
        struct.unpack(_ACTOR_STATE_FMT, payload[:ACTOR_STATE_PAYLOAD_SIZE])
    )
    return ActorStateView(
        actor_id=int(actor_id),
        pos_x=int(pos_x),
        pos_y=int(pos_y),
        pos_z=int(pos_z),
        vel_x=int(vel_x),
        vel_y=int(vel_y),
        vel_z=int(vel_z),
        input_tick=int(input_tick),
        update_seq=int(update_seq),
        product_level=int(product_level),
    )


def encode_actor_state(view: ActorStateView) -> bytes:
    """Encode an ActorStateView into a 68-byte payload.

    Used by unit tests to construct fake ``actor_state`` events the
    reactive host emits; the real payload is produced by the gateway's
    delivery serializer, not by this encoder.
    """
    return struct.pack(
        _ACTOR_STATE_FMT,
        view.actor_id,
        view.pos_x,
        view.pos_y,
        view.pos_z,
        view.vel_x,
        view.vel_y,
        view.vel_z,
        view.input_tick,
        view.update_seq,
        view.product_level,
    )


# Membership-event grammar: a record whose product_level
# word has this bit set is a membership event, not subject state; the kind
# occupies the low bits and the actor_id field names the subject (0 for the
# crowd aggregate). State records never set the marker bit.
MEMBERSHIP_EVENT_MARKER = 0x80000000

MEMBERSHIP_EVENT_ENTER = 0
MEMBERSHIP_EVENT_EXIT = 1
MEMBERSHIP_EVENT_CROWD_ENTER = 2
MEMBERSHIP_EVENT_CROWD_EXIT = 3
MEMBERSHIP_EVENT_VANISH = 4


def is_membership_record(view: ActorStateView) -> bool:
    """Return whether a decoded record is a membership event, not state."""
    return bool(view.product_level & MEMBERSHIP_EVENT_MARKER)


def decode_actor_state_batch(payload: bytes) -> list[ActorStateView]:
    """Decode a multi-subject ``actor_state_batch`` frame payload.

    The batch payload is a 4-byte header (uint16 subject_count + uint16
    reserved) followed by ``subject_count`` 68-byte records, each laid out
    identically to a single decode_actor_state payload. Returns the
    list of decoded views; trailing records shorter than 68 bytes are
    ignored (defensive against a truncated frame).

    Raises ``ValueError`` when the payload is shorter than the 4-byte header.
    """
    if len(payload) < ACTOR_STATE_BATCH_HEADER_SIZE:
        raise ValueError(
            f"actor_state_batch payload too short: {len(payload)} < {ACTOR_STATE_BATCH_HEADER_SIZE}"
        )
    count, _reserved = struct.unpack(_BATCH_HEADER_FMT, payload[:ACTOR_STATE_BATCH_HEADER_SIZE])
    views: list[ActorStateView] = []
    base = ACTOR_STATE_BATCH_HEADER_SIZE
    for _i in range(count):
        rec = payload[base : base + _BATCH_RECORD_SIZE]
        if len(rec) < ACTOR_STATE_PAYLOAD_SIZE:
            break
        views.append(decode_actor_state(rec))
        base += _BATCH_RECORD_SIZE
    return views


_STATE_ID_FIELD = struct.Struct(">Q")  # actor_id at record offset 0
_STATE_MARKER_FIELD = struct.Struct(">I")  # membership marker word at offset 64


def iter_actor_state_ids(type_id: int, payload: bytes) -> Iterator[int]:
    """Yield the actor ids of state records in one replication payload.

    Accepts both replication forms (``ACTOR_STATE_TYPE`` and
    ``ACTOR_STATE_BATCH_TYPE``) and yields nothing for any other type.
    Membership records are skipped: their ``actor_id`` names a
    subscription subject or the crowd aggregate, not a state record.
    Reads only the two fields the id needs, so a full-view wave frame
    costs two unpacks per record instead of a view construction — the
    load driver's ingest path calls this per frame while its breadth
    window is open. Truncated trailing records are ignored, matching
    decode_actor_state_batch.
    """
    if type_id == messages.ACTOR_STATE_TYPE:
        if len(payload) >= ACTOR_STATE_PAYLOAD_SIZE:
            (marker,) = _STATE_MARKER_FIELD.unpack_from(payload, 64)
            if not marker & MEMBERSHIP_EVENT_MARKER:
                yield _STATE_ID_FIELD.unpack_from(payload)[0]
        return
    if type_id != messages.ACTOR_STATE_BATCH_TYPE:
        return
    if len(payload) < ACTOR_STATE_BATCH_HEADER_SIZE:
        return
    count, _reserved = struct.unpack_from(_BATCH_HEADER_FMT, payload)
    base = ACTOR_STATE_BATCH_HEADER_SIZE
    for _i in range(count):
        if base + ACTOR_STATE_PAYLOAD_SIZE > len(payload):
            return
        (marker,) = _STATE_MARKER_FIELD.unpack_from(payload, base + 64)
        if not marker & MEMBERSHIP_EVENT_MARKER:
            yield _STATE_ID_FIELD.unpack_from(payload, base)[0]
        base += _BATCH_RECORD_SIZE


def login_bootstrap_step(principal_id: int, *, await_type_id: int | None = None) -> BootstrapStep:
    """Build the single bootstrap step: send ``login``, await replication.

    The ``on_enter`` callback encodes the principal id into a ``login``
    frame payload; the ``on_reply`` callback advances the FSM to READY on
    any replication frame, proving the full replication path fired. The
    awaited type id defaults to ``ACTOR_STATE_TYPE`` (the per-subject
    form); pass ``ACTOR_STATE_BATCH_TYPE`` when the server is configured
    for multi-subject batch delivery.
    """
    if await_type_id is None:
        await_type_id = messages.ACTOR_STATE_TYPE

    def on_enter() -> tuple[int, bytes]:
        return messages.LOGIN_TYPE, handlers.encode_login(principal_id)

    def on_reply(_frame: DecodedFrame) -> int:
        return 1

    return BootstrapStep(
        await_type_id=await_type_id,
        on_enter=on_enter,
        on_reply=on_reply,
    )


def replication_direct_types() -> frozenset[int]:
    """Return the replication type ids eligible for direct client dispatch.

    The two actor-state forms dominate inbound frame volume under load;
    handing them to ``AgenticHeadlessClient`` as ``direct_type_ids`` lets
    the driver build their events without the per-frame engine callback.
    """
    return frozenset({messages.ACTOR_STATE_TYPE, messages.ACTOR_STATE_BATCH_TYPE})


def make_ahc(
    instance_id: str,
    host: str,
    port: int,
    *,
    principal_id: int,
    event_history_size: int = 512,
    http_enabled: bool = True,
    ipc_enabled: bool = True,
    reconnect_enabled: bool = True,
    tick_interval_s: float = 0.02,
    replication_batch_type_id: int = 0,
    direct_type_ids: frozenset[int] = frozenset(),
    summary_mode: bool = False,
    evidence_budget_bytes: int = 0,
) -> AgenticHeadlessClient:
    """Build a headless client configured for the spatial wire catalog.

    Registers the spatial wire types on the client's proto codec and
    configures the login bootstrap step so the client completes its
    handshake (send ``login``, await the first replication frame) before
    the scenario submits interactive commands. ``event_history_size`` is
    forwarded to the AHC; the default suits the closed-loop scenarios (2-3
    clients), and a load harness driving a dense actor profile passes a
    larger value so the per-client event history holds enough of the
    replication stream to sample movement-continuity across the run.
    ``http_enabled`` and ``ipc_enabled`` are forwarded to the AHC; the
    closed-loop scenarios keep the defaults (both on) for introspection,
    and a load harness turns both off to shed the per-client listener
    sockets that are unused under load. ``reconnect_enabled`` and
    ``tick_interval_s`` are likewise forwarded; the closed-loop scenarios
    keep reconnect on and the 50 Hz tick cadence, while a load harness
    disables reconnect (one attempt per client) and drops the tick to
    10 Hz so the C engine's keepalive timer is not the rate limiter.
    ``replication_batch_type_id`` selects the bootstrap await type: 0
    awaits the per-subject ``actor_state`` form; a non-zero value awaits
    the multi-subject ``actor_state_batch`` form the gateway emits when
    configured for batch delivery. ``summary_mode`` drops per-frame event
    retention: replication frames advance a counter and refresh one
    periodic payload slot instead of building history entries, for load
    drivers whose bulk clients feed no fidelity metric.
    ``evidence_budget_bytes`` enables the breadth-evidence window: every
    replication frame's payload is retained (raw, no decode) in a recency
    deque bounded by that byte budget, so the breadth probe can union the
    distinct actor ids across many delivery passes under
    change-suppression delivery; 0 disables the window. The client also
    carries iter_actor_state_ids as its view-window id extractor:
    while the load driver's breadth window is open, each replication
    frame's state-record ids accumulate into the client's movement-window
    evidence set, which the probe reads instead of walking the deque.
    """
    await_type = (
        messages.ACTOR_STATE_BATCH_TYPE
        if replication_batch_type_id != 0
        else messages.ACTOR_STATE_TYPE
    )
    return AgenticHeadlessClient(
        instance_id=instance_id,
        host=host,
        port=port,
        principal_id=principal_id,
        message_types=dict(messages.TYPES),
        bootstrap_steps=[login_bootstrap_step(principal_id, await_type_id=await_type)],
        event_history_size=event_history_size,
        http_enabled=http_enabled,
        ipc_enabled=ipc_enabled,
        reconnect_enabled=reconnect_enabled,
        tick_interval_s=tick_interval_s,
        direct_type_ids=direct_type_ids,
        summary_mode=summary_mode,
        evidence_budget_bytes=evidence_budget_bytes,
        window_id_extractor=iter_actor_state_ids,
    )


def actor_state_for(actor_id: int) -> Callable[[ClientEvent], bool]:
    """Return a predicate matching ``actor_state`` events for ``actor_id``.

    For use with ~tools.agent.assertions.assert_event: the predicate
    decodes the event payload and returns ``True`` when the carried actor id
    matches. Events whose payload is too short to decode return ``False``
    rather than raising, so a malformed frame does not abort the step.

    Handles both the per-subject ``actor_state`` form (a single 68-byte
    record) and the multi-subject ``actor_state_batch`` form (a 4-byte
    header + N records): for a batch frame, the predicate returns ``True``
    when any record in the batch carries ``actor_id``.
    """

    def predicate(event: ClientEvent) -> bool:
        try:
            if event.type_id == messages.ACTOR_STATE_BATCH_TYPE:
                return any(
                    v.actor_id == actor_id and not is_membership_record(v)
                    for v in decode_actor_state_batch(event.payload)
                )
            return decode_actor_state(event.payload).actor_id == actor_id
        except ValueError:
            return False

    return predicate
