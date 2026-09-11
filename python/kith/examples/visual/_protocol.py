"""Wire catalog and payload codecs for the Open Range example.

One catalog per example is the house law: the type ids live at or above
the framework's user base (1000), both peers register the same name at
the same id so the gateway decoder and the client codec agree, and
payload (de)serialization is game-owned Python — the codec is the
example's concern, not the framework's.

The C2S surface: a login that binds a session to one actor per principal,
the hot-path movement input that maps onto ``kith_sim_input_t``, and a
chat submit. The S2C surface: a discrete ``login_reply`` carrying the
bound actor id, the replication stream (the per-subject ``actor_state``
record and its multi-subject batch form — the gateway's 68-byte delivery
format, defined in ``src/gateway/delivery/delivery.h``), the ``chat_event``
broadcast, and the ping/pong pair the C client engine times (a pong
echoes the ping's 8-byte big-endian send timestamp).
"""

from __future__ import annotations

import struct
from collections.abc import Callable
from dataclasses import dataclass

from kith import SimInput
from kith._agent.ahc import AgenticHeadlessClient, BootstrapStep, ClientEvent, DecodedFrame


_TYPE_BASE = 1200

LOGIN_TYPE: int = _TYPE_BASE + 0
LOGIN_REPLY_TYPE: int = _TYPE_BASE + 1
ACTOR_INPUT_TYPE: int = _TYPE_BASE + 2
CHAT_TYPE: int = _TYPE_BASE + 3
ACTOR_STATE_TYPE: int = _TYPE_BASE + 4
ACTOR_STATE_BATCH_TYPE: int = _TYPE_BASE + 5
CHAT_EVENT_TYPE: int = _TYPE_BASE + 6
PING_TYPE: int = _TYPE_BASE + 10
PONG_TYPE: int = _TYPE_BASE + 11

# Ordered (name, id) catalog: the single source of the example's wire
# surface. The registration loop and the client codec iterate this
# sequence so the named constants and the registry never drift apart.
TYPES: tuple[tuple[str, int], ...] = (
    ("login", LOGIN_TYPE),
    ("login_reply", LOGIN_REPLY_TYPE),
    ("actor_input", ACTOR_INPUT_TYPE),
    ("chat", CHAT_TYPE),
    ("actor_state", ACTOR_STATE_TYPE),
    ("actor_state_batch", ACTOR_STATE_BATCH_TYPE),
    ("chat_event", CHAT_EVENT_TYPE),
    ("ping", PING_TYPE),
    ("pong", PONG_TYPE),
)

# ---------------------------------------------------------------------------
# C2S payload codecs
# ---------------------------------------------------------------------------

# login: an 8-byte little-endian principal id. The principal is the
# account-level identity; one actor is bound per principal.
LOGIN_PAYLOAD_LEN: int = 8

# actor_input (hot path): 8-byte actor_id + 4-byte input_tick + three
# 2-byte signed move components + 1-byte flags = 19 bytes, a thin wrapper
# over kith_sim_input_t. No reply: the server's response is the
# replication stream.
ACTOR_INPUT_PAYLOAD_LEN: int = 19

# chat: 8-byte actor_id + 2-byte little-endian text length + the UTF-8
# text. The S2C chat_event carries the same bytes, so both peers decode
# the event with the same codec as the submit.
_CHAT_HEADER_LEN: int = 10
CHAT_MAX_TEXT: int = 256


def encode_login(principal_id: int) -> bytes:
    """Encode a ``login`` frame payload from a principal id."""
    return struct.pack("<Q", principal_id)


def decode_login(payload: bytes) -> int:
    """Decode a ``login`` frame payload into the principal id."""
    if len(payload) < LOGIN_PAYLOAD_LEN:
        raise ValueError(f"login payload too short: {len(payload)} < {LOGIN_PAYLOAD_LEN}")
    return int(struct.unpack("<Q", payload[:LOGIN_PAYLOAD_LEN])[0])


def encode_actor_input(actor_id: int, inp: SimInput) -> bytes:
    """Encode an ``actor_input`` frame payload from an actor id and input."""
    return struct.pack(
        "<QIhhhB",
        actor_id,
        inp.input_tick,
        inp.move_x,
        inp.move_y,
        inp.move_z,
        inp.flags,
    )


def decode_actor_input(payload: bytes) -> tuple[int, SimInput]:
    """Decode an ``actor_input`` frame payload into the actor id and input."""
    if len(payload) < ACTOR_INPUT_PAYLOAD_LEN:
        raise ValueError(
            f"actor_input payload too short: {len(payload)} < {ACTOR_INPUT_PAYLOAD_LEN}"
        )
    actor_id, input_tick, move_x, move_y, move_z, flags = (
        int(v) for v in struct.unpack("<QIhhhB", payload[:ACTOR_INPUT_PAYLOAD_LEN])
    )
    return actor_id, SimInput(
        input_tick=input_tick,
        move_x=move_x,
        move_y=move_y,
        move_z=move_z,
        flags=flags,
    )


def encode_chat(actor_id: int, text: str) -> bytes:
    """Encode a ``chat`` frame payload from an actor id and text."""
    text_bytes = text.encode("utf-8")
    if len(text_bytes) > CHAT_MAX_TEXT:
        raise ValueError(f"chat text too long: {len(text_bytes)} > {CHAT_MAX_TEXT}")
    return struct.pack("<QH", actor_id, len(text_bytes)) + text_bytes


def decode_chat(payload: bytes) -> tuple[int, str]:
    """Decode a ``chat``/``chat_event`` frame payload into actor id and text."""
    if len(payload) < _CHAT_HEADER_LEN:
        raise ValueError(f"chat payload too short: {len(payload)} < {_CHAT_HEADER_LEN}")
    actor_id, text_len = (int(v) for v in struct.unpack("<QH", payload[:_CHAT_HEADER_LEN]))
    if text_len > CHAT_MAX_TEXT:
        raise ValueError(f"chat text length out of range: {text_len} > {CHAT_MAX_TEXT}")
    end = _CHAT_HEADER_LEN + text_len
    if end > len(payload):
        raise ValueError(f"chat text truncated: need {end}, have {len(payload)}")
    return actor_id, payload[_CHAT_HEADER_LEN:end].decode("utf-8")


_LOGIN_REPLY_FMT = struct.Struct("<Q")


def encode_login_reply(actor_id: int) -> bytes:
    """Encode the ``login_reply`` payload: the actor id the principal bound to."""
    return _LOGIN_REPLY_FMT.pack(actor_id)


def decode_login_reply(payload: bytes) -> int:
    """Decode a ``login_reply`` payload into the bound actor id."""
    if len(payload) < _LOGIN_REPLY_FMT.size:
        raise ValueError(f"login_reply payload too short: {len(payload)}")
    return int(_LOGIN_REPLY_FMT.unpack(payload[: _LOGIN_REPLY_FMT.size])[0])


# ---------------------------------------------------------------------------
# replication record codec (the gateway's delivery format)
# ---------------------------------------------------------------------------

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

# actor_state_batch frame payload header: 4 bytes, big-endian, then
# subject_count 68-byte records laid out identically to a single payload,
# so one record parses the same way whether it arrived alone or in a
# batch.
ACTOR_STATE_BATCH_HEADER_SIZE: int = 4
_BATCH_HEADER_FMT: str = ">HH"
_BATCH_RECORD_SIZE: int = ACTOR_STATE_PAYLOAD_SIZE


@dataclass(frozen=True, slots=True)
class ActorStateView:
    """The fields of a decoded ``actor_state`` frame payload.

    Positions and velocities are Q16.16 fixed-point (the sim's native
    representation); a caller that wants world units divides by
    ``1 << 16``.
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


def decode_actor_state_batch(payload: bytes) -> list[ActorStateView]:
    """Decode a multi-subject ``actor_state_batch`` frame payload.

    Returns the decoded views; a truncated trailing record is ignored.
    Raises ``ValueError`` when the payload is shorter than the 4-byte
    header.
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


# Membership-event grammar: a record whose product_level word has this bit
# set is a membership event, not subject state; the kind occupies the low
# bits and the actor_id field names the subject (0 for the crowd
# aggregate). State records never set the marker bit.
MEMBERSHIP_MARKER: int = 0x80000000

MEMBERSHIP_EVENT_ENTER: int = 0
MEMBERSHIP_EVENT_EXIT: int = 1
MEMBERSHIP_EVENT_CROWD_ENTER: int = 2
MEMBERSHIP_EVENT_CROWD_EXIT: int = 3
MEMBERSHIP_EVENT_VANISH: int = 4


def is_membership_record(view: ActorStateView) -> bool:
    """Return whether a decoded record is a membership event, not state."""
    return bool(view.product_level & MEMBERSHIP_MARKER)


# ---------------------------------------------------------------------------
# client wiring
# ---------------------------------------------------------------------------


def login_bootstrap_step(principal_id: int, *, await_type_id: int | None = None) -> BootstrapStep:
    """Build the single bootstrap step: send ``login``, await replication.

    The ``on_enter`` callback encodes the principal id into a ``login``
    frame payload; the ``on_reply`` callback advances the FSM on any
    replication frame, proving the full replication path fired. The
    awaited type id defaults to the per-subject ``actor_state`` form; pass
    the batch form's id when the server is configured for batch delivery.
    """
    if await_type_id is None:
        await_type_id = ACTOR_STATE_TYPE

    def on_enter() -> tuple[int, bytes]:
        return LOGIN_TYPE, encode_login(principal_id)

    def on_reply(_frame: DecodedFrame) -> int:
        return 1

    return BootstrapStep(
        await_type_id=await_type_id,
        on_enter=on_enter,
        on_reply=on_reply,
    )


def make_client(
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
    ping_interval_ms: int = 500,
) -> AgenticHeadlessClient:
    """Build a headless client configured for the visual wire catalog.

    Registers the example's wire types on the client's proto codec and
    configures the login bootstrap step, so the client completes its
    handshake (send ``login``, await the first replication frame) before
    the caller submits interactive commands. ``replication_batch_type_id``
    selects the bootstrap await type: 0 awaits the per-subject form, a
    non-zero value awaits the batch form (the server's
    ``--replication-batch-type-id``). ``ping_interval_ms`` drives the
    engine's keepalive RTT probe; 0 disables it. The remaining keyword
    arguments forward to the engine: load-shaped callers turn off
    ``http_enabled``/``ipc_enabled`` to shed unused listener sockets and
    shrink ``event_history_size`` to the retention they consume.
    """
    await_type = ACTOR_STATE_BATCH_TYPE if replication_batch_type_id != 0 else ACTOR_STATE_TYPE
    return AgenticHeadlessClient(
        instance_id=instance_id,
        host=host,
        port=port,
        principal_id=principal_id,
        message_types=dict(TYPES),
        bootstrap_steps=[login_bootstrap_step(principal_id, await_type_id=await_type)],
        event_history_size=event_history_size,
        http_enabled=http_enabled,
        ipc_enabled=ipc_enabled,
        reconnect_enabled=reconnect_enabled,
        tick_interval_s=tick_interval_s,
        ping_type_id=PING_TYPE,
        pong_type_id=PONG_TYPE,
        ping_interval_ms=ping_interval_ms,
    )


def actor_state_for(actor_id: int) -> Callable[[ClientEvent], bool]:
    """Return a predicate matching replication events for ``actor_id``.

    For use with the harness assertion helpers: the predicate decodes the
    event payload and returns ``True`` when any carried record names
    ``actor_id`` as subject state. Events whose payload is too short to
    decode return ``False`` rather than raising, so a malformed frame
    does not abort the caller.
    """

    def predicate(event: ClientEvent) -> bool:
        try:
            if event.type_id == ACTOR_STATE_BATCH_TYPE:
                return any(
                    v.actor_id == actor_id and not is_membership_record(v)
                    for v in decode_actor_state_batch(event.payload)
                )
            if event.type_id != ACTOR_STATE_TYPE:
                return False
            view = decode_actor_state(event.payload)
            return view.actor_id == actor_id and not is_membership_record(view)
        except ValueError:
            return False

    return predicate
