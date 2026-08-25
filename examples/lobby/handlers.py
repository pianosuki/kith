"""Inbound wire message handlers for the lobby example.

One handler per client-to-server type in messages,
registered through register_message_handler so the
gateway dispatches a decoded frame of each type to the matching bound method.
The single server-to-client type (``presence``) has no inbound handler: the
handlers compose ``presence`` payloads from room/presence/chat state and hand
them to a PresenceSink for delivery to the affected sessions.

The handlers share the per-server lobby state (the member table, the room
membership map, and the per-room chat log). A login binds the session to
one member per principal and assigns (or reuses) the display name; a
``room_join`` adds the member to a room and composes a ``presence``
notification for the room; a ``room_leave`` removes the member and composes
a notification; a ``chat_room`` records the line and composes a notification
carrying the text to every room member; a ``presence_update`` changes the
member's status flag and composes a notification. Payloads are thin
fixed-layout byte strings, (de)serialized here in Python: the codec is the
game's concern, not the framework's.

The handler dependencies are typed as Protocol interfaces so
the logic is testable with lightweight fakes and the real facade's
``Server`` / ``Session`` satisfy them structurally.
"""

from __future__ import annotations

import struct
import threading
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

from examples._common.store import GameStateStore, InMemoryStore
from examples.lobby import messages


if TYPE_CHECKING:
    from kith import Server


__all__ = [
    "CHAT_MAX_TEXT",
    "CHAT_ROOM_PAYLOAD_MIN_LEN",
    "KIND_CHAT",
    "KIND_JOIN",
    "KIND_LEAVE",
    "KIND_STATUS",
    "LOGIN_PAYLOAD_LEN",
    "PRESENCE_UPDATE_PAYLOAD_LEN",
    "ROOM_MAX_NAME",
    "LobbyHandlers",
    "LobbyState",
    "Member",
    "OutboxEntry",
    "OutboxSink",
    "PresenceEvent",
    "PresenceSink",
    "decode_chat_room",
    "decode_login",
    "decode_presence",
    "decode_presence_update",
    "decode_room_join",
    "decode_room_leave",
    "encode_chat_room",
    "encode_login",
    "encode_presence",
    "encode_presence_update",
    "encode_room_join",
    "encode_room_leave",
    "register",
]


# ---------------------------------------------------------------------------
# payload codecs
# ---------------------------------------------------------------------------

# login (C2S): an 8-byte little-endian principal id. The principal is the
# account-level identity; one member is bound per principal.
LOGIN_PAYLOAD_LEN: int = 8

# room_join / room_leave (C2S): a 1-byte name length + the UTF-8 room name.
# Room names are short identifiers, not free-form text; the cap keeps the
# codec bounded and the wire frame small.
_ROOM_HEADER_LEN: int = 1
ROOM_MAX_NAME: int = 64

# chat_room (C2S): a 1-byte room name length + the room name + a 2-byte
# little-endian text length + the UTF-8 text. No reply: the server's
# response is the ``presence`` notification fanned out to room members.
_CHAT_ROOM_ROOM_LEN: int = 1
_CHAT_ROOM_TEXT_LEN_SIZE: int = 2
CHAT_ROOM_PAYLOAD_MIN_LEN: int = 3  # room name length + text length (empty room, empty text)
CHAT_MAX_TEXT: int = 4096

# presence_update (C2S): an 8-byte member id + a 1-byte status flag. The
# status is a small enum (0 = online, 1 = away, 2 = do-not-disturb); the
# game owns the enum, not the framework.
PRESENCE_UPDATE_PAYLOAD_LEN: int = 9

# presence (S2C): the single notification type. A 1-byte event kind + an
# 8-byte member id + a 1-byte name length + the display name + a 1-byte
# status flag + a 2-byte text length + the text. The event kind selects
# join/leave/chat/status; the text is empty except for chat events.
_PRESENCE_HEADER_LEN: int = 13  # kind + member_id + name_len + status + text_len
PRESENCE_MAX_NAME: int = 64
PRESENCE_MAX_TEXT: int = 4096

# presence event kinds (carried in the payload, not as separate wire types).
KIND_JOIN: int = 0
KIND_LEAVE: int = 1
KIND_CHAT: int = 2
KIND_STATUS: int = 3


def encode_login(principal_id: int) -> bytes:
    """Encode a ``login`` frame payload from a principal id."""
    return struct.pack("<Q", principal_id)


def decode_login(payload: bytes) -> int:
    """Decode a ``login`` frame payload into the principal id."""
    if len(payload) < LOGIN_PAYLOAD_LEN:
        raise ValueError(f"login payload too short: {len(payload)} < {LOGIN_PAYLOAD_LEN}")
    return int(struct.unpack("<Q", payload[:LOGIN_PAYLOAD_LEN])[0])


def encode_room_join(room: str) -> bytes:
    """Encode a ``room_join`` frame payload from a room name."""
    return _encode_room_op(room)


def decode_room_join(payload: bytes) -> str:
    """Decode a ``room_join`` frame payload into the room name."""
    return _decode_room_op(payload, "room_join")


def encode_room_leave(room: str) -> bytes:
    """Encode a ``room_leave`` frame payload from a room name."""
    return _encode_room_op(room)


def decode_room_leave(payload: bytes) -> str:
    """Decode a ``room_leave`` frame payload into the room name."""
    return _decode_room_op(payload, "room_leave")


def _encode_room_op(room: str) -> bytes:
    name = room.encode("utf-8")
    if len(name) > ROOM_MAX_NAME:
        raise ValueError(f"room name too long: {len(name)} > {ROOM_MAX_NAME}")
    return struct.pack("<B", len(name)) + name


def _decode_room_op(payload: bytes, label: str) -> str:
    if len(payload) < _ROOM_HEADER_LEN:
        raise ValueError(f"{label} payload too short: {len(payload)} < {_ROOM_HEADER_LEN}")
    name_len = int(payload[0])
    if name_len > ROOM_MAX_NAME:
        raise ValueError(f"{label} room name length out of range: {name_len} > {ROOM_MAX_NAME}")
    end = _ROOM_HEADER_LEN + name_len
    if end > len(payload):
        raise ValueError(f"{label} room name truncated: need {end}, have {len(payload)}")
    return payload[_ROOM_HEADER_LEN:end].decode("utf-8")


def encode_chat_room(room: str, text: str) -> bytes:
    """Encode a ``chat_room`` frame payload from a room name and text."""
    name = room.encode("utf-8")
    if len(name) > ROOM_MAX_NAME:
        raise ValueError(f"room name too long: {len(name)} > {ROOM_MAX_NAME}")
    text_bytes = text.encode("utf-8")
    if len(text_bytes) > CHAT_MAX_TEXT:
        raise ValueError(f"chat text too long: {len(text_bytes)} > {CHAT_MAX_TEXT}")
    return struct.pack("<B", len(name)) + name + struct.pack("<H", len(text_bytes)) + text_bytes


def decode_chat_room(payload: bytes) -> tuple[str, str]:
    """Decode a ``chat_room`` frame payload into the room name and text."""
    if len(payload) < _CHAT_ROOM_ROOM_LEN:
        raise ValueError(f"chat_room payload too short: {len(payload)} < {_CHAT_ROOM_ROOM_LEN}")
    name_len = int(payload[0])
    if name_len > ROOM_MAX_NAME:
        raise ValueError(f"chat_room room name length out of range: {name_len} > {ROOM_MAX_NAME}")
    name_end = _CHAT_ROOM_ROOM_LEN + name_len
    if name_end + _CHAT_ROOM_TEXT_LEN_SIZE > len(payload):
        raise ValueError(f"chat_room payload truncated after room name: have {len(payload)}")
    room = payload[_CHAT_ROOM_ROOM_LEN:name_end].decode("utf-8")
    text_len = int(struct.unpack("<H", payload[name_end : name_end + _CHAT_ROOM_TEXT_LEN_SIZE])[0])
    if text_len > CHAT_MAX_TEXT:
        raise ValueError(f"chat_room text length out of range: {text_len} > {CHAT_MAX_TEXT}")
    text_start = name_end + _CHAT_ROOM_TEXT_LEN_SIZE
    text_end = text_start + text_len
    if text_end > len(payload):
        raise ValueError(f"chat_room text truncated: need {text_end}, have {len(payload)}")
    return room, payload[text_start:text_end].decode("utf-8")


def encode_presence_update(member_id: int, status: int) -> bytes:
    """Encode a ``presence_update`` frame payload from a member id and status."""
    return struct.pack("<QB", member_id, status)


def decode_presence_update(payload: bytes) -> tuple[int, int]:
    """Decode a ``presence_update`` frame payload into the member id and status."""
    if len(payload) < PRESENCE_UPDATE_PAYLOAD_LEN:
        raise ValueError(
            f"presence_update payload too short: {len(payload)} < {PRESENCE_UPDATE_PAYLOAD_LEN}"
        )
    member_id, status = (
        int(v) for v in struct.unpack("<QB", payload[:PRESENCE_UPDATE_PAYLOAD_LEN])
    )
    return member_id, status


def encode_presence(
    kind: int,
    member_id: int,
    display_name: str,
    status: int,
    text: str = "",
) -> bytes:
    """Encode a ``presence`` (S2C) frame payload from the event fields."""
    name = display_name.encode("utf-8")
    if len(name) > PRESENCE_MAX_NAME:
        raise ValueError(f"presence display name too long: {len(name)} > {PRESENCE_MAX_NAME}")
    text_bytes = text.encode("utf-8")
    if len(text_bytes) > PRESENCE_MAX_TEXT:
        raise ValueError(f"presence text too long: {len(text_bytes)} > {PRESENCE_MAX_TEXT}")
    return (
        struct.pack("<BQB", kind, member_id, len(name))
        + name
        + struct.pack("<BH", status, len(text_bytes))
        + text_bytes
    )


def decode_presence(payload: bytes) -> PresenceEvent:
    """Decode a ``presence`` (S2C) frame payload into the event fields."""
    if len(payload) < 1 + 8 + 1:
        raise ValueError(f"presence payload too short: {len(payload)}")
    kind = int(payload[0])
    member_id = int(struct.unpack("<Q", payload[1:9])[0])
    name_len = int(payload[9])
    if name_len > PRESENCE_MAX_NAME:
        raise ValueError(f"presence name length out of range: {name_len} > {PRESENCE_MAX_NAME}")
    name_end = 10 + name_len
    if name_end + 3 > len(payload):
        raise ValueError(f"presence payload truncated after name: have {len(payload)}")
    display_name = payload[10:name_end].decode("utf-8")
    status = int(payload[name_end])
    text_len = int(struct.unpack("<H", payload[name_end + 1 : name_end + 3])[0])
    if text_len > PRESENCE_MAX_TEXT:
        raise ValueError(f"presence text length out of range: {text_len} > {PRESENCE_MAX_TEXT}")
    text_start = name_end + 3
    text_end = text_start + text_len
    if text_end > len(payload):
        raise ValueError(f"presence text truncated: need {text_end}, have {len(payload)}")
    return PresenceEvent(
        kind=kind,
        member_id=member_id,
        display_name=display_name,
        status=status,
        text=payload[text_start:text_end].decode("utf-8"),
    )


# ---------------------------------------------------------------------------
# data types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PresenceEvent:
    """One decoded ``presence`` (S2C) notification.

    Attributes:
        kind: Event kind (KIND_JOIN, KIND_LEAVE, KIND_CHAT, KIND_STATUS).
        member_id: The member the event is about.
        display_name: The member's display name at the time of the event.
        status: The member's status flag at the time of the event.
        text: The chat text (empty except for KIND_CHAT events).
    """

    kind: int
    member_id: int
    display_name: str
    status: int
    text: str


@dataclass
class Member:
    """A lobby member (one per principal).

    Attributes:
        member_id: The server-assigned member id (distinct from the principal
            id; a returning principal rebinds to its existing member id).
        principal_id: The account-level identity from the login payload.
        display_name: The persisted display name (loaded from the store on
            login, assigned a default when the principal is new).
        status: The member's presence status flag (0 = online, 1 = away,
            2 = do-not-disturb).
    """

    member_id: int
    principal_id: int
    display_name: str
    status: int


# ---------------------------------------------------------------------------
# presence sink (the S2C delivery surface)
# ---------------------------------------------------------------------------


class PresenceSink(Protocol):
    """The surface a handler delivers composed ``presence`` payloads through.

    The handler composes a ``presence`` payload (the game codec) for each
    server-to-client notification and calls deliver with the target
    session ids and the encoded bytes. The sink is responsible for framing
    the payload as a wire ``presence`` frame and enqueuing it on each target
    session's connection. The default OutboxSink records the
    entries so the control plane can expose them; a real wire-delivery sink
    reaches the borrowed gateway and net plane through the session conn.
    """

    def deliver(self, session_ids: list[int], payload: bytes) -> None: ...


@dataclass(frozen=True)
class OutboxEntry:
    """One recorded ``presence`` delivery (the outbox sink's record).

    Attributes:
        session_ids: The target session ids the notification was addressed to.
        payload: The composed ``presence`` frame payload (the game codec).
    """

    session_ids: tuple[int, ...]
    payload: bytes


class OutboxSink:
    """Default PresenceSink that records deliveries in an in-memory list.

    The lobby has no sim/fabric view composer, so the gateway's per-tick
    delivery path is not used (``replication_type_id=0``). The handlers
    compose ``presence`` payloads and hand them to this sink; the control
    plane exposes the list so a harness or test can observe what the server
    would deliver to each session. A game that wants live wire delivery
    replaces this sink with one that reaches the session connection through
    the borrowed gateway and net plane.

    The list is guarded by a Lock because the gateway
    worker pool may dispatch concurrent handlers under free-threaded Python.
    """

    __slots__ = ("_entries", "_lock")

    def __init__(self) -> None:
        self._entries: list[OutboxEntry] = []
        self._lock = threading.Lock()

    def deliver(self, session_ids: list[int], payload: bytes) -> None:
        with self._lock:
            self._entries.append(OutboxEntry(tuple(session_ids), payload))

    def entries(self) -> list[OutboxEntry]:
        """Return a snapshot of the recorded deliveries."""
        with self._lock:
            return list(self._entries)

    def clear(self) -> None:
        with self._lock:
            self._entries.clear()


# ---------------------------------------------------------------------------
# lobby state (the shared mutable game state)
# ---------------------------------------------------------------------------


class LobbyState:
    """In-memory lobby state: members, rooms, and chat.

    Owns the member table (one member per principal), the room membership
    map (room name -> set of member ids), and the per-room chat log. A
    GameStateStore is injected for a game that wires its own
    persistence; this state reads and writes nothing through it — display
    names are in-memory only and no restart restoration runs.

    All mutations go through a Lock because the gateway
    worker pool may dispatch concurrent handlers under free-threaded Python.
    """

    __slots__ = (
        "_chat_log",
        "_lock",
        "_members",
        "_members_by_principal",
        "_next_member_id",
        "_rooms",
        "_store",
    )

    def __init__(self, store: GameStateStore | None = None) -> None:
        self._members: dict[int, Member] = {}
        self._members_by_principal: dict[int, int] = {}
        self._rooms: dict[str, set[int]] = {}
        self._chat_log: list[tuple[str, int, str]] = []  # (room, member_id, text)
        self._next_member_id = 1
        self._store = store if store is not None else InMemoryStore()
        self._lock = threading.Lock()

    @property
    def store(self) -> GameStateStore:
        return self._store

    def get_or_create_member(self, principal_id: int) -> Member:
        """Return the member for ``principal_id``, creating one if new.

        A returning principal rebinds to its existing member id. A new
        principal is assigned a monotonic member id and a default display
        name held in memory only.
        """
        with self._lock:
            member_id = self._members_by_principal.get(principal_id)
            if member_id is not None:
                return self._members[member_id]
            member_id = self._next_member_id
            self._next_member_id += 1
            display_name = f"member-{member_id}"
            member = Member(
                member_id=member_id,
                principal_id=principal_id,
                display_name=display_name,
                status=0,
            )
            self._members[member_id] = member
            self._members_by_principal[principal_id] = member_id
            return member

    def set_display_name(self, member_id: int, display_name: str) -> None:
        """Set a member's display name in the in-memory table."""
        with self._lock:
            member = self._members.get(member_id)
            if member is None:
                return
            self._members[member_id] = Member(
                member_id=member.member_id,
                principal_id=member.principal_id,
                display_name=display_name,
                status=member.status,
            )

    def join_room(self, room: str, member_id: int) -> bool:
        """Add a member to a room; return True if the membership changed."""
        with self._lock:
            members = self._rooms.setdefault(room, set())
            if member_id in members:
                return False
            members.add(member_id)
            return True

    def leave_room(self, room: str, member_id: int) -> bool:
        """Remove a member from a room; return True if the membership changed."""
        with self._lock:
            members = self._rooms.get(room)
            if members is None or member_id not in members:
                return False
            members.discard(member_id)
            if not members:
                del self._rooms[room]
            return True

    def room_members(self, room: str) -> list[int]:
        with self._lock:
            members = self._rooms.get(room)
            return sorted(members) if members else []

    def room_names(self) -> list[str]:
        with self._lock:
            return sorted(self._rooms)

    def record_chat(self, room: str, member_id: int, text: str) -> None:
        with self._lock:
            self._chat_log.append((room, member_id, text))

    def chat_log(self, room: str | None = None) -> list[tuple[str, int, str]]:
        with self._lock:
            if room is None:
                return list(self._chat_log)
            return [e for e in self._chat_log if e[0] == room]

    def set_status(self, member_id: int, status: int) -> bool:
        """Set a member's status flag; return True if it changed."""
        with self._lock:
            member = self._members.get(member_id)
            if member is None or member.status == status:
                return False
            self._members[member_id] = Member(
                member_id=member.member_id,
                principal_id=member.principal_id,
                display_name=member.display_name,
                status=status,
            )
            return True

    def member(self, member_id: int) -> Member | None:
        with self._lock:
            return self._members.get(member_id)

    def members(self) -> list[Member]:
        with self._lock:
            return sorted(self._members.values(), key=lambda m: m.member_id)


# ---------------------------------------------------------------------------
# dependency protocols (structurally satisfied by the facade types)
# ---------------------------------------------------------------------------


class _BindableSession(Protocol):
    """The session surface a handler binds a member to."""

    def bind_actor(self, actor_id: int) -> None: ...


# ---------------------------------------------------------------------------
# handler set
# ---------------------------------------------------------------------------


class LobbyHandlers:
    """The five inbound (C2S) wire handlers plus their shared lobby state.

    One instance per server. The bound methods are registered through
    register via register_message_handler; the
    gateway's trampoline invokes ``handler(msg_type, payload_bytes, session)``
    on a worker, so every handler takes those three positional
    arguments even when it ignores ``msg_type``.

    Args:
        state: The shared LobbyState (members, rooms, chat, store).
        sink: The PresenceSink the handlers deliver composed
            ``presence`` payloads to. The default OutboxSink records
            deliveries for control-plane introspection.
    """

    __slots__ = ("_sink", "_state")

    def __init__(self, *, state: LobbyState, sink: PresenceSink) -> None:
        self._state = state
        self._sink = sink

    # -----------------------------------------------------------------------
    # wire handlers
    # -----------------------------------------------------------------------

    def on_login(self, msg_type: int, payload: bytes, session: _BindableSession) -> None:
        """Bind the session to one member per principal.

        Decodes the principal id from the payload, loads (or creates) the
        member, and binds the session to the member id so the gateway's view
        composer resolves the subscriber. A returning principal rebinds to
        its existing member without allocating.
        """
        del msg_type
        if len(payload) < LOGIN_PAYLOAD_LEN:
            return
        principal_id = decode_login(payload)
        member = self._state.get_or_create_member(principal_id)
        session.bind_actor(member.member_id)

    def on_room_join(self, msg_type: int, payload: bytes, session: _BindableSession) -> None:
        """Add the bound member to a room and notify the room.

        Decodes the room name, reads the bound member id from the session,
        adds it to the room, and composes a ``presence`` (KIND_JOIN)
        notification for every member currently in the room. Frames from a
        session that has not logged in (actor id 0) are dropped.
        """
        del msg_type
        try:
            room = decode_room_join(payload)
        except ValueError:
            return
        member_id = self._session_member_id(session)
        if member_id == 0:
            return
        if not self._state.join_room(room, member_id):
            return
        self._notify_room(room, KIND_JOIN, member_id)

    def on_room_leave(self, msg_type: int, payload: bytes, session: _BindableSession) -> None:
        """Remove the bound member from a room and notify the room."""
        del msg_type
        try:
            room = decode_room_leave(payload)
        except ValueError:
            return
        member_id = self._session_member_id(session)
        if member_id == 0:
            return
        if not self._state.leave_room(room, member_id):
            return
        self._notify_room(room, KIND_LEAVE, member_id)

    def on_chat_room(self, msg_type: int, payload: bytes, session: _BindableSession) -> None:
        """Record a chat line and broadcast a ``presence`` (KIND_CHAT) to the room."""
        del msg_type
        try:
            room, text = decode_chat_room(payload)
        except ValueError:
            return
        member_id = self._session_member_id(session)
        if member_id == 0:
            return
        if member_id not in self._state.room_members(room):
            return
        self._state.record_chat(room, member_id, text)
        self._notify_room(room, KIND_CHAT, member_id, text=text)

    def on_presence_update(self, msg_type: int, payload: bytes, session: _BindableSession) -> None:
        """Change the bound member's status and notify every room the member is in."""
        del msg_type, session
        if len(payload) < PRESENCE_UPDATE_PAYLOAD_LEN:
            return
        member_id, status = decode_presence_update(payload)
        if not self._state.set_status(member_id, status):
            return
        for room in self._state.room_names():
            if member_id in self._state.room_members(room):
                self._notify_room(room, KIND_STATUS, member_id)

    # -----------------------------------------------------------------------
    # helpers
    # -----------------------------------------------------------------------

    @staticmethod
    def _session_member_id(session: _BindableSession) -> int:
        """Read the member id the gateway bound on the session.

        The session view carries the bound actor id (set by
        on_login via ``session.bind_actor``). A session that has not
        logged in has actor id 0; the handlers drop frames from such
        sessions so a pre-login frame does not mutate lobby state.
        """
        info = getattr(session, "info", None)
        if info is None:
            return 0
        return int(getattr(info, "actor_id", 0))

    def _notify_room(self, room: str, kind: int, member_id: int, *, text: str = "") -> None:
        member = self._state.member(member_id)
        if member is None:
            return
        payload = encode_presence(
            kind=kind,
            member_id=member_id,
            display_name=member.display_name,
            status=member.status,
            text=text,
        )
        self._sink.deliver(self._state.room_members(room), payload)


def register(
    server: Server,
    *,
    state: LobbyState,
    sink: PresenceSink,
) -> LobbyHandlers:
    """Build the handler set and register its five C2S handlers on ``server``.

    Wire types must already be registered on the server's borrowed proto (via
    register) so the gateway decoder accepts
    frames of each type during the run loop. Returns the handler set so the
    caller holds the live lobby state and sink for the server's lifetime.

    Args:
        server: The facade the handlers register on.
        state: The shared LobbyState.
        sink: The PresenceSink for composed ``presence`` deliveries.

    Returns:
        The live LobbyHandlers instance.
    """
    handlers = LobbyHandlers(state=state, sink=sink)
    server.register_message_handler(messages.LOGIN_TYPE, handlers.on_login)
    server.register_message_handler(messages.ROOM_JOIN_TYPE, handlers.on_room_join)
    server.register_message_handler(messages.ROOM_LEAVE_TYPE, handlers.on_room_leave)
    server.register_message_handler(messages.CHAT_ROOM_TYPE, handlers.on_chat_room)
    server.register_message_handler(messages.PRESENCE_UPDATE_TYPE, handlers.on_presence_update)
    return handlers
