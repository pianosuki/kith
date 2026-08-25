"""Wire message types for the lobby example.

The lobby is a non-spatial slice: presence, rooms, and chat on Gateway +
Control + DB only, with no Sim, Fabric, or AOI. The catalog is six game-
owned types at ids at or above ``KITH_PROTO_TYPE_USER_BASE`` (1000),
registered on the server's borrowed proto handle before run. Both peers
register the same name at the same id so the gateway decoder and the client
codec agree.

This catalog is deliberately separate from the spatial library's
messages: a spatial game exchanges
``actor_input`` and a single ``actor_state`` replication type over a sim
+ fabric + AOI wiring, while a non-spatial lobby exchanges room joins and
a single ``presence`` notification type over a gateway + control + DB
wiring. The two catalogs share no names and no ids; each game owns its
wire surface. The framework defines no wire types and ships no central
enum, so every game's catalog is its own contract.

Catalog (id, name, direction, purpose):

    1100  login             C2S  principal exchange; binds the session
    1101  presence          S2C  the single notification type (roster
                                 changes, chat broadcasts, status updates)
    1102  room_join         C2S  join a named room
    1103  room_leave        C2S  leave a named room
    1104  chat_room         C2S  send chat to a named room; no reply
    1105  presence_update   C2S  update the member's own status flag

Request/reply are separate types (a correlation ID ties them; no direction
flag dispatch). ``chat_room`` carries no reply: the server's response IS
the ``presence`` notification fanned out to room members. ``presence`` is
the lobby's analog of the spatial game's single replication type: tier and
room membership are in the payload, not separate types, so a
per-connection push model's worth of join/leave/status/chat notifications
collapse into one.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from kith._generated import proto as gen_proto


if TYPE_CHECKING:
    from kith import Server

# Framework-reserved boundary: game types live at or above this id.
_USER_BASE: int = int(gen_proto.kith_proto_format.KITH_PROTO_TYPE_USER_BASE)

# The lobby's id range starts 100 above the user base. Each game picks its
# own id range within the user space; the spatial library occupies
# ``_USER_BASE + 0..5`` and the lobby occupies ``_USER_BASE + 100..105`` so
# the two catalogs are disjoint and can coexist on one server. The
# framework defines no central enum and assigns no ids; the offset is a
# game choice that keeps two game-owned catalogs from colliding.
_LOBBY_BASE: int = _USER_BASE + 100

# ---------------------------------------------------------------------------
# login
# ---------------------------------------------------------------------------
LOGIN_TYPE: int = _LOBBY_BASE + 0

# ---------------------------------------------------------------------------
# the single S2C notification type
# ---------------------------------------------------------------------------
PRESENCE_TYPE: int = _LOBBY_BASE + 1

# ---------------------------------------------------------------------------
# room membership
# ---------------------------------------------------------------------------
ROOM_JOIN_TYPE: int = _LOBBY_BASE + 2
ROOM_LEAVE_TYPE: int = _LOBBY_BASE + 3

# ---------------------------------------------------------------------------
# chat
# ---------------------------------------------------------------------------
CHAT_ROOM_TYPE: int = _LOBBY_BASE + 4

# ---------------------------------------------------------------------------
# presence status
# ---------------------------------------------------------------------------
PRESENCE_UPDATE_TYPE: int = _LOBBY_BASE + 5

# Ordered (name, id) catalog: the single source of the lobby's wire surface.
# A reader sees every type at a glance; the registration loop and the client
# codec iterate this sequence so the named constants and the registry never
# drift apart.
TYPES: tuple[tuple[str, int], ...] = (
    ("login", LOGIN_TYPE),
    ("presence", PRESENCE_TYPE),
    ("room_join", ROOM_JOIN_TYPE),
    ("room_leave", ROOM_LEAVE_TYPE),
    ("chat_room", CHAT_ROOM_TYPE),
    ("presence_update", PRESENCE_UPDATE_TYPE),
)


def register(server: Server) -> None:
    """Register all lobby wire types on the server's borrowed proto handle.

    Both peers register the same name at the same id before exchanging frames
    of that type; call this before run so the gateway decoder
    accepts frames of every lobby type during the run loop. The lobby passes
    ``replication_type_id=0`` to Server because it has no sim/fabric
    view composer; the ``presence`` S2C type is composed by the handlers, not
    by the gateway's per-tick delivery path.

    Raises:
        KithError: When a registration fails (a duplicate id bound to a
            different name raises ``KithStateError``).
    """
    for name, type_id in TYPES:
        server.register_proto_type(name, type_id)
