"""Generic wire message types for the spatial game library.

The six types every spatial kith game exchanges on the wire, registered on
the server's borrowed proto handle before run. Both peers register the same
name at the same id so the gateway decoder and the client codec agree; ids
live at or above ``KITH_PROTO_TYPE_USER_BASE`` (1000), the boundary the
framework reserves for game-owned types.

The catalog is the irreducible surface a spatial game needs: a principal
exchange to bind a session to an actor, the hot-path movement input that maps
to ``kith_sim_input_t``, a generic chat message, and the single replication
type. Request/reply are separate types (a correlation ID ties them; no
direction flag dispatch). ``actor_input`` carries no reply — the server's
response IS the replication stream. ``actor_state`` is the single replication
type (the gateway's ``replication_type_id``); tier and spawn/despawn are in
the payload, not separate types, so the four server-to-client observation
types a per-connection push model needs collapse into one under the gateway's
bounded view set.

Catalog (id, name, direction, purpose):

    1000  login          C2S  principal exchange
    1001  login_reply    S2C  auth result + principal id
    1002  actor_input    C2S  movement input (hot path); no reply
    1003  chat           C2S  chat; no reply
    1004  actor_state    S2C  the single replication type
    1006  chat_event     S2C  the cell-scoped broadcast of a chat line

A game adds its own types on top of these (rosters, chat scope
variants, trade, etc.) by registering additional ids at or above the user
base; the catalog here is the shared starting point, not a closed set. The
client codec imports these same constants so both peers agree on the ids
through the proto registry, not a shared generated enum. ``chat_event`` is
the S2C twin of ``chat``: the handler submits the chat frame's own bytes on
the gateway's broadcast queue (the gateway's cell-scoped broadcast, see
:meth:`kith.Server.broadcast_cell`), so every session whose window covers
the sender's cell receives the line, and both peers decode the event with
the same codec as the C2S submit.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from kith._generated import proto as gen_proto


if TYPE_CHECKING:
    from kith import Server

# Framework-reserved boundary: game types live at or above this id.
_USER_BASE: int = int(gen_proto.kith_proto_format.KITH_PROTO_TYPE_USER_BASE)

# ---------------------------------------------------------------------------
# login
# ---------------------------------------------------------------------------
LOGIN_TYPE: int = _USER_BASE + 0
LOGIN_REPLY_TYPE: int = _USER_BASE + 1

# ---------------------------------------------------------------------------
# hot-path input and chat
# ---------------------------------------------------------------------------
ACTOR_INPUT_TYPE: int = _USER_BASE + 2
CHAT_TYPE: int = _USER_BASE + 3

# ---------------------------------------------------------------------------
# replication (the gateway's replication_type_id)
# ---------------------------------------------------------------------------
ACTOR_STATE_TYPE: int = _USER_BASE + 4
# ---------------------------------------------------------------------------
# replication batch (the gateway's replication_batch_type_id)
# ---------------------------------------------------------------------------
# A multi-subject frame packing N actor_state records into one delivery
# frame per refresh. When the gateway is configured with this type id, it
# emits only the batch form; the client decodes it via
# decode_actor_state_batch.
ACTOR_STATE_BATCH_TYPE: int = _USER_BASE + 5

# ---------------------------------------------------------------------------
# cell-scoped broadcast (the chat line's S2C ride)
# ---------------------------------------------------------------------------
CHAT_EVENT_TYPE: int = _USER_BASE + 6

# Ordered (name, id) catalog: the single source of the game's wire surface.
# A reader sees every type at a glance; the registration loop and the client
# codec iterate this sequence so the named constants and the registry never
# drift apart.
TYPES: tuple[tuple[str, int], ...] = (
    ("login", LOGIN_TYPE),
    ("login_reply", LOGIN_REPLY_TYPE),
    ("actor_input", ACTOR_INPUT_TYPE),
    ("chat", CHAT_TYPE),
    ("actor_state", ACTOR_STATE_TYPE),
    ("actor_state_batch", ACTOR_STATE_BATCH_TYPE),
    ("chat_event", CHAT_EVENT_TYPE),
)


def register(server: Server) -> None:
    """Register all game wire types on the server's borrowed proto handle.

    Both peers register the same name at the same id before exchanging frames
    of that type; call this before run so the gateway decoder
    accepts frames of every game type during the run loop. The
    ``replication_type_id`` passed to Server carries the value of
    ``ACTOR_STATE_TYPE`` so the gateway encodes the replication stream as
    that type.

    Raises:
        KithError: When a registration fails (a duplicate id bound to a
            different name raises ``KithStateError``).
    """
    for name, type_id in TYPES:
        server.register_proto_type(name, type_id)
