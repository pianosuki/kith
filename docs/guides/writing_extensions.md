# Writing Extensions

This guide adds a new wire message type and a Python handler for it, with no
C code and no rebuild. The gateway's handler table is populated at runtime
through the `Server` facade, so registering a new game-owned type is a
pure-Python change to the game's catalog and handler modules.

The walkthrough mirrors the spatial library's catalog
(`examples/spatial/messages.py` and `examples/spatial/handlers.py`), which
is the canonical shape a game-owned extension copies.

## What "no C" means

The framework reserves wire type ids below `KITH_PROTO_TYPE_USER_BASE` (1000)
for its own types and exposes the rest to the game. Adding a game-owned type
is three Python-side steps:

1. Pick a type id at or above 1000 and register the name on the server's
   borrowed proto before `run()`.
2. Write a fixed-layout byte codec for the payload (the codec is the game's
   concern, not the framework's).
3. Register a Python handler for the id through `register_message_handler`.

No header is edited, no `.map` symbol script changes, no ABI snapshot moves,
and no shared library is rebuilt. The gateway dispatches by direct index into
a table the facade fills at registration time, so a type the framework did not
know about at compile time dispatches at runtime.

## The example: an `emote` type

Add a C2S `emote` type. An actor emits a short text emote into its cell; the
handler decodes the frame, looks the actor up, and re-broadcasts the sender's
cell product through the fabric so the subscription fan-out carries the
update to nearby subscribers. This is the same shape as the spatial library's
`chat` handler: decode, look up, re-publish.

### 1. Pick a type id

Game types live at or above `KITH_PROTO_TYPE_USER_BASE` (1000). Both peers
register the same name at the same id before exchanging frames; there is no
central enum. The spatial library's catalog occupies 1000–1004, so the next
free id is 1005.

```python
from kith._generated import proto as gen_proto

_USER_BASE: int = int(gen_proto.kith_proto_format.KITH_PROTO_TYPE_USER_BASE)

EMOTE_TYPE: int = _USER_BASE + 5  # 1005
```

The `handler_table_size` passed to `Server` must exceed the largest user
type id (the gateway dispatches by direct index). The spatial library passes
`handler_table_size=4096`, which covers 1005 with room to spare; a game whose
largest id approaches the table size raises it at construction.

### 2. Register the type on the borrowed proto

Register the name on the server's borrowed proto before `run()` so the gateway
 decoder accepts frames of this type during the run loop. The spatial library
 keeps an ordered `(name, id)` tuple and registers in a loop so the named
constants and the registry never drift apart.

```python
def register(server: Server) -> None:
    server.register_proto_type("emote", EMOTE_TYPE)
```

### 3. Write the payload codec

The framework's frame header carries a `version` byte and a `payload_len`;
payload (de)serialization is the caller's concern. The spatial library keeps
one codec per type in Python, with small fixed-layout payloads handled through
`struct`. An `emote` payload is an 8-byte actor id and a length-prefixed UTF-8
text:

```python
import struct

_EMOTE_HEADER_LEN: int = 10
EMOTE_MAX_TEXT: int = 256


def encode_emote(actor_id: int, text: str) -> bytes:
    text_bytes = text.encode("utf-8")
    if len(text_bytes) > EMOTE_MAX_TEXT:
        raise ValueError(f"emote text too long: {len(text_bytes)} > {EMOTE_MAX_TEXT}")
    return struct.pack("<QH", actor_id, len(text_bytes)) + text_bytes


def decode_emote(payload: bytes) -> tuple[int, str]:
    if len(payload) < _EMOTE_HEADER_LEN:
        raise ValueError(f"emote payload too short: {len(payload)} < {_EMOTE_HEADER_LEN}")
    actor_id, text_len = (int(v) for v in struct.unpack("<QH", payload[:_EMOTE_HEADER_LEN]))
    if text_len > EMOTE_MAX_TEXT:
        raise ValueError(f"emote text length out of range: {text_len} > {EMOTE_MAX_TEXT}")
    end = _EMOTE_HEADER_LEN + text_len
    if end > len(payload):
        raise ValueError(f"emote text truncated: need {end}, have {len(payload)}")
    return actor_id, payload[_EMOTE_HEADER_LEN:end].decode("utf-8")
```

Hot-path payloads (the spatial library's `actor_input`) stay small and
fixed-layout so the decode is a single `struct.unpack`. The `emote` payload
is variable-length but bounded; a malformed frame raises `ValueError`, which
the handler catches so one bad frame does not abort the worker.

### 4. Write the handler

A handler is a callable invoked as `handler(msg_type, payload_bytes, session)`
when a decoded frame of its type is dispatched. The `session` argument is a
borrowed view the handler must not destroy. The handler runs on the worker
pool (ADR-0004), never on the reactor thread, so it may hold game state under
a lock without blocking the run loop.

The handler shares the per-server game state (the actor table, the cell
locator, the publish surface). The spatial library's handler set types its
dependencies as `typing.Protocol` interfaces so the logic is testable with
lightweight fakes and the real facade satisfies them structurally. The `emote`
handler depends on the same publish surface the `chat` handler uses:

```python
import threading
from typing import Protocol

from kith import Actor, ArtifactKey, CellKey


class _Publisher(Protocol):
    def publish_artifact(self, key: ArtifactKey, actor: Actor) -> int: ...
    def publish_cell_product(self, key: CellKey, authority_epoch: int) -> int: ...


class _InputModel(Protocol):
    def apply_input(self, actor: Actor, inp: object) -> Actor: ...
    def step(self, actors: list[Actor], dt_ms: int) -> list[Actor]: ...


class EmoteHandler:
    """The ``emote`` C2S handler: decode, look up, re-broadcast the cell."""

    __slots__ = (
        "_actors",
        "_cell",
        "_cell_epochs",
        "_lock",
        "_model",
        "_publisher",
        "_zone_id",
    )

    def __init__(
        self,
        *,
        publisher: _Publisher,
        model: _InputModel,
        zone_id: int,
        cell: tuple[int, int, int, int] = (0, 0, 0, 0),
    ) -> None:
        self._publisher = publisher
        self._model = model
        self._zone_id = zone_id
        self._cell = cell
        self._actors: dict[int, Actor] = {}
        self._cell_epochs: dict[CellKey, int] = {}
        self._lock = threading.Lock()

    def on_emote(self, msg_type: int, payload: bytes, session: object) -> None:
        """Re-broadcast the sender's cell product via the fabric.

        Frames for an unknown actor or a malformed payload are dropped
        silently so one bad frame does not abort the worker.
        """
        del msg_type, session
        try:
            actor_id, _text = decode_emote(payload)
        except ValueError:
            return
        with self._lock:
            if actor_id not in self._actors:
                return
            self._rebroadcast()

    def _rebroadcast(self) -> None:
        key = CellKey(
            zone=self._zone_id,
            cell_x=self._cell[0],
            cell_y=self._cell[1],
            cell_z=self._cell[2],
            lod=self._cell[3],
        )
        epoch = self._cell_epochs.get(key, 0) + 1
        self._publisher.publish_cell_product(key, epoch)
        self._cell_epochs[key] = epoch
```

The handler does not send a reply frame. The gateway composes the single
replication stream as the bounded view set each tick; a discrete S2C frame
(such as the spatial library's `login_reply`) is game-composed on the
session's connection, not an inbound dispatch — a handler that wants one
sends it through `session.send` (the getting-started guide's discrete S2C
section carries the contract). The `emote`'s effect reaches
subscribers through the same cell-stream fan-out replication rides on — the
re-broadcast asks the fabric to re-deliver the sender's cell to every
subscription tracking it, so nearby subscribers observe the emote's side
effects through the stream they already subscribe to.

### 5. Register the handler

Register the bound method for the type id through `register_message_handler`.
Wire types must already be registered on the borrowed proto (step 2) so the
gateway decoder accepts frames of this type during the run loop.

```python
def register(
    server: Server,
    *,
    model: SimModel,
    zone_id: int,
    cell: tuple[int, int, int, int] = (0, 0, 0, 0),
) -> EmoteHandler:
    handler = EmoteHandler(publisher=server, model=model, zone_id=zone_id, cell=cell)
    server.register_message_handler(EMOTE_TYPE, handler.on_emote)
    return handler
```

The facade holds a `ctypes` callback keep-alive slot for every registered
handler, so the Python callable stays referenced for the registration's
lifetime; the caller holds the returned `EmoteHandler` instance so the live
actor table and cell-epoch map survive for the server's lifetime.

### 6. Wire it into the composition root

Add the two registration calls to the server's `start()` alongside the
existing catalog and handler registrations, before `run()`:

```python
from examples.spatial import messages

messages.register(server)  # the existing catalog
server.register_proto_type("emote", EMOTE_TYPE)

emotes = EmoteHandler(publisher=server, model=model, zone_id=zone_id)
server.register_message_handler(EMOTE_TYPE, emotes.on_emote)

server.run()
```

No C file is added, no header changes, and no shared library is rebuilt. The
`emote` type dispatches the next time the server boots.

## Catalog principles to keep

- **One replication type, not four.** The gateway's `replication_type_id`
  selects a single S2C type; tier and spawn/despawn are in the payload, not
  separate types. Defining `state` / `state_snapshot` / `state_crowd` /
  `despawn` as separate wire types re-introduces the per-connection-push model
  the framework was built to break. A new S2C type is rare: the replication
  stream is the S2C surface.
- **Request/reply are separate types.** A request (`login`) and its reply
  (`login_reply`) are two type ids; the client registers an unambiguous
  handler for the reply type. The framework's optional 8-byte wire correlation
  trailer (ADR-0007) traces an input across the planes for the agentic harness;
  the application-level request/reply pairing is carried by the type ids and a
  correlation value the client puts in the request payload and the server
  echoes in the reply.
- **The hot path is thin.** A movement input payload maps directly onto
  `kith_sim_input_t`; the decode is one `struct.unpack` and the handler calls
  `apply_input` on the bound actor. Keep extension payloads that ride the hot
  path small and fixed-layout.
- **The wire and the control plane are two distinct surfaces.** Player input
  and replication ride the wire; harness commands (`spawn`/`move`/`teleport`/
  `query_state`) ride the control plane through `register_control_route`. A
  new player-facing action is a wire type with a Python handler; a new
  operations or testing action is a control route. Route handlers run on
  the same worker pool as wire and tick handlers: with more than one
  worker they run concurrently, so game state shared between callbacks
  needs the game's own synchronization.

## Testing the handler

The handler's dependencies are `typing.Protocol` interfaces, so the logic is
unit-testable with lightweight fakes: a fake `_Publisher` records the
`publish_cell_product` calls and a fake `_InputModel` returns canned actors.
The real facade's `Server` / `SimModel` / `Session` satisfy the same
protocols structurally, so the same handler instance runs in the integration
test that boots the server and drives the wire. The spatial library's handlers
follow this pattern; copy the shape so a handler is testable without a build.

## When an extension does need C

A new wire type and Python handler never need C. A change that touches the
framework's own contracts does: a new sim model (registered through the sim
vtable, `docs/guides/writing_c_sim_models.md`), a new fabric storage backing,
or a new plane. Those land through the pluggability-within-planes pattern
(ADR-0013) and follow the founding ADR process (ADR-0032). The common case —
a game adding its own messages and handlers — stays in Python.

## References

- `examples/spatial/messages.py` — the spatial library's wire catalog.
- `examples/spatial/handlers.py` — the spatial library's inbound handlers.
- `examples/lobby/` — a non-spatial catalog on Gateway + Control + DB.
- `docs/guides/getting_started.md` — the example skeleton (four servers
  and the spatial game library) and the planes-to-genres map.
- `docs/architecture/planes.md` — the plane contracts a handler honors.
- ADR-0004 — the C/Python threading boundary the worker pool enforces.
- ADR-0007 — the optional wire-protocol correlation trailer.
- ADR-0013 — pluggability is within planes, not across invariant-breaking
  topologies.
