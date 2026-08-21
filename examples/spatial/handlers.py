"""Inbound wire message handlers for the generic spatial game library.

One handler per client-to-server type in messages,
registered through register_message_handler so the
gateway dispatches a decoded frame of each type to the matching bound method.
The server-to-client types (``login_reply``, the replication type
``actor_state``, and its multi-subject batch form ``actor_state_batch``) have
no inbound handler: the gateway encodes ``actor_state``
as the bounded view set each tick, and a discrete ``login_reply`` frame is a
game-composed response on the session's connection, not an inbound dispatch.

The handlers share the per-server game state (the actor table, the principal-
to-actor map, the sim model, the per-actor current cell, and the per-tick
dirty-cell set). Each actor publishes into the cell derived from its position
by floor-division of the sim's Q16.16 coordinates by a per-instance cell
size, so a move across a cell boundary evicts the stale artifact from the
old cell before the re-publish into the new one. The per-input
``publish_artifact`` stays on the handler (O(1), and the freshest state the
next tick delivers to the subscriber), but the cell's fabric product header
is bumped once per tick, not once per input: a handler marks the cell dirty
and a per-tick flush (registered on the server as its tick callback via
register_tick_handler) drains the dirty-cell set with one
``publish_cell_product`` per dirty cell. The gateway's shared cache, which
drains only cells the fabric marked pending, refreshes each dirty cell on
the tick after the flush, so the relevance composer picks up the new state
one tick after the input that dirtied it. A login binds the session to one
actor per principal, publishes the actor's initial state, and seeds the
session's subscription window to the actor's cell neighborhood so the
gateway's view composer resolves the subscriber; the seeded cell is dirtied
by the publish, so the next flush bumps it and the subscriber receives its
initial view on the tick after the flush. An ``actor_input`` frame decodes
into a SimInput applied to the bound actor and, when the moved
actor crossed a cell boundary, diffs its window against the old neighborhood
(removing cells that left, adding cells that entered) instead of clearing and
re-adding the full set; a ``chat`` frame marks the sender's cell dirty so the
next flush re-broadcasts its product into the fabric stream and the
subscription fan-out carries the update to nearby subscribers.
Payloads are thin fixed-layout byte strings, (de)serialized here in Python:
the codec is the game's concern, not the framework's, and the hot-path input
maps directly onto ``kith_sim_input_t``.

Movement can also run natively: with the ``native_apply`` wiring on, the
movement message type is handled by the C core in
native (the pool-dispatched native handler) —
the whole per-input sequence (decode, stripe-locked apply,
publish, dirty marks, identity gate, crossing window diff, record) executes
in C on a worker thread, the per-tick flush drains the C dirty set and the
C record buffer, and the control-plane accessors delegate to the C table so
exactly one actor table exists. The Python handler set keeps login, chat,
the principal map, the bind-ownership records, and the spawn/teleport/query
control surface.

The handler dependencies are typed as Protocol interfaces so
the logic is testable with lightweight fakes and the real facade's
``Server`` / ``SimModel`` / ``Session`` satisfy them structurally.
"""

from __future__ import annotations

import contextlib
import struct
import sys
import threading
from collections.abc import Sequence
from dataclasses import replace
from typing import TYPE_CHECKING, Final, Protocol

from examples._common.replay_format import Event, MoveEvent, SpawnEvent
from examples.spatial import messages

from kith import (
    Actor,
    ArtifactKey,
    CellKey,
    KithNetworkError,
    KithStateError,
    SessionInfo,
    SimInput,
)
from kith._generated import types as _gen_types


# Canonical spatial geometry shared with every tool that must reproduce
# the server's neighborhood semantics from raw positions: one cell spans
# CELL_SIZE_Q16 fixed-point units per axis (cell = floor(pos / cell_size)),
# and an actor publishes to every cell within CELL_RADIUS cells of its own
# along each axis.
CELL_SIZE_Q16: Final[int] = 1 << 16
CELL_RADIUS: Final[int] = 1

# Stripe-lock count for the per-actor apply sequence. Actor ids map to
# stripes by modulo; 64 over the 1000-actor deployment population spreads
# concurrent applies uniformly across far more stripes than the worker
# pool can occupy at once, so same-stripe collision among in-flight
# applies is rare while same-actor applies still serialize under their
# stripe.
STRIPE_COUNT: Final[int] = 64


if TYPE_CHECKING:
    from examples.spatial.native import SpatialNative

    from kith import Server, SimModel


__all__ = [
    "ACTOR_INPUT_PAYLOAD_LEN",
    "CHAT_MAX_TEXT",
    "LOGIN_PAYLOAD_LEN",
    "SpatialHandlers",
    "decode_actor_input",
    "decode_chat",
    "decode_login",
    "encode_actor_input",
    "encode_chat",
    "encode_login",
    "register",
]
# ---------------------------------------------------------------------------
# payload codecs
# ---------------------------------------------------------------------------

# login (C2S): an 8-byte little-endian principal id. The principal is the
# account-level identity; one actor is bound per principal (one actor per
# account), so the catalog carries no character-roster surface.
LOGIN_PAYLOAD_LEN: int = 8

# actor_input (C2S, hot path): 8-byte actor_id + 4-byte input_tick + three
# 2-byte signed move components + 1-byte flags = 19 bytes, a thin wrapper
# over kith_sim_input_t. No reply: the server's response is the replication
# stream.
ACTOR_INPUT_PAYLOAD_LEN: int = 19

# chat (C2S): 8-byte actor_id + 2-byte little-endian text length + the UTF-8
# text. Scope (proximity, zone, party, ...) is a game routing choice, not a
# wire type, so the catalog carries one generic chat type.
_CHAT_HEADER_LEN: int = 10
CHAT_MAX_TEXT: int = 4096


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
    """Decode a ``chat`` frame payload into the actor id and text."""
    if len(payload) < _CHAT_HEADER_LEN:
        raise ValueError(f"chat payload too short: {len(payload)} < {_CHAT_HEADER_LEN}")
    actor_id, text_len = (int(v) for v in struct.unpack("<QH", payload[:_CHAT_HEADER_LEN]))
    if text_len > CHAT_MAX_TEXT:
        raise ValueError(f"chat text length out of range: {text_len} > {CHAT_MAX_TEXT}")
    end = _CHAT_HEADER_LEN + text_len
    if end > len(payload):
        raise ValueError(f"chat text truncated: need {end}, have {len(payload)}")
    return actor_id, payload[_CHAT_HEADER_LEN:end].decode("utf-8")


# ---------------------------------------------------------------------------
# dependency protocols (structurally satisfied by the facade types)
# ---------------------------------------------------------------------------


class _Publisher(Protocol):
    """The publish surface a handler drives: sim artifacts, cell products,
    and cell-scoped broadcasts."""

    def publish_artifact(self, key: ArtifactKey, actor: Actor) -> int: ...

    def publish_cell_product(self, key: CellKey, authority_epoch: int) -> int: ...

    def broadcast_cell(self, key: CellKey, msg_type: int, payload: bytes) -> None: ...


class _InputModel(Protocol):
    """The sim model surface a handler applies movement inputs through."""

    def apply_input(self, actor: Actor, inp: SimInput) -> Actor: ...

    def step(self, actors: list[Actor], dt_ms: int) -> list[Actor]: ...


class _RecordingSink(Protocol):
    """The recording surface a handler feeds replay events into."""

    def record(self, event: Event) -> None: ...


class _BindableSession(Protocol):
    """The session surface a handler binds a principal's actor to."""

    def populate(self, actor_id: int, cells: Sequence[CellKey]) -> None: ...

    def info(self) -> _SessionInfo: ...

    def window_add(self, key: CellKey) -> None: ...

    def window_remove(self, key: CellKey) -> None: ...


class _SessionInfo(Protocol):
    """The session metadata a handler reads to match a bound actor."""

    actor_id: int
    session_id: int


# ---------------------------------------------------------------------------
# handler set
# ---------------------------------------------------------------------------


class SpatialHandlers:
    """The three inbound (C2S) wire handlers plus their shared game state.

    One instance per server. The bound methods are registered through
    register via register_message_handler; the
    gateway's trampoline invokes ``handler(msg_type, payload_bytes, session)``
    on a worker, so every handler takes those three positional
    arguments even when it ignores ``msg_type`` or ``session``.

    Locking: per-actor apply sequences run under per-stripe locks (actor id
    modulo ``STRIPE_COUNT``) so same-actor applies serialize while different
    actors proceed in parallel. A narrow bookkeeping lock guards the
    cross-actor dirty-cell set, the per-cell authority epochs, and the
    identity-gate counter; a control lock guards the low-rate lifecycle
    state (principal map, actor-id allocator, bind records). Nesting order
    is control -> stripe -> bookkeeping, with no exceptions: the spawn path
    holds the fresh actor's stripe inside the control lock, applies nest
    bookkeeping inside their stripe, and the flush holds bookkeeping alone.

    Args:
        server: Publish surface for sim artifacts and cell products (the
            facade's ~kith.Server.publish_artifact and
            ~kith.Server.publish_cell_product).
        model: Sim model the ``actor_input`` handler applies movement inputs
            through.
        zone_id: Zone id every actor publishes into.
        cell_size: Size of one cell in the sim's Q16.16 fixed-point units.
            An actor's cell is ``floor(position / cell_size)`` per axis, so a
            value of ``1 << 16`` (one tile) gives one cell per tile. Defaults
            to one tile.
        cell_radius: Chebyshev radius of the subscription window each bound
            subscriber tracks around its own cell (0 tracks only the
            subscriber's own cell; 1 adds the 8 neighbors). Defaults to 1.
        recorder: Optional replay-event sink; when set, every spawned actor
            and applied movement is recorded as a v1 replay event (the wire
            handlers record too, since they delegate to the same methods).
            A session that uses teleport_actor diverges permanently
            from the teleport tick onward when its recording is replayed:
            teleporting repositions an actor absolutely, and the closed v1
            event set has no encoding for it.
    """

    __slots__ = (
        "_actor_binds",
        "_actor_cells",
        "_actors",
        "_bind_conflicts",
        "_book_lock",
        "_cell_epochs",
        "_cell_radius",
        "_cell_size",
        "_control_lock",
        "_dirty_cells",
        "_gate_drops",
        "_model",
        "_native",
        "_next_actor_id",
        "_principals",
        "_recorder",
        "_server",
        "_session_actors",
        "_stripes",
        "_zone_id",
    )

    def __init__(
        self,
        *,
        server: _Publisher,
        model: _InputModel,
        zone_id: int,
        cell_size: int = CELL_SIZE_Q16,
        cell_radius: int = CELL_RADIUS,
        recorder: _RecordingSink | None = None,
        native: SpatialNative | None = None,
    ) -> None:
        self._server = server
        self._model = model
        self._zone_id = zone_id
        self._cell_size = cell_size
        self._cell_radius = cell_radius
        self._recorder = recorder
        # The native movement core, when wired: the actor table, the
        # per-actor cells, the dirty set, the epochs, and the gate counter
        # live in C and every actor-state path here delegates to it.
        self._native = native
        self._actors: dict[int, Actor] = {}
        # Each actor's current published cell, so a move across a cell
        # boundary evicts the stale artifact from the old cell before the
        # re-publish into the new one (the sim supersedes an artifact only
        # within a cell, not across cells).
        self._actor_cells: dict[int, tuple[int, int, int]] = {}
        self._principals: dict[int, int] = {}
        self._next_actor_id = 1
        # Session-ownership records for the one-account-one-view policy: the
        # session a bind came from (session id → actor id) and its reverse
        # (actor id → the sessions referencing it). A second, different
        # session binding an already-referenced actor is counted and logged
        # but permitted — the framework legitimately allows shared
        # subscribers; this example's policy treats it as an anomaly worth
        # seeing on the control plane rather than one worth rejecting.
        self._session_actors: dict[int, int] = {}
        self._actor_binds: dict[int, set[int]] = {}
        self._bind_conflicts = 0
        # Movement inputs dropped by the identity gate (the payload names an
        # actor other than the session's bound actor). The gate is silent on
        # the wire by design; this counter is what makes its engagement
        # visible instead of inferable.
        self._gate_drops = 0
        # Monotonic authority epoch per cell; the fabric rejects a stale
        # epoch, so the handler tracks the current value and increments it
        # on every re-broadcast.
        self._cell_epochs: dict[CellKey, int] = {}
        # The per-tick dirty-cell set: a handler marks a cell dirty here and
        # the per-tick flush (flush_dirty_cells, registered as the server's
        # tick callback) drains it with one publish_cell_product per cell,
        # coalescing the per-input cell-product bumps into a per-tick cadence.
        self._dirty_cells: set[CellKey] = set()
        # Three lock classes with a fixed nesting order, replacing the one
        # handler-wide lock that serialized every actor's apply sequence:
        #
        #   _stripes     one lock per stripe of actor ids (id % STRIPE_COUNT);
        #                guards a per-actor apply sequence end to end (table
        #                read, model apply/step, store, publish, cell
        #                bookkeeping), so same-actor applies serialize while
        #                different actors proceed in parallel
        #   _book_lock   tiny cross-actor bookkeeping the per-actor sequence
        #                touches: the dirty-cell set, the per-cell authority
        #                epochs, and the identity-gate drop counter
        #   _control_lock low-rate lifecycle state: the principal map, the
        #                actor-id allocator, and the session/actor bind
        #                records
        #
        # Nesting order: _control_lock -> stripe -> _book_lock. Nothing takes
        # a lock to the left while holding one to its right.
        self._stripes: tuple[threading.Lock, ...] = tuple(
            threading.Lock() for _ in range(STRIPE_COUNT)
        )
        self._book_lock = threading.Lock()
        self._control_lock = threading.Lock()

    def close(self) -> None:
        """Destroy the native movement core; idempotent.

        The composition root calls this after the server facade is down:
        the gateway is quiesced by the facade teardown, so no registered
        handler can reach the core once it is destroyed.
        """
        if self._native is not None:
            self._native.close()

    # -----------------------------------------------------------------------
    # public mutation surface
    # -----------------------------------------------------------------------
    #
    # The same logic the wire handlers drive; the control-plane routes of a
    # server wiring (e.g. examples/embedded/server.py) call these so the wire
    # and the harness mutate one shared actor table instead of two parallel
    # copies. The wire handlers below delegate here for the same reason: one
    # definition of spawn / move / teleport / query.

    def spawn_actor(self, principal_id: int) -> Actor:
        """Return the actor for ``principal_id``, allocating and publishing on first sight.

        One actor is bound per principal (one actor per account). The first
        time a principal is seen, an actor is allocated at the origin and
        its initial state is published into the sim while the fresh actor's
        stripe is held inside the control lock, so a concurrent movement
        input on the fresh actor (which needs the same stripe) cannot
        interleave a moved position between the store and the spawn publish;
        a returning principal rebinds to its existing actor without
        allocating or re-publishing.
        """
        with self._control_lock:
            actor_id = self._principals.get(principal_id)
            if actor_id is None:
                actor_id = self._next_actor_id
                self._next_actor_id += 1
                fresh = Actor(id=actor_id, pos_x=0, pos_y=0, pos_z=0)
                self._principals[principal_id] = actor_id
                if self._native is not None:
                    self._native.actor_insert(fresh)
                else:
                    with self._stripe_of(actor_id):
                        self._actors[actor_id] = fresh
                        self._publish_locked(fresh)
                actor = fresh
            elif self._native is not None:
                fresh = None
                fetched = self._native.actor_get(actor_id)
                if fetched is None:
                    # The principal map owns the allocation truth: a bound
                    # principal's actor always exists, so a miss is state
                    # corruption, not a recoverable absence.
                    raise KithStateError(
                        _gen_types.kith_error.KITH_ESTATE,
                        f"principal {principal_id} binds actor {actor_id} "
                        f"absent from the native table",
                    )
                actor = fetched
            else:
                fresh = None
                actor = self._actors[actor_id]
        if fresh is not None and self._recorder is not None:
            self._recorder.record(
                SpawnEvent(
                    actor_id=fresh.id,
                    pos_x=fresh.pos_x,
                    pos_y=fresh.pos_y,
                    pos_z=fresh.pos_z,
                    vel_x=fresh.vel_x,
                    vel_y=fresh.vel_y,
                    vel_z=fresh.vel_z,
                    flags=fresh.flags,
                )
            )
        return actor

    def apply_movement(self, actor_id: int, inp: SimInput, *, dt_ms: int = 50) -> Actor | None:
        """Apply a movement input, step the sim one tick, and publish the new state.

        The composition root does not step the sim itself (stepping is game
        code's concern per the server tick contract); this method applies the
        input to the actor, advances the sim by ``dt_ms`` so the actor's
        position integrates the input, publishes the stepped state, and
        stores it — all under one hold of the handlers lock, so the whole
        movement is one transaction. The default ``dt_ms`` matches a 20 Hz
        tick (50 ms); a game running at a different tick rate passes its own
        per-tick dt. Returns the updated actor, or ``None`` when no actor
        with that id is bound (a frame for an unknown actor is dropped
        silently).

        The transaction is atomic because a concurrent same-actor apply
        that lands out of order is not a benign lost update: the regressed
        stored position desynchronizes the subscription window's diff chain
        from the live position, the session's window stops tracking the
        bound actor's cell, and every composition then fails to locate the
        subscriber. Serializing same-actor inputs keeps the stored position
        monotone, which keeps the diff chain convergent. Each actor's
        movement runs under its stripe lock (actor id modulo the stripe
        count), so applies for different actors proceed in parallel while
        same-actor applies serialize; the critical section is one
        microsecond-scale integration and publish.
        """
        if self._native is not None:
            # The control path: no identity gate, no window diff; the C core
            # buffers the replay record (drained by the per-tick flush), the
            # same cadence the wire path records with.
            return self._native.apply(actor_id, inp, dt_ms=dt_ms)
        with self._stripe_of(actor_id):
            stepped = self._apply_movement_locked(actor_id, inp, dt_ms=dt_ms)
        if stepped is not None and self._recorder is not None:
            self._recorder.record(
                MoveEvent(
                    actor_id=actor_id,
                    input_tick=inp.input_tick,
                    move_x=inp.move_x,
                    move_y=inp.move_y,
                    move_z=inp.move_z,
                    flags=inp.flags,
                )
            )
        return stepped

    def _apply_movement_locked(self, actor_id: int, inp: SimInput, *, dt_ms: int) -> Actor | None:
        """Integrate one input, publish, and store; the actor's stripe is held.

        The callers hold the actor's stripe lock across the whole movement so
        a concurrent same-actor apply cannot interleave a regressed store
        between the read and the write (see apply_movement).
        """
        stored = self._actors.get(actor_id)
        if stored is None:
            return None
        actor = replace(stored)
        applied = self._model.apply_input(actor, inp)
        stepped = self._model.step([applied], dt_ms=dt_ms)[0]
        self._actors[actor_id] = stepped
        self._publish_locked(stepped)
        return stepped

    def teleport_actor(self, actor_id: int, pos_x: int, pos_y: int) -> Actor | None:
        """Set an actor's absolute position and publish the updated state.

        Zeroes velocity and preserves the actor's input tick and flags. The
        store and the publish run under the actor's stripe lock hold, so a
        concurrent movement input on the same actor cannot interleave a
        stale position between them.
        Returns the updated actor, or ``None`` when no actor with that id is
        bound.
        """
        if self._native is not None:
            current = self._native.actor_get(actor_id)
            if current is None:
                return None
            return self._native.actor_teleport(actor_id, pos_x, pos_y, current.pos_z)
        with self._stripe_of(actor_id):
            actor = self._actors.get(actor_id)
            if actor is None:
                return None
            updated = Actor(
                id=actor.id,
                pos_x=pos_x,
                pos_y=pos_y,
                pos_z=actor.pos_z,
                vel_x=0,
                vel_y=0,
                vel_z=0,
                input_tick=actor.input_tick,
                flags=actor.flags,
                # A teleport is not a movement apply: the counter certifies
                # movement coverage, so it carries unchanged.
                update_seq=actor.update_seq,
            )
            self._actors[actor_id] = updated
            self._publish_locked(updated)
        return updated

    @property
    def cell_size(self) -> int:
        """Size of one cell in the sim's Q16.16 fixed-point units."""
        return self._cell_size

    def actor_state(self, actor_id: int) -> Actor | None:
        """Return the live state of one actor, or ``None`` if absent."""
        if self._native is not None:
            return self._native.actor_get(actor_id)
        with self._stripe_of(actor_id):
            return self._actors.get(actor_id)

    def actor_states(self) -> list[Actor]:
        """Return a snapshot of every bound actor, in no defined order.

        The snapshot is quiesced: the control lock excludes concurrent
        spawns (the only structural change to the actor table) and every
        stripe is held for the duration, so no per-actor apply can replace
        a value mid-iteration. Control-plane calls use this path; it never
        runs on the apply hot path. With the native core wired, the snapshot
        is the C table's (same quiesce, inside the core).
        """
        if self._native is not None:
            return self._native.actor_snapshot()
        with self._control_lock, contextlib.ExitStack() as stack:
            for stripe in self._stripes:
                stack.enter_context(stripe)
            return list(self._actors.values())

    def binding_stats(self) -> tuple[list[tuple[int, int]], int, int]:
        """Return the principal→actor bindings plus the ownership counters.

        Returns ``(bindings, bind_conflicts, gate_drops)`` where
        ``bindings`` lists ``(principal_id, actor_id)`` pairs in principal
        allocation order. One control-lock-held snapshot with the
        bookkeeping lock nested inside, so the route never interleaves a
        torn map with the counters.
        """
        with self._control_lock, self._book_lock:
            drops = self._native.gate_drops if self._native is not None else self._gate_drops
            return (
                list(self._principals.items()),
                self._bind_conflicts,
                drops,
            )

    # -----------------------------------------------------------------------
    # wire handlers
    # -----------------------------------------------------------------------

    def on_login(self, msg_type: int, payload: bytes, session: _BindableSession) -> None:
        """Bind the session to one actor per principal and publish its state.

        Decodes the principal id from the payload, allocates an actor the
        first time the principal is seen (one actor per account), publishes the
        actor's initial state into its position-derived cell (marking the cell
        dirty so the per-tick flush bumps it), and populates the session —
        binding the actor and seeding the subscription window to the actor's
        cell neighborhood in one atomic step, so the gateway's view composer
        resolves the subscriber with no bound-but-windowless intermediate.
        The cell is marked dirty in the publish and the bump is deferred to the
        next tick's flush, which runs after the populate in the same handler
        invocation, so the subscriber receives its initial view on the tick
        after the flush (a one-tick pipeline). A returning principal rebinds
        to its existing actor without allocating; the populate re-seeds the
        same neighborhood additively.
        """
        del msg_type
        if len(payload) < LOGIN_PAYLOAD_LEN:
            return
        principal_id = decode_login(payload)
        actor = self.spawn_actor(principal_id)
        self._record_bind(session, actor.id)
        session.populate(actor.id, self._neighborhood(self._cell_of(actor)))

    def _record_bind(self, session: _BindableSession, actor_id: int) -> None:
        """Record a login's session→actor binding and flag shared references.

        A repeated login of the same principal on the same session rebinds
        silently (the recorded state already matches). A different session
        referencing an actor this map already tracks is permitted — the C
        bind still happens after this call — but counted as a bind conflict
        with one stderr line, so a reconnect to a live duplicate or an
        accidental double-subscribe shows up instead of passing unseen.
        """
        session_id = session.info().session_id
        with self._control_lock:
            previous = self._session_actors.get(session_id)
            if previous == actor_id:
                return
            holders = self._actor_binds.get(actor_id, set()) - {session_id}
            if holders:
                self._bind_conflicts += 1
                print(
                    f"spatial: session {session_id} bound actor {actor_id} while "
                    f"{len(holders)} other session(s) still reference it",
                    file=sys.stderr,
                )
            if previous is not None:
                held = self._actor_binds.get(previous)
                if held is not None:
                    held.discard(session_id)
                    if not held:
                        del self._actor_binds[previous]
            self._session_actors[session_id] = actor_id
            self._actor_binds.setdefault(actor_id, set()).add(session_id)

    def on_session_destroyed(self, info: SessionInfo) -> None:
        """Reclaim the per-session ownership records on session death.

        The gateway fires this when it destroys a session (the composition
        root registers it on the handler set's behalf), so a churn cycle
        releases the seat instead of growing the maps. A session that never
        logged in pops nothing. The lock matches ``_record_bind``'s: both
        run as pool-dispatched callbacks against the same shared pool.
        """
        with self._control_lock:
            actor_id = self._session_actors.pop(info.session_id, None)
            if actor_id is None:
                return
            binds = self._actor_binds.get(actor_id)
            if binds is not None:
                binds.discard(info.session_id)
                if not binds:
                    del self._actor_binds[actor_id]

    def on_actor_input(self, msg_type: int, payload: bytes, session: _BindableSession) -> None:
        """Apply a decoded movement input to the bound actor and re-publish.

        The payload is a thin wrapper over SimInput; the handler
        applies the input to the actor the frame names and publishes the new
        state (marking the new cell dirty, and the old cell dirty on a
        crossing so its stale artifact drops from the cache). When the moved
        actor is the session's bound subscriber and it crossed a cell
        boundary, the subscriber's window is diffed against the old
        neighborhood (removing cells that left, adding cells that entered) so
        a radius-1 crossing touches at most three of nine cells instead of
        clearing and re-adding the full set. The movement, the publish, and
        the window diff run under one hold of the actor's stripe lock so
        concurrent workers cannot interleave two same-actor inputs' diff
        operations into a lost remove (which would leak retired columns into
        the window) nor a regressed store (which would desynchronize the
        diff chain from the live position). The identity gate keeps its
        apply-movement-first
        order: a frame whose actor is not the session's bound subscriber
        still moves the shared actor but skips the window diff, and the drop
        is counted. The per-tick flush bumps the dirtied cells; frames for
        an unknown actor are dropped silently.
        """
        del msg_type
        if len(payload) < ACTOR_INPUT_PAYLOAD_LEN:
            return
        actor_id, inp = decode_actor_input(payload)
        with self._stripe_of(actor_id):
            before = self._actors.get(actor_id)
            if before is None:
                return
            stepped = self._apply_movement_locked(actor_id, inp, dt_ms=50)
            if stepped is None:
                return
            try:
                gate_ok = session.info().actor_id == actor_id
            except Exception:
                return
            if not gate_ok:
                with self._book_lock:
                    self._gate_drops += 1
            else:
                old_cell = self._cell_of(before)
                new_cell = self._cell_of(stepped)
                if old_cell != new_cell:
                    self._diff_window(session, stepped, old_cell)
        if self._recorder is not None:
            self._recorder.record(
                MoveEvent(
                    actor_id=actor_id,
                    input_tick=inp.input_tick,
                    move_x=inp.move_x,
                    move_y=inp.move_y,
                    move_z=inp.move_z,
                    flags=inp.flags,
                )
            )

    def on_chat(self, msg_type: int, payload: bytes, session: _BindableSession) -> None:
        """Broadcast the text to the sender's cell and mark the cell dirty.

        Decodes the actor id and text, looks up the sender's cell, and
        submits a ``chat_event`` broadcast on the gateway's request queue:
        the next tick pass delivers the frame to every session whose window
        covers the cell — the recipient set is the gateway's, not a roster
        this handler builds. The event payload is the ``chat`` frame's own
        bytes (actor id + text), so both peers decode the S2C event with
        the same codec as the C2S submit. A saturated broadcast queue
        drops the line: the submit refusal is counted by the gateway
        (``kith_gateway_broadcast_refused_total``) and surfaces on
        /metrics, so the handler swallows the transport-family error and
        keeps the worker alive — the operator reads the counter instead of
        the log. The dirty mark still rides: the per-tick flush bumps the
        cell's product header so subscribers also refresh the sender's
        presence through the stream. Frames for an unknown actor or a
        malformed payload are dropped silently so one bad frame does not
        abort the worker.
        """
        del msg_type, session
        try:
            actor_id, _text = decode_chat(payload)
        except ValueError:
            return
        actor = self._actor_of(actor_id)
        if actor is None:
            return
        cell = self._cell_of(actor)
        with contextlib.suppress(KithNetworkError):
            self._server.broadcast_cell(
                CellKey(zone=self._zone_id, cell_x=cell[0], cell_y=cell[1], cell_z=cell[2], lod=0),
                messages.CHAT_EVENT_TYPE,
                payload,
            )
        if self._native is not None:
            self._native.mark_actor_cell_dirty(actor_id)
            return
        self._mark_dirty(cell)

    # -----------------------------------------------------------------------
    # helpers
    # -----------------------------------------------------------------------

    def _stripe_of(self, actor_id: int) -> threading.Lock:
        """Return the stripe lock that owns ``actor_id``'s apply sequence."""
        return self._stripes[actor_id % len(self._stripes)]

    def _actor_of(self, actor_id: int) -> Actor | None:
        """Look up an actor across both apply paths.

        The native core owns the actor table when wired; the Python dict
        owns it otherwise, guarded by the actor's apply stripe.
        """
        if self._native is not None:
            return self._native.actor_get(actor_id)
        with self._stripe_of(actor_id):
            return self._actors.get(actor_id)

    def _cell_of(self, actor: Actor) -> tuple[int, int, int]:
        """Derive an actor's cell from its position by floor-division per axis."""
        size = self._cell_size
        return (actor.pos_x // size, actor.pos_y // size, actor.pos_z // size)

    def _publish_locked(self, actor: Actor) -> None:
        """Publish an actor into its position-derived cell; the lock is held.

        The publish alone moves the actor between cells: the sim store holds
        one artifact per actor and relocates it atomically under the store
        lock when the position-derived cell changed, so the artifact is
        never absent from the store mid-move. (Removing before republishing
        would open exactly that absence window between the two calls, and a
        compose landing there fails to locate the subscriber — the removal
        is reserved for actors leaving the world entirely.) The publish runs
        under the actor's stripe lock so the artifact lands at the last
        stored position even when two workers apply the same actor
        concurrently: an out-of-order publish would strand the artifact in a
        cell the subscriber's window already retired.

        After the sim publish, every cell whose contents changed is marked
        dirty so the per-tick flush bumps its fabric product header and the
        gateway's shared cache (which drains only cells the fabric marked
        pending) refreshes that cell on the next tick. Both the new cell
        (the actor arrived) and the old cell (the actor left, so its stale
        artifact must drop from the cache) are marked dirty: omitting the
        old cell leaves the cache holding a stale artifact, so a subscriber
        whose window still covers the old cell keeps seeing the actor after
        it moved out of the window. The cell-product bump is deferred to the
        per-tick flush so a cell dirtied by many inputs in one tick is
        bumped once, not once per input.
        """
        new_cell = self._cell_of(actor)
        old_cell = self._actor_cells.get(actor.id)
        key = ArtifactKey(
            zone=self._zone_id,
            cell_x=new_cell[0],
            cell_y=new_cell[1],
            cell_z=new_cell[2],
            lod=0,
        )
        self._server.publish_artifact(key, actor)
        self._actor_cells[actor.id] = new_cell
        with self._book_lock:
            self._dirty_cells.add(
                CellKey(
                    zone=self._zone_id,
                    cell_x=new_cell[0],
                    cell_y=new_cell[1],
                    cell_z=new_cell[2],
                    lod=0,
                )
            )
            if old_cell is not None and old_cell != new_cell:
                self._dirty_cells.add(
                    CellKey(
                        zone=self._zone_id,
                        cell_x=old_cell[0],
                        cell_y=old_cell[1],
                        cell_z=old_cell[2],
                        lod=0,
                    )
                )

    def _neighborhood(self, cell: tuple[int, int, int]) -> list[CellKey]:
        """Return the Chebyshev neighborhood of ``cell`` at the configured radius."""
        r = self._cell_radius
        cells: list[CellKey] = []
        for dx in range(-r, r + 1):
            for dy in range(-r, r + 1):
                for dz in range(-r, r + 1):
                    cells.append(
                        CellKey(
                            zone=self._zone_id,
                            cell_x=cell[0] + dx,
                            cell_y=cell[1] + dy,
                            cell_z=cell[2] + dz,
                            lod=0,
                        )
                    )
        return cells

    def _diff_window(
        self,
        session: _BindableSession,
        actor: Actor,
        old_cell: tuple[int, int, int],
    ) -> None:
        """Diff the session's subscription window against the old neighborhood.

        On a cell crossing the window is diffed against the old
        neighborhood: cells that left the window are removed, cells that
        entered are added. A radius-1 crossing changes at most three of
        nine cells, so the diff issues at most six fabric-subscription
        operations instead of a clear plus nine adds, and the cells that
        did not move keep their cache refcounts rather than churning
        through unsubscribe/resubscribe. The initial seed is
        :meth:`Session.populate <kith.gateway.Session.populate>`'s job —
        one atomic step at login instead of a clear-and-rebuild here.
        """
        new_neighborhood = self._neighborhood(self._cell_of(actor))
        old_set = set(self._neighborhood(old_cell))
        new_set = set(new_neighborhood)
        for key in old_set - new_set:
            session.window_remove(key)
        for key in new_set - old_set:
            session.window_add(key)

    def _mark_dirty(self, cell: tuple[int, int, int]) -> None:
        """Add a cell to the per-tick dirty set for the flush to bump.

        A handler marks a cell dirty here instead of bumping its product
        header eagerly; the per-tick flush (``flush_dirty_cells``) drains the
        set with one ``publish_cell_product`` per dirty cell, coalescing the
        per-input cell-product bumps into a per-tick cadence. The set is a
        ``set[CellKey]`` so repeated marks on the same cell between flushes
        coalesce to one bump. Marks from any stripe land under the
        bookkeeping lock, which the flush's snapshot also holds.
        """
        key = CellKey(zone=self._zone_id, cell_x=cell[0], cell_y=cell[1], cell_z=cell[2], lod=0)
        with self._book_lock:
            self._dirty_cells.add(key)

    def flush_dirty_cells(self, tick: int) -> None:
        """Drain the dirty-cell set with one cell-product bump per cell.

        Registered on the server as its per-tick callback
        (register_tick_handler) so the per-input handlers
        defer cell-product bumps and the flush coalesces them: every cell
        dirtied by any handler since the last flush is bumped exactly once per
        tick instead of once per input. The bump runs on a worker thread,
        never the reactor, and its publishes are observed by the
        next tick's gateway refresh — a one-tick pipeline, the intended
        coalescing tradeoff.

        The dirty set is snapshotted and cleared under the bookkeeping lock
        so handlers running on other workers keep marking cells dirty while
        the flush publishes; a cell re-dirtied during the flush is bumped on
        the next tick. The epoch read-modify-write is guarded by the same
        lock; the fabric publish runs outside it. A stale-epoch publish
        (``KITH_EPERM``, two flushes bumping the same cell out of order) is
        swallowed: the higher-epoch publish already marked the cell pending
        and the next cache refresh still pulls the latest sim state.

        Args:
            tick: The server's monotonic tick counter, advanced once per
                normal tick. The flush is cadence-anchored to the tick (the
                callback fires once per tick) but does not read the value.

        Thread safety:
            @thread_safety safe — the dirty set and the epoch table are
            guarded by the narrow bookkeeping lock; the fabric publish is
            thread-safe. With the native core wired, the C core owns the
            same discipline internally and this call is one ctypes
            crossing.
        """
        del tick
        if self._native is not None:
            for event in self._native.flush():
                if self._recorder is not None:
                    self._recorder.record(event)
            return
        with self._book_lock:
            cells = list(self._dirty_cells)
            self._dirty_cells.clear()
        for key in cells:
            with self._book_lock:
                epoch = self._cell_epochs.get(key, 0) + 1
                self._cell_epochs[key] = epoch
            with contextlib.suppress(KithStateError):
                self._server.publish_cell_product(key, epoch)


def register(
    server: Server,
    *,
    model: SimModel,
    zone_id: int,
    cell_size: int = 1 << 16,
    cell_radius: int = 1,
    recorder: _RecordingSink | None = None,
    native_apply: bool = False,
) -> SpatialHandlers:
    """Build the handler set and register its C2S handlers on ``server``.

    Wire types must already be registered on the server's borrowed proto (via
    register) so the gateway decoder accepts
    frames of each type during the run loop. Returns the handler set so the
    caller holds the live actor table and principal map for the server's
    lifetime, and so the caller can register the per-tick flush
    (``handlers.flush_dirty_cells``) on the server via
    register_tick_handler before run:
    the handlers defer cell-product bumps to the per-tick dirty-cell set, so
    without the flush registered no cell is ever bumped and view propagation
    never advances. The composition root owns that registration so a game that
    also wants its own per-tick logic (e.g. recording the tick's input batch
    for replay) owns the single tick slot.

    With ``native_apply`` the movement message type is handled by the C core
    (examples/spatial/native_core.c) registered with the pool-dispatched
    native-handler flag: the per-input sequence never enters the
    Python interpreter, the per-tick flush becomes one ctypes call that
    drains the C dirty set and the C record buffer, and the handler set's
    actor-state surface delegates to the C table so exactly one table
    exists. Login and chat stay Python handlers. The knob requires the
    built ``spatial_native_core`` shared library.

    The ``native_apply`` wiring reaches the server's private borrowed-handle
    accessors (``_borrowed_sim``, ``_borrowed_fabric``, ``_borrowed_gateway``):
    a deliberate composition-root exception — this example composes the
    server's own planes during setup, and no public borrowed-handle surface
    exists this release.

    Args:
        server: The facade the handlers publish through and register on.
        model: A sim model instance the ``actor_input`` handler applies
            movement inputs through.
        zone_id: Zone id every actor publishes into.
        cell_size: Size of one cell in the sim's Q16.16 fixed-point units;
            ``1 << 16`` is one tile.
        cell_radius: Chebyshev radius of the subscription window each bound
            subscriber tracks around its own cell.
        recorder: Optional replay-event sink; when set, spawns and applied
            movements (wire-driven or control-plane-driven alike) are
            recorded. Teleports have no v1 event encoding, so a recorded
            session containing one diverges permanently from the teleport
            tick onward when replayed.
        native_apply: Run the movement path in C. Default off —
            the Python path is byte-identical to its unwired behavior.

    Returns:
        The live SpatialHandlers instance.
    """
    native: SpatialNative | None = None
    if native_apply:
        from examples.spatial.native import SpatialNative

        native = SpatialNative(
            sim=server._borrowed_sim(),
            model=model.handle,
            fabric=server._borrowed_fabric(),
            zone_id=zone_id,
            cell_size=cell_size,
            cell_radius=cell_radius,
            max_actors=8192,
        )
        native.register_movement_handler(server._borrowed_gateway(), messages.ACTOR_INPUT_TYPE)
    handlers = SpatialHandlers(
        server=server,
        model=model,
        zone_id=zone_id,
        cell_size=cell_size,
        cell_radius=cell_radius,
        recorder=recorder,
        native=native,
    )
    server.register_message_handler(messages.LOGIN_TYPE, handlers.on_login)
    server.on_session_destroyed(handlers.on_session_destroyed)
    if native is None:
        server.register_message_handler(messages.ACTOR_INPUT_TYPE, handlers.on_actor_input)
    server.register_message_handler(messages.CHAT_TYPE, handlers.on_chat)
    return handlers
