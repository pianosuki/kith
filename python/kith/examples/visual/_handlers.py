"""The visual example's inbound wire handlers plus their shared world
state.

One instance per server: the login/input/chat/ping handlers, the
per-tick publish choreography (step, publish, window diff, dirty-cell
flush), the ambient wander, and the spawn/teleport/query surface the
control routes drive. The composition root in ``server.py`` builds this
set and registers its handlers on the facade.
"""

from __future__ import annotations

import contextlib
import math
import random
import sys
import threading
from dataclasses import replace
from typing import Any

from kith import (
    Actor,
    ArtifactKey,
    CellKey,
    KithNetworkError,
    KithStateError,
    SimInput,
)
from kith.examples.visual import _world
from kith.examples.visual._protocol import (
    ACTOR_INPUT_PAYLOAD_LEN,
    CHAT_EVENT_TYPE,
    LOGIN_PAYLOAD_LEN,
    LOGIN_REPLY_TYPE,
    PONG_TYPE,
    decode_actor_input,
    decode_chat,
    decode_login,
    encode_login_reply,
)


# Ambient actors wander at their assigned pace, arriving at a target
# within ~6 units before picking the next one.
_ARRIVE_Q16 = 6 * _world.Q16
_AMBIENT_STAND_CHANCE = 0.15
_AMBIENT_STAND_TICKS = 40
_AMBIENT_MARGIN_Q16 = 16 * _world.Q16

# The primary principal spawns at the plain's center; ambient principals
# keep the 10000+ range so their actor ids allocate before any session
# principal's (the _ensure_npcs gate makes that order structural).
PRIMARY_PRINCIPAL = 1
_AMBIENT_PRINCIPAL_FLOOR = 10_000
_SPAWN_RING_MIN_UNITS = 112.0
_SPAWN_RING_STEP_UNITS = 7.0

# Stripe-lock count for the per-actor apply sequence. Actor ids map to
# stripes by modulo; 64 stripes spread concurrent applies uniformly, so
# same-stripe collision among in-flight applies is rare while same-actor
# applies still serialize under their stripe.
_STRIPE_COUNT = 64


class PlaygroundHandlers:
    """The inbound wire handlers plus their shared world state.

    One instance per server. The bound methods are registered through
    ``register_message_handler``, so every handler takes the gateway
    trampoline's three positional arguments ``(msg_type, payload, session)``
    even when it ignores some of them.

    Locking: per-actor apply sequences run under per-stripe locks (actor
    id modulo ``_STRIPE_COUNT``) so same-actor applies serialize while
    different actors proceed in parallel. A narrow bookkeeping lock guards
    the cross-actor dirty-cell set, the per-cell authority epochs, and the
    identity-gate counter; a control lock guards the low-rate lifecycle
    state (principal map, actor-id allocator, bind records, the session
    map the tick-time window diff walks). Nesting order is control ->
    stripe -> bookkeeping, with no exceptions.
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
        "_dt_ms",
        "_gate_drops",
        "_model",
        "_next_actor_id",
        "_npc_count",
        "_npc_ids",
        "_npc_pace",
        "_npc_rest",
        "_npc_targets",
        "_principals",
        "_rng",
        "_server",
        "_session_actors",
        "_sessions",
        "_spawn_gate",
        "_spawned",
        "_stripes",
        "_tick_index",
        "_world_q16",
        "_zone_id",
    )

    def __init__(
        self,
        *,
        server: Any,
        model: Any,
        zone_id: int,
        cell_size: int,
        cell_radius: int,
        world_q16: int,
        npc_count: int,
        dt_ms: int,
        seed: int,
    ) -> None:
        self._server = server
        self._model = model
        self._zone_id = zone_id
        self._cell_size = cell_size
        self._cell_radius = cell_radius
        self._world_q16 = world_q16
        self._npc_count = npc_count
        self._dt_ms = dt_ms
        self._rng = random.Random(seed)
        self._actors: dict[int, Actor] = {}
        # Each actor's current published cell, so a move across a cell
        # boundary marks the old cell dirty too (the sim supersedes an
        # artifact only within a cell, not across cells).
        self._actor_cells: dict[int, tuple[int, int, int]] = {}
        self._principals: dict[int, int] = {}
        self._next_actor_id = 1
        # Session-ownership records for the one-account-one-view policy:
        # session id -> actor id and its reverse. A second, different
        # session binding an already-referenced actor is permitted (the
        # framework allows shared subscribers) but counted and logged, so
        # a reconnect to a live duplicate shows up on the control plane
        # instead of passing unseen.
        self._session_actors: dict[int, int] = {}
        self._actor_binds: dict[int, set[int]] = {}
        self._bind_conflicts = 0
        # The tick-time window diff's walk list: session id -> (session,
        # bound actor id), rebuilt by login and reclaimed on destroy.
        self._sessions: dict[int, tuple[Any, int]] = {}
        # Movement inputs dropped by the identity gate (the payload names
        # an actor other than the session's bound actor). The gate is
        # silent on the wire by design; this counter is what makes its
        # engagement visible instead of inferable.
        self._gate_drops = 0
        # Monotonic authority epoch per cell; the fabric rejects a stale
        # epoch, so the handler tracks the current value and increments it
        # on every bump.
        self._cell_epochs: dict[CellKey, int] = {}
        # The per-tick dirty-cell set: handlers mark cells dirty here and
        # the tick's flush drains it with one publish_cell_product per
        # cell, coalescing per-input bumps into a per-tick cadence.
        self._dirty_cells: set[CellKey] = set()
        self._stripes: tuple[threading.Lock, ...] = tuple(
            threading.Lock() for _ in range(_STRIPE_COUNT)
        )
        self._book_lock = threading.Lock()
        self._control_lock = threading.Lock()
        self._npc_ids: list[int] = []
        self._npc_targets: dict[int, tuple[int, int]] = {}
        self._npc_rest: dict[int, int] = {}
        self._npc_pace: dict[int, int] = {}
        self._spawned = False
        self._spawn_gate = threading.Lock()
        self._tick_index = 0

    # ---------------------------------------------------------------------------
    # wire handlers
    # ---------------------------------------------------------------------------

    def on_login(self, msg_type: int, payload: bytes, session: Any) -> None:
        """Bind the session to one actor per principal, seed its window,
        and send the discrete login_reply with the bound actor id.

        The populate is one atomic step — bind plus window seed — so the
        view composer resolves the subscriber with no bound-but-windowless
        intermediate. The login reply is a discrete frame (the replication
        stream carries no identity), and the session lands in the tick
        diff's walk list so subsequent cell crossings re-center its window.
        """
        del msg_type
        if len(payload) < LOGIN_PAYLOAD_LEN:
            return
        principal_id = decode_login(payload)
        self._ensure_npcs()
        actor = self.spawn_actor(principal_id)
        self._record_bind(session, actor.id)
        session.populate(actor.id, self._neighborhood(self._cell_of(actor)))
        self._sessions[session.info().session_id] = (session, actor.id)
        session.send(LOGIN_REPLY_TYPE, encode_login_reply(actor.id))

    def on_actor_input(self, msg_type: int, payload: bytes, session: Any) -> None:
        """Buffer a movement input into the model's pending map.

        No step and no publish here: the tick handler owns both, so
        movement speed is independent of the input send rate and the tick
        ladder's axis is the movement cadence itself. The identity gate
        counts a frame whose actor is not the session's bound subscriber;
        the buffered input still applies (the shared actor moves) — the
        gate is observability, not rejection.
        """
        del msg_type
        if len(payload) < ACTOR_INPUT_PAYLOAD_LEN:
            return
        actor_id, inp = decode_actor_input(payload)
        with self._stripe_of(actor_id):
            stored = self._actors.get(actor_id)
            if stored is None:
                return
            applied = self._model.apply_input(stored, inp)
            self._actors[actor_id] = applied
        try:
            bound = session.info().actor_id == actor_id
        except Exception:
            return
        if not bound:
            with self._book_lock:
                self._gate_drops += 1

    def on_chat(self, msg_type: int, payload: bytes, session: Any) -> None:
        """Broadcast the text to the sender's cell and mark the cell dirty.

        The recipient set is the gateway's (every session whose window
        covers the sender's cell), not a roster this handler builds. The
        event payload is the ``chat`` frame's own bytes, so both peers
        decode the S2C event with the same codec as the C2S submit. A
        saturated broadcast queue drops the line: the submit refusal is
        counted by the gateway and surfaces on the metrics route, so the
        handler swallows the transport-family error and keeps the worker
        alive. Frames for an unknown actor or a malformed payload are
        dropped silently so one bad frame does not abort the worker.
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
            self._server.broadcast_cell(self._cell_key(cell), CHAT_EVENT_TYPE, payload)
        self._mark_dirty(cell)

    def on_ping(self, msg_type: int, payload: bytes, session: Any) -> None:
        """Echo a ping payload back as a pong (the C engine times RTT by
        matching the echoed big-endian send timestamp)."""
        del msg_type
        session.send(PONG_TYPE, payload)

    def on_session_destroyed(self, info: Any) -> None:
        """Reclaim the session's bind records and tick-diff walk entry.

        The gateway fires this when it destroys a session, so a churn
        cycle releases the seat instead of growing the maps. A session
        that never logged in pops nothing. The lock matches
        ``_record_bind``'s: both run as pool-dispatched callbacks against
        the same shared pool.
        """
        with self._control_lock:
            self._sessions.pop(info.session_id, None)
            actor_id = self._session_actors.pop(info.session_id, None)
            if actor_id is None:
                return
            binds = self._actor_binds.get(actor_id)
            if binds is not None:
                binds.discard(info.session_id)
                if not binds:
                    del self._actor_binds[actor_id]

    # ---------------------------------------------------------------------------
    # tick handler (the single registered tick slot)
    # ---------------------------------------------------------------------------

    def on_tick(self, tick: int) -> None:
        """Step the world one tick and run the publish choreography."""
        self._tick_index = tick
        self._ensure_npcs()
        self._python_tick()
        self.flush_dirty_cells(tick)

    def _ensure_npcs(self) -> None:
        """Spawn the ambient actors exactly once, before any login can
        allocate: the login handler calls this too, so ambient actor ids
        always land below session actor ids no matter which worker wins
        the race (the client's ambient/player coloring reads that order).
        """
        with self._spawn_gate:
            if self._spawned:
                return
            self._spawned = True
            self._spawn_npcs()

    def _python_tick(self) -> None:
        """The choreography-law tick: steer, step all, publish, diff, flush.

        The control lock plus every stripe are held for the whole tick, so
        the stepped snapshot is race-free against concurrent spawns and
        per-input buffers; the flush drains after the stripes release.
        """
        dt_ms = self._dt_ms
        with self._control_lock, contextlib.ExitStack() as stack:
            for stripe in self._stripes:
                stack.enter_context(stripe)
            for actor_id in self._npc_ids:
                stored = self._actors.get(actor_id)
                if stored is None:
                    continue
                inp = self._npc_input(actor_id, stored)
                applied = self._model.apply_input(stored, inp)
                self._actors[actor_id] = applied
            pre = self._actors
            stepped = self._model.step(list(pre.values()), dt_ms=dt_ms)
            current: dict[int, Actor] = {}
            for actor in stepped:
                actor, moved = self._clamped(actor)
                current[actor.id] = actor
                previous = pre.get(actor.id)
                if previous is not None and not moved and self._unchanged(actor, previous):
                    continue
                old_cell = self._actor_cells.get(actor.id)
                self._publish_locked(actor)
                new_cell = self._cell_of(actor)
                if old_cell is not None and old_cell != new_cell:
                    self._diff_bound_sessions(actor, old_cell)
            self._actors = current

    def _diff_bound_sessions(self, actor: Actor, old_cell: tuple[int, int, int]) -> None:
        """Diff the subscription window of every session bound to a
        crossing actor (the tick-time counterpart of an input-time diff —
        movement happens at tick here, so the crossing is discovered
        there)."""
        for _session_id, (session, bound_actor) in list(self._sessions.items()):
            if bound_actor != actor.id:
                continue
            with contextlib.suppress(Exception):
                self._diff_window(session, actor, old_cell)

    def _unchanged(self, actor: Actor, previous: Actor) -> bool:
        """Whether one step left position and velocity untouched."""
        return (
            actor.pos_x == previous.pos_x
            and actor.pos_y == previous.pos_y
            and actor.vel_x == previous.vel_x
            and actor.vel_y == previous.vel_y
        )

    def _clamped(self, actor: Actor) -> tuple[Actor, bool]:
        """Clamp an actor into the plain, zeroing the velocity component
        that pointed out (a wall stop, not a bounce)."""
        world = self._world_q16
        x = min(max(actor.pos_x, 0), world - 1)
        y = min(max(actor.pos_y, 0), world - 1)
        if x == actor.pos_x and y == actor.pos_y:
            return actor, False
        vel_x = 0 if x != actor.pos_x else actor.vel_x
        vel_y = 0 if y != actor.pos_y else actor.vel_y
        clamped = replace(actor, pos_x=x, pos_y=y, vel_x=vel_x, vel_y=vel_y)
        return clamped, True

    # ---------------------------------------------------------------------------
    # ambient actors
    # ---------------------------------------------------------------------------

    def _spawn_npcs(self) -> None:
        """Scatter the ambient actors across the plain on the first tick.

        The spawn runs on the tick worker (after the planes are live) so
        the publish path sees fully-constructed handles.
        """
        for i in range(1, self._npc_count + 1):
            actor = self.spawn_actor(_AMBIENT_PRINCIPAL_FLOOR + i)
            self._npc_ids.append(actor.id)
            x = self._rng.randint(_AMBIENT_MARGIN_Q16, self._world_q16 - _AMBIENT_MARGIN_Q16)
            y = self._rng.randint(_AMBIENT_MARGIN_Q16, self._world_q16 - _AMBIENT_MARGIN_Q16)
            self.teleport_actor(actor.id, x, y)
            self._npc_pace[actor.id] = _world.RUN_FLAG if self._rng.random() < 0.2 else 0

    def _npc_input(self, actor_id: int, actor: Actor) -> SimInput:
        """Return the wander input for one ambient actor: steer toward the
        current target, rest a few ticks on arrival, then pick a new one."""
        target = self._npc_targets.get(actor_id)
        dx = dy = 0
        if target is not None:
            dx = target[0] - actor.pos_x
            dy = target[1] - actor.pos_y
        arrived = target is None or (dx * dx + dy * dy) < _ARRIVE_Q16 * _ARRIVE_Q16
        if arrived:
            resting = self._npc_rest.get(actor_id, 0)
            if resting > 0:
                self._npc_rest[actor_id] = resting - 1
                return SimInput(input_tick=self._tick_index, move_x=0, move_y=0)
            if target is not None and self._rng.random() < _AMBIENT_STAND_CHANCE:
                self._npc_rest[actor_id] = _AMBIENT_STAND_TICKS
                return SimInput(input_tick=self._tick_index, move_x=0, move_y=0)
            margin = _AMBIENT_MARGIN_Q16
            self._npc_targets[actor_id] = (
                self._rng.randint(margin, self._world_q16 - margin),
                self._rng.randint(margin, self._world_q16 - margin),
            )
            target = self._npc_targets[actor_id]
            dx = target[0] - actor.pos_x
            dy = target[1] - actor.pos_y
        move_x, move_y = self._direction(dx, dy)
        return SimInput(
            input_tick=self._tick_index,
            move_x=move_x,
            move_y=move_y,
            flags=self._npc_pace.get(actor_id, 0),
        )

    @staticmethod
    def _direction(dx: int, dy: int) -> tuple[int, int]:
        """Normalize a Q16 offset into i16 move components (max component
        at full stick)."""
        mag = max(abs(dx), abs(dy))
        if mag == 0:
            return 0, 0
        return (dx * 32_767) // mag, (dy * 32_767) // mag

    # ---------------------------------------------------------------------------
    # spawn, teleport, query (one definition per mutation)
    # ---------------------------------------------------------------------------

    def spawn_actor(self, principal_id: int) -> Actor:
        """Return the actor for ``principal_id``, allocating on first sight.

        One actor per principal. A session principal spawns at the plain's
        center (the primary principal) or on a golden-angle ring 112-140
        units out — just beyond the 96-unit view window, a ~1.6-2.3 s
        approach at the run pace, so scenario cohorts arrive from beyond
        the horizon. The spawn publishes at the origin and the teleport
        republishes at the ring position under the fresh actor's stripe,
        both inside the control lock, so a login's window seed always
        reads the final position; an ambient principal publishes at the
        origin and is scattered by the first tick's spawner.
        """
        with self._control_lock:
            actor_id = self._principals.get(principal_id)
            if actor_id is not None:
                return self._actors[actor_id]
            actor_id = self._next_actor_id
            self._next_actor_id += 1
            fresh = Actor(id=actor_id, pos_x=0, pos_y=0, pos_z=0)
            self._principals[principal_id] = actor_id
            with self._stripe_of(actor_id):
                self._actors[actor_id] = fresh
                self._publish_locked(fresh)
            if principal_id >= _AMBIENT_PRINCIPAL_FLOOR:
                return fresh
            if principal_id == PRIMARY_PRINCIPAL:
                xu = yu = _world.WORLD_UNITS / 2.0
            else:
                angle = principal_id * 2.39996
                radius = _SPAWN_RING_MIN_UNITS + (principal_id % 5) * _SPAWN_RING_STEP_UNITS
                xu = _world.WORLD_UNITS / 2.0 + radius * math.cos(angle)
                yu = _world.WORLD_UNITS / 2.0 + radius * math.sin(angle)
            moved = self.teleport_actor(actor_id, _world.units_to_q16(xu), _world.units_to_q16(yu))
            return fresh if moved is None else moved

    def teleport_actor(self, actor_id: int, pos_x: int, pos_y: int) -> Actor | None:
        """Set an actor's absolute position and publish the updated state.

        Zeroes velocity and preserves the actor's input tick, flags, and
        update_seq (a teleport is not a movement apply: the counter
        certifies movement coverage). The store and the publish run under
        the actor's stripe lock hold, so a concurrent movement input on
        the same actor cannot interleave a stale position between them.
        Returns the updated actor, or ``None`` when no actor with that id
        is bound.
        """
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
                update_seq=actor.update_seq,
            )
            self._actors[actor_id] = updated
            self._publish_locked(updated)
        return updated

    def actor_states(self) -> list[Actor]:
        """Return a snapshot of every bound actor, in no defined order.

        The snapshot is quiesced: the control lock excludes concurrent
        spawns and every stripe is held for the duration, so no per-actor
        apply can replace a value mid-iteration. Control-plane routes use
        this path; it never runs on the apply hot path.
        """
        with self._control_lock, contextlib.ExitStack() as stack:
            for stripe in self._stripes:
                stack.enter_context(stripe)
            return list(self._actors.values())

    def binding_stats(self) -> tuple[list[tuple[int, int]], int, int]:
        """Return the principal->actor bindings plus the ownership counters.

        Returns ``(bindings, bind_conflicts, gate_drops)`` where
        ``bindings`` lists ``(principal_id, actor_id)`` pairs in
        allocation order. One control-lock-held snapshot with the
        bookkeeping lock nested inside, so the route never interleaves a
        torn map with the counters.
        """
        with self._control_lock, self._book_lock:
            return (list(self._principals.items()), self._bind_conflicts, self._gate_drops)

    # ---------------------------------------------------------------------------
    # helpers
    # ---------------------------------------------------------------------------

    def _record_bind(self, session: Any, actor_id: int) -> None:
        """Record a login's session->actor binding and flag shared references.

        A repeated login of the same principal on the same session rebinds
        silently (the recorded state already matches). A different session
        referencing an actor this map already tracks is permitted — the C
        bind still happens — but counted as a bind conflict with one
        stderr line.
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
                    f"visual: session {session_id} bound actor {actor_id} while "
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

    def _stripe_of(self, actor_id: int) -> threading.Lock:
        """Return the stripe lock that owns ``actor_id``'s apply sequence."""
        return self._stripes[actor_id % len(self._stripes)]

    def _actor_of(self, actor_id: int) -> Actor | None:
        """Look up one actor under its apply stripe."""
        with self._stripe_of(actor_id):
            return self._actors.get(actor_id)

    def _cell_of(self, actor: Actor) -> tuple[int, int, int]:
        """Derive an actor's cell from its position by floor-division per axis."""
        size = self._cell_size
        return (actor.pos_x // size, actor.pos_y // size, actor.pos_z // size)

    def _cell_key(self, cell: tuple[int, int, int]) -> CellKey:
        """Return the lod-0 cell key for ``cell`` in the example's zone."""
        return CellKey(zone=self._zone_id, cell_x=cell[0], cell_y=cell[1], cell_z=cell[2], lod=0)

    def _publish_locked(self, actor: Actor) -> None:
        """Publish an actor into its position-derived cell; the caller holds
        the actor's stripe.

        The publish alone moves the actor between cells: the sim store
        holds one artifact per actor and relocates it atomically when the
        position-derived cell changed, so the artifact is never absent
        from the store mid-move. After the sim publish, both the new cell
        (the actor arrived) and the old cell (the actor left, so its
        stale artifact must drop from the cache) are marked dirty; the
        per-tick flush bumps each dirtied cell once, not once per input.
        """
        new_cell = self._cell_of(actor)
        old_cell = self._actor_cells.get(actor.id)
        self._server.publish_artifact(
            ArtifactKey(
                zone=self._zone_id,
                cell_x=new_cell[0],
                cell_y=new_cell[1],
                cell_z=new_cell[2],
                lod=0,
            ),
            actor,
        )
        self._actor_cells[actor.id] = new_cell
        with self._book_lock:
            self._dirty_cells.add(self._cell_key(new_cell))
            if old_cell is not None and old_cell != new_cell:
                self._dirty_cells.add(self._cell_key(old_cell))

    def _neighborhood(self, cell: tuple[int, int, int]) -> list[CellKey]:
        """Return the Chebyshev neighborhood of ``cell`` at the configured radius."""
        r = self._cell_radius
        return [
            self._cell_key((cell[0] + dx, cell[1] + dy, cell[2] + dz))
            for dx in range(-r, r + 1)
            for dy in range(-r, r + 1)
            for dz in range(-r, r + 1)
        ]

    def _diff_window(self, session: Any, actor: Actor, old_cell: tuple[int, int, int]) -> None:
        """Diff the session's subscription window against the old neighborhood.

        Cells that left the window are removed, cells that entered are
        added; a radius-1 crossing changes at most three of nine cells, so
        the diff issues at most six fabric-subscription operations instead
        of a clear plus nine adds, and the cells that did not move keep
        their cache refcounts. The initial seed is ``Session.populate``'s
        job — one atomic step at login instead of a clear-and-rebuild
        here.
        """
        old_set = set(self._neighborhood(old_cell))
        new_set = set(self._neighborhood(self._cell_of(actor)))
        for key in old_set - new_set:
            session.window_remove(key)
        for key in new_set - old_set:
            session.window_add(key)

    def _mark_dirty(self, cell: tuple[int, int, int]) -> None:
        """Add a cell to the per-tick dirty set for the flush to bump."""
        with self._book_lock:
            self._dirty_cells.add(self._cell_key(cell))

    def flush_dirty_cells(self, tick: int) -> None:
        """Drain the dirty-cell set with one cell-product bump per cell.

        Registered as the server's tick callback so per-input and
        per-step dirty marks coalesce into one bump per cell per tick.
        The set is snapshotted and cleared under the bookkeeping lock so
        workers keep marking while the flush publishes; a cell re-dirtied
        during the flush is bumped on the next tick. The epoch
        read-modify-write is guarded by the same lock; the fabric publish
        runs outside it. A stale-epoch publish is swallowed: the
        higher-epoch publish already marked the cell pending and the next
        cache refresh still pulls the latest sim state.

        Args:
            tick: The server's monotonic tick counter. The flush is
                cadence-anchored to the tick but does not read the value.
        """
        del tick
        with self._book_lock:
            cells = list(self._dirty_cells)
            self._dirty_cells.clear()
        for key in cells:
            with self._book_lock:
                epoch = self._cell_epochs.get(key, 0) + 1
                self._cell_epochs[key] = epoch
            with contextlib.suppress(KithStateError):
                self._server.publish_cell_product(key, epoch)
