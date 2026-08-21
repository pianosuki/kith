"""Tests for the spatial wire message handlers.

The pure-Python cases cover the payload codecs (round-trip and bounds) and
the handler logic against lightweight fakes of the publish, model, and
session surfaces, with no framework libraries loaded. The build-gated cases
register the three C2S handlers on a real :class:`kith.Server` facade and
confirm the gateway's handler table accepts every type.
"""

from __future__ import annotations

import struct
import threading
from collections.abc import Iterator, Sequence

import pytest
from _build_gate import _BUILD_DEBUG, needs_build
from examples.spatial import handlers, messages

from kith import Actor, ArtifactKey, CellKey, Server, ServerStatus, SimInput, SimModelConfig
from kith._bridge import reset


@pytest.fixture(autouse=True)
def _isolate_bridge(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Pin KITH_LIB to the debug build dir and reset the singleton per test."""
    monkeypatch.setenv("KITH_LIB", str(_BUILD_DEBUG))
    reset()
    yield
    reset()


# ---------------------------------------------------------------------------
# payload codecs
# ---------------------------------------------------------------------------


class TestPayloadCodecs:
    def test_login_round_trips(self) -> None:
        assert handlers.decode_login(handlers.encode_login(0)) == 0
        assert handlers.decode_login(handlers.encode_login(1 << 48)) == 1 << 48

    def test_login_truncated_raises(self) -> None:
        with pytest.raises(ValueError):
            handlers.decode_login(b"\x00\x00")

    def test_actor_input_round_trips(self) -> None:
        inp = SimInput(input_tick=7, move_x=32767, move_y=-32768, move_z=0, flags=1)
        actor_id, out = handlers.decode_actor_input(handlers.encode_actor_input(42, inp))
        assert actor_id == 42
        assert out == inp

    def test_actor_input_payload_length_is_nineteen(self) -> None:
        inp = SimInput()
        assert len(handlers.encode_actor_input(1, inp)) == handlers.ACTOR_INPUT_PAYLOAD_LEN
        assert handlers.ACTOR_INPUT_PAYLOAD_LEN == 19

    def test_actor_input_truncated_raises(self) -> None:
        with pytest.raises(ValueError):
            handlers.decode_actor_input(b"\x00" * 18)

    def test_chat_round_trips(self) -> None:
        actor_id, text = handlers.decode_chat(handlers.encode_chat(5, "hello"))
        assert actor_id == 5
        assert text == "hello"

    def test_chat_empty_text_round_trips(self) -> None:
        actor_id, text = handlers.decode_chat(handlers.encode_chat(5, ""))
        assert actor_id == 5
        assert text == ""

    def test_chat_unicode_round_trips(self) -> None:
        actor_id, text = handlers.decode_chat(handlers.encode_chat(9, "近く"))
        assert actor_id == 9
        assert text == "近く"

    def test_chat_truncated_header_raises(self) -> None:
        with pytest.raises(ValueError):
            handlers.decode_chat(b"\x00\x00")

    def test_chat_truncated_text_raises(self) -> None:
        # Header claims 5 bytes of text but the payload carries none.
        bad = struct.pack("<QH", 1, 5)
        with pytest.raises(ValueError):
            handlers.decode_chat(bad)

    def test_chat_text_length_out_of_range_raises(self) -> None:
        bad = struct.pack("<QH", 1, handlers.CHAT_MAX_TEXT + 1)
        with pytest.raises(ValueError):
            handlers.decode_chat(bad)

    def test_chat_text_too_long_to_encode_raises(self) -> None:
        with pytest.raises(ValueError):
            handlers.encode_chat(1, "x" * (handlers.CHAT_MAX_TEXT + 1))


# ---------------------------------------------------------------------------
# handler logic (no build; fakes for the publish / model / session surfaces)
# ---------------------------------------------------------------------------


class _FakeModel:
    """Records apply_input calls and advances the actor's position by move_x."""

    def __init__(self) -> None:
        self.calls: list[tuple[int, SimInput]] = []

    def apply_input(self, actor: Actor, inp: SimInput) -> Actor:
        self.calls.append((actor.id, inp))
        return Actor(
            id=actor.id,
            pos_x=actor.pos_x + inp.move_x,
            pos_y=actor.pos_y + inp.move_y,
            pos_z=actor.pos_z,
            vel_x=actor.vel_x,
            vel_y=actor.vel_y,
            vel_z=actor.vel_z,
            input_tick=inp.input_tick,
            flags=inp.flags,
            # The real models mint once per applied movement input; the
            # fake mirrors that so the handler tests see the counter a
            # live publish carries.
            update_seq=actor.update_seq + 1,
        )

    def step(self, actors: list[Actor], dt_ms: int) -> list[Actor]:
        # The fake's apply_input already integrates position; step is identity
        # so the handler's apply_movement (apply_input + step + publish) sees
        # the same final state the real model produces after one integration.
        del dt_ms
        return list(actors)


class _FakeServer:
    """Records sim-artifact publishes, fabric cell-product publishes,
    removals, and cell-scoped broadcast submissions."""

    def __init__(self) -> None:
        self.artifacts: list[tuple[ArtifactKey, Actor]] = []
        self.cells: list[tuple[CellKey, int]] = []
        self.removed: list[int] = []
        self.broadcasts: list[tuple[CellKey, int, bytes]] = []

    def publish_artifact(self, key: ArtifactKey, actor: Actor) -> int:
        self.artifacts.append((key, actor))
        return len(self.artifacts)

    def remove_artifact(self, actor_id: int) -> None:
        self.removed.append(actor_id)

    def publish_cell_product(self, key: CellKey, authority_epoch: int) -> int:
        self.cells.append((key, authority_epoch))
        return len(self.cells)

    def broadcast_cell(self, key: CellKey, msg_type: int, payload: bytes) -> None:
        self.broadcasts.append((key, msg_type, payload))


class _FakeSessionInfo:
    __slots__ = ("actor_id", "session_id")

    def __init__(self, actor_id: int | None, session_id: int) -> None:
        self.actor_id = actor_id if actor_id is not None else 0
        self.session_id = session_id


_SESSION_SEQ = iter(range(1, 1_000_000))


class _FakeSession:
    def __init__(self) -> None:
        self.bound: int | None = None
        self.session_id = next(_SESSION_SEQ)
        # The current window as a set (the real facade's effective state) plus
        # ordered add/remove logs so a window-diff test can assert exactly
        # which cells enter and leave on a crossing.
        self.window: set[CellKey] = set()
        self.added: list[CellKey] = []
        self.removed: list[CellKey] = []

    def populate(self, actor_id: int, cells: Sequence[CellKey]) -> None:
        self.bound = actor_id
        # The real populate seeds additively: each cell joins through the
        # idempotent add path.
        for key in cells:
            self.window_add(key)

    def bind_actor(self, actor_id: int) -> None:
        self.bound = actor_id

    def info(self) -> _FakeSessionInfo:
        return _FakeSessionInfo(self.bound, self.session_id)

    def window_clear(self) -> None:
        self.window.clear()
        self.added.clear()
        self.removed.clear()

    def window_add(self, key: CellKey) -> None:
        if key not in self.window:
            self.window.add(key)
            self.added.append(key)

    def window_remove(self, key: CellKey) -> None:
        if key in self.window:
            self.window.discard(key)
            self.removed.append(key)


def _make_handlers() -> tuple[handlers.SpatialHandlers, _FakeServer, _FakeModel]:
    server = _FakeServer()
    model = _FakeModel()
    h = handlers.SpatialHandlers(server=server, model=model, zone_id=1)
    return h, server, model


class TestHandlerLogic:
    def test_login_binds_session_and_publishes_initial_state(self) -> None:
        h, server, _model = _make_handlers()
        session = _FakeSession()
        h.on_login(messages.LOGIN_TYPE, handlers.encode_login(100), session)
        assert session.bound == 1
        assert len(server.artifacts) == 1
        key, actor = server.artifacts[0]
        assert key.zone == 1
        assert actor.id == 1
        assert actor.pos_x == 0
        # A spawn publish is not a movement apply: the counter stays
        # "never minted" (0).
        assert actor.update_seq == 0

    def test_movement_mints_update_seq_into_the_publish(self) -> None:
        h, server, _model = _make_handlers()
        session = _FakeSession()
        h.on_login(messages.LOGIN_TYPE, handlers.encode_login(100), session)
        h.on_actor_input(
            messages.ACTOR_INPUT_TYPE,
            handlers.encode_actor_input(1, SimInput(input_tick=1, move_x=1000)),
            session,
        )
        _, actor = server.artifacts[-1]
        assert actor.update_seq == 1
        h.on_actor_input(
            messages.ACTOR_INPUT_TYPE,
            handlers.encode_actor_input(1, SimInput(input_tick=2, move_x=1000)),
            session,
        )
        _, actor = server.artifacts[-1]
        assert actor.update_seq == 2

    def test_teleport_publishes_the_counter_unchanged(self) -> None:
        h, server, _model = _make_handlers()
        session = _FakeSession()
        h.on_login(messages.LOGIN_TYPE, handlers.encode_login(100), session)
        h.on_actor_input(
            messages.ACTOR_INPUT_TYPE,
            handlers.encode_actor_input(1, SimInput(input_tick=1, move_x=1000)),
            session,
        )
        _, moved = server.artifacts[-1]
        assert moved.update_seq == 1
        h.teleport_actor(1, 10 << 16, 0)
        _, teleported = server.artifacts[-1]
        assert teleported.pos_x == 10 << 16
        # A teleport mints nothing and regresses nothing: the published
        # counter equals the moved actor's.
        assert teleported.update_seq == moved.update_seq == 1

    def test_login_reuses_actor_for_known_principal(self) -> None:
        h, server, _model = _make_handlers()
        first = _FakeSession()
        second = _FakeSession()
        h.on_login(messages.LOGIN_TYPE, handlers.encode_login(100), first)
        h.on_login(messages.LOGIN_TYPE, handlers.encode_login(100), second)
        # One actor allocated for the principal; both sessions bind to it.
        assert first.bound == 1
        assert second.bound == 1
        # Initial state published once, on the first login only.
        assert len(server.artifacts) == 1
        # A second session holding the same actor is a bind conflict.
        bindings, conflicts, drops = h.binding_stats()
        assert bindings == [(100, 1)]
        assert conflicts == 1
        assert drops == 0

    def test_same_session_relogin_is_not_a_conflict(self) -> None:
        h, _server, _model = _make_handlers()
        session = _FakeSession()
        h.on_login(messages.LOGIN_TYPE, handlers.encode_login(100), session)
        h.on_login(messages.LOGIN_TYPE, handlers.encode_login(100), session)
        assert session.bound == 1
        bindings, conflicts, _drops = h.binding_stats()
        assert bindings == [(100, 1)]
        assert conflicts == 0

    def test_binding_stats_track_principals_in_allocation_order(self) -> None:
        h, _server, _model = _make_handlers()
        h.on_login(messages.LOGIN_TYPE, handlers.encode_login(300), _FakeSession())
        h.on_login(messages.LOGIN_TYPE, handlers.encode_login(101), _FakeSession())
        h.on_login(messages.LOGIN_TYPE, handlers.encode_login(200), _FakeSession())
        bindings, conflicts, drops = h.binding_stats()
        assert bindings == [(300, 1), (101, 2), (200, 3)]
        assert conflicts == 0
        assert drops == 0

    def test_login_distinct_principals_allocate_distinct_actors(self) -> None:
        h, _server, _model = _make_handlers()
        a = _FakeSession()
        b = _FakeSession()
        h.on_login(messages.LOGIN_TYPE, handlers.encode_login(1), a)
        h.on_login(messages.LOGIN_TYPE, handlers.encode_login(2), b)
        assert a.bound == 1
        assert b.bound == 2

    def test_login_defers_cell_bump_until_flush(self) -> None:
        # The gateway cache drains only cells the fabric marked pending, so a
        # login must bump the actor's cell product header or the subscriber
        # never receives its initial view. The handler defers the bump: the
        # publish marks the cell dirty and the per-tick flush bumps it once,
        # coalescing the per-input cadence to a per-tick cadence. No eager
        # publish lands on the handler path.
        h, server, _model = _make_handlers()
        session = _FakeSession()
        h.on_login(messages.LOGIN_TYPE, handlers.encode_login(100), session)
        assert session.bound == 1
        # The publish marks the cell dirty; no cell-product bump yet.
        assert server.cells == []
        # The flush drains the dirty set with one bump per dirty cell.
        h.flush_dirty_cells(1)
        assert len(server.cells) == 1
        key, epoch = server.cells[0]
        assert key.zone == 1
        assert key.cell_x == 0
        assert key.cell_y == 0
        assert epoch == 1
        # A second flush with nothing dirtied bumps nothing.
        h.flush_dirty_cells(2)
        assert len(server.cells) == 1

    def test_login_truncated_payload_is_dropped(self) -> None:
        h, _server, _model = _make_handlers()
        session = _FakeSession()
        h.on_login(messages.LOGIN_TYPE, b"\x00", session)
        assert session.bound is None

    def test_actor_input_applies_and_publishes(self) -> None:
        h, server, model = _make_handlers()
        h.on_login(messages.LOGIN_TYPE, handlers.encode_login(100), _FakeSession())
        h.on_actor_input(
            messages.ACTOR_INPUT_TYPE,
            handlers.encode_actor_input(1, SimInput(input_tick=1, move_x=10, move_y=0)),
            _FakeSession(),
        )
        assert model.calls == [(1, SimInput(input_tick=1, move_x=10))]
        # login published once, actor_input republished the updated state.
        assert len(server.artifacts) == 2
        _, actor = server.artifacts[1]
        assert actor.pos_x == 10
        assert actor.input_tick == 1

    def test_actor_input_unknown_actor_is_dropped(self) -> None:
        h, server, model = _make_handlers()
        h.on_actor_input(
            messages.ACTOR_INPUT_TYPE,
            handlers.encode_actor_input(999, SimInput(input_tick=1, move_x=10)),
            _FakeSession(),
        )
        assert model.calls == []
        assert server.artifacts == []

    def test_actor_input_truncated_payload_is_dropped(self) -> None:
        h, server, _model = _make_handlers()
        h.on_actor_input(messages.ACTOR_INPUT_TYPE, b"\x00" * 5, _FakeSession())
        assert server.artifacts == []

    def test_identity_gate_counts_mismatched_input_drops(self) -> None:
        # A movement frame naming an actor other than the session's bound
        # one is the identity gate's drop case: it stops there instead of
        # diffing windows or publishing as the subscriber. The frame is
        # silent on the wire; the counter makes its engagement visible.
        h, _server, _model = _make_handlers()
        session = _FakeSession()
        h.on_login(messages.LOGIN_TYPE, handlers.encode_login(100), session)
        assert session.bound == 1
        other = _FakeSession()
        other.bound = 2
        # Actor 1 exists only as principal 100's login spawn;
        # the mismatched session (bound 2) must not act as its subscriber.
        h.on_actor_input(
            messages.ACTOR_INPUT_TYPE,
            handlers.encode_actor_input(1, SimInput(input_tick=1, move_x=5)),
            other,
        )
        _bindings, conflicts, drops = h.binding_stats()
        assert drops == 1
        assert conflicts == 0
        # A matching input keeps the counter flat.
        h.on_actor_input(
            messages.ACTOR_INPUT_TYPE,
            handlers.encode_actor_input(1, SimInput(input_tick=2, move_x=5)),
            session,
        )
        assert h.binding_stats()[2] == 1

    def test_actor_input_crossing_diffs_window_not_clear_add(self) -> None:
        # On a cell crossing the window is diffed against the old
        # neighborhood: cells that leave are removed, cells that enter are
        # added, the cells that stay keep their fabric subscriptions. A
        # radius-1 crossing along one axis removes one 3x3x3 slab and adds
        # another, leaving the middle slab untouched, instead of clearing
        # all 27 cells and re-adding them.
        server = _FakeServer()
        model = _FakeModel()
        # A small cell size lets a 16-bit move_x cross a cell boundary.
        h = handlers.SpatialHandlers(server=server, model=model, zone_id=1, cell_size=1000)
        session = _FakeSession()
        h.on_login(messages.LOGIN_TYPE, handlers.encode_login(100), session)
        assert session.bound == 1
        # The login populate seeds the full 3x3x3 neighborhood (27 cells at
        # radius 1) of the origin cell (0, 0, 0) additively.
        assert len(session.added) == 27
        session.added.clear()
        session.removed.clear()
        # Move the bound actor one cell along +x (cell_size is 1000, so a
        # move_x of 1000 crosses exactly one cell).
        h.on_actor_input(
            messages.ACTOR_INPUT_TYPE,
            handlers.encode_actor_input(1, SimInput(input_tick=1, move_x=1000)),
            session,
        )
        # The diff removes the x = -1 slab (9 cells) and adds the x = 2 slab
        # (9 cells); the x in {0, 1} slabs stayed subscribed.
        assert len(session.removed) == 9
        assert len(session.added) == 9
        assert all(k.cell_x == -1 for k in session.removed)
        assert all(k.cell_x == 2 for k in session.added)
        # The window still holds the 27 cells of the new neighborhood.
        assert len(session.window) == 27

    def test_crossing_publish_relocates_without_remove(self) -> None:
        # A cross-cell publish relocates the artifact atomically in the sim
        # store: the handler must not pair it with a remove_artifact call,
        # which leaves the actor absent from the store between the two calls
        # while a concurrent compose can observe it. The fake keeps its
        # remove recorder precisely to prove the call never happens.
        server = _FakeServer()
        model = _FakeModel()
        h = handlers.SpatialHandlers(server=server, model=model, zone_id=1, cell_size=1000)
        session = _FakeSession()
        h.on_login(messages.LOGIN_TYPE, handlers.encode_login(100), session)
        assert server.removed == []
        server.artifacts.clear()
        # Move the bound actor one cell along +x.
        h.on_actor_input(
            messages.ACTOR_INPUT_TYPE,
            handlers.encode_actor_input(1, SimInput(input_tick=1, move_x=1000)),
            session,
        )
        assert server.removed == []
        # One publish landed in the new cell carrying the stepped state.
        assert len(server.artifacts) == 1
        key, actor = server.artifacts[0]
        assert (key.cell_x, key.cell_y, key.cell_z) == (1, 0, 0)
        assert actor.pos_x == 1000
        # Both the arrival cell and the departure cell are dirty for the
        # next flush (the cache must drop the moved artifact from the old
        # cell on its next refresh).
        h.flush_dirty_cells(1)
        bumped = {(cell.cell_x, cell.cell_y, cell.cell_z) for cell, _epoch in server.cells}
        assert {(1, 0, 0), (0, 0, 0)} <= bumped

    def test_concurrent_same_actor_applies_do_not_lose_updates(self) -> None:
        # The movement transaction (read, integrate, store, publish) is one
        # atomic hold of the handlers lock: concurrent workers applying
        # inputs to the same actor serialize, so every input advances the
        # position exactly once and the artifact lands at the final stored
        # position. A lost update regresses the stored position behind the
        # window diff chain, which then stops tracking the actor's own
        # cell and fails every composition's subscriber locate.
        server = _FakeServer()
        model = _FakeModel()
        h = handlers.SpatialHandlers(server=server, model=model, zone_id=1, cell_size=1000)
        session = _FakeSession()
        h.on_login(messages.LOGIN_TYPE, handlers.encode_login(100), session)
        threads = 4
        inputs_per_thread = 50
        total = threads * inputs_per_thread
        barrier = threading.Barrier(threads)

        def drive() -> None:
            barrier.wait()
            for tick_n in range(inputs_per_thread):
                h.on_actor_input(
                    messages.ACTOR_INPUT_TYPE,
                    handlers.encode_actor_input(1, SimInput(input_tick=tick_n, move_x=1)),
                    session,
                )

        workers = [threading.Thread(target=drive) for _ in range(threads)]
        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join()
        assert len(model.calls) == total
        final = h.actor_state(1)
        assert final is not None
        assert final.pos_x == total
        key, actor = server.artifacts[-1]
        assert actor.pos_x == final.pos_x
        assert (key.cell_x, key.cell_y, key.cell_z) == (final.pos_x // 1000, 0, 0)

    def test_concurrent_same_actor_applies_keep_window_tracked(self) -> None:
        # The same serialization at march scale: every input crosses a cell
        # boundary, so the window diff chain retires and adds slabs for the
        # whole corridor while four workers contend on one actor. The
        # monotone transaction keeps the diff chain convergent — the final
        # window still tracks the actor's final cell (the failure mode it
        # guards: a regressed store desynchronizes the diff chain and the
        # window stops covering the actor's own cell entirely).
        server = _FakeServer()
        model = _FakeModel()
        h = handlers.SpatialHandlers(server=server, model=model, zone_id=1, cell_size=1000)
        session = _FakeSession()
        h.on_login(messages.LOGIN_TYPE, handlers.encode_login(100), session)
        threads = 4
        inputs_per_thread = 50
        total = threads * inputs_per_thread
        barrier = threading.Barrier(threads)

        def drive() -> None:
            barrier.wait()
            for tick_n in range(inputs_per_thread):
                h.on_actor_input(
                    messages.ACTOR_INPUT_TYPE,
                    handlers.encode_actor_input(1, SimInput(input_tick=tick_n, move_x=1000)),
                    session,
                )

        workers = [threading.Thread(target=drive) for _ in range(threads)]
        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join()
        final = h.actor_state(1)
        assert final is not None
        assert final.pos_x == total * 1000
        final_cell = (final.pos_x // 1000, 0, 0)
        # The window holds exactly the 27 cells of the actor's final
        # neighborhood — the diff chain tracked the march without drift.
        assert len(session.window) == 27
        assert all(
            abs(k.cell_x - final_cell[0]) <= 1 and abs(k.cell_y) <= 1 and abs(k.cell_z) <= 1
            for k in session.window
        )

    def test_flush_coalesces_repeated_marks_on_one_cell(self) -> None:
        # Many inputs dirty the same cell between flushes; the per-tick
        # flush bumps the cell exactly once per tick (the coalescing the
        # cadence shift relies on), not once per input.
        h, server, _model = _make_handlers()
        session = _FakeSession()
        h.on_login(messages.LOGIN_TYPE, handlers.encode_login(100), session)
        for tick_n in range(1, 9):
            h.on_actor_input(
                messages.ACTOR_INPUT_TYPE,
                handlers.encode_actor_input(1, SimInput(input_tick=tick_n, move_x=1)),
                session,
            )
        # All eight inputs mark the same cell dirty; the handler bumps no
        # cell product. The flush bumps the cell exactly once.
        assert server.cells == []
        h.flush_dirty_cells(1)
        assert len(server.cells) == 1
        assert server.cells[0][1] == 1

    def test_chat_marks_cell_dirty_then_flush_bumps_it(self) -> None:
        h, server, _model = _make_handlers()
        h.on_login(messages.LOGIN_TYPE, handlers.encode_login(100), _FakeSession())
        h.flush_dirty_cells(1)
        baseline = len(server.cells)
        # Chat marks the sender's cell dirty; no eager bump lands. The
        # chat_event broadcast submits on the same call — the handler
        # builds no roster; the broadcast's recipient set is the gateway's.
        h.on_chat(messages.CHAT_TYPE, handlers.encode_chat(1, "hi"), _FakeSession())
        assert len(server.cells) == baseline
        assert server.broadcasts == [
            (
                CellKey(zone=1, cell_x=0, cell_y=0, cell_z=0, lod=0),
                messages.CHAT_EVENT_TYPE,
                handlers.encode_chat(1, "hi"),
            )
        ]
        h.flush_dirty_cells(2)
        assert len(server.cells) == baseline + 1
        key, epoch = server.cells[-1]
        assert key.zone == 1
        assert epoch == baseline + 1
        # A second chat between flushes marks the cell dirty again; the next
        # flush bumps it with the next monotonic epoch, and a second
        # broadcast submits behind the first.
        h.on_chat(messages.CHAT_TYPE, handlers.encode_chat(1, "again"), _FakeSession())
        h.flush_dirty_cells(3)
        assert server.cells[-1][1] == baseline + 2
        assert len(server.broadcasts) == 2

    def test_chat_unknown_actor_is_dropped(self) -> None:
        h, server, _model = _make_handlers()
        h.on_chat(messages.CHAT_TYPE, handlers.encode_chat(999, "hi"), _FakeSession())
        assert server.cells == []
        assert server.broadcasts == []

    def test_chat_malformed_payload_is_dropped(self) -> None:
        h, server, _model = _make_handlers()
        h.on_login(messages.LOGIN_TYPE, handlers.encode_login(100), _FakeSession())
        baseline = len(server.cells)
        h.on_chat(messages.CHAT_TYPE, b"\x00", _FakeSession())
        # A malformed chat payload bumps no cell; the count is unchanged.
        assert len(server.cells) == baseline


# ---------------------------------------------------------------------------
# registration on the real facade (build-gated)
# ---------------------------------------------------------------------------


@needs_build
class TestRegistration:
    _TABLE_SIZE: int = 4096

    def test_register_registers_three_c2s_handlers(self) -> None:
        server = Server(
            topology="embedded",
            handler_table_size=self._TABLE_SIZE,
            replication_type_id=messages.ACTOR_STATE_TYPE,
        )
        try:
            messages.register(server)
            model = server.register_sim_model("tile2d", SimModelConfig())
            zone_id = server.register_zone("world")
            h = handlers.register(server, model=model, zone_id=zone_id)
            assert server.status is ServerStatus.CREATED
            # The handler set owns the live actor table for the server.
            assert isinstance(h, handlers.SpatialHandlers)
        finally:
            server.close()

    def test_register_replaces_prior_handlers_without_error(self) -> None:
        # Registering twice for the same type replaces the prior handler
        # (the gateway table slot is overwritten), so re-wiring is safe.
        server = Server(
            topology="embedded",
            handler_table_size=self._TABLE_SIZE,
            replication_type_id=messages.ACTOR_STATE_TYPE,
        )
        try:
            messages.register(server)
            model = server.register_sim_model("tile2d", SimModelConfig())
            zone_id = server.register_zone("world")
            handlers.register(server, model=model, zone_id=zone_id)
            handlers.register(server, model=model, zone_id=zone_id)
            assert server.status is ServerStatus.CREATED
        finally:
            server.close()
