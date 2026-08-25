"""Tests for the lobby example wire message handlers.

The pure-Python cases cover the payload codecs (round-trip and bounds), the
lobby state mutations (members, rooms, chat), and the handler logic against
lightweight fakes of the session and presence-sink surfaces, with no
framework libraries loaded. The build-gated cases register the five C2S
handlers on a real :class:`kith.Server` facade and confirm the gateway's
handler table accepts every type.
"""

from __future__ import annotations

import struct
from collections.abc import Iterator
from dataclasses import dataclass

import pytest
from _build_gate import _BUILD_DEBUG, needs_build
from examples.lobby import handlers, messages

from kith import Server, ServerStatus
from kith._bridge import reset


@pytest.fixture(autouse=True)
def _isolate_bridge(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
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

    def test_room_join_round_trips(self) -> None:
        assert handlers.decode_room_join(handlers.encode_room_join("town")) == "town"

    def test_room_leave_round_trips(self) -> None:
        assert handlers.decode_room_leave(handlers.encode_room_leave("town")) == "town"

    def test_room_join_empty_name_round_trips(self) -> None:
        assert handlers.decode_room_join(handlers.encode_room_join("")) == ""

    def test_room_join_truncated_raises(self) -> None:
        with pytest.raises(ValueError):
            handlers.decode_room_join(b"")

    def test_room_join_name_too_long_raises(self) -> None:
        with pytest.raises(ValueError):
            handlers.encode_room_join("x" * (handlers.ROOM_MAX_NAME + 1))

    def test_chat_room_round_trips(self) -> None:
        room, text = handlers.decode_chat_room(handlers.encode_chat_room("town", "hello"))
        assert room == "town"
        assert text == "hello"

    def test_chat_room_empty_text_round_trips(self) -> None:
        room, text = handlers.decode_chat_room(handlers.encode_chat_room("town", ""))
        assert room == "town"
        assert text == ""

    def test_chat_room_unicode_round_trips(self) -> None:
        room, text = handlers.decode_chat_room(handlers.encode_chat_room("町", "近く"))
        assert room == "町"
        assert text == "近く"

    def test_chat_room_truncated_header_raises(self) -> None:
        with pytest.raises(ValueError):
            handlers.decode_chat_room(b"")

    def test_chat_room_text_too_long_to_encode_raises(self) -> None:
        with pytest.raises(ValueError):
            handlers.encode_chat_room("town", "x" * (handlers.CHAT_MAX_TEXT + 1))

    def test_chat_room_text_length_out_of_range_raises(self) -> None:
        bad = struct.pack("<B", 0) + struct.pack("<H", handlers.CHAT_MAX_TEXT + 1)
        with pytest.raises(ValueError):
            handlers.decode_chat_room(bad)

    def test_presence_update_round_trips(self) -> None:
        mid, status = handlers.decode_presence_update(handlers.encode_presence_update(42, 2))
        assert mid == 42
        assert status == 2

    def test_presence_update_truncated_raises(self) -> None:
        with pytest.raises(ValueError):
            handlers.decode_presence_update(b"\x00\x00")

    def test_presence_round_trips(self) -> None:
        payload = handlers.encode_presence(
            kind=handlers.KIND_CHAT,
            member_id=7,
            display_name="alice",
            status=1,
            text="hi",
        )
        event = handlers.decode_presence(payload)
        assert event.kind == handlers.KIND_CHAT
        assert event.member_id == 7
        assert event.display_name == "alice"
        assert event.status == 1
        assert event.text == "hi"

    def test_presence_empty_text_round_trips(self) -> None:
        payload = handlers.encode_presence(
            kind=handlers.KIND_JOIN,
            member_id=1,
            display_name="bob",
            status=0,
        )
        event = handlers.decode_presence(payload)
        assert event.text == ""

    def test_presence_truncated_raises(self) -> None:
        with pytest.raises(ValueError):
            handlers.decode_presence(b"\x00\x00")


# ---------------------------------------------------------------------------
# lobby state
# ---------------------------------------------------------------------------


class TestLobbyState:
    def test_get_or_create_member_assigns_monotonic_ids(self) -> None:
        state = handlers.LobbyState()
        a = state.get_or_create_member(100)
        b = state.get_or_create_member(200)
        c = state.get_or_create_member(100)
        assert a.member_id == 1
        assert b.member_id == 2
        assert c.member_id == 1  # returning principal rebinds

    def test_join_and_leave_room(self) -> None:
        state = handlers.LobbyState()
        state.get_or_create_member(1)
        state.get_or_create_member(2)
        assert state.join_room("town", 1)
        assert state.join_room("town", 2)
        assert not state.join_room("town", 1)  # already in
        assert state.room_members("town") == [1, 2]
        assert state.leave_room("town", 1)
        assert state.room_members("town") == [2]
        # Leaving a room with no members removes it.
        assert state.leave_room("town", 2)
        assert state.room_names() == []

    def test_leave_room_not_in_room_returns_false(self) -> None:
        state = handlers.LobbyState()
        assert not state.leave_room("town", 1)

    def test_record_and_query_chat_log(self) -> None:
        state = handlers.LobbyState()
        state.record_chat("town", 1, "hi")
        state.record_chat("town", 2, "yo")
        state.record_chat("dungeon", 1, "hello")
        assert len(state.chat_log("town")) == 2
        assert len(state.chat_log("dungeon")) == 1
        assert len(state.chat_log()) == 3

    def test_set_status_changes_member_status(self) -> None:
        state = handlers.LobbyState()
        member = state.get_or_create_member(1)
        assert member.status == 0
        assert state.set_status(member.member_id, 1)
        updated = state.member(member.member_id)
        assert updated is not None
        assert updated.status == 1
        # Setting the same status is a no-op.
        assert not state.set_status(member.member_id, 1)


# ---------------------------------------------------------------------------
# handler logic (no build; fakes for the session and sink surfaces)
# ---------------------------------------------------------------------------


@dataclass
class _FakeSessionInfo:
    actor_id: int


class _FakeSession:
    """A fake session with a settable bound actor id (read via ``info``)."""

    def __init__(self, actor_id: int = 0) -> None:
        self._info = _FakeSessionInfo(actor_id=actor_id)

    @property
    def info(self) -> _FakeSessionInfo:
        return self._info

    def bind_actor(self, actor_id: int) -> None:
        self._info = _FakeSessionInfo(actor_id=actor_id)


class _FakeSink:
    """Records deliver calls for test assertions."""

    def __init__(self) -> None:
        self.deliveries: list[tuple[tuple[int, ...], bytes]] = []

    def deliver(self, session_ids: list[int], payload: bytes) -> None:
        self.deliveries.append((tuple(session_ids), payload))


def _make_handlers() -> tuple[handlers.LobbyHandlers, handlers.LobbyState, _FakeSink]:
    state = handlers.LobbyState()
    sink = _FakeSink()
    h = handlers.LobbyHandlers(state=state, sink=sink)
    return h, state, sink


def _login(h: handlers.LobbyHandlers, principal_id: int) -> _FakeSession:
    session = _FakeSession()
    h.on_login(messages.LOGIN_TYPE, handlers.encode_login(principal_id), session)
    return session


class TestHandlerLogic:
    def test_login_binds_session_to_member(self) -> None:
        h, _state, _sink = _make_handlers()
        session = _login(h, 100)
        assert session.info.actor_id == 1

    def test_login_truncated_payload_is_dropped(self) -> None:
        h, state, _sink = _make_handlers()
        session = _FakeSession()
        h.on_login(messages.LOGIN_TYPE, b"\x00", session)
        assert session.info.actor_id == 0
        assert state.members() == []

    def test_login_reuses_member_for_known_principal(self) -> None:
        h, _state, _sink = _make_handlers()
        first = _login(h, 100)
        second = _login(h, 100)
        assert first.info.actor_id == second.info.actor_id

    def test_room_join_adds_member_and_notifies(self) -> None:
        h, state, sink = _make_handlers()
        session = _login(h, 100)
        h.on_room_join(messages.ROOM_JOIN_TYPE, handlers.encode_room_join("town"), session)
        assert state.room_members("town") == [1]
        assert len(sink.deliveries) == 1
        ids, payload = sink.deliveries[0]
        assert ids == (1,)
        event = handlers.decode_presence(payload)
        assert event.kind == handlers.KIND_JOIN
        assert event.member_id == 1

    def test_room_join_without_login_is_dropped(self) -> None:
        h, state, sink = _make_handlers()
        session = _FakeSession()  # actor_id 0: not logged in
        h.on_room_join(messages.ROOM_JOIN_TYPE, handlers.encode_room_join("town"), session)
        assert state.room_names() == []
        assert sink.deliveries == []

    def test_room_join_malformed_payload_is_dropped(self) -> None:
        h, state, _sink = _make_handlers()
        session = _login(h, 100)
        h.on_room_join(messages.ROOM_JOIN_TYPE, b"\xff\xff", session)
        assert state.room_names() == []

    def test_room_join_duplicate_is_dropped(self) -> None:
        h, _state, sink = _make_handlers()
        session = _login(h, 100)
        h.on_room_join(messages.ROOM_JOIN_TYPE, handlers.encode_room_join("town"), session)
        h.on_room_join(messages.ROOM_JOIN_TYPE, handlers.encode_room_join("town"), session)
        # Second join is a no-op: no new notification.
        assert len(sink.deliveries) == 1

    def test_room_leave_removes_member_and_notifies(self) -> None:
        h, state, sink = _make_handlers()
        session = _login(h, 100)
        h.on_room_join(messages.ROOM_JOIN_TYPE, handlers.encode_room_join("town"), session)
        h.on_room_leave(messages.ROOM_LEAVE_TYPE, handlers.encode_room_leave("town"), session)
        assert state.room_names() == []
        assert len(sink.deliveries) == 2
        event = handlers.decode_presence(sink.deliveries[1][1])
        assert event.kind == handlers.KIND_LEAVE

    def test_room_leave_not_in_room_is_dropped(self) -> None:
        h, _state, sink = _make_handlers()
        session = _login(h, 100)
        h.on_room_leave(messages.ROOM_LEAVE_TYPE, handlers.encode_room_leave("town"), session)
        assert sink.deliveries == []

    def test_chat_room_records_and_broadcasts(self) -> None:
        h, state, sink = _make_handlers()
        s1 = _login(h, 100)
        s2 = _login(h, 200)
        h.on_room_join(messages.ROOM_JOIN_TYPE, handlers.encode_room_join("town"), s1)
        h.on_room_join(messages.ROOM_JOIN_TYPE, handlers.encode_room_join("town"), s2)
        sink.deliveries.clear()
        h.on_chat_room(
            messages.CHAT_ROOM_TYPE,
            handlers.encode_chat_room("town", "hello"),
            s1,
        )
        assert len(state.chat_log("town")) == 1
        assert len(sink.deliveries) == 1
        ids, payload = sink.deliveries[0]
        assert ids == (1, 2)
        event = handlers.decode_presence(payload)
        assert event.kind == handlers.KIND_CHAT
        assert event.text == "hello"

    def test_chat_room_from_non_member_is_dropped(self) -> None:
        h, state, sink = _make_handlers()
        s1 = _login(h, 100)
        s2 = _login(h, 200)  # not in the room
        h.on_room_join(messages.ROOM_JOIN_TYPE, handlers.encode_room_join("town"), s1)
        sink.deliveries.clear()
        h.on_chat_room(
            messages.CHAT_ROOM_TYPE,
            handlers.encode_chat_room("town", "hi"),
            s2,
        )
        assert state.chat_log("town") == []
        assert sink.deliveries == []

    def test_chat_room_malformed_payload_is_dropped(self) -> None:
        h, state, _sink = _make_handlers()
        s1 = _login(h, 100)
        h.on_room_join(messages.ROOM_JOIN_TYPE, handlers.encode_room_join("town"), s1)
        h.on_chat_room(messages.CHAT_ROOM_TYPE, b"\x00", s1)
        assert state.chat_log("town") == []

    def test_presence_update_changes_status_and_notifies_rooms(self) -> None:
        h, state, sink = _make_handlers()
        s1 = _login(h, 100)
        h.on_room_join(messages.ROOM_JOIN_TYPE, handlers.encode_room_join("town"), s1)
        sink.deliveries.clear()
        h.on_presence_update(
            messages.PRESENCE_UPDATE_TYPE,
            handlers.encode_presence_update(1, 2),
            _FakeSession(),
        )
        member_after = state.member(1)
        assert member_after is not None
        assert member_after.status == 2
        assert len(sink.deliveries) == 1
        event = handlers.decode_presence(sink.deliveries[0][1])
        assert event.kind == handlers.KIND_STATUS
        assert event.status == 2

    def test_presence_update_unknown_member_is_dropped(self) -> None:
        h, _state, sink = _make_handlers()
        h.on_presence_update(
            messages.PRESENCE_UPDATE_TYPE,
            handlers.encode_presence_update(999, 1),
            _FakeSession(),
        )
        assert sink.deliveries == []


# ---------------------------------------------------------------------------
# registration on the real facade (build-gated)
# ---------------------------------------------------------------------------


@needs_build
class TestRegistration:
    _TABLE_SIZE: int = 4096

    def test_register_registers_five_c2s_handlers(self) -> None:
        server = Server(
            topology="embedded",
            handler_table_size=self._TABLE_SIZE,
            replication_type_id=0,
        )
        try:
            messages.register(server)
            state = handlers.LobbyState()
            sink = handlers.OutboxSink()
            h = handlers.register(server, state=state, sink=sink)
            assert server.status is ServerStatus.CREATED
            assert isinstance(h, handlers.LobbyHandlers)
        finally:
            server.close()

    def test_register_replaces_prior_handlers_without_error(self) -> None:
        server = Server(
            topology="embedded",
            handler_table_size=self._TABLE_SIZE,
            replication_type_id=0,
        )
        try:
            messages.register(server)
            state = handlers.LobbyState()
            sink = handlers.OutboxSink()
            handlers.register(server, state=state, sink=sink)
            handlers.register(server, state=state, sink=sink)
            assert server.status is ServerStatus.CREATED
        finally:
            server.close()
