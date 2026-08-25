"""Tests for the lobby example wire message catalog.

The catalog is six game-owned types at ids at or above
``KITH_PROTO_TYPE_USER_BASE`` (1000), separate from spatial's five-type
catalog: the two share no names and no ids, so each game owns its wire
surface. The pure-Python cases verify the catalog's shape (count,
uniqueness, the id boundary, the named constants matching the ordered
registry, and that the catalog is disjoint from spatial's) without
loading the framework libraries. The build-gated cases register the
catalog on a real proto and a real server and confirm the ids round-trip
through the C registry.
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest
from _build_gate import _BUILD_DEBUG, needs_build
from examples.lobby import messages
from examples.spatial import messages as spatial_messages

from kith import KithNotFoundError, KithProtocolError, KithStateError, Server
from kith._bridge import reset
from kith._generated import proto as gen_proto
from kith._generated import types as gen_types
from kith.proto import Proto


@pytest.fixture(autouse=True)
def _isolate_bridge(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    monkeypatch.setenv("KITH_LIB", str(_BUILD_DEBUG))
    reset()
    yield
    reset()


_USER_BASE = int(gen_proto.kith_proto_format.KITH_PROTO_TYPE_USER_BASE)
_LOBBY_BASE = _USER_BASE + 100

_EXPECTED: tuple[tuple[str, int], ...] = (
    ("login", _LOBBY_BASE + 0),
    ("presence", _LOBBY_BASE + 1),
    ("room_join", _LOBBY_BASE + 2),
    ("room_leave", _LOBBY_BASE + 3),
    ("chat_room", _LOBBY_BASE + 4),
    ("presence_update", _LOBBY_BASE + 5),
)


class TestCatalogShape:
    def test_six_types(self) -> None:
        assert len(messages.TYPES) == 6

    def test_all_ids_at_or_above_user_base(self) -> None:
        assert all(tid >= _USER_BASE for _, tid in messages.TYPES)

    def test_ids_unique(self) -> None:
        ids = [tid for _, tid in messages.TYPES]
        assert len(ids) == len(set(ids))

    def test_names_unique(self) -> None:
        names = [name for name, _ in messages.TYPES]
        assert len(names) == len(set(names))

    def test_catalog_matches_expected(self) -> None:
        assert messages.TYPES == _EXPECTED

    def test_named_constants_match_catalog(self) -> None:
        named = {
            "login": messages.LOGIN_TYPE,
            "presence": messages.PRESENCE_TYPE,
            "room_join": messages.ROOM_JOIN_TYPE,
            "room_leave": messages.ROOM_LEAVE_TYPE,
            "chat_room": messages.CHAT_ROOM_TYPE,
            "presence_update": messages.PRESENCE_UPDATE_TYPE,
        }
        assert named == dict(messages.TYPES)

    def test_catalog_ids_do_not_overlap_spatial(self) -> None:
        # Each game picks its own id range within the user space; the lobby
        # offsets its ids 100 above the user base so the two catalogs' ids
        # are disjoint. The framework defines no central enum and assigns no
        # ids; the offset is a game choice.
        lobby_ids = {tid for _, tid in messages.TYPES}
        tile_ids = {tid for _, tid in spatial_messages.TYPES}
        assert lobby_ids.isdisjoint(tile_ids)

    def test_user_base_matches_framework_constant(self) -> None:
        assert _USER_BASE == 1000


@needs_build
class TestRegistration:
    def test_register_round_trips_through_proto(self) -> None:
        with Proto() as proto:
            for name, type_id in messages.TYPES:
                proto.register_type(name, type_id)
            for name, type_id in messages.TYPES:
                assert proto.lookup_type(name) == type_id

    def test_duplicate_id_with_new_name_rejected(self) -> None:
        with Proto() as proto:
            proto.register_type("login", messages.LOGIN_TYPE)
            with pytest.raises(KithProtocolError) as exc_info:
                proto.register_type("not_login", messages.LOGIN_TYPE)
            assert exc_info.value.code == gen_types.kith_error.KITH_EEXIST

    def test_same_name_same_id_is_idempotent(self) -> None:
        with Proto() as proto:
            proto.register_type("login", messages.LOGIN_TYPE)
            proto.register_type("login", messages.LOGIN_TYPE)
            assert proto.lookup_type("login") == messages.LOGIN_TYPE

    def test_register_on_server_facade(self) -> None:
        server = Server(topology="embedded", handler_table_size=4096)
        try:
            messages.register(server)
            with pytest.raises(KithStateError) as exc_info:
                server.register_proto_type("not_login", messages.LOGIN_TYPE)
            assert exc_info.value.code == gen_types.kith_error.KITH_EEXIST
        finally:
            server.close()

    def test_register_on_server_facade_is_idempotent(self) -> None:
        server = Server(topology="embedded", handler_table_size=4096)
        try:
            messages.register(server)
            messages.register(server)
        finally:
            server.close()

    def test_unknown_type_lookup_is_not_found(self) -> None:
        with Proto() as proto:
            for name, type_id in messages.TYPES:
                proto.register_type(name, type_id)
            with pytest.raises(KithNotFoundError):
                proto.lookup_type("not_registered")
