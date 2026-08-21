"""Tests for the spatial wire message catalog.

The catalog is seven game-owned types at ids at or above
``KITH_PROTO_TYPE_USER_BASE`` (1000): the irreducible surface a spatial game
needs, including the per-subject and batch replication forms. The
pure-Python cases verify the catalog's shape (count, uniqueness, the id
boundary, the named constants matching the ordered registry) without
loading the framework libraries. The build-gated cases register the catalog
on a real proto and a real server and confirm the ids round-trip through the
C registry.
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest
from _build_gate import _BUILD_DEBUG, needs_build
from examples.spatial import messages

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

# The six names in registration order, mirrored from messages.TYPES so a
# drift in the catalog surfaces as a test failure rather than a silent
# reorder.
_EXPECTED: tuple[tuple[str, int], ...] = (
    ("login", _USER_BASE + 0),
    ("login_reply", _USER_BASE + 1),
    ("actor_input", _USER_BASE + 2),
    ("chat", _USER_BASE + 3),
    ("actor_state", _USER_BASE + 4),
    ("actor_state_batch", _USER_BASE + 5),
    ("chat_event", _USER_BASE + 6),
)


class TestCatalogShape:
    def test_seven_types(self) -> None:
        assert len(messages.TYPES) == 7

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

    def test_chat_event_type_is_the_highest_id(self) -> None:
        # The chat_event broadcast type carries the largest id; the cell
        # fanout rides the gateway's broadcast queue as one frame of this
        # type per covering session.
        assert max(tid for _, tid in messages.TYPES) == messages.CHAT_EVENT_TYPE

    def test_named_constants_match_catalog(self) -> None:
        named = {
            "login": messages.LOGIN_TYPE,
            "login_reply": messages.LOGIN_REPLY_TYPE,
            "actor_input": messages.ACTOR_INPUT_TYPE,
            "chat": messages.CHAT_TYPE,
            "actor_state": messages.ACTOR_STATE_TYPE,
            "actor_state_batch": messages.ACTOR_STATE_BATCH_TYPE,
            "chat_event": messages.CHAT_EVENT_TYPE,
        }
        assert named == dict(messages.TYPES)

    def test_user_base_matches_framework_constant(self) -> None:
        assert _USER_BASE == 1000


@needs_build
class TestRegistration:
    def test_register_round_trips_through_proto(self) -> None:
        # The proto registry is the contract both peers share; registering the
        # catalog and looking each name back up confirms the ids survive the C
        # round-trip.
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
            # Re-registering the same name at the same id is a no-op.
            proto.register_type("login", messages.LOGIN_TYPE)
            assert proto.lookup_type("login") == messages.LOGIN_TYPE

    def test_register_on_server_facade(self) -> None:
        # The facade registers the catalog on the server's borrowed proto; a
        # subsequent duplicate-id registration with a different name raises
        # KithStateError, proving every id lands in the registry.
        server = Server(topology="embedded")
        try:
            messages.register(server)
            with pytest.raises(KithStateError) as exc_info:
                server.register_proto_type("not_login", messages.LOGIN_TYPE)
            assert exc_info.value.code == gen_types.kith_error.KITH_EEXIST
        finally:
            server.close()

    def test_register_on_server_facade_is_idempotent(self) -> None:
        server = Server(topology="embedded")
        try:
            messages.register(server)
            # Re-running register on the same server does not raise; the
            # registry accepts the same name at the same id again.
            messages.register(server)
        finally:
            server.close()

    def test_unknown_type_lookup_is_not_found(self) -> None:
        # A name absent from the catalog is absent from the registry.
        with Proto() as proto:
            for name, type_id in messages.TYPES:
                proto.register_type(name, type_id)
            with pytest.raises(KithNotFoundError):
                proto.lookup_type("not_registered")
