"""Unit tests for the visual example's wire catalog and world geometry."""

from __future__ import annotations

import struct

import pytest

from kith import SimInput
from kith.examples.visual import _protocol as proto
from kith.examples.visual import _world


def test_catalog_ids_unique_and_user_ranged() -> None:
    ids = [type_id for _name, type_id in proto.TYPES]
    assert len(ids) == len(set(ids))
    assert min(ids) >= 1000
    assert dict(proto.TYPES)["ping"] == proto.PING_TYPE
    assert dict(proto.TYPES)["pong"] == proto.PONG_TYPE


def test_login_round_trip() -> None:
    payload = proto.encode_login(12345)
    assert len(payload) == proto.LOGIN_PAYLOAD_LEN
    assert proto.decode_login(payload) == 12345
    with pytest.raises(ValueError, match="too short"):
        proto.decode_login(payload[:4])


def test_login_reply_round_trip() -> None:
    assert proto.decode_login_reply(proto.encode_login_reply(41)) == 41
    with pytest.raises(ValueError, match="too short"):
        proto.decode_login_reply(b"\x01")


def test_actor_input_round_trip() -> None:
    inp = SimInput(input_tick=7, move_x=32_767, move_y=-100, move_z=3, flags=_world.RUN_FLAG)
    payload = proto.encode_actor_input(9001, inp)
    assert len(payload) == proto.ACTOR_INPUT_PAYLOAD_LEN
    actor_id, decoded = proto.decode_actor_input(payload)
    assert actor_id == 9001
    assert decoded == inp
    with pytest.raises(ValueError, match="too short"):
        proto.decode_actor_input(payload[:8])


def test_chat_round_trip() -> None:
    payload = proto.encode_chat(41, "hello plain")
    actor_id, text = proto.decode_chat(payload)
    assert (actor_id, text) == (41, "hello plain")
    with pytest.raises(ValueError, match="too long"):
        proto.encode_chat(41, "x" * (proto.CHAT_MAX_TEXT + 1))
    with pytest.raises(ValueError, match="too short"):
        proto.decode_chat(payload[:4])
    with pytest.raises(ValueError, match="truncated"):
        proto.decode_chat(payload[:-1])


def test_actor_state_decode() -> None:
    payload = struct.pack(
        ">QqqqqqqIII",
        41,
        2 * _world.Q16,
        3 * _world.Q16,
        0,
        100,
        -100,
        0,
        55,
        12,
        1,
    )
    view = proto.decode_actor_state(payload)
    assert view.actor_id == 41
    assert view.pos_x == 2 * _world.Q16
    assert view.pos_y == 3 * _world.Q16
    assert view.input_tick == 55
    assert view.update_seq == 12
    assert view.product_level == 1
    assert not proto.is_membership_record(view)
    with pytest.raises(ValueError, match="too short"):
        proto.decode_actor_state(payload[:32])


def test_actor_state_batch_decode() -> None:
    def rec(actor_id: int) -> bytes:
        return struct.pack(">QqqqqqqIII", actor_id, 0, 0, 0, 0, 0, 0, 0, 0, 0)

    payload = struct.pack(">HH", 2, 0) + rec(41) + rec(42)
    views = proto.decode_actor_state_batch(payload)
    assert [v.actor_id for v in views] == [41, 42]
    # A truncated trailing record is ignored, matching a cut-off frame.
    views = proto.decode_actor_state_batch(struct.pack(">HH", 2, 0) + rec(41) + rec(42)[:40])
    assert [v.actor_id for v in views] == [41]
    with pytest.raises(ValueError, match="too short"):
        proto.decode_actor_state_batch(b"\x00")


def test_membership_marker_distinguishes_events() -> None:
    state = proto.ActorStateView(
        actor_id=1, pos_x=0, pos_y=0, pos_z=0, vel_x=0, vel_y=0, vel_z=0, input_tick=0
    )
    event = proto.ActorStateView(
        actor_id=1,
        pos_x=0,
        pos_y=0,
        pos_z=0,
        vel_x=0,
        vel_y=0,
        vel_z=0,
        input_tick=0,
        product_level=proto.MEMBERSHIP_MARKER | proto.MEMBERSHIP_EVENT_ENTER,
    )
    assert not proto.is_membership_record(state)
    assert proto.is_membership_record(event)
    assert event.product_level & ~proto.MEMBERSHIP_MARKER == proto.MEMBERSHIP_EVENT_ENTER


def test_world_geometry_is_self_consistent() -> None:
    assert _world.CELL_SIZE_Q16 == _world.CELL_SIZE_UNITS * _world.Q16
    assert _world.CELL_SIZE_Q16 == 1 << _world.CELL_SHIFT
    assert _world.WORLD_CELLS * _world.CELL_SIZE_UNITS == _world.WORLD_UNITS
    assert _world.q16_to_units(_world.units_to_q16(123.5)) == pytest.approx(123.5)
