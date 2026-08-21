"""Unit tests for the spatial client's ``actor_state`` payload codec.

The codec mirrors the gateway's delivery record layout (the 68-byte
subject record defined in ``src/gateway/delivery/delivery.h``): these
tests pin the field order, the width, and the membership-event marker
word so a layout drift fails here before it reaches a live client.
"""

from __future__ import annotations

import struct

import pytest
from examples.spatial import messages as spatial_messages
from examples.spatial.client import (
    ACTOR_STATE_PAYLOAD_SIZE,
    MEMBERSHIP_EVENT_MARKER,
    ActorStateView,
    decode_actor_state,
    decode_actor_state_batch,
    encode_actor_state,
    is_membership_record,
    iter_actor_state_ids,
)


def _view(**overrides: int) -> ActorStateView:
    """A fully-specified state record with per-field overrides."""
    fields: dict[str, int] = {
        "actor_id": 1,
        "pos_x": 0,
        "pos_y": 0,
        "pos_z": 0,
        "vel_x": 0,
        "vel_y": 0,
        "vel_z": 0,
        "input_tick": 0,
        "update_seq": 0,
        "product_level": 0,
    }
    fields.update(overrides)
    return ActorStateView(**fields)


class TestActorStateCodec:
    def test_payload_is_sixty_eight_bytes(self) -> None:
        assert ACTOR_STATE_PAYLOAD_SIZE == 68
        assert len(encode_actor_state(_view(update_seq=4))) == 68

    def test_round_trip_carries_update_seq(self) -> None:
        view = _view(
            actor_id=9,
            pos_x=1 << 16,
            pos_y=-(2 << 16),
            pos_z=3 << 16,
            vel_x=-4,
            vel_y=5,
            vel_z=-6,
            input_tick=77,
            update_seq=4242,
            product_level=1,
        )
        assert decode_actor_state(encode_actor_state(view)) == view

    def test_field_order_matches_the_wire_layout(self) -> None:
        # input_tick at 56-59, update_seq at 60-63, product_level at
        # 64-67: distinct values prove no field reads a neighbor's word.
        payload = encode_actor_state(_view(input_tick=11, update_seq=22, product_level=33))
        assert struct.unpack_from(">I", payload, 56)[0] == 11
        assert struct.unpack_from(">I", payload, 60)[0] == 22
        assert struct.unpack_from(">I", payload, 64)[0] == 33

    def test_trailing_bytes_are_ignored(self) -> None:
        payload = encode_actor_state(_view(update_seq=5)) + b"\x00" * 4
        assert decode_actor_state(payload).update_seq == 5

    def test_short_payload_raises(self) -> None:
        with pytest.raises(ValueError):
            decode_actor_state(b"\x00" * (ACTOR_STATE_PAYLOAD_SIZE - 1))

    def test_membership_marker_reads_the_product_level_word(self) -> None:
        # The marker word sits at bytes 64-67: a record
        # whose update_seq word carries marker-shaped bits is still state.
        assert not is_membership_record(
            decode_actor_state(encode_actor_state(_view(update_seq=MEMBERSHIP_EVENT_MARKER)))
        )
        assert is_membership_record(
            decode_actor_state(encode_actor_state(_view(product_level=MEMBERSHIP_EVENT_MARKER | 1)))
        )


class TestActorStateBatchCodec:
    def test_batch_records_carry_update_seq(self) -> None:
        views = [
            _view(actor_id=1, input_tick=5, update_seq=10),
            _view(actor_id=2, input_tick=6, update_seq=20),
        ]
        payload = struct.pack(">HH", len(views), 0) + b"".join(encode_actor_state(v) for v in views)
        decoded = decode_actor_state_batch(payload)
        assert [v.update_seq for v in decoded] == [10, 20]
        assert [v.actor_id for v in decoded] == [1, 2]

    def test_truncated_batch_record_is_dropped(self) -> None:
        first = encode_actor_state(_view(update_seq=3))
        payload = struct.pack(">HH", 2, 0) + first + first[: ACTOR_STATE_PAYLOAD_SIZE - 1]
        decoded = decode_actor_state_batch(payload)
        assert len(decoded) == 1
        assert decoded[0].update_seq == 3


class TestActorStateIdExtractor:
    """``iter_actor_state_ids`` reads exactly the filtered decoder's ids.

    The load driver's ingest path extracts ids with this function while
    its breadth window is open, so a drift from the full decoder — a
    field offset, a membership rule, a truncation rule — would silently
    skew the breadth reading. The equivalence below is the guard.
    """

    @staticmethod
    def _batch(views: list[ActorStateView]) -> bytes:
        return struct.pack(">HH", len(views), 0) + b"".join(encode_actor_state(v) for v in views)

    @staticmethod
    def _filtered_ids(payload: bytes) -> set[int]:
        return {
            v.actor_id for v in decode_actor_state_batch(payload) if not is_membership_record(v)
        }

    def test_matches_the_filtered_decoder_including_membership(self) -> None:
        # Mixed stream: state records interleaved with enter/exit/crowd
        # membership events whose actor_id fields name subjects the
        # breadth set must never admit.
        views = [
            _view(actor_id=7, product_level=1),
            _view(actor_id=0, product_level=MEMBERSHIP_EVENT_MARKER),
            _view(actor_id=9, product_level=0),
            _view(actor_id=12, product_level=MEMBERSHIP_EVENT_MARKER | 1),
            _view(actor_id=99, product_level=MEMBERSHIP_EVENT_MARKER | 2),
            _view(actor_id=11, product_level=3),
        ]
        payload = self._batch(views)
        assert set(iter_actor_state_ids(spatial_messages.ACTOR_STATE_BATCH_TYPE, payload)) == {
            7,
            9,
            11,
        }
        assert set(
            iter_actor_state_ids(spatial_messages.ACTOR_STATE_BATCH_TYPE, payload)
        ) == self._filtered_ids(payload)

    def test_single_form_yields_the_one_state_id(self) -> None:
        payload = encode_actor_state(_view(actor_id=42, product_level=2))
        assert list(iter_actor_state_ids(spatial_messages.ACTOR_STATE_TYPE, payload)) == [42]
        membership = encode_actor_state(_view(product_level=MEMBERSHIP_EVENT_MARKER))
        assert list(iter_actor_state_ids(spatial_messages.ACTOR_STATE_TYPE, membership)) == []

    def test_unknown_type_yields_nothing(self) -> None:
        payload = encode_actor_state(_view(actor_id=1))
        assert list(iter_actor_state_ids(0, payload)) == []

    def test_truncation_matches_the_decoder(self) -> None:
        first = encode_actor_state(_view(actor_id=1))
        payload = struct.pack(">HH", 2, 0) + first + first[: ACTOR_STATE_PAYLOAD_SIZE - 1]
        assert list(iter_actor_state_ids(spatial_messages.ACTOR_STATE_BATCH_TYPE, payload)) == [1]
        assert len(self._filtered_ids(payload)) == 1

    def test_short_payload_yields_nothing(self) -> None:
        assert (
            list(iter_actor_state_ids(spatial_messages.ACTOR_STATE_BATCH_TYPE, b"\x00\x01")) == []
        )
        assert list(iter_actor_state_ids(spatial_messages.ACTOR_STATE_TYPE, b"\x00" * 67)) == []
