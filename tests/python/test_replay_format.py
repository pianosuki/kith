"""Unit tests for the binary replay-record codec.

These exercise the wire contract directly — round-trips, canonical
ordering, strict reader validation, forward-compatible skipping, and the
logical differ — without loading the simulation library.
"""

from __future__ import annotations

import struct
from dataclasses import replace

import pytest
from examples._common import replay_format as rf


def _spawn(actor_id: int = 1, pos_x: int = 0) -> rf.SpawnEvent:
    return rf.SpawnEvent(
        actor_id=actor_id,
        pos_x=pos_x,
        pos_y=0,
        pos_z=0,
        vel_x=0,
        vel_y=0,
        vel_z=0,
        flags=0,
    )


def _move(actor_id: int = 1, input_tick: int = 1, move_x: int = 100) -> rf.MoveEvent:
    return rf.MoveEvent(
        actor_id=actor_id,
        input_tick=input_tick,
        move_x=move_x,
        move_y=0,
        move_z=0,
        flags=0,
    )


def _sample_document() -> rf.ReplayDocument:
    """One document exercising every record type except checkpoints."""
    return rf.ReplayDocument(
        header=rf.ReplayHeader(tick_hz=20, flags=0),
        meta={"model": "tile2d"},
        ticks=(
            rf.TickRecord(0, (_spawn(1), _spawn(2))),
            rf.TickRecord(3, (_move(1),)),
        ),
        expected_hashes=(rf.ExpectedHash(3, 0xDEADBEEF),),
        rng_checkpoints=(),
    )


def _framed(record_type: int, payload: bytes, *, flags: int = 0) -> bytes:
    return struct.pack(">BBHI", record_type, flags, 0, len(payload)) + payload


def _header_bytes(
    *,
    version: int = rf.FORMAT_VERSION,
    header_size: int = rf.HEADER_SIZE,
    tick_hz: int = 20,
    flags: int = 0,
) -> bytes:
    return struct.pack(">4sHHII", rf.MAGIC, version, header_size, tick_hz, flags)


# ---------------------------------------------------------------------------
# round trip
# ---------------------------------------------------------------------------


def test_round_trip_preserves_logical_content() -> None:
    """Encode then read returns an identical document."""
    document = replace(
        _sample_document(),
        rng_checkpoints=(rf.RngCheckpoint(3, 7, bytes(range(rf.RNG_STATE_SIZE))),),
    )
    parsed = rf.read_document(rf.encode_document(document))
    assert parsed == document


def test_writer_orders_events_canonically() -> None:
    """The writer emits events in canonical order regardless of call order."""
    sink = rf._CollectingSink()
    writer = rf.ReplayWriter(sink, tick_hz=20)
    writer.write_tick(0, (_move(2), _spawn(1), _move(1)))
    writer.close()
    parsed = rf.read_document(bytes(sink.buffer))
    kinds = [type(event) for event in parsed.ticks[0].events]
    assert kinds == [rf.SpawnEvent, rf.MoveEvent, rf.MoveEvent]
    assert parsed.ticks[0].events[1].actor_id == 1
    assert parsed.ticks[0].events[2].actor_id == 2


def test_sniff_distinguishes_binary_from_text() -> None:
    """Only the magic marks a stream binary."""
    assert rf.sniff_is_binary(_header_bytes())
    assert not rf.sniff_is_binary(b"0 SPAWN 1 0 0 0 0 0 0 0\n")


# ---------------------------------------------------------------------------
# writer misuse
# ---------------------------------------------------------------------------


def test_writer_rejects_non_ascending_ticks() -> None:
    """Ticks must strictly ascend."""
    writer = rf.ReplayWriter(rf._CollectingSink(), tick_hz=20)
    writer.write_tick(5, ())
    with pytest.raises(ValueError, match="does not advance"):
        writer.write_tick(5, ())
    with pytest.raises(ValueError, match="does not advance"):
        writer.write_tick(4, ())


def test_writer_rejects_meta_after_tick_or_duplicate_meta() -> None:
    """META is legal exactly once and only before the first tick."""
    writer = rf.ReplayWriter(rf._CollectingSink(), tick_hz=20)
    writer.write_meta({"a": 1})
    with pytest.raises(ValueError, match="twice"):
        writer.write_meta({"a": 2})
    writer.write_tick(0, ())
    other = rf.ReplayWriter(rf._CollectingSink(), tick_hz=20)
    other.write_tick(0, ())
    with pytest.raises(ValueError, match="must precede"):
        other.write_meta({"a": 1})


def test_writer_binds_checkpoint_to_last_tick_and_ascending_keys() -> None:
    """Checkpoints follow their own tick with strictly ascending stream keys."""
    writer = rf.ReplayWriter(rf._CollectingSink(), tick_hz=20)
    state = bytes(rf.RNG_STATE_SIZE)
    with pytest.raises(ValueError, match="does not match"):
        writer.write_rng_checkpoint(0, 1, state)
    writer.write_tick(0, ())
    writer.write_rng_checkpoint(0, 5, state)
    with pytest.raises(ValueError, match="ascend"):
        writer.write_rng_checkpoint(0, 5, state)
    with pytest.raises(ValueError, match="ascend"):
        writer.write_rng_checkpoint(0, 4, state)
    writer.write_tick(1, ())
    with pytest.raises(ValueError, match="does not match"):
        writer.write_rng_checkpoint(0, 9, state)


def test_writer_rejects_wrong_state_size_and_bad_tick_hz() -> None:
    """Construction validates tick rate; checkpoints validate state length."""
    with pytest.raises(ValueError, match="tick_hz"):
        rf.ReplayWriter(rf._CollectingSink(), tick_hz=0)
    writer = rf.ReplayWriter(rf._CollectingSink(), tick_hz=20)
    writer.write_tick(0, ())
    with pytest.raises(ValueError, match="40 bytes"):
        writer.write_rng_checkpoint(0, 1, b"short")


def test_writer_rejects_use_after_close() -> None:
    """A closed writer refuses further records."""
    writer = rf.ReplayWriter(rf._CollectingSink(), tick_hz=20)
    writer.close()
    with pytest.raises(ValueError, match="closed"):
        writer.write_tick(0, ())


def test_encode_document_requires_checkpoints_to_match_a_tick() -> None:
    """Checkpoints bound to absent ticks are a caller error, not a skip."""
    document = replace(
        _sample_document(),
        rng_checkpoints=(rf.RngCheckpoint(99, 1, bytes(rf.RNG_STATE_SIZE)),),
    )
    with pytest.raises(ValueError, match="absent from the document"):
        rf.encode_document(document)


# ---------------------------------------------------------------------------
# reader strictness
# ---------------------------------------------------------------------------


def test_reader_rejects_bad_magic_short_file_and_future_version() -> None:
    """Header violations fail loudly with source context."""
    with pytest.raises(rf.ReplayFormatError, match="magic"):
        rf.read_document(b"NOPE" + bytes(12))
    with pytest.raises(rf.ReplayFormatError, match="shorter than"):
        rf.read_document(b"KRPL")
    future = _header_bytes(version=rf.FORMAT_VERSION + 1)
    with pytest.raises(rf.ReplayFormatError, match="version 2"):
        rf.read_document(future)


def test_reader_rejects_unknown_header_flags_and_small_header() -> None:
    """No v1 header flag semantics exist; undersized headers are refused."""
    with pytest.raises(rf.ReplayFormatError, match="flag bits"):
        rf.read_document(_header_bytes(flags=1 << 31))
    with pytest.raises(rf.ReplayFormatError, match="below the required"):
        rf.read_document(_header_bytes(header_size=8))


def test_reader_parses_extended_header_forward_compatibility() -> None:
    """A header larger than v1's is honored by skipping the extension."""
    body = _framed(rf.TICK_RECORD_TYPE, struct.pack(">QH", 0, 0))
    data = _header_bytes(header_size=24) + bytes(8) + body
    parsed = rf.read_document(data)
    assert parsed.ticks == (rf.TickRecord(0, ()),)


def test_reader_skips_unknown_record_types() -> None:
    """Unknown types consume their payload whole and do not disturb parsing."""
    document = _sample_document()
    encoded = rf.encode_document(document)
    unknown = _framed(0x55, b"\x01\x02\x03\x04\x05")
    insertion_point = rf.HEADER_SIZE
    patched = encoded[:insertion_point] + unknown + encoded[insertion_point:]
    parsed = rf.read_document(patched)
    assert parsed == document


def test_reader_rejects_unknown_flags_on_known_records() -> None:
    """Known record types carry no v1 flag semantics."""
    tick_payload = struct.pack(">QH", 0, 0)
    data = _header_bytes() + _framed(rf.TICK_RECORD_TYPE, tick_payload, flags=1)
    with pytest.raises(rf.ReplayFormatError, match="flag bits"):
        rf.read_document(data)


def test_reader_rejects_truncation_and_trailing_payload_bytes() -> None:
    """Cut records and unconsumed bytes are corruption, not tolerance."""
    encoded = rf.encode_document(_sample_document())
    with pytest.raises(rf.ReplayFormatError, match="truncated record framing"):
        rf.read_document(encoded + b"\x01\x02")
    first_record_len_offset = rf.HEADER_SIZE + 4
    inflated = bytearray(encoded[: rf.HEADER_SIZE + rf.RECORD_HEADER_SIZE])
    struct.pack_into(">I", inflated, first_record_len_offset, len(encoded))
    with pytest.raises(rf.ReplayFormatError, match="exceeds end of file"):
        rf.read_document(bytes(inflated))
    tick_with_extra = _framed(rf.TICK_RECORD_TYPE, struct.pack(">QH", 0, 0) + b"\x00")
    with pytest.raises(rf.ReplayFormatError, match="expected 0 payload bytes"):
        rf.read_document(_header_bytes() + tick_with_extra)


def test_reader_enforces_tick_ordering_and_meta_position() -> None:
    """Ticks must ascend; META must be unique and precede ticks."""
    two_descending = _framed(rf.TICK_RECORD_TYPE, struct.pack(">QH", 4, 0)) + _framed(
        rf.TICK_RECORD_TYPE, struct.pack(">QH", 0, 0)
    )
    with pytest.raises(rf.ReplayFormatError, match="ascend"):
        rf.read_document(_header_bytes() + two_descending)
    meta_after_tick = _framed(rf.TICK_RECORD_TYPE, struct.pack(">QH", 0, 0)) + _framed(
        rf.META_RECORD_TYPE, b"{}"
    )
    with pytest.raises(rf.ReplayFormatError, match="before the first tick"):
        rf.read_document(_header_bytes() + meta_after_tick)
    duplicate_meta = _framed(rf.META_RECORD_TYPE, b"{}") * 2
    with pytest.raises(rf.ReplayFormatError, match="exactly once"):
        rf.read_document(_header_bytes() + duplicate_meta)


def test_reader_enforces_checkpoint_ordering_law() -> None:
    """Checkpoints bind to the preceding tick with ascending stream keys."""
    state = bytes(rf.RNG_STATE_SIZE)
    orphan = _framed(rf.RNG_CHECKPOINT_RECORD_TYPE, struct.pack(">QQI", 9, 1, 40) + state)
    with pytest.raises(rf.ReplayFormatError, match="follow its tick record"):
        rf.read_document(_header_bytes() + orphan)
    tick_zero = _framed(rf.TICK_RECORD_TYPE, struct.pack(">QH", 0, 0))
    descending = _framed(rf.RNG_CHECKPOINT_RECORD_TYPE, struct.pack(">QQI", 0, 5, 40) + state)
    descending += _framed(rf.RNG_CHECKPOINT_RECORD_TYPE, struct.pack(">QQI", 0, 5, 40) + state)
    with pytest.raises(rf.ReplayFormatError, match="ascend"):
        rf.read_document(_header_bytes() + tick_zero + descending)
    wrong_state_len = _framed(rf.RNG_CHECKPOINT_RECORD_TYPE, struct.pack(">QQI", 0, 1, 39) + state)
    with pytest.raises(rf.ReplayFormatError, match="state length"):
        rf.read_document(_header_bytes() + tick_zero + wrong_state_len)


def test_reader_rejects_unknown_event_code_and_overlong_count() -> None:
    """Unknown event codes cannot be skipped; counts must fit the payload."""
    bad_event = _framed(rf.TICK_RECORD_TYPE, struct.pack(">QH", 0, 1) + b"\x7f" + bytes(8))
    with pytest.raises(rf.ReplayFormatError, match="unknown event code 0x7f"):
        rf.read_document(_header_bytes() + bad_event)
    overcount = _framed(rf.TICK_RECORD_TYPE, struct.pack(">QH", 0, 9))
    with pytest.raises(rf.ReplayFormatError, match="exceeds the payload"):
        rf.read_document(_header_bytes() + overcount)


def test_reader_rejects_non_canonical_event_order() -> None:
    """A tick whose stored events descend canonically is a writer defect."""
    move_first = struct.pack(">B", rf.MOVE_EVENT_CODE) + struct.pack(">QIhhhB", 1, 1, 1, 0, 0, 0)
    spawn_second = struct.pack(">B", rf.SPAWN_EVENT_CODE) + struct.pack(">QqqqqqqI", 1, *([0] * 7))
    payload = struct.pack(">QH", 0, 2) + move_first + spawn_second
    data = _header_bytes() + _framed(rf.TICK_RECORD_TYPE, payload)
    with pytest.raises(rf.ReplayFormatError, match="canonical order"):
        rf.read_document(data)


def test_reader_rejects_duplicate_expectations_and_bad_meta_json() -> None:
    """Expectation ticks are unique; META must be a JSON object."""
    expectation = _framed(rf.EXPECTED_HASH_RECORD_TYPE, struct.pack(">QQ", 1, 2))
    duplicated = _header_bytes() + expectation + expectation
    with pytest.raises(rf.ReplayFormatError, match="duplicate expectation"):
        rf.read_document(duplicated)
    array_meta = _framed(rf.META_RECORD_TYPE, b"[1]")
    with pytest.raises(rf.ReplayFormatError, match="JSON object"):
        rf.read_document(_header_bytes() + array_meta)


# ---------------------------------------------------------------------------
# differ
# ---------------------------------------------------------------------------


def test_diff_of_identical_documents_is_empty() -> None:
    """Equal logical content yields no differences."""
    document = _sample_document()
    parsed = rf.read_document(rf.encode_document(document))
    assert rf.diff_documents(parsed, document) == []


def test_diff_reports_each_difference_class() -> None:
    """Header, meta, per-tick, missing-tick, expectation, checkpoint diffs."""
    base = _sample_document()
    header_diff = replace(base, header=rf.ReplayHeader(tick_hz=30, flags=0))
    meta_diff = replace(base, meta={"model": "free2d"})
    tick_events_diff = replace(base, ticks=(base.ticks[0], rf.TickRecord(3, (_move(2),))))
    tick_missing_diff = replace(base, ticks=(base.ticks[0],))
    extra_tick_diff = replace(base, ticks=(base.ticks[0], base.ticks[1], rf.TickRecord(4, ())))
    expectation_diff = replace(base, expected_hashes=(rf.ExpectedHash(3, 1),))
    checkpoint_diff = replace(
        base, rng_checkpoints=(rf.RngCheckpoint(3, 1, bytes(rf.RNG_STATE_SIZE)),)
    )

    assert "header:" in rf.diff_documents(base, header_diff)[0]
    assert "meta:" in rf.diff_documents(base, meta_diff)[0]
    assert "tick 3: events differ" in rf.diff_documents(base, tick_events_diff)[0]
    assert "only in the left" in rf.diff_documents(base, tick_missing_diff)[0]
    assert "only in the right" in rf.diff_documents(tick_missing_diff, base)[0]
    assert "only in the left" in rf.diff_documents(extra_tick_diff, base)[0]
    assert "expected hashes:" in rf.diff_documents(base, expectation_diff)[0]
    assert "rng checkpoints:" in rf.diff_documents(base, checkpoint_diff)[0]


def test_canonical_key_ranks_kinds_then_actor_then_input_tick() -> None:
    """Spawns sort before moves before despawns, then id, then input tick."""
    keys = [
        rf.canonical_event_key(_spawn(actor_id=99)),
        rf.canonical_event_key(_move(actor_id=0, input_tick=9)),
        rf.canonical_event_key(_move(actor_id=1, input_tick=2)),
        rf.canonical_event_key(_move(actor_id=1, input_tick=5)),
        rf.canonical_event_key(rf.DespawnEvent(actor_id=0)),
    ]
    assert keys == sorted(keys)
