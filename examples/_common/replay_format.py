"""Shared codec for the binary simulation replay-record format.

This module is the single source of truth for the wire layout documented in
docs/guides/replay_format.md: the fixed 16-byte header, the 8-byte record
framing, and the four record types (tick, meta, expected hash, RNG
checkpoint) with their big-endian payloads. Both sides of the format live
here — the streaming writer recording call sites use and the strict reader
tools/replay.py replays with — plus the logical-content differ and the
canonical event ordering the recorder applies at tick boundaries.

The module imports the standard library ONLY. It is imported by the example
composition roots (which run under PYTHONPATH=python:examples where the repo
root and the kith package may be absent) and by tools/replay.py; neither
``kith`` nor ``ctypes`` may appear here, and that invariant is what lets the
tool boot from a bare checkout.

Versioning rules (enforced mechanically where possible):

- R1 — ``format_version`` changes only when the header or record framing
  changes structurally. Readers refuse newer versions loudly instead of
  misparsing, and no v1 field carries semantics a future version could
  reinterpret (header flags must be zero).
- R2 — layouts of known record and event types are frozen within a version.
  Additive evolution adds a new record type code; the v1 event set is
  closed, so a new event kind requires a ``format_version`` bump.
- R3 — payloads of known types must be consumed exactly (strict
  trailing-byte rejection catches corruption). Unknown record types are
  skipped whole using their length prefix.

Ordering law: META precedes the first tick; tick records are strictly
ascending and unique; a tick's events appear in canonical order (spawn,
move, despawn ranks, then actor id, then input tick); RNG checkpoints
follow their own tick record with strictly ascending stream keys.
"""

from __future__ import annotations

import json
import struct
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol


__all__ = [
    "DESPAWN_EVENT_CODE",
    "EXPECTED_HASH_RECORD_TYPE",
    "FORMAT_VERSION",
    "HEADER_SIZE",
    "MAGIC",
    "META_RECORD_TYPE",
    "MOVE_EVENT_CODE",
    "RECORD_HEADER_SIZE",
    "RNG_CHECKPOINT_RECORD_TYPE",
    "RNG_STATE_SIZE",
    "SPAWN_EVENT_CODE",
    "TICK_RECORD_TYPE",
    "DespawnEvent",
    "ExpectedHash",
    "MoveEvent",
    "ReplayFormatError",
    "ReplayHeader",
    "ReplayWriter",
    "RngCheckpoint",
    "SpawnEvent",
    "TickRecord",
    "canonical_event_key",
    "diff_documents",
    "encode_document",
    "read_document",
    "read_document_file",
    "sniff_is_binary",
]

# ---------------------------------------------------------------------------
# format constants
# ---------------------------------------------------------------------------

#: Magic identifying a binary replay record; doubles as the text/binary sniffer.
MAGIC: bytes = b"KRPL"

#: Current format version. Bumped only for header/framing structural changes.
FORMAT_VERSION: int = 1

#: Size of the v1 header: magic, version, header_size, tick_hz, flags.
HEADER_SIZE: int = 16

#: Size of the per-record framing prefix: type, flags, reserved, payload_len.
RECORD_HEADER_SIZE: int = 8

#: Byte length of a serialized ``kith_rng_state_t`` (derivation_seed + words[4]).
RNG_STATE_SIZE: int = 40

TICK_RECORD_TYPE: int = 0x01
META_RECORD_TYPE: int = 0x02
EXPECTED_HASH_RECORD_TYPE: int = 0x03
RNG_CHECKPOINT_RECORD_TYPE: int = 0x04

SPAWN_EVENT_CODE: int = 0x01
MOVE_EVENT_CODE: int = 0x02
DESPAWN_EVENT_CODE: int = 0x03

_HEADER = struct.Struct(">4sHHII")
_RECORD = struct.Struct(">BBHI")
_TICK_HEAD = struct.Struct(">QH")
_SPAWN = struct.Struct(">QqqqqqqI")
_MOVE = struct.Struct(">QIhhhB")
_DESPAWN = struct.Struct(">Q")
_EXPECTED_HASH = struct.Struct(">QQ")
_CHECKPOINT_HEAD = struct.Struct(">QQI")

_U16_MAX = 0xFFFF


class ReplayFormatError(Exception):
    """Raised when replay-record bytes violate the format contract.

    The message names the source and byte offset so a corrupt artifact can
    be located without tooling.
    """


# ---------------------------------------------------------------------------
# logical model
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SpawnEvent:
    """Create an actor at a fixed pose; positions/velocities are raw Q16.16."""

    actor_id: int
    pos_x: int
    pos_y: int
    pos_z: int
    vel_x: int
    vel_y: int
    vel_z: int
    flags: int


@dataclass(frozen=True)
class MoveEvent:
    """Apply one movement input to an actor (latest-move-wins)."""

    actor_id: int
    input_tick: int
    move_x: int
    move_y: int
    move_z: int
    flags: int


@dataclass(frozen=True)
class DespawnEvent:
    """Remove an actor from the live set."""

    actor_id: int


Event = SpawnEvent | MoveEvent | DespawnEvent


@dataclass(frozen=True)
class TickRecord:
    """All events attributed to one simulation tick, in canonical order."""

    tick: int
    events: tuple[Event, ...]


@dataclass(frozen=True)
class ExpectedHash:
    """A pinned rolling world-state hash asserted by the verify mode."""

    tick: int
    hash: int


@dataclass(frozen=True)
class RngCheckpoint:
    """A generator state snapshot bound to one tick and one named stream."""

    tick: int
    stream_key: int
    state: bytes

    def __post_init__(self) -> None:
        if len(self.state) != RNG_STATE_SIZE:
            raise ValueError(
                f"rng state must be exactly {RNG_STATE_SIZE} bytes, got {len(self.state)}"
            )


@dataclass(frozen=True)
class ReplayHeader:
    """Parsed header fields (the version always equals ``FORMAT_VERSION``)."""

    tick_hz: int
    flags: int


@dataclass(frozen=True)
class ReplayDocument:
    """The complete logical content of one replay record."""

    header: ReplayHeader
    meta: dict[str, object] | None
    ticks: tuple[TickRecord, ...]
    expected_hashes: tuple[ExpectedHash, ...]
    rng_checkpoints: tuple[RngCheckpoint, ...]


def canonical_event_key(event: Event) -> tuple[int, int, int]:
    """Return the deterministic sort key placing events in canonical order.

    Rank spawns before moves before despawns, then order by actor id, then
    by input tick. Within one tick this makes event order a pure function of
    the logical input set, so concurrently-fed recordings stay replayable.
    """
    if isinstance(event, SpawnEvent):
        return (0, event.actor_id, 0)
    if isinstance(event, MoveEvent):
        return (1, event.actor_id, event.input_tick)
    return (2, event.actor_id, 0)


def _sort_events(events: Iterable[Event]) -> tuple[Event, ...]:
    return tuple(sorted(events, key=canonical_event_key))


# ---------------------------------------------------------------------------
# encoding helpers
# ---------------------------------------------------------------------------


def _pack_event(event: Event) -> bytes:
    """Serialize one event with its one-byte type code."""
    if isinstance(event, SpawnEvent):
        return struct.pack(">B", SPAWN_EVENT_CODE) + _SPAWN.pack(
            event.actor_id,
            event.pos_x,
            event.pos_y,
            event.pos_z,
            event.vel_x,
            event.vel_y,
            event.vel_z,
            event.flags,
        )
    if isinstance(event, MoveEvent):
        return struct.pack(">B", MOVE_EVENT_CODE) + _MOVE.pack(
            event.actor_id,
            event.input_tick,
            event.move_x,
            event.move_y,
            event.move_z,
            event.flags,
        )
    return struct.pack(">B", DESPAWN_EVENT_CODE) + _DESPAWN.pack(event.actor_id)


def _unpack_event(payload: bytes, offset: int, where: str) -> Event:
    """Parse one event starting at ``payload[offset:]``.

    Event bodies are fixed-width per code, so the local check is a minimum
    remaining-length guard; consuming exactly the declared count is enforced
    by the tick-level trailing-bytes check.
    """
    code = payload[offset]
    body = payload[offset + 1 :]
    if code == SPAWN_EVENT_CODE:
        _require_at_least(body, _SPAWN.size, where)
        return SpawnEvent(*_SPAWN.unpack_from(body))
    if code == MOVE_EVENT_CODE:
        _require_at_least(body, _MOVE.size, where)
        return MoveEvent(*_MOVE.unpack_from(body))
    if code == DESPAWN_EVENT_CODE:
        _require_at_least(body, _DESPAWN.size, where)
        return DespawnEvent(*_DESPAWN.unpack_from(body))
    raise ReplayFormatError(f"{where}: unknown event code 0x{code:02x}")


def _require_exact_len(actual: bytes, expected: int, where: str) -> None:
    if len(actual) != expected:
        raise ReplayFormatError(f"{where}: expected {expected} payload bytes, found {len(actual)}")


def _require_at_least(actual: bytes, minimum: int, where: str) -> None:
    if len(actual) < minimum:
        raise ReplayFormatError(f"{where}: payload shorter than {minimum} bytes")


def _at(source: str, offset: int) -> str:
    """Format a ``source:offset`` label for error messages."""
    return f"{source}:{offset}"


# ---------------------------------------------------------------------------
# writer
# ---------------------------------------------------------------------------


class _Writable(Protocol):
    """Minimal writable-binary-stream shape the writer depends on."""

    def write(self, data: bytes, /) -> int: ...


class ReplayWriter:
    """Streaming writer producing one binary replay record.

    The writer enforces the ordering law as records are emitted: metadata
    first, strictly ascending unique ticks, canonically ordered events, and
    checkpoints immediately after their own tick with ascending stream keys.
    Misuse raises ValueError (a caller error); out-of-range field values raise
    ``struct.error``.

    Args:
        stream: A writable binary stream, positioned at the start. The
            caller owns the stream and its lifetime.
        tick_hz: Tick rate recorded in the header; at least 1.
    """

    def __init__(self, stream: _Writable, *, tick_hz: int) -> None:
        if tick_hz < 1:
            raise ValueError(f"tick_hz must be at least 1, got {tick_hz}")
        self._stream = stream
        self._closed = False
        self._meta_written = False
        self._last_tick: int | None = None
        self._last_stream_key: int | None = None
        self._stream.write(_HEADER.pack(MAGIC, FORMAT_VERSION, HEADER_SIZE, tick_hz, 0))

    def write_meta(self, meta: Mapping[str, object]) -> None:
        """Write the informational META record; legal only before any tick."""
        self._ensure_open()
        if self._meta_written:
            raise ValueError("write_meta called twice")
        if self._last_tick is not None:
            raise ValueError("write_meta must precede the first tick record")
        payload = json.dumps(meta, sort_keys=True).encode("utf-8")
        self._emit(META_RECORD_TYPE, payload)
        self._meta_written = True

    def write_tick(self, tick: int, events: Iterable[Event]) -> None:
        """Write one tick record holding ``events`` in canonical order."""
        self._ensure_open()
        if self._last_tick is not None and tick <= self._last_tick:
            raise ValueError(
                f"tick {tick} does not advance past the previously written tick {self._last_tick}"
            )
        ordered = _sort_events(events)
        if len(ordered) > _U16_MAX:
            raise ValueError(
                f"tick {tick} carries {len(ordered)} events; the u16 event "
                f"count limit is {_U16_MAX}"
            )
        payload = bytearray(_TICK_HEAD.pack(tick, len(ordered)))
        for event in ordered:
            payload += _pack_event(event)
        self._emit(TICK_RECORD_TYPE, bytes(payload))
        self._last_tick = tick
        self._last_stream_key = None

    def write_expected_hash(self, tick: int, hash_value: int) -> None:
        """Write one pinned rolling-hash expectation."""
        self._ensure_open()
        self._emit(EXPECTED_HASH_RECORD_TYPE, _EXPECTED_HASH.pack(tick, hash_value))

    def write_rng_checkpoint(self, tick: int, stream_key: int, state: bytes) -> None:
        """Write a generator snapshot bound to the most recently written tick.

        ``state`` is the serialized ``kith_rng_state_t`` (exactly
        ``RNG_STATE_SIZE`` bytes, big-endian).
        """
        self._ensure_open()
        if self._last_tick is None or tick != self._last_tick:
            raise ValueError(
                f"checkpoint tick {tick} does not match the most recently "
                f"written tick {self._last_tick}"
            )
        if self._last_stream_key is not None and stream_key <= self._last_stream_key:
            raise ValueError(
                f"stream key {stream_key} does not ascend past "
                f"{self._last_stream_key} within tick {tick}"
            )
        if len(state) != RNG_STATE_SIZE:
            raise ValueError(f"rng state must be exactly {RNG_STATE_SIZE} bytes, got {len(state)}")
        self._emit(
            RNG_CHECKPOINT_RECORD_TYPE,
            _CHECKPOINT_HEAD.pack(tick, stream_key, len(state)) + state,
        )
        self._last_stream_key = stream_key

    def close(self) -> None:
        """Mark the record complete; further writes raise ValueError."""
        self._closed = True

    def _emit(self, record_type: int, payload: bytes) -> None:
        self._stream.write(_RECORD.pack(record_type, 0, 0, len(payload)))
        self._stream.write(payload)

    def _ensure_open(self) -> None:
        if self._closed:
            raise ValueError("writer is closed")


# ---------------------------------------------------------------------------
# reader
# ---------------------------------------------------------------------------


def sniff_is_binary(data: bytes) -> bool:
    """Report whether ``data`` starts with the binary-format magic."""
    return data.startswith(MAGIC)


def read_document_file(path: Path | str) -> ReplayDocument:
    """Read and validate the replay record stored at ``path``."""
    return read_document(Path(path).read_bytes(), source=str(path))


def read_document(data: bytes, *, source: str = "<memory>") -> ReplayDocument:
    """Parse and validate a complete replay record from ``data``.

    Raises:
        ReplayFormatError: On any magic, version, framing, ordering, or
            truncation violation; messages carry ``source`` and offsets.
    """
    if len(data) < HEADER_SIZE:
        raise ReplayFormatError(f"{source}: file shorter than the {HEADER_SIZE}-byte header")
    magic, version, header_size, tick_hz, flags = _HEADER.unpack_from(data)
    if magic != MAGIC:
        raise ReplayFormatError(f"{source}: bad magic {magic!r}")
    if version != FORMAT_VERSION:
        raise ReplayFormatError(
            f"{source}: format version {version} exceeds the supported version {FORMAT_VERSION}"
        )
    if flags != 0:
        raise ReplayFormatError(f"{source}: unknown header flag bits 0x{flags:08x}")
    if tick_hz < 1:
        raise ReplayFormatError(f"{source}: header tick_hz {tick_hz} is invalid")
    if header_size < HEADER_SIZE:
        raise ReplayFormatError(
            f"{source}: header size {header_size} below the required {HEADER_SIZE}"
        )
    offset = header_size
    meta: dict[str, object] | None = None
    ticks: list[TickRecord] = []
    expected: list[ExpectedHash] = []
    checkpoints: list[RngCheckpoint] = []
    while offset < len(data):
        if len(data) - offset < RECORD_HEADER_SIZE:
            raise ReplayFormatError(f"{_at(source, offset)}: truncated record framing")
        record_type, record_flags, _reserved, payload_len = _RECORD.unpack_from(data, offset)
        record_where = _at(source, offset)
        offset += RECORD_HEADER_SIZE
        if offset + payload_len > len(data):
            raise ReplayFormatError(f"{_at(source, offset)}: payload exceeds end of file")
        payload = data[offset : offset + payload_len]
        offset += payload_len
        if record_type == TICK_RECORD_TYPE:
            if record_flags != 0:
                raise ReplayFormatError(f"{record_where}: unknown record flag bits set")
            tick_record = _parse_tick(payload, record_where)
            if ticks and tick_record.tick <= ticks[-1].tick:
                raise ReplayFormatError(
                    f"{record_where}: tick {tick_record.tick} does not ascend past {ticks[-1].tick}"
                )
            ticks.append(tick_record)
        elif record_type == META_RECORD_TYPE:
            if meta is not None or ticks:
                raise ReplayFormatError(
                    f"{record_where}: META must appear exactly once, before the first tick"
                )
            meta = _parse_meta(payload, record_where)
        elif record_type == EXPECTED_HASH_RECORD_TYPE:
            if record_flags != 0:
                raise ReplayFormatError(f"{record_where}: unknown record flag bits set")
            _require_exact_len(payload, _EXPECTED_HASH.size, record_where)
            exp_tick, exp_hash = _EXPECTED_HASH.unpack(payload)
            if any(exp_tick == seen.tick for seen in expected):
                raise ReplayFormatError(
                    f"{record_where}: duplicate expectation for tick {exp_tick}"
                )
            expected.append(ExpectedHash(exp_tick, exp_hash))
        elif record_type == RNG_CHECKPOINT_RECORD_TYPE:
            if record_flags != 0:
                raise ReplayFormatError(f"{record_where}: unknown record flag bits set")
            checkpoints.append(_parse_checkpoint(payload, record_where, ticks, checkpoints))
        else:
            continue
    return ReplayDocument(
        header=ReplayHeader(tick_hz=tick_hz, flags=flags),
        meta=meta,
        ticks=tuple(ticks),
        expected_hashes=tuple(expected),
        rng_checkpoints=tuple(checkpoints),
    )


def _parse_tick(payload: bytes, where: str) -> TickRecord:
    """Parse one TICK payload; events must arrive already canonically ordered."""
    _require_at_least(payload, _TICK_HEAD.size, where)
    tick, count = _TICK_HEAD.unpack_from(payload)
    events: list[Event] = []
    offset = _TICK_HEAD.size
    previous_key: tuple[int, int, int] | None = None
    for _ in range(count):
        if offset >= len(payload):
            raise ReplayFormatError(f"{where}: event count {count} exceeds the payload")
        event = _unpack_event(payload, offset, where)
        offset += 1 + _EVENT_BODY_SIZES[type(event)]
        key = canonical_event_key(event)
        if previous_key is not None and key < previous_key:
            raise ReplayFormatError(f"{where}: events are not in canonical order")
        previous_key = key
        events.append(event)
    _require_exact_len(payload[offset:], 0, where)
    return TickRecord(tick, tuple(events))


_EVENT_BODY_SIZES: dict[type, int] = {
    SpawnEvent: _SPAWN.size,
    MoveEvent: _MOVE.size,
    DespawnEvent: _DESPAWN.size,
}


def _parse_meta(payload: bytes, where: str) -> dict[str, object]:
    """Decode the UTF-8 JSON object carried by a META record."""
    try:
        decoded = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ReplayFormatError(f"{where}: META is not valid UTF-8 JSON ({exc})") from exc
    if not isinstance(decoded, dict):
        raise ReplayFormatError(f"{where}: META must be a JSON object")
    return decoded


def _parse_checkpoint(
    payload: bytes,
    where: str,
    ticks: list[TickRecord],
    previous_checkpoints: list[RngCheckpoint],
) -> RngCheckpoint:
    """Decode one RNG checkpoint and validate it against the ordering law."""
    _require_exact_len(payload, _CHECKPOINT_HEAD.size + RNG_STATE_SIZE, where)
    tick, stream_key, state_len = _CHECKPOINT_HEAD.unpack_from(payload)
    if not ticks or tick != ticks[-1].tick:
        anchor = ticks[-1].tick if ticks else "none"
        raise ReplayFormatError(
            f"{where}: checkpoint tick {tick} does not follow its tick record "
            f"(most recent tick: {anchor})"
        )
    if previous_checkpoints and previous_checkpoints[-1].tick == tick:
        prior_key = previous_checkpoints[-1].stream_key
        if stream_key <= prior_key:
            raise ReplayFormatError(
                f"{where}: stream key {stream_key} does not ascend past "
                f"{prior_key} within tick {tick}"
            )
    if state_len != RNG_STATE_SIZE:
        raise ReplayFormatError(f"{where}: checkpoint state length {state_len} != {RNG_STATE_SIZE}")
    state = bytes(payload[_CHECKPOINT_HEAD.size :])
    return RngCheckpoint(tick, stream_key, state)


# ---------------------------------------------------------------------------
# document assembly and differ
# ---------------------------------------------------------------------------


class _CollectingSink:
    """Minimal writable sink accumulating produced bytes."""

    def __init__(self) -> None:
        self.buffer = bytearray()

    def write(self, data: bytes, /) -> int:
        self.buffer += data
        return len(data)


def encode_document(document: ReplayDocument) -> bytes:
    """Serialize a complete document in one pass (transcode and tests).

    Raises:
        ValueError: If a checkpoint names a tick the document does not
            carry; the writer could not place it under the ordering law.
    """
    sink = _CollectingSink()
    writer = ReplayWriter(sink, tick_hz=document.header.tick_hz)
    if document.meta is not None:
        writer.write_meta(document.meta)
    checkpoints_by_tick: dict[int, list[RngCheckpoint]] = {}
    for checkpoint in document.rng_checkpoints:
        checkpoints_by_tick.setdefault(checkpoint.tick, []).append(checkpoint)
    known_ticks = {t.tick for t in document.ticks}
    unknown = sorted(set(checkpoints_by_tick) - known_ticks)
    if unknown:
        raise ValueError(f"checkpoints reference ticks absent from the document: {unknown}")
    for tick_record in document.ticks:
        writer.write_tick(tick_record.tick, tick_record.events)
        for checkpoint in checkpoints_by_tick.get(tick_record.tick, ()):
            writer.write_rng_checkpoint(checkpoint.tick, checkpoint.stream_key, checkpoint.state)
    for expectation in document.expected_hashes:
        writer.write_expected_hash(expectation.tick, expectation.hash)
    writer.close()
    return bytes(sink.buffer)


def diff_documents(left: ReplayDocument, right: ReplayDocument) -> list[str]:
    """Compare two documents logically and return human-readable differences.

    An empty result means the documents carry identical logical content:
    header fields, metadata, per-tick event sequences, expectations, and
    checkpoints. The arguments are symmetric; "left" and "right" name the
    documents as passed.
    """
    differences: list[str] = []
    if left.header != right.header:
        differences.append(f"header: {left.header} != {right.header}")
    if left.meta != right.meta:
        differences.append(f"meta: {left.meta} != {right.meta}")
    differences.extend(_diff_ticks(left.ticks, right.ticks))
    left_expectations = {e.tick: e.hash for e in left.expected_hashes}
    right_expectations = {e.tick: e.hash for e in right.expected_hashes}
    if left_expectations != right_expectations:
        differences.append(f"expected hashes: {left_expectations} != {right_expectations}")
    if left.rng_checkpoints != right.rng_checkpoints:
        differences.append(f"rng checkpoints: {left.rng_checkpoints} != {right.rng_checkpoints}")
    return differences


def _diff_ticks(left: tuple[TickRecord, ...], right: tuple[TickRecord, ...]) -> Iterator[str]:
    left_by_tick = {t.tick: t for t in left}
    right_by_tick = {t.tick: t for t in right}
    for tick in sorted(left_by_tick.keys() | right_by_tick.keys()):
        left_record = left_by_tick.get(tick)
        right_record = right_by_tick.get(tick)
        if left_record is None:
            yield f"tick {tick}: present only in the right document"
        elif right_record is None:
            yield f"tick {tick}: present only in the left document"
        elif left_record.events != right_record.events:
            yield (
                f"tick {tick}: events differ ({_summarize(left_record.events)} "
                f"vs {_summarize(right_record.events)})"
            )


def _summarize(events: tuple[Event, ...]) -> str:
    """Render a short stable description of one tick's event list."""
    parts = []
    for event in events[:4]:
        kind = type(event).__name__.removesuffix("Event").lower()
        parts.append(f"{kind}(actor {event.actor_id})")
    if len(events) > 4:
        parts.append(f"+{len(events) - 4} more")
    return ", ".join(parts)
