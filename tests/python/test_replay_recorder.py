"""Unit tests for the tick-boundary replay recorder.

These exercise the recording policy directly — canonical drain order,
idle-tick elision, checkpoint placement, tail discard on close, and
thread-safe buffering — without loading the simulation library or the
bridge.
"""

from __future__ import annotations

import io
import threading

import pytest
from examples._common import replay_format as rf
from examples._common.replay_recorder import DEFAULT_STREAM_KEY, ReplayRecorder


def _spawn(actor_id: int = 1) -> rf.SpawnEvent:
    return rf.SpawnEvent(
        actor_id=actor_id,
        pos_x=0,
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


def _recorder(**kwargs: object) -> tuple[ReplayRecorder, io.BytesIO]:
    """Build a recorder over an in-memory stream."""
    stream = io.BytesIO()
    return ReplayRecorder(stream, tick_hz=20, **kwargs), stream  # type: ignore[arg-type]


def _drain(recorder: ReplayRecorder, sink: io.BytesIO) -> rf.ReplayDocument:
    recorder.close()
    return rf.read_document(sink.getvalue())


def test_drain_orders_events_canonically_regardless_of_arrival() -> None:
    """A shuffled arrival batch drains as the canonically ordered record."""
    recorder, stream = _recorder()
    arrival: list[rf.SpawnEvent | rf.MoveEvent | rf.DespawnEvent] = [
        _move(2, input_tick=3),
        _spawn(1),
        _move(1, input_tick=2),
        rf.DespawnEvent(actor_id=1),
        _spawn(2),
    ]
    for event in reversed(arrival):
        recorder.record(event)
    recorder.flush_tick(4)
    document = _drain(recorder, stream)
    assert [type(event) for event in document.ticks[0].events] == [
        rf.SpawnEvent,
        rf.SpawnEvent,
        rf.MoveEvent,
        rf.MoveEvent,
        rf.DespawnEvent,
    ]
    assert document.ticks[0].tick == 4


def test_idle_ticks_are_elided() -> None:
    """Empty boundary drains write no records at all."""
    recorder, stream = _recorder()
    recorder.flush_tick(1)
    recorder.flush_tick(2)
    recorder.flush_tick(3)
    document = _drain(recorder, stream)
    assert document.ticks == ()
    assert document.rng_checkpoints == ()


def test_recording_resumes_after_an_idle_gap() -> None:
    """Ticks recorded after idle ones still ascend from their own number."""
    recorder, stream = _recorder()
    recorder.flush_tick(1)
    recorder.record(_spawn(7))
    recorder.flush_tick(9)
    document = _drain(recorder, stream)
    assert [(tick.tick, len(tick.events)) for tick in document.ticks] == [(9, 1)]


def test_provider_writes_one_checkpoint_per_written_tick() -> None:
    """Each non-idle drain snapshots the generator under the default key."""
    calls: list[int] = []

    def provider() -> bytes:
        calls.append(len(calls))
        return bytes([calls[-1]] * rf.RNG_STATE_SIZE)

    recorder, stream = _recorder(rng_state_provider=provider)
    recorder.flush_tick(1)  # idle: no checkpoint, provider not consulted
    recorder.record(_spawn(1))
    recorder.flush_tick(2)
    recorder.record(_move(1))
    recorder.flush_tick(5)
    document = _drain(recorder, stream)
    assert calls == [0, 1]
    assert [(c.tick, c.stream_key) for c in document.rng_checkpoints] == [
        (2, DEFAULT_STREAM_KEY),
        (5, DEFAULT_STREAM_KEY),
    ]
    assert [c.state[0] for c in document.rng_checkpoints] == [0, 1]
    assert all(len(c.state) == rf.RNG_STATE_SIZE for c in document.rng_checkpoints)


def test_provider_returning_a_wrong_length_state_raises() -> None:
    """A malformed snapshot fails the drain instead of writing a bad record."""
    recorder, _stream = _recorder(rng_state_provider=lambda: b"short")
    recorder.record(_spawn(1))
    with pytest.raises(ValueError, match="rng state provider"):
        recorder.flush_tick(1)


def test_close_discards_the_unflushed_tail_and_freezes() -> None:
    """Events buffered at close are dropped; subsequent calls are silent no-ops."""
    recorder, stream = _recorder()
    recorder.record(_spawn(1))
    recorder.close()
    recorder.record(_move(1))  # after close: dropped, not raised
    recorder.flush_tick(2)  # after close: dropped, not raised
    recorder.close()  # idempotent
    document = rf.read_document(stream.getvalue())
    assert document.ticks == ()


def test_write_meta_is_legal_only_before_the_first_drain() -> None:
    """The META passthrough inherits the ordering law from the writer."""
    recorder, stream = _recorder()
    recorder.write_meta({"model": "tile2d"})
    recorder.record(_spawn(1))
    recorder.flush_tick(1)
    with pytest.raises(ValueError, match="write_meta"):
        recorder.write_meta({"late": True})
    document = _drain(recorder, stream)
    assert document.meta == {"model": "tile2d"}


def test_concurrent_record_calls_preserve_every_event() -> None:
    """Buffering under the lock loses nothing across racing worker threads."""
    recorder, stream = _recorder()
    threads = 8
    per_thread = 50

    def feed(worker: int) -> None:
        for step in range(per_thread):
            recorder.record(_move(actor_id=worker + 1, input_tick=step + 1))

    workers = [threading.Thread(target=feed, args=(w,)) for w in range(threads)]
    for thread in workers:
        thread.start()
    for thread in workers:
        thread.join()
    recorder.flush_tick(1)
    document = _drain(recorder, stream)
    assert len(document.ticks[0].events) == threads * per_thread


def test_flush_ticks_must_advance() -> None:
    """Draining a stale tick is a caller error and raises through the writer."""
    recorder, _stream = _recorder()
    recorder.record(_spawn(1))
    recorder.flush_tick(3)
    recorder.record(_spawn(2))
    with pytest.raises(ValueError, match="does not advance"):
        recorder.flush_tick(3)


def test_artifact_is_durable_before_close() -> None:
    """Bytes reach the stream at every boundary, so an unclean exit keeps
    every drained tick readable."""
    recorder, stream = _recorder()
    recorder.write_meta({"model": "tile2d"})
    recorder.record(_spawn(3))
    recorder.flush_tick(1)
    mid_session = stream.getvalue()  # close never called
    assert len(mid_session) >= rf.HEADER_SIZE
    parsed = rf.read_document(mid_session)
    assert parsed.meta == {"model": "tile2d"}
    assert [tick.tick for tick in parsed.ticks] == [1]
