"""Tick-boundary recorder producing binary replay records.

The recording half of the replay facility defined by
docs/guides/replay_format.md: call sites fed by worker threads buffer
events here, and one drain per server tick writes the buffered batch as a
single TICK record — canonically ordered, so the artifact is a function of
the logical input set rather than of scheduler arrival order. An optional
rng-state provider callable adds one RNG_CHECKPOINT record after each
written tick, pinning the recorded run's generator state for replay.

The module imports the standard library and the replay codec ONLY. It runs
inside example composition roots (PYTHONPATH=python:examples, where the
repo root and the kith package may be absent), so neither ``kith`` nor
``ctypes`` may appear here; the generator handle that feeds the provider
lives in the composition root.

Recording policy:

- Idle-tick elision — a tick whose buffer is empty writes no records at
  all (its checkpoint, if a provider is configured, is omitted with it).
  Replay fills the gap by stepping: the replay loop advances the
  simulation once per tick from zero to the highest recorded tick and
  applies events only at their recorded tick, so elided ticks cost
  nothing, and generator continuity survives the gap because generator
  advancement is a pure function of the step count. Elision keeps an
  idle session's artifact bounded instead of growing one empty record
  set per tick.
- Tail discard — close drops any events buffered
  since the last boundary: they belong to a tick that never completed,
  and writing them would produce a TICK record the simulation never
  actually stepped past. A recording ends on a tick boundary.
"""

from __future__ import annotations

import threading
from collections.abc import Mapping
from typing import BinaryIO, Protocol

from examples._common.replay_format import (
    RNG_STATE_SIZE,
    Event,
    ReplayWriter,
    canonical_event_key,
)


__all__ = [
    "DEFAULT_STREAM_KEY",
    "ReplayRecorder",
    "RngStateProvider",
]

#: Stream key recorded for the composition root's root generator.
DEFAULT_STREAM_KEY: int = 0


class RngStateProvider(Protocol):
    """Callable supplying one serialized generator snapshot per drain.

    Returns exactly RNG_STATE_SIZE
    bytes: the big-endian serialization of a ``kith_rng_state_t``
    (derivation seed, then the four algorithm words).
    """

    def __call__(self) -> bytes: ...


class ReplayRecorder:
    """Buffers replay events per tick and drains them at tick boundaries.

    One instance per recorded server. Event call sites run on handler
    worker threads; the drain runs on the server's tick callback, so every
    operation takes the same lock and the writer is only ever touched
    while holding it — the drain's critical section covers the whole
    write (snapshot, sort, tick record, checkpoint), which also serializes
    overlapping tick callbacks when a worker pool dispatches more than one
    at a time.

    The recorder never touches the stream's lifetime: the caller opens the
    writable binary stream passed as ``stream`` and closes it after
    close. It does flush the stream after every boundary write, so
    an artifact stays readable up to the last drained tick even when the
    process ends uncleanly.

    Args:
        stream: Writable binary stream positioned at the start of the
            artifact; the caller owns it.
        tick_hz: Tick rate recorded in the artifact header; at least 1.
        rng_state_provider: Optional callable invoked once per written
            tick, under the drain lock, to snapshot the recorded
            generator; its return value becomes the tick's
            RNG_CHECKPOINT record under ``DEFAULT_STREAM_KEY``.
    """

    def __init__(
        self,
        stream: BinaryIO,
        *,
        tick_hz: int,
        rng_state_provider: RngStateProvider | None = None,
    ) -> None:
        self._writer = ReplayWriter(stream, tick_hz=tick_hz)
        self._stream = stream
        self._provider = rng_state_provider
        self._lock = threading.Lock()
        self._pending: list[Event] = []
        self._closed = False

    def write_meta(self, meta: Mapping[str, object]) -> None:
        """Write the informational META record.

        Legal only before the first drain, per the ordering law; the
        writer rejects a second META or one that follows a tick.
        """
        with self._lock:
            self._ensure_open()
            self._writer.write_meta(meta)
            self._stream.flush()

    def record(self, event: Event) -> None:
        """Buffer one event for the tick currently being recorded.

        Safe to call from any worker thread at any cadence. After
        close the call is a no-op: the recording has ended, and an
        event that arrives during shutdown belongs to no completable tick.

        Args:
            event: A spawn, move, or despawn event from the codec's
                closed v1 event set.
        """
        with self._lock:
            if self._closed:
                return
            self._pending.append(event)

    def flush_tick(self, tick: int) -> None:
        """Drain the buffer as tick ``tick``'s record (the tick boundary).

        Writes a TICK record only when the buffer holds at least one
        event; an idle tick is elided entirely (see the module docstring).
        With a provider configured, a written tick is followed by its
        RNG_CHECKPOINT. Events are emitted in canonical order regardless
        of arrival order. After close the drain is a no-op.

        Args:
            tick: The server's monotonic tick index at the boundary.

        Raises:
            ValueError: From the writer when ``tick`` does not advance
                past the last drained tick — a caller error, since the
                server hands the callback ascending indices.
        """
        with self._lock:
            if self._closed:
                return
            events = sorted(self._pending, key=canonical_event_key)
            self._pending.clear()
            if not events:
                return
            self._writer.write_tick(tick, events)
            if self._provider is not None:
                state = self._provider()
                if len(state) != RNG_STATE_SIZE:
                    raise ValueError(
                        f"rng state provider returned {len(state)} bytes, expected {RNG_STATE_SIZE}"
                    )
                self._writer.write_rng_checkpoint(tick, DEFAULT_STREAM_KEY, state)
            self._stream.flush()

    def close(self) -> None:
        """End the recording, discarding the unflushed tail.

        Idempotent. The discard and the closed flag are applied atomically
        under the lock, so an event recorded concurrently with the close
        either makes it into the last drained tick or is dropped whole —
        never written after the recording ended.
        """
        with self._lock:
            if self._closed:
                return
            self._closed = True
            self._pending.clear()
            self._writer.close()

    def _ensure_open(self) -> None:
        if self._closed:
            raise ValueError("recorder is closed")
