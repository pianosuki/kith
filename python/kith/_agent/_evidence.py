"""Bounded frame retention and immutable movement-window evaluation."""

from __future__ import annotations

import asyncio
import itertools
from collections import deque
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass


VIEW_WINDOW_ID_CAP = 256


@dataclass(frozen=True, slots=True)
class WindowEvidence:
    """One client's frozen movement-window view evidence.

    ``distinct_ids`` holds every non-membership actor id received while
    the window was open (saturated at ``_VIEW_WINDOW_ID_CAP``);
    ``frame_ts_ns`` holds each in-window frame's arrival timestamp in
    ascending order for the breadth probe's stall scan. ``incomplete``
    marks a basis that cannot prove completeness: the evidence deque
    evicted frames while the window was open, or no byte budget was
    configured, so the counts are a floor — the breadth probe reads
    this and reports the client as unfounded rather than trusting the
    counts. ``deque_distinct_ids`` runs the same saturation-capped
    extraction over the whole retained deque with the window bounds
    ignored, so a sparse window read splits into a window younger than
    the retained roster versus a roster the retained stream never
    carried; the self filter applies at read time exactly as it does
    for ``distinct_ids``.
    """

    distinct_ids: frozenset[int]
    frame_ts_ns: tuple[int, ...]
    incomplete: bool = False
    deque_distinct_ids: frozenset[int] = frozenset()


@dataclass(frozen=True, slots=True)
class WindowSnapshot:
    """Immutable retained frames and counting bounds captured on the client loop.

    Payload bytes are shared with retention. The extractor runs on the calling
    thread; cooperative evaluation yields between bounded batches of frames.
    """

    frames: tuple[tuple[int, int, bytes], ...]
    start_ns: int | None
    deadline_ns: int | None
    incomplete: bool
    extractor: Callable[[int, bytes], Iterable[int]] | None

    def evaluate(self) -> WindowEvidence:
        """Reduce captured frames synchronously into frozen window evidence."""
        for result in self._batches():
            if result is not None:
                return result
        raise AssertionError("window reduction produced no evidence")

    async def evaluate_async(self) -> WindowEvidence:
        """Reduce captured frames while yielding between bounded frame batches."""
        await asyncio.sleep(0)
        for result in self._batches():
            if result is not None:
                return result
            await asyncio.sleep(0)
        raise AssertionError("window reduction produced no evidence")

    def _batches(self) -> Iterator[WindowEvidence | None]:
        ids: set[int] = set()
        deque_ids: set[int] = set()
        frame_ts: list[int] = []
        for index, (ts_ns, type_id, payload) in enumerate(self.frames, 1):
            in_window = (self.start_ns is None or ts_ns >= self.start_ns) and (
                self.deadline_ns is None or ts_ns <= self.deadline_ns
            )
            if in_window:
                frame_ts.append(ts_ns)
            want_deque = len(deque_ids) < VIEW_WINDOW_ID_CAP
            want_window = in_window and len(ids) < VIEW_WINDOW_ID_CAP
            if self.extractor is not None and (want_deque or want_window):
                extracted = list(self.extractor(type_id, payload))
                if want_deque:
                    deque_ids.update(
                        itertools.islice(extracted, VIEW_WINDOW_ID_CAP - len(deque_ids))
                    )
                if want_window:
                    ids.update(itertools.islice(extracted, VIEW_WINDOW_ID_CAP - len(ids)))
            if index % 64 == 0:
                yield None
        yield WindowEvidence(frozenset(ids), tuple(frame_ts), self.incomplete, frozenset(deque_ids))


class EvidenceFrames:
    """Retain frame values in flat FIFOs without per-record wrappers."""

    __slots__ = ("_metadata", "_payloads")

    def __init__(self) -> None:
        self._metadata: deque[int] = deque()
        self._payloads: deque[bytes] = deque()

    def __len__(self) -> int:
        return len(self._payloads)

    def append_values(self, ts_ns: int, type_id: int, payload: bytes) -> None:
        """Append values while preserving both FIFO lengths on failure."""
        metadata, payloads = self._metadata, self._payloads
        metadata_length, payload_length = len(metadata), len(payloads)
        try:
            metadata.append(ts_ns)
            metadata.append(type_id)
            payloads.append(payload)
        except BaseException:
            while len(payloads) > payload_length:
                payloads.pop()
            while len(metadata) > metadata_length:
                metadata.pop()
            raise

    def append(self, frame: tuple[int, int, bytes]) -> None:
        """Append a cold compatibility record using the same value path."""
        self.append_values(*frame)

    def extend(self, frames: Iterable[tuple[int, int, bytes]]) -> None:
        """Append ordered cold compatibility records."""
        for frame in frames:
            self.append_values(*frame)

    def popleft(self) -> tuple[int, int, bytes]:
        """Remove and return the oldest frame."""
        if not self._payloads:
            raise IndexError("pop from empty evidence")
        result = (self._metadata[0], self._metadata[1], self._payloads[0])
        self.discard_oldest()
        return result

    def oldest_timestamp(self) -> int:
        """Read the oldest timestamp."""
        return self._metadata[0]

    def discard_oldest(self) -> bytes:
        """Remove the oldest frame and return only its shared payload."""
        if not self._payloads:
            raise IndexError("pop from empty evidence")
        self._metadata.popleft()
        self._metadata.popleft()
        return self._payloads.popleft()

    def clear(self) -> None:
        """Release live metadata and payload references."""
        self._metadata.clear()
        self._payloads.clear()

    def span_ns(self) -> int | None:
        """Return the oldest-to-newest timestamp span, or None when empty."""
        if not self._payloads:
            return None
        return self._metadata[-2] - self._metadata[0]

    def __iter__(self) -> Iterator[tuple[int, int, bytes]]:
        metadata = iter(self._metadata)
        for payload in self._payloads:
            yield next(metadata), next(metadata), payload
