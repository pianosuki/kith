"""Event correlator for the agentic harness.

A synchronous, time-windowed index that cross-references client-side and
server-side events by correlation ID and time. The orchestrator (a subsequent
module) polls :class:`~tools.agent.scenario.ScenarioContext` for client
events and :class:`~tools.agent.assertions.ServerControl` for server state,
feeding both into an :class:`EventCorrelator`; scenario steps and assertions
query the correlator to verify that a client request (correlation ID ``N``)
produced the expected server-side effect and client response, in time order.

This sits at a different layer from
:func:`~tools.agent.assertions.assert_correlation`, which polls a single
client's event history. The correlator joins events across multiple clients
*and* the server, keyed by correlation ID, ordered by time, and tagged by
kind (client vs server) so cross-side causality is first-class.

The correlator is a pure in-memory store with no I/O: ingestion and queries
are synchronous. The async boundary is at the orchestrator, which drives the
polling. A configurable time window evicts old events so a long-lived
correlator has bounded memory.
"""

from __future__ import annotations

import enum
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass

from tools.agent.ahc import ClientEvent


__all__ = [
    "CorrelatedEvent",
    "EventCorrelator",
    "EventKind",
]


_NO_CORRELATION: int = 0


class EventKind(enum.StrEnum):
    """Whether an event originated from a client or the server."""

    CLIENT = "client"
    SERVER = "server"


@dataclass(frozen=True, slots=True)
class CorrelatedEvent:
    """A normalized event from either a client or the server.

    Client events are derived from :class:`~tools.agent.ahc.ClientEvent`
    (typed, with a bytes payload); server events are derived from a JSON
    mapping returned by the control plane (with an opaque ``detail`` mapping).
    Both carry a monotonic timestamp in nanoseconds (the harness clock domain),
    a source label, a correlation ID, and a numeric type id, so cross-side
    queries return a uniform value type.
    """

    ts_mono_ns: int
    source: str
    kind: EventKind
    correlation_id: int
    type_id: int
    payload: bytes
    detail: Mapping[str, object]

    @classmethod
    def from_client(cls, source: str, event: ClientEvent) -> CorrelatedEvent:
        """Build a client-side correlated event from a ``ClientEvent``."""
        return cls(
            ts_mono_ns=event.ts_mono_ns,
            source=source,
            kind=EventKind.CLIENT,
            correlation_id=event.correlation_id,
            type_id=event.type_id,
            payload=event.payload,
            detail={},
        )

    @classmethod
    def from_server(cls, source: str, event: Mapping[str, object]) -> CorrelatedEvent:
        """Build a server-side correlated event from a control-plane mapping.

        Extracts ``correlation_id`` and ``type_id`` from the mapping by key
        (0 if absent or non-numeric), using the harness monotonic clock at
        ingest time as the timestamp. The full mapping is preserved in
        ``detail``.
        """
        return cls(
            ts_mono_ns=time.monotonic_ns(),
            source=source,
            kind=EventKind.SERVER,
            correlation_id=_to_int(event.get("correlation_id")),
            type_id=_to_int(event.get("type_id")),
            payload=b"",
            detail=dict(event),
        )


def _to_int(value: object) -> int:
    """Coerce a JSON-decoded value to int (str/int/float accepted); 0 if None."""
    if value is None:
        return 0
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int | float):
        return int(value)
    try:
        return int(str(value))
    except ValueError:
        return 0


class EventCorrelator:
    """A time-windowed index of client and server events by correlation ID.

    Events are ingested from named client sources (typed ``ClientEvent``) and
    named server sources (JSON mappings). Both are normalized to
    :class:`CorrelatedEvent` and indexed by correlation ID and by type id.
    A configurable time window evicts stale events on each ingest call so
    memory stays bounded across a long scenario.

    The correlator is synchronous and holds no I/O resources; the orchestrator
    owns the polling loop that feeds it.
    """

    def __init__(self, *, window_s: float = 30.0) -> None:
        self._window_ns: int = int(window_s * 1_000_000_000)
        self._by_correlation: dict[int, list[CorrelatedEvent]] = {}
        self._by_type: dict[int, list[CorrelatedEvent]] = {}
        self._event_count: int = 0

    # -----------------------------------------------------------------------
    # ingestion
    # -----------------------------------------------------------------------

    def ingest_client(self, source: str, event: ClientEvent) -> None:
        """Ingest a single client event, indexed immediately."""
        self.evict()
        self._index(CorrelatedEvent.from_client(source, event))

    def ingest_client_batch(self, source: str, events: Sequence[ClientEvent]) -> None:
        """Ingest a batch of client events, evicting once before indexing."""
        self.evict()
        for event in events:
            self._index(CorrelatedEvent.from_client(source, event))

    def ingest_server(self, source: str, event: Mapping[str, object]) -> None:
        """Ingest a single server event mapping, indexed immediately."""
        self.evict()
        self._index(CorrelatedEvent.from_server(source, event))

    def ingest_server_batch(self, source: str, events: Sequence[Mapping[str, object]]) -> None:
        """Ingest a batch of server event mappings, evicting once first."""
        self.evict()
        for event in events:
            self._index(CorrelatedEvent.from_server(source, event))

    def _index(self, event: CorrelatedEvent) -> None:
        """Add an event to both indices and bump the count."""
        if event.correlation_id != _NO_CORRELATION:
            self._by_correlation.setdefault(event.correlation_id, []).append(event)
        self._by_type.setdefault(event.type_id, []).append(event)
        self._event_count += 1

    # -----------------------------------------------------------------------
    # queries
    # -----------------------------------------------------------------------

    def find_by_correlation(self, correlation_id: int) -> list[CorrelatedEvent]:
        """Return all events sharing ``correlation_id``, ordered by time.

        Returns an empty list if no events carry the id (or if 0 is passed,
        which is the no-correlation sentinel and never indexed).
        """
        events = self._by_correlation.get(correlation_id, [])
        return sorted(events, key=lambda e: e.ts_mono_ns)

    def find_by_type(
        self,
        type_id: int,
        *,
        source: str | None = None,
        kind: EventKind | None = None,
    ) -> list[CorrelatedEvent]:
        """Return events of ``type_id``, optionally filtered by source or kind.

        Results are ordered by time.
        """
        return self._filter(self._by_type.get(type_id, []), source=source, kind=kind)

    def find_matching(
        self,
        predicate: Callable[[CorrelatedEvent], bool],
        *,
        source: str | None = None,
        kind: EventKind | None = None,
    ) -> list[CorrelatedEvent]:
        """Return events satisfying ``predicate``, optionally filtered.

        Scans all indexed events (across every type and correlation id),
        applying the source/kind filters first, then the predicate. Results
        are ordered by time.
        """
        matched: list[CorrelatedEvent] = []
        for bucket in self._by_type.values():
            for event in self._filter(bucket, source=source, kind=kind):
                if predicate(event):
                    matched.append(event)
        matched.sort(key=lambda e: e.ts_mono_ns)
        return matched

    def correlations(self) -> list[int]:
        """Return the distinct correlation IDs currently indexed."""
        return list(self._by_correlation)

    def correlation_count(self) -> int:
        """Return the number of distinct correlation IDs currently indexed."""
        return len(self._by_correlation)

    def event_count(self) -> int:
        """Return the total number of events currently indexed."""
        return self._event_count

    # -----------------------------------------------------------------------
    # maintenance
    # -----------------------------------------------------------------------

    def evict(self) -> None:
        """Drop events older than the time window from both indices.

        Called automatically on each ingest call; exposed for explicit cleanup
        between ingests when a scenario idles.
        """
        cutoff = time.monotonic_ns() - self._window_ns
        for cid in list(self._by_correlation):
            kept = [e for e in self._by_correlation[cid] if e.ts_mono_ns >= cutoff]
            if kept:
                self._by_correlation[cid] = kept
            else:
                del self._by_correlation[cid]
        for type_id in list(self._by_type):
            kept = [e for e in self._by_type[type_id] if e.ts_mono_ns >= cutoff]
            if kept:
                self._by_type[type_id] = kept
            else:
                del self._by_type[type_id]
        self._event_count = sum(len(b) for b in self._by_type.values())

    def flush(self) -> None:
        """Clear all indexed events and reset the count."""
        self._by_correlation.clear()
        self._by_type.clear()
        self._event_count = 0

    # -----------------------------------------------------------------------
    # internals
    # -----------------------------------------------------------------------

    @staticmethod
    def _filter(
        events: Sequence[CorrelatedEvent],
        *,
        source: str | None,
        kind: EventKind | None,
    ) -> list[CorrelatedEvent]:
        """Apply optional source and kind filters, returning time-ordered results."""
        matched = [
            e
            for e in events
            if (source is None or e.source == source) and (kind is None or e.kind == kind)
        ]
        matched.sort(key=lambda e: e.ts_mono_ns)
        return matched
