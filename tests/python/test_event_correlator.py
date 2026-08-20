"""Unit tests for the event correlator module."""

from __future__ import annotations

import time
from collections.abc import Mapping

from tools.agent.ahc import ClientEvent
from tools.agent.event_correlator import (
    CorrelatedEvent,
    EventCorrelator,
    EventKind,
)


def _client_event(
    type_id: int,
    correlation_id: int = 0,
    payload: bytes = b"",
    ts_mono_ns: int | None = None,
) -> ClientEvent:
    return ClientEvent(
        ts_mono_ns=ts_mono_ns if ts_mono_ns is not None else time.monotonic_ns(),
        type_id=type_id,
        correlation_id=correlation_id,
        payload=payload,
    )


def _server_event(
    correlation_id: int = 0,
    type_id: int = 0,
    **extra: object,
) -> dict[str, object]:
    event: dict[str, object] = {"correlation_id": correlation_id, "type_id": type_id}
    event.update(extra)
    return event


def _now() -> int:
    """Capture a base timestamp for deterministic ordering within a test."""
    return time.monotonic_ns()


# ---------------------------------------------------------------------------
# CorrelatedEvent.from_client
# ---------------------------------------------------------------------------


def test_from_client_preserves_all_fields() -> None:
    event = _client_event(8, correlation_id=42, payload=b"hello", ts_mono_ns=1_000)

    correlated = CorrelatedEvent.from_client("alice", event)

    assert correlated.ts_mono_ns == 1_000
    assert correlated.source == "alice"
    assert correlated.kind is EventKind.CLIENT
    assert correlated.correlation_id == 42
    assert correlated.type_id == 8
    assert correlated.payload == b"hello"
    assert correlated.detail == {}


def test_from_client_kind_is_client() -> None:
    correlated = CorrelatedEvent.from_client("bob", _client_event(1))
    assert correlated.kind is EventKind.CLIENT


# ---------------------------------------------------------------------------
# CorrelatedEvent.from_server
# ---------------------------------------------------------------------------


def test_from_server_extracts_correlation_and_type() -> None:
    mapping: Mapping[str, object] = _server_event(correlation_id=99, type_id=5, status="ok")

    correlated = CorrelatedEvent.from_server("server", mapping)

    assert correlated.source == "server"
    assert correlated.kind is EventKind.SERVER
    assert correlated.correlation_id == 99
    assert correlated.type_id == 5
    assert correlated.payload == b""
    assert correlated.detail["status"] == "ok"


def test_from_server_defaults_zero_when_keys_absent() -> None:
    correlated = CorrelatedEvent.from_server("server", {"unrelated": "field"})

    assert correlated.correlation_id == 0
    assert correlated.type_id == 0


def test_from_server_coerces_string_correlation_id() -> None:
    correlated = CorrelatedEvent.from_server("server", {"correlation_id": "123", "type_id": "7"})

    assert correlated.correlation_id == 123
    assert correlated.type_id == 7


def test_from_server_non_numeric_correlation_id_yields_zero() -> None:
    correlated = CorrelatedEvent.from_server("server", {"correlation_id": "abc"})

    assert correlated.correlation_id == 0


def test_from_server_detail_is_a_copy() -> None:
    mapping = _server_event(correlation_id=1, type_id=2, custom="value")

    correlated = CorrelatedEvent.from_server("server", mapping)

    assert correlated.detail["custom"] == "value"


def test_from_server_ts_is_monotonic_ns() -> None:
    before = time.monotonic_ns()
    correlated = CorrelatedEvent.from_server("server", {"type_id": 1})
    after = time.monotonic_ns()

    assert before <= correlated.ts_mono_ns <= after


def test_from_server_none_correlation_id_yields_zero() -> None:
    correlated = CorrelatedEvent.from_server("server", {"correlation_id": None})

    assert correlated.correlation_id == 0


# ---------------------------------------------------------------------------
# EventKind
# ---------------------------------------------------------------------------


def test_event_kind_values() -> None:
    assert EventKind.CLIENT.value == "client"
    assert EventKind.SERVER.value == "server"


def test_event_kind_is_str_enum() -> None:
    assert isinstance(EventKind.CLIENT, str)


# ---------------------------------------------------------------------------
# ingest_client / ingest_client_batch
# ---------------------------------------------------------------------------


def test_ingest_client_indexes_by_correlation() -> None:
    base = _now()
    corr = EventCorrelator(window_s=30.0)
    corr.ingest_client("alice", _client_event(8, correlation_id=42, ts_mono_ns=base))

    result = corr.find_by_correlation(42)

    assert len(result) == 1
    assert result[0].correlation_id == 42
    assert result[0].source == "alice"


def test_ingest_client_indexes_by_type() -> None:
    base = _now()
    corr = EventCorrelator(window_s=30.0)
    corr.ingest_client("alice", _client_event(8, correlation_id=42, ts_mono_ns=base))

    result = corr.find_by_type(8)

    assert len(result) == 1
    assert result[0].type_id == 8


def test_ingest_client_batch_indexes_all() -> None:
    base = _now()
    events = [
        _client_event(8, correlation_id=1, ts_mono_ns=base),
        _client_event(8, correlation_id=2, ts_mono_ns=base + 1),
        _client_event(9, correlation_id=3, ts_mono_ns=base + 2),
    ]

    corr = EventCorrelator(window_s=30.0)
    corr.ingest_client_batch("alice", events)

    assert corr.event_count() == 3
    assert corr.correlation_count() == 3


def test_ingest_client_no_correlation_not_in_correlation_index() -> None:
    base = _now()
    corr = EventCorrelator(window_s=30.0)
    corr.ingest_client("alice", _client_event(8, correlation_id=0, ts_mono_ns=base))

    assert corr.find_by_correlation(0) == []
    assert corr.find_by_correlation(42) == []
    assert corr.event_count() == 1


def test_ingest_client_from_multiple_sources() -> None:
    base = _now()
    corr = EventCorrelator(window_s=30.0)
    corr.ingest_client("alice", _client_event(8, correlation_id=42, ts_mono_ns=base))
    corr.ingest_client("bob", _client_event(8, correlation_id=42, ts_mono_ns=base + 1))

    result = corr.find_by_correlation(42)

    assert len(result) == 2
    assert {e.source for e in result} == {"alice", "bob"}


# ---------------------------------------------------------------------------
# ingest_server / ingest_server_batch
# ---------------------------------------------------------------------------


def test_ingest_server_indexes_by_correlation() -> None:
    corr = EventCorrelator(window_s=30.0)
    corr.ingest_server("server", _server_event(correlation_id=42, type_id=5))

    result = corr.find_by_correlation(42)

    assert len(result) == 1
    assert result[0].kind is EventKind.SERVER


def test_ingest_server_batch_indexes_all() -> None:
    events = [
        _server_event(correlation_id=1, type_id=5),
        _server_event(correlation_id=2, type_id=5),
        _server_event(correlation_id=3, type_id=6),
    ]

    corr = EventCorrelator(window_s=30.0)
    corr.ingest_server_batch("server", events)

    assert corr.event_count() == 3
    assert corr.correlation_count() == 3


def test_ingest_server_no_correlation_not_in_correlation_index() -> None:
    corr = EventCorrelator(window_s=30.0)
    corr.ingest_server("server", _server_event(correlation_id=0, type_id=5))

    assert corr.find_by_correlation(0) == []
    assert corr.event_count() == 1


# ---------------------------------------------------------------------------
# cross-referencing (client + server)
# ---------------------------------------------------------------------------


def test_cross_reference_client_and_server_by_correlation() -> None:
    base = _now()
    corr = EventCorrelator(window_s=30.0)
    corr.ingest_client("alice", _client_event(8, correlation_id=42, ts_mono_ns=base))
    corr.ingest_server("server", _server_event(correlation_id=42, type_id=5))
    corr.ingest_client("alice", _client_event(9, correlation_id=42, ts_mono_ns=base + 2))

    result = corr.find_by_correlation(42)

    assert len(result) == 3
    kinds = [e.kind for e in result]
    assert EventKind.CLIENT in kinds
    assert EventKind.SERVER in kinds
    assert sorted(e.ts_mono_ns for e in result) == [e.ts_mono_ns for e in result]


def test_find_by_correlation_ordered_by_time() -> None:
    base = _now()
    corr = EventCorrelator(window_s=30.0)
    corr.ingest_client("alice", _client_event(8, correlation_id=42, ts_mono_ns=base + 300))
    corr.ingest_client("alice", _client_event(8, correlation_id=42, ts_mono_ns=base + 100))
    corr.ingest_client("alice", _client_event(8, correlation_id=42, ts_mono_ns=base + 200))

    result = corr.find_by_correlation(42)

    assert [e.ts_mono_ns for e in result] == [base + 100, base + 200, base + 300]


def test_find_by_correlation_empty_when_not_found() -> None:
    base = _now()
    corr = EventCorrelator(window_s=30.0)
    corr.ingest_client("alice", _client_event(8, correlation_id=42, ts_mono_ns=base))

    assert corr.find_by_correlation(99) == []


def test_find_by_correlation_empty_for_zero_correlation() -> None:
    base = _now()
    corr = EventCorrelator(window_s=30.0)
    corr.ingest_client("alice", _client_event(8, correlation_id=0, ts_mono_ns=base))

    assert corr.find_by_correlation(0) == []


# ---------------------------------------------------------------------------
# find_by_type
# ---------------------------------------------------------------------------


def test_find_by_type_returns_matching_events() -> None:
    base = _now()
    corr = EventCorrelator(window_s=30.0)
    corr.ingest_client("alice", _client_event(8, correlation_id=1, ts_mono_ns=base))
    corr.ingest_client("alice", _client_event(9, correlation_id=2, ts_mono_ns=base + 1))
    corr.ingest_client("alice", _client_event(8, correlation_id=3, ts_mono_ns=base + 2))

    result = corr.find_by_type(8)

    assert len(result) == 2
    assert all(e.type_id == 8 for e in result)


def test_find_by_type_filtered_by_source() -> None:
    base = _now()
    corr = EventCorrelator(window_s=30.0)
    corr.ingest_client("alice", _client_event(8, correlation_id=1, ts_mono_ns=base))
    corr.ingest_client("bob", _client_event(8, correlation_id=2, ts_mono_ns=base + 1))

    result = corr.find_by_type(8, source="alice")

    assert len(result) == 1
    assert result[0].source == "alice"


def test_find_by_type_filtered_by_kind() -> None:
    base = _now()
    corr = EventCorrelator(window_s=30.0)
    corr.ingest_client("alice", _client_event(8, correlation_id=1, ts_mono_ns=base))
    corr.ingest_server("server", _server_event(correlation_id=2, type_id=8))

    result = corr.find_by_type(8, kind=EventKind.SERVER)

    assert len(result) == 1
    assert result[0].kind is EventKind.SERVER


def test_find_by_type_ordered_by_time() -> None:
    base = _now()
    corr = EventCorrelator(window_s=30.0)
    corr.ingest_client("alice", _client_event(8, correlation_id=1, ts_mono_ns=base + 300))
    corr.ingest_client("alice", _client_event(8, correlation_id=2, ts_mono_ns=base + 100))
    corr.ingest_client("alice", _client_event(8, correlation_id=3, ts_mono_ns=base + 200))

    result = corr.find_by_type(8)

    assert [e.ts_mono_ns for e in result] == [base + 100, base + 200, base + 300]


def test_find_by_type_empty_when_no_match() -> None:
    base = _now()
    corr = EventCorrelator(window_s=30.0)
    corr.ingest_client("alice", _client_event(8, correlation_id=1, ts_mono_ns=base))

    assert corr.find_by_type(99) == []


def test_find_by_type_includes_zero_correlation_events() -> None:
    base = _now()
    corr = EventCorrelator(window_s=30.0)
    corr.ingest_client("alice", _client_event(8, correlation_id=0, ts_mono_ns=base))

    result = corr.find_by_type(8)

    assert len(result) == 1


# ---------------------------------------------------------------------------
# find_matching
# ---------------------------------------------------------------------------


def test_find_matching_returns_predicate_matches() -> None:
    base = _now()
    corr = EventCorrelator(window_s=30.0)
    corr.ingest_client("alice", _client_event(8, correlation_id=1, payload=b"a", ts_mono_ns=base))
    corr.ingest_client(
        "alice", _client_event(8, correlation_id=2, payload=b"b", ts_mono_ns=base + 1)
    )

    result = corr.find_matching(lambda e: e.payload == b"b")

    assert len(result) == 1
    assert result[0].payload == b"b"


def test_find_matching_filtered_by_source_and_kind() -> None:
    base = _now()
    corr = EventCorrelator(window_s=30.0)
    corr.ingest_client("alice", _client_event(8, correlation_id=1, ts_mono_ns=base))
    corr.ingest_client("bob", _client_event(8, correlation_id=2, ts_mono_ns=base + 1))
    corr.ingest_server("server", _server_event(correlation_id=3, type_id=8))

    result = corr.find_matching(lambda e: True, source="alice", kind=EventKind.CLIENT)

    assert len(result) == 1
    assert result[0].source == "alice"


def test_find_matching_ordered_by_time() -> None:
    base = _now()
    corr = EventCorrelator(window_s=30.0)
    corr.ingest_client("alice", _client_event(8, correlation_id=1, ts_mono_ns=base + 300))
    corr.ingest_client("alice", _client_event(9, correlation_id=2, ts_mono_ns=base + 100))
    corr.ingest_client("alice", _client_event(7, correlation_id=3, ts_mono_ns=base + 200))

    result = corr.find_matching(lambda e: True)

    assert [e.ts_mono_ns for e in result] == [base + 100, base + 200, base + 300]


def test_find_matching_empty_when_no_predicate_match() -> None:
    base = _now()
    corr = EventCorrelator(window_s=30.0)
    corr.ingest_client("alice", _client_event(8, correlation_id=1, ts_mono_ns=base))

    assert corr.find_matching(lambda e: e.correlation_id == 99) == []


def test_find_matching_scans_all_types() -> None:
    base = _now()
    corr = EventCorrelator(window_s=30.0)
    corr.ingest_client("alice", _client_event(8, correlation_id=1, ts_mono_ns=base))
    corr.ingest_client("alice", _client_event(9, correlation_id=2, ts_mono_ns=base + 1))
    corr.ingest_client("alice", _client_event(10, correlation_id=3, ts_mono_ns=base + 2))

    result = corr.find_matching(lambda e: e.correlation_id in (1, 3))

    assert len(result) == 2
    assert {e.correlation_id for e in result} == {1, 3}


# ---------------------------------------------------------------------------
# correlations / correlation_count / event_count
# ---------------------------------------------------------------------------


def test_correlations_returns_distinct_ids() -> None:
    base = _now()
    corr = EventCorrelator(window_s=30.0)
    corr.ingest_client("alice", _client_event(8, correlation_id=1, ts_mono_ns=base))
    corr.ingest_client("alice", _client_event(8, correlation_id=2, ts_mono_ns=base + 1))
    corr.ingest_client("alice", _client_event(8, correlation_id=1, ts_mono_ns=base + 2))

    result = corr.correlations()

    assert set(result) == {1, 2}


def test_correlation_count_is_distinct() -> None:
    base = _now()
    corr = EventCorrelator(window_s=30.0)
    corr.ingest_client("alice", _client_event(8, correlation_id=1, ts_mono_ns=base))
    corr.ingest_client("alice", _client_event(8, correlation_id=1, ts_mono_ns=base + 1))
    corr.ingest_client("alice", _client_event(8, correlation_id=2, ts_mono_ns=base + 2))

    assert corr.correlation_count() == 2


def test_correlation_count_excludes_zero_correlation() -> None:
    base = _now()
    corr = EventCorrelator(window_s=30.0)
    corr.ingest_client("alice", _client_event(8, correlation_id=0, ts_mono_ns=base))
    corr.ingest_client("alice", _client_event(8, correlation_id=42, ts_mono_ns=base + 1))

    assert corr.correlation_count() == 1


def test_event_count_tracks_all_events() -> None:
    base = _now()
    corr = EventCorrelator(window_s=30.0)
    corr.ingest_client("alice", _client_event(8, correlation_id=0, ts_mono_ns=base))
    corr.ingest_client("alice", _client_event(8, correlation_id=42, ts_mono_ns=base + 1))
    corr.ingest_server("server", _server_event(correlation_id=42, type_id=5))

    assert corr.event_count() == 3


def test_event_count_empty_correlator_is_zero() -> None:
    corr = EventCorrelator(window_s=30.0)
    assert corr.event_count() == 0


# ---------------------------------------------------------------------------
# evict
# ---------------------------------------------------------------------------


def test_evict_removes_old_events() -> None:
    # The injected ts predates every real monotonic cutoff.
    corr = EventCorrelator(window_s=0.01)
    corr.ingest_client("alice", _client_event(8, correlation_id=42, ts_mono_ns=1))

    corr.evict()

    assert corr.find_by_correlation(42) == []
    assert corr.event_count() == 0


def test_evict_keeps_recent_events() -> None:
    corr = EventCorrelator(window_s=30.0)
    corr.ingest_client("alice", _client_event(8, correlation_id=42, ts_mono_ns=time.monotonic_ns()))

    corr.evict()

    assert len(corr.find_by_correlation(42)) == 1


def test_evict_removes_empty_correlation_buckets() -> None:
    corr = EventCorrelator(window_s=0.01)
    corr.ingest_client("alice", _client_event(8, correlation_id=42, ts_mono_ns=1))

    corr.evict()

    assert corr.correlation_count() == 0
    assert 42 not in corr.correlations()


def test_evict_removes_empty_type_buckets() -> None:
    corr = EventCorrelator(window_s=0.01)
    corr.ingest_client("alice", _client_event(8, correlation_id=42, ts_mono_ns=1))

    corr.evict()

    assert corr.find_by_type(8) == []


def test_ingest_triggers_eviction_automatically() -> None:
    corr = EventCorrelator(window_s=0.01)
    corr.ingest_client("alice", _client_event(8, correlation_id=1, ts_mono_ns=1))

    corr.ingest_client("alice", _client_event(9, correlation_id=2, ts_mono_ns=time.monotonic_ns()))

    assert corr.find_by_correlation(1) == []
    assert len(corr.find_by_correlation(2)) == 1


def test_evict_recounts_event_total() -> None:
    corr = EventCorrelator(window_s=0.01)
    corr.ingest_client("alice", _client_event(8, correlation_id=1, ts_mono_ns=1))
    corr.ingest_client("alice", _client_event(9, correlation_id=2, ts_mono_ns=1))

    corr.evict()

    assert corr.event_count() == 0


# ---------------------------------------------------------------------------
# flush
# ---------------------------------------------------------------------------


def test_flush_clears_everything() -> None:
    base = _now()
    corr = EventCorrelator(window_s=30.0)
    corr.ingest_client("alice", _client_event(8, correlation_id=42, ts_mono_ns=base))
    corr.ingest_server("server", _server_event(correlation_id=42, type_id=5))

    corr.flush()

    assert corr.event_count() == 0
    assert corr.correlation_count() == 0
    assert corr.find_by_correlation(42) == []
    assert corr.find_by_type(8) == []


def test_flush_idempotent_on_empty_correlator() -> None:
    corr = EventCorrelator(window_s=30.0)
    corr.flush()

    assert corr.event_count() == 0


def test_correlator_reusable_after_flush() -> None:
    base = _now()
    corr = EventCorrelator(window_s=30.0)
    corr.ingest_client("alice", _client_event(8, correlation_id=1, ts_mono_ns=base))
    corr.flush()

    corr.ingest_client("alice", _client_event(8, correlation_id=42, ts_mono_ns=base + 1))

    assert len(corr.find_by_correlation(42)) == 1
    assert corr.event_count() == 1


# ---------------------------------------------------------------------------
# window_s configuration
# ---------------------------------------------------------------------------


def test_default_window_is_thirty_seconds() -> None:
    corr = EventCorrelator()
    corr.ingest_client("alice", _client_event(8, correlation_id=42, ts_mono_ns=time.monotonic_ns()))

    corr.evict()

    assert len(corr.find_by_correlation(42)) == 1


def test_zero_window_evicts_immediately_on_next_ingest() -> None:
    corr = EventCorrelator(window_s=0.0)
    corr.ingest_client("alice", _client_event(8, correlation_id=1, ts_mono_ns=1))

    corr.ingest_client("alice", _client_event(9, correlation_id=2, ts_mono_ns=1))

    assert corr.find_by_correlation(1) == []
