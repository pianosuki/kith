"""Unit tests for the scenario assertions module."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from typing import Any

import pytest
from tools.agent.ahc import ClientEvent, ClientStatus
from tools.agent.assertions import (
    ScenarioAssertionError,
    ServerControl,
    _parse_metric,
    assert_correlation,
    assert_event,
    assert_no_event,
    assert_server_metric,
    assert_server_state,
    assert_state_predicate,
)
from tools.agent.scenario import ScenarioContext


def _event(
    type_id: int,
    payload: bytes = b"",
    correlation_id: int = 0,
    ts_mono_ns: int = 1_000_000,
) -> ClientEvent:
    return ClientEvent(
        ts_mono_ns=ts_mono_ns,
        type_id=type_id,
        correlation_id=correlation_id,
        payload=payload,
    )


def _status(connected: bool = True) -> ClientStatus:
    return ClientStatus(
        connected=connected,
        bootstrap_state=2,
        bootstrap_step=3,
        bootstrap_step_count=3,
        rtt_last_ms=4,
        reconnect_attempts=0,
    )


class _FakeHost:
    """In-memory ScenarioHost that is programmable for assertion tests."""

    def __init__(self) -> None:
        self._events: dict[str, list[ClientEvent]] = {}
        self._state: dict[str, ClientStatus] = {}

    def set_events(self, client: str, events: list[ClientEvent]) -> None:
        self._events[client] = list(events)

    def add_event(self, client: str, event: ClientEvent) -> None:
        self._events.setdefault(client, []).append(event)

    def set_state(self, client: str, status: ClientStatus) -> None:
        self._state[client] = status

    async def register_client(self, name: str, *, host: str, port: int) -> None:
        del name, host, port

    async def start_client(self, name: str) -> None:
        del name

    async def stop_client(self, name: str) -> None:
        del name

    async def submit(
        self,
        client: str,
        type_id: int,
        payload: bytes = b"",
        *,
        flags: int = 0,
        correlation_id: int = 0,
    ) -> int:
        del client, type_id, payload, flags, correlation_id
        return 0

    async def register_type(self, client: str, name: str, type_id: int) -> None:
        del client, name, type_id

    async def client_state(self, client: str) -> ClientStatus:
        return self._state.get(client, _status())

    async def recent_events(self, client: str, count: int = 64) -> list[ClientEvent]:
        return list(self._events.get(client, []))[-count:]

    async def wait_for_event(
        self, client: str, type_id: int, *, timeout_s: float
    ) -> ClientEvent | None:
        del timeout_s
        for event in self._events.get(client, []):
            if event.type_id == type_id:
                self._events[client].remove(event)
                return event
        return None

    async def wait_for_state(
        self,
        client: str,
        predicate: Any,
        *,
        timeout_s: float,
    ) -> bool:
        del timeout_s
        return bool(predicate(self._state.get(client, _status())))

    def client_principal(self, name: str) -> int | None:
        del name
        return None

    async def server_state(self, path: str) -> Mapping[str, object]:
        del path
        return {}

    async def server_command(
        self,
        path: str,
        body: Mapping[str, object] | None = None,
    ) -> Mapping[str, object]:
        del path, body
        return {}


def _ctx() -> tuple[ScenarioContext, _FakeHost]:
    host = _FakeHost()
    return ScenarioContext(host=host, name="test"), host


class _FakeServerControl:
    """In-memory ServerControl for server-side assertion tests."""

    def __init__(
        self,
        metrics_text: str = "",
        state_map: dict[str, Mapping[str, object]] | None = None,
    ) -> None:
        self._metrics = metrics_text
        self._state: dict[str, dict[str, object]] = {
            k: dict(v) for k, v in (state_map or {}).items()
        }

    def set_metrics(self, text: str) -> None:
        self._metrics = text

    def set_state(self, path: str, data: Mapping[str, object]) -> None:
        self._state[path] = dict(data)

    async def metrics(self) -> str:
        return self._metrics

    async def get(self, path: str) -> dict[str, object]:
        return self._state.get(path, {})

    async def post(self, path: str, body: Mapping[str, object]) -> dict[str, object]:
        del path, body
        return {}


# ---------------------------------------------------------------------------
# assert_event
# ---------------------------------------------------------------------------


def test_assert_event_returns_first_predicate_match() -> None:
    ctx, host = _ctx()
    host.set_events(
        "alice",
        [
            _event(8, b"first", ts_mono_ns=1),
            _event(8, b"second", ts_mono_ns=2),
            _event(8, b"third", ts_mono_ns=3),
        ],
    )

    result = asyncio.run(
        assert_event(ctx, "alice", 8, lambda e: e.payload == b"second", timeout_s=0.1)
    )

    assert result.payload == b"second"


def test_assert_event_raises_on_timeout() -> None:
    ctx, _host = _ctx()

    with pytest.raises(ScenarioAssertionError, match="no 8 event satisfying"):
        asyncio.run(assert_event(ctx, "alice", 8, lambda e: True, timeout_s=0.05))


def test_assert_event_skips_non_matching_predicate() -> None:
    ctx, host = _ctx()
    host.set_events(
        "alice",
        [
            _event(8, b"no", ts_mono_ns=1),
            _event(8, b"yes", ts_mono_ns=2),
        ],
    )

    result = asyncio.run(
        assert_event(ctx, "alice", 8, lambda e: e.payload == b"yes", timeout_s=0.1)
    )

    assert result.payload == b"yes"


def test_assert_event_ignores_other_type_ids() -> None:
    ctx, host = _ctx()
    host.set_events(
        "alice",
        [
            _event(9, b"other", ts_mono_ns=1),
            _event(8, b"target", ts_mono_ns=2),
        ],
    )

    result = asyncio.run(assert_event(ctx, "alice", 8, lambda e: True, timeout_s=0.1))

    assert result.type_id == 8
    assert result.payload == b"target"


def test_assert_event_details_contain_client_and_type() -> None:
    ctx, _host = _ctx()

    try:
        asyncio.run(assert_event(ctx, "alice", 8, lambda e: True, timeout_s=0.01))
    except ScenarioAssertionError as exc:
        assert exc.details["client"] == "alice"
        assert exc.details["type_id"] == 8
    else:
        pytest.fail("expected ScenarioAssertionError")


# ---------------------------------------------------------------------------
# assert_no_event
# ---------------------------------------------------------------------------


def test_assert_no_event_passes_when_no_match_in_window() -> None:
    ctx, host = _ctx()
    host.set_events("alice", [_event(8, b"existing", ts_mono_ns=1)])

    asyncio.run(assert_no_event(ctx, "alice", 9, lambda e: True, window_s=0.05))


def test_assert_no_event_raises_when_matching_event_arrives_during_window() -> None:
    ctx, host = _ctx()
    host.set_events("alice", [])

    async def add_later() -> None:
        await asyncio.sleep(0.02)
        host.add_event("alice", _event(8, b"match", ts_mono_ns=2))

    async def run() -> None:
        task = asyncio.create_task(add_later())
        try:
            await assert_no_event(ctx, "alice", 8, lambda e: e.payload == b"match", window_s=0.2)
        finally:
            task.cancel()

    with pytest.raises(ScenarioAssertionError, match="unexpected 8 event"):
        asyncio.run(run())


def test_assert_no_event_ignores_baseline_events() -> None:
    ctx, host = _ctx()
    host.set_events("alice", [_event(8, b"old", ts_mono_ns=1)])

    asyncio.run(assert_no_event(ctx, "alice", 8, lambda e: e.payload == b"old", window_s=0.05))


def test_assert_no_event_type_mismatch_does_not_raise() -> None:
    ctx, host = _ctx()
    host.set_events("alice", [_event(9, b"other", ts_mono_ns=1)])

    asyncio.run(assert_no_event(ctx, "alice", 8, lambda e: True, window_s=0.05))


# ---------------------------------------------------------------------------
# assert_state_predicate
# ---------------------------------------------------------------------------


def test_assert_state_predicate_returns_satisfying_state() -> None:
    ctx, host = _ctx()
    host.set_state("alice", _status(connected=True))

    result = asyncio.run(assert_state_predicate(ctx, "alice", lambda s: s.connected, timeout_s=0.1))

    assert result.connected is True


def test_assert_state_predicate_raises_on_timeout() -> None:
    ctx, host = _ctx()
    host.set_state("alice", _status(connected=False))

    with pytest.raises(ScenarioAssertionError, match="predicate not satisfied"):
        asyncio.run(assert_state_predicate(ctx, "alice", lambda s: s.connected, timeout_s=0.05))


def test_assert_state_predicate_details_contain_state() -> None:
    ctx, host = _ctx()
    host.set_state("alice", _status(connected=False))

    try:
        asyncio.run(assert_state_predicate(ctx, "alice", lambda s: s.connected, timeout_s=0.01))
    except ScenarioAssertionError as exc:
        state = exc.details["state"]
        assert isinstance(state, ClientStatus)
        assert state.connected is False
    else:
        pytest.fail("expected ScenarioAssertionError")


# ---------------------------------------------------------------------------
# assert_correlation
# ---------------------------------------------------------------------------


def test_assert_correlation_returns_matching_events() -> None:
    ctx, host = _ctx()
    host.set_events(
        "alice",
        [
            _event(8, b"a", correlation_id=42, ts_mono_ns=1),
            _event(8, b"b", correlation_id=99, ts_mono_ns=2),
            _event(8, b"c", correlation_id=42, ts_mono_ns=3),
        ],
    )

    result = asyncio.run(assert_correlation(ctx, "alice", 42, min_events=2, timeout_s=0.1))

    assert len(result) == 2
    assert all(e.correlation_id == 42 for e in result)


def test_assert_correlation_raises_on_insufficient_events() -> None:
    ctx, host = _ctx()
    host.set_events(
        "alice",
        [
            _event(8, b"a", correlation_id=42, ts_mono_ns=1),
        ],
    )

    with pytest.raises(ScenarioAssertionError, match="expected >=2 events"):
        asyncio.run(assert_correlation(ctx, "alice", 42, min_events=2, timeout_s=0.05))


def test_assert_correlation_default_min_events_is_one() -> None:
    ctx, host = _ctx()
    host.set_events(
        "alice",
        [
            _event(8, b"a", correlation_id=7, ts_mono_ns=1),
        ],
    )

    result = asyncio.run(assert_correlation(ctx, "alice", 7, timeout_s=0.1))

    assert len(result) == 1


def test_assert_correlation_no_matches_raises() -> None:
    ctx, host = _ctx()
    host.set_events(
        "alice",
        [
            _event(8, b"a", correlation_id=1, ts_mono_ns=1),
        ],
    )

    with pytest.raises(ScenarioAssertionError, match="correlation_id=99"):
        asyncio.run(assert_correlation(ctx, "alice", 99, timeout_s=0.05))


# ---------------------------------------------------------------------------
# _parse_metric
# ---------------------------------------------------------------------------


def test_parse_metric_simple_value() -> None:
    text = "# HELP kith_connections Total connections\n# TYPE kith_connections gauge\nkith_connections 42\n"
    assert _parse_metric(text, "kith_connections") == 42.0


def test_parse_metric_with_labels() -> None:
    text = 'kith_connections{zone="1"} 17\nkith_connections{zone="2"} 25\n'
    assert _parse_metric(text, "kith_connections") == 17.0


def test_parse_metric_skips_prefix_mismatches() -> None:
    text = "kith_connections_total 100\nkith_connections 5\n"
    assert _parse_metric(text, "kith_connections") == 5.0


def test_parse_metric_returns_none_when_missing() -> None:
    assert _parse_metric("other_metric 1\n", "kith_connections") is None


def test_parse_metric_skips_comments_and_blanks() -> None:
    text = "# comment\n\nkith_uptime_seconds 3600\n"
    assert _parse_metric(text, "kith_uptime_seconds") == 3600.0


def test_parse_metric_handles_float_values() -> None:
    text = "kith_rtt_ms 12.5\n"
    assert _parse_metric(text, "kith_rtt_ms") == 12.5


# ---------------------------------------------------------------------------
# assert_server_metric
# ---------------------------------------------------------------------------


def test_assert_server_metric_returns_satisfying_value() -> None:
    control = _FakeServerControl(metrics_text="kith_connections 42\n")

    result = asyncio.run(
        assert_server_metric(control, "kith_connections", lambda v: v >= 40, timeout_s=0.1)
    )

    assert result == 42.0


def test_assert_server_metric_raises_on_timeout() -> None:
    control = _FakeServerControl(metrics_text="kith_connections 5\n")

    with pytest.raises(ScenarioAssertionError, match="kith_connections not satisfying"):
        asyncio.run(
            assert_server_metric(control, "kith_connections", lambda v: v >= 100, timeout_s=0.05)
        )


def test_assert_server_metric_raises_when_metric_missing() -> None:
    control = _FakeServerControl(metrics_text="other_metric 1\n")

    with pytest.raises(ScenarioAssertionError, match="kith_connections not satisfying"):
        asyncio.run(
            assert_server_metric(control, "kith_connections", lambda v: v > 0, timeout_s=0.05)
        )


def test_assert_server_metric_details_contain_name_and_value() -> None:
    control = _FakeServerControl(metrics_text="kith_connections 5\n")

    try:
        asyncio.run(
            assert_server_metric(control, "kith_connections", lambda v: v >= 100, timeout_s=0.01)
        )
    except ScenarioAssertionError as exc:
        assert exc.details["name"] == "kith_connections"
        assert exc.details["value"] == 5.0
    else:
        pytest.fail("expected ScenarioAssertionError")


# ---------------------------------------------------------------------------
# assert_server_state
# ---------------------------------------------------------------------------


def test_assert_server_state_returns_satisfying_state() -> None:
    control = _FakeServerControl()
    control.set_state("/api/v1/health", {"status": "ok", "uptime_sec": 100})

    result = asyncio.run(
        assert_server_state(
            control, "/api/v1/health", lambda s: s.get("status") == "ok", timeout_s=0.1
        )
    )

    assert result["status"] == "ok"


def test_assert_server_state_raises_on_timeout() -> None:
    control = _FakeServerControl()
    control.set_state("/api/v1/health", {"status": "starting"})

    with pytest.raises(ScenarioAssertionError, match="not satisfying predicate"):
        asyncio.run(
            assert_server_state(
                control, "/api/v1/health", lambda s: s.get("status") == "ok", timeout_s=0.05
            )
        )


def test_assert_server_state_details_contain_path_and_state() -> None:
    control = _FakeServerControl()
    control.set_state("/api/v1/zones", {"zones": []})

    try:
        asyncio.run(
            assert_server_state(
                control, "/api/v1/zones", lambda s: bool(s.get("zones")), timeout_s=0.01
            )
        )
    except ScenarioAssertionError as exc:
        assert exc.details["path"] == "/api/v1/zones"
        assert exc.details["state"] == {"zones": []}
    else:
        pytest.fail("expected ScenarioAssertionError")


# ---------------------------------------------------------------------------
# ServerControl protocol
# ---------------------------------------------------------------------------


def test_server_control_protocol_is_runtime_checkable() -> None:
    assert isinstance(_FakeServerControl(), ServerControl)


# ---------------------------------------------------------------------------
# ScenarioAssertionError
# ---------------------------------------------------------------------------


def test_assertion_error_details_default_empty() -> None:
    exc = ScenarioAssertionError("fail")
    assert exc.details == {}


def test_assertion_error_carries_details() -> None:
    exc = ScenarioAssertionError("fail", details={"key": "value", "n": 42})
    assert exc.details["key"] == "value"
    assert exc.details["n"] == 42


def test_assertion_error_is_exception_subclass() -> None:
    assert issubclass(ScenarioAssertionError, Exception)


def test_assertion_error_does_not_shadow_builtin() -> None:
    assert ScenarioAssertionError.__name__ == "ScenarioAssertionError"
    assert not issubclass(ScenarioAssertionError, AssertionError)
