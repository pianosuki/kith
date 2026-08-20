"""Unit tests for the scenario orchestrator."""

from __future__ import annotations

import asyncio
import time
from collections import deque
from collections.abc import Callable, Coroutine, Mapping
from typing import cast

import pytest
from tools.agent.ahc import ClientEvent, ClientStatus, WindowEvidence
from tools.agent.event_correlator import EventCorrelator, EventKind
from tools.agent.orchestrator import Orchestrator
from tools.agent.scenario import ScenarioHost
from tools.agent.server_control import ServerControlError


def _event(
    type_id: int,
    payload: bytes = b"",
    correlation_id: int = 0,
    ts_mono_ns: int | None = None,
) -> ClientEvent:
    return ClientEvent(
        ts_mono_ns=ts_mono_ns if ts_mono_ns is not None else time.monotonic_ns(),
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


class _FakeClient:
    """An in-memory _ManagedClient for orchestrator tests."""

    def __init__(
        self,
        instance_id: str,
        host: str,
        port: int,
        *,
        window: int = 256,
    ) -> None:
        self.instance_id = instance_id
        self.host = host
        self.port = port
        self.principal_id = 0
        self._events: deque[ClientEvent] = deque(maxlen=window)
        self._status = _status(connected=False)
        self._started = False
        self._stopped = False
        self._next_cmd = 1
        self.types: dict[str, int] = {}

    def push(self, event: ClientEvent) -> None:
        self._events.append(event)

    def set_status(self, status: ClientStatus) -> None:
        self._status = status

    async def start(self) -> None:
        self._started = True
        self._status = _status(connected=True)

    async def stop(self) -> None:
        self._stopped = True
        self._started = False

    def submit(
        self,
        type_id: int,
        payload: bytes = b"",
        *,
        flags: int = 0,
        correlation_id: int = 0,
    ) -> int:
        cmd = self._next_cmd
        self._next_cmd += 1
        return cmd

    def register_type(self, name: str, type_id: int) -> None:
        self.types[name] = type_id

    def query_status(self) -> ClientStatus:
        return self._status

    def recent_events(self, count: int = 64) -> list[ClientEvent]:
        items = list(self._events)
        return items[-count:] if count < len(items) else items

    def evidence_frames(self) -> list[ClientEvent]:
        return []

    def evidence_span_ns(self) -> int | None:
        return None

    def begin_view_window(self) -> None: ...

    def seal_view_window(self, deadline_ns: int) -> None:
        del deadline_ns

    def view_window(self) -> WindowEvidence | None:
        return None

    def last_frame_age_ns(self) -> int | None:
        return None

    def replication_count(self) -> int:
        return sum(1 for event in self._events if event.type_id == 7)


def _fake_factory(window: int = 256) -> Callable[[str, str, int], _FakeClient]:
    def factory(instance_id: str, host: str, port: int) -> _FakeClient:
        return _FakeClient(instance_id, host, port, window=window)

    return factory


class _FakeServerControl:
    """An in-memory ServerControl for orchestrator tests."""

    def __init__(self) -> None:
        self.responses: dict[str, object] = {}
        self.error_first: set[str] = set()
        self.calls: list[str] = []

    async def metrics(self) -> str:
        return ""

    async def get(self, path: str) -> Mapping[str, object]:
        self.calls.append(path)
        if path in self.error_first and len(self.calls) <= 2:
            raise ServerControlError("transient")
        value = self.responses.get(path, {})
        if isinstance(value, Mapping):
            return dict(value)
        return {}

    async def post(self, path: str, body: Mapping[str, object]) -> Mapping[str, object]:
        self.calls.append(path)
        return {}


def _new_orchestrator(
    *,
    server: _FakeServerControl | None = None,
    server_events_path: str | None = None,
    window: int = 256,
    ingest_interval_s: float = 0.005,
) -> Orchestrator:
    # A large eviction window so the test's artificial small timestamps
    # (used only for ordering) are not dropped between ingest calls.
    correlator = EventCorrelator(window_s=1_000_000.0)
    return Orchestrator(
        correlator=correlator,
        server_control=server,
        server_events_path=server_events_path,
        ahc_factory=_fake_factory(window=window),
        ingest_interval_s=ingest_interval_s,
    )


def _client(orch: Orchestrator, name: str) -> _FakeClient:
    """Retrieve a registered fake client (tests drive it directly)."""
    return cast(_FakeClient, orch._clients[name])


def _ingest_task(orch: Orchestrator) -> asyncio.Task[None] | None:
    return orch._ingest_task


def _run(coro: Coroutine[None, None, None]) -> None:
    asyncio.run(coro)


# ---------------------------------------------------------------------------
# ScenarioHost surface
# ---------------------------------------------------------------------------


def test_satisfies_scenario_host_protocol() -> None:
    orch = _new_orchestrator()
    assert isinstance(orch, ScenarioHost)


def test_default_correlator_created() -> None:
    orch = _new_orchestrator()
    assert orch.correlator is not None
    assert orch.correlator.event_count() == 0


def test_register_start_stop_client() -> None:
    async def run() -> None:
        orch = _new_orchestrator()
        await orch.register_client("alice", host="127.0.0.1", port=9000)
        await orch.start_client("alice")
        client = _client(orch, "alice")
        assert client._started
        await orch.stop_client("alice")
        assert client._stopped

    _run(run())


def test_register_duplicate_raises() -> None:
    async def run() -> None:
        orch = _new_orchestrator()
        await orch.register_client("alice", host="h", port=1)
        with pytest.raises(KeyError, match="already registered"):
            await orch.register_client("alice", host="h", port=1)

    _run(run())


def test_unknown_client_raises() -> None:
    async def run() -> None:
        orch = _new_orchestrator()
        with pytest.raises(KeyError, match="no client"):
            await orch.start_client("ghost")

    _run(run())


def test_submit_returns_cmd_id() -> None:
    async def run() -> None:
        orch = _new_orchestrator()
        await orch.register_client("a", host="h", port=1)
        cmd = await orch.submit("a", 8, b"hi")
        assert cmd == 1

    _run(run())


def test_register_type_records() -> None:
    async def run() -> None:
        orch = _new_orchestrator()
        await orch.register_client("a", host="h", port=1)
        await orch.register_type("a", "move", 8)
        assert _client(orch, "a").types == {"move": 8}

    _run(run())


def test_client_state_returns_status() -> None:
    async def run() -> None:
        orch = _new_orchestrator()
        await orch.register_client("a", host="h", port=1)
        status = await orch.client_state("a")
        assert status.connected is False

    _run(run())


def test_recent_events_returns_list() -> None:
    async def run() -> None:
        orch = _new_orchestrator()
        await orch.register_client("a", host="h", port=1)
        _client(orch, "a").push(_event(8))
        events = await orch.recent_events("a")
        assert len(events) == 1
        assert events[0].type_id == 8

    _run(run())


# ---------------------------------------------------------------------------
# wait_for_event / wait_for_state
# ---------------------------------------------------------------------------


def test_wait_for_event_returns_matching() -> None:
    async def run() -> None:
        orch = _new_orchestrator()
        await orch.register_client("a", host="h", port=1)
        client = _client(orch, "a")
        client.push(_event(1, ts_mono_ns=1))
        client.push(_event(8, b"x", ts_mono_ns=2))
        event = await orch.wait_for_event("a", 8, timeout_s=0.1)
        assert event is not None
        assert event.payload == b"x"

    _run(run())


def test_wait_for_event_skips_non_matching_then_returns_next() -> None:
    async def run() -> None:
        orch = _new_orchestrator()
        await orch.register_client("a", host="h", port=1)
        client = _client(orch, "a")
        client.push(_event(1, ts_mono_ns=1))
        client.push(_event(8, b"first", ts_mono_ns=2))
        client.push(_event(8, b"second", ts_mono_ns=3))
        first = await orch.wait_for_event("a", 8, timeout_s=0.0)
        second = await orch.wait_for_event("a", 8, timeout_s=0.0)
        third = await orch.wait_for_event("a", 8, timeout_s=0.0)
        assert first is not None and first.payload == b"first"
        assert second is not None and second.payload == b"second"
        assert third is None

    _run(run())


def test_wait_for_event_timeout_returns_none() -> None:
    async def run() -> None:
        orch = _new_orchestrator()
        await orch.register_client("a", host="h", port=1)
        event = await orch.wait_for_event("a", 8, timeout_s=0.05)
        assert event is None

    _run(run())


def test_wait_for_state_true() -> None:
    async def run() -> None:
        orch = _new_orchestrator()
        await orch.register_client("a", host="h", port=1)
        _client(orch, "a").set_status(_status(connected=True))
        ok = await orch.wait_for_state("a", lambda s: s.connected, timeout_s=0.1)
        assert ok is True

    _run(run())


def test_wait_for_state_false_on_timeout() -> None:
    async def run() -> None:
        orch = _new_orchestrator()
        await orch.register_client("a", host="h", port=1)
        ok = await orch.wait_for_state("a", lambda s: s.connected, timeout_s=0.05)
        assert ok is False

    _run(run())


# ---------------------------------------------------------------------------
# ingest loop
# ---------------------------------------------------------------------------


def test_ingest_loop_feeds_client_events() -> None:
    async def run() -> None:
        orch = _new_orchestrator()
        await orch.register_client("a", host="h", port=1)
        client = _client(orch, "a")
        client.push(_event(8, b"one", correlation_id=11, ts_mono_ns=1))
        client.push(_event(8, b"two", correlation_id=11, ts_mono_ns=2))
        orch.start()
        await asyncio.sleep(0.03)
        await orch.shutdown()
        correlated = orch.correlator.find_by_correlation(11)
        assert len(correlated) == 2
        assert all(c.kind is EventKind.CLIENT for c in correlated)

    _run(run())


def test_ingest_loop_dedup_tail_follow() -> None:
    async def run() -> None:
        orch = _new_orchestrator()
        await orch.register_client("a", host="h", port=1)
        client = _client(orch, "a")
        client.push(_event(8, b"one", ts_mono_ns=1))
        orch.start()
        await asyncio.sleep(0.02)
        first = orch.correlator.event_count()
        client.push(_event(8, b"two", ts_mono_ns=2))
        client.push(_event(8, b"three", ts_mono_ns=3))
        await asyncio.sleep(0.03)
        await orch.shutdown()
        # Only the two new events are ingested after the first poll; the
        # first event is not re-ingested (tail-follow, no duplicate).
        assert orch.correlator.event_count() == 3
        assert first == 1

    _run(run())


def test_ingest_loop_handles_rotation() -> None:
    """When the last-ingested event rotates out, ingest the whole window."""

    async def run() -> None:
        orch = _new_orchestrator(window=2)
        await orch.register_client("a", host="h", port=1)
        client = _client(orch, "a")
        client.push(_event(8, b"e0", ts_mono_ns=1))
        client.push(_event(8, b"e1", ts_mono_ns=2))
        orch.start()
        await asyncio.sleep(0.02)
        # Push enough to rotate e1 out of the maxlen=2 window.
        client.push(_event(8, b"e2", ts_mono_ns=3))
        client.push(_event(8, b"e3", ts_mono_ns=4))
        client.push(_event(8, b"e4", ts_mono_ns=5))
        await asyncio.sleep(0.03)
        await orch.shutdown()
        payloads = {c.payload for c in orch.correlator.find_by_type(8)}
        # e3 and e4 (the current window after rotation) are present.
        assert b"e3" in payloads
        assert b"e4" in payloads

    _run(run())


def test_ingest_loop_feeds_server_events() -> None:
    async def run() -> None:
        server = _FakeServerControl()
        server.responses["/api/events"] = {
            "events": [
                {"correlation_id": 99, "type_id": 4, "detail": "spawned"},
                {"correlation_id": 99, "type_id": 5, "detail": "published"},
            ]
        }
        orch = _new_orchestrator(server=server, server_events_path="/api/events")
        await orch.register_client("a", host="h", port=1)
        orch.start()
        await asyncio.sleep(0.03)
        await orch.shutdown()
        correlated = orch.correlator.find_by_correlation(99)
        server_events = [c for c in correlated if c.kind is EventKind.SERVER]
        assert len(server_events) == 2

    _run(run())


def test_ingest_loop_swallows_server_control_error() -> None:
    async def run() -> None:
        server = _FakeServerControl()
        server.error_first.add("/api/events")
        server.responses["/api/events"] = {"events": [{"correlation_id": 5, "type_id": 1}]}
        orch = _new_orchestrator(server=server, server_events_path="/api/events")
        await orch.register_client("a", host="h", port=1)
        orch.start()
        # The first server poll raises ServerControlError; the loop must
        # survive and ingest on a subsequent poll.
        await asyncio.sleep(0.05)
        await orch.shutdown()
        assert orch.correlator.correlation_count() >= 1

    _run(run())


def test_no_server_control_skips_server_ingest() -> None:
    async def run() -> None:
        orch = _new_orchestrator()
        await orch.register_client("a", host="h", port=1)
        orch.start()
        await asyncio.sleep(0.02)
        await orch.shutdown()
        assert orch.server_control is None

    _run(run())


# ---------------------------------------------------------------------------
# lifecycle
# ---------------------------------------------------------------------------


def test_start_idempotent() -> None:
    async def run() -> None:
        orch = _new_orchestrator()
        orch.start()
        task = _ingest_task(orch)
        orch.start()
        assert _ingest_task(orch) is task
        await orch.shutdown()

    _run(run())


def test_shutdown_stops_clients_and_task() -> None:
    async def run() -> None:
        orch = _new_orchestrator()
        await orch.register_client("a", host="h", port=1)
        await orch.start_client("a")
        client = _client(orch, "a")
        orch.start()
        await orch.shutdown()
        assert client._stopped
        assert _ingest_task(orch) is None
        assert orch.client_names() == []
        assert not orch.is_running

    _run(run())


def test_server_control_property() -> None:
    async def run() -> None:
        server = _FakeServerControl()
        orch = _new_orchestrator(server=server, server_events_path="/x")
        assert orch.server_control is server
        await orch.shutdown()

    _run(run())
