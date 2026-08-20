"""Unit tests for the scenario DSL and step runner."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from typing import Any

import pytest
from tools.agent import scenario
from tools.agent.ahc import ClientEvent, ClientStatus, KithClientError
from tools.agent.event_correlator import EventCorrelator
from tools.agent.scenario import (
    ConnectAction,
    DelayAction,
    DisconnectAction,
    RegisterTypeAction,
    Scenario,
    ScenarioContext,
    ScenarioHost,
    ScenarioStatus,
    StepDiagnosis,
    StepStatus,
    SubmitAction,
    get_scenario,
    run_scenario,
    step,
)


def _event(type_id: int, payload: bytes = b"") -> ClientEvent:
    return ClientEvent(
        ts_mono_ns=1_000_000,
        type_id=type_id,
        correlation_id=0,
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
    """In-memory ScenarioHost that records every call and is programmable."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = []
        self.registered: dict[str, tuple[str, int]] = {}
        self.started: set[str] = set()
        self.stopped: set[str] = set()
        self.types: dict[str, tuple[str, int]] = {}
        self.submits: list[SubmitAction] = []
        self._events: dict[str, list[ClientEvent]] = {}
        self._state: dict[str, ClientStatus] = {}
        self._next_event_index: dict[str, int] = {}
        self._principals: dict[str, int] = {}
        self._server_state: dict[str, object] = {}

    def set_events(self, client: str, events: list[ClientEvent]) -> None:
        self._events[client] = list(events)
        self._next_event_index[client] = 0

    def set_state(self, client: str, status: ClientStatus) -> None:
        self._state[client] = status

    def _record(self, name: str, args: tuple[Any, ...], kwargs: dict[str, Any]) -> None:
        self.calls.append((name, args, kwargs))

    async def register_client(self, name: str, *, host: str, port: int) -> None:
        self._record("register_client", (name,), {"host": host, "port": port})
        self.registered[name] = (host, port)

    async def start_client(self, name: str) -> None:
        self._record("start_client", (name,), {})
        self.started.add(name)

    async def stop_client(self, name: str) -> None:
        self._record("stop_client", (name,), {})
        self.stopped.add(name)

    async def submit(
        self,
        client: str,
        type_id: int,
        payload: bytes = b"",
        *,
        flags: int = 0,
        correlation_id: int = 0,
    ) -> int:
        action = SubmitAction(client, type_id, payload, flags, correlation_id)
        self._record(
            "submit",
            (client, type_id, payload),
            {
                "flags": flags,
                "correlation_id": correlation_id,
            },
        )
        self.submits.append(action)
        return len(self.submits)

    async def register_type(self, client: str, name: str, type_id: int) -> None:
        self._record("register_type", (client, name, type_id), {})
        self.types[client] = (name, type_id)

    async def client_state(self, client: str) -> ClientStatus:
        self._record("client_state", (client,), {})
        return self._state.get(client, _status())

    async def recent_events(self, client: str, count: int = 64) -> list[ClientEvent]:
        self._record("recent_events", (client, count), {})
        return list(self._events.get(client, []))[-count:]

    async def wait_for_event(
        self, client: str, type_id: int, *, timeout_s: float
    ) -> ClientEvent | None:
        self._record("wait_for_event", (client, type_id), {"timeout_s": timeout_s})
        idx = self._next_event_index.get(client, 0)
        for ev in self._events.get(client, [])[idx:]:
            self._next_event_index[client] = idx + 1
            if ev.type_id == type_id:
                return ev
        return None

    async def wait_for_state(
        self,
        client: str,
        predicate: Any,
        *,
        timeout_s: float,
    ) -> bool:
        self._record("wait_for_state", (client, predicate), {"timeout_s": timeout_s})
        status = self._state.get(client, _status())
        return bool(predicate(status))

    async def server_command(
        self,
        path: str,
        body: Mapping[str, object] | None = None,
    ) -> Mapping[str, object]:
        self._record("server_command", (path,), {"body": dict(body) if body else {}})
        return {}

    def client_principal(self, name: str) -> int | None:
        return self._principals.get(name)

    async def server_state(self, path: str) -> Mapping[str, object]:
        self._record("server_state", (path,), {})
        return self._server_state


@pytest.fixture(autouse=True)
def _restore_registry() -> Any:
    """Snapshot/restore the global scenario registry around each test."""
    snapshot = dict(scenario._REGISTRY)
    yield
    scenario._REGISTRY.clear()
    scenario._REGISTRY.update(snapshot)


def test_step_decorator_sets_marker_and_returns_unchanged() -> None:
    def func() -> None:
        pass

    marked = step(func)
    assert marked is func
    assert getattr(marked, "_is_step", False) is True


def test_step_decorator_preserves_signature() -> None:
    async def step_one(self: Scenario, ctx: ScenarioContext) -> None:
        del self, ctx

    marked = step(step_one)
    assert marked.__name__ == "step_one"


def test_scenario_default_name_is_class_name() -> None:
    class MyNamedScenario(Scenario):
        pass

    assert MyNamedScenario.scenario_name == "MyNamedScenario"
    assert get_scenario("MyNamedScenario") is MyNamedScenario


def test_scenario_custom_name_respected() -> None:
    class _Custom(Scenario):
        scenario_name = "custom-scenario"

    assert get_scenario("custom-scenario") is _Custom


def test_get_scenario_missing_raises_with_available_list() -> None:
    class _ListedScenario(Scenario):
        scenario_name = "listed"

    with pytest.raises(KeyError, match=r"no scenario named 'missing'; available: .*\blisted\b"):
        get_scenario("missing")


def test_all_scenarios_returns_copy() -> None:
    class _CopyScenario(Scenario):
        scenario_name = "copy-scenario"

    snapshot = scenario.all_scenarios()
    assert "copy-scenario" in snapshot
    snapshot.clear()
    assert "copy-scenario" in scenario._REGISTRY


def test_scenario_collects_steps_in_source_order() -> None:
    class _OrderedScenario(Scenario):
        scenario_name = "ordered-steps"

        @step
        async def step_b(self, ctx: ScenarioContext) -> None:
            del ctx

        @step
        async def step_a(self, ctx: ScenarioContext) -> None:
            del ctx

        @step
        async def step_c(self, ctx: ScenarioContext) -> None:
            del ctx

        async def not_a_step(self, ctx: ScenarioContext) -> None:
            del ctx

    instance = _OrderedScenario()
    assert [fn.__name__ for fn in instance._steps] == ["step_b", "step_a", "step_c"]


def test_builder_appends_typed_actions_and_chains() -> None:
    class _BuilderScenario(Scenario):
        scenario_name = "builder-chains"

        def build(self, ctx: ScenarioContext) -> None:
            del ctx
            self.client("alice").connect("10.0.0.1", 7000).register_type("hello", 7)
            self.client("alice").submit(7, b"hi", correlation_id=42)
            self.client("alice").delay(0.25)
            self.client("alice").disconnect()

    instance = _BuilderScenario()
    ctx = ScenarioContext(host=_FakeHost(), name="builder-chains")
    instance.build(ctx)

    kinds = [type(a).__name__ for a in instance._actions]
    assert kinds == [
        "ConnectAction",
        "RegisterTypeAction",
        "SubmitAction",
        "DelayAction",
        "DisconnectAction",
    ]
    assert isinstance(instance._actions[0], ConnectAction)
    assert instance._actions[0].host == "10.0.0.1"
    assert instance._actions[0].port == 7000
    assert isinstance(instance._actions[3], DelayAction)
    assert instance._actions[3].seconds == 0.25
    submit = instance._actions[2]
    assert isinstance(submit, SubmitAction)
    assert submit.correlation_id == 42
    assert submit.payload == b"hi"


def test_run_scenario_passing_runs_build_actions_and_steps() -> None:
    class _PassScenario(Scenario):
        scenario_name = "passing-scenario"

        def build(self, ctx: ScenarioContext) -> None:
            del ctx
            self.client("alice").connect().register_type("hello", 7)
            self.client("alice").submit(7, b"hi")

        @step
        async def step_check_connected(self, ctx: ScenarioContext) -> None:
            ok = await ctx.wait_for_state("alice", lambda s: s.connected, timeout_s=1.0)
            assert ok, "alice should be connected"

        @step
        async def step_check_event(self, ctx: ScenarioContext) -> None:
            ev = await ctx.wait_for_event("alice", 8, timeout_s=1.0)
            assert ev is not None
            assert ev.payload == b"pong"

    host = _FakeHost()
    host.set_state("alice", _status(connected=True))
    host.set_events("alice", [_event(8, b"pong")])

    result = asyncio.run(run_scenario(_PassScenario(), host))

    assert result.status is ScenarioStatus.PASSED
    assert result.passed
    assert [s.name for s in result.steps] == ["step_check_connected", "step_check_event"]
    assert all(s.status is StepStatus.PASSED for s in result.steps)
    assert "alice" in host.registered
    assert host.registered["alice"] == ("127.0.0.1", 7777)
    assert host.started == {"alice"}
    assert len(host.submits) == 1
    assert host.submits[0].type_id == 7


def test_run_scenario_step_failure_records_error_and_continues() -> None:
    class _PartialFailScenario(Scenario):
        scenario_name = "partial-fail-scenario"

        @step
        async def step_pass(self, ctx: ScenarioContext) -> None:
            del ctx

        @step
        async def step_fail(self, ctx: ScenarioContext) -> None:
            del ctx
            raise AssertionError("boom")

        @step
        async def step_after_fail(self, ctx: ScenarioContext) -> None:
            del ctx

    result = asyncio.run(run_scenario(_PartialFailScenario(), _FakeHost()))

    assert result.status is ScenarioStatus.FAILED
    assert not result.passed
    statuses = [s.status for s in result.steps]
    assert statuses == [StepStatus.PASSED, StepStatus.FAILED, StepStatus.PASSED]
    failed = result.steps[1]
    assert failed.error is not None
    assert "AssertionError" in failed.error
    assert "boom" in failed.error


def test_run_scenario_action_failure_aborts_before_steps() -> None:
    class _ActionFailScenario(Scenario):
        scenario_name = "action-fail-scenario"

        def build(self, ctx: ScenarioContext) -> None:
            del ctx
            self.client("alice").connect("bad-host", 9999)

        @step
        async def step_never(self, ctx: ScenarioContext) -> None:
            raise RuntimeError("should not run")

    class _BoomHost(_FakeHost):
        async def start_client(self, name: str) -> None:
            raise OSError("connection refused")

    result = asyncio.run(run_scenario(_ActionFailScenario(), _BoomHost()))

    assert result.status is ScenarioStatus.FAILED
    assert result.steps == ()
    assert result.error is not None
    assert "OSError" in result.error
    assert "connection refused" in result.error


def test_run_scenario_build_failure_records_error() -> None:
    class _BuildFailScenario(Scenario):
        scenario_name = "build-fail-scenario"

        def build(self, ctx: ScenarioContext) -> None:
            del ctx
            raise ValueError("bad wiring")

        @step
        async def step_never(self, ctx: ScenarioContext) -> None:
            raise RuntimeError("should not run")

    result = asyncio.run(run_scenario(_BuildFailScenario(), _FakeHost()))

    assert result.status is ScenarioStatus.FAILED
    assert result.steps == ()
    assert result.error is not None
    assert "ValueError" in result.error
    assert "bad wiring" in result.error


def test_run_scenario_deadline_fails_hung_step_and_keeps_earlier_steps() -> None:
    class _HungScenario(Scenario):
        scenario_name = "hung-scenario"

        @step
        async def step_fast(self, ctx: ScenarioContext) -> None:
            del ctx

        @step
        async def step_hangs(self, ctx: ScenarioContext) -> None:
            del ctx
            await asyncio.sleep(30.0)

    result = asyncio.run(run_scenario(_HungScenario(), _FakeHost(), deadline_s=0.1))

    assert result.status is ScenarioStatus.FAILED
    assert [s.status for s in result.steps] == [StepStatus.PASSED]
    assert result.error is not None
    assert "deadline" in result.error
    assert result.duration_s < 5.0


def test_run_scenario_deadline_resolves_from_scenario_attribute() -> None:
    class _HungBuildScenario(Scenario):
        scenario_name = "hung-build-scenario"

        deadline_s = 0.1

        async def build(self, ctx: ScenarioContext) -> None:
            del ctx
            await asyncio.sleep(30.0)

    result = asyncio.run(run_scenario(_HungBuildScenario(), _FakeHost()))

    assert result.status is ScenarioStatus.FAILED
    assert result.steps == ()
    assert result.error is not None
    assert "deadline" in result.error


def test_run_scenario_supports_sync_and_async_steps() -> None:
    class _MixedScenario(Scenario):
        scenario_name = "mixed-sync-async"

        @step
        def step_sync(self, ctx: ScenarioContext) -> None:
            del ctx

        @step
        async def step_async(self, ctx: ScenarioContext) -> None:
            del ctx

    result = asyncio.run(run_scenario(_MixedScenario(), _FakeHost()))

    assert result.status is ScenarioStatus.PASSED
    assert [s.status for s in result.steps] == [StepStatus.PASSED, StepStatus.PASSED]


def test_run_scenario_supports_async_build() -> None:
    class _AsyncBuildScenario(Scenario):
        scenario_name = "async-build"

        async def build(self, ctx: ScenarioContext) -> None:
            await ctx.register_type("alice", "hello", 7)

        @step
        async def step_after_build(self, ctx: ScenarioContext) -> None:
            del ctx

    host = _FakeHost()
    result = asyncio.run(run_scenario(_AsyncBuildScenario(), host))

    assert result.status is ScenarioStatus.PASSED
    assert host.types == {"alice": ("hello", 7)}


def test_scenario_host_protocol_is_runtime_checkable() -> None:
    assert isinstance(_FakeHost(), ScenarioHost)


def test_context_submit_returns_command_id() -> None:
    host = _FakeHost()
    ctx = ScenarioContext(host=host, name="ctx-submit")

    cmd_id = asyncio.run(ctx.submit("alice", 7, b"hi", flags=1, correlation_id=9))

    assert cmd_id == 1
    assert host.submits[0].flags == 1
    assert host.submits[0].correlation_id == 9


def test_context_wait_for_event_returns_none_on_timeout() -> None:
    host = _FakeHost()
    host.set_events("alice", [_event(8)])
    ctx = ScenarioContext(host=host, name="ctx-timeout")

    found = asyncio.run(ctx.wait_for_event("alice", 999, timeout_s=0.01))
    assert found is None


def test_delay_action_sleeps() -> None:
    class _DelayScenario(Scenario):
        scenario_name = "delay-action"

        def build(self, ctx: ScenarioContext) -> None:
            del ctx
            self.client("alice").delay(0.05)

        @step
        async def step_trivial(self, ctx: ScenarioContext) -> None:
            del ctx

    import time as _time

    t0 = _time.perf_counter()
    result = asyncio.run(run_scenario(_DelayScenario(), _FakeHost()))
    elapsed = _time.perf_counter() - t0

    assert result.status is ScenarioStatus.PASSED
    assert elapsed >= 0.05


def test_disconnect_action_calls_stop_client() -> None:
    class _DisconnectScenario(Scenario):
        scenario_name = "disconnect-action"

        def build(self, ctx: ScenarioContext) -> None:
            del ctx
            self.client("alice").connect()
            self.client("alice").disconnect()

    host = _FakeHost()
    asyncio.run(run_scenario(_DisconnectScenario(), host))

    assert host.started == {"alice"}
    assert host.stopped == {"alice"}


def test_register_type_action_records_host_call() -> None:
    class _RegScenario(Scenario):
        scenario_name = "register-type-action"

        def build(self, ctx: ScenarioContext) -> None:
            del ctx
            self.client("alice").register_type("hello", 7)

    host = _FakeHost()
    asyncio.run(run_scenario(_RegScenario(), host))

    assert host.types["alice"] == ("hello", 7)


def test_action_union_covers_all_kinds() -> None:
    from tools.agent.scenario import Action

    samples: list[Action] = [
        ConnectAction("a"),
        DisconnectAction("a"),
        RegisterTypeAction("a", "n", 1),
        SubmitAction("a", 1),
        DelayAction(0.1),
    ]
    assert all(isinstance(s, Action) for s in samples)


# ---------------------------------------------------------------------------
# step diagnosis
# ---------------------------------------------------------------------------


class _DiagnosisHost(_FakeHost):
    """Fake host exposing the orchestrator's runner-level surfaces."""

    def __init__(self) -> None:
        super().__init__()
        self.correlator = EventCorrelator()

    def client_names(self) -> list[str]:
        return list(self.registered)

    def feed_correlator(self, source: str, events: list[ClientEvent]) -> None:
        self.correlator.ingest_client_batch(source, events)


class _StepFailScenario(Scenario):
    scenario_name = "diagnosis-probe"

    @step
    async def step_fail(self, ctx: ScenarioContext) -> None:
        del ctx
        raise AssertionError("boom")


def test_step_failure_attaches_runner_diagnosis() -> None:
    host = _DiagnosisHost()
    host.registered["alice"] = ("127.0.0.1", 7777)
    host.set_state("alice", _status(connected=False))
    tail = [
        ClientEvent(ts_mono_ns=10, type_id=5, correlation_id=42, payload=b"\x01"),
        ClientEvent(ts_mono_ns=20, type_id=6, correlation_id=0, payload=b"\x02"),
    ]
    host.set_events("alice", tail)
    host.feed_correlator("alice", tail)

    result = asyncio.run(run_scenario(_StepFailScenario(), host))

    failed = result.steps[0]
    assert failed.status is StepStatus.FAILED
    diagnosis = failed.diagnosis
    assert isinstance(diagnosis, StepDiagnosis)
    assert diagnosis.statuses["alice"] == _status(connected=False)
    assert diagnosis.event_tails["alice"] == tail
    chain = diagnosis.correlation_chains[42]
    assert [e.correlation_id for e in chain] == [42]
    assert all(e.source == "alice" for e in chain)


def test_step_failure_without_runner_surfaces_attaches_no_diagnosis() -> None:
    result = asyncio.run(run_scenario(_StepFailScenario(), _FakeHost()))

    assert result.steps[0].status is StepStatus.FAILED
    assert result.steps[0].diagnosis is None


def test_step_failure_diagnosis_skips_a_failing_client() -> None:
    class _DeadClientHost(_DiagnosisHost):
        async def client_state(self, client: str) -> ClientStatus:
            if client == "dead":
                raise KithClientError(1, "client stopped")
            return await super().client_state(client)

    host = _DeadClientHost()
    host.registered = {"alice": ("127.0.0.1", 7777), "dead": ("127.0.0.1", 7777)}
    host.set_state("alice", _status())
    host.set_events("alice", [_event(8, b"pong")])

    result = asyncio.run(run_scenario(_StepFailScenario(), host))

    diagnosis = result.steps[0].diagnosis
    assert diagnosis is not None
    assert set(diagnosis.statuses) == {"alice"}
    assert set(diagnosis.event_tails) == {"alice"}


def test_step_failure_diagnosis_without_correlator_keeps_client_evidence() -> None:
    class _NamesHost(_FakeHost):
        def client_names(self) -> list[str]:
            return list(self.registered)

    host = _NamesHost()
    host.registered["alice"] = ("127.0.0.1", 7777)
    host.set_state("alice", _status())
    host.set_events("alice", [_event(8, b"pong")])

    result = asyncio.run(run_scenario(_StepFailScenario(), host))

    diagnosis = result.steps[0].diagnosis
    assert diagnosis is not None
    assert diagnosis.statuses["alice"].connected
    assert diagnosis.event_tails["alice"] == [_event(8, b"pong")]
    assert diagnosis.correlation_chains == {}


def test_step_failure_diagnosis_gather_timeout_degrades_to_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _StalledHost(_DiagnosisHost):
        async def client_state(self, client: str) -> ClientStatus:
            del client
            await asyncio.sleep(30.0)
            raise AssertionError("unreachable")

    monkeypatch.setattr(scenario, "_DIAGNOSIS_TIMEOUT_S", 0.05)
    host = _StalledHost()
    host.registered["alice"] = ("127.0.0.1", 7777)

    result = asyncio.run(run_scenario(_StepFailScenario(), host))

    failed = result.steps[0]
    assert failed.status is StepStatus.FAILED
    assert failed.error is not None
    assert "AssertionError" in failed.error
    assert failed.diagnosis is None
