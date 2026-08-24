"""Unit tests for the scenario runner and CLI."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Mapping
from typing import Any

import pytest
from tools.agent import runner
from tools.agent import scenario as scenario_mod
from tools.agent.ahc import ClientEvent, ClientStatus
from tools.agent.assertions import ScenarioAssertionError
from tools.agent.runner import _select_scenarios, run_scenarios
from tools.agent.scenario import (
    Scenario,
    ScenarioStatus,
    StepStatus,
    step,
)


@pytest.fixture(autouse=True)
def _restore_registry() -> Any:
    """Snapshot/restore the global scenario registry around each test."""
    snapshot = dict(scenario_mod._REGISTRY)
    yield
    scenario_mod._REGISTRY.clear()
    scenario_mod._REGISTRY.update(snapshot)


class _FakeHost:
    """A minimal ScenarioHost that records start/shutdown lifecycle."""

    def __init__(self) -> None:
        self.started = False
        self.stopped = False
        self.registered: list[str] = []

    def start(self) -> None:
        self.started = True

    async def shutdown(self) -> None:
        self.stopped = True

    async def register_client(self, name: str, *, host: str, port: int) -> None:
        self.registered.append(name)

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
        return 1

    async def register_type(self, client: str, name: str, type_id: int) -> None:
        del client, name, type_id

    async def client_state(self, client: str) -> ClientStatus:
        del client
        return ClientStatus(
            connected=True,
            bootstrap_state=2,
            bootstrap_step=3,
            bootstrap_step_count=3,
            rtt_last_ms=4,
            reconnect_attempts=0,
        )

    async def recent_events(self, client: str, count: int = 64) -> list[ClientEvent]:
        del client, count
        return []

    async def wait_for_event(
        self, client: str, type_id: int, *, timeout_s: float
    ) -> ClientEvent | None:
        del client, type_id, timeout_s
        return None

    async def wait_for_state(
        self,
        client: str,
        predicate: Any,
        *,
        timeout_s: float,
    ) -> bool:
        del client, predicate, timeout_s
        return True

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


def _fake_factory(hosts: list[_FakeHost]) -> runner.HostFactory:
    def factory() -> _FakeHost:
        host = _FakeHost()
        hosts.append(host)
        return host

    return factory


# ---------------------------------------------------------------------------
# scenario fixtures
# ---------------------------------------------------------------------------


class _PassScenario(Scenario):
    scenario_name = "runner-pass"

    @step
    def step_one(self, ctx: Any) -> None:
        del ctx


class _FailScenario(Scenario):
    scenario_name = "runner-fail"

    @step
    def step_bad(self, ctx: Any) -> None:
        del ctx
        raise AssertionError("boom")


class _AssertionFailScenario(Scenario):
    scenario_name = "runner-assertion-fail"

    @step
    async def step_assert(self, ctx: Any) -> None:
        del ctx
        raise ScenarioAssertionError(
            "no 5 event",
            details={"client": "c1", "type_id": 5},
        )


# ---------------------------------------------------------------------------
# run_scenarios
# ---------------------------------------------------------------------------


def test_run_scenarios_runs_each_scenario_and_returns_results() -> None:
    hosts: list[_FakeHost] = []
    results = asyncio.run(run_scenarios([_PassScenario, _FailScenario], _fake_factory(hosts)))
    assert len(results) == 2
    assert results[0].status is ScenarioStatus.PASSED
    assert results[1].status is ScenarioStatus.FAILED


def test_run_scenarios_builds_a_fresh_host_per_scenario() -> None:
    hosts: list[_FakeHost] = []
    asyncio.run(run_scenarios([_PassScenario, _PassScenario], _fake_factory(hosts)))
    assert len(hosts) == 2
    assert all(h.started for h in hosts)
    assert all(h.stopped for h in hosts)


def test_run_scenarios_carries_assertion_details_into_step_result() -> None:
    results = asyncio.run(run_scenarios([_AssertionFailScenario], _fake_factory([])))
    assert results[0].status is ScenarioStatus.FAILED
    failed_step = next(s for s in results[0].steps if s.status is StepStatus.FAILED)
    assert failed_step.details is not None
    assert failed_step.details["client"] == "c1"
    assert failed_step.details["type_id"] == 5
    assert "ScenarioAssertionError" in (failed_step.error or "")


def test_run_scenarios_empty_batch_returns_empty() -> None:
    results = asyncio.run(run_scenarios([], _fake_factory([])))
    assert results == []


# ---------------------------------------------------------------------------
# selection
# ---------------------------------------------------------------------------


def test_select_scenarios_no_filter_returns_all() -> None:
    selected = _select_scenarios(None)
    names = {cls.scenario_name for cls in selected}
    assert "runner-pass" in names
    assert "runner-fail" in names


def test_select_scenarios_substring_filter() -> None:
    selected = _select_scenarios("pass")
    names = [cls.scenario_name for cls in selected]
    assert names == ["runner-pass"]


def test_select_scenarios_no_match_returns_empty() -> None:
    assert _select_scenarios("nonexistent") == []


# ---------------------------------------------------------------------------
# CLI: --list and --filter (no server)
# ---------------------------------------------------------------------------


def test_cli_list_prints_registered_scenarios(capsys: pytest.CaptureFixture[str]) -> None:
    rc = runner.main(["--list"])
    out = capsys.readouterr().out
    assert rc == 0
    assert "runner-pass" in out
    assert "runner-fail" in out


def test_cli_list_empty_registry(capsys: pytest.CaptureFixture[str]) -> None:
    scenario_mod._REGISTRY.clear()
    rc = runner.main(["--list"])
    out = capsys.readouterr().out
    assert rc == 0
    assert "no scenarios registered" in out


def test_cli_no_match_returns_nonzero(capsys: pytest.CaptureFixture[str]) -> None:
    rc = runner.main(["--filter", "does-not-exist"])
    err = capsys.readouterr().err
    assert rc == 1
    assert "No scenarios match" in err
    # The error names what IS registered, so a typo'd filter is actionable.
    assert "available:" in err
    assert "runner-pass" in err


# ---------------------------------------------------------------------------
# CLI: run path with a stubbed orchestrator factory
# ---------------------------------------------------------------------------


def _patch_factory(monkeypatch: pytest.MonkeyPatch) -> list[_FakeHost]:
    hosts: list[_FakeHost] = []
    monkeypatch.setattr(runner, "_orchestrator_factory", lambda h, p: _fake_factory(hosts))
    return hosts


def test_cli_run_passing_scenario_exit_zero(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _patch_factory(monkeypatch)
    rc = runner.main(["--filter", "runner-pass", "--format", "json"])
    out = capsys.readouterr().out
    assert rc == 0
    data = json.loads(out)
    assert data["summary"]["failed"] == 0
    assert data["summary"]["passed"] == 1


def test_cli_run_failing_scenario_exit_one(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _patch_factory(monkeypatch)
    rc = runner.main(["--filter", "runner-fail", "--format", "md"])
    assert rc == 1
    out = capsys.readouterr().out
    assert "FAIL" in out


def test_cli_writes_output_files(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    _patch_factory(monkeypatch)
    json_path = tmp_path / "report.json"
    junit_path = tmp_path / "report.xml"
    md_path = tmp_path / "report.md"
    rc = runner.main(
        [
            "--filter",
            "runner-pass",
            "--format",
            "md",
            "--output-json",
            str(json_path),
            "--output-junit",
            str(junit_path),
            "--output-md",
            str(md_path),
        ]
    )
    assert rc == 0
    data = json.loads(json_path.read_text(encoding="utf-8"))
    assert data["summary"]["passed"] == 1
    assert "<testsuite" in junit_path.read_text(encoding="utf-8")
    assert "Scenario Report" in md_path.read_text(encoding="utf-8")


def test_cli_junit_format_to_stdout(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _patch_factory(monkeypatch)
    rc = runner.main(["--filter", "runner-pass", "--format", "junit"])
    out = capsys.readouterr().out
    assert rc == 0
    assert "<?xml" in out
    assert "<testsuites" in out


# ---------------------------------------------------------------------------
# watch mtime scanner
# ---------------------------------------------------------------------------


def test_scan_mtimes_records_files(tmp_path: Any) -> None:
    d = tmp_path / "pkg"
    d.mkdir()
    (d / "a.py").write_text("x = 1\n", encoding="utf-8")
    cache = d / "__pycache__"
    cache.mkdir()
    (cache / "a.cpython.pyc").write_bytes(b"\x00")
    snapshot = runner._scan_mtimes([str(d)])
    assert any(p.endswith("a.py") for p in snapshot)
    assert not any(p.endswith(".pyc") for p in snapshot)


def test_scan_mtimes_detects_change(tmp_path: Any) -> None:
    f = tmp_path / "a.py"
    f.write_text("x = 1\n", encoding="utf-8")
    before = runner._scan_mtimes([str(tmp_path)])
    f.write_text("x = 2\n", encoding="utf-8")
    after = runner._scan_mtimes([str(tmp_path)])
    assert before != after
