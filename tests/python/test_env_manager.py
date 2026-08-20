"""Unit tests for the Docker Compose lifecycle manager."""

from __future__ import annotations

import subprocess
from collections.abc import Sequence
from typing import Any

import pytest
from tools.agent.env_manager import ComposeError, EnvironmentManager


class _FakeRun:
    """Records subprocess.run calls and returns canned CompletedProcess results."""

    def __init__(self) -> None:
        self.calls: list[list[str]] = []
        self._responses: list[subprocess.CompletedProcess[str]] = []

    def queue(self, stdout: str = "", stderr: str = "", returncode: int = 0) -> None:
        self._responses.append(
            subprocess.CompletedProcess(
                args=[], returncode=returncode, stdout=stdout, stderr=stderr
            )
        )

    def __call__(self, cmd: Sequence[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        self.calls.append(list(cmd))
        if self._responses:
            result = self._responses.pop(0)
            return subprocess.CompletedProcess(
                args=list(cmd),
                returncode=result.returncode,
                stdout=result.stdout,
                stderr=result.stderr,
            )
        return subprocess.CompletedProcess(args=list(cmd), returncode=0, stdout="", stderr="")


@pytest.fixture
def fake_run(monkeypatch: pytest.MonkeyPatch) -> _FakeRun:
    fake = _FakeRun()
    monkeypatch.setattr("tools.agent.env_manager.subprocess.run", fake)
    return fake


# ---------------------------------------------------------------------------
# construction
# ---------------------------------------------------------------------------


def test_default_project_is_kith() -> None:
    mgr = EnvironmentManager()
    assert mgr.project_name == "kith"
    assert mgr.compose_file is None


def test_base_cmd_includes_file_and_project(fake_run: _FakeRun) -> None:
    fake_run.queue(stdout="")
    mgr = EnvironmentManager(compose_file="compose.yaml", project_name="custom")
    mgr.ps()
    cmd = fake_run.calls[-1]
    assert cmd[:2] == ["docker", "compose"]
    assert "-f" in cmd and "compose.yaml" in cmd
    assert "-p" in cmd and "custom" in cmd


# ---------------------------------------------------------------------------
# lifecycle verbs
# ---------------------------------------------------------------------------


def test_up_runs_detach_with_scale(fake_run: _FakeRun) -> None:
    fake_run.queue()
    mgr = EnvironmentManager()
    mgr.up(scale={"gateway": 3})
    cmd = fake_run.calls[-1]
    assert "up" in cmd
    assert "-d" in cmd
    assert "--scale" in cmd
    assert "gateway=3" in cmd


def test_up_no_detach(fake_run: _FakeRun) -> None:
    fake_run.queue()
    mgr = EnvironmentManager()
    mgr.up(detach=False)
    assert "-d" not in fake_run.calls[-1]


def test_up_with_wait_polls_health(fake_run: _FakeRun, monkeypatch: pytest.MonkeyPatch) -> None:
    # up itself succeeds, then wait_for_healthy polls ps --format json.
    fake_run.queue()  # up
    fake_run.queue(stdout='{"Service":"svc","State":"running","Health":"healthy"}\n')
    monkeypatch.setattr("tools.agent.env_manager.time.sleep", lambda _s: None)
    mgr = EnvironmentManager()
    mgr.up(wait=True, wait_service="svc", wait_timeout_s=5.0)
    # The second call is the ps --format json health probe.
    assert "--format" in fake_run.calls[1]


def test_down_removes_volumes_and_timeout(fake_run: _FakeRun) -> None:
    fake_run.queue()
    mgr = EnvironmentManager()
    mgr.down(remove_volumes=True, timeout_s=10)
    cmd = fake_run.calls[-1]
    assert "down" in cmd and "-v" in cmd and "-t" in cmd and "10" in cmd


def test_restart_stop_scale_build(fake_run: _FakeRun) -> None:
    mgr = EnvironmentManager()
    for _ in range(4):
        fake_run.queue()
    mgr.restart("svc")
    mgr.stop("svc")
    mgr.scale("svc", 2)
    mgr.build("svc")
    assert fake_run.calls[0][-2:] == ["restart", "svc"]
    assert fake_run.calls[1][-1] == "stop" or fake_run.calls[1][-2:] == ["stop", "svc"]
    assert "--scale" in fake_run.calls[2] and "svc=2" in fake_run.calls[2]
    assert fake_run.calls[3][-1] == "build" or fake_run.calls[3][-2:] == ["build", "svc"]


def test_stop_without_service(fake_run: _FakeRun) -> None:
    fake_run.queue()
    mgr = EnvironmentManager()
    mgr.stop()
    assert fake_run.calls[-1][-1] == "stop"


def test_exec_preserves_pre_split_command(fake_run: _FakeRun) -> None:
    fake_run.queue(stdout="result\n")
    mgr = EnvironmentManager()
    out = mgr.exec("svc", ["sh", "-c", "echo hello world"])
    cmd = fake_run.calls[-1]
    assert "exec" in cmd and "-T" in cmd
    # The command with spaces is preserved as a single argument.
    assert "echo hello world" in cmd
    assert out == "result\n"


def test_logs_combines_stdout_and_stderr(fake_run: _FakeRun) -> None:
    fake_run.queue(stdout="out-line\n", stderr="err-line\n")
    mgr = EnvironmentManager()
    log = mgr.logs("svc", tail=50)
    assert "out-line" in log and "err-line" in log
    assert "--tail=50" in fake_run.calls[-1]


def test_ps_returns_raw_stdout(fake_run: _FakeRun) -> None:
    fake_run.queue(stdout="NAME IMAGE\nsvc img\n")
    mgr = EnvironmentManager()
    assert "svc img" in mgr.ps()


# ---------------------------------------------------------------------------
# structured status parsing
# ---------------------------------------------------------------------------


def test_running_services_from_json_lines(fake_run: _FakeRun) -> None:
    fake_run.queue(
        stdout=('{"Service":"gateway","State":"running"}\n{"Service":"coord","State":"exited"}\n')
    )
    mgr = EnvironmentManager()
    assert mgr.running_services() == ["gateway"]


def test_running_services_from_json_array(fake_run: _FakeRun) -> None:
    fake_run.queue(stdout='[{"Service":"a","State":"up"},{"Service":"b","State":"running"}]')
    mgr = EnvironmentManager()
    assert sorted(mgr.running_services()) == ["a", "b"]


def test_running_services_skips_non_json_lines(fake_run: _FakeRun) -> None:
    fake_run.queue(
        stdout=("NAME IMAGE COMMAND\n{\"Service\":\"a\",\"State\":\"running\"}\ngarbage line\n")
    )
    mgr = EnvironmentManager()
    assert mgr.running_services() == ["a"]


def test_running_services_empty(fake_run: _FakeRun) -> None:
    fake_run.queue(stdout="")
    mgr = EnvironmentManager()
    assert mgr.running_services() == []


def test_is_running_true_and_false(fake_run: _FakeRun) -> None:
    fake_run.queue(stdout='{"Service":"a","State":"running"}\n')
    fake_run.queue(stdout='{"Service":"a","State":"exited"}\n')
    mgr = EnvironmentManager()
    assert mgr.is_running("a") is True
    assert mgr.is_running("a") is False


def test_wait_for_healthy_true(fake_run: _FakeRun, monkeypatch: pytest.MonkeyPatch) -> None:
    fake_run.queue(stdout='{"Service":"svc","Health":"healthy"}\n')
    monkeypatch.setattr("tools.agent.env_manager.time.sleep", lambda _s: None)
    mgr = EnvironmentManager()
    assert mgr.wait_for_healthy("svc", timeout_s=5.0) is True


def test_wait_for_healthy_unhealthy_is_false(
    fake_run: _FakeRun, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake_run.queue(stdout='{"Service":"svc","Health":"unhealthy"}\n')
    monkeypatch.setattr("tools.agent.env_manager.time.sleep", lambda _s: None)
    mgr = EnvironmentManager()
    assert mgr.wait_for_healthy("svc", timeout_s=0.05) is False


# ---------------------------------------------------------------------------
# error handling
# ---------------------------------------------------------------------------


def test_compose_error_on_nonzero(fake_run: _FakeRun) -> None:
    fake_run.queue(returncode=1, stderr="boom")
    mgr = EnvironmentManager()
    with pytest.raises(ComposeError, match="failed"):
        mgr.restart("svc")


def test_compose_error_on_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    def raise_timeout(cmd: Sequence[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        raise subprocess.TimeoutExpired(cmd=list(cmd), timeout=5.0)

    monkeypatch.setattr("tools.agent.env_manager.subprocess.run", raise_timeout)
    mgr = EnvironmentManager()
    with pytest.raises(ComposeError, match="timed out"):
        mgr.restart("svc")


def test_check_false_does_not_raise(fake_run: _FakeRun) -> None:
    fake_run.queue(returncode=1, stderr="ignored")
    mgr = EnvironmentManager()
    mgr.down()  # check=False
    assert "down" in fake_run.calls[-1]
