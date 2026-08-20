"""Unit tests for the scenario report renderer."""

from __future__ import annotations

import json
import xml.etree.ElementTree as ET

from tools.agent.ahc import ClientEvent, ClientStatus
from tools.agent.event_correlator import CorrelatedEvent, EventKind
from tools.agent.report import Report
from tools.agent.scenario import (
    ScenarioResult,
    ScenarioStatus,
    StepDiagnosis,
    StepResult,
    StepStatus,
)


def _step(
    name: str,
    status: StepStatus = StepStatus.PASSED,
    *,
    duration_s: float = 0.1,
    error: str | None = None,
    details: dict[str, object] | None = None,
    diagnosis: StepDiagnosis | None = None,
) -> StepResult:
    return StepResult(
        name=name,
        status=status,
        duration_s=duration_s,
        error=error,
        details=details,
        diagnosis=diagnosis,
    )


def _result(
    name: str,
    status: ScenarioStatus = ScenarioStatus.PASSED,
    *,
    steps: tuple[StepResult, ...] = (),
    duration_s: float = 0.5,
    error: str | None = None,
) -> ScenarioResult:
    return ScenarioResult(
        name=name,
        status=status,
        started_s=100.0,
        duration_s=duration_s,
        steps=steps,
        error=error,
    )


def _event(type_id: int = 5, payload: bytes = b"\x00\x01") -> ClientEvent:
    return ClientEvent(
        ts_mono_ns=1_000_000,
        type_id=type_id,
        correlation_id=7,
        payload=payload,
    )


def _status() -> ClientStatus:
    return ClientStatus(
        connected=True,
        bootstrap_state=2,
        bootstrap_step=3,
        bootstrap_step_count=3,
        rtt_last_ms=4,
        reconnect_attempts=0,
    )


def _diagnosis() -> StepDiagnosis:
    correlated = CorrelatedEvent(
        ts_mono_ns=1_000_000,
        source="alpha",
        kind=EventKind.CLIENT,
        correlation_id=7,
        type_id=5,
        payload=b"\x00\x01",
        detail={},
    )
    return StepDiagnosis(
        statuses={"alpha": _status()},
        event_tails={"alpha": [_event()]},
        correlation_chains={7: [correlated]},
    )


# ---------------------------------------------------------------------------
# summary
# ---------------------------------------------------------------------------


def test_summary_counts_passed_and_failed() -> None:
    report = Report(
        results=[
            _result("a", ScenarioStatus.PASSED),
            _result(
                "b", ScenarioStatus.FAILED, steps=(_step("s", StepStatus.FAILED, error="boom"),)
            ),
            _result("c", ScenarioStatus.PASSED),
        ],
    )
    summary = report.summary()
    assert summary.total == 3
    assert summary.passed == 2
    assert summary.failed == 1
    assert summary.skipped == 0


def test_summary_empty_results() -> None:
    report = Report(results=[])
    summary = report.summary()
    assert summary.total == 0
    assert summary.passed == 0
    assert summary.failed == 0


# ---------------------------------------------------------------------------
# JSON
# ---------------------------------------------------------------------------


def test_to_json_is_parseable_and_carries_steps() -> None:
    report = Report(
        results=[
            _result(
                "alpha",
                ScenarioStatus.PASSED,
                steps=(_step("connect"), _step("verify")),
                duration_s=0.42,
            ),
        ],
        run_id="run-x",
        started_at="2026-01-02T03:04:05Z",
        duration_s=1.23,
    )
    data = json.loads(report.to_json())
    assert data["run_id"] == "run-x"
    assert data["started_at"] == "2026-01-02T03:04:05Z"
    assert data["duration_s"] == 1.23
    assert data["summary"] == {"total": 1, "passed": 1, "failed": 0, "skipped": 0}
    scenario = data["scenarios"][0]
    assert scenario["name"] == "alpha"
    assert scenario["status"] == "passed"
    assert len(scenario["steps"]) == 2
    assert scenario["steps"][0]["name"] == "connect"
    assert scenario["steps"][0]["error"] is None
    assert scenario["steps"][0]["details"] is None
    assert scenario["steps"][0]["diagnosis"] is None


def test_to_json_serializes_assertion_details_with_bytes_payload() -> None:
    details: dict[str, object] = {
        "client": "c1",
        "type_id": 5,
        "event": _event(),
        "state": _status(),
    }
    report = Report(
        results=[
            _result(
                "beta",
                ScenarioStatus.FAILED,
                steps=(
                    _step(
                        "assert_event",
                        StepStatus.FAILED,
                        error="ScenarioAssertionError: no 5 event",
                        details=details,
                    ),
                ),
            ),
        ],
    )
    data = json.loads(report.to_json())
    step = data["scenarios"][0]["steps"][0]
    assert step["details"]["client"] == "c1"
    event = step["details"]["event"]
    assert event["type_id"] == 5
    assert event["correlation_id"] == 7
    assert event["payload"] == "0001"
    assert step["details"]["state"]["connected"] is True


def test_to_json_generates_run_id_when_empty() -> None:
    report = Report(results=[])
    data = json.loads(report.to_json())
    assert data["run_id"].startswith("run-")
    assert data["started_at"]


def test_to_json_serializes_diagnosis_evidence() -> None:
    report = Report(
        results=[
            _result(
                "beta",
                ScenarioStatus.FAILED,
                steps=(
                    _step(
                        "assert_event",
                        StepStatus.FAILED,
                        error="ScenarioAssertionError: no 5 event",
                        diagnosis=_diagnosis(),
                    ),
                ),
            ),
        ],
    )
    data = json.loads(report.to_json())
    step = data["scenarios"][0]["steps"][0]
    diagnosis = step["diagnosis"]
    assert diagnosis["statuses"]["alpha"]["connected"] is True
    tail = diagnosis["event_tails"]["alpha"][0]
    assert tail["type_id"] == 5
    assert tail["correlation_id"] == 7
    assert tail["payload"] == "0001"
    chain = diagnosis["correlation_chains"]["7"]
    assert chain[0]["source"] == "alpha"
    assert chain[0]["kind"] == "client"


# ---------------------------------------------------------------------------
# JUnit XML
# ---------------------------------------------------------------------------


def test_to_junit_xml_well_formed_passed() -> None:
    report = Report(
        results=[_result("ok", ScenarioStatus.PASSED, steps=(_step("s"),))],
        duration_s=2.0,
        started_at="2026-01-02T03:04:05Z",
    )
    root = ET.fromstring(report.to_junit_xml())
    assert root.tag == "testsuites"
    assert root.attrib["tests"] == "1"
    assert root.attrib["failures"] == "0"
    suite = root.find("testsuite")
    assert suite is not None
    assert suite.attrib["name"] == "kith-scenarios"
    case = suite.find("testcase")
    assert case is not None
    assert case.attrib["name"] == "ok"
    assert case.attrib["classname"] == "tools.agent.scenarios"
    assert case.find("failure") is None


def test_to_junit_xml_failure_message_from_first_failed_step() -> None:
    report = Report(
        results=[
            _result(
                "bad",
                ScenarioStatus.FAILED,
                steps=(
                    _step("ok", StepStatus.PASSED),
                    _step("boom", StepStatus.FAILED, error="ScenarioAssertionError: timeout"),
                    _step("also", StepStatus.FAILED, error="ValueError: x"),
                ),
            ),
        ],
        duration_s=1.0,
        started_at="2026-01-02T03:04:05Z",
    )
    root = ET.fromstring(report.to_junit_xml())
    suite = root.find("testsuite")
    assert suite is not None
    case = suite.find("testcase")
    assert case is not None
    failure = case.find("failure")
    assert failure is not None
    assert failure.attrib["message"] == "ScenarioAssertionError: timeout"
    body = failure.text or ""
    assert "[boom]" in body
    assert "[also]" in body
    assert "ScenarioAssertionError: timeout" in body


def test_to_junit_xml_escapes_special_characters() -> None:
    report = Report(
        results=[
            _result(
                "a < b & c",
                ScenarioStatus.FAILED,
                steps=(_step("s", StepStatus.FAILED, error="err < 'quote' & \"q\""),),
            ),
        ],
        started_at="2026-01-02T03:04:05Z",
    )
    text = report.to_junit_xml()
    root = ET.fromstring(text)
    suite = root.find("testsuite")
    assert suite is not None
    case = suite.find("testcase")
    assert case is not None
    assert case.attrib["name"] == "a < b & c"
    failure = case.find("failure")
    assert failure is not None
    assert failure.attrib["message"] == "err < 'quote' & \"q\""


def test_to_junit_xml_failure_body_carries_diagnosis() -> None:
    report = Report(
        results=[
            _result(
                "bad",
                ScenarioStatus.FAILED,
                steps=(
                    _step(
                        "boom",
                        StepStatus.FAILED,
                        error="AssertionError: x",
                        diagnosis=_diagnosis(),
                    ),
                ),
            ),
        ],
        started_at="2026-01-02T03:04:05Z",
    )
    root = ET.fromstring(report.to_junit_xml())
    failure = root.find("testsuite/testcase/failure")
    assert failure is not None
    body = failure.text or ""
    assert "diagnosis:" in body
    assert '"correlation_id": 7' in body
    assert '"source": "alpha"' in body


# ---------------------------------------------------------------------------
# markdown
# ---------------------------------------------------------------------------


def test_to_markdown_summary_and_scenario_table() -> None:
    report = Report(
        results=[
            _result("alpha", ScenarioStatus.PASSED, steps=(_step("s"),)),
            _result(
                "beta",
                ScenarioStatus.FAILED,
                steps=(
                    _step("ok", StepStatus.PASSED),
                    _step("boom", StepStatus.FAILED, error="ScenarioAssertionError: x"),
                ),
            ),
        ],
        run_id="run-md",
        started_at="2026-01-02T03:04:05Z",
        duration_s=3.5,
    )
    md = report.to_markdown()
    assert "# Scenario Report: run-md" in md
    assert "| 2 | 1 | 1 | 0 |" in md
    assert "| alpha | PASS |" in md
    assert "| beta | FAIL |" in md
    assert "## Failures" in md
    assert "### beta" in md
    assert "**boom**" in md
    assert "ScenarioAssertionError: x" in md


def test_to_markdown_renders_details_block_for_failed_step() -> None:
    details: dict[str, object] = {"client": "c1", "type_id": 9}
    report = Report(
        results=[
            _result(
                "gamma",
                ScenarioStatus.FAILED,
                steps=(
                    _step(
                        "assert_event",
                        StepStatus.FAILED,
                        error="ScenarioAssertionError: no 9 event",
                        details=details,
                    ),
                ),
            ),
        ],
    )
    md = report.to_markdown()
    assert "```json" in md
    assert '"client": "c1"' in md
    assert '"type_id": 9' in md


def test_to_markdown_renders_diagnosis_block_for_failed_step() -> None:
    report = Report(
        results=[
            _result(
                "gamma",
                ScenarioStatus.FAILED,
                steps=(
                    _step(
                        "assert_event",
                        StepStatus.FAILED,
                        error="ScenarioAssertionError: no 5 event",
                        diagnosis=_diagnosis(),
                    ),
                ),
            ),
        ],
    )
    md = report.to_markdown()
    assert "```json" in md
    assert '"correlation_id": 7' in md
    assert '"source": "alpha"' in md
    assert '"connected": true' in md


def test_to_junit_xml_failure_message_from_scenario_error() -> None:
    report = Report(
        results=[
            _result(
                "wired-wrong",
                ScenarioStatus.FAILED,
                error="OSError: connection refused",
            ),
        ],
        started_at="2026-01-02T03:04:05Z",
    )
    root = ET.fromstring(report.to_junit_xml())
    case = root.find("testsuite/testcase")
    assert case is not None
    failure = case.find("failure")
    assert failure is not None
    assert failure.attrib["message"] == "OSError: connection refused"


def test_to_markdown_renders_scenario_error_without_steps() -> None:
    report = Report(
        results=[
            _result(
                "wired-wrong",
                ScenarioStatus.FAILED,
                error="scenario exceeded its 5.0s deadline",
            ),
        ],
    )
    md = report.to_markdown()
    assert "- **scenario**: scenario exceeded its 5.0s deadline" in md


def test_to_markdown_no_scenarios_message() -> None:
    report = Report(results=[])
    md = report.to_markdown()
    assert "_No scenarios ran._" in md
