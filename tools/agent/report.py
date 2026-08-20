"""Scenario report renderer for the agentic harness.

Renders a sequence of :class:`~tools.agent.scenario.ScenarioResult` into
JSON, JUnit XML, and Markdown. The report is a pure value object: it holds
the results and run metadata and produces strings; it performs no I/O and
holds no mutable state, so it is testable without a server, orchestrator,
or event loop.

A scenario maps to one JUnit ``testcase``; a failed scenario's failure
message is the first failed step's error, with every failed step and its
structured ``details`` (populated from
:class:`~tools.agent.assertions.ScenarioAssertionError.details` via
``StepResult.details``) carried in the failure body so assertion context
travels into CI dashboards. The runner-gathered failure diagnosis
(``StepResult.diagnosis``) rides the same three render paths.

JSON serialization handles the non-native values that flow through
``details`` — :class:`~tools.agent.ahc.ClientEvent` (a dataclass with a
``bytes`` payload) and :class:`~tools.agent.ahc.ClientStatus` — by
converting dataclasses to mappings and bytes to hex strings, so the report
is always serializable regardless of which assertion populated ``details``.
"""

from __future__ import annotations

import dataclasses
import enum
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from xml.sax.saxutils import escape, quoteattr

from tools.agent.scenario import (
    ScenarioResult,
    ScenarioStatus,
    StepResult,
    StepStatus,
)


__all__ = [
    "Report",
    "ReportSummary",
]


_SUITE_NAME = "kith-scenarios"
_CLASSNAME = "tools.agent.scenarios"


@dataclass(frozen=True, slots=True)
class ReportSummary:
    """Aggregate counts over a report's scenarios."""

    total: int
    passed: int
    failed: int
    skipped: int


@dataclass(frozen=True, slots=True)
class Report:
    """A renderable report over a batch of scenario results.

    ``results`` is the single source of truth; the renderers derive every
    field from it. Run metadata (``run_id``, ``started_at``,
    ``duration_s``) is supplied by the runner; defaults produce a
    timestamp-derived ``run_id`` and current time so a report is always
    well-formed even when constructed without metadata.
    """

    results: Sequence[ScenarioResult]
    run_id: str = ""
    started_at: str = ""
    duration_s: float = 0.0

    def summary(self) -> ReportSummary:
        """Compute aggregate counts over the results."""
        total = len(self.results)
        passed = sum(1 for r in self.results if r.status is ScenarioStatus.PASSED)
        failed = sum(1 for r in self.results if r.status is ScenarioStatus.FAILED)
        skipped = total - passed - failed
        return ReportSummary(
            total=total,
            passed=passed,
            failed=failed,
            skipped=skipped,
        )

    def _resolved_run_id(self) -> str:
        if self.run_id:
            return self.run_id
        return f"run-{datetime.now(UTC).strftime('%Y%m%d-%H%M%S')}"

    def _resolved_started_at(self) -> str:
        return self.started_at or datetime.now(UTC).isoformat()

    # -----------------------------------------------------------------------
    # serializers
    # -----------------------------------------------------------------------

    def to_json(self, *, indent: int = 2) -> str:
        """Return a JSON serialization of the report."""
        payload: dict[str, object] = {
            "run_id": self._resolved_run_id(),
            "started_at": self._resolved_started_at(),
            "duration_s": round(self.duration_s, 3),
            "summary": dataclasses.asdict(self.summary()),
            "scenarios": [self._scenario_dict(r) for r in self.results],
        }
        return json.dumps(payload, indent=indent, default=_jsonable)

    @staticmethod
    def _scenario_dict(result: ScenarioResult) -> dict[str, object]:
        return {
            "name": result.name,
            "status": result.status.value,
            "started_s": round(result.started_s, 3),
            "duration_s": round(result.duration_s, 3),
            "error": result.error,
            "steps": [Report._step_dict(s) for s in result.steps],
        }

    @staticmethod
    def _step_dict(step: StepResult) -> dict[str, object]:
        entry: dict[str, object] = {
            "name": step.name,
            "status": step.status.value,
            "duration_s": round(step.duration_s, 3),
            "error": step.error,
        }
        if step.details is not None:
            entry["details"] = dict(step.details)
        else:
            entry["details"] = None
        if step.diagnosis is not None:
            entry["diagnosis"] = step.diagnosis
        else:
            entry["diagnosis"] = None
        return entry

    def to_junit_xml(self) -> str:
        """Return a JUnit XML serialization of the report."""
        summary = self.summary()
        duration = f"{self.duration_s:.3f}"
        timestamp = self._resolved_started_at()
        suite_name = quoteattr(_SUITE_NAME)
        ts_attr = quoteattr(timestamp)
        parts: list[str] = [
            '<?xml version="1.0" encoding="UTF-8"?>',
            (
                f'<testsuites tests="{summary.total}" failures="{summary.failed}" '
                f'errors="0" skipped="{summary.skipped}" time="{duration}">'
            ),
            (
                f'  <testsuite name={suite_name} '
                f'tests="{summary.total}" failures="{summary.failed}" '
                f'errors="0" skipped="{summary.skipped}" time="{duration}" '
                f'timestamp={ts_attr}>'
            ),
        ]
        for result in self.results:
            parts.append(self._junit_testcase(result))
        parts.append("  </testsuite>")
        parts.append("</testsuites>")
        return "\n".join(parts)

    @staticmethod
    def _junit_testcase(result: ScenarioResult) -> str:
        name_attr = quoteattr(result.name)
        time_attr = f'"{result.duration_s:.3f}"'
        opening = f'    <testcase classname="{_CLASSNAME}" name={name_attr} time={time_attr}'
        if result.status is ScenarioStatus.PASSED:
            return f"{opening}/>"
        failed_steps = [s for s in result.steps if s.status is StepStatus.FAILED]
        if not failed_steps:
            if result.error:
                return f"{opening}><failure message={quoteattr(result.error)}/></testcase>"
            return f"{opening}><failure/></testcase>"
        first = failed_steps[0]
        message = first.error or "scenario failed"
        body = Report._junit_failure_body(failed_steps)
        return (
            f"{opening}><failure message={quoteattr(message)}>{escape(body)}</failure></testcase>"
        )

    @staticmethod
    def _junit_failure_body(steps: Sequence[StepResult]) -> str:
        lines: list[str] = []
        for step in steps:
            lines.append(f"[{step.name}] {step.error or 'failed'}")
            if step.details:
                details_json = json.dumps(dict(step.details), default=_jsonable)
                lines.append(f"  details: {details_json}")
            if step.diagnosis is not None:
                diagnosis_json = json.dumps(step.diagnosis, default=_jsonable)
                lines.append(f"  diagnosis: {diagnosis_json}")
        return "\n".join(lines)

    def to_markdown(self) -> str:
        """Return a Markdown serialization of the report."""
        summary = self.summary()
        lines: list[str] = [
            f"# Scenario Report: {self._resolved_run_id()}",
            "",
            f"**Started:** {self._resolved_started_at()}  ",
            f"**Duration:** {self.duration_s:.3f}s",
            "",
            "## Summary",
            "",
            "| Total | Passed | Failed | Skipped |",
            "|---|---:|---:|---:|",
            f"| {summary.total} | {summary.passed} | {summary.failed} | {summary.skipped} |",
            "",
            "## Scenarios",
            "",
        ]
        if not self.results:
            lines.append("_No scenarios ran._")
            return "\n".join(lines)
        lines.append("| Scenario | Status | Duration | Steps |")
        lines.append("|---|---|---:|---|")
        for result in self.results:
            total_steps = len(result.steps)
            passed_steps = sum(1 for s in result.steps if s.status is StepStatus.PASSED)
            status = "PASS" if result.passed else "FAIL"
            lines.append(
                f"| {result.name} | {status} | {result.duration_s:.3f}s | "
                f"{passed_steps}/{total_steps} |"
            )
        failed = [r for r in self.results if not r.passed]
        if failed:
            lines.append("")
            lines.append("## Failures")
            lines.append("")
            for result in failed:
                lines.extend(Report._markdown_failure(result))
        return "\n".join(lines)

    @staticmethod
    def _markdown_failure(result: ScenarioResult) -> list[str]:
        lines: list[str] = [f"### {result.name}"]
        if result.error:
            lines.append(f"- **scenario**: {result.error}")
            if result.details:
                details_json = json.dumps(dict(result.details), indent=2, default=_jsonable)
                lines.append(f"  ```json\n  {details_json.replace(chr(10), chr(10) + '  ')}\n  ```")
        for step in result.steps:
            if step.status is not StepStatus.FAILED:
                continue
            lines.append(f"- **{step.name}** ({step.duration_s:.3f}s): {step.error or 'failed'}")
            if step.details:
                details_json = json.dumps(dict(step.details), indent=2, default=_jsonable)
                lines.append(f"  ```json\n  {details_json.replace(chr(10), chr(10) + '  ')}\n  ```")
            if step.diagnosis is not None:
                diagnosis_json = json.dumps(step.diagnosis, indent=2, default=_jsonable)
                lines.append(
                    f"  ```json\n  {diagnosis_json.replace(chr(10), chr(10) + '  ')}\n  ```"
                )
        return lines


def _jsonable(obj: object) -> object:
    """Convert non-JSON-native objects in report data to native types.

    Handles dataclasses (e.g. ``ClientEvent`` with a ``bytes`` payload,
    ``ClientStatus``), bytes, enums, and mappings that flow through
    ``StepResult.details``. ``json.dumps`` recurses into the returned
    values and re-invokes this default for any remaining non-native
    element, so a shallow conversion here is sufficient.
    """
    if isinstance(obj, bytes):
        return obj.hex()
    if isinstance(obj, enum.Enum):
        return obj.value
    if isinstance(obj, Mapping):
        return dict(obj)
    if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
        return {f.name: getattr(obj, f.name) for f in dataclasses.fields(obj)}
    return str(obj)
