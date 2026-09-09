"""Tests for the scaling-gate baseline regression comparator.

The comparator reads a JSON gate report and a committed baseline report of
the same shape and exits non-zero when a metric regressed beyond its
tolerance. These tests cover the regression logic (each metric direction),
the tolerance bands, the absent-baseline skip, and the CLI exit codes.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from tools.perf.compare_scaling_baseline import (
    RegressionParseError,
    compare_reports,
    main,
)


def _report(
    *,
    session_ok: float = 0.995,
    selected_clients: float = 0.97,
    move_missing: float = 0.03,
    bootstrap_p95: int = 1200,
    flicker: bool = False,
    skew: float = 0.0,
) -> dict[str, object]:
    """Build a minimal gate-report dict with the given metrics."""
    return {
        "profile": {"name": "dense-1000"},
        "metrics": {
            "actor_count": 1000,
            "sampled_clients": 32,
            "session_ok_ratio": session_ok,
            "selected_clients_ratio": selected_clients,
            "move_missing_ratio": move_missing,
            "bootstrap_ms_p95": bootstrap_p95,
            "continuity_flicker": flicker,
            "publish_rate_skew": skew,
        },
        "thresholds": {},
        "passed": True,
        "started_at": "2026-08-18T00:00:00+00:00",
        "duration_s": 6.0,
    }


class TestCompareReports:
    def test_identical_reports_do_not_regress(self) -> None:
        baseline = _report()
        report = _report()
        regression = compare_reports(report, baseline)
        assert not regression.regressed
        assert len(regression.checks) == 6

    def test_session_ok_regression(self) -> None:
        baseline = _report(session_ok=0.99)
        report = _report(session_ok=0.95)
        regression = compare_reports(report, baseline)
        assert regression.regressed
        names = [c.name for c in regression.checks if c.regressed]
        assert names == ["session_ok_ratio"]

    def test_session_ok_within_tolerance(self) -> None:
        baseline = _report(session_ok=0.99)
        report = _report(session_ok=0.975)
        regression = compare_reports(report, baseline)
        assert not regression.regressed

    def test_selected_clients_regression(self) -> None:
        baseline = _report(selected_clients=0.97)
        report = _report(selected_clients=0.90)
        regression = compare_reports(report, baseline)
        assert regression.regressed
        names = [c.name for c in regression.checks if c.regressed]
        assert names == ["selected_clients_ratio"]

    def test_move_missing_regression(self) -> None:
        baseline = _report(move_missing=0.03)
        report = _report(move_missing=0.08)
        regression = compare_reports(report, baseline)
        assert regression.regressed
        names = [c.name for c in regression.checks if c.regressed]
        assert names == ["move_missing_ratio"]

    def test_move_missing_within_tolerance(self) -> None:
        baseline = _report(move_missing=0.03)
        report = _report(move_missing=0.04)
        regression = compare_reports(report, baseline)
        assert not regression.regressed

    def test_bootstrap_p95_regression(self) -> None:
        baseline = _report(bootstrap_p95=1000)
        report = _report(bootstrap_p95=1500)
        regression = compare_reports(report, baseline)
        assert regression.regressed
        names = [c.name for c in regression.checks if c.regressed]
        assert names == ["bootstrap_ms_p95"]

    def test_bootstrap_p95_within_tolerance(self) -> None:
        baseline = _report(bootstrap_p95=1000)
        report = _report(bootstrap_p95=1100)
        regression = compare_reports(report, baseline)
        assert not regression.regressed

    def test_continuity_flicker_regression(self) -> None:
        baseline = _report(flicker=False)
        report = _report(flicker=True)
        regression = compare_reports(report, baseline)
        assert regression.regressed
        names = [c.name for c in regression.checks if c.regressed]
        assert names == ["continuity_flicker"]

    def test_continuity_flicker_already_bad_is_not_regression(self) -> None:
        baseline = _report(flicker=True)
        report = _report(flicker=True)
        regression = compare_reports(report, baseline)
        assert not regression.regressed

    def test_publish_rate_skew_regression(self) -> None:
        baseline = _report(skew=1.0)
        report = _report(skew=2.0)
        regression = compare_reports(report, baseline)
        assert regression.regressed
        names = [c.name for c in regression.checks if c.regressed]
        assert names == ["publish_rate_skew"]

    def test_publish_rate_skew_within_tolerance(self) -> None:
        baseline = _report(skew=1.0)
        report = _report(skew=1.1)
        regression = compare_reports(report, baseline)
        assert not regression.regressed

    def test_missing_metric_raises(self) -> None:
        baseline = _report()
        report: dict[str, object] = {
            "profile": {"name": "dense-1000"},
            "metrics": {"actor_count": 1000},
        }
        with pytest.raises(RegressionParseError):
            compare_reports(report, baseline)

    def test_missing_metrics_field_raises(self) -> None:
        baseline = _report()
        report: dict[str, object] = {"profile": {"name": "dense-1000"}}
        with pytest.raises(RegressionParseError):
            compare_reports(report, baseline)


class TestMain:
    def test_absent_baseline_skips_comparison(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        report_path = tmp_path / "report.json"
        report_path.write_text(json.dumps(_report()), encoding="utf-8")
        baseline_path = tmp_path / "baseline.json"

        rc = main(["--report", str(report_path), "--baseline", str(baseline_path)])

        assert rc == 0
        captured = capsys.readouterr()
        assert "no baseline" in captured.out

    def test_regression_exits_nonzero(self, tmp_path: Path) -> None:
        baseline_path = tmp_path / "baseline.json"
        report_path = tmp_path / "report.json"
        baseline_path.write_text(json.dumps(_report(session_ok=0.99)), encoding="utf-8")
        report_path.write_text(json.dumps(_report(session_ok=0.90)), encoding="utf-8")

        rc = main(["--report", str(report_path), "--baseline", str(baseline_path)])

        assert rc == 1

    def test_no_regression_exits_zero(self, tmp_path: Path) -> None:
        baseline_path = tmp_path / "baseline.json"
        report_path = tmp_path / "report.json"
        baseline_path.write_text(json.dumps(_report()), encoding="utf-8")
        report_path.write_text(json.dumps(_report()), encoding="utf-8")

        rc = main(["--report", str(report_path), "--baseline", str(baseline_path)])

        assert rc == 0

    def test_missing_report_exits_two(self, tmp_path: Path) -> None:
        report_path = tmp_path / "report.json"
        baseline_path = tmp_path / "baseline.json"
        baseline_path.write_text(json.dumps(_report()), encoding="utf-8")

        rc = main(["--report", str(report_path), "--baseline", str(baseline_path)])

        assert rc == 2

    def test_malformed_report_exits_two(self, tmp_path: Path) -> None:
        report_path = tmp_path / "report.json"
        baseline_path = tmp_path / "baseline.json"
        report_path.write_text("not json", encoding="utf-8")
        baseline_path.write_text(json.dumps(_report()), encoding="utf-8")

        rc = main(["--report", str(report_path), "--baseline", str(baseline_path)])

        assert rc == 2
