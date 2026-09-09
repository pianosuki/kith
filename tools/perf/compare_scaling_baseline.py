#!/usr/bin/env python3
"""Scaling-gate baseline regression comparator.

Reads a JSON gate report (the output of ``python -m tools.agent.load_harness
--format json``) and a committed baseline report of the same shape. If the
baseline file is absent, the comparison is skipped: the harness exit code
(§4.4 thresholds) remains the gate, and no baseline exists to compare
against. If present, each metric is compared against the baseline and the
script exits non-zero when a metric regressed beyond its tolerance.

Regression direction per metric (``metrics`` field of the report):

  session_ok_ratio        lower is worse   (absolute tolerance)
  selected_clients_ratio  lower is worse   (absolute tolerance)
  move_missing_ratio      higher is worse   (absolute tolerance)
  bootstrap_ms_p95        higher is worse   (relative tolerance)
  continuity_flicker      True is worse     (a False→True flip regresses)
  publish_rate_skew       higher is worse   (relative tolerance)

The tolerances absorb run-to-run variance on shared CI runners. A ratio
metric may swing a couple of percentage points between identical builds on a
loaded host; the bootstrap p95 may swing more. The tolerances are chosen so
a real regression (a metric that dropped a noticeable margin, or a flicker
that was absent and is now present) fails the gate while noise does not.

Exit status: 0 when no regression (or no baseline), 1 on regression, 2 on
usage or parse error.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path


__all__ = [
    "MetricCheck",
    "Regression",
    "compare_reports",
]


# Absolute tolerance for ratio metrics in [0, 1]: a regression smaller than
# this is absorbed as run-to-run noise. Two percentage points covers the
# variance a loaded CI runner introduces on a 1000-actor dense run.
_RATIO_TOLERANCE: float = 0.02

# Relative tolerance for the bootstrap p95: a latency regression must exceed
# this fraction of the baseline to fail. p95 is noisier than the ratios
# (one slow poll stretches the tail), so the tolerance is wider.
_P95_RELATIVE_TOLERANCE: float = 0.20

# Relative tolerance for publish-rate skew: only meaningful on the
# distributed topology (zero on the single-instance dense gate).
_SKEW_RELATIVE_TOLERANCE: float = 0.20


@dataclass(frozen=True, slots=True)
class MetricCheck:
    """One metric's comparison result.

    ``regressed`` is True when the metric moved in the worse direction
    beyond its tolerance. ``detail`` is a human-readable line for the
    report.
    """

    name: str
    report_value: float
    baseline_value: float
    regressed: bool
    detail: str


@dataclass(frozen=True, slots=True)
class Regression:
    """The result of comparing a report against a baseline."""

    checks: tuple[MetricCheck, ...]

    @property
    def regressed(self) -> bool:
        return any(check.regressed for check in self.checks)


def compare_reports(report: dict[str, object], baseline: dict[str, object]) -> Regression:
    """Compare ``report`` metrics against ``baseline`` metrics.

    Both arguments are the parsed ``to_dict()`` output of a
    :class:`~tools.agent.load_harness.GateReport`. Returns a
    :class:`Regression` whose ``regressed`` property is True when any metric
    regressed beyond its tolerance.
    """
    report_metrics = _metrics_field(report)
    baseline_metrics = _metrics_field(baseline)
    checks: list[MetricCheck] = [
        _check_lower_better(
            "session_ok_ratio",
            report_metrics,
            baseline_metrics,
            _RATIO_TOLERANCE,
        ),
        _check_lower_better(
            "selected_clients_ratio",
            report_metrics,
            baseline_metrics,
            _RATIO_TOLERANCE,
        ),
        _check_higher_better(
            "move_missing_ratio",
            report_metrics,
            baseline_metrics,
            _RATIO_TOLERANCE,
        ),
        _check_higher_better_relative(
            "bootstrap_ms_p95",
            report_metrics,
            baseline_metrics,
            _P95_RELATIVE_TOLERANCE,
        ),
        _check_flicker(report_metrics, baseline_metrics),
        _check_higher_better_relative(
            "publish_rate_skew",
            report_metrics,
            baseline_metrics,
            _SKEW_RELATIVE_TOLERANCE,
        ),
    ]
    return Regression(checks=tuple(checks))


def _metrics_field(report: dict[str, object]) -> dict[str, object]:
    """Return the ``metrics`` sub-dict of a gate report."""
    metrics = report.get("metrics")
    if not isinstance(metrics, dict):
        raise RegressionParseError("report is missing a 'metrics' object")
    return metrics


def _check_lower_better(
    name: str,
    report_metrics: dict[str, object],
    baseline_metrics: dict[str, object],
    tolerance: float,
) -> MetricCheck:
    """Compare a lower-is-better ratio metric with an absolute tolerance."""
    report_value = _metric_float(report_metrics, name)
    baseline_value = _metric_float(baseline_metrics, name)
    regressed = report_value < baseline_value - tolerance
    detail = (
        f"{name}: report={report_value:.2%} baseline={baseline_value:.2%} "
        f"(requirement: >= baseline - {tolerance:.0%})"
    )
    return MetricCheck(name, report_value, baseline_value, regressed, detail)


def _check_higher_better(
    name: str,
    report_metrics: dict[str, object],
    baseline_metrics: dict[str, object],
    tolerance: float,
) -> MetricCheck:
    """Compare a higher-is-worse ratio metric with an absolute tolerance."""
    report_value = _metric_float(report_metrics, name)
    baseline_value = _metric_float(baseline_metrics, name)
    regressed = report_value > baseline_value + tolerance
    detail = (
        f"{name}: report={report_value:.2%} baseline={baseline_value:.2%} "
        f"(<= baseline + {tolerance:.0%})"
    )
    return MetricCheck(name, report_value, baseline_value, regressed, detail)


def _check_higher_better_relative(
    name: str,
    report_metrics: dict[str, object],
    baseline_metrics: dict[str, object],
    tolerance: float,
) -> MetricCheck:
    """Compare a higher-is-worse metric with a relative tolerance."""
    report_value = _metric_float(report_metrics, name)
    baseline_value = _metric_float(baseline_metrics, name)
    ceiling = baseline_value * (1.0 + tolerance)
    regressed = report_value > ceiling
    detail = (
        f"{name}: report={report_value:.2f} baseline={baseline_value:.2f} "
        f"(requirement: <= baseline x {1.0 + tolerance:.2f})"
    )
    return MetricCheck(name, report_value, baseline_value, regressed, detail)


def _check_flicker(
    report_metrics: dict[str, object],
    baseline_metrics: dict[str, object],
) -> MetricCheck:
    """Compare the continuity-flicker boolean: a False→True flip regresses."""
    report_value = _metric_bool(report_metrics, "continuity_flicker")
    baseline_value = _metric_bool(baseline_metrics, "continuity_flicker")
    regressed = report_value and not baseline_value
    detail = (
        f"continuity_flicker: report={report_value} baseline={baseline_value} "
        f"(False->True regresses)"
    )
    return MetricCheck(
        "continuity_flicker", float(report_value), float(baseline_value), regressed, detail
    )


def _metric_float(metrics: dict[str, object], name: str) -> float:
    """Return metric ``name`` as a float, raising on absence or wrong type."""
    value = metrics.get(name)
    if value is None:
        raise RegressionParseError(f"metrics is missing '{name}'")
    if not isinstance(value, int | float):
        raise RegressionParseError(f"metric '{name}' is not numeric")
    return float(value)


def _metric_bool(metrics: dict[str, object], name: str) -> bool:
    """Return metric ``name`` as a bool, raising on absence or wrong type."""
    value = metrics.get(name)
    if value is None:
        raise RegressionParseError(f"metrics is missing '{name}'")
    if not isinstance(value, bool):
        raise RegressionParseError(f"metric '{name}' is not boolean")
    return value


class RegressionParseError(Exception):
    """Raised when a baseline or report JSON is malformed."""


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="kith-compare-scaling-baseline",
        description=(
            "Compare a scaling-gate report against a committed baseline and "
            "exit non-zero on regression."
        ),
    )
    parser.add_argument(
        "--report",
        type=Path,
        required=True,
        help="Path to the JSON gate report produced by the load harness.",
    )
    parser.add_argument(
        "--baseline",
        type=Path,
        required=True,
        help="Path to the committed baseline JSON report. Absent => skip.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    report_path: Path = args.report
    baseline_path: Path = args.baseline

    if not report_path.is_file():
        sys.stderr.write(f"error: report not found: {report_path}\n")
        return 2

    if not baseline_path.is_file():
        sys.stdout.write(f"no baseline at {baseline_path}; skipping regression comparison\n")
        return 0

    try:
        report = _load_json(report_path)
        baseline = _load_json(baseline_path)
        regression = compare_reports(report, baseline)
    except RegressionParseError as exc:
        sys.stderr.write(f"error: {exc}\n")
        return 2

    for check in regression.checks:
        verdict = "REGRESSION" if check.regressed else "ok"
        sys.stdout.write(f"{verdict}: {check.detail}\n")

    if regression.regressed:
        sys.stderr.write("scaling-gate regression detected vs baseline\n")
        return 1
    sys.stdout.write("no regression vs baseline\n")
    return 0


def _load_json(path: Path) -> dict[str, object]:
    """Load a JSON object from ``path``, raising on malformed content."""
    try:
        with open(path, encoding="utf-8") as handle:
            data = json.load(handle)
    except OSError as exc:
        raise RegressionParseError(f"cannot read {path}: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise RegressionParseError(f"invalid JSON in {path}: {exc}") from exc
    if not isinstance(data, dict):
        raise RegressionParseError(f"{path} is not a JSON object")
    return data


if __name__ == "__main__":
    raise SystemExit(main())
