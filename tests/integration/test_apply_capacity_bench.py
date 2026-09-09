"""Integration test: the apply-capacity bench harness end-to-end.

Structural only: boots a tiny sweep (2 threads, 8 actors, two stripe
topologies, sub-second windows) and asserts the report assembles and
serializes. This is a harness-rot guard, not a performance datapoint:
under the standard interpreter the GIL serializes every config, so the
contention numbers are only meaningful on a free-threaded runtime.
"""

from __future__ import annotations

import json

import pytest
from _helpers import _BUILD_DEBUG
from tools.perf.apply_capacity_bench import BenchConfig, _find_c_driver, _run_c_native, run_bench


def test_apply_capacity_bench_report_assembles() -> None:
    """A tiny sweep produces positive rates, percentiles, and valid JSON."""
    report = run_bench(
        BenchConfig(
            threads=[2],
            stripes=[1, 4],
            actors=8,
            windows=1,
            window_s=0.2,
            warmup_s=0.1,
            require_free_threaded=False,
        )
    )

    configs = {(aggregate.config, aggregate.stripes) for aggregate in report.aggregates}
    assert ("real-single", 0) in configs
    assert ("replica", 1) in configs
    assert ("replica", 4) in configs
    for aggregate in report.aggregates:
        assert aggregate.applies_total > 0
        assert aggregate.applies_per_s > 0
        assert aggregate.mean_us >= 0
        if aggregate.config == "replica":
            assert aggregate.hold_fraction is not None
    assert report.calibration["2"]["sharded_stripes"] == 4
    assert report.calibration["2"]["replica1_over_real_single"] is not None

    encoded = json.loads(json.dumps(report.to_json_dict()))
    assert encoded["schema"] == "apply-capacity-bench/1"
    assert len(encoded["aggregates"]) == len(report.aggregates)


def test_c_native_bench_report_assembles() -> None:
    """The C driver runs a tiny sweep and the report parses and serializes.

    Skips only when no built tree exists at all; a built tree whose driver
    target is missing fails, because the target builds by default and its
    absence means a broken build, not an unprovisioned one.
    """
    try:
        _find_c_driver()
    except RuntimeError as exc:
        if (_BUILD_DEBUG / "libkith_server.so").is_file():
            pytest.fail(f"the built tree is missing its c_apply_bench target: {exc}")
        pytest.skip(f"c_apply_bench driver unavailable: {exc}")
    report = _run_c_native(
        actors=8,
        threads=[2],
        windows=1,
        window_s=0.2,
        warmup_s=0.1,
        dt_ms=50,
        seed=12345,
    )

    assert report["schema"] == "apply-capacity-bench-c/1"
    # Two aggregates (tick ON and tick OFF) and their window rows.
    assert len(report["aggregates"]) == 2
    assert len(report["windows"]) == 2
    for aggregate in report["aggregates"]:
        assert aggregate["applies_total"] > 0
        assert aggregate["applies_per_s"] > 0
        assert aggregate["cpu_mean_us"] >= 0.0
        assert aggregate["total_cpu_us_per_apply"] >= aggregate["cpu_mean_us"] - 1e-9
    tick_on = next(
        aggregate for aggregate in report["aggregates"] if aggregate["tick_enabled"] == 1
    )
    # The flush thread bumps every drained dirty cell at most once per
    # flush (the dirty-set coalescing contract: cells in an 8x8 world).
    assert tick_on["fabric_publishes"] <= tick_on["flush_count"] * 64
    assert report["envelope"]["host"]

    encoded = json.loads(json.dumps(report))
    assert encoded["schema"] == "apply-capacity-bench-c/1"
