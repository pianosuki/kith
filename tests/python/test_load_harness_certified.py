"""Unit tests for the certified move-missing collector.

The certified rule derives coverage from the publisher-minted
``update_seq`` counter delta between self echoes, so it is immune to the
echo-sampling floor and cannot over-credit dropped inputs (a dropped
input never mints). These tests pin the counting rule, its fallback
contract, the aggregate shape, and the gate report's rendering of the
certified metric next to its ``mm_event`` companion. A second class pins
the run-envelope stamp: the build and machine state every report carries.
A third pins the run-shape stamp: the resolved apply regime and the
derived certified-shape verdict the evidence consumer refuses
non-certified reports on.
"""

from __future__ import annotations

import argparse
from dataclasses import replace
from pathlib import Path

import pytest
from tools.agent import load_harness as harness_module
from tools.agent.gap_ledger import SeriesFrame
from tools.agent.load_harness import (
    DENSE_1000,
    DISTRIBUTED_2000,
    DISTRIBUTED_THRESHOLDS,
    SMOKE_DENSE,
    ClientDepthRow,
    GateMetrics,
    GateReport,
    RunEnvelope,
    RunShape,
    ScalingProfile,
    _build_type,
    _certified_metrics,
    _certified_missing,
    _certified_shape,
)


def _frames(*seqs: int) -> list[SeriesFrame]:
    """Echo frames one second apart carrying the given counters in order."""
    return [
        SeriesFrame(ts_ns=k * 1_000_000_000, input_tick=k, update_seq=seq)
        for k, seq in enumerate(seqs)
    ]


class TestCertifiedMissingRule:
    def test_sparse_echoes_with_full_coverage_read_zero(self) -> None:
        # 10 submitted inputs, 3 echoes sampling counters 0 -> 4 -> 10:
        # the counter delta certifies every application despite the
        # rebuild cadence sampling only 3 of them — the sampling floor the
        # event rule floors at does not exist here.
        missing, fallback = _certified_missing(_frames(0, 4, 10), submitted=10, observed=3)
        assert missing == 0
        assert not fallback

    def test_dropped_inputs_track_the_drop_fraction(self) -> None:
        # 10 submitted, only 6 applied (counters reach 6): the mint skips
        # the dropped inputs, so certified missing reads 4 — the
        # over-credit-freedom proof a frontier tick rule cannot give.
        missing, fallback = _certified_missing(_frames(0, 3, 6), submitted=10, observed=3)
        assert missing == 4
        assert not fallback

    def test_over_coverage_clamps_to_zero_missing(self) -> None:
        # Applies of pre-window inputs landing after the first echo can
        # push the delta past the submission count; the client clamps at
        # zero so the surplus cannot mask another client's missing.
        missing, fallback = _certified_missing(_frames(2, 9), submitted=5, observed=2)
        assert missing == 0
        assert not fallback

    def test_all_zero_counters_are_a_fallback(self) -> None:
        # A publisher with no minted counters (or stamping disabled): the client
        # rides the event rule (the collector's fallback contract).
        missing, fallback = _certified_missing(_frames(0, 0, 0), submitted=10, observed=4)
        assert missing == 6
        assert fallback

    def test_no_echo_at_all_is_a_full_miss_not_a_fallback(self) -> None:
        missing, fallback = _certified_missing([], submitted=10, observed=0)
        assert missing == 10
        assert not fallback

    def test_baseline_is_the_first_echo_not_zero(self) -> None:
        # The counter's absolute position is irrelevant: only the delta
        # from the first in-interval echo certifies, so a client with a
        # mid-run login is not credited (or charged) for earlier ones.
        missing, _ = _certified_missing(_frames(500, 503, 505), submitted=5, observed=3)
        assert missing == 0


class TestCertifiedAggregate:
    def test_ratio_sums_missing_over_submitted(self) -> None:
        rows = (
            ClientDepthRow(
                name="a", submitted=10, observed=3, certified_missing=0, certified_fallback=False
            ),
            ClientDepthRow(
                name="b", submitted=10, observed=3, certified_missing=4, certified_fallback=False
            ),
        )
        ratio, fallbacks = _certified_metrics(rows)
        assert ratio == 0.2
        assert fallbacks == 0

    def test_fallback_clients_count_but_also_contribute(self) -> None:
        rows = (
            ClientDepthRow(
                name="a", submitted=10, observed=10, certified_missing=0, certified_fallback=False
            ),
            ClientDepthRow(
                name="b", submitted=10, observed=4, certified_missing=6, certified_fallback=True
            ),
        )
        ratio, fallbacks = _certified_metrics(rows)
        assert ratio == 0.3
        assert fallbacks == 1

    def test_no_submissions_yield_none(self) -> None:
        rows = (
            ClientDepthRow(
                name="a", submitted=0, observed=0, certified_missing=0, certified_fallback=False
            ),
        )
        ratio, fallbacks = _certified_metrics(rows)
        assert ratio is None
        assert fallbacks == 0


class TestCertifiedGateRendering:
    def _report(self, certified: float | None) -> GateReport:
        return GateReport(
            profile=SMOKE_DENSE,
            metrics=GateMetrics(
                actor_count=24,
                sampled_clients=16,
                session_ok_ratio=1.0,
                selected_clients_ratio=1.0,
                move_missing_ratio=0.32,
                bootstrap_ms_p95=10,
                continuity_flicker=False,
                move_missing_ratio_certified=certified,
                certified_fallback_clients=2,
            ),
            thresholds=DISTRIBUTED_THRESHOLDS,
            started_at="2026-08-28T00:00:00+00:00",
            duration_s=0.5,
        )

    def test_markdown_renders_both_rules(self) -> None:
        text = self._report(0.02).to_markdown()
        assert "move_missing_ratio_certified" in text
        assert "mm_event (companion)" in text
        assert "certified_fallback_clients" in text

    def test_verdict_binds_the_certified_rule(self) -> None:
        assert self._report(0.02).passed
        assert not self._report(0.11).passed

    def test_absent_certified_series_fails_a_gated_run(self) -> None:
        # A run with no certifiable series must not green: None
        # against a set threshold is a failure, not a pass-by-omission.
        assert not self._report(None).passed

    def test_gating_profile_binds_a_certified_bar(self) -> None:
        # The distributed verdict binds the certified ratio, so the
        # threshold table itself must carry a value: a None bar there
        # leaves the verdict unreachable.
        assert DISTRIBUTED_THRESHOLDS.move_missing_certified_max is not None


class TestRunEnvelope:
    def test_build_type_reads_the_cmake_cache(self, tmp_path: Path) -> None:
        (tmp_path / "CMakeCache.txt").write_text(
            "CMAKE_BUILD_TYPE:STRING=Release\n", encoding="utf-8"
        )
        assert _build_type(str(tmp_path)) == "Release"

    def test_build_type_falls_back_to_the_parent_tree(self, tmp_path: Path) -> None:
        lib = tmp_path / "lib"
        lib.mkdir()
        (tmp_path / "CMakeCache.txt").write_text(
            "CMAKE_BUILD_TYPE:STRING=RelWithDebInfo\n", encoding="utf-8"
        )
        assert _build_type(str(lib)) == "RelWithDebInfo"

    def test_missing_cache_reads_none(self, tmp_path: Path) -> None:
        assert _build_type(str(tmp_path)) is None

    def test_report_renders_the_envelope(self) -> None:
        envelope = RunEnvelope(
            build_type="Release",
            kernel="9.9.9-test",
            cpu_model="Test CPU",
            cpu_threads=16,
            governor="powersave",
            interpreter="3.14.7",
            gil_status="GIL disabled",
        )
        report = GateReport(
            profile=SMOKE_DENSE,
            metrics=GateMetrics(
                actor_count=24,
                sampled_clients=16,
                session_ok_ratio=1.0,
                selected_clients_ratio=1.0,
                move_missing_ratio=0.32,
                bootstrap_ms_p95=10,
                continuity_flicker=False,
                move_missing_ratio_certified=0.02,
                certified_fallback_clients=2,
            ),
            thresholds=DISTRIBUTED_THRESHOLDS,
            started_at="2026-08-30T00:00:00+00:00",
            duration_s=0.5,
            envelope=envelope,
        )
        text = report.to_markdown()
        assert "**Build:** Release" in text
        assert "**Kernel:** 9.9.9-test" in text
        assert "**CPU:** Test CPU (16 threads)" in text
        assert "**Governor:** powersave" in text
        assert "**Interpreter:** 3.14.7 (GIL disabled)" in text
        payload = report.to_dict()
        assert payload["envelope"] is not None

    def test_report_without_envelope_renders_no_stamp(self) -> None:
        report = GateReport(
            profile=SMOKE_DENSE,
            metrics=GateMetrics(
                actor_count=24,
                sampled_clients=16,
                session_ok_ratio=1.0,
                selected_clients_ratio=1.0,
                move_missing_ratio=0.32,
                bootstrap_ms_p95=10,
                continuity_flicker=False,
                move_missing_ratio_certified=0.02,
                certified_fallback_clients=2,
            ),
            thresholds=DISTRIBUTED_THRESHOLDS,
            started_at="2026-08-30T00:00:00+00:00",
            duration_s=0.5,
        )
        assert "**Build:**" not in report.to_markdown()
        assert report.to_dict()["envelope"] is None


def _shape_args(**overrides: object) -> argparse.Namespace:
    """An argv namespace shaped like the distributed certified invocation."""
    base: dict[str, object] = {
        "subprocess_cluster": True,
        "dual_drivers": True,
        "instances": 2,
        "distributed": False,
        "embedded": False,
        "native_apply": False,
    }
    base.update(overrides)
    return argparse.Namespace(**base)


def _distributed_resolved(*, delivery_workers: int = 2, actor_count: int = 2000) -> ScalingProfile:
    """The distributed profile as ``main`` resolves it for a certified run.

    ``--delivery-workers 2`` is part of the certified invocation and is
    applied to the profile before the shape derivation reads it, so the
    unit under test is the resolved profile, not the declared one.
    """
    return replace(
        DISTRIBUTED_2000,
        delivery_workers=delivery_workers,
        actor_count=actor_count,
    )


class TestRunShape:
    def _certified(
        self,
        profile: ScalingProfile,
        args: argparse.Namespace,
        monkeypatch: pytest.MonkeyPatch,
        *,
        gil: str = "GIL disabled",
        native_apply_env: str | None = "1",
    ) -> RunShape:
        monkeypatch.setattr(harness_module, "_gil_status", lambda: gil)
        if native_apply_env is None:
            monkeypatch.delenv("KITH_NATIVE_APPLY", raising=False)
        else:
            monkeypatch.setenv("KITH_NATIVE_APPLY", native_apply_env)
        return _certified_shape(profile, args)

    def _report(self, shape: RunShape) -> GateReport:
        return GateReport(
            profile=SMOKE_DENSE,
            metrics=GateMetrics(
                actor_count=24,
                sampled_clients=16,
                session_ok_ratio=1.0,
                selected_clients_ratio=1.0,
                move_missing_ratio=0.32,
                bootstrap_ms_p95=10,
                continuity_flicker=False,
                move_missing_ratio_certified=0.02,
                certified_fallback_clients=2,
            ),
            thresholds=DISTRIBUTED_THRESHOLDS,
            started_at="2026-09-07T00:00:00+00:00",
            duration_s=0.5,
            shape=shape,
        )

    def test_distributed_certified_invocation_reads_true(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        shape = self._certified(_distributed_resolved(), _shape_args(), monkeypatch)
        assert shape == RunShape(apply_regime="native", certified_shape=True, deviations=())

    def test_missing_native_apply_deviates(self, monkeypatch: pytest.MonkeyPatch) -> None:
        shape = self._certified(
            _distributed_resolved(), _shape_args(), monkeypatch, native_apply_env=None
        )
        assert shape.certified_shape is False
        assert shape.deviations == (
            'python apply path (KITH_NATIVE_APPLY is None; native apply requires "1")',
        )

    def test_standard_interpreter_deviates(self, monkeypatch: pytest.MonkeyPatch) -> None:
        shape = self._certified(
            _distributed_resolved(), _shape_args(), monkeypatch, gil="GIL enabled"
        )
        assert shape.certified_shape is False
        assert shape.deviations == (
            "standard interpreter (GIL enabled; the certified shape runs free-threaded Python)",
        )

    def test_missing_delivery_workers_deviates(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # The declared profile keeps the inline-delivery default; the
        # certified invocation resolves --delivery-workers 2 onto it.
        shape = self._certified(DISTRIBUTED_2000, _shape_args(), monkeypatch)
        assert "--delivery-workers 0 (certified: 2)" in shape.deviations

    def test_external_target_regime_is_not_observable(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        args = _shape_args(subprocess_cluster=False, dual_drivers=False)
        shape = self._certified(_distributed_resolved(), args, monkeypatch)
        assert shape.apply_regime == "external"
        assert "apply regime not observable (external target)" in shape.deviations
        assert shape.certified_shape is False

    def test_in_process_distributed_host_stays_python(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # The in-process distributed host wires no native handlers, so the
        # regime is python even with the env knob set; the host deviation
        # fires alongside it.
        args = _shape_args(subprocess_cluster=False, dual_drivers=False, distributed=True)
        shape = self._certified(_distributed_resolved(), args, monkeypatch)
        assert shape.apply_regime == "python"
        assert "the run did not use --subprocess-cluster" in shape.deviations

    def test_clients_override_deviates(self, monkeypatch: pytest.MonkeyPatch) -> None:
        profile = _distributed_resolved(actor_count=400)
        shape = self._certified(profile, _shape_args(), monkeypatch)
        assert "--clients override to 400 (certified: 2000)" in shape.deviations

    def test_dense_certified_invocation_reads_true(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # The dense gate runs under either interpreter, so a GIL-enabled
        # host is not a dense-shape deviation.
        args = argparse.Namespace(
            subprocess_cluster=False,
            dual_drivers=False,
            instances=1,
            distributed=False,
            embedded=True,
            native_apply=False,
        )
        shape = self._certified(DENSE_1000, args, monkeypatch, gil="GIL enabled")
        assert shape == RunShape(apply_regime="python", certified_shape=True, deviations=())

    def test_dense_native_apply_deviates(self, monkeypatch: pytest.MonkeyPatch) -> None:
        args = argparse.Namespace(
            subprocess_cluster=False,
            dual_drivers=False,
            instances=1,
            distributed=False,
            embedded=True,
            native_apply=True,
        )
        shape = self._certified(DENSE_1000, args, monkeypatch)
        assert shape.certified_shape is False
        assert shape.deviations == (
            "native apply path (the dense certified configuration holds the python apply path)",
        )

    def test_uncertified_profile_reports_none(self, monkeypatch: pytest.MonkeyPatch) -> None:
        args = argparse.Namespace(
            subprocess_cluster=False,
            dual_drivers=False,
            instances=1,
            distributed=False,
            embedded=True,
            native_apply=False,
        )
        shape = self._certified(SMOKE_DENSE, args, monkeypatch)
        assert shape.certified_shape is None
        assert shape.deviations == ()

    def test_markdown_stamps_regime_and_shape(self) -> None:
        text = self._report(
            RunShape(apply_regime="native", certified_shape=True, deviations=())
        ).to_markdown()
        assert "**Apply regime:** native" in text
        assert "**Certified shape:** yes" in text
        assert "NOT A CERTIFIED SHAPE" not in text

    def test_markdown_banner_names_deviations(self) -> None:
        shape = RunShape(
            apply_regime="python",
            certified_shape=False,
            deviations=("--dual-drivers absent (single driver loop)",),
        )
        text = self._report(shape).to_markdown()
        assert "> **NOT A CERTIFIED SHAPE** — deviations from the documented" in text
        assert "> - --dual-drivers absent (single driver loop)" in text
        assert "**Certified shape:** no" in text

    def test_json_carries_the_greppable_tokens(self) -> None:
        shape = RunShape(apply_regime="native", certified_shape=True, deviations=())
        assert self._report(shape).to_dict()["shape"] == {
            "apply_regime": "native",
            "certified_shape": True,
            "deviations": (),
        }

    def test_uncertified_shape_reads_n_a_without_banner(self) -> None:
        text = self._report(
            RunShape(apply_regime="python", certified_shape=None, deviations=())
        ).to_markdown()
        assert "**Certified shape:** n/a" in text
        assert "NOT A CERTIFIED SHAPE" not in text
