"""Integration test: the scaling-gate load harness.

Drives a reduced :data:`~tools.agent.load_harness.SMOKE_DENSE` profile
(24 dense actors, short duration) against a fresh embedded server via
:func:`~tools.agent.embedded_host.embedded_host_factory` and asserts the
harness produces a well-formed :class:`~tools.agent.load_harness.GateReport`
that passes the relaxed smoke thresholds. This validates the harness
machinery end-to-end (multi-client login bootstrap, movement phase, §4.4
metric sampling, threshold comparison, report rendering) without the
1000-connection wall-clock cost of the full gate, which the AGENTS.md §4.1
tier table schedules as Weekly.

A second test drives the :data:`~tools.agent.load_harness.SMOKE_DISTRIBUTED`
profile (24 actors across two instances) against a fresh distributed
cluster via
:func:`~tools.agent.distributed_host.distributed_host_factory`, validating
the distributed drive path (multi-instance bootstrap, per-instance actor
resolution, publish-rate-skew computation) every commit.

A third test pins the /proc sampler's target resolution: the
subprocess-cluster factory appends the server pids only when the drive
invokes it, so the harness must resolve sampler targets from the live
out-list at start time instead of from a constructor-time snapshot.
"""

from __future__ import annotations

import argparse
import asyncio
import itertools
import json
import os
import random
import struct
import time
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import replace
from pathlib import Path
from typing import cast

import pytest
from _helpers import needs_build
from examples._common.query_state import QUERY_STATE_DEFAULT_PAGE_SIZE
from examples.spatial import messages as spatial_messages
from examples.spatial.client import (
    ActorStateView,
    encode_actor_state,
    iter_actor_state_ids,
)
from examples.spatial.handlers import CELL_SIZE_Q16
from tools.agent.distributed_host import DistributedControl, distributed_host_factory
from tools.agent.dual_driver import (
    _SCHEMA_VERSION,
    DriverResult,
    _anchor_problems,
    _worker_argv,
    apply_driver_affinity,
    dump_driver_result,
    load_driver_result,
    merge_driver_results,
    parse_cpu_list,
    split_clients,
    split_instrumented,
)
from tools.agent.embedded_host import (
    _history_size_for,
    _summary_retention_for,
    embedded_host_factory,
)
from tools.agent.gap_ledger import (
    ClientSeries,
    FlickerLedger,
    GapClusterRecord,
    InstanceTickSpread,
    SeriesFrame,
    attribute_clusters,
    build_flicker_ledger,
    cluster_gaps,
    tick_arrival_spreads,
)
from tools.agent.load_harness import (
    _BOOTSTRAP_READY,
    _BREADTH_EVIDENCE_BUDGET_BYTES,
    _DEFAULT_STAGGER_MS,
    _OBSERVER_RECALIBRATION_ENV,
    DENSE_1000,
    DENSE_THRESHOLDS,
    DISTRIBUTED_2000,
    DISTRIBUTED_THRESHOLDS,
    SMOKE_DENSE,
    SMOKE_DISTRIBUTED,
    SMOKE_DISTRIBUTED_THRESHOLDS,
    SMOKE_THRESHOLDS,
    BreadthAttribution,
    BreadthCensus,
    ClientDepthRow,
    ClientFlickerSample,
    CohortDepth,
    CohortRow,
    CounterSample,
    FlickerBreakdown,
    GateMetrics,
    GateReport,
    GateThresholds,
    InstanceGateMetrics,
    LoadHarness,
    LoadHostFactory,
    PhaseCounters,
    ScalingProfile,
    SparseBreadthRow,
    _anchor_delay_s,
    _apply_client_override,
    _apply_delivery_override,
    _apply_tiered_max_gap_override,
    _apply_worker_override,
    _assemble_breadth,
    _BreadthRow,
    _build_parser,
    _client_view_facts,
    _client_view_set_healthy,
    _ClientRecord,
    _collect_breadth,
    _collect_depth_metrics,
    _DriveOutcome,
    _echo_delta_histogram,
    _env_python_workers,
    _flicker_breakdown_from_series,
    _instance_gate_metrics,
    _isolated_actor_ids,
    _move_missing_ratio,
    _observer_limits_from_args,
    _parse_phase_counters,
    _per_instance_ranks,
    _phase_counters_delta,
    _principal_bindings,
    _publish_rate_counts,
    _publish_rate_skew,
    _render_counter_samples,
    _render_echo_delta,
    _resolve_own_actors_dense,
    _roster_positions,
    _selected_clients_ratio,
    _spread_probe_indices,
    _validate_driver_flags,
    _WindowCounters,
    _write_proc_trace,
    run_profile,
)
from tools.agent.observer_gates import (
    InstanceServerSample,
    ObservationVerdict,
    ObserverLimits,
    evaluate_observation,
)
from tools.agent.orchestrator import Orchestrator
from tools.agent.proc_sampler import (
    ProcSample,
    ProcSampler,
    ProcTrace,
    SamplerTarget,
    TargetSample,
    ThreadSample,
    _parse_stat,
    _thread_sample_from_texts,
)
from tools.agent.server_control import ServerControlClient, ServerControlError
from tools.agent.subprocess_cluster_host import subprocess_cluster_factory

from kith._agent.ahc import (
    _HEADER,
    _VIEW_WINDOW_ID_CAP,
    AgenticHeadlessClient,
    ClientEvent,
    ClientStatus,
    WindowEvidence,
)


_REPO_ROOT = Path(__file__).resolve().parents[2]

# The gate breadth bar, read off the certified profile: the probe tests
# straddle it to prove both sides, so fixture sizes and probe arguments
# track the bar instead of repeating its value.
_BREADTH_BAR = DISTRIBUTED_THRESHOLDS.min_distinct_others


def _summary_ahc() -> AgenticHeadlessClient:
    """Build a summary-mode AHC wired for the batch replication type."""
    return AgenticHeadlessClient(
        "test-summary-health",
        direct_type_ids=frozenset({spatial_messages.ACTOR_STATE_BATCH_TYPE}),
        http_enabled=False,
        ipc_enabled=False,
        auto_connect=False,
        reconnect_enabled=False,
        summary_mode=True,
        evidence_budget_bytes=_BREADTH_EVIDENCE_BUDGET_BYTES,
        window_id_extractor=iter_actor_state_ids,
    )


@needs_build
class TestScalingGateHarness:
    def test_smoke_dense_profile_passes(self) -> None:
        """The reduced dense profile passes the smoke thresholds.

        Exercises the harness machinery (register, bootstrap, movement,
        metric sampling, threshold comparison) end-to-end against a fresh
        embedded server. The smoke thresholds (relaxed vs. the §4.4 bars)
        assert the harness produces a sane report; the full 1000-actor gate
        runs via the CLI in the weekly tier.
        """
        factory: LoadHostFactory = embedded_host_factory(
            ahc_event_history_size=8192,
            evidence_budget_bytes=_BREADTH_EVIDENCE_BUDGET_BYTES,
        )
        report = asyncio.run(run_profile(SMOKE_DENSE, factory))
        assert report.profile is SMOKE_DENSE
        assert report.thresholds is SMOKE_THRESHOLDS
        assert report.metrics.actor_count == SMOKE_DENSE.actor_count
        assert report.passed, _format_failure(report)
        assert report.metrics.session_ok_ratio >= SMOKE_THRESHOLDS.session_ok_min

    def test_distributed_smoke_profile_passes(self) -> None:
        """The reduced distributed profile passes the smoke thresholds.

        Exercises the distributed drive path (multi-instance bootstrap,
        per-instance own-actor resolution, publish-rate-skew computation)
        end-to-end against a fresh two-instance cluster. The smoke
        thresholds (relaxed vs. the §4.4 bars) assert the harness
        produces a sane report across instances; the full 2000-actor
        gate runs via the CLI in the weekly tier.
        """
        factory: LoadHostFactory = distributed_host_factory(
            ahc_event_history_size=8192,
            instance_count=2,
            evidence_budget_bytes=_BREADTH_EVIDENCE_BUDGET_BYTES,
        )
        report = asyncio.run(run_profile(SMOKE_DISTRIBUTED, factory))
        assert report.profile is SMOKE_DISTRIBUTED
        assert report.thresholds is SMOKE_DISTRIBUTED_THRESHOLDS
        assert report.metrics.actor_count == SMOKE_DISTRIBUTED.actor_count
        assert report.passed, _format_failure(report)
        assert report.metrics.session_ok_ratio >= SMOKE_DISTRIBUTED_THRESHOLDS.session_ok_min
        assert (
            report.metrics.publish_rate_skew <= SMOKE_DISTRIBUTED_THRESHOLDS.publish_rate_skew_max
        )

    def test_subprocess_cluster_smoke_profile_passes(self) -> None:
        """The reduced distributed profile passes against a subprocess cluster.

        Exercises the out-of-process distributed host (one embedded-server OS
        process per instance) end-to-end: subprocess spawn, the
        ``embedded: gateway=.. control=..`` handshake parse, round-robin
        across the captured gateway ports, per-instance actor resolution over
        the per-instance control clients, and SIGINT shutdown. This is the
        host the full distributed-2000 gate runs against, decoupling the
        harness's client-drive loop from the servers' worker-pool GILs; the
        smoke profile validates its machinery every commit without the
        2000-connection wall-clock cost of the full gate.
        """
        factory: LoadHostFactory = subprocess_cluster_factory(
            ahc_event_history_size=8192,
            instance_count=2,
            replication_batch_type_id=spatial_messages.ACTOR_STATE_BATCH_TYPE,
            evidence_budget_bytes=_BREADTH_EVIDENCE_BUDGET_BYTES,
        )
        report = asyncio.run(run_profile(SMOKE_DISTRIBUTED, factory))
        assert report.profile is SMOKE_DISTRIBUTED
        assert report.thresholds is SMOKE_DISTRIBUTED_THRESHOLDS
        assert report.metrics.actor_count == SMOKE_DISTRIBUTED.actor_count
        assert report.passed, _format_failure(report)
        assert report.metrics.session_ok_ratio >= SMOKE_DISTRIBUTED_THRESHOLDS.session_ok_min
        assert (
            report.metrics.publish_rate_skew <= SMOKE_DISTRIBUTED_THRESHOLDS.publish_rate_skew_max
        )

    def test_smoke_distributed_tiered_strategy_passes(self) -> None:
        """The reduced distributed profile passes with the tiered strategy.

        Boots the in-process cluster with the built-in ``tiered`` delivery
        preset selected through the host factory chain (factory ->
        ``EmbeddedServer`` -> facade params -> gateway create), proving the
        strategy name and its cadence image reach the C gateway and the
        drive still satisfies the smoke thresholds under suppression.
        """
        tiered = replace(SMOKE_DISTRIBUTED, delivery_strategy="tiered")
        factory: LoadHostFactory = distributed_host_factory(
            ahc_event_history_size=8192,
            instance_count=2,
            delivery_strategy="tiered",
            evidence_budget_bytes=_BREADTH_EVIDENCE_BUDGET_BYTES,
        )
        report = asyncio.run(run_profile(tiered, factory))
        assert report.profile.delivery_strategy == "tiered"
        assert report.thresholds is SMOKE_DISTRIBUTED_THRESHOLDS
        assert report.passed, _format_failure(report)
        assert report.metrics.session_ok_ratio >= SMOKE_DISTRIBUTED_THRESHOLDS.session_ok_min

    def test_subprocess_cluster_smoke_tiered_strategy_passes(self) -> None:
        """The subprocess cluster passes with the tiered strategy.

        The full distributed-2000 gate runs the tiered preset against this
        host, so the argv threading (--delivery-strategy on each
        ``python -m examples.embedded.server`` child) is exercised at smoke
        scale every commit: spawn, handshake parse, tiered session binds,
        movement drive, and SIGINT shutdown.
        """
        tiered = replace(SMOKE_DISTRIBUTED, delivery_strategy="tiered")
        factory: LoadHostFactory = subprocess_cluster_factory(
            ahc_event_history_size=8192,
            instance_count=2,
            replication_batch_type_id=spatial_messages.ACTOR_STATE_BATCH_TYPE,
            delivery_strategy="tiered",
            evidence_budget_bytes=_BREADTH_EVIDENCE_BUDGET_BYTES,
        )
        report = asyncio.run(run_profile(tiered, factory))
        assert report.profile.delivery_strategy == "tiered"
        assert report.thresholds is SMOKE_DISTRIBUTED_THRESHOLDS
        assert report.passed, _format_failure(report)
        assert report.metrics.session_ok_ratio >= SMOKE_DISTRIBUTED_THRESHOLDS.session_ok_min

    def test_subprocess_cluster_drive_samples_server_pids(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The drive samples every server subprocess, not only the harness.

        The host factory appends the subprocess pids to the out-list when
        the drive invokes it — after ``LoadHarness`` construction, before
        the sampler starts — so the sampler must resolve its targets from
        the live sequence at start time. A constructor-time snapshot of
        the still-empty list silently reduces every gap-window attribution
        to the harness process alone.
        """
        started: list[list[SamplerTarget]] = []
        factory_pids: list[int] = []

        class _StubSampler:
            def stop(self) -> ProcTrace:
                return ProcTrace(interval_s=0.1, clk_tck=100, samples=())

        def _spy_start(
            targets: Sequence[SamplerTarget], *, interval_s: float = 0.1
        ) -> _StubSampler:
            del interval_s
            started.append(list(targets))
            return _StubSampler()

        monkeypatch.setattr(ProcSampler, "start", staticmethod(_spy_start))

        factory: LoadHostFactory = subprocess_cluster_factory(
            ahc_event_history_size=8192,
            instance_count=2,
            replication_batch_type_id=spatial_messages.ACTOR_STATE_BATCH_TYPE,
            server_pids_out=factory_pids,
            evidence_budget_bytes=_BREADTH_EVIDENCE_BUDGET_BYTES,
        )
        report = asyncio.run(run_profile(SMOKE_DISTRIBUTED, factory, server_pids=factory_pids))

        assert report.passed, _format_failure(report)
        assert len(started) == 1
        names = sorted(target.name for target in started[0])
        assert names == ["harness", "instance-0", "instance-1"]
        sampled = {target.pid for target in started[0] if target.name != "harness"}
        assert factory_pids, "the factory appended no server pids"
        assert sampled == set(factory_pids)

    def test_report_rendering(self) -> None:
        """A constructed report renders to JSON and Markdown without raising.

        The report is a pure value object; rendering must not depend on a
        running server or event loop.
        """
        report = GateReport(
            profile=SMOKE_DENSE,
            metrics=GateMetrics(
                actor_count=24,
                sampled_clients=16,
                session_ok_ratio=1.0,
                selected_clients_ratio=1.0,
                move_missing_ratio=0.0,
                bootstrap_ms_p95=10,
                continuity_flicker=False,
            ),
            thresholds=SMOKE_THRESHOLDS,
            started_at="2026-08-17T00:00:00+00:00",
            duration_s=0.5,
        )
        assert report.passed
        assert "smoke-dense" in report.to_markdown()
        assert "smoke-dense" in report.to_json()

    def test_dense_stress_gate_reports_delivery_without_thresholding(self) -> None:
        """The dense gate is a stress gate: delivery metrics are reported, not gated.

        A report with healthy session integrity but catastrophic delivery
        fidelity (every movement input missing, every instrumented client
        flickering) still passes the dense gate, and the rendered report
        shows the delivery metrics as reported (no threshold, a dash in the
        pass column). The same metrics fail the distributed fidelity gate,
        which thresholds them per instance.
        """
        metrics = GateMetrics(
            actor_count=1000,
            sampled_clients=16,
            session_ok_ratio=1.0,
            selected_clients_ratio=0.99,
            move_missing_ratio=1.0,
            bootstrap_ms_p95=500,
            continuity_flicker=True,
        )
        dense = GateReport(
            profile=DENSE_1000,
            metrics=metrics,
            thresholds=DENSE_THRESHOLDS,
            started_at="2026-08-20T00:00:00+00:00",
            duration_s=6.0,
        )
        assert dense.passed
        rendered = dense.to_markdown()
        # The reported metrics carry a dash, not a yes/no verdict; the
        # renderer's general shape is pinned in test_report_rendering.
        assert "| — |" in rendered

        distributed = GateReport(
            profile=DISTRIBUTED_2000,
            metrics=GateMetrics(
                actor_count=2000,
                sampled_clients=16,
                session_ok_ratio=1.0,
                selected_clients_ratio=0.99,
                move_missing_ratio=1.0,
                bootstrap_ms_p95=500,
                continuity_flicker=True,
                publish_rate_skew=1.0,
            ),
            thresholds=DISTRIBUTED_THRESHOLDS,
            started_at="2026-08-20T00:00:00+00:00",
            duration_s=6.0,
        )
        assert not distributed.passed
        dist_rendered = distributed.to_markdown()
        assert "< 10.00%" in dist_rendered
        assert "forbidden" in dist_rendered

    def test_report_renders_submit_round_completion(self) -> None:
        """The rounds row derives from profile cadence and pooled totals.

        Planned rounds are the movement window's cadence integral; the
        completed numerator is the summed per-client submit count, so a
        driver that paces itself below the plan shows up as its own
        diagnostic instead of silently thinning the exercised load.
        """
        metrics = GateMetrics(
            actor_count=2000,
            sampled_clients=16,
            session_ok_ratio=1.0,
            selected_clients_ratio=0.99,
            move_missing_ratio=1.0,
            bootstrap_ms_p95=500,
            continuity_flicker=False,
            publish_rate_skew=1.0,
            inputs_submitted=88_000,
        )
        report = GateReport(
            profile=DISTRIBUTED_2000,
            metrics=metrics,
            thresholds=DISTRIBUTED_THRESHOLDS,
            started_at="2026-08-20T00:00:00+00:00",
            duration_s=6.0,
        )
        rendered = report.to_markdown()
        # 88000 / 2000 clients = 44.0 completed of ceil(5 * 20) planned.
        assert "submit_rounds_per_client" in rendered
        assert "44.0 / 100 planned" in rendered
        payload = report.to_dict()["metrics"]
        assert cast("dict[str, object]", payload)["inputs_submitted"] == 88_000


class TestGateBoundarySemantics:
    """The rendered comparison operators are the enforced ones.

    The binding ceilings are strict (certified move-missing, the mm_event
    companion, bootstrap p95: an exactly-at-bar value fails) and the ratio
    floors inclusive (session_ok, selected_clients: an exactly-at-bar
    value passes), so a run at the exact bar lands on the documented side
    of every operator — the same side the report's threshold column
    prints. The per-row pass column flips with the verdict.
    """

    def _distributed_metrics(self) -> GateMetrics:
        return GateMetrics(
            actor_count=2000,
            sampled_clients=16,
            session_ok_ratio=1.0,
            selected_clients_ratio=1.0,
            move_missing_ratio=0.0,
            bootstrap_ms_p95=100,
            continuity_flicker=False,
            publish_rate_skew=1.0,
            move_missing_ratio_certified=0.0,
        )

    def _distributed_report(self, metrics: GateMetrics) -> GateReport:
        return GateReport(
            profile=DISTRIBUTED_2000,
            metrics=metrics,
            thresholds=DISTRIBUTED_THRESHOLDS,
            started_at="2026-08-20T00:00:00+00:00",
            duration_s=6.0,
        )

    def test_certified_mm_at_the_bar_fails(self) -> None:
        report = self._distributed_report(
            replace(self._distributed_metrics(), move_missing_ratio_certified=0.10)
        )
        assert not report.passed
        assert "| no |" in report.to_markdown()

    def test_certified_mm_below_the_bar_passes(self) -> None:
        report = self._distributed_report(
            replace(self._distributed_metrics(), move_missing_ratio_certified=0.0999)
        )
        assert report.passed

    def test_bootstrap_p95_at_the_bar_fails(self) -> None:
        report = self._distributed_report(
            replace(self._distributed_metrics(), bootstrap_ms_p95=3000)
        )
        assert not report.passed
        assert "| no |" in report.to_markdown()

    def test_bootstrap_p95_below_the_bar_passes(self) -> None:
        report = self._distributed_report(
            replace(self._distributed_metrics(), bootstrap_ms_p95=2999)
        )
        assert report.passed

    def test_session_ok_at_the_floor_passes_and_below_fails(self) -> None:
        at = self._distributed_report(replace(self._distributed_metrics(), session_ok_ratio=0.99))
        assert at.passed
        below = self._distributed_report(
            replace(self._distributed_metrics(), session_ok_ratio=0.9899)
        )
        assert not below.passed

    def test_selected_clients_at_the_floor_passes_and_below_fails(self) -> None:
        at = self._distributed_report(
            replace(self._distributed_metrics(), selected_clients_ratio=0.95)
        )
        assert at.passed
        below = self._distributed_report(
            replace(self._distributed_metrics(), selected_clients_ratio=0.9499)
        )
        assert not below.passed

    def test_companion_mm_event_at_the_bar_fails(self) -> None:
        metrics = GateMetrics(
            actor_count=24,
            sampled_clients=8,
            session_ok_ratio=1.0,
            selected_clients_ratio=1.0,
            move_missing_ratio=0.50,
            bootstrap_ms_p95=100,
            continuity_flicker=False,
        )
        report = GateReport(
            profile=SMOKE_DENSE,
            metrics=metrics,
            thresholds=SMOKE_THRESHOLDS,
            started_at="2026-08-20T00:00:00+00:00",
            duration_s=1.0,
        )
        assert not report.passed
        below = GateReport(
            profile=SMOKE_DENSE,
            metrics=replace(metrics, move_missing_ratio=0.4999),
            thresholds=SMOKE_THRESHOLDS,
            started_at="2026-08-20T00:00:00+00:00",
            duration_s=1.0,
        )
        assert below.passed


def _healthy_instance(index: int) -> InstanceGateMetrics:
    """One instance row whose cohort clears every distributed bar."""
    return InstanceGateMetrics(
        instance_index=index,
        actor_count=1000,
        sampled_clients=8,
        session_ok_ratio=1.0,
        selected_clients_ratio=1.0,
        eligible_clients=998,
        isolated_clients=2,
        classified_clients=1000,
        move_missing_ratio=0.0,
        move_missing_ratio_certified=0.0,
        certified_fallback_clients=0,
        bootstrap_ms_p95=300,
        continuity_flicker=False,
    )


class TestPerInstanceGateRows:
    """The distributed gate judges each instance's cohort, not the union.

    The pooled ratio is a population-weighted average, so a cohort below
    any bar can hide in a stronger partner (985/1000 and 1000/1000 pool
    to 99.25%). The verdict therefore reads the per-instance rows the
    drive and the merge reduce from the same collections as the pooled
    row, and every row must clear the bars alongside the union.
    """

    def _report(self, instances: tuple[InstanceGateMetrics, ...]) -> GateReport:
        metrics = replace(
            GateMetrics(
                actor_count=2000,
                sampled_clients=16,
                session_ok_ratio=1.0,
                selected_clients_ratio=1.0,
                move_missing_ratio=0.0,
                bootstrap_ms_p95=300,
                continuity_flicker=False,
                publish_rate_skew=1.0,
                move_missing_ratio_certified=0.0,
            ),
            instances=instances,
        )
        return GateReport(
            profile=DISTRIBUTED_2000,
            metrics=metrics,
            thresholds=DISTRIBUTED_THRESHOLDS,
            started_at="2026-08-20T00:00:00+00:00",
            duration_s=6.0,
        )

    def test_run_passes_when_every_instance_clears_the_bars(self) -> None:
        assert self._report((_healthy_instance(0), _healthy_instance(1))).passed

    def test_run_fails_when_any_instance_misses_a_bar(self) -> None:
        weak_rows = [
            replace(_healthy_instance(1), session_ok_ratio=0.985),
            replace(_healthy_instance(1), selected_clients_ratio=0.9499),
            replace(_healthy_instance(1), move_missing_ratio_certified=None),
            replace(_healthy_instance(1), move_missing_ratio_certified=0.10),
            replace(_healthy_instance(1), bootstrap_ms_p95=3000),
            replace(_healthy_instance(1), continuity_flicker=True),
        ]
        for weak in weak_rows:
            report = self._report((_healthy_instance(0), weak))
            assert not report.passed, f"instance row passed below the bar: {weak}"

    def test_markdown_renders_per_instance_rows(self) -> None:
        report = self._report(
            (_healthy_instance(0), replace(_healthy_instance(1), session_ok_ratio=0.985))
        )
        text = report.to_markdown()
        assert "## Per-instance metrics" in text
        assert "| 0 | 1000 | 8 | 100.00% | 100.00% | 0.00% | 300 | False | yes |" in text
        assert "| 1 | 1000 | 8 | 98.50% | 100.00% | 0.00% | 300 | False | no |" in text

    def test_instance_rows_slice_one_population(self) -> None:
        """Each row reads the pooled formula's slice, never a re-measurement.

        The per-instance selected denominator is that instance's
        classified-minus-isolated cohort and the certified ratio sums that
        instance's depth rows only — a partial roster or an instrumented
        cohort that lands unevenly cannot shift another instance's slice.
        """
        records = [
            _ClientRecord(
                "a0", 100, True, instance_index=0, started_ns=1_000_000_000, ready_ns=1_100_000_000
            ),
            _ClientRecord(
                "b0", 101, True, instance_index=0, started_ns=1_000_000_000, ready_ns=1_200_000_000
            ),
            _ClientRecord(
                "a1", 102, True, instance_index=1, started_ns=1_000_000_000, ready_ns=1_300_000_000
            ),
        ]
        census = BreadthCensus(
            total=3,
            session_ok_count=3,
            classified_clients=3,
            isolated_clients=1,
            eligible_clients=2,
            selected_count=2,
            instances=(
                BreadthCensus(
                    total=2,
                    session_ok_count=2,
                    classified_clients=2,
                    isolated_clients=0,
                    eligible_clients=2,
                    selected_count=2,
                    instance_index=0,
                ),
                BreadthCensus(
                    total=1,
                    session_ok_count=1,
                    classified_clients=1,
                    isolated_clients=1,
                    eligible_clients=0,
                    selected_count=0,
                    instance_index=1,
                ),
            ),
        )
        cert_rows = (
            ClientDepthRow(
                name="a0", submitted=10, observed=10, certified_missing=0, certified_fallback=False
            ),
            ClientDepthRow(
                name="a1", submitted=10, observed=6, certified_missing=4, certified_fallback=False
            ),
        )
        series = (
            ClientSeries(
                name="a1", instance_index=1, window_start_ns=1, window_end_ns=2, frames=()
            ),
        )
        rows = _instance_gate_metrics(records, census, cert_rows, series)
        assert [r.instance_index for r in rows] == [0, 1]
        assert rows[0].actor_count == 2
        assert rows[0].session_ok_ratio == pytest.approx(1.0)
        assert rows[0].selected_clients_ratio == pytest.approx(1.0)
        assert rows[0].move_missing_ratio == pytest.approx(0.0)
        assert rows[0].move_missing_ratio_certified == pytest.approx(0.0)
        assert rows[0].bootstrap_ms_p95 == 200
        assert rows[1].actor_count == 1
        assert rows[1].selected_clients_ratio == pytest.approx(0.0)
        assert rows[1].move_missing_ratio == pytest.approx(0.4)
        assert rows[1].move_missing_ratio_certified == pytest.approx(0.4)
        assert rows[1].bootstrap_ms_p95 == 300


class TestMeasuredChannelFloor:
    """A gate run that measured nothing cannot pass.

    The gate profiles require a non-empty depth sample: with no sampled
    clients the fidelity metrics read their unmeasured defaults (0.0
    ratios, no flicker), which would green a run whose instrument never
    came up. The breadth channel needs no flag — an empty population
    reads a 0.0 session ratio and fails the floor bar on its own. The
    smoke profiles opt out: their machinery contract does not include a
    measured-fidelity floor.
    """

    def _metrics(self, sampled_clients: int) -> GateMetrics:
        return GateMetrics(
            actor_count=2000,
            sampled_clients=sampled_clients,
            session_ok_ratio=1.0,
            selected_clients_ratio=1.0,
            move_missing_ratio=0.0,
            bootstrap_ms_p95=300,
            continuity_flicker=False,
            publish_rate_skew=1.0,
            move_missing_ratio_certified=0.0,
        )

    def test_empty_depth_sample_fails_the_distributed_gate(self) -> None:
        report = GateReport(
            profile=DISTRIBUTED_2000,
            metrics=self._metrics(0),
            thresholds=DISTRIBUTED_THRESHOLDS,
            started_at="2026-08-20T00:00:00+00:00",
            duration_s=6.0,
        )
        assert not report.passed

    def test_empty_depth_sample_fails_the_dense_gate(self) -> None:
        report = GateReport(
            profile=DENSE_1000,
            metrics=replace(self._metrics(0), move_missing_ratio_certified=None),
            thresholds=DENSE_THRESHOLDS,
            started_at="2026-08-20T00:00:00+00:00",
            duration_s=6.0,
        )
        assert not report.passed

    def test_a_measured_sample_passes(self) -> None:
        report = GateReport(
            profile=DENSE_1000,
            metrics=replace(self._metrics(16), move_missing_ratio_certified=None),
            thresholds=DENSE_THRESHOLDS,
            started_at="2026-08-20T00:00:00+00:00",
            duration_s=6.0,
        )
        assert report.passed

    def test_smoke_profiles_opt_out(self) -> None:
        report = GateReport(
            profile=SMOKE_DENSE,
            metrics=GateMetrics(
                actor_count=24,
                sampled_clients=0,
                session_ok_ratio=1.0,
                selected_clients_ratio=1.0,
                move_missing_ratio=0.0,
                bootstrap_ms_p95=10,
                continuity_flicker=False,
            ),
            thresholds=SMOKE_THRESHOLDS,
            started_at="2026-08-20T00:00:00+00:00",
            duration_s=1.0,
        )
        assert report.passed


class TestBindingNumberPins:
    """The gate thresholds are binding numbers, pinned literally here.

    The certified bars live in the threshold constants and every other
    surface (the scaling guide's tables, the report's threshold column,
    the per-instance rows) reads them from there, so an edit to a
    constant trips these asserts and stands out as a deliberate
    re-derivation of the certified claims rather than a silent change.
    """

    def test_dense_stress_gate_numbers(self) -> None:
        assert DENSE_THRESHOLDS.session_ok_min == 0.99
        assert DENSE_THRESHOLDS.selected_clients_min == 0.95
        assert DENSE_THRESHOLDS.bootstrap_ms_p95_max == 3000
        assert DENSE_THRESHOLDS.move_missing_max is None
        assert DENSE_THRESHOLDS.continuity_flicker_forbidden is False
        assert DENSE_THRESHOLDS.move_missing_certified_max is None
        assert DENSE_THRESHOLDS.publish_rate_skew_max == float("inf")
        assert DENSE_THRESHOLDS.min_distinct_others == 128
        assert DENSE_THRESHOLDS.observer_gates is False
        assert DENSE_THRESHOLDS.require_measured_channels is True

    def test_distributed_fidelity_gate_numbers(self) -> None:
        assert DISTRIBUTED_THRESHOLDS.session_ok_min == 0.99
        assert DISTRIBUTED_THRESHOLDS.selected_clients_min == 0.95
        assert DISTRIBUTED_THRESHOLDS.move_missing_certified_max == 0.10
        assert DISTRIBUTED_THRESHOLDS.bootstrap_ms_p95_max == 3000
        assert DISTRIBUTED_THRESHOLDS.continuity_flicker_forbidden is True
        assert DISTRIBUTED_THRESHOLDS.publish_rate_skew_max == 3.0
        assert DISTRIBUTED_THRESHOLDS.min_distinct_others == 128
        assert DISTRIBUTED_THRESHOLDS.observer_gates is True
        assert DISTRIBUTED_THRESHOLDS.require_measured_channels is True

    def test_both_gate_profiles_share_the_breadth_bar(self) -> None:
        assert DENSE_THRESHOLDS.min_distinct_others == DISTRIBUTED_THRESHOLDS.min_distinct_others

    def test_smoke_profile_numbers(self) -> None:
        assert SMOKE_THRESHOLDS.session_ok_min == 0.90
        assert SMOKE_THRESHOLDS.selected_clients_min == 0.50
        assert SMOKE_THRESHOLDS.move_missing_max == 0.50
        assert SMOKE_THRESHOLDS.bootstrap_ms_p95_max == 5000
        assert SMOKE_THRESHOLDS.continuity_flicker_forbidden is False
        assert SMOKE_THRESHOLDS.min_distinct_others == 1
        assert SMOKE_DISTRIBUTED_THRESHOLDS.session_ok_min == 0.90
        assert SMOKE_DISTRIBUTED_THRESHOLDS.selected_clients_min == 0.50
        assert SMOKE_DISTRIBUTED_THRESHOLDS.move_missing_max == 0.50
        assert SMOKE_DISTRIBUTED_THRESHOLDS.bootstrap_ms_p95_max == 5000
        assert SMOKE_DISTRIBUTED_THRESHOLDS.continuity_flicker_forbidden is False
        assert SMOKE_DISTRIBUTED_THRESHOLDS.publish_rate_skew_max == 10.0
        assert SMOKE_DISTRIBUTED_THRESHOLDS.min_distinct_others == 1


class TestHistorySizing:
    """The instrumented/bulk event-history split bounds per-client memory.

    Only the depth-metrics sample (``move_missing`` / ``continuity_flicker``)
    needs a history large enough to hold the full movement-window stream;
    the bulk clients carry a small recent window. This is a pure-logic test
    (no C build, no server) so it runs in every environment.
    """

    def test_instrumented_clients_get_the_large_window(self) -> None:
        """The first ``instrumented_count`` clients get the instrumented size."""
        for index in range(32):
            assert _history_size_for(index, 32, 8192, 64) == 8192

    def test_bulk_clients_get_the_small_window(self) -> None:
        """Clients beyond the instrumented sample get the bulk size."""
        for index in range(32, 2000):
            assert _history_size_for(index, 32, 8192, 64) == 64

    def test_no_split_keeps_every_client_at_instrumented_size(self) -> None:
        """``instrumented_count=0`` gives every client the instrumented size.

        This is the closed-loop default (scenario tests pass a single
        history size with no bulk split).
        """
        for index in (0, 1, 100, 999):
            assert _history_size_for(index, 0, 512, 64) == 512

    def test_no_bulk_size_keeps_every_client_at_instrumented_size(self) -> None:
        """``bulk_event_history_size=None`` disables the split."""
        for index in (0, 31, 32, 100):
            assert _history_size_for(index, 32, 512, None) == 512

    def test_summary_retention_follows_the_bulk_split(self) -> None:
        """Summary mode applies to exactly the clients the bulk split covers."""
        for index in range(32):
            assert _summary_retention_for(index, 32, 64) is False
        for index in range(32, 2000):
            assert _summary_retention_for(index, 32, 64) is True
        assert _summary_retention_for(50, 0, 64) is False
        assert _summary_retention_for(50, 32, None) is False


@needs_build
class TestViewSetHealthFromMovementWindow:
    """The breadth health check reads the sealed movement-window evidence.

    A client's view accumulator opens with the movement window and seals
    at its end plus the breadth drain; ``_client_view_set_healthy`` reads
    that accumulator, so the reading cannot race the collection crawl —
    the per-frame census proves the probe-moment recency window
    unreachable by delivery design.
    """

    @pytest.fixture(autouse=True)
    def _release_window_orchestrators(self) -> Iterator[None]:
        """Shut down every harness the test builds, releasing its clients."""
        orchs: list[Orchestrator] = []
        self._window_orchestrators = orchs
        yield
        first_error: Exception | None = None
        for orch in orchs:
            try:
                asyncio.run(orch.shutdown())
            except Exception as exc:
                first_error = first_error or exc
        if first_error is not None:
            raise first_error

    @staticmethod
    def _batch_payload(actor_ids: Sequence[int]) -> bytes:
        views = [
            ActorStateView(
                actor_id=actor_id,
                pos_x=0,
                pos_y=0,
                pos_z=0,
                vel_x=0,
                vel_y=0,
                vel_z=0,
                input_tick=1,
                product_level=1,
            )
            for actor_id in actor_ids
        ]
        return struct.pack(">HH", len(views), 0) + b"".join(
            encode_actor_state(view) for view in views
        )

    @staticmethod
    def _drain(client: AgenticHeadlessClient, buf: bytearray) -> None:
        client._running = True
        client._on_socket_data(bytes(buf))
        del client._inbuf[:]

    def _window_client(
        self,
        name: str,
    ) -> tuple[AgenticHeadlessClient, Orchestrator]:
        """Build an extractor-wired summary client ready to ingest frames."""
        batch_type = spatial_messages.ACTOR_STATE_BATCH_TYPE
        client = AgenticHeadlessClient(
            name,
            direct_type_ids=frozenset({batch_type}),
            http_enabled=False,
            ipc_enabled=False,
            auto_connect=False,
            reconnect_enabled=False,
            summary_mode=True,
            evidence_budget_bytes=_BREADTH_EVIDENCE_BUDGET_BYTES,
            window_id_extractor=iter_actor_state_ids,
        )
        orch = Orchestrator(ahc_factory=lambda instance_id, host, port: client)
        asyncio.run(orch.register_client(name, host="127.0.0.1", port=1))
        client._direct_ready = True
        self._window_orchestrators.append(orch)
        return client, orch

    def _drain_batch(self, client: AgenticHeadlessClient, payload: bytes) -> None:
        batch_type = spatial_messages.ACTOR_STATE_BATCH_TYPE
        buf = bytearray(_HEADER.pack(0x4B, 0x54, 1, 0, batch_type, len(payload)) + payload)
        self._drain(client, buf)

    def test_health_check_passes_from_one_in_window_batch(self) -> None:
        """≥128 distinct non-self actors received in-window satisfy the probe."""
        client, orch = self._window_client("load-5")
        rec = _ClientRecord(name="load-5", principal_id=105, instrumented=False)
        rec.own_actor_id = 5
        client.begin_view_window()
        self._drain_batch(client, self._batch_payload(range(200, 400)))
        client.seal_view_window(time.monotonic_ns() + int(5.5 * 1e9))
        assert asyncio.run(_client_view_set_healthy(orch, rec, _BREADTH_BAR)) is True
        assert not client._event_history

    def test_health_check_unions_across_window_frames(self) -> None:
        """The bar resolves from the union of passes, not one batch.

        Neither delivered batch reaches the bar alone (120 and 88 distinct
        non-self actors), so only the accumulator's cross-frame union
        satisfies the probe — the change-suppression regime the channel
        must resolve.
        """
        client, orch = self._window_client("load-9")
        rec = _ClientRecord(name="load-9", principal_id=109, instrumented=False)
        rec.own_actor_id = 9
        client.begin_view_window()
        self._drain_batch(client, self._batch_payload(range(200, 320)))
        self._drain_batch(client, self._batch_payload(range(320, 408)))
        client.seal_view_window(time.monotonic_ns() + int(5.5 * 1e9))
        assert asyncio.run(_client_view_set_healthy(orch, rec, _BREADTH_BAR)) is True

    def test_health_check_fails_without_an_open_window(self) -> None:
        """A client whose window never opened holds no breadth evidence."""
        client, orch = self._window_client("load-6")
        rec = _ClientRecord(name="load-6", principal_id=106, instrumented=False)
        rec.own_actor_id = 6
        self._drain_batch(client, self._batch_payload(range(200, 400)))
        assert asyncio.run(_client_view_set_healthy(orch, rec, _BREADTH_BAR)) is False

    def test_in_window_evidence_survives_a_late_probe(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The reading is window-scoped, not probe-moment scoped.

        Every window frame ages past any recency bound long before a
        2000-client crawl reaches its owner; the accumulator freezes at
        seal time, so the probe still reads the bar.
        """
        client, orch = self._window_client("load-10")
        rec = _ClientRecord(name="load-10", principal_id=110, instrumented=False)
        rec.own_actor_id = 10
        client.begin_view_window()
        self._drain_batch(client, self._batch_payload(range(200, 400)))
        client.seal_view_window(time.monotonic_ns() + int(5.5 * 1e9))
        real_monotonic = time.monotonic_ns
        monkeypatch.setattr(
            "kith._agent.ahc.time.monotonic_ns",
            lambda: real_monotonic() + int(60 * 1e9),
        )
        assert asyncio.run(_client_view_set_healthy(orch, rec, _BREADTH_BAR)) is True

    def test_frames_after_the_seal_deadline_are_not_view_evidence(self) -> None:
        """Post-window deliveries stay out of the sealed accumulator.

        The max-gap backstop re-delivers a full retained view ~1/s after
        the window ends; sealing at the drain keeps that wave from
        reading as in-window view evidence.
        """
        client, orch = self._window_client("load-11")
        rec = _ClientRecord(name="load-11", principal_id=111, instrumented=False)
        rec.own_actor_id = 11
        client.begin_view_window()
        deadline = time.monotonic_ns()
        client.seal_view_window(deadline)
        self._drain_batch(client, self._batch_payload(range(200, 400)))
        assert asyncio.run(_client_view_set_healthy(orch, rec, _BREADTH_BAR)) is False
        # The forensic deque still holds what the stream delivered.
        assert client.evidence_frames()

    def test_the_seal_boundary_is_inclusive(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A frame arriving exactly at the deadline counts; one ns past it does not."""
        client, _orch = self._window_client("load-12")
        rec = _ClientRecord(name="load-12", principal_id=112, instrumented=False)
        rec.own_actor_id = 12
        clock = [1_000_000_000]
        monkeypatch.setattr("kith._agent.ahc.time.monotonic_ns", lambda: clock[0])
        client.begin_view_window()
        self._drain_batch(client, self._batch_payload(range(200, 328)))
        client.seal_view_window(clock[0])
        clock[0] += 1
        self._drain_batch(client, self._batch_payload(range(400, 410)))
        window = client.view_window()
        assert window is not None
        assert 200 in window.distinct_ids
        assert 400 not in window.distinct_ids

    def test_a_mid_window_stall_fails_the_health_check(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A stream that gapped past the stall bound is not a delivered view.

        The early frames carried the bar, then the stream went quiet for
        3 s mid-window: the distinct count alone would read healthy, so
        the gap guard fails the client and names the stall.
        """
        client, orch = self._window_client("load-13")
        rec = _ClientRecord(name="load-13", principal_id=113, instrumented=False)
        rec.own_actor_id = 13
        clock = [1_000_000_000]
        monkeypatch.setattr("kith._agent.ahc.time.monotonic_ns", lambda: clock[0])
        client.begin_view_window()
        self._drain_batch(client, self._batch_payload(range(200, 400)))
        clock[0] += int(3.0 * 1e9)
        self._drain_batch(client, self._batch_payload([999]))
        client.seal_view_window(clock[0])
        observed, stalled, incomplete = asyncio.run(_client_view_facts(orch, rec, _BREADTH_BAR))
        assert observed is False
        assert stalled is True
        assert incomplete is False
        assert asyncio.run(_client_view_set_healthy(orch, rec, _BREADTH_BAR)) is False

    def test_the_window_set_saturates_at_the_cap(self) -> None:
        """The id set saturates; the frame-timestamp list does not.

        Saturation keeps the ingest path off the extraction hot path once
        the bar is answered many times over, without hiding a stall from
        the gap guard.
        """
        client, _orch = self._window_client("load-14")
        client.begin_view_window()
        self._drain_batch(client, self._batch_payload(range(200, 500)))
        self._drain_batch(client, self._batch_payload(range(500, 510)))
        window = client.view_window()
        assert window is not None
        assert len(window.distinct_ids) == _VIEW_WINDOW_ID_CAP
        assert len(window.frame_ts_ns) == 2

    def test_the_self_id_filters_at_read_time(self) -> None:
        """A late-bound own actor id subtracts from the read, not the ingest.

        The accumulator admits every state id because a late-bootstrapped
        client's own actor is unknown until the collection-time binding
        re-stamp; the read-time filter keeps the bar honest for it.
        """
        client, orch = self._window_client("load-15")
        rec = _ClientRecord(name="load-15", principal_id=115, instrumented=False)
        rec.own_actor_id = 7
        client.begin_view_window()
        self._drain_batch(client, self._batch_payload([7, *range(200, 200 + _BREADTH_BAR - 1)]))
        window = client.view_window()
        assert window is not None
        assert len(window.distinct_ids) == _BREADTH_BAR
        # The self id rides the set but does not count toward the bar.
        observed, stalled, incomplete = asyncio.run(_client_view_facts(orch, rec, _BREADTH_BAR))
        assert observed is False
        assert stalled is False
        rec.own_actor_id = 0
        observed, stalled, incomplete = asyncio.run(_client_view_facts(orch, rec, _BREADTH_BAR))
        assert observed is True
        assert stalled is False
        assert incomplete is False


class TestPublishRateCountsFromDispatchCounters:
    """``_publish_rate_counts`` reads per-client dispatch counters.

    The counter covers every dispatched replication frame, so per-instance
    sums stay exact where a history scan saturates at the retained-history
    bound (bulk clients hold 64 events).
    """

    @staticmethod
    def _counter_orch(counts: Mapping[str, int]) -> Orchestrator:
        """An orchestrator whose clients expose fixed dispatch counters."""

        class _CountingClient:
            """A _ManagedClient carrying only a replication counter."""

            def __init__(self, count: int) -> None:
                self._count = count
                self.principal_id = 0

            async def start(self) -> None: ...

            async def stop(self) -> None: ...

            def submit(
                self,
                type_id: int,
                payload: bytes = b"",
                *,
                flags: int = 0,
                correlation_id: int = 0,
            ) -> int:
                del type_id, payload, flags, correlation_id
                return 0

            def register_type(self, name: str, type_id: int) -> None:
                del name, type_id

            def query_status(self) -> ClientStatus:
                return ClientStatus(
                    connected=False,
                    bootstrap_state=0,
                    bootstrap_step=0,
                    bootstrap_step_count=0,
                    rtt_last_ms=0,
                    reconnect_attempts=0,
                )

            def recent_events(self, count: int = 64) -> list[ClientEvent]:
                del count
                return []

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
                return self._count

        def factory(instance_id: str, host: str, port: int) -> _CountingClient:
            del host, port
            return _CountingClient(counts[instance_id])

        return Orchestrator(ahc_factory=factory)

    def test_counts_sum_per_instance(self) -> None:
        orch = self._counter_orch({"a": 10, "b": 20, "c": 30})

        async def drive() -> dict[int, int]:
            for name in ("a", "b", "c"):
                await orch.register_client(name, host="127.0.0.1", port=1)
            records = [
                _ClientRecord(
                    "a", 100, True, own_actor_id=1, started_ns=1, ready_ns=2, instance_index=0
                ),
                _ClientRecord(
                    "b", 101, False, own_actor_id=2, started_ns=1, ready_ns=2, instance_index=0
                ),
                _ClientRecord(
                    "c", 102, False, own_actor_id=3, started_ns=1, ready_ns=2, instance_index=1
                ),
            ]
            return await _publish_rate_counts(orch, records)

        assert asyncio.run(drive()) == {0: 30, 1: 30}

    def test_unbooted_clients_are_excluded(self) -> None:
        orch = self._counter_orch({"a": 10})

        async def drive() -> dict[int, int]:
            await orch.register_client("a", host="127.0.0.1", port=1)
            records = [
                _ClientRecord("a", 100, True, own_actor_id=0),
                _ClientRecord("a", 101, False, own_actor_id=2, started_ns=5, ready_ns=0),
            ]
            return await _publish_rate_counts(orch, records)

        assert asyncio.run(drive()) == {}


class TestMovementPhaseInlineDrive:
    """The movement round submits synchronously through client handles.

    Each movable client submits exactly one input per tick round via the
    handle ``managed_client`` returned; unmovable records are never
    driven, and the per-round walk yields so inbound processing breathes.
    """

    def test_each_movable_client_submits_every_round(self) -> None:
        harness = LoadHarness(
            replace(DISTRIBUTED_2000, duration_s=0.25), cast(LoadHostFactory, lambda: None)
        )

        class _CountingSubmitClient:
            """A managed-client double counting synchronous submits."""

            def __init__(self) -> None:
                self.count = 0
                self.principal_id = 0

            async def start(self) -> None: ...

            async def stop(self) -> None: ...

            def submit(
                self,
                type_id: int,
                payload: bytes = b"",
                *,
                flags: int = 0,
                correlation_id: int = 0,
            ) -> int:
                del type_id, payload, flags, correlation_id
                self.count += 1
                return 1

            def register_type(self, name: str, type_id: int) -> None:
                del name, type_id

            def query_status(self) -> ClientStatus:
                return ClientStatus(
                    connected=False,
                    bootstrap_state=0,
                    bootstrap_step=0,
                    bootstrap_step_count=0,
                    rtt_last_ms=0,
                    reconnect_attempts=0,
                )

            def recent_events(self, count: int = 64) -> list[ClientEvent]:
                del count
                return []

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
                return 0

        clients = {name: _CountingSubmitClient() for name in ("a", "b", "c")}

        def factory(instance_id: str, host: str, port: int) -> _CountingSubmitClient:
            del host, port
            return clients[instance_id]

        orch = Orchestrator(ahc_factory=factory)

        async def drive() -> tuple[int, int, int]:
            for name in ("a", "b", "c"):
                await orch.register_client(name, host="127.0.0.1", port=1)
            records = [
                _ClientRecord(
                    "a", 100, True, own_actor_id=1, started_ns=1, ready_ns=2, inputs_submitted=0
                ),
                _ClientRecord(
                    "b", 101, True, own_actor_id=2, started_ns=1, ready_ns=2, inputs_submitted=0
                ),
                _ClientRecord("c", 102, False, own_actor_id=3),
            ]
            await harness._movement_phase(orch, records)
            return (clients["a"].count, clients["b"].count, records[2].inputs_submitted)

        count_a, count_b, unbooted = asyncio.run(drive())
        assert count_a >= 3
        assert count_a == count_b
        assert unbooted == 0


class TestDeliveryOverride:
    """The --delivery-strategy flag overrides the profile strategy only.

    Running the other preset re-baselines the same drive configuration
    (actor count, pool size, cadence) so a fidelity delta is attributable
    to the delivery strategy alone. This is a pure-logic test (no C build,
    no server) so it runs in every environment.
    """

    def test_none_leaves_profile_unchanged(self) -> None:
        """The CLI default (None) returns the profile untouched."""
        assert _apply_delivery_override(DISTRIBUTED_2000, None) is DISTRIBUTED_2000

    def test_override_replaces_strategy_only(self) -> None:
        """A non-empty override touches delivery_strategy and nothing else."""
        rebaselined = _apply_delivery_override(DISTRIBUTED_2000, "full")
        assert rebaselined.delivery_strategy == "full"
        assert rebaselined.actor_count == DISTRIBUTED_2000.actor_count
        assert rebaselined.python_workers == DISTRIBUTED_2000.python_workers
        assert rebaselined.duration_s == DISTRIBUTED_2000.duration_s
        assert rebaselined.input_hz == DISTRIBUTED_2000.input_hz
        assert rebaselined.settle_s == DISTRIBUTED_2000.settle_s
        assert rebaselined.name == DISTRIBUTED_2000.name
        assert rebaselined.topology == DISTRIBUTED_2000.topology

    def test_distributed_profile_opts_into_tiered(self) -> None:
        """The distributed fidelity gate selects tiered by construction.

        The move-missing bar exists to measure the suppression lever's
        effect, so the profile carries the built-in preset rather than the
        factory default; an explicit override is what produces the
        default-preset baseline.
        """
        assert SMOKE_DENSE.delivery_strategy is None
        assert DISTRIBUTED_2000.delivery_strategy == "tiered"

    def test_empty_string_raises(self) -> None:
        """An empty name is a usage error, not a silent default selection."""
        with pytest.raises(ValueError):
            _apply_delivery_override(DISTRIBUTED_2000, "")


class TestTieredMaxGapOverride:
    """The --tiered-max-gap-ms flag overrides the tiered backstop only.

    Raising the maximum-silence backstop attributes periodic gap behavior
    to the paranoia refresh rather than to any other cadence. This is a
    pure-logic test (no C build, no server) so it runs in every
    environment.
    """

    def test_none_leaves_profile_unchanged(self) -> None:
        """The CLI default (None) returns the profile untouched."""
        assert _apply_tiered_max_gap_override(DISTRIBUTED_2000, None) is DISTRIBUTED_2000

    def test_override_replaces_backstop_only(self) -> None:
        """A non-negative override touches tiered_max_gap_ms and nothing else."""
        raised = _apply_tiered_max_gap_override(DISTRIBUTED_2000, 5000)
        assert raised.tiered_max_gap_ms == 5000
        assert raised.actor_count == DISTRIBUTED_2000.actor_count
        assert raised.python_workers == DISTRIBUTED_2000.python_workers
        assert raised.delivery_strategy == DISTRIBUTED_2000.delivery_strategy
        assert raised.duration_s == DISTRIBUTED_2000.duration_s
        assert raised.input_hz == DISTRIBUTED_2000.input_hz
        assert raised.settle_s == DISTRIBUTED_2000.settle_s
        assert raised.name == DISTRIBUTED_2000.name
        assert raised.topology == DISTRIBUTED_2000.topology

    def test_zero_pins_the_documented_default(self) -> None:
        """An explicit 0 is valid: the gateway config treats 0 as default."""
        assert _apply_tiered_max_gap_override(DISTRIBUTED_2000, 0).tiered_max_gap_ms == 0

    def test_negative_raises(self) -> None:
        """Negative backstop periods are rejected."""
        with pytest.raises(ValueError):
            _apply_tiered_max_gap_override(DISTRIBUTED_2000, -1)


class TestClientOverride:
    """The --clients flag overrides the profile actor count only.

    A lighter drive re-baselines the same server configuration (worker pool,
    instance count, cadence) against fewer clients instead of shrinking the
    per-tick work the gate measures. This is a pure-logic test (no C build,
    no server) so it runs in every environment.
    """

    def test_none_leaves_profile_unchanged(self) -> None:
        """The CLI default (None) returns the profile untouched."""
        assert _apply_client_override(DISTRIBUTED_2000, None) is DISTRIBUTED_2000

    def test_positive_replaces_actor_count_only(self) -> None:
        """A positive override touches actor_count and nothing else."""
        scaled = _apply_client_override(DISTRIBUTED_2000, 1000)
        assert scaled.actor_count == 1000
        assert scaled.python_workers == DISTRIBUTED_2000.python_workers
        assert scaled.duration_s == DISTRIBUTED_2000.duration_s
        assert scaled.input_hz == DISTRIBUTED_2000.input_hz
        assert scaled.settle_s == DISTRIBUTED_2000.settle_s
        assert scaled.name == DISTRIBUTED_2000.name
        assert scaled.topology == DISTRIBUTED_2000.topology
        assert scaled.description == DISTRIBUTED_2000.description

    def test_override_decouples_from_profile_default(self) -> None:
        """The override is independent of the profile's own actor count."""
        assert _apply_client_override(DENSE_1000, 500).actor_count == 500
        assert _apply_client_override(SMOKE_DENSE, 8).actor_count == 8

    def test_below_one_raises(self) -> None:
        """Zero and negative counts are rejected."""
        with pytest.raises(ValueError):
            _apply_client_override(DISTRIBUTED_2000, 0)
        with pytest.raises(ValueError):
            _apply_client_override(DENSE_1000, -5)


class TestWorkerOverride:
    """The --python-workers flag overrides the profile pool size only.

    Measuring worker-pool sensitivity re-baselines the same drive
    configuration (actor count, instance count, cadence) against a
    different pool instead of editing profile constants. The override
    resolves as: the CLI flag, then KITH_PYTHON_WORKERS, then the
    profile's value, then the server default. This is a pure-logic test
    (no C build, no server) so it runs in every environment.
    """

    def test_none_leaves_profile_unchanged(self) -> None:
        """The CLI and environment defaults (None) return the profile."""
        assert _apply_worker_override(DISTRIBUTED_2000, None) is DISTRIBUTED_2000

    def test_override_replaces_pool_size_only(self) -> None:
        """A non-negative override touches python_workers and nothing else."""
        scaled = _apply_worker_override(DISTRIBUTED_2000, 8)
        assert scaled.python_workers == 8
        assert scaled.actor_count == DISTRIBUTED_2000.actor_count
        assert scaled.duration_s == DISTRIBUTED_2000.duration_s
        assert scaled.input_hz == DISTRIBUTED_2000.input_hz
        assert scaled.settle_s == DISTRIBUTED_2000.settle_s
        assert scaled.name == DISTRIBUTED_2000.name
        assert scaled.topology == DISTRIBUTED_2000.topology
        assert scaled.description == DISTRIBUTED_2000.description

    def test_zero_requests_the_server_default(self) -> None:
        """An explicit 0 is valid: it requests the server default pool."""
        assert _apply_worker_override(SMOKE_DENSE, 0).python_workers == 0
        assert _apply_worker_override(DISTRIBUTED_2000, 0).python_workers == 0

    def test_negative_raises(self) -> None:
        """Negative pool sizes are rejected."""
        with pytest.raises(ValueError):
            _apply_worker_override(DISTRIBUTED_2000, -1)

    def test_env_tier_between_flag_and_profile(self) -> None:
        """The environment variable supplies the value absent a flag.

        The harness reads KITH_PYTHON_WORKERS as its second precedence
        tier; an unset variable defers to the profile.
        """
        assert _env_python_workers({}) is None
        assert _env_python_workers({"KITH_PYTHON_WORKERS": "12"}) == 12

    def test_env_rejects_malformed_values(self) -> None:
        """Non-integer and negative values are rejected, not ignored."""
        with pytest.raises(ValueError):
            _env_python_workers({"KITH_PYTHON_WORKERS": "many"})
        with pytest.raises(ValueError):
            _env_python_workers({"KITH_PYTHON_WORKERS": "-4"})


class _CountingOrch:
    """Minimal orchestrator stand-in returning canned recent-event lists.

    The count collectors touch ``recent_events`` and
    ``replication_frame_count`` alone, so the stub casts to
    :class:`Orchestrator` at the call sites and nothing else of the real
    class is exercised.
    """

    def __init__(self, events_by_client: dict[str, list[ClientEvent]]) -> None:
        self._events_by_client = events_by_client

    async def recent_events(self, name: str, count: int) -> list[ClientEvent]:
        del count
        return list(self._events_by_client.get(name, []))

    async def replication_frame_count(self, name: str) -> int:
        replication = {
            spatial_messages.ACTOR_STATE_TYPE,
            spatial_messages.ACTOR_STATE_BATCH_TYPE,
        }
        events = self._events_by_client.get(name, [])
        return sum(1 for event in events if event.type_id in replication)


def _counting_orch(events_by_client: dict[str, list[ClientEvent]]) -> Orchestrator:
    return cast(Orchestrator, _CountingOrch(events_by_client))


def _self_state_event(actor_id: int, input_tick: int) -> ClientEvent:
    """One single-subject ``actor_state`` event carrying the given tick."""
    view = ActorStateView(
        actor_id=actor_id,
        pos_x=0,
        pos_y=0,
        pos_z=0,
        vel_x=0,
        vel_y=0,
        vel_z=0,
        input_tick=input_tick,
        product_level=0,
    )
    return ClientEvent(
        ts_mono_ns=0,
        type_id=spatial_messages.ACTOR_STATE_TYPE,
        correlation_id=0,
        payload=encode_actor_state(view),
    )


def _batch_event(actor_ids: Sequence[int], *, ts_mono_ns: int = 0) -> ClientEvent:
    """One multi-subject ``actor_state_batch`` event carrying the ids."""
    views = [
        ActorStateView(
            actor_id=actor_id,
            pos_x=0,
            pos_y=0,
            pos_z=0,
            vel_x=0,
            vel_y=0,
            vel_z=0,
            input_tick=1,
            product_level=1,
        )
        for actor_id in actor_ids
    ]
    payload = struct.pack(">HH", len(views), 0) + b"".join(
        encode_actor_state(view) for view in views
    )
    return ClientEvent(
        ts_mono_ns=ts_mono_ns,
        type_id=spatial_messages.ACTOR_STATE_BATCH_TYPE,
        correlation_id=0,
        payload=payload,
    )


def _booted_record(name: str, instance_index: int) -> _ClientRecord:
    """A record that passes the booted filter (ready, started, actor set)."""
    return _ClientRecord(
        name=name,
        principal_id=100,
        instrumented=True,
        own_actor_id=7,
        started_ns=1,
        ready_ns=2,
        instance_index=instance_index,
    )


class _BindingsControl:
    """A control-plane double serving fixed ``/bindings`` pages.

    Honors each request's ``offset``/``limit`` window like the real route
    (a non-final page carries exactly ``limit`` entries); ``total`` rides
    every envelope unless ``omit_total`` is set. Every request is recorded
    so tests can pin the paging sequence.
    """

    def __init__(
        self,
        bindings: Sequence[Mapping[str, int]],
        *,
        omit_total: bool = False,
        malformed_body: Mapping[str, object] | None = None,
        fail_with: Exception | None = None,
    ) -> None:
        self.requests: list[str] = []
        self._bindings = list(bindings)
        self._omit_total = omit_total
        self._malformed_body = malformed_body
        self._fail_with = fail_with

    async def metrics(self) -> str:
        return ""

    async def get(self, path: str) -> Mapping[str, object]:
        if self._fail_with is not None:
            raise self._fail_with
        self.requests.append(path)
        if self._malformed_body is not None:
            return self._malformed_body
        parts = dict(part.split("=", 1) for part in path.split("?", 1)[-1].split("&"))
        offset = int(parts["offset"])
        limit = int(parts["limit"])
        body: dict[str, object] = {"offset": offset, "limit": limit}
        if not self._omit_total:
            body["total"] = len(self._bindings)
        body["bindings"] = [dict(item) for item in self._bindings[offset : offset + limit]]
        return body

    async def post(self, path: str, body: Mapping[str, object]) -> Mapping[str, object]:
        del path, body
        return {}


class _CensusControl:
    """A control-plane double serving both census routes.

    The breadth census reads ``/bindings`` (the collection-time cohort)
    and ``/query_state`` (the window-end roster) from the same control,
    so the double serves both with the fixed paging envelope.
    """

    def __init__(
        self,
        actors: Sequence[tuple[int, int, int, int]],
        bindings: Sequence[Mapping[str, int]],
    ) -> None:
        self._actors = [
            dict(zip(("actor_id", "pos_x", "pos_y", "pos_z"), row, strict=True)) for row in actors
        ]
        self._bindings = [dict(item) for item in bindings]

    async def metrics(self) -> str:
        return ""

    async def get(self, path: str) -> Mapping[str, object]:
        parts = dict(part.split("=", 1) for part in path.split("?", 1)[-1].split("&"))
        offset = int(parts["offset"])
        limit = int(parts["limit"])
        if path.startswith("/bindings"):
            page = [dict(item) for item in self._bindings[offset : offset + limit]]
            return {
                "offset": offset,
                "limit": limit,
                "total": len(self._bindings),
                "bindings": page,
            }
        page = [dict(row) for row in self._actors[offset : offset + limit]]
        return {"offset": offset, "limit": limit, "total": len(self._actors), "actors": page}

    async def post(self, path: str, body: Mapping[str, object]) -> Mapping[str, object]:
        del path, body
        return {}


class _CensusOrch:
    """Minimal orchestrator stand-in for the breadth census.

    Serves canned client statuses and movement-window evidence; the census
    touches ``client_state``, ``view_window``, and ``evidence_span_ns``
    alone, so the stub casts to :class:`Orchestrator` at the call sites.
    Unlisted clients read as connected-and-ready with no window evidence.
    """

    def __init__(
        self,
        control: object,
        statuses: Mapping[str, ClientStatus] | None = None,
        windows: Mapping[str, WindowEvidence] | None = None,
    ) -> None:
        self.server_control = control
        self._statuses = dict(statuses or {})
        self._windows = dict(windows or {})

    async def client_state(self, name: str) -> ClientStatus:
        return self._statuses.get(
            name,
            ClientStatus(
                connected=True,
                bootstrap_state=_BOOTSTRAP_READY,
                bootstrap_step=0,
                bootstrap_step_count=0,
                rtt_last_ms=0,
                reconnect_attempts=0,
            ),
        )

    async def view_window(self, name: str) -> WindowEvidence | None:
        return self._windows.get(name)

    async def evidence_span_ns(self, name: str) -> int | None:
        del name
        return None


def _census_orch(
    control: object,
    statuses: Mapping[str, ClientStatus] | None = None,
    windows: Mapping[str, WindowEvidence] | None = None,
) -> Orchestrator:
    return cast(
        Orchestrator,
        _CensusOrch(control, statuses=statuses, windows=windows),
    )


def _dead_host_factory() -> Orchestrator:
    """Prove resolution never boots a host by raising if invoked."""
    raise AssertionError("host factory must not run during actor resolution")


class _RosterControl:
    """A control-plane double serving fixed ``/query_state`` pages.

    Rows are ``(actor_id, pos_x, pos_y, pos_z)`` tuples honoring each
    request's ``offset``/``limit`` window like the real route. The
    optional ``transform`` rewrites the final body so malformed-response
    cases stay expressible without dedicated flags.
    """

    def __init__(
        self,
        actors: Sequence[tuple[int, int, int, int]],
        *,
        transform: Callable[[dict[str, object]], Mapping[str, object]] | None = None,
        fail_with: Exception | None = None,
    ) -> None:
        self.requests: list[str] = []
        self._actors = [
            dict(zip(("actor_id", "pos_x", "pos_y", "pos_z"), row, strict=True)) for row in actors
        ]
        self._transform = transform
        self._fail_with = fail_with

    async def metrics(self) -> str:
        return ""

    async def get(self, path: str) -> Mapping[str, object]:
        if self._fail_with is not None:
            raise self._fail_with
        self.requests.append(path)
        parts = dict(part.split("=", 1) for part in path.split("?", 1)[-1].split("&"))
        offset = int(parts["offset"])
        limit = int(parts["limit"])
        body: dict[str, object] = {
            "offset": offset,
            "limit": limit,
            "total": len(self._actors),
            "actors": [dict(row) for row in self._actors[offset : offset + limit]],
        }
        return body if self._transform is None else self._transform(body)

    async def post(self, path: str, body: Mapping[str, object]) -> Mapping[str, object]:
        del path, body
        return {}


class TestMetricCounts:
    """The integer count collectors behind move-missing and publish skew.

    Ratios aggregate across drives only by summing counts over the union
    population; averaging ratios is wrong whenever denominators differ.
    This is a pure-logic test (no C build, no server) so it runs in every
    environment.
    """

    def test_depth_metrics_sum_inputs_and_distinct_ticks(self) -> None:
        """Submitted inputs sum over records; observed ticks dedupe per client."""
        rec_a = _ClientRecord(
            name="a", principal_id=100, instrumented=True, own_actor_id=1, inputs_submitted=4
        )
        rec_b = _ClientRecord(
            name="b", principal_id=101, instrumented=True, own_actor_id=2, inputs_submitted=6
        )
        orch = _counting_orch(
            {
                "a": [_self_state_event(1, 0), _self_state_event(1, 1), _self_state_event(9, 5)],
                "b": [_self_state_event(2, 2), _self_state_event(2, 2), _self_state_event(2, 3)],
            }
        )
        submitted, observed, series, _rows = asyncio.run(
            _collect_depth_metrics(orch, [rec_a, rec_b], 0.0)
        )
        assert submitted == 10
        assert observed == 4
        # No movement window stamped: the series carry empty frame lists.
        assert all(entry.frames == () for entry in series)

    def test_depth_metrics_window_scopes_series_not_tick_counts(self) -> None:
        """Tick distinctness spans full history; the series stays windowed."""
        start, end = 100, 200
        rec = _ClientRecord(
            name="a", principal_id=100, instrumented=True, own_actor_id=1, inputs_submitted=3
        )
        rec.movement_start_ns = start
        rec.movement_end_ns = end

        def state_event(ts_ns: int, tick: int) -> ClientEvent:
            event = _self_state_event(1, tick)
            return ClientEvent(
                ts_mono_ns=ts_ns, type_id=event.type_id, correlation_id=0, payload=event.payload
            )

        orch = _counting_orch({"a": [state_event(50, 1), state_event(150, 2), state_event(300, 3)]})
        submitted, observed, series, _rows = asyncio.run(_collect_depth_metrics(orch, [rec], 0.0))
        assert submitted == 3
        assert observed == 3
        (entry,) = series
        assert [frame.ts_ns for frame in entry.frames] == [150]
        assert [frame.input_tick for frame in entry.frames] == [2]

    def test_move_missing_ratio_matches_counts(self) -> None:
        """The ratio is 1 - observed/submitted over the depth counts."""
        assert _move_missing_ratio(4, 2) == pytest.approx(0.5)

    def test_move_missing_empty_sample_is_zero(self) -> None:
        """No submitted inputs yield a 0.0 ratio, not a division error."""
        assert _move_missing_ratio(0, 0) == 0.0

    def test_publish_counts_group_by_instance(self) -> None:
        """Delivered replication frames sum per instance over booted clients."""
        replication = (spatial_messages.ACTOR_STATE_TYPE, spatial_messages.ACTOR_STATE_BATCH_TYPE)
        recs = [_booted_record("a", 0), _booted_record("b", 1)]
        orch = _counting_orch(
            {
                "a": [
                    ClientEvent(ts_mono_ns=0, type_id=t, correlation_id=0, payload=b"")
                    for t in (*replication, *replication)
                ],
                "b": [
                    ClientEvent(ts_mono_ns=0, type_id=t, correlation_id=0, payload=b"")
                    for t in replication
                ],
                # Not booted: excluded from the sums entirely.
                "c": [
                    ClientEvent(ts_mono_ns=0, type_id=replication[0], correlation_id=0, payload=b"")
                ],
            }
        )
        counts = asyncio.run(
            _publish_rate_counts(
                orch, [*recs, _ClientRecord(name="c", principal_id=102, instrumented=True)]
            )
        )
        assert counts == {0: 4, 1: 2}

    def test_publish_skew_is_max_over_min_of_counts(self) -> None:
        """The skew wrapper reduces pooled per-instance counts to max/min."""
        recs = [_booted_record("a", 0), _booted_record("b", 1)]
        orch = _counting_orch(
            {
                "a": [_self_state_event(7, t) for t in range(6)],
                "b": [_self_state_event(7, t) for t in range(3)],
            }
        )
        skew = asyncio.run(_publish_rate_skew(orch, recs))
        counts = asyncio.run(_publish_rate_counts(orch, recs))
        assert counts == {0: 6, 1: 3}
        assert skew == pytest.approx(2.0)

    def test_publish_skew_degenerate_forms(self) -> None:
        """No booted clients yield 0.0; a zero-delivery instance yields inf."""
        unbooted = _ClientRecord(name="a", principal_id=100, instrumented=True)
        assert asyncio.run(_publish_rate_skew(_counting_orch({}), [unbooted])) == 0.0
        silent_b = _ClientRecord(
            name="b",
            principal_id=101,
            instrumented=True,
            own_actor_id=8,
            started_ns=1,
            ready_ns=2,
            instance_index=1,
        )
        orch = _counting_orch({"a": [_self_state_event(7, 0)]})
        assert asyncio.run(_publish_rate_skew(orch, [_booted_record("a", 0), silent_b])) == float(
            "inf"
        )


class TestPrincipalBindingResolution:
    """Own-actor attribution reads the server's binding map.

    ``_principal_bindings`` pages ``/bindings`` into one principal→actor
    map; every failure mode raises instead of degrading, because a
    silently partial or mis-ordered map mis-attributes every depth
    metric downstream. The resolve steps additionally enforce map
    completeness against the booted cohort per instance.
    """

    @staticmethod
    def _pair(principal_id: int, actor_id: int) -> dict[str, int]:
        return {"principal_id": principal_id, "actor_id": actor_id}

    def test_bindings_page_into_one_map(self) -> None:
        """Pages fold across successive requests until a short final page."""
        flat = [self._pair(100 + i, i + 5) for i in range(40)]
        control = _BindingsControl(flat)
        assert asyncio.run(_principal_bindings(control)) == {100 + i: i + 5 for i in range(40)}
        page_size = QUERY_STATE_DEFAULT_PAGE_SIZE
        assert control.requests == [
            f"/bindings?offset=0&limit={page_size}",
            f"/bindings?offset={page_size}&limit={page_size}",
        ]

    def test_unpaged_roster_stops_after_one_full_page(self) -> None:
        """A response without ``total`` declares itself complete."""
        flat = [self._pair(100 + i, 9 - i) for i in range(QUERY_STATE_DEFAULT_PAGE_SIZE)]
        control = _BindingsControl(flat, omit_total=True)
        result = asyncio.run(_principal_bindings(control))
        assert result == {100 + i: 9 - i for i in range(QUERY_STATE_DEFAULT_PAGE_SIZE)}
        assert control.requests == [f"/bindings?offset=0&limit={QUERY_STATE_DEFAULT_PAGE_SIZE}"]

    def test_transport_failure_raises(self) -> None:
        """A control-plane failure fails loudly; absence is never a fallback."""
        failure = ServerControlError("GET /bindings returned HTTP 500")
        control = _BindingsControl([], fail_with=failure)
        with pytest.raises(RuntimeError, match="binding truth unavailable"):
            asyncio.run(_principal_bindings(control))

    def test_malformed_body_raises(self) -> None:
        """A missing or non-list ``bindings`` field is unusable truth."""
        for body in ({}, {"bindings": "nope"}):
            control = _BindingsControl([], malformed_body=body)
            with pytest.raises(RuntimeError, match="malformed"):
                asyncio.run(_principal_bindings(control))

    def test_duplicate_principal_raises(self) -> None:
        """One principal with two actors is ambiguous attribution."""
        control = _BindingsControl([self._pair(100, 1), self._pair(100, 2)])
        with pytest.raises(RuntimeError, match="duplicate principal id"):
            asyncio.run(_principal_bindings(control))

    def test_dense_resolve_follows_principal_truth_over_start_order(self) -> None:
        """Booted order raced allocation order; the binding map wins.

        The client whose login lands first (highest principal id here)
        must receive its own actor, not the lowest allocated id — the
        start-order zip mis-assigns exactly this shape.
        """
        control = _BindingsControl([self._pair(100, 1), self._pair(101, 2)])
        orch = Orchestrator(server_control=control)
        late = _ClientRecord(
            name="late", principal_id=100, instrumented=True, started_ns=1, ready_ns=2
        )
        early = _ClientRecord(
            name="early", principal_id=101, instrumented=True, started_ns=1, ready_ns=2
        )
        asyncio.run(_resolve_own_actors_dense(orch, [early, late]))
        assert late.own_actor_id == 1
        assert early.own_actor_id == 2

    def test_resolve_shortfall_fails_loudly(self) -> None:
        """A map smaller than the booted cohort cannot attribute."""
        control = _BindingsControl([self._pair(100, 1)])
        orch = Orchestrator(server_control=control)
        boot_a = _ClientRecord(
            name="a", principal_id=100, instrumented=True, started_ns=1, ready_ns=2
        )
        boot_b = _ClientRecord(
            name="b", principal_id=101, instrumented=True, started_ns=1, ready_ns=2
        )
        with pytest.raises(
            RuntimeError,
            match=r"binding map covers 1 principals.*unmatched clients: \['b'\]",
        ):
            asyncio.run(_resolve_own_actors_dense(orch, [boot_a, boot_b]))
        assert boot_a.own_actor_id == 0

    def test_distributed_resolve_splits_by_instance(self) -> None:
        """Each instance's own map attributes only its clients."""
        zero = _BindingsControl([self._pair(100, 3)])
        one = _BindingsControl([self._pair(102, 8)])
        control = DistributedControl(cast(list[ServerControlClient], [zero, one]))
        orch = Orchestrator(server_control=control)
        inst0 = _ClientRecord(
            name="i0",
            principal_id=100,
            instrumented=True,
            started_ns=1,
            ready_ns=2,
            instance_index=0,
        )
        inst1 = _ClientRecord(
            name="i1",
            principal_id=102,
            instrumented=True,
            started_ns=1,
            ready_ns=2,
            instance_index=1,
        )
        harness = LoadHarness(SMOKE_DENSE, _dead_host_factory)
        asyncio.run(harness._resolve_own_actors_distributed(orch, [inst0, inst1]))
        assert inst0.own_actor_id == 3
        assert inst1.own_actor_id == 8
        assert zero.requests and one.requests

    def test_distributed_resolve_names_the_instance_on_shortfall(self) -> None:
        """A shortfall report carries the instance it came from."""
        zero = _BindingsControl([self._pair(100, 3)])
        one = _BindingsControl([])
        control = DistributedControl(cast(list[ServerControlClient], [zero, one]))
        orch = Orchestrator(server_control=control)
        inst0 = _ClientRecord(
            name="i0",
            principal_id=100,
            instrumented=True,
            started_ns=1,
            ready_ns=2,
            instance_index=0,
        )
        inst1 = _ClientRecord(
            name="i1",
            principal_id=102,
            instrumented=True,
            started_ns=1,
            ready_ns=2,
            instance_index=1,
        )
        harness = LoadHarness(SMOKE_DENSE, _dead_host_factory)
        with pytest.raises(
            RuntimeError,
            match=r"instance 1: binding map covers 0 principals.*'i1'",
        ):
            asyncio.run(harness._resolve_own_actors_distributed(orch, [inst0, inst1]))

    class _Stub:
        """A managed-client double carrying one retained principal."""

        def __init__(self, principal_id: int = 0) -> None:
            self.principal_id = principal_id

        async def start(self) -> None: ...

        async def stop(self) -> None: ...

        def submit(
            self,
            type_id: int,
            payload: bytes = b"",
            *,
            flags: int = 0,
            correlation_id: int = 0,
        ) -> int:
            del type_id, payload, flags, correlation_id
            return 1

        def register_type(self, name: str, type_id: int) -> None:
            del name, type_id

        def query_status(self) -> ClientStatus:
            return ClientStatus(
                connected=True,
                bootstrap_state=2,
                bootstrap_step=3,
                bootstrap_step_count=3,
                rtt_last_ms=4,
                reconnect_attempts=0,
            )

        def recent_events(self, count: int = 64) -> list[ClientEvent]:
            del count
            return []

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
            return 0

    def test_records_read_principals_from_registered_clients(self) -> None:
        """Record ids come from client truth, not registration order.

        A factory may number principals with its own stride (the dual
        driver splits the population across instances by parity); a
        synthesized ``100 + i`` guess diverges from that rule and every
        binding-truth lookup misses. Records read each client's retained
        principal back per name instead of guessing.
        """
        base = 100
        n = SMOKE_DENSE.actor_count
        counter = [base]

        def factory(instance_id: str, host: str, port: int) -> TestPrincipalBindingResolution._Stub:
            del instance_id, host, port
            client = self._Stub(counter[0])
            counter[0] += 2  # parity stride mirrors the dual-driver split
            return client

        orch = Orchestrator(ahc_factory=factory)
        harness = LoadHarness(SMOKE_DENSE, _dead_host_factory)
        records = asyncio.run(harness._register_and_start(orch))
        assert [rec.principal_id for rec in records] == [base + 2 * i for i in range(n)]

    def test_records_fail_loudly_without_client_truth(self) -> None:
        """A registry without retained principals cannot attribute."""
        orch = Orchestrator(ahc_factory=lambda *args: self._Stub())
        harness = LoadHarness(SMOKE_DENSE, _dead_host_factory)
        with pytest.raises(RuntimeError, match="exposes no principal id"):
            asyncio.run(harness._register_and_start(orch))

    def test_dual_parity_cohort_resolves_from_binding_truth(self) -> None:
        """End-to-end parity attribution: ids on clients, truth from server.

        Guard for the parity-mirrored unmatched lists: driver k's
        factory assigns principals with a stride-2 walk so its cohort
        lands on instance k; records must carry exactly those ids and the
        resolver must attribute each own actor through them.
        """
        n = SMOKE_DENSE.actor_count
        principals = [100 + 2 * i for i in range(n)]
        zero = _BindingsControl([self._pair(p, i + 7) for i, p in enumerate(principals)])
        control = DistributedControl(cast(list[ServerControlClient], [zero]))
        counter = [100]

        def factory(instance_id: str, host: str, port: int) -> TestPrincipalBindingResolution._Stub:
            del instance_id, host, port
            client = self._Stub(counter[0])
            counter[0] += 2
            return client

        orch = Orchestrator(server_control=control, ahc_factory=factory)
        harness = LoadHarness(SMOKE_DENSE, _dead_host_factory)
        records = asyncio.run(harness._register_and_start(orch))
        for rec in records:
            rec.ready_ns = rec.started_ns + 1
        asyncio.run(harness._resolve_own_actors_distributed(orch, records))
        assert {rec.principal_id: rec.own_actor_id for rec in records} == {
            p: i + 7 for i, p in enumerate(principals)
        }

    def test_single_control_cohort_resolves_from_binding_truth(self) -> None:
        """The child-shaped resolver reads its own instance's bindings.

        A dual-driver worker holds one instance's control client directly
        (no aggregating list), so the distributed resolver routes to the
        dense read against that client. The parity attribution and the
        completeness check must behave identically to the per-instance
        path.
        """
        n = SMOKE_DENSE.actor_count
        principals = [100 + 2 * i for i in range(n)]
        control = _BindingsControl([self._pair(p, i + 7) for i, p in enumerate(principals)])
        counter = [100]

        def factory(instance_id: str, host: str, port: int) -> TestPrincipalBindingResolution._Stub:
            del instance_id, host, port
            client = self._Stub(counter[0])
            counter[0] += 2
            return client

        orch = Orchestrator(server_control=control, ahc_factory=factory)
        harness = LoadHarness(SMOKE_DENSE, _dead_host_factory)
        records = asyncio.run(harness._register_and_start(orch))
        for rec in records:
            rec.ready_ns = rec.started_ns + 1
        asyncio.run(harness._resolve_own_actors_distributed(orch, records))
        assert {rec.principal_id: rec.own_actor_id for rec in records} == {
            p: i + 7 for i, p in enumerate(principals)
        }


class TestWindowEligibility:
    """Breadth eligibility reads the window-end roster geometry.

    A client is isolated when its own actor has fewer than
    ``min_distinct_others`` other actors inside its radius-1 cell
    neighborhood at window end; those are reported rather than counted
    against delivery, and every failure of the geometry read is loud.
    """

    def test_roster_positions_page_into_one_map(self) -> None:
        """Positions fold across successive requests until a short page."""
        rows = [(i + 1, 10 * i, 20 * i, 30 * i) for i in range(37)]
        control = _RosterControl(rows)
        positions = asyncio.run(_roster_positions(control))
        assert positions == {row[0]: row[1:] for row in rows}
        page_size = QUERY_STATE_DEFAULT_PAGE_SIZE
        assert control.requests == [
            f"/query_state?offset=0&limit={page_size}",
            f"/query_state?offset={page_size}&limit={page_size}",
        ]

    def test_roster_transport_failure_raises(self) -> None:
        """A failed roster read cannot degrade into a partial truth."""
        failure = ServerControlError("GET /query_state returned HTTP 500")
        control = _RosterControl([], fail_with=failure)
        with pytest.raises(RuntimeError, match="roster positions unavailable"):
            asyncio.run(_roster_positions(control))

    def test_roster_malformed_entry_raises(self) -> None:
        """An entry missing a coordinate under-counts neighbors silently."""
        control = _RosterControl(
            [(1, 0, 0, 0)], transform=lambda body: {"actors": [{"actor_id": 1}]}
        )
        with pytest.raises(RuntimeError, match="roster entry malformed"):
            asyncio.run(_roster_positions(control))

    def test_classify_dense_cluster_leaves_nothing_isolated(self) -> None:
        """A single-cell pileup covers every candidate's neighborhood."""
        cell = CELL_SIZE_Q16
        rows = [(i + 1, i % 7, (i * 3) % cell, 0) for i in range(40)]
        positions = {row[0]: (row[1], row[2], row[3]) for row in rows}
        assert _isolated_actor_ids(positions, [1], 8) == frozenset()

    def test_classify_reports_the_sparse_candidate(self) -> None:
        """A candidate alone in far cells counts as isolated."""
        cell = CELL_SIZE_Q16
        positions = {i + 1: (i, i % 7, 0) for i in range(25)}
        positions[99] = (cell * 100, cell * 100, 0)
        assert _isolated_actor_ids(positions, [1, 2, 99], 24) == frozenset({99})

    def test_classify_window_counts_across_cell_boundaries(self) -> None:
        """Neighbors exactly one cell away count; two cells away do not."""
        cell = CELL_SIZE_Q16
        positions = {
            1: (cell * 2, 0, 0),
            2: (cell * 3 - 1, 0, 0),  # adjacent cell along x
            3: (cell * 4 + 1, 0, 0),  # two cells away
        }
        assert _isolated_actor_ids(positions, [1], 1) == frozenset()
        assert _isolated_actor_ids(positions, [1], 2) == frozenset({1})

    def test_classify_z_axis_joins_and_splits_cells(self) -> None:
        """The neighborhood is 3D per the framework's cell identity."""
        cell = CELL_SIZE_Q16
        base = {1: (0, 0, 0), 2: (0, 0, cell), 3: (0, 0, cell * 2)}
        assert _isolated_actor_ids(base, [1], 1) == frozenset()
        assert _isolated_actor_ids(base, [1], 2) == frozenset({1})

    def test_classify_self_counts_exactly_once(self) -> None:
        """The own actor inflates its bucket once and only once."""
        rows = [(i + 1, i, 0, 0) for i in range(9)]  # all inside one cell
        positions = {row[0]: (row[1], row[2], row[3]) for row in rows}
        candidates = [row[0] for row in rows]
        others = len(candidates) - 1
        assert _isolated_actor_ids(positions, candidates, others) == frozenset()
        assert _isolated_actor_ids(positions, candidates, others + 1) == frozenset(candidates)

    def test_classify_negative_coordinates_bucket_consistently(self) -> None:
        """Floor division keeps negative positions in their true cells."""
        cell = CELL_SIZE_Q16
        positions = {
            1: (-cell // 2, 0, 0),
            2: (-cell - 1, 0, 0),  # floor-divides into cell (-2, 0, 0)
        }
        assert _isolated_actor_ids(positions, [1], 1) == frozenset()

    def test_classify_missing_own_actor_raises(self) -> None:
        """A roster without a promised own actor is unusable truth."""
        with pytest.raises(RuntimeError, match="roster omits own actor"):
            _isolated_actor_ids({2: (0, 0, 0)}, [1], 1)

    def test_census_without_control_plane_is_inconclusive(self) -> None:
        """No roster available keeps the legacy full-population behavior."""
        orch = _census_orch(control=None)
        records = [_booted_record("a", 0)]
        census = asyncio.run(_collect_breadth(orch, records, _BREADTH_BAR))
        assert census.classified_clients is None
        assert census.isolated_clients is None
        assert census.eligible_clients is None
        assert census.violation is None

    def test_census_classifies_the_collection_time_bound_cohort(self) -> None:
        """Cohort membership follows the collection-time binding map.

        A record the pre-movement snapshot left unresolved joins the cohort
        when the collection-time map binds its principal, so the numerator
        and the denominator read the same population. Principals the map
        does not bind ride neither side and render as unbound.
        """
        cell = CELL_SIZE_Q16
        rows = [(i + 1, i, 0, 0) for i in range(30)] + [(99, cell * 500, 0, 0)]
        bindings = [{"principal_id": 100 + i, "actor_id": i + 1} for i in range(30)]
        bindings.append({"principal_id": 130, "actor_id": 99})
        control = _CensusControl(rows, bindings)
        orch = _census_orch(control)
        clustered = [
            _ClientRecord(
                name=f"c{i}",
                principal_id=100 + i,
                instrumented=True,
                own_actor_id=i + 1,
                started_ns=1,
                ready_ns=2,
            )
            for i in range(30)
        ]
        far_bound = _ClientRecord(
            name="far",
            principal_id=130,
            instrumented=True,
            own_actor_id=99,
            started_ns=1,
            ready_ns=2,
        )
        late = _ClientRecord(
            name="late",
            principal_id=131,
            instrumented=True,
            started_ns=1,
        )
        idless = _ClientRecord(
            name="idless",
            principal_id=132,
            instrumented=True,
            started_ns=1,
            ready_ns=2,
        )
        records = [*clustered, far_bound, late, idless]
        census = asyncio.run(_collect_breadth(orch, records, 25))
        assert census.classified_clients == 31
        assert census.isolated_clients == 1
        assert census.eligible_clients == 30
        assert census.violation is None

    def test_census_restamps_and_contains_the_late_ghost(self) -> None:
        """A client bound after the boot snapshot is classified, not lost.

        Collection-time binding classification reads the denominator and
        the numerator from one population, so a client bound after the
        boot snapshot is classified with its current window. The ghost's
        actor never moved, so it is isolated at
        window end and rides neither side of the ratio.
        """
        cell = CELL_SIZE_Q16
        rows = [(i + 1, cell * 2, 0, 0) for i in range(999)]
        rows.append((1000, 0, 0, 0))
        bindings = [{"principal_id": 100 + i, "actor_id": i + 1} for i in range(999)]
        bindings.append({"principal_id": 1099, "actor_id": 1000})
        pile_ids = frozenset(range(2, 131))
        windows = {f"c{i}": WindowEvidence(pile_ids, (1,)) for i in range(999)}
        orch = _census_orch(_CensusControl(rows, bindings), windows=windows)
        records = [
            _ClientRecord(
                name=f"c{i}",
                principal_id=100 + i,
                instrumented=False,
                own_actor_id=i + 1,
                started_ns=1,
                ready_ns=2,
                inputs_submitted=100,
            )
            for i in range(999)
        ]
        records.append(
            _ClientRecord(
                name="ghost",
                principal_id=1099,
                instrumented=False,
                started_ns=1,
                inputs_submitted=0,
            )
        )
        census = asyncio.run(_collect_breadth(orch, records, _BREADTH_BAR))
        assert census.classified_clients == 1000
        assert census.isolated_clients == 1
        assert census.eligible_clients == 999
        assert census.selected_count == 999
        assert census.observed_but_isolated_fresh_clients == 0
        assert census.late_bootstrapped_clients == 1
        assert census.unbound_clients == 0
        assert census.violation is None
        assert census.attribution[0].name == "ghost"
        assert census.attribution[0].klass == "late_bootstrap"
        assert census.attribution[0].fresh is False

    def test_census_contains_the_frozen_actor(self) -> None:
        """A stamped client whose actor froze behind the pile is contained.

        Same containment shape as the late ghost, different cause: the
        record boots and submits, but its actor receives no applies and
        sits two cells behind the migrated pile. It is isolated at window
        end, drops from both sides, and the attribution names the frozen
        signature so a server-side apply loss cannot hide.
        """
        cell = CELL_SIZE_Q16
        rows = [(i + 1, cell * 2, 0, 0) for i in range(999)]
        rows.append((1000, 0, 0, 0))
        bindings = [{"principal_id": 100 + i, "actor_id": i + 1} for i in range(1000)]
        pile_ids = frozenset(range(2, 131))
        windows = {f"c{i}": WindowEvidence(pile_ids, (1,)) for i in range(999)}
        orch = _census_orch(_CensusControl(rows, bindings), windows=windows)
        records = [
            _ClientRecord(
                name=f"c{i}",
                principal_id=100 + i,
                instrumented=False,
                own_actor_id=i + 1,
                started_ns=1,
                ready_ns=2,
                inputs_submitted=100,
            )
            for i in range(999)
        ]
        records.append(
            _ClientRecord(
                name="frozen",
                principal_id=1099,
                instrumented=False,
                own_actor_id=1000,
                started_ns=1,
                ready_ns=2,
                inputs_submitted=100,
            )
        )
        census = asyncio.run(_collect_breadth(orch, records, _BREADTH_BAR))
        assert census.classified_clients == 1000
        assert census.isolated_clients == 1
        assert census.eligible_clients == 999
        assert census.selected_count == 999
        assert census.late_bootstrapped_clients == 0
        assert census.attribution[0].klass == "frozen"

    def test_census_excludes_an_unbound_record_with_frames(self) -> None:
        """A record the map does not bind rides neither side of the ratio.

        The protocol cannot deliver replication to a session-less
        connection, so this shape signals instrument or transport
        corruption rather than server behavior; it renders as unbound and
        the contained numerator keeps the ratio well-defined.
        """
        cell = CELL_SIZE_Q16
        rows = [(i + 1, cell * 2, 0, 0) for i in range(999)]
        bindings = [{"principal_id": 100 + i, "actor_id": i + 1} for i in range(999)]
        pile_ids = frozenset(range(2, 131))
        windows = {f"c{i}": WindowEvidence(pile_ids, (1,)) for i in range(999)}
        orch = _census_orch(_CensusControl(rows, bindings), windows=windows)
        records = [
            _ClientRecord(
                name=f"c{i}",
                principal_id=100 + i,
                instrumented=False,
                own_actor_id=i + 1,
                started_ns=1,
                ready_ns=2,
                inputs_submitted=100,
            )
            for i in range(999)
        ]
        records.append(
            _ClientRecord(
                name="ghost",
                principal_id=1099,
                instrumented=False,
                started_ns=1,
            )
        )
        census = asyncio.run(_collect_breadth(orch, records, _BREADTH_BAR))
        assert census.classified_clients == 999
        assert census.isolated_clients == 0
        assert census.eligible_clients == 999
        assert census.selected_count == 999
        assert census.unbound_clients == 1
        assert census.late_bootstrapped_clients == 0

    def test_runaway_isolation_trips_the_guardrail(self) -> None:
        """Exempting most of the cohort would lower the effective bar.

        The trip carries its measured values as a violation instead of
        raising, so the run's other metrics survive collection and the
        gate fails on the verdict.
        """
        cell = CELL_SIZE_Q16
        # Ten clients marooned on separate islands ten cells apart.
        rows = [(i + 1, cell * (200 + i * 10), 0, 0) for i in range(10)]
        bindings = [{"principal_id": 100 + i, "actor_id": i + 1} for i in range(10)]
        control = _CensusControl(rows, bindings)
        orch = _census_orch(control)
        recs = [
            _ClientRecord(
                name=f"c{i}",
                principal_id=100 + i,
                instrumented=True,
                own_actor_id=i + 1,
                started_ns=1,
                ready_ns=2,
            )
            for i in range(10)
        ]
        census = asyncio.run(_collect_breadth(orch, recs, 1))
        assert census.eligible_clients == 0
        assert census.isolated_clients == 10
        assert census.violation is not None
        assert "10 of 10" in census.violation

    def test_isolation_count_covers_child_stamped_records(self) -> None:
        """A single-instance drive classifies records stamped past index 0.

        A dual-driver worker stamps its records with the true instance
        index for the pooled payload while holding exactly one instance's
        control client. Grouping those records by control-list position
        would classify nothing; the plain single-control shape classifies
        every bound record against the drive's own roster.
        """
        cell = CELL_SIZE_Q16
        rows = [(i + 1, cell * (200 + i * 10), 0, 0) for i in range(10)]
        bindings = [{"principal_id": 100 + i, "actor_id": i + 1} for i in range(10)]
        control = _CensusControl(rows, bindings)
        orch = _census_orch(control)
        recs = [
            _ClientRecord(
                name=f"c{i}",
                principal_id=100 + i,
                instrumented=True,
                own_actor_id=i + 1,
                started_ns=1,
                ready_ns=2,
                instance_index=1,
            )
            for i in range(10)
        ]
        census = asyncio.run(_collect_breadth(orch, recs, 1))
        assert (census.eligible_clients, census.isolated_clients) == (0, 10)

    def test_census_fresh_evidence_breaches_the_divergence_guardrail(self) -> None:
        """Fresh evidence inside isolated geometry is the live divergence.

        A client whose movement-window evidence alone proves a full view
        set while its window-end neighborhood is isolated is exactly the
        classifier-narrower-than-delivery divergence the guardrail
        renders — the verdict fails while the run's artifacts stay
        intact.
        """
        cell = CELL_SIZE_Q16
        rows = [(i + 1, cell * 2, 0, 0) for i in range(999)]
        rows.append((1000, 0, 0, 0))
        bindings = [{"principal_id": 100 + i, "actor_id": i + 1} for i in range(1000)]
        windows = {f"c{i}": WindowEvidence(frozenset(range(2, 131)), (1,)) for i in range(999)}
        windows["divergent"] = WindowEvidence(frozenset(range(1001, 1129)), (1,))
        orch = _census_orch(_CensusControl(rows, bindings), windows=windows)
        records = [
            _ClientRecord(
                name=f"c{i}",
                principal_id=100 + i,
                instrumented=False,
                own_actor_id=i + 1,
                started_ns=1,
                ready_ns=2,
                inputs_submitted=100,
            )
            for i in range(999)
        ]
        records.append(
            _ClientRecord(
                name="divergent",
                principal_id=1099,
                instrumented=False,
                own_actor_id=1000,
                started_ns=1,
                ready_ns=2,
                inputs_submitted=100,
            )
        )
        census = asyncio.run(_collect_breadth(orch, records, _BREADTH_BAR))
        assert census.observed_but_isolated_fresh_clients == 1
        assert census.violation is not None
        assert "fresh" in census.violation
        assert "divergent" in census.violation

    def test_fresh_isolated_guardrail_scopes_to_cross_cell_topologies(self) -> None:
        """The divergence guardrail fires only where isolation is cross-cell.

        On an embedded single-cell topology an "isolated" client is
        pile-edge sparsity that legitimately receives the whole cell's
        publish (cell-scoped delivery), so the fresh flag renders in the
        attribution without failing the verdict; the distributed call
        keeps the breach.
        """
        cell = CELL_SIZE_Q16
        rows = [(i + 1, cell * 2, 0, 0) for i in range(999)]
        rows.append((1000, 0, 0, 0))
        bindings = [{"principal_id": 100 + i, "actor_id": i + 1} for i in range(1000)]
        windows = {f"c{i}": WindowEvidence(frozenset(range(2, 131)), (1,)) for i in range(999)}
        windows["divergent"] = WindowEvidence(frozenset(range(1001, 1129)), (1,))
        orch = _census_orch(_CensusControl(rows, bindings), windows=windows)
        records = [
            _ClientRecord(
                name=f"c{i}",
                principal_id=100 + i,
                instrumented=False,
                own_actor_id=i + 1,
                started_ns=1,
                ready_ns=2,
                inputs_submitted=100,
            )
            for i in range(999)
        ]
        records.append(
            _ClientRecord(
                name="divergent",
                principal_id=1099,
                instrumented=False,
                own_actor_id=1000,
                started_ns=1,
                ready_ns=2,
                inputs_submitted=100,
            )
        )
        census = asyncio.run(
            _collect_breadth(orch, records, _BREADTH_BAR, fresh_isolated_violates=False)
        )
        assert census.observed_but_isolated_fresh_clients == 1
        assert census.violation is None

    def test_report_fails_verdict_when_guardrail_trips(self) -> None:
        """The metrics table renders fully alongside a failing verdict.

        Collection continues through a breach: mm, sessions, and flicker
        stay in the report next to the violation text, because those are
        exactly the numbers needed to interpret why the geometry tripped.
        """
        metrics = GateMetrics(
            actor_count=2000,
            sampled_clients=16,
            session_ok_ratio=1.0,
            selected_clients_ratio=0.99,
            move_missing_ratio=1.0,
            bootstrap_ms_p95=500,
            continuity_flicker=False,
            publish_rate_skew=1.0,
            inputs_submitted=88_000,
            breadth_violation=("173 of 2000 classified clients lack their neighborhood geometry"),
        )
        report = GateReport(
            profile=DISTRIBUTED_2000,
            metrics=metrics,
            thresholds=DISTRIBUTED_THRESHOLDS,
            started_at="2026-08-20T00:00:00+00:00",
            duration_s=6.0,
        )
        assert not report.passed
        rendered = report.to_markdown()
        assert "Breadth guardrail:" in rendered
        assert "173 of 2000" in rendered
        assert "| move_missing_ratio_certified |" in rendered
        assert "| mm_event (companion) |" in rendered
        payload = cast("dict[str, object]", report.to_dict()["metrics"])
        assert isinstance(payload["breadth_violation"], str)

    def test_ratio_keeps_legacy_denominator_without_eligibility(self) -> None:
        """A run without a roster reports over the full population."""
        assert _selected_clients_ratio(90, 100, None) == pytest.approx(0.9)
        assert _selected_clients_ratio(0, 0, None) == 0.0

    def test_ratio_drops_only_the_isolated_from_the_denominator(self) -> None:
        eligible = 1988
        assert _selected_clients_ratio(1900, 2000, eligible) == pytest.approx(1900 / eligible)

    def test_assembly_containment_holds_for_arbitrary_rows(self) -> None:
        """Selected can never exceed eligible, for any row population.

        The containment is structural — the numerator ranges only over
        bound, non-isolated, observed rows — so the reduction can never
        see selected beyond eligible on well-formed inputs. Randomized
        rows pin that property instead of a single hand-built case.
        """
        rng = random.Random(20260829)
        for _ in range(200):
            n = rng.randrange(1, 40)
            rows = [
                _BreadthRow(
                    name=f"c{i}",
                    instrumented=rng.random() < 0.3,
                    submitted=rng.randrange(0, 50),
                    connected=rng.random() < 0.9,
                    observed=rng.random() < 0.8,
                    bound=rng.random() < 0.9,
                    isolated=rng.random() < 0.2,
                    fresh=rng.random() < 0.5,
                    late_bootstrapped=rng.random() < 0.1,
                )
                for i in range(n)
            ]
            census = _assemble_breadth(n, rows, classified=True)
            assert census.selected_count <= (census.eligible_clients or 0)
            assert (census.isolated_clients or 0) + (
                census.eligible_clients or 0
            ) == census.classified_clients
            assert census.classified_clients + census.unbound_clients == n
            assert census.observed_but_isolated_fresh_clients <= (census.isolated_clients or 0)

    def test_residue_rows_split_sparse_from_never_opened(self) -> None:
        """The residue carries only rows the breadth classes leave unexplained.

        Stalled, incomplete, and isolated rows have their own classes and
        the observed rows are selected; the residue is the bound,
        non-isolated, unobserved remainder, split by window presence and
        carried with both distinct-id counts.
        """
        rows = [
            _BreadthRow(
                name="sparse",
                instrumented=False,
                submitted=50,
                connected=True,
                bound=True,
                window_present=True,
                distinct_in_window=34,
                distinct_deque=501,
            ),
            _BreadthRow(
                name="never",
                instrumented=False,
                submitted=50,
                connected=True,
                bound=True,
            ),
            _BreadthRow(
                name="stalled",
                instrumented=False,
                submitted=50,
                connected=True,
                bound=True,
                window_present=True,
                distinct_in_window=200,
                distinct_deque=200,
                stalled=True,
            ),
            _BreadthRow(
                name="incomplete",
                instrumented=False,
                submitted=50,
                connected=True,
                bound=True,
                window_present=True,
                incomplete=True,
            ),
            _BreadthRow(
                name="isolated",
                instrumented=False,
                submitted=50,
                connected=True,
                bound=True,
                isolated=True,
            ),
            _BreadthRow(
                name="observed",
                instrumented=False,
                submitted=50,
                connected=True,
                bound=True,
                observed=True,
            ),
            _BreadthRow(
                name="unbound",
                instrumented=False,
                submitted=50,
                connected=True,
            ),
        ]
        census = _assemble_breadth(len(rows), rows, classified=True)
        assert census.sparse_clients == 1
        assert census.never_opened_clients == 1
        assert [(row.name, row.klass) for row in census.sparse_rows] == [
            ("sparse", "sparse"),
            ("never", "never_opened"),
        ]
        sparse = census.sparse_rows[0]
        assert (sparse.distinct_in_window, sparse.distinct_deque) == (34, 501)
        assert sparse.instance_index == 0

    def test_residue_covers_the_full_population_without_a_roster(self) -> None:
        """Without a roster the residue reads over the full population."""
        rows = [
            _BreadthRow(name="a", instrumented=False, submitted=5, connected=True),
            _BreadthRow(name="b", instrumented=False, submitted=5, connected=True, observed=True),
        ]
        census = _assemble_breadth(len(rows), rows, classified=False)
        assert census.sparse_clients == 0
        assert census.never_opened_clients == 1
        assert [row.name for row in census.sparse_rows] == ["a"]

    def test_report_renders_the_breadth_residue_rows(self) -> None:
        """Sparse residue rows render per client with both counts."""
        metrics = GateMetrics(
            actor_count=2000,
            sampled_clients=16,
            session_ok_ratio=1.0,
            selected_clients_ratio=0.9,
            move_missing_ratio=1.0,
            bootstrap_ms_p95=500,
            continuity_flicker=False,
            publish_rate_skew=1.0,
            inputs_submitted=88_000,
            breadth_sparse_clients=1,
            breadth_sparse_rows=(
                SparseBreadthRow(
                    name="load-7",
                    instance_index=0,
                    klass="sparse",
                    distinct_in_window=34,
                    distinct_deque=501,
                ),
            ),
        )
        report = GateReport(
            profile=DISTRIBUTED_2000,
            metrics=metrics,
            thresholds=DISTRIBUTED_THRESHOLDS,
            started_at="2026-08-20T00:00:00+00:00",
            duration_s=6.0,
        )
        rendered = report.to_markdown()
        assert "| breadth_sparse_clients |" in rendered
        assert "|   load-7 (instance 0) |" in rendered
        assert "sparse (window 34 distinct, deque 501 distinct)" in rendered

    def test_merge_pools_the_classified_denominator(self) -> None:
        """The pooled eligible derives from classified cohorts, not totals.

        A cohort whose collection-time binding map excludes a client must
        shrink the pooled denominator by that client: pooling totals minus
        isolation would count a client that rides neither side of the
        ratio, which is exactly the mismatch that aborted healthy runs.
        """
        results = [
            _driver_result(
                0,
                selected_count=999,
                clients_total=1000,
                isolated_clients=0,
                classified_clients=999,
            ),
            _driver_result(
                1,
                selected_count=998,
                clients_total=1000,
                isolated_clients=2,
                classified_clients=1000,
            ),
        ]
        report = merge_driver_results(
            DISTRIBUTED_2000,
            results,
            ProcTrace(interval_s=0.1, clk_tck=100, samples=()),
            started_at="2026-08-25T00:00:00+00:00",
            duration_s=1.0,
            invocation=None,
        )
        assert report.metrics.classified_clients == 1999
        assert report.metrics.isolated_clients == 2
        assert report.metrics.eligible_clients == 1997
        assert report.metrics.selected_clients_ratio == pytest.approx(1.0)
        assert report.metrics.breadth_violation is None

    def test_merge_pools_the_eligible_denominator(self) -> None:
        """Fully classified cohorts pool to the same eligible as before."""
        results = [
            _driver_result(0, selected_count=990, clients_total=1000, isolated_clients=10),
            _driver_result(1, selected_count=998, clients_total=1000, isolated_clients=2),
        ]
        report = merge_driver_results(
            DISTRIBUTED_2000,
            results,
            ProcTrace(interval_s=0.1, clk_tck=100, samples=()),
            started_at="2026-08-25T00:00:00+00:00",
            duration_s=1.0,
            invocation=None,
        )
        assert report.metrics.isolated_clients == 12
        assert report.metrics.eligible_clients == 1988
        assert report.metrics.selected_clients_ratio == pytest.approx(1.0)
        assert report.metrics.actor_count == DISTRIBUTED_2000.actor_count

    def test_merge_pools_the_breadth_residue(self) -> None:
        """The residue pools by summation with per-client rows in order."""
        results = [
            replace(
                _driver_result(0, selected_count=998, isolated_clients=2),
                sparse_clients=1,
                sparse_rows=(
                    SparseBreadthRow(
                        name="load-7",
                        instance_index=0,
                        klass="sparse",
                        distinct_in_window=34,
                        distinct_deque=501,
                    ),
                ),
            ),
            replace(
                _driver_result(1, selected_count=1000),
                never_opened_clients=2,
                sparse_rows=(
                    SparseBreadthRow(
                        name="load-1001",
                        instance_index=1,
                        klass="never_opened",
                        distinct_in_window=0,
                        distinct_deque=0,
                    ),
                ),
            ),
        ]
        report = merge_driver_results(
            DISTRIBUTED_2000,
            results,
            ProcTrace(interval_s=0.1, clk_tck=100, samples=()),
            started_at="2026-08-25T00:00:00+00:00",
            duration_s=1.0,
            invocation=None,
        )
        assert report.metrics.breadth_sparse_clients == 1
        assert report.metrics.breadth_never_opened_clients == 2
        assert [(row.name, row.klass) for row in report.metrics.breadth_sparse_rows] == [
            ("load-7", "sparse"),
            ("load-1001", "never_opened"),
        ]

    def test_merge_pools_the_breadth_class_counts(self) -> None:
        """Stall and incomplete counts pool by summation; spans by min/p50."""
        results = [
            replace(
                _driver_result(0),
                breadth_stalled_clients=7,
                breadth_incomplete_clients=0,
                breadth_evidence_span_ns_min=9_400_000_000,
                breadth_evidence_span_ns_p50=9_600_000_000,
            ),
            replace(
                _driver_result(1),
                breadth_stalled_clients=0,
                breadth_incomplete_clients=5,
                breadth_evidence_span_ns_min=9_200_000_000,
                breadth_evidence_span_ns_p50=9_800_000_000,
            ),
        ]
        report = merge_driver_results(
            DISTRIBUTED_2000,
            results,
            ProcTrace(interval_s=0.1, clk_tck=100, samples=()),
            started_at="2026-08-25T00:00:00+00:00",
            duration_s=1.0,
            invocation=None,
        )
        assert report.metrics.breadth_stalled_clients == 7
        assert report.metrics.breadth_incomplete_clients == 5
        assert report.metrics.breadth_evidence_span_ns_min == 9_200_000_000
        assert report.metrics.breadth_evidence_span_ns_p50 == 9_800_000_000

    def test_merge_fails_when_an_instance_is_below_the_session_bar(self) -> None:
        """A weak cohort cannot hide in the pooled ratio.

        98.5% and 100% pool to 99.25%, above the 99% bar — the pooled
        verdict alone would pass a run with an instance below bar. The
        merged report carries each cohort's row and the verdict fails on
        the weak one.
        """
        results = [
            _driver_result(0, session_ok_count=985),
            _driver_result(1, session_ok_count=1000),
        ]
        report = merge_driver_results(
            DISTRIBUTED_2000,
            results,
            ProcTrace(interval_s=0.1, clk_tck=100, samples=()),
            started_at="2026-08-25T00:00:00+00:00",
            duration_s=1.0,
            invocation=None,
        )
        assert report.metrics.session_ok_ratio == pytest.approx(0.9925)
        rows = {row.instance_index: row for row in report.metrics.instances}
        assert rows[0].session_ok_ratio == pytest.approx(0.985)
        assert rows[1].session_ok_ratio == pytest.approx(1.0)
        assert not report.passed

    def test_merge_fails_when_an_instance_is_below_the_certified_bar(self) -> None:
        """A certified-missing cohort fails even when the union reads 7.5%.

        The certified ratio pools as summed missing over summed submitted;
        one cohort carrying all the loss dilutes into the union. The
        per-instance rows keep the loss attributed to its instance.
        """
        results = [
            _driver_result(0, move_missing_submitted=100, certified_missing=15),
            _driver_result(1, move_missing_submitted=100, certified_missing=0),
        ]
        report = merge_driver_results(
            DISTRIBUTED_2000,
            results,
            ProcTrace(interval_s=0.1, clk_tck=100, samples=()),
            started_at="2026-08-25T00:00:00+00:00",
            duration_s=1.0,
            invocation=None,
        )
        assert report.metrics.move_missing_ratio_certified == pytest.approx(0.075)
        rows = {row.instance_index: row for row in report.metrics.instances}
        assert rows[0].move_missing_ratio_certified == pytest.approx(0.15)
        assert rows[1].move_missing_ratio_certified == pytest.approx(0.0)
        assert not report.passed

    def test_merge_rows_carry_the_instance_slice(self) -> None:
        """Each row reduces its cohort with the pooled formulas."""
        results = [
            _driver_result(
                0,
                session_ok_count=990,
                selected_count=980,
                move_missing_submitted=100,
                move_missing_observed=95,
                bootstrap_latencies_ms=[100, 200],
            ),
            _driver_result(
                1,
                session_ok_count=1000,
                selected_count=1000,
                move_missing_submitted=100,
                move_missing_observed=100,
                bootstrap_latencies_ms=[300],
            ),
        ]
        report = merge_driver_results(
            DISTRIBUTED_2000,
            results,
            ProcTrace(interval_s=0.1, clk_tck=100, samples=()),
            started_at="2026-08-25T00:00:00+00:00",
            duration_s=1.0,
            invocation=None,
        )
        rows = {row.instance_index: row for row in report.metrics.instances}
        assert rows[0].actor_count == 1000
        assert rows[0].session_ok_ratio == pytest.approx(0.99)
        assert rows[0].selected_clients_ratio == pytest.approx(0.98)
        assert rows[0].move_missing_ratio == pytest.approx(0.05)
        assert rows[0].bootstrap_ms_p95 == 200
        assert rows[1].bootstrap_ms_p95 == 300
        assert rows[1].continuity_flicker is False

    def test_merge_flags_a_payload_breaking_containment(self) -> None:
        """A child numerator outside its cohort renders as a breach.

        Containment is structural in well-formed payloads, so a pooled
        excess means a child serialized counts outside its own cohort;
        the parent fails the verdict on it instead of trusting the ratio.
        """
        results = [
            _driver_result(0, selected_count=999, isolated_clients=0),
            _driver_result(1, selected_count=999, isolated_clients=5),
        ]
        report = merge_driver_results(
            DISTRIBUTED_2000,
            results,
            ProcTrace(interval_s=0.1, clk_tck=100, samples=()),
            started_at="2026-08-25T00:00:00+00:00",
            duration_s=1.0,
            invocation=None,
        )
        assert report.metrics.breadth_violation is not None
        assert "inconsistent" in report.metrics.breadth_violation
        assert not report.passed

    def test_report_rendering_adds_rows_only_when_measured(self) -> None:
        """Byte-compatible markdown when absent; explicit when present."""
        thresholds = DENSE_THRESHOLDS
        plain_metrics = GateMetrics(
            actor_count=10,
            sampled_clients=2,
            session_ok_ratio=1.0,
            selected_clients_ratio=0.9,
            move_missing_ratio=0.0,
            bootstrap_ms_p95=1,
            continuity_flicker=False,
        )
        plain = GateReport(SMOKE_DENSE, plain_metrics, thresholds, "t", 1.0).to_markdown()
        assert "isolated_clients" not in plain
        assert "eligible" not in plain

        measured = replace(plain_metrics, eligible_clients=9, isolated_clients=1)
        report = GateReport(SMOKE_DENSE, measured, thresholds, "t", 1.0)
        text = report.to_markdown()
        assert "| isolated_clients | 1 | reported-only | — |" in text
        assert "of 9 eligible" in text

    def test_breadth_attribution_tags_only_the_fresh_window_fact(self) -> None:
        """The evidence tag never claims window evidence it cannot see.

        The counts row splits the isolated population by whether the
        movement window alone carried the bar; a fresh entry tags its
        divergence signature and breaches the guardrail.
        """
        thresholds = DENSE_THRESHOLDS
        base = GateMetrics(
            actor_count=1000,
            sampled_clients=32,
            session_ok_ratio=1.0,
            selected_clients_ratio=0.99,
            move_missing_ratio=0.0,
            bootstrap_ms_p95=400,
            continuity_flicker=False,
            eligible_clients=997,
            classified_clients=1000,
            isolated_clients=3,
        )
        none_fresh = replace(
            base,
            breadth_attribution=(
                BreadthAttribution(name="load-1", klass="frozen", fresh=False),
                BreadthAttribution(name="load-2", klass="frozen", fresh=False),
                BreadthAttribution(name="load-3", klass="frozen", fresh=False),
            ),
        )
        text = GateReport(SMOKE_DENSE, none_fresh, thresholds, "t", 1.0).to_markdown()
        assert (
            "| observed_but_isolated | 0 fresh / 3 without window evidence | reported-only | — |"
            in text
        )
        assert "|   load-1 | frozen (no fresh evidence in window) | reported-only | — |" in text
        assert "|   load-2 | frozen (no fresh evidence in window) | reported-only | — |" in text
        assert "|   load-3 | frozen (no fresh evidence in window) | reported-only | — |" in text

        fresh = replace(
            base,
            observed_but_isolated_fresh_clients=1,
            breadth_violation=(
                "1 of 1000 classified clients hold fresh evidence of a full view set "
                "while isolated (load-1); delivery reached beyond the eligibility geometry"
            ),
            breadth_attribution=(
                BreadthAttribution(name="load-1", klass="frozen", fresh=True),
                BreadthAttribution(name="load-2", klass="frozen", fresh=False),
                BreadthAttribution(name="load-3", klass="frozen", fresh=False),
            ),
        )
        report = GateReport(SMOKE_DENSE, fresh, thresholds, "t", 1.0)
        text = report.to_markdown()
        assert (
            "| observed_but_isolated | 1 fresh / 2 without window evidence | reported-only | — |"
            in text
        )
        assert "|   load-1 | frozen (fresh evidence in window) | reported-only | — |" in text
        assert not report.passed
        assert report.metrics.breadth_violation is not None
        assert "hold fresh" in report.metrics.breadth_violation


class TestDriverChildAffinity:
    """Per-child CPU pinning: parsing, argv injection, payload field."""

    def test_parse_cpu_list_accepts_distinct_ints(self) -> None:
        assert parse_cpu_list("0,8") == [0, 8]

    def test_parse_cpu_list_rejects_bad_specs(self) -> None:
        """Empty, non-numeric, negative, and duplicate lists all fail."""
        for spec in ("", "x", "0,x", "-1", "1,1"):
            with pytest.raises(ValueError):
                parse_cpu_list(spec)

    def test_worker_argv_injects_child_cpus(self) -> None:
        argv = _worker_argv(0, DISTRIBUTED_2000, 1000, 16, 40000, 40001, 123, "d.json", "0,8")
        assert argv[-2:] == ["--driver-cpus", "0,8"]

    def test_worker_argv_without_cpus_stays_legacy(self) -> None:
        argv = _worker_argv(0, DISTRIBUTED_2000, 1000, 16, 40000, 40001, 123, "d.json")
        assert "--driver-cpus" not in argv

    def test_apply_pins_and_reports_applied_set(self, monkeypatch: pytest.MonkeyPatch) -> None:
        requested: set[int] | None = None

        def fake_setaffinity(_pid: int, cpus: set[int]) -> None:
            nonlocal requested
            requested = cpus

        monkeypatch.setattr(os, "sched_setaffinity", fake_setaffinity)
        monkeypatch.setattr(os, "sched_getaffinity", lambda _pid: {8, 0})
        args = argparse.Namespace(driver_cpus="0,8")
        assert apply_driver_affinity(args) == [0, 8]
        assert requested == {0, 8}

    def test_apply_is_none_without_the_flag(self) -> None:
        assert apply_driver_affinity(argparse.Namespace()) is None

    def test_payload_roundtrip_carries_cpus(self, tmp_path: Path) -> None:
        result = replace(_driver_result(0), driver_cpus=[0, 8])
        path = tmp_path / "r.json"
        dump_driver_result(result, str(path))
        assert load_driver_result(str(path)).driver_cpus == [0, 8]

    def test_payload_roundtrip_carries_the_breadth_residue(self, tmp_path: Path) -> None:
        """The residue counts and per-client rows survive the worker payload."""
        result = replace(
            _driver_result(0),
            sparse_clients=1,
            never_opened_clients=2,
            sparse_rows=(
                SparseBreadthRow(
                    name="load-7",
                    instance_index=0,
                    klass="sparse",
                    distinct_in_window=34,
                    distinct_deque=501,
                ),
            ),
            breadth_stalled_clients=3,
            breadth_incomplete_clients=4,
            breadth_evidence_span_ns_min=9_400_000_000,
            breadth_evidence_span_ns_p50=9_500_000_000,
        )
        path = tmp_path / "r.json"
        dump_driver_result(result, str(path))
        assert load_driver_result(str(path)) == result

    def test_loader_defaults_missing_residue_to_zero(self, tmp_path: Path) -> None:
        """A pre-residue payload (no sparse keys) still loads."""
        path = tmp_path / "r.json"
        dump_driver_result(_driver_result(0), str(path))
        payload = json.loads(path.read_text(encoding="utf-8"))
        for key in (
            "sparse_clients",
            "never_opened_clients",
            "sparse_rows",
            "breadth_stalled_clients",
            "breadth_incomplete_clients",
        ):
            del payload[key]
        path.write_text(json.dumps(payload), encoding="utf-8")
        loaded = load_driver_result(str(path))
        assert loaded.sparse_clients == 0
        assert loaded.never_opened_clients == 0
        assert loaded.sparse_rows == ()
        assert loaded.breadth_stalled_clients == 0
        assert loaded.breadth_incomplete_clients == 0
        assert loaded.breadth_evidence_span_ns_min is None
        assert loaded.breadth_evidence_span_ns_p50 is None

    def test_loader_defaults_missing_cpus_to_none(self, tmp_path: Path) -> None:
        """A pre-pinning payload (no driver_cpus key) still loads."""
        path = tmp_path / "r.json"
        dump_driver_result(_driver_result(0), str(path))
        payload = json.loads(path.read_text(encoding="utf-8"))
        del payload["driver_cpus"]
        path.write_text(json.dumps(payload), encoding="utf-8")
        assert load_driver_result(str(path)).driver_cpus is None


def _driver_result(
    instance_index: int,
    *,
    clients_total: int = 1000,
    session_ok_count: int = 1000,
    selected_count: int = 1000,
    sampled_clients: int = 16,
    move_missing_submitted: int = 100,
    move_missing_observed: int = 100,
    certified_missing: int = 0,
    bootstrap_latencies_ms: list[int] | None = None,
    delivered_per_instance: dict[int, int] | None = None,
    counters_present: bool = True,
    series: list[ClientSeries] | None = None,
    movement_start_ns: int = 1_000_000_000,
    movement_end_ns: int = 1_100_000_000,
    isolated_clients: int = 0,
    inputs_submitted: int = -1,
    classified_clients: int | None = None,
) -> DriverResult:
    """A driver result with defaults that pool into a healthy report.

    The default phase counters sit inside the observer bands (compose
    2000 ms, deliver 3100 ms) with dispatches equal to the submitted
    inputs, so the merged observation stays ``ok`` unless a test
    overrides a field. The classified cohort defaults to the full cohort
    so the pooled containment holds unless a test shrinks it.
    """
    return DriverResult(
        schema_version=_SCHEMA_VERSION,
        driver_pid=100 + instance_index,
        instance_index=instance_index,
        clients_total=clients_total,
        session_ok_count=session_ok_count,
        selected_count=selected_count,
        sampled_clients=sampled_clients,
        move_missing_submitted=move_missing_submitted,
        move_missing_observed=move_missing_observed,
        inputs_submitted=(move_missing_submitted if inputs_submitted < 0 else inputs_submitted),
        bootstrap_latencies_ms=bootstrap_latencies_ms or [],
        delivered_per_instance=(
            {instance_index: 500} if delivered_per_instance is None else delivered_per_instance
        ),
        phase_counter=PhaseCounters(
            refresh_ns=instance_index,
            compose_ns=2_000_000_000,
            deliver_ns=3_100_000_000,
            dispatches=move_missing_submitted,
            publishes=1,
            compose_window_ns=10,
            compose_prior_ns=20,
            compose_lock_wait_ns=30,
            compose_scan_ns=40,
            compose_sort_ns=50,
            compose_select_ns=60,
            compose_skips=0,
            compose_deferrals=0,
            view_locate_failures=0,
            write_ns=70,
            write_deferrals=0,
        ),
        counters_present=counters_present,
        series=series or [],
        movement_start_ns=movement_start_ns,
        movement_end_ns=movement_end_ns,
        isolated_clients=isolated_clients,
        classified_clients=(
            classified_clients if classified_clients is not None else clients_total
        ),
        certified_missing=certified_missing,
    )


class TestDriverFlags:
    """Cross-flag validation for the dual-driver and worker modes.

    The validator resolves the parent's stagger (600 ms default) or
    rejects the combination with a usage error; worker mode fails fast
    when its internal wiring flags are missing. Pure-logic test (no C
    build, no server).
    """

    def test_dual_drivers_default_stagger(self) -> None:
        """The parent's stagger defaults to the documented 600 ms."""
        parser = _build_parser()
        args = parser.parse_args(["--dual-drivers", "--subprocess-cluster"])
        assert _validate_driver_flags(parser, args, DISTRIBUTED_2000) == _DEFAULT_STAGGER_MS

    def test_dual_drivers_explicit_stagger(self) -> None:
        """An explicit stagger overrides the default for this run."""
        parser = _build_parser()
        args = parser.parse_args(
            ["--dual-drivers", "--subprocess-cluster", "--driver-stagger-ms", "250"]
        )
        assert _validate_driver_flags(parser, args, DISTRIBUTED_2000) == 250

    def test_dual_drivers_requires_cluster(self) -> None:
        """Dual drivers manage a spawned cluster; nothing else boots one."""
        parser = _build_parser()
        args = parser.parse_args(["--dual-drivers"])
        with pytest.raises(SystemExit):
            _validate_driver_flags(parser, args, DISTRIBUTED_2000)

    def test_dual_drivers_rejects_other_instance_counts(self) -> None:
        """One cohort per instance pins the mode to exactly two instances."""
        parser = _build_parser()
        args = parser.parse_args(["--dual-drivers", "--subprocess-cluster", "--instances", "3"])
        with pytest.raises(SystemExit):
            _validate_driver_flags(parser, args, DISTRIBUTED_2000)

    def test_dual_drivers_needs_even_clients(self) -> None:
        """An odd client count cannot split into two equal cohorts."""
        parser = _build_parser()
        args = parser.parse_args(["--dual-drivers", "--subprocess-cluster", "--clients", "1001"])
        resolved = _apply_client_override(DISTRIBUTED_2000, 1001)
        with pytest.raises(SystemExit):
            _validate_driver_flags(parser, args, resolved)

    def test_stagger_without_dual_drivers_is_an_error(self) -> None:
        """The stagger knob is inert outside dual-driver mode."""
        parser = _build_parser()
        args = parser.parse_args(["--subprocess-cluster", "--driver-stagger-ms", "250"])
        with pytest.raises(SystemExit):
            _validate_driver_flags(parser, args, DISTRIBUTED_2000)

    def test_single_driver_bare_invocation_is_valid(self) -> None:
        """A plain cluster run parses and validates without any driver flags."""
        parser = _build_parser()
        args = parser.parse_args(["--subprocess-cluster"])
        assert _validate_driver_flags(parser, args, DISTRIBUTED_2000) is None

    def test_negative_stagger_is_an_error(self) -> None:
        """A negative stagger is meaningless; zero stays available."""
        parser = _build_parser()
        args = parser.parse_args(
            ["--dual-drivers", "--subprocess-cluster", "--driver-stagger-ms", "-5"]
        )
        with pytest.raises(SystemExit):
            _validate_driver_flags(parser, args, DISTRIBUTED_2000)

    def test_zero_stagger_is_valid(self) -> None:
        """A zero stagger drives the same-anchor control configuration."""
        parser = _build_parser()
        args = parser.parse_args(
            ["--dual-drivers", "--subprocess-cluster", "--driver-stagger-ms", "0"]
        )
        assert _validate_driver_flags(parser, args, DISTRIBUTED_2000) == 0

    def test_child_cpus_require_dual_drivers(self) -> None:
        """Per-child pinning is inert outside dual-driver mode."""
        parser = _build_parser()
        args = parser.parse_args(["--subprocess-cluster", "--driver-child-cpus-a", "0,8"])
        with pytest.raises(SystemExit):
            _validate_driver_flags(parser, args, DISTRIBUTED_2000)

    def test_child_cpus_garbage_list_is_an_error(self) -> None:
        """A malformed CPU list fails validation before any boot."""
        parser = _build_parser()
        args = parser.parse_args(
            ["--dual-drivers", "--subprocess-cluster", "--driver-child-cpus-a", "0,x"]
        )
        with pytest.raises(SystemExit):
            _validate_driver_flags(parser, args, DISTRIBUTED_2000)

    def test_child_cpus_duplicate_list_is_an_error(self) -> None:
        """A CPU list with duplicates is ambiguous and rejected."""
        parser = _build_parser()
        args = parser.parse_args(
            ["--dual-drivers", "--subprocess-cluster", "--driver-child-cpus-b", "1,1"]
        )
        with pytest.raises(SystemExit):
            _validate_driver_flags(parser, args, DISTRIBUTED_2000)

    def test_both_child_cpu_lists_validate(self) -> None:
        """Two disjoint SMT-pair lists pass dual-driver validation."""
        parser = _build_parser()
        args = parser.parse_args(
            [
                "--dual-drivers",
                "--subprocess-cluster",
                "--driver-child-cpus-a",
                "0,8",
                "--driver-child-cpus-b",
                "1,9",
            ]
        )
        assert _validate_driver_flags(parser, args, DISTRIBUTED_2000) == _DEFAULT_STAGGER_MS

    def test_worker_mode_happy_path(self) -> None:
        """A fully wired worker invocation validates to no stagger."""
        parser = _build_parser()
        argv = [
            "--profile",
            "distributed-2000",
            "--driver-worker",
            "1",
            "--gateway-port",
            "40000",
            "--control-port",
            "40001",
            "--movement-anchor-ns",
            "123456789",
            "--instrumented-sample-size",
            "16",
            "--result-output",
            "driver-1.json",
        ]
        args = parser.parse_args(argv)
        assert _validate_driver_flags(parser, args, DISTRIBUTED_2000) is None

    @pytest.mark.parametrize("missing", ["--gateway-port", "--control-port", "--result-output"])
    def test_worker_mode_requires_wiring(self, missing: str) -> None:
        """Each internal wiring flag is required; its absence fails fast."""
        parser = _build_parser()
        argv = [
            "--driver-worker",
            "0",
            "--gateway-port",
            "40000",
            "--control-port",
            "40001",
            "--movement-anchor-ns",
            "123456789",
            "--result-output",
            "driver-0.json",
        ]
        start = argv.index(missing)
        del argv[start : start + 2]
        args = parser.parse_args(argv)
        with pytest.raises(SystemExit):
            _validate_driver_flags(parser, args, DISTRIBUTED_2000)

    def test_worker_mode_rejects_cluster_flags(self) -> None:
        """The worker drives a pre-booted instance; it boots nothing."""
        parser = _build_parser()
        args = parser.parse_args(
            [
                "--driver-worker",
                "0",
                "--gateway-port",
                "40000",
                "--control-port",
                "40001",
                "--movement-anchor-ns",
                "123456789",
                "--result-output",
                "driver-0.json",
                "--subprocess-cluster",
            ]
        )
        with pytest.raises(SystemExit):
            _validate_driver_flags(parser, args, DISTRIBUTED_2000)


class TestDriverSplitArithmetic:
    """Cohort sizing reproduces the single-drive composition.

    The client population splits evenly; the depth-metric sample splits
    ceil/floor so the union across drivers equals the single drive's
    first-N instrumented set dealt round-robin. Pure-logic test.
    """

    def test_clients_split_evenly(self) -> None:
        """The gate's 2000 clients become two 1000-client cohorts."""
        assert split_clients(2000) == (1000, 1000)

    def test_odd_clients_rejected(self) -> None:
        """An odd population has no exact two-way split."""
        with pytest.raises(ValueError):
            split_clients(999)

    def test_instrumented_split_reproduces_global_composition(self) -> None:
        """32 global instrumented clients deal as 16/16 under round-robin."""
        assert split_instrumented(32) == (16, 16)

    def test_instrumented_split_handles_remainder(self) -> None:
        """Odd sample sizes give the surplus to the first driver."""
        assert split_instrumented(5) == (3, 2)
        assert split_instrumented(0) == (0, 0)

    def test_worker_factory_builds_an_orchestrator(self) -> None:
        """The partitioned factory constructs without a booted instance.

        The control client connects lazily, so building the factory's
        orchestrator exercises the real construction path offline —
        including every name the factory body resolves at runtime. The
        child drives exactly one instance, so its control plane is that
        instance's client: an aggregating control would place the
        instance's client at list position 0 while the child's records
        carry the true instance index, and every per-instance grouping
        would then drop the whole cohort.
        """
        from tools.agent.dual_driver import _worker_host_factory

        factory = _worker_host_factory(
            instance_index=1,
            gateway_port=40000,
            control_port=40001,
            instrumented_count=16,
            replication_batch_type_id=0,
        )
        orch = factory()
        assert isinstance(orch.server_control, ServerControlClient)


class TestDriverResultPayload:
    """The worker payload round-trips through JSON intact."""

    def test_round_trip_preserves_nested_values(self, tmp_path: Path) -> None:
        """Counters, per-instance maps, and frame series survive a dump/load."""
        result = _driver_result(
            1,
            bootstrap_latencies_ms=[900, 910],
            delivered_per_instance={1: 4321},
            series=[
                ClientSeries(
                    name="load-3",
                    instance_index=1,
                    window_start_ns=1_000_000_000,
                    window_end_ns=1_100_000_000,
                    frames=(SeriesFrame(ts_ns=1_010_000_000, input_tick=7),),
                )
            ],
        )
        path = tmp_path / "driver-1.json"
        dump_driver_result(result, str(path))
        loaded = load_driver_result(str(path))
        assert loaded == result

    def test_round_trip_preserves_absent_counters(self, tmp_path: Path) -> None:
        """A failed-scrape payload reloads with its presence flag still False."""
        result = _driver_result(0, counters_present=False)
        path = tmp_path / "driver-0.json"
        dump_driver_result(result, str(path))
        loaded = load_driver_result(str(path))
        assert loaded.counters_present is False
        assert loaded == result

    def test_legacy_payload_without_locate_failures_loads(self, tmp_path: Path) -> None:
        """A pre-locate-failure v2 counter block reloads with the counter zero."""
        result = _driver_result(0)
        path = tmp_path / "driver-0.json"
        dump_driver_result(result, str(path))
        payload = json.loads(path.read_text(encoding="utf-8"))
        del payload["phase_counter"]["view_locate_failures"]
        path.write_text(json.dumps(payload), encoding="utf-8")
        loaded = load_driver_result(str(path))
        assert loaded.phase_counter.view_locate_failures == 0
        assert loaded.counters_present == result.counters_present

    def test_foreign_schema_rejected(self, tmp_path: Path) -> None:
        """A payload from an incompatible generation fails loudly."""
        path = tmp_path / "stale.json"
        path.write_text(json.dumps({"schema_version": 999}), encoding="utf-8")
        with pytest.raises(ValueError):
            load_driver_result(str(path))

    def test_legacy_v1_payload_rejected(self, tmp_path: Path) -> None:
        """A pre-presence payload cannot silently pool into a v2 parent."""
        path = tmp_path / "legacy.json"
        path.write_text(json.dumps({"schema_version": 1}), encoding="utf-8")
        with pytest.raises(ValueError):
            load_driver_result(str(path))


class TestMergeDriverResults:
    """Pooling sums integer counts over the union population.

    Averaging ratios would be wrong whenever the drivers' denominators
    differ; these tests pin the pooled arithmetic, the instance ordering,
    and the ledger anchor. Pure-logic test (no C build, no server).
    """

    def test_move_missing_pools_counts_not_ratios(self) -> None:
        """The pooled ratio comes from summed counts, not averaged ones."""
        results = [
            _driver_result(0, move_missing_submitted=10, move_missing_observed=9),
            _driver_result(1, move_missing_submitted=100, move_missing_observed=50),
        ]
        report = merge_driver_results(
            DISTRIBUTED_2000,
            results,
            ProcTrace(interval_s=0.1, clk_tck=100, samples=()),
            started_at="2026-08-25T00:00:00+00:00",
            duration_s=1.0,
            invocation=None,
        )
        expected = 1.0 - 59 / 110
        assert report.metrics.move_missing_ratio == pytest.approx(expected)
        assert report.metrics.move_missing_ratio != pytest.approx(0.30)

    def test_bootstrap_percentile_over_union(self) -> None:
        """The p95 spans both cohorts' latency lists, not each alone."""
        results = [
            _driver_result(0, bootstrap_latencies_ms=[900, 910]),
            _driver_result(1, bootstrap_latencies_ms=[100, 110]),
        ]
        report = merge_driver_results(
            DISTRIBUTED_2000,
            results,
            ProcTrace(interval_s=0.1, clk_tck=100, samples=()),
            started_at="2026-08-25T00:00:00+00:00",
            duration_s=1.0,
            invocation=None,
        )
        assert report.metrics.bootstrap_ms_p95 == 910

    def test_submit_rounds_pool_over_children(self) -> None:
        """The rounds diagnostic sums every cohort's submitted inputs."""
        results = [
            _driver_result(0, inputs_submitted=40_000),
            _driver_result(1, inputs_submitted=48_000),
        ]
        report = merge_driver_results(
            DISTRIBUTED_2000,
            results,
            ProcTrace(interval_s=0.1, clk_tck=100, samples=()),
            started_at="2026-08-25T00:00:00+00:00",
            duration_s=1.0,
            invocation=None,
        )
        assert report.metrics.inputs_submitted == 88_000
        rendered = report.to_markdown()
        # 88000 / 2000 planned-client population = 44.0 of ceil(5 * 20).
        assert "44.0 / 100 planned" in rendered

    def test_skew_pools_per_instance_delivered_counts(self) -> None:
        """Delivered counts sum per instance before the max/min ratio."""
        results = [
            _driver_result(0, delivered_per_instance={0: 6}),
            _driver_result(1, delivered_per_instance={1: 3}),
        ]
        report = merge_driver_results(
            DISTRIBUTED_2000,
            results,
            ProcTrace(interval_s=0.1, clk_tck=100, samples=()),
            started_at="2026-08-25T00:00:00+00:00",
            duration_s=1.0,
            invocation=None,
        )
        assert report.metrics.publish_rate_skew == pytest.approx(2.0)

    def test_skew_infinite_when_a_booted_instance_delivered_zero(self) -> None:
        """A zero-delivery cohort mirrors the single-drive skew semantics."""
        results = [
            _driver_result(0, delivered_per_instance={0: 5}),
            _driver_result(1, delivered_per_instance={1: 0}),
        ]
        report = merge_driver_results(
            DISTRIBUTED_2000,
            results,
            ProcTrace(interval_s=0.1, clk_tck=100, samples=()),
            started_at="2026-08-25T00:00:00+00:00",
            duration_s=1.0,
            invocation=None,
        )
        assert report.metrics.publish_rate_skew == float("inf")

    def test_phase_counters_ordered_by_instance(self) -> None:
        """Concatenation follows instance order whatever the input order."""
        results = [_driver_result(1), _driver_result(0)]
        report = merge_driver_results(
            DISTRIBUTED_2000,
            results,
            ProcTrace(interval_s=0.1, clk_tck=100, samples=()),
            started_at="2026-08-25T00:00:00+00:00",
            duration_s=1.0,
            invocation=None,
        )
        first, second = report.metrics.phase_counters
        assert (first.refresh_ns, second.refresh_ns) == (0, 1)

    def test_ledger_builds_over_pooled_series(self) -> None:
        """One flicker ledger anchors at the earliest pooled series start."""
        base = 1_000_000_000
        gap_series = ClientSeries(
            name="load-b",
            instance_index=1,
            window_start_ns=base,
            window_end_ns=base + 600_000_000,
            frames=(
                SeriesFrame(ts_ns=base, input_tick=1),
                SeriesFrame(ts_ns=base + 300_000_000, input_tick=2),
            ),
        )
        quiet_series = ClientSeries(
            name="load-a",
            instance_index=0,
            window_start_ns=base - 50_000_000,
            window_end_ns=base + 550_000_000,
            frames=(SeriesFrame(ts_ns=base, input_tick=1),),
        )
        results = [
            _driver_result(0, series=[quiet_series]),
            _driver_result(1, series=[gap_series]),
        ]
        report = merge_driver_results(
            DISTRIBUTED_2000,
            results,
            ProcTrace(interval_s=0.1, clk_tck=100, samples=()),
            started_at="2026-08-25T00:00:00+00:00",
            duration_s=1.0,
            invocation=None,
        )
        assert report.metrics.continuity_flicker is True
        assert report.metrics.flicker_count == 1
        ledger = report.metrics.flicker_ledger
        assert ledger is not None
        assert ledger.window_start_ns == base - 50_000_000


class TestMovementAnchorDelay:
    """The movement phase waits out the absolute cross-process anchor."""

    def test_no_anchor_drives_immediately(self) -> None:
        """The default (None) adds no wait — the single-drive behavior."""
        assert _anchor_delay_s(None) == 0.0

    def test_past_anchor_clamps_to_zero(self) -> None:
        """A missed anchor never delays a run retroactively."""
        past = time.monotonic_ns() - 1_000_000
        assert _anchor_delay_s(past) == 0.0

    def test_future_anchor_yields_remaining_seconds(self) -> None:
        """The delay is the remaining time until the anchor, in seconds."""
        anchor = time.monotonic_ns() + 50_000_000
        assert 0.04 <= _anchor_delay_s(anchor) <= 0.06


class TestReportInvocation:
    """The report records the command line that drove its run.

    The profile in a report is already the resolved configuration (the
    CLI overrides replace fields before the drive), so the invocation
    argv is the one missing piece for reproducing a measurement from the
    report alone.
    """

    @staticmethod
    def _report(invocation: str | None) -> GateReport:
        return GateReport(
            profile=DISTRIBUTED_2000,
            metrics=GateMetrics(
                actor_count=1000,
                sampled_clients=32,
                session_ok_ratio=1.0,
                selected_clients_ratio=1.0,
                move_missing_ratio=0.0,
                bootstrap_ms_p95=450,
                continuity_flicker=False,
                publish_rate_skew=1.0,
            ),
            thresholds=DISTRIBUTED_THRESHOLDS,
            started_at="2026-08-22T00:00:00+00:00",
            duration_s=44.0,
            invocation=invocation,
        )

    def test_invocation_renders_in_markdown_and_json(self) -> None:
        """A recorded invocation appears verbatim in both renderings."""
        rendered = self._report("python -m tools.agent.load_harness --clients 1000").to_markdown()
        assert "**Invocation:**" in rendered
        assert "--clients 1000" in rendered
        as_dict = self._report("python -m tools.agent.load_harness").to_dict()
        assert as_dict["invocation"] == "python -m tools.agent.load_harness"

    def test_invocation_defaults_to_absent(self) -> None:
        """A report built without an invocation renders no invocation line.

        Programmatic drivers that construct reports directly keep the
        metadata block unchanged.
        """
        rendered = self._report(None).to_markdown()
        assert "**Invocation:**" not in rendered
        assert self._report(None).to_dict()["invocation"] is None


class TestSkewRendering:
    """The publish-rate-skew cells render as comparable ratios or 'n/a'.

    A legitimate skew is a max/min ratio flooring at 1.0, so the
    metric-not-computed default (0.0) must not render as a bogus ratio,
    and the threshold cell carries its comparison operator like every
    other gated row.
    """

    @staticmethod
    def _markdown(skew: float, thresholds: GateThresholds) -> str:
        report = GateReport(
            profile=DISTRIBUTED_2000,
            metrics=GateMetrics(
                actor_count=1000,
                sampled_clients=16,
                session_ok_ratio=1.0,
                selected_clients_ratio=1.0,
                move_missing_ratio=0.0,
                bootstrap_ms_p95=400,
                continuity_flicker=False,
                publish_rate_skew=skew,
            ),
            thresholds=thresholds,
            started_at="2026-08-25T00:00:00+00:00",
            duration_s=6.0,
        )
        return report.to_markdown()

    def test_computed_skew_renders_ratio_and_gated_threshold(self) -> None:
        """A measured skew renders as a ratio against '<= <threshold>x'."""
        rendered = self._markdown(1.25, DISTRIBUTED_THRESHOLDS)
        assert "| publish_rate_skew | 1.25x | <= 3.00x | yes |" in rendered

    def test_not_computed_sentinel_renders_n_a(self) -> None:
        """The 0.0 metric default renders 'n/a', not a zero ratio."""
        rendered = self._markdown(0.0, DISTRIBUTED_THRESHOLDS)
        assert "| publish_rate_skew | n/a | <= 3.00x | yes |" in rendered

    def test_ungated_threshold_renders_n_a(self) -> None:
        """An inf threshold (the single-instance gate) renders 'n/a'."""
        assert DENSE_THRESHOLDS.publish_rate_skew_max == float("inf")
        rendered = self._markdown(0.0, DENSE_THRESHOLDS)
        assert "| publish_rate_skew | n/a | n/a | yes |" in rendered


class TestFlickerBreakdown:
    """The per-client flicker-distribution breakdown is a pure value object.

    These tests cover the markdown rendering (the uniform-vs-concentrated
    framing and the gap-size spread) and the JSON round-trip without a C
    build or a running server, so they run in every environment alongside
    the history-sizing and client-override pure-logic tests.
    """

    @staticmethod
    def _metrics(breakdown: FlickerBreakdown | None, *, sampled: int = 32) -> GateMetrics:
        total = breakdown.total_gaps if breakdown is not None else 0
        return GateMetrics(
            actor_count=1000,
            sampled_clients=sampled,
            session_ok_ratio=1.0,
            selected_clients_ratio=1.0,
            move_missing_ratio=0.0,
            bootstrap_ms_p95=400,
            continuity_flicker=total > 0,
            flicker_count=total,
            flicker_breakdown=breakdown,
        )

    def test_no_flicker_omits_the_breakdown_block(self) -> None:
        """A healthy run renders no flicker-detail block."""
        breakdown = FlickerBreakdown(total_gaps=0, per_client=())
        report = GateReport(
            profile=DISTRIBUTED_2000,
            metrics=self._metrics(breakdown),
            thresholds=DISTRIBUTED_THRESHOLDS,
            started_at="2026-08-21T00:00:00+00:00",
            duration_s=6.0,
        )
        rendered = report.to_markdown()
        assert "Flicker gaps" not in rendered

    def test_uniform_burst_reports_all_sampled_clients_flickered(self) -> None:
        """A gap on every sampled client renders 'N of M sampled clients'."""
        per_client = tuple(
            ClientFlickerSample(
                name=f"load-{i}",
                instance_index=i % 2,
                gap_count=9,
                gap_ms=(210,) * 9,
                gap_start_ms=tuple(range(600, 600 * 9, 600)),
            )
            for i in range(32)
        )
        breakdown = FlickerBreakdown(total_gaps=288, per_client=per_client)
        report = GateReport(
            profile=DISTRIBUTED_2000,
            metrics=self._metrics(breakdown, sampled=32),
            thresholds=DISTRIBUTED_THRESHOLDS,
            started_at="2026-08-21T00:00:00+00:00",
            duration_s=6.0,
        )
        rendered = report.to_markdown()
        assert "Flicker gaps:** 288 across 32 of 32 sampled clients" in rendered
        assert "per-client gaps:" in rendered
        assert "gap size (ms):" in rendered

    def test_concentrated_burst_reports_a_few_of_sampled_clients(self) -> None:
        """Gaps on a minority of clients render that minority count."""
        per_client = tuple(
            ClientFlickerSample(
                name=f"load-{i}",
                instance_index=0,
                gap_count=92,
                gap_ms=(612,) * 92,
                gap_start_ms=(0,) * 92,
            )
            for i in (3, 17, 29)
        )
        breakdown = FlickerBreakdown(total_gaps=276, per_client=per_client)
        report = GateReport(
            profile=DISTRIBUTED_2000,
            metrics=self._metrics(breakdown, sampled=32),
            thresholds=DISTRIBUTED_THRESHOLDS,
            started_at="2026-08-21T00:00:00+00:00",
            duration_s=6.0,
        )
        rendered = report.to_markdown()
        assert "Flicker gaps:** 276 across 3 of 32 sampled clients" in rendered

    def test_breakdown_round_trips_through_json(self) -> None:
        """The breakdown serializes to JSON and parses back to the same shape."""
        per_client = (
            ClientFlickerSample(
                name="load-0",
                instance_index=1,
                gap_count=2,
                gap_ms=(205, 640),
                gap_start_ms=(300, 1800),
            ),
        )
        breakdown = FlickerBreakdown(total_gaps=2, per_client=per_client)
        report = GateReport(
            profile=DISTRIBUTED_2000,
            metrics=self._metrics(breakdown, sampled=32),
            thresholds=DISTRIBUTED_THRESHOLDS,
            started_at="2026-08-21T00:00:00+00:00",
            duration_s=6.0,
        )
        payload = json.loads(report.to_json())
        metrics = payload["metrics"]
        assert metrics["flicker_count"] == 2
        fb = metrics["flicker_breakdown"]
        assert fb["total_gaps"] == 2
        assert fb["per_client"][0]["name"] == "load-0"
        assert fb["per_client"][0]["instance_index"] == 1
        assert fb["per_client"][0]["gap_ms"] == [205, 640]
        assert fb["per_client"][0]["gap_start_ms"] == [300, 1800]


class TestPhaseCounters:
    """The reactor-path phase-counter scrape is a pure value-object path.

    The parser, the movement-window delta, the markdown rendering (the
    dominant-phase column), and the JSON round-trip are covered without a C
    build or a running server, so they run in every environment alongside
    the flicker-breakdown and pure-logic tests.
    """

    @staticmethod
    def _metrics(counters: tuple[PhaseCounters, ...]) -> GateMetrics:
        return GateMetrics(
            actor_count=1000,
            sampled_clients=32,
            session_ok_ratio=1.0,
            selected_clients_ratio=1.0,
            move_missing_ratio=0.0,
            bootstrap_ms_p95=400,
            continuity_flicker=False,
            phase_counters=counters,
        )

    def test_parse_reads_each_counter_line(self) -> None:
        """A label-less Prometheus scrape parses to the fifteen counters."""
        text = (
            "# TYPE kith_gateway_refresh_ns_total counter\n"
            "kith_gateway_refresh_ns_total 1500000000\n"
            "# TYPE kith_gateway_compose_ns_total counter\n"
            "kith_gateway_compose_ns_total 6200000000\n"
            "# TYPE kith_gateway_deliver_ns_total counter\n"
            "kith_gateway_deliver_ns_total 900000000\n"
            "# TYPE kith_gateway_dispatches_total counter\n"
            "kith_gateway_dispatches_total 50000\n"
            "# TYPE kith_fabric_publishes_total counter\n"
            "kith_fabric_publishes_total 1000\n"
            "# TYPE kith_gateway_compose_window_ns_total counter\n"
            "kith_gateway_compose_window_ns_total 300000\n"
            "# TYPE kith_gateway_compose_prior_ns_total counter\n"
            "kith_gateway_compose_prior_ns_total 400000\n"
            "# TYPE kith_gateway_compose_lock_wait_ns_total counter\n"
            "kith_gateway_compose_lock_wait_ns_total 500000\n"
            "# TYPE kith_gateway_compose_scan_ns_total counter\n"
            "kith_gateway_compose_scan_ns_total 6100000000\n"
            "# TYPE kith_gateway_compose_sort_ns_total counter\n"
            "kith_gateway_compose_sort_ns_total 600000\n"
            "# TYPE kith_gateway_compose_select_ns_total counter\n"
            "kith_gateway_compose_select_ns_total 700000\n"
            "# TYPE kith_gateway_compose_skips_total counter\n"
            "kith_gateway_compose_skips_total 42\n"
            "# TYPE kith_gateway_compose_deferrals_total counter\n"
            "kith_gateway_compose_deferrals_total 7\n"
            "# TYPE kith_gateway_view_locate_failures_total counter\n"
            "kith_gateway_view_locate_failures_total 4\n"
            "# TYPE kith_net_write_ns_total counter\n"
            "kith_net_write_ns_total 250000000\n"
            "# TYPE kith_net_write_deferrals_total counter\n"
            "kith_net_write_deferrals_total 3\n"
        )
        c = _parse_phase_counters(text)
        assert c.refresh_ns == 1_500_000_000
        assert c.compose_ns == 6_200_000_000
        assert c.deliver_ns == 900_000_000
        assert c.dispatches == 50000
        assert c.publishes == 1000
        assert c.compose_window_ns == 300_000
        assert c.compose_prior_ns == 400_000
        assert c.compose_lock_wait_ns == 500_000
        assert c.compose_scan_ns == 6_100_000_000
        assert c.compose_sort_ns == 600_000
        assert c.compose_select_ns == 700_000
        assert c.compose_skips == 42
        assert c.compose_deferrals == 7
        assert c.view_locate_failures == 4
        assert c.write_ns == 250_000_000
        assert c.write_deferrals == 3

    def test_parse_absent_series_parse_as_zero(self) -> None:
        """A scrape without the counters (stale build) parses all-zero."""
        c = _parse_phase_counters("# TYPE other_total counter\nother_total 5\n")
        assert c == PhaseCounters(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)

    def test_delta_subtracts_per_instance_and_clamps(self) -> None:
        """The movement-window delta is end - start per instance, clamped at 0."""
        start = [
            PhaseCounters(100, 200, 50, 10, 5, 1, 2, 3, 4, 5, 6, 7, 8, 17, 9, 11),
            PhaseCounters(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
        ]
        end = [
            PhaseCounters(
                1_600, 6_400, 950, 50_010, 1_005, 21, 42, 63, 84, 15, 36, 19, 28, 25, 37, 14
            ),
            PhaseCounters(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
        ]
        delta = _phase_counters_delta(start, end)
        assert delta[0] == PhaseCounters(
            1_500, 6_200, 900, 50_000, 1_000, 20, 40, 60, 80, 10, 30, 12, 20, 8, 28, 3
        )
        assert delta[1] == PhaseCounters(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
        # A monotonic reset (end < start) clamps to zero rather than wrapping.
        assert _phase_counters_delta(
            [PhaseCounters(1_000, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)],
            [PhaseCounters(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)],
        ) == [PhaseCounters(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)]

    def test_parse_worker_task_totals(self) -> None:
        """The pool's task-total series parse into their PhaseCounters fields."""
        text = (
            "# TYPE kith_worker_tasks_submitted_total counter\n"
            "kith_worker_tasks_submitted_total 17000\n"
            "# TYPE kith_worker_tasks_completed_total counter\n"
            "kith_worker_tasks_completed_total 16998\n"
        )
        c = _parse_phase_counters(text)
        assert c.worker_tasks_submitted == 17_000
        assert c.worker_tasks_completed == 16_998
        # Absent series (stale build) parse as zero like every other counter.
        absent = _parse_phase_counters("# TYPE other_total counter\nother_total 1\n")
        assert absent.worker_tasks_submitted == 0
        assert absent.worker_tasks_completed == 0

    def test_counter_samples_render_in_flight_and_drop_fraction(self) -> None:
        """Sample rows carry raw readings, in-flight depth, and drop-frac."""
        first = PhaseCounters(
            0,
            0,
            0,
            1_000,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            worker_tasks_submitted=1_000,
            worker_tasks_completed=0,
            dispatch_dropped=0,
        )
        second = PhaseCounters(
            0,
            0,
            0,
            2_000,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            worker_tasks_submitted=2_000,
            worker_tasks_completed=1_010,
            dispatch_dropped=590,
        )
        samples = [
            CounterSample(t_rel_ms=250, counters=(first,)),
            CounterSample(t_rel_ms=500, counters=(second,)),
        ]
        text = _render_counter_samples(samples)
        assert "Movement-window counter samples" in text
        # The first row: in-flight 1000 (all queued, none applied yet);
        # its drop-frac is measured against the window-start scrape baseline.
        assert "| 0 | 250 | 1000 | 0 | 1000 | 0 | 1000 | 0.00 |" in text
        # The second row: in-flight 990, interval drop-frac 590/1000.
        assert "| 0 | 500 | 2000 | 590 | 2000 | 1010 | 990 | 0.59 |" in text

    def test_counter_samples_render_empty_when_absent(self) -> None:
        """No samples (sampler absent or no window) render nothing."""
        assert _render_counter_samples(()) == ""

    def test_spread_probe_indices_span_the_body_evenly(self) -> None:
        """The spread set covers the post-head positions evenly, disjoint."""
        indices = _spread_probe_indices(1000, 32, 32)
        assert len(indices) == 32
        assert len(set(indices)) == 32
        assert indices[0] == 32
        assert indices[-1] == 999
        assert all(i >= 32 for i in indices)
        # Deterministic and evenly spaced: consecutive gaps are near-equal.
        gaps = {b - a for a, b in itertools.pairwise(indices)}
        assert max(gaps) - min(gaps) <= 1

    def test_spread_probe_indices_edge_cases(self) -> None:
        """Zero size or a too-small body degrade sanely."""
        assert _spread_probe_indices(1000, 32, 0) == ()
        assert _spread_probe_indices(40, 32, 32) == (32, 33, 34, 35, 36, 37, 38, 39)
        assert _spread_probe_indices(40, 40, 8) == ()

    def test_per_instance_ranks_count_within_each_instance(self) -> None:
        """Ranks renumber per instance, not globally."""
        records = [
            _ClientRecord(
                "load-0", 100, True, own_actor_id=1, instance_index=0, ready_ns=2, started_ns=1
            ),
            _ClientRecord(
                "load-1", 101, True, own_actor_id=2, instance_index=1, ready_ns=2, started_ns=1
            ),
            _ClientRecord(
                "load-2", 102, True, own_actor_id=3, instance_index=0, ready_ns=2, started_ns=1
            ),
            # An unbooted record never submitted, so it ranks nothing.
            _ClientRecord("load-3", 103, False, instance_index=1),
        ]
        ranks = _per_instance_ranks(records)
        assert ranks == {"load-0": 0, "load-1": 0, "load-2": 1}

    def test_render_cohorts_rows_carry_rank_and_cohort_mm(self) -> None:
        """The cohort section renders head/spread summaries and rank rows."""
        head = CohortDepth(
            name="head",
            sampled=2,
            submitted=88,
            observed=56,
            move_missing=0.25,
            rows=(
                CohortRow("load-0", 0, 0, 44, 28),
                CohortRow("load-1", 1, 0, 44, 28),
            ),
        )
        spread = CohortDepth(
            name="spread",
            sampled=2,
            submitted=88,
            observed=26,
            move_missing=0.75,
            rows=(
                CohortRow("load-500", 0, 250, 44, 13),
                CohortRow("load-999", 1, 499, 44, 13),
            ),
        )
        metrics = self._metrics(())
        metrics = replace(metrics, head_cohort=head, spread_cohort=spread, selected_head_count=2)
        report = GateReport(
            profile=DISTRIBUTED_2000,
            metrics=metrics,
            thresholds=DISTRIBUTED_THRESHOLDS,
            started_at="2026-08-28T00:00:00+00:00",
            duration_s=6.0,
        )
        rendered = report.to_markdown()
        assert "Sample-position cohorts" in rendered
        assert "selected within the head sample: 2/32 (6.25%)" in rendered
        assert "### Cohort 'head' (n=2)" in rendered
        assert "mm 25.00%" in rendered
        assert "### Cohort 'spread' (n=2)" in rendered
        assert "mm 75.00%" in rendered
        assert "| load-500 | 0 | 250 | 44 | 13 |" in rendered
        assert "| load-999 | 1 | 499 | 44 | 13 |" in rendered

    def test_render_cohorts_absent_without_probe(self) -> None:
        """No cohort section when the spread probe is disabled."""
        metrics = self._metrics(())
        report = GateReport(
            profile=DISTRIBUTED_2000,
            metrics=metrics,
            thresholds=DISTRIBUTED_THRESHOLDS,
            started_at="2026-08-28T00:00:00+00:00",
            duration_s=6.0,
        )
        rendered = report.to_markdown()
        assert "Sample-position cohorts" not in rendered

    def test_render_names_the_dominant_phase_per_instance(self) -> None:
        """The dominant columns name the largest phase and sub-phase."""
        metrics = self._metrics(
            (
                PhaseCounters(
                    1_500_000_000,
                    6_200_000_000,
                    900_000_000,
                    50_000,
                    1_000,
                    300_000,
                    400_000,
                    500_000,
                    6_100_000_000,
                    600_000,
                    700_000,
                    480,
                    5,
                    0,
                    250_000_000,
                    12,
                ),
            )
        )
        report = GateReport(
            profile=DISTRIBUTED_2000,
            metrics=metrics,
            thresholds=DISTRIBUTED_THRESHOLDS,
            started_at="2026-08-21T00:00:00+00:00",
            duration_s=6.0,
        )
        rendered = report.to_markdown()
        assert "Reactor-path phase counters" in rendered
        assert "| 0 | 1500 | 6200 | 900 | 250 | 12 | 50000 | 1000 | compose |" in rendered
        assert "Compose sub-phase split" in rendered
        assert "| 0 | 0 | 0 | 0 | 6100 | 0 | 0 | 480 | 5 | 0 | scan |" in rendered

    def test_render_shows_dash_on_an_all_zero_row(self) -> None:
        """An all-zero instance (a failed scrape) renders '-' as dominant."""
        metrics = self._metrics((PhaseCounters(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),))
        report = GateReport(
            profile=DISTRIBUTED_2000,
            metrics=metrics,
            thresholds=DISTRIBUTED_THRESHOLDS,
            started_at="2026-08-21T00:00:00+00:00",
            duration_s=6.0,
        )
        rendered = report.to_markdown()
        assert "| 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | - |" in rendered

    def test_empty_counters_omits_the_block(self) -> None:
        """No control plane means no phase-counter block in the report."""
        report = GateReport(
            profile=DISTRIBUTED_2000,
            metrics=self._metrics(()),
            thresholds=DISTRIBUTED_THRESHOLDS,
            started_at="2026-08-21T00:00:00+00:00",
            duration_s=6.0,
        )
        assert "Reactor-path phase counters" not in report.to_markdown()

    def test_counters_round_trip_through_json(self) -> None:
        """The per-instance counters serialize to JSON and parse back."""
        metrics = self._metrics(
            (
                PhaseCounters(
                    1_500_000_000,
                    6_200_000_000,
                    900_000_000,
                    50_000,
                    1_000,
                    300_000,
                    400_000,
                    500_000,
                    6_100_000_000,
                    600_000,
                    700_000,
                    480,
                    5,
                    2,
                    250_000_000,
                    12,
                ),
                PhaseCounters(
                    1_490_000_000,
                    6_180_000_000,
                    880_000_000,
                    49_800,
                    990,
                    290_000,
                    390_000,
                    490_000,
                    6_080_000_000,
                    590_000,
                    690_000,
                    470,
                    4,
                    0,
                    240_000_000,
                    9,
                ),
            )
        )
        report = GateReport(
            profile=DISTRIBUTED_2000,
            metrics=metrics,
            thresholds=DISTRIBUTED_THRESHOLDS,
            started_at="2026-08-21T00:00:00+00:00",
            duration_s=6.0,
        )
        payload = json.loads(report.to_json())
        pcs = payload["metrics"]["phase_counters"]
        assert len(pcs) == 2
        assert pcs[0]["compose_ns"] == 6_200_000_000
        assert pcs[0]["compose_scan_ns"] == 6_100_000_000
        assert pcs[0]["compose_deferrals"] == 5
        assert pcs[0]["view_locate_failures"] == 2
        assert pcs[0]["write_deferrals"] == 12
        assert pcs[1]["publishes"] == 990
        assert pcs[1]["compose_lock_wait_ns"] == 490_000
        assert pcs[1]["write_ns"] == 240_000_000
        assert pcs[1]["write_deferrals"] == 9


class TestEchoDeltaAndTotals:
    """The delivery/view totals scrape and the echo-delta histogram.

    Pure value-object paths: the totals series parse and render, and the
    histogram's bucketing, implied-missing sum, and coverage flags, with
    no C build or running server.
    """

    def test_parse_reads_the_totals_series(self) -> None:
        """The nine delivery/view totals series parse from the scrape."""
        text = (
            "kith_gateway_delivery_frames_enqueued_total 100\n"
            "kith_gateway_delivery_dropped_total 4\n"
            "kith_gateway_delivery_event_frames_enqueued_total 12\n"
            "kith_gateway_delivery_suppressed_total 5000\n"
            "kith_gateway_view_visits_total 90\n"
            "kith_gateway_view_candidate_total 45000\n"
            "kith_gateway_view_selected_total 20000\n"
            "kith_gateway_view_candidate_high_watermark 512\n"
            "kith_gateway_view_selected_high_watermark 400\n"
        )
        c = _parse_phase_counters(text)
        assert c.delivery_frames_enqueued == 100
        assert c.delivery_dropped == 4
        assert c.delivery_event_frames_enqueued == 12
        assert c.delivery_suppressed == 5000
        assert c.view_visits == 90
        assert c.view_candidates == 45000
        assert c.view_selected == 20000
        assert c.view_candidate_high_watermark == 512
        assert c.view_selected_high_watermark == 400

    def test_delta_subtracts_the_totals_counters(self) -> None:
        """The totals counters subtract per instance; the peaks do not."""
        start = [
            PhaseCounters(
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                delivery_frames_enqueued=10,
                view_visits=5,
            )
        ]
        end = [
            PhaseCounters(
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                delivery_frames_enqueued=18,
                view_visits=9,
            )
        ]
        delta = _phase_counters_delta(start, end)
        assert delta[0].delivery_frames_enqueued == 8
        assert delta[0].view_visits == 4

    @staticmethod
    def _series(
        *frames: tuple[int, int], window: tuple[int, int] = (0, 1_000_000_000)
    ) -> ClientSeries:
        return ClientSeries(
            name="c",
            instance_index=0,
            window_start_ns=window[0],
            window_end_ns=window[1],
            frames=tuple(SeriesFrame(ts_ns=ts, input_tick=tick) for ts, tick in frames),
        )

    def test_histogram_buckets_deltas_and_sums_missing(self) -> None:
        """delta 1 buckets clean echoes; deltas above 1 bucket and sum losses."""
        h = _echo_delta_histogram(
            [
                self._series((0, 10), (50_000_000, 11), (100_000_000, 13), (150_000_000, 33)),
                self._series((0, 5), (10_000_000, 5), (20_000_000, 4)),
            ]
        )
        assert h.clients == 2
        assert h.frames == 7
        assert h.delta1 == 1  # 10 -> 11
        assert h.delta2 == 1  # 11 -> 13
        assert h.delta6_20 == 1  # 13 -> 33 (delta 20)
        assert h.regressed == 2  # 5 -> 5, 5 -> 4
        assert h.missing_implied == 20  # 1 (11->13) + 19 (13->33)

    def test_histogram_flags_truncated_history_coverage(self) -> None:
        """A client whose frames span a fraction of the window is truncated."""
        covered = self._series((0, 1), (100_000_000, 2), (200_000_000, 3), (1_000_000_000, 4))
        truncated = self._series(
            (920_000_000, 1), (940_000_000, 2), (960_000_000, 3), (990_000_000, 4)
        )
        h = _echo_delta_histogram([covered, truncated])
        assert h.truncated_clients == 1
        assert h.coverage_min < 0.1

    def test_histogram_empty_series_is_the_absent_signal(self) -> None:
        """No series yields an all-zero histogram with coverage 0.0."""
        h = _echo_delta_histogram([])
        assert h.clients == 0 and h.frames == 0 and h.coverage_min == 0.0

    def test_render_echo_delta_reconciles_against_move_missing(self) -> None:
        """The render names the implied missing count and the coverage."""
        h = _echo_delta_histogram(
            [self._series((0, 10), (50_000_000, 12), window=(0, 100_000_000))]
        )
        text = _render_echo_delta(h, 0.25, 40)
        assert "missing" in text
        assert "10" in text  # 0.25 * 40 implied by the ratio
        assert "0.500" in text  # min coverage: 50 ms span over 100 ms window


class TestProcSamplerParsing:
    """The /proc sampler's parsing is a pure path over synthetic texts."""

    STAT = (
        "1234 (python3.14t) R 1 2345 2345 0 -1 4194560 0 0 0 0 77 33 "
        "0 0 20 0 1 0 12345 67890 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 "
        "-1 0 0 0 0 0 0 0 0 0 0 0 0 0\n"
    )
    STATUS = (
        "Name:\tpython3.14t\n"
        "Threads:\t2\n"
        "voluntary_ctxt_switches   :   500\n"
        "nonvoluntary_ctxt_switches:\t12\n"
    )

    def test_thread_sample_parses_cpu_and_switches(self) -> None:
        """utime and stime parse separately; the switch counters parse padded."""
        sample = _thread_sample_from_texts(1234, self.STAT, self.STATUS)
        assert sample is not None
        assert sample.tid == 1234
        assert sample.comm == "python3.14t"
        assert sample.utime_ticks == 77
        assert sample.stime_ticks == 33
        assert sample.cpu_ticks == 110
        assert sample.nvcsw == 500
        assert sample.nivcsw == 12

    def test_thread_sample_rejects_malformed_texts(self) -> None:
        """A stat without a comm paren or a status without counters is None."""
        assert _thread_sample_from_texts(1, "garbage", self.STATUS) is None
        assert _thread_sample_from_texts(1, self.STAT, "Name:\tx\n") is None

    def test_parse_stat_survives_parens_inside_comm(self) -> None:
        """A comm containing spaces and parentheses splits at the last ')') ."""
        parsed = _parse_stat("9 (weird (name) here) S 1 1 1 0 -1 0 0 0 0 0 5 6 0 0\n")
        assert parsed is not None
        comm, tail = parsed
        assert comm == "weird (name) here"
        assert tail[0] == "S"
        assert int(tail[11]) == 5
        assert int(tail[12]) == 6


class TestProcSamplerLifecycle:
    """The sampler collects monotonic samples from the live process."""

    def test_samples_self_until_stopped(self) -> None:
        """A short self-sample run produces ordered rows and stop() joins."""
        sampler = ProcSampler.start([SamplerTarget("harness", os.getpid())], interval_s=0.05)
        deadline = time.monotonic() + 5.0
        while sampler.samples_collected() < 2 and time.monotonic() < deadline:
            time.sleep(0.01)
        assert sampler.samples_collected() >= 2
        trace = sampler.stop()
        assert trace.clk_tck > 0
        assert len(trace.samples) >= 2
        times = [s.t_ns for s in trace.samples]
        assert times == sorted(times)
        threads = trace.samples[0].targets[0].threads
        assert any(t.tid == os.getpid() for t in threads)
        joined = not sampler._thread.is_alive()
        assert joined


class TestGapLedgerClustering:
    """The ledger's clustering and spread signatures over synthetic streams."""

    MS = 1_000_000
    WINDOW_END = 5_000 * MS

    @staticmethod
    def _series(
        name: str,
        instance_index: int,
        holes: list[tuple[int, int]],
        jitter_ms: int = 0,
    ) -> ClientSeries:
        """Build one client's stream: 50 ms cadence, ticks 1..100, with holes.

        Each hole ``(at_ms, skip_to_ms)`` replaces the frames strictly
        between the two bounds with a post-gap catch-up burst: the frames
        for the skipped ticks arrive back-to-back right after the hole
        closes, mirroring the queued-drain shape a punctuality stall
        produces at zero move_missing.
        """
        frames: list[SeriesFrame] = []
        tick = 1
        t_ms = 0.0
        pending_holes = list(holes)
        while t_ms < 5000.0 and tick <= 100:
            if pending_holes and t_ms >= pending_holes[0][0]:
                resume_ms = pending_holes.pop(0)[1]
                skipped = list(range(tick, tick + 5))
                tick += 5
                base = resume_ms + jitter_ms
                for offset, skipped_tick in enumerate(skipped):
                    frames.append(
                        SeriesFrame(
                            ts_ns=int((base + offset * 0.5) * TestGapLedgerClustering.MS),
                            input_tick=skipped_tick,
                        )
                    )
                t_ms = base + 50.0
                continue
            frames.append(
                SeriesFrame(ts_ns=int(t_ms * TestGapLedgerClustering.MS), input_tick=tick)
            )
            tick += 1
            t_ms += 50.0
        return ClientSeries(
            name=name,
            instance_index=instance_index,
            window_start_ns=0,
            window_end_ns=TestGapLedgerClustering.WINDOW_END,
            frames=tuple(frames),
        )

    def test_instance_wide_hole_clusters_with_aligned_signatures(self) -> None:
        """Four synchronized holes form one cluster; a quiet instance none."""
        holes = [(2000, 2226)]
        series = tuple(
            self._series(f"a{i}", 0, holes, jitter_ms=jitter)
            for i, jitter in enumerate((0, 1, 2, 2))
        ) + tuple(self._series(f"b{i}", 1, holes=[], jitter_ms=0) for i in range(2))
        clusters = cluster_gaps(series, gap_threshold_ms=200.0)
        assert len(clusters) == 1
        cluster = clusters[0]
        assert cluster.instance_index == 0
        assert cluster.participants == 4
        assert cluster.sampled_on_instance == 4
        assert cluster.onset_spread_ms <= 2
        assert cluster.aligned_onset
        assert cluster.ranges_agree
        assert cluster.last_ticks == (40,)
        assert cluster.first_ticks == (41,)
        assert cluster.resume_spread_ms is not None and cluster.resume_spread_ms <= 2
        assert cluster.catchup_median >= 3.0

    def test_tick_spread_baseline_excludes_cluster_windows(self) -> None:
        """Normal per-tick spreads are small; recovery frames stay out."""
        series = tuple(
            self._series(f"a{i}", 0, holes=[(2000, 2226)], jitter_ms=i) for i in range(4)
        )
        clusters = cluster_gaps(series)
        spreads = tick_arrival_spreads(series, clusters)
        assert len(spreads) == 1
        entry = spreads[0]
        assert entry.ticks > 50
        assert entry.median_ms is not None and entry.median_ms <= 4.0
        assert entry.p95_ms is not None and entry.p95_ms <= 8.0

    @staticmethod
    def _late_opening_series(name: str, first_frame_ms: float) -> ClientSeries:
        """Build one client whose first own-actor frame arrives late."""
        frames = [SeriesFrame(ts_ns=int(first_frame_ms * TestGapLedgerClustering.MS), input_tick=1)]
        tick = 2
        t_ms = first_frame_ms + 50.0
        while t_ms < 5000.0 and tick <= 100:
            frames.append(
                SeriesFrame(ts_ns=int(t_ms * TestGapLedgerClustering.MS), input_tick=tick)
            )
            tick += 1
            t_ms += 50.0
        return ClientSeries(
            name=name,
            instance_index=0,
            window_start_ns=0,
            window_end_ns=TestGapLedgerClustering.WINDOW_END,
            frames=tuple(frames),
        )

    def test_late_first_frame_is_a_leading_gap(self) -> None:
        """A first frame past the threshold anchors a leading-edge cluster."""
        series = (self._late_opening_series("a0", first_frame_ms=250.0),)
        clusters = cluster_gaps(series, gap_threshold_ms=200.0)
        assert len(clusters) == 1
        cluster = clusters[0]
        assert cluster.start_ns == 0
        assert cluster.duration_ms == 250
        assert cluster.participants == 1
        assert cluster.last_ticks == (-1,)
        assert cluster.aligned_onset

        breakdown = _flicker_breakdown_from_series(series)
        assert breakdown.total_gaps == 1
        sample = breakdown.per_client[0]
        assert sample.gap_ms == (250,)
        assert sample.gap_start_ms == (0,)

    def test_prompt_first_frame_produces_no_leading_gap(self) -> None:
        """A first frame inside the threshold opens no leading cluster."""
        series = (self._late_opening_series("a0", first_frame_ms=149.0),)
        assert cluster_gaps(series, gap_threshold_ms=200.0) == ()
        assert _flicker_breakdown_from_series(series).total_gaps == 0

    def test_frameless_series_yields_no_leading_gap(self) -> None:
        """A client that never received its own actor is not a gap subject."""
        series = (
            ClientSeries(
                name="a0",
                instance_index=0,
                window_start_ns=0,
                window_end_ns=TestGapLedgerClustering.WINDOW_END,
                frames=(),
            ),
        )
        assert cluster_gaps(series, gap_threshold_ms=200.0) == ()
        assert _flicker_breakdown_from_series(series).total_gaps == 0


class TestGapLedgerAttribution:
    """The /proc attribution verdicts over synthetic traces."""

    @staticmethod
    def _trace(in_window: dict[str, list[tuple[int, int, int, int]]]) -> ProcTrace:
        """Build a 2 s trace at 100 ms with per-interval counter deltas.

        ``in_window`` optionally maps ``"deltas"`` to the ``(utime,
        stime, nvcsw, nivcsw)`` steps applied to the tick row for
        samples inside the cluster window (1000-1226 ms); every other
        interval and the main/worker rows use the quiet baseline (cpu 1
        tick, nvcsw 4, nivcsw 0). The worker adds +2 utime ticks per
        interval so the total scope never mirrors the tick row. The row
        layout mirrors a server subprocess: the tick/reactor thread is
        the one named ``kith-server-run``, and tid == pid is the parked
        shutdown supervisor.
        """
        pid = 1000
        window = (1000, 1226)
        steps = in_window.get("deltas")
        samples: list[ProcSample] = []
        tick_utime = tick_stime = tick_nvcsw = tick_nivcsw = 0
        worker_utime = worker_stime = worker_nvcsw = worker_nivcsw = 0
        for i in range(21):
            t_ms = i * 100
            step: tuple[int, int, int, int] = (1, 0, 4, 0)
            if steps is not None and window[0] <= t_ms < window[1]:
                index = min(i - window[0] // 100, len(steps) - 1)
                step = steps[index]
            tick_utime += step[0]
            tick_stime += step[1]
            tick_nvcsw += step[2]
            tick_nivcsw += step[3]
            worker_utime += step[0] + 2
            worker_stime += step[1]
            worker_nvcsw += step[2] + 1
            worker_nivcsw += step[3]
            threads = (
                # The main thread holds the quiet baseline (cpu 1 tick,
                # nvcsw 4, nivcsw 0) per interval.
                ThreadSample(
                    tid=pid,
                    comm="python3.14t",
                    utime_ticks=i + 1,
                    stime_ticks=0,
                    nvcsw=4 * (i + 1),
                    nivcsw=0,
                ),
                ThreadSample(
                    tid=pid + 1,
                    comm="python3.14t",
                    utime_ticks=worker_utime,
                    stime_ticks=worker_stime,
                    nvcsw=worker_nvcsw,
                    nivcsw=worker_nivcsw,
                ),
                ThreadSample(
                    tid=pid + 2,
                    comm="kith-server-run",
                    utime_ticks=tick_utime,
                    stime_ticks=tick_stime,
                    nvcsw=tick_nvcsw,
                    nivcsw=tick_nivcsw,
                ),
            )
            samples.append(
                ProcSample(
                    t_ns=t_ms * 1_000_000,
                    targets=(TargetSample(target="instance-0", pid=pid, threads=threads),),
                )
            )
        return ProcTrace(interval_s=0.1, clk_tck=100, samples=tuple(samples))

    @staticmethod
    def _cluster() -> GapClusterRecord:
        """One 226 ms cluster at t=1000 ms with two participants."""
        return GapClusterRecord(
            instance_index=0,
            start_ns=1000 * 1_000_000,
            end_ns=1226 * 1_000_000,
            duration_ms=226,
            participants=2,
            sampled_on_instance=2,
            onset_spread_ms=1,
            aligned_onset=True,
            last_ticks=(39,),
            first_ticks=(40,),
            ranges_agree=True,
            resume_spread_ms=1,
            catchup_median=4.0,
        )

    def test_preempted_when_nonvoluntary_switches_elevate(self) -> None:
        """Rising nivcsw inside the window reads preempted on the tick thread."""
        trace = self._trace({"deltas": [(0, 0, 4, 15), (0, 0, 4, 15), (0, 0, 4, 10)]})
        rows = attribute_clusters(trace, [self._cluster()])
        by_scope = {row.scope: row for row in rows[0].rows}
        assert by_scope["tick"].verdict == "preempted"

    def test_blocked_when_voluntary_switches_elevate(self) -> None:
        """Rising nvcsw with flat nivcsw reads blocked on the tick thread."""
        trace = self._trace({"deltas": [(0, 0, 20, 0), (0, 0, 20, 0), (0, 0, 16, 0)]})
        rows = attribute_clusters(trace, [self._cluster()])
        by_scope = {row.scope: row for row in rows[0].rows}
        assert by_scope["tick"].verdict == "blocked"

    def test_busy_when_cpu_covers_half_the_window(self) -> None:
        """Tick-thread CPU over half the 226 ms window reads cpu-busy."""
        trace = self._trace({"deltas": [(8, 0, 4, 0), (8, 0, 4, 0), (8, 0, 4, 0)]})
        rows = attribute_clusters(trace, [self._cluster()])
        by_scope = {row.scope: row for row in rows[0].rows}
        assert by_scope["tick"].verdict == "cpu-busy"

    def test_stime_split_rides_the_attribution_row(self) -> None:
        """User and system cpu split the total without changing the verdict.

        Verdicts read total cpu only; the split is diagnostic surface for
        separating user-space compute from kernel time (allocator,
        reclaim, futex paths) inside the same window.
        """
        trace = self._trace({"deltas": [(5, 3, 4, 0), (5, 3, 4, 0), (5, 3, 4, 0)]})
        rows = attribute_clusters(trace, [self._cluster()])
        by_scope = {row.scope: row for row in rows[0].rows}
        tick = by_scope["tick"]
        assert tick.verdict == "cpu-busy"
        assert tick.utime_ms > 0
        assert tick.stime_ms > 0
        assert tick.cpu_ms == pytest.approx(tick.utime_ms + tick.stime_ms)

    def test_quiet_when_counters_stay_in_band(self) -> None:
        """Baseline-level activity inside the window reads quiet."""
        trace = self._trace({"deltas": [(0, 0, 4, 0), (0, 0, 5, 0), (0, 0, 4, 0)]})
        rows = attribute_clusters(trace, [self._cluster()])
        by_scope = {row.scope: row for row in rows[0].rows}
        assert by_scope["tick"].verdict == "quiet"

    def test_worker_activity_does_not_mask_tick_verdict(self) -> None:
        """A busy-enough total row leaves the tick row's verdict alone."""
        trace = self._trace({"deltas": [(0, 0, 4, 0), (0, 0, 4, 0), (0, 0, 4, 0)]})
        cluster = self._cluster()
        rows = attribute_clusters(trace, [cluster])
        by_scope = {row.scope: row for row in rows[0].rows}
        assert by_scope["tick"].verdict == "quiet"
        assert by_scope["total"].cpu_ms >= by_scope["tick"].cpu_ms * 2

    def test_main_thread_reads_under_its_own_scope(self) -> None:
        """The tid == pid supervisor stays quiet while the tick thread starves.

        A worker-wave starvation signature (elevated nonvoluntary switches)
        must land on the comm-selected tick row; the parked main thread
        keeps its baseline counters under the 'main' scope instead of
        masquerading as the reactor.
        """
        trace = self._trace({"deltas": [(0, 0, 4, 15), (0, 0, 4, 15), (0, 0, 4, 10)]})
        rows = attribute_clusters(trace, [self._cluster()])
        by_scope = {row.scope: row for row in rows[0].rows}
        assert "reactor" not in by_scope
        assert by_scope["tick"].verdict == "preempted"
        assert by_scope["main"].verdict == "quiet"
        assert by_scope["tick"].nivcsw_delta > by_scope["main"].nivcsw_delta

    def test_trace_without_the_tick_comm_has_no_tick_scope(self) -> None:
        """A target lacking the named thread contributes total + main only."""
        pid = 2000
        samples = [
            ProcSample(
                t_ns=t_ms * 1_000_000,
                targets=(
                    TargetSample(
                        target="instance-0",
                        pid=pid,
                        threads=(
                            ThreadSample(
                                tid=pid,
                                comm="python3.14t",
                                utime_ticks=1,
                                stime_ticks=0,
                                nvcsw=4,
                                nivcsw=0,
                            ),
                        ),
                    ),
                ),
            )
            for t_ms in range(900, 1400, 100)
        ]
        trace = ProcTrace(interval_s=0.1, clk_tck=100, samples=tuple(samples))
        rows = attribute_clusters(trace, [self._cluster()])
        assert {row.scope for row in rows[0].rows} == {"total", "main"}


class TestFlickerLedgerReport:
    """The ledger rides the gate report JSON and markdown surfaces."""

    @staticmethod
    def _metrics(ledger: FlickerLedger) -> GateMetrics:
        return GateMetrics(
            actor_count=1000,
            sampled_clients=32,
            session_ok_ratio=1.0,
            selected_clients_ratio=1.0,
            move_missing_ratio=0.0,
            bootstrap_ms_p95=400,
            continuity_flicker=True,
            flicker_count=8,
            flicker_ledger=ledger,
        )

    def test_json_roundtrip_carries_ledger(self) -> None:
        """asdict serializes the nested ledger; clusters and rows survive."""
        ledger = build_flicker_ledger(
            TestGapLedgerAttribution._trace({}),
            (TestGapLedgerClustering._series("a0", 0, holes=[(2000, 2226)]),),
        )
        report = GateReport(
            profile=DISTRIBUTED_2000,
            metrics=self._metrics(ledger),
            thresholds=DISTRIBUTED_THRESHOLDS,
            started_at="2026-08-21T00:00:00+00:00",
            duration_s=6.0,
        )
        payload = json.loads(report.to_json())
        attached = payload["metrics"]["flicker_ledger"]
        assert len(attached["clusters"]) == 1
        assert attached["clusters"][0]["participants"] == 1
        assert isinstance(attached["tick_spreads"], list)

    def test_markdown_renders_ledger_and_attribution(self) -> None:
        """Both diagnostic sections appear in the markdown report."""
        ledger = build_flicker_ledger(
            TestGapLedgerAttribution._trace({}),
            (TestGapLedgerClustering._series("a0", 0, holes=[(2000, 2226)]),),
        )
        report = GateReport(
            profile=DISTRIBUTED_2000,
            metrics=self._metrics(ledger),
            thresholds=DISTRIBUTED_THRESHOLDS,
            started_at="2026-08-21T00:00:00+00:00",
            duration_s=6.0,
        )
        text = report.to_markdown()
        assert "## Flicker-gap ledger" in text
        assert "## Gap-window /proc attribution" in text
        assert "| cpu ms | utime ms | stime ms |" in text
        assert "unbaselined" in text

    def test_proc_trace_persistence_roundtrips(self, tmp_path: Path) -> None:
        """The raw trace and arrival series survive the JSON file.

        The report's tables summarize; this file is the lossless record
        any thread-level join reads instead of rerunning the gate.
        """
        trace = TestGapLedgerAttribution._trace(
            {"deltas": [(5, 3, 4, 0), (5, 3, 4, 0), (5, 3, 4, 0)]}
        )
        series = (TestGapLedgerClustering._series("a0", 0, holes=[(2000, 2226)]),)
        path = tmp_path / "trace.json"
        _write_proc_trace(str(path), trace, series)

        payload = json.loads(path.read_text(encoding="utf-8"))
        assert payload["interval_s"] == 0.1
        assert payload["clk_tck"] == 100
        assert len(payload["samples"]) == 21
        target = payload["samples"][0]["targets"][0]
        assert target["target"] == "instance-0"
        assert target["pid"] == 1000
        assert target["threads"][0]["tid"] == 1000
        assert target["threads"][0]["stime_ticks"] >= 0
        entry = payload["series"][0]
        assert entry["name"] == "a0"
        assert entry["instance_index"] == 0
        assert any(frame["input_tick"] == 41 for frame in entry["frames"])


def _format_failure(report: GateReport) -> str:
    m = report.metrics
    t = report.thresholds
    move_missing_threshold = (
        "(reported, not gated)" if t.move_missing_max is None else f"(< {t.move_missing_max:.2%})"
    )
    return (
        f"smoke profile failed:\n"
        f"  session_ok={m.session_ok_ratio:.2%} (>= {t.session_ok_min:.2%})\n"
        f"  selected_clients={m.selected_clients_ratio:.2%} "
        f"(>= {t.selected_clients_min:.2%}, min_distinct={t.min_distinct_others})\n"
        f"  move_missing={m.move_missing_ratio:.2%} {move_missing_threshold}\n"
        f"  bootstrap_ms_p95={m.bootstrap_ms_p95} (< {t.bootstrap_ms_p95_max})\n"
        f"  continuity_flicker={m.continuity_flicker} "
        f"(forbidden={t.continuity_flicker_forbidden})\n"
        f"  publish_rate_skew={m.publish_rate_skew:.2f}x "
        f"(<= {t.publish_rate_skew_max:.2f}x)\n"
        f"  duration={report.duration_s:.3f}s\n"
        f"{report.to_markdown()}"
    )


class TestAnchorProblems:
    """Movement-anchor validation for the dual-driver parent.

    The realized inter-child offset must track the configured stagger
    within tolerance; shared absolute jitter cancels in the difference,
    so it passes while a collapsed stagger fails. Pure-logic test.
    """

    @staticmethod
    def _pair(start_a: int, start_b: int, *, end_b: int | None = None) -> list[DriverResult]:
        end_a = start_a + 100_000_000
        return [
            _driver_result(0, movement_start_ns=start_a, movement_end_ns=end_a),
            _driver_result(
                1,
                movement_start_ns=start_b,
                movement_end_ns=start_b + 100_000_000 if end_b is None else end_b,
            ),
        ]

    def test_healthy_stagger_passes(self) -> None:
        """Realized offset within tolerance of the stagger raises nothing."""
        base = 1_000_000_000
        assert not _anchor_problems(self._pair(base, base + 600_000_000), [base - 5, base], 600)

    def test_common_absolute_drift_passes(self) -> None:
        """Both children drifting together keeps the stagger intact."""
        base = 1_000_000_000
        stagger_ns = 600_000_000
        drift = 500_000_000
        anchors = [base, base + stagger_ns]
        assert not _anchor_problems(
            self._pair(anchors[0] + drift, anchors[1] + drift), anchors, 600
        )

    def test_collapsed_stagger_fails_relative_check(self) -> None:
        """Two windows opening together miss the 600 ms stagger outright."""
        start = 1_000_000_000
        problems = _anchor_problems(self._pair(start, start), [start - 5, start], 600)
        assert len(problems) == 1
        assert "600 ms stagger" in problems[0]

    def test_child_that_never_opened_fails_without_offset_check(self) -> None:
        """A zero start names the child and skips the cross-child check."""
        start = 1_000_000_000
        results = self._pair(0, start)
        results[0] = _driver_result(0, movement_start_ns=0, movement_end_ns=0)
        problems = _anchor_problems(results, [start - 5, start], 600)
        assert problems == ["driver-a never opened its movement window"]

    def test_absolute_sanity_bound_catches_lost_child(self) -> None:
        """A child opening seconds past its own anchor fails on the bound."""
        anchor = 1_000_000_000
        lost = anchor + 4_000_000_000
        problems = _anchor_problems(self._pair(anchor - 5, lost), [anchor - 5, anchor], 600)
        assert any("driver-b" in p and "off its anchor" in p for p in problems)

    def test_empty_window_fails_per_child(self) -> None:
        """A zero-length window is a problem even with a perfect offset."""
        start = 1_000_000_000
        results = [
            _driver_result(0, movement_start_ns=start, movement_end_ns=start),
            _driver_result(
                1,
                movement_start_ns=start + 600_000_000,
                movement_end_ns=start + 700_000_000,
            ),
        ]
        problems = _anchor_problems(results, [start - 5, start], 600)
        assert problems == ["driver-a drove no movement window"]


_OBS_WINDOW_START_NS = 1_000_000_000
_OBS_STEP_NS = 500_000_000
_OBS_DELTAS = 10
_OBS_WINDOW_END_NS = _OBS_WINDOW_START_NS + _OBS_DELTAS * _OBS_STEP_NS


def _observer_trace(rows: dict[str, list[int]], *, clk_tck: int = 1000) -> ProcTrace:
    """A fabricated trace whose rows carry cumulative cpu-tick series.

    Each row maps sample index to its CUMULATIVE tick count, so a row of
    ``[i * ticks for i in ...]`` steps ``ticks`` per 500 ms delta; with
    ``clk_tck=1000`` each tick is one millisecond, and ten deltas over the
    standard window sustain ``ticks / 500`` cores.
    """
    length = max((len(v) for v in rows.values()), default=0)
    samples = []
    for i in range(length):
        targets = tuple(
            TargetSample(
                target=name,
                pid=7000,
                threads=(
                    ThreadSample(
                        tid=7000,
                        comm="python",
                        utime_ticks=ticks[i],
                        stime_ticks=0,
                        nvcsw=0,
                        nivcsw=0,
                    ),
                ),
            )
            for name, ticks in rows.items()
        )
        samples.append(ProcSample(t_ns=_OBS_WINDOW_START_NS + i * _OBS_STEP_NS, targets=targets))
    return ProcTrace(interval_s=_OBS_STEP_NS / 1e9, clk_tck=clk_tck, samples=tuple(samples))


def _observer_spreads(
    median: float | None = 250.0, p95: float | None = 500.0, *, count: int = 40
) -> tuple[InstanceTickSpread, ...]:
    """A healthy per-instance arrival-spread baseline (or an overridden one)."""
    return (InstanceTickSpread(instance_index=0, ticks=count, median_ms=median, p95_ms=p95),)


def _observer_instances(counters_present: bool = True) -> list[InstanceServerSample]:
    """A healthy per-instance counter sample (or an absent-counters one)."""
    return [
        InstanceServerSample(
            instance_index=0,
            inputs_submitted=100_000,
            counters_present=counters_present,
            dispatches=100_000,
            write_deferrals=0,
            view_locate_failures=0,
            deliver_ns=3_000_000_000,
            compose_ns=2_000_000_000,
        )
    ]


class TestEvaluateObservation:
    """Synthetic triggers for every observer-health verdict class.

    Each test fabricates one measurement channel above or below its limit;
    violations drive the klass while unmeasured channels only contribute
    explicit inconclusive reasons. Pure-logic test.
    """

    def evaluate(
        self,
        *,
        trace: ProcTrace | None = None,
        spreads: tuple[InstanceTickSpread, ...] | None = None,
        instances: list[InstanceServerSample] | None = None,
    ) -> ObservationVerdict:
        """Evaluate with healthy defaults for every unfabricated channel."""
        return evaluate_observation(
            ObserverLimits(),
            trace=trace
            if trace is not None
            else _observer_trace({"harness": [i * 240 for i in range(11)]}),
            driver_rows=("harness",),
            window_start_ns=_OBS_WINDOW_START_NS,
            window_end_ns=_OBS_WINDOW_END_NS,
            tick_spreads=spreads if spreads is not None else _observer_spreads(),
            instances=instances if instances is not None else _observer_instances(),
        )

    def test_all_channels_healthy_is_ok(self) -> None:
        """A run inside every ceiling classifies ok with empty reasons."""
        verdict = self.evaluate()
        assert verdict.klass == "ok"
        assert verdict.reasons == ()

    def test_saturated_driver_row_is_invalid_observer(self) -> None:
        """A driver sustaining 2.0 cores breaches the 1.5-core ceiling."""
        verdict = self.evaluate(trace=_observer_trace({"harness": [i * 1000 for i in range(11)]}))
        assert verdict.klass == "invalid-observer"
        assert any("harness" in r and "2.00 cores" in r for r in verdict.reasons)

    def test_absent_driver_samples_are_inconclusive_not_failing(self) -> None:
        """Absent in-window samples yield an inconclusive verdict, never a failure."""
        verdict = self.evaluate(trace=_observer_trace({"instance-0": [5] * 11}))
        assert verdict.klass == "ok"
        assert any("inconclusive" in r for r in verdict.reasons)

    def test_smeared_median_is_invalid_observer(self) -> None:
        """An arrival-spread median past the ceiling fails the observer."""
        verdict = self.evaluate(spreads=_observer_spreads(median=500.0, p95=550.0))
        assert verdict.klass == "invalid-observer"
        assert any("median 500.0 ms" in r for r in verdict.reasons)

    def test_smeared_p95_is_invalid_observer(self) -> None:
        """An arrival-spread p95 past the ceiling fails the observer."""
        verdict = self.evaluate(spreads=_observer_spreads(median=300.0, p95=900.0))
        assert verdict.klass == "invalid-observer"
        assert any("p95 900.0 ms" in r for r in verdict.reasons)

    def test_absent_spread_baseline_is_inconclusive_not_failing(self) -> None:
        """No shared-tick baseline notes inconclusive without failing."""
        verdict = self.evaluate(spreads=_observer_spreads(None, None, count=0))
        assert verdict.klass == "ok"
        assert any("inconclusive" in r for r in verdict.reasons)

    def test_dispatch_drift_breach_is_invalid_server(self) -> None:
        """Dispatches 10% off the submitted volume breach the 2% ceiling."""
        instances = _observer_instances()
        instances[0] = replace(instances[0], dispatches=90_000)
        verdict = self.evaluate(instances=instances)
        assert verdict.klass == "invalid-server"
        assert any("10.00% drift" in r for r in verdict.reasons)

    def test_all_zero_dispatches_fail_drift_naturally(self) -> None:
        """A real all-zero dispatch reading fails against submitted volume."""
        instances = _observer_instances()
        instances[0] = replace(instances[0], dispatches=0)
        verdict = self.evaluate(instances=instances)
        assert verdict.klass == "invalid-server"

    def test_write_deferrals_breach_is_invalid_server(self) -> None:
        """Truncated output drains violate the zero-deferral assertion."""
        instances = _observer_instances()
        instances[0] = replace(instances[0], write_deferrals=3)
        verdict = self.evaluate(instances=instances)
        assert verdict.klass == "invalid-server"
        assert any("3 output drains" in r for r in verdict.reasons)

    def test_view_locate_failures_breach_is_invalid_server(self) -> None:
        """Locate failures violate the zero-failure assertion: window defect."""
        instances = _observer_instances()
        instances[0] = replace(instances[0], view_locate_failures=17)
        verdict = self.evaluate(instances=instances)
        assert verdict.klass == "invalid-server"
        assert any(
            "failed to locate subscribers in their windows on 17 compositions" in r
            for r in verdict.reasons
        )

    def test_deliver_phase_below_band_is_invalid_server(self) -> None:
        """A deliver duration under the recorded band's floor violates."""
        instances = _observer_instances()
        instances[0] = replace(instances[0], deliver_ns=1_000_000_000)
        verdict = self.evaluate(instances=instances)
        assert verdict.klass == "invalid-server"
        assert any("deliver phase took 1000 ms" in r for r in verdict.reasons)

    def test_deliver_phase_above_band_is_invalid_server(self) -> None:
        """A deliver duration over the recorded band's ceiling violates."""
        instances = _observer_instances()
        instances[0] = replace(instances[0], deliver_ns=5_000_000_000)
        verdict = self.evaluate(instances=instances)
        assert verdict.klass == "invalid-server"
        assert any("deliver phase took 5000 ms" in r for r in verdict.reasons)

    def test_compose_phase_below_band_is_invalid_server(self) -> None:
        """A compose duration under the recorded band's floor violates."""
        instances = _observer_instances()
        instances[0] = replace(instances[0], compose_ns=200_000_000)
        verdict = self.evaluate(instances=instances)
        assert verdict.klass == "invalid-server"
        assert any("compose phase took 200 ms" in r for r in verdict.reasons)

    def test_absent_counters_are_inconclusive_not_failing(self) -> None:
        """A failed scrape notes inconclusive instead of judging placeholders."""
        verdict = self.evaluate(instances=_observer_instances(counters_present=False))
        assert verdict.klass == "ok"
        assert any("counters absent" in r for r in verdict.reasons)

    def test_observer_violations_outrank_server_ones(self) -> None:
        """Both sides violating classifies invalid-observer but reports both."""
        instances = _observer_instances()
        instances[0] = replace(instances[0], dispatches=90_000)
        verdict = self.evaluate(
            trace=_observer_trace({"harness": [i * 1000 for i in range(11)]}),
            instances=instances,
        )
        assert verdict.klass == "invalid-observer"
        assert any("drift" in r for r in verdict.reasons)


class TestObserverKnobs:
    """The CLI knobs resolve onto ObserverLimits with frozen defaults."""

    @pytest.fixture(autouse=True)
    def _recalibration_mode(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Knob-resolution tests run in declared recalibration mode."""
        monkeypatch.setenv(_OBSERVER_RECALIBRATION_ENV, "1")

    def test_defaults_resolve_to_the_frozen_limits(self) -> None:
        """An invocation without observer flags yields the frozen limits."""
        parser = _build_parser()
        args = parser.parse_args(["--subprocess-cluster"])
        assert _observer_limits_from_args(args) == ObserverLimits()

    def test_overrides_refused_outside_recalibration_mode(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A gate run cannot carry an observer override by accident."""
        monkeypatch.delenv(_OBSERVER_RECALIBRATION_ENV, raising=False)
        parser = _build_parser()
        args = parser.parse_args(["--subprocess-cluster", "--observer-max-driver-cores", "0.9"])
        with pytest.raises(ValueError):
            _observer_limits_from_args(args)

    def test_defaults_resolve_without_any_escape(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Frozen defaults need no recalibration escape at all."""
        monkeypatch.delenv(_OBSERVER_RECALIBRATION_ENV, raising=False)
        parser = _build_parser()
        args = parser.parse_args(["--subprocess-cluster"])
        assert _observer_limits_from_args(args) == ObserverLimits()

    def test_explicit_knobs_override_each_field(self) -> None:
        """Every flag replaces exactly its own limits field."""
        parser = _build_parser()
        args = parser.parse_args(
            [
                "--observer-max-driver-cores",
                "0.75",
                "--observer-spread-median-ms",
                "450",
                "--observer-spread-p95-ms",
                "900",
                "--observer-dispatch-drift",
                "0.05",
                "--observer-deliver-band-ms",
                "1500",
                "5000",
                "--observer-compose-band-ms",
                "250",
                "1400",
            ]
        )
        assert _observer_limits_from_args(args) == ObserverLimits(
            max_driver_cores=0.75,
            spread_median_ms=450.0,
            spread_p95_ms=900.0,
            dispatch_drift=0.05,
            deliver_band_ms=(1500, 5000),
            compose_band_ms=(250, 1400),
        )

    @pytest.mark.parametrize(
        ("flag", "value"),
        [
            ("--observer-max-driver-cores", "0"),
            ("--observer-max-driver-cores", "-1"),
            ("--observer-spread-median-ms", "0"),
            ("--observer-spread-p95-ms", "-5"),
        ],
    )
    def test_non_positive_scalars_rejected(self, flag: str, value: str) -> None:
        """Cores and spread ceilings are meaningless at or below zero."""
        parser = _build_parser()
        args = parser.parse_args([flag, value])
        with pytest.raises(ValueError):
            _observer_limits_from_args(args)

    @pytest.mark.parametrize("value", ["-0.01", "1.01"])
    def test_dispatch_drift_outside_unit_interval_rejected(self, value: str) -> None:
        """The drift slack is a fraction of submitted inputs, bounded by [0, 1]."""
        parser = _build_parser()
        args = parser.parse_args(["--observer-dispatch-drift", value])
        with pytest.raises(ValueError):
            _observer_limits_from_args(args)

    @pytest.mark.parametrize(
        "band_flag", ["--observer-deliver-band-ms", "--observer-compose-band-ms"]
    )
    def test_inverted_or_negative_band_rejected(self, band_flag: str) -> None:
        """Bands need 0 <= LOW <= HIGH."""
        parser = _build_parser()
        args = parser.parse_args([band_flag, "500", "100"])
        with pytest.raises(ValueError):
            _observer_limits_from_args(args)


def _wiring_metrics(spreads: tuple[InstanceTickSpread, ...]) -> GateMetrics:
    """Metrics whose ledger carries only the given spread baselines."""
    return GateMetrics(
        actor_count=2000,
        sampled_clients=32,
        session_ok_ratio=1.0,
        selected_clients_ratio=1.0,
        move_missing_ratio=0.0,
        bootstrap_ms_p95=500,
        continuity_flicker=False,
        publish_rate_skew=1.0,
        flicker_ledger=FlickerLedger(
            window_start_ns=_OBS_WINDOW_START_NS,
            interval_s=0.1,
            clk_tck=1000,
            clusters=(),
            attributions=(),
            tick_spreads=spreads,
        ),
    )


def _wiring_record(instance_index: int, inputs: int) -> _ClientRecord:
    """One driven client record stamped with the standard window bounds."""
    return _ClientRecord(
        name=f"load-{instance_index}",
        principal_id=100 + instance_index,
        instrumented=True,
        inputs_submitted=inputs,
        instance_index=instance_index,
        movement_start_ns=_OBS_WINDOW_START_NS,
        movement_end_ns=_OBS_WINDOW_END_NS,
    )


class TestSingleDriveObservationWiring:
    """The single-drive observation assembles from the drive's raw pieces.

    Grouping sums inputs per instance, the window comes from the stamped
    record bounds, and the harness sampler row stands for the driver —
    which is meaningful only when the servers ran out of process under a
    profile that opts into the classification. Pure-logic test.
    """

    @staticmethod
    def harness(profile: ScalingProfile, server_pids: list[int]) -> LoadHarness:
        """A harness bound to no factory (the factory never runs here)."""
        return LoadHarness(profile, cast(LoadHostFactory, lambda: None), server_pids=server_pids)

    @staticmethod
    def outcome(records: list[_ClientRecord], windows: list[_WindowCounters]) -> _DriveOutcome:
        """A completed drive with a healthy single-row trace."""
        return _DriveOutcome(
            metrics=_wiring_metrics(_observer_spreads()),
            trace=_observer_trace({"harness": [i * 240 for i in range(11)]}),
            records=records,
            window_counters=windows,
        )

    def test_scoped_run_classifies_grouped_instances(self) -> None:
        """Per-instance input sums pair positionally with the scraped windows."""
        harness = self.harness(DISTRIBUTED_2000, [101, 102])
        records = [_wiring_record(0, 100_000), _wiring_record(1, 90_000)]
        windows = [
            _WindowCounters(True, self._counters(dispatches)) for dispatches in (100_000, 90_000)
        ]
        verdict = harness._observation_for(self.outcome(records, windows))
        assert verdict is not None
        assert verdict.klass == "ok"

    @staticmethod
    def _counters(dispatches: int) -> PhaseCounters:
        """In-band movement-window counters reconciling against ``dispatches``."""
        return PhaseCounters(
            refresh_ns=1_000_000_000,
            compose_ns=2_000_000_000,
            deliver_ns=3_000_000_000,
            dispatches=dispatches,
            publishes=100,
            compose_window_ns=300_000,
            compose_prior_ns=400_000,
            compose_lock_wait_ns=500_000,
            compose_scan_ns=600_000,
            compose_sort_ns=700_000,
            compose_select_ns=800_000,
            compose_skips=0,
            compose_deferrals=0,
            view_locate_failures=0,
            write_ns=250_000_000,
            write_deferrals=0,
        )

    def test_unscoped_profile_reports_no_observation(self) -> None:
        """Profiles without calibrated bands classify nothing, not vacuously."""
        harness = self.harness(SMOKE_DISTRIBUTED, [101, 102])
        records = [_wiring_record(0, 100)]
        windows = [_WindowCounters(True, PhaseCounters(*([0] * 16)))]
        assert harness._observation_for(self.outcome(records, windows)) is None

    def test_in_process_host_reports_no_observation(self) -> None:
        """Without separate server pids the harness row is not driver-only."""
        harness = self.harness(DISTRIBUTED_2000, [])
        records = [_wiring_record(0, 100_000)]
        windows = [_WindowCounters(True, PhaseCounters(*([0] * 16)))]
        assert harness._observation_for(self.outcome(records, windows)) is None

    def test_undriven_window_reports_no_observation(self) -> None:
        """A drive that never stamped its bounds has nothing to judge."""
        harness = self.harness(DISTRIBUTED_2000, [101])
        record = _wiring_record(0, 100_000)
        record.movement_start_ns = 0
        record.movement_end_ns = 0
        assert harness._observation_for(self.outcome([record], [])) is None


class TestMergedObservation:
    """The dual-driver parent evaluates observation from pooled payloads.

    The children's presence flags, counter reads, and submitted-input
    counts feed the same classifier the single drive uses, with the two
    driver rows as cpu evidence. Pure-logic test.
    """

    @staticmethod
    def merge(results: list[DriverResult]) -> GateReport:
        """Pool results through the parent's merge path."""
        return merge_driver_results(
            DISTRIBUTED_2000,
            results,
            ProcTrace(interval_s=0.1, clk_tck=100, samples=()),
            started_at="2026-08-26T00:00:00+00:00",
            duration_s=1.0,
            invocation=None,
        )

    def test_healthy_payloads_pool_into_an_ok_verdict(self) -> None:
        """In-band counters with matching dispatches keep the gate green."""
        report = self.merge([_driver_result(0), _driver_result(1)])
        assert report.observation is not None
        assert report.observation.klass == "ok"
        assert report.passed

    def test_absent_child_counters_report_inconclusive_without_failing(self) -> None:
        """A failed scrape surfaces as inconclusive, not as a gate failure."""
        report = self.merge([_driver_result(0, counters_present=False), _driver_result(1)])
        assert report.observation is not None
        assert report.observation.klass == "ok"
        assert any("counters absent" in r for r in report.observation.reasons)
        assert report.passed

    def test_drifting_dispatches_fail_the_gate(self) -> None:
        """A cohort whose dispatch volume misses its inputs fails invalid-server."""
        drifted = replace(
            _driver_result(0, move_missing_submitted=100_000),
            phase_counter=replace(
                _driver_result(0, move_missing_submitted=100_000).phase_counter,
                dispatches=90_000,
            ),
        )
        report = self.merge([drifted, _driver_result(1)])
        assert report.observation is not None
        assert report.observation.klass == "invalid-server"
        assert not report.passed

    def test_verdict_renders_in_markdown_and_json(self) -> None:
        """The observation section and JSON key carry the klass verbatim."""
        report = self.merge([_driver_result(0), _driver_result(1)])
        rendered = report.to_markdown()
        assert "Observer health" in rendered
        assert "**ok**" in rendered
        payload = report.to_dict()
        observation = payload["observation"]
        assert isinstance(observation, dict)
        assert observation["klass"] == "ok"


class TestGateReportObservationGate:
    """passed requires the thresholds AND an ok observation."""

    @staticmethod
    def report(observation: ObservationVerdict | None) -> GateReport:
        """A thresholds-green distributed report with the given observation."""
        return GateReport(
            profile=DISTRIBUTED_2000,
            metrics=GateMetrics(
                actor_count=2000,
                sampled_clients=32,
                session_ok_ratio=1.0,
                selected_clients_ratio=1.0,
                move_missing_ratio=0.0,
                bootstrap_ms_p95=500,
                continuity_flicker=False,
                publish_rate_skew=1.0,
                move_missing_ratio_certified=0.0,
            ),
            thresholds=DISTRIBUTED_THRESHOLDS,
            started_at="2026-08-26T00:00:00+00:00",
            duration_s=44.0,
            observation=observation,
        )

    def test_ok_observation_passes(self) -> None:
        """An ok verdict leaves a thresholds-green report passing."""
        assert self.report(ObservationVerdict("ok", ())).passed

    def test_invalid_observation_fails(self) -> None:
        """Either invalid class blocks a thresholds-green report."""
        assert not self.report(ObservationVerdict("invalid-server", ("x",))).passed
        assert not self.report(ObservationVerdict("invalid-observer", ("x",))).passed

    def test_inconclusive_only_reasons_still_pass(self) -> None:
        """Unmeasured channels surface as inconclusive without blocking the gate."""
        verdict = ObservationVerdict(
            "ok", ("instance 0 phase counters absent; server assertions inconclusive",)
        )
        assert self.report(verdict).passed

    def test_absent_observation_preserves_legacy_semantics(self) -> None:
        """Reports built without an observation keep their old verdict."""
        assert self.report(None).passed
