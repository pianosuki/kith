"""Two phase-offset load-generator processes for the scaling-gate harness.

Drives one subprocess-cluster run with two driver processes instead of
one: each driver carries exactly one instance's client cohort, and a
fixed CLOCK_MONOTONIC anchor holds a constant offset between the two
movement windows for the whole run, immune to bootstrap jitter. The
single-driver layout interleaves both instances' input submits in one
event-loop pass, which correlates the instances' handler-worker dispatch
bursts; splitting the drivers and holding the offset de-correlates them
without touching the system under test — the same servers, the same
coordination surface, and the same per-instance client split.

Division of labor: :func:`run_dual_drivers` (the parent) boots the
cluster through the ordinary subprocess factory, launches two
``load_harness --driver-worker`` children, samples /proc across the
servers and both children, validates each child's raw result including
the realized movement-window anchor, pools integer counts over the union
population, and renders one report in the standard schema.
:func:`run_driver_worker` (a child) reuses the single-driver machinery
unchanged and emits raw counts rather than rendered ratios, because
pooling must sum counts, never average ratios. Any child failure —
nonzero exit, unusable payload, or a missed movement anchor — fails the
whole run loudly; no partial report is produced.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import subprocess
import sys
import tempfile
import time
from collections.abc import Sequence
from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from tools.agent.gap_ledger import ClientSeries, SeriesFrame, build_flicker_ledger
from tools.agent.load_harness import (
    _BULK_HISTORY_SIZE,
    _INSTRUMENTED_HISTORY_SIZE,
    _INSTRUMENTED_SAMPLE_SIZE,
    BreadthAttribution,
    GateMetrics,
    GateReport,
    InstanceGateMetrics,
    LoadHarness,
    PhaseCounters,
    RunShape,
    ScalingProfile,
    SparseBreadthRow,
    _collect_breadth,
    _collect_depth_metrics,
    _collect_run_envelope,
    _echo_delta_histogram,
    _flicker_breakdown_from_series,
    _move_missing_ratio,
    _percentile,
    _publish_rate_counts,
    _selected_clients_ratio,
    _write_proc_trace,
    thresholds_for,
)
from tools.agent.observer_gates import (
    DEFAULT_OBSERVER_LIMITS,
    InstanceServerSample,
    ObserverLimits,
    evaluate_observation,
)
from tools.agent.proc_sampler import ProcSampler, SamplerTarget


if TYPE_CHECKING:
    from tools.agent.ahc import AgenticHeadlessClient
    from tools.agent.load_harness import LoadHostFactory
    from tools.agent.orchestrator import Orchestrator
    from tools.agent.proc_sampler import ProcTrace

__all__ = [
    "DriverResult",
    "dump_driver_result",
    "load_driver_result",
    "merge_driver_results",
    "run_driver_worker",
    "run_dual_drivers",
    "split_clients",
    "split_instrumented",
]


# The children's payloads are an internal interface between the parent
# and its own spawned workers; the version rejects a stale binary mixing
# generations mid-run rather than mis-pooling half-compatible fields.
# Version 2 adds ``counters_present`` so the parent's server assertions
# distinguish a failed control-plane scrape from a genuine all-zero read.
# Payload contract between the dual-driver parent and its workers.
# Version 3 carries a semantic cut, not an additive diagnostic: v3 adds
# isolated_clients, which changes what the gated selected_clients ratio
# measures (its denominator drops the geometrically isolated population),
# so children and parents must agree on it exactly — unlike an additive
# counter that changes no computed value, this one defaults wrongly if
# mixed generations load each other's payloads.
# Version 4 is a second semantic cut on the same ratio: the children's
# ``selected_count`` is contained within their classified cohort
# (observed-but-isolated clients ride neither side, rendered as additive
# diagnostics), and the pooled denominator derives from
# ``classified_clients`` rather than from ``clients_total`` — a v3
# child's uncontained numerator pools into a denominator it can exceed.
# Mixed generations are rejected for the same reason as v3.
_SCHEMA_VERSION = 4

# Each realized movement window must open within this bound of its own
# nominal anchor: a child that anchored on the wrong clock or never waited
# misses by seconds and fails here regardless of its sibling.
_ANCHOR_ABS_BOUND_NS = 1_000_000_000

# The realized inter-child offset must track the configured stagger within
# this tolerance. Common absolute jitter (scrape delay, slow bootstrap tail)
# cancels in the difference; a collapsed stagger shows up as an offset error
# of roughly the full stagger, far outside the bound. Calibrated against
# observed real deviations of 100-245 ms per child (worst pair error 145 ms).
_ANCHOR_REL_TOLERANCE_NS = 300_000_000

# Headroom added to the bootstrap deadline when placing the anchors, so a
# child that boots slowly still reaches its wait before the anchor passes.
_ANCHOR_LEAD_MARGIN_S = 1.0

# Principal numbering matches the shared cluster factory's base so the
# global id space and the modulo mapping rule are identical across modes.
_PRINCIPAL_ID_BASE = 100


@dataclass(frozen=True, slots=True)
class DriverResult:
    """Raw per-cohort outcome one driver worker emits for pooling.

    Counts, not ratios: the parent recomputes every ratio over the union
    population, so each field is the summable form. ``series`` carries the
    instrumented clients' own-actor frames with true instance indices so
    the parent builds one flicker ledger over both cohorts.
    """

    schema_version: int
    driver_pid: int
    instance_index: int
    clients_total: int
    session_ok_count: int
    selected_count: int
    sampled_clients: int
    move_missing_submitted: int
    move_missing_observed: int
    inputs_submitted: int
    bootstrap_latencies_ms: list[int]
    delivered_per_instance: dict[int, int]
    phase_counter: PhaseCounters
    # True when the cohort's window was driven and both boundary scrapes
    # succeeded; False marks the counters as placeholders so the parent's
    # server assertions report inconclusive instead of judging them.
    counters_present: bool
    series: list[ClientSeries]
    movement_start_ns: int
    movement_end_ns: int
    # Booted-cohort clients whose own actor lacked the end-of-window
    # neighborhood required to ever observe min_distinct_others others.
    # The parent drops this count from the pooled selected_clients
    # denominator (reported-only); 0 keeps the payload valid for runs
    # whose host presented no roster.
    isolated_clients: int = 0
    # Clients the collection-time binding map binds — the cohort the
    # child's contained numerator ranges over. Load-bearing for the pooled
    # denominator (eligible = Σclassified - Σisolated); None when the
    # child's host presented no roster (the numerator then keeps its
    # full-population reading, and the parent's pooled denominator
    # degrades the same way).
    classified_clients: int | None = None
    # Observed-but-isolated diagnostics: clients whose movement-window
    # evidence proves a full view set while their window-end geometry is
    # isolated (fresh — the divergence signature where isolation implies
    # cross-cell geometry). Rides neither side of the ratio. Additive
    # fields.
    observed_but_isolated_fresh_clients: int = 0
    # Records the collection-time binding map does not bind (they cannot
    # hold views and ride neither side of the ratio) and bound clients the
    # pre-movement boot snapshot missed (the late-bootstrap signature).
    # Additive fields.
    unbound_clients: int = 0
    late_bootstrapped_clients: int = 0
    # Per-client cause attribution for the observed-but-isolated
    # population (name, cause class, fresh flag), rendered pool-side.
    breadth_attribution: tuple[BreadthAttribution, ...] = ()
    # The breadth residue (bound, non-isolated, unobserved on a complete
    # basis) with its per-client window-versus-deque counts, rendered
    # pool-side so a failing run closes its own residue. Additive fields.
    sparse_clients: int = 0
    never_opened_clients: int = 0
    sparse_rows: tuple[SparseBreadthRow, ...] = ()
    # The other breadth classes the pooled report renders: streams that
    # gapped past the stall bound after clearing the bar, and retained
    # bases that cannot prove completeness. Additive fields.
    breadth_stalled_clients: int = 0
    breadth_incomplete_clients: int = 0
    # Evidence-deque span across the child's clients (min and p50, ns):
    # the retained record's reach versus the movement window. Additive
    # fields.
    breadth_evidence_span_ns_min: int | None = None
    breadth_evidence_span_ns_p50: int | None = None
    # Measured text of a breadth-guardrail breach (isolated fraction above
    # its bound, or fresh evidence inside isolated geometry): rendered
    # with the report and failing the verdict instead of aborting
    # collection.
    breadth_violation: str | None = None
    # CPUs the worker pinned itself to via --driver-cpus, sorted; None
    # when the run launched without per-child affinity.
    driver_cpus: list[int] | None = None
    # Selected within the head depth sample (summable across workers so
    # the pooled report renders the head-only ratio alongside the
    # all-client one). Additive field: payloads written before it existed
    # carry 0.
    selected_head_count: int = 0
    # Certified move-missing aggregate: the summed per-client
    # missing count under the counter-delta rule plus the count of
    # instrumented clients whose publisher never minted (fallbacks ride
    # the event rule). Additive fields: archived payloads carry 0.
    certified_missing: int = 0
    certified_fallback_clients: int = 0


def parse_cpu_list(spec: str) -> list[int]:
    """Parse a comma-separated CPU list into distinct non-negative ints."""
    try:
        cpus = [int(part) for part in spec.split(",")]
    except ValueError as exc:
        raise ValueError(f"invalid CPU list {spec!r}") from exc
    if not cpus or any(cpu < 0 for cpu in cpus):
        raise ValueError(f"invalid CPU list {spec!r}")
    if len(set(cpus)) != len(cpus):
        raise ValueError(f"CPU list has duplicates: {spec!r}")
    return cpus


def apply_driver_affinity(args: argparse.Namespace) -> list[int] | None:
    """Pin this process to ``--driver-cpus`` when the flag carries a set.

    Returns the applied (sorted) set for the result payload, or None
    when no affinity was requested. A rejected set raises before any
    cohort work starts.
    """
    spec = getattr(args, "driver_cpus", None)
    if spec is None:
        return None
    cpus = parse_cpu_list(spec)
    os.sched_setaffinity(0, set(cpus))
    return sorted(os.sched_getaffinity(0))


def split_clients(total: int) -> tuple[int, int]:
    """Split a client population into two equal driver cohorts."""
    if total % 2:
        raise ValueError(f"--dual-drivers needs an even client count, got {total}")
    return total // 2, total // 2


def split_instrumented(global_size: int) -> tuple[int, int]:
    """Split the depth-metric sample so the union reproduces it.

    The single drive marks its first ``global_size`` started clients
    instrumented, and round-robin placement deals those to the two
    instances as ceil/floor halves; per-driver sizes reproduce exactly
    that per-instance composition.
    """
    return (global_size + 1) // 2, global_size // 2


def dump_driver_result(result: DriverResult, path: str) -> None:
    """Write one driver result as JSON with its schema version."""
    payload = {"schema_version": _SCHEMA_VERSION, **asdict(result)}
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle)


def load_driver_result(path: str) -> DriverResult:
    """Read one driver result, rejecting a foreign schema version."""
    with open(path, encoding="utf-8") as handle:
        payload = json.load(handle)
    if payload.get("schema_version") != _SCHEMA_VERSION:
        raise ValueError(f"unsupported driver result schema: {payload.get('schema_version')!r}")
    data = dict(payload)
    delivered = {int(k): int(v) for k, v in data.pop("delivered_per_instance").items()}
    counter_data = data.pop("phase_counter")
    # Payloads written before the locate-failure counter existed carry a
    # 15-field phase_counter block; absent parses to zero like any absent
    # metrics series, so archived results still load and their server
    # assertions stay judged on the counters they actually had. The
    # delivery-executor counters postdate the executor commit the same
    # way: archived payloads predate the series entirely.
    counter_data.setdefault("view_locate_failures", 0)
    counter_data.setdefault("delivery_jobs", 0)
    counter_data.setdefault("delivery_inflight_skips", 0)
    counter_data.setdefault("delivery_ebusy_skips", 0)
    counter_data.setdefault("delivery_wait_budget_exhausted", 0)
    counter_data.setdefault("compose_wait_timeouts", 0)
    counter_data.setdefault("delivery_inflight_current", 0)
    counter_data.setdefault("delivery_inflight_high_watermark", 0)
    counter_data.setdefault("delivery_frames_enqueued", 0)
    counter_data.setdefault("delivery_dropped", 0)
    counter_data.setdefault("delivery_event_frames_enqueued", 0)
    counter_data.setdefault("delivery_suppressed", 0)
    counter_data.setdefault("view_visits", 0)
    counter_data.setdefault("view_candidates", 0)
    counter_data.setdefault("view_selected", 0)
    counter_data.setdefault("view_candidate_high_watermark", 0)
    counter_data.setdefault("view_selected_high_watermark", 0)
    counter = PhaseCounters(**counter_data)
    data["counters_present"] = bool(data.pop("counters_present"))
    data["series"] = [
        ClientSeries(
            name=s["name"],
            instance_index=int(s["instance_index"]),
            window_start_ns=int(s["window_start_ns"]),
            window_end_ns=int(s["window_end_ns"]),
            frames=tuple(
                SeriesFrame(ts_ns=int(f["ts_ns"]), input_tick=int(f["input_tick"]))
                for f in s["frames"]
            ),
        )
        for s in data.pop("series")
    ]
    data["bootstrap_latencies_ms"] = [int(v) for v in data.pop("bootstrap_latencies_ms")]
    raw_cpus = data.pop("driver_cpus", None)
    data["driver_cpus"] = [int(v) for v in raw_cpus] if raw_cpus is not None else None
    data["breadth_violation"] = data.pop("breadth_violation", None)
    data.setdefault("selected_head_count", 0)
    data.setdefault("certified_missing", 0)
    data.setdefault("certified_fallback_clients", 0)
    data["breadth_attribution"] = tuple(
        BreadthAttribution(
            name=str(entry["name"]),
            klass=str(entry["klass"]),
            fresh=bool(entry["fresh"]),
        )
        for entry in data.pop("breadth_attribution", ())
    )
    data["sparse_rows"] = tuple(
        SparseBreadthRow(
            name=str(entry["name"]),
            instance_index=int(entry["instance_index"]),
            klass=str(entry["klass"]),
            distinct_in_window=int(entry["distinct_in_window"]),
            distinct_deque=int(entry["distinct_deque"]),
        )
        for entry in data.pop("sparse_rows", ())
    )
    data.setdefault("sparse_clients", 0)
    data.setdefault("never_opened_clients", 0)
    data.setdefault("breadth_stalled_clients", 0)
    data.setdefault("breadth_incomplete_clients", 0)
    raw_span_min = data.pop("breadth_evidence_span_ns_min", None)
    data["breadth_evidence_span_ns_min"] = None if raw_span_min is None else int(raw_span_min)
    raw_span_p50 = data.pop("breadth_evidence_span_ns_p50", None)
    data["breadth_evidence_span_ns_p50"] = None if raw_span_p50 is None else int(raw_span_p50)
    raw_classified = data.pop("classified_clients", None)
    data["classified_clients"] = None if raw_classified is None else int(raw_classified)
    int_fields = (
        "driver_pid",
        "instance_index",
        "clients_total",
        "session_ok_count",
        "selected_count",
        "sampled_clients",
        "move_missing_submitted",
        "move_missing_observed",
        "inputs_submitted",
        "movement_start_ns",
        "movement_end_ns",
        "isolated_clients",
        "selected_head_count",
        "certified_missing",
        "certified_fallback_clients",
        "observed_but_isolated_fresh_clients",
        "unbound_clients",
        "late_bootstrapped_clients",
        "sparse_clients",
        "never_opened_clients",
    )
    for key in int_fields:
        data[key] = int(data[key])
    return DriverResult(delivered_per_instance=delivered, phase_counter=counter, **data)


def merge_driver_results(
    profile: ScalingProfile,
    results: Sequence[DriverResult],
    trace: ProcTrace,
    *,
    started_at: str,
    duration_s: float,
    invocation: str | None,
    limits: ObserverLimits = DEFAULT_OBSERVER_LIMITS,
) -> GateReport:
    """Pool driver cohorts into one gate report in the standard schema.

    Every ratio derives from summed integer counts over the union
    population; the flicker ledger builds once over the pooled series and
    the parent-collected /proc trace, anchored at the earliest series
    start exactly as the single drive anchors it. Phase counters
    concatenate in instance order because each child scraped its own
    instance at its own window bounds. The observer verdict classifies the
    run's measurement health from the same pooled material — the children's
    submitted-input counts and counter reads against the parent's trace,
    with the two driver rows as the instrument's cpu evidence.
    """
    ordered = sorted(results, key=lambda r: r.instance_index)
    totals = sum(r.clients_total for r in ordered)
    session_ok = sum(r.session_ok_count for r in ordered)
    selected = sum(r.selected_count for r in ordered)
    violations = [r.breadth_violation for r in ordered if r.breadth_violation]
    # A cohort without a roster carries a full-population numerator; the
    # pooled ratio then degrades to the legacy denominator exactly as a
    # single drive without a roster does.
    classified_counts: list[int] = []
    isolated_counts: list[int] = []
    rostered = True
    for result in ordered:
        if result.classified_clients is None:
            rostered = False
            break
        classified_counts.append(result.classified_clients)
        isolated_counts.append(result.isolated_clients)
    if rostered:
        classified = sum(classified_counts)
        isolated = sum(isolated_counts)
        eligible = classified - isolated
        if selected > eligible:
            # Containment is structural in every well-formed v4 payload, so
            # a pooled excess means a child serialized counts outside its
            # own cohort — rendered as a breach rather than trusted.
            violations.append(
                f"pooled selected {selected} exceeds the classified-minus-isolated "
                f"denominator {eligible}; a child payload is inconsistent"
            )
    else:
        classified = None
        isolated = None
        eligible = None
    fresh_isolated = sum(r.observed_but_isolated_fresh_clients for r in ordered)
    unbound = sum(r.unbound_clients for r in ordered)
    late_bootstrapped = sum(r.late_bootstrapped_clients for r in ordered)
    attribution = tuple(entry for r in ordered for entry in r.breadth_attribution)
    sparse_rows = tuple(entry for r in ordered for entry in r.sparse_rows)
    sparse_clients = sum(r.sparse_clients for r in ordered)
    never_opened = sum(r.never_opened_clients for r in ordered)
    stalled_clients = sum(r.breadth_stalled_clients for r in ordered)
    incomplete_clients = sum(r.breadth_incomplete_clients for r in ordered)
    span_mins = [r.breadth_evidence_span_ns_min for r in ordered]
    span_min = min((v for v in span_mins if v is not None), default=None)
    span_p50s = sorted(
        r.breadth_evidence_span_ns_p50
        for r in ordered
        if r.breadth_evidence_span_ns_p50 is not None
    )
    span_p50 = span_p50s[len(span_p50s) // 2] if span_p50s else None
    sampled = sum(r.sampled_clients for r in ordered)
    selected_head = sum(r.selected_head_count for r in ordered)
    submitted = sum(r.move_missing_submitted for r in ordered)
    observed = sum(r.move_missing_observed for r in ordered)
    move_missing = 0.0 if submitted == 0 else max(0.0, 1.0 - observed / submitted)
    certified_missing = sum(r.certified_missing for r in ordered)
    certified_fallbacks = sum(r.certified_fallback_clients for r in ordered)
    move_missing_certified = None if submitted == 0 else certified_missing / submitted
    latencies = [v for r in ordered for v in r.bootstrap_latencies_ms]
    p95 = _percentile(latencies, 95) if latencies else 0
    delivered: dict[int, int] = {}
    for result in ordered:
        for index, count in result.delivered_per_instance.items():
            delivered[index] = delivered.get(index, 0) + count
    if not delivered:
        skew = 0.0
    else:
        low = min(delivered.values())
        skew = float("inf") if low == 0 else max(delivered.values()) / low
    series = [s for r in ordered for s in r.series]
    flicker = _flicker_breakdown_from_series(series)
    ledger = build_flicker_ledger(trace, series)
    # One metrics row per cohort: each child drove exactly one instance,
    # so its raw counts reduce to that instance's bar-facing row with the
    # same formulas the pooled row uses. The verdict judges these rows
    # per instance alongside the pooled row.
    instance_rows: list[InstanceGateMetrics] = []
    for r in ordered:
        cohort_eligible: int | None = None
        cohort_isolated: int | None = None
        if r.classified_clients is not None:
            cohort_eligible = r.classified_clients - r.isolated_clients
            cohort_isolated = r.isolated_clients
        certified = (
            None
            if r.move_missing_submitted == 0
            else r.certified_missing / r.move_missing_submitted
        )
        cohort_flicker = _flicker_breakdown_from_series(r.series)
        instance_rows.append(
            InstanceGateMetrics(
                instance_index=r.instance_index,
                actor_count=r.clients_total,
                sampled_clients=r.sampled_clients,
                session_ok_ratio=(r.session_ok_count / r.clients_total if r.clients_total else 0.0),
                selected_clients_ratio=_selected_clients_ratio(
                    r.selected_count, r.clients_total, cohort_eligible
                ),
                eligible_clients=cohort_eligible,
                isolated_clients=cohort_isolated,
                classified_clients=r.classified_clients,
                move_missing_ratio=_move_missing_ratio(
                    r.move_missing_submitted, r.move_missing_observed
                ),
                move_missing_ratio_certified=certified,
                certified_fallback_clients=r.certified_fallback_clients,
                bootstrap_ms_p95=int(_percentile(r.bootstrap_latencies_ms, 95)),
                continuity_flicker=cohort_flicker.total_gaps > 0,
            )
        )
    observation = evaluate_observation(
        limits,
        trace=trace,
        driver_rows=("driver-a", "driver-b"),
        window_start_ns=min(r.movement_start_ns for r in ordered),
        window_end_ns=max(r.movement_end_ns for r in ordered),
        tick_spreads=ledger.tick_spreads,
        instances=[
            InstanceServerSample(
                instance_index=r.instance_index,
                inputs_submitted=r.inputs_submitted,
                counters_present=r.counters_present,
                dispatches=r.phase_counter.dispatches,
                write_deferrals=r.phase_counter.write_deferrals,
                view_locate_failures=r.phase_counter.view_locate_failures,
                deliver_ns=r.phase_counter.deliver_ns,
                compose_ns=r.phase_counter.compose_ns,
            )
            for r in ordered
        ],
    )
    metrics = GateMetrics(
        actor_count=profile.actor_count,
        sampled_clients=sampled,
        selected_head_count=selected_head,
        session_ok_ratio=session_ok / totals if totals else 0.0,
        selected_clients_ratio=_selected_clients_ratio(selected, totals, eligible),
        move_missing_ratio=move_missing,
        bootstrap_ms_p95=int(p95),
        continuity_flicker=flicker.total_gaps > 0,
        publish_rate_skew=skew,
        flicker_count=flicker.total_gaps,
        flicker_breakdown=flicker,
        phase_counters=tuple(r.phase_counter for r in ordered),
        flicker_ledger=ledger,
        eligible_clients=eligible,
        isolated_clients=isolated,
        classified_clients=classified,
        observed_but_isolated_fresh_clients=fresh_isolated,
        unbound_clients=unbound,
        late_bootstrapped_clients=late_bootstrapped,
        breadth_attribution=attribution,
        breadth_sparse_clients=sparse_clients,
        breadth_never_opened_clients=never_opened,
        breadth_sparse_rows=sparse_rows,
        breadth_stalled_clients=stalled_clients,
        breadth_incomplete_clients=incomplete_clients,
        breadth_evidence_span_ns_min=span_min,
        breadth_evidence_span_ns_p50=span_p50,
        inputs_submitted=sum(r.inputs_submitted for r in ordered),
        echo_delta=_echo_delta_histogram(series),
        breadth_violation="; ".join(violations) or None,
        move_missing_ratio_certified=move_missing_certified,
        certified_fallback_clients=certified_fallbacks,
        instances=tuple(instance_rows),
    )
    return GateReport(
        profile=profile,
        metrics=metrics,
        thresholds=thresholds_for(profile),
        started_at=started_at,
        duration_s=duration_s,
        invocation=invocation,
        observation=observation,
    )


def _anchor_problems(
    results: Sequence[DriverResult],
    anchors: Sequence[int],
    stagger_ms: int,
) -> list[str]:
    """Validate realized movement windows against their anchors.

    Per child: the window must have opened (a zero start means the anchor
    wait never completed), stay within the absolute sanity bound of its
    own nominal anchor, and span a nonzero interval. Across children: the
    realized offset between the two window starts must track the
    configured stagger within :data:`_ANCHOR_REL_TOLERANCE_NS` — the check
    that actually pins de-phasing, since shared absolute jitter cancels in
    the difference.
    """
    problems: list[str] = []
    labels = ("driver-a", "driver-b")
    for result, anchor, label in zip(results, anchors, labels, strict=False):
        if result.movement_start_ns <= 0:
            problems.append(f"{label} never opened its movement window")
            continue
        deviation = abs(result.movement_start_ns - anchor)
        if deviation > _ANCHOR_ABS_BOUND_NS:
            problems.append(
                f"{label} opened its movement window {deviation / 1e6:.0f} ms off its anchor"
            )
        if result.movement_end_ns <= result.movement_start_ns:
            problems.append(f"{label} drove no movement window")
    if len(results) == 2 and not any("never opened" in p for p in problems):
        offset_err = abs(
            (results[1].movement_start_ns - results[0].movement_start_ns) - stagger_ms * 1_000_000
        )
        if offset_err > _ANCHOR_REL_TOLERANCE_NS:
            realized = (results[1].movement_start_ns - results[0].movement_start_ns) / 1e6
            problems.append(
                f"movement-window offset realized at {realized:.0f} ms against a "
                f"{stagger_ms} ms stagger ({offset_err / 1e6:.0f} ms off)"
            )
    return problems


def _worker_host_factory(
    *,
    instance_index: int,
    gateway_port: int,
    control_port: int,
    instrumented_count: int,
    replication_batch_type_id: int,
    evidence_budget_bytes: int = 0,
) -> LoadHostFactory:
    """Return a factory whose clients all land on one pre-booted instance.

    Principals are numbered from ``100 + instance_index`` stepping by the
    instance count, so ``(principal_id - 100) % 2`` places every client of
    driver ``k`` on instance ``k`` — the same ids, mapping rule, and
    instrumented/bulk history split the shared factory produces for the
    full population.
    """
    from examples.spatial.client import make_ahc, replication_direct_types

    from tools.agent.embedded_host import _history_size_for, _summary_retention_for
    from tools.agent.orchestrator import Orchestrator
    from tools.agent.server_control import ServerControlClient

    base = _PRINCIPAL_ID_BASE + instance_index
    control = ServerControlClient(host="127.0.0.1", port=control_port)

    def factory() -> Orchestrator:
        principal_counter = [base]

        def ahc_factory(
            instance_id: str, connect_host: str, connect_port: int
        ) -> AgenticHeadlessClient:
            del connect_host, connect_port
            principal_id = principal_counter[0]
            principal_counter[0] += 2
            history_size = _history_size_for(
                (principal_id - base) // 2,
                instrumented_count,
                _INSTRUMENTED_HISTORY_SIZE,
                _BULK_HISTORY_SIZE,
            )
            return make_ahc(
                instance_id=instance_id,
                host="127.0.0.1",
                port=gateway_port,
                principal_id=principal_id,
                event_history_size=history_size,
                http_enabled=False,
                ipc_enabled=False,
                reconnect_enabled=False,
                tick_interval_s=0.1,
                replication_batch_type_id=replication_batch_type_id,
                direct_type_ids=replication_direct_types(),
                summary_mode=_summary_retention_for(
                    (principal_id - base) // 2, instrumented_count, _BULK_HISTORY_SIZE
                ),
                evidence_budget_bytes=evidence_budget_bytes,
            )

        return Orchestrator(ahc_factory=ahc_factory, server_control=control)

    return factory


async def _drive_worker_cohort(args: argparse.Namespace, profile: ScalingProfile) -> None:
    """Drive one instance's cohort and write the raw result payload."""
    from examples.spatial import messages as spatial_messages

    driver_cpus = apply_driver_affinity(args)
    instance_index = int(args.driver_worker)
    orch_factory = _worker_host_factory(
        instance_index=instance_index,
        gateway_port=int(args.gateway_port),
        control_port=int(args.control_port),
        instrumented_count=(
            _INSTRUMENTED_SAMPLE_SIZE
            if args.instrumented_sample_size is None
            else int(args.instrumented_sample_size)
        ),
        replication_batch_type_id=spatial_messages.ACTOR_STATE_BATCH_TYPE,
        evidence_budget_bytes=profile.evidence_budget_bytes,
    )
    orch = orch_factory()

    def host_factory() -> Orchestrator:
        return orch

    harness = LoadHarness(
        profile,
        host_factory,
        instrumented_sample_size=(
            _INSTRUMENTED_SAMPLE_SIZE
            if args.instrumented_sample_size is None
            else int(args.instrumented_sample_size)
        ),
        movement_anchor_ns=args.movement_anchor_ns,
        echo_settle_s=args.post_window_echo_settle_s,
    )
    try:
        result = await _collect_cohort_result(harness, orch, profile, instance_index)
        result = replace(result, driver_cpus=driver_cpus)
    finally:
        await orch.shutdown()
    dump_driver_result(result, str(args.result_output))


async def _collect_cohort_result(
    harness: LoadHarness,
    orch: Orchestrator,
    profile: ScalingProfile,
    instance_index: int,
) -> DriverResult:
    """Run the distributed drive sequence against one pre-booted instance.

    Mirrors the distributed drive's step order: register, bootstrap,
    resolve rosters, settle, anchored movement window, breadth/depth
    collection, client stop. The true instance index is stamped after
    roster resolution so the single control client resolves actors under
    index 0, while the emitted payload carries the index the parent's
    ledger needs.
    """
    records = await harness._register_and_start(orch)
    await harness._await_bootstrap(orch, records)
    await harness._resolve_instance_assignment(orch, records)
    await harness._resolve_own_actors_distributed(orch, records)
    for rec in records:
        rec.instance_index = instance_index
    await asyncio.sleep(profile.settle_s)
    # The pooled drive reports window deltas only: the child results carry
    # no sampler payload, so the in-window samples are dropped here.
    counters, _counter_samples = await harness._movement_phase(orch, records)
    thresholds = thresholds_for(profile)
    breadth = await _collect_breadth(orch, records, thresholds.min_distinct_others)
    total = breadth.total
    selected = breadth.selected_count
    selected_head = breadth.selected_head_count
    instrumented = [r for r in records if r.instrumented and r.own_actor_id != 0]
    # The pooled child holds the same post-window echo drain the single
    # drive holds before its depth fetch, so its observed and certified
    # readings measure the same interval the single drive's do.
    await harness._post_window_echo_drain(counters)
    submitted, observed, series, cert_rows = await _collect_depth_metrics(
        orch, instrumented, harness._echo_settle_s
    )
    certified_missing = sum(r.certified_missing for r in cert_rows)
    certified_fallbacks = sum(1 for r in cert_rows if r.certified_fallback)
    delivered = await _publish_rate_counts(orch, records)
    await harness._stop_clients(orch, records)
    return DriverResult(
        schema_version=_SCHEMA_VERSION,
        driver_pid=os.getpid(),
        instance_index=instance_index,
        clients_total=total,
        session_ok_count=breadth.session_ok_count,
        selected_count=selected,
        selected_head_count=selected_head,
        sampled_clients=len(instrumented),
        move_missing_submitted=submitted,
        move_missing_observed=observed,
        certified_missing=certified_missing,
        certified_fallback_clients=certified_fallbacks,
        inputs_submitted=sum(r.inputs_submitted for r in records),
        bootstrap_latencies_ms=[
            max(0, (r.ready_ns - r.started_ns) // 1_000_000)
            for r in records
            if r.ready_ns > r.started_ns
        ],
        delivered_per_instance=delivered,
        phase_counter=counters[0].counters if counters else _zero_phase_counters(),
        counters_present=counters[0].present if counters else False,
        series=list(series),
        movement_start_ns=records[0].movement_start_ns if records else 0,
        movement_end_ns=records[0].movement_end_ns if records else 0,
        isolated_clients=0 if breadth.isolated_clients is None else breadth.isolated_clients,
        classified_clients=breadth.classified_clients,
        observed_but_isolated_fresh_clients=breadth.observed_but_isolated_fresh_clients,
        unbound_clients=breadth.unbound_clients,
        late_bootstrapped_clients=breadth.late_bootstrapped_clients,
        breadth_attribution=breadth.attribution,
        sparse_clients=breadth.sparse_clients,
        never_opened_clients=breadth.never_opened_clients,
        sparse_rows=breadth.sparse_rows,
        breadth_stalled_clients=breadth.stalled_clients,
        breadth_incomplete_clients=breadth.incomplete_evidence_clients,
        breadth_evidence_span_ns_min=breadth.evidence_span_ns_min,
        breadth_evidence_span_ns_p50=breadth.evidence_span_ns_p50,
        breadth_violation=breadth.violation,
    )


def _zero_phase_counters() -> PhaseCounters:
    """Return the honest all-zero counters signal (no driven window)."""
    return PhaseCounters(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)


def run_driver_worker(args: argparse.Namespace, profile: ScalingProfile) -> int:
    """Run one partitioned cohort to completion; the process exit is 0."""
    asyncio.run(_drive_worker_cohort(args, profile))
    return 0


def _dual_targets(server_pids: Sequence[int], driver_pids: Sequence[int]) -> list[SamplerTarget]:
    """Build the sampler rows for a dual-driver run.

    Each server instance plus each driver process; the driver rows carry
    distinct names so attribution tables never conflate the two event
    loops the way a single self-row cannot.
    """
    targets = [SamplerTarget(name=f"instance-{i}", pid=pid) for i, pid in enumerate(server_pids)]
    labels = ("driver-a", "driver-b")
    targets += [
        SamplerTarget(name=label, pid=pid) for label, pid in zip(labels, driver_pids, strict=True)
    ]
    return targets


def _worker_argv(
    index: int,
    profile: ScalingProfile,
    clients: int,
    instrumented: int,
    gateway_port: int,
    control_port: int,
    anchor_ns: int,
    result_path: str,
    driver_cpus: str | None = None,
    echo_settle_s: float | None = None,
) -> list[str]:
    """Build one driver worker's command line."""
    argv = [
        sys.executable,
        "-m",
        "tools.agent.load_harness",
        "--profile",
        profile.name,
        "--clients",
        str(clients),
        "--driver-worker",
        str(index),
        "--gateway-port",
        str(gateway_port),
        "--control-port",
        str(control_port),
        "--movement-anchor-ns",
        str(anchor_ns),
        "--instrumented-sample-size",
        str(instrumented),
        "--result-output",
        result_path,
    ]
    if driver_cpus is not None:
        argv += ["--driver-cpus", driver_cpus]
    if echo_settle_s is not None:
        argv += ["--post-window-echo-settle-s", str(echo_settle_s)]
    return argv


async def run_dual_drivers(
    profile: ScalingProfile,
    *,
    python_workers: int,
    delivery_strategy: str | None,
    delivery_workers: int = 0,
    tiered_max_gap_ms: int | None = None,
    instance_count: int,
    server_affinity: str | None,
    stagger_ms: int,
    proc_trace_output: str | None,
    invocation: str | None,
    limits: ObserverLimits = DEFAULT_OBSERVER_LIMITS,
    driver_child_cpus_a: str | None = None,
    driver_child_cpus_b: str | None = None,
    echo_settle_s: float | None = None,
    shape: RunShape | None = None,
) -> GateReport:
    """Drive the cluster with two phase-offset driver processes.

    Boots the cluster through the shared factory, spawns both workers
    with absolute movement anchors stagger ms apart, samples the servers
    and both workers, pools their raw counts into one report, and tears
    everything down. ``limits`` carries the observer-health ceilings the
    pooled report's verdict is judged with. ``driver_child_cpus_a`` and
    ``driver_child_cpus_b`` pin each worker to its own CPUs so the
    scheduler cannot co-locate the two hot loops. ``shape`` carries the
    resolved apply regime and certified-shape verdict the report stamps.
    Any failure — boot, spawn, exit status, payload, or a missed anchor —
    raises after killing both workers and the cluster.
    """
    started_at = datetime.now(UTC).isoformat()
    start = time.perf_counter()
    from examples.spatial import messages as spatial_messages

    from tools.agent.subprocess_cluster_host import _child_env, subprocess_cluster_factory

    clients_a, clients_b = split_clients(profile.actor_count)
    instrumented_a, instrumented_b = split_instrumented(_INSTRUMENTED_SAMPLE_SIZE)
    shares = ((0, clients_a, instrumented_a), (1, clients_b, instrumented_b))

    handshakes: list[tuple[int, int]] = []
    server_pids: list[int] = []
    boot = subprocess_cluster_factory(
        instance_count=instance_count,
        ahc_event_history_size=_INSTRUMENTED_HISTORY_SIZE,
        replication_batch_type_id=spatial_messages.ACTOR_STATE_BATCH_TYPE,
        python_workers=python_workers,
        delivery_strategy=delivery_strategy,
        delivery_workers=delivery_workers,
        tiered_max_gap_ms=tiered_max_gap_ms,
        instrumented_count=_INSTRUMENTED_SAMPLE_SIZE,
        bulk_event_history_size=_BULK_HISTORY_SIZE,
        server_pids_out=server_pids,
        server_affinity=server_affinity,
        handshake_out=handshakes,
    )
    orch = boot()
    sampler: ProcSampler | None = None
    stopped = False
    procs: list[subprocess.Popen[str]] = []
    try:
        now_ns = time.monotonic_ns()
        lead_ns = int((max(profile.settle_s * 4.0, 2.0) + _ANCHOR_LEAD_MARGIN_S) * 1e9)
        anchors = [now_ns + lead_ns + index * stagger_ms * 1_000_000 for index in (0, 1)]
        with tempfile.TemporaryDirectory(prefix="kith-dual-drivers-") as tmpdir:
            for index, clients, instrumented in shares:
                procs.append(
                    subprocess.Popen(
                        _worker_argv(
                            index,
                            profile,
                            clients,
                            instrumented,
                            handshakes[index][0],
                            handshakes[index][1],
                            anchors[index],
                            os.path.join(tmpdir, f"driver-{index}.json"),
                            (driver_child_cpus_a, driver_child_cpus_b)[index],
                            echo_settle_s,
                        ),
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.PIPE,
                        text=True,
                        env=_child_env(),
                    )
                )
            sampler = ProcSampler.start(_dual_targets(server_pids, [proc.pid for proc in procs]))
            outputs = [proc.communicate() for proc in procs]

            results: list[DriverResult] = []
            problems: list[str] = []
            for index, proc in enumerate(procs):
                label = f"driver-{'ab'[index]}"
                if proc.returncode != 0:
                    tail = (outputs[index][1] or "").strip().splitlines()[-5:]
                    problems.append(
                        f"{label} exited {proc.returncode}; stderr tail:\n  " + "\n  ".join(tail)
                    )
                    continue
                path = os.path.join(tmpdir, f"driver-{index}.json")
                try:
                    result = load_driver_result(path)
                except (OSError, ValueError, KeyError, TypeError) as exc:
                    problems.append(f"{label} payload unusable: {exc}")
                    continue
                results.append(result)
            if len(results) != len(procs):
                raise RuntimeError("dual-driver run failed:\n" + "\n".join(problems or ["unknown"]))
            problems.extend(_anchor_problems(results, anchors, stagger_ms))
            if problems:
                raise RuntimeError("dual-driver run failed:\n" + "\n".join(problems))

        trace = sampler.stop()
        stopped = True
        if proc_trace_output is not None:
            _write_proc_trace(
                proc_trace_output,
                trace,
                [s for r in sorted(results, key=lambda x: x.instance_index) for s in r.series],
            )
        report = merge_driver_results(
            profile,
            results,
            trace,
            started_at=started_at,
            duration_s=time.perf_counter() - start,
            invocation=invocation,
            limits=limits,
        )
        # The run-level stamps ride here, not inside the pure merge: the
        # envelope and the run shape describe the run, not the pooled
        # counts.
        return replace(report, envelope=_collect_run_envelope(), shape=shape)
    except BaseException:
        for proc in procs:
            if proc.poll() is None:
                proc.kill()
                proc.wait()
        raise
    finally:
        if sampler is not None and not stopped:
            sampler.stop()
        await orch.shutdown()
