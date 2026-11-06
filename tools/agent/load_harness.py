"""Scaling-gate load harness for the numeric §4.4 gates.

A dedicated headless-client load harness that drives the 1000- and
2000-actor profiles documented in ``AGENTS.md`` §4.4 and asserts the
numeric gates the framework must pass before claiming the N² ceiling is
broken. This is a separate effort from the closed-loop scenarios in
:mod:`tools.agent.runner` (which drive 2-3 clients): the harness registers
N headless clients, walks them through the login bootstrap, drives a
movement phase at a configurable cadence, and samples the §4.4 metrics
(``session_ok``, ``selected_clients``, ``move_missing_ratio``,
``bootstrap_ms_p95``, ``continuity_flicker``, ``publish_rate_skew``) against
the real replication stream the gateway delivers.

The harness drives an :class:`~tools.agent.orchestrator.Orchestrator`
factory (a callable that boots a fresh orchestrator bound to a server):
the harness uses orchestrator-specific surfaces (the server control
plane and the ``shutdown`` hook) beyond the
:class:`~tools.agent.scenario.ScenarioHost` protocol the scenario runner
needs. The dense-1000 gate runs against the embedded topology via
:func:`~tools.agent.embedded_host.embedded_host_factory`; the
distributed-2000 gate runs against the distributed topology via
:func:`~tools.agent.subprocess_cluster_host.subprocess_cluster_factory`
(one OS process per instance, so the harness's client-drive loop is
decoupled from the servers' worker-pool GILs), and the smoke
distributed profile uses the in-process
:func:`~tools.agent.distributed_host.distributed_host_factory`
(multiple instances plus a coordination bus wired in-process with
loopback plus ``add_member``) for fast machinery validation.

Every-commit vs. weekly: the AGENTS.md §4.1 tier table lists the 1000-plus
connection stress tier as Weekly, not every commit. The integration test in
``tests/integration/test_scaling_gates.py`` drives reduced
:data:`SMOKE_DENSE` and :data:`SMOKE_DISTRIBUTED` profiles (24 actors,
short duration) validating the harness machinery end-to-end; the full
:data:`DENSE_1000` and :data:`DISTRIBUTED_2000` profiles run via the CLI
(``python -m tools.agent.load_harness --profile dense-1000 --embedded``
or ``--profile distributed-2000 --distributed``) in the weekly tier.

The harness reuses the real :class:`~tools.agent.ahc.AgenticHeadlessClient`
over the real wire (no stress-only client): the gate exercises the full
stack (TCP accept, wire dispatch, sim publish, fabric replication, gateway
delivery) through the production code path whose scaling is being gated.
"""

from __future__ import annotations

from tools.agent._load_harness.arguments import (
    FORMATS,
    OBSERVER_RECALIBRATION_ENV,
    PROFILES,
    apply_client_override,
    apply_delivery_override,
    apply_delivery_workers_override,
    apply_tiered_max_gap_override,
    apply_worker_override,
    build_parser,
    env_python_workers,
    observer_limits_from_args,
    validate_driver_flags,
)
from tools.agent._load_harness.breadth import (
    BreadthRow,
    assemble_breadth,
    assemble_with_instances,
    collect_breadth,
    collect_window_evidence,
    finish_breadth,
    isolated_actor_ids,
    isolation_cause,
    measure_publish_rate_skew,
    prepare_breadth_rows,
    probe_breadth_rows,
    publish_rate_counts,
    residue_rows,
    roster_positions,
)
from tools.agent._load_harness.cli import main as main
from tools.agent._load_harness.constants import (
    BOOTSTRAP_READY,
    BREADTH_EVIDENCE_BUDGET_BYTES,
    BREADTH_WINDOW_DRAIN_S,
    BREADTH_WINDOW_MAX_GAP_S,
    BULK_HISTORY_SIZE,
    DEFAULT_STAGGER_MS,
    FLICKER_GAP_MS,
    INSTRUMENTED_HISTORY_SIZE,
    INSTRUMENTED_SAMPLE_SIZE,
    ISOLATED_FRACTION_MAX,
    MOVE_INPUT,
    POLL_INTERVAL_S,
    POST_WINDOW_ECHO_SETTLE_S,
    QUERY_STATE_ERRORS,
    SPREAD_PROBE_SIZE,
    SUBMIT_YIELD_EVERY,
)
from tools.agent._load_harness.counters import (
    ABSENT_PHASE_COUNTERS,
    PHASE_METRIC_NAMES,
    PHASE_PATTERNS,
    SAMPLER_INTERVAL_S,
    PhaseCounters,
    WindowCounters,
    parse_phase_counters,
    phase_counters_delta,
    run_counter_sampler,
    scrape_phase_counters,
)
from tools.agent._load_harness.counters import CounterSample as CounterSample
from tools.agent._load_harness.depth import (
    COVERAGE_TRUNCATED,
    attach_backlog,
    attach_ledger,
    certified_metrics,
    client_view_facts,
    client_view_set_healthy,
    collect_cohort_depth,
    collect_depth_metrics,
    count_certified_missing,
    distinct_non_self,
    echo_delta_histogram,
    flicker_breakdown_from_series,
    instance_gate_metrics,
    measure_move_missing_ratio,
    measure_selected_clients_ratio,
    per_instance_ranks,
    principal_bindings,
    resolve_own_actors_dense,
    select_spread_probe_indices,
    start_concurrency,
    view_evidence_fresh,
    window_stream_continuous,
)
from tools.agent._load_harness.drive_support import (
    movement_anchor_delay_s,
    sampler_targets,
    submit_input,
    write_proc_trace,
)
from tools.agent._load_harness.driver import DriveOutcome, LoadHarness, run_profile
from tools.agent._load_harness.hosts import (
    distributed_factory,
    embedded_factory,
    external_factory,
    subprocess_cluster_factory,
)
from tools.agent._load_harness.metrics import BreadthAttribution as BreadthAttribution
from tools.agent._load_harness.metrics import BreadthCensus as BreadthCensus
from tools.agent._load_harness.metrics import ClientDepthRow as ClientDepthRow
from tools.agent._load_harness.metrics import (
    ClientFlickerSample,
    EchoDeltaHistogram,
    FlickerBreakdown,
    GateMetrics,
)
from tools.agent._load_harness.metrics import CohortDepth as CohortDepth
from tools.agent._load_harness.metrics import CohortRow as CohortRow
from tools.agent._load_harness.metrics import InstanceGateMetrics as InstanceGateMetrics
from tools.agent._load_harness.metrics import SparseBreadthRow as SparseBreadthRow
from tools.agent._load_harness.profiles import (
    DENSE_1000,
    DENSE_THRESHOLDS,
    DISTRIBUTED_2000,
    DISTRIBUTED_THRESHOLDS,
    MIN_DISTINCT_OTHERS,
    PROFILE_THRESHOLDS,
    SMOKE_DENSE,
    SMOKE_DISTRIBUTED,
    SMOKE_DISTRIBUTED_THRESHOLDS,
    SMOKE_THRESHOLDS,
    GateBars,
    GateThresholds,
    ScalingProfile,
    meets_bars,
)
from tools.agent._load_harness.profiles import thresholds_for as thresholds_for
from tools.agent._load_harness.records import ClientRecord, LoadHostFactory
from tools.agent._load_harness.records import MovementRound as MovementRound
from tools.agent._load_harness.records import OfferedLoad as OfferedLoad
from tools.agent._load_harness.render import (
    dominant_phase,
    dominant_subphase,
    fmt_move_missing_max,
    fmt_skew,
    fmt_skew_max,
    format_spread,
    percentile_at,
    render_backlog,
    render_cohorts,
    render_counter_samples,
    render_echo_delta,
    render_flicker_breakdown,
    render_flicker_ledger,
    render_gap_attribution,
    render_instance_rows,
    render_observation,
    render_phase_counters,
    table_row,
    verdict_label,
)
from tools.agent._load_harness.report import GateReport
from tools.agent._load_harness.runtime import RunEnvelope as RunEnvelope
from tools.agent._load_harness.runtime import RunShape as RunShape
from tools.agent._load_harness.runtime import (
    collect_run_envelope,
    evaluate_certified_shape,
    read_build_type,
    read_gil_status,
    resolve_apply_regime,
)
from tools.agent._load_harness.statistics import percentile
from tools.agent.gap_ledger import FlickerLedger
from tools.agent.observer_gates import ObservationVerdict as ObservationVerdict
from tools.agent.proc_sampler import ProcTrace as ProcTrace


_ABSENT_PHASE_COUNTERS = ABSENT_PHASE_COUNTERS
_BOOTSTRAP_READY = BOOTSTRAP_READY
_BREADTH_EVIDENCE_BUDGET_BYTES = BREADTH_EVIDENCE_BUDGET_BYTES
_BREADTH_WINDOW_DRAIN_S = BREADTH_WINDOW_DRAIN_S
_BREADTH_WINDOW_MAX_GAP_S = BREADTH_WINDOW_MAX_GAP_S
_BULK_HISTORY_SIZE = BULK_HISTORY_SIZE
_BreadthRow = BreadthRow
_COVERAGE_TRUNCATED = COVERAGE_TRUNCATED
_ClientRecord = ClientRecord
_DEFAULT_STAGGER_MS = DEFAULT_STAGGER_MS
_DriveOutcome = DriveOutcome
_FLICKER_GAP_MS = FLICKER_GAP_MS
_FORMATS = FORMATS
_GateBars = GateBars
_INSTRUMENTED_HISTORY_SIZE = INSTRUMENTED_HISTORY_SIZE
_INSTRUMENTED_SAMPLE_SIZE = INSTRUMENTED_SAMPLE_SIZE
_ISOLATED_FRACTION_MAX = ISOLATED_FRACTION_MAX
_MIN_DISTINCT_OTHERS = MIN_DISTINCT_OTHERS
_MOVE_INPUT = MOVE_INPUT
_OBSERVER_RECALIBRATION_ENV = OBSERVER_RECALIBRATION_ENV
_PHASE_METRIC_NAMES = PHASE_METRIC_NAMES
_PHASE_PATTERNS = PHASE_PATTERNS
_POLL_INTERVAL_S = POLL_INTERVAL_S
_POST_WINDOW_ECHO_SETTLE_S = POST_WINDOW_ECHO_SETTLE_S
_PROFILES = PROFILES
_PROFILE_THRESHOLDS = PROFILE_THRESHOLDS
_QUERY_STATE_ERRORS = QUERY_STATE_ERRORS
_SAMPLER_INTERVAL_S = SAMPLER_INTERVAL_S
_SPREAD_PROBE_SIZE = SPREAD_PROBE_SIZE
_SUBMIT_YIELD_EVERY = SUBMIT_YIELD_EVERY
_WindowCounters = WindowCounters
_anchor_delay_s = movement_anchor_delay_s
_apply_client_override = apply_client_override
_apply_delivery_override = apply_delivery_override
_apply_delivery_workers_override = apply_delivery_workers_override
_apply_tiered_max_gap_override = apply_tiered_max_gap_override
_apply_worker_override = apply_worker_override
_assemble_breadth = assemble_breadth
_assemble_with_instances = assemble_with_instances
_attach_backlog = attach_backlog
_attach_ledger = attach_ledger
_build_parser = build_parser
_build_type = read_build_type
_certified_metrics = certified_metrics
_certified_missing = count_certified_missing
_certified_shape = evaluate_certified_shape
_client_view_facts = client_view_facts
_client_view_set_healthy = client_view_set_healthy
_collect_breadth = collect_breadth
_collect_cohort_depth = collect_cohort_depth
_collect_depth_metrics = collect_depth_metrics
_collect_run_envelope = collect_run_envelope
_collect_window_evidence = collect_window_evidence
_distinct_non_self = distinct_non_self
_distributed_factory = distributed_factory
_dominant_phase = dominant_phase
_dominant_subphase = dominant_subphase
_echo_delta_histogram = echo_delta_histogram
_embedded_factory = embedded_factory
_env_python_workers = env_python_workers
_external_factory = external_factory
_finish_breadth = finish_breadth
_flicker_breakdown_from_series = flicker_breakdown_from_series
_fmt_move_missing_max = fmt_move_missing_max
_fmt_skew = fmt_skew
_fmt_skew_max = fmt_skew_max
_gil_status = read_gil_status
_instance_gate_metrics = instance_gate_metrics
_isolated_actor_ids = isolated_actor_ids
_isolation_cause = isolation_cause
_meets_bars = meets_bars
_move_missing_ratio = measure_move_missing_ratio
_observer_limits_from_args = observer_limits_from_args
_parse_phase_counters = parse_phase_counters
_pass = verdict_label
_pct = percentile_at
_per_instance_ranks = per_instance_ranks
_percentile = percentile
_phase_counters_delta = phase_counters_delta
_prepare_breadth_rows = prepare_breadth_rows
_principal_bindings = principal_bindings
_probe_breadth_rows = probe_breadth_rows
_publish_rate_counts = publish_rate_counts
_publish_rate_skew = measure_publish_rate_skew
_render_backlog = render_backlog
_render_cohorts = render_cohorts
_render_counter_samples = render_counter_samples
_render_echo_delta = render_echo_delta
_render_flicker_breakdown = render_flicker_breakdown
_render_flicker_ledger = render_flicker_ledger
_render_gap_attribution = render_gap_attribution
_render_instance_rows = render_instance_rows
_render_observation = render_observation
_render_phase_counters = render_phase_counters
_residue_rows = residue_rows
_resolve_apply_regime = resolve_apply_regime
_resolve_own_actors_dense = resolve_own_actors_dense
_roster_positions = roster_positions
_row = table_row
_run_counter_sampler = run_counter_sampler
_sampler_targets = sampler_targets
_scrape_phase_counters = scrape_phase_counters
_selected_clients_ratio = measure_selected_clients_ratio
_spread = format_spread
_spread_probe_indices = select_spread_probe_indices
_start_concurrency = start_concurrency
_submit_input = submit_input
_subprocess_cluster_factory = subprocess_cluster_factory
_validate_driver_flags = validate_driver_flags
_view_evidence_fresh = view_evidence_fresh
_window_stream_continuous = window_stream_continuous
_write_proc_trace = write_proc_trace


__all__ = [
    "DENSE_1000",
    "DENSE_THRESHOLDS",
    "DISTRIBUTED_2000",
    "DISTRIBUTED_THRESHOLDS",
    "SMOKE_DENSE",
    "SMOKE_DISTRIBUTED",
    "SMOKE_DISTRIBUTED_THRESHOLDS",
    "SMOKE_THRESHOLDS",
    "ClientFlickerSample",
    "EchoDeltaHistogram",
    "FlickerBreakdown",
    "FlickerLedger",
    "GateMetrics",
    "GateReport",
    "GateThresholds",
    "LoadHarness",
    "LoadHostFactory",
    "PhaseCounters",
    "ScalingProfile",
    "run_profile",
]


if __name__ == "__main__":
    raise SystemExit(main())
