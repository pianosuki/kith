"""Value records for breadth, depth and per-instance gate measurements."""

from __future__ import annotations

from dataclasses import dataclass

from tools.agent.gap_ledger import (
    FlickerLedger,
)

from .counters import CounterSample, PhaseCounters


@dataclass(frozen=True, slots=True)
class BreadthAttribution:
    """One isolated client's cause attribution.

    ``klass`` names why the client's own actor sits outside the window-end
    pile geometry: ``late_bootstrap`` (bound after the boot-poll deadline,
    never stamped or moved), ``disconnected`` (session dead at
    collection), ``stalled`` (connected, never submitted), ``frozen``
    (connected, submitted, actor left behind). ``fresh`` marks a client
    whose movement-window evidence alone satisfied the view bar — the
    live divergence signature where isolation implies cross-cell
    geometry. The render tag claims only that window fact: a
    ``fresh=False`` entry held no window evidence (its window stream
    never carried the bar).
    """

    name: str
    klass: str
    fresh: bool


@dataclass(frozen=True, slots=True)
class SparseBreadthRow:
    """One breadth-residue client's two distinct-id counts.

    The residue is the cohort the census cannot attribute to isolation,
    a stall, or an incomplete basis: bound, non-isolated, unobserved on
    a complete basis. ``klass`` names the residue class — ``sparse``
    (a window present under the bar) versus ``never_opened`` (no
    window). ``distinct_in_window`` and ``distinct_deque`` exclude the
    client's own actor id; the deque count ignores the window bounds,
    so a sparse read splits into a window younger than the retained
    roster versus a roster the retained stream never carried.
    """

    name: str
    instance_index: int
    klass: str
    distinct_in_window: int
    distinct_deque: int


@dataclass(frozen=True, slots=True)
class BreadthCensus:
    """Result of the collection-time breadth pass over one drive's records.

    The denominator split (``classified_clients`` / ``isolated_clients`` /
    ``eligible_clients``) classifies every client the collection-time
    binding map binds — including one that completed bootstrap after the
    pre-movement snapshot — against the window-end roster; both counts are
    ``None`` when no control plane exposes a roster. ``selected_count`` is
    contained by construction: only classified non-isolated clients that
    observed the view bar count, so ``selected_count <= eligible_clients``
    holds structurally. Without a roster the numerator keeps its legacy
    full-population reading. ``violation`` carries the measured guardrail
    breach (isolated fraction above :data:`_ISOLATED_FRACTION_MAX`, or any
    fresh-evidence-but-isolated client) rendered for the verdict; the
    report shows it and the gate fails on it.
    """

    total: int = 0
    session_ok_count: int = 0
    classified_clients: int | None = None
    isolated_clients: int | None = None
    eligible_clients: int | None = None
    selected_count: int = 0
    selected_head_count: int = 0
    observed_but_isolated_fresh_clients: int = 0
    unbound_clients: int = 0
    late_bootstrapped_clients: int = 0
    # Clients whose window stream gapped past the stall bound after
    # clearing the bar on distinct ids (the guard that keeps a stream
    # which died mid-window from reading as delivered).
    stalled_clients: int = 0
    # Clients whose retained-stream basis cannot prove completeness (the
    # deque evicted under the window, or no byte budget): they read
    # unobserved and are counted here so a shrunken basis is loud, never
    # silently conflated with a sparse stream.
    incomplete_evidence_clients: int = 0
    # The residue the census cannot attribute to isolation, a stall, or
    # an incomplete basis: bound, non-isolated, unobserved on a complete
    # basis. Split by window presence and carried per client with both
    # distinct-id counts so a failing run closes its own residue.
    sparse_clients: int = 0
    never_opened_clients: int = 0
    sparse_rows: tuple[SparseBreadthRow, ...] = ()
    # Evidence-deque span across clients (min and p50, ns): the retained
    # payload record's reach versus the window it summarizes. A span
    # under the window length is expected (eviction) and is why the
    # probe counts the ingest-side accumulator; the margin is rendered
    # so a shrinking deque is visible instead of silent.
    evidence_span_ns_min: int | None = None
    evidence_span_ns_p50: int | None = None
    violation: str | None = None
    attribution: tuple[BreadthAttribution, ...] = ()
    # Per-instance censuses over the same rows, one per instance the
    # clients landed on in instance order (``instance_index`` marks each;
    # a per-instance census nests no further rows). The pooled census
    # stays the union read; the distributed drive judges these rows per
    # instance.
    instances: tuple[BreadthCensus, ...] = ()
    instance_index: int | None = None


@dataclass(frozen=True, slots=True)
class ClientFlickerSample:
    """Continuity-flicker detail for one instrumented client.

    ``gap_ms`` holds the duration of each >200 ms gap between consecutive
    own-actor frames (in detection order, ascending in time); ``gap_start_ms``
    holds each gap's start, offset from the client's movement-window start,
    so inter-gap spacing within a client reveals a periodic stall versus a
    clustered/random one without carrying absolute timestamps. Only clients
    that flickered appear in a breakdown, so the entry count relative to
    ``GateMetrics.sampled_clients`` signals a uniform burst (every client)
    versus a concentrated one (a few clients).
    """

    name: str
    instance_index: int
    gap_count: int
    gap_ms: tuple[int, ...]
    gap_start_ms: tuple[int, ...]


@dataclass(frozen=True, slots=True)
class EchoDeltaHistogram:
    """Consecutive-echo input_tick deltas across the instrumented cohort.

    Built from the already-retained own-actor series: for each instrumented
    client, the delta between consecutive echoed ``input_tick`` values
    (frames time-ordered) quantifies the coalescing upstream of compose —
    ``delta1`` echoes carried one applied tick (clean), ``delta2`` implies
    one tick applied between two compose reads and never echoed
    (permanently lost), and the larger buckets measure how deep the
    coalescing runs. ``regressed`` counts non-increasing deltas —
    retained-view re-delivery repeating an already-echoed tick, the
    cadence contract's normal shape under change-suppression delivery
    (self subjects deliver every pass whether or not their tick
    advanced). Only a series that never advances while repeats
    accumulate condemns the echo path. ``missing_implied`` is the sum of
    ``delta - 1`` over the positive deltas — the between-echo loss the
    move-missing ratio should reconcile against (boundary losses before
    the first and after the last echo are invisible here by construction).

    ``coverage_min`` is the smallest per-client fraction of the movement
    window spanned by that client's frames: a coverage far below one means
    the client's 8192-event history deque evicted early frames, so its
    distinct-tick scan (and the move-missing numerator) is truncated —
    ``truncated_clients`` counts them (coverage under 0.98).
    """

    clients: int
    frames: int
    delta1: int
    delta2: int
    delta3_5: int
    delta6_20: int
    delta21_plus: int
    regressed: int
    missing_implied: int
    coverage_min: float
    truncated_clients: int


@dataclass(frozen=True, slots=True)
class FlickerBreakdown:
    """Per-client continuity-flicker distribution across the instrumented sample.

    ``total_gaps`` is the aggregate count the boolean
    :attr:`GateMetrics.continuity_flicker` derives from; ``per_client``
    carries the per-client detail (gaps only) so the distribution
    distinguishes a systematic burst hitting every client from a concentrated
    burst on a few. Empty when no instrumented client flickered.
    """

    total_gaps: int
    per_client: tuple[ClientFlickerSample, ...]


@dataclass(frozen=True, slots=True)
class InstanceGateMetrics:
    """The gate metrics one instance's client cohort measured.

    The distributed gate binds its thresholds per instance, so each
    instance's cohort reduces to the same bar-facing shape the pooled row
    reports. An instance below any bar fails the run even when the union
    pools above it — a weak cohort cannot hide in the pooled ratio.
    ``move_missing_ratio_certified`` is ``None`` when the instance's
    instrumented cohort submitted nothing (no certifiable series), which
    fails the certified bar: an unmeasured instance is not a passing one.
    """

    instance_index: int
    actor_count: int
    sampled_clients: int
    session_ok_ratio: float
    selected_clients_ratio: float
    eligible_clients: int | None
    isolated_clients: int | None
    classified_clients: int | None
    move_missing_ratio: float
    move_missing_ratio_certified: float | None
    certified_fallback_clients: int
    bootstrap_ms_p95: int
    continuity_flicker: bool


@dataclass(frozen=True, slots=True)
class GateMetrics:
    """Computed §4.4 metrics over one harness run.

    ``session_ok_ratio`` and ``selected_clients_ratio`` are measured across
    every registered client (breadth); ``move_missing_ratio`` and
    ``continuity_flicker`` are measured on the deeply instrumented sample
    (depth) whose event history holds the full movement-window stream.
    ``publish_rate_skew`` is the max/min ratio of per-instance publish
    rates (0.0 on the single-instance dense topology; populated by the
    distributed drive). ``flicker_count`` is the number of 200 ms
    continuity-flicker gaps across the instrumented sample (a non-gated
    diagnostic; the boolean ``continuity_flicker`` derives from
    ``flicker_count > 0``).     ``flicker_breakdown`` carries the per-client
    gap distribution behind that count (None on the legacy dense path that
    did not compute it; the gap sizes and per-client offsets let a run
    distinguish a systematic burst from a concentrated one and a periodic
    stall from a multi-tick blackout). ``flicker_ledger`` carries the
    diagnostic attachment behind the distribution — per-instance gap
    clusters with their cross-client arrival-spread signatures, the /proc
    attribution of each cluster window, and the per-tick arrival-spread
    baseline (None when the run produced no instrumented series).
    """

    actor_count: int
    sampled_clients: int
    session_ok_ratio: float
    selected_clients_ratio: float
    move_missing_ratio: float
    bootstrap_ms_p95: int
    continuity_flicker: bool
    publish_rate_skew: float = 0.0
    flicker_count: int = 0
    flicker_breakdown: FlickerBreakdown | None = None
    phase_counters: tuple[PhaseCounters, ...] = ()
    flicker_ledger: FlickerLedger | None = None
    # Breadth-metric eligibility geometry (None when no control plane made
    # a window-end roster available and the ratio keeps its full
    # population). ``classified_clients`` counts the clients the
    # collection-time binding map binds; ``isolated_clients`` counts those
    # whose own actor had fewer than ``min_distinct_others`` other actors
    # inside the radius-1 cell neighborhood at window end — structurally
    # unobservable breadth that is reported rather than counted against
    # the server. ``eligible_clients`` is the denominator actually used:
    # classified minus isolated. The numerator is computed over that same
    # cohort, so containment holds by construction; a client that
    # observed the view bar while isolated is dropped from both sides and
    # rendered instead (``observed_but_isolated_fresh_clients`` and the
    # per-client attribution), where a fresh-evidence occurrence breaches
    # the divergence guardrail in the topologies where isolation implies
    # cross-cell geometry. Per-client cause attribution renders for
    # every isolated client (``breadth_attribution``). ``unbound_clients``
    # counts records the collection-time map does not bind (they cannot
    # hold views); ``late_bootstrapped_clients`` counts bound clients the
    # pre-movement boot snapshot missed.
    eligible_clients: int | None = None
    isolated_clients: int | None = None
    classified_clients: int | None = None
    observed_but_isolated_fresh_clients: int = 0
    unbound_clients: int = 0
    late_bootstrapped_clients: int = 0
    breadth_attribution: tuple[BreadthAttribution, ...] = ()
    # Clients whose window stream gapped past the stall bound after
    # clearing the distinct-id bar (delivery break, verdict-affecting
    # through the observed reading; rendered for diagnosis).
    breadth_stalled_clients: int = 0
    # Clients whose retained-stream basis cannot prove completeness
    # (deque eviction under the window, or no byte budget): rendered so a
    # shrunken basis is visible instead of silently reading as sparse.
    breadth_incomplete_clients: int = 0
    # The breadth residue (bound, non-isolated, unobserved on a complete
    # basis) and its per-client counts: rendered so a failing run carries
    # the window-versus-deque split that closes the residue.
    breadth_sparse_clients: int = 0
    breadth_never_opened_clients: int = 0
    breadth_sparse_rows: tuple[SparseBreadthRow, ...] = ()
    # Evidence-deque span across clients (min/p50, ns): the forensic
    # payload record's reach versus the movement window it summarizes.
    breadth_evidence_span_ns_min: int | None = None
    breadth_evidence_span_ns_p50: int | None = None
    # Every client's submitted-input total, the numerator of the
    # submit-rounds-per-client diagnostic (planned rounds come from the
    # profile's duration and cadence).
    inputs_submitted: int = 0
    # Measured text of a breadth-guardrail breach (isolated fraction above
    # its bound): renders with the report and fails the verdict instead of
    # aborting collection.
    breadth_violation: str | None = None
    # Consecutive-echo input_tick deltas over the instrumented cohort, the
    # coalescing quantification behind the move-missing ratio (None when
    # the run produced no instrumented series).
    echo_delta: EchoDeltaHistogram | None = None
    # Per-client submit-round spread (min/median/max of inputs_submitted
    # across registered clients): the observer-side cadence evidence the
    # aggregate rounds figure averages away. Zero when no client record
    # exists; the dual-driver pooled report leaves it zero (per-client
    # counts live in the children).
    rounds_min: int = 0
    rounds_median: int = 0
    rounds_max: int = 0
    backlog_pre_drain_median_ms: int | None = None
    backlog_pre_drain_p95_ms: int | None = None
    backlog_post_drain_median_ms: int | None = None
    backlog_post_drain_p95_ms: int | None = None
    # In-window /metrics snapshots from the concurrent counter sampler
    # (raw cumulative readings per instance; empty when no movement window
    # drove or the control plane was unavailable).
    counter_samples: tuple[CounterSample, ...] = ()
    # Selected count within the head depth sample: the same breadth test
    # restricted to clients whose retention the run config does not
    # re-scope, so the ratio stays comparable across runs (the spread
    # probe upgrades 32 clients' retention, which can only raise the
    # all-client ratio). Zero when the head sample is empty.
    selected_head_count: int = 0
    # Sample-position cohort depth detail (diagnostic; None when the
    # spread probe is disabled or the cohort produced no booted clients).
    head_cohort: CohortDepth | None = None
    spread_cohort: CohortDepth | None = None
    # Certified move-missing ratio over the instrumented sample (the
    # counter-delta rule; None when no client submitted anything).
    # ``certified_fallback_clients`` counts instrumented clients whose self
    # echoes carried no minted counter — the publisher fell back to
    # event-counting for them.
    move_missing_ratio_certified: float | None = None
    certified_fallback_clients: int = 0
    # Per-instance metric rows (the distributed drive and the dual-driver
    # merge populate one row per instance; the single-instance dense drive
    # leaves it empty — the pooled row is the instance row). The verdict
    # requires every row to clear the bars alongside the pooled row.
    instances: tuple[InstanceGateMetrics, ...] = ()


@dataclass(frozen=True, slots=True)
class ClientDepthRow:
    """One instrumented client's depth counts (both move-missing rules).

    ``certified_missing`` is the client's missing count under the
    certified counter-delta rule; ``certified_fallback`` marks a client
    whose self echoes carried no minted counter at all (the publisher
    fell back to event-counting for it).
    """

    name: str
    submitted: int
    observed: int
    certified_missing: int
    certified_fallback: bool


@dataclass(frozen=True, slots=True)
class CohortRow:
    """One client's depth row inside a named cohort (diagnostic).

    ``per_instance_rank`` is the client's position within its instance's
    submit sequence — the coordinate the pool's accept/drop boundary lives
    in; rows on either side of the boundary are expected to separate.
    ``certified_missing`` / ``certified_fallback`` carry the client's
    certified-rule reading (see :func:`_certified_missing`).
    """

    name: str
    instance_index: int
    per_instance_rank: int
    submitted: int
    observed: int
    certified_missing: int = 0
    certified_fallback: bool = False


@dataclass(frozen=True, slots=True)
class CohortDepth:
    """Depth metrics for one named client cohort (diagnostic).

    ``move_missing`` uses the gate metric's definition (distinct echoed
    input ticks over the full history against submitted inputs) so cohort
    aggregates compare against the gate number directly; ``rows`` carries
    the per-client detail the aggregate averages away.
    ``move_missing_certified`` is the cohort's certified-rule ratio (None
    when the cohort submitted nothing); ``certified_fallback_clients``
    counts members whose publisher never minted.
    """

    name: str
    sampled: int
    submitted: int
    observed: int
    move_missing: float
    rows: tuple[CohortRow, ...]
    move_missing_certified: float | None = None
    certified_fallback_clients: int = 0


BreadthAttribution.__module__ = "tools.agent.load_harness"
SparseBreadthRow.__module__ = "tools.agent.load_harness"
BreadthCensus.__module__ = "tools.agent.load_harness"
ClientFlickerSample.__module__ = "tools.agent.load_harness"
EchoDeltaHistogram.__module__ = "tools.agent.load_harness"
FlickerBreakdown.__module__ = "tools.agent.load_harness"
InstanceGateMetrics.__module__ = "tools.agent.load_harness"
GateMetrics.__module__ = "tools.agent.load_harness"
ClientDepthRow.__module__ = "tools.agent.load_harness"
CohortRow.__module__ = "tools.agent.load_harness"
CohortDepth.__module__ = "tools.agent.load_harness"
