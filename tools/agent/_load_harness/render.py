"""Markdown tables for scaling-gate observations and diagnostics."""

from __future__ import annotations

from collections.abc import Sequence

from tools.agent.gap_ledger import (
    FlickerLedger,
)
from tools.agent.observer_gates import (
    ObservationVerdict,
)

from .counters import SAMPLER_INTERVAL_S, CounterSample, PhaseCounters
from .metrics import EchoDeltaHistogram, FlickerBreakdown, GateMetrics
from .profiles import GateThresholds, meets_bars
from .statistics import percentile


def render_instance_rows(m: GateMetrics, t: GateThresholds) -> str:
    """Render the per-instance metric rows with each row's own verdict.

    The pooled table's threshold column states the bars once; this table
    reads each instance against them, so an instance below any bar is
    visible as its own failing row instead of hiding in the union.
    """
    lines = [
        "",
        "## Per-instance metrics",
        "",
        "| instance | clients | sampled | session_ok | selected_clients | mm_certified | "
        "bootstrap_ms_p95 | flicker | pass |",
        "|---|---:|---:|---:|---:|---:|---:|:---:|:---:|",
    ]
    for inst in m.instances:
        certified = (
            "n/a"
            if inst.move_missing_ratio_certified is None
            else f"{inst.move_missing_ratio_certified:.2%}"
        )
        lines.append(
            f"| {inst.instance_index} | {inst.actor_count} | {inst.sampled_clients} "
            f"| {inst.session_ok_ratio:.2%} | {inst.selected_clients_ratio:.2%} "
            f"| {certified} | {inst.bootstrap_ms_p95} | {inst.continuity_flicker} "
            f"| {verdict_label(meets_bars(inst, t))} |"
        )
    return "\n".join(lines)


def render_observation(verdict: ObservationVerdict) -> str:
    """Render the observer-health verdict and its measured reasons."""
    lines = [
        "## Observer health (gated)",
        "",
        f"Verdict: **{verdict.klass}**",
        "",
    ]
    lines.extend(f"- {reason}" for reason in verdict.reasons)
    if not verdict.reasons:
        lines.append("- every check performed and within limits")
    return "\n".join(lines)


def fmt_skew(value: float) -> str:
    """Render a publish-rate-skew value, 'n/a' when no ratio exists.

    A legitimate skew is a max/min ratio and floors at 1.0; infinity is
    the undefined-ratio sentinel and 0.0 the metric-not-computed default,
    so both render as 'n/a' rather than as a bogus ratio.
    """
    if value == float("inf") or value <= 0.0:
        return "n/a"
    return f"{value:.2f}x"


def fmt_skew_max(value: float) -> str:
    """Render the publish-rate-skew threshold, 'n/a' when not gated."""
    if value == float("inf"):
        return "n/a"
    return f"<= {value:.2f}x"


def fmt_move_missing_max(value: float | None) -> str:
    """Render the move-missing threshold, 'reported' when the metric is not gated."""
    if value is None:
        return "reported"
    return f"< {value:.2%}"


def verdict_label(ok: bool) -> str:
    """Render the pass column for a gated metric: 'yes' or 'no'."""
    return "yes" if ok else "no"


def table_row(name: str, value: object, threshold: str, pass_col: str) -> str:
    return f"| {name} | {value} | {threshold} | {pass_col} |"


def render_flicker_breakdown(breakdown: FlickerBreakdown, sampled_clients: int) -> str:
    """Render the per-client flicker distribution as a compact markdown block.

    Reports how many of the sampled clients flickered (the uniform-vs-
    concentrated signal) and the per-client gap-count and gap-size spreads
    (min/p50/p95/max). The raw per-client detail and the per-gap start
    offsets ride in the JSON for closer analysis; this block is the
    at-a-glance read a gate log carries.
    """
    per_client_counts = [s.gap_count for s in breakdown.per_client]
    all_gap_ms = [g for s in breakdown.per_client for g in s.gap_ms]
    flickering = len(per_client_counts)
    counts_spread = format_spread(per_client_counts)
    sizes_spread = format_spread(all_gap_ms)
    lines = [
        f"**Flicker gaps:** {breakdown.total_gaps} across "
        f"{flickering} of {sampled_clients} sampled clients "
        f"(diagnostic, not gated)  ",
        f"- per-client gaps: {counts_spread}  ",
        f"- gap size (ms): {sizes_spread}  ",
    ]
    return "\n".join(lines)


def render_phase_counters(counters: Sequence[PhaseCounters]) -> str:
    """Render the per-instance reactor-path phase counters as markdown tables.

    Per instance: the movement-window ns spent in each reactor-thread tick
    phase (refresh/compose/deliver, as ms), the socket-drain ns (write ms),
    the drains the per-call output-drain cap truncated (write defr), the
    dispatch and publish totals, and the dominant phase (the largest of
    refresh/compose/deliver/write); then the same window's ns inside the
    compose phase split into the six sub-phases with their dominant member,
    plus the skip and deferral counts. The refresh/compose/deliver/write
    split localizes which tick phase dominates the per-tick delivery cost
    the capacity model reconciles against the tick budget; the sub-phase
    split localizes where inside that dominant phase the time goes.
    """
    lines = [
        "## Reactor-path phase counters (movement window, diagnostic)",
        "",
        "Per-instance cumulative ns in each reactor-thread tick phase over the "
        "movement window, plus the socket-drain ns, the drains the per-call "
        "output-drain cap truncated, and the dispatch and publish totals "
        "(diagnostic, not gated).",
        "",
        "| Instance | refresh ms | compose ms | deliver ms | write ms | write "
        "defr | dispatches | publishes | dominant |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---|",
    ]
    for i, c in enumerate(counters):
        lines.append(
            f"| {i} | {c.refresh_ns // 1_000_000} | {c.compose_ns // 1_000_000} "
            f"| {c.deliver_ns // 1_000_000} | {c.write_ns // 1_000_000} "
            f"| {c.write_deferrals} | {c.dispatches} | {c.publishes} "
            f"| {dominant_phase(c)} |"
        )
    lines.extend(
        [
            "",
            "### Compose sub-phase split (movement window, diagnostic)",
            "",
            "Per-instance cumulative ns inside the compose phase over the movement "
            "window (diagnostic, not gated). The six sub-phases bound the compose "
            "time from below rather than partitioning it; lock-wait is "
            "reactor-side stripe acquisition only. skips counts recompositions "
            "the composer skipped because no composition input changed (a high "
            "count with collapsed sub-phases is the expected steady state). "
            "deferrals counts recompositions a tick deferred past its compose "
            "budget; the deferred session delivered its retained set that tick. "
            "locate-fails counts compositions whose scan phase found no cached "
            "window cell holding the session's bound actor id (healthy systems "
            "report zero forever).",
            "",
            "| Instance | window ms | prior ms | lock-wait ms "
            "| scan ms | sort ms | select ms | skips | deferrals | locate-fails | dominant |",
            "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|",
        ]
    )
    for i, c in enumerate(counters):
        lines.append(
            f"| {i} | {c.compose_window_ns // 1_000_000} | {c.compose_prior_ns // 1_000_000} "
            f"| {c.compose_lock_wait_ns // 1_000_000} | {c.compose_scan_ns // 1_000_000} "
            f"| {c.compose_sort_ns // 1_000_000} | {c.compose_select_ns // 1_000_000} "
            f"| {c.compose_skips} | {c.compose_deferrals} | {c.view_locate_failures} "
            f"| {dominant_subphase(c)} |"
        )
    lines.extend(
        [
            "",
            "### Delivery executor counters (movement window, diagnostic)",
            "",
            "Per-instance delivery-executor counters over the movement window "
            "(diagnostic, not gated). jobs counts submissions to the "
            "executor; inflight-skips counts deliver passes skipped because the "
            "session's previous deliver was still in flight (the total "
            "per-session cadence-gap rate); ebusy counts submissions the task "
            "queue rejected; budget-exhausted and wait-timeouts decompose the "
            "compose-side causes. inflight is the end-of-window gauge sample "
            "(not a delta); peak is the monotonic high-watermark since instance "
            "creation. All zero on an inline run (no executor configured).",
            "",
            "| Instance | jobs | inflight-skips | ebusy | budget-exhausted "
            "| wait-timeouts | inflight | peak |",
            "|---|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for i, c in enumerate(counters):
        lines.append(
            f"| {i} | {c.delivery_jobs} | {c.delivery_inflight_skips} "
            f"| {c.delivery_ebusy_skips} | {c.delivery_wait_budget_exhausted} "
            f"| {c.compose_wait_timeouts} | {c.delivery_inflight_current} "
            f"| {c.delivery_inflight_high_watermark} |"
        )
    lines.extend(
        [
            "",
            "### Worker-pool counters (movement window, diagnostic)",
            "",
            "Per-instance movement-window deltas of the shared worker "
            "pool's counters (diagnostic, not gated). tick-dropped counts "
            "per-tick game-logic callback submissions the pool rejected; "
            "dispatch-dropped counts input-handler submissions it "
            "rejected. wk-submitted / wk-completed are the pool's own "
            "accepted-submission and apply totals, so applies = "
            "dispatches minus dispatch-dropped lands on wk-completed (small "
            "slack = the pool's non-dispatch work). A nonzero "
            "dispatch-dropped means the pool exhausted its submission "
            "capacity under load, so queued-lag effects are drops rather "
            "than latency.",
            "",
            "| Instance | tick-dropped | dispatch-dropped | dispatches "
            "| applies | wk-submitted | wk-completed |",
            "|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for i, c in enumerate(counters):
        lines.append(
            f"| {i} | {c.tick_dropped} | {c.dispatch_dropped} | {c.dispatches} "
            f"| {max(0, c.dispatches - c.dispatch_dropped)} "
            f"| {c.worker_tasks_submitted} | {c.worker_tasks_completed} |"
        )
    lines.extend(
        [
            "",
            "### Delivery and view totals (movement window, diagnostic)",
            "",
            "Per-instance cumulative totals over the movement window "
            "(diagnostic, not gated). frames-enqueued / dropped / event-frames "
            "count the delivery fold's outcome on both delivery paths (inline "
            "and executor alike): dropped is the enqueue-side backpressure "
            "loss, so a nonzero reading condemns delivery rather than "
            "selection. suppressed counts subjects the change-suppression "
            "lever skipped. visits counts delivery-pass visits of sessions "
            "holding a view (executor submissions that later skipped are "
            "included and separately counted above); candidate and selected "
            "sum each visited session's live view metadata, so selected "
            "divided by candidate is the window's selection density, and the "
            "two peaks are the candidate-cap evidence (a candidate peak at "
            "the view budget means the cap binds). high-watermarks are "
            "monotonic since instance creation (end-of-window gauge sample, "
            "not a delta).",
            "",
            "| Instance | frames-enq | dropped | event-frames | suppressed "
            "| visits | candidates | selected | cand-peak | sel-peak |",
            "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for i, c in enumerate(counters):
        lines.append(
            f"| {i} | {c.delivery_frames_enqueued} | {c.delivery_dropped} "
            f"| {c.delivery_event_frames_enqueued} | {c.delivery_suppressed} "
            f"| {c.view_visits} | {c.view_candidates} | {c.view_selected} "
            f"| {c.view_candidate_high_watermark} "
            f"| {c.view_selected_high_watermark} |"
        )
    lines.extend(
        [
            "",
            "### Malformed-input reconciliation (movement window, diagnostic)",
            "",
            "Per-instance rejection counts over the movement window "
            "(diagnostic, not gated). proto-rejections counts frames the "
            "proto decode entry point rejected (malformed header, payload "
            "bound, unknown type, correlation-trailer size); "
            "net-rejections counts connections closed because a frame's "
            "declared total exceeded the input ring ceiling. A frame "
            "reaches at most one of the two, so the sum reconciles the "
            "run's injected malformed input.",
            "",
            "| Instance | proto-rejections | net-rejections |",
            "|---:|---:|---:|",
        ]
    )
    for i, c in enumerate(counters):
        lines.append(f"| {i} | {c.proto_rejections} | {c.net_rejections} |")
    return "\n".join(lines)


def render_counter_samples(samples: Sequence[CounterSample]) -> str:
    """Render the in-window counter snapshots per instance (diagnostic).

    Rows are raw cumulative readings at each sample's offset from the
    window start; drop-frac is the interval's dispatch-drop fraction and
    in-flight is the pool's submitted-minus-completed depth at the sample
    moment — a depth pinned at the pool's node capacity is the saturation
    signature, and the first rows show the fill transient that precedes
    it.
    """
    if not samples:
        return ""
    lines = [
        "### Movement-window counter samples (diagnostic)",
        "",
        f"{len(samples)} in-window snapshots per instance "
        f"({SAMPLER_INTERVAL_S:.2f} s cadence, cumulative values). "
        "in-flight = wk-submitted minus wk-completed at the sample moment; "
        "drop-frac is the interval's share of dispatch-dropped among "
        "dispatched inputs.",
        "",
        "| Instance | t ms | dispatches | dispatch-dropped | wk-submitted "
        "| wk-completed | in-flight | drop-frac |",
        "|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    instance_count = max(len(s.counters) for s in samples)
    for i in range(instance_count):
        prev: PhaseCounters | None = None
        for s in samples:
            if i >= len(s.counters):
                continue
            c = s.counters[i]
            dispatched = c.dispatches - (prev.dispatches if prev else 0)
            dropped = c.dispatch_dropped - (prev.dispatch_dropped if prev else 0)
            drop_frac = f"{dropped / dispatched:.2f}" if dispatched > 0 else "—"
            lines.append(
                f"| {i} | {s.t_rel_ms} | {c.dispatches} | {c.dispatch_dropped} "
                f"| {c.worker_tasks_submitted} | {c.worker_tasks_completed} "
                f"| {c.worker_tasks_submitted - c.worker_tasks_completed} | {drop_frac} |"
            )
            prev = c
    return "\n".join(lines)


def render_backlog(metrics: GateMetrics) -> str:
    """Render the driver inbound-backlog sample (diagnostic, not gated).

    The newest-frame age says how far a client's inbound stream lags the
    server's delivery cadence. The pre-drain sample is the raw inbound
    lag; the post-drain residual is the echo lag beyond the fixed bound
    the drained metric still measures. A post-drain residual near zero means the drain
    covers the pipeline lag; a large residual means the observer itself
    is starved and the run's client-observed metrics are biased.
    """
    lines = [
        "## Driver inbound backlog (newest-frame age, diagnostic)",
        "",
        "Age of each client's most recently received replication frame, "
        "sampled at the movement window's end (pre-drain) and after the "
        "fixed post-window echo drain (post-drain), median and p95 over "
        "every connected client in ms. Null when no client received a "
        "frame.",
        "",
        "| Sample | median ms | p95 ms |",
        "|---|---:|---:|",
    ]
    for label, med, p95 in (
        ("pre-drain", metrics.backlog_pre_drain_median_ms, metrics.backlog_pre_drain_p95_ms),
        (
            "post-drain",
            metrics.backlog_post_drain_median_ms,
            metrics.backlog_post_drain_p95_ms,
        ),
    ):
        med_s = "n/a" if med is None else str(med)
        p95_s = "n/a" if p95 is None else str(p95)
        lines.append(f"| {label} | {med_s} | {p95_s} |")
    return "\n".join(lines)


def render_cohorts(metrics: GateMetrics) -> str:
    """Render the sample-position cohort detail (diagnostic, not gated).

    The gate's depth sample (head: the first clients by registration
    order) sits first in every submit round, so a pool whose accept/drop
    boundary lands past it protects the sample while subsequent positions
    absorb the drops. The spread probe mirrors the head sample with
    evenly spaced probe clients across the remaining submit positions;
    per-instance rank is each client's position within its instance's
    submit sequence, the coordinate the boundary lives in. Cohort mm uses
    the gate metric's definition, so the head row reads against the gate
    number directly; mm_certified is the cohort's certified-rule ratio,
    the reading the gate binds.
    """
    m = metrics
    lines = ["## Sample-position cohorts (diagnostic)", ""]
    if m.selected_head_count and m.sampled_clients:
        ratio = m.selected_head_count / m.sampled_clients
        lines.append(
            f"selected within the head sample: {m.selected_head_count}/"
            f"{m.sampled_clients} ({ratio:.2%}) — the head-sample "
            "composition is stable across runs, unlike the all-client "
            "ratio when the spread probe re-scopes 32 clients' retention."
        )
        lines.append("")
    for cohort in (m.head_cohort, m.spread_cohort):
        if cohort is None:
            continue
        lines.append(f"### Cohort '{cohort.name}' (n={cohort.sampled})")
        lines.append("")
        certified = (
            "n/a"
            if cohort.move_missing_certified is None
            else f"{cohort.move_missing_certified:.2%}"
        )
        fallback_note = (
            f" | certified fallbacks {cohort.certified_fallback_clients}"
            if cohort.certified_fallback_clients
            else ""
        )
        lines.append(
            f"submitted {cohort.submitted} | observed {cohort.observed} "
            f"| mm {cohort.move_missing:.2%} | mm_certified {certified}{fallback_note}"
        )
        lines.append("")
        lines.append("| client | instance | rank | submitted | observed |")
        lines.append("|---|---:|---:|---:|---:|")
        for row in cohort.rows:
            lines.append(
                f"| {row.name} | {row.instance_index} | {row.per_instance_rank} "
                f"| {row.submitted} | {row.observed} |"
            )
        lines.append("")
    return "\n".join(lines)


def render_echo_delta(
    echo: EchoDeltaHistogram, move_missing_ratio: float, inputs_submitted: int
) -> str:
    """Render the echo-delta histogram and its move-missing reconciliation.

    Diagnostic, not gated. The histogram is the coalescing quantification:
    delta 1 means every echo carried exactly one applied tick; delta 2 or
    more means ticks applied between two compose reads (or lost upstream)
    and never echoed. The implied between-echo loss reconciles against the
    move-missing ratio's implied count only approximately: the ratio scans
    the client's full event history and counts distinct ticks, while the
    histogram reads the window-scoped series and counts between-echo gaps,
    so boundary losses and history eviction show up in the ratio but not
    here. A coverage below one flags exactly that eviction.
    """
    mm_implied = round(move_missing_ratio * inputs_submitted)
    lines = [
        "## Echo-delta histogram (instrumented cohort, diagnostic)",
        "",
        "Consecutive-echo input_tick deltas over the instrumented sample's "
        "own-actor frames (time-ordered), the coalescing quantification "
        "behind move-missing: delta 1 is a clean echo; delta 2 implies one "
        "applied tick between two echoes that never reached the client; "
        "larger buckets measure deeper coalescing or upstream loss. "
        "regressed counts non-increasing deltas — retained-view "
        "re-delivery repeating an already-echoed tick, cadence-normal "
        "under change-suppression delivery (only a series that never "
        "advances condemns the echo path). "
        "missing-implied sums delta-1 over positive deltas — the "
        "between-echo loss; move-missing implies "
        f"{mm_implied} missing ticks at the reported ratio over "
        f"{inputs_submitted} submitted, and the two agree only within the "
        "histogram's blind spots (boundary losses, history eviction).",
        "",
        "| clients | frames | delta 1 | delta 2 | delta 3-5 | delta 6-20 "
        "| delta 21+ | regressed | missing-implied |",
        "|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
        f"| {echo.clients} | {echo.frames} | {echo.delta1} | {echo.delta2} "
        f"| {echo.delta3_5} | {echo.delta6_20} | {echo.delta21_plus} "
        f"| {echo.regressed} | {echo.missing_implied} |",
        "",
        f"- min frame-span coverage of the movement window: "
        f"{echo.coverage_min:.3f} (coverage < 0.98 flags a truncated "
        "history — the distinct-tick scan missed early frames, inflating "
        "move-missing)",
        f"- truncated clients (coverage < 0.98): {echo.truncated_clients} of {echo.clients}",
    ]
    return "\n".join(lines)


def dominant_subphase(c: PhaseCounters) -> str:
    """Name the largest of the six compose sub-phases ('-' on an all-zero row)."""
    phases = (
        ("window", c.compose_window_ns),
        ("prior", c.compose_prior_ns),
        ("lock-wait", c.compose_lock_wait_ns),
        ("scan", c.compose_scan_ns),
        ("sort", c.compose_sort_ns),
        ("select", c.compose_select_ns),
    )
    best_name, best_ns = max(phases, key=lambda p: p[1])
    if best_ns == 0:
        return "-"
    return best_name


def dominant_phase(c: PhaseCounters) -> str:
    """Name the largest of refresh/compose/deliver/write ns ('-' on an all-zero row)."""
    phases = (
        ("refresh", c.refresh_ns),
        ("compose", c.compose_ns),
        ("deliver", c.deliver_ns),
        ("write", c.write_ns),
    )
    best_name, best_ns = max(phases, key=lambda p: p[1])
    if best_ns == 0:
        return "-"
    return best_name


def render_flicker_ledger(ledger: FlickerLedger) -> str:
    """Render the per-cluster arrival ledger as a markdown table.

    One row per instance-wide gap cluster: the cross-client signatures
    that discriminate a single synchronized server-side emission event
    (tight onset spread, aligned skipped tick range, tight resume spread,
    catch-up burst above cadence) from staggered downstream delivery.
    Start offsets are milliseconds from the movement-window start; the
    per-tick arrival-spread rows beneath are the normal-delivery
    comparator the resume spreads read against.
    """
    lines = [
        "## Flicker-gap ledger (movement window, diagnostic)",
        "",
        "Per-instance gap clusters from the instrumented sample's own-actor "
        "streams (diagnostic, not gated). 'boundary' is the last→first "
        "input-tick pair across the hole; resume spread is the cross-client "
        "arrival spread of the first commonly resumed tick; catch-up counts "
        "own-actor frames in the first 100 ms after resume (normal 20 Hz "
        "cadence contributes ~2, so more indicates queued ticks draining).",
        "",
        "| Inst | start ms | dur ms | parts | onset spread | boundary | resume spread | catch-up |",
        "|---|---:|---:|---:|---:|---|---:|---:|",
    ]
    for cluster in ledger.clusters:
        if len(cluster.last_ticks) == 1 and len(cluster.first_ticks) == 1:
            skipped = f"{cluster.last_ticks[0]}→{cluster.first_ticks[0]}"
        else:
            skipped = "mixed"
        resume = "-" if cluster.resume_spread_ms is None else f"{cluster.resume_spread_ms} ms"
        start_ms = (cluster.start_ns - ledger.window_start_ns) // 1_000_000
        lines.append(
            f"| {cluster.instance_index} | {start_ms} "
            f"| {cluster.duration_ms} | {cluster.participants}/{cluster.sampled_on_instance} "
            f"| {cluster.onset_spread_ms} ms | {skipped} | {resume} "
            f"| {cluster.catchup_median:.1f} |"
        )
    lines.extend(
        [
            "",
            "### Per-tick arrival-spread baseline (away from clusters)",
            "",
            "| Inst | ticks | median spread | p95 spread |",
            "|---|---:|---:|---:|",
        ]
    )
    for spread in ledger.tick_spreads:
        median = "-" if spread.median_ms is None else f"{spread.median_ms:.1f} ms"
        p95 = "-" if spread.p95_ms is None else f"{spread.p95_ms:.1f} ms"
        lines.append(f"| {spread.instance_index} | {spread.ticks} | {median} | {p95} |")
    return "\n".join(lines)


def render_gap_attribution(ledger: FlickerLedger) -> str:
    """Render the per-cluster /proc attribution as a markdown table.

    One row per target and scope per cluster: the CPU time and
    context-switch deltas inside the widened cluster window with the
    verdict the no-gap baseline implies. Scope 'tick' isolates each
    server process's tick/reactor thread (comm ``kith-server-run``);
    scope 'main' is the process main thread (tid equals pid — the
    shutdown-signal supervisor for a server subprocess, the asyncio
    loop thread for the harness target). Scope 'total' sums every
    thread of the process.
    """
    lines = [
        "## Gap-window /proc attribution (diagnostic)",
        "",
        "Kernel accounting per target inside each cluster window "
        "(diagnostic, not gated). Verdicts: cpu-busy (real work), "
        "preempted (nonvoluntary switches elevated — scheduling "
        "contention), blocked (voluntary switches elevated — repeated "
        "kernel waits), quiet (no anomaly at this granularity), "
        "unbaselined (no clean baseline window).",
        "",
        "| Inst | start ms | target | scope | wall ms | cpu ms | utime ms | stime ms "
        "| nvcsw | nivcsw | verdict |",
        "|---|---:|---|---|---:|---:|---:|---:|---:|---:|---|",
    ]
    for attribution in ledger.attributions:
        start_ms = (attribution.start_ns - ledger.window_start_ns) // 1_000_000
        for row in attribution.rows:
            lines.append(
                f"| {attribution.instance_index} | {start_ms} | {row.target} "
                f"| {row.scope} | {row.wall_ms} | {row.cpu_ms:.1f} | {row.utime_ms:.1f} "
                f"| {row.stime_ms:.1f} | {row.nvcsw_delta} | {row.nivcsw_delta} "
                f"| {row.verdict} |"
            )
    return "\n".join(lines)


def format_spread(values: Sequence[int]) -> str:
    """Render a min/p50/p95/max spread of ``values`` ('n/a' when empty)."""
    if not values:
        return "n/a"
    ordered = sorted(values)
    return (
        f"min {percentile_at(ordered, 0)} / p50 {percentile_at(ordered, 50)} "
        f"/ p95 {percentile_at(ordered, 95)} / max {percentile_at(ordered, 100)}"
    )


def percentile_at(ordered: list[int], pct: int) -> int:
    """Nearest-rank percentile of a pre-sorted ascending list as an int."""
    return int(percentile(ordered, pct))
