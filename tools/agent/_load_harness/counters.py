"""Prometheus counter parsing and movement-window sampling."""

from __future__ import annotations

import asyncio
import re
import time
from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

from tools.agent.orchestrator import Orchestrator
from tools.agent.server_control import ServerControlError


if TYPE_CHECKING:
    from tools.agent.server_control import ServerControlClient


@dataclass(frozen=True, slots=True)
class PhaseCounters:
    """Per-instance reactor-path phase counters over the movement window.

    The movement-window delta of the cumulative ``/metrics`` counters the
    gateway and fabric expose (ns spent in each reactor-thread tick phase,
    the ns spent in each compose sub-phase, plus the dispatch and publish
    totals), scraped from each instance's control-plane ``/metrics``
    endpoint at the movement-window bounds. The refresh/compose/deliver
    split localizes which tick phase dominates the per-tick delivery cost
    the flicker-gap measurement localized as an instance-wide periodic
    stall; the six compose sub-phases split that dominant phase into window
    copy / prior-set rebuild / stripe acquisition / scan / heapsort /
    select, bounding the compose time from below (untimed slivers and
    failed compositions are not recorded); ``lock_wait_ns`` is reactor-side
    stripe acquisition only;     ``compose_skips`` counts recompositions the
    composer skipped because every composition input was unchanged;
    ``compose_deferrals`` counts recompositions a tick deferred past its
    per-tick compose budget (bounded view staleness, not missed frames);
    ``view_locate_failures`` counts compositions whose scan phase found no
    cached window cell holding the session's bound actor id (a healthy
    system reports zero forever — any nonzero reading is a
    subscription-window ownership defect, not load);
    ``write_ns`` is the socket drain of the connection output queues —
    the transport term on the reactor thread outside the gateway's
    bracketed phases; ``write_deferrals`` counts drains the per-call
    output-drain cap truncated with output still queued (the pacing
    engagement rate; kernel-bound EAGAIN stops are not counted).
    The delivery-executor counters decompose the executor's cadence
    behavior: ``delivery_jobs`` counts submitted jobs,
    ``delivery_inflight_skips`` counts deliver passes skipped because the
    session's previous deliver was still in flight (the total per-session
    cadence-gap rate), ``delivery_ebusy_skips`` counts submissions the
    executor's task queue rejected, and the wait-budget pair decomposes
    the compose-side causes (``compose_wait_timeouts`` = waits that drew
    budget and ran it out, ``delivery_wait_budget_exhausted`` = in-flight
    sessions that skipped the wait after the budget was already spent).
    The two inflight fields are gauge snapshots, not deltas: the
    high-watermark is monotonic since creation, and the current-gauge
    reading is the end-of-window sample.
    The dispatches/publishes totals give the per-tick rates the capacity model
    reconciles against the tick budget. The rejection pair decomposes the
    malformed-input accounting: ``proto_rejections`` counts frames the proto
    decode entry point rejected, ``net_rejections`` counts connections closed
    at the input ring ceiling, and a frame reaches at most one of the two, so
    the sum reconciles the run's total malformed input.
    All-zero when the control plane is unavailable or a scrape failed (the
    honest 'counters absent' signal rather than a crash).
    """

    refresh_ns: int
    compose_ns: int
    deliver_ns: int
    dispatches: int
    publishes: int
    compose_window_ns: int
    compose_prior_ns: int
    compose_lock_wait_ns: int
    compose_scan_ns: int
    compose_sort_ns: int
    compose_select_ns: int
    compose_skips: int
    compose_deferrals: int
    view_locate_failures: int
    write_ns: int
    write_deferrals: int
    # The executor fields default to 0 so a positional 16-field
    # construction (the pre-executor shape, used by archived-result
    # loaders and tests) keeps parsing: absent series read as absent.
    delivery_jobs: int = 0
    delivery_inflight_skips: int = 0
    delivery_ebusy_skips: int = 0
    delivery_wait_budget_exhausted: int = 0
    compose_wait_timeouts: int = 0
    delivery_inflight_current: int = 0
    delivery_inflight_high_watermark: int = 0
    # The delivery/view totals fields default to 0 for the same reason:
    # archived payloads predate the series entirely.
    delivery_frames_enqueued: int = 0
    delivery_dropped: int = 0
    delivery_event_frames_enqueued: int = 0
    delivery_suppressed: int = 0
    view_visits: int = 0
    view_candidates: int = 0
    view_selected: int = 0
    view_candidate_high_watermark: int = 0
    view_selected_high_watermark: int = 0
    tick_dropped: int = 0
    dispatch_dropped: int = 0
    # Worker-pool task totals (counters): submitted counts dispatches the
    # gateway and control planes accepted (EBUSY rejections excluded);
    # completed counts applies. The pair reconciles the gateway dispatch
    # counters end to end.
    worker_tasks_submitted: int = 0
    worker_tasks_completed: int = 0
    # Decode-path rejection counters: proto counts frames its decode entry
    # point rejected (malformed header, payload bound, unknown type,
    # correlation-trailer size); net counts connections closed because a
    # frame's declared total exceeded the input ring ceiling. A frame
    # reaches at most one of the two, so the pair's sum reconciles the
    # run's total malformed input (the soak's conservation check).
    proto_rejections: int = 0
    net_rejections: int = 0
    # Window-capacity counters: window adds that failed against a full
    # cache stripe or fabric interest set (retained for the retry pass,
    # not terminal), and retained adds the retry pass landed after
    # capacity freed. Against the two gauges below they separate a
    # healing flood from a stuck one.
    window_add_failures: int = 0
    window_retry_adds: int = 0
    # Window-capacity gauges (end-scrape raw): the retry queue's depth and
    # the bound-but-windowless session census (the seed-failure
    # signature).
    window_retries_pending: int = 0
    sessions_without_cells: int = 0
    # Interpreter handler-exception counter: exceptions the Python bridge's
    # handler guards caught at the trampoline boundary (tick, message,
    # session-destroyed, and control-route handlers). The process-global
    # count is the flood signal; the first traceback per registration is
    # the diagnostic sample on stderr.
    handler_exceptions: int = 0


# The cumulative counters the composition root records per tick. The
# server's metrics handle carries no prefix, so the names are
# the literal Prometheus series names. A label-less counter line is
# "<name> <uint>"; the anchored multiline match is unambiguous and tolerant
# of absent series (parses to 0 rather than raising).
PHASE_METRIC_NAMES: tuple[tuple[str, str], ...] = (
    ("refresh_ns", "kith_gateway_refresh_ns_total"),
    ("compose_ns", "kith_gateway_compose_ns_total"),
    ("deliver_ns", "kith_gateway_deliver_ns_total"),
    ("dispatches", "kith_gateway_dispatches_total"),
    ("publishes", "kith_fabric_publishes_total"),
    ("compose_window_ns", "kith_gateway_compose_window_ns_total"),
    ("compose_prior_ns", "kith_gateway_compose_prior_ns_total"),
    ("compose_lock_wait_ns", "kith_gateway_compose_lock_wait_ns_total"),
    ("compose_scan_ns", "kith_gateway_compose_scan_ns_total"),
    ("compose_sort_ns", "kith_gateway_compose_sort_ns_total"),
    ("compose_select_ns", "kith_gateway_compose_select_ns_total"),
    ("compose_skips", "kith_gateway_compose_skips_total"),
    ("compose_deferrals", "kith_gateway_compose_deferrals_total"),
    ("view_locate_failures", "kith_gateway_view_locate_failures_total"),
    ("write_ns", "kith_net_write_ns_total"),
    ("write_deferrals", "kith_net_write_deferrals_total"),
    ("delivery_jobs", "kith_gateway_delivery_jobs_submitted_total"),
    ("delivery_inflight_skips", "kith_gateway_delivery_inflight_skips_total"),
    ("delivery_ebusy_skips", "kith_gateway_delivery_ebusy_skips_total"),
    ("delivery_wait_budget_exhausted", "kith_gateway_delivery_wait_budget_exhausted_total"),
    ("compose_wait_timeouts", "kith_gateway_compose_wait_timeouts_total"),
    ("delivery_inflight_current", "kith_gateway_delivery_inflight_current"),
    ("delivery_inflight_high_watermark", "kith_gateway_delivery_inflight_high_watermark"),
    ("delivery_frames_enqueued", "kith_gateway_delivery_frames_enqueued_total"),
    ("delivery_dropped", "kith_gateway_delivery_dropped_total"),
    ("delivery_event_frames_enqueued", "kith_gateway_delivery_event_frames_enqueued_total"),
    ("delivery_suppressed", "kith_gateway_delivery_suppressed_total"),
    ("view_visits", "kith_gateway_view_visits_total"),
    ("view_candidates", "kith_gateway_view_candidate_total"),
    ("view_selected", "kith_gateway_view_selected_total"),
    ("view_candidate_high_watermark", "kith_gateway_view_candidate_high_watermark"),
    ("view_selected_high_watermark", "kith_gateway_view_selected_high_watermark"),
    ("tick_dropped", "kith_server_tick_dropped_total"),
    ("dispatch_dropped", "kith_gateway_dispatch_dropped_total"),
    ("worker_tasks_submitted", "kith_worker_tasks_submitted_total"),
    ("worker_tasks_completed", "kith_worker_tasks_completed_total"),
    ("proto_rejections", "kith_proto_rejections_total"),
    ("net_rejections", "kith_net_rejections_total"),
    ("window_add_failures", "kith_gateway_window_add_failures_total"),
    ("window_retry_adds", "kith_gateway_window_retry_adds_total"),
    ("window_retries_pending", "kith_gateway_window_retries_pending"),
    ("sessions_without_cells", "kith_gateway_sessions_without_cells"),
    ("handler_exceptions", "kith_python_handler_exceptions_total"),
)

PHASE_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = tuple(
    (field, re.compile(rf"^{re.escape(name)}\s+(\d+)\s*$", re.MULTILINE))
    for field, name in PHASE_METRIC_NAMES
)


def parse_phase_counters(text: str) -> PhaseCounters:
    """Parse the reactor-path counters from a Prometheus text scrape.

    Each counter is a label-less ``<name> <uint>`` line. Absent series (a
    stale build without the counters, or a NULL metrics handle) parse as
    zero rather than raising, so a scrape never fails the gate; an all-zero
    block is the honest 'counters absent' signal.
    """
    values: dict[str, int] = {}
    for field, pattern in PHASE_PATTERNS:
        m = pattern.search(text)
        values[field] = int(m.group(1)) if m is not None else 0
    return PhaseCounters(
        refresh_ns=values["refresh_ns"],
        compose_ns=values["compose_ns"],
        deliver_ns=values["deliver_ns"],
        dispatches=values["dispatches"],
        publishes=values["publishes"],
        compose_window_ns=values["compose_window_ns"],
        compose_prior_ns=values["compose_prior_ns"],
        compose_lock_wait_ns=values["compose_lock_wait_ns"],
        compose_scan_ns=values["compose_scan_ns"],
        compose_sort_ns=values["compose_sort_ns"],
        compose_select_ns=values["compose_select_ns"],
        compose_skips=values["compose_skips"],
        compose_deferrals=values["compose_deferrals"],
        view_locate_failures=values["view_locate_failures"],
        write_ns=values["write_ns"],
        write_deferrals=values["write_deferrals"],
        delivery_jobs=values["delivery_jobs"],
        delivery_inflight_skips=values["delivery_inflight_skips"],
        delivery_ebusy_skips=values["delivery_ebusy_skips"],
        delivery_wait_budget_exhausted=values["delivery_wait_budget_exhausted"],
        compose_wait_timeouts=values["compose_wait_timeouts"],
        delivery_inflight_current=values["delivery_inflight_current"],
        delivery_inflight_high_watermark=values["delivery_inflight_high_watermark"],
        delivery_frames_enqueued=values["delivery_frames_enqueued"],
        delivery_dropped=values["delivery_dropped"],
        delivery_event_frames_enqueued=values["delivery_event_frames_enqueued"],
        delivery_suppressed=values["delivery_suppressed"],
        view_visits=values["view_visits"],
        view_candidates=values["view_candidates"],
        view_selected=values["view_selected"],
        view_candidate_high_watermark=values["view_candidate_high_watermark"],
        view_selected_high_watermark=values["view_selected_high_watermark"],
        tick_dropped=values["tick_dropped"],
        dispatch_dropped=values["dispatch_dropped"],
        worker_tasks_submitted=values["worker_tasks_submitted"],
        worker_tasks_completed=values["worker_tasks_completed"],
        window_add_failures=values["window_add_failures"],
        window_retry_adds=values["window_retry_adds"],
        window_retries_pending=values["window_retries_pending"],
        sessions_without_cells=values["sessions_without_cells"],
    )


def phase_counters_delta(
    start: Sequence[PhaseCounters], end: Sequence[PhaseCounters]
) -> list[PhaseCounters]:
    """Per-instance movement-window delta (end - start), clamped at zero.

    The counters are monotonic, so end >= start; the clamp guards a process
    restart between scrapes. Truncated to the shorter sequence so a scrape
    count mismatch (an instance dropped between bounds) does not misalign
    positions.
    """
    out: list[PhaseCounters] = []
    for s, e in zip(start, end, strict=False):
        out.append(
            PhaseCounters(
                refresh_ns=max(0, e.refresh_ns - s.refresh_ns),
                compose_ns=max(0, e.compose_ns - s.compose_ns),
                deliver_ns=max(0, e.deliver_ns - s.deliver_ns),
                dispatches=max(0, e.dispatches - s.dispatches),
                publishes=max(0, e.publishes - s.publishes),
                compose_window_ns=max(0, e.compose_window_ns - s.compose_window_ns),
                compose_prior_ns=max(0, e.compose_prior_ns - s.compose_prior_ns),
                compose_lock_wait_ns=max(0, e.compose_lock_wait_ns - s.compose_lock_wait_ns),
                compose_scan_ns=max(0, e.compose_scan_ns - s.compose_scan_ns),
                compose_sort_ns=max(0, e.compose_sort_ns - s.compose_sort_ns),
                compose_select_ns=max(0, e.compose_select_ns - s.compose_select_ns),
                compose_skips=max(0, e.compose_skips - s.compose_skips),
                compose_deferrals=max(0, e.compose_deferrals - s.compose_deferrals),
                view_locate_failures=max(0, e.view_locate_failures - s.view_locate_failures),
                write_ns=max(0, e.write_ns - s.write_ns),
                write_deferrals=max(0, e.write_deferrals - s.write_deferrals),
                delivery_jobs=max(0, e.delivery_jobs - s.delivery_jobs),
                delivery_inflight_skips=max(
                    0, e.delivery_inflight_skips - s.delivery_inflight_skips
                ),
                delivery_ebusy_skips=max(0, e.delivery_ebusy_skips - s.delivery_ebusy_skips),
                delivery_wait_budget_exhausted=max(
                    0, e.delivery_wait_budget_exhausted - s.delivery_wait_budget_exhausted
                ),
                compose_wait_timeouts=max(0, e.compose_wait_timeouts - s.compose_wait_timeouts),
                # Gauges, not counters: the snapshot is the end scrape's
                # raw reading (the high-watermark is monotonic since
                # creation; the current gauge is the window-end sample).
                delivery_inflight_current=e.delivery_inflight_current,
                delivery_inflight_high_watermark=e.delivery_inflight_high_watermark,
                delivery_frames_enqueued=max(
                    0, e.delivery_frames_enqueued - s.delivery_frames_enqueued
                ),
                delivery_dropped=max(0, e.delivery_dropped - s.delivery_dropped),
                delivery_event_frames_enqueued=max(
                    0, e.delivery_event_frames_enqueued - s.delivery_event_frames_enqueued
                ),
                delivery_suppressed=max(0, e.delivery_suppressed - s.delivery_suppressed),
                view_visits=max(0, e.view_visits - s.view_visits),
                view_candidates=max(0, e.view_candidates - s.view_candidates),
                view_selected=max(0, e.view_selected - s.view_selected),
                # Gauges: monotonic peaks since creation, end-scrape raw.
                view_candidate_high_watermark=e.view_candidate_high_watermark,
                view_selected_high_watermark=e.view_selected_high_watermark,
                tick_dropped=max(0, e.tick_dropped - s.tick_dropped),
                dispatch_dropped=max(0, e.dispatch_dropped - s.dispatch_dropped),
                worker_tasks_submitted=max(0, e.worker_tasks_submitted - s.worker_tasks_submitted),
                worker_tasks_completed=max(0, e.worker_tasks_completed - s.worker_tasks_completed),
                proto_rejections=max(0, e.proto_rejections - s.proto_rejections),
                net_rejections=max(0, e.net_rejections - s.net_rejections),
                window_add_failures=max(0, e.window_add_failures - s.window_add_failures),
                window_retry_adds=max(0, e.window_retry_adds - s.window_retry_adds),
                # Gauges: the retry queue's depth and the seedless-session
                # census at the window-end scrape.
                window_retries_pending=e.window_retries_pending,
                sessions_without_cells=e.sessions_without_cells,
            )
        )
    return out


@dataclass(frozen=True, slots=True)
class WindowCounters:
    """One instance's movement-window counter delta plus its presence.

    ``present`` is True only when both boundary scrapes of the window
    succeeded; when False, ``counters`` carries no measurement and
    consumers must treat the reading as absent rather than as a genuine
    all-zero measurement.
    """

    present: bool
    counters: PhaseCounters


ABSENT_PHASE_COUNTERS = PhaseCounters(
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
)


async def scrape_phase_counters(orch: Orchestrator) -> list[WindowCounters]:
    """Scrape ``/metrics`` from each instance and parse the phase counters.

    Returns one :class:`_WindowCounters` per instance (dense: one;
    distributed: one per instance), positionally aligned with the instance
    order. A failed scrape yields ``present=False`` with zero
    :class:`PhaseCounters` so the positions stay aligned, a diagnostic
    scrape never crashes the gate, and a failed control-plane read stays
    distinguishable from a genuine all-zero counter reading.
    """
    from tools.agent.distributed_host import DistributedControl
    from tools.agent.server_control import ServerControlClient

    control = orch.server_control
    if control is None:
        return []
    if isinstance(control, DistributedControl):
        clients: list[ServerControlClient] = list(control.controls)
    elif isinstance(control, ServerControlClient):
        clients = [control]
    else:
        return []
    out: list[WindowCounters] = []
    for client in clients:
        try:
            text = await client.metrics()
        except (ServerControlError, OSError) as _exc:
            out.append(WindowCounters(False, ABSENT_PHASE_COUNTERS))
            continue
        out.append(WindowCounters(True, parse_phase_counters(text)))
    return out


# Cadence of the in-window counter sampler. Diagnostic only: the samples
# give the per-interval drop profile and the pool's in-flight depth over
# the movement window; the gate's own counter deltas stay sourced from the
# window-boundary scrapes alone.
SAMPLER_INTERVAL_S: float = 0.25


@dataclass(frozen=True, slots=True)
class CounterSample:
    """One in-window ``/metrics`` snapshot across every instance.

    ``t_rel_ms`` is the sample's offset from the movement-window start (the
    movement loop's clock, so rows align with submit rounds); ``counters``
    carries the raw cumulative per-instance readings. Deltas between
    consecutive samples give per-interval rates, and the last sample's
    submitted-minus-completed worker total is the pool's in-flight depth
    at that moment.
    """

    t_rel_ms: int
    counters: tuple[PhaseCounters, ...]


async def run_counter_sampler(
    orch: Orchestrator,
    samples: list[CounterSample],
    window_start_ns: int,
    interval_s: float = SAMPLER_INTERVAL_S,
) -> None:
    """Sample every instance's counters on a fixed cadence until cancelled.

    Runs as a concurrent task beside the movement loop: the loop's pacing
    is untouched (the sampler awaits between its own scrapes and the event
    loop interleaves the two), and a failed diagnostic scrape appends an
    absent reading rather than raising, mirroring the boundary
    scrape's contract. Cancellation between samples is the normal stop.
    """
    while True:
        await asyncio.sleep(interval_s)
        scrapes = await scrape_phase_counters(orch)
        samples.append(
            CounterSample(
                t_rel_ms=(time.monotonic_ns() - window_start_ns) // 1_000_000,
                counters=tuple(w.counters for w in scrapes),
            )
        )


PhaseCounters.__module__ = "tools.agent.load_harness"
WindowCounters.__module__ = "tools.agent.load_harness"
CounterSample.__module__ = "tools.agent.load_harness"
