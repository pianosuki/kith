"""Depth, continuity and cohort measurements from retained client evidence."""

from __future__ import annotations

import asyncio
import itertools
from collections.abc import Mapping, Sequence
from dataclasses import replace

from examples._common.query_state import QUERY_STATE_DEFAULT_PAGE_SIZE
from examples.spatial.client import iter_actor_states_for

from tools.agent.assertions import ServerControl
from tools.agent.gap_ledger import (
    ClientSeries,
    SeriesFrame,
    build_flicker_ledger,
    client_gaps,
)
from tools.agent.orchestrator import EvidenceReader, Orchestrator
from tools.agent.proc_sampler import ProcTrace

from .constants import BREADTH_WINDOW_MAX_GAP_S, FLICKER_GAP_MS, QUERY_STATE_ERRORS
from .metrics import (
    BreadthCensus,
    ClientDepthRow,
    ClientFlickerSample,
    CohortDepth,
    CohortRow,
    EchoDeltaHistogram,
    FlickerBreakdown,
    GateMetrics,
    InstanceGateMetrics,
)
from .records import ClientRecord
from .statistics import percentile


def attach_backlog(
    metrics: GateMetrics,
    pre: tuple[int, int] | None,
    post: tuple[int, int] | None,
) -> GateMetrics:
    """Return ``metrics`` with the driver inbound-backlog sample attached.

    Both samples are diagnostic: they quantify the client-side lag term
    the drain bounds so a gate report shows the observer's own health
    next to the metrics it feeds.
    """
    return replace(
        metrics,
        backlog_pre_drain_median_ms=pre[0] if pre else None,
        backlog_pre_drain_p95_ms=pre[1] if pre else None,
        backlog_post_drain_median_ms=post[0] if post else None,
        backlog_post_drain_p95_ms=post[1] if post else None,
    )


def attach_ledger(
    metrics: GateMetrics, trace: ProcTrace, series: Sequence[ClientSeries]
) -> GateMetrics:
    """Return ``metrics`` with the flicker ledger built from trace + series."""
    return replace(metrics, flicker_ledger=build_flicker_ledger(trace, series))


def distinct_non_self(ids: frozenset[int], own_actor_id: int) -> int:
    """Count a distinct-id set's members excluding the client's own actor id.

    Ids accumulate unfiltered at ingest (a late-bootstrapped client's own
    actor id is unknown until the collection-time binding re-stamp), so
    the self filter applies at read time — to the window set and to the
    deque-wide set alike, keeping the two counts on one footing.
    """
    if own_actor_id == 0:
        return len(ids)
    return len(ids) - (1 if own_actor_id in ids else 0)


def window_stream_continuous(frame_ts_ns: Sequence[int]) -> bool:
    """Return whether the window's frame arrivals show no stall past the bound."""
    max_gap_ns = int(BREADTH_WINDOW_MAX_GAP_S * 1e9)
    return all(b - a <= max_gap_ns for a, b in itertools.pairwise(frame_ts_ns))


async def client_view_facts(
    orch: EvidenceReader, rec: ClientRecord, min_distinct: int
) -> tuple[bool, bool, bool]:
    """Return ``(observed, stalled, incomplete)`` from one client's window evidence.

    ``observed`` is the breadth health reading: the client's
    movement-window evidence — opened at movement start, sealed at
    movement end plus the breadth drain, counted at read from the
    client's retained stream — holds at least ``min_distinct`` non-self
    actor ids, and the in-window frame arrivals are continuous. The
    window answers the documented claim ("received a non-self
    actor_state through the replication stream") over the certification
    interval the depth channel uses, where the retired probe read a
    2 s recency window at an arbitrary crawl moment and turned the
    reading into a race the per-frame census proved unreachable by
    delivery design (the tiered delivery cadence carries ~28-80 distinct
    ids per 2 s; the bar is met in-window by the window-start roster wave).

    ``stalled`` marks a client whose distinct count cleared the bar but
    whose in-window arrivals gap past :data:`_BREADTH_WINDOW_MAX_GAP_S`:
    a mid-window stream stall is a delivery break, so the client is not
    selected even though its pre-stall frames carried the bar. For
    summary clients this is the only delivery-break signal there is —
    the depth metrics ride the instrumented sample alone.

    ``incomplete`` marks a client whose retained-stream basis cannot
    prove completeness (the deque evicted while the window was open, or
    no byte budget was configured): the counts are a floor, so the
    client reads unobserved and the census reports it separately rather
    than conflating it with a genuinely sparse stream.
    """
    window = await orch.view_window(rec.name)
    if window is None:
        return False, False, False
    if window.incomplete:
        return False, False, True
    if distinct_non_self(window.distinct_ids, rec.own_actor_id) < min_distinct:
        return False, False, False
    if not window_stream_continuous(window.frame_ts_ns):
        return False, True, False
    return True, False, False


async def client_view_set_healthy(
    orch: EvidenceReader, rec: ClientRecord, min_distinct: int
) -> bool:
    """Return whether ``rec``'s movement-window stream proves the view bar."""
    observed, _stalled, _incomplete = await client_view_facts(orch, rec, min_distinct)
    return observed


async def view_evidence_fresh(orch: EvidenceReader, rec: ClientRecord, min_distinct: int) -> bool:
    """Return whether ``rec``'s movement-window evidence alone proves the view bar.

    The divergence guard for isolated clients: an isolated client whose
    own window stream carried the bar received subjects while
    geometrically isolated. Scoped to the movement window, not the probe
    moment — a post-window backstop wave re-delivering the retained view
    must not read as live divergence, and an isolated client probed
    within seconds of one must not breach the guard on it. An incomplete
    basis (retained-stream eviction under the window) proves nothing, so
    it reads not-fresh rather than trusting a floor.
    """
    window = await orch.view_window(rec.name)
    if window is None or window.incomplete:
        return False
    return distinct_non_self(window.distinct_ids, rec.own_actor_id) >= min_distinct


async def collect_depth_metrics(
    orch: EvidenceReader,
    instrumented: list[ClientRecord],
    drain_s: float,
) -> tuple[int, int, tuple[ClientSeries, ...], tuple[ClientDepthRow, ...]]:
    """Collect both depth metrics from one pass over each client's history.

    Returns ``(submitted, observed_ticks, series, rows)``. ``submitted``
    sums the input ticks each instrumented client sent; ``observed_ticks``
    counts distinct ``input_tick`` values seen in self-frames across the
    full history — distinct values, not raw frame count, so a gateway
    that batches multiple frames per tick cannot inflate the observed
    count past the submitted count and mask real drops. ``series``
    carries ``(receipt time, echoed input tick)`` pairs for own-actor
    frames scoped to the movement window stamped on every record. The
    echoed ``input_tick`` values are comparable across clients — every
    client submits the same harness tick counter per round — so the
    ledger can align skipped tick ranges per instance without any wire
    change. The caller holds the post-window echo drain before this
    fetch, so ticks echoed within the drain count as observed; the
    histories are drained at shutdown, so this runs before the host
    tears down. One fetch feeds both outputs.

    ``rows`` carries the per-client depth detail behind the certified
    move-missing reading: each row pairs the client's submitted and
    observed counts with the certified-missing count computed over the
    certification interval ``[movement_start, movement_end + drain]``
    (see :func:`_certified_missing` for the counting rule).
    """
    total_inputs = 0
    observed_ticks = 0
    series: list[ClientSeries] = []
    rows: list[ClientDepthRow] = []
    for rec in instrumented:
        total_inputs += rec.inputs_submitted
        seen_ticks: set[int] = set()
        events = await orch.recent_events(rec.name, count=100_000)
        start = rec.movement_start_ns
        end = rec.movement_end_ns
        windowed = start != 0
        cert_end = end + int(drain_s * 1_000_000_000)
        frames: list[SeriesFrame] = []
        cert_frames: list[SeriesFrame] = []
        for event_index, event in enumerate(events):
            if event_index % 16 == 0:
                await asyncio.sleep(0)
            for view in iter_actor_states_for(event.type_id, event.payload, rec.own_actor_id):
                # Tick distinctness spans the full history; the series is
                # scoped to the movement window alone.
                seen_ticks.add(view.input_tick)
                ts = event.ts_mono_ns
                if windowed and start <= ts <= cert_end:
                    frame = SeriesFrame(
                        ts_ns=ts, input_tick=view.input_tick, update_seq=view.update_seq
                    )
                    cert_frames.append(frame)
                    if ts <= end:
                        frames.append(frame)
        observed = len(seen_ticks)
        observed_ticks += observed
        cert_missing, cert_fallback = count_certified_missing(
            cert_frames, rec.inputs_submitted, observed
        )
        rows.append(
            ClientDepthRow(
                name=rec.name,
                submitted=rec.inputs_submitted,
                observed=observed,
                certified_missing=cert_missing,
                certified_fallback=cert_fallback,
            )
        )
        series.append(
            ClientSeries(
                name=rec.name,
                instance_index=rec.instance_index,
                window_start_ns=start,
                window_end_ns=end,
                frames=tuple(frames),
            )
        )
    return total_inputs, observed_ticks, tuple(series), tuple(rows)


def count_certified_missing(
    frames: Sequence[SeriesFrame], submitted: int, observed: int
) -> tuple[int, bool]:
    """Return ``(missing, fallback)`` for one client's certification interval.

    The certified rule: the counter delta between the first
    and last self echo in the interval counts the movement applications
    the source performed between them, regardless of how many echoes the
    rebuild cadence produced — so a submitted input is missing unless the
    delta certifies it. ``missing = max(0, submitted - certified)``; the
    per-client clamp keeps one client's over-coverage (applies of
    pre-window inputs landing after the first echo) from masking another
    client's missing.

    Two degenerate readings, both honest:

    - No echo at all in the interval certifies nothing: the client
      contributes its full submitted count as missing.
    - Echoes present but every counter 0 means the publisher never
      minted (or stamping is disabled): the client is a FALLBACK and
      contributes its event-rule missing count instead — the
      event-counting basis over the same submitted total.

    The counter is uint32 with per-window baselines: the delta is plain
    integer arithmetic, so a contract-violating wrap reads as a huge
    missing count rather than silently
    over-crediting.
    """
    if not frames:
        return submitted, False
    ordered = sorted(frames, key=lambda f: f.ts_ns)
    if all(f.update_seq == 0 for f in ordered):
        return max(0, submitted - observed), True
    baseline = ordered[0].update_seq
    certified = max(f.update_seq for f in ordered) - baseline
    return max(0, submitted - certified), False


def certified_metrics(
    rows: Sequence[ClientDepthRow],
) -> tuple[float | None, int]:
    """Aggregate per-client certified rows into ``(ratio, fallback_count)``.

    The ratio is the summed certified missing over the summed submitted
    total — the same shape as the event rule's aggregate, with each
    client's contribution already clamped at zero. ``None`` when no
    instrumented client submitted anything (no certifiable series).
    """
    total = sum(r.submitted for r in rows)
    fallbacks = sum(1 for r in rows if r.certified_fallback)
    if total == 0:
        return None, fallbacks
    missing = sum(r.certified_missing for r in rows)
    return missing / total, fallbacks


def select_spread_probe_indices(
    actor_count: int, instrumented_count: int, spread_size: int
) -> tuple[int, ...]:
    """Evenly spaced depth-instrumentation indices after the head sample.

    The head prefix (the first ``instrumented_count`` clients) is first in
    every submit round, so its depth sample over-represents the pool's
    friendliest positions. The spread probe covers the remaining submit
    positions evenly: ``spread_size`` indices spanning
    ``instrumented_count`` to ``actor_count - 1`` inclusive. A body too
    small to fit the request yields every remaining index; a request of
    zero (or no body) yields no probes.
    """
    body = actor_count - instrumented_count
    if spread_size <= 0 or body <= 0:
        return ()
    if body <= spread_size:
        return tuple(range(instrumented_count, actor_count))
    step = (body - 1) / (spread_size - 1)
    return tuple(instrumented_count + round(k * step) for k in range(spread_size))


def per_instance_ranks(records: Sequence[ClientRecord]) -> dict[str, int]:
    """Map each booted client name to its submit rank within its instance.

    The movement round walks the movable clients in registration order, so
    a client's per-instance rank is its position among the movable clients
    that landed on the same instance — the coordinate the pool's
    accept/drop boundary lives in, not the global registration index.
    Unbooted records rank nothing (they never submitted).
    """
    ranks: dict[str, int] = {}
    seen: dict[int, int] = {}
    for rec in records:
        if rec.own_actor_id == 0 or rec.ready_ns <= rec.started_ns:
            continue
        rank = seen.get(rec.instance_index, 0)
        ranks[rec.name] = rank
        seen[rec.instance_index] = rank + 1
    return ranks


async def collect_cohort_depth(
    orch: EvidenceReader,
    cohort: str,
    records: list[ClientRecord],
    ranks: dict[str, int],
    drain_s: float,
    *,
    depth_rows: Sequence[ClientDepthRow] = (),
) -> CohortDepth:
    """Collect per-client depth detail for one client cohort.

    Mirrors :func:`_collect_depth_metrics` per client instead of in
    aggregate: each row counts the client's distinct echoed input ticks
    over the full history against its submitted inputs, with the client's
    per-instance submit rank attached. ``ranks`` comes from
    :func:`_per_instance_ranks` over the FULL record list — a cohort's own
    subset would renumber its members instead of locating them in the real
    submit sequence.
    """
    by_name = {row.name: row for row in depth_rows}
    missing = [rec for rec in records if rec.name not in by_name]
    if missing:
        _, _, _, extra_rows = await collect_depth_metrics(orch, missing, drain_s)
        by_name.update((row.name, row) for row in extra_rows)
    rows: list[CohortRow] = []
    cert_rows: list[ClientDepthRow] = []
    submitted = 0
    observed = 0
    for rec in records:
        depth = by_name[rec.name]
        submitted += depth.submitted
        observed += depth.observed
        cert_rows.append(depth)
        rows.append(
            CohortRow(
                name=rec.name,
                instance_index=rec.instance_index,
                per_instance_rank=ranks.get(rec.name, 0),
                submitted=depth.submitted,
                observed=depth.observed,
                certified_missing=depth.certified_missing,
                certified_fallback=depth.certified_fallback,
            )
        )
    certified, fallbacks = certified_metrics(tuple(cert_rows))
    return CohortDepth(
        name=cohort,
        sampled=len(records),
        submitted=submitted,
        observed=observed,
        move_missing=measure_move_missing_ratio(submitted, observed),
        rows=tuple(rows),
        move_missing_certified=certified,
        certified_fallback_clients=fallbacks,
    )


def measure_move_missing_ratio(submitted: int, observed: int) -> float:
    """Fraction of submitted input ticks that produced no observable self-update.

    The event-count companion reading (`mm_event`): it counts
    distinct echoed input ticks against submitted inputs. Under saturation
    the composed view refreshes at the rebuild cadence, so the ratio floors
    at the rebuild-to-submit sampling ratio even at zero real loss — the
    certified rule (:func:`_certified_missing`) is the gate-binding
    reading, and this one is retained so published gate history stays
    comparable across the boundary. The drain bounds how long an in-flight
    echo may still arrive, so the ratio measures echo lag beyond that
    bound plus permanent loss rather than scrape timing. Computed over the
    deeply instrumented sample whose event history holds the full
    movement-window stream (see :func:`_collect_depth_metrics`).
    """
    if submitted == 0:
        return 0.0
    return max(0.0, 1.0 - observed / submitted)


COVERAGE_TRUNCATED = 0.98


def echo_delta_histogram(series: Sequence[ClientSeries]) -> EchoDeltaHistogram:
    """Bucket the consecutive-echo input_tick deltas of the instrumented cohort.

    See :class:`EchoDeltaHistogram` for the reading. Frames are sorted by
    receipt time first: the collector appends in event-history order, which
    is not guaranteed ascending. An empty series list (no instrumented
    clients, or no movement window stamped) yields an all-zero histogram
    with ``coverage_min`` 0.0 — the honest absent signal, rendered as n/a.
    """
    frames = 0
    delta1 = delta2 = delta3_5 = delta6_20 = delta21_plus = regressed = 0
    missing_implied = 0
    truncated = 0
    coverage_min = 1.0
    for cs in series:
        ordered = sorted(cs.frames, key=lambda f: f.ts_ns)
        frames += len(ordered)
        if not ordered:
            continue
        span_ns = ordered[-1].ts_ns - ordered[0].ts_ns
        window_ns = cs.window_end_ns - cs.window_start_ns
        if window_ns > 0:
            coverage = span_ns / window_ns
            coverage_min = min(coverage_min, coverage)
            if coverage < COVERAGE_TRUNCATED:
                truncated += 1
        prev_tick: int | None = None
        for frame in ordered:
            if prev_tick is not None:
                delta = frame.input_tick - prev_tick
                if delta <= 0:
                    regressed += 1
                elif delta == 1:
                    delta1 += 1
                elif delta == 2:
                    delta2 += 1
                elif delta <= 5:
                    delta3_5 += 1
                elif delta <= 20:
                    delta6_20 += 1
                else:
                    delta21_plus += 1
                if delta > 1:
                    missing_implied += delta - 1
            prev_tick = frame.input_tick
    if not series:
        coverage_min = 0.0
    return EchoDeltaHistogram(
        clients=len(series),
        frames=frames,
        delta1=delta1,
        delta2=delta2,
        delta3_5=delta3_5,
        delta6_20=delta6_20,
        delta21_plus=delta21_plus,
        regressed=regressed,
        missing_implied=missing_implied,
        coverage_min=coverage_min,
        truncated_clients=truncated,
    )


def measure_selected_clients_ratio(selected: int, total: int, eligible: int | None) -> float:
    """Reduce the breadth counts to the selected ratio.

    With an eligibility read the denominator is the collection-time bound
    cohort minus the geometrically isolated clients; the numerator is
    computed over that same cohort, so ``selected <= eligible`` holds by
    construction and the ratio never exceeds 1.0. Without a roster the
    ratio keeps its full-population denominator.
    """
    if eligible is None:
        return selected / total if total else 0.0
    return selected / max(1, eligible)


def instance_gate_metrics(
    records: Sequence[ClientRecord],
    breadth: BreadthCensus,
    cert_rows: Sequence[ClientDepthRow],
    series: Sequence[ClientSeries],
) -> tuple[InstanceGateMetrics, ...]:
    """Reduce each instance's cohort to its own gate-metrics row.

    The instance set comes from the breadth census (one census per
    instance the clients landed on); the depth rows, the movement-window
    series, and the bootstrap latencies partition by the same instance
    stamps, so a per-instance ratio is the pooled formula's slice of one
    population, never a re-measurement. An instance with no instrumented
    cohort reads ``move_missing_ratio_certified=None``, which fails the
    certified bar: an unmeasured instance is not a passing one.
    """
    instance_of = {rec.name: rec.instance_index for rec in records}
    depths: dict[int, list[ClientDepthRow]] = {}
    for row in cert_rows:
        depths.setdefault(instance_of[row.name], []).append(row)
    series_by_instance: dict[int, list[ClientSeries]] = {}
    for entry in series:
        series_by_instance.setdefault(entry.instance_index, []).append(entry)
    latencies: dict[int, list[int]] = {}
    for rec in records:
        if rec.ready_ns > rec.started_ns:
            latencies.setdefault(rec.instance_index, []).append(
                max(0, (rec.ready_ns - rec.started_ns) // 1_000_000)
            )
    rows: list[InstanceGateMetrics] = []
    for census in breadth.instances:
        if census.instance_index is None:
            continue
        index = census.instance_index
        depth_rows = depths.get(index, ())
        certified, fallbacks = certified_metrics(depth_rows)
        rows.append(
            InstanceGateMetrics(
                instance_index=index,
                actor_count=census.total,
                sampled_clients=len(depth_rows),
                session_ok_ratio=census.session_ok_count / census.total if census.total else 0.0,
                selected_clients_ratio=measure_selected_clients_ratio(
                    census.selected_count, census.total, census.eligible_clients
                ),
                eligible_clients=census.eligible_clients,
                isolated_clients=census.isolated_clients,
                classified_clients=census.classified_clients,
                move_missing_ratio=measure_move_missing_ratio(
                    sum(r.submitted for r in depth_rows),
                    sum(r.observed for r in depth_rows),
                ),
                move_missing_ratio_certified=certified,
                certified_fallback_clients=fallbacks,
                bootstrap_ms_p95=int(percentile(latencies.get(index, ()), 95)),
                continuity_flicker=(
                    flicker_breakdown_from_series(series_by_instance.get(index, ())).total_gaps > 0
                ),
            )
        )
    return tuple(rows)


def flicker_breakdown_from_series(series: Sequence[ClientSeries]) -> FlickerBreakdown:
    """Build the per-client continuity-flicker distribution from the series.

    A gap between consecutive own-actor frames longer than
    :data:`_FLICKER_GAP_MS` within the movement window is a continuity
    flicker (the actor was visible, dropped from the stream, then
    resumed while input was still being submitted). The per-client entry
    count separates a systematic burst hitting every client from a
    concentrated one on a few; the gap sizes separate a just-over-
    threshold periodic stall from a multi-tick blackout; the start
    offsets reveal inter-gap spacing within a client. Only clients that
    flickered appear, so an empty ``per_client`` means no instrumented
    client flickered.
    """
    total_gaps = 0
    per_client: list[ClientFlickerSample] = []
    for entry in series:
        gaps = client_gaps(entry, FLICKER_GAP_MS)
        if not gaps:
            continue
        total_gaps += len(gaps)
        per_client.append(
            ClientFlickerSample(
                name=entry.name,
                instance_index=entry.instance_index,
                gap_count=len(gaps),
                gap_ms=tuple(int((g.end_ns - g.start_ns) / 1_000_000) for g in gaps),
                gap_start_ms=tuple((g.start_ns - entry.window_start_ns) // 1_000_000 for g in gaps),
            )
        )
    return FlickerBreakdown(total_gaps=total_gaps, per_client=tuple(per_client))


def start_concurrency(actor_count: int) -> int:
    """In-flight concurrency bound for the semaphore-gated client start.

    Caps concurrent connects so 2000 simultaneous socket setups do not
    exhaust ephemeral ports or file descriptors; large enough that the
    drive completes in a handful of admission rounds. With the host
    out-of-process the harness loop does not contend with server worker
    threads for one GIL, so this is a socket-pressure guard, not a
    GIL-contention throttle.
    """
    return min(256, max(1, actor_count))


async def resolve_own_actors_dense(orch: Orchestrator, records: list[ClientRecord]) -> None:
    """Resolve own actors against the server's ``/bindings`` map.

    The path for a drive whose control plane is one instance's client: a
    single-instance host factory, or a dual-driver worker (its child
    drives exactly one pre-booted instance). The binding map is matched
    against the booted clients' principal ids with the same completeness
    check the per-instance path applies: any shortfall or unmatched
    client fails the run loudly.
    """
    control = orch.server_control
    if control is None:
        return
    bindings = await principal_bindings(control)
    booted = [r for r in records if r.ready_ns > r.started_ns and r.ready_ns != 0]
    missing = [rec.name for rec in booted if rec.principal_id not in bindings]
    if len(bindings) != len(booted) or missing:
        raise RuntimeError(
            f"binding map covers {len(bindings)} principals for "
            f"{len(booted)} booted clients (unmatched clients: {missing[:5]})"
        )
    for rec in booted:
        rec.own_actor_id = bindings[rec.principal_id]


async def principal_bindings(control: ServerControl) -> dict[int, int]:
    """Return the server's principal→actor allocation map from ``/bindings``.

    Pages the route exactly like ``/query_state`` (the envelope carries
    ``offset``/``limit``/``total``) and folds each page into one mapping. A
    transport failure, a missing route, or a duplicate principal id raises
    instead of degrading: attribution built on partial or absent truth is
    the defect this lookup exists to prevent, so absence is loud, never a
    fallback to arrival-order inference.
    """
    pairs: list[tuple[int, int]] = []
    offset = 0
    limit = QUERY_STATE_DEFAULT_PAGE_SIZE
    while True:
        try:
            mapping: Mapping[str, object] = await control.get(
                f"/bindings?offset={offset}&limit={limit}"
            )
        except QUERY_STATE_ERRORS as exc:
            raise RuntimeError(f"binding truth unavailable ({exc})") from exc
        entries = mapping.get("bindings")
        if not isinstance(entries, list):
            raise RuntimeError("binding truth unavailable (/bindings response malformed)")
        page = [
            (int(item["principal_id"]), int(item["actor_id"]))
            for item in entries
            if isinstance(item, dict)
        ]
        pairs.extend(page)
        # Stop on a short final page, or when the server does not paginate
        # (no ``total`` field — the back-compat full-roster response).
        if len(page) < limit or "total" not in mapping:
            break
        offset += limit
    bindings_map = dict(pairs)
    if len(bindings_map) != len(pairs):
        raise RuntimeError("binding map carries a duplicate principal id")
    return bindings_map
