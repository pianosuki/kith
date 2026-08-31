"""Flicker-gap ledger and /proc attribution analytics for gate reports.

Pure computation over measurement DTOs: builds per-client own-actor
frame series into gap clusters (single-linkage over gap intervals,
matching the manual analysis methodology), computes the cross-client
arrival-spread signatures that discriminate a synchronized server-side
emission event from staggered downstream delivery, attributes each gap
cluster against the :mod:`tools.agent.proc_sampler` trace, and assembles
the report-facing ledger value object.

The discriminating logic rests on one measured invariant: at zero
``move_missing`` every submitted input tick reappears post-gap as an
own-actor frame (the existing move-missing metric counts distinct
``input_tick`` values), so a skipped tick range must reappear after the
gap as an observable catch-up burst. A tight onset spread plus aligned skipped
ranges plus a tight resume spread is the signature of one server-side
emission event; staggered resume arrivals point below the server's
compose/deliver path instead. The verdicts here name measurable
signatures; the interpretation that picks a fix lives with the analyst,
not in this module.
"""

from __future__ import annotations

from bisect import bisect_right
from collections.abc import Sequence
from dataclasses import dataclass
from itertools import pairwise

from tools.agent.proc_sampler import ProcTrace


__all__ = [
    "ClientGap",
    "ClientSeries",
    "ClusterAttribution",
    "FlickerLedger",
    "GapClusterRecord",
    "InstanceTickSpread",
    "SeriesFrame",
    "TargetAttributionRow",
    "attribute_clusters",
    "build_flicker_ledger",
    "client_gaps",
    "cluster_gaps",
    "tick_arrival_spreads",
]


# Single-linkage tolerance when merging per-client gaps into instance-wide
# clusters; 100 ms is about two sim ticks at 20 Hz and matches the tolerance
# the preserved-report analysis used.
_CLUSTER_TOLERANCE_MS: float = 100.0

# Post-gap window in which additional own-actor frames count toward the
# catch-up burst size (queued refreshes draining after a stall produce
# several; timer starvation resumes at the regular cadence and produces
# none beyond the resuming frame).
_CATCHUP_WINDOW_MS: float = 100.0

# Frames within this margin of a gap cluster are excluded from the
# per-tick arrival-spread baseline so the comparator describes normal
# delivery rather than recovery delivery.
_SPREAD_BASELINE_MARGIN_MS: float = 150.0

# A target whose CPU time reaches this fraction of a gap window's wall
# time was doing real work, not waiting.
_CPU_BUSY_FRACTION: float = 0.5

# A context-switch delta is 'elevated' when it exceeds both a multiple of
# the no-gap baseline p90 and the median plus a flat margin (the margin
# keeps tiny baselines from tripping on rounding noise).
_SWITCH_ELEVATION_FACTOR: float = 3.0
_SWITCH_ELEVATION_MARGIN: int = 8

_VERDICT_BUSY = "cpu-busy"
_VERDICT_PREEMPTED = "preempted"
_VERDICT_BLOCKED = "blocked"
_VERDICT_QUIET = "quiet"
_VERDICT_UNBASELINED = "unbaselined"

# The server facade runs the tick/reactor loop on a thread carrying this
# comm (the interpreter propagates threading.Thread names to the kernel),
# while the process main thread parks in the shutdown-signal wait and
# accounts almost nothing — so comm, not tid == pid, identifies the tick
# thread in an attribution.
_TICK_THREAD_COMM = "kith-server-run"

# The implicit frame anchoring each series' leading edge: a series whose
# first own-actor frame lands well after its movement window opened has a
# leading gap the frame-to-frame scan cannot see, so the window start is
# treated as a frame carrying this sentinel tick. Real input ticks are
# non-negative, so the sentinel never collides and a leading cluster's
# skipped-range alignment reads as trivially aligned.
_LEADING_EDGE_TICK = -1


@dataclass(frozen=True, slots=True)
class SeriesFrame:
    """One own-actor replication frame: receipt time plus echoed input tick.

    ``update_seq`` is the publisher-minted counter the record carried
    ; 0 for payloads written before the field existed.
    """

    ts_ns: int
    input_tick: int
    update_seq: int = 0


@dataclass(frozen=True, slots=True)
class ClientSeries:
    """One instrumented client's own-actor stream over the movement window."""

    name: str
    instance_index: int
    window_start_ns: int
    window_end_ns: int
    frames: tuple[SeriesFrame, ...]


@dataclass(frozen=True, slots=True)
class ClientGap:
    """One per-client continuity gap between consecutive own-actor frames.

    ``last_tick`` / ``first_tick`` are the input ticks echoed by the
    frames on either side of the hole; the ticks between them are the
    observation-time skip the ledger aligns across clients. A leading
    gap (window start to first frame) carries
    :data:`_LEADING_EDGE_TICK` as ``last_tick``: no frame preceded it.
    """

    start_ns: int
    end_ns: int
    last_tick: int
    first_tick: int


@dataclass(frozen=True, slots=True)
class GapClusterRecord:
    """One instance-wide gap cluster with its cross-client signatures."""

    instance_index: int
    start_ns: int
    end_ns: int
    duration_ms: int
    participants: int
    sampled_on_instance: int
    onset_spread_ms: int
    aligned_onset: bool
    last_ticks: tuple[int, ...]
    first_ticks: tuple[int, ...]
    ranges_agree: bool
    resume_spread_ms: int | None
    # Own-actor frames strictly after the resuming frame within
    # _CATCHUP_WINDOW_MS. Normal delivery cadence alone contributes about
    # two frames per 100 ms at 20 Hz, so only values well above that
    # indicate queued ticks draining after the hole.
    catchup_median: float


@dataclass(frozen=True, slots=True)
class InstanceTickSpread:
    """Cross-client per-tick arrival spread away from any gap cluster."""

    instance_index: int
    ticks: int
    median_ms: float | None
    p95_ms: float | None


@dataclass(frozen=True, slots=True)
class TargetAttributionRow:
    """One target's kernel accounting inside one gap cluster window.

    ``scope`` is ``total`` (every thread of the process summed), ``tick``
    (the thread named ``kith-server-run``, which drives the tick/reactor
    loop in a server subprocess), or ``main`` (the process main thread,
    whose tid equals the pid). ``wall_ms`` is the gap cluster's own span,
    while the counters accumulate over the window widened by one sample
    interval on each side. The cpu split bounds user-space compute against
    kernel time (allocator, reclaim, futex paths) inside the same window;
    verdicts read total cpu only.
    """

    target: str
    scope: str
    wall_ms: int
    cpu_ms: float
    utime_ms: float
    stime_ms: float
    nvcsw_delta: int
    nivcsw_delta: int
    verdict: str


@dataclass(frozen=True, slots=True)
class ClusterAttribution:
    """Every target's attribution rows for one gap cluster."""

    instance_index: int
    start_ns: int
    rows: tuple[TargetAttributionRow, ...]


@dataclass(frozen=True, slots=True)
class FlickerLedger:
    """The complete diagnostic attachment for one harness run."""

    window_start_ns: int
    interval_s: float
    clk_tck: int
    clusters: tuple[GapClusterRecord, ...]
    attributions: tuple[ClusterAttribution, ...]
    tick_spreads: tuple[InstanceTickSpread, ...]


def client_gaps(series: ClientSeries, threshold_ms: float) -> tuple[ClientGap, ...]:
    """Return the continuity gaps in one client's series above ``threshold_ms``.

    The scan covers two shapes of hole: a late first own-actor frame (the
    leading gap from the movement window's start, anchored by an implicit
    :data:`_LEADING_EDGE_TICK` frame — a stream that opened late stalls
    the same continuity the frame-to-frame scan measures) and every
    interior gap between consecutive frames. A series with no frames
    yields no gaps: a client that never received its own actor is the
    selection metric's subject, not a continuity gap.
    """
    frames = sorted(series.frames, key=lambda frame: frame.ts_ns)
    floor_ns = threshold_ms * 1_000_000.0
    gaps: list[ClientGap] = []
    if frames and frames[0].ts_ns - series.window_start_ns > floor_ns:
        gaps.append(
            ClientGap(
                start_ns=series.window_start_ns,
                end_ns=frames[0].ts_ns,
                last_tick=_LEADING_EDGE_TICK,
                first_tick=frames[0].input_tick,
            )
        )
    for prev, curr in pairwise(frames):
        delta = curr.ts_ns - prev.ts_ns
        if delta > floor_ns:
            gaps.append(
                ClientGap(
                    start_ns=prev.ts_ns,
                    end_ns=curr.ts_ns,
                    last_tick=prev.input_tick,
                    first_tick=curr.input_tick,
                )
            )
    return tuple(gaps)


def cluster_gaps(
    series: Sequence[ClientSeries],
    gap_threshold_ms: float = 200.0,
    tolerance_ms: float = _CLUSTER_TOLERANCE_MS,
) -> tuple[GapClusterRecord, ...]:
    """Merge per-client gaps into per-instance clusters, single-linkage.

    Gaps whose intervals chain within ``tolerance_ms`` of each other on
    the same instance form one cluster. Each record carries the
    cross-client signatures the ledger reads: how many sampled clients
    participated, how far the onsets spread, whether every participant
    skipped the same tick range, how tightly the first commonly resumed
    tick arrived across participants, and the median post-gap catch-up
    burst size.
    """
    tolerance_ns = tolerance_ms * 1_000_000.0
    catchup_ns = _CATCHUP_WINDOW_MS * 1_000_000.0
    by_instance: dict[int, list[ClientSeries]] = {}
    for entry in series:
        by_instance.setdefault(entry.instance_index, []).append(entry)

    records: list[GapClusterRecord] = []
    for instance_index, entries in sorted(by_instance.items()):
        frame_times = {entry.name: sorted(f.ts_ns for f in entry.frames) for entry in entries}
        members: list[tuple[ClientSeries, ClientGap]] = []
        for entry in entries:
            for gap in client_gaps(entry, gap_threshold_ms):
                members.append((entry, gap))
        members.sort(key=lambda item: item[1].start_ns)

        group: list[tuple[ClientSeries, ClientGap]] = []
        for item in members:
            if group and item[1].start_ns > _group_end(group) + tolerance_ns:
                records.append(
                    _cluster_record(instance_index, len(entries), group, frame_times, catchup_ns)
                )
                group = []
            group.append(item)
        if group:
            records.append(
                _cluster_record(instance_index, len(entries), group, frame_times, catchup_ns)
            )
    return tuple(records)


def tick_arrival_spreads(
    series: Sequence[ClientSeries],
    clusters: Sequence[GapClusterRecord],
    margin_ms: float = _SPREAD_BASELINE_MARGIN_MS,
) -> tuple[InstanceTickSpread, ...]:
    """Compute the normal per-tick cross-client arrival spread per instance.

    For every input tick seen by two or more clients of an instance,
    outside any gap cluster widened by ``margin_ms``, the spread is the
    max-minus-min arrival time across clients. The resulting
    distribution is the honest comparator for a cluster's resume spread:
    'tight' means tight relative to how the same server delivers on an
    ordinary tick.
    """
    margin_ns = int(margin_ms * 1_000_000)
    cluster_windows: dict[int, list[tuple[int, int]]] = {}
    for cluster in clusters:
        cluster_windows.setdefault(cluster.instance_index, []).append(
            (cluster.start_ns - margin_ns, cluster.end_ns + margin_ns)
        )

    records: list[InstanceTickSpread] = []
    for instance_index in sorted({entry.instance_index for entry in series}):
        arrivals: dict[int, list[int]] = {}
        windows = cluster_windows.get(instance_index, [])
        for entry in (s for s in series if s.instance_index == instance_index):
            for frame in entry.frames:
                if any(low <= frame.ts_ns <= high for low, high in windows):
                    continue
                arrivals.setdefault(frame.input_tick, []).append(frame.ts_ns)
        spreads = sorted(max(times) - min(times) for times in arrivals.values() if len(times) >= 2)
        records.append(
            InstanceTickSpread(
                instance_index=instance_index,
                ticks=len(spreads),
                median_ms=_percentile(spreads, 50) / 1_000_000 if spreads else None,
                p95_ms=_percentile(spreads, 95) / 1_000_000 if spreads else None,
            )
        )
    return tuple(records)


def attribute_clusters(
    trace: ProcTrace, clusters: Sequence[GapClusterRecord]
) -> tuple[ClusterAttribution, ...]:
    """Attribute every gap cluster against the sampler trace.

    Consecutive-sample counter deltas fully inside a cluster window
    (widened by one sample interval) measure what each target's threads
    did while the gap was open; deltas overlapping no widened cluster
    window form the no-gap baseline band. Per target the rows cover the
    process total and, where present, the tick/reactor thread and the
    main thread separately — worker-pool activity therefore cannot mask
    a stall on the tick thread.
    """
    series_by_target = _target_series(trace)
    interval_ns = int(trace.interval_s * 1_000_000_000)
    windows = [
        (cluster.start_ns - interval_ns, cluster.end_ns + interval_ns) for cluster in clusters
    ]
    baselines = {
        key: _baseline_band([d for d in deltas if not _overlaps_any(d, windows)])
        for key, deltas in series_by_target.items()
    }

    out: list[ClusterAttribution] = []
    for cluster in clusters:
        low = cluster.start_ns - interval_ns
        high = cluster.end_ns + interval_ns
        wall_ms = int((cluster.end_ns - cluster.start_ns) / 1_000_000)
        rows: list[TargetAttributionRow] = []
        for key in sorted(series_by_target):
            target, scope = key
            in_window = [d for d in series_by_target[key] if low <= d.t0_ns and d.t1_ns <= high]
            if not in_window:
                continue
            agg = _sum_deltas(in_window)
            cpu_ms = _ticks_to_ms(
                agg.utime_ticks + agg.stime_ticks,
                trace.clk_tck,
            )
            rows.append(
                TargetAttributionRow(
                    target=target,
                    scope=scope,
                    wall_ms=wall_ms,
                    cpu_ms=cpu_ms,
                    utime_ms=_ticks_to_ms(agg.utime_ticks, trace.clk_tck),
                    stime_ms=_ticks_to_ms(agg.stime_ticks, trace.clk_tck),
                    nvcsw_delta=agg.nvcsw,
                    nivcsw_delta=agg.nivcsw,
                    verdict=_verdict(
                        cpu_ms,
                        wall_ms,
                        agg.nvcsw,
                        agg.nivcsw,
                        len(in_window),
                        baselines[key],
                    ),
                )
            )
        out.append(
            ClusterAttribution(
                instance_index=cluster.instance_index,
                start_ns=cluster.start_ns,
                rows=tuple(rows),
            )
        )
    return tuple(out)


def build_flicker_ledger(trace: ProcTrace, series: Sequence[ClientSeries]) -> FlickerLedger:
    """Assemble the full ledger: clusters, attributions, spread baselines."""
    clusters = cluster_gaps(series)
    return FlickerLedger(
        window_start_ns=min((s.window_start_ns for s in series), default=0),
        interval_s=trace.interval_s,
        clk_tck=trace.clk_tck,
        clusters=clusters,
        attributions=attribute_clusters(trace, clusters),
        tick_spreads=tick_arrival_spreads(series, clusters),
    )


@dataclass(frozen=True, slots=True)
class _Delta:
    """One consecutive-sample delta for one target/scope."""

    t0_ns: int
    t1_ns: int
    utime_ticks: int
    stime_ticks: int
    nvcsw: int
    nivcsw: int


@dataclass(frozen=True, slots=True)
class _BaselineBand:
    """No-gap baseline statistics for one target/scope's switch deltas."""

    median_nvcsw: float
    p90_nvcsw: float
    median_nivcsw: float
    p90_nivcsw: float


def _target_series(trace: ProcTrace) -> dict[tuple[str, str], list[_Delta]]:
    """Flatten the trace into per-target/per-scope consecutive deltas.

    Scope ``total`` sums every thread of the target; scope ``tick``
    isolates the thread named ``kith-server-run`` (the tick/reactor
    thread); scope ``main`` isolates the tid == pid main thread when it
    is not itself the tick row. Targets whose pid vanished contribute
    empty series.
    """
    cumulative: dict[tuple[str, str], list[tuple[int, int, int, int, int]]] = {}
    for sample in trace.samples:
        for target_sample in sample.targets:
            threads = target_sample.threads
            total_utime = sum(t.utime_ticks for t in threads)
            total_stime = sum(t.stime_ticks for t in threads)
            total_nvcsw = sum(t.nvcsw for t in threads)
            total_nivcsw = sum(t.nivcsw for t in threads)
            tick = next((t for t in threads if t.comm == _TICK_THREAD_COMM), None)
            main = next((t for t in threads if t.tid == target_sample.pid), None)
            cumulative.setdefault((target_sample.target, "total"), []).append(
                (sample.t_ns, total_utime, total_stime, total_nvcsw, total_nivcsw)
            )
            if tick is not None:
                cumulative.setdefault((target_sample.target, "tick"), []).append(
                    (sample.t_ns, tick.utime_ticks, tick.stime_ticks, tick.nvcsw, tick.nivcsw)
                )
            if main is not None and main is not tick:
                cumulative.setdefault((target_sample.target, "main"), []).append(
                    (sample.t_ns, main.utime_ticks, main.stime_ticks, main.nvcsw, main.nivcsw)
                )
    out: dict[tuple[str, str], list[_Delta]] = {}
    for key, rows in cumulative.items():
        rows.sort(key=lambda row: row[0])
        out[key] = [
            _Delta(
                t0_ns=a[0],
                t1_ns=b[0],
                utime_ticks=max(0, b[1] - a[1]),
                stime_ticks=max(0, b[2] - a[2]),
                nvcsw=max(0, b[3] - a[3]),
                nivcsw=max(0, b[4] - a[4]),
            )
            for a, b in pairwise(rows)
        ]
    return out


def _overlaps_any(delta: _Delta, windows: Sequence[tuple[int, int]]) -> bool:
    """Whether a delta interval intersects any widened cluster window."""
    return any(delta.t0_ns <= high and delta.t1_ns >= low for low, high in windows)


def _baseline_band(deltas: list[_Delta]) -> _BaselineBand | None:
    """Band statistics over clean deltas; None when no clean baseline exists."""
    if not deltas:
        return None
    nvcsw = sorted(d.nvcsw for d in deltas)
    nivcsw = sorted(d.nivcsw for d in deltas)
    return _BaselineBand(
        median_nvcsw=_percentile(nvcsw, 50),
        p90_nvcsw=_percentile(nvcsw, 90),
        median_nivcsw=_percentile(nivcsw, 50),
        p90_nivcsw=_percentile(nivcsw, 90),
    )


def _sum_deltas(deltas: Sequence[_Delta]) -> _Delta:
    """Aggregate the deltas spanning one widened cluster window."""
    return _Delta(
        t0_ns=min(d.t0_ns for d in deltas),
        t1_ns=max(d.t1_ns for d in deltas),
        utime_ticks=sum(d.utime_ticks for d in deltas),
        stime_ticks=sum(d.stime_ticks for d in deltas),
        nvcsw=sum(d.nvcsw for d in deltas),
        nivcsw=sum(d.nivcsw for d in deltas),
    )


def _ticks_to_ms(ticks: int, clk_tck: int) -> float:
    """Convert CPU clock ticks to milliseconds."""
    if clk_tck <= 0:
        return 0.0
    return ticks * 1000.0 / clk_tck


def _elevated(value: int, intervals: int, band: _BaselineBand, voluntary: bool) -> bool:
    """Whether one window delta exceeds the no-gap elevation bar.

    The band is per-interval while ``value`` aggregates ``intervals``
    consecutive samples, so the bar scales with the interval count; a
    window of perfectly normal activity stays under it.
    """
    median = band.median_nvcsw if voluntary else band.median_nivcsw
    p90 = band.p90_nvcsw if voluntary else band.p90_nivcsw
    bar = max(_SWITCH_ELEVATION_FACTOR * p90, median + _SWITCH_ELEVATION_MARGIN)
    return value >= max(1, intervals) * bar


def _verdict(
    cpu_ms: float,
    wall_ms: int,
    nvcsw: int,
    nivcsw: int,
    intervals: int,
    band: _BaselineBand | None,
) -> str:
    """Classify one target/scope inside a gap window, most specific first.

    CPU at half the window wall or more means real work regardless of
    either counter. Rising nonvoluntary switches mean the thread was
    preempted while runnable (scheduling contention). Rising voluntary
    switches mean repeated kernel-side blocking cycles (lock thrash,
    wait wakeups). Flat counters with low CPU read quiet: no scheduler
    anomaly at this granularity — a single long voluntary wait is
    indistinguishable from idle here, which is exactly the case the
    catch-up-burst signature and a deeper in-server trace separate.
    """
    if cpu_ms >= _CPU_BUSY_FRACTION * wall_ms:
        return _VERDICT_BUSY
    if band is None:
        return _VERDICT_UNBASELINED
    if _elevated(nivcsw, intervals, band, voluntary=False):
        return _VERDICT_PREEMPTED
    if _elevated(nvcsw, intervals, band, voluntary=True):
        return _VERDICT_BLOCKED
    return _VERDICT_QUIET


def _cluster_record(
    instance_index: int,
    sampled_on_instance: int,
    group: list[tuple[ClientSeries, ClientGap]],
    frame_times: dict[str, list[int]],
    catchup_ns: float,
) -> GapClusterRecord:
    """Reduce one closed cluster group into its record."""
    starts = [gap.start_ns for _, gap in group]
    ends = [gap.end_ns for _, gap in group]
    last_ticks = tuple(sorted({gap.last_tick for _, gap in group}))
    first_ticks = tuple(sorted({gap.first_tick for _, gap in group}))

    resume_spread_ms: int | None = None
    if first_ticks:
        common_first = min(first_ticks)
        arrivals = [gap.end_ns for _, gap in group if gap.first_tick == common_first]
        if len(arrivals) >= 2:
            resume_spread_ms = int((max(arrivals) - min(arrivals)) / 1_000_000)

    catchups: list[int] = []
    for entry, gap in group:
        times = frame_times[entry.name]
        upper = bisect_right(times, gap.end_ns + catchup_ns)
        lower = bisect_right(times, gap.end_ns)
        catchups.append(upper - lower)

    start_ns = min(starts)
    end_ns = max(ends)
    return GapClusterRecord(
        instance_index=instance_index,
        start_ns=start_ns,
        end_ns=end_ns,
        duration_ms=int((end_ns - start_ns) / 1_000_000),
        participants=len({entry.name for entry, _ in group}),
        sampled_on_instance=sampled_on_instance,
        onset_spread_ms=int((max(starts) - min(starts)) / 1_000_000),
        aligned_onset=len(last_ticks) == 1,
        last_ticks=last_ticks,
        first_ticks=first_ticks,
        ranges_agree=len(last_ticks) == 1 and len(first_ticks) == 1,
        resume_spread_ms=resume_spread_ms,
        catchup_median=_median(catchups),
    )


def _group_end(group: list[tuple[ClientSeries, ClientGap]]) -> int:
    """Return the latest gap end in an open cluster group."""
    return max(gap.end_ns for _, gap in group)


def _median(values: Sequence[int]) -> float:
    """Nearest-rank median of ints; 0.0 when empty."""
    if not values:
        return 0.0
    ordered = sorted(values)
    mid = len(ordered) // 2
    if len(ordered) % 2 == 1:
        return float(ordered[mid])
    return (ordered[mid - 1] + ordered[mid]) / 2.0


def _percentile(ordered: Sequence[float], pct: int) -> float:
    """Nearest-rank percentile of an ascending sequence; 0.0 when empty."""
    if not ordered:
        return 0.0
    idx = max(0, min(len(ordered) - 1, (pct * len(ordered) + 99) // 100 - 1))
    return float(ordered[idx])
