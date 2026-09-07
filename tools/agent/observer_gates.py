"""Observer-health gates for scaling-gate runs.

Pure classification over measurement DTOs: decides whether a harness run
is fit to vouch for its own statistics. An instrument whose driver
process saturated, or whose observed arrival cadence smeared far past the
healthy baseline, cannot certify the depth metrics it collected — such a
run classifies ``invalid-observer`` instead of masquerading as a system
failure. Independently, each server instance's own counters either
corroborate the submitted load (dispatch volume tracks submitted inputs,
no output-drain truncation, phase durations inside the recorded healthy
band) or contradict it (``invalid-server``); a failed control-plane
scrape yields an explicit inconclusive reason rather than a silent skip
or a fabricated failure.

The verdict rides the gate report additively: ``passed`` requires both
the numeric thresholds and an ``ok`` verdict, so classification only ever
adds failure modes. When the observer and the server both violate, the
verdict is ``invalid-observer`` — a suspect instrument also depresses
submitted load, which surfaces downstream as apparent band and drift
violations, so the observer finding takes precedence while every violated
check still appears in the reasons.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from tools.agent.gap_ledger import InstanceTickSpread
from tools.agent.proc_sampler import ProcTrace


__all__ = [
    "DEFAULT_OBSERVER_LIMITS",
    "VERDICT_INVALID_OBSERVER",
    "VERDICT_INVALID_SERVER",
    "VERDICT_OK",
    "InstanceServerSample",
    "ObservationVerdict",
    "ObserverLimits",
    "evaluate_observation",
]


VERDICT_OK = "ok"
VERDICT_INVALID_OBSERVER = "invalid-observer"
VERDICT_INVALID_SERVER = "invalid-server"


@dataclass(frozen=True, slots=True)
class ObserverLimits:
    """The frozen observer-health ceilings one run is judged against.

    ``max_driver_cores`` caps a driver sampler row's sustained cpu
    fraction over the movement window. The ceiling is structural, not
    aspirational: the driver is a single-threaded asyncio event loop
    plus the /proc sampler thread, so its sustained ceiling is ~1.0 core
    of loop work plus sampler allowance; the measured native-path runs
    sustain 0.82-0.88 cores processing the full delivered stream on
    dedicated physical cores (disjoint from the servers' slice), with
    0.1 s sampler intervals reaching 1.10 p95 and a 1.50 worst spike.
    1.5 keeps headroom over the sustained band while a runaway driver
    still fails loudly. ``spread_median_ms`` / ``spread_p95_ms`` cap the
    per-instance cross-client tick-arrival spread against the measured
    clean-run envelope (median 227-318 ms, p95 488-551 ms). The remaining
    fields bound the server side: ``dispatch_drift`` is the fractional
    slack between frames dispatched and inputs submitted,
    ``write_deferrals`` must stay at zero, and ``deliver_band_ms`` /
    ``compose_band_ms`` bracket the recorded healthy phase durations over
    the full-point movement window. The command-line knobs mirror these
    fields so a recalibration from a fresh canary run adjusts them
    without touching code.
    """

    max_driver_cores: float = 1.5
    spread_median_ms: float = 400.0
    spread_p95_ms: float = 800.0
    dispatch_drift: float = 0.02
    deliver_band_ms: tuple[int, int] = (2000, 4500)
    compose_band_ms: tuple[int, int] = (1500, 3000)


# The frozen limit set every evaluation entry point defaults to; the
# command-line knobs are its recalibration surface.
DEFAULT_OBSERVER_LIMITS = ObserverLimits()


@dataclass(frozen=True, slots=True)
class ObservationVerdict:
    """One run's measurement-health classification.

    ``klass`` is one of :data:`VERDICT_OK`,
    :data:`VERDICT_INVALID_OBSERVER`, or :data:`VERDICT_INVALID_SERVER`;
    ``reasons`` carries every violated or inconclusive check with its
    measured value, in check order (driver cpu, arrival spreads, server
    counters).
    """

    klass: str
    reasons: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class InstanceServerSample:
    """One instance's submitted-load expectation plus its scraped counters.

    ``inputs_submitted`` is the cohort's own input count over the movement
    window (what the instance's dispatch total should reconcile against).
    ``counters_present`` is False when the window was never driven or a
    boundary scrape failed, in which case the counter fields are
    placeholders and the server assertions report inconclusive instead of
    judging them.
    """

    instance_index: int
    inputs_submitted: int
    counters_present: bool
    dispatches: int
    write_deferrals: int
    view_locate_failures: int
    deliver_ns: int
    compose_ns: int


def evaluate_observation(
    limits: ObserverLimits,
    *,
    trace: ProcTrace,
    driver_rows: Sequence[str],
    window_start_ns: int,
    window_end_ns: int,
    tick_spreads: Sequence[InstanceTickSpread],
    instances: Sequence[InstanceServerSample],
) -> ObservationVerdict:
    """Classify one run's measurement health against ``limits``.

    Args:
        limits: The ceilings and bands to judge with.
        trace: The /proc sampler trace covering the movement window.
        driver_rows: Sampler row names whose cpu fraction stands for the
            load generator (a dual run names both driver processes; a
            single run names the harness row, meaningful only when the
            servers ran out of process).
        window_start_ns: Movement-window start on the monotonic clock.
        window_end_ns: Movement-window end on the monotonic clock.
        tick_spreads: Per-instance arrival-spread baselines away from gap
            clusters.
        instances: Per-instance submitted-load and counter samples.

    Returns:
        The aggregate verdict. ``klass`` derives from violations alone;
        inconclusive checks (missing samples, no shared-tick baseline,
        absent counters) contribute reasons but never fail the run. An
        empty reason tuple means every check was performed and passed.
    """
    cpu_reasons, cpu_unmeasured = _driver_cpu_reasons(
        limits, trace, driver_rows, window_start_ns, window_end_ns
    )
    spread_reasons, spread_unmeasured = _spread_reasons(limits, tick_spreads)
    server_reasons, server_unmeasured = _server_reasons(limits, instances)
    observer_reasons = cpu_reasons + spread_reasons
    reasons = observer_reasons + server_reasons + cpu_unmeasured + spread_unmeasured
    reasons += server_unmeasured
    if observer_reasons:
        return ObservationVerdict(VERDICT_INVALID_OBSERVER, tuple(reasons))
    if server_reasons:
        return ObservationVerdict(VERDICT_INVALID_SERVER, tuple(reasons))
    return ObservationVerdict(VERDICT_OK, tuple(reasons))


def _driver_cpu_reasons(
    limits: ObserverLimits,
    trace: ProcTrace,
    driver_rows: Sequence[str],
    window_start_ns: int,
    window_end_ns: int,
) -> tuple[list[str], list[str]]:
    """Judge each driver row's sustained cpu fraction over the window.

    Returns ``(violations, unmeasured)``: rows above the ceiling violate;
    rows without any in-window sample are reported unmeasured instead.
    """
    reasons: list[str] = []
    unmeasured: list[str] = []
    for row in driver_rows:
        fraction = _cpu_fraction(trace, row, window_start_ns, window_end_ns)
        if fraction is None:
            unmeasured.append(
                f"{row} has no /proc samples inside the movement window; "
                "driver-cpu check inconclusive"
            )
        elif fraction > limits.max_driver_cores:
            reasons.append(
                f"{row} sustained {fraction:.2f} cores over the movement window, "
                f"above the {limits.max_driver_cores:.2f}-core ceiling"
            )
    return reasons, unmeasured


def _spread_reasons(
    limits: ObserverLimits, tick_spreads: Sequence[InstanceTickSpread]
) -> tuple[list[str], list[str]]:
    """Judge each instance's away-from-cluster arrival-spread baseline.

    Returns ``(violations, unmeasured)``, mirroring the cpu check.
    """
    reasons: list[str] = []
    unmeasured: list[str] = []
    for spread in tick_spreads:
        if spread.median_ms is None or spread.p95_ms is None:
            unmeasured.append(
                f"instance {spread.instance_index} has no shared-tick arrival "
                "baseline; spread check inconclusive"
            )
            continue
        if spread.median_ms > limits.spread_median_ms:
            reasons.append(
                f"instance {spread.instance_index} tick-arrival spread median "
                f"{spread.median_ms:.1f} ms exceeds the "
                f"{limits.spread_median_ms:.1f} ms ceiling"
            )
        if spread.p95_ms > limits.spread_p95_ms:
            reasons.append(
                f"instance {spread.instance_index} tick-arrival spread p95 "
                f"{spread.p95_ms:.1f} ms exceeds the {limits.spread_p95_ms:.1f} ms ceiling"
            )
    return reasons, unmeasured


def _server_reasons(
    limits: ObserverLimits, instances: Sequence[InstanceServerSample]
) -> tuple[list[str], list[str]]:
    """Reconcile each instance's counters against the submitted load.

    Assertions run only on present counters: an absent scrape is reported
    unmeasured rather than judged, while a present-but-all-zero dispatch
    reading fails the drift check naturally (any submitted volume exceeds
    the drift slack). Returns ``(violations, unmeasured)``.
    """
    reasons: list[str] = []
    unmeasured: list[str] = []
    for sample in instances:
        if not sample.counters_present:
            unmeasured.append(
                f"instance {sample.instance_index} phase counters absent; "
                "server assertions inconclusive"
            )
            continue
        drift = abs(sample.dispatches - sample.inputs_submitted)
        allowed = limits.dispatch_drift * sample.inputs_submitted
        if drift > allowed:
            if sample.inputs_submitted == 0:
                reasons.append(
                    f"instance {sample.instance_index} dispatched {sample.dispatches} "
                    f"frames against 0 submitted inputs (ceiling "
                    f"{limits.dispatch_drift:.1%})"
                )
            else:
                reasons.append(
                    f"instance {sample.instance_index} dispatched {sample.dispatches} "
                    f"frames against {sample.inputs_submitted} submitted inputs "
                    f"({drift / sample.inputs_submitted:.2%} drift, ceiling "
                    f"{limits.dispatch_drift:.1%})"
                )
        if sample.write_deferrals != 0:
            reasons.append(
                f"instance {sample.instance_index} truncated "
                f"{sample.write_deferrals} output drains (write deferrals) "
                "against 0 allowed"
            )
        if sample.view_locate_failures != 0:
            reasons.append(
                f"instance {sample.instance_index} failed to locate subscribers in "
                f"their windows on {sample.view_locate_failures} compositions "
                "(view locate failures) against 0 allowed; a nonzero reading is "
                "a subscription-window ownership defect, not load"
            )
        deliver_lo, deliver_hi = limits.deliver_band_ms
        deliver_ms = sample.deliver_ns / 1_000_000
        if not deliver_lo <= deliver_ms <= deliver_hi:
            reasons.append(
                f"instance {sample.instance_index} deliver phase took {deliver_ms:.0f} ms, "
                f"outside the [{deliver_lo}, {deliver_hi}] ms band"
            )
        compose_lo, compose_hi = limits.compose_band_ms
        compose_ms = sample.compose_ns / 1_000_000
        if not compose_lo <= compose_ms <= compose_hi:
            reasons.append(
                f"instance {sample.instance_index} compose phase took {compose_ms:.0f} ms, "
                f"outside the [{compose_lo}, {compose_hi}] ms band"
            )
    return reasons, unmeasured


def _cpu_fraction(
    trace: ProcTrace, row: str, window_start_ns: int, window_end_ns: int
) -> float | None:
    """Return a sampler row's cpu fraction over the window, None if unmeasured.

    Consecutive-sample cpu-tick deltas lying fully inside the window sum
    into the numerator; the wall time is the window span itself. Rows
    without any in-window delta (an unsampled row, a vanished pid, or a
    degenerate window) measure nothing and return None.
    """
    total_ticks = 0
    measured = False
    last: tuple[int, int] | None = None
    for sample in trace.samples:
        target = next((t for t in sample.targets if t.target == row), None)
        if target is None:
            continue
        cumulative = sum(thread.cpu_ticks for thread in target.threads)
        if last is not None and window_start_ns <= last[0] and sample.t_ns <= window_end_ns:
            total_ticks += max(0, cumulative - last[1])
            measured = True
        last = (sample.t_ns, cumulative)
    wall_ms = (window_end_ns - window_start_ns) / 1_000_000
    if not measured or wall_ms <= 0 or trace.clk_tck <= 0:
        return None
    return total_ticks * 1000.0 / trace.clk_tck / wall_ms
