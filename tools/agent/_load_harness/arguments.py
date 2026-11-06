"""Scaling CLI options and validated profile overrides."""

from __future__ import annotations

import argparse
import os
from collections.abc import Mapping
from dataclasses import replace

from tools.agent.observer_gates import (
    ObserverLimits,
)

from .constants import DEFAULT_STAGGER_MS, SPREAD_PROBE_SIZE
from .profiles import DENSE_1000, DISTRIBUTED_2000, SMOKE_DENSE, SMOKE_DISTRIBUTED, ScalingProfile


PROFILES: dict[str, ScalingProfile] = {
    DENSE_1000.name: DENSE_1000,
    DISTRIBUTED_2000.name: DISTRIBUTED_2000,
    SMOKE_DENSE.name: SMOKE_DENSE,
    SMOKE_DISTRIBUTED.name: SMOKE_DISTRIBUTED,
}

FORMATS = ("md", "json")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="kith-load-harness",
        description=(
            "Drive a numeric scaling-gate profile against a kith server and "
            "render a §4.4 gate report."
        ),
    )
    parser.add_argument(
        "--profile",
        "-p",
        choices=sorted(PROFILES),
        default=DENSE_1000.name,
        help="Scaling profile to drive (default: dense-1000).",
    )
    parser.add_argument(
        "--embedded",
        action="store_true",
        help="Boot an in-process embedded server per run instead of targeting "
        "an external server with --host/--port.",
    )
    parser.add_argument(
        "--distributed",
        action="store_true",
        help="Boot an in-process multi-instance cluster (shared coordination "
        "bus) per run instead of targeting an external server with "
        "--host/--port.",
    )
    parser.add_argument(
        "--subprocess-cluster",
        action="store_true",
        help="Boot one embedded-server OS process per instance and drive it "
        "from this process, decoupling the harness loop from the servers' "
        "worker-pool GILs. The full distributed-2000 gate uses this path; "
        "use --distributed for the in-process smoke cluster.",
    )
    parser.add_argument(
        "--instances",
        type=int,
        default=2,
        help="Instance count for --subprocess-cluster (default: 2).",
    )
    parser.add_argument(
        "--server-affinity",
        type=str,
        default=None,
        help=(
            "CPU list the subprocess-cluster servers are pinned to via a "
            "taskset -c argv wrapper (e.g. '8-15' or '0,2,4'); requires "
            "--subprocess-cluster. Default: no pinning."
        ),
    )
    parser.add_argument(
        "--clients",
        type=int,
        default=None,
        help=(
            "Override the profile actor count with a lighter drive. The "
            "worker pool size, instance count, cadence, and duration stay "
            "from the profile and the other flags, so a smaller client "
            "count re-baselines the same server configuration instead of "
            "shrinking the per-tick work the gate measures. Defaults to "
            "the profile's actor count."
        ),
    )
    parser.add_argument(
        "--python-workers",
        type=int,
        default=None,
        help=(
            "Override the profile handler-worker pool size. Precedence: "
            "this flag, then KITH_PYTHON_WORKERS, then the profile's "
            "pool size, then the server default (1 under the GIL). 0 "
            "requests the server default explicitly. Defaults to the "
            "profile's worker count."
        ),
    )
    parser.add_argument(
        "--delivery-workers",
        type=int,
        default=None,
        help=(
            "Override the profile delivery-executor thread count. "
            "0 keeps delivery inline on the reactor thread; a "
            "positive count moves the tick's deliver pass onto that many "
            "executor threads. Defaults to the profile's count (0 on every "
            "shipped profile; the run command names it explicitly)."
        ),
    )
    parser.add_argument(
        "--delivery-strategy",
        type=str,
        default=None,
        help=(
            "Override the profile's gateway delivery strategy (for "
            "example 'full' or 'tiered'). Defaults to the profile's own "
            "strategy, so the distributed fidelity gate runs tiered and "
            "an explicit --delivery-strategy full re-baselines the same "
            "drive on the default preset."
        ),
    )
    parser.add_argument(
        "--tiered-max-gap-ms",
        type=int,
        default=None,
        help=(
            "Override the tiered delivery backstop: the maximum silence "
            "(ms) before any subject is refreshed regardless of change "
            "(0 pins the documented default). Diagnostic knob for "
            "attributing periodic behavior to the backstop refresh; "
            "requires --subprocess-cluster. Defaults to the preset's "
            "documented cadence."
        ),
    )
    parser.add_argument(
        "--dual-drivers",
        action="store_true",
        help=(
            "Drive the subprocess cluster with two load-generator processes "
            "instead of one: each process carries exactly one instance's "
            "clients, and a fixed monotonic-clock stagger holds between the "
            "two movement windows. The report, thresholds, and artifact "
            "schema are identical to the single-driver run; requires "
            "--subprocess-cluster and two instances."
        ),
    )
    parser.add_argument(
        "--driver-stagger-ms",
        type=int,
        default=None,
        help=(
            "Fixed offset (ms) between the two drivers' movement windows "
            "under --dual-drivers (defaults to 600 there). Must be >= 1."
        ),
    )
    parser.add_argument(
        "--native-apply",
        action="store_true",
        help=(
            "Run the movement-apply path on the C handler pool for "
            "--embedded hosts; the default keeps the Python apply "
            "path the embedded host is certified under. Requires "
            "--embedded."
        ),
    )
    parser.add_argument(
        "--driver-worker",
        type=int,
        default=None,
        metavar="INDEX",
        help=(
            "Internal: run one partitioned driver cohort against the "
            "pre-booted instance at this index, writing raw results to "
            "--result-output instead of a report. Set by the dual-driver "
            "parent, not by hand."
        ),
    )
    parser.add_argument(
        "--driver-cpus",
        type=str,
        default=None,
        metavar="CPULIST",
        help=(
            "Internal: pin this driver worker to these comma-separated "
            "CPUs before any cohort work starts. Set by the dual-driver "
            "parent, not by hand."
        ),
    )
    parser.add_argument(
        "--driver-child-cpus-a",
        type=str,
        default=None,
        metavar="CPULIST",
        help=(
            "CPUs for dual-driver worker A (e.g. 0,8 on an SMT pair). "
            "Keeps the two hot loops off one physical core."
        ),
    )
    parser.add_argument(
        "--driver-child-cpus-b",
        type=str,
        default=None,
        metavar="CPULIST",
        help="CPUs for dual-driver worker B; see --driver-child-cpus-a.",
    )
    parser.add_argument(
        "--gateway-port",
        type=int,
        default=None,
        help="Internal: gateway port the driver worker's clients connect to.",
    )
    parser.add_argument(
        "--control-port",
        type=int,
        default=None,
        help="Internal: control-plane port the driver worker scrapes.",
    )
    parser.add_argument(
        "--movement-anchor-ns",
        type=int,
        default=None,
        help=(
            "Internal: absolute CLOCK_MONOTONIC deadline (ns) the worker's "
            "movement window waits for before opening."
        ),
    )
    parser.add_argument(
        "--instrumented-sample-size",
        type=int,
        default=None,
        help=(
            "Internal: depth-metric sample size for this worker's cohort "
            "(the union across workers reproduces the single-driver "
            "composition)."
        ),
    )
    parser.add_argument(
        "--spread-probe-size",
        type=int,
        default=SPREAD_PROBE_SIZE,
        help=(
            "Spread-probe cohort size: extra depth-instrumented clients at "
            "evenly spaced submit positions after the head sample (0 "
            "disables the probe). Diagnostic only: the gate metrics stay "
            "computed over the head sample."
        ),
    )
    parser.add_argument(
        "--result-output",
        type=str,
        default=None,
        help="Internal: path the driver worker writes its raw result JSON to.",
    )
    parser.add_argument(
        "--observer-max-driver-cores",
        type=float,
        default=None,
        help=(
            "Observer gate: the sustained driver-process cpu fraction "
            "(over the movement window's wall) above which the run "
            "classifies invalid-observer. Defaults to the frozen 1.5."
        ),
    )
    parser.add_argument(
        "--observer-spread-median-ms",
        type=float,
        default=None,
        help=(
            "Observer gate: per-instance tick-arrival spread median above "
            "which the run classifies invalid-observer. Defaults to the "
            "frozen 400."
        ),
    )
    parser.add_argument(
        "--observer-spread-p95-ms",
        type=float,
        default=None,
        help=(
            "Observer gate: per-instance tick-arrival spread p95 above "
            "which the run classifies invalid-observer. Defaults to the "
            "frozen 800."
        ),
    )
    parser.add_argument(
        "--observer-dispatch-drift",
        type=float,
        default=None,
        help=(
            "Observer gate: fractional slack between frames dispatched and "
            "inputs submitted beyond which an instance classifies "
            "invalid-server. Defaults to the frozen 0.02."
        ),
    )
    parser.add_argument(
        "--observer-deliver-band-ms",
        type=int,
        nargs=2,
        default=None,
        metavar=("LOW", "HIGH"),
        help=(
            "Observer gate: the inclusive deliver-phase duration band "
            "(ms) each instance's movement-window counter must land in. "
            "Defaults to the frozen 2000 4500."
        ),
    )
    parser.add_argument(
        "--observer-compose-band-ms",
        type=int,
        nargs=2,
        default=None,
        metavar=("LOW", "HIGH"),
        help=(
            "Observer gate: the inclusive compose-phase duration band "
            "(ms) each instance's movement-window counter must land in. "
            "Defaults to the frozen 1500 3000."
        ),
    )
    parser.add_argument(
        "--post-window-echo-settle-s",
        type=float,
        default=None,
        help=(
            "Diagnostic: override the fixed post-window echo drain "
            "(seconds; frozen default 2.0). Refused without "
            "KITH_OBSERVER_RECALIBRATION=1; the report renders the value "
            "that drove the run."
        ),
    )
    parser.add_argument(
        "--host",
        type=str,
        default="127.0.0.1",
        help="Server host (default: 127.0.0.1; ignored with --embedded).",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=7777,
        help="Server gateway port (default: 7777; ignored with --embedded).",
    )
    parser.add_argument(
        "--format",
        choices=FORMATS,
        default="md",
        help="Output format printed to stdout (default: md).",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Write the report to this file (stdout otherwise).",
    )
    parser.add_argument(
        "--proc-trace-output",
        type=str,
        default=None,
        help=(
            "Write the raw /proc sampler trace and the per-client arrival "
            "series as JSON to this file, keeping every sample and frame "
            "the report's diagnostic tables summarize."
        ),
    )
    return parser


def apply_client_override(profile: ScalingProfile, clients: int | None) -> ScalingProfile:
    """Return ``profile`` with its actor count overridden by ``clients``.

    A None ``clients`` leaves the profile unchanged (the CLI default). A
    positive integer replaces ``actor_count`` only; the worker pool size,
    instance count, cadence, and duration stay from the profile and the
    other flags, so a smaller client count re-baselines the same server
    configuration against a lighter drive instead of shrinking the
    per-tick work the gate measures.
    """
    if clients is None:
        return profile
    if clients < 1:
        raise ValueError(f"--clients must be >= 1, got {clients}")
    return replace(profile, actor_count=clients)


def apply_worker_override(profile: ScalingProfile, workers: int | None) -> ScalingProfile:
    """Return ``profile`` with its worker pool size overridden by ``workers``.

    A None ``workers`` leaves the profile unchanged (the CLI and
    environment defaults). A non-negative integer replaces
    ``python_workers`` only; the actor count, instance count, cadence,
    and duration stay from the profile and the other flags, so a
    different pool size re-baselines the same drive configuration
    instead of shrinking the per-tick work the gate measures. 0 requests
    the server default explicitly (the size-1 pool under the GIL).
    """
    if workers is None:
        return profile
    if workers < 0:
        raise ValueError(f"worker pool override must be >= 0, got {workers}")
    return replace(profile, python_workers=workers)


def apply_delivery_override(profile: ScalingProfile, strategy: str | None) -> ScalingProfile:
    """Return ``profile`` with its delivery strategy overridden by ``strategy``.

    A None ``strategy`` leaves the profile unchanged (the CLI default). A
    non-empty string replaces ``delivery_strategy`` only; the actor count,
    pool size, cadence, and duration stay from the profile, so a run with
    the other preset re-baselines the same drive configuration and the
    fidelity delta is attributable to the strategy alone. An empty string
    is a usage error rather than a silent factory-default selection.
    """
    if strategy is None:
        return profile
    if not strategy:
        raise ValueError("--delivery-strategy must be a non-empty name")
    return replace(profile, delivery_strategy=strategy)


def apply_tiered_max_gap_override(
    profile: ScalingProfile, max_gap_ms: int | None
) -> ScalingProfile:
    """Return ``profile`` with the tiered backstop period overridden.

    A None ``max_gap_ms`` leaves the profile unchanged (the CLI default).
    A non-negative integer replaces ``tiered_max_gap_ms`` only; 0 requests
    the documented default explicitly (the gateway config treats 0 as its
    default selector), and a positive value raises the paranoia-refresh
    period so periodic gap behavior can be attributed to the backstop.
    The override reaches the subprocess cluster path alone, where the
    tiered tuning is attached at the server command line.
    """
    if max_gap_ms is None:
        return profile
    if max_gap_ms < 0:
        raise ValueError("--tiered-max-gap-ms must be >= 0")
    return replace(profile, tiered_max_gap_ms=max_gap_ms)


def apply_delivery_workers_override(profile: ScalingProfile, workers: int | None) -> ScalingProfile:
    """Return ``profile`` with its delivery executor count overridden.

    A None ``workers`` leaves the profile unchanged (the CLI default). A
    non-negative integer replaces ``delivery_workers`` only; 0 keeps
    delivery inline on the reactor thread, and a positive count moves the
    tick's deliver pass onto that many executor threads. The
    rest of the profile stays from the profile and the other flags, so an
    executor on/off pair re-baselines the same drive configuration.
    """
    if workers is None:
        return profile
    if workers < 0:
        raise ValueError(f"--delivery-workers must be >= 0, got {workers}")
    return replace(profile, delivery_workers=workers)


OBSERVER_RECALIBRATION_ENV = "KITH_OBSERVER_RECALIBRATION"


def observer_limits_from_args(args: argparse.Namespace) -> ObserverLimits:
    """Resolve the observer-gate knobs, frozen defaults when unset.

    The frozen limit set is the gate's calibration: an explicitly passed
    knob is refused unless ``KITH_OBSERVER_RECALIBRATION=1`` declares a
    deliberate recalibration run, so an override can never reach a gate
    report by accident.

    Raises ValueError on an unusable combination or a refused override,
    so the caller can reject the run before driving anything.
    """
    overrides = [
        flag
        for flag, value in (
            ("--observer-max-driver-cores", args.observer_max_driver_cores),
            ("--observer-spread-median-ms", args.observer_spread_median_ms),
            ("--observer-spread-p95-ms", args.observer_spread_p95_ms),
            ("--observer-dispatch-drift", args.observer_dispatch_drift),
            ("--observer-deliver-band-ms", args.observer_deliver_band_ms),
            ("--observer-compose-band-ms", args.observer_compose_band_ms),
        )
        if value is not None
    ]
    if overrides and os.environ.get(OBSERVER_RECALIBRATION_ENV) != "1":
        raise ValueError(
            "observer limits are frozen; set KITH_OBSERVER_RECALIBRATION=1 "
            f"to recalibrate deliberately (overridden: {', '.join(overrides)})"
        )
    limits = ObserverLimits()
    if args.observer_max_driver_cores is not None:
        if args.observer_max_driver_cores <= 0:
            raise ValueError("--observer-max-driver-cores must be > 0")
        limits = replace(limits, max_driver_cores=args.observer_max_driver_cores)
    if args.observer_spread_median_ms is not None:
        if args.observer_spread_median_ms <= 0:
            raise ValueError("--observer-spread-median-ms must be > 0")
        limits = replace(limits, spread_median_ms=args.observer_spread_median_ms)
    if args.observer_spread_p95_ms is not None:
        if args.observer_spread_p95_ms <= 0:
            raise ValueError("--observer-spread-p95-ms must be > 0")
        limits = replace(limits, spread_p95_ms=args.observer_spread_p95_ms)
    if args.observer_dispatch_drift is not None:
        drift = args.observer_dispatch_drift
        if not 0 <= drift <= 1:
            raise ValueError("--observer-dispatch-drift must be in [0, 1]")
        limits = replace(limits, dispatch_drift=drift)
    if args.observer_deliver_band_ms is not None:
        low, high = args.observer_deliver_band_ms
        if low < 0 or high < low:
            raise ValueError("--observer-deliver-band-ms needs 0 <= LOW <= HIGH")
        limits = replace(limits, deliver_band_ms=(low, high))
    if args.observer_compose_band_ms is not None:
        low, high = args.observer_compose_band_ms
        if low < 0 or high < low:
            raise ValueError("--observer-compose-band-ms needs 0 <= LOW <= HIGH")
        limits = replace(limits, compose_band_ms=(low, high))
    return limits


def env_python_workers(environ: Mapping[str, str]) -> int | None:
    """Return the ``KITH_PYTHON_WORKERS`` pool-size override from ``environ``.

    The variable is the framework's handler-worker-pool tunable; the
    harness honors it as the tier between an explicit ``--python-workers``
    flag and the profile's own value. An unset variable returns None;
    a non-integer or negative value raises ValueError so the caller can
    reject the run instead of silently ignoring the setting.
    """
    raw = environ.get("KITH_PYTHON_WORKERS")
    if raw is None:
        return None
    try:
        workers = int(raw)
    except ValueError:
        raise ValueError(f"KITH_PYTHON_WORKERS must be an integer, got {raw!r}") from None
    if workers < 0:
        raise ValueError(f"KITH_PYTHON_WORKERS must be >= 0, got {workers}")
    return workers


def validate_driver_flags(
    parser: argparse.ArgumentParser, args: argparse.Namespace, profile: ScalingProfile
) -> int | None:
    """Reject cross-flag combinations the driver modes cannot honor.

    Returns the resolved movement-window stagger (ms) for the dual-driver
    parent, else None. The stagger defaults to
    :data:`_DEFAULT_STAGGER_MS`; an explicit value without --dual-drivers
    is a usage error so a stray knob never silently changes a standard
    run, and worker mode fails fast here rather than mid-run when its
    internal wiring flags are missing.
    """
    if args.driver_worker is not None:
        exclusive = (
            ("--subprocess-cluster", args.subprocess_cluster),
            ("--embedded", args.embedded),
            ("--distributed", args.distributed),
            ("--dual-drivers", args.dual_drivers),
        )
        for flag, given in exclusive:
            if given:
                parser.error(f"--driver-worker cannot be combined with {flag}")
        required = (
            ("--gateway-port", args.gateway_port),
            ("--control-port", args.control_port),
            ("--movement-anchor-ns", args.movement_anchor_ns),
            ("--result-output", args.result_output),
        )
        for flag, value in required:
            if value is None:
                parser.error(f"--driver-worker requires {flag}")
        if args.instrumented_sample_size is not None and args.instrumented_sample_size < 0:
            parser.error("--instrumented-sample-size must be >= 0")
        return None
    if args.driver_stagger_ms is not None and args.driver_stagger_ms < 0:
        parser.error("--driver-stagger-ms must be >= 0")
    child_cpus = (
        ("--driver-child-cpus-a", args.driver_child_cpus_a),
        ("--driver-child-cpus-b", args.driver_child_cpus_b),
    )
    if not args.dual_drivers:
        for flag, spec in child_cpus:
            if spec is not None:
                parser.error(f"{flag} requires --dual-drivers")
        if args.driver_stagger_ms is not None:
            parser.error("--driver-stagger-ms requires --dual-drivers")
        return None
    from tools.agent.dual_driver import parse_cpu_list

    for flag, spec in child_cpus:
        if spec is None:
            continue
        try:
            parse_cpu_list(spec)
        except ValueError as exc:
            parser.error(f"{flag}: {exc}")
    if not args.subprocess_cluster:
        parser.error("--dual-drivers requires --subprocess-cluster")
    if args.instances != 2:
        parser.error("--dual-drivers drives one cohort per instance; --instances must be 2")
    if profile.actor_count % 2:
        parser.error(f"--dual-drivers needs an even client count, got {profile.actor_count}")
    return DEFAULT_STAGGER_MS if args.driver_stagger_ms is None else args.driver_stagger_ms
