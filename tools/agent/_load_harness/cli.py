"""Command-line dispatch and output for the scaling harness."""

from __future__ import annotations

import asyncio
import os
import shlex
import sys
from collections.abc import Sequence

from .arguments import (
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
from .constants import DEFAULT_STAGGER_MS, INSTRUMENTED_SAMPLE_SIZE
from .depth import select_spread_probe_indices
from .driver import run_profile
from .hosts import (
    distributed_factory,
    embedded_factory,
    external_factory,
    subprocess_cluster_factory,
)
from .report import GateReport
from .runtime import evaluate_certified_shape


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    profile = PROFILES[args.profile]
    try:
        profile = apply_client_override(profile, args.clients)
        workers = args.python_workers
        if workers is None:
            workers = env_python_workers(os.environ)
        profile = apply_worker_override(profile, workers)
        profile = apply_delivery_override(profile, args.delivery_strategy)
        profile = apply_delivery_workers_override(profile, args.delivery_workers)
        profile = apply_tiered_max_gap_override(profile, args.tiered_max_gap_ms)
        limits = observer_limits_from_args(args)
    except ValueError as exc:
        parser.error(str(exc))
    if (
        args.post_window_echo_settle_s is not None
        and os.environ.get(OBSERVER_RECALIBRATION_ENV) != "1"
    ):
        parser.error(
            "--post-window-echo-settle-s overrides the frozen drain; set "
            "KITH_OBSERVER_RECALIBRATION=1 to run the drain A/B diagnostic"
        )
    if args.spread_probe_size < 0:
        parser.error("--spread-probe-size must be >= 0")
    if args.server_affinity and not args.subprocess_cluster:
        parser.error("--server-affinity requires --subprocess-cluster")
    if args.native_apply and not args.embedded:
        parser.error("--native-apply requires --embedded")
    if args.tiered_max_gap_ms is not None and not args.subprocess_cluster:
        parser.error("--tiered-max-gap-ms requires --subprocess-cluster")
    stagger_ms = validate_driver_flags(parser, args, profile)
    if args.driver_worker is not None:
        from tools.agent.dual_driver import run_driver_worker

        return run_driver_worker(args=args, profile=profile)
    invocation = shlex.join([sys.executable, "-m", "tools.agent.load_harness", *sys.argv[1:]])
    shape = evaluate_certified_shape(profile, args)
    report: GateReport
    if args.dual_drivers:
        from tools.agent.dual_driver import run_dual_drivers

        report = asyncio.run(
            run_dual_drivers(
                profile,
                python_workers=profile.python_workers,
                delivery_strategy=profile.delivery_strategy,
                delivery_workers=profile.delivery_workers,
                tiered_max_gap_ms=profile.tiered_max_gap_ms,
                instance_count=args.instances,
                server_affinity=args.server_affinity,
                stagger_ms=stagger_ms or DEFAULT_STAGGER_MS,
                proc_trace_output=args.proc_trace_output,
                invocation=invocation,
                limits=limits,
                driver_child_cpus_a=args.driver_child_cpus_a,
                driver_child_cpus_b=args.driver_child_cpus_b,
                echo_settle_s=args.post_window_echo_settle_s,
                shape=shape,
            )
        )
    else:
        server_pids: list[int] = []
        evidence_budget = profile.evidence_budget_bytes
        spread_indices = select_spread_probe_indices(
            profile.actor_count, INSTRUMENTED_SAMPLE_SIZE, args.spread_probe_size
        )
        if args.subprocess_cluster:
            host_factory = subprocess_cluster_factory(
                python_workers=profile.python_workers,
                delivery_strategy=profile.delivery_strategy,
                delivery_workers=profile.delivery_workers,
                tiered_max_gap_ms=profile.tiered_max_gap_ms,
                instance_count=args.instances,
                server_affinity=args.server_affinity,
                server_pids_out=server_pids,
                evidence_budget_bytes=evidence_budget,
                extra_instrumented_indices=spread_indices,
            )
        elif args.distributed:
            host_factory = distributed_factory(
                python_workers=profile.python_workers,
                delivery_strategy=profile.delivery_strategy,
                delivery_workers=profile.delivery_workers,
                evidence_budget_bytes=evidence_budget,
                extra_instrumented_indices=spread_indices,
            )
        elif args.embedded:
            host_factory = embedded_factory(
                python_workers=profile.python_workers,
                delivery_strategy=profile.delivery_strategy,
                delivery_workers=profile.delivery_workers,
                native_apply=args.native_apply,
                evidence_budget_bytes=evidence_budget,
                extra_instrumented_indices=spread_indices,
            )
        else:
            host_factory = external_factory(args.host, args.port, spread_indices)
        report = asyncio.run(
            run_profile(
                profile,
                host_factory,
                server_pids=server_pids,
                proc_trace_output=args.proc_trace_output,
                invocation=invocation,
                limits=limits,
                echo_settle_s=args.post_window_echo_settle_s,
                spread_probe_indices=spread_indices,
                shape=shape,
            )
        )
    text = report.to_json(indent=2) if args.format == "json" else report.to_markdown()
    if args.output:
        with open(args.output, "w", encoding="utf-8") as handle:
            handle.write(text)
            handle.write("\n")
    else:
        sys.stdout.write(text)
        sys.stdout.write("\n")
    return 0 if report.passed else 1
