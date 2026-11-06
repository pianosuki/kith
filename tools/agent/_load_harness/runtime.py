"""Runtime metadata and certified-recipe validation."""

from __future__ import annotations

import argparse
import os
import platform
import sys
from dataclasses import dataclass
from pathlib import Path

from .profiles import ScalingProfile


@dataclass(frozen=True, slots=True)
class RunEnvelope:
    """The build and machine state a gate run executed under.

    The certified gate readings are interpreter- and build-sensitive, so
    every report carries the state its numbers were produced under: the
    build type of the C libraries the servers ran, the kernel, the CPU
    model and thread count, the cpufreq governor, and the interpreter with
    its GIL status. Fields the host does not expose read ``None``.
    """

    build_type: str | None
    kernel: str
    cpu_model: str | None
    cpu_threads: int | None
    governor: str | None
    interpreter: str
    gil_status: str


@dataclass(frozen=True, slots=True)
class RunShape:
    """The resolved movement-apply regime and the certified-shape verdict.

    The certified configurations differ from every regression baseline on
    axes an argv stamp cannot see: the apply knob is a shell environment
    assignment (``KITH_NATIVE_APPLY``) on the subprocess paths and a flag
    on the embedded path, and the certified recipes bind the cluster host,
    driver split, delivery executor, actor count, and interpreter around
    it. The report carries the resolved verdict so the evidence consumer
    can mechanically refuse a non-certified report as certified evidence.

    ``apply_regime`` is ``native``, ``python``, or ``external`` (a target
    this process does not boot and cannot observe).
    ``certified_shape`` is True/False for the profiles with a defined
    certified configuration and None for the rest;
    ``deviations`` names each departure from the certified recipe.
    """

    apply_regime: str
    certified_shape: bool | None
    deviations: tuple[str, ...]


def read_build_type(kith_lib: str | None) -> str | None:
    """Read ``CMAKE_BUILD_TYPE`` from the configured build tree.

    ``KITH_LIB`` names the directory the servers load the C libraries from
    (a CMake build preset directory, e.g. ``build/release``); its cache
    states the build type. The parent directory is checked as a fallback
    for library layouts that point one level inside the build tree.
    """
    candidates: list[Path] = []
    if kith_lib:
        root = Path(kith_lib)
        candidates += [root / "CMakeCache.txt", root.parent / "CMakeCache.txt"]
    else:
        candidates += [
            Path("build/release/CMakeCache.txt"),
            Path("build/debug/CMakeCache.txt"),
        ]
    for cache in candidates:
        try:
            for line in cache.read_text(encoding="utf-8").splitlines():
                if line.startswith("CMAKE_BUILD_TYPE:STRING="):
                    return line.split("=", 1)[1].strip() or None
        except OSError:
            continue
    return None


def read_gil_status() -> str:
    """Report the interpreter's GIL status as a human-readable string."""
    probe = getattr(sys, "_is_gil_enabled", None)
    if not callable(probe):
        return "GIL enabled"
    if probe() is False:
        return "GIL disabled"
    return "GIL enabled"


def resolve_apply_regime(args: argparse.Namespace) -> str:
    """Resolve the movement-apply regime the run's servers use.

    The subprocess paths inherit ``KITH_NATIVE_APPLY`` from the
    environment — the child validates the value and refuses to boot on
    anything but ``0`` or ``1`` — while the embedded host takes the
    explicit ``--native-apply`` flag. The in-process distributed host
    wires no native handlers, so it stays on the python path regardless
    of the environment. The host precedence mirrors ``main``'s factory
    dispatch: subprocess cluster, in-process distributed, embedded,
    external. An external target's regime is not observable from this
    process.
    """
    if args.subprocess_cluster or args.dual_drivers:
        return "native" if os.environ.get("KITH_NATIVE_APPLY") == "1" else "python"
    if args.distributed:
        return "python"
    if args.embedded:
        return "native" if args.native_apply else "python"
    return "external"


def evaluate_certified_shape(profile: ScalingProfile, args: argparse.Namespace) -> RunShape:
    """Derive the certified-shape verdict against the documented recipes.

    The certified recipes (docs/guides/scaling_checklist.md) bind the
    cluster host, driver split, delivery executor, apply regime, actor
    count, and interpreter per profile; every departure is named in the
    deviation list. The derivation never fails a run — the documented
    regression baselines (GIL runs, python-apply ladders, full-preset
    re-baselines) are legitimate non-certified shapes, because
    certification is a property of a report, judged by the evidence
    consumer. Profiles without a defined certified shape report None.
    """
    regime = resolve_apply_regime(args)
    deviations: list[str] = []
    if profile.name == "distributed-2000":
        if not args.subprocess_cluster:
            deviations.append("the run did not use --subprocess-cluster")
        if not args.dual_drivers:
            deviations.append("--dual-drivers absent (single driver loop)")
        if args.instances != 2:
            deviations.append(f"--instances {args.instances} (certified: 2)")
        if profile.delivery_workers != 2:
            deviations.append(f"--delivery-workers {profile.delivery_workers} (certified: 2)")
        if profile.delivery_strategy != "tiered":
            deviations.append(
                f"delivery strategy {profile.delivery_strategy!r} (certified: tiered)"
            )
        if profile.actor_count != 2000:
            deviations.append(f"--clients override to {profile.actor_count} (certified: 2000)")
        if regime == "external":
            deviations.append("apply regime not observable (external target)")
        elif regime != "native":
            raw = os.environ.get("KITH_NATIVE_APPLY")
            deviations.append(
                f"python apply path (KITH_NATIVE_APPLY is {raw!r}; native apply requires \"1\")"
            )
        if read_gil_status() != "GIL disabled":
            deviations.append(
                "standard interpreter (GIL enabled; the certified shape runs free-threaded Python)"
            )
    elif profile.name == "dense-1000":
        if not args.embedded:
            deviations.append("the run did not use --embedded")
        if regime == "external":
            deviations.append("apply regime not observable (external target)")
        elif regime != "python":
            deviations.append(
                "native apply path (the dense certified configuration holds the python apply path)"
            )
        if profile.actor_count != 1000:
            deviations.append(f"--clients override to {profile.actor_count} (certified: 1000)")
    else:
        return RunShape(apply_regime=regime, certified_shape=None, deviations=())
    return RunShape(
        apply_regime=regime,
        certified_shape=not deviations,
        deviations=tuple(deviations),
    )


def collect_run_envelope() -> RunEnvelope:
    """Snapshot the build and machine state at report time."""
    cpu_model: str | None = None
    try:
        for line in Path("/proc/cpuinfo").read_text(encoding="utf-8").splitlines():
            if line.startswith("model name"):
                cpu_model = line.split(":", 1)[1].strip()
                break
    except OSError:
        cpu_model = None
    governor: str | None = None
    try:
        governor = (
            Path("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor")
            .read_text(encoding="utf-8")
            .strip()
        )
    except OSError:
        governor = None
    return RunEnvelope(
        build_type=read_build_type(os.environ.get("KITH_LIB")),
        kernel=platform.release(),
        cpu_model=cpu_model,
        cpu_threads=os.cpu_count(),
        governor=governor,
        interpreter=".".join(str(part) for part in sys.version_info[:3]),
        gil_status=read_gil_status(),
    )


RunEnvelope.__module__ = "tools.agent.load_harness"
RunShape.__module__ = "tools.agent.load_harness"
