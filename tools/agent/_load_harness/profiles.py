"""Scaling profiles, immutable thresholds and shared verdict checks."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from .constants import BREADTH_EVIDENCE_BUDGET_BYTES


@dataclass(frozen=True, slots=True)
class ScalingProfile:
    """A numeric scaling-gate profile (actor count, cadence, duration).

    ``topology`` declares the topology the profile targets: ``"embedded"``
    for the dense single-instance gate, ``"distributed"`` for the
    multi-instance distributed gate. The harness dispatches on this value.

    ``python_workers`` selects the server's Python handler worker pool size
    for the profile (0 defers to the server default of 1, which keeps the
    smoke profiles fast at every-commit cadence). The dense and distributed
    gates raise the pool so the handler dispatch parallelizes across worker
    threads under free-threaded Python (the C sim calls release the GIL, so
    a multi-worker pool delivers the per-tick input volume the gate drives).

    ``delivery_strategy`` selects the gateway's registered delivery
    strategy for the profile (None keeps the factory default). The
    distributed fidelity gate names the built-in ``tiered`` preset: its
    move-missing bar exists to prove the suppression lever's effect, so a
    run on the default full preset would measure nothing new.

    ``tiered_max_gap_ms`` overrides the tiered strategy's maximum-silence
    backstop for the subprocess-cluster path (None keeps the documented
    preset cadence; 0 pins the documented default explicitly). It is a
    diagnostic knob: raising the backstop period attributes periodic
    behavior to the paranoia refresh rather than to any other cadence.

    ``delivery_workers`` selects the gateway's delivery executor thread
    count for the profile (0 keeps delivery inline on the reactor thread).
    A non-zero count moves the tick's deliver pass onto executor threads,
    so the compose budget covers composition alone; the
    per-pass compose-wait budget stays at its default unless a run
    overrides it at the server command line.
    """

    name: str
    actor_count: int
    topology: str
    duration_s: float
    input_hz: float
    settle_s: float
    description: str
    python_workers: int = 0
    delivery_strategy: str | None = None
    delivery_workers: int = 0
    tiered_max_gap_ms: int | None = None
    evidence_budget_bytes: int = BREADTH_EVIDENCE_BUDGET_BYTES


DENSE_1000: ScalingProfile = ScalingProfile(
    name="dense-1000",
    actor_count=1000,
    topology="embedded",
    duration_s=5.0,
    input_hz=20.0,
    settle_s=1.0,
    description=(
        "1000 dense actors on the embedded topology: every actor in one "
        "cell, every subscriber's view composed by the gateway's bounded "
        "relevance composer. The single-instance ceiling-breaking gate."
    ),
    python_workers=8,
    # The budget must hold the whole window at the profile's cadence: the
    # full-preset dense pileup runs ~0.70 MiB/s per client, so the 5.2 s
    # window-plus-drain needs ~3.6 MiB retained and 12 MiB carries the ~3x
    # headroom the distributed sizing established.
    evidence_budget_bytes=12 * 1024 * 1024,
)

DISTRIBUTED_2000: ScalingProfile = ScalingProfile(
    name="distributed-2000",
    actor_count=2000,
    topology="distributed",
    duration_s=5.0,
    input_hz=20.0,
    settle_s=1.5,
    description=(
        "2000 distributed actors across multiple instances: stable "
        "horizontal scaling, no single instance the fixed publish "
        "bottleneck under normal spread. Runs against the multi-process "
        "subprocess cluster factory (one OS process per instance) so the "
        "harness's client-drive loop is decoupled from the servers' "
        "worker pools; the in-process distributed host factory remains "
        "available for the smoke profile. Selects the built-in tiered "
        "delivery preset: the move-missing bar exists to measure the "
        "suppression lever's effect, so this profile gates on tiered "
        "behavior by construction (run with --delivery-strategy full for "
        "the default-preset baseline)."
    ),
    python_workers=8,
    delivery_strategy="tiered",
)

SMOKE_DENSE: ScalingProfile = ScalingProfile(
    name="smoke-dense",
    actor_count=24,
    topology="embedded",
    duration_s=0.4,
    input_hz=20.0,
    settle_s=0.3,
    description=(
        "Reduced dense profile for the every-commit integration test: "
        "exercises the harness machinery (multi-client login, movement, "
        "sampling, gate-metric computation) end-to-end without the "
        "1000-connection wall-clock cost of the full gate."
    ),
)

SMOKE_DISTRIBUTED: ScalingProfile = ScalingProfile(
    name="smoke-distributed",
    actor_count=24,
    topology="distributed",
    duration_s=0.4,
    input_hz=20.0,
    settle_s=0.3,
    description=(
        "Reduced distributed profile for the every-commit integration "
        "test: exercises the distributed drive path (multi-instance "
        "bootstrap, per-instance actor resolution, publish-rate-skew "
        "computation) without the 2000-connection wall-clock cost of "
        "the full gate."
    ),
)


@dataclass(frozen=True, slots=True)
class GateThresholds:
    """The numeric pass thresholds for one §4.4 gate.

    ``session_ok_min`` and ``selected_clients_min`` are inclusive minimum
    ratios in [0, 1] (an exactly-at-bar value passes); ``move_missing_max``
    is a strict maximum ratio in [0, 1] (an exactly-at-bar value fails), or
    ``None`` when the metric is reported without a pass/fail threshold (the
    dense-1000 stress gate reports delivery fidelity without gating on it);
    ``bootstrap_ms_p95_max`` is a strict maximum latency in milliseconds;
    ``continuity_flicker_forbidden`` is whether any flicker fails the gate
    (``False`` reports the metric without gating, the boolean analogue of
    ``move_missing_max is None``); ``publish_rate_skew_max`` is the maximum
    allowed ratio between the highest and lowest per-instance publish rates
    (``inf`` for the single-instance dense gate where skew is undefined /
    always zero); ``min_distinct_others`` is the minimum distinct non-self
    actor ids a client must observe to count as "selected" (1 = any
    delivery; higher values verify the relevance composer is delivering a
    real view set). ``observer_gates`` declares whether the run's
    measurement health classifies alongside these thresholds: the observer
    verdict needs counter bands calibrated for the full-point movement
    window and a driver-only sampler row, so only the distributed fidelity
    gate opts in.
    """

    session_ok_min: float
    selected_clients_min: float
    move_missing_max: float | None
    bootstrap_ms_p95_max: int
    continuity_flicker_forbidden: bool
    publish_rate_skew_max: float = float("inf")
    min_distinct_others: int = 1
    observer_gates: bool = False
    # Whether the run must have measured something to be judgeable: the
    # gate profiles set this so a run whose depth channel came up empty
    # (no sampled clients, no certifiable series) fails instead of
    # reading its unmeasured channels as zeros. The breadth channel needs
    # no flag — an empty population reads a 0.0 session ratio, below
    # every profile's floor.
    require_measured_channels: bool = False
    # The certified move-missing bar: the counter-delta rule is the
    # gate-binding definition and carries the threshold value (a strict
    # ceiling — an exactly-at-bar ratio fails); the event rule stays as the
    # companion (``move_missing_max``) where a profile still gates it.
    # ``None`` reports the certified ratio without gating it.
    move_missing_certified_max: float | None = None


# The breadth floor shared by the gate profiles: 25% of the 512-subject
# view-set budget. A healthy subscriber's composed view set holds up to
# 511 others, so requiring 128 proves the relevance composer is delivering
# a real view set, not a trickle of one or two neighbors.
MIN_DISTINCT_OTHERS: int = 128

# §4.4 dense stress gate: the embedded single-cell pileup is the
# adversarial case the embedded topology is not the fidelity claim for, so
# the dense gate certifies session integrity + bootstrap + selected-clients
# under load (no crash, no corruption, the harness completes). The
# delivery-fidelity metrics are reported, not thresholded:
# move_missing_max is None and continuity_flicker is allowed, so a
# degradation is visible in the report without failing the gate.
# publish_rate_skew is irrelevant on the single-instance embedded topology
# (skew is always 0.0), so the threshold defaults to infinity.
DENSE_THRESHOLDS: GateThresholds = GateThresholds(
    session_ok_min=0.99,
    selected_clients_min=0.95,
    move_missing_max=None,
    bootstrap_ms_p95_max=3000,
    continuity_flicker_forbidden=False,
    min_distinct_others=MIN_DISTINCT_OTHERS,
    require_measured_channels=True,
)

# §4.4 distributed fidelity gate: the full §4.4 thresholds apply
# per instance — this is the topology the N-squared ceiling claim is made
# for. The distributed gate additionally requires no instance's publish rate
# to exceed 3x the least-publishing instance under normal spread, encoding
# the "no single instance the fixed publish bottleneck" requirement as an
# actual threshold rather than prose. This is also the only profile whose
# runs classify observer health: its counter bands are calibrated for the
# full-point movement window and its documented host keeps the servers out
# of the driver's sampler row.
DISTRIBUTED_THRESHOLDS: GateThresholds = GateThresholds(
    session_ok_min=0.99,
    selected_clients_min=0.95,
    # The certified counter-delta rule carries the <10% bar: the
    # event-count rule floors at the rebuild-to-submit ratio (~32%) under
    # saturation even at zero real loss, so binding it keeps the gate
    # permanently red. It renders as the mm_event companion instead.
    move_missing_max=None,
    move_missing_certified_max=0.10,
    bootstrap_ms_p95_max=3000,
    continuity_flicker_forbidden=True,
    publish_rate_skew_max=3.0,
    min_distinct_others=MIN_DISTINCT_OTHERS,
    observer_gates=True,
    require_measured_channels=True,
)

# Relaxed thresholds for the smoke profile: the machinery is validated, not
# the ceiling. The smoke profile's 24 actors cannot exercise the N²
# ceiling; the thresholds assert the harness produces a well-formed,
# sane report (clients connect, bootstrap, receive replication, movement
# produces updates) without the strict §4.4 bars. min_distinct_others=1
# validates that any replication delivery happened, not the view-set size.
SMOKE_THRESHOLDS: GateThresholds = GateThresholds(
    session_ok_min=0.90,
    selected_clients_min=0.50,
    move_missing_max=0.50,
    bootstrap_ms_p95_max=5000,
    continuity_flicker_forbidden=False,
    min_distinct_others=1,
)

# Relaxed thresholds for the smoke distributed profile: the machinery is
# validated (multi-instance bootstrap, per-instance actor resolution,
# publish-rate-skew computation), not the ceiling. The 24-actor profile
# split across 2 instances cannot exercise the N-squared ceiling; the
# thresholds assert the harness produces a well-formed, sane report
# across instances. publish_rate_skew_max is loosened so a 12-actor
# per-instance sample (where one missed frame skews the ratio sharply)
# does not flake.
SMOKE_DISTRIBUTED_THRESHOLDS: GateThresholds = GateThresholds(
    session_ok_min=0.90,
    selected_clients_min=0.50,
    move_missing_max=0.50,
    bootstrap_ms_p95_max=5000,
    continuity_flicker_forbidden=False,
    publish_rate_skew_max=10.0,
    min_distinct_others=1,
)


class GateBars(Protocol):
    """The fields the binding bars judge, pooled or per instance."""

    @property
    def session_ok_ratio(self) -> float: ...

    @property
    def selected_clients_ratio(self) -> float: ...

    @property
    def move_missing_ratio(self) -> float: ...

    @property
    def move_missing_ratio_certified(self) -> float | None: ...

    @property
    def bootstrap_ms_p95(self) -> int: ...

    @property
    def continuity_flicker(self) -> bool: ...


def meets_bars(m: GateBars, t: GateThresholds) -> bool:
    """Judge one metrics row against the threshold bars.

    Shared by the pooled verdict and the per-instance rows so the two
    cannot drift: the ratio floors are inclusive, the ceilings strict,
    the certified rule fails on an absent series, and the flicker ban
    binds only when armed.
    """
    certified_ok = t.move_missing_certified_max is None or (
        m.move_missing_ratio_certified is not None
        and m.move_missing_ratio_certified < t.move_missing_certified_max
    )
    return (
        m.session_ok_ratio >= t.session_ok_min
        and m.selected_clients_ratio >= t.selected_clients_min
        and (t.move_missing_max is None or m.move_missing_ratio < t.move_missing_max)
        and certified_ok
        and m.bootstrap_ms_p95 < t.bootstrap_ms_p95_max
        and not (t.continuity_flicker_forbidden and m.continuity_flicker)
    )


PROFILE_THRESHOLDS: dict[str, GateThresholds] = {
    DENSE_1000.name: DENSE_THRESHOLDS,
    DISTRIBUTED_2000.name: DISTRIBUTED_THRESHOLDS,
    SMOKE_DENSE.name: SMOKE_THRESHOLDS,
    SMOKE_DISTRIBUTED.name: SMOKE_DISTRIBUTED_THRESHOLDS,
}


def thresholds_for(profile: ScalingProfile) -> GateThresholds:
    """Return the §4.4 thresholds for ``profile``.

    The dense and distributed profiles use the §4.4 bars; the smoke
    profiles use relaxed thresholds that validate the machinery, not the
    ceiling.
    """

    return PROFILE_THRESHOLDS.get(profile.name, DENSE_THRESHOLDS)


ScalingProfile.__module__ = "tools.agent.load_harness"
GateThresholds.__module__ = "tools.agent.load_harness"
GateBars.__module__ = "tools.agent.load_harness"
