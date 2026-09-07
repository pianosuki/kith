"""Scaling-gate load harness for the numeric §4.4 gates.

A dedicated headless-client load harness that drives the 1000- and
2000-actor profiles documented in ``AGENTS.md`` §4.4 and asserts the
numeric gates the framework must pass before claiming the N² ceiling is
broken. This is a separate effort from the closed-loop scenarios in
:mod:`tools.agent.runner` (which drive 2-3 clients): the harness registers
N headless clients, walks them through the login bootstrap, drives a
movement phase at a configurable cadence, and samples the §4.4 metrics
(``session_ok``, ``selected_clients``, ``move_missing_ratio``,
``bootstrap_ms_p95``, ``continuity_flicker``, ``publish_rate_skew``) against
the real replication stream the gateway delivers.

The harness drives an :class:`~tools.agent.orchestrator.Orchestrator`
factory (a callable that boots a fresh orchestrator bound to a server):
the harness uses orchestrator-specific surfaces (the server control
plane and the ``shutdown`` hook) beyond the
:class:`~tools.agent.scenario.ScenarioHost` protocol the scenario runner
needs. The dense-1000 gate runs against the embedded topology via
:func:`~tools.agent.embedded_host.embedded_host_factory`; the
distributed-2000 gate runs against the distributed topology via
:func:`~tools.agent.subprocess_cluster_host.subprocess_cluster_factory`
(one OS process per instance, so the harness's client-drive loop is
decoupled from the servers' worker-pool GILs), and the smoke
distributed profile uses the in-process
:func:`~tools.agent.distributed_host.distributed_host_factory`
(multiple instances plus a coordination bus wired in-process with
loopback plus ``add_member``) for fast machinery validation.

Every-commit vs. weekly: the AGENTS.md §4.1 tier table lists the 1000-plus
connection stress tier as Weekly, not every commit. The integration test in
``tests/integration/test_scaling_gates.py`` drives reduced
:data:`SMOKE_DENSE` and :data:`SMOKE_DISTRIBUTED` profiles (24 actors,
short duration) validating the harness machinery end-to-end; the full
:data:`DENSE_1000` and :data:`DISTRIBUTED_2000` profiles run via the CLI
(``python -m tools.agent.load_harness --profile dense-1000 --embedded``
or ``--profile distributed-2000 --distributed``) in the weekly tier.

The harness reuses the real :class:`~tools.agent.ahc.AgenticHeadlessClient`
over the real wire (no stress-only client): the gate exercises the full
stack (TCP accept, wire dispatch, sim publish, fabric replication, gateway
delivery) through the production code path whose scaling is being gated.
"""

from __future__ import annotations

import argparse
import asyncio
import itertools
import json
import math
import os
import platform
import re
import shlex
import sys
import time
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Protocol

from examples._common.query_state import QUERY_STATE_DEFAULT_PAGE_SIZE
from examples.spatial import handlers as spatial_handlers
from examples.spatial import messages as spatial_messages
from examples.spatial.client import (
    ActorStateView,
    decode_actor_state,
    decode_actor_state_batch,
    is_membership_record,
)
from examples.spatial.handlers import CELL_RADIUS, CELL_SIZE_Q16

from kith import SimInput
from tools.agent.ahc import ClientEvent
from tools.agent.assertions import ServerControl
from tools.agent.gap_ledger import (
    ClientSeries,
    FlickerLedger,
    SeriesFrame,
    build_flicker_ledger,
    client_gaps,
)
from tools.agent.observer_gates import (
    DEFAULT_OBSERVER_LIMITS,
    VERDICT_OK,
    InstanceServerSample,
    ObservationVerdict,
    ObserverLimits,
    evaluate_observation,
)
from tools.agent.orchestrator import ManagedClient, Orchestrator
from tools.agent.proc_sampler import ProcSampler, ProcTrace, SamplerTarget
from tools.agent.server_control import ServerControlError


if TYPE_CHECKING:
    from tools.agent.server_control import ServerControlClient


__all__ = [
    "DENSE_1000",
    "DENSE_THRESHOLDS",
    "DISTRIBUTED_2000",
    "DISTRIBUTED_THRESHOLDS",
    "SMOKE_DENSE",
    "SMOKE_DISTRIBUTED",
    "SMOKE_DISTRIBUTED_THRESHOLDS",
    "SMOKE_THRESHOLDS",
    "ClientFlickerSample",
    "EchoDeltaHistogram",
    "FlickerBreakdown",
    "FlickerLedger",
    "GateMetrics",
    "GateReport",
    "GateThresholds",
    "LoadHarness",
    "LoadHostFactory",
    "PhaseCounters",
    "ScalingProfile",
    "run_profile",
]


LoadHostFactory = Callable[[], Orchestrator]


# Bootstrap FSM state KITH_CLIENT_BOOTSTRAP_READY (include/kith/client/client.h).
_BOOTSTRAP_READY: int = 2

# Movement input magnitude (Q16.16 normalized component). Calibrated to the
# tile2d model's speed so a moving actor stays within a radius-1 cell
# neighborhood long enough for the observer stream to record continuity
# samples (mirrors the movement_quality scenario calibration).
_MOVE_INPUT: int = 200

# Submit rounds interleave a manual yield after this many clients so a
# round that outgrows its time slot cannot starve inbound processing.
_SUBMIT_YIELD_EVERY: int = 512

# How many clients are deeply instrumented (large event history) for the
# metrics that need the full movement-window stream (move_missing,
# continuity_flicker). The remaining clients carry the default small
# history and contribute only to the breadth metrics (session_ok,
# selected_clients). Kept small so the dense-1000 run's memory stays
# bounded; 32 is statistically meaningful for the continuity metric
# (binary: flicker / no flicker) while catching rarer drops a 16-sample
# window misses.
_INSTRUMENTED_SAMPLE_SIZE: int = 32

# Spread-probe cohort size: extra depth-instrumented clients at evenly
# spaced submit positions after the head sample. The head prefix is first
# in every submit round, so a pool whose accept/drop boundary lands past
# it protects the sample while the population absorbs the drops; the
# spread cohort measures the remaining positions. Diagnostic only — the
# gate metrics stay computed over the head sample.
_SPREAD_PROBE_SIZE: int = 32

# Event-history depth for the instrumented sample: large enough to hold the
# full movement-window replication stream (a 5 s run at 20 Hz produces ~100
# input ticks and a multiple of that in delivered frames; 8192 covers it
# with margin for longer runs).
_INSTRUMENTED_HISTORY_SIZE: int = 8192

# Event-history depth for the bulk (non-instrumented) clients. The breadth
# metrics (session_ok, selected_clients) only need a recent window: a
# connected subscriber receives a composed batch every refresh, so a few
# frames suffice to observe the view set. Sizing the bulk deque down from
# the instrumented depth bounds the per-client retained payload memory so
# the 2000-client gate fits in the remote host's RSS (the full-depth deque
# across every client OOMs at ~60 GB; the split drops it to ~12 GB).
_BULK_HISTORY_SIZE: int = 64

# A gap between consecutive own-actor state frames longer than this during
# continuous input submission is a continuity flicker (the actor was
# visible, dropped, then resumed while still in view and still moving).
# Three sim ticks at 20 Hz = 150 ms; one tick of delivery slack brings
# the threshold to 200 ms. A gap at or above this means at least three
# consecutive publish cycles produced no observable self-frame.
_FLICKER_GAP_MS: float = 200.0

# Post-window echo drain: the echo pipeline (sim apply through compose to
# client delivery) lags the movement window's end by up to the tiered
# max-gap backstop (1 s) plus one delivery cadence, so a depth scrape run
# at window end counts that lag as move-missing. The settle holds the
# scrape for this fixed bound, making the metric's operational definition
# — no observable self-update within the movement window plus the drain —
# identical across runs. It is a definition, not a tunable: the CLI
# override exists for the drain A/B diagnostic only and the report renders
# whichever value drove the run.
_POST_WINDOW_ECHO_SETTLE_S: float = 2.0

# Breadth-window drain: the movement-window view accumulator seals at the
# window's end plus this bound, so a frame in flight at window end counts
# toward the client's breadth evidence while the post-window max-gap
# backstop waves (observed landing ~0.9 s after window end, then ~1/s
# while the world is quiescent) stay sealed out. The depth channel keeps
# its own longer echo drain (``_POST_WINDOW_ECHO_SETTLE_S``): depth asks
# "which submitted inputs did the source certify within the window plus
# the full echo lag", breadth asks "what view set arrived within the
# window plus one delivery cadence" — the two channels share the window
# bounds but deliberately not the drain.
_BREADTH_WINDOW_DRAIN_S: float = 0.5

# Breadth-window stall bound: the largest receipt gap between consecutive
# in-window replication frames a healthy client's stream may show. Derived
# from the delivery arithmetic — the tiered max-gap backstop (1 s) plus one
# tier cadence (0.5 s) plus scheduler slack (0.5 s); the certified runs'
# census measured a worst observed gap of 209 ms, ~9x below the bound. A
# client whose in-window frames gap past the bound suffered a mid-window
# stream stall: even if its early frames carried the view bar, the stream
# that died did not deliver a meaningful view set, so the client is not
# selected. The continuity-flicker check (200 ms, own-actor frames) is a
# different question on a different stream with its own bound.
_BREADTH_WINDOW_MAX_GAP_S: float = 2.0

# Per-client byte budget for the breadth-evidence deque: summary clients
# retain every replication frame's raw payload until the deque's total
# bytes exceed this, then evict oldest-first. The deque is both the
# forensic payload record and the breadth probe's counting basis: the
# movement-window view read (:meth:`view_window`) walks the frames whose
# arrival timestamp falls inside the window bounds and extracts ids at
# read time, so the ingest path carries no per-frame window work — even a
# small ingest hook measurably perturbs the driver/server cadence at the
# distributed point. The budget must hold the whole window at the
# certified cadence: the batch replication frames average ~5.7 KB, so
# the 5.2 s window-plus-drain needs ~600 KB at the average cadence, and
# the heaviest per-client window streams on the certified distributed
# drive run past 2 MiB. 8 MiB carries ~3x headroom over that measured
# worst. The window read
# answers any residual eviction loudly: a frame evicted from under the
# window marks the client's evidence incomplete and the census reports
# it instead of trusting a shrunken basis. The O(1) span read renders
# the retention margin, and census-class post-mortem tooling decodes the
# deque after a run to reconstruct what the stream actually carried (the
# per-frame census that root-caused the probe-moment race read exactly
# this deque).
_BREADTH_EVIDENCE_BUDGET_BYTES: int = 8 * 1024 * 1024

# Poll cadence for bootstrap-latency sampling and the movement-phase
# deadline checks.
_POLL_INTERVAL_S: float = 0.02

# Movement-window offset between the two drivers in dual-driver mode when
# --driver-stagger-ms is not given: half the observed inter-cluster period,
# large enough that the two windows cannot be mistaken for coherent.
_DEFAULT_STAGGER_MS: int = 600

# Exception group caught at every /query_state probe. Defined once as a
# tuple so ruff format does not strip the grouping (it removes parentheses
# from `except (A, B):` because Python 3.14 accepts the bare-comma tuple
# form, but mypy 2.3 flags that form as a syntax error; a single name
# avoids the conflict). ServerControlError covers HTTP status and
# transport failures from the control-plane client; the harness degrades
# to an empty roster so the depth metrics fall back to the instrumented
# sample rather than aborting the bootstrap.
_QUERY_STATE_ERRORS = (RuntimeError, OSError, ValueError, ServerControlError)

# Loud guardrail on breadth eligibility: when more than this fraction of
# the classified clients count as isolated, the roster geometry itself is
# untrustworthy (a position/placement defect, or a cell-size/radius
# divergence between the server and the classifier) and exempting that
# many clients from the selected_clients denominator silently lowers
# the effective bar. The corridor movement pattern drives a natural
# leading-edge isolation above this bound at full population, so a trip
# here fails the run's verdict with its measured values rendered while
# collection continues; it never aborts measurement mid-drive.
_ISOLATED_FRACTION_MAX: float = 0.05


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


# ---------------------------------------------------------------------------
# profiles, thresholds, metrics, report
# ---------------------------------------------------------------------------


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
    evidence_budget_bytes: int = _BREADTH_EVIDENCE_BUDGET_BYTES


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
_MIN_DISTINCT_OTHERS: int = 128

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
    min_distinct_others=_MIN_DISTINCT_OTHERS,
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
    min_distinct_others=_MIN_DISTINCT_OTHERS,
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


class _GateBars(Protocol):
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


def _meets_bars(m: _GateBars, t: GateThresholds) -> bool:
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


def _build_type(kith_lib: str | None) -> str | None:
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


def _gil_status() -> str:
    """Report the interpreter's GIL status as a human-readable string."""
    probe = getattr(sys, "_is_gil_enabled", None)
    if not callable(probe):
        return "GIL enabled"
    if probe() is False:
        return "GIL disabled"
    return "GIL enabled"


def _resolve_apply_regime(args: argparse.Namespace) -> str:
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


def _certified_shape(profile: ScalingProfile, args: argparse.Namespace) -> RunShape:
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
    regime = _resolve_apply_regime(args)
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
        if _gil_status() != "GIL disabled":
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


def _collect_run_envelope() -> RunEnvelope:
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
        build_type=_build_type(os.environ.get("KITH_LIB")),
        kernel=platform.release(),
        cpu_model=cpu_model,
        cpu_threads=os.cpu_count(),
        governor=governor,
        interpreter=".".join(str(part) for part in sys.version_info[:3]),
        gil_status=_gil_status(),
    )


@dataclass(frozen=True, slots=True)
class GateReport:
    """A renderable report over one scaling-gate run.

    Pure value object: holds the profile, metrics, thresholds, and run
    metadata and produces strings; performs no I/O. The ``passed`` property
    compares metrics against thresholds and requires an ``ok`` observer
    verdict when one is attached, so classification only adds failure
    modes. ``invocation`` records the command line that drove the run (the
    profile carries the resolved configuration), so the report alone
    reproduces its own measurement.
    """

    profile: ScalingProfile
    metrics: GateMetrics
    thresholds: GateThresholds
    started_at: str
    duration_s: float
    invocation: str | None = None
    observation: ObservationVerdict | None = None
    echo_settle_s: float = _POST_WINDOW_ECHO_SETTLE_S
    envelope: RunEnvelope | None = None
    shape: RunShape | None = None

    @property
    def passed(self) -> bool:
        m = self.metrics
        t = self.thresholds
        return (
            _meets_bars(m, t)
            and m.publish_rate_skew <= t.publish_rate_skew_max
            and m.breadth_violation is None
            and (self.observation is None or self.observation.klass == VERDICT_OK)
            and (not t.require_measured_channels or m.sampled_clients > 0)
            and all(_meets_bars(inst, t) for inst in m.instances)
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "profile": asdict(self.profile),
            "metrics": asdict(self.metrics),
            "thresholds": asdict(self.thresholds),
            "passed": self.passed,
            "started_at": self.started_at,
            "duration_s": round(self.duration_s, 3),
            "invocation": self.invocation,
            "observation": (asdict(self.observation) if self.observation is not None else None),
            "echo_settle_s": self.echo_settle_s,
            "envelope": (asdict(self.envelope) if self.envelope is not None else None),
            "shape": (asdict(self.shape) if self.shape is not None else None),
        }

    def to_json(self, *, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent, default=str)

    def to_markdown(self) -> str:
        m = self.metrics
        t = self.thresholds
        verdict = "PASS" if self.passed else "FAIL"
        lines = [
            f"# Scaling Gate: {self.profile.name} — {verdict}",
            "",
        ]
        if self.shape is not None and self.shape.certified_shape is False:
            lines += [
                "> **NOT A CERTIFIED SHAPE** — deviations from the documented",
                "> certified configuration:",
                *[f"> - {deviation}" for deviation in self.shape.deviations],
                "",
            ]
        lines += [
            f"**Started:** {self.started_at}  ",
            f"**Duration:** {self.duration_s:.3f}s  ",
            f"**Topology:** {self.profile.topology}  ",
            f"**Delivery:** {self.profile.delivery_strategy or 'full'}  ",
            f"**Actors:** {m.actor_count} (sampled: {m.sampled_clients})",
        ]
        if self.invocation:
            lines.append(f"**Invocation:** `{self.invocation}`")
        if self.shape is not None:
            if self.shape.certified_shape is None:
                shape_verdict = "n/a"
            elif self.shape.certified_shape:
                shape_verdict = "yes"
            else:
                shape_verdict = "no"
            lines += [
                f"**Apply regime:** {self.shape.apply_regime}  ",
                f"**Certified shape:** {shape_verdict}  ",
            ]
        lines.append(f"**Post-window echo drain:** {self.echo_settle_s:.1f} s  ")
        if self.envelope is not None:
            env = self.envelope
            threads = f" ({env.cpu_threads} threads)" if env.cpu_threads else ""
            build = env.build_type if env.build_type else "unknown"
            governor = env.governor if env.governor else "n/a"
            lines += [
                f"**Build:** {build}  ",
                f"**Kernel:** {env.kernel}  ",
                f"**CPU:** {env.cpu_model or 'unknown'}{threads}  ",
                f"**Governor:** {governor}  ",
                f"**Interpreter:** {env.interpreter} ({env.gil_status})  ",
            ]
        lines += [
            "",
            "## Metrics vs Thresholds",
            "",
            "| Metric | Value | Threshold | Pass |",
            "|---|---:|---:|:---:|",
            _row(
                "session_ok",
                f"{m.session_ok_ratio:.2%}",
                f">= {t.session_ok_min:.2%}",
                _pass(m.session_ok_ratio >= t.session_ok_min),
            ),
            _row(
                "selected_clients",
                f"{m.selected_clients_ratio:.2%}"
                + (f" of {m.eligible_clients} eligible" if m.eligible_clients is not None else ""),
                f">= {t.selected_clients_min:.2%}",
                _pass(m.selected_clients_ratio >= t.selected_clients_min),
            ),
            _row(
                "move_missing_ratio_certified",
                "n/a"
                if m.move_missing_ratio_certified is None
                else f"{m.move_missing_ratio_certified:.2%}",
                _fmt_move_missing_max(t.move_missing_certified_max),
                "—"
                if t.move_missing_certified_max is None
                else _pass(
                    m.move_missing_ratio_certified is not None
                    and m.move_missing_ratio_certified < t.move_missing_certified_max
                ),
            ),
            _row(
                "mm_event (companion)",
                f"{m.move_missing_ratio:.2%}",
                _fmt_move_missing_max(t.move_missing_max),
                "—"
                if t.move_missing_max is None
                else _pass(m.move_missing_ratio < t.move_missing_max),
            ),
            _row(
                "bootstrap_ms_p95",
                m.bootstrap_ms_p95,
                f"< {t.bootstrap_ms_p95_max}",
                _pass(m.bootstrap_ms_p95 < t.bootstrap_ms_p95_max),
            ),
            _row(
                "continuity_flicker",
                m.continuity_flicker,
                "forbidden" if t.continuity_flicker_forbidden else "allowed",
                "—" if not t.continuity_flicker_forbidden else _pass(not m.continuity_flicker),
            ),
            _row(
                "publish_rate_skew",
                _fmt_skew(m.publish_rate_skew),
                _fmt_skew_max(t.publish_rate_skew_max),
                _pass(m.publish_rate_skew <= t.publish_rate_skew_max),
            ),
        ]
        if m.isolated_clients is not None:
            # Reported-only: isolation is a map-placement property, never a
            # delivery failure; the ratio row above names the eligible
            # population it excludes.
            lines.append(
                _row(
                    "isolated_clients",
                    m.isolated_clients,
                    "reported-only",
                    "—",
                )
            )
        if m.breadth_attribution:
            # Observed-but-isolated clients ride neither side of the ratio;
            # the counts row below splits them by evidence freshness, where
            # fresh evidence of a full view set inside isolated geometry is
            # the divergence signature and breaches the guardrail (verdict,
            # not abort) in the topologies where isolation implies
            # cross-cell geometry. Every isolated client's cause attribution
            # renders whether or not it held evidence in the window; an
            # entry without fresh evidence either holds stale evidence
            # (observed) or none at all (never observed) — the tag claims
            # only the window fact, and the counts row separates the rest.
            if m.observed_but_isolated_fresh_clients or m.isolated_clients:
                without_window = (m.isolated_clients or 0) - m.observed_but_isolated_fresh_clients
                lines.append(
                    _row(
                        "observed_but_isolated",
                        f"{m.observed_but_isolated_fresh_clients} fresh / "
                        f"{without_window} without window evidence",
                        "reported-only",
                        "—",
                    )
                )
            for entry in m.breadth_attribution:
                tag = "fresh evidence in window" if entry.fresh else "no fresh evidence in window"
                lines.append(
                    _row(
                        f"  {entry.name}",
                        f"{entry.klass} ({tag})",
                        "reported-only",
                        "—",
                    )
                )
        if m.unbound_clients:
            lines.append(
                _row(
                    "unbound_clients",
                    m.unbound_clients,
                    "reported-only",
                    "—",
                )
            )
        if m.late_bootstrapped_clients:
            lines.append(
                _row(
                    "late_bootstrapped_clients",
                    m.late_bootstrapped_clients,
                    "reported-only",
                    "—",
                )
            )
        if m.breadth_stalled_clients:
            lines.append(
                _row(
                    "breadth_stalled_clients",
                    m.breadth_stalled_clients,
                    "reported-only",
                    "—",
                )
            )
        if m.breadth_incomplete_clients:
            lines.append(
                _row(
                    "breadth_incomplete_clients",
                    m.breadth_incomplete_clients,
                    "reported-only",
                    "—",
                )
            )
        if m.breadth_never_opened_clients:
            lines.append(
                _row(
                    "breadth_never_opened_clients",
                    m.breadth_never_opened_clients,
                    "reported-only",
                    "—",
                )
            )
        if m.breadth_sparse_clients:
            lines.append(
                _row(
                    "breadth_sparse_clients",
                    m.breadth_sparse_clients,
                    "reported-only",
                    "—",
                )
            )
        for residue in m.breadth_sparse_rows:
            # The window count versus the deque count splits a sparse read
            # into window-too-young versus stream-never-carried; the
            # evidence-deque-span row below bounds what the deque basis
            # can still see.
            lines.append(
                _row(
                    f"  {residue.name} (instance {residue.instance_index})",
                    f"{residue.klass} (window {residue.distinct_in_window} distinct, "
                    f"deque {residue.distinct_deque} distinct)",
                    "reported-only",
                    "—",
                )
            )
        if m.breadth_evidence_span_ns_min is not None:
            # The retained stream's reach versus the window it summarizes:
            # the window read marks any client the deque evicted under the
            # window as incomplete (breadth_incomplete_clients), so the
            # margin renders to show how much headroom the budget carries.
            window_ns = self.profile.duration_s
            min_s = m.breadth_evidence_span_ns_min / 1e9
            p50_s = (m.breadth_evidence_span_ns_p50 or 0) / 1e9
            lines.append(
                _row(
                    "evidence_deque_span",
                    f"min {min_s:.2f} s / p50 {p50_s:.2f} s vs {window_ns:.1f} s window",
                    "diagnostic",
                    "—",
                )
            )
        if m.certified_fallback_clients:
            # Reported-only: a fallback client's publisher never minted, so
            # its coverage rides the event rule inside the certified
            # aggregate's fallback contract.
            lines.append(
                _row(
                    "certified_fallback_clients",
                    m.certified_fallback_clients,
                    "reported-only",
                    "—",
                )
            )
        planned = math.ceil(self.profile.duration_s * self.profile.input_hz)
        per_client = m.inputs_submitted / max(self.profile.actor_count, 1)
        rounds_value = f"{per_client:.1f} / {planned} planned"
        if m.rounds_max > 0:
            rounds_value += f" (min {m.rounds_min} / med {m.rounds_median} / max {m.rounds_max})"
        lines.append(
            _row(
                "submit_rounds_per_client",
                rounds_value,
                "diagnostic",
                "—",
            )
        )
        if m.breadth_violation is not None:
            lines.append(f"**Breadth guardrail:** {m.breadth_violation}")
        if m.instances:
            lines.append(_render_instance_rows(m, t))
        lines.append("")
        if self.observation is not None:
            lines.append(_render_observation(self.observation))
            lines.append("")
        if m.flicker_count > 0 and m.flicker_breakdown is not None:
            lines.append(_render_flicker_breakdown(m.flicker_breakdown, m.sampled_clients))
            lines.append("")
        if m.phase_counters:
            lines.append(_render_phase_counters(m.phase_counters))
            lines.append("")
        if m.counter_samples:
            lines.append(_render_counter_samples(m.counter_samples))
            lines.append("")
        if m.backlog_post_drain_median_ms is not None or m.backlog_pre_drain_median_ms is not None:
            lines.append(_render_backlog(m))
            lines.append("")
        if m.head_cohort is not None or m.spread_cohort is not None:
            lines.append(_render_cohorts(m))
            lines.append("")
        if m.echo_delta is not None:
            lines.append(_render_echo_delta(m.echo_delta, m.move_missing_ratio, m.inputs_submitted))
            lines.append("")
        if m.flicker_ledger is not None:
            lines.append(_render_flicker_ledger(m.flicker_ledger))
            lines.append("")
            lines.append(_render_gap_attribution(m.flicker_ledger))
            lines.append("")
        return "\n".join(lines)


def _render_instance_rows(m: GateMetrics, t: GateThresholds) -> str:
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
            f"| {_pass(_meets_bars(inst, t))} |"
        )
    return "\n".join(lines)


def _render_observation(verdict: ObservationVerdict) -> str:
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


def _fmt_skew(value: float) -> str:
    """Render a publish-rate-skew value, 'n/a' when no ratio exists.

    A legitimate skew is a max/min ratio and floors at 1.0; infinity is
    the undefined-ratio sentinel and 0.0 the metric-not-computed default,
    so both render as 'n/a' rather than as a bogus ratio.
    """
    if value == float("inf") or value <= 0.0:
        return "n/a"
    return f"{value:.2f}x"


def _fmt_skew_max(value: float) -> str:
    """Render the publish-rate-skew threshold, 'n/a' when not gated."""
    if value == float("inf"):
        return "n/a"
    return f"<= {value:.2f}x"


def _fmt_move_missing_max(value: float | None) -> str:
    """Render the move-missing threshold, 'reported' when the metric is not gated."""
    if value is None:
        return "reported"
    return f"< {value:.2%}"


def _pass(ok: bool) -> str:
    """Render the pass column for a gated metric: 'yes' or 'no'."""
    return "yes" if ok else "no"


def _row(name: str, value: object, threshold: str, pass_col: str) -> str:
    return f"| {name} | {value} | {threshold} | {pass_col} |"


def _render_flicker_breakdown(breakdown: FlickerBreakdown, sampled_clients: int) -> str:
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
    counts_spread = _spread(per_client_counts)
    sizes_spread = _spread(all_gap_ms)
    lines = [
        f"**Flicker gaps:** {breakdown.total_gaps} across "
        f"{flickering} of {sampled_clients} sampled clients "
        f"(diagnostic, not gated)  ",
        f"- per-client gaps: {counts_spread}  ",
        f"- gap size (ms): {sizes_spread}  ",
    ]
    return "\n".join(lines)


def _render_phase_counters(counters: Sequence[PhaseCounters]) -> str:
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
            f"| {_dominant_phase(c)} |"
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
            f"| {_dominant_subphase(c)} |"
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


def _render_counter_samples(samples: Sequence[CounterSample]) -> str:
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
        f"({_SAMPLER_INTERVAL_S:.2f} s cadence, cumulative values). "
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


def _render_backlog(metrics: GateMetrics) -> str:
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


def _render_cohorts(metrics: GateMetrics) -> str:
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


def _render_echo_delta(
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


def _dominant_subphase(c: PhaseCounters) -> str:
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


def _dominant_phase(c: PhaseCounters) -> str:
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


def _render_flicker_ledger(ledger: FlickerLedger) -> str:
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


def _render_gap_attribution(ledger: FlickerLedger) -> str:
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


def _spread(values: Sequence[int]) -> str:
    """Render a min/p50/p95/max spread of ``values`` ('n/a' when empty)."""
    if not values:
        return "n/a"
    ordered = sorted(values)
    return (
        f"min {_pct(ordered, 0)} / p50 {_pct(ordered, 50)} "
        f"/ p95 {_pct(ordered, 95)} / max {_pct(ordered, 100)}"
    )


def _pct(ordered: list[int], pct: int) -> int:
    """Nearest-rank percentile of a pre-sorted ascending list as an int."""
    return int(_percentile(ordered, pct))


# ---------------------------------------------------------------------------
# reactor-path phase-counter scrape (movement-window delta of /metrics)
# ---------------------------------------------------------------------------


# The cumulative counters the composition root records per tick. The
# server's metrics handle carries no prefix, so the names are
# the literal Prometheus series names. A label-less counter line is
# "<name> <uint>"; the anchored multiline match is unambiguous and tolerant
# of absent series (parses to 0 rather than raising).
_PHASE_METRIC_NAMES: tuple[tuple[str, str], ...] = (
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

_PHASE_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = tuple(
    (field, re.compile(rf"^{re.escape(name)}\s+(\d+)\s*$", re.MULTILINE))
    for field, name in _PHASE_METRIC_NAMES
)


def _parse_phase_counters(text: str) -> PhaseCounters:
    """Parse the reactor-path counters from a Prometheus text scrape.

    Each counter is a label-less ``<name> <uint>`` line. Absent series (a
    stale build without the counters, or a NULL metrics handle) parse as
    zero rather than raising, so a scrape never fails the gate; an all-zero
    block is the honest 'counters absent' signal.
    """
    values: dict[str, int] = {}
    for field, pattern in _PHASE_PATTERNS:
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


def _phase_counters_delta(
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
class _WindowCounters:
    """One instance's movement-window counter delta plus its presence.

    ``present`` is True only when both boundary scrapes of the window
    succeeded; when False, ``counters`` carries no measurement and
    consumers must treat the reading as absent rather than as a genuine
    all-zero measurement.
    """

    present: bool
    counters: PhaseCounters


_ABSENT_PHASE_COUNTERS = PhaseCounters(
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
)


async def _scrape_phase_counters(orch: Orchestrator) -> list[_WindowCounters]:
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
    out: list[_WindowCounters] = []
    for client in clients:
        try:
            text = await client.metrics()
        except (ServerControlError, OSError) as _exc:
            out.append(_WindowCounters(False, _ABSENT_PHASE_COUNTERS))
            continue
        out.append(_WindowCounters(True, _parse_phase_counters(text)))
    return out


# Cadence of the in-window counter sampler. Diagnostic only: the samples
# give the per-interval drop profile and the pool's in-flight depth over
# the movement window; the gate's own counter deltas stay sourced from the
# window-boundary scrapes alone.
_SAMPLER_INTERVAL_S: float = 0.25


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


async def _run_counter_sampler(
    orch: Orchestrator,
    samples: list[CounterSample],
    window_start_ns: int,
    interval_s: float = _SAMPLER_INTERVAL_S,
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
        scrapes = await _scrape_phase_counters(orch)
        samples.append(
            CounterSample(
                t_rel_ms=(time.monotonic_ns() - window_start_ns) // 1_000_000,
                counters=tuple(w.counters for w in scrapes),
            )
        )


def _anchor_delay_s(movement_anchor_ns: int | None) -> float:
    """Return the seconds the movement phase must wait for ``movement_anchor_ns``.

    Positive when the anchor is ahead of now (the wait that holds a
    fixed cross-process offset); zero once the anchor has passed, so a
    late drive proceeds immediately rather than shifting its window.
    """
    if movement_anchor_ns is None:
        return 0.0
    return max(0.0, (movement_anchor_ns - time.monotonic_ns()) / 1e9)


# ---------------------------------------------------------------------------
# harness
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class _ClientRecord:
    """Per-client bookkeeping for one harness run."""

    name: str
    principal_id: int
    instrumented: bool
    # Spread-probe cohort member: depth-instrumented like the head sample
    # but placed after it (evenly spaced submit positions), feeding only
    # the diagnostic cohort section, never the gate metrics.
    spread: bool = False
    own_actor_id: int = 0
    started_ns: int = 0
    ready_ns: int = 0
    inputs_submitted: int = 0
    instance_index: int = 0
    # Monotonic bounds of the movement phase, stamped by _movement_phase.
    # The continuity-flicker check scopes its gap scan to own-actor frames
    # delivered within this window so a settle/shutdown gap after the last
    # submitted input does not register as a mid-movement flicker.
    movement_start_ns: int = 0
    movement_end_ns: int = 0


@dataclass(frozen=True, slots=True)
class _DriveOutcome:
    """Everything report assembly needs from one completed drive.

    The drives hold the /proc trace and per-client bookkeeping locally so
    the sampler's raw samples never outlive the drive; report assembly
    consumes them here to build the observer verdict alongside the metrics.
    """

    metrics: GateMetrics
    trace: ProcTrace
    records: list[_ClientRecord]
    window_counters: list[_WindowCounters]
    counter_samples: tuple[CounterSample, ...] = ()


_PROFILE_THRESHOLDS: dict[str, GateThresholds] = {
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

    return _PROFILE_THRESHOLDS.get(profile.name, DENSE_THRESHOLDS)


class LoadHarness:
    """Drives a scaling-gate profile against a host factory.

    Constructed with a :class:`ScalingProfile` and a
    :class:`~tools.agent.orchestrator.Orchestrator` factory. :meth:`run` boots a fresh host
    (an :class:`~tools.agent.orchestrator.Orchestrator`), registers
    ``actor_count`` headless clients, walks them through the login
    bootstrap, drives a movement phase at ``input_hz``, samples the §4.4
    metrics, tears the host down, and returns a :class:`GateReport`.

    The orchestrator's ingest task is not started: the harness samples
    clients directly at its own cadence (the orchestrator's 50 ms per-client
    poll is wasteful at N=1000 and feeds a correlator the gate metrics do
    not use). The orchestrator's ``register_client`` / ``start_client`` /
    ``submit`` / ``client_state`` / ``recent_events`` methods all work
    without the ingest task running.

    A :class:`~tools.agent.proc_sampler.ProcSampler` runs on a daemon
    thread for the whole drive (100 ms cadence) over the server
    subprocess pids — when the host factory reports them via the
    ``server_pids`` out-list — plus the harness process itself; the trace
    attaches to the report as the flicker ledger's attribution input.
    The out-list sequence is held live: the host factory appends the
    subprocess pids when the drive invokes it, and the sampler resolves
    its targets from the sequence at start time.
    """

    __slots__ = (
        "_echo_settle_s",
        "_host_factory",
        "_instrumented_sample_size",
        "_invocation",
        "_limits",
        "_movement_anchor_ns",
        "_proc_trace_output",
        "_profile",
        "_server_pids",
        "_shape",
        "_spread_probe_indices",
    )

    def __init__(
        self,
        profile: ScalingProfile,
        host_factory: LoadHostFactory,
        server_pids: Sequence[int] | None = None,
        proc_trace_output: str | None = None,
        invocation: str | None = None,
        instrumented_sample_size: int = _INSTRUMENTED_SAMPLE_SIZE,
        spread_probe_indices: tuple[int, ...] = (),
        movement_anchor_ns: int | None = None,
        limits: ObserverLimits = DEFAULT_OBSERVER_LIMITS,
        echo_settle_s: float | None = None,
        shape: RunShape | None = None,
    ) -> None:
        self._profile = profile
        self._host_factory = host_factory
        self._proc_trace_output = proc_trace_output
        self._invocation = invocation
        self._shape = shape
        # Depth-metric sample size: how many of the first started clients
        # carry the large event history. The default keeps the documented
        # sample; a partitioned drive passes its share so the union of all
        # partitions reproduces the same per-instance composition.
        self._instrumented_sample_size = instrumented_sample_size
        # Spread-probe cohort: the exact registration indices that get the
        # depth history and feed the diagnostic cohort section (never the
        # gate metrics). Empty disables the probe (the default).
        self._spread_probe_indices = spread_probe_indices
        # Absolute CLOCK_MONOTONIC deadline (ns) the movement phase waits
        # for before scraping its start counters. CLOCK_MONOTONIC is
        # system-wide, so two processes handed anchors stagger ms apart
        # hold that offset for the whole window regardless of bootstrap
        # jitter. None drives immediately (the single-drive behavior).
        self._movement_anchor_ns = movement_anchor_ns
        # Ceilings the observer-health verdict judges with (the CLI knobs
        # are its recalibration surface; the frozen defaults stand in when
        # unset).
        self._limits = limits
        # Post-window echo drain (the metric-definition constant; the CLI
        # override is the drain A/B diagnostic surface).
        self._echo_settle_s = _POST_WINDOW_ECHO_SETTLE_S if echo_settle_s is None else echo_settle_s
        # Live reference, deliberately not copied: the host factory
        # appends the subprocess pids during the drive's single factory
        # call — after this constructor, before the sampler starts — so
        # a snapshot taken here stays empty for the whole run.
        self._server_pids = server_pids if server_pids is not None else ()

    async def run(self) -> GateReport:
        """Drive the profile and return the gate report."""
        started_at = datetime.now(UTC).isoformat()
        start = time.perf_counter()
        if self._profile.topology == "distributed":
            outcome = await self._drive_distributed()
        else:
            outcome = await self._drive_dense()
        duration = time.perf_counter() - start
        return GateReport(
            profile=self._profile,
            metrics=outcome.metrics,
            thresholds=thresholds_for(self._profile),
            started_at=started_at,
            duration_s=duration,
            invocation=self._invocation,
            observation=self._observation_for(outcome),
            echo_settle_s=self._echo_settle_s,
            envelope=_collect_run_envelope(),
            shape=self._shape,
        )

    def _observation_for(self, outcome: _DriveOutcome) -> ObservationVerdict | None:
        """Classify the drive's measurement health, or None when out of scope.

        The classification needs a driver-only sampler row — the ``harness``
        row sums every thread of its process, so an in-process host would
        read as its own instrument's load — and the calibrated counter
        bands only the distributed fidelity gate carries. Runs outside that
        scope report no observation rather than a vacuous one. The server
        pid out-list is read after the drive because the host factory fills
        it during the drive's single factory call.
        """
        if not thresholds_for(self._profile).observer_gates or not self._server_pids:
            return None
        stamped = [rec for rec in outcome.records if rec.movement_start_ns != 0]
        if not stamped:
            return None
        inputs: dict[int, int] = {}
        for rec in outcome.records:
            inputs[rec.instance_index] = inputs.get(rec.instance_index, 0) + rec.inputs_submitted
        instances = [
            InstanceServerSample(
                instance_index=index,
                inputs_submitted=inputs[index],
                counters_present=scrape.present,
                dispatches=scrape.counters.dispatches,
                write_deferrals=scrape.counters.write_deferrals,
                view_locate_failures=scrape.counters.view_locate_failures,
                deliver_ns=scrape.counters.deliver_ns,
                compose_ns=scrape.counters.compose_ns,
            )
            for index, scrape in zip(sorted(inputs), outcome.window_counters, strict=False)
        ]
        ledger = outcome.metrics.flicker_ledger
        return evaluate_observation(
            self._limits,
            trace=outcome.trace,
            driver_rows=("harness",),
            window_start_ns=min(rec.movement_start_ns for rec in stamped),
            window_end_ns=max(rec.movement_end_ns for rec in stamped),
            tick_spreads=ledger.tick_spreads if ledger is not None else (),
            instances=instances,
        )

    # -----------------------------------------------------------------------
    # dense-topology drive
    # -----------------------------------------------------------------------

    async def _drive_dense(self) -> _DriveOutcome:
        """Drive the dense profile against one embedded host."""
        orch = self._host_factory()
        sampler = ProcSampler.start(_sampler_targets(self._server_pids))
        stopped = False
        try:
            records = await self._register_and_start(orch)
            await self._await_bootstrap(orch, records)
            await self._resolve_own_actors(orch, records)
            await asyncio.sleep(self._profile.settle_s)
            phase_windows, counter_samples = await self._movement_phase(orch, records)
            backlog_pre = await self._frame_age_sample_ms(orch, records)
            await self._post_window_echo_drain(phase_windows)
            backlog_post = await self._frame_age_sample_ms(orch, records)
            metrics, series = await self._sample_metrics(
                orch, records, [w.counters for w in phase_windows]
            )
            metrics = replace(metrics, counter_samples=tuple(counter_samples))
            metrics = _attach_backlog(metrics, backlog_pre, backlog_post)
            await self._stop_clients(orch, records)
            trace = sampler.stop()
            stopped = True
            if self._proc_trace_output is not None:
                _write_proc_trace(self._proc_trace_output, trace, series)
            metrics = _attach_ledger(metrics, trace, series)
            return _DriveOutcome(metrics, trace, records, phase_windows, tuple(counter_samples))
        finally:
            if not stopped:
                sampler.stop()
            await orch.shutdown()

    async def _register_and_start(self, orch: Orchestrator) -> list[_ClientRecord]:
        """Register and start every client, returning the bookkeeping records.

        All clients are submitted to a single ``asyncio.gather`` gated by a
        semaphore, so each client stamps its own ``started_ns`` at the
        moment it acquires a slot and begins its connect — a true per-client
        connect-initiation timestamp rather than a per-batch stamp. With
        the host out-of-process (the subprocess cluster factory) the
        harness's asyncio loop does not contend with server worker
        threads for one GIL, so the drive completes well inside the
        bootstrap window even at 2000 clients. The first
        :data:`_INSTRUMENTED_SAMPLE_SIZE` clients are deeply instrumented
        (their AHC carries a large event history via the host factory); the
        spread-probe clients (``self._spread_probe_indices``) are
        depth-instrumented the same way but sit at subsequent submit positions;
        the remaining clients carry the default small history.
        """
        n = self._profile.actor_count
        names = [f"load-{i}" for i in range(n)]
        for name in names:
            await orch.register_client(name, host="127.0.0.1", port=0)
        spread_set = set(self._spread_probe_indices)
        records = [
            _ClientRecord(
                name=name,
                principal_id=0,
                instrumented=i < self._instrumented_sample_size,
                spread=i in spread_set,
            )
            for i, name in enumerate(names)
        ]
        # Read each client's retained principal id back instead of assuming
        # the factory numbers principals by registration order; a factory
        # with its own assignment rule stays truthful here, and no
        # attribution downstream inherits an ordering guess.
        for rec in records:
            principal = orch.client_principal(rec.name)
            if principal is None:
                raise RuntimeError(f"registered client {rec.name!r} exposes no principal id")
            rec.principal_id = principal
        sem = asyncio.Semaphore(_start_concurrency(n))

        async def _start_one(rec: _ClientRecord) -> None:
            async with sem:
                rec.started_ns = time.monotonic_ns()
                await orch.start_client(rec.name)

        await asyncio.gather(*(_start_one(r) for r in records))
        return records

    async def _await_bootstrap(self, orch: Orchestrator, records: list[_ClientRecord]) -> None:
        """Poll until every client reaches READY or the settle deadline passes.

        Records per-client bootstrap latency (``ready_ns - started_ns``).
        ``started_ns`` is stamped in :meth:`_register_and_start` at the
        moment each client acquires a start slot and begins its connect,
        so the latency covers connect + handshake + bootstrap, not the
        harness's drive queue. Clients that never reach READY within the
        settle window keep ``ready_ns == 0`` and are excluded from the
        bootstrap latency sample (they are counted as session failures by
        the session_ok metric); including them with the deadline latency
        would make ``bootstrap_ms_p95`` measure the drive, not the
        server's per-client bootstrap throughput.
        """
        deadline = time.monotonic() + max(self._profile.settle_s * 4.0, 2.0)
        pending = list(records)
        while pending and time.monotonic() < deadline:
            still_pending: list[_ClientRecord] = []
            for rec in pending:
                status = await orch.client_state(rec.name)
                if status.bootstrap_state == _BOOTSTRAP_READY:
                    rec.ready_ns = time.monotonic_ns()
                else:
                    still_pending.append(rec)
            pending = still_pending
            if pending:
                await asyncio.sleep(_POLL_INTERVAL_S)

    async def _resolve_own_actors(self, orch: Orchestrator, records: list[_ClientRecord]) -> None:
        """Map each client to its own actor id via the server's binding map.

        The ``/bindings`` control-plane route serves the allocation truth
        (one actor per principal, created under the handler lock), so every
        booted client's principal id looks up directly instead of assuming
        start order equals spawn order. The map must cover exactly the
        booted cohort or the run fails loudly: attribution built on partial
        truth silently mis-measures everything downstream.
        """
        control = orch.server_control
        if control is None:
            return
        bindings = await _principal_bindings(control)
        booted = [r for r in records if r.ready_ns > r.started_ns and r.ready_ns != 0]
        missing = [rec.name for rec in booted if rec.principal_id not in bindings]
        if len(bindings) != len(booted) or missing:
            raise RuntimeError(
                f"binding map covers {len(bindings)} principals for "
                f"{len(booted)} booted clients (unmatched clients: {missing[:5]})"
            )
        for rec in booted:
            rec.own_actor_id = bindings[rec.principal_id]

    async def _movement_phase(
        self, orch: Orchestrator, records: list[_ClientRecord]
    ) -> tuple[list[_WindowCounters], list[CounterSample]]:
        """Submit ``actor_input`` at ``input_hz`` for ``duration_s``.

        Each booted client with a resolved own actor submits a movement
        input on every tick. The round walks the movable clients in order,
        yielding periodically so inbound frame processing keeps breathing;
        the phase ends when the duration elapses.

        The phase stamps every record with the movement window's monotonic
        bounds so the continuity-flicker check scopes its gap scan to
        frames delivered while input was actively submitted; a delivery
        gap after the last input (settle or shutdown drain) is expected
        and is not a mid-movement flicker. The same bounds drive the
        breadth window: every record's client opens its view accumulator
        at window start and seals it at window end plus the breadth
        drain, so the breadth probe reads exactly the stream the window
        carried.

        A concurrent sampler task scrapes every instance's counters on a
        fixed cadence between the boundary scrapes (the loop's pacing is
        untouched), giving the per-interval drop profile and the pool's
        in-flight depth over the window.

        Returns the per-instance movement-window delta of the reactor-path
        ``/metrics`` counters, scraped at the movement-window bounds so the
        refresh/compose/deliver split isolates the phase the flicker-gap
        measurement localized, each carrying whether both boundary scrapes
        succeeded, plus the in-window sampler's raw cumulative snapshots.
        An empty windows list (no movable clients) means no movement
        window was driven and no counters are reported.
        """
        movable = [r for r in records if r.own_actor_id != 0 and r.ready_ns > r.started_ns]
        if not movable:
            return [], []
        anchor_delay_s = _anchor_delay_s(self._movement_anchor_ns)
        if anchor_delay_s > 0:
            await asyncio.sleep(anchor_delay_s)
        start_scrapes = await _scrape_phase_counters(orch)
        window_start = time.monotonic_ns()
        for rec in records:
            orch.managed_client(rec.name).begin_view_window()
        samples: list[CounterSample] = []
        sampler = asyncio.create_task(_run_counter_sampler(orch, samples, window_start))
        interval = 1.0 / self._profile.input_hz
        deadline = time.perf_counter() + self._profile.duration_s
        clients = [(rec, orch.managed_client(rec.name)) for rec in movable]
        tick = 0
        while time.perf_counter() < deadline:
            tick += 1
            for round_index, (rec, client) in enumerate(clients):
                _submit_input(client, rec, tick)
                if round_index % _SUBMIT_YIELD_EVERY == _SUBMIT_YIELD_EVERY - 1:
                    # Yield so inbound frame processing keeps breathing
                    # across a round that outgrows its slot.
                    await asyncio.sleep(0)
            remaining = deadline - time.perf_counter()
            if remaining <= 0:
                break
            await asyncio.sleep(min(interval, remaining))
        window_end = time.monotonic_ns()
        sampler.cancel()
        await asyncio.gather(sampler, return_exceptions=True)
        deadline = window_end + int(_BREADTH_WINDOW_DRAIN_S * 1e9)
        for rec in records:
            orch.managed_client(rec.name).seal_view_window(deadline)
        for rec in records:
            rec.movement_start_ns = window_start
            rec.movement_end_ns = window_end
        end_scrapes = await _scrape_phase_counters(orch)
        deltas = _phase_counters_delta(
            [s.counters for s in start_scrapes], [e.counters for e in end_scrapes]
        )
        windows: list[_WindowCounters] = []
        for start, end, delta in zip(start_scrapes, end_scrapes, deltas, strict=False):
            present = start.present and end.present
            windows.append(_WindowCounters(present, delta if present else _ABSENT_PHASE_COUNTERS))
        return windows, samples

    async def _post_window_echo_drain(self, phase_windows: Sequence[_WindowCounters]) -> None:
        """Hold the depth scrape for the fixed echo drain after the window.

        The echo pipeline lags the last submitted tick by up to the tiered
        max-gap backstop plus one delivery cadence; a scrape at window end
        counts that lag as move-missing. Sleeping for
        :data:`_POST_WINDOW_ECHO_SETTLE_S` lets in-flight echoes land, so
        move-missing measures echo lag beyond the bound plus permanent
        loss instead of scrape timing. The bound is fixed in the module —
        never tuned per run — and skipped when no movement window drove.
        The end-of-window phase-counter scrape stays inside the movement
        phase, so the drain adds no counter noise.
        """
        if not phase_windows:
            return
        await asyncio.sleep(self._echo_settle_s)

    @staticmethod
    async def _frame_age_sample_ms(
        orch: Orchestrator, records: Sequence[_ClientRecord]
    ) -> tuple[int, int] | None:
        """Sample the newest-frame age across clients as (median, p95) ms.

        The age says how far a client's inbound stream lags the server's
        delivery cadence — the observer-side term that biases every
        client-observed fidelity metric. None when no client received a
        frame.
        """
        ages: list[int] = []
        for rec in records:
            age_ns = await orch.last_frame_age_ns(rec.name)
            if age_ns is not None:
                ages.append(age_ns // 1_000_000)
        if not ages:
            return None
        return int(_percentile(ages, 50)), int(_percentile(ages, 95))

    async def _sample_metrics(
        self,
        orch: Orchestrator,
        records: list[_ClientRecord],
        phase_counters: Sequence[PhaseCounters],
    ) -> tuple[GateMetrics, tuple[ClientSeries, ...]]:
        """Compute the §4.4 metrics from the post-movement client state.

        Returns the metrics plus the instrumented sample's own-actor
        series so the caller can build the flicker ledger from the same
        fetch (the event histories are drained at shutdown).
        """
        thresholds = thresholds_for(self._profile)
        breadth = await _collect_breadth(
            orch,
            records,
            thresholds.min_distinct_others,
            fresh_isolated_violates=self._profile.topology != "embedded",
        )
        total = breadth.total
        session_ok = breadth.session_ok_count
        selected = breadth.selected_count
        selected_head = breadth.selected_head_count

        instrumented = [r for r in records if r.instrumented and r.own_actor_id != 0]
        submitted, observed, series, cert_rows = await _collect_depth_metrics(
            orch, instrumented, self._echo_settle_s
        )
        move_missing = _move_missing_ratio(submitted, observed)
        move_missing_certified, cert_fallbacks = _certified_metrics(cert_rows)
        flicker = _flicker_breakdown_from_series(series)
        echo = _echo_delta_histogram(series)
        head_cohort: CohortDepth | None = None
        spread_cohort: CohortDepth | None = None
        if self._spread_probe_indices:
            head_cohort = await _collect_cohort_depth(
                orch, "head", list(instrumented), _per_instance_ranks(records), self._echo_settle_s
            )
            spread_recs = [r for r in records if r.spread and r.own_actor_id != 0]
            if spread_recs:
                spread_cohort = await _collect_cohort_depth(
                    orch,
                    "spread",
                    spread_recs,
                    _per_instance_ranks(records),
                    self._echo_settle_s,
                )
        per_client_inputs = [r.inputs_submitted for r in records]
        rounds_min = min(per_client_inputs) if per_client_inputs else 0
        rounds_median = int(_percentile(per_client_inputs, 50)) if per_client_inputs else 0
        rounds_max = max(per_client_inputs) if per_client_inputs else 0

        latencies_ms = [
            max(0, (r.ready_ns - r.started_ns) // 1_000_000)
            for r in records
            if r.ready_ns > r.started_ns
        ]
        p95 = _percentile(latencies_ms, 95) if latencies_ms else 0

        return (
            GateMetrics(
                actor_count=total,
                sampled_clients=len(instrumented),
                session_ok_ratio=session_ok / total if total else 0.0,
                selected_clients_ratio=_selected_clients_ratio(
                    selected, total, breadth.eligible_clients
                ),
                move_missing_ratio=move_missing,
                bootstrap_ms_p95=int(p95),
                continuity_flicker=flicker.total_gaps > 0,
                flicker_count=flicker.total_gaps,
                flicker_breakdown=flicker,
                phase_counters=tuple(phase_counters),
                eligible_clients=breadth.eligible_clients,
                isolated_clients=breadth.isolated_clients,
                classified_clients=breadth.classified_clients,
                observed_but_isolated_fresh_clients=breadth.observed_but_isolated_fresh_clients,
                unbound_clients=breadth.unbound_clients,
                late_bootstrapped_clients=breadth.late_bootstrapped_clients,
                breadth_attribution=breadth.attribution,
                breadth_stalled_clients=breadth.stalled_clients,
                breadth_incomplete_clients=breadth.incomplete_evidence_clients,
                breadth_sparse_clients=breadth.sparse_clients,
                breadth_never_opened_clients=breadth.never_opened_clients,
                breadth_sparse_rows=breadth.sparse_rows,
                breadth_evidence_span_ns_min=breadth.evidence_span_ns_min,
                breadth_evidence_span_ns_p50=breadth.evidence_span_ns_p50,
                inputs_submitted=sum(r.inputs_submitted for r in records),
                breadth_violation=breadth.violation,
                echo_delta=echo,
                rounds_min=rounds_min,
                rounds_median=rounds_median,
                rounds_max=rounds_max,
                selected_head_count=selected_head,
                head_cohort=head_cohort,
                spread_cohort=spread_cohort,
                move_missing_ratio_certified=move_missing_certified,
                certified_fallback_clients=cert_fallbacks,
            ),
            series,
        )

    async def _stop_clients(self, orch: Orchestrator, records: list[_ClientRecord]) -> None:
        """Stop every client before the orchestrator shuts down."""
        await asyncio.gather(
            *(orch.stop_client(r.name) for r in records),
            return_exceptions=True,
        )

    # -----------------------------------------------------------------------
    # distributed-topology drive
    # -----------------------------------------------------------------------

    async def _drive_distributed(self) -> _DriveOutcome:
        """Drive the distributed profile against a multi-instance host.

        The host factory boots N embedded servers on a shared coordination
        bus; the orchestrator's AHC factory round-robins clients across
        the instances' gateway ports. Each instance allocates actor ids
        independently from 1, so own-actor resolution is per-instance:
        the k-th booted client on an instance maps to the k-th actor in
        that instance's ``/query_state`` roster (matching the dense
        drive's start-order-equals-spawn-order invariant, applied per
        instance rather than globally).

        ``publish_rate_skew`` is the max/min ratio of per-instance
        delivered-frame counts: for each instance, sum the
        ``actor_state`` events observed by every client on that instance
        over the movement window. Under even spread the ratio trends to
        1.0; an instance that is the fixed publish bottleneck drives its
        clients' delivered-frame count down, raising the skew.
        """

        orch = self._host_factory()
        sampler = ProcSampler.start(_sampler_targets(self._server_pids))
        stopped = False
        try:
            records = await self._register_and_start(orch)
            await self._await_bootstrap(orch, records)
            await self._resolve_instance_assignment(orch, records)
            await self._resolve_own_actors_distributed(orch, records)
            await asyncio.sleep(self._profile.settle_s)
            phase_windows, counter_samples = await self._movement_phase(orch, records)
            backlog_pre = await self._frame_age_sample_ms(orch, records)
            await self._post_window_echo_drain(phase_windows)
            backlog_post = await self._frame_age_sample_ms(orch, records)
            metrics, series = await self._sample_distributed_metrics(
                orch, records, [w.counters for w in phase_windows]
            )
            metrics = replace(metrics, counter_samples=tuple(counter_samples))
            metrics = _attach_backlog(metrics, backlog_pre, backlog_post)
            await self._stop_clients(orch, records)
            trace = sampler.stop()
            stopped = True
            if self._proc_trace_output is not None:
                _write_proc_trace(self._proc_trace_output, trace, series)
            metrics = _attach_ledger(metrics, trace, series)
            return _DriveOutcome(metrics, trace, records, phase_windows, tuple(counter_samples))
        finally:
            if not stopped:
                sampler.stop()
            await orch.shutdown()

    def _instance_count(self, records: list[_ClientRecord]) -> int:
        """Return the number of distinct instances the clients landed on."""
        return len({r.instance_index for r in records}) or 1

    async def _resolve_instance_assignment(
        self, orch: Orchestrator, records: list[_ClientRecord]
    ) -> None:
        """Stamp each record with the instance index its principal landed on.

        The distributed host factory's AHC factory assigns principals
        round-robin: principal ``100 + i`` targets instance
        ``(i % instance_count)``. The harness assigns principals in
        start order (``100 + i`` for client ``i``), so the instance
        index is ``(principal_id - 100) % instance_count``. The instance
        count is recovered from the server control plane (a
        :class:`~tools.agent.distributed_host.DistributedControl`); when
        the control is not distributed (a test stub or an external
        factory), every client lands on instance 0.
        """
        from tools.agent.distributed_host import DistributedControl

        control = orch.server_control
        instance_count = control.instance_count if isinstance(control, DistributedControl) else 1
        for rec in records:
            rec.instance_index = (rec.principal_id - 100) % instance_count

    async def _resolve_own_actors_distributed(
        self, orch: Orchestrator, records: list[_ClientRecord]
    ) -> None:
        """Map each booted client to its own actor id, per instance.

        For each instance, read the server's principal→actor binding map
        from ``/bindings`` and look every booted client's principal id up
        directly. The map is the server's allocation truth, so attribution
        does not depend on arrival-order assumptions that race
        concurrent bootstrap logins. The read is a completeness check as
        well as a lookup: the instance must expose exactly one binding per
        booted client, or the run fails loudly instead of measuring against
        silently wrong identities.
        """
        from tools.agent.distributed_host import DistributedControl

        control = orch.server_control
        if not isinstance(control, DistributedControl):
            await _resolve_own_actors_dense(orch, records)
            return
        booted_by_instance: dict[int, list[_ClientRecord]] = {}
        for rec in records:
            if rec.ready_ns > rec.started_ns and rec.ready_ns != 0:
                booted_by_instance.setdefault(rec.instance_index, []).append(rec)
        for instance_index, booted in booted_by_instance.items():
            if instance_index >= len(control.controls):
                continue
            bindings = await _principal_bindings(control.controls[instance_index])
            missing = [rec.name for rec in booted if rec.principal_id not in bindings]
            if len(bindings) != len(booted) or missing:
                raise RuntimeError(
                    f"instance {instance_index}: binding map covers "
                    f"{len(bindings)} principals for {len(booted)} booted "
                    f"clients (unmatched clients: {missing[:5]})"
                )
            for rec in booted:
                rec.own_actor_id = bindings[rec.principal_id]

    async def _sample_distributed_metrics(
        self,
        orch: Orchestrator,
        records: list[_ClientRecord],
        phase_counters: Sequence[PhaseCounters],
    ) -> tuple[GateMetrics, tuple[ClientSeries, ...]]:
        """Compute the §4.4 metrics plus per-instance publish-rate skew.

        The breadth and depth metrics mirror the dense drive; the
        distributed drive additionally computes ``publish_rate_skew``
        from the per-instance delivered-``actor_state`` counts (summed
        across every client on each instance). Returns the metrics plus
        the instrumented sample's own-actor series for the ledger.
        """
        thresholds = thresholds_for(self._profile)
        breadth = await _collect_breadth(
            orch,
            records,
            thresholds.min_distinct_others,
            fresh_isolated_violates=self._profile.topology != "embedded",
        )
        total = breadth.total
        session_ok = breadth.session_ok_count
        selected = breadth.selected_count
        selected_head = breadth.selected_head_count

        instrumented = [r for r in records if r.instrumented and r.own_actor_id != 0]
        submitted, observed, series, cert_rows = await _collect_depth_metrics(
            orch, instrumented, self._echo_settle_s
        )
        move_missing = _move_missing_ratio(submitted, observed)
        move_missing_certified, cert_fallbacks = _certified_metrics(cert_rows)
        flicker = _flicker_breakdown_from_series(series)
        echo = _echo_delta_histogram(series)
        head_cohort: CohortDepth | None = None
        spread_cohort: CohortDepth | None = None
        if self._spread_probe_indices:
            head_cohort = await _collect_cohort_depth(
                orch, "head", list(instrumented), _per_instance_ranks(records), self._echo_settle_s
            )
            spread_recs = [r for r in records if r.spread and r.own_actor_id != 0]
            if spread_recs:
                spread_cohort = await _collect_cohort_depth(
                    orch,
                    "spread",
                    spread_recs,
                    _per_instance_ranks(records),
                    self._echo_settle_s,
                )
        per_client_inputs = [r.inputs_submitted for r in records]
        rounds_min = min(per_client_inputs) if per_client_inputs else 0
        rounds_median = int(_percentile(per_client_inputs, 50)) if per_client_inputs else 0
        rounds_max = max(per_client_inputs) if per_client_inputs else 0

        latencies_ms = [
            max(0, (r.ready_ns - r.started_ns) // 1_000_000)
            for r in records
            if r.ready_ns > r.started_ns
        ]
        p95 = _percentile(latencies_ms, 95) if latencies_ms else 0

        skew = await _publish_rate_skew(orch, records)

        return (
            GateMetrics(
                actor_count=total,
                sampled_clients=len(instrumented),
                session_ok_ratio=session_ok / total if total else 0.0,
                selected_clients_ratio=_selected_clients_ratio(
                    selected, total, breadth.eligible_clients
                ),
                move_missing_ratio=move_missing,
                bootstrap_ms_p95=int(p95),
                continuity_flicker=flicker.total_gaps > 0,
                publish_rate_skew=skew,
                flicker_count=flicker.total_gaps,
                flicker_breakdown=flicker,
                phase_counters=tuple(phase_counters),
                eligible_clients=breadth.eligible_clients,
                isolated_clients=breadth.isolated_clients,
                classified_clients=breadth.classified_clients,
                observed_but_isolated_fresh_clients=breadth.observed_but_isolated_fresh_clients,
                unbound_clients=breadth.unbound_clients,
                late_bootstrapped_clients=breadth.late_bootstrapped_clients,
                breadth_attribution=breadth.attribution,
                breadth_stalled_clients=breadth.stalled_clients,
                breadth_incomplete_clients=breadth.incomplete_evidence_clients,
                breadth_sparse_clients=breadth.sparse_clients,
                breadth_never_opened_clients=breadth.never_opened_clients,
                breadth_sparse_rows=breadth.sparse_rows,
                breadth_evidence_span_ns_min=breadth.evidence_span_ns_min,
                breadth_evidence_span_ns_p50=breadth.evidence_span_ns_p50,
                inputs_submitted=sum(r.inputs_submitted for r in records),
                breadth_violation=breadth.violation,
                echo_delta=echo,
                rounds_min=rounds_min,
                rounds_median=rounds_median,
                rounds_max=rounds_max,
                selected_head_count=selected_head,
                head_cohort=head_cohort,
                spread_cohort=spread_cohort,
                move_missing_ratio_certified=move_missing_certified,
                certified_fallback_clients=cert_fallbacks,
                instances=_instance_gate_metrics(records, breadth, cert_rows, series),
            ),
            series,
        )


# ---------------------------------------------------------------------------
# metric helpers (operate on an orchestrator's clients)
# ---------------------------------------------------------------------------


def _sampler_targets(server_pids: Sequence[int]) -> list[SamplerTarget]:
    """Build the /proc sampler targets: each server instance plus the harness.

    The harness self-target is the instrument-side control: a stall that
    delays both instances' deliveries simultaneously would show here,
    which is the signature the per-instance gap analysis already
    exonerated the shared driver on.
    """
    targets = [SamplerTarget(name=f"instance-{i}", pid=pid) for i, pid in enumerate(server_pids)]
    targets.append(SamplerTarget(name="harness", pid=os.getpid()))
    return targets


def _write_proc_trace(path: str, trace: ProcTrace, series: Sequence[ClientSeries]) -> None:
    """Persist the raw sampler trace and the arrival series as JSON.

    The report's diagnostic tables aggregate the trace into per-window
    summaries; this file keeps every sample and every frame so follow-up
    questions can join thread-level counters against client arrivals
    without rerunning the gate.
    """
    payload = {
        "interval_s": trace.interval_s,
        "clk_tck": trace.clk_tck,
        "samples": [asdict(sample) for sample in trace.samples],
        "series": [asdict(entry) for entry in series],
    }
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle)


def _attach_backlog(
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


def _attach_ledger(
    metrics: GateMetrics, trace: ProcTrace, series: Sequence[ClientSeries]
) -> GateMetrics:
    """Return ``metrics`` with the flicker ledger built from trace + series."""
    return replace(metrics, flicker_ledger=build_flicker_ledger(trace, series))


def _submit_input(client: ManagedClient, rec: _ClientRecord, tick: int) -> None:
    """Submit one ``actor_input`` frame for ``rec``'s own actor.

    The input magnitude is fixed at :data:`_MOVE_INPUT`; the input tick
    advances per submit so the sim's movement integration steps forward.
    The call is synchronous: the drive loop interleaves clients itself
    and yields on its own cadence instead of per-input coroutines.
    """
    inp = SimInput(input_tick=tick, move_x=_MOVE_INPUT, move_y=0)
    payload = spatial_handlers.encode_actor_input(rec.own_actor_id, inp)
    rec.inputs_submitted += 1
    client.submit(spatial_messages.ACTOR_INPUT_TYPE, payload)


def _iter_actor_state_views(event: ClientEvent) -> list[ActorStateView]:
    """Decode a replication event into a list of actor state views.

    Handles both the per-subject ``actor_state`` form (one 64-byte record)
    and the multi-subject ``actor_state_batch`` form (a 4-byte header + N
    records). Returns an empty list when the payload is too short or the
    event is not a replication type.
    """
    if event.type_id == spatial_messages.ACTOR_STATE_BATCH_TYPE:
        try:
            views = decode_actor_state_batch(event.payload)
        except ValueError:
            return []
        # Membership events are not subject states: the fidelity metrics key
        # on positions and echoed input ticks, which sentinels do not carry.
        return [v for v in views if not is_membership_record(v)]
    if event.type_id == spatial_messages.ACTOR_STATE_TYPE:
        try:
            view = decode_actor_state(event.payload)
        except ValueError:
            return []
        return [] if is_membership_record(view) else [view]
    return []


def _distinct_non_self(ids: frozenset[int], own_actor_id: int) -> int:
    """Count a distinct-id set's members excluding the client's own actor id.

    Ids accumulate unfiltered at ingest (a late-bootstrapped client's own
    actor id is unknown until the collection-time binding re-stamp), so
    the self filter applies at read time — to the window set and to the
    deque-wide set alike, keeping the two counts on one footing.
    """
    if own_actor_id == 0:
        return len(ids)
    return len(ids) - (1 if own_actor_id in ids else 0)


def _window_stream_continuous(frame_ts_ns: Sequence[int]) -> bool:
    """Return whether the window's frame arrivals show no stall past the bound."""
    max_gap_ns = int(_BREADTH_WINDOW_MAX_GAP_S * 1e9)
    return all(b - a <= max_gap_ns for a, b in itertools.pairwise(frame_ts_ns))


async def _client_view_facts(
    orch: Orchestrator, rec: _ClientRecord, min_distinct: int
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
    if _distinct_non_self(window.distinct_ids, rec.own_actor_id) < min_distinct:
        return False, False, False
    if not _window_stream_continuous(window.frame_ts_ns):
        return False, True, False
    return True, False, False


async def _client_view_set_healthy(
    orch: Orchestrator, rec: _ClientRecord, min_distinct: int
) -> bool:
    """Return whether ``rec``'s movement-window stream proves the view bar."""
    observed, _stalled, _incomplete = await _client_view_facts(orch, rec, min_distinct)
    return observed


async def _view_evidence_fresh(orch: Orchestrator, rec: _ClientRecord, min_distinct: int) -> bool:
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
    return _distinct_non_self(window.distinct_ids, rec.own_actor_id) >= min_distinct


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


async def _collect_depth_metrics(
    orch: Orchestrator,
    instrumented: list[_ClientRecord],
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
        for event in events:
            for view in _iter_actor_state_views(event):
                if view.actor_id != rec.own_actor_id:
                    continue
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
        cert_missing, cert_fallback = _certified_missing(
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


def _certified_missing(
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


def _certified_metrics(
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


def _spread_probe_indices(
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


def _per_instance_ranks(records: Sequence[_ClientRecord]) -> dict[str, int]:
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


async def _collect_cohort_depth(
    orch: Orchestrator,
    cohort: str,
    records: list[_ClientRecord],
    ranks: dict[str, int],
    drain_s: float,
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
    rows: list[CohortRow] = []
    cert_rows: list[ClientDepthRow] = []
    submitted = 0
    observed = 0
    for rec in records:
        rec_submitted, rec_observed, _, rec_rows = await _collect_depth_metrics(
            orch, [rec], drain_s
        )
        submitted += rec_submitted
        observed += rec_observed
        cert_rows.extend(rec_rows)
        rows.append(
            CohortRow(
                name=rec.name,
                instance_index=rec.instance_index,
                per_instance_rank=ranks.get(rec.name, 0),
                submitted=rec_submitted,
                observed=rec_observed,
                certified_missing=rec_rows[0].certified_missing,
                certified_fallback=rec_rows[0].certified_fallback,
            )
        )
    certified, fallbacks = _certified_metrics(tuple(cert_rows))
    return CohortDepth(
        name=cohort,
        sampled=len(records),
        submitted=submitted,
        observed=observed,
        move_missing=_move_missing_ratio(submitted, observed),
        rows=tuple(rows),
        move_missing_certified=certified,
        certified_fallback_clients=fallbacks,
    )


def _move_missing_ratio(submitted: int, observed: int) -> float:
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


_COVERAGE_TRUNCATED = 0.98


def _echo_delta_histogram(series: Sequence[ClientSeries]) -> EchoDeltaHistogram:
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
            if coverage < _COVERAGE_TRUNCATED:
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


def _selected_clients_ratio(selected: int, total: int, eligible: int | None) -> float:
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


def _instance_gate_metrics(
    records: Sequence[_ClientRecord],
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
        certified, fallbacks = _certified_metrics(depth_rows)
        rows.append(
            InstanceGateMetrics(
                instance_index=index,
                actor_count=census.total,
                sampled_clients=len(depth_rows),
                session_ok_ratio=census.session_ok_count / census.total if census.total else 0.0,
                selected_clients_ratio=_selected_clients_ratio(
                    census.selected_count, census.total, census.eligible_clients
                ),
                eligible_clients=census.eligible_clients,
                isolated_clients=census.isolated_clients,
                classified_clients=census.classified_clients,
                move_missing_ratio=_move_missing_ratio(
                    sum(r.submitted for r in depth_rows),
                    sum(r.observed for r in depth_rows),
                ),
                move_missing_ratio_certified=certified,
                certified_fallback_clients=fallbacks,
                bootstrap_ms_p95=int(_percentile(latencies.get(index, ()), 95)),
                continuity_flicker=(
                    _flicker_breakdown_from_series(series_by_instance.get(index, ())).total_gaps > 0
                ),
            )
        )
    return tuple(rows)


def _flicker_breakdown_from_series(series: Sequence[ClientSeries]) -> FlickerBreakdown:
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
        gaps = client_gaps(entry, _FLICKER_GAP_MS)
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


def _percentile(values: Sequence[int], pct: int) -> float:
    """Return the ``pct``-th percentile of ``values`` (nearest-rank)."""
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = max(0, min(len(ordered) - 1, (pct * len(ordered) + 99) // 100 - 1))
    return float(ordered[idx])


def _start_concurrency(actor_count: int) -> int:
    """In-flight concurrency bound for the semaphore-gated client start.

    Caps concurrent connects so 2000 simultaneous socket setups do not
    exhaust ephemeral ports or file descriptors; large enough that the
    drive completes in a handful of admission rounds. With the host
    out-of-process the harness loop does not contend with server worker
    threads for one GIL, so this is a socket-pressure guard, not a
    GIL-contention throttle.
    """
    return min(256, max(1, actor_count))


async def _resolve_own_actors_dense(orch: Orchestrator, records: list[_ClientRecord]) -> None:
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
    bindings = await _principal_bindings(control)
    booted = [r for r in records if r.ready_ns > r.started_ns and r.ready_ns != 0]
    missing = [rec.name for rec in booted if rec.principal_id not in bindings]
    if len(bindings) != len(booted) or missing:
        raise RuntimeError(
            f"binding map covers {len(bindings)} principals for "
            f"{len(booted)} booted clients (unmatched clients: {missing[:5]})"
        )
    for rec in booted:
        rec.own_actor_id = bindings[rec.principal_id]


async def _principal_bindings(control: ServerControl) -> dict[int, int]:
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
        except _QUERY_STATE_ERRORS as exc:
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


async def _roster_positions(control: ServerControl) -> dict[int, tuple[int, int, int]]:
    """Return the server's actor→position map from paginated ``/query_state``.

    Pages the route exactly like ``/bindings`` and folds every page into
    one mapping. A transport failure or a malformed entry raises instead
    of degrading: an under-counted roster would misclassify clients as
    isolated and silently shrink the breadth denominator.

    Callers with several instances invoke this per instance control —
    each instance allocates actor ids independently from 1, so one flat
    fetch through the aggregating control would collide ids across
    instances.
    """
    positions: dict[int, tuple[int, int, int]] = {}
    offset = 0
    limit = QUERY_STATE_DEFAULT_PAGE_SIZE
    while True:
        try:
            mapping: Mapping[str, object] = await control.get(
                f"/query_state?offset={offset}&limit={limit}"
            )
        except _QUERY_STATE_ERRORS as exc:
            raise RuntimeError(f"roster positions unavailable ({exc})") from exc
        actors = mapping.get("actors")
        if not isinstance(actors, list):
            raise RuntimeError("roster positions unavailable (/query_state response malformed)")
        for item in actors:
            if (
                not isinstance(item, dict)
                or "actor_id" not in item
                or "pos_x" not in item
                or "pos_y" not in item
                or "pos_z" not in item
            ):
                raise RuntimeError("/query_state roster entry malformed")
            positions[int(item["actor_id"])] = (
                int(item["pos_x"]),
                int(item["pos_y"]),
                int(item["pos_z"]),
            )
        # Stop on a short final page, or when the server does not paginate
        # (no ``total`` field — the back-compat full-roster response).
        if len(actors) < limit or "total" not in mapping:
            break
        offset += limit
    return positions


def _isolated_actor_ids(
    positions: Mapping[int, tuple[int, int, int]],
    candidate_ids: Sequence[int],
    min_distinct_others: int,
    *,
    cell_size: int = CELL_SIZE_Q16,
    cell_radius: int = CELL_RADIUS,
) -> frozenset[int]:
    """Return candidates whose radius-1 cell neighborhood is too sparse.

    A candidate's own actor at window end must see at least
    ``min_distinct_others`` OTHER actors within ``cell_radius`` cells of
    its own on every axis (the same geometry the gateway publishes into)
    to have been structurally able to observe a meaningful view set;
    those that cannot are isolated and reported rather than counted
    against delivery. Buckets hold every roster actor per integer cell,
    so counting walks only 27 neighborhood keys regardless of world size.
    """
    if cell_radius < 1:
        raise ValueError("cell_radius must be >= 1")
    buckets: dict[tuple[int, int, int], list[int]] = {}
    for actor_id, (x, y, z) in positions.items():
        buckets.setdefault((x // cell_size, y // cell_size, z // cell_size), []).append(actor_id)
    axis_range = range(-cell_radius, cell_radius + 1)
    isolated: set[int] = set()
    for own_id in candidate_ids:
        own_pos = positions.get(own_id)
        if own_pos is None:
            # The binding map promised this principal owns an actor; a
            # roster missing it is unusable truth.
            raise RuntimeError(f"roster omits own actor {own_id}")
        cx, cy, cz = own_pos[0] // cell_size, own_pos[1] // cell_size, own_pos[2] // cell_size
        neighbors = 0
        for dx in axis_range:
            for dy in axis_range:
                for dz in axis_range:
                    members = buckets.get((cx + dx, cy + dy, cz + dz))
                    if members:
                        neighbors += len(members)
        # Self contributes once when present; subtract defensively rather
        # than trusting the binding invariant twice.
        members = buckets.get((cx, cy, cz))
        if members and own_id in members:
            neighbors -= 1
        if neighbors < min_distinct_others:
            isolated.add(own_id)
    return frozenset(isolated)


@dataclass(slots=True)
class _BreadthRow:
    """One record's collection-time breadth state, built across passes.

    The probe pass fills session and view truth; the classification pass
    fills the binding and geometry flags; the fresh pass runs only for
    isolated rows. The assembly then reads the flags without I/O.
    """

    name: str
    instrumented: bool
    submitted: int
    connected: bool
    instance_index: int = 0
    session_ok: bool = False
    observed: bool = False
    bound: bool = False
    isolated: bool = False
    fresh: bool = False
    late_bootstrapped: bool = False
    # Cleared the bar on distinct ids but gapped past the stall bound
    # mid-window: a delivery break the distinct count alone hides.
    stalled: bool = False
    # The retained-stream basis cannot prove completeness (eviction under
    # the window, or no byte budget): the counts are a floor, so the row
    # reads unobserved and is counted separately from sparse streams.
    incomplete: bool = False
    # The counts behind the observed reading, kept for the residue table:
    # the deque set ignores the window bounds, so a sparse window read
    # splits into window-too-young versus stream-never-carried. Both
    # counts exclude the client's own actor id (read-time self filter).
    window_present: bool = False
    distinct_in_window: int = 0
    distinct_deque: int = 0


def _isolation_cause(row: _BreadthRow) -> str:
    """Name why an isolated row's actor sits outside the pile geometry.

    ``late_bootstrapped`` implies never-movable (the movement phase skips
    records without a stamped boot), so its actor never left the spawn
    cell; the remaining classes split on session and submit state.
    """
    if row.late_bootstrapped:
        return "late_bootstrap"
    if not row.connected:
        return "disconnected"
    if row.submitted == 0:
        return "stalled"
    return "frozen"


def _residue_rows(rows: Iterable[_BreadthRow]) -> tuple[SparseBreadthRow, ...]:
    """Name and count the rows the breadth classes leave unexplained.

    A row lands here when it is unobserved with neither a stall nor an
    incomplete basis behind the reading; the window presence splits it
    into ``sparse`` versus ``never_opened``. The caller narrows to the
    cohort the ratio reads (bound, non-isolated rows where a roster
    classifies, the full population otherwise).
    """
    return tuple(
        SparseBreadthRow(
            name=row.name,
            instance_index=row.instance_index,
            klass="never_opened" if not row.window_present else "sparse",
            distinct_in_window=row.distinct_in_window,
            distinct_deque=row.distinct_deque,
        )
        for row in rows
        if not row.observed and not row.stalled and not row.incomplete
    )


def _assemble_breadth(
    total: int,
    rows: Sequence[_BreadthRow],
    *,
    classified: bool,
    fresh_isolated_violates: bool = True,
    evidence_spans: Sequence[int] = (),
    instance_index: int | None = None,
) -> BreadthCensus:
    """Reduce the fetched breadth rows to the census (pure; no I/O).

    Containment is structural: a row counts as selected only when it is
    bound, non-isolated, and observed, so the numerator is a subset of the
    denominator cohort. Rows observed while isolated are attributed and
    rendered instead of counted; fresh evidence among them is the
    divergence the selected>eligible guardrail exists to catch, and any
    occurrence breaches the guardrail alongside the existing
    isolated-fraction bound where ``fresh_isolated_violates`` scopes the
    guard in (see :func:`_collect_breadth` for the topology scoping).
    """
    session_ok_count = sum(1 for row in rows if row.session_ok)
    if not classified:
        residue = _residue_rows(rows)
        return BreadthCensus(
            total=total,
            session_ok_count=session_ok_count,
            selected_count=sum(1 for row in rows if row.observed),
            selected_head_count=sum(1 for row in rows if row.observed and row.instrumented),
            stalled_clients=sum(1 for row in rows if row.stalled),
            incomplete_evidence_clients=sum(1 for row in rows if row.incomplete),
            sparse_clients=sum(1 for entry in residue if entry.klass == "sparse"),
            never_opened_clients=sum(1 for entry in residue if entry.klass == "never_opened"),
            sparse_rows=residue,
            instance_index=instance_index,
        )
    bound_rows = [row for row in rows if row.bound]
    isolated_rows = [row for row in bound_rows if row.isolated]
    fresh_rows = [row for row in isolated_rows if row.fresh]
    selected = 0
    selected_head = 0
    for row in bound_rows:
        if row.isolated or not row.observed:
            continue
        selected += 1
        if row.instrumented:
            selected_head += 1
    classified_count = len(bound_rows)
    isolated_count = len(isolated_rows)
    violations: list[str] = []
    if classified_count and isolated_count > _ISOLATED_FRACTION_MAX * classified_count:
        violations.append(
            f"{isolated_count} of {classified_count} classified clients lack their "
            f"neighborhood geometry; the window-end roster cannot support "
            f"breadth eligibility"
        )
    if fresh_rows and fresh_isolated_violates:
        names = ", ".join(row.name for row in fresh_rows)
        violations.append(
            f"{len(fresh_rows)} of {classified_count} classified clients hold fresh "
            f"evidence of a full view set while isolated ({names}); delivery "
            f"reached beyond the eligibility geometry"
        )
    residue = _residue_rows(row for row in bound_rows if not row.isolated)
    return BreadthCensus(
        total=total,
        session_ok_count=session_ok_count,
        classified_clients=classified_count,
        isolated_clients=isolated_count,
        eligible_clients=classified_count - isolated_count,
        selected_count=selected,
        selected_head_count=selected_head,
        observed_but_isolated_fresh_clients=len(fresh_rows),
        unbound_clients=total - classified_count,
        late_bootstrapped_clients=sum(1 for row in bound_rows if row.late_bootstrapped),
        stalled_clients=sum(1 for row in rows if row.stalled),
        incomplete_evidence_clients=sum(1 for row in rows if row.incomplete),
        sparse_clients=sum(1 for entry in residue if entry.klass == "sparse"),
        never_opened_clients=sum(1 for entry in residue if entry.klass == "never_opened"),
        sparse_rows=residue,
        evidence_span_ns_min=evidence_spans[0] if evidence_spans else None,
        evidence_span_ns_p50=(evidence_spans[len(evidence_spans) // 2] if evidence_spans else None),
        violation="; ".join(violations) or None,
        attribution=tuple(
            BreadthAttribution(
                name=row.name,
                klass=_isolation_cause(row),
                fresh=row.fresh,
            )
            for row in isolated_rows
        ),
        instance_index=instance_index,
    )


async def _probe_breadth_rows(
    orch: Orchestrator,
    records: Sequence[_ClientRecord],
    rows: list[_BreadthRow],
    min_distinct: int,
) -> None:
    """Fill session and view truth into the census rows.

    Session truth comes from the orchestrator's client registry (a client
    counts toward ``session_ok`` when its connection survived with a READY
    bootstrap); the observed reading comes from the client's sealed
    movement-window evidence, with the stall guard folded in. Runs after
    the binding re-stamp so each row's self-filter sees the bound actor id.
    """
    for rec, row in zip(records, rows, strict=True):
        status = await orch.client_state(rec.name)
        row.connected = status.connected
        row.session_ok = status.connected and status.bootstrap_state == _BOOTSTRAP_READY
        row.observed, row.stalled, row.incomplete = await _client_view_facts(
            orch, rec, min_distinct
        )
        window = await orch.view_window(rec.name)
        if window is not None:
            row.window_present = True
            row.distinct_in_window = _distinct_non_self(window.distinct_ids, rec.own_actor_id)
            row.distinct_deque = _distinct_non_self(window.deque_distinct_ids, rec.own_actor_id)


async def _collect_breadth(
    orch: Orchestrator,
    records: list[_ClientRecord],
    min_distinct: int,
    *,
    fresh_isolated_violates: bool = True,
) -> BreadthCensus:
    """Classify the collection-time bound cohort and contain the ratio.

    The binding map is re-read at collection time (the server's append-
    only allocation truth), so a client whose bootstrap completed after
    the pre-movement snapshot joins the cohort instead of vanishing from
    the denominator while its retained frames still count it as observed —
    the membership race that aborted healthy runs with ``selected`` above
    ``eligible``. Each bound client's own actor classifies against its
    instance's window-end roster exactly as the isolation classifier
    always has; the selected numerator ranges only over classified
    non-isolated clients, so containment holds by construction. Returns a
    census with the denominator counts ``None`` when no control plane
    exposes a roster: the numerator then keeps its legacy full-population
    reading.

    ``fresh_isolated_violates`` scopes the divergence guardrail to the
    topologies where its signal is meaningful: an isolated client whose
    window stream carried the full view set is divergence only where
    isolation implies cross-cell geometry. On the embedded single-cell
    topology an "isolated" client is pile-edge sparsity that legitimately
    receives the whole cell's publish (cell-scoped delivery),
    so the fresh flag renders there without breaching the guardrail; a
    multi-cell embedded profile would need the cell-aware form.
    """
    control = orch.server_control
    rows = [
        _BreadthRow(
            name=rec.name,
            instrumented=rec.instrumented,
            submitted=rec.inputs_submitted,
            connected=False,
            instance_index=rec.instance_index,
        )
        for rec in records
    ]
    rows_by_name = {row.name: row for row in rows}
    if control is None:
        await _probe_breadth_rows(orch, records, rows, min_distinct)
        return _assemble_with_instances(records, rows, classified=False)
    from tools.agent.distributed_host import DistributedControl

    groups: list[tuple[ServerControl, list[_ClientRecord]]] = []
    if isinstance(control, DistributedControl):
        per_instance: dict[int, list[_ClientRecord]] = {}
        for rec in records:
            if rec.instance_index < len(control.controls):
                per_instance.setdefault(rec.instance_index, []).append(rec)
        groups = [(control.controls[i], recs) for i, recs in sorted(per_instance.items())]
    else:
        groups = [(control, records)]
    for group_control, recs in groups:
        bindings = await _principal_bindings(group_control)
        for rec in recs:
            if rec.principal_id not in bindings:
                continue
            row = rows_by_name[rec.name]
            row.bound = True
            if rec.own_actor_id == 0:
                row.late_bootstrapped = True
                rec.own_actor_id = bindings[rec.principal_id]
        positions = await _roster_positions(group_control)
        iso_ids = _isolated_actor_ids(
            positions,
            [rec.own_actor_id for rec in recs if rec.principal_id in bindings],
            min_distinct,
        )
        for rec in recs:
            if rec.principal_id in bindings:
                rows_by_name[rec.name].isolated = rec.own_actor_id in iso_ids
    await _probe_breadth_rows(orch, records, rows, min_distinct)
    for rec, row in zip(records, rows, strict=True):
        if row.isolated:
            row.fresh = await _view_evidence_fresh(orch, rec, min_distinct)
    spans: dict[str, int] = {}
    for rec in records:
        span = await orch.evidence_span_ns(rec.name)
        if span is not None:
            spans[rec.name] = span
    return _assemble_with_instances(
        records,
        rows,
        classified=True,
        fresh_isolated_violates=fresh_isolated_violates,
        spans=spans,
    )


def _assemble_with_instances(
    records: Sequence[_ClientRecord],
    rows: Sequence[_BreadthRow],
    *,
    classified: bool,
    fresh_isolated_violates: bool = True,
    spans: Mapping[str, int] | None = None,
) -> BreadthCensus:
    """Assemble the pooled census plus one census per instance cohort.

    The rows partition by each record's instance index and every partition
    reduces through the same :func:`_assemble_breadth` the pooled census
    uses, so a per-instance ratio reads exactly the pooled formula's slice
    of the population.
    """
    row_by_name = {row.name: row for row in rows}
    span_map = spans or {}
    census = _assemble_breadth(
        len(records),
        rows,
        classified=classified,
        fresh_isolated_violates=fresh_isolated_violates,
        evidence_spans=sorted(span_map.values()),
    )
    rows_by_instance: dict[int, list[_BreadthRow]] = {}
    for rec in records:
        rows_by_instance.setdefault(rec.instance_index, []).append(row_by_name[rec.name])
    instances = tuple(
        _assemble_breadth(
            len(part_rows),
            part_rows,
            classified=classified,
            fresh_isolated_violates=fresh_isolated_violates,
            evidence_spans=sorted(span_map[row.name] for row in part_rows if row.name in span_map),
            instance_index=index,
        )
        for index, part_rows in sorted(rows_by_instance.items())
    )
    return replace(census, instances=instances)


async def _publish_rate_counts(orch: Orchestrator, records: list[_ClientRecord]) -> dict[int, int]:
    """Sum the replication frames each instance delivered to its booted clients.

    For each instance, sums the dispatched replication-frame counts of
    every booted client on that instance. The per-client counter covers
    every frame the connection dispatched, so it stays exact where a
    history scan saturates at the client's retained-history bound. The
    integer per-instance counts are the poolable form of the publish-rate
    metric: aggregating several drives means summing these per instance,
    never averaging ratios.
    """
    per_instance: dict[int, int] = {}
    for rec in records:
        if rec.own_actor_id == 0 or rec.ready_ns <= rec.started_ns:
            continue
        count = await orch.replication_frame_count(rec.name)
        per_instance[rec.instance_index] = per_instance.get(rec.instance_index, 0) + count
    return per_instance


async def _publish_rate_skew(orch: Orchestrator, records: list[_ClientRecord]) -> float:
    """Return the max/min ratio of per-instance delivered-frame counts.

    Under even spread the ratio trends to 1.0; an instance that is the
    fixed publish bottleneck drives its clients' delivered-frame count
    down, raising the skew. Returns 0.0 when no booted client contributes
    (nothing to compare); booted clients that observed no frames at all
    drive the ratio to infinity (see :func:`_publish_rate_counts` for the
    underlying sums).
    """
    per_instance = await _publish_rate_counts(orch, records)
    if not per_instance:
        return 0.0
    low = min(per_instance.values())
    if low == 0:
        return float("inf")
    return max(per_instance.values()) / low


# ---------------------------------------------------------------------------
# async entry
# ---------------------------------------------------------------------------


async def run_profile(
    profile: ScalingProfile,
    host_factory: LoadHostFactory,
    server_pids: Sequence[int] | None = None,
    proc_trace_output: str | None = None,
    invocation: str | None = None,
    limits: ObserverLimits = DEFAULT_OBSERVER_LIMITS,
    echo_settle_s: float | None = None,
    spread_probe_indices: tuple[int, ...] = (),
    shape: RunShape | None = None,
) -> GateReport:
    """Drive ``profile`` against ``host_factory`` and return the gate report.

    ``server_pids`` is the optional out-list a subprocess host factory
    fills at boot; when given, the /proc sampler covers each server
    process in addition to the harness itself and the report carries the
    flicker ledger built from that trace. ``proc_trace_output`` names a
    JSON file that receives the raw sampler trace plus the arrival
    series the ledger was built from. ``invocation`` records the driving
    command line in the report so the report reproduces its own
    measurement; the profile it carries is already the resolved
    configuration. ``limits`` carries the observer-health ceilings the
    report's observation is judged with. ``echo_settle_s`` overrides the
    frozen post-window echo drain (the drain A/B diagnostic surface).
    ``spread_probe_indices`` names the spread-probe cohort: registration
    indices that get the depth history and feed the diagnostic cohort
    section while the gate metrics stay computed over the head sample.
    ``shape`` carries the resolved apply regime and certified-shape
    verdict the report stamps.
    """
    harness = LoadHarness(
        profile,
        host_factory,
        server_pids=server_pids,
        proc_trace_output=proc_trace_output,
        invocation=invocation,
        limits=limits,
        echo_settle_s=echo_settle_s,
        spread_probe_indices=spread_probe_indices,
        shape=shape,
    )
    return await harness.run()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


_PROFILES: dict[str, ScalingProfile] = {
    DENSE_1000.name: DENSE_1000,
    DISTRIBUTED_2000.name: DISTRIBUTED_2000,
    SMOKE_DENSE.name: SMOKE_DENSE,
    SMOKE_DISTRIBUTED.name: SMOKE_DISTRIBUTED,
}
_FORMATS = ("md", "json")


def _build_parser() -> argparse.ArgumentParser:
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
        choices=sorted(_PROFILES),
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
        default=_SPREAD_PROBE_SIZE,
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
        choices=_FORMATS,
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


def _embedded_factory(
    *,
    python_workers: int = 0,
    delivery_strategy: str | None = None,
    delivery_workers: int = 0,
    native_apply: bool = False,
    evidence_budget_bytes: int = 0,
    extra_instrumented_indices: Sequence[int] = (),
) -> LoadHostFactory:
    """Return a factory that boots a fresh embedded server per run.

    Deferred import so importing this module does not load the C shared
    libraries or the example modules. The first
    :data:`_INSTRUMENTED_SAMPLE_SIZE` clients carry the large event history
    the depth metrics need (``move_missing`` / ``continuity_flicker``
    sample the full movement-window stream); the remaining bulk clients
    carry :data:`_BULK_HISTORY_SIZE` so the per-client retained payload
    memory stays bounded.
    Multi-subject batch delivery is enabled (the gateway packs the full
    view set into one frame per refresh) so the reactor's per-refresh
    frame volume scales O(N), not O(N x K). ``python_workers`` forwards to
    the embedded server's worker pool size so a scaling profile raises the
    pool above the default of 1.
    """
    from examples.spatial import messages as spatial_messages

    from tools.agent.embedded_host import embedded_host_factory

    return embedded_host_factory(
        ahc_event_history_size=_INSTRUMENTED_HISTORY_SIZE,
        replication_batch_type_id=spatial_messages.ACTOR_STATE_BATCH_TYPE,
        python_workers=python_workers,
        delivery_strategy=delivery_strategy,
        delivery_workers=delivery_workers,
        native_apply=native_apply,
        instrumented_count=_INSTRUMENTED_SAMPLE_SIZE,
        bulk_event_history_size=_BULK_HISTORY_SIZE,
        evidence_budget_bytes=evidence_budget_bytes,
        extra_instrumented_indices=extra_instrumented_indices,
    )


def _distributed_factory(
    *,
    python_workers: int = 0,
    delivery_strategy: str | None = None,
    delivery_workers: int = 0,
    evidence_budget_bytes: int = 0,
    extra_instrumented_indices: Sequence[int] = (),
) -> LoadHostFactory:
    """Return a factory that boots a fresh multi-instance cluster per run.

    Deferred import so importing this module does not load the C shared
    libraries or the example modules. The first
    :data:`_INSTRUMENTED_SAMPLE_SIZE` clients carry the large event history
    the depth metrics need; the remaining bulk clients carry
    :data:`_BULK_HISTORY_SIZE` so the per-client retained payload memory
    stays bounded. The cluster boots two embedded servers on a shared
    coordination bus (loopback transport plus ``add_member``), matching the
    distributed example and the cross-instance split-merge integration test.
    Multi-subject batch delivery is enabled (same as the embedded factory)
    so the reactor's per-refresh frame volume scales O(N), not O(N x K).
    ``python_workers`` forwards to each instance's worker pool size so a
    scaling profile raises the pool above the default of 1.
    """
    from examples.spatial import messages as spatial_messages

    from tools.agent.distributed_host import distributed_host_factory

    return distributed_host_factory(
        ahc_event_history_size=_INSTRUMENTED_HISTORY_SIZE,
        instance_count=2,
        replication_batch_type_id=spatial_messages.ACTOR_STATE_BATCH_TYPE,
        python_workers=python_workers,
        delivery_strategy=delivery_strategy,
        delivery_workers=delivery_workers,
        instrumented_count=_INSTRUMENTED_SAMPLE_SIZE,
        bulk_event_history_size=_BULK_HISTORY_SIZE,
        evidence_budget_bytes=evidence_budget_bytes,
        extra_instrumented_indices=extra_instrumented_indices,
    )


def _external_factory(
    host: str, port: int, extra_instrumented_indices: Sequence[int] = ()
) -> LoadHostFactory:
    """Return a factory targeting an external server at ``host:port``."""

    def factory() -> Orchestrator:
        from examples.spatial.client import make_ahc, replication_direct_types

        from tools.agent.ahc import AgenticHeadlessClient
        from tools.agent.embedded_host import _history_size_for, _summary_retention_for
        from tools.agent.orchestrator import Orchestrator as _Orch

        principal_counter = [100]
        extra = frozenset(extra_instrumented_indices)

        def ahc_factory(
            instance_id: str, connect_host: str, connect_port: int
        ) -> AgenticHeadlessClient:
            del connect_host, connect_port
            principal_id = principal_counter[0]
            principal_counter[0] += 1
            client_index = principal_id - 100
            history_size = _history_size_for(
                client_index,
                _INSTRUMENTED_SAMPLE_SIZE,
                _INSTRUMENTED_HISTORY_SIZE,
                _BULK_HISTORY_SIZE,
                extra_instrumented=extra,
            )
            return make_ahc(
                instance_id=instance_id,
                host=host,
                port=port,
                principal_id=principal_id,
                event_history_size=history_size,
                http_enabled=False,
                ipc_enabled=False,
                reconnect_enabled=False,
                tick_interval_s=0.1,
                direct_type_ids=replication_direct_types(),
                summary_mode=_summary_retention_for(
                    client_index,
                    _INSTRUMENTED_SAMPLE_SIZE,
                    _BULK_HISTORY_SIZE,
                    extra_instrumented=extra,
                ),
            )

        return _Orch(ahc_factory=ahc_factory, server_control=None)

    return factory


def _subprocess_cluster_factory(
    *,
    python_workers: int = 0,
    delivery_strategy: str | None = None,
    delivery_workers: int = 0,
    tiered_max_gap_ms: int | None = None,
    instance_count: int = 2,
    server_affinity: str | None = None,
    server_pids_out: list[int] | None = None,
    evidence_budget_bytes: int = 0,
    extra_instrumented_indices: Sequence[int] = (),
) -> LoadHostFactory:
    """Return a factory that boots a fresh N-subprocess cluster per run.

    Each call to the factory spawns ``instance_count`` embedded-server
    subprocesses (one OS process per instance) and returns an
    :class:`Orchestrator` whose AHC factory round-robins clients across
    the captured gateway ports. Moving the servers out of the harness
    process decouples their worker-pool GILs from the harness's asyncio
    client-drive loop, removing the structural ceiling the in-process
    distributed host hits at 2000 clients, where drive latency dominates
    the bootstrap window. Deferred import so importing this
    module does not load the C shared libraries or the example modules.

    ``python_workers`` forwards to each instance's worker pool size so a
    scaling profile raises the pool above the default of 1.
    ``tiered_max_gap_ms`` forwards to each instance's tiered delivery
    backstop when the strategy is ``tiered`` (None keeps the documented
    preset cadence). The distributed-2000 gate passes ``instance_count=2``;
    a smoke profile may pass a smaller count. The first
    :data:`_INSTRUMENTED_SAMPLE_SIZE` clients carry the large event
    history the depth metrics need; the remaining bulk clients carry
    :data:`_BULK_HISTORY_SIZE` so the per-client retained payload memory
    stays bounded at 2000 clients.
    """
    from examples.spatial import messages as spatial_messages

    from tools.agent.subprocess_cluster_host import subprocess_cluster_factory

    return subprocess_cluster_factory(
        ahc_event_history_size=_INSTRUMENTED_HISTORY_SIZE,
        instance_count=instance_count,
        replication_batch_type_id=spatial_messages.ACTOR_STATE_BATCH_TYPE,
        python_workers=python_workers,
        delivery_strategy=delivery_strategy,
        delivery_workers=delivery_workers,
        tiered_max_gap_ms=tiered_max_gap_ms,
        instrumented_count=_INSTRUMENTED_SAMPLE_SIZE,
        bulk_event_history_size=_BULK_HISTORY_SIZE,
        server_pids_out=server_pids_out,
        server_affinity=server_affinity,
        evidence_budget_bytes=evidence_budget_bytes,
        extra_instrumented_indices=extra_instrumented_indices,
    )


def _apply_client_override(profile: ScalingProfile, clients: int | None) -> ScalingProfile:
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


def _apply_worker_override(profile: ScalingProfile, workers: int | None) -> ScalingProfile:
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


def _apply_delivery_override(profile: ScalingProfile, strategy: str | None) -> ScalingProfile:
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


def _apply_tiered_max_gap_override(
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


def _apply_delivery_workers_override(
    profile: ScalingProfile, workers: int | None
) -> ScalingProfile:
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


_OBSERVER_RECALIBRATION_ENV = "KITH_OBSERVER_RECALIBRATION"


def _observer_limits_from_args(args: argparse.Namespace) -> ObserverLimits:
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
    if overrides and os.environ.get(_OBSERVER_RECALIBRATION_ENV) != "1":
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


def _env_python_workers(environ: Mapping[str, str]) -> int | None:
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


def _validate_driver_flags(
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
    return _DEFAULT_STAGGER_MS if args.driver_stagger_ms is None else args.driver_stagger_ms


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    profile = _PROFILES[args.profile]
    try:
        profile = _apply_client_override(profile, args.clients)
        workers = args.python_workers
        if workers is None:
            workers = _env_python_workers(os.environ)
        profile = _apply_worker_override(profile, workers)
        profile = _apply_delivery_override(profile, args.delivery_strategy)
        profile = _apply_delivery_workers_override(profile, args.delivery_workers)
        profile = _apply_tiered_max_gap_override(profile, args.tiered_max_gap_ms)
        limits = _observer_limits_from_args(args)
    except ValueError as exc:
        parser.error(str(exc))
    if (
        args.post_window_echo_settle_s is not None
        and os.environ.get(_OBSERVER_RECALIBRATION_ENV) != "1"
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
    stagger_ms = _validate_driver_flags(parser, args, profile)
    if args.driver_worker is not None:
        from tools.agent.dual_driver import run_driver_worker

        return run_driver_worker(args=args, profile=profile)
    invocation = shlex.join([sys.executable, "-m", "tools.agent.load_harness", *sys.argv[1:]])
    shape = _certified_shape(profile, args)
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
                stagger_ms=stagger_ms or _DEFAULT_STAGGER_MS,
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
        spread_indices = _spread_probe_indices(
            profile.actor_count, _INSTRUMENTED_SAMPLE_SIZE, args.spread_probe_size
        )
        if args.subprocess_cluster:
            host_factory = _subprocess_cluster_factory(
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
            host_factory = _distributed_factory(
                python_workers=profile.python_workers,
                delivery_strategy=profile.delivery_strategy,
                delivery_workers=profile.delivery_workers,
                evidence_budget_bytes=evidence_budget,
                extra_instrumented_indices=spread_indices,
            )
        elif args.embedded:
            host_factory = _embedded_factory(
                python_workers=profile.python_workers,
                delivery_strategy=profile.delivery_strategy,
                delivery_workers=profile.delivery_workers,
                native_apply=args.native_apply,
                evidence_budget_bytes=evidence_budget,
                extra_instrumented_indices=spread_indices,
            )
        else:
            host_factory = _external_factory(args.host, args.port, spread_indices)
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


if __name__ == "__main__":
    raise SystemExit(main())
