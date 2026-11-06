"""Bootstrap, movement and teardown orchestration for scaling runs."""

from __future__ import annotations

import asyncio
import math
import time
from collections.abc import Sequence
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from fractions import Fraction

from tools.agent.gap_ledger import (
    ClientSeries,
)
from tools.agent.observer_gates import (
    DEFAULT_OBSERVER_LIMITS,
    InstanceServerSample,
    ObservationVerdict,
    ObserverLimits,
    evaluate_observation,
)
from tools.agent.orchestrator import Orchestrator
from tools.agent.proc_sampler import ProcSampler, ProcTrace

from .breadth import collect_window_evidence, measure_publish_rate_skew
from .constants import (
    BOOTSTRAP_READY,
    BREADTH_WINDOW_DRAIN_S,
    INSTRUMENTED_SAMPLE_SIZE,
    POLL_INTERVAL_S,
    POST_WINDOW_ECHO_SETTLE_S,
    SUBMIT_YIELD_EVERY,
)
from .counters import (
    ABSENT_PHASE_COUNTERS,
    CounterSample,
    PhaseCounters,
    WindowCounters,
    phase_counters_delta,
    run_counter_sampler,
    scrape_phase_counters,
)
from .depth import (
    attach_backlog,
    attach_ledger,
    certified_metrics,
    collect_cohort_depth,
    collect_depth_metrics,
    echo_delta_histogram,
    flicker_breakdown_from_series,
    instance_gate_metrics,
    measure_move_missing_ratio,
    measure_selected_clients_ratio,
    per_instance_ranks,
    principal_bindings,
    resolve_own_actors_dense,
    start_concurrency,
)
from .drive_support import movement_anchor_delay_s, sampler_targets, submit_input, write_proc_trace
from .metrics import CohortDepth, GateMetrics
from .profiles import ScalingProfile, thresholds_for
from .records import ClientRecord, LoadHostFactory, MovementRound, OfferedLoad
from .report import GateReport
from .runtime import RunShape, collect_run_envelope
from .statistics import percentile


@dataclass(frozen=True, slots=True)
class DriveOutcome:
    """Everything report assembly needs from one completed drive.

    The drives hold the /proc trace and per-client bookkeeping locally so
    the sampler's raw samples never outlive the drive; report assembly
    consumes them here to build the observer verdict alongside the metrics.
    """

    metrics: GateMetrics
    trace: ProcTrace
    records: list[ClientRecord]
    window_counters: list[WindowCounters]
    counter_samples: tuple[CounterSample, ...] = ()


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
        "_offered_load",
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
        instrumented_sample_size: int = INSTRUMENTED_SAMPLE_SIZE,
        spread_probe_indices: tuple[int, ...] = (),
        movement_anchor_ns: int | None = None,
        limits: ObserverLimits = DEFAULT_OBSERVER_LIMITS,
        echo_settle_s: float | None = None,
        shape: RunShape | None = None,
    ) -> None:
        if not math.isfinite(profile.input_hz) or not 0 < profile.input_hz <= 1e9:
            raise ValueError("input_hz must be finite, positive, and at most 1e9 Hz")
        if not math.isfinite(profile.duration_s) or profile.duration_s < 1e-9:
            raise ValueError("duration_s must be finite and at least one nanosecond")
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
        # CLOCK_MONOTONIC anchors survive bootstrap and scrape delays:
        # late preparation invalidates the load instead of moving its origin.
        self._movement_anchor_ns = movement_anchor_ns
        self._offered_load = OfferedLoad(0, 0, 0, 0, 0, 0, 0, ())
        # Ceilings the observer-health verdict judges with (the CLI knobs
        # are its recalibration surface; the frozen defaults stand in when
        # unset).
        self._limits = limits
        # Post-window echo drain (the metric-definition constant; the CLI
        # override is the drain A/B diagnostic surface).
        self._echo_settle_s = POST_WINDOW_ECHO_SETTLE_S if echo_settle_s is None else echo_settle_s
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
            envelope=collect_run_envelope(),
            shape=self._shape,
            offered_load=(self._offered_load,),
        )

    def _observation_for(self, outcome: DriveOutcome) -> ObservationVerdict | None:
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

    async def _drive_dense(self) -> DriveOutcome:
        """Drive the dense profile against one embedded host."""
        orch = self._host_factory()
        sampler = ProcSampler.start(sampler_targets(self._server_pids))
        stopped = False
        try:
            records = await self._register_and_start(orch)
            await self._await_bootstrap(orch, records)
            await self._resolve_own_actors(orch, records)
            await asyncio.sleep(self._profile.settle_s)
            phase_windows, counter_samples = await self._movement_phase(orch, records)
            backlog_pre = await self._frame_age_sample_ms(orch, records)
            await self._post_window_echo_drain(records)
            backlog_post = await self._frame_age_sample_ms(orch, records)
            metrics, series = await self._sample_metrics(
                orch, records, [w.counters for w in phase_windows]
            )
            metrics = replace(metrics, counter_samples=tuple(counter_samples))
            metrics = attach_backlog(metrics, backlog_pre, backlog_post)
            await self._stop_clients(orch, records)
            trace = sampler.stop()
            stopped = True
            if self._proc_trace_output is not None:
                write_proc_trace(self._proc_trace_output, trace, series)
            metrics = attach_ledger(metrics, trace, series)
            return DriveOutcome(metrics, trace, records, phase_windows, tuple(counter_samples))
        finally:
            if not stopped:
                sampler.stop()
            await orch.shutdown()

    async def _register_and_start(self, orch: Orchestrator) -> list[ClientRecord]:
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
            ClientRecord(
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
        sem = asyncio.Semaphore(start_concurrency(n))

        async def _start_one(rec: ClientRecord) -> None:
            async with sem:
                rec.started_ns = time.monotonic_ns()
                await orch.start_client(rec.name)

        await asyncio.gather(*(_start_one(r) for r in records))
        return records

    async def _await_bootstrap(self, orch: Orchestrator, records: list[ClientRecord]) -> None:
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
            still_pending: list[ClientRecord] = []
            for rec in pending:
                status = await orch.client_state(rec.name)
                if status.bootstrap_state == BOOTSTRAP_READY:
                    rec.ready_ns = time.monotonic_ns()
                else:
                    still_pending.append(rec)
            pending = still_pending
            if pending:
                await asyncio.sleep(POLL_INTERVAL_S)

    async def _resolve_own_actors(self, orch: Orchestrator, records: list[ClientRecord]) -> None:
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

    async def _movement_phase(
        self, orch: Orchestrator, records: list[ClientRecord]
    ) -> tuple[list[WindowCounters], list[CounterSample]]:
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
        self._offered_load = OfferedLoad(0, 0, 0, 0, 0, 0, 0, ())
        movable = [r for r in records if r.own_actor_id != 0 and r.ready_ns > r.started_ns]
        if not movable:
            return [], []
        start_scrapes = await scrape_phase_counters(orch)
        clients = [(rec, orch.managed_client(rec.name)) for rec in movable]
        # Keep the decimal profile cadence exact until each absolute due
        # offset is floored to nanoseconds. A rounded 30 Hz period invents
        # a 31st slot just before a one-second deadline.
        period = 1_000_000_000 / Fraction(str(self._profile.input_hz))
        period_num, period_den = period.numerator, period.denominator
        period_ns = period_num // period_den
        duration_ns = round(Fraction(str(self._profile.duration_s)) * 1_000_000_000)
        ready_ns = time.monotonic_ns()
        anchor_ns = ready_ns if self._movement_anchor_ns is None else self._movement_anchor_ns
        deadline_ns = anchor_ns + duration_ns
        planned_rounds = (duration_ns * period_den + period_num - 1) // period_num
        anchor_delay_s = movement_anchor_delay_s(anchor_ns)
        if anchor_delay_s > 0:
            await asyncio.sleep(anchor_delay_s)
            # Exclude the anchor wait from movement counter deltas. Scrape
            # latency remains part of the fixed schedule's actual lateness.
            start_scrapes = await scrape_phase_counters(orch)
        window_start = time.monotonic_ns()
        for rec in records:
            orch.managed_client(rec.name).begin_view_window()
        samples: list[CounterSample] = []
        sampler = asyncio.create_task(run_counter_sampler(orch, samples, window_start))
        try:
            rounds: list[MovementRound] = []
            slot = 0
            missed_slots = 0
            while slot < planned_rounds:
                due_ns = anchor_ns + slot * period_num // period_den
                now_ns = time.monotonic_ns()
                if now_ns < due_ns:
                    await asyncio.sleep((due_ns - now_ns) / 1e9)
                    continue
                if now_ns >= deadline_ns:
                    missed_slots += planned_rounds - slot
                    break
                next_due_ns = anchor_ns + (slot + 1) * period_num // period_den
                if now_ns >= next_due_ns:
                    next_slot = min(
                        ((now_ns - anchor_ns + 1) * period_den + period_num - 1) // period_num,
                        planned_rounds,
                    )
                    missed_slots += next_slot - slot
                    slot = next_slot
                    continue
                tick = slot + 1
                submits = 0
                yields = 0
                for round_index, (rec, client) in enumerate(clients):
                    submit_input(client, rec, tick)
                    submits += 1
                    if round_index % SUBMIT_YIELD_EVERY == SUBMIT_YIELD_EVERY - 1:
                        # Every started round reaches all clients, even on overrun.
                        await asyncio.sleep(0)
                        yields += 1
                rounds.append(
                    MovementRound(
                        tick, due_ns, now_ns, time.monotonic_ns(), now_ns - due_ns, submits, yields
                    )
                )
                slot += 1
            remaining_ns = deadline_ns - time.monotonic_ns()
            if remaining_ns > 0:
                await asyncio.sleep(remaining_ns / 1e9)
            window_end = time.monotonic_ns()
            self._offered_load = OfferedLoad(
                ready_ns,
                anchor_ns,
                deadline_ns,
                period_ns,
                len(clients),
                planned_rounds,
                missed_slots,
                tuple(rounds),
            )
        finally:
            sampler.cancel()
            await asyncio.gather(sampler, return_exceptions=True)
        deadline = window_end + int(BREADTH_WINDOW_DRAIN_S * 1e9)
        for rec in records:
            orch.managed_client(rec.name).seal_view_window(deadline)
        for rec in records:
            rec.movement_start_ns = window_start
            rec.movement_end_ns = window_end
        end_scrapes = await scrape_phase_counters(orch)
        deltas = phase_counters_delta(
            [s.counters for s in start_scrapes], [e.counters for e in end_scrapes]
        )
        windows: list[WindowCounters] = []
        for start, end, delta in zip(start_scrapes, end_scrapes, deltas, strict=False):
            present = start.present and end.present
            windows.append(WindowCounters(present, delta if present else ABSENT_PHASE_COUNTERS))
        return windows, samples

    async def _post_window_echo_drain(self, records: Sequence[ClientRecord]) -> None:
        """Wait for the absolute breadth and depth collection deadlines.

        The echo pipeline lags the last submitted tick by up to the tiered
        max-gap backstop plus one delivery cadence; a scrape at window end
        counts that lag as move-missing. Sleeping for
        :data:`_POST_WINDOW_ECHO_SETTLE_S` lets in-flight echoes land, so
        move-missing measures echo lag beyond the bound plus permanent
        loss instead of scrape timing. Work already elapsed after movement
        counts toward the wait. Breadth keeps its own fixed drain when a
        diagnostic selects a shorter echo drain. Missing phase counters
        do not remove either deadline.
        """
        ends = [rec.movement_end_ns for rec in records if rec.movement_end_ns != 0]
        if ends:
            deadline = max(ends) + int(max(self._echo_settle_s, BREADTH_WINDOW_DRAIN_S) * 1e9)
            remaining = deadline - time.monotonic_ns()
            if remaining > 0:
                await asyncio.sleep(remaining / 1e9)

    @staticmethod
    async def _frame_age_sample_ms(
        orch: Orchestrator, records: Sequence[ClientRecord]
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
        return int(percentile(ages, 50)), int(percentile(ages, 95))

    async def _sample_metrics(
        self,
        orch: Orchestrator,
        records: list[ClientRecord],
        phase_counters: Sequence[PhaseCounters],
    ) -> tuple[GateMetrics, tuple[ClientSeries, ...]]:
        """Compute the §4.4 metrics from the post-movement client state.

        Returns the metrics plus the instrumented sample's own-actor
        series so the caller can build the flicker ledger from the same
        fetch (the event histories are drained at shutdown).
        """
        thresholds = thresholds_for(self._profile)
        breadth, evidence = await collect_window_evidence(
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
        submitted, observed, series, cert_rows = await collect_depth_metrics(
            evidence, instrumented, self._echo_settle_s
        )
        move_missing = measure_move_missing_ratio(submitted, observed)
        move_missing_certified, cert_fallbacks = certified_metrics(cert_rows)
        flicker = flicker_breakdown_from_series(series)
        echo = echo_delta_histogram(series)
        head_cohort: CohortDepth | None = None
        spread_cohort: CohortDepth | None = None
        if self._spread_probe_indices:
            head_cohort = await collect_cohort_depth(
                evidence,
                "head",
                list(instrumented),
                per_instance_ranks(records),
                self._echo_settle_s,
                depth_rows=cert_rows,
            )
            spread_recs = [r for r in records if r.spread and r.own_actor_id != 0]
            if spread_recs:
                spread_cohort = await collect_cohort_depth(
                    evidence,
                    "spread",
                    spread_recs,
                    per_instance_ranks(records),
                    self._echo_settle_s,
                    depth_rows=cert_rows,
                )
        per_client_inputs = [r.inputs_submitted for r in records]
        rounds_min = min(per_client_inputs) if per_client_inputs else 0
        rounds_median = int(percentile(per_client_inputs, 50)) if per_client_inputs else 0
        rounds_max = max(per_client_inputs) if per_client_inputs else 0

        latencies_ms = [
            max(0, (r.ready_ns - r.started_ns) // 1_000_000)
            for r in records
            if r.ready_ns > r.started_ns
        ]
        p95 = percentile(latencies_ms, 95) if latencies_ms else 0

        return (
            GateMetrics(
                actor_count=total,
                sampled_clients=len(instrumented),
                session_ok_ratio=session_ok / total if total else 0.0,
                selected_clients_ratio=measure_selected_clients_ratio(
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

    async def _stop_clients(self, orch: Orchestrator, records: list[ClientRecord]) -> None:
        """Stop every client before the orchestrator shuts down."""
        await asyncio.gather(
            *(orch.stop_client(r.name) for r in records),
            return_exceptions=True,
        )

    # -----------------------------------------------------------------------
    # distributed-topology drive
    # -----------------------------------------------------------------------

    async def _drive_distributed(self) -> DriveOutcome:
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
        sampler = ProcSampler.start(sampler_targets(self._server_pids))
        stopped = False
        try:
            records = await self._register_and_start(orch)
            await self._await_bootstrap(orch, records)
            await self._resolve_instance_assignment(orch, records)
            await self._resolve_own_actors_distributed(orch, records)
            await asyncio.sleep(self._profile.settle_s)
            phase_windows, counter_samples = await self._movement_phase(orch, records)
            backlog_pre = await self._frame_age_sample_ms(orch, records)
            await self._post_window_echo_drain(records)
            backlog_post = await self._frame_age_sample_ms(orch, records)
            metrics, series = await self._sample_distributed_metrics(
                orch, records, [w.counters for w in phase_windows]
            )
            metrics = replace(metrics, counter_samples=tuple(counter_samples))
            metrics = attach_backlog(metrics, backlog_pre, backlog_post)
            await self._stop_clients(orch, records)
            trace = sampler.stop()
            stopped = True
            if self._proc_trace_output is not None:
                write_proc_trace(self._proc_trace_output, trace, series)
            metrics = attach_ledger(metrics, trace, series)
            return DriveOutcome(metrics, trace, records, phase_windows, tuple(counter_samples))
        finally:
            if not stopped:
                sampler.stop()
            await orch.shutdown()

    def _instance_count(self, records: list[ClientRecord]) -> int:
        """Return the number of distinct instances the clients landed on."""
        return len({r.instance_index for r in records}) or 1

    async def _resolve_instance_assignment(
        self, orch: Orchestrator, records: list[ClientRecord]
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
        self, orch: Orchestrator, records: list[ClientRecord]
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
            await resolve_own_actors_dense(orch, records)
            return
        booted_by_instance: dict[int, list[ClientRecord]] = {}
        for rec in records:
            if rec.ready_ns > rec.started_ns and rec.ready_ns != 0:
                booted_by_instance.setdefault(rec.instance_index, []).append(rec)
        for instance_index, booted in booted_by_instance.items():
            if instance_index >= len(control.controls):
                continue
            bindings = await principal_bindings(control.controls[instance_index])
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
        records: list[ClientRecord],
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
        breadth, evidence = await collect_window_evidence(
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
        submitted, observed, series, cert_rows = await collect_depth_metrics(
            evidence, instrumented, self._echo_settle_s
        )
        move_missing = measure_move_missing_ratio(submitted, observed)
        move_missing_certified, cert_fallbacks = certified_metrics(cert_rows)
        flicker = flicker_breakdown_from_series(series)
        echo = echo_delta_histogram(series)
        head_cohort: CohortDepth | None = None
        spread_cohort: CohortDepth | None = None
        if self._spread_probe_indices:
            head_cohort = await collect_cohort_depth(
                evidence,
                "head",
                list(instrumented),
                per_instance_ranks(records),
                self._echo_settle_s,
                depth_rows=cert_rows,
            )
            spread_recs = [r for r in records if r.spread and r.own_actor_id != 0]
            if spread_recs:
                spread_cohort = await collect_cohort_depth(
                    evidence,
                    "spread",
                    spread_recs,
                    per_instance_ranks(records),
                    self._echo_settle_s,
                    depth_rows=cert_rows,
                )
        per_client_inputs = [r.inputs_submitted for r in records]
        rounds_min = min(per_client_inputs) if per_client_inputs else 0
        rounds_median = int(percentile(per_client_inputs, 50)) if per_client_inputs else 0
        rounds_max = max(per_client_inputs) if per_client_inputs else 0

        latencies_ms = [
            max(0, (r.ready_ns - r.started_ns) // 1_000_000)
            for r in records
            if r.ready_ns > r.started_ns
        ]
        p95 = percentile(latencies_ms, 95) if latencies_ms else 0

        skew = await measure_publish_rate_skew(evidence, records)

        return (
            GateMetrics(
                actor_count=total,
                sampled_clients=len(instrumented),
                session_ok_ratio=session_ok / total if total else 0.0,
                selected_clients_ratio=measure_selected_clients_ratio(
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
                instances=instance_gate_metrics(records, breadth, cert_rows, series),
            ),
            series,
        )


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


DriveOutcome.__module__ = "tools.agent.load_harness"
LoadHarness.__module__ = "tools.agent.load_harness"
