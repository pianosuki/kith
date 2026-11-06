"""Scaling-gate verdicts and report serialization."""

from __future__ import annotations

import json
import math
from dataclasses import asdict, dataclass
from fractions import Fraction

from tools.agent.observer_gates import (
    VERDICT_OK,
    ObservationVerdict,
)

from .constants import POST_WINDOW_ECHO_SETTLE_S
from .metrics import GateMetrics
from .profiles import GateThresholds, ScalingProfile, meets_bars
from .records import OfferedLoad
from .render import (
    fmt_move_missing_max,
    fmt_skew,
    fmt_skew_max,
    render_backlog,
    render_cohorts,
    render_counter_samples,
    render_echo_delta,
    render_flicker_breakdown,
    render_flicker_ledger,
    render_gap_attribution,
    render_instance_rows,
    render_observation,
    render_phase_counters,
    table_row,
    verdict_label,
)
from .runtime import RunEnvelope, RunShape


@dataclass(frozen=True, slots=True)
class GateReport:
    """A renderable report over one scaling-gate run.

    Pure value object: holds the profile, metrics, thresholds, and run
    metadata and produces strings; performs no I/O. The ``passed`` property
    compares metrics against thresholds, rejects incomplete offered load,
    and requires an ``ok`` observer verdict when one is attached, so
    classification only adds failure modes. ``invocation`` records the command
    line that drove the run (the profile carries the resolved configuration),
    so the report alone
    reproduces its own measurement.
    """

    profile: ScalingProfile
    metrics: GateMetrics
    thresholds: GateThresholds
    started_at: str
    duration_s: float
    invocation: str | None = None
    observation: ObservationVerdict | None = None
    echo_settle_s: float = POST_WINDOW_ECHO_SETTLE_S
    envelope: RunEnvelope | None = None
    shape: RunShape | None = None
    offered_load: tuple[OfferedLoad, ...] = ()

    @property
    def offered_load_complete(self) -> bool | None:
        """Require every cohort to be ready before the first movement anchor."""
        if not self.offered_load:
            return None
        first_anchor = min(load.anchor_ns for load in self.offered_load)
        return all(load.complete and load.ready_ns <= first_anchor for load in self.offered_load)

    @property
    def passed(self) -> bool:
        m = self.metrics
        t = self.thresholds
        return (
            meets_bars(m, t)
            and self.offered_load_complete is not False
            and m.publish_rate_skew <= t.publish_rate_skew_max
            and m.breadth_violation is None
            and (self.observation is None or self.observation.klass == VERDICT_OK)
            and (not t.require_measured_channels or m.sampled_clients > 0)
            and all(meets_bars(inst, t) for inst in m.instances)
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "profile": asdict(self.profile),
            "metrics": asdict(self.metrics),
            "thresholds": asdict(self.thresholds),
            "passed": self.passed,
            "offered_load": [asdict(load) for load in self.offered_load],
            "offered_load_complete": self.offered_load_complete,
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
        if self.offered_load_complete is False:
            lines.append("**Offered load incomplete:** timing evidence does not qualify this run.")
        for index, load in enumerate(self.offered_load):
            state = "complete" if load.complete else "INCOMPLETE"
            first_offset = load.rounds[0].start_ns - load.anchor_ns if load.rounds else None
            lines.append(
                f"**Offered load {index}:** {state}; {len(load.rounds)}/{load.planned_rounds} "
                f"rounds; {load.missed_slots} missed slots; "
                f"ready offset {load.ready_ns - load.anchor_ns} ns; "
                f"first-round offset {first_offset} ns"
            )
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
            table_row(
                "session_ok",
                f"{m.session_ok_ratio:.2%}",
                f">= {t.session_ok_min:.2%}",
                verdict_label(m.session_ok_ratio >= t.session_ok_min),
            ),
            table_row(
                "selected_clients",
                f"{m.selected_clients_ratio:.2%}"
                + (f" of {m.eligible_clients} eligible" if m.eligible_clients is not None else ""),
                f">= {t.selected_clients_min:.2%}",
                verdict_label(m.selected_clients_ratio >= t.selected_clients_min),
            ),
            table_row(
                "move_missing_ratio_certified",
                "n/a"
                if m.move_missing_ratio_certified is None
                else f"{m.move_missing_ratio_certified:.2%}",
                fmt_move_missing_max(t.move_missing_certified_max),
                "—"
                if t.move_missing_certified_max is None
                else verdict_label(
                    m.move_missing_ratio_certified is not None
                    and m.move_missing_ratio_certified < t.move_missing_certified_max
                ),
            ),
            table_row(
                "mm_event (companion)",
                f"{m.move_missing_ratio:.2%}",
                fmt_move_missing_max(t.move_missing_max),
                "—"
                if t.move_missing_max is None
                else verdict_label(m.move_missing_ratio < t.move_missing_max),
            ),
            table_row(
                "bootstrap_ms_p95",
                m.bootstrap_ms_p95,
                f"< {t.bootstrap_ms_p95_max}",
                verdict_label(m.bootstrap_ms_p95 < t.bootstrap_ms_p95_max),
            ),
            table_row(
                "continuity_flicker",
                m.continuity_flicker,
                "forbidden" if t.continuity_flicker_forbidden else "allowed",
                "—"
                if not t.continuity_flicker_forbidden
                else verdict_label(not m.continuity_flicker),
            ),
            table_row(
                "publish_rate_skew",
                fmt_skew(m.publish_rate_skew),
                fmt_skew_max(t.publish_rate_skew_max),
                verdict_label(m.publish_rate_skew <= t.publish_rate_skew_max),
            ),
        ]
        if m.isolated_clients is not None:
            # Reported-only: isolation is a map-placement property, never a
            # delivery failure; the ratio row above names the eligible
            # population it excludes.
            lines.append(
                table_row(
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
                    table_row(
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
                    table_row(
                        f"  {entry.name}",
                        f"{entry.klass} ({tag})",
                        "reported-only",
                        "—",
                    )
                )
        if m.unbound_clients:
            lines.append(
                table_row(
                    "unbound_clients",
                    m.unbound_clients,
                    "reported-only",
                    "—",
                )
            )
        if m.late_bootstrapped_clients:
            lines.append(
                table_row(
                    "late_bootstrapped_clients",
                    m.late_bootstrapped_clients,
                    "reported-only",
                    "—",
                )
            )
        if m.breadth_stalled_clients:
            lines.append(
                table_row(
                    "breadth_stalled_clients",
                    m.breadth_stalled_clients,
                    "reported-only",
                    "—",
                )
            )
        if m.breadth_incomplete_clients:
            lines.append(
                table_row(
                    "breadth_incomplete_clients",
                    m.breadth_incomplete_clients,
                    "reported-only",
                    "—",
                )
            )
        if m.breadth_never_opened_clients:
            lines.append(
                table_row(
                    "breadth_never_opened_clients",
                    m.breadth_never_opened_clients,
                    "reported-only",
                    "—",
                )
            )
        if m.breadth_sparse_clients:
            lines.append(
                table_row(
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
                table_row(
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
                table_row(
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
                table_row(
                    "certified_fallback_clients",
                    m.certified_fallback_clients,
                    "reported-only",
                    "—",
                )
            )
        duration_ns = round(Fraction(str(self.profile.duration_s)) * 1_000_000_000)
        planned = math.ceil(duration_ns * Fraction(str(self.profile.input_hz)) / 1_000_000_000)
        per_client = m.inputs_submitted / max(self.profile.actor_count, 1)
        rounds_value = f"{per_client:.1f} / {planned} planned"
        if m.rounds_max > 0:
            rounds_value += f" (min {m.rounds_min} / med {m.rounds_median} / max {m.rounds_max})"
        lines.append(
            table_row(
                "submit_rounds_per_client",
                rounds_value,
                "diagnostic",
                "—",
            )
        )
        if m.breadth_violation is not None:
            lines.append(f"**Breadth guardrail:** {m.breadth_violation}")
        if m.instances:
            lines.append(render_instance_rows(m, t))
        lines.append("")
        if self.observation is not None:
            lines.append(render_observation(self.observation))
            lines.append("")
        if m.flicker_count > 0 and m.flicker_breakdown is not None:
            lines.append(render_flicker_breakdown(m.flicker_breakdown, m.sampled_clients))
            lines.append("")
        if m.phase_counters:
            lines.append(render_phase_counters(m.phase_counters))
            lines.append("")
        if m.counter_samples:
            lines.append(render_counter_samples(m.counter_samples))
            lines.append("")
        if m.backlog_post_drain_median_ms is not None or m.backlog_pre_drain_median_ms is not None:
            lines.append(render_backlog(m))
            lines.append("")
        if m.head_cohort is not None or m.spread_cohort is not None:
            lines.append(render_cohorts(m))
            lines.append("")
        if m.echo_delta is not None:
            lines.append(render_echo_delta(m.echo_delta, m.move_missing_ratio, m.inputs_submitted))
            lines.append("")
        if m.flicker_ledger is not None:
            lines.append(render_flicker_ledger(m.flicker_ledger))
            lines.append("")
            lines.append(render_gap_attribution(m.flicker_ledger))
            lines.append("")
        return "\n".join(lines)


GateReport.__module__ = "tools.agent.load_harness"
