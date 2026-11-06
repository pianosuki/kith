"""Per-client bookkeeping and offered-load timing records."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from tools.agent.orchestrator import Orchestrator


LoadHostFactory = Callable[[], Orchestrator]


@dataclass(frozen=True, slots=True)
class MovementRound:
    """One completed client walk on the absolute offered-load schedule.

    Submit counts denote returned client API calls, not kernel writes.
    """

    input_tick: int
    due_ns: int
    start_ns: int
    end_ns: int
    lateness_ns: int
    submits: int
    yields: int


@dataclass(frozen=True, slots=True)
class OfferedLoad:
    """Actual movement timing and completeness, separate from delivery bars.

    A full-slot overrun skips all elapsed slots and resumes at the next
    future slot. Missing slots, readiness after the anchor, or a client
    walk finishing after the planned deadline make the attempt incomplete.
    Sub-period wakeup lateness is recorded without a jitter allowance.
    ``period_ns`` is the whole-nanosecond minimum interval; adjacent slots
    can differ by one nanosecond. Each due time floors its exact schedule
    offset independently; the duration rounds to the nearest nanosecond.
    """

    ready_ns: int
    anchor_ns: int
    deadline_ns: int
    period_ns: int
    movable_clients: int
    planned_rounds: int
    missed_slots: int
    rounds: tuple[MovementRound, ...]

    @property
    def complete(self) -> bool:
        return (
            self.movable_clients > 0
            and self.ready_ns <= self.anchor_ns
            and self.missed_slots == 0
            and len(self.rounds) == self.planned_rounds
            and bool(self.rounds)
            and self.rounds[-1].end_ns <= self.deadline_ns
            and all(r.submits == self.movable_clients for r in self.rounds)
        )


@dataclass(slots=True)
class ClientRecord:
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


MovementRound.__module__ = "tools.agent.load_harness"
OfferedLoad.__module__ = "tools.agent.load_harness"
ClientRecord.__module__ = "tools.agent.load_harness"
