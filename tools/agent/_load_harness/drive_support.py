"""Input submission, movement anchors and process-trace capture."""

from __future__ import annotations

import json
import os
import time
from collections.abc import Sequence
from dataclasses import asdict

from examples.spatial import handlers as spatial_handlers
from examples.spatial import messages as spatial_messages

from kith import SimInput
from tools.agent.gap_ledger import (
    ClientSeries,
)
from tools.agent.orchestrator import ManagedClient
from tools.agent.proc_sampler import ProcTrace, SamplerTarget

from .constants import MOVE_INPUT
from .records import ClientRecord


def movement_anchor_delay_s(movement_anchor_ns: int | None) -> float:
    """Return the seconds the movement phase must wait for ``movement_anchor_ns``.

    Positive when the anchor is ahead of now (the wait that holds a
    fixed cross-process offset); zero once the anchor has passed, so a
    late drive proceeds immediately rather than shifting its window.
    """
    if movement_anchor_ns is None:
        return 0.0
    return max(0.0, (movement_anchor_ns - time.monotonic_ns()) / 1e9)


def sampler_targets(server_pids: Sequence[int]) -> list[SamplerTarget]:
    """Build the /proc sampler targets: each server instance plus the harness.

    The harness self-target is the instrument-side control: a stall that
    delays both instances' deliveries simultaneously would show here,
    which is the signature the per-instance gap analysis already
    exonerated the shared driver on.
    """
    targets = [SamplerTarget(name=f"instance-{i}", pid=pid) for i, pid in enumerate(server_pids)]
    targets.append(SamplerTarget(name="harness", pid=os.getpid()))
    return targets


def write_proc_trace(path: str, trace: ProcTrace, series: Sequence[ClientSeries]) -> None:
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


def submit_input(client: ManagedClient, rec: ClientRecord, tick: int) -> None:
    """Submit one ``actor_input`` frame for ``rec``'s own actor.

    The input magnitude is fixed at :data:`_MOVE_INPUT`; the input tick
    advances per submit so the sim's movement integration steps forward.
    The call is synchronous: the drive loop interleaves clients itself
    and yields on its own cadence instead of per-input coroutines.
    """
    inp = SimInput(input_tick=tick, move_x=MOVE_INPUT, move_y=0)
    payload = spatial_handlers.encode_actor_input(rec.own_actor_id, inp)
    rec.inputs_submitted += 1
    client.submit(spatial_messages.ACTOR_INPUT_TYPE, payload)
