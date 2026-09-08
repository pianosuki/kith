"""Integration test: the live scale view against a real subprocess cluster.

Boots the two-instance subprocess cluster through the ordinary factory,
runs a lengthened smoke profile as a concurrent task, and polls the view's
once pass against the captured control ports until a frame renders at
least one actor: the every-commit stand-in for watching a full gate run
from a second terminal.
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import replace
from pathlib import Path

from examples.spatial import messages as spatial_messages
from tools.agent.live_view import (
    InstanceEndpoint,
    LiveFrame,
    _once_exit_code,
    once_frame,
)
from tools.agent.load_harness import (
    _BREADTH_EVIDENCE_BUDGET_BYTES,
    SMOKE_DISTRIBUTED,
    GateReport,
    LoadHostFactory,
    run_profile,
)
from tools.agent.subprocess_cluster_host import subprocess_cluster_factory


def test_live_view_once_renders_during_smoke_run(tmp_path: Path) -> None:
    handshakes: list[tuple[int, int]] = []
    factory: LoadHostFactory = subprocess_cluster_factory(
        ahc_event_history_size=8192,
        instance_count=2,
        replication_batch_type_id=spatial_messages.ACTOR_STATE_BATCH_TYPE,
        evidence_budget_bytes=_BREADTH_EVIDENCE_BUDGET_BYTES,
        handshake_out=handshakes,
    )
    profile = replace(SMOKE_DISTRIBUTED, name="smoke-distributed-live-view", duration_s=3.0)
    record_path = tmp_path / "live_view.jsonl"

    async def scenario() -> tuple[LiveFrame, GateReport]:
        loop = asyncio.get_running_loop()
        task = asyncio.create_task(run_profile(profile, factory))
        frame: LiveFrame | None = None
        report: GateReport | None = None
        try:
            deadline = loop.time() + 30.0
            while loop.time() < deadline and not task.done():
                if not handshakes:
                    await asyncio.sleep(0.2)
                    continue
                endpoints = tuple(
                    InstanceEndpoint(host="127.0.0.1", port=control_port, origin="hosts")
                    for _gateway, control_port in handshakes
                )
                frame = await once_frame(endpoints, sample_size=200, record_path=str(record_path))
                if any(inst.actors for inst in frame.instances):
                    break
                await asyncio.sleep(0.5)
        finally:
            report = await task
        assert frame is not None, "the view never rendered an actor while the run executed"
        assert report is not None
        return frame, report

    frame, _report = asyncio.run(scenario())
    assert _once_exit_code(frame) == 0
    assert any(inst.actors for inst in frame.instances)
    for inst in frame.instances:
        assert inst.roster_total is not None
        assert inst.counters is not None
        assert inst.counters.dispatch_dropped is not None
    lines = record_path.read_text().splitlines()
    assert lines
    for line in lines:
        record = json.loads(line)
        assert set(record) == {"ts_mono_ns", "mode", "instances"}
