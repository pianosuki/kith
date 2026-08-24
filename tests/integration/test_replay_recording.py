"""Integration test: embedded-server replay recording end-to-end.

Boots :class:`examples.embedded.server.EmbeddedServer` with ``--record``
wiring (artifact path plus a root seed for generator checkpoints), drives
the control plane (``login`` / ``move``), and verifies the resulting
binary artifact against the format contract: it parses under the strict
reader (which enforces the ordering law), carries the expected metadata,
elides idle ticks, pins one generator checkpoint per recorded tick, and
replays deterministically through the replay tool.
"""

from __future__ import annotations

import json
import subprocess
import sys
import threading
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any, cast
from urllib import request

from _helpers import _BUILD_DEBUG, needs_build, wait_for_replay_records
from examples._common import replay_format as rf
from examples.embedded.server import EmbeddedServer

from kith import Server, ServerStatus


_REPO_ROOT = Path(__file__).resolve().parents[2]
_SIM_LIB = (_BUILD_DEBUG / "libkith_sim.so.1").resolve()
_REPLAY = _REPO_ROOT / "tools" / "replay.py"
_RECORD_SEED = 0xDEADBEEF


def _status(server: EmbeddedServer) -> ServerStatus:
    assert server._server is not None
    return server._server.status


def _post_json(url: str, body: dict[str, Any]) -> dict[str, Any]:
    data = json.dumps(body).encode("utf-8")
    req = request.Request(url, data=data, method="POST")
    req.add_header("Content-Type", "application/json")
    with request.urlopen(req, timeout=5.0) as resp:
        return cast("dict[str, Any]", json.loads(resp.read().decode("utf-8")))


def _wait_running(server: EmbeddedServer, facade: Server) -> threading.Thread:
    """Start the run loop on a daemon thread and wait for RUNNING."""
    run_thread = threading.Thread(target=facade.run, daemon=True)
    run_thread.start()
    deadline = time.monotonic() + 5.0
    while _status(server) is not ServerStatus.RUNNING and time.monotonic() < deadline:
        time.sleep(0.01)
    assert _status(server) is ServerStatus.RUNNING
    return run_thread


def _drive_session(
    base: str, artifact: Path, principals: tuple[int, ...], moves_per_actor: int
) -> None:
    """Log the principals in and move each actor over separated windows.

    Every recording event is paced on the artifact's own drain: the next
    POST goes out only after the prior event's tick record is readable,
    so each event occupies its own boundary window.
    """
    moves = 0
    for spawns, principal in enumerate(principals, start=1):
        login = _post_json(f"{base}/login", {"principal_id": principal})
        actor_id = int(login["actor_id"])
        wait_for_replay_records(artifact, _events_recorded(spawns, moves))
        for _step in range(moves_per_actor):
            moves += 1
            _post_json(f"{base}/move", {"actor_id": actor_id, "move_x": 1000, "move_y": 0})
            wait_for_replay_records(artifact, _events_recorded(spawns, moves))


def _play_hashes(artifact: Path) -> dict[int, int]:
    """Replay the artifact through the tool twice; return the hash sequence."""
    args = [sys.executable, str(_REPLAY)]
    if _SIM_LIB.is_file():
        args += ["--lib", str(_SIM_LIB)]
    args += ["--input", str(artifact)]
    runs = [
        subprocess.run(
            [*args, "--hash-every", "5"],
            capture_output=True,
            text=True,
            check=True,
            timeout=120,
        )
        for _ in range(2)
    ]
    assert runs[0].stdout == runs[1].stdout
    hashes: dict[int, int] = {}
    for line in runs[0].stdout.strip().splitlines():
        tick_part, hash_part = line.split()
        hashes[int(tick_part.split("=")[1])] = int(hash_part.split("=")[1], 16)
    return hashes


def _events_recorded(spawns: int, moves: int) -> Callable[[rf.ReplayDocument], bool]:
    """True once the artifact holds ``spawns`` spawns and ``moves`` moves."""

    def satisfied(doc: rf.ReplayDocument) -> bool:
        events = [event for tick in doc.ticks for event in tick.events]
        seen_spawns = sum(isinstance(event, rf.SpawnEvent) for event in events)
        seen_moves = sum(isinstance(event, rf.MoveEvent) for event in events)
        return seen_spawns >= spawns and seen_moves >= moves

    return satisfied


def test_cli_parses_record_flags() -> None:
    """The CLI surface exposes the recording flags under their dest names."""
    from examples.embedded.server import _build_parser

    args = _build_parser().parse_args(["--record", "s.krpl", "--record-seed", "7"])
    assert args.record_path == Path("s.krpl")
    assert args.record_seed == 7
    bare = _build_parser().parse_args([])
    assert bare.record_path is None and bare.record_seed is None


@needs_build
class TestReplayRecording:
    def test_record_parse_verify_and_checkpoint_determinism(self, tmp_path: Path) -> None:
        """A driven session produces a valid, self-consistent artifact."""
        artifact = tmp_path / "session.krpl"
        server = EmbeddedServer(record_path=artifact, record_seed=_RECORD_SEED)
        facade, _gateway_port, control_port = server.start()
        try:
            run_thread = _wait_running(server, facade)
            _drive_session(
                f"http://127.0.0.1:{control_port}", artifact, (100, 200), moves_per_actor=2
            )
            facade.shutdown()
            run_thread.join(timeout=5.0)
            assert _status(server) is ServerStatus.STOPPED
        finally:
            server.stop()

        document = rf.read_document_file(artifact)

        # Header and META: the composition root's tick rate and seed.
        assert document.header.tick_hz == 20
        assert document.meta == {"model": "tile2d", "seed": _RECORD_SEED}

        # Ticks are present, 1-based, strictly ascending (reader-enforced),
        # and carry the spawns and moves the routes drove.
        assert document.ticks, "a driven session records at least one tick"
        ticks = [tick.tick for tick in document.ticks]
        assert ticks[0] >= 1
        spawned = {
            event.actor_id
            for tick in document.ticks
            for event in tick.events
            if isinstance(event, rf.SpawnEvent)
        }
        assert spawned == {1, 2}
        moved = [
            event
            for tick in document.ticks
            for event in tick.events
            if isinstance(event, rf.MoveEvent)
        ]
        assert len(moved) == 4

        # Idle-tick elision: only driven ticks appear, and every written
        # tick carries its checkpoint.
        assert len(document.rng_checkpoints) == len(document.ticks)
        assert all(c.stream_key == 0 for c in document.rng_checkpoints)
        states = {c.state for c in document.rng_checkpoints}
        assert len(states) == 1  # nothing draws from the root generator

        # The artifact replays deterministically: two runs, one hash column.
        assert len(_play_hashes(artifact)) >= 1

    def test_same_seed_yields_identical_checkpoints_and_meta(self, tmp_path: Path) -> None:
        """Two recordings under one seed agree on metadata and checkpoints."""
        artifacts: list[rf.ReplayDocument] = []
        for index in range(2):
            artifact = tmp_path / f"session{index}.krpl"
            server = EmbeddedServer(record_path=artifact, record_seed=_RECORD_SEED)
            facade, _gateway_port, control_port = server.start()
            try:
                run_thread = _wait_running(server, facade)
                _drive_session(
                    f"http://127.0.0.1:{control_port}", artifact, (300,), moves_per_actor=1
                )
                facade.shutdown()
                run_thread.join(timeout=5.0)
            finally:
                server.stop()
            artifacts.append(rf.read_document_file(artifact))

        first, second = artifacts
        assert first.meta == second.meta == {"model": "tile2d", "seed": _RECORD_SEED}
        first_states = [c.state for c in first.rng_checkpoints]
        second_states = [c.state for c in second.rng_checkpoints]
        assert first_states and first_states == second_states
