"""Integration test: replay-record parity between the two movement paths.

Drives one scripted wire stream through the embedded example twice — the
Python movement handler and the pool-dispatched native C handler — with
recording enabled, and proves the replay contract across the paths:
identical live trajectories, identical v1 record streams, and an
identical golden-hash replay sequence pinned against a committed
baseline. Re-login (a duplicate login on the live
session and a reconnect after a socket close) and session close are
exercised mid-stream.

Record comparison happens on the input-tick grid the wire protocol
carries: the recorder attributes events to the server's wall-clock tick
counter, so absolute tick numbers and per-boundary grouping are cadence
metadata that differs across any two live runs. Canonicalizing both
artifacts onto the scripted input-tick grid reduces them to the logical
event stream, which is the path-independent content the replay contract
speaks of.
"""

from __future__ import annotations

import asyncio
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
from examples.spatial import messages
from examples.spatial.client import make_ahc
from examples.spatial.handlers import encode_actor_input, encode_login
from examples.tile_rpg import physics

from kith import ServerStatus, SimInput


_REPO_ROOT = Path(__file__).resolve().parents[2]
_REPLAY = _REPO_ROOT / "tools" / "replay.py"
_SIM_LIB = _BUILD_DEBUG / "libkith_sim.so.1"

_PRINCIPAL_ID = 4242

# The scripted movement stream every run receives: nine inputs with
# client-side input ticks 1..9, deliberately varied so the actor crosses
# fabric cell boundaries throughout.
_MOVES: tuple[tuple[int, int], ...] = (
    (32767, 0),
    (32767, 0),
    (0, 32767),
    (32767, -16384),
    (-32767, 0),
    (0, -32767),
    (16384, 16384),
    (-16384, 8192),
    (8192, -16384),
)

# Pinned rolling world-state hashes of the canonical record, replayed
# through tools/replay.py with the embedded wiring's behavior grid and
# tile2d tuning. Regenerate whenever the model, the grid, or the scripted
# stream changes deliberately; an unexplained mismatch is a determinism
# regression.
GOLDEN_HASHES: dict[int, int] = {
    0: 0x8211F7334C893665,
    1: 0xB245B4759270F742,
    2: 0x9729E732952EFCCF,
    3: 0xDE1AABEB084AA25C,
    4: 0x2C81911B195B310A,
    5: 0xD40966227D02863B,
    6: 0x1839F2D0C9D3E35D,
    7: 0x7FBE280063A40ED8,
    8: 0x33BFEF4BE6A42ED0,
    9: 0x1ECD00BDF72702D6,
}


def _status(server: EmbeddedServer) -> ServerStatus:
    assert server._server is not None
    return server._server.status


def _post_json(url: str, body: dict[str, Any]) -> dict[str, Any]:
    data = json.dumps(body).encode("utf-8")
    req = request.Request(url, data=data, method="POST")
    req.add_header("Content-Type", "application/json")
    with request.urlopen(req, timeout=5.0) as resp:
        return cast("dict[str, Any]", json.loads(resp.read().decode("utf-8")))


def _get_json(url: str) -> dict[str, Any]:
    with request.urlopen(url, timeout=5.0) as resp:
        return cast("dict[str, Any]", json.loads(resp.read().decode("utf-8")))


def _binding_snapshot(base: str) -> dict[str, Any]:
    body = _get_json(f"{base}/bindings")
    return {
        "bindings": sorted(
            (int(pair["principal_id"]), int(pair["actor_id"])) for pair in body["bindings"]
        ),
        "bind_conflicts": int(body["bind_conflicts"]),
        "identity_gate_drops": int(body["identity_gate_drops"]),
    }


async def _wait_bootstrap(client: Any) -> None:
    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline:
        if client.query_status().bootstrap_state == 2:
            return
        await asyncio.sleep(0.02)
    raise AssertionError("wire bootstrap did not complete")


def _bound_actor_id(base: str, principal_id: int) -> int:
    """Poll the control plane until the principal's binding appears."""
    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline:
        for bound_principal, actor_id in _binding_snapshot(base)["bindings"]:
            if bound_principal == principal_id:
                return int(actor_id)
        time.sleep(0.05)
    raise AssertionError(f"principal {principal_id} never bound")


def _moves_flushed(count: int) -> Callable[[rf.ReplayDocument], bool]:
    """True once the spawn and the input ticks 1..count are all recorded."""

    def satisfied(doc: rf.ReplayDocument) -> bool:
        events = [event for tick in doc.ticks for event in tick.events]
        move_ticks = {event.input_tick for event in events if isinstance(event, rf.MoveEvent)}
        has_spawn = any(isinstance(event, rf.SpawnEvent) for event in events)
        return has_spawn and move_ticks == set(range(1, count + 1))

    return satisfied


async def _drive_stream(
    gateway_port: int, base: str, artifact: Path, facade: Any
) -> list[dict[str, int]]:
    """Run the scripted session lifecycle and return the movement trace.

    The stream is one actor's movement across a login, a duplicate
    re-login on the live session, a socket close, a reconnect that
    rebinds the same actor on a new session, and nine movement inputs
    spread across those phases. Every submit is paced on the artifact:
    the next one goes out only after the prior input's tick record has
    drained, so no two events share a boundary window, and the sampled
    position and input tick read the applied state.
    """
    trace: list[dict[str, int]] = []

    client = make_ahc(
        "record-parity-1",
        "127.0.0.1",
        gateway_port,
        principal_id=_PRINCIPAL_ID,
        reconnect_enabled=False,
    )
    await client.start()
    await _wait_bootstrap(client)
    actor_id = _bound_actor_id(base, _PRINCIPAL_ID)

    for phase_moves in (_MOVES[:4], _MOVES[4:6]):
        for move_x, move_y in phase_moves:
            input_tick = len(trace) + 1
            client.submit(
                messages.ACTOR_INPUT_TYPE,
                encode_actor_input(
                    actor_id, SimInput(input_tick=input_tick, move_x=move_x, move_y=move_y)
                ),
            )
            wait_for_replay_records(artifact, _moves_flushed(input_tick))
            after = _get_json(f"{base}/query_state?actor_id={actor_id}")
            trace.append(
                {
                    "pos_x": int(after["pos_x"]),
                    "pos_y": int(after["pos_y"]),
                    "input_tick": int(after["input_tick"]),
                }
            )
        if len(trace) == 4:
            # Duplicate login on the live session: a returning-principal
            # rebind that allocates nothing and records nothing — the next
            # input's poll needs no gap for it.
            client.submit(messages.LOGIN_TYPE, encode_login(_PRINCIPAL_ID))
            # A second live session binding the same principal is the
            # genuine double-subscribe the bind-conflict counter exists
            # for: the first session still holds the seat, so the second
            # bind is counted and permitted. The conflict is paced on the
            # binding snapshot (the rebind writes no artifact record).
            second = make_ahc(
                "record-parity-conflict",
                "127.0.0.1",
                gateway_port,
                principal_id=_PRINCIPAL_ID,
                reconnect_enabled=False,
            )
            await second.start()
            await _wait_bootstrap(second)
            second.submit(messages.LOGIN_TYPE, encode_login(_PRINCIPAL_ID))
            deadline = time.monotonic() + 5.0
            while _binding_snapshot(base)["bind_conflicts"] != 1 and time.monotonic() < deadline:
                time.sleep(0.01)
            assert _binding_snapshot(base)["bind_conflicts"] == 1
            # The second session's disconnect reclaims its seat: the
            # destroyed-session callback pops it, the first session's bind
            # is untouched, and the counter stays at the one conflict.
            await second.stop()
            deadline = time.monotonic() + 5.0
            while facade.session_count != 1 and time.monotonic() < deadline:
                time.sleep(0.01)

    # Socket close: the gateway destroys the session on a reactor pass and
    # the destroyed-session callback reclaims the seat, so the reconnect's
    # rebind records nothing. The count poll above proves the teardown
    # processed before the reconnect starts; the pool is one worker under
    # the standard interpreter, so the reclaim callback runs before any
    # dispatch of the reconnect's login.
    await client.stop()

    client = make_ahc(
        "record-parity-2",
        "127.0.0.1",
        gateway_port,
        principal_id=_PRINCIPAL_ID,
        reconnect_enabled=False,
    )
    await client.start()
    await _wait_bootstrap(client)
    rebound = _bound_actor_id(base, _PRINCIPAL_ID)
    assert rebound == actor_id

    for move_x, move_y in _MOVES[6:]:
        input_tick = len(trace) + 1
        client.submit(
            messages.ACTOR_INPUT_TYPE,
            encode_actor_input(
                actor_id, SimInput(input_tick=input_tick, move_x=move_x, move_y=move_y)
            ),
        )
        wait_for_replay_records(artifact, _moves_flushed(input_tick))
        after = _get_json(f"{base}/query_state?actor_id={actor_id}")
        trace.append(
            {
                "pos_x": int(after["pos_x"]),
                "pos_y": int(after["pos_y"]),
                "input_tick": int(after["input_tick"]),
            }
        )

    await client.stop()
    return trace


def _canonical_document(doc: rf.ReplayDocument) -> rf.ReplayDocument:
    """Regroup an artifact onto the input-tick grid its events carry.

    Spawns move to tick 0 and each movement input to the tick its client
    input names, dropping the wall-clock tick attribution the recorder
    applied. The result is the logical event stream: equal iff both runs
    saw and recorded the same inputs in the same order.
    """
    by_tick: dict[int, list[rf.Event]] = {}
    for tick_record in doc.ticks:
        for event in tick_record.events:
            if isinstance(event, rf.SpawnEvent):
                key = 0
            elif isinstance(event, rf.MoveEvent):
                key = event.input_tick
            else:
                raise AssertionError(f"unexpected despawn record: {event!r}")
            by_tick.setdefault(key, []).append(event)
    ticks = tuple(rf.TickRecord(tick, tuple(by_tick[tick])) for tick in sorted(by_tick))
    return rf.ReplayDocument(
        header=doc.header,
        meta=doc.meta,
        ticks=ticks,
        expected_hashes=(),
        rng_checkpoints=(),
    )


def _behavior_grid(path: Path) -> Path:
    """Write the embedded wiring's behavior grid for the replay tool."""
    from examples.tile_rpg import zone

    tmx_text = (_REPO_ROOT / "examples" / "embedded" / "world.tmx").read_text(encoding="utf-8")
    grid = zone.tmx_to_text_grid(tmx_text)
    path.write_text(grid, encoding="utf-8")
    return path


def _replay_hashes(record: Path, behavior: Path) -> dict[int, int]:
    """Replay one canonical record and return its rolling hash sequence.

    The invocation carries the embedded wiring's model tuning and the
    world grid the live runs loaded, so the replayed trajectory is the
    program the live runs executed.
    """
    cfg = physics.DEFAULT_CONFIG
    args = [
        sys.executable,
        str(_REPLAY),
        "play",
        "--input",
        str(record),
        "--model",
        "tile2d",
        "--hash-every",
        "1",
        "--behavior",
        str(behavior),
        "--base-speed",
        str(cfg.base_speed),
        "--run-speed",
        str(cfg.run_speed),
        "--accel",
        str(cfg.accel),
        "--decel",
        str(cfg.decel),
        "--move-eps",
        str(cfg.move_eps),
        "--collision-radius",
        str(cfg.collision_radius),
    ]
    if _SIM_LIB.is_file():
        args += ["--lib", str(_SIM_LIB)]
    proc = subprocess.run(args, capture_output=True, text=True, check=True, timeout=120)
    hashes: dict[int, int] = {}
    for line in proc.stdout.strip().splitlines():
        tick_part, hash_part = line.split()
        hashes[int(tick_part.split("=")[1])] = int(hash_part.split("=")[1], 16)
    return hashes


@needs_build
class TestSpatialNativeReplayParity:
    def test_record_and_trajectory_parity_across_paths(self, tmp_path: Path) -> None:
        """The scripted stream moves both paths identically, on the wire
        and in the recorded artifact."""
        runs: list[tuple[list[dict[str, int]], Path, dict[str, Any]]] = []
        for native_apply in (False, True):
            artifact = tmp_path / ("python.krp" if not native_apply else "native.krp")
            server = EmbeddedServer(native_apply=native_apply, record_path=artifact)
            facade, gateway_port, control_port = server.start()
            base = f"http://127.0.0.1:{control_port}"
            run_thread = threading.Thread(target=facade.run, daemon=True)
            run_thread.start()
            try:
                deadline = time.monotonic() + 5.0
                while _status(server) is not ServerStatus.RUNNING and time.monotonic() < deadline:
                    time.sleep(0.01)
                assert _status(server) is ServerStatus.RUNNING
                trace = asyncio.run(_drive_stream(gateway_port, base, artifact, facade))
                snapshot = _binding_snapshot(base)
            finally:
                facade.shutdown()
                run_thread.join(timeout=5.0)
                server.stop()
            runs.append((trace, artifact, snapshot))

        trace_python, artifact_python, snapshot_python = runs[0]
        trace_native, artifact_native, snapshot_native = runs[1]
        assert trace_python == trace_native
        # One binding (the principal's actor); the one conflict is the
        # genuine double-subscribe mid-stream (a second live session
        # binding the held principal), not the reconnect: the destroyed
        # session's seat is reclaimed by the destroyed-session callback,
        # so the reconnecting rebind records nothing.
        assert snapshot_native == {
            "bindings": [(_PRINCIPAL_ID, 1)],
            "bind_conflicts": 1,
            "identity_gate_drops": 0,
        }
        assert snapshot_python == snapshot_native

        documents = [rf.read_document_file(a) for a in (artifact_python, artifact_native)]
        for doc in documents:
            # The driver spaces inputs by two tick periods, so every
            # event lands in its own boundary window: one spawn tick and
            # one tick per input, nothing coalesced.
            assert len(doc.ticks) == 1 + len(_MOVES)
            assert all(len(tick.events) == 1 for tick in doc.ticks)
            events = [event for tick in doc.ticks for event in tick.events]
            spawns = [event for event in events if isinstance(event, rf.SpawnEvent)]
            moves = [event for event in events if isinstance(event, rf.MoveEvent)]
            assert len(spawns) == 1
            assert (spawns[0].actor_id, spawns[0].pos_x, spawns[0].pos_y) == (1, 0, 0)
            assert [(m.input_tick, m.move_x, m.move_y, m.move_z, m.flags) for m in moves] == [
                (tick, move_x, move_y, 0, 0)
                for tick, (move_x, move_y) in enumerate(_MOVES, start=1)
            ]
            assert all(move.actor_id == 1 for move in moves)
            assert not any(isinstance(event, rf.DespawnEvent) for event in events)

        canonical = [_canonical_document(doc) for doc in documents]
        assert rf.diff_documents(canonical[0], canonical[1]) == []

        behavior = _behavior_grid(tmp_path / "world.grid")
        hashes = []
        for index, doc in enumerate(canonical):
            record = tmp_path / f"canonical-{index}.krpl"
            record.write_bytes(rf.encode_document(doc))
            hashes.append(_replay_hashes(record, behavior))
        assert hashes[0] == hashes[1]
        assert hashes[0] == GOLDEN_HASHES
