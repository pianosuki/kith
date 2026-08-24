"""Integration test: the native movement-apply path end-to-end.

Boots :class:`examples.embedded.server.EmbeddedServer` with the native-apply
knob (the movement message type handled by the C core through the
pool-dispatched native handler) and proves the C core carries the
game: the control-plane routes drive the C actor table, the same scripted
inputs produce identical positions through the Python and the native path,
the per-tick flush drains the C record buffer into the replay recorder, and
a wire client's movement frames run the full pool-dispatched C handler —
apply, identity gate, and drop counting included.
"""

from __future__ import annotations

import asyncio
import json
import threading
import time
from pathlib import Path
from typing import Any, cast
from urllib import request

from _helpers import needs_build
from examples._common import replay_format
from examples.embedded.server import EmbeddedServer
from examples.spatial import messages
from examples.spatial.client import make_ahc
from examples.spatial.handlers import encode_actor_input

from kith import Server, ServerStatus, SimInput


_REPO_ROOT = Path(__file__).resolve().parents[2]


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


def _boot(**kwargs: Any) -> tuple[EmbeddedServer, Server, str, threading.Thread]:
    """Boot an embedded server and run its facade on a daemon thread."""
    server = EmbeddedServer(**kwargs)
    facade, _gateway_port, control_port = server.start()
    run_thread = threading.Thread(target=facade.run, daemon=True)
    run_thread.start()
    deadline = time.monotonic() + 5.0
    while _status(server) is not ServerStatus.RUNNING and time.monotonic() < deadline:
        time.sleep(0.01)
    assert _status(server) is ServerStatus.RUNNING
    return server, facade, f"http://127.0.0.1:{control_port}", run_thread


def _stop(server: EmbeddedServer, facade: Server, run_thread: threading.Thread) -> None:
    facade.shutdown()
    run_thread.join(timeout=5.0)
    server.stop()


@needs_build
class TestSpatialNativeControlPlane:
    def test_login_move_teleport_bindings_end_to_end(self) -> None:
        server, facade, base, run_thread = _boot(native_apply=True)
        try:
            login = _post_json(f"{base}/login", {"principal_id": 100})
            actor_id = int(login["actor_id"])
            assert actor_id == 1
            assert int(login["pos_x"]) == 0

            move = _post_json(f"{base}/move", {"actor_id": actor_id, "move_x": 32767, "move_y": 0})
            moved_x = int(move["pos_x"])
            assert moved_x > 0
            assert int(move["input_tick"]) == 1

            after = _get_json(f"{base}/query_state?actor_id={actor_id}")
            assert int(after["pos_x"]) == moved_x

            teleport = _post_json(
                f"{base}/teleport", {"actor_id": actor_id, "pos_x": 98304, "pos_y": 65536}
            )
            assert int(teleport["pos_x"]) == 98304
            assert int(teleport["vel_x"]) == 0

            bindings = _get_json(f"{base}/bindings")
            assert bindings["identity_gate_drops"] == 0
            assert any(int(pair["actor_id"]) == actor_id for pair in bindings["bindings"])
        finally:
            _stop(server, facade, run_thread)

    def test_roster_paging_serves_the_c_table(self) -> None:
        """The paged roster routes read the C table in actor-id slot order.

        The load harness collects every instance's roster by paging
        ``/query_state?offset&limit`` and pairing each slice against the
        next page, so the slice ordering is load-bearing: the C snapshot
        walks the actor table's slots in id order and the paginator slices
        that sequence.
        """
        server, facade, base, run_thread = _boot(native_apply=True)
        try:
            for principal_id in (100, 101, 102):
                login = _post_json(f"{base}/login", {"principal_id": principal_id})
                if principal_id == 101:
                    move = _post_json(
                        f"{base}/move",
                        {"actor_id": int(login["actor_id"]), "move_x": 32767, "move_y": 0},
                    )
                    moved_x = int(move["pos_x"])

            roster = _get_json(f"{base}/query_state")
            assert [int(a["actor_id"]) for a in roster["actors"]] == [1, 2, 3]
            assert int(roster["actors"][1]["pos_x"]) == moved_x

            head = _get_json(f"{base}/query_state?offset=0&limit=2")
            assert [int(a["actor_id"]) for a in head["actors"]] == [1, 2]
            assert (int(head["offset"]), int(head["limit"]), int(head["total"])) == (0, 2, 3)

            tail = _get_json(f"{base}/query_state?offset=2&limit=2")
            assert [int(a["actor_id"]) for a in tail["actors"]] == [3]
            assert (int(tail["offset"]), int(tail["limit"]), int(tail["total"])) == (2, 2, 3)
        finally:
            _stop(server, facade, run_thread)

    def test_roster_snapshot_empty_table(self) -> None:
        """A freshly booted native server serves an empty roster both forms."""
        server, facade, base, run_thread = _boot(native_apply=True)
        try:
            assert _get_json(f"{base}/query_state") == {"actors": []}
            page = _get_json(f"{base}/query_state?offset=0&limit=2")
            assert page == {"actors": [], "offset": 0, "limit": 2, "total": 0}
        finally:
            _stop(server, facade, run_thread)

    def test_native_path_matches_python_path(self) -> None:
        """The same control-route inputs move both paths' actors identically."""
        scripted = ((1000, 500), (0, 1000), (-500, 0), (2000, -1500))
        traces: list[list[dict[str, int]]] = []
        for native_apply in (False, True):
            server, facade, base, run_thread = _boot(native_apply=native_apply)
            try:
                login = _post_json(f"{base}/login", {"principal_id": 7})
                actor_id = int(login["actor_id"])
                positions: list[dict[str, int]] = []
                for move_x, move_y in scripted:
                    _post_json(
                        f"{base}/move",
                        {"actor_id": actor_id, "move_x": move_x, "move_y": move_y},
                    )
                    after = _get_json(f"{base}/query_state?actor_id={actor_id}")
                    positions.append(
                        {
                            "pos_x": int(after["pos_x"]),
                            "pos_y": int(after["pos_y"]),
                            "input_tick": int(after["input_tick"]),
                        }
                    )
                traces.append(positions)
            finally:
                _stop(server, facade, run_thread)
        assert traces[0] == traces[1]

    def test_native_recorder_drains_move_events(self, tmp_path: Path) -> None:
        artifact = tmp_path / "native.krp"
        server, facade, base, run_thread = _boot(native_apply=True, record_path=artifact)
        try:
            login = _post_json(f"{base}/login", {"principal_id": 5})
            actor_id = int(login["actor_id"])
            for _ in range(3):
                time.sleep(0.15)  # three tick periods: one move per window
                _post_json(f"{base}/move", {"actor_id": actor_id, "move_x": 1000, "move_y": 0})
            time.sleep(0.25)  # the tick flush lands the buffered records
        finally:
            _stop(server, facade, run_thread)
        document = replay_format.read_document_file(artifact)
        moves = [
            event
            for tick in document.ticks
            for event in tick.events
            if isinstance(event, replay_format.MoveEvent)
        ]
        assert len(moves) >= 1
        assert all(move.actor_id == actor_id for move in moves)


@needs_build
class TestSpatialNativeWire:
    def test_wire_movement_and_identity_gate(self) -> None:
        """Drive the pool-dispatched C handler over the wire.

        A spatial wire client logs in (its bootstrap sends the login frame
        and awaits the first replication frame), then submits movement
        frames: a valid frame moves the bound actor through the C apply
        sequence on a worker, and a frame naming another principal's actor
        drops at the identity gate and counts. The gateway dispatches the
        movement type to the C handler with the pool-bound flag, so this
        exercises the full native seam end to end.
        """
        server = EmbeddedServer(native_apply=True)
        facade, gateway_port, control_port = server.start()
        run_thread = threading.Thread(target=facade.run, daemon=True)
        run_thread.start()
        try:
            deadline = time.monotonic() + 5.0
            while _status(server) is not ServerStatus.RUNNING and time.monotonic() < deadline:
                time.sleep(0.01)
            assert _status(server) is ServerStatus.RUNNING
            base = f"http://127.0.0.1:{control_port}"

            # A second principal joins through the control plane so the wire
            # session's spoofed frame names a live, foreign actor.
            _post_json(f"{base}/login", {"principal_id": 999})
            foreign_bindings = _get_json(f"{base}/bindings")["bindings"]
            assert len(foreign_bindings) == 1
            foreign_actor_id = int(foreign_bindings[0]["actor_id"])
            assert foreign_actor_id == 1

            asyncio.run(_drive_wire_movement(gateway_port, base, principal_id=4242))
        finally:
            _stop(server, facade, run_thread)


async def _drive_wire_movement(gateway_port: int, base: str, principal_id: int) -> None:
    """Log in over the wire, move the bound actor, then spoof a foreign one."""
    client = make_ahc("native-wire", "127.0.0.1", gateway_port, principal_id=principal_id)
    try:
        await client.start()
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline:
            if client.query_status().bootstrap_state == 2:
                break
            await asyncio.sleep(0.02)
        assert client.query_status().bootstrap_state == 2

        # The wire login binds the session through the Python login handler;
        # the binding shows on the control plane.
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline:
            pairs = _binding_pairs(base)
            if principal_id in pairs:
                break
            await asyncio.sleep(0.05)
        pairs = _binding_pairs(base)
        assert principal_id in pairs
        bound_actor_id = pairs[principal_id]

        # Valid frames move the bound actor through the C apply sequence.
        for input_tick in range(1, 4):
            client.submit(
                messages.ACTOR_INPUT_TYPE,
                encode_actor_input(
                    bound_actor_id, SimInput(input_tick=input_tick, move_x=32767, move_y=0)
                ),
            )
            await asyncio.sleep(0.1)
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline:
            if int(_get_json(f"{base}/query_state?actor_id={bound_actor_id}")["pos_x"]) > 0:
                break
            await asyncio.sleep(0.05)
        after = _get_json(f"{base}/query_state?actor_id={bound_actor_id}")
        assert int(after["pos_x"]) > 0

        # A frame naming another principal's actor moves that actor (the
        # gate keeps its apply-first order) but skips the window diff and
        # counts the drop.
        client.submit(
            messages.ACTOR_INPUT_TYPE,
            encode_actor_input(1, SimInput(input_tick=1, move_x=32767, move_y=0)),
        )
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline:
            if int(_get_json(f"{base}/bindings")["identity_gate_drops"]) == 1:
                break
            await asyncio.sleep(0.05)
        assert int(_get_json(f"{base}/bindings")["identity_gate_drops"]) == 1
    finally:
        await client.stop()


def _binding_pairs(base: str) -> dict[int, int]:
    """Return the server's principal-to-actor bindings as a dict."""
    body = _get_json(f"{base}/bindings")
    return {int(pair["principal_id"]): int(pair["actor_id"]) for pair in body["bindings"]}
