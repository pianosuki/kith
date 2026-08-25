"""Integration test: the free-movement walking skeleton end-to-end.

Boots the :mod:`examples.free_movement.server`, verifies the gateway
listener accepts a TCP connection, drives the control plane
(``spawn``/``move``/``query_state``), and confirms the actor's state changes
in the sim. The choreography test additionally pins the per-tick publish
chain: the step integrates a persisted move intent, and the dirty-cell flush
bumps the cell's fabric product once per tick. This is the thinnest
end-to-end slice: one model, one replication type, one wire input, four
control routes.
"""

from __future__ import annotations

import ctypes
import json
import socket
import threading
import time
from pathlib import Path
from typing import Any, cast
from urllib import error, request

import pytest
from _helpers import needs_build
from examples.free_movement.server import FreeMovementServer

from kith import ServerStatus
from kith._bridge import Bridge
from kith._generated import fabric as gen_fabric
from kith._generated import types as gen_types


_REPO_ROOT = Path(__file__).resolve().parents[2]


def _status(server: FreeMovementServer) -> ServerStatus:
    assert server._server is not None
    return server._server.status


def _post_json(url: str, body: dict[str, int]) -> dict[str, Any]:
    data = json.dumps(body).encode("utf-8")
    req = request.Request(url, data=data, method="POST")
    req.add_header("Content-Type", "application/json")
    with request.urlopen(req, timeout=5.0) as resp:
        return cast("dict[str, Any]", json.loads(resp.read().decode("utf-8")))


def _get_json(url: str) -> dict[str, Any]:
    with request.urlopen(url, timeout=5.0) as resp:
        return cast("dict[str, Any]", json.loads(resp.read().decode("utf-8")))


def _cell_product(
    bridge: Bridge, fabric_handle: object, zone_id: int
) -> gen_fabric.kith_fabric_cell_product_t | None:
    """Read the example's single cell product through the C accessor.

    A read-only observation over the server's borrowed fabric handle: the
    composition-root borrowed-handle exception (examples/spatial/native.py)
    extended into the test layer for this pin. No public borrowed-fabric
    surface exists this release.
    """
    key = gen_fabric.kith_fabric_cell_key_t(
        zone=ctypes.c_uint32(zone_id),
        cell_x=ctypes.c_int32(0),
        cell_y=ctypes.c_int32(0),
        cell_z=ctypes.c_int32(0),
        lod=ctypes.c_uint8(0),
        pad=(ctypes.c_uint8 * 3)(),
    )
    out = gen_fabric.kith_fabric_cell_product_t()
    rc = int(
        bridge.lib("fabric").kith_fabric_cell_product(
            fabric_handle,
            ctypes.byref(key),
            ctypes.byref(out),
        )
    )
    if rc == -int(gen_types.kith_error.KITH_ENOENT):
        return None
    assert rc == 0, f"kith_fabric_cell_product: rc={rc}"
    return out


@needs_build
class TestFreeMovementSkeleton:
    def test_spawn_move_query_end_to_end(self) -> None:
        server = FreeMovementServer()
        facade, gateway_port, control_port = server.start()
        try:
            assert _status(server) is ServerStatus.CREATED

            run_thread = threading.Thread(target=facade.run, daemon=True)
            run_thread.start()

            deadline = time.monotonic() + 5.0
            while _status(server) is not ServerStatus.RUNNING and time.monotonic() < deadline:
                time.sleep(0.01)
            assert _status(server) is ServerStatus.RUNNING

            # The gateway listener accepts one TCP connection. Opening a
            # socket proves the listener is live; the skeleton does not
            # require a full wire handshake here.
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5.0)
            sock.connect(("127.0.0.1", gateway_port))
            assert sock.getpeername()[1] == gateway_port
            sock.close()

            base = f"http://127.0.0.1:{control_port}"

            # Spawn one actor at the origin.
            spawn = _post_json(f"{base}/spawn", {})
            actor_id = int(spawn["actor_id"])
            assert actor_id == 1

            # Query the spawned actor: it is at the origin (0, 0).
            state = _get_json(f"{base}/query_state?actor_id={actor_id}")
            assert int(state["actor_id"]) == actor_id
            assert int(state["pos_x"]) == 0
            assert int(state["pos_y"]) == 0

            # Move the actor via the control plane. The sim steps once and
            # the actor's position changes from the origin.
            move = _post_json(f"{base}/move", {"actor_id": actor_id, "move_x": 32767, "move_y": 0})
            moved_x = int(move["pos_x"])
            assert moved_x > 0

            # Query again: the position persisted in the actor table. The
            # move intent persists in free2d's pending map until replaced, so
            # the per-tick step keeps integrating it between the two calls
            # and the exact pre-tick position is not stable; monotonic
            # advance under a positive velocity is the pin.
            after = _get_json(f"{base}/query_state?actor_id={actor_id}")
            assert int(after["pos_x"]) >= moved_x
            assert int(after["input_tick"]) == 1

            # Replace the move intent with a zero-component input before the
            # teleport: a persisted intent drifts the actor off the
            # teleported position on the next tick's step. One step halts the
            # actor — free2d's decel (120) exceeds the largest velocity delta
            # an input can request (run speed 64) — so the position is stable
            # across ticks from here, and a model tuning that breaks that
            # relation breaks this pin loudly.
            _post_json(f"{base}/move", {"actor_id": actor_id, "move_x": 0, "move_y": 0})

            # Teleport the actor to an absolute position.
            teleported = _post_json(
                f"{base}/teleport", {"actor_id": actor_id, "pos_x": 1 << 16, "pos_y": 2 << 16}
            )
            assert int(teleported["pos_x"]) == 1 << 16
            assert int(teleported["pos_y"]) == 2 << 16

            # Query all actors: the list reflects the teleported state, still
            # exact because no move intent persists.
            all_states = _get_json(f"{base}/query_state")
            actors = all_states["actors"]
            assert isinstance(actors, list)
            assert len(actors) == 1
            assert int(actors[0]["pos_x"]) == 1 << 16

            facade.shutdown()
            run_thread.join(timeout=5.0)
            assert _status(server) is ServerStatus.STOPPED
        finally:
            server.stop()

    # The per-tick choreography test: a persisted move intent integrates on
    # every tick, and the dirty-cell flush bumps the cell's fabric product
    # once per tick, so the replication stream carries fresh state. A frozen
    # product (a flush that stops bumping) or a frozen position (a step that
    # stops integrating) fails this test.
    def test_tick_choreography_refreshes_cell_product(self, bridge: Bridge) -> None:
        server = FreeMovementServer()
        facade, _gateway_port, control_port = server.start()
        try:
            assert _status(server) is ServerStatus.CREATED

            run_thread = threading.Thread(target=facade.run, daemon=True)
            run_thread.start()

            deadline = time.monotonic() + 5.0
            while _status(server) is not ServerStatus.RUNNING and time.monotonic() < deadline:
                time.sleep(0.01)
            assert _status(server) is ServerStatus.RUNNING

            base = f"http://127.0.0.1:{control_port}"

            spawn = _post_json(f"{base}/spawn", {})
            actor_id = int(spawn["actor_id"])

            # A move intent persists in free2d's pending map until replaced,
            # so the per-tick step integrates it every tick: the position
            # advances across polls with no further input.
            _post_json(f"{base}/move", {"actor_id": actor_id, "move_x": 32767, "move_y": 0})
            first = int(_get_json(f"{base}/query_state?actor_id={actor_id}")["pos_x"])
            advanced = False
            deadline = time.monotonic() + 2.0
            while time.monotonic() < deadline:
                time.sleep(0.05)
                current = int(_get_json(f"{base}/query_state?actor_id={actor_id}")["pos_x"])
                if current > first:
                    advanced = True
                    break
            assert advanced

            # The flush bumped the cell product on each of those ticks: the
            # fabric's per-cell publish sequence climbs with the bumps, and
            # the product renders the actor the artifact publish placed in
            # the sim store — the cell stream carries fresh state every tick,
            # which is the replication a wire subscriber receives. The
            # example's epoch bookkeeping climbs on the same cadence and
            # never regresses.
            fabric_handle = facade._borrowed_fabric()
            zone_id = server._zone_id
            before = _cell_product(bridge, fabric_handle, zone_id)
            assert before is not None
            assert before.actor_count == 1
            deadline = time.monotonic() + 2.0
            bumped = False
            while time.monotonic() < deadline:
                time.sleep(0.05)
                product = _cell_product(bridge, fabric_handle, zone_id)
                if product is not None and product.publish_seq > before.publish_seq:
                    bumped = True
                    break
            assert bumped
            assert max(server._cell_epochs.values()) >= 2

            facade.shutdown()
            run_thread.join(timeout=5.0)
            assert _status(server) is ServerStatus.STOPPED
        finally:
            server.stop()

    def test_query_unknown_actor_returns_404(self) -> None:
        server = FreeMovementServer()
        facade, _gateway_port, control_port = server.start()
        try:
            run_thread = threading.Thread(target=facade.run, daemon=True)
            run_thread.start()

            deadline = time.monotonic() + 5.0
            while _status(server) is not ServerStatus.RUNNING and time.monotonic() < deadline:
                time.sleep(0.01)

            base = f"http://127.0.0.1:{control_port}"
            with pytest.raises(error.HTTPError) as exc_info:
                _get_json(f"{base}/query_state?actor_id=999")
            assert exc_info.value.code == 404
            # urllib's HTTPError aliases the HTTP response (and its socket);
            # close it so the connection is released deterministically rather
            # than by GC finalization (which emits a ResourceWarning that the
            # suite's filterwarnings=error policy turns into an error).
            exc_info.value.close()

            facade.shutdown()
            run_thread.join(timeout=5.0)
        finally:
            server.stop()
