"""Walking-skeleton server for the free-movement example.

Boots a Server that registers the built-in ``free2d``
simulation model, an ``actor_input`` wire handler, ``actor_state`` as the
gateway replication type, and four control-plane commands
(``spawn``/``move``/``teleport``/``query_state``). The server boots, accepts
one connection, spawns one actor, and moves it via a control-plane command.

A per-tick handler owns the replication cadence: it steps every actor,
publishes each actor's state as a cell artifact, and bumps one cell product
per dirty cell with a monotonic per-cell authority epoch — the publish chain
wire replication rides. Mutators only mutate the actor table and mark the
cell dirty. A wire session additionally needs the startup contract (bind the
session, seed its subscription window) documented in
docs/guides/getting_started.md, "From input to delivery" — a contract this
login-less skeleton leaves to the caller.

This is the thinnest end-to-end slice of the framework: one model, one
replication type, one wire input, four control routes, no persistence, no
chat, no login. It is a shipped example, not throwaway scaffolding.
"""

from __future__ import annotations

import contextlib
import json
import threading
from dataclasses import asdict, dataclass
from typing import Any

from examples._common.query_state import paginate_query_state

from kith import (
    Actor,
    ArtifactKey,
    CellKey,
    KithStateError,
    Server,
    SimInput,
    SimModel,
    SimModelConfig,
)
from kith.control import Request, Response


# Wire message type ids live at or above KITH_PROTO_TYPE_USER_BASE (1000).
# Both peers register the same name at the same id before exchanging frames.
ACTOR_INPUT_TYPE: int = 1000
ACTOR_STATE_TYPE: int = 1001

# The single authoritative zone for this example. Zones are an implicit
# uint32 namespace; the facade reserves a monotonic id per name.
ZONE_NAME: str = "world"

# A cell is a coarse spatial bucket the fabric indexes; the free-movement
# model uses a single cell at the origin so every actor lives in it.
_CELL_X: int = 0
_CELL_Y: int = 0
_CELL_Z: int = 0
_CELL_LOD: int = 0

# actor_input wire payload: 8-byte actor_id + 4-byte input_tick + three
# 2-byte signed move components + 1-byte flags = 19 bytes. The handler
# decodes this into a SimInput applied to the bound actor.
_INPUT_PAYLOAD_LEN: int = 19

# The per-tick step's integration interval. The facade runs at tick_hz=20,
# so one tick is 50 ms; the two constants stay coupled.
_TICK_DT_MS: int = 50


@dataclass(frozen=True)
class ActorState:
    """One actor's published state, exposed to the control plane.

    Positions are Q16.16 fixed-point (the sim's native representation);
    the control plane returns them as integers so a client that wants to
    render in world units divides by 2**16.
    """

    actor_id: int
    pos_x: int
    pos_y: int
    pos_z: int
    vel_x: int
    vel_y: int
    vel_z: int
    input_tick: int


class FreeMovementServer:
    """The walking-skeleton server.

    Owns the Server facade, one ``free2d`` model instance, and
    the actor table the control-plane commands read and mutate. The wire
    handler translates a decoded ``actor_input`` frame into a sim
    ``apply_input`` call; the per-tick handler steps the model and drives
    the publish chain; the control routes mutate the actor table and the
    sim and return JSON. All shared game state is guarded by one lock,
    which every worker-dispatched callback (wire, tick, routes) contends.
    """

    __slots__ = (
        "_actors",
        "_cell_epochs",
        "_dirty_cells",
        "_lock",
        "_model",
        "_server",
        "_zone_id",
    )

    def __init__(self) -> None:
        self._actors: dict[int, Actor] = {}
        self._lock = threading.Lock()
        self._model: SimModel | None = None
        self._server: Server | None = None
        self._zone_id: int = 0
        # The per-cell authority epoch the flush claims on each bump; a
        # stale epoch raises, so the table is the monotonicity contract.
        self._cell_epochs: dict[CellKey, int] = {}
        # Cells mutated since the last flush; one bump per dirty cell per
        # tick coalesces the per-mutation cadence.
        self._dirty_cells: set[CellKey] = set()

    # -----------------------------------------------------------------------
    # lifecycle
    # -----------------------------------------------------------------------

    def start(self) -> tuple[Server, int, int]:
        """Build the server, register the surface, and return it ready to run.

        Returns the facade, the gateway port, and the control-plane port so
        a caller can connect a client and issue control commands. The caller
        drives run on its own thread and calls stop
        to shut down.
        """
        server = Server(
            topology="embedded",
            listen_port=0,
            tick_hz=20,
            handler_table_size=4096,
            replication_type_id=ACTOR_STATE_TYPE,
        )
        self._server = server

        # Register the wire types on the borrowed proto before run so the
        # gateway decoder accepts frames of these types during the run loop.
        server.register_proto_type("actor_input", ACTOR_INPUT_TYPE)
        server.register_proto_type("actor_state", ACTOR_STATE_TYPE)

        # Reserve the single zone id and instantiate the free2d model.
        self._zone_id = server.register_zone(ZONE_NAME)
        self._model = server.register_sim_model("free2d", SimModelConfig())

        server.register_message_handler(ACTOR_INPUT_TYPE, self._on_actor_input)

        # The per-tick handler owns the replication cadence: step, artifact
        # publishes, and one cell-product bump per dirty cell. Registered
        # before run, as every registration on the facade requires.
        server.register_tick_handler(self._on_tick)

        # The control-plane routes are the harness surface: spawn, move,
        # teleport, and query_state. Each mutates the actor table and the sim
        # and returns a JSON response.
        server.register_control_route("POST", "/spawn", self._route_spawn)
        server.register_control_route("POST", "/move", self._route_move)
        server.register_control_route("POST", "/teleport", self._route_teleport)
        server.register_control_route("GET", "/query_state", self._route_query_state)

        return server, server.listen_port, server.control_port

    def stop(self) -> None:
        """Release the server facade and the actor table."""
        if self._server is not None:
            # Request the run loop to drain before releasing the facade, so a
            # caller that never called shutdown() (or a test that raised mid-
            # body) does not destroy the server out from under a reactor thread
            # still inside kith_server_run. shutdown() is idempotent and safe
            # from any thread; close() then frees the now-quiescent facade.
            self._server.shutdown()
            self._server.close()
            self._server = None
        with self._lock:
            self._actors.clear()
        self._model = None

    # -----------------------------------------------------------------------
    # wire handler
    # -----------------------------------------------------------------------

    def _on_actor_input(self, msg_type: int, payload: bytes, session: Any) -> None:
        """Translate an actor_input frame into a sim apply_input call.

        The payload is a thin wrapper over SimInput: a 19-byte
        body (8-byte actor_id, 4-byte input_tick, three 2-byte signed move
        components, 1-byte flags). The handler reads the actor id from the
        payload, applies the input, stores the updated actor, and marks the
        cell dirty; the per-tick step integrates the pending input and the
        flush bumps the cell, so the fresh state replicates on the next
        tick's refresh.
        """
        del msg_type, session  # unused: the bound actor id is in the payload
        if self._model is None or len(payload) < _INPUT_PAYLOAD_LEN:
            return
        actor_id = int.from_bytes(payload[0:8], "little", signed=False)
        input_tick = int.from_bytes(payload[8:12], "little", signed=False)
        move_x = int.from_bytes(payload[12:14], "little", signed=True)
        move_y = int.from_bytes(payload[14:16], "little", signed=True)
        move_z = int.from_bytes(payload[16:18], "little", signed=True)
        flags = payload[18]
        with self._lock:
            actor = self._actors.get(actor_id)
            if actor is None:
                return
            updated = self._model.apply_input(
                actor,
                SimInput(
                    input_tick=input_tick,
                    move_x=move_x,
                    move_y=move_y,
                    move_z=move_z,
                    flags=flags,
                ),
            )
            self._actors[actor_id] = updated
            self._mark_dirty()

    # -----------------------------------------------------------------------
    # per-tick replication
    # -----------------------------------------------------------------------

    def _on_tick(self, tick: int) -> None:
        """Step every actor, publish their state, and bump dirty cells.

        This is the per-tick choreography wire replication rides: the step
        integrates each actor's pending input (free2d's pending map persists
        an input until replaced, so a wire-driven actor keeps moving), the
        artifact publishes refresh the sim store's cell contents, and one
        ``publish_cell_product`` per dirty cell marks the cell pending in
        every subscription tracking it — the next gateway cache refresh
        re-snapshots the cell from the store and the composer delivers the
        fresh state. The publishes are observed by the next tick's refresh:
        a one-tick pipeline. Control routes needing a synchronous response
        step inline; this step re-integrates the persisted input, which is
        the intended cadence under free2d's non-consuming pending map.
        """
        del tick  # the flush is cadence-anchored; the value is not read
        server = self._server
        model = self._model
        if server is None or model is None:
            return
        with self._lock:
            stepped = model.step(list(self._actors.values()), dt_ms=_TICK_DT_MS)
            if stepped:
                key = self._artifact_key()
                for actor in stepped:
                    self._actors[actor.id] = actor
                    server.publish_artifact(key, actor)
                self._dirty_cells.add(self._fabric_key())
            dirty = set(self._dirty_cells)
            self._dirty_cells.clear()
        for cell in dirty:
            with self._lock:
                epoch = self._cell_epochs.get(cell, 0) + 1
                self._cell_epochs[cell] = epoch
            # A stale epoch (two flushes of one cell out of order on
            # concurrent workers) is safe to drop: the higher-epoch publish
            # already marked the cell pending and the next refresh still
            # pulls the store's latest state.
            with contextlib.suppress(KithStateError):
                server.publish_cell_product(cell, epoch)

    # -----------------------------------------------------------------------
    # control-plane routes
    # -----------------------------------------------------------------------

    def _route_spawn(self, req: Request, resp: Response) -> None:
        """Spawn an actor at the origin; replication follows on the next tick."""
        del req
        with self._lock:
            actor_id = (max(self._actors) + 1) if self._actors else 1
            actor = Actor(id=actor_id, pos_x=0, pos_y=0, pos_z=0)
            assert self._model is not None
            self._actors[actor_id] = actor
            self._mark_dirty()
        self._write_json(resp, 200, {"actor_id": actor_id})

    def _route_move(self, req: Request, resp: Response) -> None:
        """Apply a movement input to an actor and step the sim once."""
        body = json.loads(req.body.decode("utf-8")) if req.body else {}
        actor_id = int(body["actor_id"])
        move_x = int(body.get("move_x", 0))
        move_y = int(body.get("move_y", 0))
        with self._lock:
            actor = self._actors.get(actor_id)
            if actor is None:
                self._write_json(resp, 404, {"error": "actor not found"})
                return
            assert self._model is not None
            tick = actor.input_tick + 1
            updated = self._model.apply_input(
                actor,
                SimInput(input_tick=tick, move_x=move_x, move_y=move_y),
            )
            stepped = self._model.step([updated], dt_ms=_TICK_DT_MS)[0]
            self._actors[actor_id] = stepped
            self._mark_dirty()
        self._write_json(resp, 200, self._state_dict(stepped))

    def _route_teleport(self, req: Request, resp: Response) -> None:
        """Teleport an actor to an absolute position."""
        body = json.loads(req.body.decode("utf-8")) if req.body else {}
        actor_id = int(body["actor_id"])
        pos_x = int(body.get("pos_x", 0))
        pos_y = int(body.get("pos_y", 0))
        with self._lock:
            actor = self._actors.get(actor_id)
            if actor is None:
                self._write_json(resp, 404, {"error": "actor not found"})
                return
            teleported = Actor(
                id=actor.id,
                pos_x=pos_x,
                pos_y=pos_y,
                pos_z=actor.pos_z,
                vel_x=0,
                vel_y=0,
                vel_z=0,
                input_tick=actor.input_tick,
                flags=actor.flags,
                # A teleport is not a movement apply: the counter certifies
                # movement coverage, so it carries unchanged.
                update_seq=actor.update_seq,
            )
            self._actors[actor_id] = teleported
            self._mark_dirty()
        self._write_json(resp, 200, self._state_dict(teleported))

    def _route_query_state(self, req: Request, resp: Response) -> None:
        """Return the live state of one actor (or all actors)."""
        actor_id_str = req.path.rpartition("=")[2] if "actor_id=" in req.path else ""
        with self._lock:
            if actor_id_str:
                actor = self._actors.get(int(actor_id_str))
                if actor is None:
                    self._write_json(resp, 404, {"error": "actor not found"})
                    return
                self._write_json(resp, 200, self._state_dict(actor))
                return
            states = [self._state_dict(a) for a in self._actors.values()]
        self._write_json(resp, 200, paginate_query_state(req.path, states))

    # -----------------------------------------------------------------------
    # helpers
    # -----------------------------------------------------------------------

    def _mark_dirty(self) -> None:
        """Mark the example's single cell for the next flush's product bump.

        Called with the actor-table lock held: every mutator and the tick's
        publish loop already hold it, and the mark must be atomic with the
        mutation it follows. The tick's flush drains the set, so a cell
        dirtied repeatedly between ticks coalesces to one bump.
        """
        self._dirty_cells.add(self._fabric_key())

    def _artifact_key(self) -> ArtifactKey:
        """The sim artifact locator for the example's single origin cell."""
        return ArtifactKey(
            zone=self._zone_id,
            cell_x=_CELL_X,
            cell_y=_CELL_Y,
            cell_z=_CELL_Z,
            lod=_CELL_LOD,
        )

    def _fabric_key(self) -> CellKey:
        """The fabric cell locator matching ``_artifact_key``'s cell.

        The sim store and the fabric stream key cells by the same five
        fields; the two locator types exist because the planes publish
        different records (actor artifacts vs cell product headers).
        """
        return CellKey(
            zone=self._zone_id,
            cell_x=_CELL_X,
            cell_y=_CELL_Y,
            cell_z=_CELL_Z,
            lod=_CELL_LOD,
        )

    @staticmethod
    def _state_dict(actor: Actor) -> dict[str, int]:
        return asdict(
            ActorState(
                actor_id=actor.id,
                pos_x=actor.pos_x,
                pos_y=actor.pos_y,
                pos_z=actor.pos_z,
                vel_x=actor.vel_x,
                vel_y=actor.vel_y,
                vel_z=actor.vel_z,
                input_tick=actor.input_tick,
            )
        )

    @staticmethod
    def _write_json(resp: Response, status: int, body: dict[str, Any]) -> None:
        resp.status(status, "application/json")
        resp.body(json.dumps(body).encode("utf-8"))


def main() -> None:
    """Boot the server and block until interrupted.

    A real client connects to the gateway port (the wire protocol) and a
    harness drives the control plane (spawn/move/teleport/query_state). The
    integration test drives the same surface programmatically.
    """
    server = FreeMovementServer()
    facade, gateway_port, control_port = server.start()
    print(f"free_movement: gateway={gateway_port} control={control_port}", flush=True)
    try:
        facade.serve()
    finally:
        server.stop()


if __name__ == "__main__":
    main()
