"""Embedded-topology server: the spatial game library on a single process.

The spatial game library — the wire catalog, the login / actor_input
/ chat handlers, the tile2d model, the TMX-loaded behavior grid — wired onto
the embedded topology. The composition root runs every plane in one process:
the coordinator owns every cell, the fabric uses in-memory storage, and no
coordination bus, no Postgres pool, and no second instance are configured.
The same game logic the distributed wiring would run on a cluster runs here
on one process, with no game-code change.

This is the "no cluster required" story. The example proves topology is
a configuration knob of the composition root, not a property of the game:
the embedded topology and the distributed topology present the same plane
contracts to game code, so a game developed and tested on one process ships
to a cluster by changing the topology the facade is constructed with, not by
forking the wiring. A flag on the server would not prove this — a
flag is a branch, not a second wiring. This example is a genuinely stripped
wiring (no coord bus, no Postgres pool) reusing the spatial library
and the tile2d extension, so the proof is that the same modules run in both
wirings.

The actor table the wire handlers mutate is in-memory and scoped to the
process — that is the in-memory store story on the embedded topology: one
process means process-local state is the whole state. The framework's
generic GameStateStore interface backs the
same handler set on the distributed topology with a Postgres pool, so a game
that needs cross-restart durability wires a login-save handler on the same
typed view; the embedded wiring does not, because one process has no restart
to survive. The handler set and the control routes share one actor table
either way.

The control-plane routes are the harness surface: the agentic scenarios and
the integration test drive login / move / teleport / query_state over HTTP,
delegating to the spatial handlers' public mutation methods so the wire and
the harness share one actor table.

Passing ``--record PATH`` turns on binary replay recording
(docs/guides/replay_format.md): spawns and applied movements are buffered by
a recorder and drained at each tick boundary into a versioned artifact,
optionally with per-tick generator checkpoints when ``--record-seed`` names
a root seed. The recorder drains in the single tick-callback slot, composed
with the handlers' dirty-cell flush. Teleports have no v1 event encoding:
a recorded session containing one diverges permanently from the teleport
tick onward when replayed.
"""

from __future__ import annotations

import argparse
import ctypes
import json
import os
import struct
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, BinaryIO

from examples._common.query_state import paginate_query_state
from examples._common.replay_recorder import ReplayRecorder
from examples.spatial import handlers as spatial_handlers
from examples.spatial import messages
from examples.spatial.handlers import SpatialHandlers
from examples.tile_rpg import physics

from kith import Actor, Server, SimInput, SimModel
from kith._bridge import load as load_bridge
from kith._generated import configure as configure_all
from kith._generated import util as gen_util
from kith._generated import version as gen_version
from kith.control import Request, Response
from kith.exceptions import check_error
from kith.gateway import tiered_delivery_config


# The single authoritative zone for this example. The facade reserves a
# monotonic id per name; the embedded topology has one instance, so one zone
# is the whole world.
_ZONE_NAME: str = "world"

# The TMX map that defines the tile2d behavior grid: an 8x8 grid with a solid
# border and a walkable interior. The path is resolved relative to this
# module so the example boots regardless of the caller's working directory.
_WORLD_TMX: Path = Path(__file__).resolve().parent / "world.tmx"

# Tick rate of the embedded wiring: passed to the facade and recorded in a
# replay artifact's header.
_TICK_HZ: int = 20

# Sim model the tile_rpg physics wiring instantiates; recorded in a replay
# artifact's META record so replay tooling picks the matching model.
_MODEL_NAME: str = "tile2d"


@dataclass(frozen=True)
class ActorState:
    """One actor's published state, exposed to the control plane.

    Positions are Q16.16 fixed-point (the sim's native representation); the
    control plane returns them as integers so a client that wants to render
    in world units divides by 2**16.
    """

    actor_id: int
    pos_x: int
    pos_y: int
    pos_z: int
    vel_x: int
    vel_y: int
    vel_z: int
    input_tick: int


class _KithRngProvider:
    """Generator handle backing the recorder's rng-state provider.

    Builds a ``kith_rng`` through the generated ctypes bindings (the util
    library must be present in the loaded bridge) and serializes each
    snapshot the way the replay format defines it: big-endian derivation
    seed followed by the four algorithm words, 40 bytes total. Owns the
    handle and destroys it on close.
    """

    __slots__ = ("_handle", "_lib")

    def __init__(self, lib: ctypes.CDLL, *, seed: int) -> None:
        params = gen_util.kith_rng_params_t()
        params.size = ctypes.sizeof(gen_util.kith_rng_params_t)
        params.abi_version = gen_version.KITH_ABI_VERSION
        params.seed = seed
        handle = ctypes.POINTER(gen_util.kith_rng_t)()
        check_error(
            int(lib.kith_rng_create(ctypes.byref(params), None, ctypes.byref(handle))),
            "kith_rng_create",
        )
        self._lib = lib
        self._handle: object | None = handle

    def state(self) -> bytes:
        """Return one 40-byte big-endian generator snapshot."""
        if self._handle is None:
            raise ValueError("rng provider is closed")
        snapshot = gen_util.kith_rng_state_t()
        check_error(
            int(self._lib.kith_rng_state_save(self._handle, ctypes.byref(snapshot))),
            "kith_rng_state_save",
        )
        return struct.pack(">5Q", snapshot.derivation_seed, *snapshot.words)

    def close(self) -> None:
        """Destroy the generator handle; idempotent."""
        if self._handle is not None:
            self._lib.kith_rng_destroy(self._handle)
            self._handle = None


class EmbeddedServer:
    """The spatial game library on the embedded topology.

    Owns the Server facade, the tile2d model, and the spatial
    handler set. The wire handlers and the control-plane routes drive the
    same handler set so the wire and the harness mutate one actor table. No
    coordination bus, no Postgres pool: the composition root wires every
    plane in one process and the handler set's in-memory actor table is the
    whole game state.
    """

    __slots__ = (
        "_delivery_strategy",
        "_delivery_wait_budget_us",
        "_delivery_workers",
        "_handlers",
        "_model",
        "_native_apply",
        "_python_workers",
        "_record_path",
        "_record_seed",
        "_record_stream",
        "_recorder",
        "_replication_batch_type_id",
        "_rng",
        "_server",
        "_tiered_max_gap_ms",
        "_zone_id",
    )

    def __init__(
        self,
        *,
        replication_batch_type_id: int = 0,
        python_workers: int = 0,
        delivery_strategy: str | None = None,
        delivery_workers: int = 0,
        delivery_wait_budget_us: int = 0,
        tiered_max_gap_ms: int | None = None,
        record_path: Path | None = None,
        record_seed: int | None = None,
        native_apply: bool = False,
    ) -> None:
        if record_seed is not None and record_path is None:
            raise ValueError("record_seed requires record_path")
        self._server: Server | None = None
        self._model: object | None = None
        self._handlers: SpatialHandlers | None = None
        self._zone_id: int = 0
        # When non-zero, the gateway packs the full view set into one
        # multi-subject frame per refresh instead of one frame per subject.
        self._replication_batch_type_id: int = replication_batch_type_id
        # Registered delivery strategy name; None keeps the factory default.
        self._delivery_strategy: str | None = delivery_strategy
        # Tiered backstop override (maximum silence before an unconditional
        # refresh); None keeps the documented preset cadence.
        self._tiered_max_gap_ms: int | None = tiered_max_gap_ms
        # Handler worker pool size; 0 defers to the server default (1 under
        # the GIL). A scaling profile raises it so dispatch parallelizes.
        self._python_workers: int = python_workers
        # Delivery executor thread count; 0 keeps gateway delivery inline on
        # the reactor thread. A non-zero count moves the tick's deliver pass
        # onto executor threads, bounded by the per-pass
        # compose-wait budget alongside.
        self._delivery_workers: int = delivery_workers
        self._delivery_wait_budget_us: int = delivery_wait_budget_us
        # Run the movement-apply path in the C core through the
        # pool-dispatched native handler. Default off — the
        # Python path is byte-identical to its unwired behavior.
        self._native_apply: bool = native_apply
        # Replay recording (docs/guides/replay_format.md): the artifact path
        # and the optional root seed for per-tick generator checkpoints.
        self._record_path: Path | None = record_path
        self._record_seed: int | None = record_seed
        self._recorder: ReplayRecorder | None = None
        self._record_stream: BinaryIO | None = None
        self._rng: _KithRngProvider | None = None

    # -----------------------------------------------------------------------
    # in-process surfaces
    # -----------------------------------------------------------------------

    @property
    def handlers(self) -> SpatialHandlers:
        """The live handler set the wire and control-plane routes delegate to.

        Raises:
            RuntimeError: If the server has not been started.
        """
        handlers = self._handlers
        if handlers is None:
            msg = "server not started"
            raise RuntimeError(msg)
        return handlers

    @property
    def model(self) -> SimModel:
        """The sim model instance the handler set applies inputs through.

        Raises:
            RuntimeError: If the server has not been started.
        """
        model = self._model
        if not isinstance(model, SimModel):
            msg = "server not started"
            raise RuntimeError(msg)
        return model

    @property
    def zone_id(self) -> int:
        """The zone id every actor publishes into.

        Raises:
            RuntimeError: If the server has not been started.
        """
        if self._server is None:
            msg = "server not started"
            raise RuntimeError(msg)
        return self._zone_id

    # -----------------------------------------------------------------------
    # lifecycle
    # -----------------------------------------------------------------------

    def start(self) -> tuple[Server, int, int]:
        """Build the server, register the surface, and return it ready to run.

        Returns the facade, the gateway port, and the control-plane port so a
        caller can connect a wire client and issue control commands. A
        standalone process calls serve on the main thread to
        block until SIGINT/SIGTERM; a programmatic harness drives
        run on its own thread. Either way stop shuts
        down.
        """
        # topology="embedded" is the whole point of this example: the
        # composition root wires the coordinator with no bus and the fabric
        # with in-memory storage, so every plane runs in one process. A game
        # switching to the distributed topology wires the same game code onto
        # the distributed topology and supplies the shared coordination bus
        # and Postgres pool as explicit handles; the game code is unchanged.
        server = Server(
            topology="embedded",
            listen_port=0,
            tick_hz=_TICK_HZ,
            python_workers=self._python_workers,
            handler_table_size=4096,
            replication_type_id=messages.ACTOR_STATE_TYPE,
            replication_batch_type_id=self._replication_batch_type_id,
            delivery_strategy=self._delivery_strategy,
            delivery_workers=self._delivery_workers,
            delivery_wait_budget_us=self._delivery_wait_budget_us,
            # The cadence image is attached for the built-in preset only; a
            # game-registered strategy carries its own config schema and its
            # name passes through config-less. An explicit backstop override
            # rides the image so a run records the tuning under test.
            delivery_config=(
                self._tiered_config() if self._delivery_strategy == "tiered" else None
            ),
        )
        self._server = server

        # Wire types register on the borrowed proto before run so the gateway
        # decoder accepts frames of every game type during the run loop.
        messages.register(server)

        # Reserve the zone and instantiate the tile2d model with the embedded
        # world's behavior grid. The physics helper loads the TMX, converts it
        # to a text grid, and hands it to the model's load_behavior.
        self._zone_id, self._model = physics.register(
            server,
            zone_name=_ZONE_NAME,
            tmx_path=_WORLD_TMX,
        )

        # Replay recording: the recorder buffers events fed by the handler
        # hooks and drains them at tick boundaries; the optional generator
        # handle supplies per-tick checkpoints. The bridge is already loaded
        # and configured (the facade above loaded it), so the util library is
        # reachable for the generator handle.
        if self._record_path is not None:
            bridge = load_bridge()
            configure_all(bridge)
            if self._record_seed is not None:
                self._rng = _KithRngProvider(bridge.lib("util"), seed=self._record_seed)
            stream = self._record_path.open("wb")
            self._record_stream = stream
            try:
                self._recorder = ReplayRecorder(
                    stream,
                    tick_hz=_TICK_HZ,
                    rng_state_provider=self._rng.state if self._rng is not None else None,
                )
                if self._record_seed is not None:
                    self._recorder.write_meta({"model": _MODEL_NAME, "seed": self._record_seed})
            except BaseException:
                self._record_stream = None
                stream.close()
                raise

        # The handler set owns the actor table, the principal map, and the
        # cell locator; the wire handlers and the control-plane routes below
        # both drive it. The in-memory actor table is process-scoped state,
        # which on the embedded topology is the whole game state.
        self._handlers = spatial_handlers.register(
            server,
            model=self._model,
            zone_id=self._zone_id,
            recorder=self._recorder,
            native_apply=self._native_apply,
        )

        # The spatial handlers defer cell-product bumps to a per-tick
        # dirty-cell set; register the flush as the server's tick callback so
        # every cell dirtied since the last flush is bumped once per tick
        # (coalescing the per-input cadence to a per-tick cadence). The
        # composition root owns this slot, so when recording is enabled the
        # callback composes the flush with the recorder's boundary drain —
        # the flush first, then the tick's replay record.
        if self._recorder is not None:
            recorder = self._recorder

            def _recorded_tick(tick: int) -> None:
                assert self._handlers is not None
                self._handlers.flush_dirty_cells(tick)
                recorder.flush_tick(tick)

            server.register_tick_handler(_recorded_tick)
        else:
            server.register_tick_handler(self._handlers.flush_dirty_cells)

        # The control-plane routes are the harness surface. Each delegates to
        # the handler set's public mutation methods so the harness and the
        # wire share one actor table.
        server.register_control_route("POST", "/login", self._route_login)
        server.register_control_route("POST", "/move", self._route_move)
        server.register_control_route("POST", "/teleport", self._route_teleport)
        server.register_control_route("GET", "/query_state", self._route_query_state)
        server.register_control_route("GET", "/bindings", self._route_bindings)

        return server, server.listen_port, server.control_port

    def _tiered_config(self) -> bytes:
        """Return the tiered cadence image with any backstop override pinned.

        The image pins the documented defaults explicitly so a run records
        the tuning under test; a configured ``tiered_max_gap_ms`` replaces
        the maximum-silence backstop value (0 requests the documented
        default through the config's own default selector).
        """
        if self._tiered_max_gap_ms is None:
            return tiered_delivery_config()
        return tiered_delivery_config(max_gap_ms=self._tiered_max_gap_ms)

    def stop(self) -> None:
        """Release the server facade, the handler set (destroying the native
        movement core), and the model.

        Ends an active recording first: the recorder drops its unflushed
        tail (events buffered for a tick that never completed), then the
        artifact stream and the generator handle — both owned by this
        composition root — are released.
        """
        if self._server is not None:
            self._server.shutdown()
            self._server.close()
            self._server = None
        if self._recorder is not None:
            self._recorder.close()
            self._recorder = None
        if self._record_stream is not None:
            self._record_stream.close()
            self._record_stream = None
        if self._rng is not None:
            self._rng.close()
            self._rng = None
        if self._handlers is not None:
            self._handlers.close()
            self._handlers = None
        self._model = None

    # -----------------------------------------------------------------------
    # control-plane routes
    # -----------------------------------------------------------------------

    def _route_login(self, req: Request, resp: Response) -> None:
        body = _read_json(req)
        principal_id = int(body["principal_id"])
        assert self._handlers is not None
        actor = self._handlers.spawn_actor(principal_id)
        self._write_json(resp, 200, _actor_dict(actor))

    def _route_move(self, req: Request, resp: Response) -> None:
        body = _read_json(req)
        actor_id = int(body["actor_id"])
        move_x = int(body.get("move_x", 0))
        move_y = int(body.get("move_y", 0))
        assert self._handlers is not None
        actor = self._handlers.actor_state(actor_id)
        if actor is None:
            self._write_json(resp, 404, {"error": "actor not found"})
            return
        updated = self._handlers.apply_movement(
            actor_id,
            SimInput(input_tick=actor.input_tick + 1, move_x=move_x, move_y=move_y),
        )
        assert updated is not None
        self._write_json(resp, 200, _actor_dict(updated))

    def _route_teleport(self, req: Request, resp: Response) -> None:
        body = _read_json(req)
        actor_id = int(body["actor_id"])
        pos_x = int(body.get("pos_x", 0))
        pos_y = int(body.get("pos_y", 0))
        assert self._handlers is not None
        updated = self._handlers.teleport_actor(actor_id, pos_x, pos_y)
        if updated is None:
            self._write_json(resp, 404, {"error": "actor not found"})
            return
        self._write_json(resp, 200, _actor_dict(updated))

    def _route_query_state(self, req: Request, resp: Response) -> None:
        actor_id_str = req.path.rpartition("=")[2] if "actor_id=" in req.path else ""
        assert self._handlers is not None
        if actor_id_str:
            actor = self._handlers.actor_state(int(actor_id_str))
            if actor is None:
                self._write_json(resp, 404, {"error": "actor not found"})
                return
            self._write_json(resp, 200, _actor_dict(actor))
            return
        actors = [_actor_dict(a) for a in self._handlers.actor_states()]
        self._write_json(resp, 200, paginate_query_state(req.path, actors))

    def _route_bindings(self, req: Request, resp: Response) -> None:
        """Serve the principal→actor binding map plus ownership counters.

        The bindings are the server's allocation truth — the same pairs the
        login path created under the handler lock — so a load driver that
        must know which actor a principal owns reads them here instead of
        inferring from arrival order. The bind-conflict and identity-gate
        counters ride every response as envelope fields.
        """
        assert self._handlers is not None
        bindings, conflicts, drops = self._handlers.binding_stats()
        body = paginate_query_state(
            req.path,
            [
                {"principal_id": principal_id, "actor_id": actor_id}
                for principal_id, actor_id in bindings
            ],
            key="bindings",
            extra={"bind_conflicts": conflicts, "identity_gate_drops": drops},
        )
        self._write_json(resp, 200, body)

    # -----------------------------------------------------------------------
    # helpers
    # -----------------------------------------------------------------------

    @staticmethod
    def _write_json(resp: Response, status: int, body: dict[str, Any]) -> None:
        resp.status(status, "application/json")
        resp.body(json.dumps(body).encode("utf-8"))


def _read_json(req: Request) -> dict[str, Any]:
    return json.loads(req.body.decode("utf-8")) if req.body else {}


def _actor_dict(actor: Actor) -> dict[str, Any]:
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


def _python_workers_default(environ: Mapping[str, str]) -> int:
    """Resolve the worker-pool fallback from ``KITH_PYTHON_WORKERS``.

    Returns 0 — defer to the server default — when the variable is unset or
    empty. A non-integer or negative value exits with a usage error.
    """
    return _non_negative_env_default(environ, "KITH_PYTHON_WORKERS")


def _delivery_workers_default(environ: Mapping[str, str]) -> int:
    """Resolve the delivery-executor fallback from ``KITH_DELIVERY_WORKERS``.

    Returns 0 — inline delivery on the reactor thread — when the variable is
    unset or empty. A non-integer or negative value exits with a usage error.
    """
    return _non_negative_env_default(environ, "KITH_DELIVERY_WORKERS")


def _non_negative_env_default(environ: Mapping[str, str], name: str) -> int:
    """Parse an optional non-negative integer environment variable.

    Returns 0 when the variable is unset or empty; a malformed or negative
    value exits with a usage error naming the variable.
    """
    raw = environ.get(name, "").strip()
    if not raw:
        return 0
    try:
        workers = int(raw)
    except ValueError:
        message = f"{name} must be an integer, got {raw!r}"
        raise SystemExit(message) from None
    if workers < 0:
        message = f"{name} must not be negative, got {workers}"
        raise SystemExit(message)
    return workers


def _native_apply_default(environ: Mapping[str, str]) -> bool:
    """Resolve the native-apply knob from ``KITH_NATIVE_APPLY``.

    Returns False when the variable is unset or empty. ``1`` enables the
    native movement path, ``0`` keeps the Python path; any other value
    exits with a usage error naming the variable.
    """
    raw = environ.get("KITH_NATIVE_APPLY", "").strip()
    if not raw:
        return False
    if raw not in ("0", "1"):
        message = f"KITH_NATIVE_APPLY must be 0 or 1, got {raw!r}"
        raise SystemExit(message)
    return raw == "1"


def _build_parser() -> argparse.ArgumentParser:
    """Build the embedded-server argument parser."""
    parser = argparse.ArgumentParser(
        prog="embedded-server",
        description="Run the spatial game on the embedded topology.",
    )
    parser.add_argument(
        "--python-workers",
        type=int,
        default=_python_workers_default(os.environ),
        help=(
            "Handler worker pool size; falls back to KITH_PYTHON_WORKERS "
            "when absent, then to the server default."
        ),
    )
    parser.add_argument(
        "--delivery-workers",
        type=int,
        default=_delivery_workers_default(os.environ),
        help=(
            "Delivery executor thread count; falls back to "
            "KITH_DELIVERY_WORKERS when absent, then to inline delivery on "
            "the reactor thread."
        ),
    )
    parser.add_argument(
        "--replication-batch-type-id",
        type=int,
        default=0,
        help="Multi-subject replication frame type id; 0 keeps per-subject frames.",
    )
    parser.add_argument(
        "--delivery-strategy",
        default=None,
        help=(
            "Registered delivery strategy name for the gateway (for example "
            "'tiered'); omitted keeps the factory default."
        ),
    )
    parser.add_argument(
        "--tiered-max-gap-ms",
        type=int,
        default=None,
        help=(
            "With --delivery-strategy tiered: override the backstop that "
            "refreshes any subject silent this many milliseconds regardless "
            "of change (0 pins the documented default). Omitted keeps the "
            "preset cadence."
        ),
    )
    parser.add_argument(
        "--record",
        dest="record_path",
        type=Path,
        default=None,
        metavar="PATH",
        help=(
            "Record a binary replay artifact (.krpl) of spawns and applied "
            "movements; idle ticks are elided."
        ),
    )
    parser.add_argument(
        "--record-seed",
        type=int,
        default=None,
        metavar="N",
        help=("Root seed for the recorded generator's per-tick checkpoints (requires --record)."),
    )
    parser.add_argument(
        "--native-apply",
        action="store_true",
        default=_native_apply_default(os.environ),
        help=(
            "Run the movement-apply path in the C core through the "
            "pool-dispatched native handler; falls back to "
            "KITH_NATIVE_APPLY when absent, then to the Python path."
        ),
    )
    return parser


def main(argv: Sequence[str] | None = None) -> None:
    """Boot the embedded server and block until interrupted.

    A wire client connects to the gateway port; a harness drives the control
    plane. The integration test drives the same surface programmatically.

    The first stdout line is a stable handshake a multi-process launcher
    parses to recover the gateway and control ports the OS assigned (the
    server binds to ephemeral ports when ``listen_port`` is 0):

        embedded: gateway=<port> control=<port>

    Args:
        argv: Optional CLI argument vector; ``None`` reads ``sys.argv``.

        ``--python-workers`` forwards to the handler worker pool size,
        resolving through a fixed ladder: the command-line flag, else the
        ``KITH_PYTHON_WORKERS`` environment variable, else the server
        default (one worker under the GIL; scaling profiles raise it under
        free-threaded Python). A multi-process scaling gate raises it so
        handler dispatch parallelizes across worker threads under
        free-threaded Python, with the server in its own process so the
        worker pool does not contend with the harness's client-drive
        loop for one GIL.
        ``--delivery-workers`` forwards to the gateway's delivery executor
        thread count, through the same ladder with
        ``KITH_DELIVERY_WORKERS``: 0 (the default) keeps delivery inline on
        the reactor thread; a non-zero count moves the tick's deliver pass
        onto executor threads so the compose budget covers composition
        alone.
        ``--replication-batch-type-id`` forwards to the gateway so it packs
        the full view set into one multi-subject frame per refresh (O(N)
        frame volume, not O(N x K)); a load harness passes a non-zero value.
        ``--delivery-strategy`` selects a registered delivery strategy for
        the gateway; when the built-in ``tiered`` preset is named, its
        documented cadence image is attached explicitly so a scaling run
        records the exact tuning under test. ``--tiered-max-gap-ms``
        replaces the backstop value inside that cadence image (maximum
        silence before any subject is refreshed regardless of change), a
        diagnostic override for attributing periodic behavior to the
        paranoia refresh rather than to another cadence.
        ``--record`` enables binary replay recording to the named artifact;
        ``--record-seed`` additionally records a root-seeded generator's
        state after each recorded tick. A seed without an artifact path is a
        usage error.
    """
    parser = _build_parser()
    args = parser.parse_args(argv)
    if args.record_seed is not None:
        if args.record_path is None:
            parser.error("--record-seed requires --record")
        if not 0 <= args.record_seed < 2**64:
            parser.error(f"--record-seed must be in [0, 2**64), got {args.record_seed}")
    server = EmbeddedServer(
        replication_batch_type_id=args.replication_batch_type_id,
        python_workers=args.python_workers,
        delivery_strategy=args.delivery_strategy,
        delivery_workers=args.delivery_workers,
        tiered_max_gap_ms=args.tiered_max_gap_ms,
        record_path=args.record_path,
        record_seed=args.record_seed,
        native_apply=args.native_apply,
    )
    facade, gateway_port, control_port = server.start()
    print(f"embedded: gateway={gateway_port} control={control_port}", flush=True)
    try:
        facade.serve()
    finally:
        server.stop()


if __name__ == "__main__":
    main()
