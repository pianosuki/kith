"""Open Range server: the composition root for the visual example.

One embedded-topology ``Server``, one ``free2d`` sim model, a 512x512-unit
seamless plain cut into 32-unit cells, a Chebyshev radius-1 subscription
window (3x3 cells) per bound session, and ambient actors that wander the
plain under the same momentum model the players use, so the world is
alive with zero client code. Chat rides the cell-scoped broadcast; a
ping/pong pair gives the C client engine an RTT probe.

Movement follows the publish-choreography law: inbound ``actor_input``
frames only buffer into the model's pending map, and the registered tick
handler advances every actor by the tick interval in one ``SimModel.step``
call, publishes each changed actor as a cell artifact, diffs the
subscription window of any bound session whose actor crossed a cell
boundary, and drains the dirty-cell set with one ``publish_cell_product``
per dirty cell — motion is the tick's product, so movement speed is
independent of both the input send rate and the tick rate.

Session principals spawn around the plain's center — the primary
principal exactly on it, every other principal on a golden-angle ring
just outside the view window, so scenario cohorts arrive from beyond the
horizon. Ambient principals scatter across the plain at boot.

Control routes: ``/playground/metrics`` (the counters a client's metrics
panel shows, including the principal bindings), ``/query_state`` (every
actor's live state, the drivers' steering truth), and ``/teleport``
(diagnostics). Run standalone::

    python -m kith.examples.visual.server

and read the printed ``playground:`` handshake line for the ports.
"""

from __future__ import annotations

import argparse
import json
import os
from typing import Any

from kith import Server, SimModelConfig
from kith.control import Request, Response
from kith.examples.visual import _world
from kith.examples.visual._handlers import PlaygroundHandlers
from kith.examples.visual._protocol import (
    ACTOR_INPUT_TYPE,
    ACTOR_STATE_TYPE,
    CHAT_TYPE,
    LOGIN_TYPE,
    PING_TYPE,
    TYPES,
)
from kith.gateway import tiered_delivery_config


__all__ = ["PlaygroundServer", "main", "parse_args"]


# Movement speeds in the sim model's native unit — world units per 50 ms
# reference tick (the step scales by dt_ms/50, so physical speed is
# tick-rate independent): base 1 = 20 u/s walk, run 2 = 40 u/s, ramping
# one unit per tick both ways.
_MODEL_SPEEDS = {"base_speed": 1, "run_speed": 2, "accel": 1, "decel": 1}


class PlaygroundServer:
    """The composition root: facade construction, registration, routes."""

    def __init__(self, args: argparse.Namespace) -> None:
        self._args = args
        self._server: Server | None = None
        self._handlers: PlaygroundHandlers | None = None

    def start(self) -> Server:
        """Build the facade and register the example surface, ready to run."""
        args = self._args
        delivery_config = (
            tiered_delivery_config(max_gap_ms=args.tiered_max_gap_ms)
            if args.delivery_preset == "tiered"
            else None
        )
        server = Server(
            topology="embedded",
            listen_port=args.port,
            tick_hz=args.tick_hz,
            handler_table_size=4096,
            python_workers=args.python_workers,
            delivery_strategy=args.delivery_preset,
            delivery_config=delivery_config,
            delivery_workers=args.delivery_workers,
            replication_type_id=ACTOR_STATE_TYPE,
            replication_batch_type_id=args.replication_batch_type_id,
            view_max_subjects=args.view_max_subjects,
            view_refresh_interval_ms=args.view_refresh_ms,
            cache_refresh_interval_ms=args.cache_refresh_ms,
        )
        self._server = server
        for name, type_id in TYPES:
            server.register_proto_type(name, type_id)
        zone_id = server.register_zone("openrange")
        model = server.register_sim_model("free2d", SimModelConfig(**_MODEL_SPEEDS))

        dt_ms = max(1, round(1000 / args.tick_hz))
        handlers = PlaygroundHandlers(
            server=server,
            model=model,
            zone_id=zone_id,
            cell_size=_world.CELL_SIZE_Q16,
            cell_radius=_world.CELL_RADIUS,
            world_q16=_world.WORLD_Q16,
            npc_count=args.npcs,
            dt_ms=dt_ms,
            seed=args.seed,
        )
        self._handlers = handlers
        server.register_message_handler(LOGIN_TYPE, handlers.on_login)
        server.register_message_handler(ACTOR_INPUT_TYPE, handlers.on_actor_input)
        server.register_message_handler(CHAT_TYPE, handlers.on_chat)
        server.register_message_handler(PING_TYPE, handlers.on_ping)
        server.on_session_destroyed(handlers.on_session_destroyed)
        server.register_tick_handler(handlers.on_tick)

        server.register_control_route("GET", "/playground/metrics", self._route_metrics)
        server.register_control_route("GET", "/query_state", self._route_query_state)
        server.register_control_route("POST", "/teleport", self._route_teleport)
        return server

    @property
    def handlers(self) -> PlaygroundHandlers:
        """The live handler set (actor table, session map, counters)."""
        assert self._handlers is not None
        return self._handlers

    def stop(self) -> None:
        """Drain and release the facade."""
        if self._server is not None:
            self._server.shutdown()
            self._server.close()
            self._server = None
        self._handlers = None

    # ---------------------------------------------------------------------------
    # control routes
    # ---------------------------------------------------------------------------

    def _route_metrics(self, req: Request, resp: Response) -> None:
        """Scrape the counters a client's metrics panel shows."""
        del req
        self._write_json(resp, 200, self._metrics_body())

    def _metrics_body(self) -> dict[str, Any]:
        """The metrics counters: delivery, bindings, and the world shape."""
        server = self._server
        handlers = self._handlers
        assert server is not None and handlers is not None
        totals = server.delivery_totals()
        bindings, bind_conflicts, gate_drops = handlers.binding_stats()
        return {
            "tick_hz": self._args.tick_hz,
            "npcs": self._args.npcs,
            "ambient_count": len(handlers._npc_ids),
            "delivery_preset": self._args.delivery_preset,
            "sessions": server.session_count,
            "actors": len(bindings),
            "tick": handlers._tick_index,
            "delivery": {
                "enqueued": totals.enqueued,
                "dropped": totals.dropped,
                "events_enqueued": totals.events_enqueued,
                "suppressed": totals.suppressed,
            },
            "bindings": [[principal, actor] for principal, actor in bindings],
            "bind_conflicts": bind_conflicts,
            "gate_drops": gate_drops,
        }

    def _route_query_state(self, req: Request, resp: Response) -> None:
        """Return the live state of every actor (the drivers' steering truth)."""
        del req
        handlers = self._handlers
        assert handlers is not None
        states = {
            str(actor.id): {
                "x": actor.pos_x,
                "y": actor.pos_y,
                "vx": actor.vel_x,
                "vy": actor.vel_y,
                "input_tick": actor.input_tick,
                "update_seq": actor.update_seq,
            }
            for actor in handlers.actor_states()
        }
        self._write_json(resp, 200, {"actors": states})

    def _route_teleport(self, req: Request, resp: Response) -> None:
        """Set one actor's absolute position (world units in the body)."""
        handlers = self._handlers
        assert handlers is not None
        try:
            body = json.loads(req.body.decode("utf-8")) if req.body else {}
            actor_id = int(body["actor_id"])
            x = int(float(body["x"]) * _world.Q16)
            y = int(float(body["y"]) * _world.Q16)
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as _exc:
            self._write_json(resp, 400, {"error": "actor_id, x, y required"})
            return
        updated = handlers.teleport_actor(actor_id, x, y)
        if updated is None:
            self._write_json(resp, 404, {"error": "actor not found"})
            return
        self._write_json(resp, 200, {"actor_id": actor_id})

    @staticmethod
    def _write_json(resp: Response, status: int, body: dict[str, Any]) -> None:
        resp.status(status, "application/json")
        resp.body(json.dumps(body).encode("utf-8"))


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse the server's knobs."""
    parser = argparse.ArgumentParser(prog="kith-visual serve")
    parser.add_argument("--port", type=int, default=0, help="gateway port (0 = auto)")
    parser.add_argument("--tick-hz", type=int, default=20, help="simulation tick rate")
    parser.add_argument("--npcs", type=int, default=40, help="ambient actor count")
    parser.add_argument(
        "--python-workers",
        type=int,
        default=int(os.environ.get("KITH_PYTHON_WORKERS", "0")),
        help="handler worker pool size (0 = server default)",
    )
    parser.add_argument(
        "--delivery-workers",
        type=int,
        default=int(os.environ.get("KITH_DELIVERY_WORKERS", "0")),
        help="delivery executor workers (0 = gateway default)",
    )
    parser.add_argument(
        "--delivery-preset",
        choices=("full", "tiered"),
        default="full",
        help="delivery preset (tiered adds interval tiers + max gap)",
    )
    parser.add_argument(
        "--tiered-max-gap-ms",
        type=int,
        default=1000,
        help="tiered preset's max gap between sends",
    )
    parser.add_argument(
        "--view-max-subjects",
        type=int,
        default=0,
        help="view-set subject capacity (0 = gateway default)",
    )
    parser.add_argument(
        "--view-refresh-ms",
        type=int,
        default=0,
        help="view refresh interval (0 = tick interval)",
    )
    parser.add_argument(
        "--cache-refresh-ms",
        type=int,
        default=0,
        help="cache refresh interval (0 = tick interval)",
    )
    parser.add_argument(
        "--replication-batch-type-id",
        type=int,
        default=0,
        help="multi-subject batch replication type (0 = per-subject)",
    )
    parser.add_argument("--seed", type=int, default=7, help="wander RNG seed")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    """Boot the server and block until SIGINT/SIGTERM."""
    args = parse_args(argv)
    game = PlaygroundServer(args)
    server = game.start()
    print(
        f"playground: gateway={server.listen_port} control={server.control_port} "
        f"tick_hz={args.tick_hz} npcs={args.npcs} preset={args.delivery_preset}",
        flush=True,
    )
    try:
        server.serve()
    finally:
        game.stop()


if __name__ == "__main__":
    main()
