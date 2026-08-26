"""Distributed-topology cluster: two embedded servers on one coordination bus.

The spatial game library wired onto two
EmbeddedServer instances that share one real ``kith_coord_bus_t``. The bus
carries cell authority rebalance contracts between the instances; the
receiving coord applies them via on_rebalance,
transferring cell ownership across the instance boundary. This is the
split/merge transfer contract — the overlapping cell-stream handoff is a
bus-carried rebalance, not an actor-by-actor state transfer — exercised
end-to-end on real C handles in one process.

Why two embedded servers and not two distributed-topology servers: the
composition root owns every plane handle it creates, and the ``Server``
facade does not expose a borrowed coord handle for an externally-built bus
to be injected. The embedded topology's coordinator is a stub with no bus,
so each server here runs its wire and control surface on the embedded
topology while the coordination contract runs on a shared bus the launcher
owns. The wire catalog, the handlers, and the tile2d physics are the same
modules the embedded example loads; the distributed layer is the bus and
the two coords that borrow it.

What this demonstrates and what it does not: the bus-carried ownership
transfer (split a cell to instance 2, merge it back to hash-fallback) is
real — the contract is published, drained, and applied, and both coords
converge on the new authority. Density-driven automatic split (the
coordinator's threshold evaluator firing without manual publication)
requires a multi-member bus transport; the loopback transport the bus
constructs with carries one member, so the evaluator short-circuits. The
ownership-transfer contract itself is what the bus guarantees, and this
example exercises that contract directly.

The control-plane routes on instance 1 expose the coordination surface to a
harness: ``/authority`` queries either coord's view of a cell, ``/split``
and ``/merge`` drive one ownership-transfer cycle through the bus, and
``/members`` reports bus membership. ``main`` boots both servers, runs one
demonstration cycle, and prints the ports.
"""

from __future__ import annotations

import argparse
import json
import signal
import threading
import time
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from examples.embedded.server import EmbeddedServer

from kith import Server, ServerStatus
from kith.control import Request, Response
from kith.coord import Coord, CoordBus, RebalanceContract
from kith.fabric import CellKey


# The bus event type for a rebalance contract (kith_coord_bus_event_type).
_REBALANCE_EVENT: int = 0

# The shutdown signals the launcher coordinates: blocked on the run threads
# (mask inherited at creation) so a Ctrl-C is deliverable only to the main
# thread, whose wait raises KeyboardInterrupt.
_SHUTDOWN_SIGNALS: frozenset[signal.Signals] = frozenset({signal.SIGINT, signal.SIGTERM})

# The single zone the two instances share. Both coords subscribe the bus to
# this zone so rebalance contracts fan out to every borrower.
_WORLD_ZONE: int = 1

# Instance identifiers for the two cluster members.
_INSTANCE_A: int = 1
_INSTANCE_B: int = 2


@dataclass(frozen=True)
class AuthorityView:
    """One coord's resolved authority for a cell, exposed over HTTP."""

    instance_id: int
    authority_epoch: int
    owner: str


class DistributedCluster:
    """Two embedded servers and a shared coordination bus.

    Owns two EmbeddedServer instances (the wire/control surface),
    one CoordBus (the inter-instance transport), and two
    Coord handles borrowing that bus (one per instance). The
    servers run on the embedded topology; the coordination contract runs on
    the shared bus. Control routes on instance 1 drive the coordination
    surface so a harness can trigger and observe ownership transfers over
    HTTP.
    """

    __slots__ = (
        "_bus",
        "_coord_a",
        "_coord_b",
        "_delivery_strategy",
        "_epoch",
        "_lock",
        "_servers",
        "_tiered_max_gap_ms",
    )

    def __init__(
        self,
        *,
        delivery_strategy: str | None = None,
        tiered_max_gap_ms: int | None = None,
    ) -> None:
        """Store the delivery-preset selection forwarded to both servers.

        Args:
            delivery_strategy: Registered delivery strategy name forwarded
                to every server at construction (``"tiered"`` selects the
                relevance-tiered preset); ``None`` keeps the factory
                default.
            tiered_max_gap_ms: With ``delivery_strategy="tiered"``, the
                backstop override forwarded to every server; ``None``
                keeps the preset cadence.
        """
        self._servers: list[EmbeddedServer] = []
        self._bus: CoordBus | None = None
        self._coord_a: Coord | None = None
        self._coord_b: Coord | None = None
        self._lock = threading.Lock()
        self._epoch: int = 0
        self._delivery_strategy = delivery_strategy
        self._tiered_max_gap_ms = tiered_max_gap_ms

    # -----------------------------------------------------------------------
    # lifecycle
    # -----------------------------------------------------------------------

    def start(self) -> list[tuple[Server, int, int]]:
        """Boot both servers and the shared coordination bus.

        Returns a list of ``(facade, gateway_port, control_port)`` tuples,
        one per instance, in instance-id order. The caller drives each
        facade's run on its own thread and calls
        stop to shut down.
        """
        server_a = EmbeddedServer(
            delivery_strategy=self._delivery_strategy,
            tiered_max_gap_ms=self._tiered_max_gap_ms,
        )
        server_b = EmbeddedServer(
            delivery_strategy=self._delivery_strategy,
            tiered_max_gap_ms=self._tiered_max_gap_ms,
        )
        facade_a, gw_a, ctrl_a = server_a.start()
        facade_b, gw_b, ctrl_b = server_b.start()
        self._servers = [server_a, server_b]

        # The bus is created with instance A's identity as its single
        # member; both coords borrow the same handle so a contract
        # published by one is drainable by the other.
        self._bus = CoordBus(instance_id=_INSTANCE_A)
        self._bus.subscribe(_WORLD_ZONE)
        self._coord_a = Coord(self._bus, instance_id=_INSTANCE_A)
        self._coord_b = Coord(self._bus, instance_id=_INSTANCE_B)

        self._register_routes(server_a)

        return [
            (facade_a, gw_a, ctrl_a),
            (facade_b, gw_b, ctrl_b),
        ]

    def stop(self) -> None:
        """Release the coords, the bus, and both servers in reverse order."""
        if self._coord_b is not None:
            self._coord_b.close()
            self._coord_b = None
        if self._coord_a is not None:
            self._coord_a.close()
            self._coord_a = None
        if self._bus is not None:
            self._bus.close()
            self._bus = None
        for server in self._servers:
            server.stop()
        self._servers = []

    # -----------------------------------------------------------------------
    # coordination
    # -----------------------------------------------------------------------

    def _next_epoch(self) -> int:
        self._epoch += 1
        return self._epoch

    def split_cell(self, cell: CellKey) -> AuthorityView:
        """Transfer a cell's authority to instance B through the bus.

        Instance A records the override locally (the source authority marks
        the cell as handed off), publishes a rebalance contract on the bus,
        and instance B drains and applies it. Both coords converge on
        instance B as the new authority at the new epoch.

        Thread safety:
            @thread_safety unsafe — coordinates through an internal lock so
            concurrent harness calls serialize, but is not safe to call
            during stop.
        """
        with self._lock:
            assert self._coord_a is not None and self._coord_b is not None
            assert self._bus is not None
            epoch = self._next_epoch()
            contract = RebalanceContract(
                key=cell,
                source_instance_id=_INSTANCE_A,
                target_instance_id=_INSTANCE_B,
                authority_epoch=epoch,
            )
            self._coord_a.set_authority(cell, _INSTANCE_B)
            self._bus.publish(
                event_type=_REBALANCE_EVENT,
                zone=_WORLD_ZONE,
                payload=contract.to_bytes(),
            )
            self._drain_and_apply(self._coord_b)
            return self._authority_view(self._coord_a, cell, "A")

    def merge_cell(self, cell: CellKey) -> AuthorityView:
        """Revert a cell's authority to hash-fallback through the bus.

        Instance A clears its local override and publishes a merge contract
        (target 0); instance B drains and applies it, clearing the override
        and reverting to hash-fallback distribution.

        Thread safety:
            @thread_safety unsafe — see split_cell.
        """
        with self._lock:
            assert self._coord_a is not None and self._coord_b is not None
            assert self._bus is not None
            epoch = self._next_epoch()
            contract = RebalanceContract(
                key=cell,
                source_instance_id=_INSTANCE_B,
                target_instance_id=0,
                authority_epoch=epoch,
            )
            self._coord_a.clear_authority(cell)
            self._bus.publish(
                event_type=_REBALANCE_EVENT,
                zone=_WORLD_ZONE,
                payload=contract.to_bytes(),
            )
            self._drain_and_apply(self._coord_b)
            return self._authority_view(self._coord_a, cell, "A")

    def _drain_and_apply(self, coord: Coord) -> None:
        assert self._bus is not None
        events = self._bus.drain()
        for ev in events:
            if ev.event_type != _REBALANCE_EVENT:
                continue
            contract = RebalanceContract.from_bytes(ev.payload)
            coord.on_rebalance(contract)

    def authority(self, cell: CellKey, owner: str) -> AuthorityView:
        """Query one coord's resolved authority for a cell.

        Args:
            cell: The cell locator.
            owner: ``"A"`` or ``"B"`` selecting the coord to query.
        """
        coord = self._select_coord(owner)
        return self._authority_view(coord, cell, owner)

    def members(self) -> list[dict[str, Any]]:
        """Return the bus membership table as a list of status dicts."""
        assert self._bus is not None
        count = self._bus.member_count()
        out: list[dict[str, Any]] = []
        for i in range(count):
            m = self._bus.member_status(i)
            out.append(
                {
                    "instance_id": m.instance_id,
                    "owned_cell_count": m.owned_cell_count,
                    "active_input_count": m.active_input_count,
                }
            )
        return out

    def _select_coord(self, owner: str) -> Coord:
        if owner == "A":
            assert self._coord_a is not None
            return self._coord_a
        assert self._coord_b is not None
        return self._coord_b

    def _authority_view(self, coord: Coord, cell: CellKey, owner: str) -> AuthorityView:
        auth = coord.authority(cell)
        return AuthorityView(
            instance_id=auth.instance_id,
            authority_epoch=auth.authority_epoch,
            owner=owner,
        )

    # -----------------------------------------------------------------------
    # control routes
    # -----------------------------------------------------------------------

    def _register_routes(self, server: EmbeddedServer) -> None:
        facade = server._server
        assert facade is not None
        facade.register_control_route("GET", "/authority", self._route_authority)
        facade.register_control_route("POST", "/split", self._route_split)
        facade.register_control_route("POST", "/merge", self._route_merge)
        facade.register_control_route("GET", "/members", self._route_members)

    def _route_authority(self, req: Request, resp: Response) -> None:
        params = _parse_query(req.path)
        try:
            cell = _cell_from_params(params)
            owner = params.get("owner", "B")
        except (KeyError, ValueError) as exc:
            _write_json(resp, 400, {"error": str(exc)})
            return
        view = self.authority(cell, owner)
        _write_json(resp, 200, _authority_dict(view))

    def _route_split(self, req: Request, resp: Response) -> None:
        body = _read_json(req)
        try:
            cell = _cell_from_params(body)
        except (KeyError, ValueError) as exc:
            _write_json(resp, 400, {"error": str(exc)})
            return
        view = self.split_cell(cell)
        _write_json(resp, 200, _authority_dict(view))

    def _route_merge(self, req: Request, resp: Response) -> None:
        body = _read_json(req)
        try:
            cell = _cell_from_params(body)
        except (KeyError, ValueError) as exc:
            _write_json(resp, 400, {"error": str(exc)})
            return
        view = self.merge_cell(cell)
        _write_json(resp, 200, _authority_dict(view))

    def _route_members(self, req: Request, resp: Response) -> None:
        _write_json(resp, 200, {"members": self.members()})


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------


def _read_json(req: Request) -> dict[str, Any]:
    return json.loads(req.body.decode("utf-8")) if req.body else {}


def _parse_query(path: str) -> dict[str, str]:
    q = path.rpartition("?")[2]
    if not q:
        return {}
    out: dict[str, str] = {}
    for pair in q.split("&"):
        if not pair:
            continue
        k, sep, v = pair.partition("=")
        out[k] = v if sep else ""
    return out


def _cell_from_params(params: dict[str, Any]) -> CellKey:
    return CellKey(
        zone=int(params["zone"]),
        cell_x=int(params["cell_x"]),
        cell_y=int(params["cell_y"]),
        cell_z=int(params.get("cell_z", 0)),
        lod=int(params.get("lod", 0)),
    )


def _authority_dict(view: AuthorityView) -> dict[str, Any]:
    return {
        "instance_id": view.instance_id,
        "authority_epoch": view.authority_epoch,
        "owner": view.owner,
    }


def _write_json(resp: Response, status: int, body: dict[str, Any]) -> None:
    resp.status(status, "application/json")
    resp.body(json.dumps(body).encode("utf-8"))


# ---------------------------------------------------------------------------
# launcher
# ---------------------------------------------------------------------------


def _run_until_running(server: EmbeddedServer, facade: Server, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while server._server is not None and server._server.status is not ServerStatus.RUNNING:
        if time.monotonic() > deadline:
            raise TimeoutError("server did not reach RUNNING state")
        time.sleep(0.01)


def _build_parser() -> argparse.ArgumentParser:
    """Build the distributed-launcher argument parser."""
    parser = argparse.ArgumentParser(
        prog="distributed-launcher",
        description="Run the two-instance cluster on one coordination bus.",
    )
    parser.add_argument(
        "--delivery-strategy",
        default=None,
        help=(
            "Registered delivery strategy name forwarded to both servers "
            "(for example 'tiered'); omitted keeps the factory default."
        ),
    )
    parser.add_argument(
        "--tiered-max-gap-ms",
        type=int,
        default=None,
        help=(
            "With --delivery-strategy tiered: override the backstop that "
            "refreshes any subject silent this many milliseconds regardless "
            "of change (0 pins the documented default), forwarded to both "
            "servers. Omitted keeps the preset cadence."
        ),
    )
    return parser


def main(argv: Sequence[str] | None = None) -> None:
    """Boot the cluster, run a demonstration cycle, and block until interrupted.

    Starts both embedded servers on background threads, runs one
    split-then-merge cycle through the bus, prints the ports and the
    authority transition, and blocks until interrupted. A harness drives
    further cycles over the control plane. SIGINT drains both servers and
    the bus before exiting; the run threads never carry the shutdown
    signals, so the interrupt always lands in the main thread's wait.

    Args:
        argv: Optional CLI argument vector; ``None`` reads ``sys.argv``.

        ``--delivery-strategy`` forwards a registered delivery strategy
        name to both servers (``"tiered"`` selects the relevance-tiered
        preset); omitted, each server keeps the factory default. A cluster
        always runs one preset cluster-wide: both servers receive the same
        name. ``--tiered-max-gap-ms`` forwards the tiered backstop override
        to both servers — the maximum silence before any subject is
        refreshed regardless of change (0 pins the documented default);
        omitted keeps the preset cadence.
    """
    args = _build_parser().parse_args(argv)
    cluster = DistributedCluster(
        delivery_strategy=args.delivery_strategy,
        tiered_max_gap_ms=args.tiered_max_gap_ms,
    )
    endpoints = cluster.start()
    threads: list[threading.Thread] = []
    # Block the shutdown signals before the run threads exist: the mask is
    # inherited at thread creation, so the threads parked inside
    # ``kith_server_run`` can never be the ones the kernel picks for a
    # process-directed SIGINT (a thread blocked in C never runs the Python
    # handler, which left the main thread's join uninterruptible).
    blocked = signal.pthread_sigmask(signal.SIG_BLOCK, _SHUTDOWN_SIGNALS)
    for facade, _, _ in endpoints:
        t = threading.Thread(target=facade.run, daemon=True)
        t.start()
        threads.append(t)
    try:
        # Restore the main thread's mask as the first statement in the try:
        # a SIGINT arriving after this point must be caught by the handler
        # below, not kill the process before the cluster is torn down.
        signal.pthread_sigmask(signal.SIG_SETMASK, blocked)
        for server, (_, _, _) in zip(cluster._servers, endpoints, strict=True):
            _run_until_running(server, server._server)  # type: ignore[arg-type]

        cell = CellKey(zone=_WORLD_ZONE, cell_x=1, cell_y=1, cell_z=0, lod=0)
        before = cluster.authority(cell, "A")
        split = cluster.split_cell(cell)
        merged = cluster.merge_cell(cell)
        gw_a, ctrl_a = endpoints[0][1], endpoints[0][2]
        gw_b, ctrl_b = endpoints[1][1], endpoints[1][2]
        print(
            f"distributed: gateway_a={gw_a} control_a={ctrl_a} gateway_b={gw_b} control_b={ctrl_b}",
            flush=True,
        )
        print(
            f"  cell({cell.cell_x},{cell.cell_y}): "
            f"before=inst{before.instance_id} "
            f"split=inst{split.instance_id}@ep{split.authority_epoch} "
            f"merge=inst{merged.instance_id}@ep{merged.authority_epoch}",
            flush=True,
        )
        # Polling liveness keeps the wait interruptible: sleep in the main
        # thread raises KeyboardInterrupt on SIGINT, where a join parked on
        # a worker's lock may not wake.
        while any(t.is_alive() for t in threads):
            time.sleep(0.1)
    except KeyboardInterrupt:
        # Teardown runs with the mask re-blocked so a repeated Ctrl-C
        # cannot abort the drain halfway.
        signal.pthread_sigmask(signal.SIG_SETMASK, blocked)
        for facade, _, _ in endpoints:
            facade.shutdown()
        for t in threads:
            t.join()
    finally:
        cluster.stop()


if __name__ == "__main__":
    main()
