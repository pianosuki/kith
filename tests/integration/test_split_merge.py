"""Integration test: density-driven split/merge across two embedded servers.

Boots two :class:`examples.embedded.server.EmbeddedServer` instances (the
wire/control surface) and a shared :class:`kith.coord.CoordBus` extended with
:meth:`~kith.coord.CoordBus.add_member` to a real two-member cluster. Reports
cell density above the split threshold on one coord; the coordinator's
threshold evaluator fires automatically (no manual rebalance publication),
broadcasts a rebalance contract on the bus, and the peer coord drains and
applies it so both converge on the new authority. A subsequent density drop
below the merge threshold fires the merge path, reverting both coords to
hash-fallback. This is the overlapping cell-stream handoff contract
exercised end-to-end on real C handles with live wire listeners.

The embedded servers run on the embedded topology (no bus of their own); the
coordination contract runs on the shared bus the test owns, borrowing it from
two :class:`~kith.coord.Coord` handles. This mirrors the composition in
``examples.distributed.server.DistributedCluster`` but drives the
density-driven evaluator directly instead of publishing rebalance contracts
by hand. The wire listeners accepting TCP connections while the bus transfers
authority is what makes this an integration test rather than a unit test on
bare coord handles.
"""

from __future__ import annotations

import socket
import threading
import time
from dataclasses import dataclass
from pathlib import Path

from _helpers import needs_build
from examples.embedded.server import EmbeddedServer

from kith import Server, ServerStatus
from kith.coord import Coord, CoordBus, RebalanceContract
from kith.fabric import CellKey


_REPO_ROOT = Path(__file__).resolve().parents[2]


# The bus event type for a rebalance contract (kith_coord_bus_event_type).
_REBALANCE_EVENT: int = 0

# The single zone both coords share on the bus.
_WORLD_ZONE: int = 1

# Instance identifiers for the two cluster members. The bus is created with
# instance B as its local member; add_member registers instance A, giving a
# membership table of [2, 1] (bus owner first, then the joined member).
_INSTANCE_A: int = 1
_INSTANCE_B: int = 2

# Density-driven split/merge tunables. The split threshold is high enough that
# a report of 100 actors arms the evaluator; the merge threshold is low enough
# that a report of 0 actors fires the merge. The dwell windows are short so
# the tick advancing past them fires immediately. density_stride=1 means every
# report updates the tracked density (no stride subsampling).
_SPLIT_THRESHOLD: int = 10
_MERGE_THRESHOLD: int = 5
_SPLIT_MIN_DWELL_MS: int = 100
_MERGE_MIN_DWELL_MS: int = 100
_DENSITY_STRIDE: int = 1

# The cell every transfer concerns. Hash-fallback for this cell with members
# [2, 1] is (cell_x + cell_y + zone) % member_count = (0 + 0 + 1) % 2 = 1,
# mapping to members[1] = instance 1 (coord A owns it pre-split).
_CELL = CellKey(zone=_WORLD_ZONE, cell_x=0, cell_y=0, cell_z=0, lod=0)


@dataclass
class _Cluster:
    """A two-instance cluster: two wire surfaces and a shared coord bus.

    Attributes:
        servers: The two embedded servers (wire/control surface), in
            instance-id order [A, B].
        bus: The shared coordination bus both coords borrow.
        coord_a: Coord handle for instance A (the split source).
        coord_b: Coord handle for instance B (the split target).
        endpoints: ``(facade, gateway_port, control_port)`` per server.
    """

    servers: list[EmbeddedServer]
    bus: CoordBus
    coord_a: Coord
    coord_b: Coord
    endpoints: list[tuple[Server, int, int]]


def _coord(bus: CoordBus, instance_id: int) -> Coord:
    return Coord(
        bus,
        instance_id=instance_id,
        split_threshold=_SPLIT_THRESHOLD,
        merge_threshold=_MERGE_THRESHOLD,
        split_min_dwell_ms=_SPLIT_MIN_DWELL_MS,
        merge_min_dwell_ms=_MERGE_MIN_DWELL_MS,
        density_stride=_DENSITY_STRIDE,
    )


def _build_cluster() -> _Cluster:
    """Boot two embedded servers and a shared two-member coord bus.

    The bus is created with instance B's identity as its single member, then
    extended with instance A via :meth:`CoordBus.add_member` so the
    coordinator's split target picker sees a real peer. Both coords borrow the
    same handle, so a rebalance contract published by one is drainable by the
    other.
    """
    server_a = EmbeddedServer()
    server_b = EmbeddedServer()
    facade_a, gw_a, ctrl_a = server_a.start()
    facade_b, gw_b, ctrl_b = server_b.start()

    bus = CoordBus(instance_id=_INSTANCE_B)
    bus.add_member(_INSTANCE_A)
    coord_a = _coord(bus, _INSTANCE_A)
    coord_b = _coord(bus, _INSTANCE_B)

    return _Cluster(
        servers=[server_a, server_b],
        bus=bus,
        coord_a=coord_a,
        coord_b=coord_b,
        endpoints=[(facade_a, gw_a, ctrl_a), (facade_b, gw_b, ctrl_b)],
    )


def _stop_cluster(cluster: _Cluster) -> None:
    """Release coords, the bus, and both servers in reverse construction order."""
    cluster.coord_a.close()
    cluster.coord_b.close()
    cluster.bus.close()
    for server in cluster.servers:
        server.stop()


def _start_runners(cluster: _Cluster) -> list[threading.Thread]:
    threads: list[threading.Thread] = []
    for facade, _, _ in cluster.endpoints:
        t = threading.Thread(target=facade.run, daemon=True)
        t.start()
        threads.append(t)
    return threads


def _wait_running(cluster: _Cluster, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    for srv in cluster.servers:
        while srv._server is not None and srv._server.status is not ServerStatus.RUNNING:
            if time.monotonic() > deadline:
                raise TimeoutError("server did not reach RUNNING state")
            time.sleep(0.01)


def _assert_gateways_accept_connections(cluster: _Cluster) -> None:
    """Both wire listeners accept TCP connections while the bus is live."""
    for _, gw_port, _ in cluster.endpoints:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5.0)
        sock.connect(("127.0.0.1", gw_port))
        try:
            assert sock.getpeername()[1] == gw_port
        finally:
            sock.close()


def _drain_rebalance_and_apply(bus: CoordBus, coord: Coord) -> RebalanceContract:
    """Drain one rebalance event from the bus and apply it to ``coord``.

    Returns the applied contract so the caller can assert its fields. Asserts
    the drained event is a rebalance carrying a contract payload.
    """
    events = bus.drain()
    assert len(events) == 1
    assert events[0].event_type == _REBALANCE_EVENT
    contract = RebalanceContract.from_bytes(events[0].payload)
    coord.on_rebalance(contract)
    return contract


def _stop_runners(cluster: _Cluster, threads: list[threading.Thread]) -> None:
    for facade, _, _ in cluster.endpoints:
        facade.shutdown()
    for t in threads:
        t.join(timeout=5.0)


@needs_build
class TestDensityDrivenSplitMerge:
    def test_density_driven_split_transfers_authority_across_instances(self) -> None:
        cluster = _build_cluster()
        threads: list[threading.Thread] = []
        try:
            threads = _start_runners(cluster)
            _wait_running(cluster)
            _assert_gateways_accept_connections(cluster)

            # Hash-fallback for the cell with members [2, 1] resolves to
            # instance 1 (coord A) at epoch 0.
            before = cluster.coord_a.authority(_CELL)
            assert before.instance_id == _INSTANCE_A
            assert before.authority_epoch == 0

            # Report density above the split threshold; advance past the
            # dwell. The report timestamp must be non-zero: report_density arms
            # split_since_ms only when it is zero, and tick treats a zero
            # split_since_ms as "not armed."
            cluster.coord_a.report_density(_CELL, actor_count=100, now_ms=1000)
            cluster.coord_a.tick(now_ms=2000)

            # Coord A applied the split: the override points at instance B.
            after_split_a = cluster.coord_a.authority(_CELL)
            assert after_split_a.instance_id == _INSTANCE_B
            assert after_split_a.authority_epoch == 1

            # Coord B drains the broadcast rebalance and applies it,
            # converging on the same authority as coord A.
            contract = _drain_rebalance_and_apply(cluster.bus, cluster.coord_b)
            assert contract.target_instance_id == _INSTANCE_B
            assert contract.authority_epoch == 1
            after_split_b = cluster.coord_b.authority(_CELL)
            assert after_split_b.instance_id == _INSTANCE_B
            assert after_split_b.authority_epoch == 1

            _stop_runners(cluster, threads)
        finally:
            _stop_cluster(cluster)

    def test_density_driven_merge_reverts_authority_across_instances(self) -> None:
        cluster = _build_cluster()
        threads: list[threading.Thread] = []
        try:
            threads = _start_runners(cluster)
            _wait_running(cluster)
            _assert_gateways_accept_connections(cluster)

            # Seed an override on coord A by driving a density-driven split,
            # then drain the rebalance so the bus queue is empty before the
            # merge.
            cluster.coord_a.report_density(_CELL, actor_count=100, now_ms=1000)
            cluster.coord_a.tick(now_ms=2000)
            assert cluster.coord_a.authority(_CELL).instance_id == _INSTANCE_B
            _drain_rebalance_and_apply(cluster.bus, cluster.coord_b)

            # Density drops below the merge threshold; advance past the merge
            # dwell. Coord A clears its override and broadcasts a target-0
            # contract.
            cluster.coord_a.report_density(_CELL, actor_count=0, now_ms=3000)
            cluster.coord_a.tick(now_ms=4000)

            after_merge_a = cluster.coord_a.authority(_CELL)
            assert after_merge_a.instance_id == _INSTANCE_A
            assert after_merge_a.authority_epoch == 0

            # Coord B applies the target-0 contract and reverts to
            # hash-fallback, converging on coord A's authority.
            merge = _drain_rebalance_and_apply(cluster.bus, cluster.coord_b)
            assert merge.target_instance_id == 0
            after_merge_b = cluster.coord_b.authority(_CELL)
            assert after_merge_b.instance_id == _INSTANCE_A
            assert after_merge_b.authority_epoch == 0

            _stop_runners(cluster, threads)
        finally:
            _stop_cluster(cluster)
