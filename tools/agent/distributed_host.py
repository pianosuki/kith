"""In-process distributed-topology host factory for the scaling-gate harness.

Boots N :class:`examples.embedded.server.EmbeddedServer` instances on a
shared :class:`kith.coord.CoordBus` extended with
:meth:`~kith.coord.CoordBus.add_member` to a real N-member cluster, and
returns an :class:`~tools.agent.orchestrator.Orchestrator` whose AHC
factory round-robins clients across the instances' gateway ports. The
composition mirrors :class:`examples.distributed.server.DistributedCluster`
and the cross-instance split-merge integration test: each server runs its
wire and control surface on the embedded topology while the coordination
contract runs on the shared bus the factory owns.

A load-harness run via ``--distributed`` is a closed-loop exercise of the
full stack across multiple instances (TCP accept, wire dispatch, sim
publish, fabric replication, gateway delivery, coord-bus membership) with
no external process to start. The loopback transport the bus constructs
with plus ``add_member`` forms the in-process cluster; no real network
transport is needed for the in-process gate, matching the embedded
host's single-process story at N instances.

The ``_DistributedControl`` the orchestrator carries exposes the
per-instance :class:`~tools.agent.server_control.ServerControlClient`
list so the harness can resolve own actors per instance (each instance
allocates actor ids independently from 1, so a flat merge would
collide) and measure per-instance delivery rates for the
``publish_rate_skew`` metric.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable, Mapping, Sequence

from examples._common.query_state import QUERY_STATE_DEFAULT_PAGE_SIZE as _QUERY_STATE_PAGE_SIZE
from examples.embedded.server import EmbeddedServer
from examples.spatial.client import make_ahc, replication_direct_types
from tools.agent.ahc import AgenticHeadlessClient
from tools.agent.embedded_host import _history_size_for, _summary_retention_for
from tools.agent.orchestrator import Orchestrator
from tools.agent.server_control import ServerControlClient

from kith import Server, ServerStatus
from kith.coord import Coord, CoordBus


__all__ = [
    "DistributedControl",
    "distributed_host_factory",
]


_BOOT_TIMEOUT_S: float = 5.0
_PRINCIPAL_ID_BASE: int = 100

# The single zone every cluster member shares on the bus. Both coords
# subscribe the bus to this zone so rebalance contracts fan out to every
# borrower; the scaling gate does not drive splits, but the bus is wired
# exactly as the distributed example wires it so the topology is real.
_WORLD_ZONE: int = 1

# Instance identifiers for the cluster members. The bus is created with
# instance 1 as its local member; add_member registers instance 2, giving
# a membership table of [1, 2] (bus owner first, then the joined member).
_INSTANCE_IDS: tuple[int, ...] = (1, 2)


class DistributedControl:
    """Aggregating control plane over N per-instance control clients.

    Exposes the per-instance :class:`ServerControlClient` list via
    :attr:`controls` so the harness can query each instance's
    ``/query_state`` separately (actor ids are per-instance, so a flat
    merge would collide). The :class:`~tools.agent.assertions.ServerControl`
    protocol methods delegate to instance 0 for paths the harness does not
    aggregate (``post``, ``metrics``); ``get("/query_state")`` aggregates
    across instances, tagging each actor with its source instance so the
    harness can group actors per instance if it chooses not to call the
    per-instance controls directly. ``close`` closes every client.

    Attributes:
        controls: The per-instance control clients, in instance-id order.
        instance_count: The number of instances (len(controls)).
    """

    __slots__ = ("_controls",)

    def __init__(self, controls: list[ServerControlClient]) -> None:
        self._controls = list(controls)

    @property
    def controls(self) -> list[ServerControlClient]:
        return self._controls

    @property
    def instance_count(self) -> int:
        return len(self._controls)

    async def metrics(self) -> str:
        """Return the instance-0 Prometheus scrape.

        The harness does not merge per-instance metrics; the scaling gate
        measures per-instance delivery rates through the replication
        stream, not through the metrics endpoint.
        """
        return await self._controls[0].metrics()

    async def get(self, path: str) -> Mapping[str, object]:
        """Return the JSON response for a control-plane GET ``path``.

        ``/query_state`` aggregates across instances: each actor dict in
        the merged ``actors`` list carries an ``instance`` field naming
        the source instance (1-based, matching the bus membership table).
        Every other path delegates to instance 0.
        """
        if path.partition("?")[0] == "/query_state":
            return await self._merged_query_state()
        return await self._controls[0].get(path)

    async def post(self, path: str, body: Mapping[str, object]) -> Mapping[str, object]:
        return await self._controls[0].post(path, body)

    async def close(self) -> None:
        for control in self._controls:
            await control.close()

    async def _merged_query_state(self) -> dict[str, object]:
        actors: list[dict[str, object]] = []
        for index, control in enumerate(self._controls):
            offset = 0
            limit = _QUERY_STATE_PAGE_SIZE
            while True:
                mapping = await control.get(f"/query_state?offset={offset}&limit={limit}")
                instance_actors = mapping.get("actors")
                if not isinstance(instance_actors, list):
                    break
                for actor in instance_actors:
                    if isinstance(actor, dict):
                        tagged = dict(actor)
                        tagged["instance"] = _INSTANCE_IDS[index]
                        actors.append(tagged)
                # Stop on a short final page, or when the server does not
                # paginate (no ``total`` field — the back-compat full-roster
                # response).
                if len(instance_actors) < limit or "total" not in mapping:
                    break
                offset += limit
        return {"actors": actors}


def _wait_for_running(server: EmbeddedServer, timeout_s: float) -> None:
    """Poll the server's status until it reaches RUNNING or the deadline."""
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        facade = server._server
        if facade is not None and facade.status is ServerStatus.RUNNING:
            return
        time.sleep(0.01)
    raise RuntimeError("embedded server did not reach RUNNING state")


def distributed_host_factory(
    *,
    ahc_event_history_size: int = 512,
    instance_count: int = 2,
    replication_batch_type_id: int = 0,
    python_workers: int = 0,
    delivery_strategy: str | None = None,
    delivery_workers: int = 0,
    instrumented_count: int = 0,
    bulk_event_history_size: int | None = None,
    evidence_budget_bytes: int = 0,
    extra_instrumented_indices: Sequence[int] = (),
) -> Callable[[], Orchestrator]:
    """Return a factory that boots a fresh N-instance cluster per scenario.

    Each call boots ``instance_count`` :class:`EmbeddedServer` instances,
    starts each on a background thread, waits for every one to reach
    RUNNING, and constructs a shared :class:`CoordBus` extended with
    :meth:`~kith.coord.CoordBus.add_member` to a real N-member cluster.
    The returned :class:`Orchestrator`'s AHC factory round-robins clients
    across the instances' gateway ports (client counter modulo
    ``instance_count``), and its server control plane is a
    :class:`DistributedControl` wrapping one
    :class:`ServerControlClient` per instance. The orchestrator's
    ``on_shutdown`` hook tears the cluster down after the host is stopped.

    Args:
        ahc_event_history_size: Forwards to the game-aware AHC factory so
            a caller driving a dense actor profile (a load harness) gets a
            per-client event history large enough to sample the
            replication stream across the run; the default suits the
            closed-loop scenarios (2-3 clients).
        instance_count: Number of embedded servers to boot and wire onto
            the shared bus. The distributed example and the
            ``distributed-2000`` gate use 2; a smoke profile may pass a
            smaller count.
        python_workers: Forwards to each instance's handler worker pool
            size; 0 defers to the server default (1 under the GIL), and a
            scaling profile raises it so the handler dispatch parallelizes
            across worker threads under free-threaded Python.
        delivery_workers: Forwards to each instance's delivery executor
            thread count; 0 keeps delivery inline on the reactor
            thread.
        delivery_strategy: Forwards to each instance's gateway; None keeps
            the factory default, and naming the built-in ``tiered`` preset
            attaches its documented cadence image so a run records the
            tuning under test.
        instrumented_count: Number of clients (by start order) that carry
            the large event history the depth metrics need; the remaining
            bulk clients carry ``bulk_event_history_size``. 0 disables the
            split (every client gets ``ahc_event_history_size``).
        bulk_event_history_size: Event-history depth for bulk clients. A
            small recent window suffices for the breadth metrics; passing
            None keeps every client at ``ahc_event_history_size``.
    """

    def factory() -> Orchestrator:
        servers: list[EmbeddedServer] = []
        endpoints: list[tuple[Server, int, int]] = []
        facades: list[Server] = []
        try:
            for _ in range(instance_count):
                server = EmbeddedServer(
                    replication_batch_type_id=replication_batch_type_id,
                    python_workers=python_workers,
                    delivery_strategy=delivery_strategy,
                    delivery_workers=delivery_workers,
                )
                facade, gw_port, ctrl_port = server.start()
                servers.append(server)
                endpoints.append((facade, gw_port, ctrl_port))
                facades.append(facade)

            for facade in facades:
                run_thread = threading.Thread(target=facade.run, daemon=True)
                run_thread.start()
            for server in servers:
                _wait_for_running(server, _BOOT_TIMEOUT_S)

            bus = CoordBus(instance_id=_INSTANCE_IDS[0])
            bus.subscribe(_WORLD_ZONE)
            coords: list[Coord] = []
            for member_id in _INSTANCE_IDS[1:instance_count]:
                bus.add_member(member_id)
            for member_id in _INSTANCE_IDS[:instance_count]:
                coords.append(Coord(bus, instance_id=member_id))

            gateway_ports = [ep[1] for ep in endpoints]
            control_ports = [ep[2] for ep in endpoints]
            controls = [ServerControlClient(host="127.0.0.1", port=port) for port in control_ports]
            principal_counter = [_PRINCIPAL_ID_BASE]
            extra_instrumented = frozenset(extra_instrumented_indices)

            def ahc_factory(
                instance_id: str, connect_host: str, connect_port: int
            ) -> AgenticHeadlessClient:
                del connect_host, connect_port
                principal_id = principal_counter[0]
                principal_counter[0] += 1
                target_index = (principal_id - _PRINCIPAL_ID_BASE) % instance_count
                client_index = principal_id - _PRINCIPAL_ID_BASE
                history_size = _history_size_for(
                    client_index,
                    instrumented_count,
                    ahc_event_history_size,
                    bulk_event_history_size,
                    extra_instrumented=extra_instrumented,
                )
                return make_ahc(
                    instance_id=instance_id,
                    host="127.0.0.1",
                    port=gateway_ports[target_index],
                    principal_id=principal_id,
                    event_history_size=history_size,
                    http_enabled=False,
                    ipc_enabled=False,
                    reconnect_enabled=False,
                    tick_interval_s=0.1,
                    replication_batch_type_id=replication_batch_type_id,
                    direct_type_ids=replication_direct_types(),
                    summary_mode=_summary_retention_for(
                        client_index,
                        instrumented_count,
                        bulk_event_history_size,
                        extra_instrumented=extra_instrumented,
                    ),
                    evidence_budget_bytes=evidence_budget_bytes,
                )

            server_control = DistributedControl(controls)

            def on_shutdown() -> None:
                for coord in coords:
                    coord.close()
                bus.close()
                for facade in facades:
                    facade.shutdown()
                # Every embedded server stops deterministically here, mirroring
                # the failure path below; stopping only one instance left the
                # others to reclaim their C handles at interpreter gc.
                for embedded in servers:
                    embedded.stop()

            return Orchestrator(
                ahc_factory=ahc_factory,
                server_control=server_control,
                on_shutdown=on_shutdown,
            )
        except BaseException:
            for server in servers:
                server.stop()
            raise

    return factory
