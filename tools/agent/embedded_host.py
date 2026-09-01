"""In-process embedded server host factory for the agentic harness.

Boots :class:`examples.embedded.server.EmbeddedServer` on a background
thread and returns a :class:`~tools.agent.orchestrator.Orchestrator`
bound to it: the orchestrator's AHC factory produces game-aware
headless clients (the spatial wire catalog + login bootstrap) targeting
the server's gateway port, and its server control plane targets the
server's control port. Each call to the factory boots a fresh server so
scenarios run in isolation; the orchestrator's ``on_shutdown`` hook
tears the server down after the scenario's host is stopped.

The factory is the composition point where the embedded topology meets
the agentic harness: the same game logic the distributed wiring would
run on a cluster runs here on one process, and the harness drives it
through the real wire and control-plane surfaces a cluster would
expose. A scenario run via ``--embedded`` is a closed-loop exercise of
the full stack (TCP accept, wire dispatch, sim publish, fabric
replication, gateway delivery, control-plane commands) with no external
process to start.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable, Sequence

from examples.embedded.server import EmbeddedServer
from examples.spatial.client import make_ahc, replication_direct_types

from kith import ServerStatus
from tools.agent.ahc import AgenticHeadlessClient
from tools.agent.orchestrator import Orchestrator
from tools.agent.server_control import ServerControlClient


__all__ = [
    "embedded_host_factory",
]


_BOOT_TIMEOUT_S: float = 5.0
_PRINCIPAL_ID_BASE: int = 100


def _history_size_for(
    client_index: int,
    instrumented_count: int,
    instrumented_size: int,
    bulk_size: int | None,
    extra_instrumented: frozenset[int] = frozenset(),
) -> int:
    """Return the per-client event-history depth for a factory's AHC.

    The first ``instrumented_count`` clients (by start order) plus any
    index in ``extra_instrumented`` carry the large history the depth
    metrics need (``move_missing`` / ``continuity_flicker`` sample the
    full movement-window stream); the remaining bulk clients carry
    ``bulk_size`` (a small recent window suffices for the breadth
    metrics). The split is active only when both ``instrumented_count``
    is positive and ``bulk_size`` is set; otherwise every client gets
    ``instrumented_size`` (the closed-loop default).
    """
    if instrumented_count > 0 and bulk_size is not None and client_index >= instrumented_count:
        return bulk_size if client_index not in extra_instrumented else instrumented_size
    return instrumented_size


def _summary_retention_for(
    client_index: int,
    instrumented_count: int,
    bulk_size: int | None,
    extra_instrumented: frozenset[int] = frozenset(),
) -> bool:
    """Return whether a factory's AHC rides summary retention.

    Summary retention pairs with the bulk history split: clients beyond
    ``instrumented_count`` keep periodic replication payloads only, since
    their event streams feed no fidelity metric (the breadth health check
    reads the evidence window, which every replication frame feeds).
    Extra-instrumented indices ride full retention like the head sample.
    """
    if instrumented_count > 0 and bulk_size is not None and client_index >= instrumented_count:
        return client_index not in extra_instrumented
    return False


def _wait_for_running(server: EmbeddedServer, timeout_s: float) -> None:
    """Poll the server's status until it reaches RUNNING or the deadline."""
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        facade = server._server
        if facade is not None and facade.status is ServerStatus.RUNNING:
            return
        time.sleep(0.01)
    raise RuntimeError("embedded server did not reach RUNNING state")


def embedded_host_factory(
    *,
    ahc_event_history_size: int = 512,
    replication_batch_type_id: int = 0,
    python_workers: int = 0,
    delivery_strategy: str | None = None,
    delivery_workers: int = 0,
    native_apply: bool = False,
    instrumented_count: int = 0,
    bulk_event_history_size: int | None = None,
    evidence_budget_bytes: int = 0,
    extra_instrumented_indices: Sequence[int] = (),
) -> Callable[[], Orchestrator]:
    """Return a factory that boots a fresh embedded server per scenario.

    Each call boots an :class:`EmbeddedServer`, starts its run loop on a
    background thread, waits for it to reach RUNNING, and returns an
    :class:`Orchestrator` whose AHC factory targets the gateway port and
    whose server control plane targets the control port. The
    orchestrator's ``on_shutdown`` hook tears the server down after the
    host is stopped.

    ``ahc_event_history_size`` forwards to the game-aware AHC factory so a
    caller driving a dense actor profile (a load harness) gets a per-client
    event history large enough to sample the replication stream across the
    run; the default suits the closed-loop scenarios (2-3 clients).
    ``replication_batch_type_id`` forwards to the embedded server and to
    the AHC factory: when non-zero, the gateway packs the full view set
    into one multi-subject frame per refresh and the clients await the
    batch type id; a load harness driving a dense actor profile passes a
    non-zero value to reduce the reactor's per-refresh frame volume from
    O(N x K) to O(N). ``python_workers`` forwards to the embedded server's
    handler worker pool size; 0 defers to the server default (1 under the
    GIL), and a scaling profile raises it so the handler dispatch
    parallelizes across worker threads under free-threaded Python.
    ``delivery_strategy`` forwards to the embedded server's gateway; None
    keeps the factory default, and naming the built-in ``tiered`` preset
    attaches its documented cadence image so a run records the tuning
    under test.     ``delivery_workers`` forwards to the embedded server's
    delivery executor thread count; 0 keeps delivery inline on
    the reactor thread. ``native_apply`` forwards to the embedded server's
    movement-apply path: True runs applies on the C handler
    pool, False (the default) keeps the Python apply path the embedded
    host is certified under.

    ``instrumented_count`` and ``bulk_event_history_size`` bound the
    per-client memory at scale. Only the depth metrics
    (``move_missing``, ``continuity_flicker``) need a history large
    enough to hold the full movement-window stream; those clients (the
    first ``instrumented_count`` by principal id) get
    ``ahc_event_history_size``. The remaining bulk clients carry
    ``bulk_event_history_size`` (a small recent window suffices for the
    breadth metrics). When ``instrumented_count`` is 0 every client gets
    ``ahc_event_history_size`` (the closed-loop default).
    """

    def factory() -> Orchestrator:
        server = EmbeddedServer(
            replication_batch_type_id=replication_batch_type_id,
            python_workers=python_workers,
            delivery_strategy=delivery_strategy,
            delivery_workers=delivery_workers,
            native_apply=native_apply,
        )
        facade, gateway_port, control_port = server.start()
        run_thread = threading.Thread(target=facade.run, daemon=True)
        run_thread.start()
        _wait_for_running(server, _BOOT_TIMEOUT_S)

        principal_counter = [_PRINCIPAL_ID_BASE]
        extra_instrumented = frozenset(extra_instrumented_indices)

        def ahc_factory(
            instance_id: str, connect_host: str, connect_port: int
        ) -> AgenticHeadlessClient:
            del connect_host, connect_port
            principal_id = principal_counter[0]
            principal_counter[0] += 1
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
                port=gateway_port,
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

        control = ServerControlClient(host="127.0.0.1", port=control_port)

        def on_shutdown() -> None:
            facade.shutdown()
            run_thread.join(timeout=5.0)
            if run_thread.is_alive():
                raise RuntimeError("embedded server run thread did not exit within 5 s")
            server.stop()

        return Orchestrator(
            ahc_factory=ahc_factory,
            server_control=control,
            on_shutdown=on_shutdown,
        )

    return factory
