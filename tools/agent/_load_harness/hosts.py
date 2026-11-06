"""Host factories for embedded, distributed and external scaling runs."""

from __future__ import annotations

from collections.abc import Sequence

from tools.agent.orchestrator import Orchestrator

from .constants import BULK_HISTORY_SIZE, INSTRUMENTED_HISTORY_SIZE, INSTRUMENTED_SAMPLE_SIZE
from .records import LoadHostFactory


def embedded_factory(
    *,
    python_workers: int = 0,
    delivery_strategy: str | None = None,
    delivery_workers: int = 0,
    native_apply: bool = False,
    evidence_budget_bytes: int = 0,
    extra_instrumented_indices: Sequence[int] = (),
) -> LoadHostFactory:
    """Return a factory that boots a fresh embedded server per run.

    Deferred import so importing this module does not load the C shared
    libraries or the example modules. The first
    :data:`_INSTRUMENTED_SAMPLE_SIZE` clients carry the large event history
    the depth metrics need (``move_missing`` / ``continuity_flicker``
    sample the full movement-window stream); the remaining bulk clients
    carry :data:`_BULK_HISTORY_SIZE` so the per-client retained payload
    memory stays bounded.
    Multi-subject batch delivery is enabled (the gateway packs the full
    view set into one frame per refresh) so the reactor's per-refresh
    frame volume scales O(N), not O(N x K). ``python_workers`` forwards to
    the embedded server's worker pool size so a scaling profile raises the
    pool above the default of 1.
    """
    from examples.spatial import messages as spatial_messages

    from tools.agent.embedded_host import embedded_host_factory

    return embedded_host_factory(
        ahc_event_history_size=INSTRUMENTED_HISTORY_SIZE,
        replication_batch_type_id=spatial_messages.ACTOR_STATE_BATCH_TYPE,
        python_workers=python_workers,
        delivery_strategy=delivery_strategy,
        delivery_workers=delivery_workers,
        native_apply=native_apply,
        instrumented_count=INSTRUMENTED_SAMPLE_SIZE,
        bulk_event_history_size=BULK_HISTORY_SIZE,
        evidence_budget_bytes=evidence_budget_bytes,
        extra_instrumented_indices=extra_instrumented_indices,
    )


def distributed_factory(
    *,
    python_workers: int = 0,
    delivery_strategy: str | None = None,
    delivery_workers: int = 0,
    evidence_budget_bytes: int = 0,
    extra_instrumented_indices: Sequence[int] = (),
) -> LoadHostFactory:
    """Return a factory that boots a fresh multi-instance cluster per run.

    Deferred import so importing this module does not load the C shared
    libraries or the example modules. The first
    :data:`_INSTRUMENTED_SAMPLE_SIZE` clients carry the large event history
    the depth metrics need; the remaining bulk clients carry
    :data:`_BULK_HISTORY_SIZE` so the per-client retained payload memory
    stays bounded. The cluster boots two embedded servers on a shared
    coordination bus (loopback transport plus ``add_member``), matching the
    distributed example and the cross-instance split-merge integration test.
    Multi-subject batch delivery is enabled (same as the embedded factory)
    so the reactor's per-refresh frame volume scales O(N), not O(N x K).
    ``python_workers`` forwards to each instance's worker pool size so a
    scaling profile raises the pool above the default of 1.
    """
    from examples.spatial import messages as spatial_messages

    from tools.agent.distributed_host import distributed_host_factory

    return distributed_host_factory(
        ahc_event_history_size=INSTRUMENTED_HISTORY_SIZE,
        instance_count=2,
        replication_batch_type_id=spatial_messages.ACTOR_STATE_BATCH_TYPE,
        python_workers=python_workers,
        delivery_strategy=delivery_strategy,
        delivery_workers=delivery_workers,
        instrumented_count=INSTRUMENTED_SAMPLE_SIZE,
        bulk_event_history_size=BULK_HISTORY_SIZE,
        evidence_budget_bytes=evidence_budget_bytes,
        extra_instrumented_indices=extra_instrumented_indices,
    )


def external_factory(
    host: str, port: int, extra_instrumented_indices: Sequence[int] = ()
) -> LoadHostFactory:
    """Return a factory targeting an external server at ``host:port``."""

    def factory() -> Orchestrator:
        from examples.spatial.client import make_ahc, replication_direct_types

        from tools.agent.ahc import AgenticHeadlessClient
        from tools.agent.embedded_host import _history_size_for, _summary_retention_for
        from tools.agent.orchestrator import Orchestrator as _Orch

        principal_counter = [100]
        extra = frozenset(extra_instrumented_indices)

        def ahc_factory(
            instance_id: str, connect_host: str, connect_port: int
        ) -> AgenticHeadlessClient:
            del connect_host, connect_port
            principal_id = principal_counter[0]
            principal_counter[0] += 1
            client_index = principal_id - 100
            history_size = _history_size_for(
                client_index,
                INSTRUMENTED_SAMPLE_SIZE,
                INSTRUMENTED_HISTORY_SIZE,
                BULK_HISTORY_SIZE,
                extra_instrumented=extra,
            )
            return make_ahc(
                instance_id=instance_id,
                host=host,
                port=port,
                principal_id=principal_id,
                event_history_size=history_size,
                http_enabled=False,
                ipc_enabled=False,
                reconnect_enabled=False,
                tick_interval_s=0.1,
                direct_type_ids=replication_direct_types(),
                summary_mode=_summary_retention_for(
                    client_index,
                    INSTRUMENTED_SAMPLE_SIZE,
                    BULK_HISTORY_SIZE,
                    extra_instrumented=extra,
                ),
            )

        return _Orch(ahc_factory=ahc_factory, server_control=None)

    return factory


def subprocess_cluster_factory(
    *,
    python_workers: int = 0,
    delivery_strategy: str | None = None,
    delivery_workers: int = 0,
    tiered_max_gap_ms: int | None = None,
    instance_count: int = 2,
    server_affinity: str | None = None,
    server_pids_out: list[int] | None = None,
    evidence_budget_bytes: int = 0,
    extra_instrumented_indices: Sequence[int] = (),
) -> LoadHostFactory:
    """Return a factory that boots a fresh N-subprocess cluster per run.

    Each call to the factory spawns ``instance_count`` embedded-server
    subprocesses (one OS process per instance) and returns an
    :class:`Orchestrator` whose AHC factory round-robins clients across
    the captured gateway ports. Moving the servers out of the harness
    process decouples their worker-pool GILs from the harness's asyncio
    client-drive loop, removing the structural ceiling the in-process
    distributed host hits at 2000 clients, where drive latency dominates
    the bootstrap window. Deferred import so importing this
    module does not load the C shared libraries or the example modules.

    ``python_workers`` forwards to each instance's worker pool size so a
    scaling profile raises the pool above the default of 1.
    ``tiered_max_gap_ms`` forwards to each instance's tiered delivery
    backstop when the strategy is ``tiered`` (None keeps the documented
    preset cadence). The distributed-2000 gate passes ``instance_count=2``;
    a smoke profile may pass a smaller count. The first
    :data:`_INSTRUMENTED_SAMPLE_SIZE` clients carry the large event
    history the depth metrics need; the remaining bulk clients carry
    :data:`_BULK_HISTORY_SIZE` so the per-client retained payload memory
    stays bounded at 2000 clients.
    """
    from examples.spatial import messages as spatial_messages

    from tools.agent.subprocess_cluster_host import subprocess_cluster_factory

    return subprocess_cluster_factory(
        ahc_event_history_size=INSTRUMENTED_HISTORY_SIZE,
        instance_count=instance_count,
        replication_batch_type_id=spatial_messages.ACTOR_STATE_BATCH_TYPE,
        python_workers=python_workers,
        delivery_strategy=delivery_strategy,
        delivery_workers=delivery_workers,
        tiered_max_gap_ms=tiered_max_gap_ms,
        instrumented_count=INSTRUMENTED_SAMPLE_SIZE,
        bulk_event_history_size=BULK_HISTORY_SIZE,
        server_pids_out=server_pids_out,
        server_affinity=server_affinity,
        evidence_budget_bytes=evidence_budget_bytes,
        extra_instrumented_indices=extra_instrumented_indices,
    )
