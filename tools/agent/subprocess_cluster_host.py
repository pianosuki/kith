"""Multi-process distributed-topology host factory for the scaling-gate harness.

Boots N :class:`examples.embedded.server.EmbeddedServer` instances as
separate OS processes (one Python interpreter each) and returns an
:class:`~tools.agent.orchestrator.Orchestrator` whose AHC factory
round-robins clients across the instances' gateway ports. Each instance
runs the embedded topology with its own handler worker pool and its own
GIL, so the worker threads do not contend with the harness's asyncio
client-drive loop for a single GIL — the structural ceiling the in-process
:mod:`tools.agent.distributed_host` hits at 2000 clients, where the
harness's batched ``start_client`` drive and the 16 server worker threads
share one interpreter and the drive latency dominates the bootstrap
window.

The wire surface is unchanged versus the in-process factory: the
agentic headless clients connect to the instances' gateway ports over
real TCP (``127.0.0.1:<port>``), exactly as they do against an
in-process embedded server. Moving the servers to subprocesses only
decouples their GILs from the harness process; the client-to-server
path is the same socket path the production wire runs.

What the cluster is and is not: each subprocess builds the embedded
topology, so each instance owns its whole cell space and its fabric
storage is process-local. The coordination bus is not shared across
the subprocesses (a real cross-process coord transport is a separate,
additive capability the gate does not exercise). The distributed-2000
gate validates horizontal scaling — N instances each serving 1000
clients with no mutual degradation and no single instance the fixed
publish bottleneck — not cross-instance fabric replication or
bus-driven split/merge. The gate's contract is per-instance fidelity
plus publish-rate skew; the subprocess cluster is a faithful host for
that contract.

The handshake: each subprocess prints one stdout line before entering
its run loop, of the form ``embedded: gateway=<port> control=<port>``,
parsed here to recover the OS-assigned ephemeral ports. A read deadline
bounds the wait so a boot failure (a missing shared library, a port
bind error) surfaces as a parse timeout rather than an indefinite
block.
"""

from __future__ import annotations

import os
import re
import select
import signal
import subprocess
import sys
import time
from collections.abc import Callable, Sequence

from examples.spatial.client import make_ahc, replication_direct_types
from tools.agent.ahc import AgenticHeadlessClient
from tools.agent.distributed_host import DistributedControl
from tools.agent.embedded_host import _history_size_for, _summary_retention_for
from tools.agent.orchestrator import Orchestrator
from tools.agent.server_control import ServerControlClient


__all__ = [
    "subprocess_cluster_factory",
]


# Bound on the wait for each subprocess to print its handshake line. The
# embedded server boots in well under a second on a warm build; a failure
# (missing KITH_LIB, a bind error) surfaces as a timeout rather than a hang.
_HANDSHAKE_TIMEOUT_S: float = 30.0

# Grace period for a subprocess to exit after SIGINT before SIGKILL is sent.
_SHUTDOWN_GRACE_S: float = 5.0

# Principal ids are assigned monotonically from this base, matching the
# in-process distributed host so the round-robin instance assignment is
# identical across the two factories (the harness maps principal_id to
# instance index by modulo instance_count).
_PRINCIPAL_ID_BASE: int = 100

# The handshake line a booted embedded-server subprocess prints. Captures the
# gateway and control ports the OS assigned (the server binds to ephemeral
# ports when listen_port is 0).
_HANDSHAKE_RE = re.compile(r"gateway=(\d+)\s+control=(\d+)")


def _child_env() -> dict[str, str]:
    """Build the subprocess environment mirroring the parent's import paths.

    pytest's ``pythonpath`` config (and an editable/``PYTHONPATH`` install)
    add ``.``, ``python``, and ``tests/integration`` to the parent's
    ``sys.path`` without exporting them as environment variables, so a
    subprocess would not find ``examples`` or ``kith`` from the inherited
    environment alone. Forward the parent's full ``sys.path`` as
    ``PYTHONPATH`` so the child resolves modules exactly as the parent
    does; the child also inherits ``KITH_LIB`` and the rest of the parent
    environment (so the C shared libraries are found at the same path).
    """
    env = os.environ.copy()
    env["PYTHONPATH"] = os.pathsep.join(sys.path)
    return env


def _await_handshake(proc: subprocess.Popen[bytes], timeout_s: float) -> tuple[int, int]:
    """Read the subprocess's stdout until the handshake line arrives.

    Polls the pipe with ``select`` so the wait is bounded by ``timeout_s``;
    a boot failure that prints no handshake line (or prints only errors)
    raises a :class:`RuntimeError` carrying the partial stdout, so the
    caller sees why the boot failed instead of blocking forever.

    Returns the parsed ``(gateway_port, control_port)`` pair.

    Raises:
        RuntimeError: the deadline passed before a handshake line arrived,
            or the subprocess exited before printing one.
    """
    assert proc.stdout is not None
    deadline = time.monotonic() + timeout_s
    collected: list[str] = []
    while time.monotonic() < deadline:
        remaining = deadline - time.monotonic()
        ready, _, _ = select.select([proc.stdout], [], [], min(remaining, 0.5))
        if not ready:
            if proc.poll() is not None:
                break
            continue
        line = proc.stdout.readline()
        if not line:
            if proc.poll() is not None:
                break
            continue
        text = line.decode("utf-8", errors="replace").rstrip()
        collected.append(text)
        match = _HANDSHAKE_RE.search(text)
        if match is not None:
            return int(match.group(1)), int(match.group(2))
    raise RuntimeError(
        f"subprocess did not print a handshake within {timeout_s:.0f}s; "
        f"stdout so far:\n{''.join(c for c in collected)}"
    )


def _terminate(proc: subprocess.Popen[bytes]) -> None:
    """Signal the subprocess to shut down and reap it, escalating to SIGKILL."""
    if proc.poll() is not None:
        if proc.stdout is not None:
            proc.stdout.close()
        return
    try:
        proc.send_signal(signal.SIGINT)
    except ProcessLookupError:
        return
    try:
        proc.wait(timeout=_SHUTDOWN_GRACE_S)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()
    if proc.stdout is not None:
        proc.stdout.close()


_AFFINITY_RE = re.compile(r"\d+(?:-\d+)?(?:,\d+(?:-\d+)?)*")


def _affinity_prefix(affinity: str | None) -> list[str]:
    """Return the ``taskset -c`` argv prefix for a CPU list, validated.

    The list goes through ``Popen`` argv (never a shell), so validation
    is about failing a typo fast rather than escaping anything; a
    malformed list raises before any subprocess spawns.
    """
    if affinity is None:
        return []
    if not _AFFINITY_RE.fullmatch(affinity):
        raise ValueError(f"invalid CPU affinity list: {affinity!r}")
    return ["taskset", "-c", affinity]


def subprocess_cluster_factory(
    *,
    instance_count: int = 2,
    ahc_event_history_size: int = 512,
    replication_batch_type_id: int = 0,
    python_workers: int = 0,
    delivery_strategy: str | None = None,
    delivery_workers: int = 0,
    tiered_max_gap_ms: int | None = None,
    instrumented_count: int = 0,
    bulk_event_history_size: int | None = None,
    server_pids_out: list[int] | None = None,
    server_affinity: str | None = None,
    handshake_out: list[tuple[int, int]] | None = None,
    evidence_budget_bytes: int = 0,
    extra_instrumented_indices: Sequence[int] = (),
) -> Callable[[], Orchestrator]:
    """Return a factory that boots a fresh N-subprocess cluster per scenario.

    Each call spawns ``instance_count`` copies of the embedded-server entry
    point (``python -m examples.embedded.server``) as separate OS
    processes, waits for each to print its handshake line, and constructs
    an :class:`Orchestrator` whose AHC factory round-robins clients across
    the captured gateway ports and whose server control plane is a
    :class:`DistributedControl` wrapping one
    :class:`ServerControlClient` per instance. The orchestrator's
    ``on_shutdown`` hook sends SIGINT to every subprocess and reaps them
    after the host is stopped.

    Args:
        instance_count: Number of embedded-server subprocesses to boot.
            The distributed-2000 gate uses 2; a smoke profile may pass a
            smaller count.
        ahc_event_history_size: Forwards to the game-aware AHC factory so
            a caller driving a dense actor profile gets a per-client event
            history large enough to sample the replication stream across
            the run; the default suits the closed-loop scenarios.
        replication_batch_type_id: Forwards to each subprocess (via the
            server CLI) and to the AHC factory so the gateway packs the
            full view set into one multi-subject frame per refresh and the
            clients await the batch type id.
        python_workers: Forwards to each subprocess's handler worker pool
            size; 0 defers to the server default (1 under the GIL), and a
            scaling profile raises it so handler dispatch parallelizes
            across worker threads under free-threaded Python.
        delivery_workers: Forwards to each subprocess's delivery executor
            thread count via the ``--delivery-workers`` command line
            0 keeps delivery inline on the reactor thread.
        delivery_strategy: Forwards to each subprocess's gateway via the
            server CLI; None keeps the factory default, and naming the
            built-in ``tiered`` preset attaches its documented cadence
            image so a run records the tuning under test.
        tiered_max_gap_ms: Overrides the tiered backstop (maximum silence
            before any subject is refreshed regardless of change) via the
            server CLI; forwarded only when not None, and 0 pins the
            documented default. Requires ``delivery_strategy="tiered"`` to
            have any effect.
        instrumented_count: Number of clients (by start order) that carry
            the large event history the depth metrics need; the remaining
            bulk clients carry ``bulk_event_history_size``. 0 disables the
            split (every client gets ``ahc_event_history_size``).
        bulk_event_history_size: Event-history depth for bulk clients.
            Bounds the per-client memory at scale (only the depth-metrics
            sample needs the full movement-window stream); passing None
            keeps every client at ``ahc_event_history_size``.
        server_pids_out: When given, the factory clears it and appends the
            pid of each successfully booted subprocess, in instance order,
            so a caller can observe the servers from outside (the /proc
            sampler). Cleared per factory call, so the list always
            reflects the live cluster of the most recent boot.
        server_affinity: Optional CPU list (``"0-7"`` / ``"0,2,4"``) the
            server subprocesses are pinned to via a ``taskset -c`` argv
            wrapper; None leaves the scheduler free placement. The
            wrapper is validated and fails the boot on a malformed list.
        handshake_out: When given, the factory clears it and appends one
            ``(gateway_port, control_port)`` pair per successfully booted
            subprocess, in instance order, so a caller can hand the ports
            to another process (a second driver) that must reach the same
            cluster. Cleared per factory call like ``server_pids_out``.
    """

    def factory() -> Orchestrator:
        if server_pids_out is not None:
            server_pids_out.clear()
        if handshake_out is not None:
            handshake_out.clear()
        procs: list[subprocess.Popen[bytes]] = []
        gateway_ports: list[int] = []
        control_ports: list[int] = []
        prefix = _affinity_prefix(server_affinity)
        try:
            for _ in range(instance_count):
                cmd = [
                    *prefix,
                    sys.executable,
                    "-m",
                    "examples.embedded.server",
                    "--python-workers",
                    str(python_workers),
                    "--replication-batch-type-id",
                    str(replication_batch_type_id),
                ]
                cmd.extend(["--delivery-workers", str(delivery_workers)])
                if delivery_strategy is not None:
                    cmd.extend(["--delivery-strategy", delivery_strategy])
                if tiered_max_gap_ms is not None:
                    cmd.extend(["--tiered-max-gap-ms", str(tiered_max_gap_ms)])
                proc = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=None,
                    text=False,
                    env=_child_env(),
                )
                procs.append(proc)
                gw, ctrl = _await_handshake(proc, _HANDSHAKE_TIMEOUT_S)
                gateway_ports.append(gw)
                control_ports.append(ctrl)
                if server_pids_out is not None:
                    server_pids_out.append(proc.pid)
                if handshake_out is not None:
                    handshake_out.append((gw, ctrl))

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
                # The orchestrator closes the control clients (DistributedControl.
                # close) before invoking this hook, so only the subprocesses
                # remain to terminate; reaping them after the control sockets
                # closed avoids a close against a dead server port.
                for proc in procs:
                    _terminate(proc)

            return Orchestrator(
                ahc_factory=ahc_factory,
                server_control=server_control,
                on_shutdown=on_shutdown,
            )
        except BaseException:
            for proc in procs:
                _terminate(proc)
            raise

    return factory
