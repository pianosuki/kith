"""Integration test: concurrent gateway handler dispatch under real parallelism.

Boots an embedded-topology :class:`kith.Server` with a multi-thread Python
worker pool, registers a Python wire handler that mutates a shared counter,
and drives N agentic headless clients concurrently over the wire. The
gateway dispatches each inbound frame to the worker pool; under
free-threaded Python (``python3.14t``, GIL disabled) the worker threads
execute handlers in true parallelism, so a handler that mutates shared
state without synchronization loses updates.

The locked variant asserts the counter reaches exactly N*M (no lost
updates) under both standard and free-threaded Python. The unlocked variant
is marked ``xfail`` under free-threaded Python: its handler performs an
unsynchronized read-increment-store on the shared counter with the window
held open, so concurrently executing workers lose updates and the counter
falls short. It is skipped under standard Python (the GIL serializes the
increment, so no race occurs and the assertion would pass trivially). An
XPASS — lost updates never materializing — is reported but not gating; the
rendezvous variant is the serialization tripwire, failing whenever dispatch
stops running two handlers at once. It asserts the concurrency itself: two
handler invocations must release a
two-party barrier together, which a serialized dispatch cannot do — the
deterministic carrier for the unordered-execution contract. The rendezvous
runs under both interpreters: the explicit pool size is honored either way,
and a barrier wait releases the GIL, so two pool threads can hold handlers
at the same instant even where bytecode execution serializes.

A second class applies the same two assertions to the shared pool itself:
a control route handler and a tick handler must be able to hold the pool
at the same instant (the route is hit over HTTP while the tick stream
runs), and with one worker both callbacks run on that single thread.
"""

from __future__ import annotations

import asyncio
import contextlib
import sys
import threading
import time
from collections.abc import Callable
from pathlib import Path
from urllib.request import urlopen

import pytest
from _helpers import needs_build
from tools.agent.ahc import AgenticHeadlessClient

from kith import Server, ServerStatus, Session
from kith.control import Request, Response


_REPO_ROOT = Path(__file__).resolve().parents[2]


def _is_free_threaded() -> bool:
    """Return True when the interpreter runs with the GIL disabled."""
    return not getattr(sys, "_is_gil_enabled", lambda: True)()


class _Counter:
    """Shared mutable counter incremented by gateway dispatch handlers."""

    __slots__ = ("value",)

    def __init__(self) -> None:
        self.value = 0


# A user-base wire type id (>= KITH_PROTO_TYPE_USER_BASE = 1000) for the
# counter-increment handler. The id is above the tile rpg and lobby catalogs
# so the test's type registration never collides with an example's.
_COUNTER_TYPE_ID: int = 2001
_COUNTER_TYPE_NAME: str = "counter_inc"

# Worker pool size: >1 so worker threads execute handlers concurrently under
# free-threaded Python. Under standard Python the GIL serializes Python
# execution regardless of pool size, so the locked variant passes and the
# unlocked variant is skipped.
_WORKER_COUNT: int = 4

# N clients each submit M counter-increment frames. The total (N*M) stays
# below the worker pool's default task capacity (KITH_WORKER_DEFAULT_TASK_CAPACITY
# = 4096) so the pool never saturates: a Python-bound dispatch dropped on
# EBUSY is honest backpressure, not a read-modify-write race loss,
# and conflates with the race-loss this test isolates. Saturation/drop
# behavior is covered by the C test in test_gateway_handler.c.
_CLIENT_COUNT: int = 4
_INCS_PER_CLIENT: int = 512

# Race-variant pressure profile. The unlocked variant boots its own server
# with more workers and clients than the locked one, and its handler holds
# the load/store window open for a fixed wall-clock span instead of relying
# on instruction-level timing luck. The total frame budget stays below the
# worker pool's default task capacity (8 * 480 = 3840 < 4096) so honest
# EBUSY backpressure drops never conflate with race losses. Under parallel
# execution the windows of concurrent handlers overlap and updates are lost;
# if dispatch serializes, the windows stop overlapping and the counter
# reaches its expected value — the xfail this variant reports. The
# rendezvous variant is the serialization tripwire: it hard-fails when no
# two workers ever hold a handler at once.
_RACE_WORKER_COUNT: int = 8
_RACE_CLIENT_COUNT: int = 8
_RACE_INCS_PER_CLIENT: int = 480
_RACE_INTERLEAVE_NS: int = 8_000

# Rendezvous-variant profile. Two clients submit one frame each, so the
# handler runs exactly twice: the two invocations release a two-party
# barrier together or the test fails. The barrier timeout is generous
# against scheduler noise; under serialized dispatch every wait times out
# and the assertion reads zero arrivals.
_RENDEZVOUS_WORKER_COUNT: int = 4
_RENDEZVOUS_CLIENTS: int = 2
_RENDEZVOUS_INCS_PER_CLIENT: int = 1
_RENDEZVOUS_TIMEOUT_S: float = 5.0

# Control-route profile: the same rendezvous against the shared pool's
# route-vs-tick pair. Ticks run at 20 Hz, so a waiting tick partner is
# already parked when the HTTP request lands; the barrier timeout bounds
# the wait if the route never overlaps.
_ROUTE_RENDEZVOUS_WORKER_COUNT: int = 4
_ROUTE_BARRIER_TIMEOUT_S: float = 5.0
_ROUTE_SINGLE_WORKER_COUNT: int = 1
_ROUTE_SINGLE_TICKS_MIN: int = 3
_ROUTE_DRIVE_TIMEOUT_S: float = 5.0


def _wait_for_status(server: Server, target: ServerStatus, timeout: float = 5.0) -> None:
    """Poll the server status until it reaches ``target`` or the deadline."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if server.status is target:
            return
        time.sleep(0.01)
    assert server.status is target


def _boot_counter_server(
    handler: Callable[[int, bytes, Session], None],
    *,
    python_workers: int = _WORKER_COUNT,
) -> tuple[Server, int, threading.Thread]:
    """Build an embedded server with the counter handler and run it on a thread.

    Args:
        handler: The wire handler invoked for every counter-increment frame.
        python_workers: Worker pool size; defaults to the locked variant's
            pool size.

    Returns:
        The facade, the gateway port, and the run thread. The caller
        shuts the server down by calling ``server.shutdown()`` followed by
        ``server.close()`` after the run thread has exited.
    """
    server = Server(
        topology="embedded",
        listen_port=0,
        tick_hz=20,
        python_workers=python_workers,
        handler_table_size=4096,
        replication_type_id=0,
    )
    server.register_proto_type(_COUNTER_TYPE_NAME, _COUNTER_TYPE_ID)
    server.register_message_handler(_COUNTER_TYPE_ID, handler)
    run_thread = threading.Thread(target=server.run, daemon=True)
    run_thread.start()
    _wait_for_status(server, ServerStatus.RUNNING)
    return server, server.listen_port, run_thread


async def _drive_clients(
    gateway_port: int,
    counter: _Counter,
    expected: int,
    *,
    client_count: int = _CLIENT_COUNT,
    incs_per_client: int = _INCS_PER_CLIENT,
) -> int:
    """Start N AHC clients, submit M increments each, then stop them.

    Args:
        gateway_port: The gateway port to connect the clients to.
        counter: The shared counter the server-side handler increments.
        expected: The counter value that means every frame was dispatched.
        client_count: Clients to start; defaults to the locked variant's
            concurrency.
        incs_per_client: Frames per client; defaults to the locked variant's
            per-client load.

    Returns:
        The final counter value. The clients run without bootstrap steps:
        on connect the C engine transitions to READY immediately (0 configured
        steps), so interactive commands are accepted as soon as the TCP connection
        is established. After all submissions, the counter is polled until it
        reaches ``expected`` or stabilizes (no change for 2 s), so the server has
        dispatched every frame before the clients are stopped.
    """
    clients = [
        AgenticHeadlessClient(
            instance_id=f"concurrent-{i}",
            host="127.0.0.1",
            port=gateway_port,
            message_types={_COUNTER_TYPE_NAME: _COUNTER_TYPE_ID},
            outbound_cap=8192,
        )
        for i in range(client_count)
    ]
    try:
        for client in clients:
            await client.start()

        for client in clients:
            deadline = time.monotonic() + 5.0
            while time.monotonic() < deadline:
                if client.query_status().bootstrap_state == 2:
                    break
                await asyncio.sleep(0.02)
            assert client.query_status().bootstrap_state == 2

        for client in clients:
            for _ in range(incs_per_client):
                client.submit(_COUNTER_TYPE_ID)

        deadline = time.monotonic() + 15.0
        last_val = -1
        last_change = time.monotonic()
        while time.monotonic() < deadline:
            val = counter.value
            if val != last_val:
                last_val = val
                last_change = time.monotonic()
            if val >= expected:
                break
            if val > 0 and (time.monotonic() - last_change) >= 2.0:
                break
            await asyncio.sleep(0.02)
    finally:
        for client in clients:
            with contextlib.suppress(Exception):
                await client.stop()
    return counter.value


@needs_build
class TestConcurrentHandlerDispatch:
    """Validate the gateway dispatches handlers concurrently through the worker pool.

    The locked variant passes under both standard and free-threaded Python:
    the :class:`threading.Lock` serializes the increment. The unlocked variant
    is skipped under standard Python (the GIL serializes the increment, so no
    race occurs) and marked ``xfail`` under free-threaded Python (the worker
    pool's threads execute the unsynchronized read-increment-store in
    parallel, losing updates).
    """

    def test_locked_handler_no_lost_updates(self) -> None:
        counter = _Counter()
        lock = threading.Lock()

        def handler(_msg_type: int, _payload: bytes, _session: Session) -> None:
            with lock:
                counter.value += 1

        server, gateway_port, run_thread = _boot_counter_server(handler)
        try:
            expected = _CLIENT_COUNT * _INCS_PER_CLIENT
            actual = asyncio.run(_drive_clients(gateway_port, counter, expected))
            assert actual == expected, f"lost updates: counter={actual}, expected={expected}"
        finally:
            server.shutdown()
            run_thread.join(timeout=5.0)
            server.close()

    @pytest.mark.skipif(
        not _is_free_threaded(),
        reason=(
            "under the GIL a lost update needs a thread switch inside "
            "the handler's race window — vanishingly unlikely against "
            "the default 5 ms switch interval"
        ),
    )
    @pytest.mark.xfail(
        reason=(
            "without a lock, the handler's unsynchronized read-increment-store "
            "on shared state loses updates when worker threads interleave"
        ),
        strict=False,
    )
    def test_unlocked_handler_races_under_free_threaded(self) -> None:
        counter = _Counter()

        def handler(_msg_type: int, _payload: bytes, _session: Session) -> None:
            # Hold the load/store window open across a fixed wall-clock span.
            # Overlapping windows from concurrently executing workers lose an
            # update per overlap; serialized dispatch produces no overlaps and
            # no losses, which surfaces as the xpass this variant reports.
            current = counter.value
            deadline = time.monotonic_ns() + _RACE_INTERLEAVE_NS
            while time.monotonic_ns() < deadline:
                current = counter.value
            counter.value = current + 1

        server, gateway_port, run_thread = _boot_counter_server(
            handler, python_workers=_RACE_WORKER_COUNT
        )
        try:
            expected = _RACE_CLIENT_COUNT * _RACE_INCS_PER_CLIENT
            actual = asyncio.run(
                _drive_clients(
                    gateway_port,
                    counter,
                    expected,
                    client_count=_RACE_CLIENT_COUNT,
                    incs_per_client=_RACE_INCS_PER_CLIENT,
                )
            )
            assert actual == expected, f"lost updates: counter={actual}, expected={expected}"
        finally:
            server.shutdown()
            run_thread.join(timeout=5.0)
            server.close()

    def test_handlers_execute_concurrently(self) -> None:
        arrivals: list[int] = []
        rendezvous = threading.Barrier(2, timeout=_RENDEZVOUS_TIMEOUT_S)

        def handler(_msg_type: int, _payload: bytes, _session: Session) -> None:
            # The first two in-flight invocations release together: a
            # rendezvous proves two worker threads hold handlers at the
            # same instant. A serialized dispatch times the wait out,
            # breaks the barrier, and reads as zero arrivals.
            try:
                arrivals.append(rendezvous.wait())
            except threading.BrokenBarrierError:
                return

        server, gateway_port, run_thread = _boot_counter_server(
            handler, python_workers=_RENDEZVOUS_WORKER_COUNT
        )
        try:
            asyncio.run(
                _drive_clients(
                    gateway_port,
                    _Counter(),
                    expected=1,
                    client_count=_RENDEZVOUS_CLIENTS,
                    incs_per_client=_RENDEZVOUS_INCS_PER_CLIENT,
                )
            )
        finally:
            server.shutdown()
            run_thread.join(timeout=_RENDEZVOUS_TIMEOUT_S)
            server.close()

        assert len(arrivals) == 2, (
            "handler dispatch never ran concurrently: no two worker threads "
            f"held a handler at the same instant (arrivals={arrivals})"
        )


def _boot_route_server(
    tick_handler: Callable[[int], None],
    route_handler: Callable[[Request, Response], None],
    *,
    python_workers: int,
) -> tuple[Server, threading.Thread]:
    """Build an embedded server with a tick handler and a control route.

    Args:
        tick_handler: Registered as the per-tick callback (the facade
            registers the pool-dispatched kind).
        route_handler: Registered as the GET /rendezvous control route
            (the facade registers the pool-dispatched kind).
        python_workers: Worker pool size.

    Returns:
        The facade and the run thread. The caller shuts the server down
        with ``shutdown()`` / ``close()`` around the thread join.
    """
    server = Server(
        topology="embedded",
        listen_port=0,
        tick_hz=20,
        python_workers=python_workers,
        handler_table_size=4096,
        replication_type_id=0,
    )
    server.register_tick_handler(tick_handler)
    server.register_control_route("GET", "/rendezvous", route_handler)
    run_thread = threading.Thread(target=server.run, daemon=True)
    run_thread.start()
    _wait_for_status(server, ServerStatus.RUNNING)
    return server, run_thread


@needs_build
class TestConcurrentControlRouteDispatch:
    """Validate the route and tick callbacks share one worker pool.

    The shared-pool concurrency contract: a control route handler, a tick
    handler, and a message handler all submit onto the same pool, and with
    more than one worker any two of them can be in flight at once — game
    state shared between callbacks is the game's to synchronize. The
    rendezvous leg proves a route handler and a tick handler held at the
    same instant: only the first tick invocation may wait at the barrier,
    so two ticks can never pair without a route in flight. The single-
    worker leg pins the structural serialization: one pool thread runs
    both callbacks.
    """

    def test_control_route_concurrent_with_tick_handler(self) -> None:
        arrivals: list[int] = []
        rendezvous = threading.Barrier(2, timeout=_ROUTE_BARRIER_TIMEOUT_S)
        tick_gate = threading.Lock()
        tick_first = threading.Event()

        def tick_handler(_tick: int) -> None:
            with tick_gate:
                if tick_first.is_set():
                    return
                tick_first.set()
            try:
                arrivals.append(rendezvous.wait())
            except threading.BrokenBarrierError:
                return

        def route(_req: Request, resp: Response) -> None:
            # Wait before writing: the rendezvous proves the route handler
            # body itself was in flight, not merely the flush of a response
            # it had already written.
            try:
                arrivals.append(rendezvous.wait())
            except threading.BrokenBarrierError:
                return
            resp.status(200, "application/json")
            resp.body(b'{"ok": true}')

        server, run_thread = _boot_route_server(
            tick_handler, route, python_workers=_ROUTE_RENDEZVOUS_WORKER_COUNT
        )
        try:
            with urlopen(
                f"http://127.0.0.1:{server.control_port}/rendezvous", timeout=10.0
            ) as resp:
                assert resp.status == 200
        finally:
            server.shutdown()
            run_thread.join(timeout=_ROUTE_BARRIER_TIMEOUT_S)
            server.close()

        assert len(arrivals) == 2, (
            "control route and tick handler never ran concurrently: no tick "
            f"held the pool while the route handler was in flight "
            f"(arrivals={arrivals})"
        )

    def test_control_route_shares_single_worker(self) -> None:
        idents: set[int] = set()
        ticks = [0]
        lock = threading.Lock()
        route_done = threading.Event()

        def tick_handler(_tick: int) -> None:
            with lock:
                idents.add(threading.get_ident())
                ticks[0] += 1

        def route(_req: Request, resp: Response) -> None:
            with lock:
                idents.add(threading.get_ident())
            resp.status(200, "application/json")
            resp.body(b'{"ok": true}')
            route_done.set()

        server, run_thread = _boot_route_server(
            tick_handler, route, python_workers=_ROUTE_SINGLE_WORKER_COUNT
        )
        try:
            with urlopen(
                f"http://127.0.0.1:{server.control_port}/rendezvous", timeout=10.0
            ) as resp:
                assert resp.status == 200
            deadline = time.monotonic() + _ROUTE_DRIVE_TIMEOUT_S
            while time.monotonic() < deadline and (
                ticks[0] < _ROUTE_SINGLE_TICKS_MIN or not route_done.is_set()
            ):
                time.sleep(0.02)
        finally:
            server.shutdown()
            run_thread.join(timeout=_ROUTE_BARRIER_TIMEOUT_S)
            server.close()

        assert ticks[0] >= _ROUTE_SINGLE_TICKS_MIN and route_done.is_set(), (
            "the single-worker drive never observed both callbacks "
            f"(ticks={ticks[0]}, route_done={route_done.is_set()})"
        )
        assert len(idents) == 1, (
            "the single-worker pool ran the route and tick callbacks on "
            f"different threads (idents={len(idents)})"
        )
