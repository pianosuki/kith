"""Shared client tick and outbound-probe scheduling."""

from __future__ import annotations

import asyncio
import contextlib
import threading
from typing import Protocol


OUTBOUND_IDLE_PROBE_S = 1.0


class TickClient(Protocol):
    """The slice of ``AgenticHeadlessClient`` the shared ticker drives.

    Defined as a protocol so tests substitute fakes without loading the
    C shared libraries; the real client satisfies it structurally.
    """

    _tick_interval_s: float

    def tick_engine(self) -> None: ...


class ClientTicker:
    """Drive registered clients' engine ticks from one task per cadence.

    Replaces one asyncio sleep loop per client with a single shared task
    per distinct tick interval: each fire calls ``tick_engine`` on every
    registered client, so N clients cost one timer wake per interval
    instead of N. Registry mutations take a lock (set membership is not
    atomic under free-threaded Python); each fire iterates a snapshot and
    rotates its start index so no client holds a systematic first-in-batch
    phase advantage. Cadence is relative-from-yield like the per-client
    sleeps it replaces: sustained overload slips the nominal interval for
    the whole bucket equally.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._buckets: dict[float, list[TickClient]] = {}
        self._tasks: dict[float, asyncio.Task[None]] = {}
        self._cursors: dict[float, int] = {}

    def register(self, client: TickClient) -> None:
        """Add ``client`` to the bucket for its configured cadence."""
        with self._lock:
            bucket = self._buckets.setdefault(client._tick_interval_s, [])
            bucket.append(client)
            task = self._tasks.get(client._tick_interval_s)
            if task is None or task.done():
                loop = asyncio.get_running_loop()
                self._tasks[client._tick_interval_s] = loop.create_task(
                    self._run(client._tick_interval_s),
                    name=f"ahc-ticker-{client._tick_interval_s}",
                )

    def unregister(self, client: TickClient) -> None:
        """Remove ``client`` from its bucket; the last removal keeps the
        task alive so re-registration never pays a restart."""
        with self._lock:
            bucket = self._buckets.get(client._tick_interval_s)
            if bucket is not None:
                with contextlib.suppress(ValueError):
                    bucket.remove(client)

    async def _run(self, interval: float) -> None:
        while True:
            await asyncio.sleep(interval)
            with self._lock:
                bucket = list(self._buckets.get(interval, ()))
                cursor = self._cursors.get(interval, 0)
                self._cursors[interval] = cursor + 1
            if not bucket:
                continue
            start = cursor % len(bucket)
            for client in bucket[start:] + bucket[:start]:
                client.tick_engine()


ENGINE_TICKER = ClientTicker()


class ProbeClient(Protocol):
    """The slice of ``AgenticHeadlessClient`` the outbound sweeper drives."""

    def probe_stalled_outbound(self) -> None: ...


class OutboundSweeper:
    """Probe pumping connections for outbound stranded without a trigger.

    Inline draining runs synchronously at every enqueue point, so a
    queued frame with no drain in progress can only come from an
    enqueue site that skipped its flush request. One shared task sweeps
    the registered connections each interval and lets each one detect —
    and warn about — such a miss. Registry mutations take a lock; like
    the engine ticker, the task persists after the last removal so
    re-registration never pays a restart.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._clients: set[ProbeClient] = set()
        self._task: asyncio.Task[None] | None = None

    def register(self, client: ProbeClient) -> None:
        """Add ``client`` to the sweep set, starting the shared task once."""
        with self._lock:
            self._clients.add(client)
            if self._task is None or self._task.done():
                loop = asyncio.get_running_loop()
                self._task = loop.create_task(self._run(), name="ahc-outbound-sweeper")

    def unregister(self, client: ProbeClient) -> None:
        """Remove ``client`` from the sweep set."""
        with self._lock:
            self._clients.discard(client)

    async def _run(self) -> None:
        while True:
            await asyncio.sleep(OUTBOUND_IDLE_PROBE_S)
            with self._lock:
                clients = list(self._clients)
            for client in clients:
                client.probe_stalled_outbound()


OUTBOUND_SWEEPER = OutboundSweeper()
