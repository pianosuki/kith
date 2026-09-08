"""Live terminal view of a running cluster's actors and publish-path counters.

A standalone observer for a second terminal while a load-harness run
executes: it discovers the cluster's server processes through ``/proc`` (or
takes explicit ``--hosts`` control endpoints), polls each instance's control
plane read-only, and renders a live map of sampled actor positions plus the
publish-path counters at 2 Hz. The tool issues GET requests only; it has no
code path that mutates server state. It never probes a discovered port to
identify it: a GET landing on a gateway port would feed the binary wire
protocol's decode path, so ``/proc`` evidence that cannot settle the
gateway-vs-control question renders as absence instead of a guess.

The default sample mode keeps at most one request in flight per instance:
a rotating window of single-actor reads (up to 256 per tick, serialized on
one keep-alive connection) plus one full-roster probe request every ten
ticks and the metrics scrape. The opt-in ``--full-roster`` mode pages the
whole roster per tick and is for non-certified viewing. The view is never
load-bearing: killing it mid-run changes nothing about the run or its
report.

Run::

    python -m tools.agent.live_view [--hosts host:port,host:port]
        [--sample 200] [--full-roster] [--interval 1.0] [--record PATH]
        [--once]
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import math
import os
import re
import signal
import sys
import time
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, replace
from pathlib import Path
from typing import Final, Literal, TextIO

from tools.agent.assertions import ServerControl
from tools.agent.load_harness import (
    PhaseCounters,
    _parse_phase_counters,
    _phase_counters_delta,
    _roster_positions,
)
from tools.agent.server_control import ServerControlClient, ServerControlError


__all__ = [
    "CounterStrip",
    "InstanceEndpoint",
    "InstanceFrame",
    "LiveFrame",
    "SampleRotation",
    "ViewMode",
    "discover_endpoints",
    "main",
    "once_frame",
    "render",
]


# ---------------------------------------------------------------------------
# constants
# ---------------------------------------------------------------------------

ViewMode = Literal["sample", "full-roster"]

_SAMPLE_CAP: Final[int] = 256
_DEFAULT_SAMPLE: Final[int] = 200
_DEFAULT_INTERVAL_S: Final[float] = 1.0
_METRICS_INTERVAL_S: Final[float] = 0.5
_RENDER_INTERVAL_S: Final[float] = 0.5
_REQUEST_DEADLINE_FACTOR: Final[float] = 0.45
_BACKOFF_CAP_FACTOR: Final[float] = 8.0
_PROBE_EVERY_TICKS: Final[int] = 10
_STALE_MIN_S: Final[float] = 2.0
_REDISCOVER_STALE_S: Final[float] = 10.0
_MAP_RAMP: Final[str] = " .:-=+*#%@"

_PROBE_PATH: Final[str] = "/query_state?offset=0&limit=1"
_CLUSTER_SERVER_MODULE: Final[bytes] = b"examples.embedded.server"
_TCP_LISTEN_STATE: Final[int] = 0x0A
_TCP_ESTABLISHED_STATE: Final[int] = 0x01
_SOCKET_INODE_RE: Final[re.Pattern[str]] = re.compile(r"^socket:\[(\d+)\]$")
# The gateway-vs-control classification needs the winner's established-peer
# count to clear a floor the control plane can never reach (a handful of
# scrapers): during the login ramp the gateway may still hold fewer peers
# than the control plane, and a guess then feeds the wire decoder.
_GATEWAY_PEER_FLOOR: Final[int] = 8

_COLOR_RESET: Final[str] = "\x1b[0m"
_COLOR_RED: Final[str] = "\x1b[31m"
_COLOR_GREEN: Final[str] = "\x1b[32m"
_COLOR_DIM: Final[str] = "\x1b[2m"
_PANEL_COLORS: Final[tuple[str, ...]] = ("\x1b[36m", "\x1b[35m", "\x1b[33m", "\x1b[32m")


# ---------------------------------------------------------------------------
# frame types
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class InstanceEndpoint:
    """One instance's control-plane endpoint and where it came from."""

    host: str
    port: int
    pid: int | None = None
    origin: Literal["proc", "hosts"] = "hosts"

    @property
    def label(self) -> str:
        """Render the endpoint as ``host:port``."""
        return f"{self.host}:{self.port}"


@dataclass(frozen=True, slots=True)
class CounterStrip:
    """Derived publish-path rates and absolutes between two metrics scrapes.

    Rate fields are per second across the scrape delta; ``None`` marks a
    value the strip could not derive (no previous scrape, or a zero
    denominator), so a render can distinguish "not observed" from a real
    zero: the healthy-at-zero counters are only meaningful once observed.
    """

    dt_s: float
    dispatches_ps: float | None = None
    publishes_ps: float | None = None
    write_deferrals_ps: float | None = None
    deliver_share: float | None = None
    compose_share: float | None = None
    executor_jobs_ps: float | None = None
    compose_deferrals: int | None = None
    delivery_inflight: int | None = None
    worker_inflight: int | None = None
    dispatch_dropped: int | None = None
    delivery_dropped: int | None = None
    tick_dropped: int | None = None
    view_locate_failures: int | None = None


@dataclass(frozen=True, slots=True)
class InstanceFrame:
    """One instance's renderable state: sampled positions, coverage, counters."""

    endpoint: InstanceEndpoint
    actors: tuple[tuple[int, int, int], ...] = ()
    roster_total: int | None = None
    window: tuple[int, int] | None = None
    sampled: int = 0
    absent: int = 0
    counters: CounterStrip | None = None
    roster_age_s: float | None = None
    metrics_age_s: float | None = None
    roster_error: str | None = None
    metrics_error: str | None = None
    stale: bool = False


@dataclass(frozen=True, slots=True)
class LiveFrame:
    """One renderable snapshot across every watched instance."""

    ts_mono_ns: int
    mode: ViewMode
    elapsed_s: float
    sample_size: int
    interval_s: float
    instances: tuple[InstanceFrame, ...]


class SampleRotation:
    """Rotating actor-id window through one instance's id space.

    Actor ids allocate sequentially from 1 per instance, so windows of
    ``sample_size`` ids sweep the roster and wrap; a roster that grows or
    shrinks reshapes the windows on the next tick.
    """

    def __init__(self, sample_size: int) -> None:
        if not 1 <= sample_size <= _SAMPLE_CAP:
            raise ValueError(f"sample size must be in [1, {_SAMPLE_CAP}]")
        self._size = sample_size
        self._index = 0

    @property
    def size(self) -> int:
        """The configured window size in ids."""
        return self._size

    def window(self, total: int, *, base: int = 1) -> tuple[int, int] | None:
        """Return ``(first_id, last_id)`` for the current window.

        Args:
            total: The roster size the probe last reported.
            base: The lowest allocated actor id (1 in the reference game).

        Returns:
            The inclusive id range to read this tick, or ``None`` when the
            roster is empty.
        """
        if total <= 0:
            return None
        windows = max(1, math.ceil(total / self._size))
        index = self._index % windows
        first = base + index * self._size
        last = min(first + self._size - 1, base + total - 1)
        return (first, last)

    def advance(self, total: int) -> None:
        """Advance to the next window; a sweep over a nonempty roster wraps."""
        if total > 0:
            self._index += 1


# ---------------------------------------------------------------------------
# discovery
# ---------------------------------------------------------------------------


def _parse_tcp_tables(texts: Sequence[str]) -> tuple[dict[int, int], dict[int, int]]:
    """Parse ``/proc/net/tcp``-format tables into listener and peer counts.

    Returns ``(listen_by_inode, established_by_port)``. Rows are split on
    whitespace; the local address, state, and inode columns are the only
    ones read, and malformed rows are skipped so a kernel format surprise
    degrades to "nothing discovered" rather than a crash.
    """
    listen_by_inode: dict[int, int] = {}
    established_by_port: dict[int, int] = {}
    for text in texts:
        for line in text.splitlines():
            fields = line.split()
            if len(fields) < 10 or fields[0] == "sl":
                continue
            try:
                port = int(fields[1].rsplit(":", 1)[1], 16)
                state = int(fields[3], 16)
                inode = int(fields[9])
            except ValueError:
                continue
            if state == _TCP_LISTEN_STATE:
                listen_by_inode[inode] = port
            elif state == _TCP_ESTABLISHED_STATE and inode != 0:
                established_by_port[port] = established_by_port.get(port, 0) + 1
    return listen_by_inode, established_by_port


def _pid_socket_inodes(proc_root: Path, pid: int) -> set[int]:
    """Return the socket inodes one pid owns, from its ``fd`` symlinks."""
    fd_dir = proc_root / str(pid) / "fd"
    try:
        entries = os.listdir(fd_dir)
    except OSError:
        return set()
    inodes: set[int] = set()
    for entry in entries:
        with contextlib.suppress(OSError):
            target = os.readlink(fd_dir / entry)
        match = _SOCKET_INODE_RE.match(target)
        if match is not None:
            inodes.add(int(match.group(1)))
    return inodes


def _pid_is_cluster_server(proc_root: Path, pid: int) -> bool:
    """Return whether the pid's argv runs the cluster's server module."""
    try:
        raw = (proc_root / str(pid) / "cmdline").read_bytes()
    except OSError:
        return False
    argv = raw.split(b"\0")
    return any(
        argv[i] == b"-m" and argv[i + 1] == _CLUSTER_SERVER_MODULE for i in range(len(argv) - 1)
    )


def discover_endpoints(proc_root: Path = Path("/proc")) -> tuple[InstanceEndpoint, ...]:
    """Discover the cluster's control-plane endpoints from ``/proc`` alone.

    A server pid contributes its listening ports; when it owns exactly two
    and the many-peer listener clears the gateway peer floor while
    out-peering the other, the many-peer listener is the gateway and the
    other is the control plane. Any other shape (one listener, extra
    listeners, tied or below-floor counts — the login ramp's control-heavy
    window among them) is skipped: a guess that landed an HTTP GET on the
    gateway port would feed the wire decoder, and ambiguity renders as
    absence instead. Ports are never contacted.
    """
    texts: list[str] = []
    for name in ("tcp", "tcp6"):
        with contextlib.suppress(OSError):
            texts.append((proc_root / "net" / name).read_text())
    listen_by_inode, established_by_port = _parse_tcp_tables(texts)
    endpoints: list[InstanceEndpoint] = []
    for entry in os.listdir(proc_root):
        if not entry.isdigit():
            continue
        pid = int(entry)
        if not _pid_is_cluster_server(proc_root, pid):
            continue
        ports = {
            listen_by_inode[inode]
            for inode in _pid_socket_inodes(proc_root, pid)
            if inode in listen_by_inode
        }
        if len(ports) != 2:
            continue
        ordered = sorted(ports, key=lambda p: established_by_port.get(p, 0), reverse=True)
        winner_peers = established_by_port.get(ordered[0], 0)
        if winner_peers < _GATEWAY_PEER_FLOOR:
            continue
        if winner_peers == established_by_port.get(ordered[1], 0):
            continue
        endpoints.append(
            InstanceEndpoint(host="127.0.0.1", port=ordered[1], pid=pid, origin="proc")
        )
    endpoints.sort(key=lambda e: e.pid if e.pid is not None else 0)
    return tuple(endpoints)


# ---------------------------------------------------------------------------
# cadence, backoff, counter strip
# ---------------------------------------------------------------------------


def _next_slot(start: float, interval: float, now: float) -> float:
    """Return the first fixed-cadence slot start at or after ``now``.

    Slots are absolute multiples of ``interval`` from ``start``, so a tick
    that overruns collapses the missed slots instead of bursting catch-up
    ticks (drop-late-not-queue).
    """
    if now <= start:
        return start
    return start + math.ceil((now - start) / interval) * interval


def _backoff_s(base: float, failures: int) -> float:
    """Exponential backoff for consecutive failures, capped at 8x the base."""
    return min(base * _BACKOFF_CAP_FACTOR, base * 2.0 ** min(failures, 16))


def _error_text(exc: BaseException) -> str:
    """Compact one-line rendering of an exception for the status display."""
    text = f"{type(exc).__name__}: {exc}"
    return text if len(text) <= 120 else text[:117] + "..."


def _int_field(mapping: Mapping[str, object], key: str) -> int:
    """Read one integer field from a control-plane JSON object."""
    value = mapping.get(key)
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    raise RuntimeError(f"control response field {key!r} missing or non-integer")


def _counter_strip(
    prev: tuple[PhaseCounters, float] | None, cur: PhaseCounters, cur_t: float
) -> CounterStrip:
    """Derive the rendered strip from the last two metrics scrapes.

    Absolutes read from the latest scrape alone; rates need a previous
    scrape. A scrape that predates a server restart reads lower than the
    current value, so the reused delta helper clamps at zero and the
    restart renders as a lull rather than a negative rate. The server
    records each healthy-at-zero counter only when its drop delta is
    nonzero, so an absent series parses as 0 and that 0 is the healthy
    reading, not an unobserved one.
    """
    strip = CounterStrip(
        dt_s=0.0 if prev is None else max(0.0, cur_t - prev[1]),
        compose_deferrals=cur.compose_deferrals,
        delivery_inflight=cur.delivery_inflight_current,
        worker_inflight=max(0, cur.worker_tasks_submitted - cur.worker_tasks_completed),
        dispatch_dropped=cur.dispatch_dropped,
        delivery_dropped=cur.delivery_dropped,
        tick_dropped=cur.tick_dropped,
        view_locate_failures=cur.view_locate_failures,
    )
    if prev is None:
        return strip
    dt = cur_t - prev[1]
    if dt <= 0:
        return strip
    delta = _phase_counters_delta([prev[0]], [cur])[0]
    refresh = delta.refresh_ns
    return replace(
        strip,
        dispatches_ps=delta.dispatches / dt,
        publishes_ps=delta.publishes / dt,
        write_deferrals_ps=delta.write_deferrals / dt,
        deliver_share=(delta.deliver_ns / refresh) if refresh > 0 else None,
        compose_share=(delta.compose_ns / refresh) if refresh > 0 else None,
        executor_jobs_ps=delta.delivery_jobs / dt,
    )


# ---------------------------------------------------------------------------
# instance poller
# ---------------------------------------------------------------------------


class _InstancePoller:
    """One instance's read-only poll state and cadence loops.

    All mutable state is touched by the poll tasks and read by the render
    loop within one event loop, so no locking is needed. Every failure
    degrades to an error or absent marking on the next frame: the observer
    must never take itself, or the run it watches, down.
    """

    def __init__(
        self,
        endpoint: InstanceEndpoint,
        control: ServerControl,
        *,
        mode: ViewMode,
        sample_size: int,
        interval_s: float,
    ) -> None:
        self.endpoint = endpoint
        self._control = control
        self._mode: ViewMode = mode
        self._interval_s = interval_s
        self._rotation = SampleRotation(sample_size) if mode == "sample" else None
        self._roster_failures = 0
        self._metrics_failures = 0
        self._probe_countdown = 0
        self._actors: tuple[tuple[int, int, int], ...] = ()
        self._roster_total: int | None = None
        self._first_id = 1
        self._window: tuple[int, int] | None = None
        self._sampled = 0
        self._absent = 0
        self._counters: CounterStrip | None = None
        self._prev_scrape: tuple[PhaseCounters, float] | None = None
        self._last_roster_ok: float | None = None
        self._last_metrics_ok: float | None = None
        self._roster_error: str | None = None
        self._metrics_error: str | None = None

    def last_ok(self) -> float | None:
        """The monotonic time of the last successful read on either stream."""
        stamps = [t for t in (self._last_roster_ok, self._last_metrics_ok) if t is not None]
        return max(stamps) if stamps else None

    async def close(self) -> None:
        """Close the underlying keep-alive connection when it owns one."""
        if isinstance(self._control, ServerControlClient):
            await self._control.close()

    async def run_roster(self) -> None:
        """Fixed-cadence roster task; failed slots back off, slots collapse."""
        loop = asyncio.get_running_loop()
        slot = loop.time()
        while True:
            delay = slot - loop.time()
            if delay > 0:
                await asyncio.sleep(delay)
            started = loop.time()
            try:
                if self._mode == "sample":
                    await self._sample_tick(started + self._interval_s)
                else:
                    await self._full_roster_tick(started + self._interval_s)
                self._roster_error = None
                self._last_roster_ok = loop.time()
                self._roster_failures = 0
            except (ServerControlError, OSError, RuntimeError, TimeoutError) as exc:
                self._roster_failures += 1
                self._roster_error = _error_text(exc)
            failures = self._roster_failures
            step = _backoff_s(self._interval_s, failures) if failures else self._interval_s
            slot = _next_slot(slot, step, loop.time())

    async def run_metrics(self) -> None:
        """Fixed-cadence metrics task; failed slots back off, slots collapse."""
        loop = asyncio.get_running_loop()
        slot = loop.time()
        while True:
            delay = slot - loop.time()
            if delay > 0:
                await asyncio.sleep(delay)
            try:
                text = await self._control.metrics()
                counters = _parse_phase_counters(text)
                now = loop.time()
                self._counters = _counter_strip(self._prev_scrape, counters, now)
                self._prev_scrape = (counters, now)
                self._metrics_error = None
                self._metrics_failures = 0
                self._last_metrics_ok = now
            except (ServerControlError, OSError, RuntimeError, TimeoutError) as exc:
                self._metrics_failures += 1
                self._metrics_error = _error_text(exc)
            failures = self._metrics_failures
            step = _backoff_s(_METRICS_INTERVAL_S, failures) if failures else _METRICS_INTERVAL_S
            slot = _next_slot(slot, step, loop.time())

    async def poll_once(self) -> None:
        """One roster tick plus one metrics scrape; failures mark, not raise."""
        loop = asyncio.get_running_loop()
        started = loop.time()
        try:
            if self._mode == "sample":
                await self._sample_tick(started + self._interval_s)
            else:
                await self._full_roster_tick(started + self._interval_s)
            self._roster_error = None
            self._last_roster_ok = loop.time()
            self._roster_failures = 0
        except (ServerControlError, OSError, RuntimeError, TimeoutError) as exc:
            self._roster_failures += 1
            self._roster_error = _error_text(exc)
        try:
            text = await self._control.metrics()
        except (ServerControlError, OSError, RuntimeError, TimeoutError) as exc:
            self._metrics_failures += 1
            self._metrics_error = _error_text(exc)
            return
        counters = _parse_phase_counters(text)
        now = loop.time()
        self._counters = _counter_strip(self._prev_scrape, counters, now)
        self._prev_scrape = (counters, now)
        self._metrics_error = None
        self._metrics_failures = 0
        self._last_metrics_ok = now

    async def _sample_tick(self, deadline: float) -> None:
        """One rotating-window tick; reads stop past the tick's deadline."""
        loop = asyncio.get_running_loop()
        assert self._rotation is not None
        self._probe_countdown -= 1
        if self._roster_total is None or self._probe_countdown <= 0:
            await self._probe()
            self._probe_countdown = _PROBE_EVERY_TICKS
        total = self._roster_total or 0
        window = self._rotation.window(total, base=self._first_id)
        self._window = window
        sampled = 0
        absent = 0
        actors: list[tuple[int, int, int]] = []
        try:
            if window is not None:
                for actor_id in range(window[0], window[1] + 1):
                    if loop.time() >= deadline:
                        break
                    sampled += 1
                    try:
                        actors.append(await self._read_actor(actor_id))
                    except ServerControlError as exc:
                        if exc.status == 404:
                            absent += 1
                            continue
                        raise
        finally:
            self._actors = tuple(actors)
            self._sampled = sampled
            self._absent = absent
            self._rotation.advance(total)

    async def _full_roster_tick(self, deadline: float) -> None:
        """One paged whole-roster fold, bounded by the tick's deadline."""
        loop = asyncio.get_running_loop()
        remaining = deadline - loop.time()
        positions = await asyncio.wait_for(
            _roster_positions(self._control), timeout=max(0.05, remaining)
        )
        self._actors = tuple((actor_id, x, y) for actor_id, (x, y, _z) in sorted(positions.items()))
        self._roster_total = len(positions)
        self._window = None
        self._sampled = len(positions)
        self._absent = 0

    async def _probe(self) -> None:
        """Learn the roster size and first id with one page request.

        One request of exactly the class a full-roster tick issues 32 times,
        re-run every ``_PROBE_EVERY_TICKS`` ticks so bootstrap growth (and
        the coverage marker that depends on it) stays honest.
        """
        mapping = await self._control.get(_PROBE_PATH)
        actors = mapping.get("actors")
        if not isinstance(actors, list):
            raise RuntimeError("/query_state probe response malformed")
        total_raw = mapping.get("total")
        if isinstance(total_raw, int) and not isinstance(total_raw, bool):
            total = total_raw
        else:
            # A server without pagination ignores the query params and
            # returns the whole roster, whose length is then the total.
            total = len(actors)
        first = 1
        if actors and isinstance(actors[0], dict):
            raw = actors[0].get("actor_id")
            if isinstance(raw, int) and not isinstance(raw, bool):
                first = raw
        self._roster_total = max(0, total)
        self._first_id = first

    async def _read_actor(self, actor_id: int) -> tuple[int, int, int]:
        """Read one actor's position; a missing actor raises its 404."""
        mapping = await self._control.get(f"/query_state?actor_id={actor_id}")
        return (actor_id, _int_field(mapping, "pos_x"), _int_field(mapping, "pos_y"))

    def snapshot(self, now_mono: float) -> InstanceFrame:
        """Freeze the current state into a renderable frame."""
        roster_age = now_mono - self._last_roster_ok if self._last_roster_ok is not None else None
        metrics_age = (
            now_mono - self._last_metrics_ok if self._last_metrics_ok is not None else None
        )
        stale_after = max(_STALE_MIN_S, 2.0 * self._interval_s)
        # Staleness keys on the staler stream: a dead metrics feed must
        # flag the panel even while the roster stream keeps answering.
        ages = [age for age in (roster_age, metrics_age) if age is not None]
        return InstanceFrame(
            endpoint=self.endpoint,
            actors=self._actors,
            roster_total=self._roster_total,
            window=self._window,
            sampled=self._sampled,
            absent=self._absent,
            counters=self._counters,
            roster_age_s=roster_age,
            metrics_age_s=metrics_age,
            roster_error=self._roster_error,
            metrics_error=self._metrics_error,
            stale=not ages or max(ages) > stale_after,
        )


def _make_poller(
    endpoint: InstanceEndpoint, *, mode: ViewMode, sample_size: int, interval_s: float
) -> _InstancePoller:
    """Build a poller with its own keep-alive control client.

    The per-request deadline sits strictly below both poll cadences, so a
    stalled control plane delays a request, not a cadence.
    """
    timeout_s = _REQUEST_DEADLINE_FACTOR * min(_METRICS_INTERVAL_S, interval_s)
    control: ServerControl = ServerControlClient(endpoint.host, endpoint.port, timeout_s=timeout_s)
    return _InstancePoller(
        endpoint, control, mode=mode, sample_size=sample_size, interval_s=interval_s
    )


# ---------------------------------------------------------------------------
# render
# ---------------------------------------------------------------------------


def _map_bounds(
    actors: Sequence[tuple[int, int, int]],
) -> tuple[float, float, float, float] | None:
    """Auto-fitted ``(xmin, ymin, xmax, ymax)`` with padding; None when empty.

    A zero-span axis pads half a unit each way, so a roster moving on one
    line (the reference game's +x sweep) still spans a renderable map.
    """
    if not actors:
        return None
    xs = [actor[1] for actor in actors]
    ys = [actor[2] for actor in actors]
    pad_x = max((max(xs) - min(xs)) * 0.05, 0.5)
    pad_y = max((max(ys) - min(ys)) * 0.05, 0.5)
    return (min(xs) - pad_x, min(ys) - pad_y, max(xs) + pad_x, max(ys) + pad_y)


def _map_grid(
    actors: Sequence[tuple[int, int, int]],
    bounds: tuple[float, float, float, float],
    cols: int,
    rows: int,
) -> list[list[str]]:
    """Bucket positions into a ``rows x cols`` grid, glyph density by count."""
    xmin, ymin, xmax, ymax = bounds
    span_x = max(xmax - xmin, 1e-9)
    span_y = max(ymax - ymin, 1e-9)
    counts: dict[tuple[int, int], int] = {}
    for _actor_id, x, y in actors:
        col = min(cols - 1, int((x - xmin) / span_x * cols))
        row = min(rows - 1, int((ymax - y) / span_y * rows))
        counts[(row, col)] = counts.get((row, col), 0) + 1
    peak = max(counts.values(), default=0)
    grid = [[" "] * cols for _ in range(rows)]
    for (row, col), count in counts.items():
        index = 1 if peak <= 1 else 1 + (count - 1) * (len(_MAP_RAMP) - 2) // (peak - 1)
        grid[row][col] = _MAP_RAMP[index]
    return grid


def _rate(value: float | None) -> str:
    """Format a per-second rate; ``n/a`` when not observed."""
    return "n/a" if value is None else f"{value:.0f}/s"


def _abs(value: int | None) -> str:
    """Format an absolute counter; ``n/a`` when not observed."""
    return "n/a" if value is None else str(value)


def _pct(value: float | None) -> str:
    """Format a tick share as a percentage; ``n/a`` when not observed."""
    return "n/a" if value is None else f"{value * 100:.0f}%"


def _counter_rows(inst: InstanceFrame) -> list[tuple[str, str]]:
    """The three counter-strip rows: rates, shares/gauges, healthy-at-zero."""
    if inst.metrics_error is not None:
        return [(f"metrics ERR {inst.metrics_error}", "plain"), ("", "plain"), ("", "plain")]
    strip = inst.counters
    if strip is None:
        return [("counters n/a", "plain"), ("", "plain"), ("", "plain")]
    line1 = (
        f"dispatch {_rate(strip.dispatches_ps)}  publish {_rate(strip.publishes_ps)}  "
        f"wdeferr {_rate(strip.write_deferrals_ps)}  cdeferr {_abs(strip.compose_deferrals)}"
    )
    line2 = (
        f"deliver {_pct(strip.deliver_share)}  compose {_pct(strip.compose_share)}  "
        f"exec {_rate(strip.executor_jobs_ps)}  inflight {_abs(strip.delivery_inflight)}  "
        f"pool {_abs(strip.worker_inflight)}"
    )
    dropped = (
        strip.dispatch_dropped,
        strip.delivery_dropped,
        strip.tick_dropped,
        strip.view_locate_failures,
    )
    detail = (
        f"dispatch={_abs(strip.dispatch_dropped)} delivery={_abs(strip.delivery_dropped)} "
        f"tick={_abs(strip.tick_dropped)} view_locate={_abs(strip.view_locate_failures)}"
    )
    if all(value is not None for value in dropped):
        if any((value or 0) != 0 for value in dropped):
            return [(line1, "plain"), (line2, "plain"), (f"DROPPED {detail}", "alert")]
        return [(line1, "plain"), (line2, "plain"), (f"dropped {detail}  ok", "ok")]
    return [(line1, "plain"), (line2, "plain"), (f"dropped {detail}", "plain")]


def _coverage_text(inst: InstanceFrame, mode: ViewMode) -> str:
    """How much of the roster this frame represents (the coverage marker)."""
    total = "n/a" if inst.roster_total is None else str(inst.roster_total)
    if mode == "full-roster":
        text = f"full {total}"
    elif inst.window is not None:
        first, last = inst.window
        text = f"win {first}-{last}/{total}"
    else:
        text = f"win 0/{total}"
    if inst.absent:
        text += f" absent {inst.absent}"
    return text


def _panel_lines(
    inst: InstanceFrame,
    *,
    index: int,
    mode: ViewMode,
    width: int,
    map_h: int,
) -> list[tuple[str, str]]:
    """One instance's panel rows as ``(text, kind)`` pairs.

    Kinds select the color treatment: ``header``/``map`` take the instance
    color, ``alert`` red, ``ok`` green, ``plain`` none.
    """
    head = f"instance {index}  {inst.endpoint.label}"
    if inst.endpoint.pid is not None:
        head += f"  pid {inst.endpoint.pid}"
    age = "n/a" if inst.roster_age_s is None else f"{inst.roster_age_s:.1f}s"
    if inst.stale:
        head += f"  STALE {age}"
        head_kind = "alert"
    elif inst.roster_error is not None:
        # The error leads the truncated header: it is the signal the owner
        # needs, and age/coverage can return when the stream recovers.
        head += f"  ERR {inst.roster_error}"
        head_kind = "alert"
    else:
        head += f"  age {age}  {_coverage_text(inst, mode)}"
        head_kind = "header"
    rows: list[tuple[str, str]] = [(head[:width], head_kind)]
    bounds = _map_bounds(inst.actors)
    if bounds is None:
        rows.extend(((" " * width, "map")) for _ in range(map_h))
    else:
        grid = _map_grid(inst.actors, bounds, width, map_h)
        rows.extend(("".join(row), "map") for row in grid)
    rows.extend(_counter_rows(inst))
    return rows


def render(frame: LiveFrame, width: int, height: int, *, color: bool = True) -> str:
    """Render one frame as a fixed-size terminal grid.

    Pure: the frame carries all state (including the elapsed clock), so the
    same frame renders byte-identically at a given size and color flag.

    Args:
        frame: The snapshot to render.
        width: Terminal width in columns; clamped to at least 20.
        height: Terminal height in rows; clamped to at least 6.
        color: Emit ANSI color codes.

    Returns:
        The rendered screen: exactly ``height`` lines joined with newlines,
        every line padded to ``width``.
    """
    width = max(width, 20)
    height = max(height, 6)
    mode_desc = f"{frame.mode}({frame.sample_size})" if frame.mode == "sample" else frame.mode
    title = (
        f"kith live view  elapsed {_fmt_elapsed(frame.elapsed_s)}  mode {mode_desc}  "
        f"interval {frame.interval_s:g}s"
    )
    lines: list[str] = [_pad(_colorize(title, _COLOR_DIM, color), width)]
    if not frame.instances:
        note = "no instances discovered; pass --hosts or start the cluster's server processes"
        lines.append(_pad(note, width))
        lines.extend(_pad("", width) for _ in range(height - 2))
        return "\n".join(lines[:height])
    count = len(frame.instances)
    gutter = 1 if count > 1 else 0
    panel_w = (width - gutter * (count - 1)) // count
    panel_h = height - 1
    map_h = max(1, panel_h - 4)
    panels = [
        _panel_lines(inst, index=index, mode=frame.mode, width=panel_w, map_h=map_h)
        for index, inst in enumerate(frame.instances)
    ]
    for row in range(panel_h):
        segments: list[str] = []
        for index in range(count):
            panel = panels[index]
            if row < len(panel):
                text, kind = panel[row]
            else:
                text, kind = "", "plain"
            text = _pad(text, panel_w)
            if kind in ("header", "map"):
                text = _colorize(text, _PANEL_COLORS[index % len(_PANEL_COLORS)], color)
            elif kind == "alert":
                text = _colorize(text.rstrip(), _COLOR_RED, color)
                text = _pad(text, panel_w)
            elif kind == "ok":
                text = _colorize(text.rstrip(), _COLOR_GREEN, color)
                text = _pad(text, panel_w)
            segments.append(text)
        lines.append(_pad(" ".join(segments), width))
    return "\n".join(lines[:height])


# ---------------------------------------------------------------------------
# record, once, watch
# ---------------------------------------------------------------------------


def _instance_record(inst: InstanceFrame) -> dict[str, object]:
    """The JSONL record for one instance's reading within a frame."""
    bounds = _map_bounds(inst.actors)
    return {
        "endpoint": inst.endpoint.label,
        "bounds": list(bounds) if bounds is not None else None,
        "actors": [[actor_id, x, y] for actor_id, x, y in inst.actors],
        "counters": asdict(inst.counters) if inst.counters is not None else None,
    }


def _record_frame(frame: LiveFrame) -> dict[str, object]:
    """The JSONL record for one frame: the observer's own readings."""
    return {
        "ts_mono_ns": frame.ts_mono_ns,
        "mode": frame.mode,
        "instances": [_instance_record(inst) for inst in frame.instances],
    }


class _Recorder:
    """Append-only JSONL frame log; opened lazily, flushed per line."""

    def __init__(self, path: str) -> None:
        self._path = Path(path)
        self._handle: TextIO | None = None

    def write(self, frame: LiveFrame) -> None:
        """Append one frame's record line."""
        if self._handle is None:
            self._handle = self._path.open("a", encoding="utf-8")
        self._handle.write(json.dumps(_record_frame(frame), separators=(",", ":")) + "\n")
        self._handle.flush()

    def close(self) -> None:
        """Close the record file if one was opened."""
        if self._handle is not None:
            self._handle.close()
            self._handle = None


def _terminal_size() -> tuple[int, int]:
    """The stdout terminal size, or a fixed fallback when stdout is not one."""
    try:
        size = os.get_terminal_size(sys.stdout.fileno())
    except (OSError, ValueError) as _exc:
        return (100, 30)
    return (max(size.columns, 20), max(size.lines, 6))


def _fmt_elapsed(seconds: float) -> str:
    """Format an elapsed duration as ``mm:ss.d``."""
    minutes, rest = divmod(max(0.0, seconds), 60.0)
    return f"{int(minutes):02d}:{rest:04.1f}"


def _pad(text: str, width: int) -> str:
    """Pad ``text`` with trailing spaces to ``width``, truncating overflow."""
    return text.ljust(width)[:width]


def _colorize(text: str, code: str, enabled: bool) -> str:
    """Wrap ``text`` in an ANSI color when enabled; empty text stays bare."""
    if not enabled or not text:
        return text
    return f"{code}{text}{_COLOR_RESET}"


async def once_frame(
    endpoints: Sequence[InstanceEndpoint],
    *,
    mode: ViewMode = "sample",
    sample_size: int = _DEFAULT_SAMPLE,
    interval_s: float = _DEFAULT_INTERVAL_S,
    record_path: str | None = None,
    color: bool = False,
) -> LiveFrame:
    """Run one poll pass against explicit endpoints and return the frame.

    Performs one roster tick and one metrics scrape per endpoint, tolerating
    per-instance failure (errors render as absence), then renders one frame
    to stdout. This is the tests' and the gate-day smoke's entry point: the
    frame's per-instance fields tell the caller exactly what was seen.

    Args:
        endpoints: Explicit control endpoints, in instance order.
        mode: Sample or full-roster polling.
        sample_size: Rotating window size (sample mode).
        interval_s: Roster cadence; also bounds the once pass's deadline.
        record_path: When set, append the frame's JSONL record here.
        color: Emit ANSI color codes in the rendered frame.

    Returns:
        The frame the pass produced.

    Raises:
        ValueError: If ``sample_size`` is outside ``[1, 256]``.
    """
    loop = asyncio.get_running_loop()
    pollers = [
        _make_poller(endpoint, mode=mode, sample_size=sample_size, interval_s=interval_s)
        for endpoint in endpoints
    ]
    try:
        for poller in pollers:
            await poller.poll_once()
        frame = LiveFrame(
            ts_mono_ns=time.monotonic_ns(),
            mode=mode,
            elapsed_s=0.0,
            sample_size=sample_size,
            interval_s=interval_s,
            instances=tuple(poller.snapshot(loop.time()) for poller in pollers),
        )
        sys.stdout.write(render(frame, *_terminal_size(), color=color) + "\n")
        if record_path is not None:
            recorder = _Recorder(record_path)
            recorder.write(frame)
            recorder.close()
        return frame
    finally:
        for poller in pollers:
            await poller.close()


def _once_exit_code(frame: LiveFrame) -> int:
    """0 when at least one instance produced a reading, 1 otherwise."""
    for inst in frame.instances:
        if inst.roster_total is not None or inst.counters is not None:
            return 0
    return 1


def _all_stale(pollers: Sequence[_InstancePoller], now_mono: float) -> bool:
    """Whether every poller has been silent past the rediscovery threshold."""
    if not pollers:
        return True
    return all(
        (stamp := poller.last_ok()) is None or now_mono - stamp > _REDISCOVER_STALE_S
        for poller in pollers
    )


async def _watch(
    args: argparse.Namespace, endpoints: tuple[InstanceEndpoint, ...], mode: ViewMode
) -> int:
    """The interactive watch loop; returns the process exit status."""
    loop = asyncio.get_running_loop()
    pollers = [
        _make_poller(endpoint, mode=mode, sample_size=args.sample, interval_s=args.interval)
        for endpoint in endpoints
    ]
    tasks: list[asyncio.Task[None]] = []
    for poller in pollers:
        tasks.append(
            asyncio.create_task(
                poller.run_roster(), name=f"live-view-roster:{poller.endpoint.label}"
            )
        )
        tasks.append(
            asyncio.create_task(
                poller.run_metrics(), name=f"live-view-metrics:{poller.endpoint.label}"
            )
        )
    recorder = _Recorder(args.record) if args.record is not None else None
    start_mono = time.monotonic()
    stop = asyncio.Event()

    def _request_stop() -> None:
        stop.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, _request_stop)

    out = sys.stdout
    interactive = out.isatty()
    if interactive:
        out.write("\x1b[?1049h\x1b[H\x1b[?25l")
        out.flush()
    try:
        slot = loop.time()
        while True:
            now_mono = time.monotonic()
            frame = LiveFrame(
                ts_mono_ns=time.monotonic_ns(),
                mode=mode,
                elapsed_s=now_mono - start_mono,
                sample_size=args.sample,
                interval_s=args.interval,
                instances=tuple(poller.snapshot(now_mono) for poller in pollers),
            )
            frame_code, clear_code = ("\x1b[H", "\x1b[0J") if interactive else ("", "")
            out.write(frame_code + render(frame, *_terminal_size(), color=interactive) + clear_code)
            out.flush()
            if recorder is not None:
                recorder.write(frame)
            # With discovered (not explicit) endpoints, a cluster that went
            # away and came back on new ports re-attaches; explicit hosts
            # stay fixed because the owner chose them.
            if args.hosts is None and _all_stale(pollers, now_mono):
                fresh = discover_endpoints()
                fresh_keys = [(e.host, e.port, e.pid) for e in fresh]
                current_keys = [(p.endpoint.host, p.endpoint.port, p.endpoint.pid) for p in pollers]
                if fresh_keys and fresh_keys != current_keys:
                    for task in tasks:
                        task.cancel()
                    await asyncio.gather(*tasks, return_exceptions=True)
                    for poller in pollers:
                        await poller.close()
                    pollers = [
                        _make_poller(
                            endpoint, mode=mode, sample_size=args.sample, interval_s=args.interval
                        )
                        for endpoint in fresh
                    ]
                    tasks = []
                    for poller in pollers:
                        tasks.append(
                            asyncio.create_task(
                                poller.run_roster(),
                                name=f"live-view-roster:{poller.endpoint.label}",
                            )
                        )
                        tasks.append(
                            asyncio.create_task(
                                poller.run_metrics(),
                                name=f"live-view-metrics:{poller.endpoint.label}",
                            )
                        )
            slot = _next_slot(slot, _RENDER_INTERVAL_S, loop.time())
            delay = max(0.0, slot - loop.time())
            try:
                await asyncio.wait_for(stop.wait(), timeout=delay)
                break
            except TimeoutError:
                pass
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        for poller in pollers:
            await poller.close()
        if recorder is not None:
            recorder.close()
        if interactive:
            out.write("\x1b[0J\x1b[?25h\x1b[?1049l")
            out.flush()
    return 0


def _parse_hosts(spec: str) -> tuple[InstanceEndpoint, ...]:
    """Parse ``host:port,host:port`` into explicit endpoints, order preserved."""
    endpoints: list[InstanceEndpoint] = []
    for part in spec.split(","):
        part = part.strip()
        if not part:
            continue
        host, sep, port_text = part.rpartition(":")
        if not sep or not host:
            raise ValueError(f"invalid --hosts entry {part!r}: expected host:port")
        try:
            port = int(port_text)
        except ValueError as exc:
            raise ValueError(f"invalid --hosts entry {part!r}: port is not a number") from exc
        if not 1 <= port <= 65535:
            raise ValueError(f"invalid --hosts entry {part!r}: port out of range")
        endpoints.append(InstanceEndpoint(host=host, port=port, origin="hosts"))
    if not endpoints:
        raise ValueError("--hosts matched no endpoints")
    return tuple(endpoints)


def _build_parser() -> argparse.ArgumentParser:
    """The tool's CLI surface."""
    parser = argparse.ArgumentParser(
        prog="python -m tools.agent.live_view",
        description=(
            "Live read-only terminal view of a running cluster's actor "
            "positions and publish-path counters."
        ),
    )
    parser.add_argument(
        "--hosts",
        default=None,
        help="explicit control endpoints host:port[,host:port] (default: /proc discovery)",
    )
    parser.add_argument(
        "--sample",
        type=int,
        default=_DEFAULT_SAMPLE,
        help=f"rotating sample size per instance, 1-{_SAMPLE_CAP} (default {_DEFAULT_SAMPLE})",
    )
    parser.add_argument(
        "--full-roster",
        action="store_true",
        help="page the whole roster per tick (non-certified viewing)",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=_DEFAULT_INTERVAL_S,
        help=f"seconds between roster ticks (default {_DEFAULT_INTERVAL_S:g})",
    )
    parser.add_argument(
        "--record",
        default=None,
        help="append rendered frames to this JSONL file (keep it outside the run's artifact tree)",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="one poll + render pass, then exit (tests and gate-day smoke)",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run the live view; returns the process exit status.

    Args:
        argv: Argument vector; defaults to ``sys.argv[1:]``.

    Returns:
        0 on a completed watch or a once pass that reached at least one
        instance, 1 when nothing was discovered or nothing responded.
    """
    parser = _build_parser()
    args = parser.parse_args(argv)
    if not 1 <= args.sample <= _SAMPLE_CAP:
        parser.error(f"--sample must be in [1, {_SAMPLE_CAP}]")
    if args.interval <= 0:
        parser.error("--interval must be positive")
    # Runs niced and pinned to nothing: the view takes neither pinned CPU
    # slice and yields to everything on the host.
    with contextlib.suppress(OSError):
        os.nice(10)
    try:
        endpoints = _parse_hosts(args.hosts) if args.hosts is not None else discover_endpoints()
    except ValueError as exc:
        parser.error(str(exc))
    if not endpoints:
        print(
            "live_view: no cluster server processes discovered "
            "(use --hosts to pass explicit control endpoints)",
            file=sys.stderr,
        )
        return 1
    mode: ViewMode = "full-roster" if args.full_roster else "sample"
    try:
        if args.once:
            frame = asyncio.run(
                once_frame(
                    endpoints,
                    mode=mode,
                    sample_size=args.sample,
                    interval_s=args.interval,
                    record_path=args.record,
                    color=sys.stdout.isatty(),
                )
            )
            return _once_exit_code(frame)
        return asyncio.run(_watch(args, endpoints, mode))
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    sys.exit(main())
