"""Contended apply-capacity micro-bench for the spatial handler set.

Boots the embedded-topology server stack in-process and drives the
apply path (handler lock -> tile2d apply/step -> sim artifact publish)
from N always-backlogged threads, measuring the aggregate apply throughput
the path sustains under three lock topologies:

- ``real-single``: the shipped handler set (its single handler lock).
- ``replica-<k>``: a bench-local replica of the exact per-apply sequence
  under ``k`` stripe locks, actor id -> stripe by modulo. ``k=1`` is the
  single-lock replica that cross-validates the replica against the
  shipped path under identical conditions.

The replica shares the server's model instance and publish facade, so a
per-stripe critical section performs the same work as the shipped path's
single critical section; only the lock object differs. Throughput numbers
from this bench are bench-environment numbers (no wire traffic, no worker
pool, no second instance): compare configs against each other, and
calibrate the single-lock config against independently measured service
truth externally before any capacity decision. The report's
``calibration`` block carries the internal ratios that comparison needs.

``--c-native`` runs the C driver instead (tools/perf/c_apply_bench.c,
built as ``c_apply_bench``): the same per-input apply sequence — decode,
bind-map resolve, stripe lock, model apply + step, store publish, dirty
marks, record append, plus the per-tick flush — with no Python on the
measured path, reporting per-apply thread-CPU cost. This module locates
the binary, converts the behavior grid, runs both tick modes, and assembles
the JSON report with the run envelope.

Run on the machine whose core placement matches the deployment slice
(affinity is set externally with ``taskset``; the recorded envelope
captures it as found).
"""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import platform
import random
import subprocess
import sys
import tempfile
import threading
import time
from collections.abc import Callable, Sequence
from dataclasses import asdict, dataclass, field, replace
from datetime import UTC, datetime
from functools import partial
from itertools import count
from pathlib import Path
from typing import TypedDict

from examples.embedded.server import EmbeddedServer
from examples.spatial.handlers import SpatialHandlers
from examples.tile_rpg.zone import tmx_to_text_grid

from kith import (
    Actor,
    ArtifactKey,
    CellKey,
    KithStateError,
    Server,
    ServerStatus,
    SimInput,
    SimModel,
)


_TICK_PERIOD_S: float = 0.05  # the embedded server's tick cadence
_SCHEMA: str = "apply-capacity-bench/1"
_SCHEMA_C_NATIVE: str = "apply-capacity-bench-c/1"
_TILE_FIX: int = 1 << 16  # one tile in the sim's Q16.16 fixed-point units
_WORLD_TILES: int = 8  # the embedded world's tile extent (examples/embedded/world.tmx)
_MOVE_MAGNITUDE: int = 200  # the harness movement scenario's input deflection
# The tile2d border does not block out-of-bounds positions (out-of-grid tiles
# are skipped by the collision check), so unsteered actors escape the world
# and their position-derived cells diverge without bound. The patrol steers
# each actor back at half a tile from the border; one apply at the scenario
# deflection travels ~0.2 tiles, so reflected actors stay inside the world.
_PATROL_LOW_FIX: int = _TILE_FIX // 2
_PATROL_HIGH_FIX: int = _WORLD_TILES * _TILE_FIX - _TILE_FIX // 2

# The embedded world's TMX map; the C-native driver consumes it as the
# tile2d text behavior grid (converted at run time into a scratch file).
_WORLD_TMX: Path = Path(__file__).resolve().parents[2] / "examples" / "embedded" / "world.tmx"

# The C driver's binary name and the AGG line-record field order it prints
# (mirrors bench_print_report in tools/perf/c_apply_bench.c).
_C_DRIVER_NAME: str = "c_apply_bench"
_AGG_FIELDS: tuple[str, ...] = (
    "threads",
    "tick_enabled",
    "applies_total",
    "elapsed_s",
    "applies_per_s",
    "cpu_mean_us",
    "cpu_p50_us",
    "cpu_p95_us",
    "cpu_p99_us",
    "wall_mean_us",
    "wall_p50_us",
    "wall_p95_us",
    "wall_p99_us",
    "total_cpu_us_per_apply",
    "flush_cpu_us_per_apply",
    "flush_total_cpu_s",
    "flush_count",
    "fabric_publishes",
    "eperm_suppressed",
    "dirty_cells_drained",
    "drain_products",
    "records_appended",
)
_WIN_FIELDS: tuple[str, ...] = (
    "threads",
    "tick",
    "window",
    "applies",
    "elapsed_s",
    "cpu_mean_us",
    "wall_mean_us",
)


class CNativeAggregate(TypedDict):
    """One (threads, tick-mode) config's pooled numbers from the C driver."""

    threads: int
    tick_enabled: int
    applies_total: int
    elapsed_s: float
    applies_per_s: float
    cpu_mean_us: float
    cpu_p50_us: float
    cpu_p95_us: float
    cpu_p99_us: float
    wall_mean_us: float
    wall_p50_us: float
    wall_p95_us: float
    wall_p99_us: float
    total_cpu_us_per_apply: float
    flush_cpu_us_per_apply: float
    flush_total_cpu_s: float
    flush_count: int
    fabric_publishes: int
    eperm_suppressed: int
    dirty_cells_drained: int
    drain_products: int
    records_appended: int


class CNativeWindowRow(TypedDict):
    """One timed window's headline numbers from the C driver."""

    threads: int
    tick: int
    window: int
    applies: int
    elapsed_s: float
    cpu_mean_us: float
    wall_mean_us: float


class CNativeReport(TypedDict):
    """The C-native bench report: schema, params, aggregates, windows."""

    schema: str
    params: dict[str, object]
    aggregates: list[CNativeAggregate]
    windows: list[CNativeWindowRow]
    envelope: dict[str, object]


@dataclass(frozen=True)
class BenchConfig:
    """Parameters for one bench invocation.

    Attributes:
        threads: Worker thread counts to sweep (one full config pass each).
        stripes: Stripe-lock counts for the replica configs.
        actors: Actor population; the deployment point runs 1000.
        windows: Timed windows per config (warmup is separate, discarded).
        window_s: Duration of one timed window.
        warmup_s: Duration of the discarded warmup window per config.
        dt_ms: Per-apply step duration, matching the wire handler.
        seed: Root seed for the per-thread deterministic schedules.
        tick_enabled: Run the server's tick loop (real flush cadence for
            the shipped config; the replica runs its matching mimic). When
            false, the bench isolates pure apply capacity with no flush.
        require_free_threaded: Refuse to run unless the interpreter has the
            GIL disabled. Structural smoke runs may relax this; contention
            numbers from a GIL build are meaningless.
    """

    threads: list[int] = field(default_factory=lambda: [8])
    stripes: list[int] = field(default_factory=lambda: [1, 64])
    actors: int = 1000
    windows: int = 5
    window_s: float = 1.0
    warmup_s: float = 1.0
    dt_ms: int = 50
    seed: int = 12345
    tick_enabled: bool = True
    require_free_threaded: bool = True


@dataclass(frozen=True)
class WindowSample:
    """One timed window's headline numbers."""

    config: str
    threads: int
    stripes: int
    window: int
    applies: int
    elapsed_s: float
    applies_per_s: float
    p50_us: float
    p95_us: float
    p99_us: float
    hold_fraction: float | None


@dataclass(frozen=True)
class ConfigAggregate:
    """One config's pooled numbers across all its timed windows."""

    config: str
    threads: int
    stripes: int
    applies_total: int
    elapsed_s: float
    applies_per_s: float
    mean_us: float
    p50_us: float
    p95_us: float
    p99_us: float
    hold_fraction: float | None


@dataclass(frozen=True)
class BenchReport:
    """The full bench record: envelope, parameters, samples, calibration."""

    envelope: dict[str, object]
    params: dict[str, object]
    windows: list[WindowSample]
    aggregates: list[ConfigAggregate]
    calibration: dict[str, dict[str, float | None]]

    def to_json_dict(self) -> dict[str, object]:
        """Return the JSON-serializable report with its schema tag."""
        return {
            "schema": _SCHEMA,
            "envelope": self.envelope,
            "params": self.params,
            "windows": [asdict(sample) for sample in self.windows],
            "aggregates": [asdict(aggregate) for aggregate in self.aggregates],
            "calibration": self.calibration,
        }


class _ThreadTally:
    """Per-thread accumulators; each thread writes only its own instance."""

    __slots__ = ("applies", "error", "holds", "latencies")

    def __init__(self) -> None:
        self.applies: int = 0
        self.latencies: list[int] = []
        self.holds: list[int] = []
        self.error: BaseException | None = None


class _ShardedApplyReplica:
    """Bench-local replica of the handler set's apply sequence under stripe locks.

    Reproduces the shipped single-lock path's per-apply work (table read,
    actor copy, model apply and step, table store, position-derived
    publish, actor-cell bookkeeping, dirty-cell marks) against the same
    model instance and publish facade, differing only in lock topology:
    one lock per stripe, actor id -> stripe by modulo. Owns its actor
    table, dirty set, and epoch table.

    Known bias vs the shipped path, both in the replica's favor: the real
    per-tick flush runs on a worker-pool thread and competes with applies
    for pool slots, while the replica's flush mimic runs on a dedicated
    thread and competes only for the bookkeeping lock; and the mimic's
    dirty-set snapshot never waits behind a full apply hold as the real
    flush's snapshot does behind the single handler lock.
    """

    def __init__(
        self,
        *,
        model: SimModel,
        publish_artifact: Callable[[ArtifactKey, Actor], int],
        publish_cell_product: Callable[[CellKey, int], int],
        zone_id: int,
        cell_size: int,
        stripes: int,
        actors: dict[int, Actor],
        tick_period_s: float | None,
    ) -> None:
        self._model = model
        self._publish_artifact = publish_artifact
        self._publish_cell_product = publish_cell_product
        self._zone_id = zone_id
        self._cell_size = cell_size
        self._stripes = [threading.Lock() for _ in range(stripes)]
        self._actors = actors
        self._actor_cells: dict[int, tuple[int, int, int]] = {}
        self._book = threading.Lock()
        self._dirty: set[CellKey] = set()
        self._epochs: dict[CellKey, int] = {}
        self._flush_stop = threading.Event() if tick_period_s is not None else None
        self._flush_thread: threading.Thread | None = None
        self._flush_error: BaseException | None = None
        if tick_period_s is not None:
            self._flush_thread = threading.Thread(
                target=self._flush_loop,
                args=(tick_period_s,),
                name="bench-flush-mimic",
                daemon=True,
            )
            self._flush_thread.start()

    def apply_one(self, actor_id: int, inp: SimInput, *, dt_ms: int) -> tuple[int, Actor]:
        """Apply one input under the actor's stripe.

        Returns the lock hold time in nanoseconds and the stepped actor.

        Raises:
            RuntimeError: If the actor id is absent from the replica table.
        """
        stripe = self._stripes[actor_id % len(self._stripes)]
        with stripe:
            hold_begin = time.perf_counter_ns()
            stored = self._actors.get(actor_id)
            if stored is None:
                msg = f"actor {actor_id} missing from the replica table"
                raise RuntimeError(msg)
            actor = replace(stored)
            applied = self._model.apply_input(actor, inp)
            stepped = self._model.step([applied], dt_ms=dt_ms)[0]
            self._actors[actor_id] = stepped
            self._publish(stepped)
            return time.perf_counter_ns() - hold_begin, stepped

    def flush(self) -> None:
        """Drain the dirty set with one cell-product bump per cell.

        Mirrors the handler set's per-tick flush shape: snapshot and clear
        under the bookkeeping lock, one lock-held epoch read-modify-write
        per cell, publish outside the lock.
        """
        with self._book:
            cells = list(self._dirty)
            self._dirty.clear()
        for key in cells:
            with self._book:
                epoch = self._epochs.get(key, 0) + 1
                self._epochs[key] = epoch
            with contextlib.suppress(KithStateError):
                self._publish_cell_product(key, epoch)

    def stop_flush(self) -> None:
        """Stop the flush-mimic thread, if one was started."""
        stop = self._flush_stop
        if stop is not None:
            stop.set()
        thread = self._flush_thread
        if thread is not None:
            thread.join(timeout=5.0)
            self._flush_thread = None

    @property
    def flush_error(self) -> BaseException | None:
        """The first failure the flush-mimic thread hit, if any."""
        return self._flush_error

    def _flush_loop(self, period_s: float) -> None:
        stop = self._flush_stop
        if stop is None:
            return
        while not stop.wait(period_s):
            try:
                self.flush()
            except Exception as exc:  # surfaced to the caller via flush_error
                self._flush_error = exc
                return

    def _publish(self, actor: Actor) -> None:
        """Publish into the position-derived cell and mark dirty cells.

        Runs with the actor's stripe held, mirroring the shipped path's
        publish-under-lock: the store relocates a moved actor atomically,
        and both the arrival and departure cells are marked dirty.
        """
        new_cell = (
            actor.pos_x // self._cell_size,
            actor.pos_y // self._cell_size,
            actor.pos_z // self._cell_size,
        )
        old_cell = self._actor_cells.get(actor.id)
        self._publish_artifact(
            ArtifactKey(
                zone=self._zone_id,
                cell_x=new_cell[0],
                cell_y=new_cell[1],
                cell_z=new_cell[2],
                lod=0,
            ),
            actor,
        )
        self._actor_cells[actor.id] = new_cell
        with self._book:
            self._dirty.add(
                CellKey(
                    zone=self._zone_id,
                    cell_x=new_cell[0],
                    cell_y=new_cell[1],
                    cell_z=new_cell[2],
                    lod=0,
                )
            )
            if old_cell is not None and old_cell != new_cell:
                self._dirty.add(
                    CellKey(
                        zone=self._zone_id,
                        cell_x=old_cell[0],
                        cell_y=old_cell[1],
                        cell_z=old_cell[2],
                        lod=0,
                    )
                )


def _gil_enabled() -> bool:
    """Return whether the running interpreter has the GIL enabled."""
    check = getattr(sys, "_is_gil_enabled", None)
    if check is None:
        return True
    return bool(check())


def _envelope() -> dict[str, object]:
    """Record the execution environment as found."""
    governor: str | None = None
    with contextlib.suppress(OSError):
        governor = (
            Path("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor")
            .read_text(encoding="ascii")
            .strip()
        )
    return {
        "host": platform.node(),
        "python": sys.version,
        "implementation": sys.implementation.name,
        "gil_enabled": _gil_enabled(),
        "kernel": platform.release(),
        "cpus": os.cpu_count(),
        "affinity": sorted(os.sched_getaffinity(0)),
        "governor": governor,
        "timestamp_utc": datetime.now(UTC).isoformat(),
    }


def _boot_stack(tick_enabled: bool) -> tuple[EmbeddedServer, Server, threading.Thread | None]:
    """Start the embedded stack; optionally run its tick loop on a thread.

    Returns the embedded server, the facade, and the run thread (``None``
    when the tick loop is disabled). Raises on boot failure after tearing
    the stack back down.
    """
    embedded = EmbeddedServer()
    facade, _gateway_port, _control_port = embedded.start()
    run_thread: threading.Thread | None = None
    if tick_enabled:
        try:
            run_thread = threading.Thread(target=facade.run, daemon=True)
            run_thread.start()
            deadline = time.monotonic() + 5.0
            while facade.status is not ServerStatus.RUNNING and time.monotonic() < deadline:
                time.sleep(0.01)
            if facade.status is not ServerStatus.RUNNING:
                msg = "embedded server did not reach RUNNING"
                raise RuntimeError(msg)
        except BaseException:
            _shutdown(facade, run_thread, embedded)
            raise
    return embedded, facade, run_thread


def _shutdown(
    facade: Server,
    run_thread: threading.Thread | None,
    embedded: EmbeddedServer,
) -> None:
    """Shut the stack down in the composition root's canonical order."""
    facade.shutdown()
    if run_thread is not None:
        run_thread.join(timeout=5.0)
    embedded.stop()


def _spawn_population(handlers: SpatialHandlers, actors: int) -> None:
    """Allocate the actor population through the shipped spawn path."""
    for principal_id in range(1, actors + 1):
        handlers.spawn_actor(principal_id)


def _snapshot_actors(handlers: SpatialHandlers) -> dict[int, Actor]:
    """Copy the live actor table into an independent replica table."""
    return {actor.id: replace(actor) for actor in handlers.actor_states()}


def _initial_heading(actor_id: int) -> list[int]:
    """Return the patrol heading for one actor, spread across directions."""
    return [
        _MOVE_MAGNITUDE if actor_id % 2 else -_MOVE_MAGNITUDE,
        _MOVE_MAGNITUDE if (actor_id >> 1) % 2 else -_MOVE_MAGNITUDE,
    ]


def _steer(heading: list[int], actor: Actor) -> None:
    """Reflect a patrol heading per axis when the actor nears the border."""
    if actor.pos_x <= _PATROL_LOW_FIX:
        heading[0] = _MOVE_MAGNITUDE
    elif actor.pos_x >= _PATROL_HIGH_FIX:
        heading[0] = -_MOVE_MAGNITUDE
    if actor.pos_y <= _PATROL_LOW_FIX:
        heading[1] = _MOVE_MAGNITUDE
    elif actor.pos_y >= _PATROL_HIGH_FIX:
        heading[1] = -_MOVE_MAGNITUDE


def _real_apply(
    handlers: SpatialHandlers,
    dt_ms: int,
) -> Callable[[int, SimInput], tuple[int | None, Actor]]:
    """Bind the shipped single-lock apply path for one config pass."""

    def call(actor_id: int, inp: SimInput) -> tuple[int | None, Actor]:
        stepped = handlers.apply_movement(actor_id, inp, dt_ms=dt_ms)
        if stepped is None:
            msg = f"actor {actor_id} missing from the handler set"
            raise RuntimeError(msg)
        return None, stepped

    return call


def _run_window(
    *,
    apply_one: Callable[[int, SimInput], tuple[int | None, Actor]],
    headings: dict[int, list[int]],
    threads: int,
    actors: int,
    seconds: float,
    dt_ms: int,
    seed: int,
) -> list[_ThreadTally]:
    """Run one window of N always-backlogged applying threads.

    Each thread picks actors uniformly at random (the deployment shape:
    one client per actor, so per-apply actor choice is uniform over the
    population), drives the actor with its shared patrol heading at the
    harness scenario's deflection, and measures the wall time of every
    apply call. Holds returned by replica callables are recorded
    alongside. Raises the first worker failure after all threads join.
    """
    tallies = [_ThreadTally() for _ in range(threads)]

    def worker(index: int) -> None:
        tally = tallies[index]
        try:
            rng = random.Random(seed + index)
            input_tick = count(1)
            deadline = time.perf_counter() + seconds
            while time.perf_counter() < deadline:
                actor_id = rng.randrange(1, actors + 1)
                heading = headings.setdefault(actor_id, _initial_heading(actor_id))
                inp = SimInput(
                    input_tick=next(input_tick),
                    move_x=heading[0],
                    move_y=heading[1],
                    move_z=0,
                    flags=0,
                )
                began = time.perf_counter_ns()
                hold, stepped = apply_one(actor_id, inp)
                latency = time.perf_counter_ns() - began
                _steer(heading, stepped)
                tally.applies += 1
                tally.latencies.append(latency)
                if hold is not None:
                    tally.holds.append(hold)
        except Exception as exc:  # re-raised on the main thread after join
            tally.error = exc

    workers = [threading.Thread(target=worker, args=(i,), daemon=True) for i in range(threads)]
    for thread in workers:
        thread.start()
    for thread in workers:
        thread.join()
    for tally in tallies:
        if tally.error is not None:
            raise tally.error
    return tallies


def _percentile(sorted_us: list[float], fraction: float) -> float:
    """Return the nearest-rank percentile of an ascending list."""
    if not sorted_us:
        return 0.0
    index = min(len(sorted_us) - 1, max(0, round(fraction * (len(sorted_us) - 1))))
    return sorted_us[index]


def _us_stats(latencies_ns: list[int]) -> tuple[float, float, float, float]:
    """Return mean, p50, p95, p99 in microseconds for apply latencies."""
    if not latencies_ns:
        return 0.0, 0.0, 0.0, 0.0
    us = sorted(value / 1000.0 for value in latencies_ns)
    mean = sum(us) / len(us)
    return mean, _percentile(us, 0.50), _percentile(us, 0.95), _percentile(us, 0.99)


def _aggregate(
    *,
    config: str,
    threads: int,
    stripes: int,
    tallies: list[_ThreadTally],
    elapsed_s: float,
) -> ConfigAggregate:
    """Pool one config's window tallies into its headline numbers."""
    latencies: list[int] = []
    holds: list[int] = []
    applies = 0
    for tally in tallies:
        applies += tally.applies
        latencies.extend(tally.latencies)
        holds.extend(tally.holds)
    mean_us, p50_us, p95_us, p99_us = _us_stats(latencies)
    hold_fraction: float | None = None
    if holds:
        hold_fraction = sum(holds) / (threads * elapsed_s * 1e9)
    return ConfigAggregate(
        config=config,
        threads=threads,
        stripes=stripes,
        applies_total=applies,
        elapsed_s=elapsed_s,
        applies_per_s=applies / elapsed_s if elapsed_s > 0 else 0.0,
        mean_us=mean_us,
        p50_us=p50_us,
        p95_us=p95_us,
        p99_us=p99_us,
        hold_fraction=hold_fraction,
    )


def _run_config_pass(
    *,
    apply_one: Callable[[int, SimInput], tuple[int | None, Actor]],
    config: BenchConfig,
    threads: int,
    stripes: int,
    name: str,
) -> tuple[list[WindowSample], ConfigAggregate]:
    """Warm up, then run and pool the timed windows for one config."""
    headings: dict[int, list[int]] = {}
    _run_window(
        apply_one=apply_one,
        headings=headings,
        threads=threads,
        actors=config.actors,
        seconds=config.warmup_s,
        dt_ms=config.dt_ms,
        seed=config.seed,
    )
    samples: list[WindowSample] = []
    pooled: list[_ThreadTally] = []
    elapsed_total = 0.0
    for window in range(config.windows):
        began = time.perf_counter()
        tallies = _run_window(
            apply_one=apply_one,
            headings=headings,
            threads=threads,
            actors=config.actors,
            seconds=config.window_s,
            dt_ms=config.dt_ms,
            seed=config.seed + 1000 * (window + 1),
        )
        elapsed = time.perf_counter() - began
        elapsed_total += elapsed
        pooled.extend(tallies)
        applies = sum(tally.applies for tally in tallies)
        _mean_us, p50_us, p95_us, p99_us = _us_stats(
            [value for tally in tallies for value in tally.latencies]
        )
        holds = [value for tally in tallies for value in tally.holds]
        hold_fraction: float | None = None
        if holds:
            hold_fraction = sum(holds) / (threads * elapsed * 1e9)
        samples.append(
            WindowSample(
                config=name,
                threads=threads,
                stripes=stripes,
                window=window + 1,
                applies=applies,
                elapsed_s=elapsed,
                applies_per_s=applies / elapsed,
                p50_us=p50_us,
                p95_us=p95_us,
                p99_us=p99_us,
                hold_fraction=hold_fraction,
            )
        )
    return samples, _aggregate(
        config=name,
        threads=threads,
        stripes=stripes,
        tallies=pooled,
        elapsed_s=elapsed_total,
    )


def _calibrate(aggregates: list[ConfigAggregate]) -> dict[str, dict[str, float | None]]:
    """Compute the internal cross-config ratios per thread count."""
    calibration: dict[str, dict[str, float | None]] = {}
    for thread_count in sorted({aggregate.threads for aggregate in aggregates}):
        rows = [a for a in aggregates if a.threads == thread_count]
        real = next((a for a in rows if a.config == "real-single"), None)
        single = next((a for a in rows if a.config == "replica" and a.stripes == 1), None)
        sharded = max(
            (a for a in rows if a.config == "replica"),
            key=lambda a: a.stripes,
            default=None,
        )
        calibration[str(thread_count)] = {
            "sharded_stripes": float(sharded.stripes) if sharded else None,
            "replica1_over_real_single": (
                single.applies_per_s / real.applies_per_s
                if single is not None and real is not None
                else None
            ),
            "sharded_over_real_single": (
                sharded.applies_per_s / real.applies_per_s
                if sharded is not None and real is not None
                else None
            ),
            "sharded_over_replica1": (
                sharded.applies_per_s / single.applies_per_s
                if sharded is not None and single is not None
                else None
            ),
        }
    return calibration


def run_bench(config: BenchConfig) -> BenchReport:
    """Run the bench and return the full report.

    Raises:
        RuntimeError: If a free-threaded interpreter is required and absent,
            the server fails to reach RUNNING, or any apply call fails.
    """
    if config.require_free_threaded and _gil_enabled():
        msg = (
            "the GIL is enabled in this interpreter; contention numbers are "
            "meaningless here - run under a free-threaded build (python3.14t)"
        )
        raise RuntimeError(msg)
    params: dict[str, object] = {
        "threads": list(config.threads),
        "stripes": list(config.stripes),
        "actors": config.actors,
        "windows": config.windows,
        "window_s": config.window_s,
        "warmup_s": config.warmup_s,
        "dt_ms": config.dt_ms,
        "seed": config.seed,
        "tick_enabled": config.tick_enabled,
        "gil_enabled": _gil_enabled(),
    }
    embedded, facade, run_thread = _boot_stack(config.tick_enabled)
    try:
        handlers = embedded.handlers
        _spawn_population(handlers, config.actors)
        zone_id = embedded.zone_id
        model = embedded.model
        windows: list[WindowSample] = []
        aggregates: list[ConfigAggregate] = []
        for thread_count in config.threads:
            samples, aggregate = _run_config_pass(
                apply_one=_real_apply(handlers, config.dt_ms),
                config=config,
                threads=thread_count,
                stripes=0,
                name="real-single",
            )
            windows.extend(samples)
            aggregates.append(aggregate)
            for stripe_count in config.stripes:
                replica = _ShardedApplyReplica(
                    model=model,
                    publish_artifact=facade.publish_artifact,
                    publish_cell_product=facade.publish_cell_product,
                    zone_id=zone_id,
                    cell_size=handlers.cell_size,
                    stripes=stripe_count,
                    actors=_snapshot_actors(handlers),
                    tick_period_s=_TICK_PERIOD_S if config.tick_enabled else None,
                )
                try:
                    samples, aggregate = _run_config_pass(
                        apply_one=partial(replica.apply_one, dt_ms=config.dt_ms),
                        config=config,
                        threads=thread_count,
                        stripes=stripe_count,
                        name="replica",
                    )
                finally:
                    replica.stop_flush()
                if replica.flush_error is not None:
                    raise replica.flush_error
                windows.extend(samples)
                aggregates.append(aggregate)
        return BenchReport(
            envelope=_envelope(),
            params=params,
            windows=windows,
            aggregates=aggregates,
            calibration=_calibrate(aggregates),
        )
    finally:
        _shutdown(facade, run_thread, embedded)


def _find_c_driver() -> Path:
    """Locate the built C driver binary.

    Resolution order: ``$KITH_C_APPLY_BENCH`` (explicit path), then
    ``$KITH_BUILD_DIR``, then the repo preset build directories
    (``build/release``, ``build/debug``) under ``tools/``.

    Raises:
        RuntimeError: When no candidate exists.
    """
    candidates: list[Path] = []
    override = os.environ.get("KITH_C_APPLY_BENCH", "")
    if override:
        candidates.append(Path(override))
    build_dir = os.environ.get("KITH_BUILD_DIR")
    if build_dir:
        candidates.append(Path(build_dir) / "tools" / _C_DRIVER_NAME)
    repo_root = Path(__file__).resolve().parents[2]
    for preset in ("release", "debug"):
        candidates.append(repo_root / "build" / preset / "tools" / _C_DRIVER_NAME)
    for candidate in candidates:
        if candidate.is_file():
            return candidate
    msg = (
        f"the {_C_DRIVER_NAME} driver was not found; build it with "
        f"cmake --build --preset release --target {_C_DRIVER_NAME}"
    )
    raise RuntimeError(msg)


def _parse_agg_record(fields: Sequence[str]) -> CNativeAggregate:
    """Convert one AGG line's fields into the typed aggregate (field order
    mirrors bench_print_report in tools/perf/c_apply_bench.c)."""
    return CNativeAggregate(
        threads=int(fields[1]),
        tick_enabled=int(fields[2]),
        applies_total=int(fields[3]),
        elapsed_s=float(fields[4]),
        applies_per_s=float(fields[5]),
        cpu_mean_us=float(fields[6]),
        cpu_p50_us=float(fields[7]),
        cpu_p95_us=float(fields[8]),
        cpu_p99_us=float(fields[9]),
        wall_mean_us=float(fields[10]),
        wall_p50_us=float(fields[11]),
        wall_p95_us=float(fields[12]),
        wall_p99_us=float(fields[13]),
        total_cpu_us_per_apply=float(fields[14]),
        flush_cpu_us_per_apply=float(fields[15]),
        flush_total_cpu_s=float(fields[16]),
        flush_count=int(fields[17]),
        fabric_publishes=int(fields[18]),
        eperm_suppressed=int(fields[19]),
        dirty_cells_drained=int(fields[20]),
        drain_products=int(fields[21]),
        records_appended=int(fields[22]),
    )


def _parse_win_record(fields: Sequence[str]) -> CNativeWindowRow:
    """Convert one WIN line's fields into the typed window row."""
    return CNativeWindowRow(
        threads=int(fields[1]),
        tick=int(fields[2]),
        window=int(fields[3]),
        applies=int(fields[4]),
        elapsed_s=float(fields[5]),
        cpu_mean_us=float(fields[6]),
        wall_mean_us=float(fields[7]),
    )


def _c_native_report(
    *,
    driver: Path,
    behavior_path: Path,
    actors: int,
    threads: Sequence[int],
    windows: int,
    window_s: float,
    warmup_s: float,
    dt_ms: int,
    seed: int,
    zone: int,
    tiles: int,
    envelope: dict[str, object],
) -> CNativeReport:
    """Run the C driver (tick ON then OFF) and parse its line records.

    The driver prints one ``AGG <fields...>`` line per (threads, mode)
    config, one ``WIN <field...>`` row per timed window, and a final
    ``DONE``; the field order mirrors ``_AGG_FIELDS`` / ``_WIN_FIELDS``.

    Raises:
        RuntimeError: On a non-zero driver exit or a truncated report.
    """
    command = [
        str(driver),
        "--behavior",
        str(behavior_path),
        "--actors",
        str(actors),
        "--threads",
        ",".join(str(count) for count in threads),
        "--windows",
        str(windows),
        "--window-s",
        str(window_s),
        "--warmup-s",
        str(warmup_s),
        "--dt-ms",
        str(dt_ms),
        "--seed",
        str(seed),
        "--zone",
        str(zone),
        "--tiles",
        str(tiles),
    ]
    # The sanitizer legs preload a shared sanitizer runtime into this
    # interpreter and tune it through the sanitizer option variables. The
    # driver statically links its own runtime: an inherited preload aborts
    # it with incompatible-runtime errors, and inherited options subject
    # the child to tuning meant for the interpreter process. The child
    # runs in the ambient environment minus the sanitizer-leg injections.
    child_env = {
        key: value
        for key, value in os.environ.items()
        if key not in ("LD_PRELOAD", "ASAN_OPTIONS", "LSAN_OPTIONS", "TSAN_OPTIONS")
    }
    completed = subprocess.run(command, check=False, capture_output=True, text=True, env=child_env)
    if completed.returncode != 0:
        msg = f"{_C_DRIVER_NAME} failed: {completed.stderr.strip()}"
        raise RuntimeError(msg)
    aggregates: list[CNativeAggregate] = []
    window_rows: list[CNativeWindowRow] = []
    done = False
    for line in completed.stdout.splitlines():
        fields = line.split()
        if not fields:
            continue
        if fields[0] == "AGG" and len(fields) == 1 + len(_AGG_FIELDS):
            aggregates.append(_parse_agg_record(fields))
        elif fields[0] == "WIN" and len(fields) == 1 + len(_WIN_FIELDS):
            window_rows.append(_parse_win_record(fields))
        elif fields[0] == "DONE":
            done = True
    if not done:
        msg = f"{_C_DRIVER_NAME} produced no DONE record"
        raise RuntimeError(msg)
    return CNativeReport(
        schema=_SCHEMA_C_NATIVE,
        params={
            "actors": actors,
            "threads": list(threads),
            "windows": windows,
            "window_s": window_s,
            "warmup_s": warmup_s,
            "dt_ms": dt_ms,
            "seed": seed,
            "zone": zone,
            "tiles": tiles,
            "tick_modes": [1, 0],
        },
        aggregates=aggregates,
        windows=window_rows,
        envelope=envelope,
    )


def _run_c_native(
    *,
    actors: int,
    threads: Sequence[int],
    windows: int,
    window_s: float,
    warmup_s: float,
    dt_ms: int,
    seed: int,
) -> CNativeReport:
    """Convert the behavior grid, run the C driver, and assemble the report."""
    driver = _find_c_driver()
    grid_fd, grid_name = tempfile.mkstemp(suffix=".grid", prefix="kith_c_bench_")
    grid_path = Path(grid_name)
    try:
        with os.fdopen(grid_fd, "w", encoding="ascii") as handle:
            handle.write(tmx_to_text_grid(_WORLD_TMX.read_text(encoding="utf-8")))
        report = _c_native_report(
            driver=driver,
            behavior_path=grid_path,
            actors=actors,
            threads=threads,
            windows=windows,
            window_s=window_s,
            warmup_s=warmup_s,
            dt_ms=dt_ms,
            seed=seed,
            zone=1,
            tiles=_WORLD_TILES,
            envelope=_envelope(),
        )
    finally:
        with contextlib.suppress(FileNotFoundError):
            grid_path.unlink()
    return report


def _ratio_text(value: float | None) -> str:
    """Render one calibration ratio for the stdout table."""
    return "n/a" if value is None else f"{value:.3f}"


def _print_report(report: BenchReport) -> None:
    """Render the aggregate table and calibration ratios to stdout."""
    header = (
        f"{'config':<12} {'threads':>7} {'stripes':>7} {'applies/s':>12} "
        f"{'p50 us':>9} {'p95 us':>9} {'p99 us':>9} {'hold %':>7}"
    )
    print(header)
    for aggregate in report.aggregates:
        hold = aggregate.hold_fraction
        hold_text = f"{hold * 100.0:.1f}" if hold is not None else "-"
        print(
            f"{aggregate.config:<12} {aggregate.threads:>7} {aggregate.stripes:>7} "
            f"{aggregate.applies_per_s:>12.1f} {aggregate.p50_us:>9.1f} "
            f"{aggregate.p95_us:>9.1f} {aggregate.p99_us:>9.1f} {hold_text:>7}"
        )
    print("\ncalibration (internal ratios):")
    for thread_count, entry in report.calibration.items():
        ratios = ", ".join(f"{name}={_ratio_text(value)}" for name, value in entry.items())
        print(f"  threads={thread_count}: {ratios}")


def _int_list(value: str) -> list[int]:
    """Parse a comma-separated list of positive integers."""
    parsed = [int(item) for item in value.split(",")]
    if not parsed or any(item < 1 for item in parsed):
        msg = f"expected a comma-separated list of positive integers, got {value!r}"
        raise argparse.ArgumentTypeError(msg)
    return parsed


def main(argv: Sequence[str] | None = None) -> int:
    """Entry point: parse arguments, run the bench, print and persist."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--threads", type=_int_list, default="8", help="thread counts to sweep")
    parser.add_argument("--stripes", type=_int_list, default="1,64", help="replica stripe counts")
    parser.add_argument("--actors", type=int, default=1000, help="actor population")
    parser.add_argument("--windows", type=int, default=5, help="timed windows per config")
    parser.add_argument("--window-s", type=float, default=1.0, help="timed window duration")
    parser.add_argument("--warmup-s", type=float, default=1.0, help="discarded warmup duration")
    parser.add_argument("--dt-ms", type=int, default=50, help="per-apply step duration")
    parser.add_argument("--seed", type=int, default=12345, help="root schedule seed")
    parser.add_argument(
        "--no-tick",
        action="store_true",
        help="isolate pure apply capacity: no server tick loop, no flush",
    )
    parser.add_argument(
        "--c-native",
        action="store_true",
        help=(
            "run the C driver (c_apply_bench) instead of the Python paths: "
            "per-apply thread-CPU of the C apply sequence, tick ON and OFF"
        ),
    )
    parser.add_argument("--json", type=Path, default=None, help="write the JSON report here")
    args = parser.parse_args(argv)
    if args.c_native:
        c_report = _run_c_native(
            actors=args.actors,
            threads=args.threads,
            windows=args.windows,
            window_s=args.window_s,
            warmup_s=args.warmup_s,
            dt_ms=args.dt_ms,
            seed=args.seed,
        )
        print(
            f"{'threads':>7} {'tick':>5} {'applies/s':>12} {'cpu mean':>9} {'p50':>7} "
            f"{'p95':>7} {'p99':>7} {'total+flush':>12}"
        )
        for aggregate in c_report["aggregates"]:
            print(
                f"{aggregate['threads']:>7} {aggregate['tick_enabled']:>5} "
                f"{aggregate['applies_per_s']:>12.0f} {aggregate['cpu_mean_us']:>7.1f} "
                f"{aggregate['cpu_p50_us']:>7.1f} {aggregate['cpu_p95_us']:>7.1f} "
                f"{aggregate['cpu_p99_us']:>7.1f} {aggregate['total_cpu_us_per_apply']:>12.1f}"
            )
        if args.json is not None:
            args.json.write_text(
                json.dumps(c_report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
            )
            print(f"\nreport written to {args.json}")
        return 0
    config = BenchConfig(
        threads=args.threads,
        stripes=args.stripes,
        actors=args.actors,
        windows=args.windows,
        window_s=args.window_s,
        warmup_s=args.warmup_s,
        dt_ms=args.dt_ms,
        seed=args.seed,
        tick_enabled=not args.no_tick,
    )
    report = run_bench(config)
    _print_report(report)
    if args.json is not None:
        args.json.write_text(
            json.dumps(report.to_json_dict(), indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        print(f"\nreport written to {args.json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
