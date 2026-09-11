"""Scenario drivers for the visual example: one command per feel
scenario, each spawning (or attaching to) an example server and driving
a harness-engine bot cohort over the same wire the graphical client
uses.

Scenarios:
- crowd-in          bots converge on the primary session's cell (view
                    budget + tiers)
- density-pile      bots run into the primary session's current cell
                    (full vs tiered budget)
- churn-storm       session connect/disconnect cycles
- membership-churn  bots patrol back and forth across a cell edge (no
                    disconnects — the window-diff path under load)
- kill-reconnect    SIGKILL the server on a fixed port; the player's
                    client and the bots ride the harness reconnect; the
                    world resumes
- tick-ladder       relaunch the server down the tick rungs (20/62/125/142)
                    so the player feels smoothness scale with tick rate
- soak              sustained wander load for N minutes

Each scenario owns its server lifecycle end to end: spawn (or attach via
``gw:ctl``), run the cohort, and stop — with a loud failure when a
fixed-port server cannot rebind.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import math
import random
import subprocess
import time
from collections.abc import Callable
from typing import Any

from kith._agent.server_control import ServerControlClient
from kith.examples.visual import _launch, _world
from kith.examples.visual._cohort import (
    Bot,
    WorldView,
    cohort_spread,
    scrape_loop,
    spawn_cohort,
    spawn_synthetic_primary,
    teardown_bots,
)


__all__ = [
    "main",
    "parse_args",
    "scenario_churn_storm",
    "scenario_crowd_in",
    "scenario_density_pile",
    "scenario_kill_reconnect",
    "scenario_membership_churn",
    "scenario_soak",
    "scenario_tick_ladder",
]


async def scenario_crowd_in(args: argparse.Namespace) -> None:
    """Bots converge on the primary session's live position and mill around it."""
    proc, gw, ctl = _server_from(args)
    control = ServerControlClient("127.0.0.1", ctl, timeout_s=2.0)
    view = WorldView(control)

    def target(view: WorldView, bot: Bot) -> tuple[float, float] | None:
        primary = view.primary_position()
        if primary is None:
            return None
        angle = bot.index * 2.39996  # golden-angle ring spread
        radius = 6.0 + (bot.index % 9) * 1.7
        return primary[0] + radius * math.cos(angle), primary[1] + radius * math.sin(angle)

    stop = asyncio.Event()
    scraper = asyncio.create_task(scrape_loop(view, stop))
    primary_bot = await spawn_synthetic_primary(view, gw) if args.synthetic_primary else None
    bots, ready = await spawn_cohort(
        view, args.bots, args.host, gw, input_hz=args.input_hz, target_fn=target
    )
    await asyncio.wait_for(ready.wait(), 20.0)
    print(f"crowd-in: {args.bots} bots converging (ctrl-C ends)", flush=True)
    with contextlib.suppress(asyncio.CancelledError):
        await asyncio.sleep(args.seconds)
    primary_actor = view.primary_actor()
    print(f"crowd-in: convergence {cohort_spread(view, bots, primary_actor)}", flush=True)
    stop.set()
    scraper.cancel()
    if primary_bot is not None:
        await primary_bot.stop()
    await teardown_bots(bots)
    await control.close()
    _stop_server(proc)
    print("crowd-in: done", flush=True)


async def scenario_density_pile(args: argparse.Namespace) -> None:
    """Bots run into the primary session's current cell and jitter — the
    budget/tier case. The pile tracks the primary cell-to-cell (bots spawn
    at the origin like every actor), so the budget is felt wherever the
    primary goes."""
    proc, gw, ctl = _server_from(args)
    control = ServerControlClient("127.0.0.1", ctl, timeout_s=2.0)
    view = WorldView(control)
    rng = random.Random(11)
    jitter: dict[int, tuple[float, float]] = {}

    def target(view: WorldView, bot: Bot) -> tuple[float, float] | None:
        primary = view.primary_position()
        if primary is None:
            return None
        if bot.index not in jitter:
            jitter[bot.index] = (rng.uniform(-9.0, 9.0), rng.uniform(-9.0, 9.0))
        cell_x = int(primary[0] // _world.CELL_SIZE_UNITS) * _world.CELL_SIZE_UNITS + 16.0
        cell_y = int(primary[1] // _world.CELL_SIZE_UNITS) * _world.CELL_SIZE_UNITS + 16.0
        jx, jy = jitter[bot.index]
        return (cell_x + jx + rng.uniform(-1.5, 1.5), cell_y + jy + rng.uniform(-1.5, 1.5))

    stop = asyncio.Event()
    scraper = asyncio.create_task(scrape_loop(view, stop))
    primary_bot = await spawn_synthetic_primary(view, gw) if args.synthetic_primary else None
    bots, ready = await spawn_cohort(
        view, args.bots, args.host, gw, input_hz=args.input_hz, run=True, target_fn=target
    )
    await asyncio.wait_for(ready.wait(), 30.0)
    print(
        f"density-pile: {args.bots} bots into the primary's cell "
        f"(preset={args.preset}; ctrl-C ends)",
        flush=True,
    )
    with contextlib.suppress(asyncio.CancelledError):
        await asyncio.sleep(args.seconds)
    primary_actor = view.primary_actor()
    print(f"density-pile: pile {cohort_spread(view, bots, primary_actor)}", flush=True)
    stop.set()
    scraper.cancel()
    if primary_bot is not None:
        await primary_bot.stop()
    await teardown_bots(bots)
    await control.close()
    _stop_server(proc)
    print("density-pile: done", flush=True)


async def scenario_churn_storm(args: argparse.Namespace) -> None:
    """Session connect/disconnect cycles against the live world."""
    proc, gw, ctl = _server_from(args)
    control = ServerControlClient("127.0.0.1", ctl, timeout_s=2.0)
    view = WorldView(control)
    cycles = max(1, args.seconds // max(1, args.cycle_s))

    def target(view: WorldView, bot: Bot) -> tuple[float, float] | None:
        primary = view.primary_position()
        if primary is None:
            return None
        angle = bot.index * 2.39996  # golden-angle spread
        radius = 2.0 + (bot.index % 6) * 1.5
        return primary[0] + radius * math.cos(angle), primary[1] + radius * math.sin(angle)

    print(
        f"churn-storm: {args.bots} sessions x {cycles} cycles "
        f"(dwell {args.cycle_s}s, {args.cycle_s}s gap; ctrl-C ends)",
        flush=True,
    )
    stop = asyncio.Event()
    scraper = asyncio.create_task(scrape_loop(view, stop))
    try:
        for cycle in range(cycles):
            bots, ready = await spawn_cohort(
                view, args.bots, args.host, gw, input_hz=args.input_hz, run=True, target_fn=target
            )
            await asyncio.wait_for(ready.wait(), 20.0)
            await asyncio.sleep(args.cycle_s)
            await teardown_bots(bots)
            print(f"  cycle {cycle + 1}/{cycles} done", flush=True)
            await asyncio.sleep(args.cycle_s)
    except asyncio.CancelledError:
        pass
    stop.set()
    scraper.cancel()
    await control.close()
    _stop_server(proc)
    print("churn-storm: done", flush=True)


async def scenario_membership_churn(args: argparse.Namespace) -> None:
    """Bots patrol across one cell edge near the primary session — the
    window-diff path under load, with no disconnects."""
    proc, gw, ctl = _server_from(args)
    control = ServerControlClient("127.0.0.1", ctl, timeout_s=2.0)
    view = WorldView(control)
    edges: dict[int, tuple[tuple[float, float], tuple[float, float]]] = {}

    def target(view: WorldView, bot: Bot) -> tuple[float, float] | None:
        primary = view.primary_position()
        if primary is None:
            return None
        if bot.index not in edges:
            edge_x = (int(primary[0] // _world.CELL_SIZE_UNITS) + 1) * _world.CELL_SIZE_UNITS
            y = primary[1]
            edges[bot.index] = ((edge_x - 5.0, y), (edge_x + 5.0, y))
        a, b = edges[bot.index]
        phase = (time.monotonic() * 0.5 + bot.index * 0.13) % 2.0
        return b if phase < 1.0 else a

    stop = asyncio.Event()
    scraper = asyncio.create_task(scrape_loop(view, stop))
    bots, ready = await spawn_cohort(
        view, args.bots, args.host, gw, input_hz=args.input_hz, target_fn=target
    )
    await asyncio.wait_for(ready.wait(), 20.0)
    print(f"membership-churn: {args.bots} bots crossing one cell edge (ctrl-C ends)", flush=True)
    with contextlib.suppress(asyncio.CancelledError):
        await asyncio.sleep(args.seconds)
    stop.set()
    scraper.cancel()
    await teardown_bots(bots)
    await control.close()
    _stop_server(proc)
    print("membership-churn: done", flush=True)


async def scenario_kill_reconnect(args: argparse.Namespace) -> None:
    """SIGKILL the fixed-port server on a cadence; everything reconnects."""
    port = args.port
    proc, gw, ctl = await _ensure_fixed_server(args, port)
    control = ServerControlClient("127.0.0.1", ctl, timeout_s=2.0)
    view = WorldView(control)

    def target(view: WorldView, bot: Bot) -> tuple[float, float] | None:
        pos = view.positions.get(bot.actor_id)
        if pos is None:
            return None
        return ((pos[0] + 97.0) % 448.0 + 32.0, (pos[1] + 61.0) % 448.0 + 32.0)

    stop = asyncio.Event()
    scraper = asyncio.create_task(scrape_loop(view, stop))
    bots, _ready = await spawn_cohort(
        view, args.bots, "127.0.0.1", gw, input_hz=args.input_hz, target_fn=target
    )
    print(
        f"kill-reconnect: server on port {port}, SIGKILL every {args.kill_every}s "
        f"(keep the client open — it rebinds; ctrl-C ends)",
        flush=True,
    )
    start = time.monotonic()
    try:
        while time.monotonic() - start < args.seconds:
            await asyncio.sleep(args.kill_every)
            print(f"  SIGKILL server (t+{time.monotonic() - start:.0f}s)", flush=True)
            proc.kill()
            proc.wait(timeout=8)
            # Drop every cached id and position now: the next world
            # reassigns actor ids, and a scrape straddling the kill must
            # not hand the old map to the steering loop.
            async with view._lock:
                view.positions = {}
                view.bindings = {}
            await asyncio.sleep(5.0)
            proc, gw, ctl = await _ensure_fixed_server(args, port)
            print(f"  server back: gw={gw} ctl={ctl}", flush=True)
    except asyncio.CancelledError:
        pass
    stop.set()
    scraper.cancel()
    await teardown_bots(bots)
    await control.close()
    _stop_server(proc)
    print("kill-reconnect: done", flush=True)


async def scenario_tick_ladder(args: argparse.Namespace) -> None:
    """Relaunch the server down the tick rungs; the player feels each rung."""
    port = args.port
    rungs = [int(r) for r in args.rungs.split(",")]
    proc: _ServerProc | None = None
    try:
        for rung in rungs:
            if proc is not None:
                _stop_server(proc)
            await asyncio.sleep(1.0)
            proc, gw, ctl = await _ensure_fixed_server(args, port, tick_hz=rung)
            print(
                f"=== tick-ladder rung {rung} Hz — server gw={gw} ctl={ctl} "
                f"(dwell {args.dwell}s; tab toggles RAW/INTERP) ===",
                flush=True,
            )
            await asyncio.sleep(args.dwell)
        print(
            f"tick-ladder: done; last server left running on port {port} (gw={gw} ctl={ctl})",
            flush=True,
        )
    except asyncio.CancelledError:
        pass


async def scenario_soak(args: argparse.Namespace) -> None:
    """Sustained wander load for ``minutes``."""
    proc, gw, ctl = _server_from(args)
    control = ServerControlClient("127.0.0.1", ctl, timeout_s=2.0)
    view = WorldView(control)
    rng = random.Random(23)
    waypoints: dict[int, tuple[float, float]] = {}

    def target(view: WorldView, bot: Bot) -> tuple[float, float] | None:
        pos = view.positions.get(bot.actor_id)
        if pos is None:
            return None
        wp = waypoints.get(bot.index)
        if wp is None or (pos[0] - wp[0]) ** 2 + (pos[1] - wp[1]) ** 2 < 36.0:
            wp = (rng.uniform(32.0, 480.0), rng.uniform(32.0, 480.0))
            waypoints[bot.index] = wp
        return wp

    stop = asyncio.Event()
    scraper = asyncio.create_task(scrape_loop(view, stop))
    bots, ready = await spawn_cohort(
        view, args.bots, args.host, gw, input_hz=args.input_hz, target_fn=target
    )
    await asyncio.wait_for(ready.wait(), 30.0)
    print(f"soak: {args.bots} bots wandering for {args.minutes} min (ctrl-C ends)", flush=True)
    with contextlib.suppress(asyncio.CancelledError):
        await asyncio.sleep(args.minutes * 60.0)
    stop.set()
    scraper.cancel()
    await teardown_bots(bots)
    await control.close()
    _stop_server(proc)
    print("soak: done", flush=True)


# ---------------------------------------------------------------------------
# shared plumbing
# ---------------------------------------------------------------------------


def _server_from(args: argparse.Namespace) -> tuple[_ServerProc, int, int]:
    """The scenario's server: attached (gw:ctl) or freshly spawned."""
    if args.attach:
        gw_s, ctl_s = args.attach.split(":")
        return _AttachedServer(), int(gw_s), int(ctl_s)
    proc, gw, ctl = _launch.spawn_server(
        tick_hz=args.tick_hz,
        npcs=args.npcs,
        preset=args.preset,
        port=args.port,
        python_workers=args.python_workers,
    )
    print(f"playground ports: gw={gw} ctl={ctl}", flush=True)
    return proc, gw, ctl


class _AttachedServer:
    """A stand-in for a server the driver does not own (nothing to stop)."""

    def kill(self) -> None:
        pass

    def wait(self, timeout: float | None = None) -> int:
        return 0

    def poll(self) -> int:
        return 0

    def send_signal(self, sig: int) -> None:
        pass


# A spawned server or the attach stand-in; both satisfy the stop/kill
# surface the scenarios drive.
_ServerProc = subprocess.Popen[bytes] | _AttachedServer


def _stop_server(proc: _ServerProc) -> None:
    """Stop a scenario's server (the attach stand-in owns nothing)."""
    if not isinstance(proc, _AttachedServer):
        _launch.stop_server(proc)


async def _ensure_fixed_server(
    args: argparse.Namespace, port: int, *, tick_hz: int | None = None
) -> tuple[_ServerProc, int, int]:
    """(Re)spawn the fixed-port server, waiting out a slow port release."""
    for _attempt in range(4):
        try:
            proc, gw, ctl = _launch.spawn_server(
                tick_hz=tick_hz or args.tick_hz,
                npcs=args.npcs,
                preset=args.preset,
                port=port,
                python_workers=args.python_workers,
            )
            print(f"playground ports: gw={gw} ctl={ctl}", flush=True)
            return proc, gw, ctl
        except (RuntimeError, OSError) as _exc:
            del _exc
            await asyncio.sleep(2.0)
    raise RuntimeError(
        f"could not (re)bind port {port} — a stale server may hold it; "
        "stop it with: pkill -f 'kith.examples.visual.serve[r]'"
    )


_SCENARIOS: dict[str, Callable[[argparse.Namespace], Any]] = {
    "crowd-in": scenario_crowd_in,
    "density-pile": scenario_density_pile,
    "churn-storm": scenario_churn_storm,
    "membership-churn": scenario_membership_churn,
    "kill-reconnect": scenario_kill_reconnect,
    "tick-ladder": scenario_tick_ladder,
    "soak": scenario_soak,
}

_DEFAULT_BOTS = {
    "crowd-in": 60,
    "density-pile": 60,
    "churn-storm": 40,
    "membership-churn": 24,
    "kill-reconnect": 40,
    "tick-ladder": 0,
    "soak": 60,
}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse the driver's scenario + cohort knobs."""
    parser = argparse.ArgumentParser(prog="kith-visual run")
    parser.add_argument("scenario", choices=sorted(_SCENARIOS))
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument(
        "--port", type=int, default=7777, help="fixed server port (relaunch scenarios)"
    )
    parser.add_argument("--attach", default=None, help="use an existing server: gw:ctl")
    parser.add_argument("--bots", type=int, default=None, help="cohort size (scenario default)")
    parser.add_argument(
        "--input-hz",
        type=float,
        default=None,
        help="per-bot input rate (default 10, 4 when the cohort is over 50)",
    )
    parser.add_argument("--seconds", type=int, default=120, help="scenario duration")
    parser.add_argument("--minutes", type=int, default=10, help="soak duration")
    parser.add_argument("--cycle-s", type=int, default=3, help="churn dwell per cycle")
    parser.add_argument("--kill-every", type=int, default=30, help="seconds between SIGKILLs")
    parser.add_argument("--dwell", type=int, default=45, help="seconds per ladder rung")
    parser.add_argument("--rungs", default="20,62,125,142", help="comma tick rungs")
    parser.add_argument("--tick-hz", type=int, default=20)
    parser.add_argument("--npcs", type=int, default=40)
    parser.add_argument("--preset", choices=("full", "tiered"), default="full")
    parser.add_argument("--python-workers", type=int, default=8)
    parser.add_argument(
        "--synthetic-primary",
        action="store_true",
        help="drive a principal-1 bot to the center (headless primary-relative runs)",
    )
    args = parser.parse_args(argv)
    if args.bots is None:
        args.bots = _DEFAULT_BOTS[args.scenario]
    if args.input_hz is None:
        args.input_hz = 10.0 if args.bots <= 50 else 4.0
    return args


def main(argv: list[str] | None = None) -> int:
    """Run one scenario to completion."""
    args = parse_args(argv)
    try:
        asyncio.run(_SCENARIOS[args.scenario](args))
        return 0
    except KeyboardInterrupt:
        print(f"{args.scenario}: interrupted", flush=True)
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
