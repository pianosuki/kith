"""The visual example's bot cohort machinery: harness-engine bots, the
server-truth view they steer from, and the cohort lifecycle helpers.

One asyncio loop drives the whole cohort; bots steer from the server's
own ``/query_state`` truth (one scrape feeds every bot), so the drivers
carry no per-bot record parsing. Cohort defaults stay at or under 60 on
purpose: the loopback scenarios are feel tests, not gate re-runs, and
one asyncio loop saturates around ~100 live engines (past that, bot
steering starves and pile convergence collapses while the server itself
stays healthy). Larger cohorts want the multi-process driver shape the
scaling gates use.
"""

from __future__ import annotations

import asyncio
import contextlib
import math
import time
from collections.abc import Callable
from typing import Any

from kith import SimInput
from kith.examples.visual._handlers import PRIMARY_PRINCIPAL
from kith.examples.visual._protocol import ACTOR_INPUT_TYPE, encode_actor_input, make_client
from kith.examples.visual._world import RUN_FLAG


_STICK = 32_767
_BOT_PRINCIPAL_BASE = 9000
# Arrival hysteresis for the drivers' chase targets: inside REST_RADIUS a
# bot rests (no further submits, the pending zero-stick input decelerates
# it), past RESUME_RADIUS it steers again. The gap keeps a momentum
# overshoot from flap-resting at the target.
REST_RADIUS = 3.0
RESUME_RADIUS = 6.0

TargetFn = Callable[["WorldView", "Bot"], tuple[float, float] | None]


def direction(dx: float, dy: float) -> tuple[int, int]:
    """Normalize an offset into i16 stick components (max at full stick)."""
    mag = max(abs(dx), abs(dy))
    if mag <= 1e-6:
        return 0, 0
    return int(dx * _STICK / mag), int(dy * _STICK / mag)


class Bot:
    """One harness-engine bot: connect, login, steer toward a target, repeat."""

    def __init__(
        self,
        index: int,
        port: int,
        *,
        principal: int | None = None,
        input_hz: float = 10.0,
        run: bool = False,
        target_fn: TargetFn | None = None,
    ) -> None:
        self.index = index
        self.client_port = port
        self.principal = principal if principal is not None else _BOT_PRINCIPAL_BASE + index
        self.target_fn: TargetFn = target_fn or (lambda view, bot: None)
        self.actor_id = 0
        self._input_period = 1.0 / input_hz
        self._run = run
        self._tick = 1
        self._resting = False
        self.client: Any = None
        self._task: asyncio.Task[None] | None = None

    async def run(self, view: WorldView, stop_at: float | None, ready: asyncio.Event) -> None:
        self.client = make_client(
            f"bot-{self.index}",
            "127.0.0.1",
            self.client_port,
            principal_id=self.principal,
            event_history_size=8,
            http_enabled=False,
            ipc_enabled=False,
        )
        await self.client.start()
        deadline = time.monotonic() + 15.0
        while time.monotonic() < deadline:
            status = self.client.query_status()
            if status.bootstrap_state >= 2:
                break
            await asyncio.sleep(0.1)
        ready.set()
        while stop_at is None or time.monotonic() < stop_at:
            await asyncio.sleep(self._input_period)
            # Re-resolve from the current scrape every iteration: a server
            # respawn reassigns actor ids by bind order, so a cached id can
            # come to name another session's actor (the wire handler's
            # identity gate is observability-only — the buffered input
            # still applies).
            self.actor_id = view.bindings.get(self.principal, 0)
            if self.actor_id == 0:
                continue
            target = self.target_fn(view, self)
            if target is None:
                continue
            pos = view.positions.get(self.actor_id)
            if pos is None:
                continue
            dist_sq = (target[0] - pos[0]) ** 2 + (target[1] - pos[1]) ** 2
            if self._resting:
                if dist_sq > RESUME_RADIUS**2:
                    self._resting = False
                else:
                    continue
            move_x, move_y = direction(target[0] - pos[0], target[1] - pos[1])
            if dist_sq < REST_RADIUS**2:
                move_x, move_y = 0, 0
                self._resting = True
            with contextlib.suppress(Exception):
                self.client.submit(
                    ACTOR_INPUT_TYPE,
                    encode_actor_input(
                        self.actor_id,
                        SimInput(
                            input_tick=self._tick,
                            move_x=move_x,
                            move_y=move_y,
                            flags=RUN_FLAG if self._run else 0,
                        ),
                    ),
                )
            self._tick += 1

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError, asyncio.TimeoutError):
                await asyncio.wait_for(asyncio.gather(self._task), 3.0)
        if self.client is not None:
            with contextlib.suppress(Exception):
                await asyncio.wait_for(self.client.stop(), 3.0)


class WorldView:
    """Server-truth scrape: actor positions, the primary actor, metrics."""

    def __init__(self, control: Any) -> None:
        self._control = control
        self._lock = asyncio.Lock()
        self.positions: dict[int, tuple[float, float]] = {}
        self.bindings: dict[int, int] = {}
        self.metrics: dict[str, Any] = {}

    async def refresh(self) -> None:
        try:
            state = await asyncio.wait_for(self._control.get("/query_state"), 3.0)
            metrics = await asyncio.wait_for(self._control.get("/playground/metrics"), 3.0)
        except Exception:
            # A failed scrape means the world may be gone: the old map
            # names dead (or reassigned) actor ids, so drop it.
            async with self._lock:
                self.positions = {}
                self.bindings = {}
            return
        actors: dict[str, Any] = state.get("actors", {})
        positions = {
            int(actor_id): (int(row["x"]) / 65536.0, int(row["y"]) / 65536.0)
            for actor_id, row in actors.items()
        }
        bindings = {int(principal): int(actor) for principal, actor in metrics.get("bindings", [])}
        async with self._lock:
            self.positions = positions
            self.bindings = bindings
            self.metrics = dict(metrics)

    def primary_actor(self) -> int:
        return self.bindings.get(PRIMARY_PRINCIPAL, 0)

    def primary_position(self) -> tuple[float, float] | None:
        actor = self.primary_actor()
        return self.positions.get(actor)


async def scrape_loop(view: WorldView, stop: asyncio.Event) -> None:
    """Refresh the view every 0.5 s until ``stop`` is set."""
    while not stop.is_set():
        await view.refresh()
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(stop.wait(), 0.5)


async def spawn_cohort(
    view: WorldView,
    count: int,
    host: str,
    port: int,
    *,
    input_hz: float,
    run: bool = False,
    target_fn: TargetFn,
) -> tuple[list[Bot], asyncio.Event]:
    """Start ``count`` bots staggered; the event settles when the first bot
    has attempted its bootstrap (staggered starts make the join flood gentle)."""
    bots = [Bot(i, port, input_hz=input_hz, run=run, target_fn=target_fn) for i in range(count)]
    ready = asyncio.Event()

    async def _run(bot: Bot) -> None:
        await bot.run(view, None, ready if bot.index == 0 else asyncio.Event())

    for bot in bots:
        bot._task = asyncio.create_task(_run(bot))
        await asyncio.sleep(0.05)
    return bots, ready


async def spawn_synthetic_primary(view: WorldView, port: int) -> Bot | None:
    """A principal-1 bot driven to the plain's center, so the
    primary-relative scenarios run headless. Skipped when a real primary
    session is already bound."""
    for _attempt in range(20):
        if view.primary_actor():
            return None
        await asyncio.sleep(0.25)
    primary = Bot(-1, port, principal=PRIMARY_PRINCIPAL, input_hz=10.0)

    def target(view: WorldView, bot: Bot) -> tuple[float, float] | None:
        return 256.0, 256.0

    primary.target_fn = target
    primary._task = asyncio.create_task(primary.run(view, None, asyncio.Event()))
    return primary


def cohort_spread(view: WorldView, bots: list[Bot], primary_actor: int) -> str:
    """Mean/ max distance of bound cohort actors to the primary (report row)."""
    primary = view.positions.get(primary_actor)
    if primary is None:
        return "no primary position"
    distances = []
    for bot in bots:
        pos = view.positions.get(bot.actor_id)
        if pos is not None:
            distances.append(math.hypot(pos[0] - primary[0], pos[1] - primary[1]))
    if not distances:
        return "no bound bots"
    return (
        f"n={len(distances)} mean {sum(distances) / len(distances):.1f}u max {max(distances):.1f}u"
    )


async def teardown_bots(bots: list[Bot]) -> None:
    for bot in bots:
        await bot.stop()
