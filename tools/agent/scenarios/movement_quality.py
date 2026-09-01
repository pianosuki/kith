"""Movement quality scenario.

Two clients connect; one drives continuous movement input at a fixed
cadence while the scenario samples the mover's replicated position from
its own ``actor_state`` stream and asserts the movement is smooth (no
rubber-banding, no jitter, the position advances on the primary axis).
A second client observes the mover and the scenario asserts the
observer receives at least one ``actor_state`` for the mover during the
traversal, proving the replication stream keeps up with movement.

The mover drives the sim through the wire (``actor_input`` frames, the
hot path that maps to ``kith_sim_input_t``); the scenario reads the
mover's position from the ``actor_state`` frames the gateway delivers
back to the mover's own subscription. The movement direction is +y
(vertical) from the origin so the mover's path does not cross the
center pillar in the embedded world's 8x8 grid.

Actor ids are pinned against the server's binding map at the guard step:
the observer and mover constants hold only when the server allocated the
first two principals as the fresh-server convention does, and a warm
server fails the guard loudly instead of mismatching actors silently.
"""

from __future__ import annotations

import asyncio
import math
import time

from examples.spatial import handlers, messages
from examples.spatial.client import decode_actor_state, is_membership_record

from kith import SimInput
from tools.agent.assertions import ScenarioAssertionError, assert_state_predicate
from tools.agent.scenario import Scenario, ScenarioContext, step
from tools.agent.scenarios._support import assert_bound_actor


__all__ = ["MovementQuality"]


_CLIENT_OBSERVER = "observer"
_CLIENT_MOVER = "mover"

_ACTOR_MOVER: int = 2

_MOVE_INPUT: int = 200

_MIN_SAMPLES: int = 3
_MAX_REVERSALS: int = 2
# Per-tick displacement at _MOVE_INPUT is ~9600 Q16.16 units (the full
# 65536 input carries the base speed's 48 tiles per tick). The ceiling
# admits a ten-tick gap between samples while a rubber-band snap of one
# cell (65536 units) or more cannot pass.
_MAX_JUMP: float = 100_000.0


def _move_input(tick: int) -> SimInput:
    """Build a movement input driving +y at a low magnitude.

    The magnitude is calibrated so the mover advances slowly enough to
    stay within the observer's radius-1 subscription window for the
    full traversal (the cell size is one tile and the base speed is 48
    tiles/tick at full input, so a small fraction keeps the mover on
    the order of one tile over the drive duration).
    """
    return SimInput(input_tick=tick, move_x=0, move_y=_MOVE_INPUT, move_z=0, flags=0)


class MovementQuality(Scenario):
    """Assert continuous movement is smooth and observable by a peer."""

    scenario_name = "movement_quality"

    connect_timeout_s: float = 30.0
    binding_timeout_s: float = 10.0
    move_duration_s: float = 3.0
    move_interval_s: float = 0.05
    settle_s: float = 0.5

    def build(self, ctx: ScenarioContext) -> None:
        del ctx
        self.client(_CLIENT_OBSERVER).connect()
        self.client(_CLIENT_MOVER).connect()
        self._samples: list[tuple[int, int]] = []
        self._observer_saw_mover: bool = False

    @step
    async def assert_both_connected(self, ctx: ScenarioContext) -> None:
        await assert_state_predicate(
            ctx, _CLIENT_OBSERVER, lambda s: s.connected, timeout_s=self.connect_timeout_s
        )
        await assert_state_predicate(
            ctx, _CLIENT_MOVER, lambda s: s.connected, timeout_s=self.connect_timeout_s
        )

    @step
    async def assert_bound_actors(self, ctx: ScenarioContext) -> None:
        await assert_bound_actor(ctx, _CLIENT_MOVER, _ACTOR_MOVER, timeout_s=self.binding_timeout_s)

    @step
    async def continuous_movement(self, ctx: ScenarioContext) -> None:
        await asyncio.sleep(self.settle_s)
        deadline = time.monotonic() + self.move_duration_s
        tick = 0
        while time.monotonic() < deadline:
            tick += 1
            await ctx.submit(
                _CLIENT_MOVER,
                messages.ACTOR_INPUT_TYPE,
                handlers.encode_actor_input(_ACTOR_MOVER, _move_input(tick)),
            )
            await asyncio.sleep(self.move_interval_s)
            await self._sample_mover(ctx)
            await self._check_observer(ctx)

    @step
    async def verify_movement_quality(self, ctx: ScenarioContext) -> None:
        del ctx
        if len(self._samples) < _MIN_SAMPLES:
            raise ScenarioAssertionError(
                f"too few position samples: {len(self._samples)}",
                details={"sample_count": len(self._samples), "samples": self._samples},
            )

        reversals = 0
        for i in range(1, len(self._samples)):
            if self._samples[i][1] < self._samples[i - 1][1] - 1:
                reversals += 1
        if reversals > _MAX_REVERSALS:
            raise ScenarioAssertionError(
                f"rubber-banding: {reversals} reversals on y-axis",
                details={
                    "reversals": reversals,
                    "max_reversals": _MAX_REVERSALS,
                    "samples": self._samples,
                },
            )

        max_jump = 0.0
        for i in range(1, len(self._samples)):
            dx = abs(self._samples[i][0] - self._samples[i - 1][0])
            dy = abs(self._samples[i][1] - self._samples[i - 1][1])
            max_jump = max(max_jump, math.sqrt(dx * dx + dy * dy))
        if max_jump > _MAX_JUMP:
            raise ScenarioAssertionError(
                f"position jitter: max jump {max_jump:.0f} between samples",
                details={
                    "max_jump": max_jump,
                    "limit": _MAX_JUMP,
                    "samples": self._samples,
                },
            )

        start_y = self._samples[0][1]
        end_y = self._samples[-1][1]
        if abs(end_y - start_y) < 1:
            raise ScenarioAssertionError(
                f"insufficient movement: {abs(end_y - start_y)} units over {self.move_duration_s}s",
                details={
                    "start_y": start_y,
                    "end_y": end_y,
                    "move_duration_s": self.move_duration_s,
                    "samples": self._samples,
                },
            )

        if not self._observer_saw_mover:
            raise ScenarioAssertionError(
                "observer did not see mover during traversal",
                details={
                    "sample_count": len(self._samples),
                    "samples": self._samples,
                },
            )

    async def _sample_mover(self, ctx: ScenarioContext) -> None:
        """Record the mover's latest position from its actor_state stream."""
        for event in reversed(await ctx.events(_CLIENT_MOVER)):
            if event.type_id != messages.ACTOR_STATE_TYPE:
                continue
            try:
                view = decode_actor_state(event.payload)
                if is_membership_record(view):
                    continue
            except ValueError:
                continue
            if view.actor_id == _ACTOR_MOVER:
                self._samples.append((view.pos_x, view.pos_y))
                return

    async def _check_observer(self, ctx: ScenarioContext) -> None:
        """Mark the observer as having seen the mover if any actor_state matches."""
        if self._observer_saw_mover:
            return
        for event in await ctx.events(_CLIENT_OBSERVER):
            if event.type_id != messages.ACTOR_STATE_TYPE:
                continue
            try:
                view = decode_actor_state(event.payload)
                if is_membership_record(view):
                    continue
            except ValueError:
                continue
            if view.actor_id == _ACTOR_MOVER:
                self._observer_saw_mover = True
                return
