"""Area-of-interest boundary scenario.

Two clients start co-located (same cell); one is teleported far outside
the other's subscription window and the scenario asserts the observer's
view withdraws the far actor (no ``actor_state`` for it), then teleports
it back and asserts the view restores it. This exercises the
cell-scoped publish boundary and the gateway's view
composition: an actor outside the subscriber's Chebyshev window is
absent from the composed view set, so no ``actor_state`` frame for it is
delivered.

Actor ids are pinned against the server's binding map at the guard step:
the observer and mover constants hold only when the server allocated the
first two principals as the fresh-server convention does, and a warm
server fails the guard loudly instead of mismatching actors silently.
The cell size is one tile (``1 << 16`` Q16.16 units) and the subscription
radius is 1, so 16 tiles away is well outside the neighborhood.
"""

from __future__ import annotations

import asyncio

from examples.spatial import messages
from examples.spatial.client import actor_state_for

from tools.agent.ahc import ClientEvent
from tools.agent.assertions import (
    assert_new_event,
    assert_no_event,
    assert_state_predicate,
)
from tools.agent.scenario import Scenario, ScenarioContext, step
from tools.agent.scenarios._support import assert_bound_actor


__all__ = ["AoiBoundary"]


_CLIENT_OBSERVER = "observer"
_CLIENT_MOVER = "mover"

_ACTOR_OBSERVER: int = 1
_ACTOR_MOVER: int = 2

_FAR_X: int = 16 << 16
_FAR_Y: int = 16 << 16


class AoiBoundary(Scenario):
    """Assert the view withdraws a far actor and restores it on return."""

    scenario_name = "aoi_boundary"

    connect_timeout_s: float = 30.0
    no_event_window_s: float = 1.5
    event_timeout_s: float = 10.0
    binding_timeout_s: float = 10.0
    view_refresh_s: float = 1.0

    def build(self, ctx: ScenarioContext) -> None:
        del ctx
        self.client(_CLIENT_OBSERVER).connect()
        self.client(_CLIENT_MOVER).connect()
        self._baseline: set[ClientEvent] = set()

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
        await assert_bound_actor(
            ctx, _CLIENT_OBSERVER, _ACTOR_OBSERVER, timeout_s=self.binding_timeout_s
        )
        await assert_bound_actor(ctx, _CLIENT_MOVER, _ACTOR_MOVER, timeout_s=self.binding_timeout_s)

    @step
    async def teleport_mover_far(self, ctx: ScenarioContext) -> None:
        await ctx.server_command(
            "/teleport",
            {"actor_id": _ACTOR_MOVER, "pos_x": _FAR_X, "pos_y": _FAR_Y},
        )

    @step
    async def assert_no_mover_state_while_far(self, ctx: ScenarioContext) -> None:
        await asyncio.sleep(self.view_refresh_s)
        await assert_no_event(
            ctx,
            _CLIENT_OBSERVER,
            messages.ACTOR_STATE_TYPE,
            actor_state_for(_ACTOR_MOVER),
            window_s=self.no_event_window_s,
        )

    @step
    async def teleport_mover_near(self, ctx: ScenarioContext) -> None:
        self._baseline = set(await ctx.events(_CLIENT_OBSERVER))
        await ctx.server_command(
            "/teleport",
            {"actor_id": _ACTOR_MOVER, "pos_x": 0, "pos_y": 0},
        )

    @step
    async def assert_mover_state_restored(self, ctx: ScenarioContext) -> None:
        await assert_new_event(
            ctx,
            _CLIENT_OBSERVER,
            messages.ACTOR_STATE_TYPE,
            actor_state_for(_ACTOR_MOVER),
            baseline=self._baseline,
            timeout_s=self.event_timeout_s,
        )
