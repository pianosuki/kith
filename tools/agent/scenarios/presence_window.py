"""Presence window scenario.

Two clients start co-located (same cell) and confirm mutual visibility.
One client is then teleported into a separate cell outside the other's
subscription window — the view withdraws it — and teleported back — the
view restores it. The withdrawal is asserted as an absence window on the
staying client's stream, and the restore as a new event after a baseline
snapshot, pinning both directions of the cell-scoped publish contract at
a subscription-window boundary.

The observer (alpha) stays at the origin so its subscription window
remains rooted on the cell where presence is tested; the mover (beta)
is the actor the control plane teleports across the boundary. Both
clients still observe each other's initial presence and the withdrawal
is asserted from alpha's perspective.

Actor ids are pinned against the server's binding map at the guard step:
the alpha and beta constants hold only when the server allocated the
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
    assert_event,
    assert_new_event,
    assert_no_event,
    assert_state_predicate,
)
from tools.agent.scenario import Scenario, ScenarioContext, step
from tools.agent.scenarios._support import assert_bound_actor


__all__ = ["PresenceWindow"]


_CLIENT_ALPHA = "alpha"
_CLIENT_BETA = "beta"

_ACTOR_ALPHA: int = 1
_ACTOR_BETA: int = 2

_APART_X: int = 16 << 16


class PresenceWindow(Scenario):
    """Assert presence withdraws across a window boundary and restores on return."""

    scenario_name = "presence_window"

    connect_timeout_s: float = 30.0
    no_event_window_s: float = 1.5
    event_timeout_s: float = 10.0
    binding_timeout_s: float = 10.0
    view_refresh_s: float = 1.0

    def build(self, ctx: ScenarioContext) -> None:
        del ctx
        self.client(_CLIENT_ALPHA).connect()
        self.client(_CLIENT_BETA).connect()
        self._baseline: set[ClientEvent] = set()

    @step
    async def assert_both_connected(self, ctx: ScenarioContext) -> None:
        await assert_state_predicate(
            ctx, _CLIENT_ALPHA, lambda s: s.connected, timeout_s=self.connect_timeout_s
        )
        await assert_state_predicate(
            ctx, _CLIENT_BETA, lambda s: s.connected, timeout_s=self.connect_timeout_s
        )

    @step
    async def assert_bound_actors(self, ctx: ScenarioContext) -> None:
        await assert_bound_actor(ctx, _CLIENT_ALPHA, _ACTOR_ALPHA, timeout_s=self.binding_timeout_s)
        await assert_bound_actor(ctx, _CLIENT_BETA, _ACTOR_BETA, timeout_s=self.binding_timeout_s)

    @step
    async def assert_initial_presence(self, ctx: ScenarioContext) -> None:
        await assert_event(
            ctx,
            _CLIENT_ALPHA,
            messages.ACTOR_STATE_TYPE,
            actor_state_for(_ACTOR_BETA),
            timeout_s=self.event_timeout_s,
        )

    @step
    async def teleport_apart(self, ctx: ScenarioContext) -> None:
        await ctx.server_command(
            "/teleport",
            {"actor_id": _ACTOR_BETA, "pos_x": _APART_X, "pos_y": 0},
        )

    @step
    async def assert_no_presence_while_apart(self, ctx: ScenarioContext) -> None:
        await asyncio.sleep(self.view_refresh_s)
        await assert_no_event(
            ctx,
            _CLIENT_ALPHA,
            messages.ACTOR_STATE_TYPE,
            actor_state_for(_ACTOR_BETA),
            window_s=self.no_event_window_s,
        )
        # Presence is mutual: the withdrawal reads the same from the other
        # side, so beta's stream must go quiet about alpha too.
        await assert_no_event(
            ctx,
            _CLIENT_BETA,
            messages.ACTOR_STATE_TYPE,
            actor_state_for(_ACTOR_ALPHA),
            window_s=self.no_event_window_s,
        )

    @step
    async def teleport_together(self, ctx: ScenarioContext) -> None:
        self._baseline = set(await ctx.events(_CLIENT_ALPHA))
        await ctx.server_command(
            "/teleport",
            {"actor_id": _ACTOR_BETA, "pos_x": 0, "pos_y": 0},
        )

    @step
    async def assert_presence_restored(self, ctx: ScenarioContext) -> None:
        await assert_new_event(
            ctx,
            _CLIENT_ALPHA,
            messages.ACTOR_STATE_TYPE,
            actor_state_for(_ACTOR_BETA),
            baseline=self._baseline,
            timeout_s=self.event_timeout_s,
        )
