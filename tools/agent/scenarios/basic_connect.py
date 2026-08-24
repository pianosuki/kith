"""Basic connection scenario.

One client connects, completes the login bootstrap (send ``login``, await
the first ``actor_state`` replication frame), and asserts the connection
reaches the ready runtime state. This is the smallest closed-loop exercise
of the harness against the real server: the game-aware headless client's
bootstrap fires end-to-end through the gateway's accept, dispatch, and
delivery path, and the scenario asserts the bootstrap FSM reached READY
(state 2), proving the full wire-replication round trip completed.
"""

from __future__ import annotations

from tools.agent.assertions import assert_state_predicate
from tools.agent.scenario import Scenario, ScenarioContext, step


__all__ = ["BasicConnect"]


_CLIENT = "alpha"

# The bootstrap FSM's READY state (kith_client_bootstrap_state). The client
# reaches READY after the login frame's awaited actor_state arrives, proving
# the gateway accepted the connection, dispatched the login handler, and
# delivered a replication frame.
_BOOTSTRAP_READY: int = 2


class BasicConnect(Scenario):
    """Connect one client and assert the login bootstrap reaches ready."""

    scenario_name = "basic_connect"

    connect_timeout_s: float = 30.0

    def build(self, ctx: ScenarioContext) -> None:
        del ctx
        self.client(_CLIENT).connect()

    @step
    async def assert_connected_and_ready(self, ctx: ScenarioContext) -> None:
        await assert_state_predicate(
            ctx,
            _CLIENT,
            lambda s: s.connected and s.bootstrap_state == _BOOTSTRAP_READY,
            timeout_s=self.connect_timeout_s,
        )
