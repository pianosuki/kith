"""Proximity message-exchange scenario.

Two co-located clients exchange a chat message. One client submits a
``chat`` frame (the spatial C2S type 1003); the ``on_chat`` handler
submits a ``chat_event`` broadcast on the gateway's request queue — the
next tick pass delivers the frame to every session whose window covers
the sender's cell — and marks the cell dirty so the per-tick flush also
re-broadcasts the sender's cell product into the fabric, which triggers
the gateway to refresh the cell and re-deliver the sender's
``actor_state`` to nearby subscribers. The scenario asserts the nearby
subscriber receives both the ``chat_event`` carrying the chat line and a
fresh ``actor_state`` for the sender after the chat, proving the
chat -> broadcast -> delivery fanout and the chat -> fabric -> gateway ->
delivery path fired.

Actor ids are pinned against the server's binding map at the guard step:
the alpha and beta constants hold only when the server allocated the
first two principals as the fresh-server convention does, and a warm
server fails the guard loudly instead of mismatching actors silently.
The scenario snapshots the subscriber's event history immediately before
submitting the chat; the post-chat assertions match only deliveries
outside that baseline, so the retained bootstrap-era state cannot
satisfy them. The chat submit carries a correlation id so the runner's
failure diagnosis joins the chat's path through the event correlator.
"""

from __future__ import annotations

from collections.abc import Callable

from examples.spatial import handlers, messages
from examples.spatial.client import actor_state_for
from tools.agent.ahc import ClientEvent
from tools.agent.assertions import assert_new_event, assert_state_predicate
from tools.agent.scenario import Scenario, ScenarioContext, step
from tools.agent.scenarios._support import assert_bound_actor


__all__ = ["ProximityChat"]


_CLIENT_ALPHA = "alpha"
_CLIENT_BETA = "beta"

_ACTOR_ALPHA: int = 1
_ACTOR_BETA: int = 2

_CHAT_TEXT = "hello from alpha"

_CHAT_CORRELATION_ID: int = 4242


def _chat_event_for(actor_id: int, text: str) -> Callable[[ClientEvent], bool]:
    """Match ``chat_event`` events carrying ``actor_id``'s ``text``.

    The event payload is the ``chat`` frame's own layout (actor id +
    length + text), so the predicate decodes it with the C2S chat codec.
    Events whose payload does not decode return ``False`` rather than
    raising, so a malformed frame does not abort the step.
    """

    def predicate(event: ClientEvent) -> bool:
        try:
            seen_actor, seen_text = handlers.decode_chat(event.payload)
        except ValueError:
            return False
        return seen_actor == actor_id and seen_text == text

    return predicate


class ProximityChat(Scenario):
    """Assert a chat from one client reaches a nearby subscriber."""

    scenario_name = "proximity_chat"

    connect_timeout_s: float = 30.0
    event_timeout_s: float = 10.0
    binding_timeout_s: float = 10.0

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
    async def alpha_sends_chat(self, ctx: ScenarioContext) -> None:
        self._baseline = set(await ctx.events(_CLIENT_BETA))
        await ctx.submit(
            _CLIENT_ALPHA,
            messages.CHAT_TYPE,
            handlers.encode_chat(_ACTOR_ALPHA, _CHAT_TEXT),
            correlation_id=_CHAT_CORRELATION_ID,
        )

    @step
    async def assert_beta_receives_chat_event(self, ctx: ScenarioContext) -> None:
        await assert_new_event(
            ctx,
            _CLIENT_BETA,
            messages.CHAT_EVENT_TYPE,
            _chat_event_for(_ACTOR_ALPHA, _CHAT_TEXT),
            baseline=self._baseline,
            timeout_s=self.event_timeout_s,
        )

    @step
    async def assert_beta_receives_alpha_state(self, ctx: ScenarioContext) -> None:
        await assert_new_event(
            ctx,
            _CLIENT_BETA,
            messages.ACTOR_STATE_TYPE,
            actor_state_for(_ACTOR_ALPHA),
            baseline=self._baseline,
            timeout_s=self.event_timeout_s,
        )
