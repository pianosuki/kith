"""Scenario assertions for the agentic harness.

Async assertion functions called inside ``@step`` bodies. Each polls a
:class:`~tools.agent.scenario.ScenarioContext` (or a :class:`ServerControl`
for server-side checks) until a predicate is satisfied or a deadline elapses,
then returns the matched value on success or raises
:class:`ScenarioAssertionError` on failure. The scenario runner catches the
exception per step and records it in the :class:`~tools.agent.scenario.StepResult`,
so an assertion failure feeds into the existing result-capture mechanism
without a parallel results list.

Client-side assertions (``assert_event`` / ``assert_no_event`` /
``assert_state_predicate`` / ``assert_correlation``) compose against
:class:`~tools.agent.scenario.ScenarioContext`, which wraps the
:class:`~tools.agent.scenario.ScenarioHost` and delegates event/state access.

Server-side assertions (``assert_server_metric`` / ``assert_server_state``)
compose against the :class:`ServerControl` protocol, a structural surface
satisfied by the server control-plane client. The protocol is defined
here so assertions depend on the structural surface, not on the client's
concrete type.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable, Mapping
from typing import Protocol, runtime_checkable

from tools.agent.ahc import ClientEvent, ClientStatus
from tools.agent.scenario import ScenarioContext


__all__ = [
    "ScenarioAssertionError",
    "ServerControl",
    "assert_correlation",
    "assert_event",
    "assert_new_event",
    "assert_no_event",
    "assert_server_metric",
    "assert_server_state",
    "assert_state_predicate",
]


_POLL_INTERVAL_S: float = 0.05


class ScenarioAssertionError(Exception):
    """Raised when a scenario assertion fails.

    Carries a human-readable message and a structured ``details`` mapping
    so the reporter can render context (the offending event, the actual
    state, the metric value) alongside the message.
    """

    def __init__(
        self,
        message: str,
        *,
        details: Mapping[str, object] | None = None,
    ) -> None:
        super().__init__(message)
        self.details: Mapping[str, object] = dict(details) if details else {}


# ---------------------------------------------------------------------------
# ServerControl protocol
#
# The server control-plane client implements this
# protocol by issuing HTTP requests to the server control plane. A stub or
# fake satisfies it for unit tests, so server-side assertions are testable
# without the full control-plane stack.
# ---------------------------------------------------------------------------


@runtime_checkable
class ServerControl(Protocol):
    """The surface server-side assertions require from the control plane.

    All methods are coroutines so polling is non-blocking under the async
    scenario runner.
    """

    async def metrics(self) -> str:
        """Return the Prometheus text-format metrics scrape."""
        ...

    async def get(self, path: str) -> Mapping[str, object]:
        """Return the JSON response for a control-plane GET ``path``."""
        ...

    async def post(self, path: str, body: Mapping[str, object]) -> Mapping[str, object]:
        """POST a JSON ``body`` to ``path``; return the JSON response."""
        ...


# ---------------------------------------------------------------------------
# client-side assertions
# ---------------------------------------------------------------------------


async def assert_event(
    ctx: ScenarioContext,
    client: str,
    type_id: int,
    predicate: Callable[[ClientEvent], bool],
    *,
    timeout_s: float = 30.0,
) -> ClientEvent:
    """Assert that an event of ``type_id`` satisfying ``predicate`` arrives.

    Polls :meth:`ScenarioContext.wait_for_event` (which filters by
    ``type_id``) and applies ``predicate`` to each matched event. Returns
    the first predicate-satisfying event or raises on timeout.
    """
    deadline = time.monotonic() + timeout_s
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        event = await ctx.wait_for_event(client, type_id, timeout_s=remaining)
        if event is None:
            break
        if predicate(event):
            return event
    raise ScenarioAssertionError(
        f"assert_event: no {type_id} event satisfying predicate on {client!r} within {timeout_s}s",
        details={"client": client, "type_id": type_id},
    )


async def assert_no_event(
    ctx: ScenarioContext,
    client: str,
    type_id: int,
    predicate: Callable[[ClientEvent], bool],
    *,
    window_s: float = 5.0,
) -> None:
    """Assert that no matching event arrives within ``window_s`` seconds.

    Snapshots the current event history at call time, then polls for the
    window duration. Raises immediately if a new event of ``type_id``
    satisfying ``predicate`` arrives; passes (returns ``None``) if the
    window elapses with no match.
    """
    baseline = set(await ctx.events(client))
    deadline = time.monotonic() + window_s
    while time.monotonic() < deadline:
        current = await ctx.events(client)
        for event in current:
            if event not in baseline and event.type_id == type_id and predicate(event):
                raise ScenarioAssertionError(
                    f"assert_no_event: unexpected {type_id} event on {client!r}",
                    details={"client": client, "type_id": type_id, "event": event},
                )
        await asyncio.sleep(_POLL_INTERVAL_S)


async def assert_new_event(
    ctx: ScenarioContext,
    client: str,
    type_id: int,
    predicate: Callable[[ClientEvent], bool],
    *,
    baseline: set[ClientEvent],
    timeout_s: float = 30.0,
) -> ClientEvent:
    """Assert that a new event (not in ``baseline``) arrives within ``timeout_s``.

    Complements :func:`assert_no_event`: the caller snapshots the event
    history as ``baseline`` before the triggering action, then this function
    polls for a matching event outside the baseline. Returns the matching
    event or raises on timeout. The set-based baseline comparison is robust
    against cursor drift — it matches only events that did not exist at the
    snapshot, regardless of how many prior events the host's cursor has
    advanced past.
    """
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        current = set(await ctx.events(client))
        for event in current - baseline:
            if event.type_id == type_id and predicate(event):
                return event
        await asyncio.sleep(_POLL_INTERVAL_S)
    raise ScenarioAssertionError(
        f"assert_new_event: no new {type_id} event satisfying predicate on "
        f"{client!r} within {timeout_s}s",
        details={"client": client, "type_id": type_id, "baseline_size": len(baseline)},
    )


async def assert_state_predicate(
    ctx: ScenarioContext,
    client: str,
    predicate: Callable[[ClientStatus], bool],
    *,
    timeout_s: float = 30.0,
) -> ClientStatus:
    """Assert that ``predicate(state)`` becomes true within ``timeout_s``.

    Delegates to :meth:`ScenarioContext.wait_for_state` (which polls
    internally) and returns the satisfying status on success. Raises with
    the final status snapshot on timeout.
    """
    ok = await ctx.wait_for_state(client, predicate, timeout_s=timeout_s)
    if ok:
        return await ctx.state(client)
    state = await ctx.state(client)
    raise ScenarioAssertionError(
        f"assert_state_predicate: predicate not satisfied on {client!r} within {timeout_s}s",
        details={"client": client, "state": state},
    )


async def assert_correlation(
    ctx: ScenarioContext,
    client: str,
    correlation_id: int,
    *,
    min_events: int = 1,
    timeout_s: float = 30.0,
) -> list[ClientEvent]:
    """Assert that at least ``min_events`` events share ``correlation_id``.

    Polls the client's recent event history for events whose
    ``correlation_id`` matches. Returns the matching list once it reaches
    ``min_events``; raises on timeout.
    """
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        matched = [e for e in await ctx.events(client) if e.correlation_id == correlation_id]
        if len(matched) >= min_events:
            return matched
        await asyncio.sleep(_POLL_INTERVAL_S)
    matched = [e for e in await ctx.events(client) if e.correlation_id == correlation_id]
    raise ScenarioAssertionError(
        f"assert_correlation: expected >={min_events} events with "
        f"correlation_id={correlation_id} on {client!r}, got {len(matched)}",
        details={
            "client": client,
            "correlation_id": correlation_id,
            "count": len(matched),
        },
    )


# ---------------------------------------------------------------------------
# server-side assertions
# ---------------------------------------------------------------------------


def _parse_metric(text: str, name: str) -> float | None:
    """Extract the first value for Prometheus metric ``name`` from ``text``.

    Handles both ``name value`` and ``name{labels} value`` forms. Skips
    comment lines (``#``) and lines where the metric name is only a prefix
    of a longer name (e.g. ``foo`` must not match ``foo_bar``).
    """
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if not stripped.startswith(name):
            continue
        rest = stripped[len(name) :]
        if rest and rest[0] not in (" ", "\t", "{"):
            continue
        tokens = rest.rsplit(None, 1)
        if not tokens:
            continue
        try:
            return float(tokens[-1])
        except ValueError:
            continue
    return None


async def assert_server_metric(
    control: ServerControl,
    name: str,
    predicate: Callable[[float], bool],
    *,
    timeout_s: float = 10.0,
) -> float:
    """Assert that server metric ``name`` satisfies ``predicate``.

    Scrapes Prometheus text from :meth:`ServerControl.metrics`, extracts
    the metric value, and evaluates ``predicate``. Retries until
    ``timeout_s`` elapses. Returns the satisfying value or raises on
    timeout.
    """
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        text = await control.metrics()
        value = _parse_metric(text, name)
        if value is not None and predicate(value):
            return value
        await asyncio.sleep(_POLL_INTERVAL_S)
    text = await control.metrics()
    value = _parse_metric(text, name)
    raise ScenarioAssertionError(
        f"assert_server_metric: {name} not satisfying predicate within {timeout_s}s",
        details={"name": name, "value": value},
    )


async def assert_server_state(
    control: ServerControl,
    path: str,
    predicate: Callable[[Mapping[str, object]], bool],
    *,
    timeout_s: float = 10.0,
) -> Mapping[str, object]:
    """Assert that server state at ``path`` satisfies ``predicate``.

    Fetches JSON from :meth:`ServerControl.get` and evaluates ``predicate``.
    Retries until ``timeout_s`` elapses. Returns the satisfying state or
    raises on timeout.
    """
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        state = await control.get(path)
        if predicate(state):
            return state
        await asyncio.sleep(_POLL_INTERVAL_S)
    state = await control.get(path)
    raise ScenarioAssertionError(
        f"assert_server_state: {path} not satisfying predicate within {timeout_s}s",
        details={"path": path, "state": dict(state)},
    )
