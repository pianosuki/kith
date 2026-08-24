"""Unit tests for the built-in agentic scenarios.

Each test programs a :class:`_ReactiveHost` with the server contract the
scenario exercises and runs the scenario via
:func:`~tools.agent.scenario.run_scenario`. The host is deterministic:
``wait_for_event`` returns a queued event or ``None`` immediately, so
timeouts do not stall; ``assert_no_event`` windows and view-refresh
sleeps are overridden to a fraction of a second on the scenario instance.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping
from typing import Any

from examples.spatial import handlers, messages
from examples.spatial.client import (
    ActorStateView,
    encode_actor_state,
)
from tools.agent.ahc import ClientEvent, ClientStatus
from tools.agent.scenario import ScenarioStatus, StepStatus, run_scenario
from tools.agent.scenarios import (
    AoiBoundary,
    BasicConnect,
    MovementQuality,
    PresenceWindow,
    ProximityChat,
)


# ---------------------------------------------------------------------------
# reactive host
# ---------------------------------------------------------------------------


class _ReactiveHost:
    """A deterministic ScenarioHost that emits events on matched actions.

    Supports three reaction kinds:
    - ``react``: emit events when a client submits an exact ``(type_id, payload)``.
    - ``react_type``: emit events when a client submits any frame of ``type_id``,
      via a factory that receives the payload (for advancing state per submit).
    - ``command_react``: emit events when ``server_command`` is called on a
      ``path`` whose body satisfies a predicate.
    """

    def __init__(self) -> None:
        self._events: dict[str, list[ClientEvent]] = {}
        self._reactions: dict[tuple[str, int, bytes], list[tuple[str, int, bytes]]] = {}
        self._type_reactions: dict[
            tuple[str, int], Callable[[bytes], list[tuple[str, int, bytes]]]
        ] = {}
        self._command_reactions: list[
            tuple[str, Callable[[Mapping[str, object]], bool], list[tuple[str, int, bytes]]]
        ] = []
        self._clients: set[str] = set()
        self._cursor: dict[str, int] = {}
        self._submit_count: int = 0
        self._ts_counter: int = 0
        self._principals: dict[str, int] = {}

    def react(
        self,
        client: str,
        type_id: int,
        payload: bytes,
        *,
        emit: list[tuple[str, int, bytes]],
    ) -> None:
        self._reactions.setdefault((client, type_id, payload), []).extend(emit)

    def react_type(
        self,
        client: str,
        type_id: int,
        emit_factory: Callable[[bytes], list[tuple[str, int, bytes]]],
    ) -> None:
        self._type_reactions[(client, type_id)] = emit_factory

    def command_react(
        self,
        path: str,
        predicate: Callable[[Mapping[str, object]], bool],
        *,
        emit: list[tuple[str, int, bytes]],
    ) -> None:
        self._command_reactions.append((path, predicate, emit))

    def seed(self, target: str, ev_type: int, payload: bytes) -> None:
        """Pre-queue an event for a client as if delivered by the server."""
        self._emit(target, ev_type, payload, 0)

    def _emit(self, target: str, ev_type: int, ev_payload: bytes, corr: int) -> None:
        self._ts_counter += 1
        self._events.setdefault(target, []).append(
            ClientEvent(
                ts_mono_ns=self._ts_counter,
                type_id=ev_type,
                correlation_id=corr,
                payload=ev_payload,
            )
        )

    async def register_client(self, name: str, *, host: str, port: int) -> None:
        del host, port
        self._principals[name] = _PRINCIPAL_BASE + len(self._clients)
        self._clients.add(name)

    async def server_state(self, path: str) -> Mapping[str, object]:
        del path
        bindings = [
            {"principal_id": principal, "actor_id": index + 1}
            for index, principal in enumerate(self._principals.values())
        ]
        return {"bindings": bindings}

    def client_principal(self, name: str) -> int | None:
        return self._principals.get(name)

    async def start_client(self, name: str) -> None:
        del name

    async def stop_client(self, name: str) -> None:
        del name

    async def submit(
        self,
        client: str,
        type_id: int,
        payload: bytes = b"",
        *,
        flags: int = 0,
        correlation_id: int = 0,
    ) -> int:
        self._submit_count += 1
        for target, ev_type, ev_payload in self._reactions.get((client, type_id, payload), []):
            self._emit(target, ev_type, ev_payload, correlation_id)
        factory = self._type_reactions.get((client, type_id))
        if factory is not None:
            for target, ev_type, ev_payload in factory(payload):
                self._emit(target, ev_type, ev_payload, correlation_id)
        return self._submit_count

    async def register_type(self, client: str, name: str, type_id: int) -> None:
        del client, name, type_id

    async def server_command(
        self,
        path: str,
        body: Mapping[str, object] | None = None,
    ) -> Mapping[str, object]:
        for cmd_path, predicate, emit in self._command_reactions:
            if cmd_path == path and predicate(body or {}):
                for target, ev_type, ev_payload in emit:
                    self._emit(target, ev_type, ev_payload, 0)
        return {}

    async def client_state(self, client: str) -> ClientStatus:
        del client
        return self._connected_status()

    async def recent_events(self, client: str, count: int = 64) -> list[ClientEvent]:
        events = self._events.get(client, [])
        return list(events[-count:])

    async def wait_for_event(
        self, client: str, type_id: int, *, timeout_s: float
    ) -> ClientEvent | None:
        del timeout_s
        events = self._events.get(client, [])
        cursor = self._cursor.get(client, 0)
        for index in range(cursor, len(events)):
            if events[index].type_id == type_id:
                self._cursor[client] = index + 1
                return events[index]
        self._cursor[client] = len(events)
        return None

    async def wait_for_state(
        self,
        client: str,
        predicate: Any,
        *,
        timeout_s: float,
    ) -> bool:
        del client, timeout_s
        return bool(predicate(self._connected_status()))

    def _connected_status(self) -> ClientStatus:
        return ClientStatus(
            connected=True,
            bootstrap_state=2,
            bootstrap_step=3,
            bootstrap_step_count=3,
            rtt_last_ms=4,
            reconnect_attempts=0,
        )


class _DisconnectedHost(_ReactiveHost):
    """A reactive host where every client reports disconnected."""

    async def client_state(self, client: str) -> ClientStatus:
        del client
        return ClientStatus(
            connected=False,
            bootstrap_state=0,
            bootstrap_step=0,
            bootstrap_step_count=3,
            rtt_last_ms=0,
            reconnect_attempts=0,
        )

    async def wait_for_state(
        self,
        client: str,
        predicate: Any,
        *,
        timeout_s: float,
    ) -> bool:
        del client, timeout_s
        status = ClientStatus(
            connected=False,
            bootstrap_state=0,
            bootstrap_step=0,
            bootstrap_step_count=3,
            rtt_last_ms=0,
            reconnect_attempts=0,
        )
        return bool(predicate(status))


_SHRINK_ATTRS: dict[str, float] = {
    "connect_timeout_s": 0.02,
    "event_timeout_s": 0.02,
    "binding_timeout_s": 0.02,
    "no_event_window_s": 0.02,
    "view_refresh_s": 0.02,
    "move_duration_s": 0.05,
    "move_interval_s": 0.001,
    "settle_s": 0.001,
}


def _shrink_timeouts(scenario: Any) -> None:
    """Override a scenario's tunable timeouts to keep tests fast."""
    for attr, value in _SHRINK_ATTRS.items():
        if hasattr(scenario, attr):
            setattr(scenario, attr, value)


def _actor_state_payload(actor_id: int, pos_x: int = 0, pos_y: int = 0) -> bytes:
    return encode_actor_state(
        ActorStateView(
            actor_id=actor_id,
            pos_x=pos_x,
            pos_y=pos_y,
            pos_z=0,
            vel_x=0,
            vel_y=0,
            vel_z=0,
            input_tick=0,
            product_level=0,
        )
    )


def _to_int(value: object) -> int:
    if isinstance(value, bool | int | float):
        return int(value)
    return 0


def _is_near(body: Mapping[str, object]) -> bool:
    return _to_int(body.get("pos_x", 0)) == 0 and _to_int(body.get("pos_y", 0)) == 0


def _is_far(body: Mapping[str, object]) -> bool:
    return not _is_near(body)


_ACTOR_ALPHA = 1
_ACTOR_BETA = 2

_PRINCIPAL_BASE = 100


# ---------------------------------------------------------------------------
# registration
# ---------------------------------------------------------------------------


def test_builtin_scenarios_register_via_import() -> None:
    from tools.agent import scenario as scenario_mod

    names = scenario_mod.all_scenarios()
    assert "basic_connect" in names
    assert "proximity_chat" in names
    assert "aoi_boundary" in names
    assert "presence_window" in names
    assert "movement_quality" in names


def test_registry_classes_match_names() -> None:
    from tools.agent import scenario as scenario_mod

    assert scenario_mod.all_scenarios()["basic_connect"] is BasicConnect
    assert scenario_mod.all_scenarios()["proximity_chat"] is ProximityChat
    assert scenario_mod.all_scenarios()["aoi_boundary"] is AoiBoundary
    assert scenario_mod.all_scenarios()["presence_window"] is PresenceWindow
    assert scenario_mod.all_scenarios()["movement_quality"] is MovementQuality


# ---------------------------------------------------------------------------
# basic_connect
# ---------------------------------------------------------------------------


def test_basic_connect_passes_when_connected() -> None:
    scenario = BasicConnect()
    _shrink_timeouts(scenario)
    result = asyncio.run(run_scenario(scenario, _ReactiveHost()))
    assert result.status is ScenarioStatus.PASSED
    assert [s.name for s in result.steps] == ["assert_connected_and_ready"]


def test_basic_connect_fails_when_disconnected() -> None:
    scenario = BasicConnect()
    _shrink_timeouts(scenario)
    result = asyncio.run(run_scenario(scenario, _DisconnectedHost()))
    assert result.status is ScenarioStatus.FAILED
    assert result.steps[0].status is StepStatus.FAILED


# ---------------------------------------------------------------------------
# proximity_chat
# ---------------------------------------------------------------------------


def test_proximity_chat_passes_when_state_delivered() -> None:
    host = _ReactiveHost()
    # The retained bootstrap-era state the subscriber already holds: the
    # baseline snapshot must absorb it so only a post-chat delivery can
    # satisfy the headline assertion.
    host.seed("beta", messages.ACTOR_STATE_TYPE, _actor_state_payload(_ACTOR_ALPHA))
    host.react(
        "alpha",
        messages.CHAT_TYPE,
        handlers.encode_chat(_ACTOR_ALPHA, "hello from alpha"),
        emit=[
            (
                "beta",
                messages.CHAT_EVENT_TYPE,
                handlers.encode_chat(_ACTOR_ALPHA, "hello from alpha"),
            ),
            ("beta", messages.ACTOR_STATE_TYPE, _actor_state_payload(_ACTOR_ALPHA)),
        ],
    )
    scenario = ProximityChat()
    _shrink_timeouts(scenario)
    result = asyncio.run(run_scenario(scenario, host))
    assert result.status is ScenarioStatus.PASSED
    assert [s.name for s in result.steps] == [
        "assert_both_connected",
        "assert_bound_actors",
        "alpha_sends_chat",
        "assert_beta_receives_chat_event",
        "assert_beta_receives_alpha_state",
    ]


def test_proximity_chat_fails_on_retained_history_alone() -> None:
    """Retained pre-chat state does not satisfy the headline assertion.

    The subscriber's history holds the sender's bootstrap-era actor_state;
    with no chat-triggered delivery the scenario fails instead of matching
    the stale event.
    """
    host = _ReactiveHost()
    host.seed("beta", messages.ACTOR_STATE_TYPE, _actor_state_payload(_ACTOR_ALPHA))
    scenario = ProximityChat()
    _shrink_timeouts(scenario)
    result = asyncio.run(run_scenario(scenario, host))
    assert result.status is ScenarioStatus.FAILED
    failed = [s for s in result.steps if s.status is StepStatus.FAILED]
    assert [s.name for s in failed] == [
        "assert_beta_receives_chat_event",
        "assert_beta_receives_alpha_state",
    ]


def test_proximity_chat_fails_when_no_delivery() -> None:
    scenario = ProximityChat()
    _shrink_timeouts(scenario)
    result = asyncio.run(run_scenario(scenario, _ReactiveHost()))
    assert result.status is ScenarioStatus.FAILED
    failed = [s for s in result.steps if s.status is StepStatus.FAILED]
    assert failed


# ---------------------------------------------------------------------------
# aoi_boundary
# ---------------------------------------------------------------------------


def test_aoi_boundary_passes_full_cycle() -> None:
    host = _ReactiveHost()
    host.command_react(
        "/teleport",
        _is_near,
        emit=[("observer", messages.ACTOR_STATE_TYPE, _actor_state_payload(_ACTOR_BETA))],
    )
    scenario = AoiBoundary()
    _shrink_timeouts(scenario)
    result = asyncio.run(run_scenario(scenario, host))
    assert result.status is ScenarioStatus.PASSED
    assert [s.name for s in result.steps] == [
        "assert_both_connected",
        "assert_bound_actors",
        "teleport_mover_far",
        "assert_no_mover_state_while_far",
        "teleport_mover_near",
        "assert_mover_state_restored",
    ]


def test_aoi_boundary_fails_when_not_restored() -> None:
    host = _ReactiveHost()
    scenario = AoiBoundary()
    _shrink_timeouts(scenario)
    result = asyncio.run(run_scenario(scenario, host))
    assert result.status is ScenarioStatus.FAILED
    failed_names = {s.name for s in result.steps if s.status is StepStatus.FAILED}
    assert "assert_mover_state_restored" in failed_names


# ---------------------------------------------------------------------------
# presence_window
# ---------------------------------------------------------------------------


def test_presence_window_passes_full_cycle() -> None:
    host = _ReactiveHost()
    host.seed("alpha", messages.ACTOR_STATE_TYPE, _actor_state_payload(_ACTOR_BETA))
    host.command_react(
        "/teleport",
        _is_near,
        emit=[("alpha", messages.ACTOR_STATE_TYPE, _actor_state_payload(_ACTOR_BETA))],
    )
    scenario = PresenceWindow()
    _shrink_timeouts(scenario)
    result = asyncio.run(run_scenario(scenario, host))
    assert result.status is ScenarioStatus.PASSED


def test_presence_window_fails_when_not_restored() -> None:
    host = _ReactiveHost()
    scenario = PresenceWindow()
    _shrink_timeouts(scenario)
    result = asyncio.run(run_scenario(scenario, host))
    assert result.status is ScenarioStatus.FAILED


def test_binding_guard_fails_when_server_allocated_other_ids() -> None:
    class _WarmServerHost(_ReactiveHost):
        """A host whose binding map disagrees with the fresh-server ids."""

        async def server_state(self, path: str) -> Mapping[str, object]:
            del path
            bindings = [
                {"principal_id": principal, "actor_id": index + 50}
                for index, principal in enumerate(self._principals.values())
            ]
            return {"bindings": bindings}

    host = _WarmServerHost()
    scenario = PresenceWindow()
    _shrink_timeouts(scenario)
    result = asyncio.run(run_scenario(scenario, host))
    assert result.status is ScenarioStatus.FAILED
    failed = [s for s in result.steps if s.status is StepStatus.FAILED]
    assert failed[0].name == "assert_bound_actors"
    details = failed[0].details
    assert details is not None
    assert details["expected_actor_id"] == _ACTOR_ALPHA
    assert details["observed_bindings"] == {100: 50, 101: 51}


# ---------------------------------------------------------------------------
# movement_quality
# ---------------------------------------------------------------------------


def test_movement_quality_passes_with_advancing_state() -> None:
    host = _ReactiveHost()
    pos = [0]

    def emit_factory(_payload: bytes) -> list[tuple[str, int, bytes]]:
        pos[0] += 1000
        payload = _actor_state_payload(_ACTOR_BETA, pos_y=pos[0])
        return [
            ("mover", messages.ACTOR_STATE_TYPE, payload),
            ("observer", messages.ACTOR_STATE_TYPE, payload),
        ]

    host.react_type("mover", messages.ACTOR_INPUT_TYPE, emit_factory)
    scenario = MovementQuality()
    _shrink_timeouts(scenario)
    result = asyncio.run(run_scenario(scenario, host))
    assert result.status is ScenarioStatus.PASSED, [
        (s.name, s.status, s.error) for s in result.steps
    ]


def test_movement_quality_fails_with_no_movement() -> None:
    scenario = MovementQuality()
    _shrink_timeouts(scenario)
    result = asyncio.run(run_scenario(scenario, _ReactiveHost()))
    assert result.status is ScenarioStatus.FAILED
    failed = [s for s in result.steps if s.status is StepStatus.FAILED]
    assert failed
