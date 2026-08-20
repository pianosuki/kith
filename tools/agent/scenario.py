"""Scenario DSL and step runner for the agentic harness.

A scenario is a subclass of :class:`Scenario` whose ``@step``-decorated
methods run in declaration order against a :class:`ScenarioHost`. The host
is a structural protocol satisfied by the orchestrator; a
stub or fake satisfies it for unit tests, so scenarios are testable without
the full client stack.

The DSL is type-id agnostic: a scenario registers and references numeric
message type ids and opaque payload bytes. No game vocabulary (sessions,
entities, chat, movement) lives here; a scenario attaches meaning to type
ids via :meth:`ScenarioClient.register_type` and :meth:`ScenarioClient.submit`.

A scenario is built in two phases:

- *Declarative actions* appended via the fluent :class:`ScenarioClient`
  builder (``connect``/``disconnect``/``register_type``/``submit``/``delay``).
  These run before steps and express setup + initial traffic.
- *Imperative steps* marked with ``@step``. These run in source-line order
  after the actions and perform waits and assertions. A step is a method
  ``async def step_name(self, ctx: ScenarioContext) -> None`` (sync methods
  are also accepted); it returns ``None`` on success or raises on failure.

:meth:`run_scenario` runs a scenario against a host, catching per-step
exceptions so one failure does not mask the rest, and returns a structured
:class:`ScenarioResult`.

When a step fails, the runner also gathers the cross-side evidence only it
can see — each client's recent-event tail and connection status, plus
event-correlator chains for every nonzero correlation id in those tails —
and attaches it to the failed step as :class:`StepResult.diagnosis`. Hosts
without the runner-level surfaces attach no diagnosis.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import enum
import inspect
import time
from collections.abc import Callable, Coroutine, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Protocol, runtime_checkable

from tools.agent.ahc import ClientEvent, ClientStatus
from tools.agent.event_correlator import CorrelatedEvent


__all__ = [
    "DEFAULT_DEADLINE_S",
    "Action",
    "ConnectAction",
    "DelayAction",
    "DisconnectAction",
    "RegisterTypeAction",
    "Scenario",
    "ScenarioClient",
    "ScenarioContext",
    "ScenarioHost",
    "ScenarioResult",
    "ScenarioStatus",
    "StepDiagnosis",
    "StepResult",
    "StepStatus",
    "all_scenarios",
    "get_scenario",
    "run_scenario",
    "step",
]


# ---------------------------------------------------------------------------
# step decorator
# ---------------------------------------------------------------------------


def step[T](func: Callable[..., T]) -> Callable[..., T]:
    """Mark a ``Scenario`` method as a runnable step.

    The decorator only sets a marker attribute and returns the function
    unchanged; the :class:`Scenario` base class collects marked methods in
    source-line order at instantiation. A step receives the
    :class:`ScenarioContext` as its sole argument (besides ``self``) and
    returns ``None`` on success or raises on failure.
    """
    func._is_step = True  # type: ignore[attr-defined]
    return func


# ---------------------------------------------------------------------------
# ScenarioHost protocol
#
# The orchestrator implements this protocol by name-routing
# to AgenticHeadlessClient instances. A stub or fake satisfies it for unit
# tests, so scenarios are testable without the full client stack.
# ---------------------------------------------------------------------------


@runtime_checkable
class ScenarioHost(Protocol):
    """The surface a scenario requires from its runtime environment.

    All methods are coroutines so the host may poll with ``asyncio.sleep``
    (for waits) and bridge synchronous AgenticHeadlessClient calls inside
    async wrappers.
    """

    async def register_client(self, name: str, *, host: str, port: int) -> None:
        """Register a named headless client targeting ``host:port``."""
        ...

    async def start_client(self, name: str) -> None:
        """Start a registered client's connection driver."""
        ...

    async def stop_client(self, name: str) -> None:
        """Stop a client's connection driver and release it."""
        ...

    async def submit(
        self,
        client: str,
        type_id: int,
        payload: bytes = b"",
        *,
        flags: int = 0,
        correlation_id: int = 0,
    ) -> int | concurrent.futures.Future[int]:
        """Submit an interactive command; returns the assigned command id."""
        ...

    async def register_type(self, client: str, name: str, type_id: int) -> None:
        """Register a message type by name and numeric id with the codec."""
        ...

    async def client_state(self, client: str) -> ClientStatus:
        """Snapshot the client's connection and bootstrap state."""
        ...

    async def recent_events(self, client: str, count: int = 64) -> list[ClientEvent]:
        """Return up to the last ``count`` events (newest last)."""
        ...

    async def wait_for_event(
        self, client: str, type_id: int, *, timeout_s: float
    ) -> ClientEvent | None:
        """Wait for an event of ``type_id``; returns it or ``None`` on timeout."""
        ...

    async def wait_for_state(
        self,
        client: str,
        predicate: Callable[[ClientStatus], bool],
        *,
        timeout_s: float,
    ) -> bool:
        """Wait until ``predicate(state)`` is true; returns whether it held."""
        ...

    def client_principal(self, name: str) -> int | None:
        """Return the principal id the client's login carries, if known.

        Hosts that assign principal identity at registration retain it so
        scenarios can attribute server-side state (bindings, per-principal
        counters) without inferring it from registration order.
        """
        ...

    async def server_state(self, path: str) -> Mapping[str, object]:
        """GET a JSON control-plane ``path``; returns the response body.

        The read-side counterpart of :meth:`server_command`: scenarios read
        server-owned truth (allocation maps, counters) through it.
        """
        ...

    async def server_command(
        self,
        path: str,
        body: Mapping[str, object] | None = None,
    ) -> Mapping[str, object]:
        """POST a JSON ``body`` to a server control-plane ``path``.

        Returns the JSON response. A scenario issues game-specific control
        commands (positioning, queries) through this method; the host's
        implementation routes the request to the server's control plane.
        """
        ...


# ---------------------------------------------------------------------------
# declarative actions (typed; appended by the ScenarioClient builder)
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ConnectAction:
    client: str
    host: str = "127.0.0.1"
    port: int = 7777


@dataclass(frozen=True, slots=True)
class DisconnectAction:
    client: str


@dataclass(frozen=True, slots=True)
class RegisterTypeAction:
    client: str
    name: str
    type_id: int


@dataclass(frozen=True, slots=True)
class SubmitAction:
    client: str
    type_id: int
    payload: bytes = b""
    flags: int = 0
    correlation_id: int = 0


@dataclass(frozen=True, slots=True)
class DelayAction:
    seconds: float


Action = ConnectAction | DisconnectAction | RegisterTypeAction | SubmitAction | DelayAction


# ---------------------------------------------------------------------------
# fluent builder
# ---------------------------------------------------------------------------


class ScenarioClient:
    """Accumulates declarative actions against a named client.

    Returned by :meth:`Scenario.client`; methods chain. Actions run before
    steps during :meth:`run_scenario`.
    """

    def __init__(self, name: str, actions: list[Action]) -> None:
        self.name = name
        self._actions = actions

    def connect(self, host: str = "127.0.0.1", port: int = 7777) -> ScenarioClient:
        self._actions.append(ConnectAction(self.name, host, port))
        return self

    def disconnect(self) -> ScenarioClient:
        self._actions.append(DisconnectAction(self.name))
        return self

    def register_type(self, name: str, type_id: int) -> ScenarioClient:
        self._actions.append(RegisterTypeAction(self.name, name, type_id))
        return self

    def submit(
        self,
        type_id: int,
        payload: bytes = b"",
        *,
        flags: int = 0,
        correlation_id: int = 0,
    ) -> ScenarioClient:
        self._actions.append(SubmitAction(self.name, type_id, payload, flags, correlation_id))
        return self

    def delay(self, seconds: float) -> ScenarioClient:
        self._actions.append(DelayAction(seconds))
        return self


# ---------------------------------------------------------------------------
# runtime context
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ScenarioContext:
    """The runtime handle a step receives.

    Wraps the :class:`ScenarioHost` and the scenario name, delegating the
    common operations so step bodies read as ``await ctx.submit(...)`` rather
    than ``await ctx.host.submit(name, ...)``.
    """

    host: ScenarioHost
    name: str

    async def connect(self, client: str, host: str = "127.0.0.1", port: int = 7777) -> None:
        await self.host.register_client(client, host=host, port=port)
        await self.host.start_client(client)

    async def disconnect(self, client: str) -> None:
        await self.host.stop_client(client)

    async def register_type(self, client: str, name: str, type_id: int) -> None:
        await self.host.register_type(client, name, type_id)

    async def submit(
        self,
        client: str,
        type_id: int,
        payload: bytes = b"",
        *,
        flags: int = 0,
        correlation_id: int = 0,
    ) -> int | concurrent.futures.Future[int]:
        return await self.host.submit(
            client,
            type_id,
            payload,
            flags=flags,
            correlation_id=correlation_id,
        )

    async def state(self, client: str) -> ClientStatus:
        return await self.host.client_state(client)

    async def events(self, client: str, count: int = 64) -> list[ClientEvent]:
        return await self.host.recent_events(client, count)

    async def wait_for_event(
        self, client: str, type_id: int, *, timeout_s: float = 30.0
    ) -> ClientEvent | None:
        return await self.host.wait_for_event(client, type_id, timeout_s=timeout_s)

    async def wait_for_state(
        self,
        client: str,
        predicate: Callable[[ClientStatus], bool],
        *,
        timeout_s: float = 30.0,
    ) -> bool:
        return await self.host.wait_for_state(client, predicate, timeout_s=timeout_s)

    def client_principal(self, client: str) -> int | None:
        """Return the principal id the client's login carries, if known."""
        return self.host.client_principal(client)

    async def server_state(self, path: str) -> Mapping[str, object]:
        """GET a JSON control-plane ``path``; returns the response body.

        Delegates to :meth:`ScenarioHost.server_state`; scenarios read
        server-owned truth (allocation maps, counters) through it.
        """
        return await self.host.server_state(path)

    async def server_command(
        self,
        path: str,
        body: Mapping[str, object] | None = None,
    ) -> Mapping[str, object]:
        """POST a JSON ``body`` to a server control-plane ``path``.

        Delegates to :meth:`ScenarioHost.server_command`; returns the JSON
        response. Scenarios use this to issue game-specific control commands
        (teleport, query) that have no wire type.
        """
        return await self.host.server_command(path, body)


# ---------------------------------------------------------------------------
# result types
# ---------------------------------------------------------------------------


class StepStatus(enum.StrEnum):
    PASSED = "passed"
    FAILED = "failed"
    SKIPPED = "skipped"


class ScenarioStatus(enum.StrEnum):
    PASSED = "passed"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class StepDiagnosis:
    """Cross-side evidence the runner gathered for a failed step.

    Carries what only the runner sees: each client's connection status and
    recent-event tail, and the event correlator's chains for every nonzero
    correlation id observed in those tails.
    """

    statuses: Mapping[str, ClientStatus]
    event_tails: Mapping[str, Sequence[ClientEvent]]
    correlation_chains: Mapping[int, Sequence[CorrelatedEvent]]


@dataclass(frozen=True, slots=True)
class StepResult:
    name: str
    status: StepStatus
    duration_s: float
    error: str | None = None
    details: Mapping[str, object] | None = None
    diagnosis: StepDiagnosis | None = None


@dataclass(frozen=True, slots=True)
class ScenarioResult:
    name: str
    status: ScenarioStatus
    started_s: float
    duration_s: float
    steps: Sequence[StepResult] = field(default_factory=tuple)
    error: str | None = None
    details: Mapping[str, object] | None = None

    @property
    def passed(self) -> bool:
        return self.status is ScenarioStatus.PASSED


# ---------------------------------------------------------------------------
# scenario base class + registry
# ---------------------------------------------------------------------------


_REGISTRY: dict[str, type[Scenario]] = {}


class Scenario:
    """Base class for agentic scenarios.

    Subclass and decorate step methods with :func:`step`. Optionally override
    :meth:`build` for setup that does not fit the declarative action list.
    A scenario is host-agnostic; the host is passed to :func:`run_scenario`.

    A scenario that exceeds :attr:`deadline_s` fails with a timeout error
    instead of stalling its tier; ``None`` applies :data:`DEFAULT_DEADLINE_S`.
    """

    scenario_name: str = ""
    deadline_s: float | None = None

    def __init_subclass__(cls, **kwargs: object) -> None:
        super().__init_subclass__(**kwargs)
        if not cls.scenario_name:
            cls.scenario_name = cls.__name__
        _REGISTRY[cls.scenario_name] = cls

    def __init__(self) -> None:
        self._actions: list[Action] = []
        self._steps: list[Callable[..., object]] = []
        for _name, method in inspect.getmembers(self, predicate=inspect.ismethod):
            if getattr(method, "_is_step", False):
                self._steps.append(method)
        self._steps.sort(key=lambda fn: inspect.getsourcelines(fn)[1])

    def client(self, name: str) -> ScenarioClient:
        """Return a fluent builder bound to this scenario's action list."""
        return ScenarioClient(name, self._actions)

    def build(self, ctx: ScenarioContext) -> Coroutine[None, None, None] | None:
        """Hook for scenario-specific setup; override as sync or async."""
        del ctx  # unused by default
        return None

    @property
    def name(self) -> str:
        return self.scenario_name


def get_scenario(name: str) -> type[Scenario]:
    """Look up a registered scenario subclass by name."""
    try:
        return _REGISTRY[name]
    except KeyError:
        available = ", ".join(sorted(_REGISTRY)) or "(none)"
        raise KeyError(f"no scenario named {name!r}; available: {available}") from None


def all_scenarios() -> dict[str, type[Scenario]]:
    """Return a copy of the registered scenario registry."""
    return dict(_REGISTRY)


# ---------------------------------------------------------------------------
# runner
# ---------------------------------------------------------------------------


#: Wall-clock cap for one :func:`run_scenario` call. A scenario that
#: exceeds it fails with a timeout error; the cap exists so a hung host
#: call fails its scenario instead of stalling the tier that runs it.
DEFAULT_DEADLINE_S: float = 300.0

#: Event tail per client in a step diagnosis.
_DIAGNOSIS_TAIL: int = 16

#: Wall-clock cap for one diagnosis gather.
_DIAGNOSIS_TIMEOUT_S: float = 2.0

_StepCallable = Callable[..., object]


async def _maybe_await(value: object) -> None:
    if inspect.isawaitable(value):
        await value


async def _run_actions(actions: Sequence[Action], host: ScenarioHost) -> None:
    for action in actions:
        if isinstance(action, ConnectAction):
            await host.register_client(action.client, host=action.host, port=action.port)
            await host.start_client(action.client)
        elif isinstance(action, DisconnectAction):
            await host.stop_client(action.client)
        elif isinstance(action, RegisterTypeAction):
            await host.register_type(action.client, action.name, action.type_id)
        elif isinstance(action, SubmitAction):
            await host.submit(
                action.client,
                action.type_id,
                action.payload,
                flags=action.flags,
                correlation_id=action.correlation_id,
            )
        elif isinstance(action, DelayAction):
            await asyncio.sleep(action.seconds)


def _correlation_chains(
    host: ScenarioHost,
    event_tails: Mapping[str, Sequence[ClientEvent]],
) -> dict[int, list[CorrelatedEvent]]:
    """Query the host's correlator for every nonzero id in the tails."""
    correlator = getattr(host, "correlator", None)
    find = getattr(correlator, "find_by_correlation", None)
    if not callable(find):
        return {}
    ids = {
        event.correlation_id
        for events in event_tails.values()
        for event in events
        if event.correlation_id != 0
    }
    chains: dict[int, list[CorrelatedEvent]] = {}
    for correlation_id in sorted(ids):
        try:
            chains[correlation_id] = list(find(correlation_id))
        except Exception:
            continue
    return chains


# The diagnose stage reads host surfaces beyond the ScenarioHost protocol
# (client_names, correlator) that only the real orchestrator carries, so the
# gather gates on getattr and hosts without them attach no diagnosis.
async def _gather_diagnosis(ctx: ScenarioContext) -> StepDiagnosis | None:
    """Gather failure evidence from the host's runner-level surfaces.

    Returns ``None`` when the host lacks the runner-level surfaces or the
    gather fails; a diagnosis is best-effort evidence and never masks the
    step failure it accompanies.
    """
    client_names = getattr(ctx.host, "client_names", None)
    if not callable(client_names):
        return None
    try:
        async with asyncio.timeout(_DIAGNOSIS_TIMEOUT_S):
            names = list(client_names())
            statuses: dict[str, ClientStatus] = {}
            event_tails: dict[str, list[ClientEvent]] = {}
            for name in names:
                try:
                    status = await ctx.host.client_state(name)
                    events = await ctx.host.recent_events(name, _DIAGNOSIS_TAIL)
                except Exception:
                    continue
                statuses[name] = status
                event_tails[name] = events
            return StepDiagnosis(
                statuses=statuses,
                event_tails=event_tails,
                correlation_chains=_correlation_chains(ctx.host, event_tails),
            )
    except Exception:
        return None


async def _run_step(
    step_fn: _StepCallable,
    ctx: ScenarioContext,
) -> StepResult:
    started = time.perf_counter()
    try:
        result = step_fn(ctx)
        await _maybe_await(result)
    except Exception as exc:
        details = getattr(exc, "details", None)
        if not isinstance(details, Mapping):
            details = None
        return StepResult(
            name=step_fn.__name__,
            status=StepStatus.FAILED,
            duration_s=time.perf_counter() - started,
            error=f"{type(exc).__name__}: {exc}",
            details=details,
            diagnosis=await _gather_diagnosis(ctx),
        )
    return StepResult(
        name=step_fn.__name__,
        status=StepStatus.PASSED,
        duration_s=time.perf_counter() - started,
    )


async def run_scenario(
    scenario: Scenario,
    host: ScenarioHost,
    *,
    deadline_s: float | None = None,
) -> ScenarioResult:
    """Run a scenario against ``host`` and return a structured result.

    Order: ``build`` hook → declarative actions → steps in source order. A
    failure in ``build`` or the action phase fails the scenario with no steps
    recorded and the exception reported in :attr:`ScenarioResult.error`; a
    failure in one step is recorded and does not abort the rest. A scenario
    that exceeds its deadline fails the same way, with the steps completed
    before the deadline preserved and a timeout reported in ``error``.

    The deadline resolves to ``deadline_s`` when given, else the scenario's
    ``deadline_s`` attribute, else :data:`DEFAULT_DEADLINE_S`; ``None`` at
    any layer leaves the next layer to decide.
    """
    limit = deadline_s if deadline_s is not None else scenario.deadline_s
    if limit is None:
        limit = DEFAULT_DEADLINE_S
    started = time.perf_counter()
    ctx = ScenarioContext(host=host, name=scenario.name)

    step_results: list[StepResult] = []
    try:
        async with asyncio.timeout(limit):
            await _maybe_await(scenario.build(ctx))
            await _run_actions(scenario._actions, host)
            for step_fn in scenario._steps:
                step_results.append(await _run_step(step_fn, ctx))
    except TimeoutError:
        return ScenarioResult(
            name=scenario.name,
            status=ScenarioStatus.FAILED,
            started_s=started,
            duration_s=time.perf_counter() - started,
            steps=tuple(step_results),
            error=f"scenario exceeded its {limit}s deadline",
        )
    except Exception as exc:
        details = getattr(exc, "details", None)
        if not isinstance(details, Mapping):
            details = None
        return ScenarioResult(
            name=scenario.name,
            status=ScenarioStatus.FAILED,
            started_s=started,
            duration_s=time.perf_counter() - started,
            steps=tuple(step_results),
            error=f"{type(exc).__name__}: {exc}",
            details=details,
        )

    status = ScenarioStatus.PASSED
    if any(r.status is StepStatus.FAILED for r in step_results):
        status = ScenarioStatus.FAILED

    return ScenarioResult(
        name=scenario.name,
        status=status,
        started_s=started,
        duration_s=time.perf_counter() - started,
        steps=tuple(step_results),
    )
