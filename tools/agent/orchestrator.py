"""Scenario orchestrator for the agentic harness.

Bridges the :class:`~tools.agent.scenario.ScenarioHost` protocol (the
surface scenarios drive) to concrete runtime components: in-process
:class:`~tools.agent.ahc.AgenticHeadlessClient` instances running on the
scenario's asyncio loop, an optional
:class:`~tools.agent.assertions.ServerControl` for server-side state, and
an :class:`~tools.agent.event_correlator.EventCorrelator` that the
orchestrator feeds by polling both.

The orchestrator is the composition point where the harness's async
contracts meet. It satisfies ``ScenarioHost`` structurally (no
inheritance): every host method is a coroutine that wraps a synchronous
``AgenticHeadlessClient`` call. Because asyncio is single-threaded and
the AHC connection drivers run as tasks on the same loop, those sync
calls are race-free — no locks, no threads. This is the key difference
from a threading-based design: under free-threaded Python
(``python3.14t``, which the framework supports) a second thread touching
the AHC's event deque would be a data race; a same-loop task never is.

A single asyncio ingest task polls each client's ``recent_events`` and
the server control plane on a fixed cadence and feeds both into the
correlator. Client-side dedup uses a per-client count cursor (robust
against the event history's ``maxlen`` rotation), not object identity,
so a recycled ``id()`` never skips a genuinely new event. The task is
opt-in: constructed idle, started by :meth:`start`, stopped by
:meth:`shutdown`.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import contextlib
import inspect
import time
from collections.abc import Callable, Mapping, Sequence
from typing import Protocol, runtime_checkable

from tools.agent.ahc import (
    AgenticHeadlessClient,
    ClientEvent,
    ClientStatus,
    KithClientError,
    WindowEvidence,
)
from tools.agent.assertions import ServerControl
from tools.agent.event_correlator import EventCorrelator
from tools.agent.server_control import ServerControlError


__all__ = [
    "Orchestrator",
]


_POLL_INTERVAL_S: float = 0.05
_EVENT_POLL_COUNT: int = 100_000


@runtime_checkable
class ManagedClient(Protocol):
    """The slice of ``AgenticHeadlessClient`` the orchestrator drives.

    Defined as a protocol so unit tests substitute a fake without
    loading the C shared libraries; the real client satisfies it
    structurally.
    """

    async def start(self) -> None: ...

    async def stop(self) -> None: ...

    def submit(
        self,
        type_id: int,
        payload: bytes = b"",
        *,
        flags: int = 0,
        correlation_id: int = 0,
    ) -> int | concurrent.futures.Future[int]: ...

    def register_type(self, name: str, type_id: int) -> None: ...

    def query_status(self) -> ClientStatus: ...

    def recent_events(self, count: int = 64) -> list[ClientEvent]: ...

    def evidence_frames(self) -> list[ClientEvent]: ...

    def evidence_span_ns(self) -> int | None: ...

    def begin_view_window(self) -> None: ...

    def seal_view_window(self, deadline_ns: int) -> None: ...

    def view_window(self) -> WindowEvidence | None: ...

    def last_frame_age_ns(self) -> int | None: ...

    def replication_count(self) -> int: ...

    principal_id: int


_AhcFactory = Callable[[str, str, int], ManagedClient]

_ServerEventExtractor = Callable[[Mapping[str, object]], Sequence[Mapping[str, object]]]


def _default_server_extractor(
    mapping: Mapping[str, object],
) -> Sequence[Mapping[str, object]]:
    """Extract the ``"events"`` list from a control-plane response."""
    value = mapping.get("events")
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, Mapping)]


class Orchestrator:
    """Implements ``ScenarioHost`` by driving in-process AHC instances.

    Owns a name → client registry, an :class:`EventCorrelator`, and (when
    started) one asyncio ingest task that feeds client and server events
    into the correlator. The correlator is injectable; if omitted a
    fresh one is created.
    """

    def __init__(
        self,
        *,
        correlator: EventCorrelator | None = None,
        server_control: ServerControl | None = None,
        server_events_path: str | None = None,
        server_event_extractor: _ServerEventExtractor | None = None,
        ingest_interval_s: float = _POLL_INTERVAL_S,
        ahc_factory: _AhcFactory | None = None,
        on_shutdown: Callable[[], None] | None = None,
    ) -> None:
        self._correlator = correlator if correlator is not None else EventCorrelator()
        self._server_control = server_control
        self._server_events_path = server_events_path
        self._server_extractor = (
            server_event_extractor
            if server_event_extractor is not None
            else _default_server_extractor
        )
        self._ingest_interval_s = ingest_interval_s
        self._ahc_factory: _AhcFactory = (
            ahc_factory if ahc_factory is not None else _default_ahc_factory
        )
        self._on_shutdown = on_shutdown
        self._clients: dict[str, ManagedClient] = {}
        self._last_ingested: dict[str, ClientEvent | None] = {}
        self._server_ingested_count: int = 0
        self._wait_cursor: dict[str, int] = {}
        self._ingest_task: asyncio.Task[None] | None = None
        self._running = False

    @property
    def correlator(self) -> EventCorrelator:
        return self._correlator

    @property
    def server_control(self) -> ServerControl | None:
        return self._server_control

    @property
    def is_running(self) -> bool:
        return self._running

    def client_names(self) -> list[str]:
        return list(self._clients)

    def client_principal(self, name: str) -> int | None:
        """Return the retained principal id of one registered client.

        Returns ``None`` for an unknown name or for a client built by
        the default factory, which carries no principal identity. The
        real headless client retains the principal its login bootstrap
        encodes, so callers attribute server-side state through this
        value instead of inferring it from registration order.
        """
        client = self._clients.get(name)
        if client is None or client.principal_id == 0:
            return None
        return int(client.principal_id)

    # -----------------------------------------------------------------------
    # ScenarioHost: client registry
    # -----------------------------------------------------------------------

    async def register_client(
        self,
        name: str,
        *,
        host: str,
        port: int,
    ) -> None:
        """Create a headless client targeting ``host:port`` (not yet started).

        AHC-level configuration (message types, bootstrap steps, library
        paths) is supplied through the ``ahc_factory`` passed at
        construction, not per registration, so the call matches the
        ``ScenarioHost`` protocol exactly.
        """
        if name in self._clients:
            raise KeyError(f"client already registered: {name!r}")
        client = self._ahc_factory(name, host, port)
        self._clients[name] = client
        self._last_ingested[name] = None
        self._wait_cursor[name] = 0

    async def start_client(self, name: str) -> None:
        await self._require_client(name).start()

    async def stop_client(self, name: str) -> None:
        client = self._clients.get(name)
        if client is not None:
            await client.stop()

    async def submit(
        self,
        client: str,
        type_id: int,
        payload: bytes = b"",
        *,
        flags: int = 0,
        correlation_id: int = 0,
    ) -> int | concurrent.futures.Future[int]:
        return self._require_client(client).submit(
            type_id, payload, flags=flags, correlation_id=correlation_id
        )

    async def register_type(self, client: str, name: str, type_id: int) -> None:
        self._require_client(client).register_type(name, type_id)

    async def client_state(self, client: str) -> ClientStatus:
        return self._require_client(client).query_status()

    async def recent_events(self, client: str, count: int = 64) -> list[ClientEvent]:
        return self._require_client(client).recent_events(count)

    async def evidence_frames(self, client: str) -> list[ClientEvent]:
        """Return the client's breadth-evidence window, oldest first.

        A forensic payload record under the client's byte budget; the
        breadth probe's counting basis is :meth:`view_window`.
        """
        return self._require_client(client).evidence_frames()

    async def evidence_span_ns(self, client: str) -> int | None:
        """Return the client's evidence-deque span, or None when empty."""
        return self._require_client(client).evidence_span_ns()

    async def view_window(self, client: str) -> WindowEvidence | None:
        """Return the client's movement-window view evidence, or None."""
        return self._require_client(client).view_window()

    async def last_frame_age_ns(self, client: str) -> int | None:
        """Return ns since the client's last replication frame, or None."""
        return self._require_client(client).last_frame_age_ns()

    async def replication_frame_count(self, client: str) -> int:
        """Return how many replication frames ``client`` has dispatched."""
        return self._require_client(client).replication_count()

    async def wait_for_event(
        self, client: str, type_id: int, *, timeout_s: float
    ) -> ClientEvent | None:
        """Return the next unseen event of ``type_id``, or ``None`` on timeout.

        Maintains a per-client cursor so successive calls do not re-return
        the same event; non-matching events are also skipped past.
        """
        deadline = time.monotonic() + timeout_s
        ahc = self._require_client(client)
        while True:
            events = ahc.recent_events(_EVENT_POLL_COUNT)
            cursor = self._wait_cursor.get(client, 0)
            if len(events) < cursor:
                cursor = 0
            for index in range(cursor, len(events)):
                if events[index].type_id == type_id:
                    self._wait_cursor[client] = index + 1
                    return events[index]
            self._wait_cursor[client] = len(events)
            if time.monotonic() >= deadline:
                return None
            await asyncio.sleep(_POLL_INTERVAL_S)

    async def wait_for_state(
        self,
        client: str,
        predicate: Callable[[ClientStatus], bool],
        *,
        timeout_s: float,
    ) -> bool:
        """Poll the client status until ``predicate`` holds or the deadline passes."""
        deadline = time.monotonic() + timeout_s
        ahc = self._require_client(client)
        while True:
            if predicate(ahc.query_status()):
                return True
            if time.monotonic() >= deadline:
                return False
            await asyncio.sleep(_POLL_INTERVAL_S)

    async def server_state(self, path: str) -> Mapping[str, object]:
        """GET a JSON control-plane ``path``; returns the response body.

        Delegates to the configured ``ServerControl``; raises
        ``RuntimeError`` when no control plane is configured (the host
        factory must supply one for scenarios that read server state).
        """
        if self._server_control is None:
            raise RuntimeError("no server control plane configured on this orchestrator")
        return await self._server_control.get(path)

    async def server_command(
        self,
        path: str,
        body: Mapping[str, object] | None = None,
    ) -> Mapping[str, object]:
        """POST a JSON ``body`` to a server control-plane ``path``.

        Delegates to the configured ``ServerControl``; raises
        ``RuntimeError`` when no control plane is configured (the host
        factory must supply one for scenarios that issue control commands).
        """
        if self._server_control is None:
            raise RuntimeError("no server control plane configured on this orchestrator")
        return await self._server_control.post(path, body or {})

    # -----------------------------------------------------------------------
    # ingest loop
    # -----------------------------------------------------------------------

    def start(self) -> None:
        """Start the background ingest task feeding the correlator.

        Idempotent: a second call while running is a no-op.
        """
        if self._ingest_task is not None and not self._ingest_task.done():
            return
        self._running = True
        self._ingest_task = asyncio.create_task(self._ingest_loop(), name="orchestrator-ingest")

    async def shutdown(self) -> None:
        """Stop the ingest task and every client, then clear the registry.

        If an ``on_shutdown`` callable was supplied at construction, it is
        called after the clients are stopped so an embedding host (e.g. an
        in-process server) is torn down once the harness is done with it.
        Every teardown step runs even when an earlier one raises; the
        failures surface together in one group at the end.
        """
        self._running = False
        task = self._ingest_task
        self._ingest_task = None
        if task is not None:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        errors: list[Exception] = []
        for client in self._clients.values():
            try:
                await client.stop()
            except Exception as exc:
                errors.append(exc)
        self._clients.clear()
        self._last_ingested.clear()
        self._server_ingested_count = 0
        self._wait_cursor.clear()
        if self._server_control is not None:
            close = getattr(self._server_control, "close", None)
            if callable(close):
                try:
                    result = close()
                    if inspect.isawaitable(result):
                        await result
                except Exception as exc:
                    errors.append(exc)
        if self._on_shutdown is not None:
            on_shutdown = self._on_shutdown
            self._on_shutdown = None
            try:
                on_shutdown()
            except Exception as exc:
                errors.append(exc)
        if errors:
            raise ExceptionGroup("orchestrator shutdown left failures", errors)

    async def _ingest_loop(self) -> None:
        """Poll clients and the server, feeding the correlator, until stopped."""
        while self._running:
            self._ingest_clients()
            await self._ingest_server()
            await asyncio.sleep(self._ingest_interval_s)

    def _ingest_clients(self) -> None:
        # Tail-follow each client's event history: find the position of the
        # event last ingested (by object identity, held by reference so it
        # is not garbage-collected and its identity cannot be reused) and
        # ingest only the events after it. When the last-ingested event has
        # rotated out of the bounded history, ingest the whole window (at
        # most one window of overlap, bounded).
        for name, client in list(self._clients.items()):
            try:
                events = client.recent_events(_EVENT_POLL_COUNT)
            except KithClientError:
                continue
            if not events:
                continue
            last = self._last_ingested.get(name)
            if last is None:
                new_events = events
            else:
                pos = _find_position(events, last)
                new_events = events if pos < 0 else events[pos + 1 :]
            if new_events:
                self._correlator.ingest_client_batch(name, new_events)
            self._last_ingested[name] = events[-1]

    async def _ingest_server(self) -> None:
        if self._server_control is None or self._server_events_path is None:
            return
        try:
            mapping = await self._server_control.get(self._server_events_path)
            events = self._server_extractor(mapping)
        except (ServerControlError, KeyError, ValueError, TypeError) as _exc:
            return
        if not events:
            return
        # The control plane exposes a cumulative append-only event log; ingest
        # only events beyond the last-seen count so repeated polls do not
        # re-ingest the same entries. If the log shrank (a restart), reset.
        ingested = self._server_ingested_count
        if len(events) < ingested:
            new_events = events
            self._server_ingested_count = len(events)
        else:
            new_events = events[ingested:]
            self._server_ingested_count = len(events)
        if new_events:
            self._correlator.ingest_server_batch("server", new_events)

    def managed_client(self, name: str) -> ManagedClient:
        """Return ``name``'s client handle for a direct synchronous drive.

        The movement phase calls this once per client before its submit
        rounds so the drive loop avoids re-resolving the registry (and
        the coroutine machinery of an async passthrough) on every input.
        """
        return self._require_client(name)

    # -----------------------------------------------------------------------
    # internals
    # -----------------------------------------------------------------------

    def _require_client(self, name: str) -> ManagedClient:
        try:
            return self._clients[name]
        except KeyError:
            raise KeyError(f"no client registered as {name!r}") from None


def _find_position(events: Sequence[ClientEvent], target: ClientEvent) -> int:
    """Return the index of ``target`` in ``events`` by identity (``is``).

    Scans from the newest end (most likely location for a just-ingested
    tail event) and returns ``-1`` if the event has rotated out of the
    window. Identity comparison (not equality) is correct here: the
    caller holds a reference to ``target``, so it cannot be garbage-
    collected and its identity cannot be recycled while it is still
    reachable.
    """
    for index in range(len(events) - 1, -1, -1):
        if events[index] is target:
            return index
    return -1


def _default_ahc_factory(
    instance_id: str,
    host: str,
    port: int,
) -> ManagedClient:
    """Construct the real ``AgenticHeadlessClient`` (loads the C libraries)."""
    return AgenticHeadlessClient(
        instance_id=instance_id,
        host=host,
        port=port,
    )
