# Agentic Headless Client Guide

The harness in `tools/agent/` is type-id
agnostic at its core; the built-in scenarios attach game meaning through the
`examples/spatial/` library's wire catalog and control-plane
routes, and a scenario written against any other game's wire types uses the
same DSL.

## Why agentic testing is first-class

The framework is developed and operated by agentic AI workflows. Closed-loop
testing — an agent that spins up a server, connects clients, injects inputs,
observes both client and server state, and asserts on the joint outcome — is
the primary verification surface, not a test afterthought. See ADR-0006.

The closed loop depends on three surfaces being present together:

- The **server control plane** — an HTTP introspection and command API the
  server serves (default port 8080). The harness polls it for Prometheus
  metrics and JSON state and posts game-specific commands (for example
  `/teleport`) through it.
- The **headless client engine** (`libkith_client` + `libkith_proto`, driven
  by `kith/_agent/ahc.py`, re-exported at `tools/agent/ahc.py`) — a headless
  client that owns the TCP socket, decodes inbound frames through the proto
  codec, pops encoded outbound frames onto the socket, and drives the
  keepalive tick, all over asyncio. Inputs carry an optional correlation ID.
  The engine ships inside the framework package (private `kith._agent`
  modules) so packaged examples import the same engine the harness drives.
- **Correlation IDs** — an optional 8-byte wire-protocol trailer that carries
  a client input's correlation ID upstream; the surfaces that support it
  (the client event history, the correlator) join on that ID
  (ADR-0007, `docs/event_schema.md`).

## The harness

The harness lives in `tools/agent/`. Components:

| File | Role |
|---|---|
| `ahc.py` | Agentic headless client (`AgenticHeadlessClient`). Wraps `libkith_client`/`libkith_proto` via ctypes and drives a headless connection over asyncio — owns the TCP socket, decodes inbound frames, pops outbound frames, ticks keepalive. Also runs a Unix-socket NDJSON IPC server and a minimal HTTP/1.1 introspection server on the same loop. Type-id agnostic: message types register by name and numeric id. |
| `scenario.py` | Scenario DSL. `Scenario` is a base class to subclass; `@step` marks runnable methods; `ScenarioClient` is a fluent builder for declarative setup actions; `ScenarioContext` is the runtime handle a step receives; `run_scenario` runs a scenario against a `ScenarioHost` and returns a `ScenarioResult`. |
| `assertions.py` | Async assertion functions called inside `@step` bodies: `assert_event`, `assert_no_event`, `assert_new_event`, `assert_state_predicate`, `assert_correlation` (client-side, against `ScenarioContext`); `assert_server_metric`, `assert_server_state` (server-side, against the `ServerControl` protocol). Raises `ScenarioAssertionError` with a structured `.details` mapping on failure. |
| `event_correlator.py` | Synchronous, time-windowed index that joins client-side and server-side events by correlation ID and time. The orchestrator feeds it; scenario steps query it to verify a request (correlation ID `N`) produced the expected cross-side effect in time order. A different layer from `assert_correlation`, which polls a single client. |
| `orchestrator.py` | Bridges the `ScenarioHost` protocol to concrete `AgenticHeadlessClient` instances, a `ServerControl`, and an `EventCorrelator`. Satisfies `ScenarioHost` structurally. Same-loop design (no locks): the AHC connection drivers run as asyncio tasks on the scenario's loop, so synchronous AHC calls from host methods are race-free. An opt-in ingest task polls each client and the control plane and feeds the correlator. |
| `server_control.py` | `ServerControlClient`: an async HTTP/1.1 client over raw asyncio streams (stdlib only). Satisfies the `ServerControl` protocol structurally. Exposes `metrics` (Prometheus text scrape), `get`/`post` (JSON control-plane), and `health`. Reuses a keep-alive connection and serializes requests with an `asyncio.Lock`. |
| `env_manager.py` | `EnvironmentManager`: a typed synchronous wrapper over `docker compose` v2 for cluster lifecycle (`up`/`down`/`restart`/`stop`/`scale`/`logs`/`ps`/`build`/`exec` plus structured health checks). Used for distributed-topology scenarios; the embedded topology does not need it. |
| `embedded_host.py` | `embedded_host_factory`: boots `examples.embedded.server.EmbeddedServer` on a background thread and returns an `Orchestrator` bound to it. The `--embedded` runner path runs the full stack (TCP accept, wire dispatch, sim publish, fabric replication, gateway delivery, control-plane commands) in one process with no external server to start. |
| `runner.py` | CLI test runner. Selects scenarios by substring (`--filter`), lists the registry (`--list`), writes JSON / JUnit XML / Markdown (`--format`, `--output-json`/`--output-junit`/`--output-md`), boots an in-process server (`--embedded`), and re-runs on source change (`--watch`). The execution core (`run_scenarios`) is host-agnostic and loop-owned. |
| `report.py` | `Report`: renders a sequence of `ScenarioResult` into JSON, JUnit XML, and Markdown. A pure value object — no I/O, no mutable state — so it is testable without a server or event loop. Carries each failed step's structured `details` into the report body. |
| `scenarios/` | Built-in scenarios: `basic_connect`, `proximity_chat`, `aoi_boundary`, `presence_window`, `movement_quality`. Importing the package registers every subclass via `Scenario.__init_subclass__`. |

## The closed loop

```
        ┌──────────────────────────────────────────────────────┐
        │  agent (tools/agent/)                                │
        └──────────────────────────────────────────────────────┘
            │ setup                    │ act          │ assert/diagnose
            ▼                          ▼              ▼
   env_manager or           ScenarioContext         event_correlator
   embedded_host            submits interactive     joins client +
   boots the server          inputs via the          server events by
   (compose, or in           ScenarioHost ->         correlation_id +
   process for embedded)     AgenticHeadlessClient   time
                                     │
                                     ▼
                           orchestrator + server_control
                           poll the control plane (port 8080):
                           metrics scrape, JSON state, commands
```

1. **setup** — the runner builds a host per scenario. For `--embedded` the
   `embedded_host_factory` boots an in-process server on a background thread;
   for distributed topologies `env_manager` brings the cluster up through
   `docker compose`. The orchestrator creates the requested headless clients.
2. **act** — declarative actions (from the fluent `ScenarioClient` builder)
   and `@step` bodies drive the clients. Each client connects, completes its
   bootstrap, and submits interactive inputs through the headless client
   engine. Inputs carry correlation IDs when the scenario wants to
   cross-reference downstream events.
3. **assert** — `@step` bodies `await` assertion functions from
   `assertions.py` against the client event history and the server state /
   metrics exposed by the control plane.
4. **diagnose** — `event_correlator` reconstructs the path of a failed input
   across the planes (and across nodes in the distributed topology) by
   joining on `correlation_id`.
5. **report** — `runner` renders structured results (JSON, JUnit XML,
   Markdown) for the agent to consume and act on.

## The Scenario DSL

A scenario is a subclass of `Scenario` with `@step`-decorated methods. The
base class collects marked methods in source-line order at instantiation and
registers the subclass by `scenario_name` (which defaults to the class name)
via `__init_subclass__`.

A scenario runs in three phases, in order:

1. **`build(self, ctx)`** — an optional override for setup that does not fit
   the declarative action list (for example, initializing per-scenario
   state). May be sync or async.
2. **Declarative actions** — appended through the fluent
   `self.client(name)` builder (`connect`, `disconnect`, `register_type`,
   `submit`, `delay`). These express setup and initial traffic and run before
   steps.
3. **Steps** — `@step`-decorated methods, run in source-line order. A step is
   `async def step_name(self, ctx: ScenarioContext) -> None` (sync methods
   are also accepted); it returns `None` on success or raises on failure. A
   step `await`s assertions — it does not `yield` them.

`run_scenario(scenario, host)` runs the three phases. A failure in `build` or
the action phase fails the whole scenario with no steps recorded; a failure
in one step is recorded in a `StepResult` and does not abort the rest, so one
assertion failure does not mask the others.

Result types:

- `ScenarioResult` — `name`, `status` (`ScenarioStatus.PASSED` / `FAILED`),
  `started_s`, `duration_s`, and a sequence of `StepResult`.
- `StepResult` — `name`, `status` (`StepStatus.PASSED` / `FAILED` /
  `SKIPPED`), `duration_s`, `error` (string), and `details` (the structured
  mapping from `ScenarioAssertionError.details`).

## ScenarioContext

Each step receives a `ScenarioContext` as its sole argument (besides `self`).
It wraps the `ScenarioHost` and delegates the common operations so step
bodies read as `await ctx.submit(...)` rather than `await ctx.host.submit(...)`.

| Method | Returns | Notes |
|---|---|---|
| `connect(client, host, port)` | `None` | Register and start a named client. |
| `disconnect(client)` | `None` | Stop and release a client. |
| `register_type(client, name, type_id)` | `None` | Register a message type with the codec. |
| `submit(client, type_id, payload, *, flags, correlation_id)` | `int` | Submit an interactive command; returns the assigned command id. |
| `state(client)` | `ClientStatus` | Snapshot connection and bootstrap state. |
| `events(client, count=64)` | `list[ClientEvent]` | Up to the last `count` events (newest last). |
| `wait_for_event(client, type_id, *, timeout_s)` | `ClientEvent \| None` | Wait for an event of `type_id`. |
| `wait_for_state(client, predicate, *, timeout_s)` | `bool` | Wait until `predicate(state)` holds. |
| `server_command(path, body)` | `Mapping[str, object]` | POST a JSON `body` to a control-plane `path`; returns the JSON response. Game-specific control calls (teleport, query) go through here. |

## Assertions

Assertions are async functions called inside `@step` bodies. Each polls a
`ScenarioContext` (or a `ServerControl` for server-side checks) until a
predicate is satisfied or a deadline elapses, then returns the matched value
on success or raises `ScenarioAssertionError` on failure. The scenario runner
catches the exception per step and records it in the `StepResult`, so an
assertion failure feeds into the existing result-capture mechanism without a
parallel results list.

Client-side assertions (against `ScenarioContext`):

```python
async def assert_event(ctx, client, type_id, predicate, *, timeout_s=30.0) -> ClientEvent
async def assert_no_event(ctx, client, type_id, predicate, *, window_s=5.0) -> None
async def assert_new_event(ctx, client, type_id, predicate, *, baseline, timeout_s=30.0) -> ClientEvent
async def assert_state_predicate(ctx, client, predicate, *, timeout_s=30.0) -> ClientStatus
async def assert_correlation(ctx, client, correlation_id, *, min_events=1, timeout_s=30.0) -> list[ClientEvent]
```

`assert_event` polls for a `type_id` event satisfying `predicate` (a callable
`ClientEvent -> bool`) and returns it. `assert_no_event` snapshots the event
history and asserts no matching event arrives within `window_s`.
`assert_new_event` complements it: the caller snapshots `baseline` before the
triggering action, then this polls for a matching event outside the baseline.
`assert_state_predicate` waits for a `ClientStatus -> bool` predicate to
hold. `assert_correlation` polls for at least `min_events` events sharing a
correlation ID.

Server-side assertions (against the `ServerControl` protocol):

```python
async def assert_server_metric(control, name, predicate, *, timeout_s=10.0) -> float
async def assert_server_state(control, path, predicate, *, timeout_s=10.0) -> Mapping[str, object]
```

`assert_server_metric` scrapes Prometheus text from `control.metrics()`,
extracts the metric value, and evaluates `predicate` (a callable
`float -> bool`). `assert_server_state` fetches JSON from `control.get(path)`
and evaluates `predicate` (a callable `Mapping -> bool`). The
`ServerControlClient` in `server_control.py` satisfies the protocol
structurally.

A failed assertion raises `ScenarioAssertionError(message, *, details=...)`
whose `.details` mapping carries the offending event, actual state, or
metric value into the report.

## What the agent sees

- **Client side** — the headless client's event history, one `ClientEvent`
  per inbound frame: `ts_mono_ns` (monotonic timestamp in nanoseconds),
  `type_id`, `correlation_id`, and `payload` (bytes). Connection and
  bootstrap state is a `ClientStatus`: `connected`, `bootstrap_state`,
  `bootstrap_step`, `bootstrap_step_count`, `rtt_last_ms`,
  `reconnect_attempts`.
- **Server side** — the control plane's Prometheus metrics scrape and JSON
  state, polled by `server_control.py` over HTTP on port 8080.
- **Cross-referenced** — `event_correlator` output keyed by `correlation_id`,
  showing the plane-by-plane path each input took, tagged by kind (client
  vs server) and ordered by time.

The agent never reads raw zone state or per-connection queues directly; it
reasons through the same plane-bounded surfaces the framework exposes for
operations.

## Writing a scenario

A scenario is a Python module under `tools/agent/scenarios/` that subclasses
`Scenario`. The example below mirrors the shape of the built-in
`proximity_chat` scenario: two clients connect, one submits a chat frame,
and a step asserts the nearby subscriber receives the sender's replication
frame.

```python
from examples.spatial import handlers, messages
from examples.spatial.client import actor_state_for

from tools.agent.assertions import assert_event, assert_state_predicate
from tools.agent.scenario import Scenario, ScenarioContext, step


class ProximityChat(Scenario):
    """Assert a chat from one client reaches a nearby subscriber."""

    scenario_name = "proximity_chat"

    connect_timeout_s: float = 30.0
    event_timeout_s: float = 10.0

    def build(self, ctx: ScenarioContext) -> None:
        del ctx
        self.client("alpha").connect()
        self.client("beta").connect()

    @step
    async def assert_both_connected(self, ctx: ScenarioContext) -> None:
        await assert_state_predicate(
            ctx, "alpha", lambda s: s.connected, timeout_s=self.connect_timeout_s
        )
        await assert_state_predicate(
            ctx, "beta", lambda s: s.connected, timeout_s=self.connect_timeout_s
        )

    @step
    async def alpha_sends_chat(self, ctx: ScenarioContext) -> None:
        await ctx.submit(
            "alpha",
            messages.CHAT_TYPE,
            handlers.encode_chat(actor_id=1, text="hello from alpha"),
        )

    @step
    async def assert_beta_receives_alpha_state(self, ctx: ScenarioContext) -> None:
        await assert_event(
            ctx,
            "beta",
            messages.ACTOR_STATE_TYPE,
            actor_state_for(actor_id=1),
            timeout_s=self.event_timeout_s,
        )
```

`actor_state_for` returns a `ClientEvent -> bool` predicate that matches an
`actor_state` frame for the given actor id; it is the predicate argument to
`assert_event`. The `@step` decorator takes no arguments — it only marks the
method. Steps run in source-line order after the declarative actions.

## Running scenarios

The runner is `tools/agent/runner.py`. Importing it also imports the
`tools.agent.scenarios` package, so the built-in subclasses register and are
visible to `--list` and `--filter`.

```
python -m tools.agent.runner --list
python -m tools.agent.runner --filter aoi --embedded --format md
python -m tools.agent.runner --output-junit results.xml
python -m tools.agent.runner --watch --watch-path src --watch-path tools
```

Key options:

- `--filter` / `-f` — run scenarios whose name contains the substring.
- `--list` — list registered scenarios and exit.
- `--format` — stdout format: `md` (default), `json`, `junit`.
- `--output-json` / `--output-junit` / `--output-md` — write the report to a
  file in addition to stdout.
- `--host` / `--port` — target an external server (default 127.0.0.1:7777).
- `--embedded` — boot an in-process server per scenario instead of targeting
  an external server.
- `--watch` / `--watch-path` / `--watch-interval` — re-run when source files
  change.

## Built-in scenarios

The five built-ins live in `tools/agent/scenarios/` and exercise the
`examples/spatial/` library's wire catalog and control-plane surface:

- **`basic_connect`** — one client connects, completes the login bootstrap,
  and asserts the connection reaches the ready runtime state. The smallest
  closed-loop exercise of the harness (under fifty lines).
- **`proximity_chat`** — two co-located clients exchange a chat frame; asserts
  the nearby subscriber receives a fresh `actor_state` for the sender,
  proving the chat → fabric → gateway → delivery path fired.
- **`aoi_boundary`** — two clients start co-located; one is teleported far
  outside the other's subscription window and the scenario asserts the view
  withdraws the far actor, then teleports it back and asserts the view
  restores it. Exercises the cell-scoped publish boundary (ADR-0002).
- **`presence_window`** — two clients start co-located, confirm mutual
  visibility, then one is teleported across a cell boundary and back.
  Asserts the staying client's view withdraws the moved actor while it
  sits outside the subscription window and restores it on return,
  pinning the cell-scoped publish boundary from both directions
  (ADR-0002).
- **`movement_quality`** — two clients connect; one drives continuous movement
  input at a fixed cadence while the scenario samples the mover's replicated
  position and asserts the movement is smooth (no rubber-banding, no jitter,
  position advances on the primary axis), and the observer receives the
  mover's replication during the traversal.

## References

- ADR-0002 — publish products are immutable and cell-scoped.
- ADR-0003 — split/merge transfers with overlapping cell-stream handoff.
- ADR-0006 — agentic testing is first-class.
- ADR-0007 — correlation ID wire trailer.
- `docs/event_schema.md` — the carrier records and the `type` namespace this
  harness consumes.
- `docs/architecture/planes.md` — the plane boundaries the harness reasons
  across.
- `docs/architecture/topologies.md` — embedded versus distributed scenarios.
- `AGENTS.md` §4.1 — the agentic test tier; §4.4 — scaling gates verified by
  scenarios.
