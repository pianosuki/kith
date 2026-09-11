"""Integration: the visual example server over the real wire loop.

Spawns ``python -m kith.examples.visual.server`` through the example's
own process plumbing, connects one harness-engine client through the
example's client factory, and walks the full loop against the live
server: bootstrap (login, await replication), the metrics route, movement
advance, the chat broadcast round trip, and the RTT probe.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from collections.abc import Callable
from typing import Any

import pytest
from _helpers import _BUILD_DEBUG, needs_build

from kith import SimInput
from kith._agent.server_control import ServerControlClient
from kith.examples.visual import _launch, _store, _world
from kith.examples.visual import _protocol as proto
from kith.examples.visual._net import NetClient


pytestmark = needs_build

_PRINCIPAL = 9001


async def _wait_until(check: Callable[[], bool], timeout_s: float, what: str) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if check():
            return
        await asyncio.sleep(0.05)
    raise AssertionError(f"timed out waiting for {what}")


async def _probe(client: Any, control: ServerControlClient) -> None:
    # bootstrap: login sent, first replication frame awaited
    await _wait_until(lambda: client.query_status().bootstrap_state >= 2, 15.0, "client bootstrap")

    # metrics route: the world is alive and the bind is recorded
    metrics: dict[str, Any] = dict(await asyncio.wait_for(control.get("/playground/metrics"), 3.0))
    assert metrics["sessions"] >= 1
    assert metrics["ambient_count"] == 40
    bindings = {int(p): int(a) for p, a in metrics["bindings"]}
    actor_id = bindings[_PRINCIPAL]
    assert metrics["actors"] >= 1

    # replication: the client's event history carries the bot's records
    assert any(proto.actor_state_for(actor_id)(e) for e in client.recent_events(64))

    # movement: hold +x at run pace; the server truth advances the actor
    async def actor_x() -> float:
        state: dict[str, Any] = dict(await asyncio.wait_for(control.get("/query_state"), 3.0))
        return _world.q16_to_units(int(state["actors"][str(actor_id)]["x"]))

    start_x = await actor_x()
    tick = 1
    for _ in range(80):
        client.submit(
            proto.ACTOR_INPUT_TYPE,
            proto.encode_actor_input(
                actor_id,
                SimInput(input_tick=tick, move_x=32_767, move_y=0, flags=_world.RUN_FLAG),
            ),
        )
        tick += 1
        await asyncio.sleep(0.05)
    assert await actor_x() - start_x >= 30.0

    # chat: the sender's own window covers the broadcast cell, so the
    # round trip is visible in the sender's own event history
    client.submit(proto.CHAT_TYPE, proto.encode_chat(actor_id, "integration"))

    def chat_seen() -> bool:
        return any(
            e.type_id == proto.CHAT_EVENT_TYPE and proto.decode_chat(e.payload)[1] == "integration"
            for e in client.recent_events(64)
        )

    await _wait_until(chat_seen, 10.0, "chat broadcast round trip")

    # the C engine's RTT probe rode the ping/pong pair
    await _wait_until(lambda: client.query_status().rtt_last_ms > 0, 10.0, "rtt probe round trip")


async def _teardown(client: Any, control: ServerControlClient | None) -> None:
    current = asyncio.current_task()
    tasks = [t for t in asyncio.all_tasks() if t is not current]
    for task in tasks:
        task.cancel()
    with contextlib.suppress(asyncio.TimeoutError, asyncio.CancelledError):
        await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), 3.0)
    await client.stop()
    if control is not None:
        with contextlib.suppress(Exception):
            await asyncio.wait_for(control.close(), 2.0)


def test_visual_server_wire_loop() -> None:
    proc, gw, ctl = _launch.spawn_server(
        npcs=40,
        python_workers=2,
        env_extra={"KITH_LIB": str(_BUILD_DEBUG)},
    )
    control = ServerControlClient("127.0.0.1", ctl, timeout_s=2.0)
    client = proto.make_client(
        "integration-bot",
        "127.0.0.1",
        gw,
        principal_id=_PRINCIPAL,
        event_history_size=64,
        http_enabled=False,
        ipc_enabled=False,
    )
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(client.start())
        loop.run_until_complete(_probe(client, control))
    finally:
        try:
            loop.run_until_complete(_teardown(client, control))
        finally:
            loop.close()
            _launch.stop_server(proc)


def test_standalone_visual_client_receives_records() -> None:
    # The standalone `kith-visual client` wiring (NetClient) registers the
    # visual wire catalog on its engine, so replication records reach the
    # snapshot store instead of failing decode — the two-terminal flow's
    # engine half. The store counters are the --hud-log payload: a dead
    # client reads connected:false / records:0, the audited symptom.
    proc, gw, _ctl = _launch.spawn_server(
        npcs=40,
        python_workers=2,
        env_extra={"KITH_LIB": str(_BUILD_DEBUG)},
    )
    store = _store.SnapshotStore(tick_hz=20.0, max_speed_units=40.0)
    client = NetClient(store, host="127.0.0.1", port=gw, principal_id=_PRINCIPAL)

    def store_is_alive() -> bool:
        view = store.debug_view(0.0, 0.0)
        records = view["records"]
        return bool(view["connected"]) and isinstance(records, int) and records > 0

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(client.start())
        loop.run_until_complete(
            _wait_until(store_is_alive, 15.0, "the standalone client's records counter")
        )
    finally:
        try:
            loop.run_until_complete(_teardown(client, None))
        finally:
            loop.close()
            _launch.stop_server(proc)


def test_spawn_server_failure_names_the_boot_error() -> None:
    # A server that dies before the handshake carries its output into the
    # error: point the child at a library directory with no kith libs.
    with pytest.raises(RuntimeError, match="visual server"):
        _launch.spawn_server(
            npcs=4,
            env_extra={"KITH_LIB": "/nonexistent-kith-libs"},
        )
