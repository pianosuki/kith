"""Integration test: end-to-end wire replication through the embedded server.

Boots :class:`examples.embedded.server.EmbeddedServer` and drives a real
:class:`tools.agent.ahc.AgenticHeadlessClient` (the production headless wire
client) through the gateway's accept/dispatch/delivery path. The bootstrap
step sends a ``login`` frame and awaits an ``actor_state`` replication frame
(the gateway's ``replication_type_id``), so the test exercises the full
wire-replication fabric the gateway implements:

- the reactor accepts the TCP connection into the gateway's session table
  (a subscriber session auto-created on accept);
- the client's ``login`` frame is decoded and dispatched to the spatial
  ``on_login`` handler, which binds the session, seeds its subscription
  window, and notifies the cell so the gateway cache refreshes it;
- the gateway's per-tick pass refreshes the cache, composes the subscriber's
  view set, and delivers ``actor_state`` frames on the connection;
- the client's runtime frame handler decodes the frames and records them as
  events, so the test asserts decoded ``actor_state`` (type 1004) frames
  arrive carrying the bound actor's id.

A second actor then logs in and is teleported via the control plane into a
cell far outside the first subscriber's Chebyshev neighborhood; the first
subscriber's view withdraws the far actor (its cell is outside the window, so
the composer produces no candidate for it) and restores it when the far actor
is teleported back into the neighborhood.
"""

from __future__ import annotations

import asyncio
import json
import struct
import threading
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any, cast
from urllib import request

from _helpers import needs_build
from examples.embedded.server import EmbeddedServer
from examples.spatial import handlers as spatial_handlers
from examples.spatial import messages
from tools.agent.ahc import AgenticHeadlessClient, BootstrapStep, DecodedFrame

from kith import ServerStatus


_REPO_ROOT = Path(__file__).resolve().parents[2]


def _status(server: EmbeddedServer) -> ServerStatus:
    assert server._server is not None
    return server._server.status


def _post_json(url: str, body: dict[str, Any]) -> dict[str, Any]:
    data = json.dumps(body).encode("utf-8")
    req = request.Request(url, data=data, method="POST")
    req.add_header("Content-Type", "application/json")
    with request.urlopen(req, timeout=5.0) as resp:
        return cast("dict[str, Any]", json.loads(resp.read().decode("utf-8")))


# actor_state payload layout (big-endian / network byte order, 68 bytes;
# matches the proto frame header):
#   0-7   actor_id (uint64), 8-15 pos_x (int64), 16-23 pos_y (int64),
#   24-31 pos_z (int64), 56-59 input_tick (uint32), 60-63 update_seq
#   (uint32), 64-67 level (uint32).
_ACTOR_STATE_FMT: str = ">Qqqq"  # actor_id, pos_x, pos_y, pos_z (first 32 bytes)


def _decode_actor_state_actor_id(payload: bytes) -> int:
    """Return the actor id carried in an ``actor_state`` frame payload."""
    if len(payload) < 32:
        return 0
    return int(struct.unpack(_ACTOR_STATE_FMT, payload[:32])[0])


async def _wait_for_actor_state(
    ahc: AgenticHeadlessClient,
    *,
    cursor: int,
    timeout_s: float,
) -> tuple[list[int], int]:
    """Poll NEW AHC events since ``cursor`` until an actor_state frame lands.

    Returns the actor ids carried in every NEW actor_state frame observed and
    the updated cursor (the number of events consumed). The bootstrap FSM
    consumes the awaited frame (it does not forward it to the runtime
    handler), so the bootstrap-completing frame is read from the event history
    the C frame handler records for every inbound frame (bootstrap or
    runtime). The cursor advances past every event the poll has already
    inspected so a subsequent drain does not re-count stale history.
    """
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        events = ahc.recent_events(512)
        new_events = events[cursor:]
        ids = [
            _decode_actor_state_actor_id(ev.payload)
            for ev in new_events
            if ev.type_id == messages.ACTOR_STATE_TYPE
        ]
        cursor = len(events)
        if ids:
            return ids, cursor
        await asyncio.sleep(0.05)
    return [], cursor


async def _wait_for_view(
    ahc: AgenticHeadlessClient,
    *,
    cursor: int,
    expected: Callable[[set[int]], bool],
    timeout_s: float,
) -> tuple[set[int], int]:
    """Poll NEW actor_state frames until one tick's composed view matches.

    The gateway delivers every subject in the current view each tick, so a
    single poll's actor_state frames mirror that tick's view set. The wait
    ends at the first poll whose id set satisfies ``expected``. For an
    absence predicate a poll spanning a view change unions pre- and
    post-change frames, over-satisfies, and is rejected — the wait
    self-corrects; for an existence predicate the wait is sound when the
    expected membership is monotonic across the wait window (present
    once means present until the next change), which the scenario's steps
    below provide. The deadline must cover the gateway's cache and view
    refresh intervals (100 ms each by default): a publish that changes a
    cell's contents is not reflected in the delivered view until both
    fire.

    Returns the satisfying view and the updated cursor (the number of
    events consumed, so a subsequent wait does not re-count stale
    history). Raises AssertionError with the last observed view on
    timeout.
    """
    deadline = time.monotonic() + timeout_s
    latest: set[int] = set()
    while time.monotonic() < deadline:
        events = ahc.recent_events(512)
        new_events = events[cursor:]
        poll_ids = {
            _decode_actor_state_actor_id(ev.payload)
            for ev in new_events
            if ev.type_id == messages.ACTOR_STATE_TYPE
        }
        cursor = len(events)
        if poll_ids:
            latest = poll_ids
            if expected(poll_ids):
                return poll_ids, cursor
        await asyncio.sleep(0.05)
    raise AssertionError(
        f"composed view never matched within {timeout_s}s; last observed: {sorted(latest)}"
    )


def _login_bootstrap_step(principal_id: int) -> BootstrapStep:
    """Build the single bootstrap step: send ``login`` and await ``actor_state``.

    The spatial ``on_login`` handler binds the session, seeds the
    subscription window, and notifies the cell, but it does not enqueue a
    discrete ``login_reply`` frame: the server's response IS the replication
    stream, so the bootstrap awaits the first ``actor_state`` frame instead
    (the gateway's ``replication_type_id``). Awaiting the replication type
    proves the full publish→cache→compose→deliver path fired end-to-end.
    """

    def on_enter() -> tuple[int, bytes]:
        return messages.LOGIN_TYPE, spatial_handlers.encode_login(principal_id)

    def on_reply(_frame: DecodedFrame) -> int:
        # Advance to READY; any actor_state frame completes the bootstrap.
        return 1

    return BootstrapStep(
        await_type_id=messages.ACTOR_STATE_TYPE,
        on_enter=on_enter,
        on_reply=on_reply,
    )


@needs_build
class TestWireReplication:
    def test_login_delivers_actor_state_then_view_withdraws_and_restores(self) -> None:
        server = EmbeddedServer()
        facade, gateway_port, control_port = server.start()
        run_thread = threading.Thread(target=facade.run, daemon=True)
        run_thread.start()
        try:
            deadline = time.monotonic() + 5.0
            while _status(server) is not ServerStatus.RUNNING and time.monotonic() < deadline:
                time.sleep(0.01)
            assert _status(server) is ServerStatus.RUNNING

            base = f"http://127.0.0.1:{control_port}"

            async def scenario() -> None:
                # The subscriber logs in over the wire; the bootstrap step
                # sends login and awaits the first actor_state frame. The
                # gateway creates the session on accept, dispatches the
                # login frame to the handler, and the per-tick delivery
                # path enqueues the subscriber's own actor_state.
                subscriber = AgenticHeadlessClient(
                    instance_id="sub",
                    host="127.0.0.1",
                    port=gateway_port,
                    message_types=dict(messages.TYPES),
                    bootstrap_steps=[_login_bootstrap_step(principal_id=100)],
                    event_history_size=512,
                )
                await subscriber.start()
                try:
                    # Wait for the bootstrap to complete (the first
                    # actor_state frame arrives and the FSM reaches READY).
                    ready_deadline = time.monotonic() + 8.0
                    while time.monotonic() < ready_deadline:
                        status = subscriber.query_status()
                        if status.bootstrap_state == 2:  # READY
                            break
                        await asyncio.sleep(0.05)
                    assert subscriber.query_status().bootstrap_state == 2

                    ids, cursor = await _wait_for_actor_state(subscriber, cursor=0, timeout_s=5.0)
                    assert 1 in ids, "subscriber did not receive its own actor_state"

                    # A second principal logs in via the control plane (the
                    # harness path) so the shared actor table holds a second
                    # actor; it is spawned at the origin, in the same cell as
                    # the subscriber, so the subscriber's view includes it.
                    _post_json(f"{base}/login", {"principal_id": 200})
                    near, cursor = await _wait_for_view(
                        subscriber,
                        cursor=cursor,
                        expected=lambda ids: 2 in ids,
                        timeout_s=5.0,
                    )
                    assert 2 in near, "subscriber did not see the near actor"

                    # Teleport the far actor many cells away — outside the
                    # subscriber's Chebyshev-1 window — so its cell leaves the
                    # window and the composer withdraws it from the
                    # subscriber's view. The cell size is one tile
                    # (1 << 16 Q16.16 units), so 16 tiles away is well
                    # outside the radius-1 neighborhood.
                    _post_json(
                        f"{base}/teleport",
                        {"actor_id": 2, "pos_x": 16 << 16, "pos_y": 16 << 16},
                    )
                    far, cursor = await _wait_for_view(
                        subscriber,
                        cursor=cursor,
                        expected=lambda ids: 2 not in ids and 1 in ids,
                        timeout_s=5.0,
                    )
                    assert 2 not in far, (
                        "subscriber still sees the far actor after its cell left the window"
                    )
                    assert 1 in far, "subscriber lost its own actor state"

                    # Teleport the far actor back into the subscriber's
                    # neighborhood; the window re-includes its cell and the
                    # composer restores it to the view.
                    _post_json(
                        f"{base}/teleport",
                        {"actor_id": 2, "pos_x": 0, "pos_y": 0},
                    )
                    back, _cursor = await _wait_for_view(
                        subscriber,
                        cursor=cursor,
                        expected=lambda ids: 2 in ids and 1 in ids,
                        timeout_s=5.0,
                    )
                    assert 2 in back, (
                        "subscriber did not see the actor restored after it re-entered the window"
                    )
                finally:
                    await subscriber.stop()

            asyncio.run(scenario())
        finally:
            # The server handle is @thread_safety unsafe to destroy while
            # the run loop is in flight (server.h: kith_server_destroy).
            # The run loop runs on run_thread; shutdown() is safe from this
            # thread and asks the loop to drain and return, then join waits
            # for kith_server_run to exit before server.stop() destroys the
            # handle. Doing this in the finally keeps the run thread from
            # being torn down mid-loop when the scenario raises, which under
            # free-threaded Python (no GIL serializing the two threads)
            # unmaps the reactor's io_uring ring while kith_reactor_run is
            # still draining its completion queue.
            facade.shutdown()
            run_thread.join(timeout=5.0)
            assert _status(server) is ServerStatus.STOPPED
            server.stop()
