"""The visual client's network half: the harness engine subclass and its
thread.

``NetClient`` subclasses the certified harness client engine and routes
every runtime frame into the snapshot store: replication records parse
into it, chat lands in its ring, and the discrete login reply carries
the bound actor id. A 1 Hz poll refreshes the engine's status (connected,
RTT, reconnects) and the server's metrics scrape into the store.
``NetThread`` owns the daemon thread and its asyncio loop: start before
the window opens, stop after it closes.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import contextlib
import ctypes
import threading
import time
from typing import Any

from kith import SimInput
from kith._agent.ahc import AgenticHeadlessClient
from kith._agent.server_control import ServerControlClient
from kith.examples.visual import _store
from kith.examples.visual._protocol import (
    ACTOR_INPUT_TYPE,
    ACTOR_STATE_BATCH_TYPE,
    ACTOR_STATE_PAYLOAD_SIZE,
    ACTOR_STATE_TYPE,
    CHAT_EVENT_TYPE,
    CHAT_TYPE,
    LOGIN_REPLY_TYPE,
    PING_TYPE,
    PONG_TYPE,
    TYPES,
    decode_actor_state,
    decode_actor_state_batch,
    decode_chat,
    decode_login_reply,
    encode_actor_input,
    encode_chat,
    login_bootstrap_step,
)


POLL_INTERVAL_S = 1.0


class NetClient(AgenticHeadlessClient):
    """The harness engine with the visual client's frame intake.

    Every runtime frame lands here on the network thread (the C engine's
    pump drives the callback); replication parses into the snapshot
    store, chat into its ring, and the login_reply carries the bound
    actor id. The base class's event history is not used — the store is
    the consumer.
    """

    def __init__(
        self, store: _store.SnapshotStore, *, host: str, port: int, principal_id: int
    ) -> None:
        self._store = store
        super().__init__(
            instance_id=f"visual-{principal_id}",
            host=host,
            port=port,
            principal_id=principal_id,
            message_types=dict(TYPES),
            bootstrap_steps=[login_bootstrap_step(principal_id)],
            event_history_size=64,
            ipc_enabled=False,
            http_enabled=False,
            reconnect_enabled=True,
            ping_type_id=PING_TYPE,
            pong_type_id=PONG_TYPE,
            ping_interval_ms=500,
        )
        self._control: ServerControlClient | None = None

    def set_control_client(self, control: ServerControlClient | None) -> None:
        self._control = control

    def _on_frame_c(self, _client: int, _ctx: int, frame_ptr: Any) -> None:
        frame = frame_ptr.contents
        addr = frame.payload
        payload = ctypes.string_at(addr, int(frame.payload_len)) if addr else b""
        self._store.frame_received(len(payload))
        now_ns = time.monotonic_ns()
        type_id = int(frame.type_id)
        if type_id in (ACTOR_STATE_TYPE, ACTOR_STATE_BATCH_TYPE):
            if type_id == ACTOR_STATE_TYPE and len(payload) >= ACTOR_STATE_PAYLOAD_SIZE:
                records = [decode_actor_state(payload)]
            elif type_id == ACTOR_STATE_BATCH_TYPE:
                records = decode_actor_state_batch(payload)
            else:
                records = []
            self._store.absorb_records(records, now_ns)
        elif type_id == LOGIN_REPLY_TYPE:
            try:
                self._store.set_self_actor(decode_login_reply(payload))
                self._store.mark_link_up()  # a login reply IS a live wire
            except ValueError:
                pass
        elif type_id == CHAT_EVENT_TYPE:
            try:
                actor_id, text = decode_chat(payload)
            except ValueError:
                return
            self._store.record_chat(now_ns, actor_id, text)

    def submit_input(self, actor_id: int, inp: SimInput) -> Any:
        return self._watch(self.submit(ACTOR_INPUT_TYPE, encode_actor_input(actor_id, inp)))

    def submit_chat(self, actor_id: int, text: str) -> Any:
        return self._watch(self.submit(CHAT_TYPE, encode_chat(actor_id, text)))

    def _watch(self, result: Any) -> Any:
        """Foreign-thread submits resolve on the loop thread; a refused
        submit surfaces through the done-callback, so count it as a drop
        there."""
        if isinstance(result, concurrent.futures.Future):
            result.add_done_callback(self._submit_done)
        return result

    def _submit_done(self, fut: concurrent.futures.Future[Any]) -> None:
        if fut.exception() is not None:
            self._store.count_drop()

    async def _poll_status(self) -> None:
        """Refresh connection state, RTT, and the server scrape every second."""
        while True:
            try:
                status = self.query_status()
                self._store.set_status(
                    status.connected, status.rtt_last_ms, status.reconnect_attempts
                )
            except Exception:
                self._store.fail_status_poll()
            if self._control is not None:
                try:
                    data = await asyncio.wait_for(self._control.get("/playground/metrics"), 3.0)
                    self._store.set_metrics(dict(data))
                except Exception:
                    self._store.set_metrics(None)
            await asyncio.sleep(POLL_INTERVAL_S)


class NetThread:
    """The client's network thread: one asyncio loop driving the engine."""

    def __init__(self, client: NetClient) -> None:
        self._client = client
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._run, name="visual-net", daemon=True)

    def start(self) -> None:
        self._thread.start()

    def stop(self, timeout: float = 3.0) -> None:
        # The loop may already be closed (the thread finished its teardown).
        with contextlib.suppress(RuntimeError):
            self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join(timeout=timeout)

    def _run(self) -> None:
        asyncio.set_event_loop(self._loop)
        loop = self._loop
        loop.run_until_complete(self._client.start())
        loop.create_task(self._client._poll_status())
        try:
            loop.run_forever()
        finally:
            current = asyncio.current_task()
            tasks = [t for t in asyncio.all_tasks(loop) if t is not current]
            for task in tasks:
                task.cancel()
            with contextlib.suppress(asyncio.TimeoutError, asyncio.CancelledError):
                loop.run_until_complete(
                    asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), 3.0)
                )
            loop.run_until_complete(self._client.stop())
            control = self._client._control
            if control is not None:
                with contextlib.suppress(Exception):
                    loop.run_until_complete(asyncio.wait_for(control.close(), 2.0))
            with contextlib.suppress(RuntimeError):
                loop.close()
