"""Client IPC commands, HTTP introspection and event serialization."""

from __future__ import annotations

import asyncio
import contextlib
import json
from collections.abc import Callable, Coroutine, Mapping
from typing import TYPE_CHECKING

from ._errors import KithClientError
from ._types import ClientEvent, ClientStatus


if TYPE_CHECKING:
    from .ahc import AgenticHeadlessClient


async def ipc_handle_peer(
    self: AgenticHeadlessClient, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
) -> None:
    self._ipc_peers.add((reader, writer))
    buf = b""
    try:
        while self._running:
            data = await reader.read(IPC_BUFFER_SIZE)
            if not data:
                break
            buf += data
            while b"\n" in buf:
                line, buf = buf.split(b"\n", 1)
                line = line.strip()
                if not line:
                    continue
                try:
                    req = json.loads(line.decode("utf-8"))
                except JSON_DECODE_ERRORS:
                    continue
                resp = await _dispatch_ipc(self, req)
                if resp is not None:
                    out = json.dumps(resp, separators=(",", ":")) + "\n"
                    writer.write(out.encode("utf-8"))
                    await writer.drain()
    except CONN_RESET_ERRORS:
        pass
    finally:
        self._ipc_peers.discard((reader, writer))
        writer.close()


async def _dispatch_ipc(
    self: AgenticHeadlessClient, req: Mapping[str, object]
) -> dict[str, object] | None:
    req_id = req.get("id")
    cmd = str(req.get("cmd", ""))
    try:
        handler = IPC_COMMANDS.get(cmd)
        if handler is None:
            return ack(req_id, "error", error=f"unknown command: {cmd}")
        return await handler(self, req_id, req)
    except KithClientError as exc:
        return ack(req_id, "error", error=str(exc))
    except (KeyError, ValueError, TypeError) as exc:
        return ack(req_id, "error", error=f"{type(exc).__name__}: {exc}")


async def http_handle_peer(
    self: AgenticHeadlessClient, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
) -> None:
    try:
        request_line = await reader.readline()
        if not request_line:
            return
        parts = request_line.decode("iso-8859-1").split()
        if len(parts) < 2:
            http_respond(writer, 400, b"bad request\n")
            return
        method, path = parts[0], parts[1]
        headers: dict[str, str] = {}
        while True:
            hline = await reader.readline()
            if hline in (b"\r\n", b"\n", b""):
                break
            key, _, val = hline.decode("iso-8859-1").partition(":")
            headers[key.strip().lower()] = val.strip()

        body = b""
        cl = headers.get("content-length")
        if cl:
            body = await reader.readexactly(min(int(cl), HTTP_MAX_BODY))

        await _http_route(self, method, path, headers, body, writer)
    except HTTP_READ_ERRORS:
        pass
    finally:
        if writer not in self._sse_writers:
            writer.close()


async def _http_route(
    self: AgenticHeadlessClient,
    method: str,
    path: str,
    headers: dict[str, str],
    body: bytes,
    writer: asyncio.StreamWriter,
) -> None:
    del headers
    if method == "GET" and path.startswith("/health"):
        http_respond(writer, 200, json_bytes({"status": "ok", "id": self.instance_id}))
        return
    if method == "GET" and path.startswith("/state"):
        http_respond(writer, 200, json_bytes(status_dict(self.query_status())))
        return
    if method == "GET" and path.startswith("/events"):
        qs = path.partition("?")[2]
        count = 64
        for pair in qs.split("&"):
            if pair.startswith("count="):
                count = int(pair[7:])
        http_respond(writer, 200, json_bytes([event_to_dict(e) for e in self.recent_events(count)]))
        return
    if method == "GET" and path.startswith("/stream"):
        _start_sse(self, writer)
        return
    if method == "POST" and path.startswith("/cmd"):
        try:
            req = json.loads(body.decode("utf-8")) if body else {}
        except JSON_DECODE_ERRORS:
            http_respond(writer, 400, b"bad json\n")
            return
        resp = await _dispatch_ipc(self, req)
        http_respond(writer, 200, json_bytes(resp or {}))
        return
    http_respond(writer, 404, b"not found\n")


def _start_sse(self: AgenticHeadlessClient, writer: asyncio.StreamWriter) -> None:
    head = (
        b"HTTP/1.1 200 OK\r\n"
        b"Content-Type: text/event-stream\r\n"
        b"Cache-Control: no-cache\r\n"
        b"Connection: keep-alive\r\n"
        b"\r\n"
    )
    writer.write(head)
    self._sse_writers.add(writer)


IPC_BUFFER_SIZE = 65536


HTTP_MAX_BODY = 1 << 20


JSON_DECODE_ERRORS = (json.JSONDecodeError, UnicodeDecodeError)


CONN_RESET_ERRORS = (ConnectionResetError, BrokenPipeError)


HTTP_READ_ERRORS = (asyncio.IncompleteReadError, ConnectionError, OSError)


def to_int(value: object) -> int:
    """Coerce a JSON-decoded value to int (str/int/float all accepted)."""
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int | float):
        return int(value)
    return int(str(value))


def event_list(value: object) -> list[object]:
    """Extract a list from a JSON-decoded events field, or empty."""
    if isinstance(value, list):
        return list(value)
    return []


async def cmd_connect(
    ahc: AgenticHeadlessClient, req_id: object, _req: Mapping[str, object]
) -> dict[str, object]:
    if ahc._connected:
        return ack(req_id, "ok")
    ahc.auto_connect = True
    return ack(req_id, "ok")


async def cmd_disconnect(
    ahc: AgenticHeadlessClient, req_id: object, _req: Mapping[str, object]
) -> dict[str, object]:
    ahc.auto_connect = False
    if ahc._transport is not None:
        ahc._transport.close()
    return ack(req_id, "ok")


async def cmd_submit(
    ahc: AgenticHeadlessClient, req_id: object, req: Mapping[str, object]
) -> dict[str, object]:
    type_id = to_int(req["type_id"])
    flags = to_int(req.get("flags", 0))
    corr = to_int(req.get("correlation_id", 0))
    payload_hex = req.get("payload_hex")
    payload = bytes.fromhex(str(payload_hex)) if payload_hex else b""
    cmd_id = ahc.submit(type_id, payload, flags=flags, correlation_id=corr)
    return ack(req_id, "ok", cmd_id=cmd_id)


async def cmd_register_type(
    ahc: AgenticHeadlessClient, req_id: object, req: Mapping[str, object]
) -> dict[str, object]:
    name = str(req["name"])
    type_id = to_int(req["type_id"])
    ahc.register_type(name, type_id)
    return ack(req_id, "ok")


async def cmd_query_state(
    ahc: AgenticHeadlessClient, req_id: object, _req: Mapping[str, object]
) -> dict[str, object]:
    result = status_dict(ahc.query_status())
    result["id"] = req_id
    result["type"] = "state"
    return result


async def cmd_recent_events(
    ahc: AgenticHeadlessClient, req_id: object, req: Mapping[str, object]
) -> dict[str, object]:
    count = to_int(req.get("count", 64))
    return {
        "id": req_id,
        "type": "events",
        "events": [event_to_dict(e) for e in ahc.recent_events(count)],
    }


async def cmd_subscribe(
    ahc: AgenticHeadlessClient, req_id: object, req: Mapping[str, object]
) -> dict[str, object]:
    events = event_list(req.get("events", []))
    if events == ["*"]:
        ahc._subscribe_all = True
    else:
        for ev in events:
            ahc._subscriptions.add(to_int(ev))
    return ack(req_id, "ok", subscriptions=subscription_state(ahc))


async def cmd_unsubscribe(
    ahc: AgenticHeadlessClient, req_id: object, req: Mapping[str, object]
) -> dict[str, object]:
    events = event_list(req.get("events", []))
    if events == ["*"]:
        ahc._subscribe_all = False
    else:
        for ev in events:
            ahc._subscriptions.discard(to_int(ev))
    return ack(req_id, "ok", subscriptions=subscription_state(ahc))


async def cmd_shutdown(
    ahc: AgenticHeadlessClient, req_id: object, _req: Mapping[str, object]
) -> dict[str, object]:
    ahc.auto_connect = False
    ahc._running = False
    return ack(req_id, "ok")


IpcHandler = Callable[
    ["AgenticHeadlessClient", object, Mapping[str, object]],
    Coroutine[None, None, dict[str, object] | None],
]


IPC_COMMANDS: Mapping[str, IpcHandler] = {
    "connect": cmd_connect,
    "disconnect": cmd_disconnect,
    "submit": cmd_submit,
    "register_type": cmd_register_type,
    "query_state": cmd_query_state,
    "recent_events": cmd_recent_events,
    "subscribe": cmd_subscribe,
    "unsubscribe": cmd_unsubscribe,
    "shutdown": cmd_shutdown,
}


def ack(req_id: object, status: str, **fields: object) -> dict[str, object]:
    result: dict[str, object] = {"id": req_id, "type": "ack", "status": status}
    result.update(fields)
    return result


def subscription_state(ahc: AgenticHeadlessClient) -> dict[str, object]:
    return {
        "all": ahc._subscribe_all,
        "type_ids": sorted(ahc._subscriptions),
    }


def status_dict(status: ClientStatus) -> dict[str, object]:
    return {
        "connected": status.connected,
        "bootstrap_state": status.bootstrap_state,
        "bootstrap_step": status.bootstrap_step,
        "bootstrap_step_count": status.bootstrap_step_count,
        "rtt_last_ms": status.rtt_last_ms,
        "reconnect_attempts": status.reconnect_attempts,
    }


def event_to_dict(event: ClientEvent) -> dict[str, object]:
    return {
        "ts_mono_ns": event.ts_mono_ns,
        "type_id": event.type_id,
        "correlation_id": event.correlation_id,
        "payload_hex": event.payload.hex(),
    }


def json_bytes(obj: object) -> bytes:
    return json.dumps(obj, separators=(",", ":")).encode("utf-8")


def http_respond(writer: asyncio.StreamWriter, code: int, body: bytes) -> None:
    reason = {200: "OK", 400: "Bad Request", 404: "Not Found"}.get(code, "OK")
    head = (
        f"HTTP/1.1 {code} {reason}\r\n"
        f"Content-Type: application/json\r\n"
        f"Content-Length: {len(body)}\r\n"
        f"Connection: close\r\n"
        f"\r\n"
    ).encode("iso-8859-1")
    writer.write(head + body)


def safe_write(writer: asyncio.StreamWriter, data: bytes) -> None:
    with contextlib.suppress(ConnectionError, OSError):
        writer.write(data)
