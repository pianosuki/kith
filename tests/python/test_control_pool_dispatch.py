"""Integration tests for the standalone control-plane wrapper: Python routes
dispatch through a worker pool (owned by default), the no-pool case answers
503, and teardown joins the owned pool before the C handle is freed."""

from __future__ import annotations

import json
import threading
import urllib.error
import urllib.request
from collections.abc import Generator

import pytest
from _build_gate import needs_build

from kith._generated import types as gen_types
from kith.control import Control, Request, Response
from kith.exceptions import KithError, KithStateError
from kith.reactor import Reactor
from kith.worker import Worker


@pytest.fixture()
def control_stack() -> Generator[Control]:
    """One started control plane on an ephemeral port, driven by a reactor
    loop on a background thread. Teardown mirrors the C contract: the
    reactor stops first, then the control handle (and its owned pool) is
    released."""
    reactor = Reactor()
    control = Control(reactor, port=0, sse_flush_ms=50)
    control.start()
    thread = threading.Thread(target=reactor.run, daemon=True)
    thread.start()
    yield control
    reactor.stop()
    thread.join(timeout=5.0)
    assert not thread.is_alive()
    control.close()
    reactor.close()


@needs_build
def test_owned_pool_dispatches_routes(control_stack: Control) -> None:
    """A route registered on the wrapper runs on the wrapper-owned pool and
    answers over real HTTP; the handler observes the request view."""
    seen: dict[str, str] = {}

    def handler(req: Request, resp: Response) -> None:
        seen["path"] = req.path
        resp.status(200, "application/json")
        resp.body(b'{"ok":true}')

    control_stack.register_route("GET", "/hello", handler)
    port = control_stack.listen_port()
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/hello", timeout=5.0) as response:
        assert response.status == 200
        assert json.loads(response.read()) == {"ok": True}
    assert seen["path"] == "/hello"


@needs_build
def test_no_pool_answers_503() -> None:
    """With no pool (worker_count=0), a Python-bound route answers 503 and
    the handler never runs: the reactor never enters the interpreter."""
    reactor = Reactor()
    control = Control(reactor, port=0, worker_count=0, sse_flush_ms=50)
    control.start()
    thread = threading.Thread(target=reactor.run, daemon=True)
    thread.start()
    try:
        ran = False

        def handler(req: Request, resp: Response) -> None:
            nonlocal ran
            ran = True
            resp.status(200, "application/json")
            resp.body(b'{"ok":true}')

        control.register_route("GET", "/unattached", handler)
        port = control.listen_port()
        with pytest.raises(urllib.error.HTTPError) as excinfo:
            urllib.request.urlopen(f"http://127.0.0.1:{port}/unattached", timeout=5.0)
        assert excinfo.value.code == 503
        assert json.loads(excinfo.value.read()) == {"error": "unavailable"}
        excinfo.value.close()
        assert not ran
    finally:
        reactor.stop()
        thread.join(timeout=5.0)
        assert not thread.is_alive()
        control.close()
        reactor.close()


@needs_build
def test_explicit_pool_replaces_owned() -> None:
    """Attaching a caller-owned pool replaces the wrapper-owned default: the
    route dispatches through the attached pool, detaching answers 503, and
    teardown releases only what the wrapper still owns."""
    reactor = Reactor()
    control = Control(reactor, port=0, sse_flush_ms=50)
    control.start()
    thread = threading.Thread(target=reactor.run, daemon=True)
    thread.start()
    try:
        pool = Worker(worker_count=1)
        control.attach_worker_pool(pool)

        def handler(req: Request, resp: Response) -> None:
            resp.status(200, "application/json")
            resp.body(b'{"ok":true}')

        control.register_route("GET", "/attached", handler)
        port = control.listen_port()
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/attached", timeout=5.0) as response:
            assert response.status == 200

        control.attach_worker_pool(None)
        with pytest.raises(urllib.error.HTTPError) as excinfo:
            urllib.request.urlopen(f"http://127.0.0.1:{port}/attached", timeout=5.0)
        assert excinfo.value.code == 503
        excinfo.value.close()

        pool.close()
    finally:
        reactor.stop()
        thread.join(timeout=5.0)
        assert not thread.is_alive()
        control.close()
        reactor.close()


@needs_build
def test_start_twice_raises_state_error() -> None:
    """A second start answers the C contract's ESTATE as KithStateError."""
    reactor = Reactor()
    control = Control(reactor, port=0)
    control.start()
    try:
        with pytest.raises(KithStateError):
            control.start()
    finally:
        control.close()
        reactor.close()


@needs_build
def test_bind_failure_raises_kith_error() -> None:
    """A bind failure answers KithError, not KithStateError: a caller that
    treats KithStateError as a recoverable lifecycle state must not
    misdiagnose a fatal bind failure."""
    holder = Reactor()
    first = Control(holder, port=0)
    first.start()
    port = first.listen_port()
    reactor = Reactor()
    second = Control(reactor, port=port)
    try:
        with pytest.raises(KithError) as exc_info:
            second.start()
        assert not isinstance(exc_info.value, KithStateError)
        assert exc_info.value.code == gen_types.kith_error.KITH_EIO
    finally:
        second.close()
        reactor.close()
        first.close()
        holder.close()


@needs_build
def test_failed_handler_500_names_the_cause(control_stack: Control) -> None:
    """A handler exception answers 500 with the cause named in the body, so
    the operator reading the response sees which field broke."""

    def handler(req: Request, resp: Response) -> None:
        del req, resp
        raise KeyError("command")

    control_stack.register_route("POST", "/boom", handler)
    port = control_stack.listen_port()
    request = urllib.request.Request(f"http://127.0.0.1:{port}/boom", data=b"{}", method="POST")
    try:
        with urllib.request.urlopen(request, timeout=5.0):
            pytest.fail("a failing handler must answer 500")
    except urllib.error.HTTPError as exc:
        assert exc.code == 500
        body = json.loads(exc.read())
        exc.close()
        assert body["error"] == "internal"
        assert "command" in body["detail"]
