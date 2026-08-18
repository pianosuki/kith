"""Policy tests for the Python handler-exception guards.

Every pool-dispatched Python handler seam — tick, gateway message,
session-destroyed, and control route — catches ``Exception`` at its ctypes
trampoline: the exception is counted in the interpreter-scoped util counter
(``kith_python_handler_exceptions_total`` through the server's tick-path
recorder), the first exception per registration prints its traceback to
stderr under a header naming the seam, and subsequent ones count silently. The
dispatch continues either way; ``BaseException`` still escapes to ctypes'
unraisable print by contract and is not driven here.

The unit pins cover the guard factory itself (the seam all seven
trampolines share); the embedded-server pin covers the tick seam end to end
through a real worker pool, and the control-plane pin covers the route
seam's 500-plus-count shape. The message and destroyed seams wire the same
factory with the same two-line edit as the tick seam.
"""

from __future__ import annotations

import ctypes
import json
import threading
import time
import urllib.error
import urllib.request
from collections.abc import Generator

import pytest
from _build_gate import needs_build

from kith import Server, ServerStatus
from kith._bridge import (
    guarded_handler,
    handler_exception_reporter,
    load,
    response_overflow_reporter,
)
from kith.control import Control, Request, Response
from kith.reactor import Reactor


def _handler_exceptions() -> int:
    """Read the interpreter-scoped exception counter."""
    getter = load().lib("util").kith_python_handler_exceptions
    getter.argtypes = []
    getter.restype = ctypes.c_uint64
    return int(getter())


def test_guarded_handler_counts_and_prints_once(capsys: pytest.CaptureFixture[str]) -> None:
    """Each raise counts; the traceback prints once per registration."""
    before = _handler_exceptions()
    calls: list[int] = []

    def handler(n: int) -> None:
        calls.append(n)
        raise ValueError("unit boom")

    guarded = guarded_handler(handler, "unit handler")
    guarded(1)
    guarded(2)
    assert calls == [1, 2]
    assert _handler_exceptions() == before + 2
    err = capsys.readouterr().err
    assert err.count("kith: unit handler raised") == 1
    assert err.count("Traceback (most recent call last)") == 1
    assert "ValueError: unit boom" in err


def test_guarded_handler_returns_normally_after_a_raise() -> None:
    """The guard swallows the raise: the dispatch sees a completed callback
    and the next invocation still reaches the handler."""
    state = {"raise_next": True, "second_reached": False}

    def handler() -> None:
        if state["raise_next"]:
            state["raise_next"] = False
            raise RuntimeError("first call fails")
        state["second_reached"] = True

    guarded = guarded_handler(handler, "unit handler")
    guarded()
    guarded()
    assert state["second_reached"]


def test_reporter_counts_repeat_exceptions_silently(capsys: pytest.CaptureFixture[str]) -> None:
    """The reporter form (the route trampoline's seam) prints one sample and
    counts every raise without further output."""
    before = _handler_exceptions()
    report = handler_exception_reporter("unit reporter")
    try:
        raise KeyError("route boom")
    except KeyError as exc:
        report(exc)
        report(exc)
    assert _handler_exceptions() == before + 2
    err = capsys.readouterr().err
    assert err.count("kith: unit reporter raised") == 1
    assert "KeyError: 'route boom'" in err


@needs_build
def test_raising_tick_handler_is_counted_and_prints_once(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A tick handler that raises on every tick: the run drains normally,
    each raise is counted, and exactly one traceback lands on stderr."""
    server = Server()
    try:
        before = _handler_exceptions()
        raises = 0

        def on_tick(_tick: int) -> None:
            nonlocal raises
            raises += 1
            if raises >= 3:
                server.shutdown()
            raise ValueError("tick boom")

        server.register_tick_handler(on_tick)
        server.run()
        assert raises >= 3
        assert _handler_exceptions() >= before + 3
        assert _handler_exceptions() - before <= raises
        assert _status(server) is ServerStatus.STOPPED
        err = capsys.readouterr().err
        assert err.count("kith: tick handler raised") == 1
        assert err.count("Traceback (most recent call last)") == 1
        assert "ValueError: tick boom" in err
    finally:
        server.close()


def _status(server: Server) -> ServerStatus:
    return server.status


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


def test_raising_route_handler_answers_500_and_is_counted(
    control_stack: Control,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A route handler that raises still answers a 500 naming the cause,
    the exception is counted, and one traceback lands on stderr."""

    def handler(req: Request, resp: Response) -> None:
        raise KeyError("command")

    control_stack.register_route("GET", "/boom", handler)
    port = control_stack.listen_port()
    before = _handler_exceptions()
    with pytest.raises(urllib.error.HTTPError) as excinfo:
        urllib.request.urlopen(f"http://127.0.0.1:{port}/boom", timeout=5.0)
    assert excinfo.value.code == 500
    body = json.loads(excinfo.value.read())
    # The error response is a file-like object; leave it for the garbage
    # collector and its implicit close trips the unraisable collector.
    excinfo.value.close()
    assert "command" in body["detail"]
    assert _handler_exceptions() == before + 1
    err = capsys.readouterr().err
    assert err.count("kith: control route handler raised") == 1
    assert "KeyError: 'command'" in err


def test_response_overflow_reporter_prints_once(capsys: pytest.CaptureFixture[str]) -> None:
    """The oversize reporter prints the first oversize response per
    registration — route, attempted size, capacity, counter pointer — and
    stays silent afterwards; no traceback prints for a sizing condition."""
    note = response_overflow_reporter()
    note("/big", 300000, 262144)
    note("/big", 300000, 262144)
    note("/other", 10, 262144)
    err = capsys.readouterr().err
    assert err.count("kith: control route /big response of 300000 bytes") == 1
    assert "kith_control_response_overflow_total" in err
    assert "Traceback" not in err


def test_oversize_route_answers_canonical_reject(
    control_stack: Control,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A route response larger than the write buffer is a sizing condition,
    not a handler exception: every oversize request answers the canonical
    rejection naming route, attempted size, and capacity, the
    handler-exception counter does not move, and the stderr line prints once."""

    def handler(req: Request, resp: Response) -> None:
        resp.status(200, "application/json")
        resp.body(b"x" * 300000)

    control_stack.register_route("GET", "/big", handler)
    port = control_stack.listen_port()
    before = _handler_exceptions()
    for _ in range(2):
        with pytest.raises(urllib.error.HTTPError) as excinfo:
            urllib.request.urlopen(f"http://127.0.0.1:{port}/big", timeout=5.0)
        assert excinfo.value.code == 500
        body = json.loads(excinfo.value.read())
        excinfo.value.close()
        assert body == {
            "error": "response_too_large",
            "route": "/big",
            "attempted": 300000,
            "cap": 262144,
        }
    assert _handler_exceptions() == before
    err = capsys.readouterr().err
    assert err.count("kith: control route /big response of 300000 bytes") == 1
    assert "Traceback" not in err


def _wait_running(server: Server, timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if server.status is ServerStatus.RUNNING:
            return
        time.sleep(0.01)
    raise AssertionError("server did not reach RUNNING")


@needs_build
def test_facade_oversize_route_reclassifies_and_counts(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The facade route trampoline answers the canonical rejection — the
    ``control_write_buffer_cap`` kwarg sizes it end to end — the counter
    series shows on the server's own scrape, and the handler-exception
    counter does not move."""

    def handler(req: Request, resp: Response) -> None:
        resp.status(200, "application/json")
        resp.body(b"y" * 300000)

    server = Server()
    try:
        server.register_control_route("GET", "/listing", handler)
        run_thread = threading.Thread(target=server.run, daemon=True)
        run_thread.start()
        try:
            _wait_running(server)
            port = server.control_port
            before = _handler_exceptions()
            with pytest.raises(urllib.error.HTTPError) as excinfo:
                urllib.request.urlopen(f"http://127.0.0.1:{port}/listing", timeout=10.0)
            assert excinfo.value.code == 500
            body = json.loads(excinfo.value.read())
            excinfo.value.close()
            assert body == {
                "error": "response_too_large",
                "route": "/listing",
                "attempted": 300000,
                "cap": 262144,
            }
            assert _handler_exceptions() == before
            err = capsys.readouterr().err
            assert err.count("kith: control route /listing response of 300000 bytes") == 1
            scrape = urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=10.0)
            text = scrape.read().decode()
            scrape.close()
            line = next(
                series
                for series in text.splitlines()
                if series.startswith("kith_control_response_overflow_total")
            )
            assert line.split()[-1] == "1"
        finally:
            server.shutdown()
            run_thread.join(timeout=5.0)
    finally:
        server.close()


@needs_build
def test_facade_control_write_buffer_cap_sizes_the_rejection(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The facade kwarg is the sizing knob the docs directive names: a
    server built with a small capacity answers an oversize listing with the
    canonical rejection naming that capacity."""

    def handler(req: Request, resp: Response) -> None:
        resp.status(200, "application/json")
        resp.body(b"z" * 5000)

    server = Server(control_write_buffer_cap=2048)
    try:
        server.register_control_route("GET", "/listing", handler)
        run_thread = threading.Thread(target=server.run, daemon=True)
        run_thread.start()
        try:
            _wait_running(server)
            port = server.control_port
            with pytest.raises(urllib.error.HTTPError) as excinfo:
                urllib.request.urlopen(f"http://127.0.0.1:{port}/listing", timeout=10.0)
            assert excinfo.value.code == 500
            body = json.loads(excinfo.value.read())
            excinfo.value.close()
            assert body == {
                "error": "response_too_large",
                "route": "/listing",
                "attempted": 5000,
                "cap": 2048,
            }
        finally:
            server.shutdown()
            run_thread.join(timeout=5.0)
    finally:
        server.close()
