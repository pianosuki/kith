"""Integration test: the embedded-topology server end-to-end.

Boots :class:`examples.embedded.server.EmbeddedServer`, verifies the gateway
listener accepts a TCP connection, drives the control plane
(``login``/``move``/``teleport``/``query_state``), and confirms the spatial
game logic runs unchanged on the embedded topology: the same wire catalog,
handlers, and tile2d physics as the spatial reference game, wired onto a
composition root with no coordination bus and no Postgres pool. The
control-plane routes delegate to the handler set's public mutation methods,
so the wire and the harness share one actor table. A final class boots the
standalone entry point as a real subprocess and proves SIGINT/SIGTERM drain
the server to a clean exit instead of the default-disposition kill.
"""

from __future__ import annotations

import json
import os
import re
import signal
import socket
import struct
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any, cast
from urllib import error, request

import pytest
from _helpers import _BUILD_DEBUG, needs_build
from examples.embedded.server import EmbeddedServer, _build_parser
from examples.spatial import messages
from examples.spatial.handlers import encode_login

from kith import ServerStatus, SimModel
from kith._generated import gateway as gen_gateway


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


def _get_json(url: str) -> dict[str, Any]:
    with request.urlopen(url, timeout=5.0) as resp:
        return cast("dict[str, Any]", json.loads(resp.read().decode("utf-8")))


@needs_build
class TestEmbeddedServer:
    def test_session_destroyed_callback_reclaims_ownership_maps(self) -> None:
        # The destroyed-session callback wired into the spatial handler
        # set: a real wire login binds the session (the ownership maps
        # carry the seat), the client's disconnect drives the framework's
        # close path, and the pool-dispatched callback pops the seat — so
        # a churn cycle releases the maps instead of growing them.
        server = EmbeddedServer()
        facade, gateway_port, _control_port = server.start()
        try:
            run_thread = threading.Thread(target=facade.run, daemon=True)
            run_thread.start()

            deadline = time.monotonic() + 5.0
            while _status(server) is not ServerStatus.RUNNING and time.monotonic() < deadline:
                time.sleep(0.01)
            assert _status(server) is ServerStatus.RUNNING

            def _login(principal_id: int) -> socket.socket:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(5.0)
                sock.connect(("127.0.0.1", gateway_port))
                frame = struct.pack(">2sBBHI", b"KT", 1, 0, messages.LOGIN_TYPE, 8) + encode_login(
                    principal_id
                )
                sock.sendall(frame)
                return sock

            handlers = server.handlers
            assert handlers is not None
            sock = _login(100)
            try:
                deadline = time.monotonic() + 5.0
                while len(handlers._session_actors) != 1 and time.monotonic() < deadline:
                    time.sleep(0.01)
                assert len(handlers._session_actors) == 1
                assert next(iter(handlers._session_actors.values())) >= 1
                assert all(handlers._actor_binds.values())
            finally:
                sock.close()

            # The disconnect fires the destroyed-session callback: the seat
            # pops and the bind set empties. A never-logged-in session
            # (the accept above delivered no login) leaves nothing behind.
            deadline = time.monotonic() + 5.0
            while handlers._session_actors and time.monotonic() < deadline:
                time.sleep(0.01)
            assert handlers._session_actors == {}
            assert handlers._actor_binds == {}
            assert handlers._bind_conflicts == 0

            # A reconnect of the same principal reclaims a fresh seat: the
            # relogin rebinds without a conflict against the reclaimed
            # state.
            sock2 = _login(100)
            try:
                deadline = time.monotonic() + 5.0
                while len(handlers._session_actors) != 1 and time.monotonic() < deadline:
                    time.sleep(0.01)
                assert len(handlers._session_actors) == 1
                assert handlers._bind_conflicts == 0
            finally:
                sock2.close()

            facade.shutdown()
            run_thread.join(timeout=5.0)
            assert _status(server) is ServerStatus.STOPPED
        finally:
            server.stop()

    def test_login_move_teleport_query_end_to_end(self) -> None:
        server = EmbeddedServer()
        facade, gateway_port, control_port = server.start()
        try:
            assert _status(server) is ServerStatus.CREATED

            run_thread = threading.Thread(target=facade.run, daemon=True)
            run_thread.start()

            deadline = time.monotonic() + 5.0
            while _status(server) is not ServerStatus.RUNNING and time.monotonic() < deadline:
                time.sleep(0.01)
            assert _status(server) is ServerStatus.RUNNING

            # The gateway listener accepts a TCP connection. Opening a socket
            # proves the listener is live; the embedded wiring does not
            # require a full wire handshake here.
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5.0)
            sock.connect(("127.0.0.1", gateway_port))
            try:
                assert sock.getpeername()[1] == gateway_port
            finally:
                sock.close()

            base = f"http://127.0.0.1:{control_port}"

            # Login one principal; the handler set allocates one actor at the
            # origin and publishes its initial state.
            login = _post_json(f"{base}/login", {"principal_id": 100})
            actor_id = int(login["actor_id"])
            assert actor_id == 1
            assert int(login["pos_x"]) == 0
            assert int(login["pos_y"]) == 0

            # Move the actor via the control plane. The tile2d model steps the
            # input and the actor's position advances from the origin.
            move = _post_json(f"{base}/move", {"actor_id": actor_id, "move_x": 32767, "move_y": 0})
            moved_x = int(move["pos_x"])
            assert moved_x > 0
            assert int(move["input_tick"]) == 1

            # Query the moved actor: the position persisted in the shared
            # actor table the wire handlers and the control routes drive.
            after = _get_json(f"{base}/query_state?actor_id={actor_id}")
            assert int(after["actor_id"]) == actor_id
            assert int(after["pos_x"]) == moved_x

            # Teleport the actor to an absolute position inside the walkable
            # interior of the 8x8 grid (one tile in from the wall).
            teleported = _post_json(
                f"{base}/teleport",
                {"actor_id": actor_id, "pos_x": 1 << 16, "pos_y": 1 << 16},
            )
            assert int(teleported["pos_x"]) == 1 << 16
            assert int(teleported["pos_y"]) == 1 << 16

            # Query all actors: the list reflects the teleported state.
            all_states = _get_json(f"{base}/query_state")
            actors = all_states["actors"]
            assert isinstance(actors, list)
            assert len(actors) == 1
            assert int(actors[0]["pos_x"]) == 1 << 16

            # A second principal logs in and gets a distinct actor id; the
            # shared actor table holds both.
            login2 = _post_json(f"{base}/login", {"principal_id": 200})
            assert int(login2["actor_id"]) == 2
            all_states = _get_json(f"{base}/query_state")
            assert len(all_states["actors"]) == 2

            facade.shutdown()
            run_thread.join(timeout=5.0)
            assert _status(server) is ServerStatus.STOPPED
        finally:
            server.stop()

    def test_composition_surfaces_exposed_for_in_process_harnesses(self) -> None:
        """The composition root exposes its live surfaces without private access."""
        server = EmbeddedServer()
        facade, _gateway_port, _control_port = server.start()
        try:
            run_thread = threading.Thread(target=facade.run, daemon=True)
            run_thread.start()
            deadline = time.monotonic() + 5.0
            while _status(server) is not ServerStatus.RUNNING and time.monotonic() < deadline:
                time.sleep(0.01)
            assert _status(server) is ServerStatus.RUNNING

            actor = server.handlers.spawn_actor(9001)
            assert actor is not None
            assert actor.id >= 1
            assert server.handlers.cell_size == 1 << 16
            assert isinstance(server.model, SimModel)
            assert server.zone_id >= 0

            facade.shutdown()
            run_thread.join(timeout=5.0)
            assert _status(server) is ServerStatus.STOPPED
        finally:
            server.stop()

    def test_bindings_serve_principal_truth_and_counters(self) -> None:
        server = EmbeddedServer()
        facade, _gateway_port, control_port = server.start()
        try:
            run_thread = threading.Thread(target=facade.run, daemon=True)
            run_thread.start()

            deadline = time.monotonic() + 5.0
            while _status(server) is not ServerStatus.RUNNING and time.monotonic() < deadline:
                time.sleep(0.01)
            assert _status(server) is ServerStatus.RUNNING

            base = f"http://127.0.0.1:{control_port}"
            first = int(_post_json(f"{base}/login", {"principal_id": 300})["actor_id"])
            second = int(_post_json(f"{base}/login", {"principal_id": 101})["actor_id"])

            body = _get_json(f"{base}/bindings")
            # The bindings are allocation truth: one pair per login principal,
            # plus ownership counters a healthy server keeps at zero.
            assert body["bindings"] == [
                {"principal_id": 300, "actor_id": first},
                {"principal_id": 101, "actor_id": second},
            ]
            assert body["bind_conflicts"] == 0
            assert body["identity_gate_drops"] == 0

            paged = _get_json(f"{base}/bindings?offset=1&limit=1")
            assert paged["bindings"] == [{"principal_id": 101, "actor_id": second}]
            assert paged["offset"] == 1
            assert paged["limit"] == 1
            assert paged["total"] == 2
            # The counters ride every page.
            assert paged["bind_conflicts"] == 0

            facade.shutdown()
            run_thread.join(timeout=5.0)
            assert _status(server) is ServerStatus.STOPPED
        finally:
            server.stop()

    def test_tiered_delivery_strategy_boots_and_serves(self) -> None:
        server = EmbeddedServer(delivery_strategy="tiered")
        facade, gateway_port, control_port = server.start()
        try:
            run_thread = threading.Thread(target=facade.run, daemon=True)
            run_thread.start()

            deadline = time.monotonic() + 5.0
            while _status(server) is not ServerStatus.RUNNING and time.monotonic() < deadline:
                time.sleep(0.01)
            assert _status(server) is ServerStatus.RUNNING

            # The gateway listener accepts a TCP connection with the tiered
            # strategy bound: the strategy name and its cadence image flow
            # through the facade params to the C gateway at create time, so
            # construction itself is the assertion.
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5.0)
            sock.connect(("127.0.0.1", gateway_port))
            sock.close()

            # Login drives a session bind against the selected strategy; the
            # handler allocates one actor and the response carries it.
            base = f"http://127.0.0.1:{control_port}"
            login = _post_json(f"{base}/login", {"principal_id": 100})
            assert int(login["actor_id"]) == 1

            facade.shutdown()
            run_thread.join(timeout=5.0)
            assert _status(server) is ServerStatus.STOPPED
        finally:
            server.stop()

    def test_move_unknown_actor_returns_404(self) -> None:
        server = EmbeddedServer()
        facade, _gateway_port, control_port = server.start()
        try:
            run_thread = threading.Thread(target=facade.run, daemon=True)
            run_thread.start()

            deadline = time.monotonic() + 5.0
            while _status(server) is not ServerStatus.RUNNING and time.monotonic() < deadline:
                time.sleep(0.01)

            base = f"http://127.0.0.1:{control_port}"
            with pytest.raises(error.HTTPError) as exc_info:
                _post_json(f"{base}/move", {"actor_id": 999, "move_x": 1})
            assert exc_info.value.code == 404
            # urllib's HTTPError aliases the HTTP response (and its socket);
            # close it so the connection is released deterministically rather
            # than by GC finalization (which emits a ResourceWarning that the
            # suite's filterwarnings=error policy turns into an error).
            exc_info.value.close()

            facade.shutdown()
            run_thread.join(timeout=5.0)
        finally:
            server.stop()

    def test_query_unknown_actor_returns_404(self) -> None:
        server = EmbeddedServer()
        facade, _gateway_port, control_port = server.start()
        try:
            run_thread = threading.Thread(target=facade.run, daemon=True)
            run_thread.start()

            deadline = time.monotonic() + 5.0
            while _status(server) is not ServerStatus.RUNNING and time.monotonic() < deadline:
                time.sleep(0.01)

            base = f"http://127.0.0.1:{control_port}"
            with pytest.raises(error.HTTPError) as exc_info:
                _get_json(f"{base}/query_state?actor_id=999")
            assert exc_info.value.code == 404
            exc_info.value.close()

            facade.shutdown()
            run_thread.join(timeout=5.0)
        finally:
            server.stop()


class TestPythonWorkersResolution:
    """The --python-workers ladder: CLI flag, else KITH_PYTHON_WORKERS,
    else the server default."""

    def test_unset_env_and_flag_defers_to_server_default(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("KITH_PYTHON_WORKERS", raising=False)
        args = _build_parser().parse_args([])
        assert args.python_workers == 0

    def test_env_applies_when_flag_absent(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("KITH_PYTHON_WORKERS", "4")
        args = _build_parser().parse_args([])
        assert args.python_workers == 4

    def test_cli_flag_overrides_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("KITH_PYTHON_WORKERS", "4")
        args = _build_parser().parse_args(["--python-workers", "8"])
        assert args.python_workers == 8

    def test_non_integer_env_is_a_usage_error(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("KITH_PYTHON_WORKERS", "many")
        with pytest.raises(SystemExit):
            _build_parser()

    def test_negative_env_is_a_usage_error(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("KITH_PYTHON_WORKERS", "-2")
        with pytest.raises(SystemExit):
            _build_parser()


class TestTieredMaxGapParsing:
    """The --tiered-max-gap-ms CLI surface: absent by default, int when set."""

    def test_default_is_none(self) -> None:
        args = _build_parser().parse_args([])
        assert args.tiered_max_gap_ms is None

    def test_flag_parses_milliseconds(self) -> None:
        args = _build_parser().parse_args(["--tiered-max-gap-ms", "5000"])
        assert args.tiered_max_gap_ms == 5000


class TestTieredBackstopConfig:
    """The tiered cadence image carries the configured backstop.

    The image is what the gateway copies at create time, so decoding it
    through the generated binding asserts that the override rides the
    tuning a run records.
    """

    @staticmethod
    def _config(server: EmbeddedServer) -> gen_gateway.kith_gateway_tiered_config_t:
        return gen_gateway.kith_gateway_tiered_config_t.from_buffer_copy(server._tiered_config())

    def test_default_image_pins_the_documented_backstop(self) -> None:
        server = EmbeddedServer(delivery_strategy="tiered")
        config = self._config(server)
        documented = gen_gateway.kith_gateway_tiered_default.KITH_GATEWAY_TIERED_DEFAULT_MAX_GAP_MS
        assert config.max_gap_ms == documented

    def test_override_replaces_the_backstop_in_the_image(self) -> None:
        server = EmbeddedServer(delivery_strategy="tiered", tiered_max_gap_ms=5000)
        assert self._config(server).max_gap_ms == 5000

    def test_zero_pins_the_documented_default_explicitly(self) -> None:
        server = EmbeddedServer(delivery_strategy="tiered", tiered_max_gap_ms=0)
        assert self._config(server).max_gap_ms == 0


@needs_build
class TestStandaloneSignalShutdown:
    """The standalone entry point drains on SIGINT/SIGTERM to a clean exit."""

    @staticmethod
    def _spawn_entry_point() -> subprocess.Popen[bytes]:
        env = dict(os.environ)
        env["PYTHONPATH"] = os.pathsep.join(
            [str(_REPO_ROOT / "python"), str(_REPO_ROOT / "examples")]
        )
        env["KITH_LIB"] = str(_BUILD_DEBUG)
        return subprocess.Popen(
            [sys.executable, "-m", "examples.embedded.server"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=str(_REPO_ROOT),
            env=env,
        )

    @pytest.mark.parametrize("sig", [signal.SIGINT, signal.SIGTERM])
    def test_entry_point_exits_cleanly_on_signal(self, sig: signal.Signals) -> None:
        proc = self._spawn_entry_point()
        try:
            stdout = proc.stdout
            assert stdout is not None
            handshake = stdout.readline().decode("utf-8").strip()
            match = re.fullmatch(r"embedded: gateway=(\d+) control=(\d+)", handshake)
            assert match is not None, f"unexpected handshake line: {handshake!r}"
            control_url = f"http://127.0.0.1:{int(match.group(2))}/query_state"

            # Signal only once the run loop is demonstrably serving: the
            # control plane answering proves reactor dispatch is live, and
            # serve() blocks the signal mask before it starts its run
            # thread. A signal before that point takes the default
            # disposition and kills the child mid-startup — correct Unix
            # behavior for a process that has not reached its shutdown
            # coordinator, but not this test's subject.
            deadline = time.monotonic() + 10.0
            while True:
                try:
                    _get_json(control_url)
                    break
                except OSError:
                    if time.monotonic() > deadline:
                        raise
                    time.sleep(0.05)

            os.kill(proc.pid, int(sig))
            exit_code = proc.wait(timeout=20.0)
            stderr_text = b""
            if proc.stderr is not None:
                stderr_text = proc.stderr.read()
            assert exit_code == 0, (
                f"child exited {exit_code} on {sig.name}; stderr:\n"
                f"{stderr_text.decode('utf-8', errors='replace')}"
            )
        finally:
            if proc.poll() is None:
                proc.kill()
                proc.wait()
            # Close the pipe readers deterministically: the suite's
            # filterwarnings=error policy turns a GC-finalized unclosed
            # pipe into a test failure.
            if proc.stdout is not None:
                proc.stdout.close()
            if proc.stderr is not None:
                proc.stderr.close()
