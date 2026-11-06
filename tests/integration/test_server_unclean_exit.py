"""Integration test: a process that dies with a live server exits cleanly.

Boots the facade in a child process on a daemon thread with a python-bound
tick handler and a python-bound control route both dispatching, then raises
uncaught out of ``main`` — the interpreter exits with the run loop alive and
no ``shutdown``/``close`` call. The C-side interpreter exit gate (armed by
the bridge's exit hook) refuses the python-bound callback entries the
worker pool attempts during interpreter finalization; an entry into the
finalizing interpreter has no graceful path there and kills the process.

The assertion is deterministic: the child exits with its uncaught
``RuntimeError`` (status 1), never a signal. The C-level pins in
``tests/c/test_python_exit_gate.c`` catch the gate logic directly; this
test is the end-to-end proof. The child runs under whatever interpreter
the suite leg provides, so both the standard and the free-threaded legs
exercise the gate.
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
from pathlib import Path

import pytest
from _helpers import _BUILD_DEBUG, needs_build


_REPO_ROOT = Path(__file__).resolve().parents[2]

_CHILD_SOURCE = '''\
"""Child process: boot a live server, then crash out of main uncaught."""

import json
import sys
import threading
import time
from urllib import request

from kith import Server, SimModelConfig

TICKS = [0]


def main() -> int:
    server = Server(topology="embedded", listen_port=0, tick_hz=200)
    server.register_proto_type("state", 1001)
    server.register_zone("m")
    server.register_sim_model("tile2d", SimModelConfig())

    def on_tick(tick: int) -> None:
        TICKS[0] = tick

    server.register_tick_handler(on_tick)

    def route(req: object, resp: object) -> None:
        resp.status(200, "application/json")
        resp.body(json.dumps({"tick": TICKS[0]}).encode())

    server.register_control_route("GET", "/tick", route)

    run_thread = threading.Thread(target=server.run, daemon=True)
    run_thread.start()
    control = server.control_port

    def hit_route() -> None:
        while True:
            try:
                with request.urlopen(f"http://127.0.0.1:{control}/tick", timeout=5.0) as resp:
                    resp.read()
            except Exception:
                pass
            time.sleep(0.01)

    hitter = threading.Thread(target=hit_route, daemon=True)
    hitter.start()
    time.sleep(2.0)
    print(
        f"child: ticks={TICKS[0]}; gil_enabled={sys._is_gil_enabled()}; crashing uncaught",
        flush=True,
    )
    raise RuntimeError("driver crash: unclean exit with a live server")


if __name__ == "__main__":
    sys.exit(main())
'''


def _assert_live_callback_exit(proc: subprocess.CompletedProcess[str]) -> None:
    """Require callback execution before the intended uncaught exception."""
    assert proc.returncode == 1, (
        f"child exited {proc.returncode} (negative = signal death); "
        f"stdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
    )
    assert "Fatal Python error" not in proc.stderr
    marker = re.search(
        r"child: ticks=(\d+); gil_enabled=(True|False); crashing uncaught", proc.stdout
    )
    assert marker is not None, f"child never reported live callbacks: {proc.stdout}\n{proc.stderr}"
    assert int(marker.group(1)) > 0, "child exited without a native-dispatched Python tick"
    assert (marker.group(2) == "True") == sys._is_gil_enabled(), "child GIL mode differs from suite"
    assert "RuntimeError: driver crash: unclean exit with a live server" in proc.stderr


def test_setup_failure_does_not_pass_unclean_exit_check() -> None:
    child = subprocess.CompletedProcess(
        args=[sys.executable],
        returncode=1,
        stdout="",
        stderr="ModuleNotFoundError: missing dependency",
    )
    with pytest.raises(AssertionError, match="never reported live callbacks"):
        _assert_live_callback_exit(child)


@needs_build
class TestServerUncleanExit:
    def test_process_dying_with_live_server_exits_cleanly(self, tmp_path: Path) -> None:
        child = tmp_path / "unclean_child.py"
        child.write_text(_CHILD_SOURCE)
        env = {**os.environ}
        env["KITH_BUILD_DIR"] = str(_BUILD_DEBUG)
        package_root = str(_REPO_ROOT / "python")
        existing = env.get("PYTHONPATH")
        env["PYTHONPATH"] = f"{package_root}{os.pathsep}{existing}" if existing else package_root
        proc = subprocess.run(
            [sys.executable, str(child)],
            cwd=str(_REPO_ROOT),
            env=env,
            capture_output=True,
            text=True,
            timeout=60.0,
        )
        _assert_live_callback_exit(proc)
