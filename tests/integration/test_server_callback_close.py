"""A callback must not close its own server or block an owner draining it."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

from _helpers import _BUILD_DEBUG, needs_build


_CHILD = '''\
import sys
import threading

from kith import Server
from kith.exceptions import KithStateError

expected_ft = bool(int(sys.argv[1]))
assert sys._is_gil_enabled() is not expected_ft
server = Server(topology="embedded", listen_port=0, python_workers=1)
entered = threading.Event()
release = threading.Event()
rejected = threading.Event()

def tick(index):
    entered.set()
    assert release.wait(5), "owner never released callback"
    try:
        server.close()
    except KithStateError as exc:
        assert "callback" in str(exc)
        print("callback-close-rejected", flush=True)
        rejected.set()
    else:
        raise AssertionError("callback close unexpectedly succeeded")

server.register_tick_handler(tick)
runner = threading.Thread(target=server.run, daemon=True)
runner.start()
assert entered.wait(5), "no callback ran"
print("callback-entered", flush=True)
closer = threading.Thread(target=server.close, daemon=True)
closer.start()
runner.join(5)
assert not runner.is_alive(), "run loop did not stop"
release.set()
closer.join(5)
assert not closer.is_alive(), "owner and callback deadlocked"
assert rejected.is_set(), "close was not rejected"
assert sys._is_gil_enabled() is not expected_ft
print("owner-close-completed", flush=True)
'''


@needs_build
def test_callback_close_is_rejected_while_owner_drains() -> None:
    env = os.environ.copy()
    env["KITH_BUILD_DIR"] = str(_BUILD_DEBUG)
    env["KITH_LIB"] = str(_BUILD_DEBUG)
    package_root = str(Path(__file__).resolve().parents[2] / "python")
    existing = env.get("PYTHONPATH")
    env["PYTHONPATH"] = f"{package_root}{os.pathsep}{existing}" if existing else package_root
    result = subprocess.run(
        [sys.executable, "-c", _CHILD, str(int(not sys._is_gil_enabled()))],
        env=env,
        capture_output=True,
        text=True,
        timeout=20,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    for marker in ("callback-entered", "callback-close-rejected", "owner-close-completed"):
        assert marker in result.stdout, result.stdout + result.stderr
