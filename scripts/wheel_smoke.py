"""Release-wheel smoke: load the bridge, then boot a server to RUNNING.

Runs against an installed wheel in a clean interpreter (never the source
tree): the bridge must discover the bundled per-module libraries through
its wheel layout search, the composition root must wire every plane far
enough to serve, and the C handle must tear down cleanly. A missing
bundled library, SONAME drift, or a broken $ORIGIN runpath surfaces here
as a failed boot instead of a shipped wheel.
"""

from __future__ import annotations

import os
import socket
import threading
import time

from kith import Server, ServerStatus
from kith._bridge import load


_RUNNING_TIMEOUT_S = 15.0
_JOIN_TIMEOUT_S = 5.0


def _status(server: Server) -> ServerStatus:
    # A fresh read per call: mypy narrows a member expression after the
    # first identity assert, and a narrowed CREATED read makes the RUNNING
    # poll look non-overlapping.
    return server.status


def main() -> None:
    """Smoke the installed wheel: bridge load, server boot, clean teardown."""
    # The wheel must self-locate its bundled libraries; an inherited
    # KITH_LIB redirects the load at a build tree and the smoke passes
    # without proving the bundling contract.
    os.environ.pop("KITH_LIB", None)

    bridge = load()
    print(bridge.mode, bridge.version_string, flush=True)
    assert bridge.mode == "individual", bridge.mode
    assert bridge.version_string, "empty version string"

    server = Server(listen_port=0, topology="embedded")
    port = server.listen_port
    try:
        assert _status(server) is ServerStatus.CREATED
        assert port != 0

        run_thread = threading.Thread(target=server.run, daemon=True)
        run_thread.start()

        deadline = time.monotonic() + _RUNNING_TIMEOUT_S
        while _status(server) is not ServerStatus.RUNNING and time.monotonic() < deadline:
            time.sleep(0.01)
        assert _status(server) is ServerStatus.RUNNING, (
            f"server never reached RUNNING (status={_status(server)})"
        )

        # Connecting proves the listener is bound, not merely that the
        # status enumeration flipped.
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(_RUNNING_TIMEOUT_S)
        sock.connect(("127.0.0.1", port))
        try:
            assert sock.getpeername()[1] == port
        finally:
            sock.close()
        print(f"wheel smoke: server RUNNING, gateway on {port}", flush=True)

        server.shutdown()
        run_thread.join(timeout=_JOIN_TIMEOUT_S)
        assert _status(server) is ServerStatus.STOPPED, (
            f"server did not stop cleanly (status={_status(server)})"
        )
    finally:
        server.close()
    print("wheel smoke: boot and shutdown clean", flush=True)


if __name__ == "__main__":
    main()
