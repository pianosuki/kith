"""Minimal kith server.

Boots a Server and runs it to completion. No wire types are
registered, no handler is wired, no simulation model is instantiated, no
zone is reserved, no control route is exposed, and no persistence pool is
configured. The composition root wires every plane with its default
tuning; the gateway listener comes up on an OS-assigned ephemeral port
and accepts connections, but a peer that connects sees no decoded frames
(the handler table is empty and the replication type id is 0).

This is the thinnest end-to-end slice of the framework: it proves the
facade loads the shared libraries, builds the size-versioned creation
struct, drives the C run loop, and tears down cleanly with no
game-owned wiring. It is strictly thinner than ``free_movement/``, which
adds one model, one handler, and four control routes on top.
"""

from __future__ import annotations

from kith import Server


__all__ = ["Server", "main"]


def main() -> None:
    """Boot a server with all defaults and block until interrupted.

    The server is constructed with ``listen_port=0`` so the OS assigns an
    ephemeral gateway port, then enters the run loop on a worker thread.
    ``SIGINT`` or ``SIGTERM`` requests a graceful shutdown; the loop drains,
    serve returns, and the context manager releases the
    handle.
    """
    with Server(listen_port=0, topology="embedded") as server:
        print(f"minimal: gateway={server.listen_port}", flush=True)
        server.serve()


if __name__ == "__main__":
    main()
