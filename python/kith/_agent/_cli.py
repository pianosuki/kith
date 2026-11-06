"""Command-line configuration for one headless client."""

from __future__ import annotations

import argparse
import asyncio
import os
from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from .ahc import AgenticHeadlessClient
_OUTBOUND_INITIAL_BUFFER_SIZE = 4096
_UINT32_MAX = (1 << 32) - 1
_INTERRUPT_ERRORS = (KeyboardInterrupt, asyncio.CancelledError)


async def run_client(client_factory: type[AgenticHeadlessClient]) -> int:
    """CLI entry: run a single AHC until interrupted.

    Connects to ``KITH_AHC_HOST``:``KITH_AHC_PORT`` (default 127.0.0.1:7777),
    exposes IPC at ``KITH_AHC_IPC`` and HTTP at ``KITH_AHC_HTTP_PORT``.
    """
    parser = argparse.ArgumentParser(description="kith agentic headless client")
    parser.add_argument("--id", default="ahc-0", help="instance id")
    parser.add_argument("--host", default=os.environ.get("KITH_AHC_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("KITH_AHC_PORT", "7777")))
    parser.add_argument("--ipc", default=os.environ.get("KITH_AHC_IPC"))
    parser.add_argument(
        "--http-port", type=int, default=int(os.environ.get("KITH_AHC_HTTP_PORT", "0"))
    )
    parser.add_argument(
        "--outbound-buffer-initial-bytes",
        type=int,
        default=_OUTBOUND_INITIAL_BUFFER_SIZE,
        help="initial outbound scratch bytes (default: %(default)s); grows for larger frames",
    )
    args = parser.parse_args()
    if not 0 < args.outbound_buffer_initial_bytes <= _UINT32_MAX:
        parser.error("--outbound-buffer-initial-bytes must be a positive uint32_t")

    ahc = client_factory(
        instance_id=args.id,
        host=args.host,
        port=args.port,
        outbound_buffer_initial_bytes=args.outbound_buffer_initial_bytes,
        ipc_socket_path=args.ipc,
        http_port=args.http_port,
    )
    await ahc.start()
    print(f"ahc {args.id}: ipc={ahc.ipc_socket_path} http=127.0.0.1:{ahc.http_port}", flush=True)
    try:
        while ahc._running:
            await asyncio.sleep(3600)
    except _INTERRUPT_ERRORS:
        pass
    await ahc.stop()
    return 0
