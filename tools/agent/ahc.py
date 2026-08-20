"""Re-export shim for the agentic headless client engine.

The canonical engine ships with the framework package at
``kith/_agent/ahc.py`` so the packaged visual example can import it; this
module keeps the harness tooling's import path and ``python -m
tools.agent.ahc`` invocation working unchanged.
"""

from __future__ import annotations

import asyncio

from kith._agent.ahc import (
    AgenticHeadlessClient as AgenticHeadlessClient,
)
from kith._agent.ahc import (
    BootstrapStep as BootstrapStep,
)
from kith._agent.ahc import (
    ClientEvent as ClientEvent,
)
from kith._agent.ahc import (
    ClientStatus as ClientStatus,
)
from kith._agent.ahc import (
    DecodedFrame as DecodedFrame,
)
from kith._agent.ahc import (
    KithClientError as KithClientError,
)
from kith._agent.ahc import (
    WindowEvidence as WindowEvidence,
)
from kith._agent.ahc import (
    main as main,
)


__all__ = [
    "AgenticHeadlessClient",
    "BootstrapStep",
    "ClientEvent",
    "ClientStatus",
    "DecodedFrame",
    "KithClientError",
    "WindowEvidence",
    "main",
]

if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
