"""Re-export shim for the server control client.

The canonical module ships with the framework package at
``kith/_agent/server_control.py`` so the packaged visual example can import
it; this module keeps the harness tooling's import path working unchanged.
"""

from __future__ import annotations

from kith._agent.server_control import (
    ServerControlClient as ServerControlClient,
)
from kith._agent.server_control import (
    ServerControlError as ServerControlError,
)


__all__ = [
    "ServerControlClient",
    "ServerControlError",
]
