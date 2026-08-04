"""The kith Python distribution: ctypes bindings to the kith C libraries.

kith._generated mirrors the public C headers under include/kith; the bridge
module loads the shared libraries and the generated signatures attach to
them. The exception families below are the package's public error surface:
every C error code crosses the boundary as one of these. The typed
configuration source wrapper and the protocol wrapper are re-exported
alongside them.
"""

from kith.config import Config
from kith.exceptions import (
    ABIVersionError,
    BridgeError,
    BridgeLoadError,
    KithConfigError,
    KithError,
    KithNetworkError,
    KithNotFoundError,
    KithProtocolError,
    KithResponseOverflowError,
    KithStateError,
)
from kith.proto import Proto


__all__ = [
    "ABIVersionError",
    "BridgeError",
    "BridgeLoadError",
    "Config",
    "KithConfigError",
    "KithError",
    "KithNetworkError",
    "KithNotFoundError",
    "KithProtocolError",
    "KithResponseOverflowError",
    "KithStateError",
    "Proto",
]
