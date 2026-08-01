"""The kith Python distribution: ctypes bindings to the kith C libraries.

kith._generated mirrors the public C headers under include/kith; the bridge
module loads the shared libraries and the generated signatures attach to
them. The exception families below are the package's public error surface:
every C error code crosses the boundary as one of these.
"""

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


__all__ = [
    "ABIVersionError",
    "BridgeError",
    "BridgeLoadError",
    "KithConfigError",
    "KithError",
    "KithNetworkError",
    "KithNotFoundError",
    "KithProtocolError",
    "KithResponseOverflowError",
    "KithStateError",
]
