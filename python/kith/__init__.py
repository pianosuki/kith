"""The kith Python distribution: ctypes bindings to the kith C libraries.

kith._generated mirrors the public C headers under include/kith; the bridge
module loads the shared libraries and the generated signatures attach to
them. The exception families below are the package's public error surface:
every C error code crosses the boundary as one of these. The typed
configuration source wrapper, the protocol wrapper, the reactor
wrapper, the worker pool wrapper, and the persistence wrapper are
re-exported alongside them.
"""

from kith.config import Config
from kith.db import Database, Reply, Transaction
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
from kith.reactor import Reactor
from kith.worker import Worker


__all__ = [
    "ABIVersionError",
    "BridgeError",
    "BridgeLoadError",
    "Config",
    "Database",
    "KithConfigError",
    "KithError",
    "KithNetworkError",
    "KithNotFoundError",
    "KithProtocolError",
    "KithResponseOverflowError",
    "KithStateError",
    "Proto",
    "Reactor",
    "Reply",
    "Transaction",
    "Worker",
]
