"""The kith Python distribution: ctypes bindings to the kith C libraries.

kith._generated mirrors the public C headers under include/kith; the bridge
module loads the shared libraries and the generated signatures attach to
them. The exception families below are the package's public error surface:
every C error code crosses the boundary as one of these. The typed
configuration source wrapper, the protocol wrapper, the reactor
wrapper, the worker pool wrapper, the persistence wrapper, the
area-of-interest wrapper, the simulation wrapper, and the fabric
wrapper are re-exported alongside them.
"""

from kith.aoi import Aoi
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
from kith.fabric import CellKey, Fabric, Subscription
from kith.proto import Proto
from kith.reactor import Reactor
from kith.sim import (
    Actor,
    ArtifactKey,
    Sim,
    SimInput,
    SimModel,
    SimModelConfig,
)
from kith.worker import Worker


__all__ = [
    "ABIVersionError",
    "Actor",
    "Aoi",
    "ArtifactKey",
    "BridgeError",
    "BridgeLoadError",
    "CellKey",
    "Config",
    "Database",
    "Fabric",
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
    "Sim",
    "SimInput",
    "SimModel",
    "SimModelConfig",
    "Subscription",
    "Transaction",
    "Worker",
]
