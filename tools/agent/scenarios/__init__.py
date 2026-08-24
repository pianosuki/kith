"""Built-in agentic scenarios for the spatial reference game.

Importing this package registers every built-in scenario subclass with
:func:`tools.agent.scenario.all_scenarios` via
``Scenario.__init_subclass__``. The runner imports this package at module
load so ``--list`` and ``--filter`` discover the built-ins; the execution
core and selection helpers remain registry-agnostic and accept any
registered scenario.

The scenarios exercise the spatial reference game's wire catalog and
control-plane surface end-to-end: they register the game's wire types on
each client, drive the server through ``actor_input`` and ``chat`` frames
and the control-plane ``/teleport`` route, and observe the replication
stream (``actor_state`` events) the gateway delivers. The generic
scenario DSL (``Scenario`` / ``ScenarioHost`` / ``ScenarioContext``) is
type-id-agnostic; the scenario implementations attach game meaning
through the spatial wire-type constants and payload codecs.
"""

from __future__ import annotations

from tools.agent.scenarios.aoi_boundary import AoiBoundary
from tools.agent.scenarios.basic_connect import BasicConnect
from tools.agent.scenarios.movement_quality import MovementQuality
from tools.agent.scenarios.presence_window import PresenceWindow
from tools.agent.scenarios.proximity_chat import ProximityChat


__all__ = [
    "AoiBoundary",
    "BasicConnect",
    "MovementQuality",
    "PresenceWindow",
    "ProximityChat",
]
