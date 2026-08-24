"""Integration test: agentic scenarios against the embedded server.

Boots a fresh :class:`examples.embedded.server.EmbeddedServer` per
scenario via
:func:`tools.agent.embedded_host.embedded_host_factory` and runs each
built-in scenario (basic_connect, proximity_chat, aoi_boundary,
presence_window, movement_quality) green against it. This is the
closed-loop exercise of the full stack: the game-aware headless client
logs in over the wire, the scenario drives the server through
``actor_input`` / ``chat`` frames and the control-plane ``/teleport``
route, and the assertions verify the gateway's replication stream
delivers ``actor_state`` frames with the expected visibility and
movement quality.
"""

from __future__ import annotations

import asyncio
from collections.abc import Iterator
from pathlib import Path

import pytest
from _helpers import needs_build
from tools.agent import scenario as scenario_module
from tools.agent.ahc import ClientStatus
from tools.agent.assertions import ScenarioAssertionError
from tools.agent.embedded_host import embedded_host_factory
from tools.agent.runner import run_scenarios
from tools.agent.scenario import (
    Scenario,
    ScenarioContext,
    ScenarioResult,
    StepDiagnosis,
    StepStatus,
    step,
)
from tools.agent.scenarios import (
    AoiBoundary,
    BasicConnect,
    MovementQuality,
    PresenceWindow,
    ProximityChat,
)


_REPO_ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(autouse=True)
def _restore_scenario_registry() -> Iterator[None]:
    """Snapshot/restore the global scenario registry around each test."""
    snapshot = dict(scenario_module.all_scenarios())
    yield
    scenario_module._REGISTRY.clear()
    scenario_module._REGISTRY.update(snapshot)


@needs_build
class TestScenariosEmbedded:
    def test_all_scenarios_green(self) -> None:
        scenarios = [
            BasicConnect,
            ProximityChat,
            AoiBoundary,
            PresenceWindow,
            MovementQuality,
        ]
        factory = embedded_host_factory()
        results = asyncio.run(run_scenarios(scenarios, factory))
        for result in results:
            assert result.passed, _format_failure(result)


@needs_build
class TestScenarioDiagnosisEmbedded:
    def test_failed_step_attaches_runner_diagnosis(self) -> None:
        class _DiagnosisProbe(Scenario):
            scenario_name = "diagnosis_probe"

            def build(self, ctx: ScenarioContext) -> None:
                del ctx
                self.client("alpha").connect()

            @step
            async def fail_deliberately(self, ctx: ScenarioContext) -> None:
                del ctx
                raise ScenarioAssertionError("probe failure", details={"probe": True})

        factory = embedded_host_factory()
        (result,) = asyncio.run(run_scenarios([_DiagnosisProbe], factory))

        assert not result.passed
        failed = [s for s in result.steps if s.status is StepStatus.FAILED]
        assert len(failed) == 1
        diagnosis = failed[0].diagnosis
        assert isinstance(diagnosis, StepDiagnosis)
        assert set(diagnosis.statuses) == {"alpha"}
        assert isinstance(diagnosis.statuses["alpha"], ClientStatus)
        assert "alpha" in diagnosis.event_tails
        # No inbound frame in this topology echoes a submit correlation id
        # (chat carries no reply; replication frames carry no trailer), so
        # the built-in suite's chain set stays empty; a request/reply game
        # populates it through the same gather.
        assert diagnosis.correlation_chains == {}


def _format_failure(result: ScenarioResult) -> str:
    steps = "\n".join(
        f"  {s.name}: {s.status}" + (f"\n    error: {s.error}" if s.error else "")
        for s in result.steps
    )
    return f"scenario {result.name!r} failed ({result.duration_s:.1f}s):\n{steps}"
