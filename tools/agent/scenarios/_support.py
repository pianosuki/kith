"""Shared helpers for the built-in scenarios.

The scenarios in this package assert against hard-coded actor ids that
hold only when the server allocated them the expected way. The guard here
pins those ids against the server's own allocation truth instead of
trusting the fresh-server convention, so a warm server fails loudly at
the guard step instead of mismatching actors silently in the steps that
use them.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Mapping
from typing import cast

from tools.agent.assertions import ScenarioAssertionError
from tools.agent.scenario import ScenarioContext


__all__ = ["assert_bound_actor"]

_BINDINGS_PATH: str = "/bindings"
_POLL_INTERVAL_S: float = 0.05


async def assert_bound_actor(
    ctx: ScenarioContext,
    client: str,
    actor_id: int,
    *,
    timeout_s: float = 10.0,
) -> None:
    """Assert the client's principal owns ``actor_id`` in the server's bindings.

    Reads the control plane's binding map — the pairs the login path
    created under the handler lock — and matches it against the client's
    retained principal identity, so the scenario's hard-coded actor ids
    are checked against the server's allocation truth rather than assumed.
    The host must expose a server control plane serving the bindings route;
    the id is verified here instead of at use time so a mismatch surfaces
    as a guard failure with the observed bindings attached.

    Args:
        ctx: The scenario context to read the host through.
        client: The registered client name whose principal is looked up.
        actor_id: The actor id the scenario's remaining steps operate on.
        timeout_s: How long to poll the binding map before failing.
    """
    principal = ctx.client_principal(client)
    if principal is None:
        raise ScenarioAssertionError(
            f"client {client!r} carries no principal identity to bind-check",
            details={"client": client, "expected_actor_id": actor_id},
        )
    deadline = time.monotonic() + timeout_s
    observed: dict[int, int] = {}
    while time.monotonic() < deadline:
        body = await ctx.server_state(_BINDINGS_PATH)
        records = cast("list[object]", body.get("bindings", []))
        observed = {
            int(record["principal_id"]): int(record["actor_id"])
            for record in records
            if isinstance(record, Mapping)
        }
        if observed.get(principal) == actor_id:
            return
        await asyncio.sleep(_POLL_INTERVAL_S)
    raise ScenarioAssertionError(
        f"client {client!r} (principal {principal}) never bound actor {actor_id}",
        details={
            "client": client,
            "principal_id": principal,
            "expected_actor_id": actor_id,
            "observed_bindings": observed,
        },
    )
