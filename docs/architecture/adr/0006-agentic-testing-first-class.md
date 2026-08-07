# ADR-0006: Agentic testing is first-class under pragmatic AI guardrails

**Status:** Accepted

## Context

Agentic, AI-assisted workflows are an inevitable part of software's future.
This framework takes a deliberately balanced stance: it neither bans AI
assistance where it is genuinely helpful, nor lets unconstrained code
generation erode the consistency, planning, and quality that keep the codebase
reliable.
The safest and most valuable use of an agent is verification — exercising code
far more broadly and quickly than a human can reasonably do, to catch defects
before they ship. An earlier monolithic, actor-centered MMO server demonstrated
this with a harness that drove a headless client in closed-loop tests against a
living server; that harness is the seed of the framework's agentic QA.

## Decision

Agentic testing is a first-class framework capability, exercised
inside explicit guardrails. The control plane, a headless client core,
correlation IDs, and a headless-agent harness are framework subsystems built
from the outset, not testing afterthoughts. An agent runs a full closed-loop
scenario end-to-end against a living server, verifying server behavior
programmatically and deterministically. Guardrails keep agents beneficial and
bounded: deterministic, reviewable scenarios; correlation IDs tying every
observation to server state; and the framework's normal quality gates — the
deterministic simulation contract (ADR-0014), hardening (ADR-0017), the error
contract (ADR-0018), and the ABI gate (ADR-0008) — applied to agent-generated
output exactly as to human output.
Agents assist; humans own architecture, consistency, and planning. This
project operates that way itself: its development and verification run
through agentic workflows under these guardrails.

## Consequences

Positive — dramatically broader and faster verification than manual testing,
earlier defect detection, and a testing harness whose cost grows little with
effort — a step change for QA in the game domain. Negative — agent output is
only as good as its guardrails; mitigated by deterministic scenarios,
correlation tracing, and the unchanged human-owned quality gates. The stance
deliberately keeps the traditional engineering discipline that makes the
codebase consistent, planned, and high-quality.
