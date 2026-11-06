# ADR-0006: Agentic testing is first-class under pragmatic AI guardrails

**Status:** Accepted

## Context

Agentic, AI-assisted workflows are an inevitable part of software development.
This framework permits useful AI assistance under constraints that protect
consistency, planning, and code quality. Verification is the safest and most
valuable use of an agent: it can exercise code more broadly and quickly than a
human can reasonably do, catching defects before they ship. An earlier
monolithic, actor-centered MMO server demonstrated this with a harness that
drove a headless client in closed-loop tests against a living server; that
harness is the seed of the framework's agentic QA.

## Decision

Agentic testing is a first-class framework capability, exercised inside explicit
guardrails. The control plane, a headless client core, correlation IDs, and a
headless-agent harness are framework subsystems built from the outset, not
testing afterthoughts. An agent runs a full closed-loop scenario end-to-end
against a living server, verifying server behavior programmatically and
deterministically. Guardrails keep agents beneficial and bounded: deterministic,
reviewable scenarios; correlation IDs tying every observation to server state;
and the framework's normal quality gates (the deterministic simulation contract
(ADR-0014), hardening (ADR-0017), the error contract (ADR-0018), and the ABI
gate (ADR-0008)) applied to agent-generated output exactly as to human output.
Agents assist; humans own architecture, consistency, and planning. This project
operates that way itself: its development and verification run through agentic
workflows under these guardrails.

**Amendment (documentation verification):** the quality gate includes canonical
document formatting. Pinned development-only formatting and parsing tools keep
prose and structural markup consistent while validating unchanged rendered
meaning. Verification reports drift without rewriting files; explicit formatting
applies corrections. These tools add no runtime package dependency. Mechanical
enforcement complements the existing review rules so presentation does not
silently diverge between contributors.

**Amendment (test evidence and cost):** test quantity and execution coverage are
diagnostics, not proofs of correctness. Tests assert distinct observable failure
modes or independent oracles, with critical ownership, cancellation, error and
concurrency contracts exercised directly. Repeated immutable preparation may be
consolidated while preserving assertions and isolation. Instrumentation must
account for child processes and native callback execution, and its side effects
remain separate from uninstrumented qualification.

## Consequences

Positive: much broader and faster verification than manual testing, earlier
defect detection, and a testing harness whose cost grows little with effort,
substantially improving game QA. Negative: agent output is only as good as its
guardrails; mitigated by deterministic scenarios, correlation tracing, and the
unchanged human-owned quality gates. The guardrails retain the engineering
discipline that keeps the codebase consistent, planned, and high quality.
