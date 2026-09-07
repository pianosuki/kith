# ADR-0020: Scaling claims are certified by two gate classes

**Status:** Accepted

## Context

The framework's public scaling claim — that the five-plane architecture
breaks the N² ceiling — must be backed by reproducible numeric gates, not
anecdotes. The original gate definitions demanded full delivery fidelity
from every topology, including the single-instance embedded pileup — the
adversarial case whose egress and reactor demand, measured with the
harness, the clients, and the metrics computation co-resident in one
process, quantify the N² byte cost the embedded topology exists to absorb
in development and testing. That is not the distributed claim the
architecture makes, and every failing run cost twenty minutes of fighting
physics the claim never rested on.

A second gap surfaced once the fidelity numbers proved interpreter- and
configuration-sensitive: which interpreter the gate certifies under, and on
which movement-apply path, was undecided. Under a standard GIL build a wide
worker pool collapses to one active worker, so the configured parallelism
is fictional and the fidelity bar is structurally unreachable — the gate
would measure the interpreter's ceiling, not the framework's delivery
contract. A gate reading is only as honest as the configuration and the
instrument it was measured under.

## Decision

The scaling claim is certified through two gate classes, and the project
always maintains a written gate for each class:

1. **Stress gates.** Liveness and integrity under the adversarial pileup:
   session integrity, view-selection coverage, bootstrap latency, no
   crash, no corruption. Delivery-fidelity metrics are reported for
   diagnosis, not thresholded — the stress topology is the single-instance
   adversarial case, not the fidelity claim.
2. **Fidelity gates.** The full per-instance delivery-fidelity thresholds —
   session integrity, view-selection coverage, certified movement
   coverage, bootstrap latency, no continuity flicker — plus a bounded
   publish-rate skew across instances. This is the class the N² ceiling
   claim is certified on.

The gate numbers, metric definitions, and harness profiles are living
policy, not record constants: they are versioned in `AGENTS.md` §4.4 and
the scaling guide, which are the single source of truth. A threshold never
weakens silently — a change is an explicit, review-enforced decision
recorded in the ADR set.

The certified configuration is part of the claim. The fidelity gate
certifies under free-threaded Python (ADR-0004's N-worker rule) with the
native movement-apply path (ADR-0024) enabled — the configuration under
which the certified movement-coverage bar is reachable — and every report
stamps the build type and machine state its numbers were produced under
(build type, kernel, CPU model, cpufreq governor, interpreter GIL status).
A run in any other configuration is a regression baseline or a
configuration-drift diagnostic, never a certified reading. The instrument
is part of the measurement contract: it must not be a CPU co-tenant of the
system under test (the certified harness shape runs the client cohorts
out-of-process, one per instance cohort).

These gates certify the framework's own claim. Adopters define their own
acceptance gates; the certified profiles are the project's published
policy, not a constraint on users.

The gate classes conform to the founding set: the five-plane architecture
(ADR-0001), the bounded relevance budgets (ADR-0005), and the
deterministic simulation contract (ADR-0014) are unchanged; no plane,
topology, or ABI surface changes. The numeric definitions live in
`AGENTS.md` §4.4; running and interpreting the gates is documented in
`docs/guides/scaling_checklist.md`; the embedded-vs-distributed split in
`docs/architecture/topologies.md`.

## Consequences

Positive — the scaling claim measures the configuration it certifies; each
gate class measures what its topology is for; gate numbers evolve with
hardware and adopter needs without freezing a founding record; the
interpreter and apply-path conditions of every certified number are
explicit rather than ambient. Negative — the no-silent-weakening
discipline is review-enforced, not tool-enforced: a careless threshold
edit in `AGENTS.md` §4.4 has no mechanical guard, only the
explicit-history requirement and review. The stress gate does not
certify delivery fidelity, so a fidelity regression on the embedded
topology lands green — the metrics are still reported, and the baseline
comparator remains the run-to-run guard for reported metrics.
