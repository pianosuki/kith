# ADR-0020: Scaling claims are certified by two gate classes

**Status:** Accepted

## Context

The public claim that the five-plane architecture breaks the N² ceiling requires
reproducible numeric gates. The original gate definitions demanded full delivery
fidelity from every topology, including the single-instance embedded pileup. Its
egress and reactor demand, measured with the harness, the clients, and the
metrics computation co-resident in one process, quantify the N² byte cost the
embedded topology exists to absorb in development and testing. The
architecture's distributed claim does not depend on that case, yet each failing
run spent twenty minutes measuring it.

A second gap surfaced once the fidelity numbers proved interpreter- and
configuration-sensitive: which interpreter the gate certifies under, and on
which movement-apply path, was undecided. Under a standard GIL build a wide
worker pool collapses to one active worker, so the configured parallelism is
unavailable and the fidelity bar is structurally unreachable. The gate would
measure the interpreter's ceiling rather than the framework's delivery contract.
A valid gate reading requires a stated configuration and measurement instrument.

## Decision

The scaling claim is certified through two gate classes, and the project always
maintains a written gate for each class:

1. **Stress gates.** Liveness and integrity under the adversarial pileup:
   session integrity, view-selection coverage, bootstrap latency, no crash, no
   corruption. Delivery-fidelity metrics are reported for diagnosis, not
   thresholded: the stress topology is the single-instance adversarial case, not
   the fidelity claim.
1. **Fidelity gates.** The full per-instance delivery-fidelity thresholds
   (session integrity, view-selection coverage, certified movement coverage,
   bootstrap latency, no continuity flicker) plus a bounded publish-rate skew
   across instances. This is the class the N² ceiling claim is certified on.

The gate numbers, metric definitions, and harness profiles are living policy,
not record constants: the repository's scaling-gate policy owns the numeric
definitions, metric definitions, and harness profiles, with the scaling guide
owning their operational interpretation. A threshold never weakens silently: a
change is an explicit, review-enforced decision recorded in the ADR set.

The certified configuration is part of the claim. The fidelity gate certifies
under free-threaded Python (ADR-0004's N-worker rule) with the native
movement-apply path (ADR-0024) enabled, the configuration in which the certified
movement-coverage bar is reachable. Every report records the build type and
machine state used (build type, kernel, CPU model, cpufreq governor, interpreter
GIL status). A run in any other configuration is a regression baseline or a
configuration-drift diagnostic, never a certified reading. The instrument is
part of the measurement contract: it must not be a CPU co-tenant of the system
under test (the certified harness shape runs the client cohorts out-of-process,
one per instance cohort).

These gates certify the framework's own claim. Adopters define their own
acceptance gates; the certified profiles are the project's published policy, not
a constraint on users.

The gate classes conform to the founding set: the five-plane architecture
(ADR-0001), the bounded relevance budgets (ADR-0005), and the deterministic
simulation contract (ADR-0014) are unchanged; no plane, topology, or ABI surface
changes. The scaling-gate policy owns the numeric definitions; the scaling guide
owns execution and interpretation, and the topology documentation owns the
embedded-versus-distributed split.

## Consequences

Positive: the scaling claim measures the configuration it certifies; each gate
class measures what its topology is for; gate numbers evolve with hardware and
adopter needs without freezing a founding record; the interpreter and apply-path
conditions of every certified number are explicit rather than ambient. Negative:
the no-silent-weakening discipline is review-enforced, not tool-enforced: a
careless threshold edit to the scaling-gate policy has no mechanical guard, only
the explicit-history requirement and review. The stress gate does not certify
delivery fidelity, so a fidelity regression on the embedded topology passes the
gate. The metrics are still reported, and the baseline comparator remains the
run-to-run guard for reported metrics.
