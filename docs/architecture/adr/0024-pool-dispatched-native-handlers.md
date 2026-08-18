# ADR-0024: Pool-dispatched native C message handlers

**Status:** Accepted

## Context

Gateway message handlers come in two execution shapes: plain C function
pointers that run inline on the reactor thread, and Python-bound trampolines
that the dispatch routes through the worker pool (ADR-0004). The inline
default exists because a plain C handler never enters the Python
interpreter, so the pool hop would only add latency.

That reasoning holds only while C handlers are short. The scaling-gate
decomposition at the distributed fidelity gate's operating point (ADR-0020)
measured the opposite regime for a full game-logic handler: applying one
movement input end to end (decode, resolve, model apply/step, artifact
publish, cell bookkeeping) costs a few microseconds per apply in C, while
driving the same sequence through a Python handler costs milliseconds of
interpreter, collector, and allocator-churn thread time per apply — against
a per-apply thread budget of a few hundred microseconds at that point. The
C path is orders of magnitude under the budget; the per-input Python
crossing is the binding constraint. Running such a handler inline on the
reactor thread is not an alternative either: that thread already ran near
saturation at that point, and an inline apply path pushes it past its
budget.

## Decision

Message handler registration gains a third execution value, pool-bound: a
native C handler registered with it is submitted to the attached worker
pool instead of invoked on the reactor thread. The pool-bound value
conforms to the founding set, including the additive-components rule
(ADR-0032).

Pool-bound dispatch is identical for both pool-bound modes: the payload is
copied across the reactor→worker handoff, the session is referenced for the
dispatch duration, and a submit beyond the pool's task capacity drops the
dispatch and counts it rather than running anything inline — the ADR-0004
conformance rule, unchanged. A handler is a Python-bound trampoline or a
pool-dispatched C function, never both: registering with both values is
rejected. With no pool attached the dispatch drops and counts exactly as
queue exhaustion does; a pool-bound handler never runs inline.

## Consequences

Positive — a hot-path C handler leaves the reactor thread without entering
Python, and the per-input interpreter, collector, and allocator-churn cost
disappears from the apply path; the pool's existing task machinery (payload
copy, session lifetime, drop accounting) carries C work with no new
dispatch surface.

Negative — a pool-bound C handler removes the interpreter lock as an
incidental serialization backstop on standard builds: where the interpreter
once serialized Python handlers, the handler's own lock discipline is now
load-bearing under both interpreter builds, so pool-dispatched C handlers
must be thread-safe by construction, not by interpreter accident. The
pool's capacity now bounds C handler work too; exhaustion drops and counts
C dispatches exactly as it drops Python ones. The empirical basis for the
value is a decision-time measurement: at the gate point of the day the
C-native apply path measured orders of magnitude under its per-apply thread
budget, and the value makes that measured path reachable from a registered
handler.
