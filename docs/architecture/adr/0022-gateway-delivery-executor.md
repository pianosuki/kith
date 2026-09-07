# ADR-0022: The gateway delivery executor

**Status:** Accepted

## Context

The gateway tick composes and delivers every session's view inline on the
reactor thread. At the fidelity gate's operating point (ADR-0020) the
measured per-session demand — compose plus deliver plus the cache refresh —
exceeds the compose budget, the budget defers recompositions wholesale, and
the gate fails on delivery throughput: a measured capacity boundary, not a
logic defect. Moving deliver work off the reactor thread is the structural
lever the demand arithmetic points at: tick demand drops under the budget
and delivery wall time divides across several threads.

Two contracts shape the answer. ADR-0021 binds delivery strategies to the
gateway's delivery threading model and defers the executor case to this
record. ADR-0004 is not breached: its pool contract is scoped to handler
dispatch, and executor jobs never touch Python, the fabric, the sim plane,
or the io_uring ring (the reactor-owned edge, ADR-0011). The worker-pool
primitive is shared infrastructure; the executor is a second instance of it
holding a disjoint contract.

## Decision

The gateway gains an optional **delivery executor**: when one is
configured, the tick's per-session deliver pass submits a job per session
to a gateway-owned executor instead of running the strategy inline. With no
executor (the default), delivery is the inline path, byte-identical, with
no executor code on it.

**Threading model.** This record owns the delivery threading model:
strategies execute inline in the gateway tick path when no executor is
configured and on executor threads when one is, with session initialization
and teardown staying on the reactor thread; ADR-0021 carries the
corresponding conditional at its execution clause.

**Per-session in-flight serialization.** Each session carries an atomic
in-flight flag and an embedded, zero-allocation job record. The reactor
sets the flag before submitting; the worker clears it as the job's last
write to the session, then releases the session reference. The job captures
the owning gateway at submit, so a session destroyed mid-flight cannot hand
the worker a dangling owner.

**The submit-time stamp is a contract.** The job carries the tick's time
value; deliver decisions use the submit-time stamp. A worker re-stamping at
execution time would advance the delivery cadence clock past the reactor's
(stamp-time comparison tolerates one-tick jitter; re-stamping desynchronizes
it). Re-stamping is forbidden.

**The compose-wait: a per-pass wait budget, not a per-wait timeout.** A
composition must not overwrite a session's view while that session's
deliver is in flight (the torn-read closure). Executor mode therefore waits
until no deliver is in flight before composing, and the wait draws from one
per-pass budget (a documented tunable, defaulting well below the tick
period); once the budget is exhausted, subsequent in-flight sessions skip
the wait immediately (counted). A per-wait bound was rejected: under
multi-session saturation it amplifies to one full bound of reactor stall
per session in a single pass — worse than inline delivery, and an
ADR-0004-shaped violation. On timeout the reactor skips the composition
(the session keeps its retained view) and proceeds to the delivery section,
which re-checks the flag: a job that finished inside the deadline window
still delivers the retained view this tick. The wait is the sole
reactor-block point the executor introduces, and the reactor never
force-overwrites a view while a deliver is in flight.

**Counted skip, never inline.** A submit that returns the pool's busy
failure clears the in-flight flag (uncontended — no job exists), releases
the session reference, and increments a counter. The reactor never falls
back to inline delivery, by the ADR-0004 rule: pool-bound work never runs
inline. The executor's task capacity equals the session count — at most one
in-flight job per session makes steady-state busy failures impossible. The
one micro-window where a busy failure can appear is designed and
self-healing: a worker returns its task node only after the callback's last
write, so a resubmit can observe the flag clear against an exhausted free
list for one tick, and the counted skip degrades exactly that session for
exactly that tick.

**Memory-model licensing.** The worker's plain (non-atomic) reads of the
session's view fields are licensed by the submit/dequeue ordering plus the
compose-wait's release/acquire pairing across ticks. Swapping the
mutex-protected queue for a lock-free one without equivalent fencing
silently introduces torn reads.

**Scratch privatization.** Executor threads claim a per-thread batch scratch
registered for teardown; the reactor thread and inline mode keep the
gateway-owned scratch. The delivery vtable ABI is untouched.

**Accounting.** Job durations accumulate and the tick folds them into the
delivery-time total, which keeps its meaning (total deliver work, wherever
executed) with a one-tick skew for late stragglers and a microsecond-scale
unattributed submit overhead.

**Observability.** One exposed-layout stats snapshot freezes the counter
surface (the delivery header owns its layout): jobs submitted, in-flight
skips, busy skips, wait-budget exhaustions, compose-wait timeouts, current
in-flight, and the in-flight high-water mark. In-flight skips count the
per-session delivery cadence gaps, and the two compose-wait counters —
timeouts plus budget exhaustions — decompose the compose-wait causes of
those gaps, which is why exhaustion is counted separately from the timeout. A separate deferred-session counter was considered and
collapsed: the fresh-compose path provably cannot skip, so every in-flight
skip is already retained-view-shaped and a second counter would split one
population across two names. The snapshot freezes at v1.0.0.

**Teardown order.** Gateway destroy drains and joins the executor first,
then frees the claimed per-thread scratches (workers may still be using
them during the drain), then its own teardown proceeds. In-flight jobs hold
session references, so a mid-drain session release may free a detached
session — safe, since a non-owner release never touches the session table.

**Error disposition.** Compose errors propagate as they do inline. The
worker's deliver return code is discarded: deliver failures stay visible
through the delivery counters (dropped frames), and an executor delivery
failure is not a reactor-path error.

**Cadence invariant (conditional).** The inline promise — frame cadence per
session is untouched, the view converges once its turn comes around — holds
when the executor keeps up. Under backpressure an in-flight session is
skipped for a tick: bounded gaps, staleness bounded by the max-gap
backstop, the gap rate observable through the counters above.

## Consequences

Positive — the reactor thread's per-session demand drops below the compose
budget at the gate operating point; delivery wall time divides across the
executor; the default configuration is byte-identical to the inline path.
Negative — one slow session can consume the whole pass wait budget (a
fairness loss accepted deliberately: reactor latency outranks delivery
fidelity); backpressure produces bounded cadence gaps instead of the inline
guarantee; the capacity model carries the submit overhead and the one-tick
accounting skew; the counter surface adds seven series.
