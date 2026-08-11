# ADR-0021: Delivery semantics are pluggable encoding and scheduling presets

**Status:** Accepted

## Context

The gateway delivers replication by serializing a subscriber's entire
bounded view set every deliver pass: fixed-size big-endian records
(ADR-0007), one frame per subject or one batch frame, enqueued on the
session's TCP connection. This is correct and self-healing — a lost pass is
repaired by the next one — but its cost scales with budget × tick rate
regardless of how much actually changed, and it dominates the measured
per-instance delivery ceiling. The pluggability rule (ADR-0013) sanctions
the alternative: a delivery strategy is an implementation choice inside the
gateway plane, bounded by the plane's invariants and by the founding
contracts — products stay immutable and cell-scoped with supersession in
the fabric (ADR-0002), and the send set stays budgeted (ADR-0005).

## Decision

Delivery semantics decompose into two orthogonal policy axes — **encoding**
(full-record or delta: the delta encoder diffs the composed view against a
per-session, per-subject send ledger of last-enqueued records and emits
membership add/remove events plus field diffs) and **scheduling**
(every-tick or rate-limited: the rate-limited scheduler derives
per-subject due times from representation tier and subject class). The
presets are `full` (full encoding, every-tick scheduling — the shipped
behavior, byte-identical), `delta` (delta encoding, every-tick scheduling),
and `tiered` (full encoding, rate-limited scheduling). The fourth
combination (delta + rate-limited) is a configuration of the same two
policies, not a fourth strategy; both axes share one per-subject send
ledger.

The seam sits inside the gateway's delivery path, between the composed view
and the connection enqueue. Strategies follow the registry pattern of the
sim model registry (ADR-0013): a size-versioned vtable with reserved slots
(ADR-0008) registered by name on the gateway handle, selected through the
gateway params under additive size-versioned evolution, rejected with the
established registry error conventions. Strategies execute inline in the
gateway tick path when no delivery executor is configured; when one is, the
deliver step runs on executor threads under ADR-0022's contract, and the
correctness rules below are unchanged. The `full` preset is the factory
default and the reference implementation.

Correctness rules, binding on every strategy:

1. **Transport precondition.** Delivery runs over an ordered, reliable,
   FIFO transport (TCP). A strategy needs no client acknowledgements: the
   ledger is the last successfully enqueued state, and session teardown
   frees it. A lossy transport invalidates this rule and the no-ack design
   together.
2. **Transactional commit.** A ledger entry advances only at successful
   enqueue. A dropped or backpressured frame leaves the ledger untouched,
   so the next delta is computed against the state the client actually
   holds.
3. **State, never frames.** A strategy holds session-scoped encoding state,
   not buffered frames. Buffering frames across ticks is a per-connection
   supersession queue, which ADR-0002 forbids; supersession stays in the
   fabric, and a strategy never requests product history from it.
4. **Delta grammar.** Subjects entering or leaving the view, and
   crowd↔individual transitions, are membership events — the crowd
   aggregate is a synthetic subject whose id exists only while overflow
   persists, so a transition replaces records rather than diffing them.
   Tier transitions within one actor id are field diffs while the record
   interpretation is tier-invariant. An empty composed tick emits nothing;
   an empty ledger makes the first frame definitionally full. A delta at
   least as large as the full record set falls back to the full encoding; a
   long-interval full refresh is a paranoia backstop, not the primary drift
   guard.
5. **Wire contract.** Strategy payload grammars are big-endian (ADR-0007)
   and register framework-owned message types below the user type floor
   per the proto registry convention.
6. **Budget invariance.** Every strategy delivers the budgeted send set
   (ADR-0005); strategies vary bytes and cadence, never the budget.

The `tiered` preset ships in the gateway library: full-record encoding with
rate-limited scheduling, record-granular change suppression against the
per-subject send ledger, membership-event emission for departures and crowd
transitions, and cadence intervals exposed as a size-versioned config blob.
Suppression compares the record's content fields against the ledger —
exactly "would the client's bytes change" — so tier transitions self-heal
(a reduced-to-full upgrade re-widens the input-tick field, differing the
record even when the source artifact did not) and dense-cell granularity is
exact; serialization happens only for subjects actually sent, an
optimization that must not change suppression semantics. Under the tiered
preset an entry conveys through the subject record itself (a state arriving
for an unseen id is unambiguous), so only absence-disambiguating events
exist on that path; the `full` preset, whose resends cannot distinguish new
from repeated subjects, emits enter events as well. The `delta` preset
remains contract-only; its encoding axis shares the ledger substrate the
tiered preset exercises.

## Consequences

Positive — replication traffic scales with the change rate instead of with
budget × tick rate; adopters choose semantics per game without leaving the
gateway plane; the dense-topology ceiling gains a lever that does not touch
the plane contracts. Negative — the delta preset adds per-session ledger
memory proportional to the budget plus a per-subject diff pass; the tiered
preset adds far-ring staleness a client interpolates within the max-gap
floor. The factory default stays `full`, byte-identical on its state
records; dense-topology deployments select `tiered` through params and the
scaling gates re-measure the alternatives against it.
