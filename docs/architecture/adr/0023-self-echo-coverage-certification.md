# ADR-0023: Self-echo coverage certification via publisher-minted update_seq

**Status:** Accepted

## Context

The distributed fidelity gate bounds the certified move-missing ratio per
instance. At the gate operating point the harness measures that
ratio by counting self-echo events: replication frames a client receives
describing its own actor, compared against the movement inputs it
submitted. Two measured facts make event counting dishonest at the
operating point:

- The composed view refreshes at the rebuild cadence, far more slowly than
  inputs arrive under saturation, so even a gateway that loses nothing
  reads ~32% missing. The event-counting rule floors at the sampling
  ratio; no client-side counting rule over the echo series can remove that
  floor.
- Replacing events with frontier certification (a submitted tick counts as
  observed when a later echo's input tick passes it) over-credits the
  opposite way: a dropped input is passed over by the frontier, so a
  gateway dropping most of its inputs would still read ~0% missing. A
  tick-delta series genuinely cannot decompose an echo gap into
  applied-and-superseded inputs versus dropped ones — applied-then-
  superseded and never-applied inputs leave identical tick deltas.

The disambiguator is an apply count, and only the source of the applies can
mint one.

## Decision

Every actor carries a publisher-minted **`update_seq`**: a monotone
per-actor counter that models increment once per applied movement input.
The counter is minted at publish time, alongside the per-cell publish
sequence (ADR-0002), and travels as payload under the wire contract
(ADR-0007). `0` is reserved for "never minted" — a publisher that does not
participate, or an actor that has not applied movement yet. Spawn and
teleport publishes carry the field unchanged: the counter certifies
movement coverage, and inflating it with non-movement publishes would
reintroduce over-credit through the harness denominator.

**Pipeline.** The counter flows verbatim from the simulation actor through
the published artifact and the fabric's full rendering into the gateway's
composed self subject; reduced and crowd renderings zero it alongside the
input tick, since they do not vouch for per-actor coverage. The serialized
replication record widens 64 → 68 bytes per subject (a wire-layout change
under ADR-0007); the membership-event marker word moves with it, preserving
the single shared record shape ADR-0021 binds. Legacy decoders that read
the 64-byte prefix stay functional (trailing bytes ignored).

**Certification semantics.** A client that receives its self subject with
`update_seq` at n learns that the source has applied at least n of its
movement inputs. Consecutive self echoes therefore certify coverage in
bulk: the counter delta between two echoes counts the movement
applications that happened between them, regardless of how many echoes the
rebuild cadence produced. The harness rule derived from this — a submitted
input is missing unless a self echo within the movement window plus the
post-window drain certifies it — is the gate's move-missing reading going
forward: immune to the sampling floor and unable to over-credit drops,
since a dropped input never mints.

**Metric naming.** The certified rule takes the gate name
`move_missing_ratio_certified`; the historical event-counting rule is
retained unchanged as `move_missing_ratio`, a continuity companion. The
certified rule binds the existing threshold through the gate documentation,
which records the correction — renaming rather than redefining in place
keeps the published gate history comparable across the boundary.

**Suppression exclusion.** Tiered suppression's change test compares the
record's content fields — position, velocity, the input tick, and the
representation level. `update_seq` stays out of it: it is certification
metadata, not content, and it advances in lockstep with the input tick on
every movement publish anyway. The self subject bypasses suppression
regardless.

**Observability.** Two cumulative counters ride the phase stats (stamps and
fallbacks), advanced only on rebuilt self subjects while stamping is
enabled: a rebuild whose source counter is minted stamps; one whose counter
is 0 is a fallback (the subscriber falls back to event counting for that
client). Retained-view ticks re-deliver the previous counter and write no
new stamp.

**Wrap contract.** The counter is 32-bit; at gate-point submit rates it
wraps only over long horizons. Consumers derive coverage from per-window
counter deltas with in-window baselines, so a wrap inside a measurement
window is impossible for gate-shaped windows; comparing raw counter values
across windows is not a supported use. Production consumers that need
absolute positions must extend the width before relying on one.

**Determinism.** The minted counter is deterministic replayed state
(ADR-0014): identical ordered inputs produce identical counters, so the sim
replay golden hash shifts once at introduction and stays stable.

## Consequences

Positive — the move-missing metric measures real movement loss at any
rebuild cadence; the sampling floor disappears without weakening any
threshold; dropped inputs cannot hide behind frontier advancement.
Negative — the replication record widens by four bytes per subject (~6% per
record, shrinkage in records-per-batch); publishers that never mint leave
their clients on the fallback path; the counter adds one deterministic
field to replayed state.
