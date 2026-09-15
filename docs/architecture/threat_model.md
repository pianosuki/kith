# Threat model

This document states the trust boundary the framework's security claims
rest on: the in-scope denial-of-service surface in `SECURITY.md`, the
hardening defaults in `cmake/hardening.cmake`, and the adversarial soak
plan below all read against the model defined here. The two documents
cannot drift — `SECURITY.md`'s scope section carries the one-sentence
consequence for the surface whose posture inverts on a setting, and
this document owns the full statement.

## Trust boundaries

Attacker control is a property of a datum's origin, never of its
position relative to a component. Two origins carry attacker control in
the default posture — the wire listener and, once rebound, the control
plane — and everything derived from them stays attacker-controlled no
matter how deep it travels: through decode, through dispatch, into
handlers, into logs and metrics. A byte the operator's own process
wrote is trusted wherever it travels; "past the gateway" means
"structurally checked", never "trusted".

### Wire ingress — untrusted

The gateway's net listener binds the wildcard address unless
`listen_host` pins it (`include/kith/server/server.h`), so the model
treats the listener as internet-exposed by default. Every byte on that
socket is attacker-controlled before, during, and after decode. The
framework authenticates no wire peer and recognizes no trusted client;
all sessions are attacker-equal.

The decoder chain is the hardening boundary: header validation, checked
length arithmetic, the payload cap, the per-connection ring-buffer cap,
and per-connection close on any decode failure. Structural validity is
a framework warranty; semantic validity never becomes one.

### Gateway-to-handler handoff — structurally checked, semantically untrusted

A frame that passes decode and reaches a game handler is warranted
structurally only: well-formed header, a type id within the handler
table, payload within the cap, correlation trailer consistent with its
flag bit. Pool dispatch delivers a copy of the payload; inline dispatch
lends the frame buffer for the duration of the call. The framework
never inspects or sanitizes payload contents: game handlers own all
semantic validation, and every payload is hostile data regardless of
which session carried it. Omitting this boundary invites handlers to
treat gateway-accepted data as clean — the most exploitable misreading
a game author can make.

### Trusted-local planes — control, coordination, state backends

The control plane binds `127.0.0.1` and serves unauthenticated HTTP;
the coordination bus is a loopback transport whose only member is the
local instance; the state backends are trusted service dependencies
reached with operator-held credentials. Their inputs are trusted as
operator-local, and the framework does not harden them against a
hostile peer.

The control plane carries Python-bound game routes behind its own
smaller caps (64 connections, a 64 KiB body cap by default). A deployer
who rebinds the control host away from loopback opens a second,
unauthenticated ingress into game logic that bypasses the wire decoder
entirely, and owns whatever external access controls must then stand in
front of it. The trust statement of this surface inverts on one
parameter — a boundary whose default posture differs from its reachable
posture must be stated, not implied; `SECURITY.md` carries the
consequence.

### Residual consumer risk — the human log sink

The human-readable sink writes caller-chosen field values verbatim;
game code that logs attacker-derived strings (player names, chat text)
through the caller-chosen-fields API into that sink accepts log forgery
and terminal-escape injection. The JSONL sink escapes values and is the
safe path for untrusted-origin data. The framework's own emission is a
single static string with no fields.

## Adversarial soak plan

The soak is the exercise behind the in-scope DoS claim: a dedicated
degradation run on the certified distributed-2000 shape
(`docs/guides/scaling_checklist.md` recipe, reference envelope
recorded) with the full legitimate certified load running as background
for the entire run. Certified gates are reported with deltas, never
enforced, in any run containing attack traffic — an attack mix is a
different configuration than the one the thresholds were certified
under, and a gate miss under attack is ambiguous between attack effect
and regression. Certified gate runs stay attack-free.

### Prerequisite

The soak's accounting needs per-class rejection counters at the decode
and transport rejection sites (header, declared length, ring-buffer
cap, trailer, type-id bounds, I/O close) plus a tattle counter in the
soak driver's handlers. Neutralization is measurable only against
those counters, so the soak run follows their landing; the decode
close path itself stays silent and counter-driven by design.

### Attack budget

Phase one, decode saturation: malformed frames at ≥ 3× the legitimate
frame-arrival rate (or the harness ceiling, stated), connection churn
≥ 500 connections per second, ≥ 30% link headroom above the combined
legitimate-plus-attack load, held ≥ 60 minutes plus ≥ 10 minutes of
observed drain. Phase two, link saturation: 5 minutes at true line
rate, invariants only — at that rate, drops are link arithmetic, not
framework behavior. The ramp profile and RNG seed are recorded with the
artifacts.

### Malformed-input taxonomy

All classes present, each ≥ 5% of the malformed budget:

1. Bad magic or version.
2. Declared payload length above the cap.
3. Declared-legal-but-large length with the body never sent (ring
   accounting and memory hold).
4. Truncation and mid-frame disconnect.
5. Structurally valid header with a garbage payload — this class must
   reach handlers; it is the live test of the handoff boundary
   statement.
6. Type id out of table bounds (numeric edges, base-plus-offset) and
   unregistered-but-in-bounds type ids.
7. Correlation-trailer flag-bit and byte mismatch, both orders.
8. Connect, one bad byte, close — churn.
9. Idle connects holding partial frames.
10. Interleaved valid and invalid frames on a single connection — the
    per-connection close discipline test: after the first reject, no
    later frame on that connection is ever delivered.

### Criteria

Pre-registered before the run; post-hoc threshold adjustment fails the
run by default.

Invariants (zero-tolerance):

- **Liveness** — every instance survives the full window; no fatal
  signal or assertion; the health endpoint answers every probe
  throughout (the probe count stays inside the control plane's
  connection cap, or the soak config raises the cap explicitly).
- **Neutralization and conservation** — zero structurally invalid
  frames delivered to any handler, verified by accounting:
  malformed-injected equals the sum of rejection-class counters (±
  frames in flight at phase boundaries), and structurally-valid-injected
  equals the handler tattle count.
- **Recovery** — within 120 seconds of flood end, per-process open-fd
  count within +2% of the pre-flood baseline, thread count identical,
  live sessions back to the legitimate-load baseline. These, not RSS,
  are the leak tests.
- **Log volume** — bytes per sink during the flood ≤ 10× the pre-flood
  baseline and ≤ a stated absolute cap; the decode-failure path stays
  counter-based or silent — a per-rejection log line at flood rate is a
  self-inflicted denial of service and fails this criterion.

Bounds (pre-registered):

- **Memory** — after warm-up, per-process RSS growth ≤ max(+5%, +64 MiB)
  over steady state and a post-warmup slope ≤ 1% of steady state per
  hour. RSS return-to-baseline is reported only: the allocator retains
  freed arenas, and non-return is not evidence of a leak. The idle-
  connect class must hold per-connection state within the session and
  ring-buffer worst case under the same cap.
- **Service degradation** — against a same-host certified-configuration
  baseline run without attack traffic: `session_ok ≥ 94%`,
  `selected_clients ≥ 90%` (5 percentage points below the certified
  floors), bootstrap p95 ≤ 6000 ms and ≤ 1.5× the baseline's measured
  p95, legitimate continuity flicker ≤ 1 per 10 legitimate clients per
  flood phase, and zero legitimate dispatch drops.
- **Recovery time** — every service metric returns to baseline values
  within 60 seconds of flood end.

### Exclusions and positioning

The control plane and coordination bus are loopback in the certified
shape and are not attack surfaces in this soak; a rebound-control drill
is a separate, non-certified exercise. Postgres and Redis are out of
scope. The soak is complementary to the nightly libFuzzer decoder tier:
fuzzing owns decode memory safety on arbitrary input; the soak owns
liveness, resources, and degradation under sustained attack at scale.
It is reported, not gated — promoting it to a certified claim class is
an ADR-0021 amendment, and after v1.0.0 the record set evolves only by
supersession or explicit amendment.

The soak config sets `handler_table_size` to cover the game catalog
(the default covers framework types only — user type ids above the
reserved base would otherwise turn legitimate traffic into rejects and
spoil the conservation accounting).
