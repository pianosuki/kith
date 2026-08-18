# ADR-0014: Deterministic simulation is a correctness contract

**Status:** Accepted

## Context

The framework's SIM plane executes game-model logic — movement, collision,
spawning, state transitions — in lockstep ticks. Without a determinism
guarantee, the same inputs can produce different outputs across runs,
machines, or architectures, breaking replay testing (the framework's
primary regression methodology), agentic closed-loop scenarios, and
distributed consensus. An earlier monolithic, actor-centered MMO server's
simulation layer was non-deterministic: timestamps, random-number state,
and iteration order varied between runs, making replay useless and agentic
testing unreliable.

## Decision

Given an identical initial world state and an identical ordered input event
stream, the simulation produces an identical final world state and output
event stream, regardless of host architecture, compiler, libc version,
number of worker threads, or wall-clock time. The contract rests on seven
mechanisms:

1. **Deterministic randomness service** — the framework's randomness
   service (`include/kith/util/rng.h`) is the sole randomness source: an
   xoshiro256** generator whose advance and output are pure 64-bit unsigned
   arithmetic, so sequences are bit-identical on every platform. The
   composition root seeds it once through size-versioned params and injects
   it into models; models never draw from system entropy sources, and
   nothing reseeds behind the caller's back. Independent streams derive
   from a derivation seed plus a stream key alone, so opening one stream
   never advances its parent and keys are stable identities chosen by the
   composition root. The full generator state, derivation seed included, is
   capturable and restorable as a plain value, so a checkpoint resumes
   mid-sequence and derives sub-streams identically after a rewind.

2. **Fixed iteration order** — model logic iterates actors through their
   contiguous storage order (the model step entry point receives the actor
   array and count), never pointer-keyed container order, so traversal is
   independent of addresses, allocation history, or ASLR layout. Gateway
   view composition sorts staged artifacts with a content-keyed comparator
   before emission, so delivered ordering is a function of artifact
   identity, not of arrival order or thread scheduling.

3. **Time is a tick counter** — the tick clock service
   (`include/kith/util/tick_clock.h`) carries the tick count the composition
   root advances once per simulation step and converts it against the
   configured tick rate with pure integer arithmetic: conversions floor the
   per-tick period rather than accumulating rounding drift, and overflow is
   an error, not a wrap. Wall-clock time stays available to infrastructure
   code (session timeouts, metrics, log timestamps) through a wall-clock
   query documented outside this contract. Model code never reads wall
   time; the duration a model sees in a step is the configured tick
   cadence, not a measured interval.

4. **Floating-point environment** — the build pins the conditions bit-exact
   math requires: `-ffp-contract=off` on every target (FMA contraction
   would otherwise fuse expressions differently across compilers and ISA
   baselines), SSE2 math forced on 32-bit x86, and a compile-time assertion
   that FLT_EVAL_METHOD is 0, which x86-64's SSE2 baseline guarantees —
   excess x87 precision cannot silently diverge intermediates. Model math
   that must be portable beyond that uses fixed-point arithmetic (64-bit
   micro-units; the spatial example and its replay fixture use Q16.16).
   The four basic double operations are deterministic under the pinned
   environment; transcendental functions vary across libm implementations
   and stay outside the guarantee.

5. **Deterministic publication ordering** — cell products carry a monotonic
   per-cell publish sequence; a newer product supersedes the earlier one
   under a fixed rule rather than a race. Downstream composition emits in
   content-keyed order (mechanism 2), so what a client observes is a
   function of what was published, never of poll order, network arrival, or
   worker interleaving.

6. **Replay recording and regression hashing** — the replay tool replays a
   recorded input stream against the simulation shared library through the
   public ABI and prints a rolling FNV-1a hash of the live actor set. The
   recording is a development-tool format whose logical content — tick
   number plus ordered inputs, with optional generator checkpoints — is
   what the contract cares about; the encoding is not a public ABI surface.
   A pinned golden-hash regression in the verification suite asserts the
   hash sequence every run, flagging any behavior change without reasoning
   about internals.

7. **Testing mandate** — every simulation-behavior change is validated
   against the golden-hash replay regression in the standard gate, and
   agentic scenarios (ADR-0006) run closed-loop against the deterministic
   simulator by default.

### The tick boundary

The tick boundary has a runtime substrate: a per-tick game-logic callback
on the server (the composition root). Without it, game code steps its model
eagerly per input from concurrent workers, producing per-input cell
publishes and leaving the tick-boundary and event-ordering clauses without
an anchor.

The server exposes a single per-tick registration carrying one execution
flag (Python-bound, or unset for a C callback). Once per normal tick the
reactor advances a monotonic 64-bit tick counter — the substrate the tick
clock tracks (mechanism 3) — and dispatches the registered callback. A
Python-bound callback is submitted to the worker pool as one task; a C
callback runs inline on the reactor: tick work is ordered simulation work
on the critical path, not the parallelizable handler work whose pool-bound
dispatch ADR-0004 governs, and the reactor never runs a Python-bound
callback inline. An exhausted pool queue drops the callback and counts it
under the observability contract (ADR-0019) — honest backpressure: a
skipped per-tick flush rather than a reactor stalled in Python. The drain
tick neither advances the counter nor dispatches the callback: the tick
boundary is a normal-tick event, not a teardown event. The counter advances
once per normal tick whether or not a callback is registered, so game code
that reads it sees a stable, monotonic, replay-deterministic tick index.

The tick substrate conforms to the founding set, including the
additive-components rule (ADR-0032): composition-root dispatch adds no
plane (ADR-0001), Python callbacks are pool-bound (ADR-0004), the error and
allocation contract is the standard one (ADR-0018), and the new symbols
live under the existing versioned server symbol node and the ABI gate
(ADR-0008, ADR-0017). The determinism guarantee is scoped to the model
layer: the model's deterministic step and input apply over a contiguous
actor set, given identical ordered inputs and tick cadence; plane-level
behavior outside the model (session timeouts, reactor scheduling, fabric
transport timing) is not part of the replay contract.

## Consequences

Positive — replay is trustworthy, agentic testing is deterministic,
distributed simulation can merge independently-replayed cell histories, and
model debugging is reproducible. Negative — model authors cannot use
convenient non-deterministic patterns (entropy calls, wall timestamps,
pointer-order iteration); floating-point portability requires staying
inside the pinned environment or using fixed point; stream keys must be
stable identities, since keys derived from creation order reintroduce order
dependence. The replay recording remains a development-tool format free to
evolve until declared stable; freezing it as a compatibility surface is a
separate decision. The contract is locked at v1.0.0; relaxing it for
specific models requires a superseding ADR documenting which determinism
guarantees are relaxed and why.
