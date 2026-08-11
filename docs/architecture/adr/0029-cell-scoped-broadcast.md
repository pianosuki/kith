# ADR-0029: The gateway carries the cell-scoped broadcast

**Status:** Accepted

## Context

Dense subscriber populations generate traffic that is not kinematic state:
announcements, channels, and per-cell social events are free-form bytes with
no representation in the replication record, which pins one fixed kinematic
layout. The fanout machinery exists — a cell product bump reaches every
session whose subscription window covers the cell — but its payload is the
rendered artifact scalars only. The targeted discrete send covers one
session by reference; no surface covers the cell's recipient set. Filling
the gap game-side means a cached recipient roster: per-event session
bookkeeping the gateway already owns, and a roster reference dangles under
churn.

## Decision

The gateway carries a cell-scoped broadcast. A caller submits a cell key, a
message type id, and caller-owned bytes from any thread; on the next tick
pass the reactor enumerates the sessions whose subscription windows cover
the cell and delivers the bytes as one frame per recipient connection — one
encode, fanned through the connection queues on a shared refcounted frame.
The recipient set is derived, never submitted: window membership is the
gateway-side image of the cell's subscription set, so a broadcast is not
budget-gated — a tier-suppressed subscriber still receives its cell's
events — and no game-built roster forms.

The broadcast queue is distinct from its two neighboring shapes: it is not
the discrete send's deferred queue (that surface is conn-targeted,
reference-required, and ships no deferred queue), and it is not
a delivery strategy's per-connection frame buffer (the delivery contract is
state, never frames — ADR-0021; the broadcast queue is gateway-wide,
buffers caller bytes, and encodes at drain time). The drain is not subject
to the composition budget: event fanout must not starve behind state
recomposition, and it runs ahead of the pass's composed frames, the same
ordering edge membership events ride.

The queue is bounded at a fixed depth; a submit past capacity is refused
with backpressure and counted (`kith_gateway_broadcast_refused_total`). The
payload is bounds-checked against the codec's maximum frame payload at
submit, so the queue never holds an undeliverable request. Delivery is
best-effort: a recipient whose connection queue is full or whose connection
has closed is counted as a drop (`kith_gateway_broadcast_dropped_total`), a
request with zero covering recipients completes as a no-op, and requests
pending at gateway teardown are counted as drops. Delivery latency is one
tick: a request submitted between passes is delivered at the top of the
next.

The record layout, the delivery presets, and the fabric's product stream are
untouched: the broadcast frame is an ordinary message frame — caller type
id, caller payload — both peers register the type out of band, and the bytes
are the game's own format.

## Consequences

Positive — free-form cell-local traffic has a public route whose recipient
set stays framework-derived; one encode per broadcast; refusal and drop
classes are separately visible; the state pipeline, the delivery presets,
and the wire record are untouched.

Negative — the fixed queue depth surfaces saturation as submit failure; the
refusal count is the evidence path for any future depth tuning. Broadcast
delivery bypasses the presets' suppression and tiering by design, so event
rates ride the reactor pass directly and a game keeps them commensurate
with social traffic. Best-effort delivery trades a per-recipient backstop
for pass progress: a saturated recipient queue drops that recipient's copy
rather than blocking the fanout.
