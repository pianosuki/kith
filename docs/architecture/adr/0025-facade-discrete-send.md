# ADR-0025: The facade carries the discrete send

**Status:** Accepted

## Context

A connected session receives the framework's state stream as its composed
view set: the relevance composer builds a bounded per-subscriber selection
each tick and the deliver pass encodes it. Games also compose one-off
messages aimed at one client — a login ack, a targeted notice, a reply —
that belong to no view set. The gateway's delivery core (encode one frame
and enqueue it on the connection's output queue) is internally
synchronized: the encode step reads only create-fixed state, and the frame
pool and the per-connection output queue are mutex-protected. The delivery
executor already exercises that core from its own threads whenever the
tick's deliver pass is configured off the reactor thread, so placing one
frame on a connection is already a multi-producer operation in the shipped
runtime.

The public contract did not say so. The conn-targeted delivery function
was documented reactor-thread-only, and the Python facade carried no send
surface at all. A game that needed a one-off wire message from a
pool-dispatched Python handler had no sanctioned route: it routed the
traffic through the control plane (HTTP), traded a handshake for a
predictable actor id, or hand-bound the C support surface through private
accessors. The control plane serves its own purpose; a wire-grade send
should not depend on it.

## Decision

The delivery core is callable from any context that holds a reference to
the target session — the dispatch reference a pool-dispatched handler
carries, or a session the composition root owns — from the reactor thread,
or while holding a direct connection reference. The conn-targeted delivery
function's contract states this condition and names the pinning hazard
that makes reference-free contexts unsafe: an unpinned connection may be
released concurrently by session teardown. The Python boundary carries the
send as the primary surface — a method on the session a handler was
handed, mirrored on the server facade and the gateway wrapper for
composition roots. Backpressure raises at the call site in the transport
family; a closed connection raises in the lifecycle family.

No deferred send queue ships. A context that holds no session reference —
a tick handler, a control route, an external thread — has no sanctioned
send; an acquisition or enumeration surface that would hand such contexts
a reference is its own decision (ADR-0026) and composes with this one. The send
carries no correlation trailer: request/reply correlation remains a
payload-level convention of the game.

## Consequences

Positive — the dominant one-off pattern stops routing around the wire and
the private accessors; the encode-and-enqueue path gains one documented
truth instead of a new mechanism; the control plane returns to HTTP-only
traffic. The widening changes no code: the executor's cross-thread use
already proved the path, and the connection-reference form covers the C
callers the support surface was written for. Discrete sends are
transport-plane behavior; the deterministic simulation contract scopes the
replay hash to the model layer (ADR-0014), so the send surface adds no
replay coupling.

Negative — sends from reference-free contexts stay unsupported until an
acquisition surface exists; backpressure surfaces as an exception at the
call site, so a handler that ignores it loses frames silently — the
handler's own retry policy is load-bearing.
