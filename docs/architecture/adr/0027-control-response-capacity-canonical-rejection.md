# ADR-0027: Control response capacity is fixed and oversize rejection is canonical

**Status:** Accepted

## Context

The control plane's per-connection response buffer is fixed storage,
allocated when the connection is accepted and never resized. A route whose
response exceeds the capacity fails the response build; the Python route
boundary surfaced that failure as a handler exception and answered with a
500 whose body named an internal error symbol, and the capacity condition
counted in the handler-exception gauge — a sizing signal read as a code
defect. The built-in metrics route rendered its own ad-hoc 500 for the same
condition. The sizing parameter itself was unreachable from the embedded
server facade, so the documentation's directive to size the buffer for the
largest listing a route returns could not be followed on the default
topology, and the shipped default undersized real listings by an order of
magnitude.

## Decision

The response capacity stays fixed storage sized once at create; capacity
errors are legal outcomes and the buffer does not grow on demand. The
shipped default budget is 262144 bytes per connection.

An over-cap response answers with one canonical rejection: a 500 whose JSON
body names the route, the attempted size, and the capacity, counted in its
own control-plane counter (`kith_control_response_overflow_total`). The
Python boundary raises the oversize response in its own exception family —
a public subclass of the base error carrying the attempted size — and the
route trampoline answers it with the canonical rejection instead of the
handler-exception path, so the handler-exception gauge keeps its meaning.
C route handlers keep their existing close-on-error contract; the exported
reject helper is the sanctioned alternative where the caller holds the
control handle, and the metrics built-in renders the same canonical
rejection.

The sizing parameter is reachable from the embedded facade: server creation
parameters forward the control plane's response capacity, alongside the
facade's other tuning knobs.

## Consequences

Positive — an oversize response diagnoses itself in the response body and
one counter, the sizing directive becomes actionable on the default
topology, and a rising handler-exception rate reads as handler defects
again. The rejection shape is one body and one counter across the Python
boundary, the built-in metrics route, and C callers that adopt the helper.

Negative — the per-connection memory floor at accept rises to the new
default; deployments that size the capacity explicitly set their own floor.
A listing beyond any fixed capacity still fails — pagination stays the
game's own discipline for unbounded rosters.
