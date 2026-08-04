# ADR-0007: Framework wire fields are big-endian

**Status:** Accepted

## Context

A wire contract needs one byte order. The proto frame header and its
optional correlation trailer were big-endian from the start (see
`include/kith/proto/proto.h`), but the gateway replication payload was
written in host byte order — each field copied from its native struct slot,
and the Python decoders read the result as little-endian. On a little-endian
host (x86/ARM-LE, the development and production target) the mismatch
happens to round-trip, so the defect was latent: the wire format was
incoherent (big-endian header, host-endian payload) and non-portable to a
big-endian peer, which would decode every payload field byte-swapped. The
single-subject and batch paths shared the per-subject record parser, so both
carried the same defect.

## Decision

Every framework-managed field on the wire is big-endian (network byte
order), matching the proto frame header:

1. The frame header: the message-type and payload-length fields, each
   big-endian.
2. The optional correlation-ID trailer: a fixed 8-byte big-endian value
   appended to the frame and marked by a reserved flag bit in the header.
   Frames without the flag carry no trailer, so the trailer is
   backward-compatible. The trailer rides the upstream direction: the
   gateway's own downstream frames are encoded without it, and downstream
   correlation exists on the surfaces that explicitly support it.
3. The replication delivery payload: the per-subject record and the batch
   frame header, each field big-endian. The concrete field order and widths
   are the delivery header's contract. The serializer writes every byte of a
   payload explicitly — no host-order bulk copies — so the wire layout is
   defined independent of the host's byte order.

Application-level message formats a game defines on top of the proto frame
(the `examples/` login, chat, input, and presence payloads) are outside this
contract: they are game-defined, self-consistent on both sides, and free to
choose their own byte order.

## Consequences

Positive — header, trailer, and payload share one byte order; the wire is
portable to big-endian peers, at the cost of a handful of shifts per field
per subject per refresh, negligible against the frame I/O the header already
swaps; the decoders state the byte order explicitly, making the contract
readable at the decode site; the deterministic replay hash (already
big-endian over sim actor state) is unaffected. Negative — the correction
was a wire-format break for any consumer that decoded the old host-order
payload; before v1.0.0 there was no external consumer to migrate. Byte-order
helpers remain module-local in proto, client, and delivery — the
established idiom, with consolidation a styling choice rather than a
contract need. The per-cell publish sequence and the
publisher-minted coverage counter are distinct counter spaces,
both minted at publish; neither meaning changes with byte order. The
correlation ID propagates into observability explicitly: a call site that
has one in scope passes it as the correlation-ID caller field on a log entry
or as a caller-supplied metric label — the convention `docs/event_schema.md`
defines — tying client observations to server-side observability (ADR-0019).
