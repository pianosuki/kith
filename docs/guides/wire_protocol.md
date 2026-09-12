# Wire Protocol

This page is the v1 wire contract for a peer implementing a client or a
server by hand: the frame header every kith frame carries, the optional
correlation trailer, the message-type registry, the S2C replication
record, the multi-subject batch variant, and the delivery cadence that
shapes when records arrive. The codec that produces and consumes these
frames lives in the proto plane (`include/kith/proto/proto.h`); it frames
opaque payloads and enforces nothing about their field layout. C2S
payload layouts are game-owned: both peers agree on them out of band and
serialize their own fields. The one payload the framework pins is the
S2C replication record documented here.

## The frame header

Every frame starts with a 10-byte header followed by the payload region:

| offset | field       | width | encoding                  |
|--------|-------------|-------|---------------------------|
| 0      | magic       | 2     | `0x4B 0x54` (`K` `T`)     |
| 2      | version     | 1     | `1` (`KITH_PROTO_VERSION`)|
| 3      | flags       | 1     | bit mask (see below)      |
| 4      | type_id     | 2     | big-endian unsigned       |
| 6      | payload_len | 4     | big-endian unsigned       |
| 10     | payload     | N     | `payload_len` bytes       |

The total wire size of a frame is `10 + payload_len`. A non-kith stream
fails at the magic bytes, which act as a first-line reject.

- The **version byte is the dialect boundary**: a version the decoder
  does not implement is rejected with `KITH_EPROTO`. A future framing
  change is a new version byte, not a new flag bit.
- **Unknown flag bits are inert**: within an implemented version, flag
  bits the decoder does not know are ignored on decode and passed
  through in the frame view unchanged. They never alter the meaning of a
  known bit, the header layout, or the length semantics.
- **`payload_len` excludes the header** and is bounded by the codec's
  maximum payload (1 MiB by default, `kith_proto_params_t.max_payload`).
  A decode whose `payload_len` exceeds the bound is rejected with
  `KITH_EPROTO` before any allocation. A header-only frame
  (`payload_len` 0) is always accepted.

Decoding is incremental: `kith_proto_decode` returns `KITH_EAGAIN` when
the buffer holds an incomplete frame; the caller reads more bytes and
retries the same buffer. A type id the registry does not hold is
rejected with `KITH_EPROTO`. `kith_proto_declared_total` reports the
declared total (header plus `payload_len`) from the header bytes alone,
so a reader of a bounded buffer can reject frames that can never
complete before the body arrives.

## The correlation trailer

When `KITH_PROTO_FLAG_CORRELATION` (`0x02`) is set in the header flags, an
8-byte big-endian correlation ID occupies the last 8 bytes of the payload
region and is included in `payload_len`. The codec appends the trailer on
encode and strips it on decode, so the caller observes the message
payload without it; the decoded view carries the ID in host byte order.
A frame without the flag carries no trailer, and both forms are accepted
on decode. The trailer is the request/reply correlation mechanism
(ADR-0007); `kith_proto_correlation_hex` renders it in the lowercase hex
form the event schema names.

## Message types

The registry maps message-type names to numeric ids, and both peers
register the same name at the same id before exchanging frames of that
type — there is no central enum. Game-owned types use ids at or above
`KITH_PROTO_TYPE_USER_BASE` (1000); ids below it are reserved for
framework types. Decode enforces the registry, so a frame of an
unregistered type never reaches a handler. The gateway's handler table
is direct-indexed by type id: register types at 1000+ with a
`handler_table_size` larger than the largest id (see the getting-started
guide's "Wire type ids").

Broadcasts are ordinary message frames on this wire. The gateway's
cell-scoped broadcast (`Server.broadcast_cell`) fans one frame carrying
the game's type id and bytes — no correlation trailer — to every
session whose subscription window covers the broadcast's cell. No new
wire layout exists for them; a client decodes a broadcast like any
registered message type, and the replication record stays the one
framework-pinned payload.

## The replication record

The gateway's replication stream is one message type
(`replication_type_id` on the `Server` facade). Each record is 68 bytes,
big-endian, matching the frame header's byte order:

| offset | field         | width | encoding                |
|--------|---------------|-------|-------------------------|
| 0      | actor_id      | 8     | big-endian unsigned     |
| 8      | pos_x         | 8     | big-endian Q16.16       |
| 16     | pos_y         | 8     | big-endian Q16.16       |
| 24     | pos_z         | 8     | big-endian Q16.16       |
| 32     | vel_x         | 8     | big-endian Q16.16       |
| 40     | vel_y         | 8     | big-endian Q16.16       |
| 48     | vel_z         | 8     | big-endian Q16.16       |
| 56     | input_tick    | 4     | big-endian unsigned     |
| 60     | update_seq    | 4     | big-endian unsigned     |
| 64     | product_level | 4     | big-endian unsigned     |

Positions and velocities are Q16.16 fixed-point (the sim's native
representation); a client rendering in world units divides by `2**16`.

The `product_level` word discriminates the record. For state records it
is the representation tier:

- `0` — full: complete position, velocity, and input tick.
- `1` — reduced: position and velocity; `input_tick` and `update_seq`
  are zero.
- `2` — crowd: the aggregate for a dense cell — `actor_id` is 0, the
  position is the mean of the overflow candidates it represents, and the
  velocity, input tick, and update sequence are zero.

A state record for the subscriber's own actor carries the actor's
publisher-minted `update_seq` — the movement-application counter the sim
model advances once per applied input (0 when the publisher never minted
it, or when self-echo stamping is disabled by `self_echo_disabled`). A
full-tier record for another actor carries that actor's minted counter;
reduced-tier and crowd records carry zero.

For membership events the marker bit `0x80000000` is set and the low bits
carry the kind:

- `0` — enter: the subject entered the delivered view set.
- `1` — exit: the subject left the delivered set but remains in the
  subscription window (budget eviction or crowd absorption).
- `2` — crowd enter: the crowd aggregate engaged for this subscriber.
- `3` — crowd exit: the aggregate dissolved.
- `4` — vanish: the subject is gone from every cached window cell.

An event record names its subject in `actor_id` (0 for the crowd
aggregate) and zeroes the remaining fields. Events ride ahead of the
same pass's state records and are never suppressed, under both built-in
delivery presets; an event the output queue cannot take yet is retried
on a later pass, in order.

## The batch variant

When the composition root sets `replication_batch_type_id`, delivery
packs the session's full composed view-subject set into one frame per
refresh — a 4-byte big-endian count header followed by the records:

| offset | field         | width | encoding            |
|--------|---------------|-------|---------------------|
| 0      | subject_count | 2     | big-endian unsigned |
| 2      | reserved      | 2     | zero                |

The batch frame's type id is `replication_batch_type_id`. Without it,
each record is its own frame carrying `replication_type_id`.

## Delivery cadence

The gateway's run loop refreshes every bound session's composed view.
Compose and deliver run at most once per `view_refresh_interval_ms`
(100 ms by default) — a pass between refresh points leaves the session's
delivered view untouched. A session that has not bound an actor composes
nothing, and a session whose bound actor's cell is not tracked composes
nothing for that pass.

On the server shapes the composition root derives both refresh intervals
from the tick interval and forwards the view-set subject capacity; the
gateway shape accepts all three directly. A refresh interval wider than
the tick leaves inputs published inside the gap undelivered until the
next refresh — an explicit value owns that trade.

The full preset enqueues every record of the composed set on every
refresh. The tiered preset sends a record only when it differs from what
the subscriber was last sent and the subject's tier cadence allows the
send — full-tier subjects every pass by default, reduced-tier at least
200 ms apart, crowd at least 500 ms apart — with the subscriber's own
actor exempt (it updates every pass) and a 1 s max-gap backstop that
refreshes any silent subject regardless of change.

## Implementing a client

A client framer mirrors the decoder's incremental contract against a TCP
stream, which may coalesce or split frames arbitrarily: read until at
least the 10-byte header is present, compute the total from
`payload_len`, keep reading until the frame is complete, then decode and
advance. The record layout decodes as six 8-byte fields, three 8-byte
pairs, and three 4-byte words — in Python's `struct` spelling,
`">QqqqqqqIII"`. Track the last seen `update_seq` per actor to measure
freshness and loss; a record whose sequence does not advance over
several passes signals a stalled publisher rather than a missed frame.
Handle the membership-event kinds as state transitions rather than
display records: enter and exit bracket a subject's presence in the view
set, the crowd pair brackets the aggregate, and vanish means the subject
is gone from every tracked cell.
