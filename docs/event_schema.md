# Event Schema

The framework's event surfaces are line-oriented UTF-8 JSON: one JSON object
per record, compact (no pretty-printing), newline-terminated. The record
shape is per carrier — three carriers, three distinct records:

- The **log side-channel** — the structured logger's optional JSONL file
  sink (`include/kith/logger/logger.h`). One record per log entry.
- The **control event bus** — an in-memory delivery ring on the control
  plane, published through `kith_control_publish_event` and drained to
  stream subscribers as server-sent events.
- The **agentic headless client IPC bus** — a Unix-socket JSON channel used
  by `tools/agent/` to drain per-client events during closed-loop testing
  (see `docs/guides/agentic_headless_client.md`).

## Wire-protocol correlation ID trailer

Correlation IDs are carried as an optional 8-byte trailer appended to the
wire frame, defined by ADR-0007:

- `KITH_PROTO_FLAG_CORRELATION` (0x02) in the frame header flags enables the
  trailer.
- The trailer is an 8-byte big-endian correlation ID occupying the last 8
  bytes of the payload region; it is counted in `payload_len` and the codec
  strips it on decode.
- Backward-compatible: a frame without the flag carries no trailer.
- The trailer rides the upstream direction: client-submitted frames may
  carry it; the gateway's own downstream frames — replication and the
  per-session deliver path — are encoded without it. Downstream correlation
  exists on the surfaces that explicitly support it: the control event bus
  record carries an optional correlation ID, and the headless client
  preserves the correlation ID of every decoded frame in its event history.

A correlation ID is a 64-bit unsigned integer. When printed in JSON it appears
as a decimal or a 16-character lowercase hex string (the hex form is preferred
for readability and to avoid lossy float parsing in some JSON tooling). Both
forms are accepted on the read side.

## Carrier records

### The log side-channel record

Each entry the logger writes to its JSONL sink is one JSON object:

| Field | Type | Present | Meaning |
|---|---|---|---|
| `ts` | string (ISO 8601, UTC, `Z` suffix) | always | Entry timestamp, taken from the calendar clock. |
| `level` | string | always | One of `trace`, `debug`, `info`, `warn`, `error`, lowercase. |
| `module` | string | always | The logger's name — the emitting module. |
| `file`, `line` | string, integer | together, when the call site passes a file | The call site location. |
| `message` | string | when the call site passes one | A short human-readable summary. ASCII, English. |

Every other top-level key is a caller-supplied field, emitted as a string
value under the caller's key. The reserved names are exactly `ts`, `level`,
`module`, `file`, `line`, and `message`; a caller field whose key collides
with one is dropped so the record stays single-key. Any other key is emitted
verbatim. Consumers must tolerate unknown top-level keys: caller fields are
the record's extension point, and new reserved names or field conventions
appear additively.

### The control event bus record

`kith_control_publish_event` publishes one record per call:

| Field | Type | Meaning |
|---|---|---|
| `ts_mono_ns` | integer | Publish timestamp, `CLOCK_MONOTONIC` nanoseconds. |
| `type` | string | The event type, a free-form string (see the type namespace below). |
| `correlation_id` | 8 bytes | Optional. The correlation ID in scope at publish. |
| `payload` | bytes | Optional. Caller payload, truncated to the bus's per-record capacity if longer. |

The bus is a delivery ring, not a history log: a new stream subscriber
receives the events published after it attached. The control plane serves
two stream routes over the same bus, `GET /events/stream` and
`GET /logs/stream`; both render each record as one SSE `data:` line carrying
`type` and `ts` only — the correlation ID and payload ride the ring record
but are not rendered. Neither route streams the log side-channel: the
logger's JSONL sink is a file, and an operator who wants log records on the
control plane publishes them through the bus.

### The headless client event record

The agentic headless client emits, per client event:

| Field | Type | Meaning |
|---|---|---|
| `ts_mono_ns` | integer | Observation timestamp, `CLOCK_MONOTONIC` nanoseconds. |
| `type_id` | integer | The frame's registered message type id. |
| `correlation_id` | integer | The correlation ID decoded from the frame's trailer, or `0` when the frame carries none. |
| `payload_hex` | string | The frame payload, hex-encoded. |

## The `type` namespace

`kith_control_publish_event` takes a free-form `type` string. The framework
reserves the module namespaces below so publishers — games, operations
tooling, future framework code — share one vocabulary. The framework itself
publishes no events today; this registry is the naming contract for the
publishers that do. Games register game-specific names (prefixed with the
game's own namespace) at startup; game-specific names do not appear in this
document, and core names carry no game-specific vocabulary.

Core names:

| `module` | `type` | Meaning |
|---|---|---|
| `server` | `lifecycle.start` | Server entered the run loop. |
| `server` | `lifecycle.shutdown` | Server began shutdown. |
| `net` | `connection.accept` | A TCP connection was accepted. |
| `net` | `connection.close` | A TCP connection closed. |
| `gateway` | `session.bootstrap` | A client completed bootstrap. |
| `gateway` | `session.select` | A client selected an actor (session establishment). |
| `gateway` | `view.compose` | The relevance composer built a send set for a player. |
| `gateway` | `view.deliver` | A send set was flushed to a connection. |
| `sim` | `actor.spawn` | An actor entered authoritative state. |
| `sim` | `actor.despawn` | An actor left authoritative state. Not used for handoff in normal same-zone migration (ADR-0003). |
| `sim` | `actor.move` | An actor's authoritative position changed. |
| `sim` | `publish.artifact` | An immutable per-cell publish artifact was emitted. |
| `fabric` | `stream.append` | A cell product was appended to a cell stream. |
| `fabric` | `stream.supersede` | A newer cell product superseded an older one. |
| `fabric` | `subscribe` | A gateway subscribed to a cell stream. |
| `fabric` | `unsubscribe` | A gateway unsubscribed from a cell stream. |
| `coord` | `ownership.assign` | A cell's authority was assigned. |
| `coord` | `ownership.handoff.begin` | A split/merge began for a cell (overlapping handoff). |
| `coord` | `ownership.handoff.done` | The overlap window closed and the new authority is sole publisher. |
| `control` | `command` | A command was injected over the control plane. |

## Cross-referencing

`tools/agent/event_correlator.py` is a synchronous, time-windowed index that
joins client-side and server-side events by correlation ID and time. The
orchestrator feeds it from the headless clients' event histories, where each
event carries the correlation ID decoded from its frame's trailer; a
server-side feed can be added from a control-plane endpoint when a
deployment publishes correlated events. The join fires on the surfaces that
carry the ID — a deployment that wants a server input traced to its
server-side effects passes the ID through those surfaces explicitly. This is
the same join that backs the agentic closed-loop assertions (see
`docs/guides/agentic_headless_client.md`).

Correlation IDs generalize into OpenTelemetry spans for production
distributed tracing; the mapping is documented in
`docs/architecture/tracing.md`.

## Logging correlation

The logger has no correlation parameter. A call site that has a correlation
ID in scope passes it as a caller field named `correlation_id`, and the
logger emits it as a top-level string key like any caller field (the name is
not reserved). The hex print rule above is the convention for the field's
value. Framework modules pass no correlation fields today; the convention
binds the call sites that hold an ID. Log lines carry no game-specific
vocabulary in core modules (see `AGENTS.md` §1.4).

## References

- ADR-0006 — agentic testing is first-class.
- ADR-0007 — correlation ID wire trailer.
- ADR-0019 — observability contract (structured logging, the JSONL
  side-channel).
- `include/kith/control/control.h` — the event bus record and the publish
  API.
- `docs/guides/agentic_headless_client.md` — the IPC bus that consumes
  the headless client record.
- `docs/architecture/tracing.md` — OpenTelemetry span mapping.
