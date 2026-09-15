# Tracing

The framework's tracing is correlation-first
(ADR-0007, ADR-0019): a 64-bit correlation ID rides an optional wire-frame
trailer upstream, and the surfaces that carry it into observability do so
explicitly — a log entry's `correlation_id` caller field, a metric series
keyed by a caller-supplied label, a control event bus record published with
the ID. The correlation ID is the single join key that ties a client
observation to the server-side work it caused on the surfaces that carry it.

The contract is defined here as a mapping. The framework ships the pieces
the mapping is built from — the wire trailer, the event records of its three
carriers, the in-process correlator, and the OTLP/JSON metrics exporter —
and an operator wires them into an OpenTelemetry pipeline by following the
mapping below. The framework does not ship an OpenTelemetry span exporter;
the mapping is the contract an exporter follows.

## The correlation ID

A correlation ID is a 64-bit unsigned integer carried as an optional 8-byte
big-endian trailer appended to the wire frame (ADR-0007):

- `KITH_PROTO_FLAG_CORRELATION` (0x02) in the frame header flags enables the
  trailer.
- The trailer is 8 bytes, big-endian, occupying the last 8 bytes of the
  payload region (counted in `payload_len`; the codec strips it on decode).
- A frame without the flag carries no trailer; the format is
  backward-compatible.
- The trailer rides the upstream direction: client-submitted frames may
  carry it; the gateway's own downstream frames are encoded without it.
  Downstream correlation exists on the surfaces that explicitly support it
  (see `docs/event_schema.md`).

A correlation ID is set by the client on an originating input frame. When
printed in JSON it appears as a decimal or a 16-character lowercase hex
string (the hex form is preferred for readability and to avoid lossy float
parsing in some JSON tooling); both forms are accepted on read. The value
`0` means no correlation in scope.

## The event records

The tracing mapping consumes the framework's event records — the three
carriers defined in `docs/event_schema.md`: the log side-channel, the
control event bus, and the headless client IPC bus. Each carrier has its
own record shape; the fields the mapping relies on:

| Field | Carrier | Role in the mapping |
|---|---|---|
| `correlation_id` | a `correlation_id` caller field on a log entry; the control bus record; the headless client record | The correlation ID in scope, wherever the carrier carries one. The join key across the client, the server, and the logs. |
| `ts` | log record | Entry timestamp, ISO 8601, UTC, `Z` suffix. The ordering key for entries that share a correlation ID. |
| `ts_mono_ns` | control bus and headless client records | Monotonic observation timestamp; orders records within one clock domain. |
| `level`, `message` | log record | Severity and summary; map to the OTel log-record severity and body. |
| `module` | log record | The emitting module. Maps to an OTel span attribute. |
| `type` / `type_id` | control bus / headless client records | The event's type. Maps to a span event name. |
| caller fields | log record | Caller-supplied key/value pairs. Map to OTel log-record attributes. |

Unknown top-level fields on a record are ignored on read, so the records are
additively extensible without breaking the mapping.

## Three event carriers

Three surfaces carry the records (`docs/event_schema.md`):

- The **log side-channel** — the structured logger's JSONL file sink
  (`include/kith/logger/logger.h`). One record per log entry.
- The **control event bus** — `kith_control_publish_event` feeds the ring;
  the control plane's stream routes render its records as server-sent
  events. The framework publishes no events itself today.
- The **agentic headless client IPC bus** — a Unix-socket JSON channel
  `tools/agent/` drains per-client events through during closed-loop
  testing (`docs/guides/agentic_headless_client.md`).

## The in-process correlator

`tools/agent/event_correlator.py` is a synchronous, time-windowed index that
cross-references client-side and server-side events by correlation ID and
time. The orchestrator feeds it client events (from the headless clients)
and, when a host configures a server-side events endpoint, server events
from that endpoint. Scenario steps and assertions query the correlator to
verify that a client request with correlation ID `N` produced the expected
server-side effect, in time order. This is the same join that backs the
agentic closed-loop assertions.

The correlator is pure in-memory with no I/O; ingestion and queries are
synchronous, and the async boundary is at the orchestrator. A configurable
time window evicts old events so a long-lived correlator has bounded memory.
It is the testing-time consumer of the correlation contract; the
OpenTelemetry mapping below is the production-time consumer.

## Mapping to OpenTelemetry

The correlation-first model maps onto OpenTelemetry traces and spans without
losing the records' structure. The mapping is defined per field; an
operator implements it by feeding the log side-channel (the JSONL file) and
any correlated event streams a deployment publishes into a collector that
translates each JSON record into OTel spans and span events.

### Trace and span identity

A correlation ID is the trace context. A single client input with
correlation ID `N` is one trace; every record that carries `N` is a span (or
a span event on a parent span) within that trace.

| kith | OpenTelemetry |
|---|---|
| `correlation_id` (64-bit) | The low 8 bytes of a 16-byte W3C `trace-id` (the high 8 bytes are zero-padded), or a custom trace identifier the collector assigns while recording `correlation_id` as a span attribute. The latter preserves the exact value across tooling that narrows 128-bit trace IDs. |
| one originating input frame | one trace, rooted at the span for the first record that carries its correlation ID. |
| each record with the same `correlation_id` | a span within that trace (a plane transition recorded under the same correlation ID opens a child span under the parent). |
| ordering by timestamp | span start time and span-event timestamps. |

### Span attributes

Every structural field of a record maps to an OTel span attribute. The
names below are the record field names; an operator can rename them at
the collector without losing the structure, but keeping the record names
makes the JSON records and the OTel spans cross-referenceable by grep.

| Record field | OTel attribute |
|---|---|
| `module` | `kith.module` |
| caller fields | flattened under the `kith.` namespace |

### Span events and names

A log entry (a record with a `level` and a `message`) maps to an OTel span
event on the span for its correlation ID, with the `message` as the event
name and its caller fields as event attributes. A registry-typed event
published on the control bus maps to a child span whose name is the record's
`type`, started at the record's timestamp.

### Logs and the OTel logs signal

The log side-channel is the framework's structured log stream. An operator
feeding it into an OpenTelemetry logs pipeline maps each record to an OTel
log record: the `level` maps to the OTel severity, the `message` to the
body, and the caller fields to attributes. A `correlation_id` caller field,
when the site passes one, ties the log record to its trace, so a trace's
span view and log view join on the same key.

### Metrics

Metrics go through the metrics library (`include/kith/metrics/metrics.h`),
serialized as Prometheus text (`kith_metrics_render_prometheus`) or
OpenTelemetry OTLP/JSON (`kith_metrics_export_otlp`). The OTLP/JSON payload
is one `resource_metrics` → `scope_metrics` → `metrics` object where
counters become `sum` (monotonic, cumulative temporality), gauges become
`gauge`, and histograms become `histogram` with `explicit_bounds`,
`bucket_counts`, `sum`, and `count`. Each data point carries the observation
timestamp as `time_unix_nano` and its labels as `attributes`.

A correlation ID reaches the metrics the same way every label does: the
caller passes `correlation_id` as a label key (a `kith_metrics_label_t`) at
the observation call, so the OTLP `attributes` of the data point include it.
A metric series keyed by
correlation ID is high-cardinality; the caller scopes it to debugging or
per-request observations, and uses plane- or zone-keyed series for the
steady-state dashboard. The OTLP/JSON export is a metrics signal, not a
traces signal; it carries the correlation ID as an attribute, not as a
trace context.

## What ships and what an operator wires

The framework ships:

- The wire-frame correlation trailer (ADR-0007) on the upstream direction.
- The event records of its three carriers (`docs/event_schema.md`).
- The JSONL logging side-channel (ADR-0019, `include/kith/logger/logger.h`).
- The in-process correlator (`tools/agent/event_correlator.py`) for
  testing.
- The OTLP/JSON metrics exporter (`kith_metrics_export_otlp`).

The framework does not ship:

- An OpenTelemetry span exporter. The mapping above is the contract an
  exporter follows; the operator feeds the log side-channel (and any
  correlated event streams a deployment publishes) into a collector that
  emits OTel spans and span events.
- A W3C trace-context propagation on the wire. The wire carries the
  8-byte correlation trailer; the collector maps it to a 16-byte
  `trace-id` (or records it as an attribute) per the table above.

The split keeps the wire format compact (8 bytes per flagged frame, no
trace-state header) and the framework free of an OTel SDK dependency, while
preserving the single join key — the correlation ID — that ties a client
observation to the records and metrics that carry its ID and answer to one
trace.

## References

- `docs/event_schema.md` — the carrier records, the `type` namespace, and
  the cross-referencing contract.
- `docs/guides/agentic_headless_client.md` — the IPC bus that
  consumes the headless client record in closed-loop testing.
- `include/kith/metrics/metrics.h` — the metrics registry and the
  Prometheus / OTLP/JSON serializers.
- `include/kith/logger/logger.h` — the structured logger and the JSONL
  side-channel.
- `tools/agent/event_correlator.py` — the in-process correlator.
- ADR-0007 — the optional 8-byte wire-protocol correlation trailer.
- ADR-0019 — the single observability contract (logs, metrics, tracing)
  the mapping follows.
