# ADR-0019: One observability contract across all libraries

**Status:** Accepted

## Context

Each library with its own opinions about logging, metrics, and tracing would
make production behavior impossible to correlate. Every module must emit
observability data through a single contract so operators can trace one
request across the sim, fabric, gateway, and coordination planes, and so log
queries and dashboards stay consistent.

## Decision

All kith libraries share one observability contract. Logging is structured
through the logging library: entries carry a severity level, a short
human-readable message, and caller-supplied key/value fields, written to an
optional JSONL side-channel file. Metrics go through
the metrics library: a named registry of counters, gauges, and histograms —
series identified by caller-chosen names and label sets — rendered by
registered serializers; Prometheus text and OTLP/JSON ship
with the library. Neither
the logger nor the registry is a singleton: each handle is owned by the
composition root and passed in by pointer, with no global accessor. Tracing is
correlation-first (ADR-0007): a call site that has a correlation ID in scope
passes it explicitly — as the correlation-ID caller field on a log entry
(the convention `docs/event_schema.md` defines), as a caller-supplied label
on a metric observation, or in the correlation
slot of a published control event record — and those carriers generalize
into OpenTelemetry spans. Every module logs and records metrics only through
this
contract, so a single perimeter captures the whole runtime.

## Consequences

Positive — uniform observability and a single, navigable picture of any path
through the fabric; correlation IDs (ADR-0007) tie the log entries, metric
series, and event records that carry them to one observation. Negative —
framework overhead (handles passed by pointer,
the JSONL and OTLP serializers), accepted for the operational visibility it
provides.
