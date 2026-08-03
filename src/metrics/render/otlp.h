#pragma once

#include <stddef.h>
#include <stdint.h>

#include "kith/metrics/metrics.h"

/* Serialize the registry as an OpenTelemetry OTLP/JSON metrics payload. The
 * caller must hold @p m->lock for the duration of the call so the series
 * array is stable. @p time_unix_nano is the observation timestamp applied
 * to every data point (start_time_unix_nano is set equal to it for the
 * cumulative temporality the registry emits). Returns the byte count the
 * payload occupies, not counting a NUL terminator; @p buf receives the
 * leading @p cap bytes (snprintf-style) when non-NULL. */
KITH_LOCAL size_t kith_render_otlp(const kith_metrics_t *m,
                                   char *buf,
                                   size_t cap,
                                   uint64_t time_unix_nano);
