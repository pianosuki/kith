#pragma once

#include <stddef.h>

#include "kith/metrics/metrics.h"

/* Serialize the registry as a Prometheus text-exposition payload. The
 * caller must hold @p m->lock for the duration of the call so the series
 * array is stable. Returns the byte count the payload occupies, not
 * counting a NUL terminator; @p buf receives the leading @p cap bytes
 * (snprintf-style) when non-NULL. */
KITH_LOCAL size_t kith_render_prometheus(const kith_metrics_t *m, char *buf, size_t cap);
