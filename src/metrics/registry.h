#pragma once

#include <stddef.h>
#include <stdint.h>

#include <pthread.h>

#include "kith/metrics/metrics.h"
#include "kith/types.h"

/* Internal registry model shared by the implementation and the two
 * serialization modules (render/prometheus.c, render/otlp.c). This header
 * is private to the metrics module; nothing outside src/metrics/ includes
 * it. The render modules read the registry under the handle's lock, so
 * every field here is stable for the lifetime of the handle. */

/* Number of finite histogram boundaries. A histogram series stores
 * KITH_HIST_BOUNDS_COUNT finite buckets plus one +Inf overflow bucket. */
#define KITH_HIST_BOUNDS_COUNT 12

/* Default finite histogram boundaries. A value <= bounds[i] lands in
 * bucket i; a value above the last boundary lands in the overflow bucket.
 * Per-metric bucket configuration is an additive (non-breaking) evolution. */
extern const uint64_t kith_hist_bounds[KITH_HIST_BOUNDS_COUNT];

/* An owned label pair stored on a series. Copied from a caller's borrowed
 * kith_metrics_label_t at series creation, then freed on destroy. */
struct label_slot
{
    char *key;
    char *value;
};

/* One metric series. A series is identified by (name, kind, sorted labels).
 * The value storage is a union over kinds; only the field matching @p kind
 * is meaningful. */
struct series
{
    char *name;
    kith_metrics_kind_t kind;
    struct label_slot *labels;
    size_t label_count;
    uint64_t counter;
    int64_t gauge;
    uint64_t hist_buckets[KITH_HIST_BOUNDS_COUNT + 1];
    uint64_t hist_sum;
    uint64_t hist_count;
};
struct kith_metrics
{
    const kith_allocator_t *allocator;
    char *prefix;
    struct series *series;
    size_t series_count;
    size_t series_cap;
    pthread_mutex_t lock;
};
