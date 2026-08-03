/* Public handle, series registry, and recording path for the metrics module:
 * counter, gauge, and histogram series keyed by (name, kind, sorted labels),
 * atomic recording, and snapshot accessors consumed by the renders
 * (render/otlp.c, render/prometheus.c). */

#include "kith/metrics/metrics.h"

#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "kith/types.h"
#include "kith/version.h"
#include "metrics/registry.h"
#include "metrics/render/otlp.h"
#include "metrics/render/prometheus.h"

const uint64_t kith_hist_bounds[KITH_HIST_BOUNDS_COUNT] = {
    1,
    2,
    5,
    10,
    20,
    50,
    100,
    200,
    500,
    1000,
    2000,
    5000,
};

static int label_cmp(const void *a, const void *b)
{
    const struct label_slot *la = a;
    const struct label_slot *lb = b;
    return strcmp(la->key, lb->key);
}

/* Count caller labels with a non-NULL key; the canonicalized stored set
 * skips NULL-key labels so two calls that differ only by a NULL-key entry
 * hit the same series. */
static size_t valid_label_count(const kith_metrics_label_t *labels, size_t label_count)
{
    size_t n = 0;
    for (size_t i = 0; i < label_count; ++i)
    {
        if (labels[i].key != nullptr)
        {
            n += 1;
        }
    }
    return n;
}

/* Compare a stored (sorted) label set against a caller-supplied borrowed
 * set. Order does not matter on the caller side: the stored set is sorted
 * and this linear-scans it for each caller key. NULL-key caller labels are
 * skipped on both the count and the scan so a caller that passes one still
 * hits the series its remaining labels name. Returns true on a full
 * key/value match. */
static bool labels_match(const struct label_slot *stored,
                         size_t stored_count,
                         const kith_metrics_label_t *caller,
                         size_t caller_count)
{
    if (stored_count != valid_label_count(caller, caller_count))
    {
        return false;
    }
    for (size_t i = 0; i < caller_count; ++i)
    {
        if (caller[i].key == nullptr)
        {
            continue;
        }
        bool found = false;
        for (size_t j = 0; j < stored_count; ++j)
        {
            if (strcmp(stored[j].key, caller[i].key) == 0 &&
                strcmp(stored[j].value != nullptr ? stored[j].value : "",
                       caller[i].value != nullptr ? caller[i].value : "") == 0)
            {
                found = true;
                break;
            }
        }
        if (!found)
        {
            return false;
        }
    }
    return true;
}

static bool series_match(const struct series *s,
                         const char *name,
                         kith_metrics_kind_t kind,
                         const kith_metrics_label_t *labels,
                         size_t label_count)
{
    return s->kind == kind && strcmp(s->name, name) == 0 &&
           labels_match(s->labels, s->label_count, labels, label_count);
}

static int copy_labels(const kith_allocator_t *allocator,
                       struct label_slot *out,
                       const kith_metrics_label_t *labels,
                       size_t label_count)
{
    size_t out_i = 0;
    for (size_t i = 0; i < label_count; ++i)
    {
        if (labels[i].key == nullptr)
        {
            continue;
        }
        char *k = kith_strdup(allocator, labels[i].key);
        char *v = kith_strdup(allocator, labels[i].value != nullptr ? labels[i].value : "");
        if (k == nullptr || v == nullptr)
        {
            kith_free(allocator, k);
            kith_free(allocator, v);
            for (size_t j = 0; j < out_i; ++j)
            {
                kith_free(allocator, out[j].key);
                kith_free(allocator, out[j].value);
            }
            return kith_error_return(KITH_ENOMEM);
        }
        out[out_i].key = k;
        out[out_i].value = v;
        out_i += 1;
    }
    qsort(out, out_i, sizeof(*out), label_cmp);
    return 0;
}

static int ensure_series_cap(kith_metrics_t *m)
{
    if (m->series_count < m->series_cap)
    {
        return 0;
    }
    size_t next_cap = m->series_cap == 0 ? 16 : m->series_cap * 2;
    struct series *next = kith_realloc(m->allocator, m->series, next_cap * sizeof(*next));
    if (next == nullptr)
    {
        return kith_error_return(KITH_ENOMEM);
    }
    m->series = next;
    m->series_cap = next_cap;
    return 0;
}

/* Create a new series of @p kind with a zeroed value. The caller holds
 * @p m->lock. Returns 0 and sets @p *out_idx on success. */
static int series_create(kith_metrics_t *m,
                         kith_metrics_kind_t kind,
                         const char *name,
                         const kith_metrics_label_t *labels,
                         size_t label_count,
                         size_t *out_idx)
{
    if (ensure_series_cap(m) != 0)
    {
        return kith_error_return(KITH_ENOMEM);
    }
    struct series *s = &m->series[m->series_count];
    s->name = kith_strdup(m->allocator, name);
    if (s->name == nullptr)
    {
        return kith_error_return(KITH_ENOMEM);
    }
    s->kind = kind;
    s->labels = nullptr;
    s->label_count = 0;
    s->counter = 0;
    s->gauge = 0;
    s->hist_sum = 0;
    s->hist_count = 0;
    memset(s->hist_buckets, 0, sizeof(s->hist_buckets));
    const size_t n = valid_label_count(labels, label_count);
    if (n != 0)
    {
        s->labels = kith_alloc_zero(m->allocator, n, sizeof(*s->labels));
        if (s->labels == nullptr)
        {
            kith_free(m->allocator, s->name);
            s->name = nullptr;
            return kith_error_return(KITH_ENOMEM);
        }
        const int rc = copy_labels(m->allocator, s->labels, labels, label_count);
        if (rc != 0)
        {
            kith_free(m->allocator, s->name);
            kith_free(m->allocator, s->labels);
            s->name = nullptr;
            s->labels = nullptr;
            return rc;
        }
    }
    s->label_count = n;
    *out_idx = m->series_count;
    m->series_count += 1;
    return 0;
}

/* Find or create a series of @p kind for @p name + @p labels. On a miss the
 * series is created with a zeroed value. The caller must already hold
 * @p m->lock. Returns 0 and sets @p *out_idx to the series index on
 * success, negative kith_error on failure. */
static int kith_metrics_find_or_create(kith_metrics_t *m,
                                       kith_metrics_kind_t kind,
                                       const char *name,
                                       const kith_metrics_label_t *labels,
                                       size_t label_count,
                                       size_t *out_idx)
{
    for (size_t i = 0; i < m->series_count; ++i)
    {
        if (series_match(&m->series[i], name, kind, labels, label_count))
        {
            *out_idx = i;
            return 0;
        }
    }
    return series_create(m, kind, name, labels, label_count, out_idx);
}

static void destroy_series(const kith_allocator_t *allocator, struct series *s)
{
    kith_free(allocator, s->name);
    for (size_t j = 0; j < s->label_count; ++j)
    {
        kith_free(allocator, s->labels[j].key);
        kith_free(allocator, s->labels[j].value);
    }
    kith_free(allocator, s->labels);
}

int kith_metrics_create(const kith_metrics_params_t *params,
                        const kith_allocator_t *alloc,
                        kith_metrics_t **out_metrics)
{
    if (out_metrics == nullptr)
    {
        return kith_error_return(KITH_EINVAL);
    }
    *out_metrics = nullptr;

    if (params != nullptr)
    {
        if (params->abi_version != KITH_ABI_VERSION)
        {
            return kith_error_return(KITH_EABIVER);
        }
        if (params->size < sizeof(*params))
        {
            return kith_error_return(KITH_ESIZE);
        }
    }
    if (alloc != nullptr)
    {
        const kith_error_t alloc_rc = kith_allocator_check(alloc);
        if (alloc_rc != 0)
        {
            return kith_error_return(alloc_rc);
        }
    }

    const kith_allocator_t *allocator = alloc != nullptr ? alloc : kith_allocator_default();
    kith_metrics_t *m = kith_alloc(allocator, sizeof(*m));
    if (m == nullptr)
    {
        return kith_error_return(KITH_ENOMEM);
    }
    m->allocator = allocator;
    m->prefix = nullptr;
    m->series = nullptr;
    m->series_count = 0;
    m->series_cap = 0;
    if (pthread_mutex_init(&m->lock, nullptr) != 0)
    {
        kith_free(m->allocator, m);
        return kith_error_return(KITH_ESTATE);
    }
    if (params != nullptr && params->prefix != nullptr)
    {
        m->prefix = kith_strdup(m->allocator, params->prefix);
        if (m->prefix == nullptr)
        {
            kith_metrics_destroy(m);
            return kith_error_return(KITH_ENOMEM);
        }
    }
    *out_metrics = m;
    return 0;
}

void kith_metrics_destroy(kith_metrics_t *metrics)
{
    if (metrics == nullptr)
    {
        return;
    }
    kith_metrics_t *m = metrics;
    for (size_t i = 0; i < m->series_count; ++i)
    {
        destroy_series(m->allocator, &m->series[i]);
    }
    kith_free(m->allocator, m->series);
    kith_free(m->allocator, m->prefix);
    (void)pthread_mutex_destroy(&m->lock);
    kith_free(m->allocator, m);
}

static int validate_observation(kith_metrics_t *metrics,
                                const char *name,
                                const kith_metrics_label_t *labels,
                                size_t label_count)
{
    if (metrics == nullptr || name == nullptr)
    {
        return kith_error_return(KITH_EINVAL);
    }
    if (labels == nullptr && label_count != 0)
    {
        return kith_error_return(KITH_EINVAL);
    }
    return 0;
}

static void hist_observe(struct series *s, uint64_t value)
{
    s->hist_count += 1;
    s->hist_sum += value;
    for (size_t i = 0; i < KITH_HIST_BOUNDS_COUNT; ++i)
    {
        if (value <= kith_hist_bounds[i])
        {
            s->hist_buckets[i] += 1;
            return;
        }
    }
    s->hist_buckets[KITH_HIST_BOUNDS_COUNT] += 1;
}

int kith_metrics_counter_add(kith_metrics_t *metrics,
                             const char *name,
                             const kith_metrics_label_t *labels,
                             size_t label_count,
                             uint64_t delta)
{
    const int vr = validate_observation(metrics, name, labels, label_count);
    if (vr != 0)
    {
        return vr;
    }
    if (pthread_mutex_lock(&metrics->lock) != 0)
    {
        return kith_error_return(KITH_ESTATE);
    }
    size_t idx = 0;
    const int rc = kith_metrics_find_or_create(
        metrics, KITH_METRICS_KIND_COUNTER, name, labels, label_count, &idx);
    if (rc == 0)
    {
        metrics->series[idx].counter += delta;
    }
    (void)pthread_mutex_unlock(&metrics->lock);
    return rc;
}

int kith_metrics_gauge_set(kith_metrics_t *metrics,
                           const char *name,
                           const kith_metrics_label_t *labels,
                           size_t label_count,
                           int64_t value)
{
    const int vr = validate_observation(metrics, name, labels, label_count);
    if (vr != 0)
    {
        return vr;
    }
    if (pthread_mutex_lock(&metrics->lock) != 0)
    {
        return kith_error_return(KITH_ESTATE);
    }
    size_t idx = 0;
    const int rc = kith_metrics_find_or_create(
        metrics, KITH_METRICS_KIND_GAUGE, name, labels, label_count, &idx);
    if (rc == 0)
    {
        metrics->series[idx].gauge = value;
    }
    (void)pthread_mutex_unlock(&metrics->lock);
    return rc;
}

int kith_metrics_histogram_observe(kith_metrics_t *metrics,
                                   const char *name,
                                   const kith_metrics_label_t *labels,
                                   size_t label_count,
                                   uint64_t value)
{
    const int vr = validate_observation(metrics, name, labels, label_count);
    if (vr != 0)
    {
        return vr;
    }
    if (pthread_mutex_lock(&metrics->lock) != 0)
    {
        return kith_error_return(KITH_ESTATE);
    }
    size_t idx = 0;
    const int rc = kith_metrics_find_or_create(
        metrics, KITH_METRICS_KIND_HISTOGRAM, name, labels, label_count, &idx);
    if (rc == 0)
    {
        hist_observe(&metrics->series[idx], value);
    }
    (void)pthread_mutex_unlock(&metrics->lock);
    return rc;
}

size_t kith_metrics_render_prometheus(const kith_metrics_t *metrics, char *buf, size_t cap)
{
    if (metrics == nullptr)
    {
        return 0;
    }
    kith_metrics_t *m = (kith_metrics_t *)metrics;
    if (pthread_mutex_lock(&m->lock) != 0)
    {
        return 0;
    }
    const size_t n = kith_render_prometheus(m, buf, cap);
    (void)pthread_mutex_unlock(&m->lock);
    return n;
}

size_t kith_metrics_export_otlp(const kith_metrics_t *metrics, char *buf, size_t cap)
{
    if (metrics == nullptr)
    {
        return 0;
    }
    struct timespec ts;
    if (clock_gettime(CLOCK_REALTIME, &ts) != 0)
    {
        return 0;
    }
    const uint64_t ns = (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
    kith_metrics_t *m = (kith_metrics_t *)metrics;
    if (pthread_mutex_lock(&m->lock) != 0)
    {
        return 0;
    }
    const size_t n = kith_render_otlp(m, buf, cap, ns);
    (void)pthread_mutex_unlock(&m->lock);
    return n;
}
