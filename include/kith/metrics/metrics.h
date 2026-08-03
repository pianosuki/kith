#ifndef KITH_METRICS_METRICS_H
#define KITH_METRICS_METRICS_H

#include <stddef.h>
#include <stdint.h>

#include "kith/api.h"
#include "kith/types.h"

/**
 * Generic named metric registry with Prometheus and OTLP serialization.
 *
 * A registry owns a set of named metric series identified by a metric name
 * and a sorted label set. Three kinds are supported: a counter (monotonic
 * unsigned total), a gauge (a settable signed value), and a histogram (a
 * fixed-boundary bucket distribution plus a sum and a count). Callers
 * register series implicitly by observing them: a counter or histogram
 * observation for an unknown name/label pair creates the series on first
 * use, and a gauge set does the same.
 *
 * The handle is owned by the composition root and passed by pointer to
 * each module that records metrics; there is no global accessor. Registry
 * operations and serialization reads are serialized by a per-handle mutex,
 * so concurrent calls on the same handle do not interleave. Lifecycle calls
 * (create/destroy) must not race with each other or with an in-flight
 * observation or render.
 *
 * The library produces two text serializations of the registry into a
 * caller-owned buffer: a Prometheus text-exposition payload
 * (kith_metrics_render_prometheus) and an OpenTelemetry OTLP/JSON metrics
 * payload (kith_metrics_export_otlp). The caller owns the transport that
 * ships either payload; the library performs no network I/O.
 */

/**
 * @defgroup kith_metrics Metrics
 * @{
 */

/**
 * Metric kinds. The underlying type is fixed so a series kind stored in an
 * ABI surface stays a fixed width.
 */
enum kith_metrics_kind : unsigned int
{
    KITH_METRICS_KIND_COUNTER = 0u,
    KITH_METRICS_KIND_GAUGE = 1,
    KITH_METRICS_KIND_HISTOGRAM = 2,
};

/** Alias of enum kith_metrics_kind. */
typedef enum kith_metrics_kind kith_metrics_kind_t;

/**
 * A single label key/value pair on a metric series. Both @p key and
 * @p value are NUL-terminated UTF-8 strings borrowed for the duration of
 * the observation call that receives them; the registry copies them when
 * it creates a new series, so the caller may reuse or free the storage
 * after the call returns. Callers commonly pass a compound-literal array:
 *
 *   kith_metrics_counter_add(m, "conn_accepted",
 *       &(kith_metrics_label_t[]){{"transport", "tcp"}}, 1, 1);
 *
 * This is an exposed-layout value type (like struct iovec and
 * kith_logger_field_t), not an opaque handle: the two-pointer pair is part of
 * the public contract and stays stable.
 */
struct kith_metrics_label
{
    /** Label key; borrowed for the duration of the observation call. */
    const char *key;
    /** Label value; borrowed for the duration of the observation call. */
    const char *value;
};

/** Alias of struct kith_metrics_label. */
typedef struct kith_metrics_label kith_metrics_label_t;

/**
 * Opaque metric registry handle.
 *
 * @ownership callee — created by kith_metrics_create, destroyed by
 *           kith_metrics_destroy. The handle owns every series in the
 *           registry and the mutex that serializes access; all owned
 *           storage is released on destroy.
 */
typedef struct kith_metrics kith_metrics_t;

/**
 * Creation parameters. Size-versioned: callers set @p size to
 * sizeof(kith_metrics_params_t) and @p abi_version to KITH_ABI_VERSION at
 * their compile time; the runtime rejects structs from an incompatible
 * generation or an undersized size. Future additive fields occupy the
 * reserved slots so the layout below stays stable across generations.
 */
struct kith_metrics_params
{
    /** Must be sizeof(kith_metrics_params_t). */
    uint32_t size;
    /** Must be KITH_ABI_VERSION (from kith/version.h). */
    uint32_t abi_version;

    /**
     * Optional name prefix prepended to every metric name in both
     * serializations (e.g. "kith_"). NULL means no prefix. The prefix is
     * copied at creation; the caller may free @p prefix after the call.
     */
    const char *prefix;

    /** Reserved for future additive fields. Must be zero-filled. */
    void *reserved[8];
};

/** Alias of struct kith_metrics_params. */
typedef struct kith_metrics_params kith_metrics_params_t;

/**
 * Build an empty metric registry from @p params.
 *
 * @param params       Creation parameters; @c size and @c abi_version must
 *                     match the runtime generation. NULL requests a
 *                     registry with no prefix and the default histogram
 *                     boundaries.
 * @param alloc        Allocator for the new handle, the prefix copy, and
 *                     every series the registry records, used again when
 *                     kith_metrics_destroy frees them. NULL selects the
 *                     default allocator; a supplied allocator is validated
 *                     (see kith_allocator_t) and must outlive the handle.
 * @param out_metrics  Receives the new handle on success.
 * @return             0 on success, negative kith_error on failure:
 *                     - -KITH_EINVAL if @p out_metrics is NULL, or @p alloc
 *                       is missing an operation,
 *                     - -KITH_EABIVER if @p params or @p alloc has an
 *                       incompatible abi_version,
 *                     - -KITH_ESIZE if @p params or @p alloc has an
 *                       undersized size,
 *                     - -KITH_ENOMEM on allocation failure,
 *                     - -KITH_ESTATE if the registry's mutex cannot be
 *                       initialized.
 * @thread_safety unsafe — must not be called concurrently with another
 *                kith_metrics_create on the same @p out_metrics slot.
 * @ownership callee — the returned handle is freed by the caller with
 *           kith_metrics_destroy.
 */
[[nodiscard]] KITH_API int kith_metrics_create(const kith_metrics_params_t *params,
                                               const kith_allocator_t *alloc,
                                               kith_metrics_t **out_metrics);

/**
 * Release a registry and every series it owns. Passing NULL is a no-op.
 *
 * @param metrics Registry handle. NULL is a no-op.
 * @thread_safety unsafe — no observation or render may be in flight on
 *                @p metrics when this is called.
 * @ownership callee — @p metrics is consumed and freed by the call.
 */
KITH_API void kith_metrics_destroy(kith_metrics_t *metrics);

/**
 * Add @p delta to a counter series, creating it on first observation. A
 * counter is a monotonic unsigned total; @p delta is added to the stored
 * value without overflow checking (callers wrap if they need it).
 *
 * @param metrics      Registry handle. NULL is accepted and drops the
 *                     observation.
 * @param name         Metric name, NUL-terminated, non-NULL. The registry
 *                     copies it on first use.
 * @param labels       Borrowed label array, or NULL if @p label_count is 0.
 *                     A label whose key is NULL is skipped. The registry
 *                     canonicalizes the set by key on first use, so two
 *                     observations whose labels differ only in order hit
 *                     the same series.
 * @param label_count  Number of labels at @p labels.
 * @param delta        Non-negative amount to add.
 * @return             0 on success, negative kith_error on failure:
 *                     - -KITH_EINVAL if @p name or @p metrics is NULL, or
 *                       @p labels is NULL but @p label_count is non-zero,
 *                     - -KITH_ENOMEM on allocation failure (new series).
 * @thread_safety safe — concurrent calls on the same handle are serialized.
 * @ownership caller — @p name and @p labels (and their strings) are
 *           borrowed for the call only and are not retained past it.
 */
[[nodiscard]] KITH_API int kith_metrics_counter_add(kith_metrics_t *metrics,
                                                    const char *name,
                                                    const kith_metrics_label_t *labels,
                                                    size_t label_count,
                                                    uint64_t delta);

/**
 * Set a gauge series to @p value, creating it on first observation. A
 * gauge is a settable signed value (it may go up or down).
 *
 * @param metrics      Registry handle. NULL is accepted and drops the
 *                     observation.
 * @param name         Metric name, NUL-terminated, non-NULL.
 * @param labels       Borrowed label array, or NULL if @p label_count is 0.
 * @param label_count  Number of labels at @p labels.
 * @param value        New gauge value.
 * @return             0 on success, negative kith_error on failure:
 *                     - -KITH_EINVAL if @p name or @p metrics is NULL, or
 *                       @p labels is NULL but @p label_count is non-zero,
 *                     - -KITH_ENOMEM on allocation failure (new series).
 * @thread_safety safe — concurrent calls on the same handle are serialized.
 * @ownership caller — @p name and @p labels are borrowed for the call only.
 */
[[nodiscard]] KITH_API int kith_metrics_gauge_set(kith_metrics_t *metrics,
                                                  const char *name,
                                                  const kith_metrics_label_t *labels,
                                                  size_t label_count,
                                                  int64_t value);

/**
 * Observe @p value into a histogram series, creating it on first
 * observation. A histogram accumulates a count and sum plus a count per
 * fixed boundary; a value above the last boundary lands in the +Inf
 * overflow bucket. The boundaries are the registry's default set; per-
 * metric bucket configuration is not available in this generation.
 *
 * @param metrics      Registry handle. NULL is accepted and drops the
 *                     observation.
 * @param name         Metric name, NUL-terminated, non-NULL.
 * @param labels       Borrowed label array, or NULL if @p label_count is 0.
 * @param label_count  Number of labels at @p labels.
 * @param value        Non-negative observation.
 * @return             0 on success, negative kith_error on failure:
 *                     - -KITH_EINVAL if @p name or @p metrics is NULL, or
 *                       @p labels is NULL but @p label_count is non-zero,
 *                     - -KITH_ENOMEM on allocation failure (new series).
 * @thread_safety safe — concurrent calls on the same handle are serialized.
 * @ownership caller — @p name and @p labels are borrowed for the call only.
 */
[[nodiscard]] KITH_API int kith_metrics_histogram_observe(kith_metrics_t *metrics,
                                                          const char *name,
                                                          const kith_metrics_label_t *labels,
                                                          size_t label_count,
                                                          uint64_t value);

/**
 * Serialize the registry as a Prometheus text-exposition payload into
 * @p buf.
 *
 * The payload contains one `# TYPE <name> <kind>` header per metric name
 * followed by one `<name>{k="v",...} <value>` line per label set. A
 * histogram emits a cumulative `<name>_bucket{le="..."}` series through
 * `le="+Inf"`, then `<name>_sum` and `<name>_count`. The output is
 * NUL-terminated only when it fits; callers that need a NUL terminator
 * must pass a buffer at least one byte larger than the returned length.
 *
 * @param metrics  Registry handle. NULL yields a length-0 payload.
 * @param buf      Output buffer, or NULL to only measure.
 * @param cap      Bytes available at @p buf. 0 measures without writing.
 * @return         The byte count the payload would occupy, not counting a
 *                 NUL terminator. When @p cap is smaller than the payload,
 *                 @p buf receives the leading @p cap bytes and the full
 *                 length is returned (snprintf-style).
 * @thread_safety safe — concurrent calls on the same handle are serialized.
 * @ownership caller — @p metrics is borrowed for the call only; @p buf is the
 *           caller's output storage.
 */
KITH_API size_t kith_metrics_render_prometheus(const kith_metrics_t *metrics,
                                               char *buf,
                                               size_t cap);

/**
 * Serialize the registry as an OpenTelemetry OTLP/JSON metrics payload into
 * @p buf.
 *
 * The payload is one JSON object: `resource_metrics` -> `scope_metrics` ->
 * `metrics`, where counters become `sum` (monotonic, cumulative
 * temporality 2), gauges become `gauge`, and histograms become `histogram`
 * with `explicit_bounds`, `bucket_counts`, `sum`, and `count`. Every data
 * point carries the payload's export timestamp as `time_unix_nano` (and
 * `start_time_unix_nano`) and its labels as `attributes`; the registry
 * records no per-observation times. The output is NUL-terminated only when
 * it fits.
 *
 * @param metrics  Registry handle. NULL yields a length-0 payload.
 * @param buf      Output buffer, or NULL to only measure.
 * @param cap      Bytes available at @p buf. 0 measures without writing.
 * @return         The byte count the payload would occupy, not counting a
 *                 NUL terminator (snprintf-style).
 * @thread_safety safe — concurrent calls on the same handle are serialized.
 * @ownership caller — @p metrics is borrowed for the call only; @p buf is the
 *           caller's output storage.
 */
KITH_API size_t kith_metrics_export_otlp(const kith_metrics_t *metrics, char *buf, size_t cap);

/** @} */

#endif /* KITH_METRICS_METRICS_H */
