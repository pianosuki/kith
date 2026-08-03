/* OTLP/JSON exposition for the metrics module: serializes the registry
 * snapshot into the OTLP JSON text format with quoted-decimal 64-bit values.
 * Reads the registry under the handle lock via registry.h; shares the builder
 * and JSON escape discipline with prometheus.c and logger/structured.c. */

#include "metrics/render/otlp.h"

#include <stdlib.h>
#include <string.h>

#include "kith/metrics/metrics.h"
#include "kith/types.h"
#include "metrics/registry.h"
#include "metrics/render/render.h"

/* Emit a JSON string literal with the canonical JSON escapes: " \ and the
 * control bytes b f n r t plus \uXXXX for the rest below 0x20. Mirrors the
 * escape discipline in logger/structured/structured.c so the two writers
 * agree. */
static void json_emit_string(builder_t *b, const char *s)
{
    b_putc(b, '"');
    if (s != nullptr)
    {
        for (size_t i = 0; s[i] != '\0'; ++i)
        {
            const unsigned char c = (unsigned char)s[i];
            switch (c)
            {
                case '"':
                    b_puts(b, "\\\"", 2);
                    break;
                case '\\':
                    b_puts(b, "\\\\", 2);
                    break;
                case '\b':
                    b_puts(b, "\\b", 2);
                    break;
                case '\f':
                    b_puts(b, "\\f", 2);
                    break;
                case '\n':
                    b_puts(b, "\\n", 2);
                    break;
                case '\r':
                    b_puts(b, "\\r", 2);
                    break;
                case '\t':
                    b_puts(b, "\\t", 2);
                    break;
                default:
                    if (c < 0x20)
                    {
                        char buf[7];
                        const int w = snprintf(buf, sizeof(buf), "\\u%04x", (unsigned int)c);
                        if (w > 0)
                        {
                            b_puts(b, buf, (size_t)w);
                        }
                    }
                    else
                    {
                        b_putc(b, (char)c);
                    }
                    break;
            }
        }
    }
    b_putc(b, '"');
}

/* Emit a uint64/int64 as a quoted decimal string (the OTLP/JSON encoding
 * for fixed64/int64 fields, which keeps 64-bit values exact across JSON
 * parsers). */
static void emit_u64_str(builder_t *b, uint64_t v)
{
    b_putc(b, '"');
    b_put_u64(b, v);
    b_putc(b, '"');
}

static void emit_i64_str(builder_t *b, int64_t v)
{
    b_putc(b, '"');
    b_put_i64(b, v);
    b_putc(b, '"');
}

static void emit_name(builder_t *b, const kith_metrics_t *m, const char *name)
{
    if (m->prefix != nullptr)
    {
        b_putcstr(b, m->prefix);
    }
    b_putcstr(b, name);
}

static void emit_attributes(builder_t *b, const struct label_slot *labels, size_t count)
{
    b_putc(b, '[');
    for (size_t i = 0; i < count; ++i)
    {
        if (i != 0)
        {
            b_putc(b, ',');
        }
        b_putcstr(b, "{\"key\":");
        json_emit_string(b, labels[i].key);
        b_putcstr(b, ",\"value\":{\"stringValue\":");
        json_emit_string(b, labels[i].value);
        b_putc(b, '}');
        b_putc(b, '}');
    }
    b_putc(b, ']');
}

/* Emit one counter data point. A counter is monotonic with cumulative
 * temporality (AGGREGATION_TEMPORALITY_CUMULATIVE = 2). The value rides the
 * schema's integer point member (as_int), exact for every counter total
 * below INT64_MAX. */
static void emit_counter_point(builder_t *b, const struct series *s, uint64_t time_unix_nano)
{
    b_putc(b, '{');
    b_putcstr(b, "\"attributes\":");
    emit_attributes(b, s->labels, s->label_count);
    b_putcstr(b, ",\"start_time_unix_nano\":");
    emit_u64_str(b, time_unix_nano);
    b_putcstr(b, ",\"time_unix_nano\":");
    emit_u64_str(b, time_unix_nano);
    b_putcstr(b, ",\"as_int\":");
    emit_u64_str(b, s->counter);
    b_putc(b, '}');
}

static void emit_gauge_point(builder_t *b, const struct series *s, uint64_t time_unix_nano)
{
    b_putc(b, '{');
    b_putcstr(b, "\"attributes\":");
    emit_attributes(b, s->labels, s->label_count);
    b_putcstr(b, ",\"time_unix_nano\":");
    emit_u64_str(b, time_unix_nano);
    b_putcstr(b, ",\"as_int\":");
    emit_i64_str(b, s->gauge);
    b_putc(b, '}');
}

/* Emit one histogram data point: sum (double), count (uint64 string), the
 * explicit bounds array (doubles), and the bucket counts (uint64 strings,
 * one more than bounds to include the +Inf overflow bucket). */
static void emit_histogram_point(builder_t *b, const struct series *s, uint64_t time_unix_nano)
{
    b_putc(b, '{');
    b_putcstr(b, "\"attributes\":");
    emit_attributes(b, s->labels, s->label_count);
    b_putcstr(b, ",\"start_time_unix_nano\":");
    emit_u64_str(b, time_unix_nano);
    b_putcstr(b, ",\"time_unix_nano\":");
    emit_u64_str(b, time_unix_nano);
    b_putcstr(b, ",\"sum\":");
    b_put_u64(b, s->hist_sum);
    b_putcstr(b, ",\"count\":");
    emit_u64_str(b, s->hist_count);
    b_putcstr(b, ",\"explicit_bounds\":[");
    for (size_t i = 0; i < KITH_HIST_BOUNDS_COUNT; ++i)
    {
        if (i != 0)
        {
            b_putc(b, ',');
        }
        b_put_u64(b, kith_hist_bounds[i]);
    }
    b_putcstr(b, "],\"bucket_counts\":[");
    for (size_t i = 0; i <= KITH_HIST_BOUNDS_COUNT; ++i)
    {
        if (i != 0)
        {
            b_putc(b, ',');
        }
        emit_u64_str(b, s->hist_buckets[i]);
    }
    b_putc(b, ']');
    b_putc(b, '}');
}

/* Group series by (name, kind) so each metric object carries every label
 * set that shares its name and kind as separate data points. */
struct group_key
{
    const char *name;
    kith_metrics_kind_t kind;
};

static int group_cmp(const void *a, const void *b)
{
    const struct group_key *ea = a;
    const struct group_key *eb = b;
    const int c = strcmp(ea->name, eb->name);
    if (c != 0)
    {
        return c;
    }
    if (ea->kind < eb->kind)
    {
        return -1;
    }
    return ea->kind > eb->kind ? 1 : 0;
}

/* Emit one metric object: its name, kind-specific value container, and the
 * data points of every series sharing this (name, kind) group. */
static void emit_metric_object(builder_t *b,
                               const kith_metrics_t *m,
                               const struct group_key *g,
                               const size_t *series_order,
                               const size_t *group_start,
                               size_t group_len,
                               uint64_t time_unix_nano)
{
    b_putcstr(b, "{\"name\":\"");
    emit_name(b, m, g->name);
    b_putcstr(b, "\",\"description\":\"\",\"unit\":\"\"");
    if (g->kind == KITH_METRICS_KIND_COUNTER)
    {
        b_putcstr(
            b, ",\"sum\":{\"is_monotonic\":true,\"aggregation_temporality\":2,\"data_points\":[");
    }
    else if (g->kind == KITH_METRICS_KIND_GAUGE)
    {
        b_putcstr(b, ",\"gauge\":{\"data_points\":[");
    }
    else
    {
        b_putcstr(b, ",\"histogram\":{\"aggregation_temporality\":2,\"data_points\":[");
    }
    for (size_t gi = 0; gi < group_len; ++gi)
    {
        if (gi != 0)
        {
            b_putc(b, ',');
        }
        const struct series *s = &m->series[series_order[group_start[gi]]];
        if (g->kind == KITH_METRICS_KIND_COUNTER)
        {
            emit_counter_point(b, s, time_unix_nano);
        }
        else if (g->kind == KITH_METRICS_KIND_GAUGE)
        {
            emit_gauge_point(b, s, time_unix_nano);
        }
        else
        {
            emit_histogram_point(b, s, time_unix_nano);
        }
    }
    /* Close data_points, the kind-specific value container, and the metric
     * object: ] + } + }. */
    b_putcstr(b, "]}}");
}

/* Build a per-series order array sorted by (name, kind) so the metric
 * objects group same-(name,kind) series as adjacent data points. Returns
 * the order (caller frees) and the matching keys array, or returns 0 with
 * both left NULL when allocation fails or the registry is empty. */
static int
build_series_order(const kith_metrics_t *m, size_t **out_order, struct group_key **out_keys)
{
    *out_order = nullptr;
    *out_keys = nullptr;
    if (m->series_count == 0)
    {
        return 0;
    }
    size_t *order = kith_alloc(m->allocator, m->series_count * sizeof(*order));
    struct group_key *keys = kith_alloc(m->allocator, m->series_count * sizeof(*keys));
    if (order == nullptr || keys == nullptr)
    {
        kith_free(m->allocator, order);
        kith_free(m->allocator, keys);
        return kith_error_return(KITH_ENOMEM);
    }
    for (size_t i = 0; i < m->series_count; ++i)
    {
        order[i] = i;
        keys[i].name = m->series[i].name;
        keys[i].kind = m->series[i].kind;
    }
    /* Sort order by the (name, kind) keys. A small selection sort keeps the
     * code dependency-free and the registry small. */
    for (size_t i = 0; i + 1 < m->series_count; ++i)
    {
        size_t best = i;
        for (size_t j = i + 1; j < m->series_count; ++j)
        {
            if (group_cmp(&keys[order[j]], &keys[order[best]]) < 0)
            {
                best = j;
            }
        }
        const size_t tmp = order[i];
        order[i] = order[best];
        order[best] = tmp;
    }
    *out_order = order;
    *out_keys = keys;
    return 0;
}

static void emit_payload(builder_t *b,
                         const kith_metrics_t *m,
                         const size_t *order,
                         const struct group_key *keys,
                         uint64_t time_unix_nano)
{
    b_putcstr(
        b,
        "{\"resource_metrics\":[{\"scope_metrics\":[{\"scope\":{\"name\":\"kith\"},\"metrics\":[");
    size_t oi = 0;
    bool first_metric = true;
    while (oi < m->series_count)
    {
        const size_t group_start = oi;
        const struct group_key g = keys[order[oi]];
        while (oi < m->series_count && group_cmp(&keys[order[oi]], &g) == 0)
        {
            oi += 1;
        }
        const size_t group_len = oi - group_start;
        if (!first_metric)
        {
            b_putc(b, ',');
        }
        first_metric = false;
        emit_metric_object(b, m, &g, order, &group_start, group_len, time_unix_nano);
    }
    b_putcstr(b, "]}]}]}");
}

KITH_LOCAL size_t kith_render_otlp(const kith_metrics_t *m,
                                   char *buf,
                                   size_t cap,
                                   uint64_t time_unix_nano)
{
    if (m == nullptr)
    {
        return 0;
    }
    size_t *order = nullptr;
    struct group_key *keys = nullptr;
    if (build_series_order(m, &order, &keys) != 0)
    {
        return 0;
    }
    builder_t b = {.cur = nullptr, .cap = 0, .used = 0};
    for (int pass = 0; pass < 2; ++pass)
    {
        b.cur = pass == 0 ? nullptr : buf;
        b.cap = pass == 0 ? 0 : cap;
        b.used = 0;
        emit_payload(&b, m, order, keys, time_unix_nano);
    }
    kith_free(m->allocator, order);
    kith_free(m->allocator, keys);
    return b.used;
}
