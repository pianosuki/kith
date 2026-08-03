/* Prometheus text exposition for the metrics module: emits the counter,
 * gauge, and histogram series in the 0.0.4 text format with label and "le"
 * bucket escaping. Reads the registry snapshot via registry.h; the sibling
 * render is otlp.c. */

#include "metrics/render/prometheus.h"

#include <stdlib.h>
#include <string.h>

#include "kith/metrics/metrics.h"
#include "metrics/registry.h"
#include "metrics/render/render.h"

static const char *kind_type_name(kith_metrics_kind_t kind)
{
    switch (kind)
    {
        case KITH_METRICS_KIND_COUNTER:
            return "counter";
        case KITH_METRICS_KIND_GAUGE:
            return "gauge";
        case KITH_METRICS_KIND_HISTOGRAM:
            return "histogram";
        default:
            return "untyped";
    }
}

/* Emit a label value with Prometheus escaping: backslash, double-quote,
 * and newline are escaped so a value cannot break out of the quoted
 * label context. */
static void emit_label_value(builder_t *b, const char *v)
{
    b_putc(b, '"');
    if (v != nullptr)
    {
        for (size_t i = 0; v[i] != '\0'; ++i)
        {
            switch (v[i])
            {
                case '\\':
                    b_puts(b, "\\\\", 2);
                    break;
                case '"':
                    b_puts(b, "\\\"", 2);
                    break;
                case '\n':
                    b_puts(b, "\\n", 2);
                    break;
                default:
                    b_putc(b, v[i]);
                    break;
            }
        }
    }
    b_putc(b, '"');
}

static void emit_label_set(builder_t *b,
                           const struct label_slot *labels,
                           size_t count,
                           const char *extra_key,
                           const char *extra_value)
{
    /* Prometheus text format omits the braces entirely for a label-less
     * series (no caller labels and no synthetic extra like "le"). A
     * histogram bucket line always carries "le", so it always emits. */
    if (count == 0 && extra_key == nullptr)
    {
        return;
    }
    b_putc(b, '{');
    bool first = true;
    for (size_t i = 0; i < count; ++i)
    {
        if (!first)
        {
            b_putc(b, ',');
        }
        first = false;
        b_putcstr(b, labels[i].key);
        b_putc(b, '=');
        emit_label_value(b, labels[i].value);
    }
    if (extra_key != nullptr)
    {
        if (!first)
        {
            b_putc(b, ',');
        }
        b_putcstr(b, extra_key);
        b_putc(b, '=');
        emit_label_value(b, extra_value);
    }
    b_putc(b, '}');
}

static void emit_name(builder_t *b, const kith_metrics_t *m, const char *name)
{
    if (m->prefix != nullptr)
    {
        b_putcstr(b, m->prefix);
    }
    b_putcstr(b, name);
}

static void emit_scalar_series(builder_t *b, const kith_metrics_t *m, const struct series *s)
{
    emit_name(b, m, s->name);
    emit_label_set(b, s->labels, s->label_count, nullptr, nullptr);
    b_putc(b, ' ');
    if (s->kind == KITH_METRICS_KIND_COUNTER)
    {
        b_put_u64(b, s->counter);
    }
    else
    {
        b_put_i64(b, s->gauge);
    }
    b_putc(b, '\n');
}

/* Emit the cumulative bucket series, then sum and count, for a histogram.
 * bucket i is cumulative over buckets[0..i]; the +Inf bucket carries the
 * total observation count. */
static void emit_histogram_series(builder_t *b, const kith_metrics_t *m, const struct series *s)
{
    uint64_t cumulative = 0;
    char lebuf[24];
    for (size_t i = 0; i < KITH_HIST_BOUNDS_COUNT; ++i)
    {
        cumulative += s->hist_buckets[i];
        emit_name(b, m, s->name);
        b_puts(b, "_bucket", 7);
        const int n =
            snprintf(lebuf, sizeof(lebuf), "%llu", (unsigned long long)kith_hist_bounds[i]);
        if (n > 0)
        {
            emit_label_set(b, s->labels, s->label_count, "le", lebuf);
        }
        b_putc(b, ' ');
        b_put_u64(b, cumulative);
        b_putc(b, '\n');
    }
    cumulative += s->hist_buckets[KITH_HIST_BOUNDS_COUNT];
    emit_name(b, m, s->name);
    b_puts(b, "_bucket", 7);
    emit_label_set(b, s->labels, s->label_count, "le", "+Inf");
    b_putc(b, ' ');
    b_put_u64(b, cumulative);
    b_putc(b, '\n');

    emit_name(b, m, s->name);
    b_puts(b, "_sum", 4);
    emit_label_set(b, s->labels, s->label_count, nullptr, nullptr);
    b_putc(b, ' ');
    b_put_u64(b, s->hist_sum);
    b_putc(b, '\n');

    emit_name(b, m, s->name);
    b_puts(b, "_count", 6);
    emit_label_set(b, s->labels, s->label_count, nullptr, nullptr);
    b_putc(b, ' ');
    b_put_u64(b, s->hist_count);
    b_putc(b, '\n');
}

struct idx_entry
{
    size_t idx;
    const char *name;
    kith_metrics_kind_t kind;
};

static int idx_cmp(const void *a, const void *b)
{
    const struct idx_entry *ea = a;
    const struct idx_entry *eb = b;
    const int c = strcmp(ea->name, eb->name);
    if (c != 0)
    {
        return c;
    }
    if (ea->kind < eb->kind)
    {
        return -1;
    }
    if (ea->kind > eb->kind)
    {
        return 1;
    }
    if (ea->idx < eb->idx)
    {
        return -1;
    }
    return ea->idx > eb->idx ? 1 : 0;
}

KITH_LOCAL size_t kith_render_prometheus(const kith_metrics_t *m, char *buf, size_t cap)
{
    if (m == nullptr)
    {
        return 0;
    }
    /* First pass measures; second pass fills, clamped to cap. The builder
     * keeps a single code path so the two passes cannot diverge. */
    builder_t b = {.cur = nullptr, .cap = 0, .used = 0};

    /* Sort indices by (name, kind) so every group sharing both is
     * contiguous, letting one # TYPE header precede each group. Series
     * identity is (name, kind, labels): a counter and a gauge sharing a
     * name are separate families and each renders under its own header. */
    struct idx_entry *order = nullptr;
    if (m->series_count != 0)
    {
        order = kith_alloc(m->allocator, m->series_count * sizeof(*order));
    }
    if (m->series_count != 0 && order == nullptr)
    {
        return 0;
    }
    for (size_t i = 0; i < m->series_count; ++i)
    {
        order[i].idx = i;
        order[i].name = m->series[i].name;
        order[i].kind = m->series[i].kind;
    }
    if (m->series_count > 1)
    {
        qsort(order, m->series_count, sizeof(*order), idx_cmp);
    }

    const char *prev_name = nullptr;
    kith_metrics_kind_t prev_kind = KITH_METRICS_KIND_COUNTER;
    for (int pass = 0; pass < 2; ++pass)
    {
        b.cur = pass == 0 ? nullptr : buf;
        b.cap = pass == 0 ? 0 : cap;
        b.used = 0;
        prev_name = nullptr;
        prev_kind = KITH_METRICS_KIND_COUNTER;
        for (size_t oi = 0; oi < m->series_count; ++oi)
        {
            const struct series *s = &m->series[order[oi].idx];
            if (prev_name == nullptr || strcmp(prev_name, s->name) != 0 || prev_kind != s->kind)
            {
                b_putcstr(&b, "# TYPE ");
                emit_name(&b, m, s->name);
                b_putc(&b, ' ');
                b_putcstr(&b, kind_type_name(s->kind));
                b_putc(&b, '\n');
                prev_name = s->name;
                prev_kind = s->kind;
            }
            if (s->kind == KITH_METRICS_KIND_HISTOGRAM)
            {
                emit_histogram_series(&b, m, s);
            }
            else
            {
                emit_scalar_series(&b, m, s);
            }
        }
    }
    kith_free(m->allocator, order);
    return b.used;
}
