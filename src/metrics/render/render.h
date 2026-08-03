#pragma once

#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>

/* Shared text-builder used by the Prometheus and OTLP serializers. A
 * two-pass measure/fill builder (cur == NULL measures, cur != NULL fills,
 * clamped to cap) so a single code path produces both the byte count and
 * the truncated output. Mirrors the builder in logger/structured/structured.c
 * so the two serializers cannot diverge in clamping behavior. */

typedef struct
{
    char *cur;
    size_t cap;
    size_t used;
} builder_t;

static inline void b_putc(builder_t *b, char c)
{
    if (b->cur != nullptr && b->used < b->cap)
    {
        b->cur[b->used] = c;
    }
    b->used += 1;
}

static inline void b_puts(builder_t *b, const char *s, size_t n)
{
    if (b->cur != nullptr && b->used < b->cap && n != 0)
    {
        const size_t room = b->cap - b->used;
        const size_t k = n < room ? n : room;
        for (size_t i = 0; i < k; ++i)
        {
            b->cur[b->used + i] = s[i];
        }
    }
    b->used += n;
}

static inline void b_putcstr(builder_t *b, const char *s)
{
    if (s != nullptr)
    {
        for (size_t i = 0; s[i] != '\0'; ++i)
        {
            b_putc(b, s[i]);
        }
    }
}

/* Append an unsigned 64-bit value as decimal text. */
static inline void b_put_u64(builder_t *b, uint64_t v)
{
    char tmp[20];
    size_t n = 0;
    if (v == 0)
    {
        b_putc(b, '0');
        return;
    }
    while (v != 0 && n < sizeof(tmp))
    {
        tmp[n] = (char)('0' + (v % 10u));
        v /= 10u;
        n += 1;
    }
    while (n > 0)
    {
        n -= 1;
        b_putc(b, tmp[n]);
    }
}

/* Append a signed 64-bit value as decimal text, with a leading '-' for
 * negatives. */
static inline void b_put_i64(builder_t *b, int64_t v)
{
    if (v < 0)
    {
        b_putc(b, '-');
        const uint64_t u = (uint64_t)(-(v + 1)) + 1u;
        b_put_u64(b, u);
        return;
    }
    b_put_u64(b, (uint64_t)v);
}
