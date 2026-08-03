/* NDJSON record builder for the logger: two-pass measure-then-fill over caller
 * fields, JSON escaping, and reserved-key deduplication. Produces the line
 * that logger.c writes to the JSONL sink; shares its escape discipline with
 * the metrics renders. */

#include "structured.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <unistd.h>

#include "kith/logger/logger.h"
#include "kith/types.h"

/* NDJSON record keys owned by the envelope; a caller field whose key matches
 * one of these is dropped so the record stays single-key. */
static const char *const reserved_keys[] = {
    "ts",
    "level",
    "module",
    "file",
    "line",
    "message",
};

static bool key_is_reserved(const char *key, size_t klen)
{
    for (size_t i = 0; i < sizeof(reserved_keys) / sizeof(reserved_keys[0]); ++i)
    {
        const size_t rlen = strlen(reserved_keys[i]);
        if (rlen == klen && memcmp(key, reserved_keys[i], klen) == 0)
        {
            return true;
        }
    }
    return false;
}

/* A growable-neutral builder: when cur is NULL it only counts bytes; when
 * cur is non-NULL it writes them. Measuring and filling share one code path
 * so the two passes cannot diverge. */
typedef struct
{
    char *cur;
    size_t used;
} builder_t;

static void b_putc(builder_t *b, char c)
{
    if (b->cur != nullptr)
    {
        b->cur[b->used] = c;
    }
    b->used += 1;
}

static void b_puts(builder_t *b, const char *s, size_t n)
{
    if (b->cur != nullptr && n != 0)
    {
        memcpy(b->cur + b->used, s, n);
    }
    b->used += n;
}

/* Append a NUL-terminated literal. Using strlen avoids hand-counting the
 * literal length, which risks copying the terminator into the record. */
static void b_putcstr(builder_t *b, const char *s)
{
    b_puts(b, s, strlen(s));
}

/* Write one byte as its JSON escape sequence (or verbatim when no escape is
 * needed). Control bytes below 0x20 become \uXXXX. */
static void b_putc_escaped(builder_t *b, unsigned char c)
{
    switch (c)
    {
        case '"':
            b_puts(b, "\\\"", 2);
            break;
        case '\\':
            b_puts(b, "\\\\", 2);
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

static void b_put_escaped(builder_t *b, const char *s, size_t n)
{
    if (s == nullptr)
    {
        return;
    }
    for (size_t i = 0; i < n; ++i)
    {
        b_putc_escaped(b, (unsigned char)s[i]);
    }
}

static void b_put_uint(builder_t *b, unsigned int v)
{
    char buf[12];
    size_t n = 0;
    if (v == 0)
    {
        b_putc(b, '0');
        return;
    }
    while (v != 0 && n < sizeof(buf))
    {
        buf[n] = (char)('0' + (v % 10u));
        v /= 10u;
        n += 1;
    }
    while (n > 0)
    {
        n -= 1;
        b_putc(b, buf[n]);
    }
}

static const char *level_to_string(kith_logger_level_t level)
{
    switch (level)
    {
        case KITH_LOG_LEVEL_TRACE:
            return "trace";
        case KITH_LOG_LEVEL_DEBUG:
            return "debug";
        case KITH_LOG_LEVEL_INFO:
            return "info";
        case KITH_LOG_LEVEL_WARN:
            return "warn";
        case KITH_LOG_LEVEL_ERROR:
            return "error";
        default:
            return "info";
    }
}

/* Emit the fixed prefix of the record: ts, level, module, file/line, message.
 * Caller fields and the closing brace are emitted by the caller. */
static void build_prefix(builder_t *b, const kith_log_entry_t *e)
{
    b_putcstr(b, "{\"ts\":\"");
    b_put_escaped(b, e->ts, strlen(e->ts));
    b_putcstr(b, "\",\"level\":\"");
    b_putcstr(b, level_to_string(e->level));
    b_putcstr(b, "\",\"module\":\"");
    b_put_escaped(b, e->module, e->module != nullptr ? strlen(e->module) : 0);
    b_putc(b, '"');
    if (e->file != nullptr)
    {
        b_putcstr(b, ",\"file\":\"");
        b_put_escaped(b, e->file, strlen(e->file));
        b_putcstr(b, "\",\"line\":");
        b_put_uint(b, e->line);
    }
    if (e->message != nullptr)
    {
        b_putcstr(b, ",\"message\":\"");
        b_put_escaped(b, e->message, strlen(e->message));
        b_putc(b, '"');
    }
}

static void build_fields(builder_t *b, const kith_log_entry_t *e)
{
    for (size_t i = 0; i < e->field_count; ++i)
    {
        const kith_logger_field_t *f = &e->fields[i];
        if (f->key == nullptr)
        {
            continue;
        }
        const size_t klen = strlen(f->key);
        if (key_is_reserved(f->key, klen))
        {
            continue;
        }
        b_putcstr(b, ",\"");
        b_put_escaped(b, f->key, klen);
        b_putcstr(b, "\":\"");
        b_put_escaped(b, f->value, f->value != nullptr ? strlen(f->value) : 0);
        b_putc(b, '"');
    }
}

/* Run the builder over the record in the chosen mode (cur == NULL measures,
 * cur != NULL fills). Returns the byte count. */
static size_t build_record(char *cur, const kith_log_entry_t *e)
{
    builder_t b = {.cur = cur, .used = 0};
    build_prefix(&b, e);
    build_fields(&b, e);
    b_putc(&b, '}');
    b_putc(&b, '\n');
    return b.used;
}

static int write_all(int fd, const char *buf, size_t len)
{
    size_t remaining = len;
    const char *p = buf;
    while (remaining > 0)
    {
        ssize_t n = write(fd, p, remaining);
        if (n > 0)
        {
            p += (size_t)n;
            remaining -= (size_t)n;
            continue;
        }
        if (n < 0 && errno == EINTR)
        {
            continue;
        }
        return kith_error_return(KITH_EIO);
    }
    return 0;
}

KITH_LOCAL int
kith_structured_write(const kith_allocator_t *alloc, int fd, const kith_log_entry_t *entry)
{
    if (fd < 0 || entry == nullptr)
    {
        return 0;
    }
    const size_t len = build_record(nullptr, entry);
    char *buf = kith_alloc(alloc, len);
    if (buf == nullptr)
    {
        return kith_error_return(KITH_ENOMEM);
    }
    (void)build_record(buf, entry);
    const int rc = write_all(fd, buf, len);
    kith_free(alloc, buf);
    return rc;
}
