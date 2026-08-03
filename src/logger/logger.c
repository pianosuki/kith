/* Public handle and emit path for the logger: level filtering, ISO 8601
 * timestamps, file, stdout, and JSONL sinks, and the structured-field handoff
 * to structured.c. The public contract is include/kith/logger/logger.h; the
 * NDJSON record builder is the sibling. */

#include "kith/logger/logger.h"

#include <errno.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <fcntl.h>
#include <pthread.h>
#include <unistd.h>

#include "kith/types.h"
#include "kith/version.h"
#include "logger/structured/structured.h"

#define KITH_LOG_LINE_CAP 4096
#define KITH_LOG_TS_CAP   40

struct kith_logger
{
    const kith_allocator_t *allocator;
    char *name;
    int file_fd;
    int jsonl_fd;
    bool stdout_enabled;
    _Atomic kith_logger_level_t min_level;
    pthread_mutex_t lock;
};

static const char *level_str_upper(kith_logger_level_t level)
{
    switch (level)
    {
        case KITH_LOG_LEVEL_TRACE:
            return "TRACE";
        case KITH_LOG_LEVEL_DEBUG:
            return "DEBUG";
        case KITH_LOG_LEVEL_INFO:
            return "INFO";
        case KITH_LOG_LEVEL_WARN:
            return "WARN";
        case KITH_LOG_LEVEL_ERROR:
            return "ERROR";
        default:
            return "INFO";
    }
}

/* Format @p ts as an ISO 8601 UTC string with millisecond precision and a
 * trailing Z, into @p buf. Returns the string length on success, 0 on a
 * conversion failure or truncation. */
static size_t format_iso8601(const struct timespec *ts, char *buf, size_t cap)
{
    struct tm tm;
    if (gmtime_r(&ts->tv_sec, &tm) == nullptr)
    {
        return 0;
    }
    const unsigned long ms = (unsigned long)(ts->tv_nsec / 1000000L);
    const int n = snprintf(buf,
                           cap,
                           "%04d-%02d-%02dT%02d:%02d:%02d.%03luZ",
                           tm.tm_year + 1900,
                           tm.tm_mon + 1,
                           tm.tm_mday,
                           tm.tm_hour,
                           tm.tm_min,
                           tm.tm_sec,
                           ms);
    if (n < 0 || (size_t)n >= cap)
    {
        return 0;
    }
    return (size_t)n;
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

/* Bounded line buffer for the human-readable sink. Writes are clamped to the
 * capacity so a long entry truncates rather than overflows; truncation of a
 * human line is best-effort and never affects the machine-readable record. */
typedef struct
{
    char *buf;
    size_t cap;
    size_t len;
} line_t;

static void line_putc(line_t *l, char c)
{
    if (l->cap > 0u && l->len < l->cap - 1u)
    {
        l->buf[l->len] = c;
    }
    l->len += 1;
}

static void line_puts(line_t *l, const char *s)
{
    if (s == nullptr)
    {
        return;
    }
    for (size_t i = 0; s[i] != '\0'; ++i)
    {
        line_putc(l, s[i]);
    }
}

static void build_human_line(line_t *l,
                             const char *name,
                             kith_logger_level_t level,
                             const char *ts,
                             const char *message,
                             const kith_logger_field_t *fields,
                             size_t field_count)
{
    line_puts(l, ts);
    line_puts(l, " | ");
    line_puts(l, level_str_upper(level));
    line_puts(l, " | ");
    line_puts(l, name != nullptr ? name : "");
    if (message != nullptr)
    {
        line_puts(l, " | ");
        line_puts(l, message);
    }
    if (field_count != 0 && fields != nullptr)
    {
        line_puts(l, " [");
        bool first = true;
        for (size_t i = 0; i < field_count; ++i)
        {
            if (fields[i].key == nullptr)
            {
                continue;
            }
            if (!first)
            {
                line_puts(l, ", ");
            }
            first = false;
            line_puts(l, fields[i].key);
            line_puts(l, "=");
            line_puts(l, fields[i].value != nullptr ? fields[i].value : "");
        }
        line_puts(l, "]");
    }
    line_putc(l, '\n');
}

static int open_sink(const char *path)
{
    const int fd = open(path, O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, 0644);
    if (fd < 0)
    {
        return -1;
    }
    return fd;
}

/* Apply non-null params to a freshly initialized logger: copy the name, set
 * the level and stdout flag, and open the configured sinks. On failure the
 * logger is left partially initialized and the caller runs kith_logger_destroy
 * to release whatever was opened. */
static int apply_params(struct kith_logger *lg, const kith_logger_params_t *params)
{
    if (params->name != nullptr)
    {
        lg->name = kith_strdup(lg->allocator, params->name);
        if (lg->name == nullptr)
        {
            return kith_error_return(KITH_ENOMEM);
        }
    }
    lg->stdout_enabled = params->stdout_enabled;
    atomic_store_explicit(&lg->min_level, params->min_level, memory_order_relaxed);
    if (params->file_path != nullptr)
    {
        lg->file_fd = open_sink(params->file_path);
        if (lg->file_fd < 0)
        {
            return kith_error_return(KITH_EIO);
        }
    }
    if (params->jsonl_path != nullptr)
    {
        lg->jsonl_fd = open_sink(params->jsonl_path);
        if (lg->jsonl_fd < 0)
        {
            return kith_error_return(KITH_EIO);
        }
    }
    return 0;
}

int kith_logger_create(const kith_logger_params_t *params,
                       const kith_allocator_t *alloc,
                       kith_logger_t **out_logger)
{
    if (out_logger == nullptr)
    {
        return kith_error_return(KITH_EINVAL);
    }
    *out_logger = nullptr;

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
    struct kith_logger *lg = kith_alloc(allocator, sizeof(*lg));
    if (lg == nullptr)
    {
        return kith_error_return(KITH_ENOMEM);
    }
    lg->allocator = allocator;
    lg->name = nullptr;
    lg->file_fd = -1;
    lg->jsonl_fd = -1;
    lg->stdout_enabled = false;
    atomic_init(&lg->min_level, KITH_LOG_LEVEL_ERROR);
    if (pthread_mutex_init(&lg->lock, nullptr) != 0)
    {
        kith_free(lg->allocator, lg);
        return kith_error_return(KITH_ESTATE);
    }

    int rc = 0;
    if (params != nullptr)
    {
        rc = apply_params(lg, params);
    }
    if (rc != 0)
    {
        kith_logger_destroy(lg);
        return rc;
    }
    *out_logger = lg;
    return 0;
}

void kith_logger_destroy(kith_logger_t *logger)
{
    if (logger == nullptr)
    {
        return;
    }
    struct kith_logger *lg = logger;
    if (lg->file_fd >= 0)
    {
        (void)close(lg->file_fd);
    }
    if (lg->jsonl_fd >= 0)
    {
        (void)close(lg->jsonl_fd);
    }
    (void)pthread_mutex_destroy(&lg->lock);
    kith_free(lg->allocator, lg->name);
    kith_free(lg->allocator, lg);
}

bool kith_logger_enabled(const kith_logger_t *logger, kith_logger_level_t level)
{
    if (logger == nullptr)
    {
        return false;
    }
    const kith_logger_level_t min = atomic_load_explicit(&logger->min_level, memory_order_relaxed);
    return level >= min;
}

int kith_logger_log(kith_logger_t *logger,
                    kith_logger_level_t level,
                    const char *file,
                    unsigned int line,
                    const kith_logger_field_t *fields,
                    size_t field_count,
                    const char *message)
{
    if (logger == nullptr)
    {
        return 0;
    }
    if (fields == nullptr && field_count != 0)
    {
        return kith_error_return(KITH_EINVAL);
    }
    if (!kith_logger_enabled(logger, level))
    {
        return 0;
    }

    struct timespec ts;
    if (clock_gettime(CLOCK_REALTIME, &ts) != 0)
    {
        return kith_error_return(KITH_EIO);
    }
    char tsbuf[KITH_LOG_TS_CAP];
    if (format_iso8601(&ts, tsbuf, sizeof(tsbuf)) == 0)
    {
        return kith_error_return(KITH_EIO);
    }

    char linebuf[KITH_LOG_LINE_CAP];
    line_t ln = {.buf = linebuf, .cap = sizeof(linebuf), .len = 0};
    build_human_line(&ln, logger->name, level, tsbuf, message, fields, field_count);
    const size_t line_len = ln.len < ln.cap ? ln.len : ln.cap - 1;
    linebuf[line_len] = '\0';

    const kith_log_entry_t entry = {
        .ts = tsbuf,
        .level = level,
        .module = logger->name,
        .file = file,
        .line = line,
        .message = message,
        .fields = fields,
        .field_count = field_count,
    };

    if (pthread_mutex_lock(&logger->lock) != 0)
    {
        return kith_error_return(KITH_ESTATE);
    }
    int rc = 0;
    if (logger->file_fd >= 0)
    {
        rc = write_all(logger->file_fd, linebuf, line_len);
    }
    if (rc == 0 && logger->stdout_enabled)
    {
        rc = write_all(STDOUT_FILENO, linebuf, line_len);
    }
    if (logger->jsonl_fd >= 0)
    {
        const int jrc = kith_structured_write(logger->allocator, logger->jsonl_fd, &entry);
        if (jrc != 0 && rc == 0)
        {
            rc = jrc;
        }
    }
    (void)pthread_mutex_unlock(&logger->lock);
    return rc;
}

int kith_logger_set_level(kith_logger_t *logger, kith_logger_level_t level)
{
    if (logger == nullptr)
    {
        return kith_error_return(KITH_EINVAL);
    }
    atomic_store_explicit(&logger->min_level, level, memory_order_relaxed);
    return 0;
}

kith_logger_level_t kith_logger_level(const kith_logger_t *logger)
{
    if (logger == nullptr)
    {
        return KITH_LOG_LEVEL_ERROR;
    }
    return atomic_load_explicit(&logger->min_level, memory_order_relaxed);
}
