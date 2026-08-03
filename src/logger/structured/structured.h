#pragma once

#include <stddef.h>

#include "kith/logger/logger.h"
#include "kith/types.h"

/* One log entry handed to the JSONL side-channel. Every string is borrowed
 * for the duration of kith_structured_write; the writer neither copies nor
 * retains them. The entry is serialized as one NDJSON record matching the
 * canonical envelope in docs/event_schema.md. */
typedef struct kith_log_entry
{
    const char *ts;                    /* ISO 8601 UTC string, borrowed (always non-NULL) */
    kith_logger_level_t level;
    const char *module;                /* logger name, borrowed (NULL emitted as empty) */
    const char *file;                  /* call-site file, borrowed or NULL */
    unsigned int line;
    const char *message;               /* human summary, borrowed or NULL */
    const kith_logger_field_t *fields; /* borrowed, may be NULL */
    size_t field_count;
} kith_log_entry_t;

/* Serialize @p entry as one NDJSON record and write it to @p fd in a single
 * write so concurrent loggers on the same append-only file cannot interleave
 * a record. The record buffer routes through @p alloc for the duration of
 * the call. Returns 0 on success, -KITH_ENOMEM if the record buffer cannot
 * be allocated, -KITH_EIO on a write failure. */
KITH_LOCAL int
kith_structured_write(const kith_allocator_t *alloc, int fd, const kith_log_entry_t *entry);
