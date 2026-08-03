#ifndef KITH_LOGGER_LOGGER_H
#define KITH_LOGGER_LOGGER_H

#include <stddef.h>
#include <stdint.h>

#include "kith/api.h"
#include "kith/types.h"

/**
 * Structured logger with a JSONL side-channel.
 *
 * A logger emits structured log entries: each entry carries a severity level,
 * a short human-readable message, and a set of caller-supplied key/value
 * fields. A logger owns up to three sinks, all optional: a human-readable log
 * file (one formatted line per entry), a mirror of that line to stdout, and a
 * JSONL side-channel file whose records follow the log side-channel record
 * defined in docs/event_schema.md, ready to stream or ship.
 *
 * The handle is owned by the composition root and passed by pointer to each
 * module that logs; there is no global accessor. The write path is internally
 * serialized (a per-handle mutex), so concurrent kith_logger_log calls on the
 * same handle do not interleave. Lifecycle calls (create/destroy) must not
 * race with each other or with an in-flight log call.
 */

/**
 * @defgroup kith_logger Logging
 * @{
 */

/**
 * Severity levels, ordered so that a higher value is more severe. A logger
 * drops every entry whose level is below its configured minimum.
 */
enum kith_logger_level : unsigned int
{
    KITH_LOG_LEVEL_TRACE = 0u,
    KITH_LOG_LEVEL_DEBUG = 1,
    KITH_LOG_LEVEL_INFO = 2,
    KITH_LOG_LEVEL_WARN = 3,
    KITH_LOG_LEVEL_ERROR = 4,
};

/** Alias of enum kith_logger_level. */
typedef enum kith_logger_level kith_logger_level_t;

/**
 * A single structured key/value field on a log entry. Both @p key and
 * @p value are NUL-terminated UTF-8 strings borrowed for the duration of the
 * kith_logger_log call that receives them; the logger neither copies nor
 * retains them. Callers commonly pass a compound-literal array:
 *
 *   kith_logger_log(logger, KITH_LOG_LEVEL_INFO, __FILE__, __LINE__,
 *                   &(kith_logger_field_t[]){{"actor", id}, {"zone", name}}, 2,
 *                   "entered");
 *
 * This is an exposed-layout value type (like struct iovec), not an opaque
 * handle: the two-pointer pair is part of the public contract and stays
 * stable.
 */
struct kith_logger_field
{
    /** Field key; borrowed for the duration of the log call. */
    const char *key;
    /** Field value; borrowed for the duration of the log call. */
    const char *value;
};

/** Alias of struct kith_logger_field. */
typedef struct kith_logger_field kith_logger_field_t;

/**
 * Opaque logger handle.
 *
 * @ownership callee — created by kith_logger_create, destroyed by
 *           kith_logger_destroy. The handle owns its file descriptors and
 *           mutex; all owned storage is released on destroy.
 */
typedef struct kith_logger kith_logger_t;

/**
 * Creation parameters. Size-versioned: callers set @p size to
 * sizeof(kith_logger_params_t) and @p abi_version to KITH_ABI_VERSION at
 * their compile time; the runtime rejects structs from an incompatible
 * generation or an undersized size. Future additive fields occupy the
 * reserved slots so the layout below stays stable across generations.
 */
struct kith_logger_params
{
    /** Must be sizeof(kith_logger_params_t). */
    uint32_t size;
    /** Must be KITH_ABI_VERSION (from kith/version.h). */
    uint32_t abi_version;

    /**
     * Module name emitted as the @c module field of the JSONL record and
     * the third column of a human-readable line. May be NULL only when no
     * sink is configured (a no-op logger); otherwise must be a non-empty
     * NUL-terminated string. Copied at create time; the caller may
     * release its buffer once kith_logger_create returns.
     */
    const char *name;

    /**
     * Path to a human-readable log file opened append-only, or NULL to skip
     * the file sink. The file is created if absent and must be writable;
     * creation fails with -KITH_EIO if it cannot be opened. Consumed at
     * create; the path is not retained and the caller may release its
     * buffer once kith_logger_create returns.
     */
    const char *file_path;

    /**
     * Path to the JSONL side-channel file opened append-only, or NULL to
     * skip the side-channel. Each entry is written as one JSON record
     * matching the log side-channel record in docs/event_schema.md.
     * Consumed at create; the path is not retained and the caller may
     * release its buffer once kith_logger_create returns.
     */
    const char *jsonl_path;

    /**
     * Whether the human-readable line is also written to stdout.
     */
    bool stdout_enabled;

    /**
     * Entries below this level are dropped before formatting.
     */
    kith_logger_level_t min_level;

    /** Reserved for future additive fields. Must be zero-filled. */
    void *reserved[8];
};

/** Alias of struct kith_logger_params. */
typedef struct kith_logger_params kith_logger_params_t;

/**
 * Build a logger from @p params.
 *
 * Every configured sink is opened eagerly so a bad path fails here rather
 * than on the first log entry. A params pointer of NULL produces a no-op
 * logger (no sinks, min_level ERROR) that accepts and discards every entry;
 * this lets a module compiled without logging produce a valid handle without
 * conditional code at every call site.
 *
 * @param params       Creation parameters; @c size and @c abi_version must
 *                     match the runtime generation. NULL requests the no-op
 *                     logger.
 * @param alloc        Allocator for the new handle, the name copy, and every
 *                     record buffer it writes, used again when
 *                     kith_logger_destroy frees them. NULL selects the
 *                     default allocator; a supplied allocator is validated
 *                     (see kith_allocator_t) and must outlive the handle.
 * @param out_logger   Receives the new handle on success.
 * @return             0 on success, negative kith_error on failure:
 *                     - -KITH_EINVAL if @p out_logger is NULL, or @p alloc
 *                       is missing an operation,
 *                     - -KITH_EABIVER if @p params or @p alloc has an
 *                       incompatible abi_version,
 *                     - -KITH_ESIZE if @p params or @p alloc has an
 *                       undersized size,
 *                     - -KITH_ENOMEM on allocation failure,
 *                     - -KITH_ESTATE if the logger's internal lock cannot
 *                       be initialized,
 *                     - -KITH_EIO if a configured sink path cannot be
 *                       opened.
 * @thread_safety unsafe — must not be called concurrently with another
 *                kith_logger_create on the same @p out_logger slot.
 * @ownership callee — the returned handle is freed by the caller with
 *           kith_logger_destroy.
 */
[[nodiscard]] KITH_API int kith_logger_create(const kith_logger_params_t *params,
                                              const kith_allocator_t *alloc,
                                              kith_logger_t **out_logger);

/**
 * Release a logger and all storage it owns. File descriptors are closed.
 * Passing NULL is a no-op.
 *
 * @param logger Logger handle. NULL is a no-op.
 * @thread_safety unsafe — no kith_logger_log may be in flight on @p logger
 *                when this is called.
 * @ownership callee — @p logger is consumed and freed by the call.
 */
KITH_API void kith_logger_destroy(kith_logger_t *logger);

/**
 * Report whether an entry at @p level would be emitted by @p logger (it is
 * at or above the configured minimum). Use this to skip building expensive
 * fields for a level that would be dropped.
 *
 * @param logger Logger handle.
 * @param level  Entry severity to test.
 * @return       true if @p logger is non-NULL and @p level is at or above the
 *               minimum, false otherwise.
 * @thread_safety safe
 * @ownership caller — @p logger is borrowed for the call only.
 */
[[nodiscard]] KITH_API bool kith_logger_enabled(const kith_logger_t *logger,
                                                kith_logger_level_t level);

/**
 * Emit one structured entry.
 *
 * @p fields is an array of @p field_count key/value pairs borrowed for the
 * call; passing NULL with a non-zero count is an error. @p file and @p line
 * locate the call site (typically __FILE__ and __LINE__). @p message is a
 * short, game-vocabulary-free NUL-terminated summary; dynamic values belong
 * in @p fields, not in @p message. Entries below the configured minimum are
 * dropped silently (success, no write).
 *
 * The human-readable line (file and/or stdout sinks) is formatted as
 * `<ts> | <LEVEL> | <name> | <message> [k=v, ...]`. The JSONL sink emits one
 * record per entry: @c ts (ISO 8601 UTC), @c level (lowercase), @c module
 * (the logger name), @c file, @c line, @c message, plus every caller field
 * as a top-level key (a caller field whose key collides with one of those
 * reserved names is dropped so the record stays single-key).
 *
 * @param logger       Logger handle. NULL is accepted and drops the entry.
 * @param level        Entry severity.
 * @param file         Call-site file, or NULL to omit.
 * @param line         Call-site line.
 * @param fields       Borrowed field array, or NULL if @p field_count is 0.
 * @param field_count  Number of fields at @p fields.
 * @param message      Human-readable summary, or NULL to omit.
 * @return             0 on success (including a dropped entry), negative
 *                     kith_error on failure:
 *                     - -KITH_EINVAL if @p fields is NULL but
 *                       @p field_count is non-zero,
 *                     - -KITH_ENOMEM if the JSONL record could not be
 *                       built,
 *                     - -KITH_ESTATE if the write lock could not be
 *                       acquired,
 *                     - -KITH_EIO if a write to an open sink fails.
 * @thread_safety safe — concurrent calls on the same handle are serialized.
 * @ownership caller — @p fields, its strings, @p file, and @p message are
 *           borrowed for the call only and are not retained.
 *
 * @warning The human-readable sink renders @p message and every field
 *          key/value verbatim: no newline or ANSI-escape handling, and the
 *          composed line truncates silently at 4096 bytes. A value an
 *          attacker can influence can forge or spoof lines in that sink.
 *          The structured JSONL sink escapes values by construction and is
 *          the integrity-preserving path for attacker-derived values.
 */
[[nodiscard]] KITH_API int kith_logger_log(kith_logger_t *logger,
                                           kith_logger_level_t level,
                                           const char *file,
                                           unsigned int line,
                                           const kith_logger_field_t *fields,
                                           size_t field_count,
                                           const char *message);

/**
 * Update the minimum level. Entries already in flight are unaffected.
 *
 * @param logger Logger handle.
 * @param level  New minimum level.
 * @return       0 on success, -KITH_EINVAL if @p logger is NULL.
 * @thread_safety safe
 * @ownership caller — @p logger is borrowed for the call only.
 */
[[nodiscard]] KITH_API int kith_logger_set_level(kith_logger_t *logger, kith_logger_level_t level);

/**
 * Report the configured minimum level. Returns KITH_LOG_LEVEL_ERROR for a
 * NULL logger.
 *
 * @param logger Logger handle.
 * @return       The configured minimum level; KITH_LOG_LEVEL_ERROR for a NULL logger.
 * @thread_safety safe
 * @ownership caller — @p logger is borrowed for the call only.
 */
KITH_API kith_logger_level_t kith_logger_level(const kith_logger_t *logger);

/** @} */

#endif /* KITH_LOGGER_LOGGER_H */
