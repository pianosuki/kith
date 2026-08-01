#ifndef KITH_CONFIG_CONFIG_H
#define KITH_CONFIG_CONFIG_H

#include <stdint.h>

#include "kith/api.h"
#include "kith/types.h"

/**
 * Typed, layered configuration source.
 *
 * A configuration source is a read-only snapshot built at creation time from
 * two layers: a key=value file (optional) and the process environment
 * (optional, restricted to a prefix namespace). The environment wins over the
 * file: a prefixed env var overrides a file entry with the same key. Callers
 * query keys through typed accessors that parse, range-check, and report a
 * missing key as an error so required fields surface at startup rather than
 * silently taking a default.
 *
 * The handle is owned by the composition root and passed by pointer to each
 * module's init function; there is no global accessor. The snapshot is
 * immutable for the lifetime of the handle, so accessors are safe to call
 * concurrently with each other. The handle must not be destroyed while any
 * accessor is in flight.
 */

/**
 * @defgroup kith_config Configuration
 * @{
 */

/**
 * Opaque configuration source handle.
 *
 * @ownership callee — created by kith_config_create, destroyed by
 *           kith_config_destroy. Pointers returned by accessors alias storage
 *           owned by the handle and remain valid until kith_config_destroy.
 */
typedef struct kith_config kith_config_t;

/**
 * Creation parameters. Size-versioned: callers set @p size to
 * sizeof(kith_config_params_t) and @p abi_version to KITH_ABI_VERSION at their
 * compile time; the runtime rejects structs from an incompatible generation
 * or an undersized size. Future additive fields occupy the reserved slots so
 * the layout of the fields below stays stable across generations.
 */
struct kith_config_params
{
    /** Must be sizeof(kith_config_params_t). */
    uint32_t size;
    /** Must be KITH_ABI_VERSION (from kith/version.h). */
    uint32_t abi_version;

    /**
     * Environment-variable namespace prefix, or NULL to load the file only.
     * When non-NULL, every env var whose name starts with this prefix is
     * snapshotted into the source under the key formed by stripping the
     * prefix (case preserved). Env entries override file entries with the
     * same key. Consumed at create; the prefix is not retained and the
     * caller may release its buffer once kith_config_create returns.
     */
    const char *env_prefix;

    /**
     * Path to a key=value file to load, or NULL to load the environment only.
     * The format is one entry per line: `KEY=VALUE`, with `#` starting a
     * comment and surrounding double quotes optional on the value. Blank
     * lines, comment-only lines, and lines without `=` are skipped; a
     * skipped line is not an error. A missing file is not an error; the
     * source is built from the environment alone. Consumed at create; the
     * path is not retained and the caller may release its buffer once
     * kith_config_create returns.
     */
    const char *file_path;

    /** Reserved for future additive fields. Must be zero-filled. */
    void *reserved[8];
};

/** Alias of struct kith_config_params. */
typedef struct kith_config_params kith_config_params_t;

/**
 * Build a configuration source from the file and environment layers.
 *
 * The file (if @p params.file_path names a readable file) is loaded first,
 * then the environment (if @p params.env_prefix is non-NULL) overlays it. The
 * snapshot is immutable after this call returns.
 *
 * @param params    Creation parameters; @c size and @c abi_version must match
 *                  the runtime generation. May be NULL only to request the
 *                  empty source (no file, no env), which still succeeds.
 * @param alloc     Allocator for the new handle and every entry it loads,
 *                  used again when kith_config_destroy frees it. NULL selects
 *                  the default allocator; a supplied allocator is validated
 *                  (see kith_allocator_t) and must outlive the handle.
 * @param out_cfg   Receives the new handle on success.
 * @return          0 on success, negative kith_error on failure:
 *                  - -KITH_EINVAL if @p out_cfg is NULL, or @p alloc is
 *                    missing an operation,
 *                  - -KITH_EABIVER if @p params or @p alloc has an
 *                    incompatible abi_version,
 *                  - -KITH_ESIZE if @p params or @p alloc has an undersized
 *                    size,
 *                  - -KITH_ENOMEM on allocation failure,
 *                  - -KITH_EIO if @p params.file_path exists but cannot be
 *                    read.
 * @thread_safety unsafe — must not be called concurrently with another
 *                kith_config_create on the same @p out_cfg slot.
 * @ownership callee — the returned handle is freed by the caller with
 *           kith_config_destroy.
 */
[[nodiscard]] KITH_API int kith_config_create(const kith_config_params_t *params,
                                              const kith_allocator_t *alloc,
                                              kith_config_t **out_cfg);

/**
 * Release a configuration source and all storage it owns. Pointers returned
 * by earlier accessor calls are invalidated. Passing NULL is a no-op.
 *
 * @param cfg  Configuration source handle. NULL is a no-op.
 * @thread_safety unsafe — no accessor may be in flight on @p cfg when this is
 *                called.
 * @ownership callee — @p cfg is consumed and freed by the call.
 */
KITH_API void kith_config_destroy(kith_config_t *cfg);

/**
 * Report whether @p key is present in the source. A present key may still
 * fail a typed accessor if its value cannot be parsed for the requested type.
 *
 * @param cfg  Source to query.
 * @param key  Key to look up.
 * @return     true if @p key is present, false otherwise (including when
 *             @p cfg or @p key is NULL).
 * @thread_safety safe
 * @ownership caller — @p cfg and @p key are borrowed for the call only.
 */
[[nodiscard]] KITH_API bool kith_config_has(const kith_config_t *cfg, const char *key);

/**
 * Look up @p key as a string. String values are stored verbatim (after
 * quote/whitespace trimming from the file).
 *
 * @param cfg      Source to query.
 * @param key      Key to look up.
 * @param out_val  Receives a pointer to the value, owned by @p cfg and valid
 *                 until kith_config_destroy. Must not be NULL.
 * @return         0 on success, -KITH_ENOENT if @p key is absent,
 *                 -KITH_EINVAL if @p cfg, @p key, or @p out_val is NULL.
 * @thread_safety safe
 * @ownership callee — the pointer stored in @p out_val is borrowed from
 *           @p cfg and is valid until kith_config_destroy; the caller must not
 *           free it.
 */
[[nodiscard]] KITH_API int
kith_config_string(const kith_config_t *cfg, const char *key, const char **out_val);

/**
 * Look up @p key as a boolean. Accepted spellings in lower, upper, or
 * capitalized form: @c true/@c false, @c 1/@c 0, @c yes/@c no, @c on/@c off.
 * File values are trimmed before matching; environment values are taken
 * verbatim.
 *
 * @param cfg      Source to query.
 * @param key      Key to look up.
 * @param out_val  Receives the parsed boolean.
 * @return         0 on success, -KITH_ENOENT if @p key is absent, -KITH_EINVAL
 *                 if @p cfg or @p key is NULL, or if the value is not a
 *                 recognized boolean spelling.
 * @thread_safety safe
 * @ownership caller — @p cfg and @p key are borrowed for the call only.
 */
[[nodiscard]] KITH_API int
kith_config_bool(const kith_config_t *cfg, const char *key, bool *out_val);

/**
 * Look up @p key as an unsigned 16-bit integer in [@p min, @p max]. The value
 * is parsed as base 10; a value that parses but falls outside the range is
 * reported as a range error distinct from a parse error.
 *
 * @param cfg      Source to query.
 * @param key      Key to look up.
 * @param min      Inclusive lower bound of the accepted range.
 * @param max      Inclusive upper bound of the accepted range.
 * @param out_val  Receives the parsed value.
 * @return         0 on success, -KITH_ENOENT if @p key is absent, -KITH_EINVAL
 *                 if @p cfg or @p key is NULL or the value is not a base-10
 *                 integer,
 *                 -KITH_ERANGE if @p min > @p max, the parsed value is outside
 *                 [@p min, @p max], or its magnitude exceeds the destination
 *                 type.
 * @thread_safety safe
 * @ownership caller — @p cfg and @p key are borrowed for the call only.
 */
[[nodiscard]] KITH_API int kith_config_u16(
    const kith_config_t *cfg, const char *key, uint16_t min, uint16_t max, uint16_t *out_val);

/**
 * Look up @p key as an unsigned 32-bit integer in [@p min, @p max]. See
 * kith_config_u16 for the parse and range semantics.
 *
 * @param cfg      Source to query.
 * @param key      Key to look up.
 * @param min      Inclusive lower bound of the accepted range.
 * @param max      Inclusive upper bound of the accepted range.
 * @param out_val  Receives the parsed value.
 * @return         0 on success, -KITH_ENOENT if @p key is absent, -KITH_EINVAL
 *                 if @p cfg or @p key is NULL or the value is not a base-10
 *                 integer,
 *                 -KITH_ERANGE if @p min > @p max, the parsed value is outside
 *                 [@p min, @p max], or its magnitude exceeds the destination
 *                 type.
 * @thread_safety safe
 * @ownership caller — @p cfg and @p key are borrowed for the call only.
 */
[[nodiscard]] KITH_API int kith_config_u32(
    const kith_config_t *cfg, const char *key, uint32_t min, uint32_t max, uint32_t *out_val);

/**
 * Look up @p key as an unsigned 64-bit integer in [@p min, @p max]. See
 * kith_config_u16 for the parse and range semantics.
 *
 * @param cfg      Source to query.
 * @param key      Key to look up.
 * @param min      Inclusive lower bound of the accepted range.
 * @param max      Inclusive upper bound of the accepted range.
 * @param out_val  Receives the parsed value.
 * @return         0 on success, -KITH_ENOENT if @p key is absent, -KITH_EINVAL
 *                 if @p cfg or @p key is NULL or the value is not a base-10
 *                 integer,
 *                 -KITH_ERANGE if @p min > @p max, the parsed value is outside
 *                 [@p min, @p max], or its magnitude exceeds the destination
 *                 type.
 * @thread_safety safe
 * @ownership caller — @p cfg and @p key are borrowed for the call only.
 */
[[nodiscard]] KITH_API int kith_config_u64(
    const kith_config_t *cfg, const char *key, uint64_t min, uint64_t max, uint64_t *out_val);

/** @} */

#endif /* KITH_CONFIG_CONFIG_H */
