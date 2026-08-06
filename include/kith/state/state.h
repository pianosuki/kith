#ifndef KITH_STATE_STATE_H
#define KITH_STATE_STATE_H

#include <stddef.h>
#include <stdint.h>

#include "kith/api.h"
#include "kith/types.h"

/**
 * Redis-backed asynchronous state store.
 *
 * A state handle wraps a non-blocking Redis connection (via hiredis async)
 * and integrates with a reactor for event-driven I/O. Operations —
 * SET, GET, DEL, EXISTS — are submitted asynchronously; the result
 * arrives via a caller-supplied callback invoked from the reactor's event
 * loop.
 *
 * Keys and values are opaque byte buffers with explicit lengths, not
 * null-terminated strings. This supports binary data, structured
 * serialization formats, or plain text at the caller's discretion.
 *
 * Pipelining is handled automatically: multiple commands submitted in a
 * single reactor iteration are coalesced into a single write by hiredis.
 * Reply matching respects RESP's strict request-response ordering.
 *
 * The state store does not own a reactor — it borrows one from the
 * composition root for the lifetime of the state handle. The reactor
 * thread dispatches reply callbacks.
 */

/* Forward declaration — the full type lives in kith/reactor/reactor.h. */
typedef struct kith_reactor kith_reactor_t;

/**
 * @defgroup kith_state State Store
 * @{
 */

/**
 * Opaque state store handle.
 *
 * @ownership callee — created by kith_state_create, destroyed by
 *           kith_state_destroy. Owns the hiredis async context, the
 *           reactor event adapter, and the command queue.
 */
typedef struct kith_state kith_state_t;

/**
 * Default values used when the corresponding field in
 * @c kith_state_params_t is set to zero.
 */
enum kith_state_default : unsigned int
{
    /** Default Redis host (params.host = NULL → this). */
    KITH_STATE_DEFAULT_HOST = 0u,
    /** Default Redis TCP port (params.port = 0 → this). */
    KITH_STATE_DEFAULT_PORT = 6379u,
    /** Default connect timeout in milliseconds (params.timeout_ms = 0 → this). */
    KITH_STATE_DEFAULT_TIMEOUT_MS = 5000u,
};

/**
 * Creation parameters.  Size-versioned: callers set @p size to
 * sizeof(kith_state_params_t) and @p abi_version to KITH_ABI_VERSION at
 * their compile time; the runtime rejects structs from an incompatible
 * generation or an undersized size.  A field set to 0 selects the
 * corresponding @c kith_state_default value (except host — NULL maps to
 * the hiredis default, "127.0.0.1").
 */
struct kith_state_params
{
    /** Must be sizeof(kith_state_params_t). */
    uint32_t size;
    /** Must be KITH_ABI_VERSION (from kith/version.h). */
    uint32_t abi_version;

    /**
     * Redis server hostname or IP address. NULL selects the hiredis
     * default ("127.0.0.1"). The string is borrowed; the caller must
     * keep it alive for the lifetime of the state handle.
     */
    const char *host;

    /**
     * Redis server TCP port. 0 selects KITH_STATE_DEFAULT_PORT (6379).
     */
    uint16_t port;

    /**
     * Redis connection timeout in milliseconds. 0 selects
     * KITH_STATE_DEFAULT_TIMEOUT_MS (5000).
     */
    uint32_t timeout_ms;

    /**
     * Optional prefix prepended to every key. If non-NULL, the prefix
     * is prepended verbatim; the total key resolved by the store is
     * prefix_bytes + key_bytes. This provides namespace isolation
     * without baking a prefix convention into the library. The pointer
     * is borrowed; the caller must keep it alive for the lifetime of
     * the state handle.
     */
    const char *key_prefix;

    /**
     * Length of @p key_prefix in bytes. Ignored when @p key_prefix is
     * NULL. A non-NULL prefix with zero length is treated as if the
     * prefix were NULL (no prefix applied).
     */
    uint32_t key_prefix_len;

    /** Reserved for future additive fields. Must be zero-filled. */
    void *reserved[8];
};

/** Alias of struct kith_state_params. */
typedef struct kith_state_params kith_state_params_t;

/**
 * Reply delivered to the caller's completion callback.
 *
 * Pointers inside this struct (@p err_str, @p value) are valid only for
 * the duration of the callback. The caller must copy any data it needs
 * to retain beyond the callback. This is an exposed-layout value type.
 */
struct kith_state_reply
{
    /**
     * Operation status. 0 on success, negative kith_error on failure.
     * A nil Redis reply (GET on a nonexistent key) is not an error:
     * @p status is 0 and @p value is NULL.
     */
    int status;

    /**
     * Human-readable error string when @p status is non-zero, NULL
     * otherwise. Valid only during the callback.
     */
    const char *err_str;

    /**
     * Value payload for GET operations. NULL when the key does not
     * exist. Valid only during the callback.
     */
    const void *value;

    /** Length of @p value in bytes. 0 when @p value is NULL. */
    size_t value_len;

    /**
     * Existence flag for EXISTS operations. true if the key exists,
     * false otherwise. Undefined for other operations.
     */
    bool exists;

    /**
     * The @p user_data pointer supplied when the command was issued.
     * The caller uses this to correlate the reply with its own context.
     */
    void *user_data;
};

/** Alias of struct kith_state_reply. */
typedef struct kith_state_reply kith_state_reply_t;

/**
 * Completion callback invoked by the reactor when a Redis command
 * completes.
 *
 * @param reply The command result. All pointer fields are valid only for
 *              the duration of this call; copy any data that must
 *              outlive the callback.
 * @thread_safety unsafe — called on the reactor's event-loop thread.
 *                 The callback must not block or call back into the
 *                 reactor.
 */
typedef void (*kith_state_reply_fn)(kith_state_reply_t *reply);

/*---------------------------------------------------------------------------
 * lifecycle
 *-------------------------------------------------------------------------*/

/**
 * Create a state store handle.
 *
 * Initiates a non-blocking Redis connection via hiredis async. The
 * connection establishment is asynchronous; commands submitted before
 * the connect callback fires are queued internally by hiredis and
 * flushed once the connection is writable.
 *
 * @param params     Creation parameters. Must be non-NULL with a valid
 *                   @p size and @p abi_version.
 * @param reactor    Borrowed reactor handle. Must outlive the state
 *                   handle. Non-NULL.
 * @param alloc      Allocator for the state handle and every command
 *                   block the operations allocate, used again when
 *                   kith_state_destroy frees them. NULL selects the
 *                   default allocator; a supplied allocator is validated
 *                   (see kith_allocator_t) and must outlive the handle.
 * @param out_state  Receives the new handle on success.
 * @return           0 on success, negative kith_error on failure:
 *                   - -KITH_EINVAL if @p params, @p reactor, or
 *                     @p out_state is NULL, or @p alloc is missing an
 *                     operation,
 *                   - -KITH_EABIVER if @p params or @p alloc has an
 *                     incompatible abi_version,
 *                   - -KITH_ESIZE if @p params or @p alloc has an
 *                     undersized size,
 *                   - -KITH_ENOMEM on allocation failure,
 *                   - -KITH_EBUSY if the reactor cannot register the
 *                     connection's socket (registration table full).
 * @thread_safety unsafe — must not race with another kith_state_create
 *                on the same @p out_state slot.
 * @ownership callee — the returned handle is freed by the caller with
 *           kith_state_destroy.
 */
[[nodiscard]] KITH_API int kith_state_create(const kith_state_params_t *params,
                                             kith_reactor_t *reactor,
                                             const kith_allocator_t *alloc,
                                             kith_state_t **out_state);

/**
 * Release all resources held by @p state. Passing NULL is a no-op.
 *
 * Disconnects from Redis and frees the hiredis async context, the
 * reactor event adapter, and any pending command state. Pending
 * commands complete during the call with -KITH_ECONNRESET replies.
 *
 * @param state State store handle. NULL is a no-op.
 * @thread_safety unsafe — no operation may be in flight, including one
 *                whose submission task is queued but not yet drained on
 *                the reactor thread, and no callback may be running when
 *                this is called.
 * @ownership callee — @p state is consumed and freed by the call.
 */
KITH_API void kith_state_destroy(kith_state_t *state);

/*---------------------------------------------------------------------------
 * key-value operations
 *-------------------------------------------------------------------------*/

/**
 * Set a key to a value asynchronously.
 *
 * Issues a Redis SET command. The command is issued on the reactor
 * thread: the call validates its arguments, copies @p key and @p value,
 * and queues the work. The result arrives via @p callback, invoked from
 * the reactor's event loop. Failures discovered after the call returns —
 * a connection that is broken or lost, a command the server cannot
 * process — surface as a non-zero status in the completion callback
 * rather than the return value.
 *
 * @param state      State store handle. Must be non-NULL.
 * @param key        Key bytes. Non-NULL.
 * @param key_len    Length of @p key in bytes. Non-zero.
 * @param value      Value bytes. May be NULL only if @p value_len is 0.
 * @param value_len  Length of @p value in bytes. 0 stores an empty value.
 * @param callback   Completion callback. Non-NULL. Invoked once when the
 *                   command completes or fails.
 * @param user_data  Opaque pointer echoed in kith_state_reply.user_data.
 * @return           0 if the command was queued for the reactor thread,
 *                   negative kith_error on failure:
 *                   - -KITH_EINVAL if @p state, @p key, or @p callback
 *                     is NULL, or @p key_len is zero,
 *                   - -KITH_EOVERFLOW if @p key_len (with the configured
 *                     key prefix) plus @p value_len overflows the command
 *                     block arithmetic,
 *                   - -KITH_ENOMEM if the command cannot be allocated,
 *                   - -KITH_EBUSY if the reactor task queue is full.
 * @thread_safety safe-if the state handle is not being destroyed
 *                concurrently. May be called from any thread; callbacks
 *                always fire on the reactor thread.
 * @ownership caller — @p state, @p key, and @p value are borrowed for the call
 *           only; @p user_data is borrowed until the completion callback fires.
 */
[[nodiscard]] KITH_API int kith_state_set(kith_state_t *state,
                                          const void *key,
                                          size_t key_len,
                                          const void *value,
                                          size_t value_len,
                                          kith_state_reply_fn callback,
                                          void *user_data);

/**
 * Get the value of a key asynchronously.
 *
 * Issues a Redis GET command. The result arrives via @p callback,
 * invoked from the reactor's event loop. If the key does not exist,
 * the callback receives @p status = 0 with @p value = NULL.
 *
 * @param state      State store handle. Must be non-NULL.
 * @param key        Key bytes. Non-NULL.
 * @param key_len    Length of @p key in bytes. Non-zero.
 * @param callback   Completion callback. Non-NULL. Invoked once.
 * @param user_data  Opaque pointer echoed in kith_state_reply.user_data.
 * @return           0 if the command was queued for the reactor thread,
 *                   negative kith_error on failure:
 *                   - -KITH_EINVAL if @p state, @p key, or @p callback
 *                     is NULL, or @p key_len is zero,
 *                   - -KITH_EOVERFLOW if @p key_len (with the configured
 *                     key prefix) plus the command block overhead overflows
 *                     the block size arithmetic,
 *                   - -KITH_ENOMEM if the command cannot be allocated,
 *                   - -KITH_EBUSY if the reactor task queue is full.
 * @thread_safety safe-if the state handle is not being destroyed
 *                concurrently. May be called from any thread; callbacks
 *                always fire on the reactor thread.
 * @ownership caller — @p state and @p key are borrowed for the call only;
 *           @p user_data is borrowed until the completion callback fires.
 */
[[nodiscard]] KITH_API int kith_state_get(kith_state_t *state,
                                          const void *key,
                                          size_t key_len,
                                          kith_state_reply_fn callback,
                                          void *user_data);

/**
 * Delete a key asynchronously.
 *
 * Issues a Redis DEL command. The result arrives via @p callback,
 * invoked from the reactor's event loop. Deleting a nonexistent key
 * is not an error — the callback receives @p status = 0.
 *
 * @param state      State store handle. Must be non-NULL.
 * @param key        Key bytes. Non-NULL.
 * @param key_len    Length of @p key in bytes. Non-zero.
 * @param callback   Completion callback. Non-NULL. Invoked once.
 * @param user_data  Opaque pointer echoed in kith_state_reply.user_data.
 * @return           0 if the command was queued for the reactor thread,
 *                   negative kith_error on failure:
 *                   - -KITH_EINVAL if @p state, @p key, or @p callback
 *                     is NULL, or @p key_len is zero,
 *                   - -KITH_EOVERFLOW if @p key_len (with the configured
 *                     key prefix) plus the command block overhead overflows
 *                     the block size arithmetic,
 *                   - -KITH_ENOMEM if the command cannot be allocated,
 *                   - -KITH_EBUSY if the reactor task queue is full.
 * @thread_safety safe-if the state handle is not being destroyed
 *                concurrently. May be called from any thread; callbacks
 *                always fire on the reactor thread.
 * @ownership caller — @p state and @p key are borrowed for the call only;
 *           @p user_data is borrowed until the completion callback fires.
 */
[[nodiscard]] KITH_API int kith_state_del(kith_state_t *state,
                                          const void *key,
                                          size_t key_len,
                                          kith_state_reply_fn callback,
                                          void *user_data);

/**
 * Check whether a key exists asynchronously.
 *
 * Issues a Redis EXISTS command. The result arrives via @p callback,
 * invoked from the reactor's event loop. The @p exists field of the
 * reply indicates whether the key was present.
 *
 * @param state      State store handle. Must be non-NULL.
 * @param key        Key bytes. Non-NULL.
 * @param key_len    Length of @p key in bytes. Non-zero.
 * @param callback   Completion callback. Non-NULL. Invoked once.
 * @param user_data  Opaque pointer echoed in kith_state_reply.user_data.
 * @return           0 if the command was queued for the reactor thread,
 *                   negative kith_error on failure:
 *                   - -KITH_EINVAL if @p state, @p key, or @p callback
 *                     is NULL, or @p key_len is zero,
 *                   - -KITH_EOVERFLOW if @p key_len (with the configured
 *                     key prefix) plus the command block overhead overflows
 *                     the block size arithmetic,
 *                   - -KITH_ENOMEM if the command cannot be allocated,
 *                   - -KITH_EBUSY if the reactor task queue is full.
 * @thread_safety safe-if the state handle is not being destroyed
 *                concurrently. May be called from any thread; callbacks
 *                always fire on the reactor thread.
 * @ownership caller — @p state and @p key are borrowed for the call only;
 *           @p user_data is borrowed until the completion callback fires.
 */
[[nodiscard]] KITH_API int kith_state_exists(kith_state_t *state,
                                             const void *key,
                                             size_t key_len,
                                             kith_state_reply_fn callback,
                                             void *user_data);

/** @} */

#endif /* KITH_STATE_STATE_H */
