#ifndef KITH_UTIL_RNG_H
#define KITH_UTIL_RNG_H

#include <stdint.h>

#include "kith/api.h"
#include "kith/types.h"

/**
 * Deterministic pseudo-random number service.
 *
 * A generator produces a fixed sequence of 64-bit words from a seed; the
 * same seed always yields the same words on every platform, compiler, and
 * run, which is what makes seeded simulation replayable (see
 * docs/architecture/adr/0014-deterministic-simulation-contract.md). The
 * algorithm is integer-only: no floating-point state, no system entropy,
 * no hidden reseeding.
 *
 * Independent streams come from kith_rng_create_stream: a stream's whole
 * sequence is a pure function of the parent's derivation seed and the
 * caller's stream key, so opening one stream never perturbs another
 * stream's output and streams may be created in any order. Keys must be
 * stable identities chosen by the composition root (a model id, a name
 * hash), not values derived from creation order.
 *
 * A handle is owned by one execution context: draw calls mutate the state
 * and are not synchronized. Snapshot/restore via kith_rng_state_save and
 * kith_rng_state_restore captures the complete generator state, including
 * the derivation seed, so a checkpointed handle can be resumed, or can
 * derive further streams identically after a replay rewind.
 */

/**
 * @addtogroup kith_util
 * @{
 */

/** Opaque generator handle. */
typedef struct kith_rng kith_rng_t;

/**
 * Creation parameters. Size-versioned: callers set @p size to
 * sizeof(kith_rng_params_t) and @p abi_version to KITH_ABI_VERSION at
 * their compile time; the runtime rejects structs from an incompatible
 * generation or an undersized size. Future additive fields occupy the
 * reserved slots so the layout below stays stable across generations.
 */
struct kith_rng_params
{
    /** Must be sizeof(kith_rng_params_t). */
    uint32_t size;
    /** Must be KITH_ABI_VERSION (from kith/version.h). */
    uint32_t abi_version;

    /**
     * Root seed. Every value produces a valid, distinct sequence,
     * including zero.
     */
    uint64_t seed;

    /** Reserved for future additive fields. Must be zero-filled. */
    void *reserved[8];
};

/** Alias of struct kith_rng_params. */
typedef struct kith_rng_params kith_rng_params_t;

/**
 * Complete captured generator state: the derivation seed plus the four
 * algorithm state words. Restoring a saved value reproduces the exact
 * sequence position of the source handle, and further streams derived
 * from the restored handle match those the source would have produced.
 *
 * This is an exposed-layout value type (like struct iovec): the field set
 * is part of the public contract and stays stable, so snapshots can be
 * compared, hashed, and serialized directly.
 */
struct kith_rng_state
{
    /** Seed the handle was created from or derived with. */
    uint64_t derivation_seed;

    /** Raw algorithm state words. */
    uint64_t words[4];
};

/** Alias of struct kith_rng_state. */
typedef struct kith_rng_state kith_rng_state_t;

/**
 * Build a generator from @p params.
 *
 * @param params   Creation parameters; @c size and @c abi_version must
 *                 match the runtime generation.
 * @param alloc    Allocator for the new handle, used again when
 *                 kith_rng_destroy frees it. NULL selects the default
 *                 allocator; a supplied allocator is validated (see
 *                 kith_allocator_t) and must outlive the handle.
 * @param out_rng  Receives the new handle on success.
 * @return         0 on success, negative kith_error on failure:
 *                 - -KITH_EINVAL if @p out_rng or @p params is NULL, or
 *                   @p alloc is missing an operation,
 *                 - -KITH_EABIVER if @p params or @p alloc has an
 *                   incompatible abi_version,
 *                 - -KITH_ESIZE if @p params or @p alloc has an undersized
 *                   size,
 *                 - -KITH_ENOMEM on allocation failure.
 * @thread_safety unsafe — must not be called concurrently with another
 *                kith_rng_create on the same @p out_rng slot.
 * @ownership callee — the returned handle is freed by the caller with
 *           kith_rng_destroy.
 */
[[nodiscard]] KITH_API int kith_rng_create(const kith_rng_params_t *params,
                                           const kith_allocator_t *alloc,
                                           kith_rng_t **out_rng);

/**
 * Release a generator and all storage it owns. Passing NULL is a no-op.
 *
 * @param rng Handle to release; NULL is a no-op.
 * @thread_safety unsafe — no draw, save, or restore call may be in flight
 *                on @p rng when this is called.
 * @ownership callee — @p rng is consumed and freed by the call.
 */
KITH_API void kith_rng_destroy(kith_rng_t *rng);

/**
 * Derive an independent stream from @p parent keyed by @p stream_key.
 *
 * The child's entire sequence is a function of @p parent's derivation seed
 * and @p stream_key alone: creating it does not advance the parent, two
 * children created in either order produce their own identical sequences,
 * and distinct keys yield distinct states. The key must be a stable
 * identity (a model id, a name hash); keys derived from creation order
 * reintroduce order dependence through the back door. Children of
 * children derive from the child's own derivation seed the same way.
 *
 * @param parent   Handle to derive from; borrowed for the call only. NULL
 *                 is rejected.
 * @param stream_key Caller-chosen stable identity for the stream.
 * @param alloc    Allocator for the new handle, used again when
 *                 kith_rng_destroy frees it. NULL selects the default
 *                 allocator; a supplied allocator is validated (see
 *                 kith_allocator_t) and must outlive the handle.
 * @param out_rng  Receives the new handle on success.
 * @return         0 on success, negative kith_error on failure:
 *                 - -KITH_EINVAL if @p parent or @p out_rng is NULL, or
 *                   @p alloc is missing an operation,
 *                 - -KITH_EABIVER if @p alloc has an incompatible
 *                   abi_version,
 *                 - -KITH_ESIZE if @p alloc has an undersized size,
 *                 - -KITH_ENOMEM on allocation failure.
 * @thread_safety unsafe — concurrent calls sharing @p parent are not
 *                synchronized, though distinct parents may proceed in
 *                parallel; as with kith_rng_create, only one call may
 *                write a given @p out_rng slot.
 * @ownership callee — the returned handle is freed by the caller with
 *           kith_rng_destroy.
 */
[[nodiscard]] KITH_API int kith_rng_create_stream(const kith_rng_t *parent,
                                                  uint64_t stream_key,
                                                  const kith_allocator_t *alloc,
                                                  kith_rng_t **out_rng);

/**
 * Draw the next raw 64-bit word and advance the state. This is the
 * primitive every other draw consumes exactly one word from.
 *
 * @param rng Generator handle.
 * @return The next word, or 0 if @p rng is NULL.
 * @thread_safety unsafe — draws mutate the shared state; a handle belongs
 *                to one execution context.
 * @ownership caller — @p rng is borrowed for the call only.
 */
[[nodiscard]] KITH_API uint64_t kith_rng_next(kith_rng_t *rng);

/**
 * Draw a value uniformly from [0, @p bound).
 *
 * Uses rejection sampling against a computed threshold, so every residue
 * class is exactly equally likely — plain modulo over the raw word would
 * bias low values. Bounds below 2 return 0 and leave the state untouched;
 * the number of words consumed for a given bound sequence is itself
 * deterministic.
 *
 * @param rng   Generator handle.
 * @param bound Exclusive upper limit.
 * @return A value below @p bound, or 0 if @p rng is NULL or @p bound is
 *         below 2.
 * @thread_safety unsafe — draws mutate the shared state; a handle belongs
 *                to one execution context.
 * @ownership caller — @p rng is borrowed for the call only.
 */
[[nodiscard]] KITH_API uint64_t kith_rng_next_below(kith_rng_t *rng, uint64_t bound);

/**
 * Draw a double uniformly from [0, 1).
 *
 * Implemented as a 53-bit integer scaled by an exact power of two, so the
 * result requires no rounding and is bit-identical on every IEEE-754
 * platform regardless of excess precision or contraction settings.
 *
 * @param rng Generator handle.
 * @return The drawn unit-interval value, or 0.0 if @p rng is NULL.
 * @thread_safety unsafe — draws mutate the shared state; a handle belongs
 *                to one execution context.
 * @ownership caller — @p rng is borrowed for the call only.
 */
[[nodiscard]] KITH_API double kith_rng_next_unit(kith_rng_t *rng);

/**
 * Report the derivation seed the handle carries: the root seed for a
 * handle built by kith_rng_create, the mixed key-derived seed for a
 * stream. Two handles with equal derivation seeds and equal snapshot
 * states produce identical sequences.
 *
 * @param rng Generator handle.
 * @return The derivation seed, or 0 if @p rng is NULL.
 * @thread_safety safe — draws, snapshot saves, and stream derivation never
 *                mutate it; kith_rng_state_restore sets it, and its unsafe
 *                contract keeps that write serialized.
 * @ownership caller — @p rng is borrowed for the call only.
 */
[[nodiscard]] KITH_API uint64_t kith_rng_derivation_seed(const kith_rng_t *rng);

/**
 * Capture the complete generator state.
 *
 * @param rng        Handle to capture. NULL is rejected.
 * @param out_state  Receives the state.
 * @return           0 on success, -KITH_EINVAL if @p rng or @p out_state
 *                   is NULL.
 * @thread_safety unsafe — the state mutates under concurrent draws.
 * @ownership caller — @p rng is borrowed for the call; @p out_state is
 *           written and then owned by the caller.
 */
[[nodiscard]] KITH_API int kith_rng_state_save(const kith_rng_t *rng, kith_rng_state_t *out_state);

/**
 * Restore a captured state, rewinding or fast-forwarding the
 * sequence to the captured position.
 *
 * @param rng     Handle to modify. NULL is rejected.
 * @param state   State filled by kith_rng_state_save.
 * @return        0 on success, negative kith_error on failure:
 *                - -KITH_EINVAL if @p rng or @p state is NULL, or the
 *                  captured state is the all-zero invalid state (the
 *                  algorithm's absorbing point, reachable only through
 *                  corruption).
 * @thread_safety unsafe — the state mutates under concurrent draws.
 * @ownership caller — both arguments are borrowed for the call only.
 */
[[nodiscard]] KITH_API int kith_rng_state_restore(kith_rng_t *rng, const kith_rng_state_t *state);

/** @} */

#endif /* KITH_UTIL_RNG_H */
