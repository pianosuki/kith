/* Deterministic pseudo-random number service: xoshiro256** core with
 * splitmix64 seed expansion and key-derived stream splitting. The public
 * contract is include/kith/util/rng.h. */

#include "kith/util/rng.h"

#include "kith/version.h"

/* splitmix64 finalizer: bijective 64-bit mixer used both to expand a
 * derivation seed into fresh algorithm state and to fold stream keys into
 * child seeds. Its bijection is what makes distinct stream keys map to
 * distinct states, so order-independence holds by construction. */
static uint64_t mix64(uint64_t z)
{
    z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
    z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
    return z ^ (z >> 31);
}

static uint64_t rotl45(uint64_t x)
{
    return (x << 45U) | (x >> 19U);
}

struct kith_rng
{
    const kith_allocator_t *allocator;
    uint64_t derivation_seed;
    uint64_t words[4];
};

/* Fill the algorithm state by splitmix expansion of the derivation seed.
 * Expansion from an arbitrary seed cannot produce the all-zero state: the
 * counter starts past zero and mix64 maps only zero to zero. */
static void rng_expand(struct kith_rng *rng)
{
    uint64_t z = rng->derivation_seed;
    for (unsigned i = 0; i < 4U; i++)
    {
        z += 0x9E3779B97F4A7C15ULL;
        rng->words[i] = mix64(z);
    }
}

/* Returns KITH_OK when the params are usable, otherwise the error enumerator
 * the create call must report: the caller-supplied struct is validated one
 * field at a time so each failure class keeps its dedicated code. */
static kith_error_t params_error(const kith_rng_params_t *params)
{
    if (params == nullptr)
    {
        return KITH_EINVAL;
    }
    if (params->abi_version != KITH_ABI_VERSION)
    {
        return KITH_EABIVER;
    }
    if (params->size < sizeof(kith_rng_params_t))
    {
        return KITH_ESIZE;
    }
    return KITH_OK;
}

/* Advance the state and emit one word. Callers guarantee rng != NULL. */
static uint64_t rng_draw(struct kith_rng *rng)
{
    const uint64_t t = rng->words[1] << 17U;
    rng->words[2] ^= rng->words[0];
    rng->words[3] ^= rng->words[1];
    rng->words[1] ^= rng->words[2];
    rng->words[0] ^= rng->words[3];
    rng->words[2] ^= t;
    rng->words[3] = rotl45(rng->words[3]);
    return rotl45(rng->words[1] * 5U) * 9U;
}

[[nodiscard]] KITH_API int kith_rng_create(const kith_rng_params_t *params,
                                           const kith_allocator_t *alloc,
                                           kith_rng_t **out_rng)
{
    if (out_rng == nullptr)
    {
        return kith_error_return(KITH_EINVAL);
    }
    *out_rng = nullptr;
    const kith_error_t params_rc = params_error(params);
    if (params_rc != 0)
    {
        return kith_error_return(params_rc);
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
    kith_rng_t *rng = kith_alloc(allocator, sizeof(*rng));
    if (rng == nullptr)
    {
        return kith_error_return(KITH_ENOMEM);
    }
    rng->allocator = allocator;
    rng->derivation_seed = params->seed;
    rng_expand(rng);
    *out_rng = rng;
    return 0;
}

KITH_API void kith_rng_destroy(kith_rng_t *rng)
{
    if (rng == nullptr)
    {
        return;
    }
    kith_free(rng->allocator, rng);
}

[[nodiscard]] KITH_API int kith_rng_create_stream(const kith_rng_t *parent,
                                                  uint64_t stream_key,
                                                  const kith_allocator_t *alloc,
                                                  kith_rng_t **out_rng)
{
    if (parent == nullptr || out_rng == nullptr)
    {
        return kith_error_return(KITH_EINVAL);
    }
    *out_rng = nullptr;
    if (alloc != nullptr)
    {
        const kith_error_t alloc_rc = kith_allocator_check(alloc);
        if (alloc_rc != 0)
        {
            return kith_error_return(alloc_rc);
        }
    }
    const kith_allocator_t *allocator = alloc != nullptr ? alloc : kith_allocator_default();
    kith_rng_t *rng = kith_alloc(allocator, sizeof(*rng));
    if (rng == nullptr)
    {
        return kith_error_return(KITH_ENOMEM);
    }
    rng->allocator = allocator;
    /* The child seed folds the key into the parent's derivation seed:
     * sibling streams are independent of creation order, and snapshots
     * carry the seed, so sub-streams derive identically after a
     * restore. */
    rng->derivation_seed = mix64(parent->derivation_seed ^ mix64(stream_key));
    rng_expand(rng);
    *out_rng = rng;
    return 0;
}

[[nodiscard]] KITH_API uint64_t kith_rng_next(kith_rng_t *rng)
{
    if (rng == nullptr)
    {
        return 0;
    }
    return rng_draw(rng);
}

[[nodiscard]] KITH_API uint64_t kith_rng_next_below(kith_rng_t *rng, uint64_t bound)
{
    if (rng == nullptr || bound < 2U)
    {
        return 0;
    }
    if ((bound & (bound - 1U)) == 0U)
    {
        return rng_draw(rng) & (bound - 1U);
    }
    /* Reject words below (2^64 mod bound); the accepted range is exactly
     * divisible by bound, so the modulo is unbiased. */
    const uint64_t threshold = (UINT64_MAX - bound + 1U) % bound;
    uint64_t r = rng_draw(rng);
    while (r < threshold)
    {
        r = rng_draw(rng);
    }
    return r % bound;
}

[[nodiscard]] KITH_API double kith_rng_next_unit(kith_rng_t *rng)
{
    if (rng == nullptr)
    {
        return 0.0;
    }
    /* A 53-bit integer times an exact power of two: representable without
     * rounding, so the result is bit-identical on every IEEE-754 target. */
    return (double)(rng_draw(rng) >> 11) * 0x1.0p-53;
}

[[nodiscard]] KITH_API uint64_t kith_rng_derivation_seed(const kith_rng_t *rng)
{
    if (rng == nullptr)
    {
        return 0;
    }
    return rng->derivation_seed;
}

[[nodiscard]] KITH_API int kith_rng_state_save(const kith_rng_t *rng, kith_rng_state_t *out_state)
{
    if (rng == nullptr || out_state == nullptr)
    {
        return kith_error_return(KITH_EINVAL);
    }
    out_state->derivation_seed = rng->derivation_seed;
    for (unsigned i = 0; i < 4U; i++)
    {
        out_state->words[i] = rng->words[i];
    }
    return 0;
}

[[nodiscard]] KITH_API int kith_rng_state_restore(kith_rng_t *rng, const kith_rng_state_t *state)
{
    if (rng == nullptr || state == nullptr)
    {
        return kith_error_return(KITH_EINVAL);
    }
    bool all_zero = state->derivation_seed == 0;
    for (unsigned i = 0; i < 4U && all_zero; i++)
    {
        all_zero = state->words[i] == 0;
    }
    if (all_zero)
    {
        return kith_error_return(KITH_EINVAL);
    }
    rng->derivation_seed = state->derivation_seed;
    for (unsigned i = 0; i < 4U; i++)
    {
        rng->words[i] = state->words[i];
    }
    return 0;
}
