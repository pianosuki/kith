/* Deterministic RNG service contract tests: golden cross-checked against an
 * independent implementation of the published algorithm, stream
 * order-independence, exact-uniformity bounds, and snapshot round-trips.
 * Each contract lives in its own helper; failure codes are namespaced per
 * helper (helper base + offset) so a return value pinpoints the assertion. */

#include <string.h>

#include "kith/util/rng.h"
#include "kith/version.h"

/* Sentinel address handed to create calls as the out slot, so a failure
 * path that forgets to clear it is observable. Never dereferenced. */
static uint64_t sentinel;

static kith_rng_params_t valid_params(uint64_t seed)
{
    return (kith_rng_params_t)
    {
        .size = sizeof(kith_rng_params_t), .abi_version = KITH_ABI_VERSION
        , .seed = seed
    };
}

static int params_rejected(kith_rng_params_t params, kith_error_t want_code)
{
    kith_rng_t *rng = (kith_rng_t *)&sentinel;
    if (kith_rng_create(&params, nullptr, &rng) != kith_error_return(want_code))
    {
        return 1;
    }
    return rng != nullptr;
}

static int expect_draws(kith_rng_t *rng, const uint64_t *want, unsigned count)
{
    for (unsigned i = 0; i < count; i++)
    {
        if (kith_rng_next(rng) != want[i])
        {
            return 1;
        }
    }
    return 0;
}

/* Handles shared across contract groups. */
static kith_rng_t *root_handle;
static kith_rng_t *seven_handle;
static kith_rng_t *nine_handle;

static int test_creation_validation(void)
{
    kith_rng_t *rng = (kith_rng_t *)&sentinel;
    if (kith_rng_create(nullptr, nullptr, &rng) != kith_error_return(KITH_EINVAL))
    {
        return 1;
    }
    if (params_rejected((kith_rng_params_t) { .size = 8U, .abi_version = KITH_ABI_VERSION },
                        KITH_ESIZE))
    {
        return 2;
    }
    if (params_rejected(
            (kith_rng_params_t) {
                .size = sizeof(kith_rng_params_t), .abi_version = KITH_ABI_VERSION
                +1U
            },
            KITH_EABIVER))
    {
        return 3;
    }
    kith_rng_params_t good = valid_params(0x12345678);
    /* An allocator missing most of its struct is undersized against the
     * allocator contract, so the create call rejects it before allocating. */
    const kith_allocator_t undersized = {.size = 8U, .abi_version = KITH_ABI_VERSION };
    if (kith_rng_create(&good, &undersized, &rng) != kith_error_return(KITH_ESIZE))
    {
        return 4;
    }
    if (kith_rng_create_stream(nullptr, 0U, nullptr, &rng) != kith_error_return(KITH_EINVAL))
    {
        return 5;
    }
    return 0;
}

static int test_golden_root_and_streams(void)
{
    static const uint64_t root_want[] = {0xeb639e13b1a90fddULL,
                                         0x54ccc6305e3ad5f0ULL,
                                         0x1c5402dc4d2b844fULL,
                                         0x5c416e3bc4216c70ULL,
                                         0x5c94adbffe8197eeULL};
    static const uint64_t seven_want[] = {0x37e61007e41a4e69ULL,
                                          0xd4a36e2a05ef175aULL,
                                          0x5baf1a79171dc23dULL,
                                          0xa138fc38c694b152ULL,
                                          0x2a86e59e64d3f773ULL};
    static const uint64_t nine_want[] = {0xdce572a9cc47673cULL,
                                         0x69f647be375d4765ULL,
                                         0x91c27433b585b6c6ULL,
                                         0xdbd3f6c70c7c889ULL,
                                         0xbdffcea191def0daULL};

    kith_rng_params_t params = valid_params(0x12345678);
    if (kith_rng_create(&params, nullptr, &root_handle) != 0)
    {
        return 1;
    }
    if (kith_rng_derivation_seed(root_handle) != 0x12345678ULL ||
        expect_draws(root_handle, root_want, 5U))
    {
        return 2;
    }
    if (kith_rng_create_stream(root_handle, 7U, nullptr, &seven_handle) != 0 ||
        kith_rng_create_stream(root_handle, 9U, nullptr, &nine_handle) != 0)
    {
        return 3;
    }
    if (kith_rng_derivation_seed(seven_handle) != 0x12977f407a532148ULL ||
        kith_rng_derivation_seed(nine_handle) != 0x752d74c7eaeba1dbULL)
    {
        return 4;
    }
    if (expect_draws(seven_handle, seven_want, 5U) || expect_draws(nine_handle, nine_want, 5U))
    {
        return 5;
    }
    /* Creating streams consumed nothing from the parent. */
    if (kith_rng_next(root_handle) != 0x3deed82e62824625ULL)
    {
        return 6;
    }
    return 0;
}

static int test_stream_order_independence(void)
{
    static const uint64_t seven_want[] = {0x37e61007e41a4e69ULL,
                                          0xd4a36e2a05ef175aULL,
                                          0x5baf1a79171dc23dULL,
                                          0xa138fc38c694b152ULL,
                                          0x2a86e59e64d3f773ULL};
    static const uint64_t nine_want[] = {0xdce572a9cc47673cULL,
                                         0x69f647be375d4765ULL,
                                         0x91c27433b585b6c6ULL,
                                         0xdbd3f6c70c7c889ULL,
                                         0xbdffcea191def0daULL};

    kith_rng_params_t params = valid_params(0x12345678);
    kith_rng_t *fresh = nullptr;
    kith_rng_t *nine_rev = nullptr;
    kith_rng_t *seven_rev = nullptr;
    if (kith_rng_create(&params, nullptr, &fresh) != 0 ||
        kith_rng_create_stream(fresh, 9U, nullptr, &nine_rev) != 0 ||
        kith_rng_create_stream(fresh, 7U, nullptr, &seven_rev) != 0)
    {
        return 1;
    }
    if (kith_rng_derivation_seed(seven_rev) != 0x12977f407a532148ULL ||
        expect_draws(seven_rev, seven_want, 5U) ||
        kith_rng_derivation_seed(nine_rev) != 0x752d74c7eaeba1dbULL ||
        expect_draws(nine_rev, nine_want, 5U))
    {
        return 2;
    }
    kith_rng_destroy(seven_rev);
    kith_rng_destroy(nine_rev);
    kith_rng_destroy(fresh);
    return 0;
}

static int test_bounded_draws(kith_rng_t *seven)
{
    kith_rng_state_t before;
    kith_rng_state_t after;
    static const uint64_t below_want[] = {39U, 878U, 579U, 829U, 993U, 171U, 620U, 44U};

    if (kith_rng_next_below(nullptr, 10U) != 0)
    {
        return 1;
    }
    if (kith_rng_state_save(seven, &before) != 0)
    {
        return 2;
    }
    if (kith_rng_next_below(seven, 0U) != 0 || kith_rng_next_below(seven, 1U) != 0)
    {
        return 3;
    }
    if (kith_rng_state_save(seven, &after) != 0 || memcmp(&before, &after, sizeof(before)) != 0)
    {
        return 4;
    }
    for (unsigned i = 0; i < 8U; i++)
    {
        if (kith_rng_next_below(seven, 1000U) != below_want[i])
        {
            return 5;
        }
    }
    for (unsigned i = 0; i < 100U; i++)
    {
        if (kith_rng_next_below(seven, 16U) >= 16U)
        {
            return 6;
        }
    }
    return 0;
}

static int test_unit_doubles(void)
{
    static const uint64_t unit_want[] = {
        0x3fcbf30803f20d24ULL, 0x3fea946dc540bde2ULL, 0x3fd6ebc69e45c770ULL};
    kith_rng_t *unit_src = nullptr;
    if (kith_rng_create_stream(root_handle, 7U, nullptr, &unit_src) != 0)
    {
        return 1;
    }
    for (unsigned i = 0; i < 3U; i++)
    {
        double v = kith_rng_next_unit(unit_src);
        uint64_t bits;
        memcpy(&bits, &v, sizeof(bits));
        if (bits != unit_want[i] || !(v >= 0.0 && v < 1.0))
        {
            kith_rng_destroy(unit_src);
            return 2;
        }
    }
    kith_rng_destroy(unit_src);
    double zero = kith_rng_next_unit(nullptr);
    uint64_t zero_bits;
    memcpy(&zero_bits, &zero, sizeof(zero_bits));
    if (zero_bits != 0u)
    {
        return 3;
    }
    return 0;
}

static int test_snapshot_restore(void)
{
    kith_rng_params_t params = valid_params(42U);
    kith_rng_t *src = nullptr;
    kith_rng_t *resumed = nullptr;
    kith_rng_t *twin = nullptr;
    kith_rng_t *forked = nullptr;
    kith_rng_t *twin_fork = nullptr;
    kith_rng_state_t captured;
    const kith_rng_state_t zero = {.derivation_seed = 0, .words = {0, 0, 0, 0}};
    int rc = 0;

    if (kith_rng_state_save(nullptr, &captured) != kith_error_return(KITH_EINVAL) ||
        kith_rng_state_restore(nullptr, &captured) != kith_error_return(KITH_EINVAL) ||
        kith_rng_state_restore(src, nullptr) != kith_error_return(KITH_EINVAL))
    {
        return 1;
    }
    if (kith_rng_create(&params, nullptr, &src) != 0 ||
        kith_rng_create(&params, nullptr, &resumed) != 0 ||
        kith_rng_create(&params, nullptr, &twin) != 0)
    {
        rc = 2;
        goto done;
    }
    for (unsigned i = 0; i < 3U; i++)
    {
        (void)kith_rng_next(src);
    }
    if (kith_rng_state_save(src, &captured) != 0)
    {
        rc = 3;
        goto done;
    }
    const uint64_t first = kith_rng_next(src);
    const uint64_t second = kith_rng_next(src);
    if (kith_rng_state_restore(resumed, &captured) != 0 || kith_rng_next(resumed) != first ||
        kith_rng_next(resumed) != second)
    {
        rc = 4;
        goto done;
    }
    if (kith_rng_state_restore(resumed, &zero) != kith_error_return(KITH_EINVAL))
    {
        rc = 5;
        goto done;
    }
    /* Sub-streams derived from equivalent states match draw-for-draw. */
    if (kith_rng_state_restore(src, &captured) != 0 ||
        kith_rng_create_stream(src, 555U, nullptr, &forked) != 0 ||
        kith_rng_create_stream(twin, 555U, nullptr, &twin_fork) != 0 ||
        kith_rng_next(forked) != kith_rng_next(twin_fork))
    {
        rc = 6;
        goto done;
    }
    if (kith_rng_derivation_seed(nullptr) != 0)
    {
        rc = 7;
    }
done:
    kith_rng_destroy(twin_fork);
    kith_rng_destroy(forked);
    kith_rng_destroy(twin);
    kith_rng_destroy(resumed);
    kith_rng_destroy(src);
    return rc;
}

int main(void)
{
    int rc = test_creation_validation();
    if (rc == 0)
    {
        rc = 10 * test_golden_root_and_streams();
    }
    if (rc == 0)
    {
        rc = 20 * test_stream_order_independence();
    }
    if (rc == 0 && seven_handle != nullptr)
    {
        rc = 30 * test_bounded_draws(seven_handle);
    }
    if (rc == 0)
    {
        rc = 40 * test_unit_doubles();
    }
    if (rc == 0)
    {
        rc = 50 * test_snapshot_restore();
    }
    kith_rng_destroy(nine_handle);
    kith_rng_destroy(seven_handle);
    kith_rng_destroy(nullptr);
    kith_rng_destroy(root_handle);
    return rc;
}
