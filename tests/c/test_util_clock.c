/* Tick-clock and wall-clock contract tests: tick-driven conversions with
 * pinned floor behavior, overflow handling, and monotonicity of the wall
 * path. Failure codes are namespaced per helper. */

#include <stdint.h>

#include "kith/util/tick_clock.h"
#include "kith/util/wall_clock.h"
#include "kith/version.h"

/* Sentinel address handed to create calls as the out slot, so a failure
 * path that forgets to clear it is observable. Never dereferenced. */
static uint64_t sentinel;

static kith_tick_clock_params_t valid_params(uint32_t tick_hz)
{
    return (kith_tick_clock_params_t)
    {
        .size = sizeof(kith_tick_clock_params_t), .abi_version = KITH_ABI_VERSION
        , .initial_tick = 0, .tick_hz = tick_hz
    };
}

static int params_rejected(kith_tick_clock_params_t params, kith_error_t want_code)
{
    kith_tick_clock_t *clock = (kith_tick_clock_t *)&sentinel;
    if (kith_tick_clock_create(&params, nullptr, &clock) != kith_error_return(want_code))
    {
        return 1;
    }
    return clock != nullptr;
}

static int test_creation_validation(void)
{
    kith_tick_clock_t *clock = (kith_tick_clock_t *)&sentinel;
    if (kith_tick_clock_create(nullptr, nullptr, &clock) != kith_error_return(KITH_EINVAL))
    {
        return 1;
    }
    if (params_rejected((kith_tick_clock_params_t) { .size = 8U, .abi_version = KITH_ABI_VERSION },
                        KITH_ESIZE))
    {
        return 2;
    }
    if (params_rejected(
            (kith_tick_clock_params_t) {
                .size = sizeof(kith_tick_clock_params_t), .abi_version = KITH_ABI_VERSION
                +1U
            },
            KITH_EABIVER))
    {
        return 3;
    }
    if (params_rejected(
            (kith_tick_clock_params_t) {
                .size = sizeof(kith_tick_clock_params_t), .abi_version = KITH_ABI_VERSION
                , .tick_hz = 0
            },
            KITH_EINVAL))
    {
        return 4;
    }
    kith_tick_clock_params_t good = valid_params(20U);
    /* An allocator missing most of its struct is undersized against the
     * allocator contract, so the create call rejects it before allocating. */
    const kith_allocator_t undersized = {.size = 8U, .abi_version = KITH_ABI_VERSION };
    if (kith_tick_clock_create(&good, &undersized, &clock) != kith_error_return(KITH_ESIZE))
    {
        return 5;
    }
    if (kith_tick_clock_create(nullptr, nullptr, nullptr) != kith_error_return(KITH_EINVAL))
    {
        return 6;
    }
    return 0;
}

static int test_advance_and_conversions(void)
{
    kith_tick_clock_params_t params = valid_params(20U);
    params.initial_tick = 7U;
    kith_tick_clock_t *clock = nullptr;
    uint64_t value = 0;

    if (kith_tick_clock_create(&params, nullptr, &clock) != 0)
    {
        return 1;
    }
    /* The initial tick counts toward elapsed time: elapsed spans from tick
     * zero, not from creation. */
    if (kith_tick_clock_tick(clock) != 7U)
    {
        kith_tick_clock_destroy(clock);
        return 2;
    }
    if (kith_tick_clock_elapsed_ms(clock, &value) != 0 || value != 350U ||
        kith_tick_clock_elapsed_ns(clock, &value) != 0 || value != 350'000'000ULL)
    {
        kith_tick_clock_destroy(clock);
        return 3;
    }
    if (kith_tick_clock_advance(clock, 3U) != 0 || kith_tick_clock_tick(clock) != 10U)
    {
        kith_tick_clock_destroy(clock);
        return 4;
    }
    if (kith_tick_clock_elapsed_ms(clock, &value) != 0 || value != 500U ||
        kith_tick_clock_elapsed_ns(clock, &value) != 0 || value != 500'000'000ULL)
    {
        kith_tick_clock_destroy(clock);
        return 5;
    }
    /* A 30 Hz rate floors to 33'333'333 ns per tick; three ticks report
     * the exact floored multiple, not a rounded second. */
    kith_tick_clock_params_t odd = valid_params(30U);
    kith_tick_clock_t *odd_clock = nullptr;
    if (kith_tick_clock_create(&odd, nullptr, &odd_clock) != 0)
    {
        kith_tick_clock_destroy(clock);
        return 6;
    }
    if (kith_tick_clock_advance(odd_clock, 3U) != 0 ||
        kith_tick_clock_elapsed_ns(odd_clock, &value) != 0 || value != 99'999'999ULL)
    {
        kith_tick_clock_destroy(odd_clock);
        kith_tick_clock_destroy(clock);
        return 7;
    }
    kith_tick_clock_destroy(odd_clock);
    kith_tick_clock_destroy(clock);
    return 0;
}

static int test_overflow_and_reset(void)
{
    kith_tick_clock_params_t params = valid_params(20U);
    kith_tick_clock_t *clock = nullptr;
    uint64_t value = 0;

    if (kith_tick_clock_create(&params, nullptr, &clock) != 0)
    {
        return 1;
    }
    if (kith_tick_clock_advance(clock, UINT64_MAX) != 0 ||
        kith_tick_clock_tick(clock) != UINT64_MAX)
    {
        kith_tick_clock_destroy(clock);
        return 2;
    }
    /* A wrapping advance fails and leaves the counter at its prior value. */
    if (kith_tick_clock_advance(clock, 1U) != kith_error_return(KITH_ERANGE) ||
        kith_tick_clock_tick(clock) != UINT64_MAX)
    {
        kith_tick_clock_destroy(clock);
        return 3;
    }
    /* Both conversions overflow at the maximal tick: the millisecond
     * period is smaller than the nanosecond one, but not by enough. */
    if (kith_tick_clock_elapsed_ns(clock, &value) != kith_error_return(KITH_ERANGE) ||
        kith_tick_clock_elapsed_ms(clock, &value) != kith_error_return(KITH_ERANGE))
    {
        kith_tick_clock_destroy(clock);
        return 4;
    }
    if (kith_tick_clock_reset(clock, 12U) != 0 || kith_tick_clock_tick(clock) != 12U)
    {
        kith_tick_clock_destroy(clock);
        return 5;
    }
    if (kith_tick_clock_elapsed_ns(clock, &value) != 0 || value != 600'000'000ULL)
    {
        kith_tick_clock_destroy(clock);
        return 6;
    }
    kith_tick_clock_destroy(clock);
    return 0;
}

static int test_null_contracts(void)
{
    uint64_t value = 0;
    if (kith_tick_clock_tick(nullptr) != 0)
    {
        return 1;
    }
    if (kith_tick_clock_advance(nullptr, 1U) != kith_error_return(KITH_EINVAL) ||
        kith_tick_clock_reset(nullptr, 0U) != kith_error_return(KITH_EINVAL))
    {
        return 2;
    }
    if (kith_tick_clock_elapsed_ms(nullptr, &value) != kith_error_return(KITH_EINVAL) ||
        kith_tick_clock_elapsed_ns(nullptr, &value) != kith_error_return(KITH_EINVAL))
    {
        return 3;
    }
    kith_tick_clock_t *clock = nullptr;
    if (kith_tick_clock_elapsed_ms(clock, &value) != kith_error_return(KITH_EINVAL))
    {
        return 4;
    }
    kith_tick_clock_destroy(nullptr);
    return 0;
}

static int test_wall_monotonic(void)
{
    uint64_t previous = kith_wall_clock_ns();
    if (previous == 0)
    {
        return 1;
    }
    for (unsigned i = 0; i < 1000U; i++)
    {
        uint64_t now = kith_wall_clock_ns();
        if (now < previous)
        {
            return 2;
        }
        previous = now;
    }
    return 0;
}

int main(void)
{
    int rc = test_creation_validation();
    if (rc == 0)
    {
        rc = 10 * test_advance_and_conversions();
    }
    if (rc == 0)
    {
        rc = 20 * test_overflow_and_reset();
    }
    if (rc == 0)
    {
        rc = 30 * test_null_contracts();
    }
    if (rc == 0)
    {
        rc = 40 * test_wall_monotonic();
    }
    return rc;
}
