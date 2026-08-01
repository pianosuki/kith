/* Deterministic simulation clock driven by the tick counter, plus its
 * integer elapsed-time conversions. The public contract is
 * include/kith/util/tick_clock.h. */

#include "kith/util/tick_clock.h"

#include <stdckdint.h>

#include "kith/version.h"

struct kith_tick_clock
{
    const kith_allocator_t *allocator;
    uint64_t tick;
    uint64_t ns_per_tick;
    uint64_t ms_per_tick;
};

/* Returns KITH_OK when the params are usable, otherwise the error enumerator
 * the create call must report: the caller-supplied struct is validated one
 * field at a time so each failure class keeps its dedicated code. */
static kith_error_t params_error(const kith_tick_clock_params_t *params)
{
    if (params == nullptr)
    {
        return KITH_EINVAL;
    }
    if (params->abi_version != KITH_ABI_VERSION)
    {
        return KITH_EABIVER;
    }
    if (params->size < sizeof(kith_tick_clock_params_t))
    {
        return KITH_ESIZE;
    }
    if (params->tick_hz < 1U)
    {
        return KITH_EINVAL;
    }
    return KITH_OK;
}

[[nodiscard]] KITH_API int kith_tick_clock_create(const kith_tick_clock_params_t *params,
                                                  const kith_allocator_t *alloc,
                                                  kith_tick_clock_t **out_clock)
{
    if (out_clock == nullptr)
    {
        return kith_error_return(KITH_EINVAL);
    }
    *out_clock = nullptr;
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
    kith_tick_clock_t *clock = kith_alloc(allocator, sizeof(*clock));
    if (clock == nullptr)
    {
        return kith_error_return(KITH_ENOMEM);
    }
    clock->allocator = allocator;
    clock->tick = params->initial_tick;
    /* Floors are the contract: conversions report exact multiples of the
     * floored period instead of drifting through accumulated rounding. */
    clock->ns_per_tick = 1'000'000'000ULL / params->tick_hz;
    clock->ms_per_tick = 1'000ULL / params->tick_hz;
    *out_clock = clock;
    return 0;
}

KITH_API void kith_tick_clock_destroy(kith_tick_clock_t *clock)
{
    if (clock == nullptr)
    {
        return;
    }
    kith_free(clock->allocator, clock);
}

[[nodiscard]] KITH_API int kith_tick_clock_advance(kith_tick_clock_t *clock, uint64_t ticks)
{
    if (clock == nullptr)
    {
        return kith_error_return(KITH_EINVAL);
    }
    uint64_t advanced = 0;
    if (ckd_add(&advanced, clock->tick, ticks))
    {
        return kith_error_return(KITH_ERANGE);
    }
    clock->tick = advanced;
    return 0;
}

[[nodiscard]] KITH_API int kith_tick_clock_reset(kith_tick_clock_t *clock, uint64_t tick)
{
    if (clock == nullptr)
    {
        return kith_error_return(KITH_EINVAL);
    }
    clock->tick = tick;
    return 0;
}

[[nodiscard]] KITH_API uint64_t kith_tick_clock_tick(const kith_tick_clock_t *clock)
{
    if (clock == nullptr)
    {
        return 0;
    }
    return clock->tick;
}

static int elapsed(const struct kith_tick_clock *clock, uint64_t period, uint64_t *out)
{
    if (out == nullptr)
    {
        return kith_error_return(KITH_EINVAL);
    }
    uint64_t value = 0;
    if (ckd_mul(&value, clock->tick, period))
    {
        return kith_error_return(KITH_ERANGE);
    }
    *out = value;
    return 0;
}

[[nodiscard]] KITH_API int kith_tick_clock_elapsed_ms(const kith_tick_clock_t *clock,
                                                      uint64_t *out_ms)
{
    if (clock == nullptr)
    {
        return kith_error_return(KITH_EINVAL);
    }
    return elapsed(clock, clock->ms_per_tick, out_ms);
}

[[nodiscard]] KITH_API int kith_tick_clock_elapsed_ns(const kith_tick_clock_t *clock,
                                                      uint64_t *out_ns)
{
    if (clock == nullptr)
    {
        return kith_error_return(KITH_EINVAL);
    }
    return elapsed(clock, clock->ns_per_tick, out_ns);
}
