#ifndef KITH_UTIL_TICK_CLOCK_H
#define KITH_UTIL_TICK_CLOCK_H

#include <stdint.h>

#include "kith/api.h"
#include "kith/types.h"

/**
 * Deterministic simulation clock driven by the tick counter.
 *
 * An application owns the clock and advances its tick count once per
 * simulation step. Model code receives the handle by injection and reads
 * it through the accessors. The server does not create or advance this
 * clock automatically. Identical tick sequences and rates produce
 * identical reported time, independent of wall-clock time or scheduling.
 *
 * Precise elapsed-time conversions floor the total rational elapsed time
 * once, bounding rounding error below one output unit. The deprecated
 * conversions instead multiply the tick count by a floored per-tick
 * period, preserving their original results for compatible callers.
 */

/**
 * @addtogroup kith_util
 * @{
 */

/** Opaque tick-clock handle. */
typedef struct kith_tick_clock kith_tick_clock_t;

/**
 * Creation parameters. Size-versioned: callers set @p size to
 * sizeof(kith_tick_clock_params_t) and @p abi_version to KITH_ABI_VERSION at
 * their compile time; the runtime rejects structs from an incompatible
 * generation or an undersized size. Future additive fields occupy the
 * reserved slots so the layout below stays stable across generations.
 */
struct kith_tick_clock_params
{
    /** Must be sizeof(kith_tick_clock_params_t). */
    uint32_t size;
    /** Must be KITH_ABI_VERSION (from kith/version.h). */
    uint32_t abi_version;

    /**
     * Tick value the clock starts from, usually zero.
     */
    uint64_t initial_tick;

    /**
     * Tick rate in ticks per second, at least 1. Drives the elapsed-time
     * conversions. Rates above 1000 floor the deprecated millisecond period
     * to zero; rates above one billion also floor its nanosecond period to zero.
     */
    uint32_t tick_hz;

    /** Reserved for future additive fields. Must be zero-filled. */
    void *reserved[8];
};

/** Alias of struct kith_tick_clock_params. */
typedef struct kith_tick_clock_params kith_tick_clock_params_t;

/**
 * Build a tick clock from @p params.
 *
 * @param params    Creation parameters; @c size and @c abi_version must
 *                  match the runtime generation and @c tick_hz must be at
 *                  least 1.
 * @param alloc     Allocator for the new handle, used again when
 *                  kith_tick_clock_destroy frees it. NULL selects the
 *                  default allocator; a supplied allocator is validated
 *                  (see kith_allocator_t) and must outlive the handle.
 * @param out_clock Receives the new handle on success.
 * @return          0 on success, negative kith_error on failure:
 *                  - -KITH_EINVAL if @p out_clock or @p params is NULL, or
 *                    @c tick_hz is zero, or @p alloc is missing an
 *                    operation,
 *                  - -KITH_EABIVER if @p params or @p alloc has an
 *                    incompatible abi_version,
 *                  - -KITH_ESIZE if @p params or @p alloc has an undersized
 *                    size,
 *                  - -KITH_ENOMEM on allocation failure.
 * @thread_safety unsafe — must not be called concurrently with another
 *                kith_tick_clock_create on the same @p out_clock slot.
 * @ownership callee — the returned handle is freed by the caller with
 *           kith_tick_clock_destroy.
 */
[[nodiscard]] KITH_API int kith_tick_clock_create(const kith_tick_clock_params_t *params,
                                                  const kith_allocator_t *alloc,
                                                  kith_tick_clock_t **out_clock);

/**
 * Release a tick clock and all storage it owns. Passing NULL is a no-op.
 *
 * @param clock Clock handle. NULL is a no-op.
 * @thread_safety unsafe — no advance, reset, or read call may be in flight
 *                on @p clock when this is called.
 * @ownership callee — @p clock is consumed and freed by the call.
 */
KITH_API void kith_tick_clock_destroy(kith_tick_clock_t *clock);

/**
 * Advance the clock by @p ticks. The application's serialized simulation
 * owner advances it for steps actually performed. Server callback indices
 * may have gaps and do not advance this application-owned clock.
 *
 * @param clock  Handle to modify. NULL is rejected.
 * @param ticks  Number of ticks to add.
 * @return       0 on success, negative kith_error on failure:
 *               - -KITH_EINVAL if @p clock is NULL,
 *               - -KITH_ERANGE if the addition would wrap the 64-bit tick
 *                 counter (the clock keeps its prior value).
 * @thread_safety unsafe — concurrent advance/reset/read calls on the same
 *                handle are not synchronized.
 * @ownership caller — @p clock is borrowed for the call only.
 */
[[nodiscard]] KITH_API int kith_tick_clock_advance(kith_tick_clock_t *clock, uint64_t ticks);

/**
 * Set the clock to an absolute tick, rewinding or fast-forwarding without
 * stepping through the intermediate values. The application's serialized
 * simulation owner uses this for replay rewinds or sub-simulation restarts;
 * it does not change the server's tick counter.
 *
 * @param clock Handle to modify. NULL is rejected.
 * @param tick  Absolute tick value to store.
 * @return      0 on success, -KITH_EINVAL if @p clock is NULL.
 * @thread_safety unsafe — concurrent advance/reset/read calls on the same
 *                handle are not synchronized.
 * @ownership caller — @p clock is borrowed for the call only.
 */
[[nodiscard]] KITH_API int kith_tick_clock_reset(kith_tick_clock_t *clock, uint64_t tick);

/**
 * Report the current tick.
 *
 * @param clock Clock handle.
 * @return      The current tick, or 0 if @p clock is NULL.
 * @thread_safety safe-if (no advance or reset runs concurrently).
 * @ownership caller — @p clock is borrowed for the call only.
 */
[[nodiscard]] KITH_API uint64_t kith_tick_clock_tick(const kith_tick_clock_t *clock);

/**
 * Convert the current tick to elapsed milliseconds since tick zero.
 *
 * The result is @c tick times the floored millisecond period implied by
 * @c tick_hz; see the module documentation for the rounding rule.
 *
 * @deprecated Since 1.1.0. Use kith_tick_clock_elapsed_ms_precise for
 *             elapsed time at the configured tick rate. This function
 *             retains its integral-period result throughout 1.x.
 *             Scheduled for removal in 2.0.0.
 * @param clock   Clock handle. NULL is rejected.
 * @param out_ms  Receives the elapsed milliseconds on success.
 * @return        0 on success, negative kith_error on failure:
 *                - -KITH_EINVAL if @p clock or @p out_ms is NULL,
 *                - -KITH_ERANGE if the conversion overflows 64 bits.
 * @thread_safety safe-if (no advance or reset runs concurrently).
 * @ownership caller — both arguments are borrowed for the call only.
 */
[[nodiscard]] KITH_API KITH_DEPRECATED int
kith_tick_clock_elapsed_ms(const kith_tick_clock_t *clock, uint64_t *out_ms);

/**
 * Convert the current tick to elapsed nanoseconds since tick zero.
 *
 * The result is @c tick times the floored nanosecond period implied by
 * @c tick_hz; see the module documentation for the rounding rule.
 *
 * @deprecated Since 1.1.0. Use kith_tick_clock_elapsed_ns_precise for
 *             elapsed time at the configured tick rate. This function
 *             retains its integral-period result throughout 1.x.
 *             Scheduled for removal in 2.0.0.
 * @param clock   Clock handle. NULL is rejected.
 * @param out_ns  Receives the elapsed nanoseconds on success.
 * @return        0 on success, negative kith_error on failure:
 *                - -KITH_EINVAL if @p clock or @p out_ns is NULL,
 *                - -KITH_ERANGE if the conversion overflows 64 bits.
 * @thread_safety safe-if (no advance or reset runs concurrently).
 * @ownership caller — both arguments are borrowed for the call only.
 */
[[nodiscard]] KITH_API KITH_DEPRECATED int
kith_tick_clock_elapsed_ns(const kith_tick_clock_t *clock, uint64_t *out_ns);

/**
 * Convert the total rational tick time to milliseconds, rounding down once.
 *
 * The result is floor(tick * units_per_second / tick_hz), with error
 * strictly below one output unit and no accumulated period truncation.
 * Intermediate multiplication cannot cause a false overflow rejection.
 *
 * @param clock  Clock handle. NULL is rejected.
 * @param out_ms Receives elapsed milliseconds on success; unchanged on error.
 * @return       0 on success, -KITH_EINVAL for a NULL argument, or
 *               -KITH_ERANGE if the result exceeds 64 bits.
 * @thread_safety safe-if (no advance or reset runs concurrently).
 * @ownership caller — both arguments are borrowed for the call only.
 */
[[nodiscard]] KITH_API int kith_tick_clock_elapsed_ms_precise(const kith_tick_clock_t *clock,
                                                              uint64_t *out_ms);

/**
 * Convert the total rational tick time to nanoseconds, rounding down once.
 *
 * The result is floor(tick * units_per_second / tick_hz), with error
 * strictly below one output unit and no accumulated period truncation.
 * Intermediate multiplication cannot cause a false overflow rejection.
 *
 * @param clock  Clock handle. NULL is rejected.
 * @param out_ns Receives elapsed nanoseconds on success; unchanged on error.
 * @return       0 on success, -KITH_EINVAL for a NULL argument, or
 *               -KITH_ERANGE if the result exceeds 64 bits.
 * @thread_safety safe-if (no advance or reset runs concurrently).
 * @ownership caller — both arguments are borrowed for the call only.
 */
[[nodiscard]] KITH_API int kith_tick_clock_elapsed_ns_precise(const kith_tick_clock_t *clock,
                                                              uint64_t *out_ns);

/** @} */

#endif /* KITH_UTIL_TICK_CLOCK_H */
