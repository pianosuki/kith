#ifndef KITH_UTIL_WALL_CLOCK_H
#define KITH_UTIL_WALL_CLOCK_H

#include <stdint.h>

#include "kith/api.h"

/**
 * Wall-clock time for infrastructure code.
 *
 * This is the framework's one named door to real time: monotonic nanoseconds
 * for session timeouts, metrics intervals, log timestamps, and similar
 * infrastructure concerns. It is explicitly outside the replay contract (see
 * docs/architecture/adr/0014-deterministic-simulation-contract.md): model
 * code must never call it, because wall-clock readings change from run to
 * run and would make replays diverge. Model-visible time comes from the tick
 * clock instead (see include/kith/util/tick_clock.h).
 */

/**
 * @addtogroup kith_util
 * @{
 */

/**
 * Read the monotonic wall clock in nanoseconds.
 *
 * The reading derives from CLOCK_MONOTONIC, so it advances at real seconds
 * and never goes backwards within a boot; its absolute value has no
 * relationship to any calendar and two runs of the same simulation report
 * different readings by design.
 *
 * @return Monotonic nanoseconds since an unspecified boot-relative epoch.
 * @thread_safety safe — the read takes no locks and shares no state.
 * @ownership caller — no ownership transfer; returns a value.
 */
[[nodiscard]] KITH_API uint64_t kith_wall_clock_ns(void);

/** @} */

#endif /* KITH_UTIL_WALL_CLOCK_H */
