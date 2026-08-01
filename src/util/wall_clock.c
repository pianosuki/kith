/* Wall-clock accessor for infrastructure code: the one named door to real
 * time, outside the replay contract. The public contract is
 * include/kith/util/wall_clock.h. */

#include "kith/util/wall_clock.h"

#include <time.h>

KITH_API uint64_t kith_wall_clock_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1'000'000'000ULL + (uint64_t)ts.tv_nsec;
}
