#pragma once

#include <stdint.h>

/* Millisecond deadlines on a rational, epoch-anchored frequency grid.
 * Rates above 1000 retain the legacy zero-interval scheduling mode. */

static inline uint64_t server_tick_deadline(uint64_t epoch_ms, uint64_t now_ms, uint16_t hz)
{
    if (hz > 1000u)
    {
        return now_ms;
    }
    const uint64_t phase_ms = (now_ms - epoch_ms) % 1000u;
    const uint64_t slot = phase_ms * hz / 1000u + 1u;
    const uint64_t offset_ms = (slot * 1000u + hz - 1u) / hz;
    return now_ms + (offset_ms - phase_ms);
}
