/* Rational server deadlines under ideal wakeups, jitter, and long stalls. */

#include <stdint.h>
#include <stdio.h>

#include "server/timing.h"

static int test_deadlines(void)
{
    const uint64_t epoch = 12'345u;
    for (uint16_t hz = 1u; hz <= 1000u; ++hz)
    {
        uint64_t now = epoch;
        for (uint64_t tick = 1u; tick <= (uint64_t)hz * 3u; ++tick)
        {
            const uint64_t deadline = server_tick_deadline(epoch, now, hz);
            const uint64_t expected = epoch + (tick * 1000u + hz - 1u) / hz;
            if (deadline != expected || deadline <= now)
            {
                return 1;
            }
            now = deadline;
        }
    }
    return 0;
}

static int test_jitter_and_stalls(void)
{
    const uint64_t epoch = 123u;
    uint64_t now = epoch;
    for (uint64_t tick = 1u; tick <= 60'000u; ++tick)
    {
        const uint64_t deadline = server_tick_deadline(epoch, now, 60u);
        if (deadline != epoch + (tick * 1000u + 59u) / 60u)
        {
            return 1;
        }
        now = deadline + 3u;
    }
    const uint64_t stalled = epoch + 1'000'000'000'037u;
    if (server_tick_deadline(epoch, stalled, 60u) != epoch + 1'000'000'000'050u ||
        server_tick_deadline(epoch, epoch + 1000u, 60u) != epoch + 1017u)
    {
        return 2;
    }
    return 0;
}

int main(void)
{
    int rc = test_deadlines() | test_jitter_and_stalls();
    rc |= server_tick_deadline(123u, 456u, 1001u) != 456u;
    rc |= server_tick_deadline(123u, 456u, UINT16_MAX) != 456u;
    if (rc != 0)
    {
        (void)fprintf(stderr, "server rational timing tests failed\n");
    }
    return rc;
}
