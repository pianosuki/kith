/* Wiring pins for the gateway params the composition root builds: the
 * compose-wait budget resolves 0 to the gateway default on tick rates
 * where that default still sits below one tick interval, clamps to half
 * the tick where it does not (62 Hz is the equality boundary and stays
 * unclamped, 63 Hz is the first clamp rate), passes explicit caller
 * values through unclamped, and writes 0 at tick rates above 1000 where
 * the integer tick interval is 0 ms — the gateway's own 0 resolution
 * then selects the default, so the effective budget at those rates is
 * unchanged. The view and cache refresh intervals follow the same
 * explicit-passes-unclamped law with the tick interval as the 0
 * derivation, and the view-set capacity forwards raw for the gateway's
 * own 0 resolution. Compiled against src/server/wiring.c so the internal
 * server_gateway_params is directly observable. */

#include <stddef.h>
#include <stdio.h>
#include <string.h>

#include "server/server_internal.h"

static int check_cond(bool ok, int line)
{
    if (!ok)
    {
        (void)fprintf(stderr, "check failed at line %d\n", line);
        return 1;
    }
    return 0;
}

#define CHECK(cond) failures += check_cond((cond), __LINE__)

static int test_wait_budget_resolution(void)
{
    int failures = 0;
    static const struct
    {
        uint16_t tick_hz;
        uint32_t caller_budget;
        uint32_t expected;
    } cases[] = {
        {20u, 0u, 8000u},       // default intact: half-tick 25000 >= 8000
        {62u, 0u, 8000u},       // equality boundary: 16 ms * 500 == 8000
        {63u, 0u, 7500u},       // first clamp rate: 15 ms * 500 == 7500
        {100u, 0u, 5000u},      // clamp tracks half the tick
        {2000u, 0u, 0u},        // integer interval 0 ms; gateway re-resolves
        {100u, 12000u, 12000u}, // explicit value passes unclamped
        {20u, 4000u, 4000u},    // explicit below the default passes too
    };
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); ++i)
    {
        struct kith_server s;
        memset(&s, 0, sizeof(s));
        s.tick_hz = cases[i].tick_hz;
        s.tick_interval_ms = 1000u / s.tick_hz;
        s.delivery_wait_budget_us = cases[i].caller_budget;
        kith_gateway_params_t p = server_gateway_params(&s);
        CHECK(p.delivery_wait_budget_us == cases[i].expected);
        CHECK(p.compose_budget_us == s.tick_interval_ms * 500u);
    }
    return failures;
}

static int test_view_tuning_forward(void)
{
    int failures = 0;
    // The view and cache refresh intervals resolve a caller 0 to the tick
    // interval (the composition root's derivation), pass explicit values
    // through unclamped, and leave the 0 ms integer interval at tick rates
    // above 1000 for the gateway's own 0 resolution to back up to its
    // default — the same shape the wait budget's 2000 Hz case pins.
    static const struct
    {
        uint16_t tick_hz;
        uint32_t caller_view_ms;
        uint32_t caller_cache_ms;
        uint32_t expected_view;
        uint32_t expected_cache;
    } intervals[] = {
        {20u, 0u, 0u, 50u, 50u},       // 0 selects the tick interval
        {63u, 0u, 0u, 15u, 15u},       // derivation tracks the tick
        {2000u, 0u, 0u, 0u, 0u},       // integer interval 0 ms; gateway re-resolves
        {20u, 200u, 300u, 200u, 300u}, // explicit wider than the tick passes
        {20u, 25u, 10u, 25u, 10u},     // explicit below the tick passes too
    };
    for (size_t i = 0; i < sizeof(intervals) / sizeof(intervals[0]); ++i)
    {
        struct kith_server s;
        memset(&s, 0, sizeof(s));
        s.tick_hz = intervals[i].tick_hz;
        s.tick_interval_ms = 1000u / s.tick_hz;
        s.view_refresh_interval_ms = intervals[i].caller_view_ms;
        s.cache_refresh_interval_ms = intervals[i].caller_cache_ms;
        kith_gateway_params_t p = server_gateway_params(&s);
        CHECK(p.view_refresh_interval_ms == intervals[i].expected_view);
        CHECK(p.cache_refresh_interval_ms == intervals[i].expected_cache);
    }
    // The view-set capacity forwards raw: the gateway resolves a 0 to its
    // own default, so the wiring passes the caller's value unchanged.
    struct kith_server s;
    memset(&s, 0, sizeof(s));
    s.tick_hz = 20u;
    s.tick_interval_ms = 50u;
    kith_gateway_params_t p = server_gateway_params(&s);
    CHECK(p.view_max_subjects == 0u);
    s.view_max_subjects = 1024u;
    p = server_gateway_params(&s);
    CHECK(p.view_max_subjects == 1024u);
    return failures;
}

int main(void)
{
    int rc = 0;
    rc |= test_wait_budget_resolution();
    rc |= test_view_tuning_forward();
    if (rc != 0)
    {
        (void)fprintf(stderr, "server wiring tests FAILED\n");
    }
    return rc;
}
