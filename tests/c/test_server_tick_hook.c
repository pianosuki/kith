/* Per-tick game-logic callback tests: the framework tick hook dispatches one
 * callback per normal tick. A Python-flagged callback runs on a worker
 * thread, never the reactor; a C callback runs inline on the
 * reactor. The callback receives the server's monotonic tick counter
 * (the tick-boundary time base). Driven by entering
 * kith_server_run on the main thread (the reactor thread) and requesting
 * shutdown from inside the callback — or from the poll observer's nonzero
 * return when no tick hook is registered — mirroring test_server_run's
 * run-and-shutdown harness. */

#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include <pthread.h>

#include "kith/server/server.h"
#include "kith/types.h"
#include "kith/version.h"

static int check_cond(bool ok, int line)
{
    if (!ok)
    {
        (void)fprintf(stderr, "server tick hook: assertion at line %d failed\n", line);
        return 1;
    }
    return 0;
}

#define CHECK(cond) failures += check_cond((cond), __LINE__)

// Build a server on an OS-assigned ephemeral port with the default tick rate
// and one Python worker, so the tick tests never contend with the other
// server tests under parallel ctest.
static int make_server(kith_server_t **out)
{
    kith_server_params_t params = {0};
    params.size = sizeof(params);
    params.abi_version = KITH_ABI_VERSION;
    return kith_server_create(&params, nullptr, out);
}

// The callback requests shutdown after this many invocations so the run loop
// drains and kith_server_run returns without a separate shutdown thread.
#define TICK_HOOK_SHUTDOWN_AFTER 3u

// The poll observer requests the drain after this many polls, bounding a run
// with no tick hook registered (no callback exists to request shutdown). Two
// full normal ticks elapse before the drain tick, which skips the dispatch.
#define POLL_HOOK_SHUTDOWN_AFTER 3u

struct tick_ctx
{
    kith_server_t *server;
    pthread_t reactor_tid;
    _Atomic uint32_t count;
    _Atomic uint64_t last_tick;
    _Atomic bool reactor_entered;
    _Atomic bool tick_non_monotonic;
};

static void tick_handler_python(uint64_t tick, void *user_data)
{
    struct tick_ctx *ctx = (struct tick_ctx *)user_data;
    if (pthread_equal(pthread_self(), ctx->reactor_tid))
    {
        atomic_store_explicit(&ctx->reactor_entered, true, memory_order_relaxed);
    }
    uint64_t prev = atomic_load_explicit(&ctx->last_tick, memory_order_relaxed);
    if (tick <= prev)
    {
        atomic_store_explicit(&ctx->tick_non_monotonic, true, memory_order_relaxed);
    }
    atomic_store_explicit(&ctx->last_tick, tick, memory_order_relaxed);
    uint32_t n = atomic_fetch_add_explicit(&ctx->count, 1u, memory_order_relaxed) + 1u;
    if (n == TICK_HOOK_SHUTDOWN_AFTER)
    {
        // Safe from any thread (the worker here); the reactor observes the
        // request on its next tick and drains.
        (void)kith_server_shutdown(ctx->server);
    }
}

static void tick_handler_c_inline(uint64_t tick, void *user_data)
{
    struct tick_ctx *ctx = (struct tick_ctx *)user_data;
    if (pthread_equal(pthread_self(), ctx->reactor_tid))
    {
        atomic_store_explicit(&ctx->reactor_entered, true, memory_order_relaxed);
    }
    uint64_t prev = atomic_load_explicit(&ctx->last_tick, memory_order_relaxed);
    if (tick <= prev)
    {
        atomic_store_explicit(&ctx->tick_non_monotonic, true, memory_order_relaxed);
    }
    atomic_store_explicit(&ctx->last_tick, tick, memory_order_relaxed);
    uint32_t n = atomic_fetch_add_explicit(&ctx->count, 1u, memory_order_relaxed) + 1u;
    if (n == TICK_HOOK_SHUTDOWN_AFTER)
    {
        // Inline on the reactor thread; shutdown is observed on the next tick.
        (void)kith_server_shutdown(ctx->server);
    }
}

static int test_tick_hook_null_args(void)
{
    int failures = 0;
    uint64_t out = 0;
    CHECK(kith_server_register_tick_handler(
              nullptr, tick_handler_c_inline, nullptr, KITH_SERVER_HANDLER_NONE) ==
          kith_error_return(KITH_EINVAL));
    CHECK(kith_server_unregister_tick_handler(nullptr) == kith_error_return(KITH_EINVAL));
    CHECK(kith_server_tick_drops(nullptr, &out) == kith_error_return(KITH_EINVAL));

    kith_server_t *s = nullptr;
    CHECK(make_server(&s) == 0);
    // A NULL callback is rejected even with a valid server.
    CHECK(kith_server_register_tick_handler(s, nullptr, nullptr, KITH_SERVER_HANDLER_NONE) ==
          kith_error_return(KITH_EINVAL));
    CHECK(kith_server_tick_drops(s, nullptr) == kith_error_return(KITH_EINVAL));
    // A valid read returns 0 and the monotonic count starts at zero.
    out = 1u;
    CHECK(kith_server_tick_drops(s, &out) == 0);
    CHECK(out == 0u);
    kith_server_destroy(s);
    return failures;
}

static int test_tick_hook_python_dispatches_to_worker(void)
{
    int failures = 0;
    kith_server_t *s = nullptr;
    CHECK(make_server(&s) == 0);

    struct tick_ctx ctx = {0};
    ctx.server = s;
    ctx.reactor_tid = pthread_self();
    atomic_init(&ctx.count, 0u);
    atomic_init(&ctx.last_tick, 0u);
    atomic_init(&ctx.reactor_entered, false);
    atomic_init(&ctx.tick_non_monotonic, false);
    CHECK(kith_server_register_tick_handler(
              s, tick_handler_python, &ctx, KITH_SERVER_HANDLER_PYTHON) == 0);

    // The run loop runs on this (the main) thread, which is the reactor
    // thread; the worker pool thread is the only other thread touching the
    // callback. Blocks until the callback requests shutdown and the reactor
    // drains.
    CHECK(kith_server_run(s) == 0);

    uint64_t drops = 1u;
    CHECK(kith_server_tick_drops(s, &drops) == 0);
    // The server handle is destroyed before the context atomics are read so
    // the worker pool is joined (kith_worker_destroy drains in-flight tasks)
    // and no callback is still running.
    kith_server_destroy(s);

    uint32_t count = atomic_load_explicit(&ctx.count, memory_order_acquire);
    bool reactor_entered = atomic_load_explicit(&ctx.reactor_entered, memory_order_relaxed);
    bool tick_non_monotonic = atomic_load_explicit(&ctx.tick_non_monotonic, memory_order_relaxed);
    uint64_t last_tick = atomic_load_explicit(&ctx.last_tick, memory_order_relaxed);
    CHECK(count >= TICK_HOOK_SHUTDOWN_AFTER); // at least the shutdown threshold
    CHECK(!reactor_entered);                  // no reactor-inline Python
    CHECK(!tick_non_monotonic);               // tick counter strictly increases
    CHECK(last_tick >= TICK_HOOK_SHUTDOWN_AFTER);
    CHECK(drops == 0u);                       // one task per tick never exhausts the pool
    return failures;
}

static int test_tick_hook_c_handler_runs_inline(void)
{
    int failures = 0;
    kith_server_t *s = nullptr;
    CHECK(make_server(&s) == 0);

    struct tick_ctx ctx = {0};
    ctx.server = s;
    ctx.reactor_tid = pthread_self();
    atomic_init(&ctx.count, 0u);
    atomic_init(&ctx.last_tick, 0u);
    atomic_init(&ctx.reactor_entered, false);
    atomic_init(&ctx.tick_non_monotonic, false);
    CHECK(kith_server_register_tick_handler(
              s, tick_handler_c_inline, &ctx, KITH_SERVER_HANDLER_NONE) == 0);

    CHECK(kith_server_run(s) == 0);
    kith_server_destroy(s);

    uint32_t count = atomic_load_explicit(&ctx.count, memory_order_acquire);
    bool reactor_entered = atomic_load_explicit(&ctx.reactor_entered, memory_order_relaxed);
    bool tick_non_monotonic = atomic_load_explicit(&ctx.tick_non_monotonic, memory_order_relaxed);
    CHECK(count >= TICK_HOOK_SHUTDOWN_AFTER);
    CHECK(reactor_entered); // the C callback runs inline on the reactor
    CHECK(!tick_non_monotonic);
    return failures;
}

struct drain_poll_ctx
{
    _Atomic uint32_t calls;
};

// Counts observer invocations and requests the drain at the threshold, so a
// run with no tick hook registered still returns deterministically.
static int drain_after_threshold_polls(void *user_data)
{
    struct drain_poll_ctx *ctx = (struct drain_poll_ctx *)user_data;
    uint32_t n = atomic_fetch_add_explicit(&ctx->calls, 1u, memory_order_relaxed) + 1u;
    return n >= POLL_HOOK_SHUTDOWN_AFTER ? 1 : 0;
}

// Unregister roundtrip: a handler registered and then unregistered before
// the run is never dispatched, and unregistering an empty registration
// succeeds. The poll observer bounds the run and proves the reactor
// executed full normal ticks — the dispatch site is reached on every tick
// before the drain — so a zero invocation count is a real observation, not
// a run that never ticked.
static int test_tick_hook_unregister_roundtrip_not_dispatched(void)
{
    int failures = 0;
    kith_server_t *s = nullptr;
    CHECK(make_server(&s) == 0);

    // Unregistering with nothing registered succeeds.
    CHECK(kith_server_unregister_tick_handler(s) == 0);

    struct tick_ctx ctx = {0};
    ctx.server = s;
    ctx.reactor_tid = pthread_self();
    atomic_init(&ctx.count, 0u);
    atomic_init(&ctx.last_tick, 0u);
    atomic_init(&ctx.reactor_entered, false);
    atomic_init(&ctx.tick_non_monotonic, false);
    CHECK(kith_server_register_tick_handler(
              s, tick_handler_c_inline, &ctx, KITH_SERVER_HANDLER_NONE) == 0);
    CHECK(kith_server_unregister_tick_handler(s) == 0);

    struct drain_poll_ctx polls = {.calls = 0};
    atomic_init(&polls.calls, 0u);
    CHECK(kith_server_register_poll_observer(s, drain_after_threshold_polls, &polls) == 0);

    CHECK(kith_server_run(s) == 0);
    CHECK(kith_server_status(s) == KITH_SERVER_STATUS_STOPPED);
    kith_server_destroy(s);

    uint32_t count = atomic_load_explicit(&ctx.count, memory_order_acquire);
    uint32_t poll_calls = atomic_load_explicit(&polls.calls, memory_order_relaxed);
    CHECK(count == 0u);                            // the unregistered hook never fired
    CHECK(poll_calls >= POLL_HOOK_SHUTDOWN_AFTER); // the reactor ticked past the drain threshold
    return failures;
}

int main(void)
{
    int rc = 0;
    rc |= test_tick_hook_null_args();
    rc |= test_tick_hook_python_dispatches_to_worker();
    rc |= test_tick_hook_c_handler_runs_inline();
    rc |= test_tick_hook_unregister_roundtrip_not_dispatched();
    if (rc != 0)
    {
        (void)fprintf(stderr, "server tick hook tests FAILED\n");
    }
    return rc;
}
