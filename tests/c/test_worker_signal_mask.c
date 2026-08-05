#include <signal.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <pthread.h>

#include "kith/types.h"
#include "kith/version.h"
#include "kith/worker/worker.h"

// ---------------------------------------------------------------------------
// test harness
// ---------------------------------------------------------------------------

static int check_cond(bool ok, int line)
{
    if (!ok)
    {
        (void)fprintf(stderr, "worker signal mask: assertion at line %d failed\n", line);
        return 1;
    }
    return 0;
}

#define CHECK(cond) failures += check_cond((cond), __LINE__)

// ---------------------------------------------------------------------------
// worker-side mask observation
// ---------------------------------------------------------------------------

// Each task execution records whether SIGINT and SIGTERM are blocked on
// the executing thread. Every pool worker blocks both from entry, so the
// observation must hold whichever thread runs the task.
struct mask_ctx
{
    _Atomic uint32_t observed;
    _Atomic uint32_t blocked;
};

static void mask_task(void *arg)
{
    struct mask_ctx *m = arg;
    sigset_t current;
    bool ok = pthread_sigmask(SIG_BLOCK, nullptr, &current) == 0 &&
              sigismember(&current, SIGINT) > 0 && sigismember(&current, SIGTERM) > 0;
    if (!ok)
    {
        atomic_store_explicit(&m->blocked, 0u, memory_order_release);
    }
    atomic_fetch_add_explicit(&m->observed, 1u, memory_order_release);
}

// Wait until @p counter reaches @p target, polling every 1 ms up to 5 s.
static bool wait_for(_Atomic uint32_t *counter, uint32_t target)
{
    for (int i = 0; i < 5000; i++)
    {
        if (atomic_load_explicit(counter, memory_order_acquire) >= target)
        {
            return true;
        }
        usleep(1000);
    }
    return false;
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

// A submitted task observes SIGINT and SIGTERM blocked on its executing
// worker thread, across two dispatches (which may land on one or two
// workers of a two-worker pool).
static int test_worker_blocks_shutdown_signals(void)
{
    int failures = 0;
    kith_worker_params_t params = {
        .size = sizeof(kith_worker_params_t),
        .abi_version = KITH_ABI_VERSION,
        .worker_count = 2u,
        .task_capacity = 16u,
    };
    kith_worker_t *pool = nullptr;
    CHECK(kith_worker_create(&params, nullptr, &pool) == 0);

    struct mask_ctx ctx;
    atomic_init(&ctx.observed, 0u);
    atomic_init(&ctx.blocked, 1u);
    CHECK(kith_worker_submit(pool, mask_task, &ctx) == 0);
    CHECK(kith_worker_submit(pool, mask_task, &ctx) == 0);
    CHECK(wait_for(&ctx.observed, 2u));
    CHECK(atomic_load_explicit(&ctx.blocked, memory_order_acquire) == 1u);
    CHECK(atomic_load_explicit(&ctx.observed, memory_order_acquire) == 2u);

    kith_worker_destroy(pool);
    return failures;
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------
int main(void)
{
    int failures = 0;
    failures += test_worker_blocks_shutdown_signals();
    if (failures)
    {
        (void)fprintf(stderr, "worker signal mask: %d failure(s)\n", failures);
    }
    else
    {
        (void)printf("worker signal mask: all tests passed\n");
    }
    return failures ? 1 : 0;
}
