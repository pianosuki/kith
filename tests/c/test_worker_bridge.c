/* Worker reactor-bridge tests: the reactor fires a C hop that submits the
 * user callback to the worker pool; the callback runs on a worker thread, not
 * the reactor thread. Exercises the ready bridge (kith_reactor_add path) and
 * the task bridge (kith_reactor_schedule path). */

#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <unistd.h>

#include "kith/reactor/reactor.h"
#include "kith/types.h"
#include "kith/version.h"
#include "kith/worker/worker.h"

#include <sys/eventfd.h>

// ---------------------------------------------------------------------------
// test harness
// ---------------------------------------------------------------------------

static int check_cond(bool ok, int line)
{
    if (!ok)
    {
        (void)fprintf(stderr, "worker bridge: assertion at line %d failed\n", line);
        return 1;
    }
    return 0;
}

#define CHECK(cond) failures += check_cond((cond), __LINE__)

// Wait until *counter reaches target, polling every 1 ms up to 5 s.
static bool wait_for(atomic_uint *counter, uint32_t target)
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
// ready bridge: reactor readiness → C hop → worker runs user callback
// ---------------------------------------------------------------------------

struct ready_ctx
{
    _Atomic uint32_t *fired;
    int efd;
    kith_reactor_t *reactor;
};

static void ready_user_cb(int fd, unsigned int events, void *arg)
{
    (void)events;
    struct ready_ctx *c = arg;
    // Drain the eventfd counter so the reactor does not re-fire.
    uint64_t val = 0;
    const ssize_t drained = read(fd, &val, sizeof(val));
    (void)drained;
    kith_reactor_stop(c->reactor);
    // The release store publishes the completed stop: a waiter that
    // observes the flag may destroy the reactor.
    atomic_store_explicit(c->fired, 1u, memory_order_release);
}

static int test_bridge_ready(void)
{
    int failures = 0;
    kith_worker_t *pool = nullptr;
    kith_reactor_t *reactor = nullptr;
    kith_worker_bridge_t *bridge = nullptr;

    kith_worker_params_t wparams = {
        .size = sizeof(kith_worker_params_t),
        .abi_version = KITH_ABI_VERSION,
        .worker_count = 1u,
        .task_capacity = 16u,
    };
    CHECK(kith_worker_create(&wparams, nullptr, &pool) == 0);

    kith_reactor_params_t rparams = {
        .size = sizeof(kith_reactor_params_t),
        .abi_version = KITH_ABI_VERSION,
        .max_fds = 64,
        .task_capacity = 64,
    };
    CHECK(kith_reactor_create(&rparams, nullptr, &reactor) == 0);

    int efd = eventfd(0, EFD_NONBLOCK);
    CHECK(efd >= 0);

    _Atomic uint32_t fired = 0u;
    struct ready_ctx ctx = {.fired = &fired, .efd = efd, .reactor = reactor};
    CHECK(kith_worker_bridge_create_ready(pool, ready_user_cb, &ctx, &bridge) == 0);

    // The hop signature (void(int, unsigned, void*)) is structurally
    // identical to kith_reactor_cb; the cast is between compatible types.
    kith_reactor_cb hop = (kith_reactor_cb)kith_worker_bridge_ready_cb;
    CHECK(kith_reactor_add(reactor, efd, KITH_REACTOR_IN, hop, bridge) == 0);

    // Signal the eventfd to trigger readiness.
    uint64_t one = 1u;
    CHECK(write(efd, &one, sizeof(one)) == (ssize_t)sizeof(one));

    // Run the reactor: it polls, sees readiness, calls the hop (C, no Python),
    // the hop submits to the worker, the worker runs ready_user_cb which
    // drains the eventfd and stops the reactor. Run returns.
    CHECK(kith_reactor_run(reactor) == 0);

    // The callback ran on a worker thread; wait for the flag.
    CHECK(wait_for(&fired, 1u));
    CHECK(atomic_load_explicit(&fired, memory_order_acquire) == 1u);

    CHECK(kith_reactor_del(reactor, efd) == 0);
    kith_worker_bridge_destroy(bridge);
    close(efd);
    kith_reactor_destroy(reactor);
    kith_worker_destroy(pool);
    return failures;
}

// ---------------------------------------------------------------------------
// task bridge: reactor timer → C hop → worker runs user callback
// ---------------------------------------------------------------------------

struct task_ctx
{
    _Atomic uint32_t *fired;
    kith_reactor_t *reactor;
};

static void task_user_cb(void *arg)
{
    struct task_ctx *c = arg;
    kith_reactor_stop(c->reactor);
    // The release store publishes the completed stop: a waiter that
    // observes the flag may destroy the reactor.
    atomic_store_explicit(c->fired, 1u, memory_order_release);
}

static int test_bridge_task(void)
{
    int failures = 0;
    kith_worker_t *pool = nullptr;
    kith_reactor_t *reactor = nullptr;
    kith_worker_bridge_t *bridge = nullptr;

    kith_worker_params_t wparams = {
        .size = sizeof(kith_worker_params_t),
        .abi_version = KITH_ABI_VERSION,
        .worker_count = 1u,
        .task_capacity = 16u,
    };
    CHECK(kith_worker_create(&wparams, nullptr, &pool) == 0);

    kith_reactor_params_t rparams = {
        .size = sizeof(kith_reactor_params_t),
        .abi_version = KITH_ABI_VERSION,
        .max_fds = 64,
        .task_capacity = 64,
    };
    CHECK(kith_reactor_create(&rparams, nullptr, &reactor) == 0);

    _Atomic uint32_t fired = 0u;
    struct task_ctx ctx = {.fired = &fired, .reactor = reactor};
    CHECK(kith_worker_bridge_create_task(pool, task_user_cb, &ctx, &bridge) == 0);

    // Schedule the hop 1 ms ahead (fires on the first run iteration).
    uint64_t deadline = kith_reactor_now_ms(reactor) + 1u;
    kith_reactor_task_cb hop = (kith_reactor_task_cb)kith_worker_bridge_task_cb;
    CHECK(kith_reactor_schedule(reactor, deadline, hop, bridge) == 0);
    // Run the reactor: the timer fires on the reactor thread, the hop submits
    // to the worker (C, no Python), the worker runs task_user_cb which stops
    // the reactor. Run returns.
    CHECK(kith_reactor_run(reactor) == 0);

    CHECK(wait_for(&fired, 1u));
    CHECK(atomic_load_explicit(&fired, memory_order_acquire) == 1u);

    kith_worker_bridge_destroy(bridge);
    kith_reactor_destroy(reactor);
    kith_worker_destroy(pool);
    return failures;
}

// ---------------------------------------------------------------------------
// arg validation
// ---------------------------------------------------------------------------

static int test_bridge_arg_validation(void)
{
    int failures = 0;
    kith_worker_t *pool = nullptr;
    kith_worker_bridge_t *bridge = nullptr;

    kith_worker_params_t wparams = {
        .size = sizeof(kith_worker_params_t),
        .abi_version = KITH_ABI_VERSION,
        .worker_count = 1u,
        .task_capacity = 16u,
    };
    CHECK(kith_worker_create(&wparams, nullptr, &pool) == 0);

    CHECK(kith_worker_bridge_create_ready(nullptr, ready_user_cb, nullptr, &bridge) ==
          kith_error_return(KITH_EINVAL));
    CHECK(kith_worker_bridge_create_ready(pool, nullptr, nullptr, &bridge) ==
          kith_error_return(KITH_EINVAL));
    CHECK(kith_worker_bridge_create_ready(pool, ready_user_cb, nullptr, nullptr) ==
          kith_error_return(KITH_EINVAL));
    CHECK(kith_worker_bridge_create_task(nullptr, task_user_cb, nullptr, &bridge) ==
          kith_error_return(KITH_EINVAL));
    CHECK(kith_worker_bridge_create_task(pool, nullptr, nullptr, &bridge) ==
          kith_error_return(KITH_EINVAL));

    kith_worker_bridge_destroy(nullptr);         // NULL is a no-op
    kith_worker_bridge_ready_cb(0, 0u, nullptr); // NULL ctx is a no-op
    kith_worker_bridge_task_cb(nullptr);         // NULL ctx is a no-op

    kith_worker_destroy(pool);
    return failures;
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------
int main(void)
{
    int failures = 0;
    failures += test_bridge_ready();
    failures += test_bridge_task();
    failures += test_bridge_arg_validation();
    if (failures)
    {
        (void)fprintf(stderr, "worker bridge: %d failure(s)\n", failures);
    }
    else
    {
        (void)printf("worker bridge: all tests passed\n");
    }
    return failures ? 1 : 0;
}
