/* Native tick admission, serialization, and captured callback lifetimes. */

#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <time.h>

#include "server/server_internal.h"

struct callback_state
{
    _Atomic bool entered;
    _Atomic bool release;
    _Atomic unsigned calls;
    _Atomic uint64_t tick;
};

static int check_cond(bool ok, int line)
{
    if (!ok)
    {
        (void)fprintf(stderr, "native tick assertion at line %d failed\n", line);
    }
    return ok ? 0 : 1;
}

#define CHECK(cond) failures += check_cond((cond), __LINE__)

static bool wait_flag(const _Atomic bool *flag)
{
    for (unsigned i = 0; i < 5000u; ++i)
    {
        if (atomic_load(flag))
        {
            return true;
        }
        struct timespec pause = {.tv_nsec = 1000000};
        (void)nanosleep(&pause, nullptr);
    }
    return false;
}

static void blocking_task(void *data)
{
    struct callback_state *state = data;
    atomic_fetch_add(&state->calls, 1u);
    atomic_store(&state->entered, true);
    (void)wait_flag(&state->release);
}

static void blocked_tick(uint64_t tick, void *data)
{
    struct callback_state *state = data;
    atomic_store(&state->tick, tick);
    blocking_task(state);
}

static void record_tick(uint64_t tick, void *data)
{
    struct callback_state *state = data;
    atomic_store(&state->tick, tick);
    atomic_fetch_add(&state->calls, 1u);
    atomic_store(&state->entered, true);
}

static void record_task(void *data)
{
    record_tick(0u, data);
}

static int init_server(struct kith_server *server, uint32_t workers, uint32_t capacity)
{
    *server = (struct kith_server){.allocator = kith_allocator_default()};
    atomic_init(&server->tick_native_busy, false);
    atomic_init(&server->tick_dropped, 0u);
    int rc = pthread_mutex_init(&server->tick_lock, nullptr);
    if (rc != 0 || workers == 0u)
    {
        return rc;
    }
    kith_worker_params_t params = {.size = sizeof(params),
                                   .abi_version = KITH_ABI_VERSION,
                                   .worker_count = workers,
                                   .task_capacity = capacity};
    return kith_worker_create(&params, nullptr, &server->workers);
}

static void finish_server(struct kith_server *server)
{
    kith_worker_destroy(server->workers);
    pthread_mutex_destroy(&server->tick_lock);
}

static int test_native_missing_pool_releases_slot(void)
{
    int failures = 0;
    struct kith_server server;
    struct callback_state state = {0};
    CHECK(init_server(&server, 0u, 0u) == 0);
    CHECK(kith_server_register_tick_handler(
              &server, record_tick, &state, KITH_SERVER_HANDLER_POOL) == 0);
    server_dispatch_tick_hook(&server, 1u);
    CHECK(atomic_load(&server.tick_dropped) == 1u);
    CHECK(!atomic_load(&server.tick_native_busy));
    CHECK(atomic_load(&state.calls) == 0u);
    finish_server(&server);
    return failures;
}

// Occupied worker nodes refuse admission without retaining the native slot.
static int test_native_full_pool_recovers(void)
{
    int failures = 0;
    struct kith_server server;
    struct callback_state blocker = {0};
    struct callback_state queued = {0};
    struct callback_state state = {0};
    CHECK(init_server(&server, 1u, 2u) == 0);
    CHECK(kith_worker_submit(server.workers, blocking_task, &blocker) == 0);
    CHECK(wait_flag(&blocker.entered));
    CHECK(kith_worker_submit(server.workers, record_task, &queued) == 0);
    CHECK(kith_server_register_tick_handler(
              &server, record_tick, &state, KITH_SERVER_HANDLER_POOL) == 0);
    server_dispatch_tick_hook(&server, 1u);
    CHECK(atomic_load(&server.tick_dropped) == 1u);
    CHECK(!atomic_load(&server.tick_native_busy));
    CHECK(atomic_load(&state.calls) == 0u);
    atomic_store(&blocker.release, true);
    CHECK(wait_flag(&queued.entered));
    for (unsigned i = 0; i < 5000u && !atomic_load(&state.entered); ++i)
    {
        server_dispatch_tick_hook(&server, 2u + i);
        struct timespec pause = {.tv_nsec = 1000000};
        (void)nanosleep(&pause, nullptr);
    }
    finish_server(&server);
    CHECK(atomic_load(&state.calls) == 1u);
    CHECK(atomic_load(&state.tick) >= 2u);
    return failures;
}

// Replacement retains the queued callback's context and shares its serial slot.
static int test_native_replacement_keeps_serial_slot(void)
{
    int failures = 0;
    struct kith_server server;
    struct callback_state original = {0};
    struct callback_state replacement = {0};
    CHECK(init_server(&server, 4u, 8u) == 0);
    CHECK(kith_server_register_tick_handler(
              &server, blocked_tick, &original, KITH_SERVER_HANDLER_POOL) == 0);
    server_dispatch_tick_hook(&server, 1u);
    CHECK(wait_flag(&original.entered));
    CHECK(kith_server_register_tick_handler(
              &server, record_tick, &replacement, KITH_SERVER_HANDLER_POOL) == 0);
    for (uint64_t tick = 2u; tick <= 20u; ++tick)
    {
        server_dispatch_tick_hook(&server, tick);
    }
    CHECK(atomic_load(&server.tick_dropped) == 19u);
    CHECK(atomic_load(&original.calls) == 1u);
    CHECK(atomic_load(&replacement.calls) == 0u);
    CHECK(kith_server_unregister_tick_handler(&server) == 0);
    server_dispatch_tick_hook(&server, 21u);
    CHECK(atomic_load(&server.tick_dropped) == 19u);
    atomic_store(&original.release, true);
    finish_server(&server);
    CHECK(atomic_load(&original.tick) == 1u);
    CHECK(atomic_load(&replacement.calls) == 0u);
    CHECK(!atomic_load(&server.tick_native_busy));
    return failures;
}

// A queued native tick owns the original registration snapshot through unregister.
static int test_native_queued_snapshot_survives_unregister(void)
{
    int failures = 0;
    struct kith_server server;
    struct callback_state blocker = {0};
    struct callback_state original = {0};
    struct callback_state replacement = {0};
    CHECK(init_server(&server, 1u, 3u) == 0);
    CHECK(kith_worker_submit(server.workers, blocking_task, &blocker) == 0);
    CHECK(wait_flag(&blocker.entered));
    CHECK(kith_server_register_tick_handler(
              &server, record_tick, &original, KITH_SERVER_HANDLER_POOL) == 0);
    server_dispatch_tick_hook(&server, 10u);
    CHECK(atomic_load(&server.tick_native_busy));
    CHECK(atomic_load(&original.calls) == 0u);
    CHECK(kith_server_register_tick_handler(
              &server, record_tick, &replacement, KITH_SERVER_HANDLER_POOL) == 0);
    server_dispatch_tick_hook(&server, 11u);
    CHECK(kith_server_unregister_tick_handler(&server) == 0);
    atomic_store(&blocker.release, true);
    finish_server(&server);
    CHECK(atomic_load(&original.calls) == 1u);
    CHECK(atomic_load(&original.tick) == 10u);
    CHECK(atomic_load(&replacement.calls) == 0u);
    CHECK(atomic_load(&server.tick_dropped) == 1u);
    CHECK(!atomic_load(&server.tick_native_busy));
    return failures;
}

int main(void)
{
    return test_native_missing_pool_releases_slot() | test_native_full_pool_recovers() |
           test_native_replacement_keeps_serial_slot() |
           test_native_queued_snapshot_survives_unregister();
}
