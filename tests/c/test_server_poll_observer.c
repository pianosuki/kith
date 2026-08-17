/* Poll-observer registration and dispatch: the synchronous per-tick hook
 * the run-loop thread invokes before plane work (the bounded
 * exception for main-thread embeddings), covering NULL rejection,
 * invocation counting, nonzero-return drain, and clearing. */

#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>

#include <pthread.h>

#include "kith/server/server.h"
#include "kith/types.h"
#include "kith/version.h"

static int check_cond(bool ok, int line)
{
    if (!ok)
    {
        (void)fprintf(stderr, "server poll observer: assertion at line %d failed\n", line);
        return 1;
    }
    return 0;
}

#define CHECK(cond) failures += check_cond((cond), __LINE__)

// Build a server on an OS-assigned ephemeral port so parallel ctest never
// contends on a fixed listener.
static int make_server(kith_server_t **out)
{
    kith_server_params_t params = {0};
    params.size = sizeof(params);
    params.abi_version = KITH_ABI_VERSION;
    return kith_server_create(&params, nullptr, out);
}

// register rejects a NULL handle with EINVAL; fn may be NULL (clears).
static int test_null_and_clear_semantics(void)
{
    int failures = 0;
    CHECK(kith_server_register_poll_observer(nullptr, nullptr, nullptr) ==
          kith_error_return(KITH_EINVAL));

    kith_server_t *s = nullptr;
    CHECK(make_server(&s) == 0);
    CHECK(kith_server_register_poll_observer(s, nullptr, nullptr) == 0);
    kith_server_destroy(s);
    return failures;
}

struct poll_counter
{
    atomic_uint calls;
};

// Counts invocations and keeps the loop running; the helper thread drives
// shutdown from outside so the observer's continue path is exercised.
static int counting_observe(void *user_data)
{
    struct poll_counter *counter = (struct poll_counter *)user_data;
    atomic_fetch_add_explicit(&counter->calls, 1u, memory_order_relaxed);
    return 0;
}

// Blocks until the handle reports RUNNING, bounded so a wedged run cannot
// hang the test. kith_server_run stores run_in_flight before the RUNNING
// status, so RUNNING is proof the run loop is in flight.
static void wait_until_running(kith_server_t *server)
{
    int waited_ms = 0;
    while (waited_ms < 5000 && kith_server_status(server) != KITH_SERVER_STATUS_RUNNING)
    {
        struct timespec ts = {.tv_sec = 0, .tv_nsec = 1L * 1000 * 1000};
        (void)nanosleep(&ts, nullptr);
        waited_ms += 1;
    }
}

struct shutdown_arg
{
    kith_server_t *server;
};

static void *shutdown_thread_fn(void *raw)
{
    struct shutdown_arg *arg = (struct shutdown_arg *)raw;
    // Shutdown lands while the run loop is in flight (the contract these
    // tests drive); a timeout degrades into shutdown-before-run, which the
    // callers' observer-count assertions then fail on.
    wait_until_running(arg->server);
    (void)kith_server_shutdown(arg->server);
    return nullptr;
}

static int test_observer_invoked_each_tick(void)
{
    int failures = 0;
    kith_server_t *s = nullptr;
    CHECK(make_server(&s) == 0);

    struct poll_counter counter = {.calls = 0};
    CHECK(kith_server_register_poll_observer(s, counting_observe, &counter) == 0);

    struct shutdown_arg arg = {.server = s};
    pthread_t tid;
    int prc = pthread_create(&tid, nullptr, shutdown_thread_fn, &arg);
    CHECK(prc == 0);

    CHECK(kith_server_run(s) == 0);
    CHECK(kith_server_status(s) == KITH_SERVER_STATUS_STOPPED);
    CHECK(atomic_load_explicit(&counter.calls, memory_order_relaxed) >= 1u);

    if (prc == 0)
    {
        (void)pthread_join(tid, nullptr);
    }

    kith_server_destroy(s);
    return failures;
}

// A nonzero observer return requests the graceful shutdown: the drain starts
// on the current tick and run returns without any external shutdown call.
static int drain_on_first_call(void *user_data)
{
    struct poll_counter *counter = (struct poll_counter *)user_data;
    atomic_fetch_add_explicit(&counter->calls, 1u, memory_order_relaxed);
    return 1;
}

static int test_observer_nonzero_return_drains(void)
{
    int failures = 0;
    kith_server_t *s = nullptr;
    CHECK(make_server(&s) == 0);

    struct poll_counter counter = {.calls = 0};
    CHECK(kith_server_register_poll_observer(s, drain_on_first_call, &counter) == 0);

    CHECK(kith_server_run(s) == 0);
    CHECK(kith_server_status(s) == KITH_SERVER_STATUS_STOPPED);
    CHECK(atomic_load_explicit(&counter.calls, memory_order_relaxed) == 1u);

    // Destroy succeeding proves the run-in-flight flag was cleared on the
    // drain path (a stale flag aborts here).
    kith_server_destroy(s);
    return failures;
}

// Clearing the registration (fn NULL) stops the invocation.
static int test_cleared_observer_not_invoked(void)
{
    int failures = 0;
    kith_server_t *s = nullptr;
    CHECK(make_server(&s) == 0);

    struct poll_counter counter = {.calls = 0};
    CHECK(kith_server_register_poll_observer(s, counting_observe, &counter) == 0);
    CHECK(kith_server_register_poll_observer(s, nullptr, nullptr) == 0);

    struct shutdown_arg arg = {.server = s};
    pthread_t tid;
    int prc = pthread_create(&tid, nullptr, shutdown_thread_fn, &arg);
    CHECK(prc == 0);

    CHECK(kith_server_run(s) == 0);
    CHECK(kith_server_status(s) == KITH_SERVER_STATUS_STOPPED);
    CHECK(atomic_load_explicit(&counter.calls, memory_order_relaxed) == 0u);

    if (prc == 0)
    {
        (void)pthread_join(tid, nullptr);
    }

    kith_server_destroy(s);
    return failures;
}

int main(void)
{
    int rc = 0;
    rc |= test_null_and_clear_semantics();
    rc |= test_observer_invoked_each_tick();
    rc |= test_observer_nonzero_return_drains();
    rc |= test_cleared_observer_not_invoked();
    if (rc != 0)
    {
        (void)fprintf(stderr, "server poll observer tests FAILED\n");
    }
    return rc;
}
