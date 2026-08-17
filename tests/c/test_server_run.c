#include <signal.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

#include <pthread.h>
#include <unistd.h>

#include "kith/server/server.h"
#include "kith/types.h"
#include "kith/version.h"

#include <sys/wait.h>

static int check_cond(bool ok, int line)
{
    if (!ok)
    {
        (void)fprintf(stderr, "server run: assertion at line %d failed\n", line);
        return 1;
    }
    return 0;
}

#define CHECK(cond) failures += check_cond((cond), __LINE__)

// run and shutdown reject a NULL handle with EINVAL. status on NULL returns
// CREATED.
static int test_null_handle(void)
{
    int failures = 0;
    CHECK(kith_server_run(nullptr) == kith_error_return(KITH_EINVAL));
    CHECK(kith_server_shutdown(nullptr) == kith_error_return(KITH_EINVAL));
    CHECK(kith_server_status(nullptr) == KITH_SERVER_STATUS_CREATED);
    return failures;
}

// Build a server on an OS-assigned ephemeral port (listen_port 0) so the
// run tests never contend with the other server tests under parallel ctest.
// The tests run sequentially in one process; each create/destroy cycle binds
// a fresh ephemeral port.
static int make_server(kith_server_t **out)
{
    kith_server_params_t params = {0};
    params.size = sizeof(params);
    params.abi_version = KITH_ABI_VERSION;
    return kith_server_create(&params, nullptr, out);
}

// Blocks until the handle reports RUNNING, bounded so a wedged run cannot
// hang the test. kith_server_run stores run_in_flight before the RUNNING
// status (both release stores, read with acquire), so RUNNING is proof the
// run loop is in flight.
static int wait_until_running(kith_server_t *server, int timeout_ms)
{
    int waited_ms = 0;
    while (waited_ms < timeout_ms)
    {
        if (kith_server_status(server) == KITH_SERVER_STATUS_RUNNING)
        {
            return 1;
        }
        struct timespec ts = {.tv_sec = 0, .tv_nsec = 1L * 1000 * 1000};
        (void)nanosleep(&ts, nullptr);
        waited_ms += 1;
    }
    return 0;
}

// shutdown before run transitions the handle to STOPPED without entering
// the reactor loop; run on a stopped handle returns immediately. The handle
// is destroyed after shutdown.
static int test_shutdown_before_run(void)
{
    int failures = 0;
    kith_server_t *s = nullptr;
    CHECK(make_server(&s) == 0);
    CHECK(kith_server_status(s) == KITH_SERVER_STATUS_CREATED);

    CHECK(kith_server_shutdown(s) == 0);
    CHECK(kith_server_status(s) == KITH_SERVER_STATUS_STOPPED);

    CHECK(kith_server_run(s) == 0);
    CHECK(kith_server_status(s) == KITH_SERVER_STATUS_STOPPED);

    kith_server_destroy(s);
    return failures;
}

// shutdown is idempotent: calling it repeatedly (on a created, then on a
// stopped handle) returns 0 each time and leaves the handle STOPPED.
static int test_shutdown_idempotent(void)
{
    int failures = 0;
    kith_server_t *s = nullptr;
    CHECK(make_server(&s) == 0);

    CHECK(kith_server_shutdown(s) == 0);
    CHECK(kith_server_shutdown(s) == 0);
    CHECK(kith_server_shutdown(s) == 0);
    CHECK(kith_server_status(s) == KITH_SERVER_STATUS_STOPPED);

    kith_server_destroy(s);
    return failures;
}

// A handle may be run again after it has stopped (run accepts the STOPPED
// state). Because the shutdown flag remains set, the second run returns 0
// immediately without re-entering the reactor loop.
static int test_run_after_run(void)
{
    int failures = 0;
    kith_server_t *s = nullptr;
    CHECK(kith_server_create(nullptr, nullptr, &s) == 0);
    CHECK(kith_server_shutdown(s) == 0);

    CHECK(kith_server_run(s) == 0);
    CHECK(kith_server_status(s) == KITH_SERVER_STATUS_STOPPED);

    // A second run on the stopped handle is accepted and returns 0.
    CHECK(kith_server_run(s) == 0);
    CHECK(kith_server_status(s) == KITH_SERVER_STATUS_STOPPED);

    kith_server_destroy(s);
    return failures;
}

// shutdown is safe to call from a thread other than the run-loop thread. The
// run loop blocks on the reactor (driving the periodic tick and the plane
// maintenance) until the tick callback observes the shutdown flag, stops the
// reactor, and lets kith_server_run return. This mirrors the documented
// contract: shutdown from another thread or a signal handler drains the loop.
struct shutdown_arg
{
    kith_server_t *server;
    int saw_running;
};

static void *shutdown_thread_fn(void *raw)
{
    struct shutdown_arg *arg = (struct shutdown_arg *)raw;
    // The contract under test is shutdown arriving while the run loop is in
    // flight; RUNNING is the observable witness, recorded for the caller's
    // assertion so a timeout surfaces as a failure rather than silently
    // degrading into a shutdown-before-run pass.
    arg->saw_running = wait_until_running(arg->server, 5000);
    (void)kith_server_shutdown(arg->server);
    return nullptr;
}

static int test_run_loop_shutdown_from_thread(void)
{
    int failures = 0;
    kith_server_t *s = nullptr;
    CHECK(make_server(&s) == 0);
    CHECK(kith_server_status(s) == KITH_SERVER_STATUS_CREATED);

    struct shutdown_arg arg = {.server = s, .saw_running = 0};
    pthread_t tid;
    int prc = pthread_create(&tid, nullptr, shutdown_thread_fn, &arg);
    CHECK(prc == 0);

    // Blocks until the tick callback observes the shutdown flag.
    CHECK(kith_server_run(s) == 0);
    CHECK(kith_server_status(s) == KITH_SERVER_STATUS_STOPPED);

    if (prc == 0)
    {
        (void)pthread_join(tid, nullptr);
        CHECK(arg.saw_running);
    }

    kith_server_destroy(s);
    return failures;
}

// kith_server_destroy while the run loop is in flight aborts the process:
// the @thread_safety contract is enforced at runtime rather than left as a
// convention. The violation runs in a forked child so the abort terminates
// the child, not the test process; the parent asserts the child died from
// SIGABRT.
struct run_only_arg
{
    kith_server_t *server;
};

static void *run_only_thread_fn(void *raw)
{
    struct run_only_arg *arg = (struct run_only_arg *)raw;
    (void)kith_server_run(arg->server);
    return nullptr;
}

static int test_destroy_while_run_aborts(void)
{
    int failures = 0;
    pid_t pid = fork();
    if (pid < 0)
    {
        (void)fprintf(stderr, "server run: fork failed\n");
        return 1;
    }
    if (pid == 0)
    {
        kith_server_t *s = nullptr;
        if (make_server(&s) != 0)
        {
            _exit(2);
        }
        struct run_only_arg arg = {.server = s};
        pthread_t tid;
        int prc = pthread_create(&tid, nullptr, run_only_thread_fn, &arg);
        if (prc != 0)
        {
            // No run in flight; destroy cleanly and signal a setup failure.
            kith_server_destroy(s);
            _exit(3);
        }
        // Destroy aborts only under a live run loop; RUNNING is the
        // observable witness of run_in_flight.
        if (!wait_until_running(s, 2000))
        {
            _exit(4);
        }
        // Contract violation: destroy while the run loop is in flight.
        kith_server_destroy(s);
        _exit(0); // Unreachable: the prior call aborts.
    }

    int status = 0;
    if (waitpid(pid, &status, 0) < 0)
    {
        (void)fprintf(stderr, "server run: waitpid failed\n");
        return 1;
    }
    if (!WIFSIGNALED(status) || WTERMSIG(status) != SIGABRT)
    {
        // A child that exited on its own never entered the run loop; the
        // exit code distinguishes the setup failure paths.
        (void)fprintf(stderr, "server run: child exited without SIGABRT (status %d)\n", status);
    }
    CHECK(WIFSIGNALED(status));
    CHECK(WTERMSIG(status) == SIGABRT);
    return failures;
}

int main(void)
{
    int rc = 0;
    rc |= test_null_handle();
    rc |= test_shutdown_before_run();
    rc |= test_shutdown_idempotent();
    rc |= test_run_after_run();
    rc |= test_run_loop_shutdown_from_thread();
    rc |= test_destroy_while_run_aborts();
    if (rc != 0)
    {
        (void)fprintf(stderr, "server run tests FAILED\n");
    }
    return rc;
}
