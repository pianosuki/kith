#include <errno.h>
#include <signal.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <fcntl.h>
#include <pthread.h>
#include <unistd.h>

#include "kith/reactor/reactor.h"
#include "kith/types.h"
#include "kith/version.h"

#include <sys/socket.h>
#include <sys/wait.h>

// ---------------------------------------------------------------------------
// test harness
// ---------------------------------------------------------------------------

static int check_cond(bool ok, int line)
{
    if (!ok)
    {
        (void)fprintf(stderr, "reactor basic: assertion at line %d failed\n", line);
        return 1;
    }
    return 0;
}

#define CHECK(cond) failures += check_cond((cond), __LINE__)

// ---------------------------------------------------------------------------
// shared dummy callback (used by multiple tests)
// ---------------------------------------------------------------------------

static void reactor_basic_dummy_cb(int fd, unsigned int events, void *ctx)
{
    (void)fd;
    (void)events;
    (void)ctx;
}

// Create a reactor with a small io_uring ring. Unit tests poll a handful of
// fds at most; the production default max_fds rounds up to a 4096-entry ring
// (~300 KB of memcg-accounted kernel memory each), and a ctest run creating
// several such rings per process can pressure the cgroup memory budget. The
// test that exercises the NULL-params default path (test_create_defaults) is
// the only one that keeps the full-size ring.
static int reactor_create_bounded(kith_reactor_t **out)
{
    kith_reactor_params_t params = {
        .size = sizeof(kith_reactor_params_t),
        .abi_version = 1u,
        .max_fds = 64,
        .task_capacity = 64,
    };
    return kith_reactor_create(&params, nullptr, out);
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

// Create with all defaults (NULL params).
static int test_create_defaults(void)
{
    int failures = 0;
    kith_reactor_t *reactor = nullptr;

    CHECK(kith_reactor_create(nullptr, nullptr, &reactor) == 0);
    CHECK(reactor != nullptr);

    kith_reactor_destroy(reactor);
    return failures;
}

// Create with custom params.
static int test_create_params(void)
{
    int failures = 0;
    kith_reactor_t *reactor = nullptr;

    kith_reactor_params_t params = {
        .size = sizeof(kith_reactor_params_t),
        .abi_version = KITH_ABI_VERSION,
        .max_fds = 64,
        .task_capacity = 32,
    };
    CHECK(kith_reactor_create(&params, nullptr, &reactor) == 0);
    CHECK(reactor != nullptr);

    kith_reactor_destroy(reactor);
    return failures;
}

// Create with NULL out_reactor.
static int test_create_null_out(void)
{
    int failures = 0;
    CHECK(kith_reactor_create(nullptr, nullptr, nullptr) == kith_error_return(KITH_EINVAL));
    return failures;
}

// Create with bad ABI version.
static int test_create_bad_abi(void)
{
    int failures = 0;
    kith_reactor_t *reactor = nullptr;

    kith_reactor_params_t params = {
        .size = sizeof(kith_reactor_params_t),
        .abi_version = 9999u,
    };
    CHECK(kith_reactor_create(&params, nullptr, &reactor) == kith_error_return(KITH_EABIVER));
    CHECK(reactor == nullptr);
    return failures;
}

// Create with undersized size.
static int test_create_undersized(void)
{
    int failures = 0;
    kith_reactor_t *reactor = nullptr;

    kith_reactor_params_t params = {
        .size = 4, // < sizeof(kith_reactor_params_t)
        .abi_version = KITH_ABI_VERSION,
    };
    CHECK(kith_reactor_create(&params, nullptr, &reactor) == kith_error_return(KITH_ESIZE));
    CHECK(reactor == nullptr);
    return failures;
}

// Destroy with NULL (no-op).
static int test_destroy_null(void)
{
    int failures = 0;
    kith_reactor_destroy(nullptr);
    // Must not crash.
    return failures;
}

// Register a file descriptor.
static int test_add_fd(void)
{
    int failures = 0;
    kith_reactor_t *reactor = nullptr;

    CHECK(reactor_create_bounded(&reactor) == 0);

    int fds[2];
    CHECK(pipe(fds) == 0);

    // NULL callback
    CHECK(kith_reactor_add(reactor, fds[0], KITH_REACTOR_IN, nullptr, nullptr) ==
          kith_error_return(KITH_EINVAL));

    // Valid registration
    CHECK(kith_reactor_add(reactor, fds[0], KITH_REACTOR_IN, reactor_basic_dummy_cb, nullptr) == 0);

    // Duplicate add → EEXIST
    CHECK(kith_reactor_add(reactor, fds[0], KITH_REACTOR_IN, reactor_basic_dummy_cb, nullptr) ==
          kith_error_return(KITH_EEXIST));

    // Del
    CHECK(kith_reactor_del(reactor, fds[0]) == 0);

    // Del again → ENOENT
    CHECK(kith_reactor_del(reactor, fds[0]) == kith_error_return(KITH_ENOENT));

    close(fds[0]);
    close(fds[1]);
    kith_reactor_destroy(reactor);
    return failures;
}

// Register, modify, deregister.
static int test_mod_del(void)
{
    int failures = 0;
    kith_reactor_t *reactor = nullptr;

    CHECK(reactor_create_bounded(&reactor) == 0);

    int fds[2];
    CHECK(pipe(fds) == 0);

    CHECK(kith_reactor_add(reactor, fds[0], KITH_REACTOR_IN, reactor_basic_dummy_cb, nullptr) == 0);

    // Modify to OUT
    CHECK(kith_reactor_mod(reactor, fds[0], KITH_REACTOR_OUT) == 0);

    // Modify non-registered fd → ENOENT
    CHECK(kith_reactor_mod(reactor, 9999, KITH_REACTOR_IN) == kith_error_return(KITH_ENOENT));

    // Del
    CHECK(kith_reactor_del(reactor, fds[0]) == 0);

    close(fds[0]);
    close(fds[1]);
    kith_reactor_destroy(reactor);
    return failures;
}

// Run with NULL reactor.
static int test_run_null(void)
{
    int failures = 0;
    CHECK(kith_reactor_run(nullptr) == kith_error_return(KITH_EINVAL));
    return failures;
}

// Run on an empty reactor (no fds, no tasks, no timers) returns after one
// idle iteration instead of blocking indefinitely.
static int test_run_empty_returns(void)
{
    int failures = 0;
    kith_reactor_t *reactor = nullptr;
    CHECK(reactor_create_bounded(&reactor) == 0);
    CHECK(kith_reactor_run(reactor) == 0);
    kith_reactor_destroy(reactor);
    return failures;
}

// Stop with NULL (no-op).
static int test_stop_null(void)
{
    int failures = 0;
    kith_reactor_stop(nullptr);
    return failures;
}

// A run that exits via kith_reactor_stop leaves the reactor reusable: the
// next run drains newly submitted tasks and stops on its own request.
static int g_reactor_basic_tasks_run = 0;

static void reactor_basic_stop_cb(void *ctx)
{
    kith_reactor_stop((kith_reactor_t *)ctx);
}

static void reactor_basic_count_stop_cb(void *ctx)
{
    g_reactor_basic_tasks_run++;
    kith_reactor_stop((kith_reactor_t *)ctx);
}

static int test_run_resumes_after_stop(void)
{
    int failures = 0;
    kith_reactor_t *reactor = nullptr;
    CHECK(reactor_create_bounded(&reactor) == 0);

    CHECK(kith_reactor_submit(reactor, reactor_basic_stop_cb, reactor) == 0);
    CHECK(kith_reactor_run(reactor) == 0);

    g_reactor_basic_tasks_run = 0;
    CHECK(kith_reactor_submit(reactor, reactor_basic_count_stop_cb, reactor) == 0);
    CHECK(kith_reactor_run(reactor) == 0);
    CHECK(g_reactor_basic_tasks_run == 1);

    kith_reactor_destroy(reactor);
    return failures;
}

// A stop delivered before the next run enters stops that run: the loop's
// first check honors the pending request instead of erasing it, so a
// caller that stops between runs cannot resurrect the loop.
static void reactor_basic_timer_count_cb(void *ctx)
{
    (*(int *)ctx)++;
}

static int test_stop_before_run_stops_next_run(void)
{
    int failures = 0;
    kith_reactor_t *reactor = nullptr;
    int timer_fires = 0;
    CHECK(reactor_create_bounded(&reactor) == 0);

    // The far-out timer keeps a loop that erased the request blocked until
    // it fires; honoring the request returns before that can happen.
    CHECK(kith_reactor_schedule(reactor,
                                kith_reactor_now_ms(reactor) + 5000u,
                                reactor_basic_timer_count_cb,
                                &timer_fires) == 0);
    kith_reactor_stop(reactor);
    CHECK(kith_reactor_run(reactor) == 0);
    CHECK(timer_fires == 0);

    // The consuming run left a clean flag: a fresh run drains a submitted
    // task and stops on its own request.
    g_reactor_basic_tasks_run = 0;
    CHECK(kith_reactor_submit(reactor, reactor_basic_count_stop_cb, reactor) == 0);
    CHECK(kith_reactor_run(reactor) == 0);
    CHECK(g_reactor_basic_tasks_run == 1);

    kith_reactor_destroy(reactor);
    return failures;
}

// now_ms with NULL returns 0.
static int test_now_ms_null(void)
{
    int failures = 0;
    CHECK(kith_reactor_now_ms(nullptr) == 0);
    return failures;
}

// Create → destroy → create (reuse pattern).
static int test_create_destroy_cycle(void)
{
    int failures = 0;
    for (int i = 0; i < 3; i++)
    {
        kith_reactor_t *reactor = nullptr;
        CHECK(reactor_create_bounded(&reactor) == 0);
        CHECK(reactor != nullptr);
        kith_reactor_destroy(reactor);
    }
    return failures;
}

// add with NULL reactor.
static int test_add_null_reactor(void)
{
    int failures = 0;
    CHECK(kith_reactor_add(nullptr, 0, 0, reactor_basic_dummy_cb, nullptr) ==
          kith_error_return(KITH_EINVAL));
    return failures;
}

// add with negative fd.
static int test_add_negative_fd(void)
{
    int failures = 0;
    kith_reactor_t *reactor = nullptr;

    CHECK(reactor_create_bounded(&reactor) == 0);

    CHECK(kith_reactor_add(reactor, -1, 0, reactor_basic_dummy_cb, nullptr) ==
          kith_error_return(KITH_EINVAL));

    kith_reactor_destroy(reactor);
    return failures;
}

// mod/del with NULL reactor.
static int test_mod_del_null(void)
{
    int failures = 0;
    CHECK(kith_reactor_mod(nullptr, 0, 0) == kith_error_return(KITH_EINVAL));
    CHECK(kith_reactor_del(nullptr, 0) == kith_error_return(KITH_EINVAL));
    return failures;
}

// ---------------------------------------------------------------------------
// threaded re-registration tests
//
// The backend keeps one outstanding readiness registration per fd. Re-issuing
// a mod with an unchanged mask must be a no-op, and a mask-changing mod made
// from inside a readiness handler must replace (not duplicate) the backend's
// post-dispatch re-registration — two outstanding registrations complete
// twice per event and double-dispatch every subsequent readiness. A writer
// sends exactly MESSAGES discrete bytes, waiting for each to be dispatched
// before sending the next; the dispatch count must land exactly on MESSAGES
// (duplicate registrations inflate it, lost wakeups stall it).
// ---------------------------------------------------------------------------

struct mod_stress_ctx
{
    kith_reactor_t *reactor;
    atomic_int dispatches;
    int mode; // 0: same-mask re-issue; 1: pause/resume transitions per dispatch
};

static void *mod_stress_run(void *raw)
{
    kith_reactor_t *reactor = raw;
    (void)kith_reactor_run(reactor);
    return nullptr;
}

static void mod_stress_cb(int fd, unsigned int events, void *ctx)
{
    (void)events;
    struct mod_stress_ctx *st = ctx;

    // Drain everything available so level-triggered readiness does not
    // redispatch for bytes this callback already consumed.
    for (;;)
    {
        uint8_t buf[64];
        ssize_t n = read(fd, buf, sizeof(buf));
        if (n <= 0)
        {
            break; // EAGAIN at the drain boundary
        }
    }
    atomic_fetch_add_explicit(&st->dispatches, 1, memory_order_relaxed);

    if (st->mode == 0)
    {
        // Same mask as registered: the no-op path callers hit every tick.
        (void)kith_reactor_mod(st->reactor, fd, KITH_REACTOR_IN);
    }
    else
    {
        // Pause (mask 0), resume, and re-issue the resume unchanged: two
        // real transitions plus the unchanged-mask tail in one dispatch.
        (void)kith_reactor_mod(st->reactor, fd, 0u);
        (void)kith_reactor_mod(st->reactor, fd, KITH_REACTOR_IN);
        (void)kith_reactor_mod(st->reactor, fd, KITH_REACTOR_IN);
    }
}

static bool mod_stress_wait(const struct mod_stress_ctx *st, int target, unsigned int ms)
{
    for (unsigned int elapsed = 0u; elapsed < ms; elapsed += 2u)
    {
        if (atomic_load_explicit(&st->dispatches, memory_order_acquire) >= target)
        {
            return true;
        }
        struct timespec ts = {.tv_sec = 0, .tv_nsec = 2L * 1000 * 1000};
        (void)nanosleep(&ts, nullptr);
    }
    return false;
}

static int test_mod_dispatch_invariant(int mode)
{
    int failures = 0;
    kith_reactor_t *reactor = nullptr;
    CHECK(reactor_create_bounded(&reactor) == 0);

    int fds[2];
    CHECK(socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, fds) == 0);
    int flags = fcntl(fds[0], F_GETFL);
    CHECK(flags >= 0);
    CHECK(fcntl(fds[0], F_SETFL, flags | O_NONBLOCK) == 0);

    struct mod_stress_ctx st = {.reactor = reactor, .dispatches = 0, .mode = mode};
    atomic_init(&st.dispatches, 0);
    CHECK(kith_reactor_add(reactor, fds[0], KITH_REACTOR_IN, mod_stress_cb, &st) == 0);

    pthread_t tid;
    int prc = pthread_create(&tid, nullptr, mod_stress_run, reactor);
    CHECK(prc == 0);

    enum
    {
        MESSAGES = 32
    };
    for (int i = 1; i <= MESSAGES; i++)
    {
        uint8_t byte = (uint8_t)i;
        CHECK(send(fds[1], &byte, 1u, 0) == 1);
        CHECK(mod_stress_wait(&st, i, 2000u));
    }

    kith_reactor_stop(reactor);
    if (prc == 0)
    {
        (void)pthread_join(tid, nullptr);
    }
    CHECK(atomic_load_explicit(&st.dispatches, memory_order_acquire) == MESSAGES);

    close(fds[0]);
    close(fds[1]);
    kith_reactor_destroy(reactor);
    return failures;
}

// ---------------------------------------------------------------------------
// destroy-during-run contract guard
// ---------------------------------------------------------------------------

struct run_only_arg
{
    kith_reactor_t *reactor;
};

static void *run_only_thread_fn(void *raw)
{
    struct run_only_arg *arg = (struct run_only_arg *)raw;
    (void)kith_reactor_run(arg->reactor);
    return nullptr;
}

// Dispatch witness for the destroy-while-run guard: kith_reactor_run sets
// run_in_flight at entry, before the loop can dispatch anything, so one
// observed dispatch proves the loop is in flight.
static _Atomic bool run_dispatch_observed;

static void reactor_basic_dispatch_flag_cb(int fd, unsigned int events, void *ctx)
{
    (void)fd;
    (void)events;
    (void)ctx;
    atomic_store_explicit(&run_dispatch_observed, true, memory_order_release);
}

// Writes one byte to the registered socketpair and waits until the dispatch
// callback observed it; a dispatch can only happen inside kith_reactor_run,
// so the observed flag pins the loop in flight for the destroy that
// follows. A timeout or a failed write exits the child with a distinct
// setup-failure code.
static void wake_and_wait_for_dispatch(int notify_fd)
{
    atomic_store_explicit(&run_dispatch_observed, false, memory_order_relaxed);
    if (write(notify_fd, "x", 1) != 1)
    {
        _exit(5);
    }
    int waited_ms = 0;
    while (waited_ms < 2000 && !atomic_load_explicit(&run_dispatch_observed, memory_order_acquire))
    {
        struct timespec pause = {.tv_sec = 0, .tv_nsec = 1L * 1000 * 1000};
        (void)nanosleep(&pause, nullptr);
        waited_ms += 1;
    }
    if (!atomic_load_explicit(&run_dispatch_observed, memory_order_acquire))
    {
        _exit(4);
    }
}

// Destroying a reactor while its run loop is in flight aborts the process:
// the @thread_safety contract is enforced at runtime rather than left as a
// convention, mirroring the server handle's guard. The violation runs in a
// forked child so the abort terminates the child, not the test process; the
// parent asserts the child died from SIGABRT.
static int test_destroy_while_run_aborts(void)
{
    int failures = 0;
    pid_t pid = fork();
    if (pid < 0)
    {
        (void)fprintf(stderr, "reactor basic: fork failed\n");
        return 1;
    }
    if (pid == 0)
    {
        // Ring memory from prior queue exits reclaims asynchronously, so
        // back off until creation fits the cgroup memory budget.
        kith_reactor_t *reactor = nullptr;
        for (unsigned int attempt = 0u; attempt < 60u && reactor == nullptr; attempt++)
        {
            if (reactor_create_bounded(&reactor) == 0)
            {
                break;
            }
            struct timespec pause = {.tv_sec = 0, .tv_nsec = 25L * 1000 * 1000};
            (void)nanosleep(&pause, nullptr);
        }
        if (reactor == nullptr)
        {
            _exit(2);
        }
        // A registered fd keeps the loop out of the idle single-pass path,
        // so run() stays parked inside the backend wait.
        int fds[2];
        if (socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, fds) != 0 ||
            kith_reactor_add(
                reactor, fds[0], KITH_REACTOR_IN, reactor_basic_dispatch_flag_cb, nullptr) != 0)
        {
            kith_reactor_destroy(reactor);
            _exit(2);
        }
        struct run_only_arg arg = {.reactor = reactor};
        pthread_t tid;
        int prc = pthread_create(&tid, nullptr, run_only_thread_fn, &arg);
        if (prc != 0)
        {
            // No run in flight; destroy cleanly and signal a setup failure.
            kith_reactor_destroy(reactor);
            close(fds[0]);
            close(fds[1]);
            _exit(3);
        }
        // One wakeup dispatches the callback; the observed flag pins the
        // loop in flight for the destroy below.
        wake_and_wait_for_dispatch(fds[1]);
        // Contract violation: destroy while the run loop is in flight.
        kith_reactor_destroy(reactor);
        _exit(0); // Unreachable: the prior call aborts.
    }

    int status = 0;
    while (waitpid(pid, &status, 0) < 0 && errno == EINTR)
    {
    }
    if (!WIFSIGNALED(status) || WTERMSIG(status) != SIGABRT)
    {
        // A child that exited on its own never entered the run loop; the
        // exit code distinguishes the setup failure paths.
        (void)fprintf(stderr, "reactor basic: child exited without SIGABRT (status %d)\n", status);
    }
    CHECK(WIFSIGNALED(status));
    CHECK(WTERMSIG(status) == SIGABRT);
    return failures;
}

// ---------------------------------------------------------------------------
// multi-producer submit churn
// ---------------------------------------------------------------------------

// Producers on separate threads submit tasks while the run loop drains
// them: the task-node free list runs at its concurrent limits (pop under
// contention on the submit path, recycle on the drain path, EBUSY backoff
// at capacity). Every accepted task runs exactly once.
enum
{
    SUBMIT_PRODUCERS = 4,
    SUBMIT_TASKS_PER_PRODUCER = 250,
};

struct submit_churn_ctx
{
    kith_reactor_t *reactor;
    _Atomic uint64_t runs;     // task executions
    _Atomic uint64_t accepted; // successful submits
    _Atomic bool stop;         // the runner exits when set
};

static void submit_churn_task(void *arg)
{
    struct submit_churn_ctx *st = arg;
    usleep(50); // hold nodes in flight so producers hit EBUSY
    atomic_fetch_add_explicit(&st->runs, 1u, memory_order_release);
}

static void *submit_churn_runner(void *raw)
{
    struct submit_churn_ctx *st = raw;
    // The run loop returns after one pass whenever the reactor looks idle
    // (no fds, no timers, no queued tasks at the loop top), so the runner
    // spins run() until the test stops the reactor.
    while (!atomic_load_explicit(&st->stop, memory_order_acquire))
    {
        (void)kith_reactor_run(st->reactor);
    }
    return nullptr;
}

static void *submit_churn_producer(void *raw)
{
    struct submit_churn_ctx *st = raw;
    for (int done = 0; done < SUBMIT_TASKS_PER_PRODUCER; done++)
    {
        while (kith_reactor_submit(st->reactor, submit_churn_task, st) != 0)
        {
            usleep(100); // free list at capacity; the run loop is draining
        }
        atomic_fetch_add_explicit(&st->accepted, 1u, memory_order_release);
    }
    return nullptr;
}

static int test_multi_producer_submit(void)
{
    int failures = 0;
    kith_reactor_t *reactor = nullptr;
    CHECK(reactor_create_bounded(&reactor) == 0);

    struct submit_churn_ctx st = {.reactor = reactor};
    atomic_init(&st.runs, 0);
    atomic_init(&st.accepted, 0);
    atomic_init(&st.stop, false);

    pthread_t runner;
    CHECK(pthread_create(&runner, nullptr, submit_churn_runner, &st) == 0);

    pthread_t producers[SUBMIT_PRODUCERS];
    bool started[SUBMIT_PRODUCERS] = {false};
    for (int i = 0; i < SUBMIT_PRODUCERS; i++)
    {
        if (pthread_create(&producers[i], nullptr, submit_churn_producer, &st) == 0)
        {
            started[i] = true;
        }
        else
        {
            CHECK(false);
        }
    }
    for (int i = 0; i < SUBMIT_PRODUCERS; i++)
    {
        if (started[i])
        {
            pthread_join(producers[i], nullptr);
        }
    }

    CHECK(atomic_load_explicit(&st.accepted, memory_order_acquire) ==
          (uint64_t)(SUBMIT_PRODUCERS * SUBMIT_TASKS_PER_PRODUCER));

    // The run loop keeps draining after the producers finish; wait until
    // every accepted task has run before stopping it.
    uint64_t accepted = atomic_load_explicit(&st.accepted, memory_order_acquire);
    bool drained = false;
    for (int i = 0; i < 5000; i++)
    {
        if (atomic_load_explicit(&st.runs, memory_order_acquire) == accepted)
        {
            drained = true;
            break;
        }
        struct timespec ts = {.tv_sec = 0, .tv_nsec = 1000L * 1000};
        (void)nanosleep(&ts, nullptr);
    }
    CHECK(drained);

    atomic_store_explicit(&st.stop, true, memory_order_release);
    kith_reactor_stop(reactor);
    pthread_join(runner, nullptr);
    kith_reactor_destroy(reactor);
    return failures;
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

int main(void)
{
    int rc = 0;
    rc |= test_create_defaults();
    rc |= test_create_params();
    rc |= test_create_null_out();
    rc |= test_create_bad_abi();
    rc |= test_create_undersized();
    rc |= test_destroy_null();
    rc |= test_add_fd();
    rc |= test_mod_del();
    rc |= test_run_null();
    rc |= test_run_empty_returns();
    rc |= test_stop_null();
    rc |= test_run_resumes_after_stop();
    rc |= test_stop_before_run_stops_next_run();
    rc |= test_now_ms_null();
    rc |= test_create_destroy_cycle();
    rc |= test_add_null_reactor();
    rc |= test_add_negative_fd();
    rc |= test_mod_del_null();
    rc |= test_mod_dispatch_invariant(0);
    rc |= test_mod_dispatch_invariant(1);
    rc |= test_multi_producer_submit();
    rc |= test_destroy_while_run_aborts();

    if (rc != 0)
    {
        (void)fprintf(stderr, "reactor basic tests FAILED\n");
        return EXIT_FAILURE;
    }
    return EXIT_SUCCESS;
}
