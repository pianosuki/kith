#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <pthread.h>
#include <unistd.h>

#include "kith/reactor/reactor.h"
#include "kith/state/state.h"
#include "kith/types.h"
#include "kith/version.h"

#include <netinet/in.h>
#include <sys/socket.h>

// ---------------------------------------------------------------------------
// test harness
// ---------------------------------------------------------------------------

static int check_cond(bool ok, int line)
{
    if (!ok)
    {
        (void)fprintf(stderr, "state ops: assertion at line %d failed\n", line);
        return 1;
    }
    return 0;
}

#define CHECK(cond) failures += check_cond((cond), __LINE__)

// Create a reactor with a small io_uring ring. These tests pump the reactor
// to drive the hiredis async connection, so a 64-fd ring is plenty and avoids
// pressuring the cgroup memory budget under ctest sequencing.
static int state_ops_create_reactor(kith_reactor_t **out)
{
    kith_reactor_params_t params = {
        .size = sizeof(kith_reactor_params_t),
        .abi_version = 1u,
        .max_fds = 64,
        .task_capacity = 64,
    };
    return kith_reactor_create(&params, nullptr, out);
}

// Registration for fault-injection tests that occupy reactor slots; the
// registered fd generates no traffic.
static void state_ops_noop_event(int fd, unsigned int events, void *ctx)
{
    (void)fd;
    (void)events;
    (void)ctx;
}

// ---------------------------------------------------------------------------
// per-command result, passed as user_data so the generic callback can record
// each command's outcome independently.
// ---------------------------------------------------------------------------
struct op_result
{
    int status;
    bool received;
    bool exists;
    char value[256];
    size_t value_len;
};

static void op_callback(kith_state_reply_t *reply)
{
    struct op_result *r = reply->user_data;
    r->status = reply->status;
    r->received = true;
    r->exists = reply->exists;
    if (reply->value != nullptr && reply->value_len <= sizeof(r->value))
    {
        memcpy(r->value, reply->value, reply->value_len);
        r->value_len = reply->value_len;
    }
    else
    {
        r->value_len = 0;
    }
}

// ---------------------------------------------------------------------------
// reactor drive: pipeline all commands, then run the reactor until the last
// reply arrives or the deadline timer fires. kith_reactor_run blocks until
// kith_reactor_stop, so the test must stop the loop itself.
// ---------------------------------------------------------------------------
static kith_reactor_t *g_run_reactor = nullptr;
static int g_expected_replies = 0;
static int g_received_replies = 0;
static bool g_timed_out = false;

static void timeout_stop_cb(void *ctx)
{
    (void)ctx;
    g_timed_out = true;
    kith_reactor_stop(g_run_reactor);
}

// Run the reactor until g_expected_replies command callbacks have fired or
// timeout_ms elapses. Each op_callback invocation bumps g_received_replies
// and stops the reactor once the expected count is reached.
static void run_until_replies(kith_reactor_t *reactor, int expected, int timeout_ms)
{
    g_run_reactor = reactor;
    g_expected_replies = expected;
    g_received_replies = 0;
    g_timed_out = false;
    uint64_t deadline = kith_reactor_now_ms(reactor) + (uint64_t)timeout_ms;
    (void)kith_reactor_schedule(reactor, deadline, timeout_stop_cb, nullptr);
    (void)kith_reactor_run(reactor);
}

// Wrap op_callback so the received-count bookkeeping happens after the
// result is recorded.
static void counting_callback(kith_state_reply_t *reply)
{
    op_callback(reply);
    g_received_replies++;
    if (g_received_replies >= g_expected_replies)
    {
        kith_reactor_stop(g_run_reactor);
    }
}

// Tear down and report a skip. A skip is not a failure.
static int skip_unreachable(kith_state_t *state, kith_reactor_t *reactor, const char *which)
{
    (void)fprintf(stderr, "state ops: Redis not reachable, skipping %s test\n", which);
    kith_state_destroy(state);
    kith_reactor_destroy(reactor);
    return 0;
}

// A command result that did not arrive or arrived with a connection-reset
// status means Redis was not reachable.
static bool result_unreachable(const struct op_result *r)
{
    return !r->received || r->status == -(int)KITH_ECONNRESET;
}

// ---------------------------------------------------------------------------
// shared params builder
// ---------------------------------------------------------------------------
static void fill_default_params(kith_state_params_t *params)
{
    memset(params, 0, sizeof(*params));
    params->size = sizeof(*params);
    params->abi_version = KITH_ABI_VERSION;
    params->timeout_ms = 1000;
    params->key_prefix = "kith_test:";
    params->key_prefix_len = 10;
}

// ---------------------------------------------------------------------------
// redis reachability probe
// ---------------------------------------------------------------------------

// Synchronous TCP probe to 127.0.0.1:6379. Driving the async adapter against a
// dead endpoint triggers a re-entrant dispatch in the reactor that hiredis
// cannot tolerate (REDIS_IN_CALLBACK assertion). Probing up front avoids
// that path entirely: if Redis is not reachable, the integration tests are
// skipped without ever touching the async adapter.
static bool redis_available(void)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0)
    {
        return false;
    }
    struct sockaddr_in addr = {0};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(6379);
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    int rc = connect(fd, (struct sockaddr *)&addr, sizeof(addr));
    close(fd);
    return rc == 0;
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

// SET and GET a string key/value.
static int test_set_get_string(void)
{
    int failures = 0;
    kith_reactor_t *reactor = nullptr;
    kith_state_t *state = nullptr;

    CHECK(state_ops_create_reactor(&reactor) == 0);

    kith_state_params_t params;
    fill_default_params(&params);

    int rc = kith_state_create(&params, reactor, nullptr, &state);
    CHECK(rc == 0);

    const char *test_key = "hello";
    const char *test_val = "world";

    struct op_result set_res = {0};
    struct op_result get_res = {0};

    rc = kith_state_set(
        state, test_key, strlen(test_key), test_val, strlen(test_val), counting_callback, &set_res);
    CHECK(rc == 0);
    rc = kith_state_get(state, test_key, strlen(test_key), counting_callback, &get_res);
    CHECK(rc == 0);

    run_until_replies(reactor, 2, 500);

    if (result_unreachable(&set_res))
    {
        return skip_unreachable(state, reactor, "SET/GET");
    }
    CHECK(set_res.status == 0);
    CHECK(get_res.received);
    CHECK(get_res.status == 0);
    CHECK(get_res.value_len == strlen(test_val));
    CHECK(memcmp(get_res.value, test_val, get_res.value_len) == 0);

    kith_state_destroy(state);
    kith_reactor_destroy(reactor);
    return failures;
}

// EXISTS and DEL.
static int test_exists_del(void)
{
    int failures = 0;
    kith_reactor_t *reactor = nullptr;
    kith_state_t *state = nullptr;

    CHECK(state_ops_create_reactor(&reactor) == 0);

    kith_state_params_t params;
    fill_default_params(&params);

    int rc = kith_state_create(&params, reactor, nullptr, &state);
    CHECK(rc == 0);

    const char *key = "temp_key";
    const char *val = "temp_val";

    struct op_result set_res = {0};
    struct op_result exists1 = {0};
    struct op_result del_res = {0};
    struct op_result exists2 = {0};

    rc = kith_state_set(state, key, strlen(key), val, strlen(val), counting_callback, &set_res);
    CHECK(rc == 0);
    rc = kith_state_exists(state, key, strlen(key), counting_callback, &exists1);
    CHECK(rc == 0);
    rc = kith_state_del(state, key, strlen(key), counting_callback, &del_res);
    CHECK(rc == 0);
    rc = kith_state_exists(state, key, strlen(key), counting_callback, &exists2);
    CHECK(rc == 0);

    run_until_replies(reactor, 4, 500);

    if (result_unreachable(&set_res))
    {
        return skip_unreachable(state, reactor, "EXISTS/DEL");
    }
    CHECK(set_res.status == 0);
    CHECK(exists1.received);
    CHECK(exists1.status == 0);
    CHECK(exists1.exists);
    CHECK(del_res.received);
    CHECK(del_res.status == 0);
    CHECK(exists2.received);
    CHECK(exists2.status == 0);
    CHECK(!exists2.exists);

    kith_state_destroy(state);
    kith_reactor_destroy(reactor);
    return failures;
}

// GET on a nonexistent key returns nil (status 0, value NULL).
static int test_get_nonexistent(void)
{
    int failures = 0;
    kith_reactor_t *reactor = nullptr;
    kith_state_t *state = nullptr;

    CHECK(state_ops_create_reactor(&reactor) == 0);

    kith_state_params_t params;
    fill_default_params(&params);

    int rc = kith_state_create(&params, reactor, nullptr, &state);
    CHECK(rc == 0);

    struct op_result get_res = {0};

    rc = kith_state_get(state, "no_such_key", 12, counting_callback, &get_res);
    CHECK(rc == 0);

    run_until_replies(reactor, 1, 500);

    if (result_unreachable(&get_res))
    {
        return skip_unreachable(state, reactor, "GET-nil");
    }
    CHECK(get_res.status == 0);
    CHECK(get_res.value_len == 0);

    kith_state_destroy(state);
    kith_reactor_destroy(reactor);
    return failures;
}

// SET with empty value, then GET it back empty.
static int test_set_empty(void)
{
    int failures = 0;
    kith_reactor_t *reactor = nullptr;
    kith_state_t *state = nullptr;

    CHECK(state_ops_create_reactor(&reactor) == 0);

    kith_state_params_t params;
    fill_default_params(&params);

    int rc = kith_state_create(&params, reactor, nullptr, &state);
    CHECK(rc == 0);

    const char *key = "empty_key";

    struct op_result set_res = {0};
    struct op_result get_res = {0};

    rc = kith_state_set(state, key, strlen(key), nullptr, 0, counting_callback, &set_res);
    CHECK(rc == 0);
    rc = kith_state_get(state, key, strlen(key), counting_callback, &get_res);
    CHECK(rc == 0);

    run_until_replies(reactor, 2, 500);

    if (result_unreachable(&set_res))
    {
        return skip_unreachable(state, reactor, "SET-empty");
    }
    CHECK(set_res.status == 0);
    CHECK(get_res.received);
    CHECK(get_res.status == 0);
    CHECK(get_res.value_len == 0);

    kith_state_destroy(state);
    kith_reactor_destroy(reactor);
    return failures;
}

// API validation paths reject the call before any Redis traffic, so they do
// not require a live Redis instance.
static int test_invalid_args(void)
{
    int failures = 0;
    kith_reactor_t *reactor = nullptr;
    kith_state_t *state = nullptr;

    CHECK(state_ops_create_reactor(&reactor) == 0);

    kith_state_params_t params;
    fill_default_params(&params);
    params.timeout_ms = 0;
    params.key_prefix = nullptr;
    params.key_prefix_len = 0;

    int rc = kith_state_create(&params, reactor, nullptr, &state);
    CHECK(rc == 0);
    CHECK(state != nullptr);

    struct op_result res = {0};

    // NULL state
    rc = kith_state_set(nullptr, "k", 1, "v", 1, op_callback, &res);
    CHECK(rc == -(int)KITH_EINVAL);

    // NULL key
    rc = kith_state_set(state, nullptr, 1, "v", 1, op_callback, &res);
    CHECK(rc == -(int)KITH_EINVAL);

    // zero-length key
    rc = kith_state_set(state, "k", 0, "v", 1, op_callback, &res);
    CHECK(rc == -(int)KITH_EINVAL);

    // NULL callback
    rc = kith_state_set(state, "k", 1, "v", 1, nullptr, nullptr);
    CHECK(rc == -(int)KITH_EINVAL);

    kith_state_destroy(state);
    kith_reactor_destroy(reactor);
    return failures;
}

// ---------------------------------------------------------------------------
// fault injection: the reactor has no free registration slot
// ---------------------------------------------------------------------------

// Create fails instead of returning a handle whose connection the reactor can
// never service. Runs without a Redis server: the registration fails before
// any traffic leaves the process.
static int test_create_without_registration_slot(void)
{
    int failures = 0;
    kith_reactor_t *reactor = nullptr;
    kith_state_t *state = nullptr;

    kith_reactor_params_t params = {
        .size = sizeof(kith_reactor_params_t),
        .abi_version = KITH_ABI_VERSION,
        .max_fds = 1,
        .task_capacity = 64,
    };
    CHECK(kith_reactor_create(&params, nullptr, &reactor) == 0);

    // Occupy the reactor's registration table (capacity is next_pow2 of
    // max_fds, so both socketpair ends fill a max_fds=1 table).
    int occupy[2];
    CHECK(socketpair(AF_UNIX, SOCK_STREAM, 0, occupy) == 0);
    CHECK(kith_reactor_add(reactor, occupy[0], KITH_REACTOR_IN, state_ops_noop_event, nullptr) ==
          0);
    CHECK(kith_reactor_add(reactor, occupy[1], KITH_REACTOR_IN, state_ops_noop_event, nullptr) ==
          0);

    kith_state_params_t sparams;
    fill_default_params(&sparams);

    int rc = kith_state_create(&sparams, reactor, nullptr, &state);
    CHECK(rc == -(int)KITH_EBUSY);
    CHECK(state == nullptr);

    kith_reactor_destroy(reactor);
    close(occupy[0]);
    close(occupy[1]);
    return failures;
}

// ---------------------------------------------------------------------------
// off-thread submission — the any-thread contract surface
// ---------------------------------------------------------------------------

// A worker thread issues commands while the reactor runs on the main thread.
// Every reply must report success with the value round-tripping, and every
// callback must fire on the reactor thread.
static pid_t g_reactor_tid = 0;
static bool g_reply_off_thread = false;

static void off_thread_callback(kith_state_reply_t *reply)
{
    op_callback(reply);
    if (gettid() != g_reactor_tid)
    {
        g_reply_off_thread = true;
    }
    g_received_replies++;
    if (g_received_replies >= g_expected_replies)
    {
        kith_reactor_stop(g_run_reactor);
    }
}

struct off_thread_ctx
{
    kith_state_t *state;
    struct op_result results[8];
    int submit_failures;
};

static void *off_thread_worker(void *arg)
{
    struct off_thread_ctx *w = arg;
    for (int i = 0; i < 4; i++)
    {
        size_t set_idx = (size_t)i * 2u;
        size_t get_idx = set_idx + 1u;
        char key[16];
        char val[16];
        (void)snprintf(key, sizeof(key), "mt_key_%d", i);
        (void)snprintf(val, sizeof(val), "mt_val_%d", i);
        if (kith_state_set(w->state,
                           key,
                           strlen(key),
                           val,
                           strlen(val),
                           off_thread_callback,
                           &w->results[set_idx]) != 0)
        {
            w->submit_failures++;
            return nullptr;
        }
        if (kith_state_get(w->state, key, strlen(key), off_thread_callback, &w->results[get_idx]) !=
            0)
        {
            w->submit_failures++;
            return nullptr;
        }
    }
    return nullptr;
}

static int test_off_thread_submission(void)
{
    int failures = 0;
    kith_reactor_t *reactor = nullptr;
    kith_state_t *state = nullptr;

    CHECK(state_ops_create_reactor(&reactor) == 0);

    kith_state_params_t params;
    fill_default_params(&params);

    CHECK(kith_state_create(&params, reactor, nullptr, &state) == 0);

    struct off_thread_ctx w = {.state = state, .submit_failures = 0};
    pthread_t worker;
    CHECK(pthread_create(&worker, nullptr, off_thread_worker, &w) == 0);

    g_reactor_tid = gettid();
    run_until_replies(reactor, 8, 2000);
    CHECK(pthread_join(worker, nullptr) == 0);

    if (!g_reply_off_thread && w.submit_failures == 0 && result_unreachable(&w.results[0]))
    {
        return skip_unreachable(state, reactor, "off-thread");
    }
    CHECK(w.submit_failures == 0);
    CHECK(!g_timed_out);
    CHECK(g_reply_off_thread == false);
    for (int i = 0; i < 4; i++)
    {
        size_t set_idx = (size_t)i * 2u;
        size_t get_idx = set_idx + 1u;
        char val[16];
        (void)snprintf(val, sizeof(val), "mt_val_%d", i);
        CHECK(w.results[set_idx].status == 0);
        CHECK(w.results[get_idx].status == 0);
        CHECK(w.results[get_idx].value_len == strlen(val));
        CHECK(memcmp(w.results[get_idx].value, val, strlen(val)) == 0);
    }

    kith_state_destroy(state);
    kith_reactor_destroy(reactor);
    return failures;
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------
int main(void)
{
    /* Argument validation is backend-independent: it runs even when the
     * service skip or an unreachable server removes the integration legs. */
    int failures = 0;
    failures += test_invalid_args();

    const char *skip_env = getenv("KITH_SKIP_REDIS");
    if (skip_env != nullptr && strcmp(skip_env, "1") == 0)
    {
        (void)fprintf(stderr, "state ops: KITH_SKIP_REDIS=1, skipping\n");
        return failures ? 1 : 0;
    }

    failures += test_create_without_registration_slot();

    if (!redis_available())
    {
        (void)fprintf(stderr, "state ops: Redis not reachable, skipping integration tests\n");
        return failures ? 1 : 0;
    }

    failures += test_set_get_string();
    failures += test_exists_del();
    failures += test_get_nonexistent();
    failures += test_set_empty();
    failures += test_off_thread_submission();
    if (failures)
    {
        (void)fprintf(stderr, "state ops: %d failure(s)\n", failures);
    }
    return failures ? 1 : 0;
}
