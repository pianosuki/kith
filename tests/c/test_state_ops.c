#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <pthread.h>
#include <unistd.h>

#include "kith/reactor/reactor.h"
#include "kith/state/state.h"
#include "kith/types.h"
#include "kith/version.h"
#include "state/state_internal.h"

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
    unsigned int callback_count;
    pthread_t callback_thread;
    bool exists;
    char value[256];
    size_t value_len;
};

static void op_callback(kith_state_reply_t *reply)
{
    struct op_result *r = reply->user_data;
    r->status = reply->status;
    r->received = true;
    r->callback_count++;
    r->callback_thread = pthread_self();
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
static uint64_t g_wait_deadline_ms = 0;

static void timeout_stop_cb(void *ctx)
{
    (void)ctx;
    if (g_run_reactor != nullptr && g_received_replies < g_expected_replies &&
        kith_reactor_now_ms(g_run_reactor) >= g_wait_deadline_ms)
    {
        g_timed_out = true;
        kith_reactor_stop(g_run_reactor);
    }
}

// Run the reactor until g_expected_replies command callbacks have fired or
// timeout_ms elapses. Each counting_callback bumps g_received_replies
// and stops the reactor once the expected count is reached.
static void run_until_replies(kith_reactor_t *reactor, int expected, int timeout_ms)
{
    struct timespec now;
    if (clock_gettime(CLOCK_MONOTONIC, &now) != 0)
    {
        (void)fprintf(stderr, "state ops: cannot read reply-wait clock\n");
        exit(EXIT_FAILURE);
    }
    g_run_reactor = reactor;
    g_expected_replies = expected;
    g_received_replies = 0;
    g_timed_out = false;
    g_wait_deadline_ms =
        (uint64_t)now.tv_sec * 1000u + (uint64_t)now.tv_nsec / 1000000u + (uint64_t)timeout_ms;
    int rc = kith_reactor_schedule(reactor, g_wait_deadline_ms, timeout_stop_cb, nullptr);
    if (rc == 0)
    {
        rc = kith_reactor_run(reactor);
    }
    g_run_reactor = nullptr;
    if (rc != 0 || g_timed_out || g_received_replies != expected)
    {
        (void)fprintf(stderr,
                      "state ops: reply wait failed: expected=%d received=%d timeout=%d rc=%d\n",
                      expected,
                      g_received_replies,
                      g_timed_out,
                      rc);
        exit(EXIT_FAILURE);
    }
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

static int refused_endpoint_bind(uint16_t *out_port)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0)
    {
        return -1;
    }
    struct sockaddr_in addr = {0};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0)
    {
        close(fd);
        return -1;
    }
    socklen_t addr_len = sizeof(addr);
    if (getsockname(fd, (struct sockaddr *)&addr, &addr_len) != 0)
    {
        close(fd);
        return -1;
    }
    *out_port = ntohs(addr.sin_port);
    return fd;
}

// ---------------------------------------------------------------------------
// redis reachability probe
// ---------------------------------------------------------------------------

// Only the live-service cases depend on this optional local endpoint.
// Argument validation and connection-failure coverage run independently.
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

static int test_dead_endpoint_completion(void)
{
    int failures = 0;
    kith_reactor_t *reactor = nullptr;
    kith_state_t *state = nullptr;
    struct op_result replies[2] = {0};
    uint16_t port = 0;
    int endpoint = refused_endpoint_bind(&port);
    CHECK(endpoint >= 0);
    if (endpoint < 0)
    {
        return failures;
    }

    int rc = state_ops_create_reactor(&reactor);
    CHECK(rc == 0);
    if (rc != 0)
    {
        goto cleanup;
    }
    kith_state_params_t params;
    fill_default_params(&params);
    params.host = "127.0.0.1";
    params.port = port;
    rc = kith_state_create(&params, reactor, nullptr, &state);
    CHECK(rc == 0);
    if (rc != 0)
    {
        goto cleanup;
    }
    const char key[] = "dead_endpoint_probe";
    for (size_t i = 0; i < 2u; i++)
    {
        rc = kith_state_get(state, key, strlen(key), counting_callback, &replies[i]);
        CHECK(rc == 0);
        if (rc != 0)
        {
            goto cleanup;
        }
        run_until_replies(reactor, 1, 1000);
        CHECK(replies[i].received);
        CHECK(replies[i].status == -(int)KITH_ECONNRESET);
    }

cleanup:
    kith_state_destroy(state);
    for (size_t i = 0; i < 2u; i++)
    {
        CHECK(replies[i].callback_count == (replies[i].received ? 1u : 0u));
    }
    kith_reactor_destroy(reactor);
    close(endpoint);
    return failures;
}

// Repeated interest removals leave the connection usable, including at zero.
static int test_adapter_interest_removal(void)
{
    int failures = 0;
    kith_reactor_t *reactor = nullptr;
    kith_state_t *state = nullptr;
    CHECK(state_ops_create_reactor(&reactor) == 0);
    if (reactor == nullptr)
    {
        return failures;
    }
    kith_state_params_t params;
    fill_default_params(&params);
    CHECK(kith_state_create(&params, reactor, nullptr, &state) == 0);
    if (state != nullptr)
    {
        struct kith_state_adapter *adapter = &state->adapter;
        redisAsyncContext *ac = adapter->ac;
        CHECK(adapter->event_mask == (unsigned int)KITH_REACTOR_OUT);
        ac->ev.delRead(ac->ev.data);
        ac->ev.delRead(ac->ev.data);
        CHECK(adapter->event_mask == (unsigned int)KITH_REACTOR_OUT);
        ac->ev.addRead(ac->ev.data);
        CHECK(adapter->event_mask == ((unsigned int)KITH_REACTOR_IN | KITH_REACTOR_OUT));
        ac->ev.delWrite(ac->ev.data);
        ac->ev.delWrite(ac->ev.data);
        CHECK(adapter->event_mask == (unsigned int)KITH_REACTOR_IN);
        ac->ev.delRead(ac->ev.data);
        ac->ev.delRead(ac->ev.data);
        ac->ev.delWrite(ac->ev.data);
        CHECK(adapter->event_mask == 0u);
        ac->ev.addWrite(ac->ev.data);
        CHECK(adapter->event_mask == (unsigned int)KITH_REACTOR_OUT);
        CHECK(!adapter->registration_failed);
    }
    kith_state_destroy(state);
    kith_reactor_destroy(reactor);
    return failures;
}

static void stop_reactor_task(void *ctx)
{
    kith_reactor_stop(ctx);
}

// An owned TCP listener keeps commands pending without a Redis dependency.
static int test_destroy_pending_commands(void)
{
    int failures = 0;
    kith_reactor_t *reactor = nullptr;
    kith_state_t *state = nullptr;
    struct op_result replies[2] = {0};
    uint16_t port = 0;
    int endpoint = refused_endpoint_bind(&port);
    CHECK(endpoint >= 0);
    if (endpoint < 0)
    {
        return failures;
    }
    int rc = listen(endpoint, 1);
    CHECK(rc == 0);
    if (rc != 0)
    {
        goto cleanup;
    }
    rc = state_ops_create_reactor(&reactor);
    CHECK(rc == 0);
    if (rc != 0)
    {
        goto cleanup;
    }
    kith_state_params_t params;
    fill_default_params(&params);
    params.host = "127.0.0.1";
    params.port = port;
    rc = kith_state_create(&params, reactor, nullptr, &state);
    CHECK(rc == 0);
    if (rc != 0)
    {
        goto cleanup;
    }
    const char key[] = "pending_completion";
    for (size_t i = 0; i < 2u; i++)
    {
        rc = kith_state_get(state, key, strlen(key), op_callback, &replies[i]);
        CHECK(rc == 0);
    }
    rc = kith_reactor_submit(reactor, stop_reactor_task, reactor);
    if (rc != 0 || kith_reactor_run(reactor) != 0)
    {
        (void)fprintf(stderr, "state ops: cannot drain pending-command submissions\n");
        exit(EXIT_FAILURE);
    }
    for (size_t i = 0; i < 2u; i++)
    {
        CHECK(replies[i].callback_count == 0u);
    }
    kith_state_destroy(state);
    for (size_t i = 0; i < 2u; i++)
    {
        CHECK(replies[i].callback_count == 1u);
        CHECK(replies[i].status == -(int)KITH_ECONNRESET);
        if (replies[i].callback_count == 1u)
        {
            CHECK(pthread_equal(replies[i].callback_thread, pthread_self()) != 0);
        }
    }
cleanup:
    kith_reactor_destroy(reactor);
    close(endpoint);
    return failures;
}

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

    // Two registration slots fill the table at max_fds=1.
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
    failures += test_dead_endpoint_completion();
    failures += test_adapter_interest_removal();
    failures += test_destroy_pending_commands();
    failures += test_create_without_registration_slot();

    const char *skip_env = getenv("KITH_SKIP_REDIS");
    if (skip_env != nullptr && strcmp(skip_env, "1") == 0)
    {
        (void)fprintf(stderr, "state ops: KITH_SKIP_REDIS=1, skipping\n");
        return failures ? 1 : 0;
    }

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
