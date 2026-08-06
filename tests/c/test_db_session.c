#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <pthread.h>
#include <unistd.h>

#include "kith/db/db.h"
#include "kith/reactor/reactor.h"
#include "kith/types.h"
#include "kith/version.h"

#include <netinet/in.h>
#include <sys/socket.h>

// ---------------------------------------------------------------------------
// test harness (the test_db_ops shape: pipeline, then run to a reply count)
// ---------------------------------------------------------------------------

static int check_cond(bool ok, int line)
{
    if (!ok)
    {
        (void)fprintf(stderr, "db session: assertion at line %d failed\n", line);
        return 1;
    }
    return 0;
}

#define CHECK(cond) failures += check_cond((cond), __LINE__)

static int session_create_reactor(kith_reactor_t **out)
{
    kith_reactor_params_t params = {
        .size = sizeof(kith_reactor_params_t),
        .abi_version = KITH_ABI_VERSION,
        .max_fds = 64,
        .task_capacity = 64,
    };
    return kith_reactor_create(&params, nullptr, out);
}

struct op_result
{
    int status;
    bool received;
    const char *err_str;
    uint32_t n_rows;
    uint32_t n_cols;
    char value[64];
    size_t value_len;
};

static void op_callback(kith_db_reply_t *reply)
{
    struct op_result *r = kith_db_reply_user_data(reply);
    r->status = kith_db_reply_status(reply);
    r->received = true;
    r->err_str = kith_db_reply_err_str(reply);
    r->n_rows = kith_db_reply_n_rows(reply);
    r->n_cols = kith_db_reply_n_cols(reply);
    const char *val = kith_db_reply_value(reply, 0, 0, &r->value_len);
    if (val != nullptr && r->value_len < sizeof(r->value))
    {
        memcpy(r->value, val, r->value_len);
        r->value[r->value_len] = '\0';
    }
    else
    {
        r->value_len = 0;
    }
}

struct session_result
{
    kith_db_session_t *session;
    int status;
    bool received;
};

static kith_reactor_t *g_run_reactor = nullptr;
static int g_expected_replies = 0;
static int g_received_replies = 0;
static bool g_timed_out = false;
static int g_skipped_tests = 0;

static void open_callback(kith_db_session_t *session, int status, void *user_data)
{
    struct session_result *r = user_data;
    r->session = session;
    r->status = status;
    r->received = true;
    g_received_replies++;
    if (g_received_replies >= g_expected_replies)
    {
        kith_reactor_stop(g_run_reactor);
    }
}

struct close_result
{
    int status;
    bool received;
};

static void close_callback(int status, void *user_data)
{
    struct close_result *r = user_data;
    r->status = status;
    r->received = true;
    g_received_replies++;
    if (g_received_replies >= g_expected_replies)
    {
        kith_reactor_stop(g_run_reactor);
    }
}

static void timeout_stop_cb(void *ctx)
{
    (void)ctx;
    g_timed_out = true;
    kith_reactor_stop(g_run_reactor);
}

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

static int skip_unreachable(kith_db_t *db, kith_reactor_t *reactor, const char *which)
{
    (void)fprintf(stderr, "db session: Postgres not reachable, skipping %s test\n", which);
    kith_db_destroy(db);
    kith_reactor_destroy(reactor);
    g_skipped_tests++;
    return 0;
}

static bool result_unreachable(const struct op_result *r)
{
    return !r->received || r->status == -(int)KITH_ECONNRESET;
}

static bool postgres_available(void)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0)
    {
        return false;
    }
    struct sockaddr_in addr = {0};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(5432);
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    int rc = connect(fd, (struct sockaddr *)&addr, sizeof(addr));
    close(fd);
    return rc == 0;
}

static void fill_pg_params(kith_db_params_t *params, uint32_t min_c, uint32_t max_c)
{
    const char *host = getenv("KITH_PG_HOST");
    const char *port = getenv("KITH_PG_PORT");
    const char *db_name = getenv("KITH_PG_DB");
    const char *user = getenv("KITH_PG_USER");
    const char *password = getenv("KITH_PG_PASSWORD");

    memset(params, 0, sizeof(*params));
    params->size = sizeof(*params);
    params->abi_version = KITH_ABI_VERSION;
    params->host = (host != nullptr) ? host : "127.0.0.1";
    if (port != nullptr)
    {
        char *end = nullptr;
        long parsed = strtol(port, &end, 10);
        params->port =
            (end != port && parsed > 0 && parsed <= UINT16_MAX) ? (uint16_t)parsed : 5432u;
    }
    else
    {
        params->port = 5432u;
    }
    params->db_name = (db_name != nullptr) ? db_name : "kith_example";
    params->user = (user != nullptr) ? user : "kith";
    params->password = (password != nullptr) ? password : "kith";
    params->min_connections = min_c;
    params->max_connections = max_c;
    params->connect_timeout_ms = 2000;
}

static int
setup_pool(kith_reactor_t **out_reactor, kith_db_t **out_db, uint32_t min_c, uint32_t max_c)
{
    if (session_create_reactor(out_reactor) != 0)
    {
        return -1;
    }
    kith_db_params_t params;
    fill_pg_params(&params, min_c, max_c);
    if (kith_db_create(&params, *out_reactor, nullptr, out_db) != 0)
    {
        kith_reactor_destroy(*out_reactor);
        return -1;
    }
    return 0;
}

// A pool's eager establishment is asynchronous, but a session open either
// pins a ready connection or starts the connect itself, so no extra wait is
// needed before the first open on a freshly created pool.

// ---------------------------------------------------------------------------
// validation ladder (backend-independent)
// ---------------------------------------------------------------------------

// Session-exec argument validation runs without a session handle by casting a
// scratch object: the NULL-shape checks fire before the handle is dereferenced.
static int test_session_invalid_args(void)
{
    int failures = 0;
    kith_reactor_t *reactor = nullptr;
    kith_db_t *db = nullptr;
    struct op_result scratch = {0};

    CHECK(setup_pool(&reactor, &db, 1u, 1u) == 0);

    struct session_result open_res = {0};
    struct close_result close_res = {0};

    // NULL db / NULL callback on open
    CHECK(kith_db_session_open(nullptr, open_callback, &open_res) == -(int)KITH_EINVAL);
    CHECK(kith_db_session_open(db, nullptr, &open_res) == -(int)KITH_EINVAL);

    // NULL session / NULL sql / NULL callback / params-without-count on exec
    CHECK(kith_db_session_exec(nullptr, "SELECT 1", nullptr, 0, op_callback, &scratch) ==
          -(int)KITH_EINVAL);
    CHECK(kith_db_session_exec(
              (kith_db_session_t *)&scratch, nullptr, nullptr, 0, op_callback, &scratch) ==
          -(int)KITH_EINVAL);
    CHECK(kith_db_session_exec(
              (kith_db_session_t *)&scratch, "SELECT 1", nullptr, 0, nullptr, nullptr) ==
          -(int)KITH_EINVAL);
    CHECK(kith_db_session_exec((kith_db_session_t *)&scratch,
                               "SELECT $1::int AS v",
                               nullptr,
                               1,
                               op_callback,
                               &scratch) == -(int)KITH_EINVAL);

    // NULL session / NULL callback on close
    CHECK(kith_db_session_close(nullptr, close_callback, &close_res) == -(int)KITH_EINVAL);
    CHECK(kith_db_session_close((kith_db_session_t *)&scratch, nullptr, &close_res) ==
          -(int)KITH_EINVAL);

    kith_db_destroy(db);
    kith_reactor_destroy(reactor);
    return failures;
}

// An open against an unreachable server pins a disconnected slot, the connect
// fails, and the module delivers the failure and frees the session itself —
// the caller never holds a handle for a failed open.
static int test_session_open_dead_endpoint(void)
{
    int failures = 0;
    kith_reactor_t *reactor = nullptr;
    kith_db_t *db = nullptr;

    CHECK(session_create_reactor(&reactor) == 0);
    kith_db_params_t params;
    fill_pg_params(&params, 1u, 1u);
    params.host = "127.0.0.1";
    params.port = 5431u; // nothing listens here
    params.connect_timeout_ms = 500;
    CHECK(kith_db_create(&params, reactor, nullptr, &db) == 0);

    struct session_result open_res = {0};
    CHECK(kith_db_session_open(db, open_callback, &open_res) == 0);

    run_until_replies(reactor, 1, 3000);
    CHECK(!g_timed_out);
    CHECK(open_res.received);
    CHECK(open_res.session == nullptr);
    CHECK(open_res.status == -(int)KITH_ECONNRESET);

    kith_db_destroy(db);
    kith_reactor_destroy(reactor);
    return failures;
}

// ---------------------------------------------------------------------------
// live-postgres session behavior
// ---------------------------------------------------------------------------

static int exec_wait(kith_reactor_t *reactor, kith_db_t *db, const char *sql, struct op_result *res)
{
    if (kith_db_exec_sql(db, sql, nullptr, 0, op_callback, res) != 0)
    {
        return -1;
    }
    run_until_replies(reactor, 1, 5000);
    return (res->received && res->status == 0) ? 0 : -1;
}

static int exec_wait_session(kith_reactor_t *reactor,
                             kith_db_session_t *session,
                             const char *sql,
                             struct op_result *res)
{
    if (kith_db_session_exec(session, sql, nullptr, 0, op_callback, res) != 0)
    {
        return -1;
    }
    run_until_replies(reactor, 1, 5000);
    return (res->received && res->status == 0) ? 0 : -1;
}

static int reset_pin_table(kith_reactor_t *reactor, kith_db_t *db)
{
    struct op_result res = {0};
    if (exec_wait(reactor, db, "DROP TABLE IF EXISTS db_session_pin_test", &res) != 0)
    {
        return -1;
    }
    if (exec_wait(
            reactor, db, "CREATE TABLE db_session_pin_test (k int PRIMARY KEY, v int)", &res) != 0)
    {
        return -1;
    }
    return 0;
}

// The full bracket, one statement at a time (the shape the async transaction
// context produces): BEGIN, INSERT, COMMIT through one pinned connection;
// then a second bracket that rolls back. The committed row survives, the
// rolled-back row does not — both read back through the pool path.
static int test_session_txn_commit_rollback(void)
{
    int failures = 0;
    kith_reactor_t *reactor = nullptr;
    kith_db_t *db = nullptr;

    CHECK(setup_pool(&reactor, &db, 1u, 4u) == 0);
    CHECK(reset_pin_table(reactor, db) == 0);

    struct session_result open_res = {0};
    struct op_result begin = {0};
    struct op_result insert = {0};
    struct op_result commit = {0};
    struct op_result select = {0};
    struct close_result close_res = {0};

    CHECK(kith_db_session_open(db, open_callback, &open_res) == 0);
    run_until_replies(reactor, 1, 3000);
    if (open_res.session == nullptr)
    {
        return skip_unreachable(db, reactor, "TXN-COMMIT open");
    }
    CHECK(open_res.status == 0);

    CHECK(exec_wait_session(reactor, open_res.session, "BEGIN", &begin) == 0);
    CHECK(exec_wait_session(reactor,
                            open_res.session,
                            "INSERT INTO db_session_pin_test (k, v) VALUES (1, 10)",
                            &insert) == 0);
    CHECK(exec_wait_session(reactor, open_res.session, "COMMIT", &commit) == 0);

    CHECK(exec_wait(reactor, db, "SELECT v FROM db_session_pin_test WHERE k = 1", &select) == 0);
    CHECK(select.n_rows == 1);
    CHECK(strcmp(select.value, "10") == 0);

    // The rollback leg: an inserted row vanishes when the bracket rolls back.
    struct op_result begin2 = {0};
    struct op_result insert2 = {0};
    struct op_result rollback = {0};
    struct op_result select2 = {0};
    struct session_result open2 = {0};
    struct close_result close_res2 = {0};

    CHECK(kith_db_session_open(db, open_callback, &open2) == 0);
    run_until_replies(reactor, 1, 3000);
    CHECK(open2.status == 0);
    CHECK(exec_wait_session(reactor, open2.session, "BEGIN", &begin2) == 0);
    CHECK(exec_wait_session(reactor,
                            open2.session,
                            "INSERT INTO db_session_pin_test (k, v) VALUES (2, 20)",
                            &insert2) == 0);
    CHECK(exec_wait_session(reactor, open2.session, "ROLLBACK", &rollback) == 0);
    CHECK(exec_wait(
              reactor, db, "SELECT count(*) FROM db_session_pin_test WHERE k = 2", &select2) == 0);
    CHECK(strcmp(select2.value, "0") == 0);

    CHECK(kith_db_session_close(open_res.session, close_callback, &close_res) == 0);
    CHECK(kith_db_session_close(open2.session, close_callback, &close_res2) == 0);
    run_until_replies(reactor, 2, 3000);
    CHECK(close_res.status == 0);
    CHECK(close_res2.status == 0);

    kith_db_destroy(db);
    kith_reactor_destroy(reactor);
    return failures;
}

// A pinned connection is invisible to the pool's round-robin: with the pool
// at its ceiling and the only connection pinned, a pool exec answers EBUSY,
// and the connection serves the pool again after the release.
static int test_session_exclusion(void)
{
    int failures = 0;
    kith_reactor_t *reactor = nullptr;
    kith_db_t *db = nullptr;

    CHECK(setup_pool(&reactor, &db, 1u, 1u) == 0);

    struct session_result open_res = {0};
    struct op_result pool_exec = {0};
    struct op_result after_close = {0};
    struct close_result close_res = {0};

    CHECK(kith_db_session_open(db, open_callback, &open_res) == 0);
    run_until_replies(reactor, 1, 3000);
    if (open_res.session == nullptr && open_res.status == -(int)KITH_ECONNRESET)
    {
        return skip_unreachable(db, reactor, "EXCLUSION open");
    }
    CHECK(open_res.status == 0);

    CHECK(kith_db_exec_sql(db, "SELECT 1", nullptr, 0, op_callback, &pool_exec) == 0);
    run_until_replies(reactor, 1, 3000);
    CHECK(pool_exec.status == -(int)KITH_EBUSY);

    CHECK(kith_db_session_close(open_res.session, close_callback, &close_res) == 0);
    run_until_replies(reactor, 1, 3000);
    CHECK(close_res.status == 0);

    CHECK(kith_db_exec_sql(db, "SELECT 1", nullptr, 0, op_callback, &after_close) == 0);
    run_until_replies(reactor, 1, 3000);
    CHECK(after_close.status == 0);

    kith_db_destroy(db);
    kith_reactor_destroy(reactor);
    return failures;
}

// Saturated opens: with the pool at its ceiling and its only connection
// pinned, a second open reports -KITH_EBUSY through the open callback.
static int test_session_open_saturation(void)
{
    int failures = 0;
    kith_reactor_t *reactor = nullptr;
    kith_db_t *db = nullptr;

    CHECK(setup_pool(&reactor, &db, 1u, 1u) == 0);

    struct session_result first = {0};
    struct session_result second = {0};
    struct close_result close_res = {0};

    CHECK(kith_db_session_open(db, open_callback, &first) == 0);
    CHECK(kith_db_session_open(db, open_callback, &second) == 0);
    run_until_replies(reactor, 2, 3000);
    if (first.session == nullptr && first.status == -(int)KITH_ECONNRESET)
    {
        return skip_unreachable(db, reactor, "SATURATION first open");
    }
    CHECK(first.status == 0);
    CHECK(second.received);
    CHECK(second.session == nullptr);
    CHECK(second.status == -(int)KITH_EBUSY);

    CHECK(kith_db_session_close(first.session, close_callback, &close_res) == 0);
    run_until_replies(reactor, 1, 3000);
    CHECK(close_res.status == 0);

    kith_db_destroy(db);
    kith_reactor_destroy(reactor);
    return failures;
}

// Close-while-in-flight: the release reports -KITH_EBUSY through its callback
// while a statement is running, the session stays open, and a retry after the
// statement's reply succeeds.
static int test_session_close_while_in_flight(void)
{
    int failures = 0;
    kith_reactor_t *reactor = nullptr;
    kith_db_t *db = nullptr;

    CHECK(setup_pool(&reactor, &db, 1u, 1u) == 0);

    struct session_result open_res = {0};
    struct op_result slow = {0};
    struct close_result close_busy = {0};
    struct close_result close_ok = {0};

    CHECK(kith_db_session_open(db, open_callback, &open_res) == 0);
    run_until_replies(reactor, 1, 3000);
    if (open_res.session == nullptr)
    {
        return skip_unreachable(db, reactor, "CLOSE-IN-FLIGHT open");
    }

    CHECK(kith_db_session_exec(
              open_res.session, "SELECT pg_sleep(0.5)", nullptr, 0, op_callback, &slow) == 0);
    CHECK(kith_db_session_close(open_res.session, close_callback, &close_busy) == 0);
    run_until_replies(reactor, 2, 3000);
    CHECK(close_busy.received);
    CHECK(close_busy.status == -(int)KITH_EBUSY);
    CHECK(slow.status == 0);

    CHECK(kith_db_session_close(open_res.session, close_callback, &close_ok) == 0);
    run_until_replies(reactor, 1, 3000);
    CHECK(close_ok.status == 0);

    kith_db_destroy(db);
    kith_reactor_destroy(reactor);
    return failures;
}

// The reconnect trap: a session whose backend is terminated mid-bracket must
// never land a follow-up statement on a reconnected slot. The terminated
// connection reports the loss, the bracket's work is gone (the server rolled
// it back), a COMMIT attempt on the session answers the loss, and the pool
// keeps serving after the release.
static int test_session_conn_death(void)
{
    int failures = 0;
    kith_reactor_t *reactor = nullptr;
    kith_db_t *db = nullptr;

    CHECK(setup_pool(&reactor, &db, 1u, 1u) == 0);
    CHECK(reset_pin_table(reactor, db) == 0);

    struct op_result pid = {0};
    CHECK(kith_db_exec_sql(db, "SELECT pg_backend_pid()", nullptr, 0, op_callback, &pid) == 0);
    run_until_replies(reactor, 1, 3000);
    if (result_unreachable(&pid))
    {
        return skip_unreachable(db, reactor, "CONN-DEATH pid probe");
    }
    CHECK(pid.status == 0);

    struct session_result open_res = {0};
    struct op_result begin = {0};
    struct op_result insert = {0};
    struct op_result commit = {0};
    struct close_result close_res = {0};
    struct op_result after = {0};

    CHECK(kith_db_session_open(db, open_callback, &open_res) == 0);
    run_until_replies(reactor, 1, 3000);
    CHECK(open_res.status == 0);

    CHECK(exec_wait_session(reactor, open_res.session, "BEGIN", &begin) == 0);
    CHECK(exec_wait_session(reactor,
                            open_res.session,
                            "INSERT INTO db_session_pin_test (k, v) VALUES (3, 30)",
                            &insert) == 0);

    char kill_sql[64];
    (void)snprintf(kill_sql, sizeof(kill_sql), "SELECT pg_terminate_backend(%s)", pid.value);
    struct op_result kill = {0};
    CHECK(kith_db_session_exec(open_res.session, kill_sql, nullptr, 0, op_callback, &kill) == 0);
    // The backend dies before answering; the connection loss surfaces as the
    // statement's reply status.
    run_until_replies(reactor, 1, 3000);
    CHECK(kill.received);
    CHECK(kill.status != 0);
    CHECK(kill.status == -(int)KITH_EIO || kill.status == -(int)KITH_ECONNRESET);

    // The bracket cannot land on a reconnected slot: the session's connection
    // is gone, and the commit must answer the loss, not succeed.
    CHECK(kith_db_session_exec(open_res.session, "COMMIT", nullptr, 0, op_callback, &commit) == 0);
    run_until_replies(reactor, 1, 3000);
    CHECK(commit.status == -(int)KITH_ECONNRESET);

    CHECK(kith_db_session_close(open_res.session, close_callback, &close_res) == 0);
    run_until_replies(reactor, 1, 3000);
    CHECK(close_res.status == 0);

    // The pool's connection reestablishes lazily and serves again.
    CHECK(kith_db_exec_sql(db, "SELECT 1", nullptr, 0, op_callback, &after) == 0);
    run_until_replies(reactor, 1, 5000);
    CHECK(after.status == 0);

    kith_db_destroy(db);
    kith_reactor_destroy(reactor);
    return failures;
}

// Destroy with an open session: the sweep unlinks and frees the session
// alongside the pool teardown; the pin proves destroy with a live session
// completes without a crash or a dangling pin.
static int test_session_destroy_with_open_session(void)
{
    int failures = 0;
    kith_reactor_t *reactor = nullptr;
    kith_db_t *db = nullptr;

    CHECK(setup_pool(&reactor, &db, 1u, 1u) == 0);

    struct session_result open_res = {0};
    CHECK(kith_db_session_open(db, open_callback, &open_res) == 0);
    run_until_replies(reactor, 1, 3000);
    if (open_res.session == nullptr)
    {
        return skip_unreachable(db, reactor, "DESTROY-OPEN open");
    }
    CHECK(open_res.status == 0);

    kith_db_destroy(db);
    kith_reactor_destroy(reactor);
    return failures;
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

struct db_leg
{
    int (*run)(void);
    bool env_gated;
    bool needs_service;
};

static const struct db_leg k_legs[] = {
    {test_session_invalid_args, false, false},
    {test_session_open_dead_endpoint, true, false},
    {test_session_txn_commit_rollback, true, true},
    {test_session_exclusion, true, true},
    {test_session_open_saturation, true, true},
    {test_session_close_while_in_flight, true, true},
    {test_session_conn_death, true, true},
    {test_session_destroy_with_open_session, true, true},
};

int main(void)
{
    const char *skip_env = getenv("KITH_SKIP_POSTGRES");
    const bool env_skip = skip_env != nullptr && strcmp(skip_env, "1") == 0;
    if (env_skip)
    {
        (void)fprintf(stderr, "db session: KITH_SKIP_POSTGRES=1, skipping integration tests\n");
    }
    const bool service_up = postgres_available();
    if (!service_up)
    {
        (void)fprintf(stderr, "db session: Postgres not reachable, skipping integration tests\n");
    }

    int failures = 0;
    for (size_t i = 0; i < sizeof(k_legs) / sizeof(k_legs[0]); i++)
    {
        if ((k_legs[i].env_gated && env_skip) || (k_legs[i].needs_service && !service_up))
        {
            g_skipped_tests++;
            continue;
        }
        failures += k_legs[i].run();
    }
    if (failures || g_skipped_tests)
    {
        (void)fprintf(
            stderr, "db session: %d failure(s), %d test(s) skipped\n", failures, g_skipped_tests);
    }
    return failures ? 1 : 0;
}
