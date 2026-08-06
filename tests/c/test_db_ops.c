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
// test harness
// ---------------------------------------------------------------------------

static int check_cond(bool ok, int line)
{
    if (!ok)
    {
        (void)fprintf(stderr, "db ops: assertion at line %d failed\n", line);
        return 1;
    }
    return 0;
}

#define CHECK(cond) failures += check_cond((cond), __LINE__)

static int db_ops_create_reactor(kith_reactor_t **out)
{
    kith_reactor_params_t params = {
        .size = sizeof(kith_reactor_params_t),
        .abi_version = KITH_ABI_VERSION,
        .max_fds = 64,
        .task_capacity = 64,
    };
    return kith_reactor_create(&params, nullptr, out);
}

// ---------------------------------------------------------------------------
// per-query result, passed as user_data so the callback can record each
// query's outcome independently.
// ---------------------------------------------------------------------------
struct op_result
{
    int status;
    bool received;
    const char *err_str;
    uint32_t n_rows;
    uint32_t n_cols;
    char value[256];
    size_t value_len;
    bool is_null;
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
    r->is_null = kith_db_reply_is_null(reply, 0, 0);
}

// ---------------------------------------------------------------------------
// reactor drive: pipeline all queries, then run the reactor until the last
// reply arrives or the deadline timer fires.
// ---------------------------------------------------------------------------
static kith_reactor_t *g_run_reactor = nullptr;
static int g_expected_replies = 0;
static int g_received_replies = 0;
static bool g_timed_out = false;
static int g_skipped_tests = 0;

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

static void counting_callback(kith_db_reply_t *reply)
{
    op_callback(reply);
    g_received_replies++;
    if (g_received_replies >= g_expected_replies)
    {
        kith_reactor_stop(g_run_reactor);
    }
}

static int skip_unreachable(kith_db_t *db, kith_reactor_t *reactor, const char *which)
{
    (void)fprintf(stderr, "db ops: Postgres not reachable, skipping %s test\n", which);
    kith_db_destroy(db);
    kith_reactor_destroy(reactor);
    g_skipped_tests++;
    return 0;
}

static bool result_unreachable(const struct op_result *r)
{
    return !r->received || r->status == -(int)KITH_ECONNRESET;
}

// ---------------------------------------------------------------------------
// recording allocator
// ---------------------------------------------------------------------------

// Captures every plain allocation's address and size so a test can assert
// the handle copied the connection-parameter strings through the caller's
// allocator at create time.

struct copy_alloc_ctx
{
    void *ptrs[16];
    size_t sizes[16];
    unsigned count;
    unsigned dropped;
};

static void *copy_alloc_fn(void *user_data, size_t size)
{
    struct copy_alloc_ctx *ctx = user_data;
    void *p = malloc(size);
    if (p != nullptr)
    {
        if (ctx->count < 16u)
        {
            ctx->ptrs[ctx->count] = p;
            ctx->sizes[ctx->count] = size;
            ctx->count++;
        }
        else
        {
            ctx->dropped++;
        }
    }
    return p;
}

static void *copy_alloc_zero_fn(void *user_data, size_t count, size_t size)
{
    struct copy_alloc_ctx *ctx = user_data;
    void *p = calloc(count, size);
    if (p != nullptr && count != 0u && size != 0u)
    {
        size_t total = count * size;
        if (ctx->count < 16u)
        {
            ctx->ptrs[ctx->count] = p;
            ctx->sizes[ctx->count] = total;
            ctx->count++;
        }
        else
        {
            ctx->dropped++;
        }
    }
    return p;
}

static void *copy_realloc_fn(void *user_data, void *ptr, size_t size)
{
    (void)user_data;
    return realloc(ptr, size);
}

static void copy_free_fn(void *user_data, void *ptr)
{
    (void)user_data;
    free(ptr);
}

// 0 when one recorded allocation carries exactly @p s (length + NUL); the
// only allocation of that size with that content is the handle's copy.
static int copied_string_present(const struct copy_alloc_ctx *ctx, const char *s)
{
    size_t need = strlen(s) + 1u;
    for (unsigned i = 0u; i < ctx->count; i++)
    {
        if (ctx->sizes[i] == need && memcmp(ctx->ptrs[i], s, need) == 0)
        {
            return 0;
        }
    }
    (void)fprintf(stderr, "db copy probe: no allocator copy of '%s'\n", s);
    return 1;
}

// The create-argument string copies: the handle owns images of every
// non-NULL connection string, made through the caller's allocator, so the
// caller's buffers may be released or overwritten once the create returns.
static int test_create_copies_param_strings(void)
{
    struct copy_alloc_ctx ctx = {0};
    kith_allocator_t alloc = {
        .size = sizeof(alloc),
        .abi_version = KITH_ABI_VERSION,
        .user_data = &ctx,
        .alloc = copy_alloc_fn,
        .alloc_zero = copy_alloc_zero_fn,
        .realloc = copy_realloc_fn,
        .free = copy_free_fn,
    };
    char host[] = "127.0.0.1";
    char db_name[] = "kith_copy_probe";
    char user[] = "kith_copy_user";
    kith_db_params_t params = {
        .size = sizeof(params),
        .abi_version = KITH_ABI_VERSION,
        .host = host,
        .port = 5432u,
        .db_name = db_name,
        .user = user,
        .password = nullptr,
        .min_connections = 1u,
        .max_connections = 1u,
        .connect_timeout_ms = 2000u,
    };
    kith_reactor_t *reactor = nullptr;
    kith_db_t *db = nullptr;

    int failures = 0;
    if (db_ops_create_reactor(&reactor) != 0)
    {
        return 1;
    }
    int rc = kith_db_create(&params, reactor, &alloc, &db);
    CHECK(rc == 0);
    CHECK(db != nullptr);
    if (rc != 0 || db == nullptr)
    {
        kith_reactor_destroy(reactor);
        return 1;
    }

    // The caller's buffers are dead weight after the create: overwrite
    // them. A handle still reading them observes garbage; the copies made
    // at create time are unaffected.
    memset(host, 'X', sizeof(host) - 1u);
    memset(db_name, 'X', sizeof(db_name) - 1u);
    memset(user, 'X', sizeof(user) - 1u);

    failures += copied_string_present(&ctx, "127.0.0.1");
    failures += copied_string_present(&ctx, "kith_copy_probe");
    failures += copied_string_present(&ctx, "kith_copy_user");
    CHECK(ctx.dropped == 0u);

    kith_db_destroy(db);
    kith_reactor_destroy(reactor);
    return failures;
}

// ---------------------------------------------------------------------------
// postgres reachability probe
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// shared setup
// ---------------------------------------------------------------------------

// The integration CI services and scripts/dev-postgres.sh define the local
// database; the environment overrides match the Python integration tests.
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

// Bind a loopback listener whose queue nothing drains. The kernel completes
// the TCP handshake into the accept queue without an accept, so the peer
// looks connected but never answers the startup packet.
static int silent_listener_bind(uint16_t *out_port)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0)
    {
        return -1;
    }
    struct sockaddr_in addr = {0};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;
    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0 || listen(fd, 1) != 0)
    {
        close(fd);
        return -1;
    }
    struct sockaddr_in bound = {0};
    socklen_t addr_len = sizeof(bound);
    if (getsockname(fd, (struct sockaddr *)&bound, &addr_len) != 0)
    {
        close(fd);
        return -1;
    }
    *out_port = ntohs(bound.sin_port);
    return fd;
}

static int setup(kith_reactor_t **out_reactor, kith_db_t **out_db)
{
    if (db_ops_create_reactor(out_reactor) != 0)
    {
        return -1;
    }
    kith_db_params_t params;
    fill_pg_params(&params, 1u, 4u);
    if (kith_db_create(&params, *out_reactor, nullptr, out_db) != 0)
    {
        kith_reactor_destroy(*out_reactor);
        return -1;
    }
    return 0;
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

// SELECT 1 AS one — one row, one column, text value "1".
static int test_exec_select(void)
{
    int failures = 0;
    kith_reactor_t *reactor = nullptr;
    kith_db_t *db = nullptr;

    CHECK(setup(&reactor, &db) == 0);
    CHECK(kith_db_register_query(db, "select_one", "SELECT 1 AS one", 0) == 0);

    struct op_result res = {0};
    int rc = kith_db_exec(db, "select_one", nullptr, 0, counting_callback, &res);
    CHECK(rc == 0);

    run_until_replies(reactor, 1, 3000);

    if (result_unreachable(&res))
    {
        return skip_unreachable(db, reactor, "SELECT");
    }
    CHECK(res.status == 0);
    CHECK(res.n_rows == 1);
    CHECK(res.n_cols == 1);
    CHECK(res.value_len == 1);
    CHECK(strcmp(res.value, "1") == 0);

    kith_db_destroy(db);
    kith_reactor_destroy(reactor);
    return failures;
}

// SELECT $1::int AS val with param "42" — verify parameter substitution.
static int test_exec_params(void)
{
    int failures = 0;
    kith_reactor_t *reactor = nullptr;
    kith_db_t *db = nullptr;

    CHECK(setup(&reactor, &db) == 0);
    CHECK(kith_db_register_query(db, "select_param", "SELECT $1::int AS val", 1) == 0);

    const char *params[] = {"42"};
    struct op_result res = {0};
    int rc = kith_db_exec(db, "select_param", params, 1, counting_callback, &res);
    CHECK(rc == 0);

    run_until_replies(reactor, 1, 3000);

    if (result_unreachable(&res))
    {
        return skip_unreachable(db, reactor, "PARAMS");
    }
    CHECK(res.status == 0);
    CHECK(res.n_rows == 1);
    CHECK(strcmp(res.value, "42") == 0);

    kith_db_destroy(db);
    kith_reactor_destroy(reactor);
    return failures;
}

// SELECT $1::int AS val with a NULL param — verify is_null reports true.
static int test_exec_null_param(void)
{
    int failures = 0;
    kith_reactor_t *reactor = nullptr;
    kith_db_t *db = nullptr;

    CHECK(setup(&reactor, &db) == 0);
    CHECK(kith_db_register_query(db, "select_null", "SELECT $1::int AS val", 1) == 0);

    const char *params[] = {nullptr};
    struct op_result res = {0};
    int rc = kith_db_exec(db, "select_null", params, 1, counting_callback, &res);
    CHECK(rc == 0);

    run_until_replies(reactor, 1, 3000);

    if (result_unreachable(&res))
    {
        return skip_unreachable(db, reactor, "NULL-PARAM");
    }
    CHECK(res.status == 0);
    CHECK(res.n_rows == 1);
    CHECK(res.is_null);
    CHECK(res.value_len == 0);

    kith_db_destroy(db);
    kith_reactor_destroy(reactor);
    return failures;
}

// A command (non-SELECT) returns n_rows == 0.
static int test_exec_command(void)
{
    int failures = 0;
    kith_reactor_t *reactor = nullptr;
    kith_db_t *db = nullptr;

    CHECK(setup(&reactor, &db) == 0);
    CHECK(kith_db_register_query(db, "do_nothing", "DO $$ BEGIN END $$", 0) == 0);

    struct op_result res = {0};
    int rc = kith_db_exec(db, "do_nothing", nullptr, 0, counting_callback, &res);
    CHECK(rc == 0);

    run_until_replies(reactor, 1, 3000);

    if (result_unreachable(&res))
    {
        return skip_unreachable(db, reactor, "COMMAND");
    }
    CHECK(res.status == 0);
    CHECK(res.n_rows == 0);

    kith_db_destroy(db);
    kith_reactor_destroy(reactor);
    return failures;
}

// A SQL error surfaces as a non-zero status with an error string.
static int test_exec_sql_error(void)
{
    int failures = 0;
    kith_reactor_t *reactor = nullptr;
    kith_db_t *db = nullptr;

    CHECK(setup(&reactor, &db) == 0);
    CHECK(kith_db_register_query(db, "bad_sql", "SELECT * FROM no_such_table", 0) == 0);

    struct op_result res = {0};
    int rc = kith_db_exec(db, "bad_sql", nullptr, 0, counting_callback, &res);
    CHECK(rc == 0);

    run_until_replies(reactor, 1, 3000);

    if (result_unreachable(&res))
    {
        return skip_unreachable(db, reactor, "SQL-ERROR");
    }
    CHECK(res.status != 0);
    CHECK(res.err_str != nullptr);

    kith_db_destroy(db);
    kith_reactor_destroy(reactor);
    return failures;
}

// API validation paths reject the call before any Postgres traffic.
static int test_invalid_args(void)
{
    int failures = 0;
    kith_reactor_t *reactor = nullptr;
    kith_db_t *db = nullptr;

    CHECK(setup(&reactor, &db) == 0);
    CHECK(kith_db_register_query(db, "q", "SELECT 1", 0) == 0);

    struct op_result res = {0};

    // NULL db
    CHECK(kith_db_exec(nullptr, "q", nullptr, 0, op_callback, &res) == -(int)KITH_EINVAL);

    // NULL query name
    CHECK(kith_db_exec(db, nullptr, nullptr, 0, op_callback, &res) == -(int)KITH_EINVAL);

    // NULL callback
    CHECK(kith_db_exec(db, "q", nullptr, 0, nullptr, nullptr) == -(int)KITH_EINVAL);

    // unregistered query
    CHECK(kith_db_exec(db, "no_such", nullptr, 0, op_callback, &res) == -(int)KITH_ENOENT);

    // wrong param count
    CHECK(kith_db_exec(db, "q", nullptr, 1, op_callback, &res) == -(int)KITH_ERANGE);

    kith_db_destroy(db);
    kith_reactor_destroy(reactor);
    return failures;
}

// Raw SQL executes without a registry entry: "SELECT 42 AS v" runs although
// nothing was registered on the pool.
static int test_execsql_select(void)
{
    int failures = 0;
    kith_reactor_t *reactor = nullptr;
    kith_db_t *db = nullptr;

    CHECK(setup(&reactor, &db) == 0);

    struct op_result res = {0};
    int rc = kith_db_exec_sql(db, "SELECT 42 AS v", nullptr, 0, counting_callback, &res);
    CHECK(rc == 0);

    run_until_replies(reactor, 1, 3000);

    if (result_unreachable(&res))
    {
        return skip_unreachable(db, reactor, "EXEC-SQL SELECT");
    }
    CHECK(res.status == 0);
    CHECK(res.n_rows == 1);
    CHECK(res.n_cols == 1);
    CHECK(strcmp(res.value, "42") == 0);

    kith_db_destroy(db);
    kith_reactor_destroy(reactor);
    return failures;
}

// Raw SQL with parameters substitutes them; the statement text is copied at
// call time, so the caller releases its buffer immediately after the call.
static int test_execsql_params(void)
{
    int failures = 0;
    kith_reactor_t *reactor = nullptr;
    kith_db_t *db = nullptr;

    CHECK(setup(&reactor, &db) == 0);

    char sql[] = "SELECT $1::int AS val";
    const char *params[] = {"7"};
    struct op_result res = {0};
    int rc = kith_db_exec_sql(db, sql, params, 1, counting_callback, &res);
    CHECK(rc == 0);
    sql[0] = 'X'; // rewrite the caller's buffer before the reply arrives

    run_until_replies(reactor, 1, 3000);

    if (result_unreachable(&res))
    {
        return skip_unreachable(db, reactor, "EXEC-SQL PARAMS");
    }
    CHECK(res.status == 0);
    CHECK(res.n_rows == 1);
    CHECK(strcmp(res.value, "7") == 0);

    kith_db_destroy(db);
    kith_reactor_destroy(reactor);
    return failures;
}

// A SQL error on the raw path surfaces as a non-zero status with an error
// string, exactly like the registry path.
static int test_execsql_sql_error(void)
{
    int failures = 0;
    kith_reactor_t *reactor = nullptr;
    kith_db_t *db = nullptr;

    CHECK(setup(&reactor, &db) == 0);

    struct op_result res = {0};
    int rc =
        kith_db_exec_sql(db, "SELECT * FROM no_such_table", nullptr, 0, counting_callback, &res);
    CHECK(rc == 0);

    run_until_replies(reactor, 1, 3000);

    if (result_unreachable(&res))
    {
        return skip_unreachable(db, reactor, "EXEC-SQL ERROR");
    }
    CHECK(res.status != 0);
    CHECK(res.err_str != nullptr);

    kith_db_destroy(db);
    kith_reactor_destroy(reactor);
    return failures;
}

// API validation paths reject the raw-SQL call before any Postgres traffic.
static int test_execsql_invalid_args(void)
{
    int failures = 0;
    kith_reactor_t *reactor = nullptr;
    kith_db_t *db = nullptr;

    CHECK(setup(&reactor, &db) == 0);

    struct op_result res = {0};

    // NULL db
    CHECK(kith_db_exec_sql(nullptr, "SELECT 1", nullptr, 0, op_callback, &res) ==
          -(int)KITH_EINVAL);

    // NULL sql
    CHECK(kith_db_exec_sql(db, nullptr, nullptr, 0, op_callback, &res) == -(int)KITH_EINVAL);

    // NULL callback
    CHECK(kith_db_exec_sql(db, "SELECT 1", nullptr, 0, nullptr, nullptr) == -(int)KITH_EINVAL);

    // params without a count
    CHECK(kith_db_exec_sql(db, "SELECT $1::int AS v", nullptr, 1, op_callback, &res) ==
          -(int)KITH_EINVAL);

    kith_db_destroy(db);
    kith_reactor_destroy(reactor);
    return failures;
}

// ---------------------------------------------------------------------------
// fault injection: the reactor has no free registration slot
// ---------------------------------------------------------------------------

static void db_ops_noop_event(int fd, unsigned int events, void *ctx)
{
    (void)fd;
    (void)events;
    (void)ctx;
}

// Create still succeeds (its per-slot failures leave the slot for lazy
// reconnect), and the queued query fails with the registration error instead
// of stalling on events that can never fire. The db points at a bound-but-
// silent listener so the connect attempt is still in flight when the socket
// registration is attempted: the registration failure decides on every host,
// with or without a Postgres server.
static int test_exec_without_registration_slot(void)
{
    int failures = 0;
    kith_reactor_t *reactor = nullptr;
    kith_db_t *db = nullptr;

    uint16_t port = 0;
    int listener = silent_listener_bind(&port);
    CHECK(listener >= 0);
    if (listener < 0)
    {
        return 1;
    }

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
    CHECK(kith_reactor_add(reactor, occupy[0], KITH_REACTOR_IN, db_ops_noop_event, nullptr) == 0);
    CHECK(kith_reactor_add(reactor, occupy[1], KITH_REACTOR_IN, db_ops_noop_event, nullptr) == 0);

    kith_db_params_t dparams;
    fill_pg_params(&dparams, 1u, 1u);
    dparams.host = "127.0.0.1";
    dparams.port = port;
    CHECK(kith_db_create(&dparams, reactor, nullptr, &db) == 0);
    CHECK(kith_db_register_query(db, "q", "SELECT 1", 0) == 0);

    struct op_result res = {0};
    CHECK(kith_db_exec(db, "q", nullptr, 0, counting_callback, &res) == 0);

    run_until_replies(reactor, 1, 2000);
    CHECK(!g_timed_out);
    CHECK(res.received);
    CHECK(res.status == -(int)KITH_EBUSY);

    kith_db_destroy(db);
    kith_reactor_destroy(reactor);
    close(occupy[0]);
    close(occupy[1]);
    close(listener);
    return failures;
}

// A pool saturated at its connection ceiling reports -KITH_EBUSY through the
// callback: the first query attaches to the connecting connection, the second
// finds every slot busy or carrying a queued query. The db points at a
// bound-but-silent listener, so the connecting slot never completes: the
// saturation decision precedes any query traffic, and the first attempt ends
// at the per-attempt deadline, with or without a Postgres server.
static int test_exec_pool_saturation(void)
{
    int failures = 0;
    kith_reactor_t *reactor = nullptr;
    kith_db_t *db = nullptr;

    uint16_t port = 0;
    int listener = silent_listener_bind(&port);
    CHECK(listener >= 0);
    if (listener < 0)
    {
        return 1;
    }

    CHECK(db_ops_create_reactor(&reactor) == 0);

    kith_db_params_t params;
    fill_pg_params(&params, 1u, 1u);
    params.host = "127.0.0.1";
    params.port = port;
    params.connect_timeout_ms = 400;
    CHECK(kith_db_create(&params, reactor, nullptr, &db) == 0);
    CHECK(kith_db_register_query(db, "q", "SELECT 1", 0) == 0);

    struct op_result first = {0};
    struct op_result second = {0};
    CHECK(kith_db_exec(db, "q", nullptr, 0, counting_callback, &first) == 0);
    CHECK(kith_db_exec(db, "q", nullptr, 0, counting_callback, &second) == 0);

    run_until_replies(reactor, 2, 5000);
    CHECK(!g_timed_out);
    CHECK(second.received);
    CHECK(second.status == -(int)KITH_EBUSY);
    CHECK(second.err_str != nullptr);
    CHECK(first.received);
    CHECK(first.status == -(int)KITH_ECONNRESET);

    kith_db_destroy(db);
    kith_reactor_destroy(reactor);
    close(listener);
    return failures;
}

// ---------------------------------------------------------------------------
// connect deadline: a silent listener has no kernel ceiling
// ---------------------------------------------------------------------------

// A handshake-completed-then-silent peer leaves libpq's async connect waiting
// on readability forever: the kernel keeps ACKing keepalive probes and no
// connect-timeout keyword applies to PQconnectPoll. The pool's per-attempt
// deadline is what bounds the attempt, so the query answers the deadline
// failure well inside the run window instead of hanging. The listener stays
// open for the whole test; closing it early surfaces as a hangup rather
// than the deadline.
static int test_connect_deadline_silent_listener(void)
{
    int failures = 0;
    kith_reactor_t *reactor = nullptr;
    kith_db_t *db = nullptr;

    uint16_t port = 0;
    int listener = silent_listener_bind(&port);
    CHECK(listener >= 0);
    if (listener < 0)
    {
        return 1;
    }

    CHECK(db_ops_create_reactor(&reactor) == 0);
    kith_db_params_t params;
    fill_pg_params(&params, 1u, 1u);
    params.host = "127.0.0.1";
    params.port = port;
    params.connect_timeout_ms = 400;
    CHECK(kith_db_create(&params, reactor, nullptr, &db) == 0);
    CHECK(kith_db_register_query(db, "q", "SELECT 1", 0) == 0);

    struct op_result res = {0};
    CHECK(kith_db_exec(db, "q", nullptr, 0, counting_callback, &res) == 0);

    // The window covers the eager attempt's deadline (armed at create) plus
    // the query's own attempt, with margin for scheduler contention.
    run_until_replies(reactor, 1, 3000);
    CHECK(!g_timed_out);
    CHECK(res.received);
    CHECK(res.status == -(int)KITH_ECONNRESET);
    CHECK(res.err_str != nullptr);

    kith_db_destroy(db);
    kith_reactor_destroy(reactor);
    close(listener);
    return failures;
}

// ---------------------------------------------------------------------------
// off-thread submission — the any-thread contract surface
// ---------------------------------------------------------------------------

// A worker thread issues queries while the reactor runs on the main thread.
// Every reply must report success with the value round-tripping, and every
// callback must fire on the reactor thread.
static pid_t g_reactor_tid = 0;
static bool g_reply_off_thread = false;

static void off_thread_callback(kith_db_reply_t *reply)
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
    kith_db_t *db;
    struct op_result result;
    int submit_failures;
};

static void *off_thread_worker(void *arg)
{
    struct off_thread_ctx *w = arg;
    if (kith_db_exec(w->db, "select_one", nullptr, 0, off_thread_callback, &w->result) != 0)
    {
        w->submit_failures++;
    }
    return nullptr;
}

static int test_off_thread_submission(void)
{
    int failures = 0;
    kith_reactor_t *reactor = nullptr;
    kith_db_t *db = nullptr;

    CHECK(setup(&reactor, &db) == 0);
    CHECK(kith_db_register_query(db, "select_one", "SELECT 1 AS one", 0) == 0);

    g_reactor_tid = gettid();
    for (int round = 0; round < 4; round++)
    {
        struct off_thread_ctx w = {.db = db, .submit_failures = 0};
        g_reply_off_thread = false;
        pthread_t worker;
        CHECK(pthread_create(&worker, nullptr, off_thread_worker, &w) == 0);
        run_until_replies(reactor, 1, 3000);
        CHECK(pthread_join(worker, nullptr) == 0);

        if (w.submit_failures == 0 && !g_reply_off_thread && result_unreachable(&w.result))
        {
            return skip_unreachable(db, reactor, "off-thread");
        }
        CHECK(w.submit_failures == 0);
        CHECK(!g_timed_out);
        CHECK(g_reply_off_thread == false);
        CHECK(w.result.status == 0);
        CHECK(w.result.n_rows == 1);
        CHECK(w.result.value_len == 1);
        CHECK(w.result.value[0] == '1');
    }

    kith_db_destroy(db);
    kith_reactor_destroy(reactor);
    return failures;
}

// ---------------------------------------------------------------------------
// pool creation against a running reactor — establishment must ride the
// reactor's task queue
// ---------------------------------------------------------------------------

// The reactor pumps on a background thread while the main thread creates a
// second pool against the live reactor. A round-trip on the first pool
// proves the pump is active before the second pool is created; the second
// pool's establishment task must then drain on the reactor thread and its
// round-trip must complete. On the main thread this shape races
// kith_reactor_run's fd table with the second pool's socket registration.
// The pump spins run because an idle-ready pool deregisters every socket:
// a bare run returns on the empty-reactor pass, leaving the second pool
// created against a stopped reactor.
static atomic_int g_pump_replies;
static atomic_bool g_pump_stop;

static void pump_phase_callback(kith_db_reply_t *reply)
{
    op_callback(reply);
    atomic_fetch_add(&g_pump_replies, 1);
}

static bool await_pump_replies(int want, int timeout_ms)
{
    struct timespec step = {.tv_sec = 0, .tv_nsec = 2'000'000};
    int waited = 0;
    while (atomic_load(&g_pump_replies) < want && waited < timeout_ms)
    {
        nanosleep(&step, nullptr);
        waited += 2;
    }
    return atomic_load(&g_pump_replies) >= want;
}

static void *reactor_pump(void *arg)
{
    kith_reactor_t *reactor = arg;
    while (!atomic_load_explicit(&g_pump_stop, memory_order_acquire))
    {
        (void)kith_reactor_run(reactor);
    }
    return nullptr;
}

static int test_off_thread_pool_create(void)
{
    int failures = 0;
    kith_reactor_t *reactor = nullptr;
    kith_db_t *first = nullptr;
    kith_db_t *second = nullptr;
    pthread_t pump;

    CHECK(db_ops_create_reactor(&reactor) == 0);
    kith_db_params_t params;
    fill_pg_params(&params, 1u, 4u);
    CHECK(kith_db_create(&params, reactor, nullptr, &first) == 0);
    CHECK(kith_db_register_query(first, "select_one", "SELECT 1 AS one", 0) == 0);

    atomic_store(&g_pump_stop, false);
    CHECK(pthread_create(&pump, nullptr, reactor_pump, reactor) == 0);

    struct op_result first_res = {0};
    atomic_store(&g_pump_replies, 0);
    CHECK(kith_db_exec(first, "select_one", nullptr, 0, pump_phase_callback, &first_res) == 0);
    CHECK(await_pump_replies(1, 3000));
    if (result_unreachable(&first_res))
    {
        atomic_store(&g_pump_stop, true);
        kith_reactor_stop(reactor);
        CHECK(pthread_join(pump, nullptr) == 0);
        (void)fprintf(stderr, "db ops: Postgres not reachable, skipping off-thread pool create\n");
        kith_db_destroy(first);
        kith_reactor_destroy(reactor);
        return failures;
    }
    CHECK(first_res.status == 0);

    CHECK(kith_db_create(&params, reactor, nullptr, &second) == 0);
    CHECK(kith_db_register_query(second, "select_one", "SELECT 1 AS one", 0) == 0);
    struct op_result second_res = {0};
    atomic_store(&g_pump_replies, 0);
    CHECK(kith_db_exec(second, "select_one", nullptr, 0, pump_phase_callback, &second_res) == 0);
    CHECK(await_pump_replies(1, 3000));
    CHECK(second_res.status == 0);
    CHECK(second_res.n_rows == 1);
    CHECK(second_res.value_len == 1);
    CHECK(second_res.value[0] == '1');

    atomic_store(&g_pump_stop, true);
    kith_reactor_stop(reactor);
    CHECK(pthread_join(pump, nullptr) == 0);

    kith_db_destroy(second);
    kith_db_destroy(first);
    kith_reactor_destroy(reactor);
    return failures;
}

// ---------------------------------------------------------------------------
// destroy with a reply still pending — the cancel branch
// ---------------------------------------------------------------------------

// A query dispatched to the server but not yet replied to is pinned to its
// connection when kith_db_destroy runs: the destroy frees the command block
// and tears the live connections down. The pump proves the slow query's
// submission task drained before the stop by round-tripping a second probe
// after it — the task queue is FIFO, so the probe's reply cannot be observed
// while the slow submission is still queued. Stopping the pump leaves the
// reactor quiescent (no submission queued, no callback running), the
// precondition kith_db_destroy documents, with the slow query's reply
// pending server-side for the whole window. An exec that fast-fails on pool
// pressure fires its callback immediately, so the received flag staying
// false is what pins the pending-command shape.
static int test_destroy_cancels_pending_query(void)
{
    int failures = 0;
    kith_reactor_t *reactor = nullptr;
    kith_db_t *db = nullptr;
    pthread_t pump;

    CHECK(db_ops_create_reactor(&reactor) == 0);
    kith_db_params_t params;
    fill_pg_params(&params, 2u, 4u);
    CHECK(kith_db_create(&params, reactor, nullptr, &db) == 0);
    CHECK(kith_db_register_query(db, "select_one", "SELECT 1 AS one", 0) == 0);
    CHECK(kith_db_register_query(db, "slow_one", "SELECT pg_sleep(2.0)", 0) == 0);

    atomic_store(&g_pump_stop, false);
    CHECK(pthread_create(&pump, nullptr, reactor_pump, reactor) == 0);

    struct op_result probe_res = {0};
    atomic_store(&g_pump_replies, 0);
    CHECK(kith_db_exec(db, "select_one", nullptr, 0, pump_phase_callback, &probe_res) == 0);
    CHECK(await_pump_replies(1, 3000));
    if (result_unreachable(&probe_res))
    {
        atomic_store(&g_pump_stop, true);
        kith_reactor_stop(reactor);
        CHECK(pthread_join(pump, nullptr) == 0);
        (void)fprintf(stderr, "db ops: Postgres not reachable, skipping destroy cancel test\n");
        kith_db_destroy(db);
        kith_reactor_destroy(reactor);
        return failures;
    }
    CHECK(probe_res.status == 0);

    struct op_result slow_res = {0};
    CHECK(kith_db_exec(db, "slow_one", nullptr, 0, pump_phase_callback, &slow_res) == 0);

    struct op_result drain_probe_res = {0};
    atomic_store(&g_pump_replies, 0);
    CHECK(kith_db_exec(db, "select_one", nullptr, 0, pump_phase_callback, &drain_probe_res) == 0);
    CHECK(await_pump_replies(1, 3000));
    CHECK(drain_probe_res.status == 0);

    // The 2.0 s server-side sleep outlasts the probe round-trip and the
    // shutdown below, so the only reply since the reset is the probe's.
    CHECK(atomic_load(&g_pump_replies) == 1);
    CHECK(slow_res.received == false);

    atomic_store(&g_pump_stop, true);
    kith_reactor_stop(reactor);
    CHECK(pthread_join(pump, nullptr) == 0);

    CHECK(atomic_load(&g_pump_replies) == 1);
    CHECK(slow_res.received == false);

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
    {test_invalid_args, false, false},
    {test_execsql_invalid_args, false, false},
    {test_create_copies_param_strings, false, false},
    {test_exec_without_registration_slot, true, false},
    {test_exec_pool_saturation, true, false},
    {test_connect_deadline_silent_listener, true, false},
    {test_exec_select, true, true},
    {test_exec_params, true, true},
    {test_exec_null_param, true, true},
    {test_exec_command, true, true},
    {test_exec_sql_error, true, true},
    {test_execsql_select, true, true},
    {test_execsql_params, true, true},
    {test_execsql_sql_error, true, true},
    {test_off_thread_submission, true, true},
    {test_off_thread_pool_create, true, true},
    {test_destroy_cancels_pending_query, true, true},
};

int main(void)
{
    const char *skip_env = getenv("KITH_SKIP_POSTGRES");
    const bool env_skip = skip_env != nullptr && strcmp(skip_env, "1") == 0;
    if (env_skip)
    {
        (void)fprintf(stderr, "db ops: KITH_SKIP_POSTGRES=1, skipping integration tests\n");
    }
    const bool service_up = postgres_available();
    if (!service_up)
    {
        (void)fprintf(stderr, "db ops: Postgres not reachable, skipping integration tests\n");
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
            stderr, "db ops: %d failure(s), %d test(s) skipped\n", failures, g_skipped_tests);
    }
    return failures ? 1 : 0;
}
