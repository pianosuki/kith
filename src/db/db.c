/* Public handle, command allocator, and query registry for the db module:
 * params validation, create and destroy, per-command SQL and parameter copies,
 * and the registered-query table. Sits above conn.c (the libpq connection
 * drive); the public contract is include/kith/db/db.h. */

#include <stdckdint.h>
#include <string.h>

#include "db/db_internal.h"
#include "kith/types.h"
#include "kith/version.h"

// ---------------------------------------------------------------------------
// cmd helpers
// ---------------------------------------------------------------------------
struct kith_db_cmd *cmd_alloc(struct kith_db *db,
                              const char *sql,
                              uint32_t n_params,
                              const char *const *params,
                              kith_db_reply_fn callback,
                              void *user_data)
{
    const kith_allocator_t *alloc = db->allocator;
    struct kith_db_cmd *cmd = kith_alloc_zero(alloc, 1, sizeof(*cmd));
    if (cmd == NULL)
    {
        return NULL;
    }
    cmd->db = db;
    cmd->callback = callback;
    cmd->user_data = user_data;
    cmd->sql = sql;
    cmd->n_params = n_params;

    if (n_params == 0u)
    {
        return cmd;
    }

    cmd->param_copies = kith_alloc_zero(alloc, n_params, sizeof(char *));
    if (cmd->param_copies == NULL)
    {
        kith_free(alloc, cmd);
        return NULL;
    }
    for (uint32_t i = 0u; i < n_params; i++)
    {
        if (params[i] != NULL)
        {
            cmd->param_copies[i] = kith_strdup(alloc, params[i]);
            if (cmd->param_copies[i] == NULL)
            {
                cmd_free(cmd);
                return NULL;
            }
        }
    }
    return cmd;
}

void cmd_free_params(struct kith_db_cmd *cmd)
{
    if (cmd == NULL || cmd->param_copies == NULL)
    {
        return;
    }
    const kith_allocator_t *alloc = cmd->db->allocator;
    for (uint32_t i = 0u; i < cmd->n_params; i++)
    {
        kith_free(alloc, cmd->param_copies[i]);
        cmd->param_copies[i] = NULL;
    }
    kith_free(alloc, cmd->param_copies);
    cmd->param_copies = NULL;
}

void cmd_free(struct kith_db_cmd *cmd)
{
    if (cmd == NULL)
    {
        return;
    }
    cmd_free_params(cmd);
    kith_free(cmd->db->allocator, cmd->sql_owned);
    kith_free(cmd->db->allocator, cmd);
}

void cmd_fail(struct kith_db_cmd *cmd, int status, const char *err_str)
{
    if (cmd == NULL)
    {
        return;
    }
    struct kith_db_reply reply = {0};
    reply.status = status;
    reply.err_str = err_str;
    reply.user_data = cmd->user_data;
    cmd->callback(&reply);
    cmd_free(cmd);
}

// ---------------------------------------------------------------------------
// query registry
// ---------------------------------------------------------------------------
static struct kith_db_query *registry_lookup(const kith_db_t *db, const char *name)
{
    for (struct kith_db_query *q = db->queries; q != NULL; q = q->next)
    {
        if (strcmp(q->name, name) == 0)
        {
            return q;
        }
    }
    return NULL;
}

static int registry_add(kith_db_t *db, const char *name, const char *sql, uint32_t n_params)
{
    if (registry_lookup(db, name) != NULL)
    {
        return kith_error_return(KITH_EEXIST);
    }
    struct kith_db_query *q = kith_alloc_zero(db->allocator, 1, sizeof(*q));
    if (q == NULL)
    {
        return kith_error_return(KITH_ENOMEM);
    }
    q->name = kith_strdup(db->allocator, name);
    q->sql = kith_strdup(db->allocator, sql);
    if (q->name == NULL || q->sql == NULL)
    {
        kith_free(db->allocator, q->name);
        kith_free(db->allocator, q->sql);
        kith_free(db->allocator, q);
        return kith_error_return(KITH_ENOMEM);
    }
    q->n_params = n_params;
    q->next = db->queries;
    db->queries = q;
    return 0;
}

static void registry_free(kith_db_t *db)
{
    struct kith_db_query *q = db->queries;
    while (q != NULL)
    {
        struct kith_db_query *next = q->next;
        kith_free(db->allocator, q->name);
        kith_free(db->allocator, q->sql);
        kith_free(db->allocator, q);
        q = next;
    }
    db->queries = NULL;
}

// ---------------------------------------------------------------------------
// pool selection — find a slot for a new command (reactor thread only)
// ---------------------------------------------------------------------------
// Every path leaves the command in flight or fires its callback; the return
// value is diagnostic only.
static int pool_submit(kith_db_t *db, struct kith_db_cmd *cmd)
{
    // 1. round-robin scan for a READY connection (pinned slots excluded:
    //    they belong to sessions)
    for (uint32_t i = 0u; i < db->max_connections; i++)
    {
        uint32_t idx = (db->next_conn + i) % db->max_connections;
        struct kith_db_conn *c = &db->conns[idx];
        if (c->state == KITH_DB_CONN_READY && c->session == NULL)
        {
            db->next_conn = (idx + 1u) % db->max_connections;
            return conn_submit_immediate(c, cmd);
        }
    }

    // 2. attach to a CONNECTING connection that has no pending cmd
    for (uint32_t i = 0u; i < db->max_connections; i++)
    {
        struct kith_db_conn *c = &db->conns[i];
        if (c->state == KITH_DB_CONN_CONNECTING && c->cmd == NULL && c->session == NULL)
        {
            c->cmd = cmd;
            return 0;
        }
    }

    // 3. start a fresh connect on a DISCONNECTED slot and attach the cmd
    for (uint32_t i = 0u; i < db->max_connections; i++)
    {
        struct kith_db_conn *c = &db->conns[i];
        if (c->state == KITH_DB_CONN_DISCONNECTED && c->session == NULL)
        {
            c->cmd = cmd;
            int rc = conn_start(c, db);
            if (rc != 0)
            {
                c->cmd = NULL;
                cmd_fail(cmd, rc, NULL);
                return rc;
            }
            return 0;
        }
    }

    // 4. every slot is busy, connecting-with-a-pending-cmd, or pinned
    cmd_fail(cmd, kith_error_return(KITH_EBUSY), "pool has no available connection");
    return kith_error_return(KITH_EBUSY);
}

// ---------------------------------------------------------------------------
// submission task — runs the pool selection on the reactor thread
// ---------------------------------------------------------------------------
static void db_exec_task(void *ctx)
{
    struct kith_db_cmd *cmd = ctx;
    // Every pool_submit path leaves the command in flight or fires its
    // callback; the return value is diagnostic.
    (void)pool_submit(cmd->db, cmd);
}

// ---------------------------------------------------------------------------
// establishment task — connects the eager pool slots on the reactor thread
// ---------------------------------------------------------------------------
// Socket registration must not race kith_reactor_run's fd table, so the
// eager loop rides the task queue instead of running on the creating
// caller's thread.
static void db_eager_connect_task(void *ctx)
{
    kith_db_t *db = ctx;
    for (uint32_t i = 0u; i < db->min_connections; i++)
    {
        (void)conn_start(&db->conns[i], db);
    }
}

// ---------------------------------------------------------------------------
// password helpers — the handle owns a scrubbed copy of the caller's secret
// ---------------------------------------------------------------------------
// The copy is a plain NUL-terminated C string that nothing mutates until the
// scrub, so strlen at scrub time still reports its full length.
static void password_scrub(char *password)
{
    if (password == NULL)
    {
        return;
    }
    size_t len = strlen(password) + 1u;
    // The scrub writes through volatile stores, which the compiler
    // cannot elide; memset_explicit is not referenced because doing so
    // pins the binary's install floor to the glibc that introduced the
    // symbol, above the documented 2.38 floor.
    volatile unsigned char *p = (volatile unsigned char *)password;
    for (size_t i = 0u; i < len; i++)
    {
        p[i] = 0u;
    }
}

static int string_copy(const kith_allocator_t *alloc, char **out_copy, const char *src)
{
    if (src == NULL)
    {
        *out_copy = NULL;
        return 0;
    }
    size_t alloc_len;
    if (ckd_add(&alloc_len, strlen(src), 1u))
    {
        return kith_error_return(KITH_ERANGE);
    }
    char *copy = kith_alloc(alloc, alloc_len);
    if (copy == NULL)
    {
        return kith_error_return(KITH_ENOMEM);
    }
    memcpy(copy, src, alloc_len);
    *out_copy = copy;
    return 0;
}

static void conn_strings_free(const kith_allocator_t *allocator, struct kith_db *db)
{
    password_scrub(db->password);
    kith_free(allocator, db->password);
    kith_free(allocator, db->user);
    kith_free(allocator, db->db_name);
    kith_free(allocator, db->host);
}

static int conn_strings_copy(const kith_allocator_t *allocator,
                             struct kith_db *db,
                             const kith_db_params_t *params)
{
    int rc = string_copy(allocator, &db->host, params->host);
    if (rc == 0)
    {
        rc = string_copy(allocator, &db->db_name, params->db_name);
    }
    if (rc == 0)
    {
        rc = string_copy(allocator, &db->user, params->user);
    }
    if (rc == 0)
    {
        rc = string_copy(allocator, &db->password, params->password);
    }
    if (rc != 0)
    {
        conn_strings_free(allocator, db);
    }
    return rc;
}

// ---------------------------------------------------------------------------
// create-argument validation
// ---------------------------------------------------------------------------
static int db_check_create_args(const kith_db_params_t *params, const kith_allocator_t *alloc)
{
    if (params->size < sizeof(kith_db_params_t))
    {
        return kith_error_return(KITH_ESIZE);
    }
    if (params->abi_version != KITH_ABI_VERSION)
    {
        return kith_error_return(KITH_EABIVER);
    }
    if (alloc != NULL)
    {
        const kith_error_t alloc_rc = kith_allocator_check(alloc);
        if (alloc_rc != 0)
        {
            return kith_error_return(alloc_rc);
        }
    }
    return 0;
}

// ---------------------------------------------------------------------------
// lifecycle
// ---------------------------------------------------------------------------
[[nodiscard]] KITH_API int kith_db_create(const kith_db_params_t *params,
                                          kith_reactor_t *reactor,
                                          const kith_allocator_t *alloc,
                                          kith_db_t **out_db)
{
    if (params == NULL || reactor == NULL || out_db == NULL)
    {
        return kith_error_return(KITH_EINVAL);
    }
    *out_db = NULL;

    const int check_rc = db_check_create_args(params, alloc);
    if (check_rc != 0)
    {
        return check_rc;
    }
    const kith_allocator_t *allocator = (alloc != NULL) ? alloc : kith_allocator_default();

    uint32_t min_c =
        (params->min_connections != 0u) ? params->min_connections : KITH_DB_DEFAULT_MIN_CONNECTIONS;
    uint32_t max_c =
        (params->max_connections != 0u) ? params->max_connections : KITH_DB_DEFAULT_MAX_CONNECTIONS;
    if (min_c > max_c)
    {
        return kith_error_return(KITH_ERANGE);
    }

    kith_db_t *db = kith_alloc_zero(allocator, 1, sizeof(*db));
    if (db == NULL)
    {
        return kith_error_return(KITH_ENOMEM);
    }
    db->allocator = allocator;
    db->reactor = reactor;
    db->max_connections = max_c;
    db->min_connections = min_c;
    db->next_conn = 0u;
    db->port = params->port;
    db->connect_timeout_ms = params->connect_timeout_ms;
    db->queries = NULL;
    const int copy_rc = conn_strings_copy(allocator, db, params);
    if (copy_rc != 0)
    {
        kith_free(allocator, db);
        return copy_rc;
    }

    db->conns = kith_alloc_zero(allocator, max_c, sizeof(struct kith_db_conn));
    if (db->conns == NULL)
    {
        conn_strings_free(allocator, db);
        kith_free(allocator, db);
        return kith_error_return(KITH_ENOMEM);
    }

    for (uint32_t i = 0u; i < max_c; i++)
    {
        db->conns[i].pg = NULL;
        db->conns[i].state = KITH_DB_CONN_DISCONNECTED;
        db->conns[i].event_mask = 0u;
        db->conns[i].last_fd = -1;
        db->conns[i].cmd = NULL;
        db->conns[i].reactor = reactor;
    }

    // Establish the minimum connections on the reactor thread. Individual
    // connect failures leave a slot DISCONNECTED for lazy reconnect; create
    // still succeeds, and a full task queue defers the whole establishment
    // the same way.
    (void)kith_reactor_submit(db->reactor, db_eager_connect_task, db);

    *out_db = db;
    return 0;
}

KITH_API void kith_db_destroy(kith_db_t *db)
{
    if (db == NULL)
    {
        return;
    }
    for (uint32_t i = 0u; i < db->max_connections; i++)
    {
        struct kith_db_conn *c = &db->conns[i];
        if (c->cmd != NULL)
        {
            cmd_free(c->cmd);
            c->cmd = NULL;
        }
        if (c->pg != NULL)
        {
            conn_teardown(c);
        }
        conn_timers_free(c);
    }
    // Free every open session. A queued release or an in-flight operation
    // at destroy time is the caller-contract violation destroy documents
    // for execs; the sweep keeps the pool's own storage consistent either
    // way.
    struct kith_db_session *s = db->sessions;
    while (s != NULL)
    {
        struct kith_db_session *next = s->next;
        if (s->conn != NULL && s->conn->session == s)
        {
            s->conn->session = NULL;
        }
        kith_free(db->allocator, s);
        s = next;
    }
    db->sessions = NULL;
    conn_strings_free(db->allocator, db);
    kith_free(db->allocator, db->conns);
    registry_free(db);
    kith_free(db->allocator, db);
}

// ---------------------------------------------------------------------------
// query registry public API
// ---------------------------------------------------------------------------
[[nodiscard]] KITH_API int
kith_db_register_query(kith_db_t *db, const char *name, const char *sql, uint32_t n_params)
{
    if (db == NULL || name == NULL || sql == NULL || name[0] == '\0')
    {
        return kith_error_return(KITH_EINVAL);
    }
    return registry_add(db, name, sql, n_params);
}

[[nodiscard]] KITH_API int
kith_db_lookup_query(const kith_db_t *db, const char *name, uint32_t *out_n_params)
{
    if (db == NULL || name == NULL || out_n_params == NULL)
    {
        return kith_error_return(KITH_EINVAL);
    }
    struct kith_db_query *q = registry_lookup(db, name);
    if (q == NULL)
    {
        return kith_error_return(KITH_ENOENT);
    }
    *out_n_params = q->n_params;
    return 0;
}

// ---------------------------------------------------------------------------
// execution
// ---------------------------------------------------------------------------
[[nodiscard]] KITH_API int kith_db_exec(kith_db_t *db,
                                        const char *query_name,
                                        const char *const *params,
                                        uint32_t n_params,
                                        kith_db_reply_fn callback,
                                        void *user_data)
{
    if (db == NULL || query_name == NULL || callback == NULL)
    {
        return kith_error_return(KITH_EINVAL);
    }
    struct kith_db_query *q = registry_lookup(db, query_name);
    if (q == NULL)
    {
        return kith_error_return(KITH_ENOENT);
    }
    if (n_params != q->n_params)
    {
        return kith_error_return(KITH_ERANGE);
    }
    if (n_params != 0u && params == NULL)
    {
        return kith_error_return(KITH_EINVAL);
    }

    struct kith_db_cmd *cmd = cmd_alloc(db, q->sql, n_params, params, callback, user_data);
    if (cmd == NULL)
    {
        return kith_error_return(KITH_ENOMEM);
    }
    if (kith_reactor_submit(db->reactor, db_exec_task, cmd) != 0)
    {
        cmd_free(cmd);
        return kith_error_return(KITH_EBUSY);
    }
    return 0;
}

[[nodiscard]] KITH_API int kith_db_exec_sql(kith_db_t *db,
                                            const char *sql,
                                            const char *const *params,
                                            uint32_t n_params,
                                            kith_db_reply_fn callback,
                                            void *user_data)
{
    if (db == NULL || sql == NULL || callback == NULL)
    {
        return kith_error_return(KITH_EINVAL);
    }
    if (n_params != 0u && params == NULL)
    {
        return kith_error_return(KITH_EINVAL);
    }

    struct kith_db_cmd *cmd = cmd_alloc(db, sql, n_params, params, callback, user_data);
    if (cmd == NULL)
    {
        return kith_error_return(KITH_ENOMEM);
    }
    cmd->sql_owned = kith_strdup(db->allocator, sql);
    if (cmd->sql_owned == NULL)
    {
        cmd_free(cmd);
        return kith_error_return(KITH_ENOMEM);
    }
    cmd->sql = cmd->sql_owned;
    if (kith_reactor_submit(db->reactor, db_exec_task, cmd) != 0)
    {
        cmd_free(cmd);
        return kith_error_return(KITH_EBUSY);
    }
    return 0;
}

// ---------------------------------------------------------------------------
// sessions — a connection pinned out of the pool for a caller's exclusive use
// ---------------------------------------------------------------------------
// Session state mutations and pinned-connection selection run on the reactor
// thread, serialized through the same task queue as the pool's exec path; the
// public calls only validate and submit.

static struct kith_db_session *session_pin(kith_db_t *db, struct kith_db_conn *c)
{
    struct kith_db_session *s = kith_alloc_zero(db->allocator, 1, sizeof(*s));
    if (s == NULL)
    {
        return NULL;
    }
    s->db = db;
    s->conn = c;
    s->generation = c->generation;
    s->next = db->sessions;
    db->sessions = s;
    c->session = s;
    return s;
}

static void session_unpin_free(struct kith_db_session *s)
{
    if (s->conn != NULL && s->conn->session == s)
    {
        s->conn->session = NULL;
    }
    struct kith_db_session **link = &s->db->sessions;
    while (*link != NULL && *link != s)
    {
        link = &(*link)->next;
    }
    if (*link != NULL)
    {
        *link = s->next;
    }
    kith_free(s->db->allocator, s);
}

void session_open_deliver(struct kith_db_conn *c)
{
    struct kith_db_session *s = c->session;
    if (s == NULL || !s->open_pending)
    {
        return;
    }
    s->open_pending = false;
    s->open_cb(s, 0, s->open_user_data);
}

void session_open_fail(struct kith_db_conn *c, int status)
{
    struct kith_db_session *s = c->session;
    if (s == NULL || !s->open_pending)
    {
        return;
    }
    s->open_pending = false;
    s->open_cb(NULL, status, s->open_user_data);
    session_unpin_free(s);
}

// Deliver an open through the request that carried it: the connection was
// already usable at selection time, so the session completes immediately.
static void
session_open_ready(kith_db_t *db, struct kith_db_conn *c, struct kith_db_session_open_req *req)
{
    db->next_conn = (uint32_t)(c - db->conns) + 1u;
    if (db->next_conn == db->max_connections)
    {
        db->next_conn = 0u;
    }
    struct kith_db_session *s = session_pin(db, c);
    if (s == NULL)
    {
        req->callback(NULL, kith_error_return(KITH_ENOMEM), req->user_data);
        return;
    }
    req->callback(s, 0, req->user_data);
}

// Pin a connecting slot and park the open on it; the open completes through
// the connection's own transitions (session_open_deliver / session_open_fail).
static void
session_pin_connecting(kith_db_t *db, struct kith_db_conn *c, struct kith_db_session_open_req *req)
{
    struct kith_db_session *s = session_pin(db, c);
    if (s != NULL)
    {
        s->open_pending = true;
        s->open_cb = req->callback;
        s->open_user_data = req->user_data;
    }
    else
    {
        req->callback(NULL, kith_error_return(KITH_ENOMEM), req->user_data);
    }
}

// Pin a disconnected slot and start its connect; the open completes through
// the connection's own transitions (session_open_deliver / session_open_fail).
static int
session_open_connect(kith_db_t *db, struct kith_db_conn *c, struct kith_db_session_open_req *req)
{
    struct kith_db_session *s = session_pin(db, c);
    if (s == NULL)
    {
        return kith_error_return(KITH_ENOMEM);
    }
    s->open_pending = true;
    s->open_cb = req->callback;
    s->open_user_data = req->user_data;
    int rc = conn_start(c, db);
    if (rc != 0)
    {
        session_unpin_free(s);
    }
    return rc;
}

static void db_session_open_task(void *ctx)
{
    struct kith_db_session_open_req *req = ctx;
    kith_db_t *db = req->db;
    struct kith_db_conn *c = NULL;
    for (uint32_t i = 0u; i < db->max_connections && c == NULL; i++)
    {
        uint32_t idx = (db->next_conn + i) % db->max_connections;
        struct kith_db_conn *scan = &db->conns[idx];
        if (scan->state == KITH_DB_CONN_READY && scan->cmd == NULL && scan->session == NULL)
        {
            c = scan;
        }
    }
    if (c != NULL)
    {
        session_open_ready(db, c, req);
        kith_free(db->allocator, req);
        return;
    }

    // A connecting slot without a pending cmd: pin it and let the connect's
    // own completion deliver the open — the boot shape, where the eager
    // establishment is still in flight when the first open arrives.
    for (uint32_t i = 0u; i < db->max_connections && c == NULL; i++)
    {
        struct kith_db_conn *scan = &db->conns[i];
        if (scan->state == KITH_DB_CONN_CONNECTING && scan->cmd == NULL && scan->session == NULL)
        {
            c = scan;
        }
    }
    if (c != NULL)
    {
        session_pin_connecting(db, c, req);
        kith_free(db->allocator, req);
        return;
    }

    for (uint32_t i = 0u; i < db->max_connections && c == NULL; i++)
    {
        struct kith_db_conn *scan = &db->conns[i];
        if (scan->state == KITH_DB_CONN_DISCONNECTED && scan->cmd == NULL && scan->session == NULL)
        {
            c = scan;
        }
    }
    if (c == NULL)
    {
        req->callback(NULL, kith_error_return(KITH_EBUSY), req->user_data);
    }
    else
    {
        int rc = session_open_connect(db, c, req);
        if (rc != 0)
        {
            req->callback(NULL, rc, req->user_data);
        }
    }
    kith_free(db->allocator, req);
}

static void db_session_exec_task(void *ctx)
{
    struct kith_db_cmd *cmd = ctx;
    struct kith_db_session *s = cmd->session;
    struct kith_db_conn *c = s->conn;
    if (c->cmd != NULL)
    {
        cmd_fail(cmd, kith_error_return(KITH_EBUSY), "session operation already in flight");
        return;
    }
    if (c->generation != s->generation || c->state != KITH_DB_CONN_READY)
    {
        cmd_fail(cmd, kith_error_return(KITH_ECONNRESET), "session connection lost");
        return;
    }
    (void)conn_submit_immediate(c, cmd);
}

static void db_session_close_task(void *ctx)
{
    struct kith_db_session_close_req *req = ctx;
    struct kith_db_session *s = req->session;
    const kith_allocator_t *alloc = s->db->allocator;
    if (s->conn != NULL && s->conn->cmd != NULL)
    {
        // An operation is in flight; the session stays open for a retry.
        req->callback(kith_error_return(KITH_EBUSY), req->user_data);
    }
    else
    {
        session_unpin_free(s);
        req->callback(0, req->user_data);
    }
    kith_free(alloc, req);
}

[[nodiscard]] KITH_API int
kith_db_session_open(kith_db_t *db, kith_db_session_open_fn callback, void *user_data)
{
    if (db == NULL || callback == NULL)
    {
        return kith_error_return(KITH_EINVAL);
    }
    struct kith_db_session_open_req *req = kith_alloc_zero(db->allocator, 1, sizeof(*req));
    if (req == NULL)
    {
        return kith_error_return(KITH_ENOMEM);
    }
    req->db = db;
    req->callback = callback;
    req->user_data = user_data;
    if (kith_reactor_submit(db->reactor, db_session_open_task, req) != 0)
    {
        kith_free(db->allocator, req);
        return kith_error_return(KITH_EBUSY);
    }
    return 0;
}

[[nodiscard]] KITH_API int kith_db_session_exec(kith_db_session_t *session,
                                                const char *sql,
                                                const char *const *params,
                                                uint32_t n_params,
                                                kith_db_reply_fn callback,
                                                void *user_data)
{
    if (session == NULL || sql == NULL || callback == NULL)
    {
        return kith_error_return(KITH_EINVAL);
    }
    if (n_params != 0u && params == NULL)
    {
        return kith_error_return(KITH_EINVAL);
    }

    struct kith_db_cmd *cmd = cmd_alloc(session->db, sql, n_params, params, callback, user_data);
    if (cmd == NULL)
    {
        return kith_error_return(KITH_ENOMEM);
    }
    cmd->session = session;
    cmd->sql_owned = kith_strdup(session->db->allocator, sql);
    if (cmd->sql_owned == NULL)
    {
        cmd_free(cmd);
        return kith_error_return(KITH_ENOMEM);
    }
    cmd->sql = cmd->sql_owned;
    if (kith_reactor_submit(session->db->reactor, db_session_exec_task, cmd) != 0)
    {
        cmd_free(cmd);
        return kith_error_return(KITH_EBUSY);
    }
    return 0;
}

[[nodiscard]] KITH_API int kith_db_session_close(kith_db_session_t *session,
                                                 kith_db_session_close_fn callback,
                                                 void *user_data)
{
    if (session == NULL || callback == NULL)
    {
        return kith_error_return(KITH_EINVAL);
    }
    struct kith_db_session_close_req *req =
        kith_alloc_zero(session->db->allocator, 1, sizeof(*req));
    if (req == NULL)
    {
        return kith_error_return(KITH_ENOMEM);
    }
    req->session = session;
    req->callback = callback;
    req->user_data = user_data;
    if (kith_reactor_submit(session->db->reactor, db_session_close_task, req) != 0)
    {
        kith_free(session->db->allocator, req);
        return kith_error_return(KITH_EBUSY);
    }
    return 0;
}

// ---------------------------------------------------------------------------
// reply accessors
// ---------------------------------------------------------------------------
KITH_API const char *
kith_db_reply_value(const kith_db_reply_t *reply, uint32_t row, uint32_t col, size_t *out_len)
{
    if (reply == NULL || reply->result == NULL || row >= reply->n_rows || col >= reply->n_cols)
    {
        if (out_len != NULL)
        {
            *out_len = 0u;
        }
        return NULL;
    }
    PGresult *res = reply->result;
    if (PQgetisnull(res, (int)row, (int)col) != 0)
    {
        if (out_len != NULL)
        {
            *out_len = 0u;
        }
        return NULL;
    }
    if (out_len != NULL)
    {
        *out_len = (size_t)PQgetlength(res, (int)row, (int)col);
    }
    return PQgetvalue(res, (int)row, (int)col);
}

KITH_API bool kith_db_reply_is_null(const kith_db_reply_t *reply, uint32_t row, uint32_t col)
{
    if (reply == NULL || reply->result == NULL || row >= reply->n_rows || col >= reply->n_cols)
    {
        return false;
    }
    return PQgetisnull(reply->result, (int)row, (int)col) != 0;
}

KITH_API int kith_db_reply_status(const kith_db_reply_t *reply)
{
    return (reply != NULL) ? reply->status : 0;
}

KITH_API const char *kith_db_reply_err_str(const kith_db_reply_t *reply)
{
    return (reply != NULL) ? reply->err_str : NULL;
}

KITH_API uint32_t kith_db_reply_n_rows(const kith_db_reply_t *reply)
{
    return (reply != NULL) ? reply->n_rows : 0u;
}

KITH_API uint32_t kith_db_reply_n_cols(const kith_db_reply_t *reply)
{
    return (reply != NULL) ? reply->n_cols : 0u;
}

KITH_API void *kith_db_reply_user_data(const kith_db_reply_t *reply)
{
    return (reply != NULL) ? reply->user_data : NULL;
}
