#pragma once

#include <stdint.h>

#include <libpq-fe.h>

#include "kith/db/db.h"
#include "kith/reactor/reactor.h"
#include "kith/types.h"

// ---------------------------------------------------------------------------
// reply handle — private layout (opaque from the public header)
// ---------------------------------------------------------------------------
struct kith_db_reply
{
    /** Operation status. 0 on success, negative kith_error on failure. */
    int status;
    /** Human-readable error string when status is non-zero, NULL otherwise. */
    const char *err_str;
    /** Number of rows in the result set. 0 for non-SELECT queries. */
    uint32_t n_rows;
    /** Number of columns per row. 0 for non-SELECT queries. */
    uint32_t n_cols;
    /** Internal libpq result handle used by the reply accessors. */
    PGresult *result;
    /** Caller-supplied correlation pointer echoed from kith_db_exec. */
    void *user_data;
};

// ---------------------------------------------------------------------------
// connection state machine
// ---------------------------------------------------------------------------
enum kith_db_conn_state : unsigned int
{
    KITH_DB_CONN_DISCONNECTED, // no PGconn, available for (re)connect
    KITH_DB_CONN_CONNECTING,   // PQconnectStartParams issued, polling
    KITH_DB_CONN_READY,        // idle, available for a query
    KITH_DB_CONN_BUSY,         // query in flight, flushing or draining result
};

// ---------------------------------------------------------------------------
// query registry entry — name-keyed linked list (registry sizes are small)
// ---------------------------------------------------------------------------
struct kith_db_query
{
    char *name;                 // owned, NUL-terminated
    char *sql;                  // owned, NUL-terminated parameterized SQL
    uint32_t n_params;          // number of $N placeholders in sql
    struct kith_db_query *next; // next entry in the list
};

// ---------------------------------------------------------------------------
// per-command context — carried while a query is pending or in flight
// ---------------------------------------------------------------------------
struct kith_db_cmd
{
    kith_db_reply_fn callback;
    void *user_data;
    struct kith_db *db;              // owning pool (non-owning): the submission task's
                                     // pool and the allocator for this block's storage
    struct kith_db_session *session; // owning session on the session path,
                                     // NULL on the pool paths
    const char *sql;                 // registry entry's SQL, or the owned copy below on
                                     // the raw-SQL path
    char *sql_owned;                 // owned SQL duplicate on the raw-SQL path; the
                                     // submission task may drain after the caller's
                                     // buffer is gone, so caller-supplied SQL is copied
                                     // at call time. NULL on the registry path, whose
                                     // SQL outlives the command in the registry entry.
    uint32_t n_params;
    char **param_copies;             // owned array of owned strings; NULL after submit
};

// ---------------------------------------------------------------------------
// session — a connection pinned out of the pool for a caller's exclusive use
// ---------------------------------------------------------------------------
struct kith_db_session
{
    struct kith_db *db;        // owning pool (non-owning)
    struct kith_db_conn *conn; // pinned connection (non-owning)
    uint32_t generation;       // the conn's generation at pin time; a
                               // mismatch at submit means the conn died
                               // and the slot must not serve the session
    bool open_pending;         // open callback not yet delivered
    kith_db_session_open_fn open_cb;
    void *open_user_data;
    struct kith_db_session *next; // next session in the destroy sweep list
};

// open request — task context carrying the open callback until delivery
struct kith_db_session_open_req
{
    struct kith_db *db;
    kith_db_session_open_fn callback;
    void *user_data;
};

// close request — task context carrying the release callback
struct kith_db_session_close_req
{
    struct kith_db_session *session;
    kith_db_session_close_fn callback;
    void *user_data;
};

// ---------------------------------------------------------------------------
// a single pooled connection
// ---------------------------------------------------------------------------
struct connect_timer_rec; // armed connect-deadline record (conn.c)

struct kith_db_conn
{
    PGconn *pg;
    enum kith_db_conn_state state;
    unsigned int event_mask;          // reactor registration mask (0 = deregistered)
    int last_fd;                      // last fd registered with reactor (-1 if none)
    struct kith_db_cmd *cmd;          // in-flight or pending command (NULL when idle)
    kith_reactor_t *reactor;          // borrowed, for self-registration
    uint32_t generation;              // bumped at teardown; sessions pin a generation
                                      // so a reconnected slot never serves an old
                                      // session's open transaction bracket
    struct kith_db_session *session;  // owning session when pinned, NULL when
                                      // the slot serves the pool round-robin
    struct connect_timer_rec *timers; // armed connect-deadline records, newest
                                      // first; freed by their own firing or by
                                      // kith_db_destroy (the wheel has no
                                      // cancellation)
};

// ---------------------------------------------------------------------------
// db handle
// ---------------------------------------------------------------------------
struct kith_db
{
    const kith_allocator_t *allocator; // resolved at create; every kith-owned
                                       // block allocates and frees through it
    kith_reactor_t *reactor;           // borrowed
    struct kith_db_conn *conns;
    uint32_t max_connections;
    uint32_t min_connections; // eager slots established by the create task
    uint32_t next_conn;       // round-robin cursor for ready selection
    // connection params — owned copies made at kith_db_create, freed at
    // destroy (password scrubbed first)
    char *host;
    uint16_t port;
    char *db_name;
    char *user;
    char *password;
    uint32_t connect_timeout_ms;
    // query registry
    struct kith_db_query *queries;
    // open sessions, newest first — swept by kith_db_destroy
    struct kith_db_session *sessions;
};

// ---------------------------------------------------------------------------
// cmd helpers (db.c) — allocate/free per-command contexts through the owning
// pool's stored allocator
// ---------------------------------------------------------------------------
struct kith_db_cmd *cmd_alloc(struct kith_db *db,
                              const char *sql,
                              uint32_t n_params,
                              const char *const *params,
                              kith_db_reply_fn callback,
                              void *user_data);
void cmd_free_params(struct kith_db_cmd *cmd);
void cmd_free(struct kith_db_cmd *cmd);

// Fire @p cmd's callback with an error reply, then free it. Used by the
// submission paths for failures discovered before the query reaches libpq;
// the caller passes the reply status and an optional error string.
void cmd_fail(struct kith_db_cmd *cmd, int status, const char *err_str);

// ---------------------------------------------------------------------------
// connection state machine (conn.c)
// ---------------------------------------------------------------------------

// Start an asynchronous connect on a disconnected connection slot. Returns 0
// if the connect was initiated (or completed synchronously), negative
// kith_error on immediate failure. When a pending cmd is attached
// (c->cmd != NULL) and the connect completes, the cmd is submitted
// automatically; its callback fires on the reactor thread.
int conn_start(struct kith_db_conn *c, struct kith_db *db);

// Submit a command on a READY connection immediately. Returns 0 on success
// (the callback fires when the query completes) and negative kith_error on
// submission failure; both outcomes fire the callback exactly once — the
// failure paths deliver the error status through cmd_fail. Runs on the
// reactor thread only.
int conn_submit_immediate(struct kith_db_conn *c, struct kith_db_cmd *cmd);

// Tear down a connection: deregister from the reactor, PQfinish, reset state
// to DISCONNECTED. Callers clear any in-flight cmd first; teardown never
// touches it and fires no callback.
void conn_teardown(struct kith_db_conn *c);

// Free every armed connect-deadline record tracked on @p c. kith_db_destroy's
// teardown path: the reactor's timer wheel has no cancellation, so records
// armed past the pool's lifetime are freed here instead.
void conn_timers_free(struct kith_db_conn *c);

// Reactor fd readiness callback — dispatches by connection state. Passed to
// kith_reactor_add as the callback for every pooled connection's socket.
void conn_event_handler(int fd, unsigned int events, void *ctx);

// ---------------------------------------------------------------------------
// session open delivery (db.c) — called from conn.c's connection transitions
// ---------------------------------------------------------------------------

// Deliver a pending session open on a connection that just became usable.
// No-op when the connection holds no session or the open already fired.
void session_open_deliver(struct kith_db_conn *c);

// Fail a pending session open on a connection that died before becoming
// usable: fires the open callback with NULL and @p status, unlinks and
// frees the session. No-op when no pending open exists.
void session_open_fail(struct kith_db_conn *c, int status);
