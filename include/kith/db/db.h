#ifndef KITH_DB_DB_H
#define KITH_DB_DB_H

#include <stddef.h>
#include <stdint.h>

#include "kith/api.h"
#include "kith/types.h"

/**
 * Postgres-backed asynchronous persistence pool with a runtime query
 * registry.
 *
 * A db handle owns a pool of non-blocking Postgres connections (via the
 * libpq async API) and integrates with a reactor for event-driven I/O.
 * The composition root registers named parameterized queries at startup;
 * a generic execute submits a registered query by name and a raw execute
 * submits caller-supplied SQL directly, both with caller-supplied
 * parameter values. The result arrives via a caller-supplied callback
 * invoked from the reactor's event loop.
 *
 * The module knows nothing about the application's schema, tables, or
 * query semantics: every query is a name → SQL string + parameter count
 * entry in the runtime registry, registered by the caller. This keeps the
 * persistence library generic; game-specific SQL lives in the composition
 * root, not in the library.
 *
 * The pool establishes @c min_connections connections asynchronously at
 * create time and grows on demand up to @c max_connections. When every
 * connection is busy and the pool is at its maximum, the completion
 * callback receives a -KITH_EBUSY status and the caller applies backoff on
 * that failure path; the call itself rejects with -KITH_EBUSY only when
 * the reactor's task queue has no free node. Parameter values are
 * text (NUL-terminated strings, with a NULL element denoting SQL NULL).
 *
 * The db handle does not own a reactor — it borrows one from the
 * composition root for the lifetime of the handle. The reactor thread
 * dispatches reply callbacks.
 */

/* Forward declaration — the full type lives in kith/reactor/reactor.h. */
typedef struct kith_reactor kith_reactor_t;

/**
 * @defgroup kith_db Persistence
 * @{
 */

/**
 * Opaque db pool handle.
 *
 * @ownership callee — created by kith_db_create, destroyed by
 *           kith_db_destroy. Owns the connection pool, the query registry,
 *           and the reactor event registrations for each connection.
 */
typedef struct kith_db kith_db_t;

/**
 * Default values used when the corresponding field in
 * @c kith_db_params_t is set to zero.
 */
enum kith_db_default : unsigned int
{
    /** Default Postgres TCP port (params.port = 0 → this). */
    KITH_DB_DEFAULT_PORT = 5432u,
    /** Default connections established eagerly at create (params.min_connections = 0 → this). */
    KITH_DB_DEFAULT_MIN_CONNECTIONS = 1u,
    /** Default pool capacity (params.max_connections = 0 → this). */
    KITH_DB_DEFAULT_MAX_CONNECTIONS = 4u,
    /** Default connect timeout in milliseconds (params.connect_timeout_ms = 0 → this). */
    KITH_DB_DEFAULT_TIMEOUT_MS = 5000u,
};

/**
 * Creation parameters.  Size-versioned: callers set @p size to
 * sizeof(kith_db_params_t) and @p abi_version to KITH_ABI_VERSION at
 * their compile time; the runtime rejects structs from an incompatible
 * generation or an undersized size.  A field set to 0 selects the
 * corresponding @c kith_db_default value (except the string fields —
 * NULL host selects 127.0.0.1, NULL db_name/user/password select the
 * libpq defaults).
 */
struct kith_db_params
{
    /** Must be sizeof(kith_db_params_t). */
    uint32_t size;
    /** Must be KITH_ABI_VERSION (from kith/version.h). */
    uint32_t abi_version;

    /**
     * Postgres server hostname, IP address, or socket directory. NULL
     * selects 127.0.0.1. A numeric address skips name resolution; a
     * hostname resolves during connection establishment on the reactor
     * thread. Copied at create time; the caller may release its buffer
     * once kith_db_create returns. The handle frees its copy at
     * kith_db_destroy.
     */
    const char *host;

    /**
     * Postgres server TCP port. 0 selects KITH_DB_DEFAULT_PORT (5432).
     */
    uint16_t port;

    /**
     * Database name to connect to. NULL selects the libpq default (the
     * username). Copied at create time; the caller may release its buffer
     * once kith_db_create returns. The handle frees its copy at
     * kith_db_destroy.
     */
    const char *db_name;

    /**
     * Postgres role name to authenticate as. NULL selects the libpq
     * default (the process user). Copied at create time; the caller may
     * release its buffer once kith_db_create returns. The handle frees its
     * copy at kith_db_destroy.
     */
    const char *user;

    /**
     * Postgres role password. NULL selects the libpq default (no password
     * supplied, relying on .pgpass or peer auth). Copied at create time;
     * the caller may release its buffer once kith_db_create returns. The
     * handle scrubs and frees its copy at kith_db_destroy.
     */
    const char *password;

    /**
     * Number of connections to establish asynchronously at create time.
     * 0 selects KITH_DB_DEFAULT_MIN_CONNECTIONS (1). Must not exceed
     * @p max_connections.
     */
    uint32_t min_connections;

    /**
     * Maximum number of connections the pool may open. 0 selects
     * KITH_DB_DEFAULT_MAX_CONNECTIONS (4). The pool grows on demand up to
     * this ceiling when @c kith_db_exec finds no ready connection.
     */
    uint32_t max_connections;

    /**
     * Asynchronous connect timeout in milliseconds. 0 selects
     * KITH_DB_DEFAULT_TIMEOUT_MS (5000). Applied to each connection
     * establishment attempt.
     */
    uint32_t connect_timeout_ms;

    /** Reserved for future additive fields. Must be zero-filled. */
    void *reserved[8];
};

/** Alias of struct kith_db_params. */
typedef struct kith_db_params kith_db_params_t;

/**
 * Reply handle delivered to the caller's completion callback.
 *
 * Opaque handle: the layout is private to the db module. Every field is
 * read through an accessor (@c kith_db_reply_status, @c kith_db_reply_err_str,
 * @c kith_db_reply_n_rows, @c kith_db_reply_n_cols, @c kith_db_reply_user_data,
 * @c kith_db_reply_value, @c kith_db_reply_is_null).
 *
 * All pointer-valued accessors (@c kith_db_reply_err_str, and the values
 * returned by @c kith_db_reply_value) are valid only for the duration of
 * the callback. The caller must copy any data it needs to retain beyond
 * the callback.
 *
 * @ownership callee — created by the module on the reactor thread for the
 *           duration of the completion callback; the caller does not own
 *           the handle and must not free it.
 */
typedef struct kith_db_reply kith_db_reply_t;

/**
 * Completion callback invoked by the reactor when a query completes.
 *
 * @param reply The query result. All pointer fields are valid only for
 *              the duration of this call; copy any data that must outlive
 *              the callback.
 * @thread_safety unsafe — called on the reactor's event-loop thread.
 *                 The callback must not block or call back into the
 *                 reactor.
 */
typedef void (*kith_db_reply_fn)(kith_db_reply_t *reply);

/**
 * Opaque session handle: a connection pinned from a db pool for the
 * caller's exclusive use.
 *
 * The session binds one pool connection for its lifetime; statements
 * issued on the session always run on that connection, which is what
 * makes a multi-statement transaction bracket (BEGIN through COMMIT or
 * ROLLBACK) composable across calls. The connection is excluded from the
 * pool's round-robin selection while pinned and returns to it at
 * release. A session whose connection dies reports the loss through its
 * in-flight or next operation's reply status and never inherits a
 * reconnected slot's fresh state.
 *
 * This is the SQL sense of a session — a connection with its statement
 * state — not a network client's session.
 *
 * @ownership callee — delivered by kith_db_session_open, released by
 *           kith_db_session_close (or freed by kith_db_destroy).
 */
typedef struct kith_db_session kith_db_session_t;

/**
 * Open-completion callback invoked by the reactor when a session open
 * completes.
 *
 * @param session    The pinned session, or NULL when the open failed.
 * @param status     0 on success, negative kith_error on failure (the
 *                   pool-saturation -KITH_EBUSY arrives here).
 * @param user_data  The pointer supplied to kith_db_session_open.
 * @thread_safety unsafe — called on the reactor's event-loop thread.
 */
typedef void (*kith_db_session_open_fn)(kith_db_session_t *session, int status, void *user_data);

/**
 * Release-completion callback invoked by the reactor when a session
 * release completes.
 *
 * @param status     0 when the connection returned to the pool;
 *                   -KITH_EBUSY when an operation was still in flight on
 *                   the session (the session stays open; retry).
 * @param user_data  The pointer supplied to kith_db_session_close.
 * @thread_safety unsafe — called on the reactor's event-loop thread.
 */
typedef void (*kith_db_session_close_fn)(int status, void *user_data);

/*---------------------------------------------------------------------------
 * lifecycle
 *-------------------------------------------------------------------------*/

/**
 * Create a db pool handle.
 *
 * Initiates non-blocking Postgres connections via the libpq async API for
 * @c min_connections connections. The establishment loop runs on the
 * reactor thread as a submitted task: a handle created before
 * kith_reactor_run initiates its connections on the first run iteration
 * and completes them as the reactor is pumped; a handle created against a
 * running reactor initiates at the next task drain. Keeping establishment
 * on the reactor thread is what keeps its socket registration from racing
 * kith_reactor_run's file-descriptor table. When the reactor's task queue
 * is full the submission is skipped and every slot stays DISCONNECTED for
 * lazy reconnect on the first query. Queries submitted before the
 * connects complete are queued on a connecting connection and submitted
 * once it is ready.
 *
 * @param params    Creation parameters. Must be non-NULL with a valid
 *                  @p size and @p abi_version.
 * @param reactor   Borrowed reactor handle. Must outlive the db handle.
 *                  Non-NULL.
 * @param alloc     Allocator for the pool handle, the connection table, the
 *                  connection-parameter string copies (host, db_name, user,
 *                  password), the query registry, and every command
 *                  block the operations allocate, used again when
 *                  kith_db_destroy frees them. NULL selects the default
 *                  allocator; a supplied allocator is validated (see
 *                  kith_allocator_t) and must outlive the handle.
 * @param out_db    Receives the new handle on success.
 * @return          0 on success, negative kith_error on failure:
 *                  - -KITH_EINVAL if @p params, @p reactor, or @p out_db
 *                    is NULL, or @p alloc is missing an operation,
 *                  - -KITH_EABIVER if @p params or @p alloc has an
 *                    incompatible abi_version,
 *                  - -KITH_ESIZE if @p params or @p alloc has an
 *                    undersized size,
 *                  - -KITH_ERANGE if @p min_connections exceeds
 *                    @p max_connections, or a connection-parameter string
 *                    length overflows,
 *                  - -KITH_ENOMEM on allocation failure.
 * @thread_safety unsafe — must not race with another kith_db_create on
 *                the same @p out_db slot.
 * @ownership callee — the returned handle is freed by the caller with
 *           kith_db_destroy.
 */
[[nodiscard]] KITH_API int kith_db_create(const kith_db_params_t *params,
                                          kith_reactor_t *reactor,
                                          const kith_allocator_t *alloc,
                                          kith_db_t **out_db);

/**
 * Release all resources held by @p db. Passing NULL is a no-op.
 *
 * Cancels any in-flight queries, deregisters every connection from the
 * reactor, closes the libpq connections, and frees the pool and registry.
 *
 * The pool's eager-establishment task counts as an in-flight operation:
 * while the reactor may still drain it, the handle must outlive the task.
 * Destroying the reactor first drops a queued establishment task without
 * dispatching it. Connect-deadline records are freed with the handle, and
 * the same ordering covers them: after the reactor stops (or the handle is
 * destroyed) no wheel node dispatches into the pool, and the reactor must
 * not run again once the handle is gone.
 *
 * @param db Db handle. NULL is a no-op.
 * @thread_safety unsafe — no operation may be in flight, including one
 *                whose submission task is queued but not yet drained on
 *                the reactor thread, and no callback may be running when
 *                this is called.
 * @ownership callee — @p db is consumed and freed by the call.
 */
KITH_API void kith_db_destroy(kith_db_t *db);

/*---------------------------------------------------------------------------
 * query registry
 *-------------------------------------------------------------------------*/

/**
 * Register a named parameterized query.
 *
 * The query is identified by @p name for subsequent @c kith_db_exec calls.
 * The @p sql string is a PostgreSQL parameterized command using $1, $2, …
 * positional placeholders; @p n_params is the number of placeholders. The
 * module does not parse or validate @p sql; Postgres reports a syntax
 * error at execute time via the reply's @p status / @p err_str.
 *
 * @param db        Db handle. Must be non-NULL.
 * @param name      NUL-terminated query name, non-NULL, non-empty. Copied
 *                  at registration; the caller may free @p name after the
 *                  call.
 * @param sql        NUL-terminated SQL string with @p n_params positional
 *                  placeholders. Copied at registration; the caller may
 *                  free @p sql after the call.
 * @param n_params  Number of positional placeholders in @p sql.
 * @return          0 on success, negative kith_error on failure:
 *                  - -KITH_EINVAL if @p db, @p name, or @p sql is NULL or
 *                    @p name is empty,
 *                  - -KITH_EEXIST if @p name is already registered,
 *                  - -KITH_ENOMEM on allocation failure.
 * @thread_safety unsafe — must not race with kith_db_exec or
 *                kith_db_lookup_query on the same handle.
 * @ownership caller — @p name and @p sql are borrowed for the call only
 *           and copied on success.
 */
[[nodiscard]] KITH_API int
kith_db_register_query(kith_db_t *db, const char *name, const char *sql, uint32_t n_params);

/**
 * Look up the parameter count for a registered query by name.
 *
 * @param db            Db handle. Must be non-NULL.
 * @param name          NUL-terminated query name.
 * @param out_n_params  Receives the parameter count on success.
 * @return              0 on success, negative kith_error on failure:
 *                      - -KITH_EINVAL if @p db or @p name is NULL,
 *                      - -KITH_ENOENT if @p name is not registered.
 * @thread_safety unsafe — must not race with kith_db_register_query.
 * @ownership caller — @p db and @p name are borrowed for the call only;
 *           @p out_n_params is the caller's output storage.
 */
[[nodiscard]] KITH_API int
kith_db_lookup_query(const kith_db_t *db, const char *name, uint32_t *out_n_params);

/*---------------------------------------------------------------------------
 * execution
 *-------------------------------------------------------------------------*/

/**
 * Execute a registered query asynchronously.
 *
 * Selects a ready connection from the pool (round-robin), submits @p
 * params via the libpq async API, and delivers the result through
 * @p callback on the reactor thread. When no connection is ready, the
 * query is queued on a connecting connection or a fresh connect is
 * started; a pool saturated at its ceiling reports -KITH_EBUSY through
 * the callback's status.
 *
 * The query is issued on the reactor thread: the call validates its
 * arguments, copies the parameters, and queues the work. Failures
 * discovered after the call returns — a broken or failed connection, a
 * saturated pool, a submission error on an otherwise-healthy connection
 * — surface as the reply's status rather than the return value.
 *
 * @param db         Db handle. Must be non-NULL.
 * @param query_name Name of a query registered via
 *                   kith_db_register_query. Non-NULL.
 * @param params     Array of @p n_params text parameter values. Each
 *                   element is a NUL-terminated string; a NULL element
 *                   denotes SQL NULL. May be NULL only when
 *                   @p n_params is 0.
 * @param n_params   Number of parameters. Must match the registered
 *                   query's parameter count.
 * @param callback   Completion callback. Non-NULL. Invoked once when the
 *                   query completes or fails; kith_db_destroy cancels
 *                   pending queries without invoking their callbacks.
 * @param user_data  Opaque pointer echoed back via kith_db_reply_user_data.
 * @return           0 if the query was queued for the reactor thread,
 *                   negative kith_error on failure:
 *                   - -KITH_EINVAL if @p db, @p query_name, or
 *                     @p callback is NULL,
 *                   - -KITH_ENOENT if @p query_name is not registered,
 *                   - -KITH_ERANGE if @p n_params does not match the
 *                     registered query's parameter count,
 *                   - -KITH_ENOMEM if the command cannot be allocated,
 *                   - -KITH_EBUSY if the reactor task queue is full.
 * @thread_safety safe-if the db handle is not being destroyed
 *                concurrently. May be called from any thread; callbacks
 *                always fire on the reactor thread.
 * @ownership caller — @p params is borrowed for the call only; @p user_data is
 *           borrowed until the completion callback fires.
 */
[[nodiscard]] KITH_API int kith_db_exec(kith_db_t *db,
                                        const char *query_name,
                                        const char *const *params,
                                        uint32_t n_params,
                                        kith_db_reply_fn callback,
                                        void *user_data);

/**
 * Execute a raw SQL statement asynchronously on a pooled connection.
 *
 * The statement bypasses the query registry: @p sql is submitted directly
 * to a connection selected by the same pool rules as @c kith_db_exec
 * (round-robin over ready connections, queued on a connecting connection,
 * or a fresh connect). Use it for statements that do not warrant a
 * registry entry; named queries keep the registry path. The statement
 * text is copied at call time; the caller may release its buffer once
 * the call returns.
 *
 * The module does not parse or validate @p sql; Postgres reports a
 * syntax error at execute time via the reply's @p status / @p err_str.
 * The statement must be a single SQL command: the parameterized
 * submission rejects multi-statement strings.
 *
 * @param db         Db handle. Must be non-NULL.
 * @param sql        NUL-terminated SQL statement. Non-NULL.
 * @param params     Array of @p n_params text parameter values. Each
 *                   element is a NUL-terminated string; a NULL element
 *                   denotes SQL NULL. May be NULL only when
 *                   @p n_params is 0.
 * @param n_params   Number of parameters. 0 for statements without
 *                   placeholders.
 * @param callback   Completion callback. Non-NULL. Invoked once when the
 *                   statement completes or fails; kith_db_destroy cancels
 *                   pending statements without invoking their callbacks.
 * @param user_data  Opaque pointer echoed back via kith_db_reply_user_data.
 * @return           0 if the statement was queued for the reactor thread,
 *                   negative kith_error on failure:
 *                   - -KITH_EINVAL if @p db, @p sql, or @p callback is
 *                     NULL, or @p params is NULL with @p n_params > 0,
 *                   - -KITH_ENOMEM if the command cannot be allocated,
 *                   - -KITH_EBUSY if the reactor task queue is full.
 * @thread_safety safe-if the db handle is not being destroyed
 *                concurrently. May be called from any thread; callbacks
 *                always fire on the reactor thread.
 * @ownership caller — @p sql and @p params are borrowed for the call only;
 *           @p user_data is borrowed until the completion callback fires.
 */
[[nodiscard]] KITH_API int kith_db_exec_sql(kith_db_t *db,
                                            const char *sql,
                                            const char *const *params,
                                            uint32_t n_params,
                                            kith_db_reply_fn callback,
                                            void *user_data);

/*---------------------------------------------------------------------------
 * sessions
 *-------------------------------------------------------------------------*/

/**
 * Pin a pool connection for exclusive use.
 *
 * Selects a ready idle connection (round-robin), else attaches to a
 * connection already connecting or starts a fresh connect within the
 * pool ceiling, and delivers the session through @p callback once the
 * connection is usable. When the pool is at its ceiling and every
 * connection is busy or pinned, the callback reports -KITH_EBUSY and the
 * caller retries; no connection is established for a refused open. The
 * callback fires exactly once, on the reactor thread: with the session
 * handle and status 0, or with NULL and a negative kith_error.
 *
 * Opening a session does not begin a database transaction; the caller
 * issues BEGIN as a statement.
 *
 * @param db         Db handle. Must be non-NULL.
 * @param callback   Open-completion callback. Non-NULL.
 * @param user_data  Opaque pointer passed to the callback.
 * @return           0 if the open was queued for the reactor thread,
 *                   negative kith_error on failure:
 *                   - -KITH_EINVAL if @p db or @p callback is NULL,
 *                   - -KITH_ENOMEM if the open request cannot be
 *                     allocated,
 *                   - -KITH_EBUSY if the reactor task queue is full.
 * @thread_safety safe-if the db handle is not being destroyed
 *                concurrently. May be called from any thread; the
 *                callback fires on the reactor thread.
 * @ownership callee — the delivered session is released by
 *           kith_db_session_close.
 */
[[nodiscard]] KITH_API int
kith_db_session_open(kith_db_t *db, kith_db_session_open_fn callback, void *user_data);

/**
 * Execute one raw SQL statement on a session's pinned connection.
 *
 * The statement bypasses the query registry and always runs on the
 * session's connection — the property that makes a multi-statement
 * transaction bracket composable. The statement text is copied at call
 * time; the caller may release its buffer once the call returns. The
 * statement must be a single SQL command: the parameterized submission
 * rejects multi-statement strings.
 *
 * One operation runs at a time on a session: submitting while an earlier
 * statement is still in flight reports -KITH_EBUSY through the reply, in
 * place of a result. A session whose connection was lost reports
 * -KITH_ECONNRESET (the connection is gone) or -KITH_EIO (the loss
 * surfaced while the reply was being collected) through the reply; the
 * bracket the session carried is rolled back server-side by the loss
 * itself, and the caller releases the session. The module tracks no
 * transaction state: BEGIN, COMMIT, and ROLLBACK are statements the
 * caller owns, exactly like any other SQL. Statements that bind
 * server-side session state — role or authorization changes, search
 * path, named prepared statements, listen registrations, temporary
 * tables, copy streams, two-phase prepare — outlive the connection's
 * return to the pool or wedge it; they are outside the session contract.
 *
 * @param session    Session handle from kith_db_session_open. Non-NULL.
 * @param sql        NUL-terminated SQL statement. Non-NULL.
 * @param params     Array of @p n_params text parameter values. Each
 *                   element is a NUL-terminated string; a NULL element
 *                   denotes SQL NULL. May be NULL only when
 *                   @p n_params is 0.
 * @param n_params   Number of parameters. 0 for statements without
 *                   placeholders.
 * @param callback   Completion callback. Non-NULL. Invoked once when the
 *                   statement completes or fails; kith_db_destroy cancels
 *                   pending statements without invoking their callbacks.
 * @param user_data  Opaque pointer echoed back via kith_db_reply_user_data.
 * @return           0 if the statement was queued for the reactor thread,
 *                   negative kith_error on failure:
 *                   - -KITH_EINVAL if @p session, @p sql, or @p callback
 *                     is NULL, or @p params is NULL with @p n_params > 0,
 *                   - -KITH_ENOMEM if the command cannot be allocated,
 *                   - -KITH_EBUSY if the reactor task queue is full.
 * @thread_safety safe-if the db handle is not being destroyed
 *                concurrently and no kith_db_session_close for this
 *                session is queued. May be called from any thread;
 *                callbacks always fire on the reactor thread.
 * @ownership caller — @p sql and @p params are borrowed for the call only;
 *           @p user_data is borrowed until the completion callback fires.
 */
[[nodiscard]] KITH_API int kith_db_session_exec(kith_db_session_t *session,
                                                const char *sql,
                                                const char *const *params,
                                                uint32_t n_params,
                                                kith_db_reply_fn callback,
                                                void *user_data);

/**
 * Release a session's pinned connection back to the pool.
 *
 * The release is queued on the reactor thread and serialized with the
 * session's other operations: the callback reports -KITH_EBUSY when an
 * operation is still in flight on the session — the session stays open
 * and the caller retries after its reply — and 0 once the connection is
 * unpinned. After the call returns, the session must not be used; an
 * operation submitted before the close may still be queued and its reply
 * still fires.
 *
 * Releasing a session whose transaction bracket is still open leaves the
 * bracket on the pooled connection for the next user; the caller commits
 * or rolls back before closing.
 *
 * @param session    Session handle. Non-NULL.
 * @param callback   Release-completion callback. Non-NULL.
 * @param user_data  Opaque pointer passed to the callback.
 * @return           0 if the release was queued for the reactor thread,
 *                   negative kith_error on failure:
 *                   - -KITH_EINVAL if @p session or @p callback is NULL,
 *                   - -KITH_ENOMEM if the release request cannot be
 *                     allocated,
 *                   - -KITH_EBUSY if the reactor task queue is full.
 * @thread_safety safe-if the db handle is not being destroyed
 *                concurrently and no operation on this session is being
 *                submitted. May be called from any thread; the callback
 *                fires on the reactor thread.
 * @ownership callee — @p session is consumed by the call (freed when the
 *           release completes).
 */
[[nodiscard]] KITH_API int kith_db_session_close(kith_db_session_t *session,
                                                 kith_db_session_close_fn callback,
                                                 void *user_data);

/*---------------------------------------------------------------------------
 * reply accessors
 *-------------------------------------------------------------------------*/

/**
 * Return the text value of cell (@p row, @p col) from @p reply's result
 * set, or NULL when the cell is SQL NULL or the indices are out of range.
 *
 * Valid only during the completion callback (the underlying storage is
 * freed after the callback returns).
 *
 * @param reply   Reply handle. Must be non-NULL.
 * @param row     Row index in [0, kith_db_reply_n_rows(reply)).
 * @param col     Column index in [0, kith_db_reply_n_cols(reply)).
 * @param out_len Receives the value length in bytes on success (not
 *                counting a NUL terminator). May be NULL.
 * @return        NUL-terminated value pointer (borrowed, valid during the
 *                callback), or NULL for SQL NULL or out-of-range indices.
 * @thread_safety unsafe — call only during the completion callback.
 * @ownership callee — the returned pointer is borrowed from the reply's
 *           internal result set and is freed after the callback returns;
 *           the caller must copy any data it needs to retain.
 */
KITH_API const char *
kith_db_reply_value(const kith_db_reply_t *reply, uint32_t row, uint32_t col, size_t *out_len);

/**
 * Report whether cell (@p row, @p col) is SQL NULL.
 *
 * Valid only during the completion callback. Out-of-range indices report
 * false.
 *
 * @param reply Reply handle. Must be non-NULL.
 * @param row   Row index in [0, kith_db_reply_n_rows(reply)).
 * @param col   Column index in [0, kith_db_reply_n_cols(reply)).
 * @return      True when the cell is SQL NULL; false for out-of-range indices.
 * @thread_safety unsafe — call only during the completion callback.
 * @ownership callee — no ownership transfer; returns a value copy.
 */
KITH_API bool kith_db_reply_is_null(const kith_db_reply_t *reply, uint32_t row, uint32_t col);

/**
 * Return the operation status of @p reply.
 *
 * A query that returns zero rows is not an error: the status is 0 and
 * @c kith_db_reply_n_rows reports 0.
 *
 * @param reply Reply handle. Must be non-NULL.
 * @return      0 on success, negative kith_error on failure:
 *              - -KITH_EIO if the backend rejected the query or the result
 *                could not be collected (details via
 *                @c kith_db_reply_err_str),
 *              - -KITH_ECONNRESET if the backend connection was lost,
 *              - the @c kith_reactor_add/@c kith_reactor_mod return code
 *                when arming the connection for completion events failed.
 * @thread_safety unsafe — call only during the completion callback.
 * @ownership callee — returns a value copy; no ownership transfer.
 */
KITH_API int kith_db_reply_status(const kith_db_reply_t *reply);

/**
 * Return the human-readable error string of @p reply.
 *
 * NULL when the status is 0 (success). Valid only during the completion
 * callback; the underlying storage is freed after the callback returns.
 *
 * @param reply Reply handle. Must be non-NULL.
 * @return      The error string, or NULL when the status is 0.
 * @thread_safety unsafe — call only during the completion callback.
 * @ownership callee — the returned pointer is borrowed from the reply and
 *           is freed after the callback returns; the caller must copy any
 *           data it needs to retain.
 */
KITH_API const char *kith_db_reply_err_str(const kith_db_reply_t *reply);

/**
 * Return the number of rows in @p reply's result set.
 *
 * 0 for non-SELECT queries (commands and empty queries).
 *
 * @param reply Reply handle. Must be non-NULL.
 * @return      The row count; 0 for non-SELECT queries.
 * @thread_safety unsafe — call only during the completion callback.
 * @ownership callee — returns a value copy; no ownership transfer.
 */
KITH_API uint32_t kith_db_reply_n_rows(const kith_db_reply_t *reply);

/**
 * Return the number of columns per row in @p reply's result set.
 *
 * 0 for non-SELECT queries (commands and empty queries).
 *
 * @param reply Reply handle. Must be non-NULL.
 * @return      The column count; 0 for non-SELECT queries.
 * @thread_safety unsafe — call only during the completion callback.
 * @ownership callee — returns a value copy; no ownership transfer.
 */
KITH_API uint32_t kith_db_reply_n_cols(const kith_db_reply_t *reply);

/**
 * Return the @p user_data pointer the caller supplied when the query was
 * issued, for correlating the reply with the caller's own context.
 *
 * @param reply Reply handle. Must be non-NULL.
 * @return      The @p user_data pointer supplied at query submission.
 * @thread_safety unsafe — call only during the completion callback.
 * @ownership caller — the returned pointer is the caller-supplied handle;
 *           ownership is unchanged (the module does not claim or release it).
 */
KITH_API void *kith_db_reply_user_data(const kith_db_reply_t *reply);

/** @} */

#endif /* KITH_DB_DB_H */
