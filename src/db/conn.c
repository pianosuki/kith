/* libpq connection management for the db module: reactor registration of the
 * Postgres socket, send/recv readiness drive of PQconnectPoll and PQgetResult,
 * and per-command callback firing. Pairs with db.c (handle, command allocator,
 * query registry); the public contract is include/kith/db/db.h. */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "db/db_internal.h"
#include "kith/types.h"

// libpq connect keyword table — order matches the value array built inline.
static const char *const conn_keywords[] = {
    "host",
    "port",
    "dbname",
    "user",
    "password",
    "application_name",
    "connect_timeout",
    NULL,
};

// ---------------------------------------------------------------------------
// helper: register a connection's socket with the reactor for @p events
// ---------------------------------------------------------------------------
// Deregistrations (events == 0 and stale-fd switches) are best-effort: a
// deregistration that misses cannot stall the connection. Arming calls fail
// the connection instead — a mask updated without an armed registration leaves
// the query waiting on events that never fire.
static int conn_register(struct kith_db_conn *c, unsigned int events)
{
    int fd = PQsocket(c->pg);
    if (fd < 0)
    {
        if (events == 0u)
        {
            c->event_mask = 0u;
            c->last_fd = -1;
            return 0;
        }
        return kith_error_return(KITH_EIO); // no socket to arm: unusable connection
    }

    if (c->last_fd >= 0 && fd != c->last_fd)
    {
        (void)kith_reactor_del(c->reactor, c->last_fd);
        c->last_fd = -1;
        c->event_mask = 0u;
    }

    if (events == 0u)
    {
        if (c->event_mask != 0u)
        {
            (void)kith_reactor_del(c->reactor, fd);
            c->event_mask = 0u;
            c->last_fd = -1;
        }
        return 0;
    }

    int rc;
    if (c->event_mask == 0u)
    {
        rc = kith_reactor_add(c->reactor, fd, events, conn_event_handler, c);
    }
    else if (c->event_mask != events)
    {
        rc = kith_reactor_mod(c->reactor, fd, events);
    }
    else
    {
        return 0; // unchanged mask; the registration is already armed
    }
    if (rc != 0)
    {
        return rc;
    }
    c->event_mask = events;
    c->last_fd = fd;
    return 0;
}

// ---------------------------------------------------------------------------
// helper: fire a pending cmd's callback with an error, then free it
// ---------------------------------------------------------------------------
static void conn_fire_error(struct kith_db_conn *c, int status, const char *err_str)
{
    if (c->cmd == NULL)
    {
        return;
    }
    kith_db_reply_t reply = {0};
    reply.status = status;
    reply.err_str = err_str;
    reply.user_data = c->cmd->user_data;
    c->cmd->callback(&reply);
    cmd_free(c->cmd);
    c->cmd = NULL;
}

// ---------------------------------------------------------------------------
// helper: tear down PGconn and reset to DISCONNECTED (drops cmd, no callback)
// ---------------------------------------------------------------------------
void conn_teardown(struct kith_db_conn *c)
{
    if (c->event_mask != 0u && c->last_fd >= 0)
    {
        (void)kith_reactor_del(c->reactor, c->last_fd);
    }
    c->event_mask = 0u;
    c->last_fd = -1;
    if (c->pg != NULL)
    {
        PQfinish(c->pg);
        c->pg = NULL;
    }
    c->state = KITH_DB_CONN_DISCONNECTED;
    // A session pinned to the old connection must never submit onto the
    // reconnected slot: its open transaction bracket died with the backend
    // and the fresh session has none.
    c->generation++;
}

// ---------------------------------------------------------------------------
// helper: fail a connection — fire the pending callback, then tear down
// ---------------------------------------------------------------------------
static void conn_fail(struct kith_db_conn *c, int status, const char *err_str)
{
    session_open_fail(c, status);
    conn_fire_error(c, status, err_str);
    conn_teardown(c);
}

// ---------------------------------------------------------------------------
// connect deadline — per-attempt timer on the reactor's wheel
// ---------------------------------------------------------------------------
// One record per armed attempt, tracked on the connection so kith_db_destroy
// can free records still in the wheel: the wheel has no cancellation, and
// callbacks never fire after the reactor stops.
struct connect_timer_rec
{
    struct kith_db *db;             // owning pool (non-owning): the allocator
    struct kith_db_conn *conn;      // slot the attempt rides (non-owning)
    uint32_t generation;            // the conn's generation at arm time
    struct connect_timer_rec *next; // next armed record on the same slot
};

// Deadline callback: a stale record (teardown already bumped the generation,
// or the slot left CONNECTING) is a no-op that frees itself; a live one fails
// the attempt. Either way the record unlinks from the slot and frees.
static void conn_connect_deadline(void *ctx)
{
    struct connect_timer_rec *rec = ctx;
    struct kith_db_conn *c = rec->conn;
    uint32_t generation = rec->generation;
    struct connect_timer_rec **link = &c->timers;
    while (*link != NULL && *link != rec)
    {
        link = &(*link)->next;
    }
    if (*link != NULL)
    {
        *link = rec->next;
    }
    kith_free(rec->db->allocator, rec);
    if (c->state == KITH_DB_CONN_CONNECTING && c->generation == generation)
    {
        conn_fail(c, kith_error_return(KITH_ECONNRESET), "connect deadline exceeded");
    }
}

// Arm the attempt's deadline on the wheel. The record links onto the slot's
// list before the schedule so a fire cannot outrun the tracking; a schedule
// failure unlinks and frees it, and the caller fails the attempt.
static int conn_arm_deadline(struct kith_db_conn *c, struct kith_db *db, uint32_t timeout_ms)
{
    struct connect_timer_rec *rec = kith_alloc_zero(db->allocator, 1, sizeof(*rec));
    if (rec == NULL)
    {
        return kith_error_return(KITH_ENOMEM);
    }
    rec->db = db;
    rec->conn = c;
    rec->generation = c->generation;
    rec->next = c->timers;
    c->timers = rec;
    int rc = kith_reactor_schedule(
        db->reactor, kith_reactor_now_ms(db->reactor) + timeout_ms, conn_connect_deadline, rec);
    if (rc != 0)
    {
        c->timers = rec->next;
        kith_free(db->allocator, rec);
    }
    return rc;
}

void conn_timers_free(struct kith_db_conn *c)
{
    struct connect_timer_rec *rec = c->timers;
    while (rec != NULL)
    {
        struct connect_timer_rec *next = rec->next;
        kith_free(rec->db->allocator, rec);
        rec = next;
    }
    c->timers = NULL;
}

// ---------------------------------------------------------------------------
// helper: collect the final PGresult and fire the completion callback
// ---------------------------------------------------------------------------
static void conn_finish_result(struct kith_db_conn *c)
{
    PGresult *res = NULL;
    PGresult *last = NULL;
    while ((res = PQgetResult(c->pg)) != NULL)
    {
        PQclear(last);
        last = res;
    }

    kith_db_reply_t reply = {0};
    reply.user_data = c->cmd->user_data;

    if (last == NULL)
    {
        reply.status = kith_error_return(KITH_EIO);
        reply.err_str = "query returned no result";
    }
    else
    {
        ExecStatusType st = PQresultStatus(last);
        if (st == PGRES_TUPLES_OK)
        {
            reply.status = 0;
            reply.n_rows = (uint32_t)PQntuples(last);
            reply.n_cols = (uint32_t)PQnfields(last);
            reply.result = last;
        }
        else if (st == PGRES_COMMAND_OK || st == PGRES_EMPTY_QUERY)
        {
            reply.status = 0;
            reply.result = last;
        }
        else
        {
            reply.status = kith_error_return(KITH_EIO);
            reply.err_str = PQresultErrorMessage(last);
            reply.result = last;
        }
    }

    c->cmd->callback(&reply);
    cmd_free(c->cmd);
    c->cmd = NULL;
    PQclear(last);

    if (PQstatus(c->pg) != CONNECTION_OK)
    {
        conn_teardown(c);
    }
    else
    {
        c->state = KITH_DB_CONN_READY;
        conn_register(c, 0u);
    }
}

// ---------------------------------------------------------------------------
// helper: consume available input; if the query is done, finish the result
// ---------------------------------------------------------------------------
static void conn_query_consume(struct kith_db_conn *c)
{
    if (PQconsumeInput(c->pg) != 1)
    {
        conn_fail(c, kith_error_return(KITH_EIO), PQerrorMessage(c->pg));
        return;
    }
    if (PQisBusy(c->pg) != 0)
    {
        return; // still running, keep waiting for readability
    }
    conn_finish_result(c);
}

// ---------------------------------------------------------------------------
// helper: flush the outbound query buffer; switch to read wait when done
// ---------------------------------------------------------------------------
static void conn_query_flush(struct kith_db_conn *c)
{
    int rc = PQflush(c->pg);
    int reg;
    if (rc == 1)
    {
        reg = conn_register(c, KITH_REACTOR_OUT); // more to flush
    }
    else if (rc == 0)
    {
        reg = conn_register(c, KITH_REACTOR_IN); // flushed, wait for the result
    }
    else
    {
        conn_fail(c, kith_error_return(KITH_EIO), PQerrorMessage(c->pg));
        return;
    }
    if (reg != 0)
    {
        conn_fail(c, reg, "reactor registration failed");
    }
}

// ---------------------------------------------------------------------------
// helper: submit a pending cmd on a freshly-READY connection
// ---------------------------------------------------------------------------
static void conn_submit_pending(struct kith_db_conn *c)
{
    struct kith_db_cmd *cmd = c->cmd;
    int rc = PQsendQueryParams(c->pg,
                               cmd->sql,
                               (int)cmd->n_params,
                               NULL,
                               (const char *const *)cmd->param_copies,
                               NULL,
                               NULL,
                               0);
    cmd_free_params(cmd);
    if (rc != 1)
    {
        conn_fail(c, kith_error_return(KITH_EIO), PQerrorMessage(c->pg));
        return;
    }
    c->state = KITH_DB_CONN_BUSY;
    conn_query_flush(c);
}

// ---------------------------------------------------------------------------
// helper: connect completed — mark READY and submit any pending cmd
// ---------------------------------------------------------------------------
static void conn_on_connect_ok(struct kith_db_conn *c)
{
    c->state = KITH_DB_CONN_READY;
    if (c->cmd != NULL)
    {
        conn_submit_pending(c);
    }
    else
    {
        conn_register(c, 0u); // idle, deregister
    }
    session_open_deliver(c);
}

// ---------------------------------------------------------------------------
// helper: drive one PQconnectPoll step from a reactor readiness callback
// ---------------------------------------------------------------------------
static void conn_connect_poll(struct kith_db_conn *c)
{
    PostgresPollingStatusType st = PQconnectPoll(c->pg);
    int reg;
    switch (st)
    {
        case PGRES_POLLING_READING:
            reg = conn_register(c, KITH_REACTOR_IN);
            break;
        case PGRES_POLLING_WRITING:
            reg = conn_register(c, KITH_REACTOR_OUT);
            break;
        case PGRES_POLLING_OK:
            conn_on_connect_ok(c);
            return;
        case PGRES_POLLING_FAILED:
            conn_fail(c, kith_error_return(KITH_ECONNRESET), PQerrorMessage(c->pg));
            return;
        case PGRES_POLLING_ACTIVE:
            // Intermediate state (unused in modern libpq) — poll either way.
            reg = conn_register(c, KITH_REACTOR_IN | KITH_REACTOR_OUT);
            break;
        default:
            // Unknown polling status: the handshake state is unusable.
            conn_fail(c, kith_error_return(KITH_ECONNRESET), PQerrorMessage(c->pg));
            return;
    }
    if (reg != 0)
    {
        conn_fail(c, reg, "reactor registration failed");
    }
}

// ---------------------------------------------------------------------------
// reactor fd readiness callback — dispatches by connection state
// ---------------------------------------------------------------------------
void conn_event_handler(int fd, unsigned int events, void *ctx)
{
    (void)fd;
    struct kith_db_conn *c = ctx;
    if (c->state == KITH_DB_CONN_CONNECTING)
    {
        conn_connect_poll(c);
    }
    else if (c->state == KITH_DB_CONN_BUSY)
    {
        if ((events & KITH_REACTOR_OUT) != 0u)
        {
            conn_query_flush(c);
        }
        else if ((events & KITH_REACTOR_IN) != 0u)
        {
            conn_query_consume(c);
        }
    }
}

// ---------------------------------------------------------------------------
// start an asynchronous connect on a disconnected slot
// ---------------------------------------------------------------------------

// First poll step of a fresh attempt. The switch mirrors the readiness-path
// poll (conn_connect_poll); an unknown polling status fails the attempt the
// same way. Only the teardown differs — the starter owns it, because a
// pending cmd attached by the submission path belongs to the caller's
// failure branch.
static int conn_poll_first(struct kith_db_conn *c)
{
    PostgresPollingStatusType st = PQconnectPoll(c->pg);
    switch (st)
    {
        case PGRES_POLLING_READING:
            return conn_register(c, KITH_REACTOR_IN);
        case PGRES_POLLING_WRITING:
            return conn_register(c, KITH_REACTOR_OUT);
        case PGRES_POLLING_OK:
            conn_on_connect_ok(c);
            return 0;
        case PGRES_POLLING_FAILED:
            return kith_error_return(KITH_ECONNRESET);
        case PGRES_POLLING_ACTIVE:
            // Intermediate state (unused in modern libpq) — poll either way.
            return conn_register(c, KITH_REACTOR_IN | KITH_REACTOR_OUT);
        default:
            return kith_error_return(KITH_ECONNRESET);
    }
}

int conn_start(struct kith_db_conn *c, struct kith_db *db)
{
    char port_buf[16];
    char timeout_buf[16];
    uint16_t port = (db->port != 0u) ? db->port : (uint16_t)KITH_DB_DEFAULT_PORT;
    uint32_t timeout =
        (db->connect_timeout_ms != 0u) ? db->connect_timeout_ms : KITH_DB_DEFAULT_TIMEOUT_MS;
    uint32_t timeout_s = (timeout + 999u) / 1000u;

    int pn = snprintf(port_buf, sizeof(port_buf), "%u", (unsigned int)port);
    if (pn < 0 || (size_t)pn >= sizeof(port_buf))
    {
        return kith_error_return(KITH_ERANGE);
    }
    int tn = snprintf(timeout_buf, sizeof(timeout_buf), "%u", timeout_s);
    if (tn < 0 || (size_t)tn >= sizeof(timeout_buf))
    {
        return kith_error_return(KITH_ERANGE);
    }

    const char *values[8];
    values[0] = (db->host != NULL) ? db->host : "127.0.0.1";
    values[1] = port_buf;
    values[2] = db->db_name;
    values[3] = db->user;
    values[4] = db->password;
    values[5] = "kith-db";
    values[6] = timeout_buf;
    values[7] = NULL;

    PGconn *pg = PQconnectStartParams(conn_keywords, values, 0);
    if (pg == NULL)
    {
        return kith_error_return(KITH_ENOMEM);
    }
    if (PQstatus(pg) == CONNECTION_BAD)
    {
        PQfinish(pg);
        return kith_error_return(KITH_ECONNRESET);
    }

    c->pg = pg;
    c->reactor = db->reactor;
    c->state = KITH_DB_CONN_CONNECTING;
    c->event_mask = 0u;
    c->last_fd = -1;

    int rc = conn_poll_first(c);
    if (rc != 0)
    {
        // The pending cmd (when pool_submit attached one) belongs to the
        // caller's failure path; teardown only resets the slot.
        conn_teardown(c);
        return rc;
    }
    // The libpq connect keyword is inert on the async path, so the deadline
    // rides the reactor's wheel: libpq documents connect_timeout as ignored
    // under PQconnectPoll, and a handshake-completed-then-silent peer has no
    // kernel ceiling. Slots that left CONNECTING (synchronous completion or
    // failure) arm nothing.
    if (c->state == KITH_DB_CONN_CONNECTING)
    {
        int arm_rc = conn_arm_deadline(c, db, timeout);
        if (arm_rc != 0)
        {
            conn_teardown(c);
            return arm_rc;
        }
    }
    return 0;
}

// ---------------------------------------------------------------------------
// submit a command on a READY connection immediately
// ---------------------------------------------------------------------------
int conn_submit_immediate(struct kith_db_conn *c, struct kith_db_cmd *cmd)
{
    int rc = PQsendQueryParams(c->pg,
                               cmd->sql,
                               (int)cmd->n_params,
                               NULL,
                               (const char *const *)cmd->param_copies,
                               NULL,
                               NULL,
                               0);
    cmd_free_params(cmd);
    if (rc != 1)
    {
        int err = (PQstatus(c->pg) != CONNECTION_OK) ? kith_error_return(KITH_ECONNRESET)
                                                     : kith_error_return(KITH_EIO);
        const char *err_str = PQerrorMessage(c->pg);
        cmd_fail(cmd, err, err_str);
        if (PQstatus(c->pg) != CONNECTION_OK)
        {
            conn_teardown(c);
        }
        return err;
    }
    c->cmd = cmd;
    c->state = KITH_DB_CONN_BUSY;
    conn_query_flush(c);
    return 0;
}
