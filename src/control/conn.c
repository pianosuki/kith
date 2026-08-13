/* Connection lifecycle for the control plane HTTP/SSE server: non-blocking
 * socket setup, reactor registration, read buffering, request dispatch to the
 * router, and writev flushes of queued responses. Pairs with http_parser.c
 * (request decoding) and router.c (route matching); the handle and listener
 * live in control.c. */

#include <ctype.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <fcntl.h>
#include <unistd.h>

#include "control/control_internal.h"
#include "kith/types.h"
#include "kith/util/util.h"

#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>

// ---------------------------------------------------------------------------
// forward declarations
// ---------------------------------------------------------------------------
// handle_read dispatches a complete request inline (rather than waiting for
// a separate OUT readiness event), so it needs the responder and writer
// declared before its definition.
static void handle_respond(struct kith_control *ctrl, struct kith_control_conn *conn);
static void handle_write(struct kith_control *ctrl, struct kith_control_conn *conn);

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------
static bool set_nonblocking(int fd)
{
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0)
    {
        return false;
    }
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK) == 0;
}

static void
conn_register(struct kith_control *ctrl, struct kith_control_conn *conn, unsigned int events)
{
    if (conn->fd < 0)
    {
        return;
    }
    (void)kith_reactor_add(ctrl->reactor, conn->fd, events, control_conn_event_handler, conn);
}

static void conn_deregister(struct kith_control *ctrl, struct kith_control_conn *conn)
{
    if (conn->fd >= 0)
    {
        (void)kith_reactor_del(ctrl->reactor, conn->fd);
    }
}

// ---------------------------------------------------------------------------
// case-insensitive ASCII compare (replaces strcasecmp from strings.h)
// ---------------------------------------------------------------------------
static bool ci_equal(const char *a, const char *b, size_t n)
{
    for (size_t i = 0; i < n; i++)
    {
        if (tolower((unsigned char)a[i]) != tolower((unsigned char)b[i]))
        {
            return false;
        }
        if (a[i] == '\0')
        {
            return true;
        }
    }
    return true;
}

static const char *find_header_value(struct kith_control_conn *conn, const char *name)
{
    size_t name_len = strlen(name);
    for (uint32_t i = 0u; i < conn->req_header_count; i++)
    {
        if (ci_equal(conn->req_headers[i].name, name, name_len))
        {
            return conn->req_headers[i].value;
        }
    }
    return NULL;
}

// ---------------------------------------------------------------------------
// allocate a connection slot
// ---------------------------------------------------------------------------
struct kith_control_conn *control_conn_alloc(struct kith_control *ctrl)
{
    // Find the first free slot. The connection table is never compacted: a
    // closed connection's fd is deregistered from the reactor before its
    // slot is freed, so the reactor never holds a callback ctx pointer that
    // moves when another connection closes. The slot's fd stays at -1 until
    // reallocated here.
    for (uint32_t i = 0u; i < ctrl->max_connections; i++)
    {
        struct kith_control_conn *conn = &ctrl->conns[i];
        if (conn->in_use)
        {
            continue;
        }
        memset(conn, 0, sizeof(*conn));
        conn->ctrl = ctrl;
        conn->fd = -1;
        conn->read_buf = kith_alloc(ctrl->allocator, ctrl->read_buffer_cap);
        if (conn->read_buf == NULL)
        {
            return NULL;
        }
        conn->read_cap = ctrl->read_buffer_cap;
        conn->write_buf = kith_alloc(ctrl->allocator, ctrl->write_buffer_cap);
        if (conn->write_buf == NULL)
        {
            kith_free(ctrl->allocator, conn->read_buf);
            conn->read_buf = NULL;
            return NULL;
        }
        conn->write_cap = ctrl->write_buffer_cap;
        conn->header_scratch_cap =
            CONTROL_MAX_HEADERS * (CONTROL_MAX_HEADER_NAME + CONTROL_MAX_HEADER_VALUE);
        conn->header_scratch = kith_alloc(ctrl->allocator, conn->header_scratch_cap);
        if (conn->header_scratch == NULL)
        {
            kith_free(ctrl->allocator, conn->write_buf);
            conn->write_buf = NULL;
            kith_free(ctrl->allocator, conn->read_buf);
            conn->read_buf = NULL;
            return NULL;
        }
        conn->header_scratch_used = 0u;
        conn->resp.buf = conn->write_buf;
        conn->resp.cap = ctrl->write_buffer_cap;
        conn->resp.len = 0u;
        conn->resp.sse = false;
        conn->in_use = true;
        conn->state = KITH_CONTROL_CONN_READING;
        if (i >= ctrl->conn_count)
        {
            ctrl->conn_count = i + 1u;
        }
        return conn;
    }
    return NULL;
}

// ---------------------------------------------------------------------------
// close a connection: deregister, close fd, free buffers, mark slot free
// ---------------------------------------------------------------------------
// When a connection is mid-dispatch on the worker pool (state == DISPATCHING),
// the worker thread aliases this conn's read buffer (request body and header
// strings) and write buffer (response). Freeing the buffers here is a
// use-after-free, and recycling the slot lets a new connection reuse it
// while the worker is still running. In that case the fd is closed and
// deregistered now (so no further reactor events arrive) but the buffer free
// and slot recycle are deferred to control_conn_flush_task, which the worker
// posts after the handler returns.
void control_conn_close(struct kith_control *ctrl, struct kith_control_conn *conn)
{
    (void)ctrl;
    if (conn->fd < 0)
    {
        return;
    }
    conn_deregister(ctrl, conn);
    close(conn->fd);
    conn->fd = -1;
    if (conn->sse_subscribed)
    {
        conn->sse_subscribed = false;
        atomic_fetch_sub_explicit(&ctrl->sse_subscribers, 1u, memory_order_release);
    }

    if (conn->state == KITH_CONTROL_CONN_DISPATCHING)
    {
        return;
    }

    kith_free(ctrl->allocator, conn->read_buf);
    kith_free(ctrl->allocator, conn->write_buf);
    kith_free(ctrl->allocator, conn->header_scratch);
    conn->read_buf = NULL;
    conn->write_buf = NULL;
    conn->header_scratch = NULL;
    conn->resp.buf = NULL;
    conn->in_use = false;
}

// ---------------------------------------------------------------------------
// accept new connections
// ---------------------------------------------------------------------------
static void handle_accept(struct kith_control *ctrl)
{
    while (true)
    {
        struct sockaddr_in client_addr;
        socklen_t addr_len = sizeof(client_addr);
        int client_fd = accept(ctrl->listener_fd, (struct sockaddr *)&client_addr, &addr_len);
        if (client_fd < 0)
        {
            break;
        }

        struct kith_control_conn *conn = control_conn_alloc(ctrl);
        if (conn == NULL)
        {
            close(client_fd);
            continue;
        }

        if (!set_nonblocking(client_fd))
        {
            // The slot was fully allocated above: release it the way a
            // close does (the fd was never registered with the reactor)
            // and leave conn_count alone — it is the slot-scan high-water
            // mark, not a live count.
            close(client_fd);
            kith_free(ctrl->allocator, conn->read_buf);
            kith_free(ctrl->allocator, conn->write_buf);
            kith_free(ctrl->allocator, conn->header_scratch);
            conn->read_buf = NULL;
            conn->write_buf = NULL;
            conn->header_scratch = NULL;
            conn->resp.buf = NULL;
            conn->in_use = false;
            continue;
        }

        int opt = 1;
        (void)setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));

        conn->fd = client_fd;
        conn->state = KITH_CONTROL_CONN_READING;
        conn_register(ctrl, conn, KITH_REACTOR_IN);
    }
}

// ---------------------------------------------------------------------------
// read from a connection
// ---------------------------------------------------------------------------
static void handle_read(struct kith_control *ctrl, struct kith_control_conn *conn)
{
    while (true)
    {
        uint32_t space = conn->read_cap - conn->read_pos;
        if (space == 0u)
        {
            control_conn_close(ctrl, conn);
            return;
        }

        ssize_t n = read(conn->fd, conn->read_buf + conn->read_pos, space);
        if (n < 0)
        {
            break;
        }
        if (n == 0)
        {
            control_conn_close(ctrl, conn);
            return;
        }

        conn->read_pos += (uint32_t)n;

        int rc = control_http_parser_consume(conn);
        if (rc == 0)
        {
            // A complete request is fully buffered: dispatch the route and
            // attempt the response write inline. The connection is registered
            // for IN; OUT is only armed when the write does
            // not complete (EAGAIN), so a request that fits in one read is
            // answered without waiting for a separate readiness event.
            handle_respond(ctrl, conn);
            if (conn->fd >= 0 && conn->state == KITH_CONTROL_CONN_WRITING)
            {
                handle_write(ctrl, conn);
                if (conn->fd >= 0 && conn->state == KITH_CONTROL_CONN_WRITING)
                {
                    // Partial write: arm OUT to drain the remainder.
                    conn_register(ctrl, conn, KITH_REACTOR_IN | KITH_REACTOR_OUT);
                }
            }
            return;
        }
        if (rc < 0)
        {
            control_conn_close(ctrl, conn);
            return;
        }
    }
}

// ---------------------------------------------------------------------------
// write to a connection (drain the write buffer)
// ---------------------------------------------------------------------------
static void handle_write(struct kith_control *ctrl, struct kith_control_conn *conn)
{
    while (conn->write_pos < conn->resp.len)
    {
        ssize_t n =
            write(conn->fd, conn->resp.buf + conn->write_pos, conn->resp.len - conn->write_pos);
        if (n < 0)
        {
            // An interrupted transfer made no progress rather than
            // failing: re-drive it like an EWOULDBLOCK, matching the read
            // side's leave-open treatment of transient errors.
            if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)
            {
                return;
            }
            control_conn_close(ctrl, conn);
            return;
        }
        conn->write_pos += (uint32_t)n;
    }

    if (conn->resp.sse)
    {
        conn->write_pos = 0u;
        conn->resp.len = 0u;
        conn->state = KITH_CONTROL_CONN_SSE;
        if (!conn->sse_subscribed)
        {
            conn->sse_subscribed = true;
            // A new stream starts at the publish head: the bus is a
            // delivery ring, not a history log — records are reclaimed
            // once every attached stream has drained past them, so there
            // is no retained history to replay.
            conn->sse_cursor = atomic_load_explicit(&ctrl->event_bus.head, memory_order_acquire);
            // The count lands after the cursor store: a reader that sees a
            // nonzero count knows every counted stream receives events
            // published after the read.
            atomic_fetch_add_explicit(&ctrl->sse_subscribers, 1u, memory_order_release);
        }
        return;
    }

    control_conn_close(ctrl, conn);
}

// ---------------------------------------------------------------------------
// worker task: run a Python-bound route handler off the reactor thread
// ---------------------------------------------------------------------------
// The reactor thread submits this task (via kith_worker_submit) when a
// Python-flagged route matches and a worker pool is attached.
// The worker invokes the route handler, which fills conn->resp; the worker
// then posts the socket-flush back to the reactor via kith_reactor_submit so
// the write() happens on the reactor thread (the reactor owns the fd and
// the connection's registration).
void control_route_worker_task(void *ctx)
{
    struct control_route_work *work = ctx;
    struct kith_control *ctrl = work->ctrl;
    struct kith_control_conn *conn = work->conn;
    // This task runs only for python-bound routes; a foreign C thread
    // entering a finalizing interpreter has no graceful path there. The
    // connection stays in DISPATCHING and its buffers are freed directly
    // by kith_control_destroy.
    if (kith_python_finalizing())
    {
        (void)kith_metrics_counter_add(
            ctrl->metrics, "kith_control_dispatch_dropped_total", nullptr, 0u, 1u);
        return;
    }
    kith_control_request_t req;
    req.method = conn->req_method;
    req.path = conn->req_path;
    req.headers = conn->req_headers;
    req.header_count = conn->req_header_count;
    req.body = conn->req_body;
    req.body_len = conn->req_body_len;

    conn->resp.len = 0u;
    conn->resp.sse = false;
    // The handler is a Python-bound ctypes trampoline. It runs here on the
    // worker thread, never on the reactor thread.
    (void)work->route->handler(&req, &conn->resp, work->route->ctx);

    // Post the flush back to the reactor. The reactor re-enters WRITING and
    // drains conn->resp.buf. kith_reactor_submit is safe from any thread;
    // the reactor's wake() rouses a blocked poll if necessary.
    (void)kith_reactor_submit(ctrl->reactor, control_conn_flush_task, conn);
}

// ---------------------------------------------------------------------------
// reactor task: flush a connection's response buffer after the worker fills it
// ---------------------------------------------------------------------------
void control_conn_flush_task(void *ctx)
{
    struct kith_control_conn *conn = ctx;
    struct kith_control *ctrl = conn->ctrl;

    // A stale flush (the conn slot was already reused for a new connection, or
    // the flush already ran) is detected by the dispatch state: the reactor
    // sets DISPATCHING before submitting, and clears it below before writing.
    if (conn->state != KITH_CONTROL_CONN_DISPATCHING)
    {
        return;
    }

    // The worker has finished: it posted this task after the handler returned,
    // so the handler is done with the conn's buffers. Either the connection is
    // still open (flush the response to the socket) or it was closed (HUP/ERR)
    // while the worker was running; in the latter case control_conn_close
    // deferred the buffer free and slot recycle to here.
    if (conn->fd < 0)
    {
        kith_free(ctrl->allocator, conn->read_buf);
        kith_free(ctrl->allocator, conn->write_buf);
        kith_free(ctrl->allocator, conn->header_scratch);
        conn->read_buf = NULL;
        conn->write_buf = NULL;
        conn->header_scratch = NULL;
        conn->resp.buf = NULL;
        conn->in_use = false;
        conn->state = KITH_CONTROL_CONN_READING;
        return;
    }

    conn->state = KITH_CONTROL_CONN_WRITING;
    conn->write_pos = 0u;
    handle_write(ctrl, conn);
    if (conn->fd >= 0 && conn->state == KITH_CONTROL_CONN_WRITING)
    {
        // Partial write: arm OUT to drain the remainder. The connection is
        // already registered for IN; adding OUT keeps both.
        conn_register(ctrl, conn, KITH_REACTOR_IN | KITH_REACTOR_OUT);
    }
}

// ---------------------------------------------------------------------------
// dispatch a parsed request via the router
// ---------------------------------------------------------------------------
static void handle_respond(struct kith_control *ctrl, struct kith_control_conn *conn)
{
    conn->resp.len = 0u;
    conn->resp.sse = false;
    conn->write_pos = 0u;

    // An upgrade to the WebSocket protocol requests frame-by-frame
    // servicing the control plane does not implement; refuse it rather
    // than accepting the handshake.
    const char *upgrade = find_header_value(conn, "upgrade");
    if (upgrade != NULL && ci_equal(upgrade, "websocket", 10u))
    {
        control_router_write_501(conn);
        conn->state = KITH_CONTROL_CONN_WRITING;
        return;
    }

    int handler_rc = 0;
    int rc = control_router_dispatch(ctrl, conn, &handler_rc);
    if (rc == 1)
    {
        // Python-bound route deferred to the worker pool. The worker runs the
        // handler and posts control_conn_flush_task back to the reactor; the
        // reactor re-enters WRITING and flushes the socket on its own thread.
        // Arm nothing here: the connection stays registered for IN only, and
        // HUP/ERR events still close it while the worker is in flight.
        conn->state = KITH_CONTROL_CONN_DISPATCHING;
        conn->work.ctrl = ctrl;
        conn->work.conn = conn;
        conn->work.route = control_router_match(ctrl, conn);
        if (ctrl->workers != NULL &&
            kith_worker_submit(ctrl->workers, control_route_worker_task, &conn->work) == 0)
        {
            return;
        }
        // No pool, or the pool's queue is exhausted: the request cannot run
        // off the reactor thread and the reactor never enters the
        // interpreter. Answer 503 and count the drop; the caller's pool
        // sizing is the tuning knob for saturation.
        control_router_write_503(conn);
        (void)kith_metrics_counter_add(
            ctrl->metrics, "kith_control_dispatch_dropped_total", nullptr, 0u, 1u);
    }
    if (handler_rc != 0)
    {
        // The inline handler rejected the request: the response it wrote is
        // discarded and the connection closes with nothing on the wire. The
        // connection never left READING here, so the close frees the buffers
        // and recycles the slot immediately — no worker aliases them.
        control_conn_close(ctrl, conn);
        return;
    }
    conn->state = KITH_CONTROL_CONN_WRITING;
}

// ---------------------------------------------------------------------------
// reactor fd readiness callback for per-connection sockets
// ---------------------------------------------------------------------------
void control_conn_event_handler(int fd, unsigned int events, void *ctx)
{
    (void)fd;
    struct kith_control_conn *conn = ctx;
    struct kith_control *ctrl = conn->ctrl;

    // ERR is a hard socket fault: close immediately, even mid-dispatch (the
    // flush task's write fails with EPIPE anyway; the deferred-close
    // path in control_conn_close keeps the worker's buffers live until the
    // flush task frees them).
    if ((events & KITH_REACTOR_ERR) != 0u)
    {
        control_conn_close(ctrl, conn);
        return;
    }

    // A HUP reports the peer closed its read side. During a Python dispatch
    // the conn's response is in flight on the worker pool; the io_uring
    // re-armed poll fires during the dispatch window and a HUP here
    // abandons the in-flight response (and the write side may still be open,
    // so the response is still deliverable). Ignore HUP while DISPATCHING:
    // the flush task writes the response when the worker posts it back, and
    // closes on completion or on EPIPE (the write-side peer-closed case).
    if ((events & KITH_REACTOR_HUP) != 0u && conn->state != KITH_CONTROL_CONN_DISPATCHING)
    {
        control_conn_close(ctrl, conn);
        return;
    }

    if ((events & KITH_REACTOR_IN) != 0u)
    {
        if (conn->state == KITH_CONTROL_CONN_READING)
        {
            handle_read(ctrl, conn);
        }
    }

    if (conn->fd < 0)
    {
        return;
    }

    if ((events & KITH_REACTOR_OUT) != 0u)
    {
        if (conn->state == KITH_CONTROL_CONN_RESPONDING)
        {
            handle_respond(ctrl, conn);
        }
        if (conn->fd >= 0 && conn->state == KITH_CONTROL_CONN_WRITING)
        {
            handle_write(ctrl, conn);
        }
    }
}

// ---------------------------------------------------------------------------
// reactor fd readiness callback for the listener socket
// ---------------------------------------------------------------------------
void control_listener_event_handler(int fd, unsigned int events, void *ctx)
{
    (void)fd;
    (void)events;
    struct kith_control *ctrl = ctx;
    handle_accept(ctrl);
}

// Reclaim ring space once the deliveries are accounted: the oldest record
// any attached stream still needs is the minimum live cursor, and with no
// subscribers the whole ring up to the head is reclaimable. A stream that
// stalls longer than one ring capacity of published events pins the tail
// and forces publish drops until it drains.
static void control_bus_reclaim(struct kith_control *ctrl)
{
    const uint32_t capacity = ctrl->event_bus.capacity;
    const uint32_t tail = atomic_load_explicit(&ctrl->event_bus.tail, memory_order_acquire);
    uint32_t min_cursor = atomic_load_explicit(&ctrl->event_bus.head, memory_order_acquire);
    uint32_t min_dist = 0u;
    bool have_subscriber = false;
    for (uint32_t i = 0u; i < ctrl->conn_count; i++)
    {
        const struct kith_control_conn *conn = &ctrl->conns[i];
        if (!conn->in_use || !conn->sse_subscribed)
        {
            continue;
        }
        const uint32_t dist = (conn->sse_cursor + capacity - tail) % capacity;
        if (!have_subscriber || dist < min_dist)
        {
            min_dist = dist;
            min_cursor = conn->sse_cursor;
            have_subscriber = true;
        }
    }
    control_event_bus_release(&ctrl->event_bus, min_cursor);
}

// ---------------------------------------------------------------------------
// SSE periodic flush: drain event bus, write SSE data lines to subscribers
// ---------------------------------------------------------------------------
void control_sse_flush(void *ctx)
{
    struct kith_control *ctrl = ctx;

    struct kith_control_event_record records[16];
    uint32_t new_cursor = 0u;

    for (uint32_t i = 0u; i < ctrl->conn_count; i++)
    {
        struct kith_control_conn *conn = &ctrl->conns[i];
        if (!conn->in_use || !conn->sse_subscribed || conn->state == KITH_CONTROL_CONN_WRITING)
        {
            continue;
        }

        const uint32_t start_cursor = conn->sse_cursor;
        uint32_t n =
            control_event_bus_drain(&ctrl->event_bus, conn->sse_cursor, records, 16u, &new_cursor);
        conn->sse_cursor = new_cursor;

        for (uint32_t j = 0u; j < n; j++)
        {
            char line[CONTROL_EVENT_MAX_PAYLOAD + 128];
            int written = snprintf(line,
                                   sizeof(line),
                                   "data: {\"type\":\"%s\",\"ts\":%llu}\n\n",
                                   records[j].type,
                                   (unsigned long long)records[j].ts_mono_ns);
            if (written > 0 && (uint32_t)written < conn->write_cap)
            {
                memcpy(conn->write_buf, line, (size_t)written);
                conn->resp.len = (uint32_t)written;
                conn->write_pos = 0u;
                // write synchronously (non-blocking)
                handle_write(ctrl, conn);
                if (conn->fd < 0)
                {
                    break;
                }
                if (conn->write_pos < conn->resp.len)
                {
                    // The socket stalled mid-line: the undrained tail stays
                    // buffered while OUT readiness finishes it, so the next
                    // record's copy cannot clobber it. The cursor rewinds
                    // past the parked line, and flush ticks skip the
                    // connection until the tail drains.
                    conn->sse_cursor = (start_cursor + j + 1u) % ctrl->event_bus.capacity;
                    conn->state = KITH_CONTROL_CONN_WRITING;
                    conn_register(ctrl, conn, KITH_REACTOR_IN | KITH_REACTOR_OUT);
                    break;
                }
            }
        }
    }

    control_bus_reclaim(ctrl);

    // kith_reactor_schedule takes an absolute monotonic deadline; passing the
    // flush interval alone is always in the past and fires immediately.
    uint64_t const flush_deadline = kith_reactor_now_ms(ctrl->reactor) + ctrl->sse_flush_ms;
    (void)kith_reactor_schedule(ctrl->reactor, flush_deadline, control_sse_flush, ctrl);
}
