#pragma once

#include <stdatomic.h>
#include <stdint.h>

#include "kith/control/control.h"
#include "kith/logger/logger.h"
#include "kith/metrics/metrics.h"
#include "kith/reactor/reactor.h"
#include "kith/worker/worker.h"

#include <netinet/in.h>

// ---------------------------------------------------------------------------
// capacity limits
// ---------------------------------------------------------------------------
#define CONTROL_MAX_ROUTES        64u
#define CONTROL_MAX_HEADERS       32u
#define CONTROL_MAX_URL           256u
#define CONTROL_MAX_METHOD        8u
#define CONTROL_MAX_HEADER_NAME   64u
#define CONTROL_MAX_HEADER_VALUE  256u
#define CONTROL_MAX_BODY          65536u
#define CONTROL_EVENT_MAX_PAYLOAD 1024u
#define CONTROL_EVENT_MAX_TYPE    64u

// ---------------------------------------------------------------------------
// connection state machine
// ---------------------------------------------------------------------------
enum kith_control_conn_state : unsigned int
{
    KITH_CONTROL_CONN_READING,     // accumulating bytes, parsing HTTP request
    KITH_CONTROL_CONN_RESPONDING,  // request parsed, ready to dispatch handler
    KITH_CONTROL_CONN_DISPATCHING, // Python route submitted to the worker pool;
                                   // the reactor waits for the flush task before
                                   // writing. If a HUP/ERR closes the conn while
                                   // a dispatch is in flight, control_conn_close
                                   // defers the buffer free and slot recycle to
                                   // the flush task (the worker aliases the
                                   // buffers until it posts the flush).
    KITH_CONTROL_CONN_WRITING,     // draining the write buffer to the socket
    KITH_CONTROL_CONN_SSE,         // SSE streaming: long-lived, periodic flush
    KITH_CONTROL_CONN_CLOSING,     // marked for close after the current batch
};

// ---------------------------------------------------------------------------
// worker-task context for a Python-bound route dispatch
// ---------------------------------------------------------------------------
// Carries the connection and the matched route entry across the
// reactor→worker→reactor handoff. The record aliases the control handle's
// stable route table (route) and the connection (conn) for the duration of
// the dispatch. The worker also aliases the conn's read buffer (request body
// and header strings) and write buffer (response); control_conn_close
// detects a mid-dispatch close and defers freeing those buffers and
// recycling the slot to control_conn_flush_task, which the worker posts
// after the handler returns, so the buffers stay live for the worker's
// entire handler call.
struct control_route_work
{
    struct kith_control *ctrl;
    struct kith_control_conn *conn;
    struct kith_control_route_entry *route;
};

// ---------------------------------------------------------------------------
// per-connection state
// ---------------------------------------------------------------------------
struct kith_control_conn
{
    struct kith_control *ctrl; // back-pointer for reactor callbacks
    enum kith_control_conn_state state;
    int fd;
    bool in_use; // slot is occupied (false after control_conn_close)
    uint8_t *read_buf;
    uint32_t read_cap;
    uint32_t read_pos;
    uint8_t *write_buf;
    uint32_t write_cap;
    uint32_t write_len;
    uint32_t write_pos;
    // Scratch arena for header name/value strings. Header names and values are
    // copied here as NUL-terminated strings so a borrowed header pointer is a
    // valid C string for callers that read it as one (the Python ctypes binding
    // reads h.name/h.value as c_char_p). The read buffer is never mutated, so a
    // request that arrives in chunks can be re-parsed without corrupted
    // delimiters. Stays live for the handler's duration (like read_buf); freed
    // by control_conn_close (immediately, or deferred to the flush task when a
    // dispatch is in flight).
    char *header_scratch;
    uint32_t header_scratch_cap;
    uint32_t header_scratch_used;
    // parsed request (borrowed pointers into header_scratch / read_buf)
    kith_control_method_t req_method;
    char req_path[CONTROL_MAX_URL];
    kith_control_header_t req_headers[CONTROL_MAX_HEADERS];
    uint32_t req_header_count;
    const void *req_body;
    uint32_t req_body_len;
    // response
    kith_control_response_t resp;
    bool sse_subscribed; // SSE event/log stream subscriber
    uint32_t sse_cursor; // event bus drain cursor for this connection
    // in-flight Python-bound dispatch (one per connection; HTTP/1.1 is
    // serial per connection). Populated by the reactor thread before
    // submitting to the worker pool; read by the worker; the reactor
    // re-reads on the flush task. Stable while state == DISPATCHING.
    struct control_route_work work;
};

// ---------------------------------------------------------------------------
// route table entry
// ---------------------------------------------------------------------------
struct kith_control_route_entry
{
    char method[CONTROL_MAX_METHOD];
    char path[CONTROL_MAX_URL];
    kith_control_handler_fn handler;
    void *ctx;
    // Route flags (copied from kith_control_route_t.flags at registration).
    // KITH_CONTROL_ROUTE_PYTHON routes are submitted to the worker pool when
    // one is attached; C routes (NONE) always run inline.
    uint32_t flags;
};

// ---------------------------------------------------------------------------
// event bus ring buffer record (stored copy)
// ---------------------------------------------------------------------------
struct kith_control_event_record
{
    uint64_t ts_mono_ns;
    char type[CONTROL_EVENT_MAX_TYPE];
    uint8_t correlation_id[8];
    bool has_correlation;
    uint8_t payload[CONTROL_EVENT_MAX_PAYLOAD];
    uint32_t payload_len;
};

// ---------------------------------------------------------------------------
// event bus — SPSC ring buffer with subscriber-driven reclamation
// ---------------------------------------------------------------------------
// head is the publish cursor; tail is the oldest record any attached stream
// still needs. The flush path releases reclaimed records once every
// subscriber has drained past them, so a full ring (publish reports -EBUSY)
// is transient for as long as the streams keep up.
struct kith_control_event_bus
{
    struct kith_control_event_record *records;
    uint32_t capacity;
    _Atomic uint32_t head;
    _Atomic uint32_t tail;
};

// ---------------------------------------------------------------------------
// control handle
// ---------------------------------------------------------------------------
struct kith_control
{
    /** Allocator resolved at create; the handle, the connection table,
     *  and the event-bus records allocate and free through it, as do the
     *  per-connection buffers over the handle's lifetime. */
    const kith_allocator_t *allocator;
    kith_reactor_t *reactor; // borrowed
    kith_logger_t *logger;   // borrowed, may be NULL
    kith_metrics_t *metrics; // borrowed, may be NULL
    // Worker pool for Python-bound route dispatch. Borrowed for
    // the control handle's lifetime; NULL when no pool is attached (route
    // handlers run inline on the reactor thread — the built-in C handlers
    // and any C-registered route, which never enter Python).
    kith_worker_t *workers; // borrowed, may be NULL

    // listener
    int listener_fd;
    bool started;
    _Atomic uint32_t sse_subscribers; // attached SSE streams whose cursor is recorded

    // params (copied at create time)
    const char *host; // borrowed from params
    uint16_t port;
    uint32_t max_connections;
    uint32_t read_buffer_cap;
    uint32_t write_buffer_cap;
    uint32_t sse_flush_ms;

    // connection table
    struct kith_control_conn *conns;
    uint32_t conn_count;

    // route table
    struct kith_control_route_entry routes[CONTROL_MAX_ROUTES];
    uint32_t route_count;

    // event bus
    struct kith_control_event_bus event_bus;
};

// ---------------------------------------------------------------------------
// http_parser.c — incremental HTTP/1.1 parser
// ---------------------------------------------------------------------------

// Feed accumulated read-buffer bytes to the parser. Returns 0 when a complete
// request has been parsed (conn->state advances to RESPONDING), 1 when more
// bytes are needed (conn stays in READING), -1 on a parse error (conn set to
// CLOSING).
int control_http_parser_consume(struct kith_control_conn *conn);

// Reset the parser state for a new request on the same connection.
void control_http_parser_reset(struct kith_control_conn *conn);

// ---------------------------------------------------------------------------
// event_bus.c — SPSC ring buffer
// ---------------------------------------------------------------------------

// Initialize the event bus with @p capacity pre-allocated records. The
// records allocate through @p alloc and are released by
// control_event_bus_free with the same instance.
void control_event_bus_init(struct kith_control_event_bus *bus,
                            uint32_t capacity,
                            const kith_allocator_t *alloc);

// Free the event bus's record storage through @p alloc.
void control_event_bus_free(struct kith_control_event_bus *bus, const kith_allocator_t *alloc);

// Publish an event record into the ring. Returns 0 on success, -1 if the
// ring is full (event dropped).
int control_event_bus_publish(struct kith_control_event_bus *bus,
                              const kith_control_event_t *event);

// Drain events since @p cursor into @p out (up to @p out_cap records).
// Returns the number of records drained, and updates @p cursor to the new
// position.
uint32_t control_event_bus_drain(struct kith_control_event_bus *bus,
                                 uint32_t cursor,
                                 struct kith_control_event_record *out,
                                 uint32_t out_cap,
                                 uint32_t *out_new_cursor);

// Release records up to @p cursor (the oldest record any stream still
// needs): advances tail so publish can reuse the slots. The advance is
// monotone — a cursor outside the live window [tail, head] leaves the ring
// untouched. Called from the flush path with the minimum live cursor (or
// the head when no stream is attached).
void control_event_bus_release(struct kith_control_event_bus *bus, uint32_t cursor);

// ---------------------------------------------------------------------------
// router.c — route table + dispatch
// ---------------------------------------------------------------------------

// Find the first route matching the connection's parsed method+path, or
// NULL when no route matches. The returned entry aliases stable storage
// inside ctrl->routes (valid for the control handle's lifetime).
struct kith_control_route_entry *control_router_match(struct kith_control *ctrl,
                                                      struct kith_control_conn *conn);

// Write a 404 Not Found response into conn->resp (no route matched).
void control_router_write_404(struct kith_control_conn *conn);

// Write a 501 Not Implemented response into conn->resp (unsupported
// protocol upgrade requested).
void control_router_write_501(struct kith_control_conn *conn);

// Write a 503 Service Unavailable response into conn->resp (a Python-bound
// route cannot dispatch off the reactor thread).
void control_router_write_503(struct kith_control_conn *conn);

// Dispatch a parsed request to the first matching route. Writes the HTTP
// response into conn->resp for inline (C) handlers and no-match 404, and
// hands the inline handler's own return value to *out_handler_rc (0 for a
// 404). Returns:
//   0 — an inline handler ran (or a 404 was written); the caller flushes
//       the response, or closes the connection with the response discarded
//       when the handler reported non-zero.
//   1 — the matched route is Python-bound (KITH_CONTROL_ROUTE_PYTHON); the
//       handler is NOT invoked here. The caller (reactor thread) submits
//       conn to the pool; with no pool attached, or when the pool's queue
//       is exhausted, the caller answers 503 and counts the drop — the
//       reactor never runs a Python-bound handler inline.
// The route decision and the handler's own return value ride separate
// channels: the handler's close contract admits any non-zero value,
// including 1, so the deferral signal cannot share the handler's return
// namespace.
int control_router_dispatch(struct kith_control *ctrl,
                            struct kith_control_conn *conn,
                            int *out_handler_rc);

// Worker task: runs a Python-bound route handler off the reactor thread. The
// ctx is a control_route_work record carrying the connection and the matched
// route entry; the worker invokes r->handler, then posts the socket-flush
// back to the reactor via kith_reactor_submit.
void control_route_worker_task(void *ctx);

// Reactor task: flush a connection's response buffer on the reactor thread
// after a worker has filled it. The ctx is the connection.
void control_conn_flush_task(void *ctx);

// ---------------------------------------------------------------------------
// handlers.c — built-in handlers
// ---------------------------------------------------------------------------

// Register the built-in routes (/health, /metrics, /events/stream,
// /logs/stream) on the control handle.
int control_handlers_register(struct kith_control *ctrl);

// Built-in handler: GET /health
int control_handler_health(const kith_control_request_t *req,
                           kith_control_response_t *resp,
                           void *ctx);

// Built-in handler: GET /metrics
int control_handler_metrics(const kith_control_request_t *req,
                            kith_control_response_t *resp,
                            void *ctx);

// Built-in handler: GET /events/stream (SSE)
int control_handler_events_stream(const kith_control_request_t *req,
                                  kith_control_response_t *resp,
                                  void *ctx);

// Built-in handler: GET /logs/stream (SSE)
int control_handler_logs_stream(const kith_control_request_t *req,
                                kith_control_response_t *resp,
                                void *ctx);

// ---------------------------------------------------------------------------
// conn.c — connection management + reactor event handler
// ---------------------------------------------------------------------------

// Allocate a connection slot from the table, with the slot's read/write
// buffers and header scratch through the handle's allocator. Returns NULL
// if the table is full or a buffer allocation fails (a failed attempt
// leaves the slot free).
struct kith_control_conn *control_conn_alloc(struct kith_control *ctrl);

// Close a connection: deregister from reactor, close fd, reset state.
void control_conn_close(struct kith_control *ctrl, struct kith_control_conn *conn);

// Reactor fd readiness callback for per-connection sockets.
void control_conn_event_handler(int fd, unsigned int events, void *ctx);

// Reactor fd readiness callback for the listener socket.
void control_listener_event_handler(int fd, unsigned int events, void *ctx);

// SSE periodic flush: drain the event bus and write SSE data lines to all
// subscribed connections. Called via kith_reactor_schedule.
void control_sse_flush(void *ctx);

// ---------------------------------------------------------------------------
// control.c — lifecycle + start/stop (public API)
// ---------------------------------------------------------------------------

// Create the non-blocking listener socket, bind, and listen. Returns the
// listener fd on success, -1 on failure (errno set).
int control_listener_create(struct kith_control *ctrl);
