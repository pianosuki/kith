#ifndef KITH_CONTROL_CONTROL_H
#define KITH_CONTROL_CONTROL_H

#include <stddef.h>
#include <stdint.h>

#include "kith/api.h"
#include "kith/types.h"

/**
 * Opaque worker pool handle (defined in kith/worker/worker.h). Forward-
 * declared here so @c kith_control_attach_worker_pool can take a pool
 * pointer without forcing every consumer of the control header to also
 * include the worker header. The worker module owns no game state and no
 * file descriptors; the control handle borrows a pool for the lifetime of
 * its Python-bound route dispatch.
 */
typedef struct kith_worker kith_worker_t;

/**
 * Embedded HTTP/1.1 + WebSocket server, event bus, and route registry for
 * the control plane.
 *
 * A control handle owns a non-blocking TCP listener, a connection table
 * with per-connection read/write buffers, an HTTP/1.1 incremental parser, a
 * WebSocket frame codec, a route table, and a single-producer/single-consumer
 * event bus. The handle borrows a reactor for file-descriptor readiness
 * (io_uring): the listener and per-connection
 * sockets are registered with the reactor, and accept/read/write are driven
 * from reactor callbacks. No threads are spawned — all I/O happens on the
 * reactor's event-loop thread.
 *
 * The library ships built-in handlers for @c /health (liveness JSON),
 * @c /metrics (Prometheus text-exposition scrape), @c /events/stream (SSE
 * event stream fed by the event bus), and @c /logs/stream (SSE log stream
 * fed by the logger's JSONL side-channel). The composition root registers
 * additional routes via @c kith_control_register_route. The library knows
 * nothing about the application's query or command vocabulary — every
 * game-specific handler is registered by the caller.
 *
 * The library is zero-cost when the build-time CMake option
 * @c CONTROL_PLANE_ENABLED is set to OFF: the shared library is not built
 * and no code is linked. There are no source-level @c \#ifdef guards; the
 * library is absent from the build.
 */

/* Forward declarations — the full types live in their respective headers. */
typedef struct kith_reactor kith_reactor_t;
typedef struct kith_logger kith_logger_t;
typedef struct kith_metrics kith_metrics_t;

/**
 * @defgroup kith_control Control
 * @{
 */

/**
 * Opaque control plane handle.
 *
 * @ownership callee — created by kith_control_create, destroyed by
 *           kith_control_destroy. Owns the listener fd, the connection
 *           table, the route table, and the event bus. The reactor,
 *           logger, and metrics handles are borrowed for the handle's
 *           lifetime.
 */
typedef struct kith_control kith_control_t;

/**
 * Default values used when the corresponding field in
 * @c kith_control_params_t is set to zero.
 */
enum kith_control_default : unsigned int
{
    /** Suggested TCP listen port. A params port of 0 selects an
     *  OS-assigned ephemeral port; pass this value explicitly to bind it. */
    KITH_CONTROL_DEFAULT_PORT = 8080u,
    /** Default max simultaneous connections (params.max_connections = 0). */
    KITH_CONTROL_DEFAULT_MAX_CONNECTIONS = 64u,
    /** Default per-connection read buffer capacity in bytes. */
    KITH_CONTROL_DEFAULT_READ_BUF = 4096u,
    /** Default per-connection write buffer capacity in bytes. */
    KITH_CONTROL_DEFAULT_WRITE_BUF = 262144u,
    /** Default event bus ring capacity in event records. */
    KITH_CONTROL_DEFAULT_EVENT_BUS_CAP = 4096u,
    /** Default SSE flush interval in milliseconds (0 = flush on publish). */
    KITH_CONTROL_DEFAULT_SSE_FLUSH_MS = 100u,
};

/**
 * HTTP request method. The underlying type is fixed so a method stored in
 * an ABI surface stays a fixed width.
 */
enum kith_control_method : unsigned int
{
    KITH_CONTROL_METHOD_GET = 0u,
    KITH_CONTROL_METHOD_POST = 1,
    KITH_CONTROL_METHOD_PUT = 2,
    KITH_CONTROL_METHOD_DELETE = 3,
    KITH_CONTROL_METHOD_UNKNOWN = 4,
};

/** Alias of enum kith_control_method. */
typedef enum kith_control_method kith_control_method_t;

/**
 * A single HTTP request header. Both @p name and @p value are borrowed
 * pointers into the connection's read buffer, valid only for the duration
 * of the handler callback. This is an exposed-layout value type (like
 * @c kith_logger_field_t and @c kith_metrics_label_t).
 */
struct kith_control_header
{
    /** Header name; borrowed into the connection's read buffer. */
    const char *name;
    /** Header value; borrowed into the connection's read buffer. */
    const char *value;
};

/** Alias of struct kith_control_header. */
typedef struct kith_control_header kith_control_header_t;

/**
 * A parsed HTTP/1.1 request, passed to a route handler. All pointer fields
 * are borrowed from the connection's read buffer and are valid only for the
 * duration of the handler callback. This is an exposed-layout value type
 * (like @c kith_state_reply_t).
 */
struct kith_control_request
{
    /** Request method. */
    kith_control_method_t method;
    /** Request path (NUL-terminated, borrowed). Does not include the query
     * string; the full request line URL is available here for simple
     * path matching. */
    const char *path;
    /** Borrowed header array, or NULL if @p header_count is 0. */
    const kith_control_header_t *headers;
    /** Number of headers at @p headers. */
    uint32_t header_count;
    /** Borrowed request body, or NULL if @p body_len is 0. */
    const void *body;
    /** Request body length in bytes. */
    uint32_t body_len;
};

/** Alias of struct kith_control_request. */
typedef struct kith_control_request kith_control_request_t;

/**
 * HTTP response builder, passed to a route handler. The handler writes the
 * HTTP response into the response buffer via the builder helpers
 * (@c kith_control_response_status, @c kith_control_response_header,
 * @c kith_control_response_body) or by writing directly into @p buf.
 *
 * This is an exposed-layout value type. The server populates @p buf and
 * @p cap before calling the handler; the handler fills @p len with the
 * total response length. Set @p sse to @c true for SSE streaming (the
 * connection stays open and the server periodically writes SSE data lines
 * from the event bus).
 */
struct kith_control_response
{
    /** Write buffer (borrowed from the connection). The handler writes the
     * full HTTP response (status line + headers + body) into this buffer. */
    uint8_t *buf;
    /** Buffer capacity in bytes. */
    uint32_t cap;
    /** Current response length in bytes (set by the handler). */
    uint32_t len;
    /** Set to true for SSE streaming (keep the connection open). */
    bool sse;
};

/** Alias of struct kith_control_response. */
typedef struct kith_control_response kith_control_response_t;

/**
 * Route handler callback. The handler receives a parsed request and writes
 * the HTTP response into @p resp. The @p ctx pointer supplied at
 * registration time is passed through.
 *
 * @param req  Parsed request. All pointer fields are valid only for the
 *             duration of this call.
 * @param resp Response builder. Write the full HTTP response into
 *             @p resp->buf and set @p resp->len.
 * @param ctx  Opaque context pointer from route registration.
 * @return     0 on success, non-zero to discard the response and close the
 *             connection.
 * @thread_safety unsafe — called on the reactor's event-loop thread. Must
 *                not block or call back into the reactor.
 */
typedef int (*kith_control_handler_fn)(const kith_control_request_t *req,
                                       kith_control_response_t *resp,
                                       void *ctx);

/**
 * Route registration flags. The underlying type is fixed.
 */
enum kith_control_route_flag : unsigned int
{
    /** Default: the handler is a C function pointer and runs inline on the
     *  reactor thread (no pool hop). */
    KITH_CONTROL_ROUTE_NONE = 0u,
    /** The handler is a Python-bound ctypes trampoline. It is submitted to
     *  the attached worker pool instead of invoked on the reactor thread,
     *  keeping the reactor thread out of the Python interpreter. The
     *  reactor never runs a Python-bound handler inline: with no pool
     *  attached, or when the pool's queue is exhausted, the request
     *  answers 503 Service Unavailable and the drop is counted on the
     *  metrics registry. The handler runs on a pool thread alongside
     *  every other Python-bound callback the composition dispatches: the
     *  server attaches one pool to the gateway and the control plane, and
     *  per-tick handlers, message handlers, and route handlers all submit
     *  onto it. With more than one worker the callbacks run concurrently;
     *  game state shared between callbacks is the game's responsibility
     *  to synchronize. */
    KITH_CONTROL_ROUTE_PYTHON = 1u,
};

/** Alias of enum kith_control_route_flag. */
typedef enum kith_control_route_flag kith_control_route_flag_t;

/**
 * A route registration entry. The composition root passes an array of these
 * to @c kith_control_register_route. This is an exposed-layout value type.
 */
struct kith_control_route
{
    /** HTTP method string ("GET", "POST", etc.), NUL-terminated. Borrowed. */
    const char *method;
    /**
     * URL path pattern, NUL-terminated, borrowed. Supports @c :param
     * segments (e.g. @c "/api/v1/items/:id" matches
     * @c "/api/v1/items/42"). The parameter value is not captured; the
     * handler receives the full path in @c kith_control_request_t.path.
     */
    const char *path;
    /** Handler callback. */
    kith_control_handler_fn handler;
    /** Opaque context pointer passed to @p handler. May be NULL. */
    void *ctx;
    /** Route flags (bitwise OR of @c kith_control_route_flag). */
    uint32_t flags;
};

/** Alias of struct kith_control_route. */
typedef struct kith_control_route kith_control_route_t;

/**
 * An event bus record. Published via @c kith_control_publish_event and
 * drained by the SSE stream handler. This is an exposed-layout value type.
 */
struct kith_control_event
{
    /** Monotonic timestamp in nanoseconds (CLOCK_MONOTONIC). */
    uint64_t ts_mono_ns;
    /** Event type string, NUL-terminated (borrowed for the publish call,
     * copied into the ring buffer). */
    const char *type;
    /** Correlation ID (8 bytes, the optional correlation trailer), or NULL.
     *  Borrowed. */
    const uint8_t *correlation_id;
    /** Event payload (borrowed for the publish call, copied into the ring
     * buffer). May be NULL if @p payload_len is 0. */
    const void *payload;
    /** Payload length in bytes. */
    uint32_t payload_len;
};

/** Alias of struct kith_control_event. */
typedef struct kith_control_event kith_control_event_t;

/**
 * Creation parameters. Size-versioned: callers set @p size to
 * sizeof(kith_control_params_t) and @p abi_version to KITH_ABI_VERSION at
 * their compile time; the runtime rejects structs from an incompatible
 * generation or an undersized size. A field set to 0 selects the
 * corresponding @c kith_control_default value.
 */
struct kith_control_params
{
    /** Must be sizeof(kith_control_params_t). */
    uint32_t size;
    /** Must be KITH_ABI_VERSION (from kith/version.h). */
    uint32_t abi_version;

    /**
     * Bind address for the control-plane listener. NULL selects the
     * loopback address (127.0.0.1): the plane serves unauthenticated
     * HTTP, so it stays local unless a deployer opts out. A non-NULL
     * value is a numeric IPv4 address or a host name ("0.0.0.0" binds
     * the wildcard). The name resolves when @c kith_control_start
     * binds; an unresolvable name fails the start with -KITH_EIO.
     * Borrowed for the handle's lifetime.
     */
    const char *host;

    /** TCP listen port; 0 selects an OS-assigned ephemeral port. */
    uint16_t port;

    /** Max simultaneous connections. 0 selects the default (64). */
    uint32_t max_connections;

    /** Per-connection read buffer capacity. 0 selects the default (4096). */
    uint32_t read_buffer_cap;

    /**
     * Per-connection write buffer capacity in bytes. 0 selects the default
     * (262144). A response larger than this capacity fails the response
     * build with -KITH_EOVERFLOW;
     * kith_control_reject_overflow renders the canonical counted 500 for
     * that condition. Size the capacity for the largest listing a route
     * returns.
     */
    uint32_t write_buffer_cap;

    /** Event bus ring capacity in event records. 0 selects the default. */
    uint32_t event_bus_cap;

    /**
     * SSE flush interval in milliseconds. 0 selects the default (100 ms).
     * The control handle schedules a periodic timer on the reactor that
     * drains the event bus and writes SSE data lines to subscribed
     * connections.
     */
    uint32_t sse_flush_ms;

    /** Reserved for future additive fields. Must be zero-filled. */
    void *reserved[8];
};

/** Alias of struct kith_control_params. */
typedef struct kith_control_params kith_control_params_t;

/*---------------------------------------------------------------------------
 * lifecycle
 *-------------------------------------------------------------------------*/

/**
 * Create a control plane handle.
 *
 * Allocates the handle, connection table, route table, and event bus. Does
 * not bind or listen — call @c kith_control_start to begin accepting
 * connections. The reactor, logger, and metrics handles are borrowed for the
 * control handle's lifetime.
 *
 * @param params    Creation parameters. Must be non-NULL with a valid
 *                  @p size and @p abi_version. NULL selects all defaults.
 * @param reactor   Borrowed reactor handle. Must outlive the control
 *                  handle. Non-NULL.
 * @param logger    Borrowed logger handle. May be NULL (logging is
 *                  silently dropped).
 * @param metrics   Borrowed metrics handle. May be NULL (the /metrics
 *                  endpoint returns an empty payload).
 * @param alloc     Allocator for the new handle, its connection table,
 *                  and its event-bus records, used again when
 *                  kith_control_destroy frees them and when
 *                  per-connection buffers allocate and free over the
 *                  handle's lifetime. NULL selects the default allocator;
 *                  a supplied allocator is validated (see
 *                  kith_allocator_t) and must outlive the handle.
 * @param out_ctrl  Receives the new handle on success.
 * @return          0 on success, negative kith_error on failure:
 *                  - -KITH_EINVAL if @p reactor or @p out_ctrl is NULL,
 *                    or @p alloc is missing an operation,
 *                  - -KITH_EABIVER if @p params or @p alloc has an
 *                    incompatible abi_version,
 *                  - -KITH_ESIZE if @p params or @p alloc has an
 *                    undersized size,
 *                  - -KITH_ENOMEM on allocation failure.
 * @thread_safety unsafe — must not race with another
 *                kith_control_create on the same @p out_ctrl slot.
 * @ownership callee — the returned handle is freed by the caller with
 *           kith_control_destroy.
 */
[[nodiscard]] KITH_API int kith_control_create(const kith_control_params_t *params,
                                               kith_reactor_t *reactor,
                                               kith_logger_t *logger,
                                               kith_metrics_t *metrics,
                                               const kith_allocator_t *alloc,
                                               kith_control_t **out_ctrl);

/**
 * Release all resources held by @p ctrl. Passing NULL is a no-op.
 *
 * Closes the listener (if started), closes every connection, deregisters
 * all fds from the reactor, and frees the connection table, route table,
 * and event bus. The borrowed reactor, logger, and metrics handles are
 * not freed.
 *
 * @param ctrl  Control handle. NULL is a no-op.
 * @thread_safety unsafe — the reactor must be stopped and every
 *                attached worker pool drained and destroyed before this
 *                call. The SSE flush timer reschedules itself on the
 *                reactor, so after @c kith_control_start a pending timer
 *                always exists: destroying while the reactor runs leaves
 *                that timer firing into freed memory. A Python-bound
 *                dispatch in flight aliases the connection buffers this
 *                call frees, so the pool must be joined first (see
 *                @c kith_control_attach_worker_pool).
 * @ownership callee — @p ctrl is consumed and freed by the call.
 */
KITH_API void kith_control_destroy(kith_control_t *ctrl);

/*---------------------------------------------------------------------------
 * start / Stop
 *-------------------------------------------------------------------------*/

/**
 * Bind the listener socket and register it with the reactor for
 * accept-readiness. The listener fd is non-blocking with close-on-exec and
 * TCP_NODELAY. May be called at most once per handle.
 *
 * @param ctrl  Control handle. Must be non-NULL.
 * @return      0 on success, negative kith_error on failure:
 *              - -KITH_EINVAL if @p ctrl is NULL,
 *              - -KITH_ESTATE if already started,
 *              - -KITH_EIO on socket/bind/listen failure.
 * @thread_safety unsafe — call from the composition root before the
 *                reactor starts or from the reactor thread.
 * @ownership caller — @p ctrl is borrowed for the call only.
 */
[[nodiscard]] KITH_API int kith_control_start(kith_control_t *ctrl);

/**
 * Stop accepting new connections. Closes the listener fd and deregisters
 * it from the reactor. Existing connections are not closed (they drain
 * naturally or are closed on @c kith_control_destroy). Passing NULL or a
 * not-started handle is a no-op.
 *
 * @param ctrl  Control handle. NULL or a not-started handle is a no-op.
 * @thread_safety unsafe — call from the reactor thread or before the
 *                reactor starts.
 * @ownership caller — @p ctrl is borrowed for the call only.
 */
KITH_API void kith_control_stop(kith_control_t *ctrl);

/**
 * Return the actual TCP port the control listener is bound to. When the
 * params @c port is 0, the OS assigns an ephemeral port at bind time; this
 * accessor reads it back via @c getsockname so a caller that bound
 * ephemerally can connect to or advertise the real endpoint. Returns 0
 * when @p ctrl is NULL or the listener is not started.
 *
 * @param ctrl  Control handle. NULL returns 0.
 * @return      the bound port in host byte order, or 0 when unbound.
 * @thread_safety safe — the listener fd and bound port are fixed after
 *                @c kith_control_start.
 * @ownership caller — @p ctrl is borrowed for the call only.
 */
KITH_API uint16_t kith_control_listen_port(const kith_control_t *ctrl);

/**
 * Read the number of currently attached SSE stream subscribers.
 *
 * A subscriber is a connection serving an event or log stream; the count
 * drops when the connection closes. A subscriber counted here has its
 * subscription cursor recorded, so events published after this call are
 * delivered to it.
 *
 * @param ctrl      Control handle. Must be non-NULL.
 * @param out_count Receives the subscriber count. Must be non-NULL.
 * @return          0 on success, -KITH_EINVAL if @p ctrl or @p out_count is
 *                  NULL.
 * @thread_safety safe — the count is maintained and read atomically on the
 *                reactor thread.
 * @ownership caller — @p ctrl and @p out_count are borrowed for the call
 *           only.
 */
[[nodiscard]] KITH_API int kith_control_subscriber_count(const kith_control_t *ctrl,
                                                         uint32_t *out_count);

/**
 * Attach a worker pool for Python-bound route dispatch.
 *
 * When a non-NULL @p pool is attached, route handlers that are registered
 * via @c kith_control_register_route with the @c KITH_CONTROL_ROUTE_PYTHON
 * flag are submitted to @p pool instead of invoked inline on the reactor
 * thread; the worker runs the handler, fills the response buffer, and posts
 * the socket-flush back to the reactor via @c kith_reactor_submit. This
 * keeps the reactor thread out of the Python interpreter: a Python route
 * handler never blocks I/O readiness for other connections.
 *
 * C-registered routes (the default, no @c KITH_CONTROL_ROUTE_PYTHON flag)
 * always run inline on the reactor thread — they are plain C function
 * pointers and never enter Python, so the pool hop would only add latency.
 *
 * Pass NULL to detach (Python-bound routes answer 503 while detached). The
 * pool is borrowed for the control handle's lifetime; the caller owns it.
 *
 * @param ctrl Control handle. Must be non-NULL.
 * @param pool Worker pool handle, or NULL to detach.
 * @return     0 on success, negative kith_error on failure:
 *             - -KITH_EINVAL if @p ctrl is NULL.
 * @thread_safety unsafe — call from the composition root before the reactor
 *                starts (the dispatch path reads @p pool without a lock).
 * @ownership caller — @p pool is borrowed for the control handle's
 *           lifetime; the caller destroys it before
 *           @c kith_control_destroy: @c kith_worker_destroy joins the
 *           pool's threads, so no in-flight dispatch aliases the
 *           connection buffers the control destroy frees.
 */
[[nodiscard]] KITH_API int kith_control_attach_worker_pool(kith_control_t *ctrl,
                                                           kith_worker_t *pool);

/*---------------------------------------------------------------------------
 * route registry
 *-------------------------------------------------------------------------*/

/**
 * Register a route. Routes are matched in registration order on each
 * request. The @p route's @p method and @p path strings are copied; the
 * caller may free them after the call returns. A Python-flagged route's
 * handler runs on the attached pool — the pool the server composition
 * shares with per-tick and message handlers — so with more than one
 * worker it can run concurrently with those callbacks; game state shared
 * between callbacks is the game's responsibility to synchronize.
 *
 * @param ctrl  Control handle. Must be non-NULL.
 * @param route Route to register. Must be non-NULL with non-NULL @p method,
 *              @p path, and @p handler.
 * @return      0 on success, negative kith_error on failure:
 *              - -KITH_EINVAL if @p ctrl or @p route is NULL, or
 *                @p route->method, @p route->path, or
 *                @p route->handler is NULL,
 *              - -KITH_EBUSY if the route table is full.
 * @thread_safety unsafe — must not race with request dispatch on the same
 *                handle.
 * @ownership caller — @p route strings are borrowed for the call and copied
 *           on success.
 */
[[nodiscard]] KITH_API int kith_control_register_route(kith_control_t *ctrl,
                                                       const kith_control_route_t *route);

/*---------------------------------------------------------------------------
 * event bus
 *-------------------------------------------------------------------------*/

/**
 * Publish an event to the event bus. The event record is copied into the
 * ring buffer. If the ring is full the event is dropped (the return value
 * reports the drop); space reclaims once every attached stream has drained
 * past the records, so a drop is transient while the streams keep up — a
 * stream that stalls longer than one ring capacity of published events
 * forces drops for the whole bus until it drains or disconnects. A payload
 * longer than the bus's per-record capacity is truncated to fit; the
 * record still publishes with the truncated payload. A new stream
 * subscriber receives the events published after it attached; the bus is a
 * delivery ring, not a history log.
 *
 * @param ctrl  Control handle. Must be non-NULL.
 * @param event Event to publish. Must be non-NULL. The @p type string and
 *              @p payload are copied into the ring buffer; the caller may
 *              free them after the call returns.
 * @return      0 on success, negative kith_error on failure:
 *              - -KITH_EINVAL if @p ctrl or @p event is NULL,
 *              - -KITH_EBUSY if the event bus ring is full (event dropped).
 * @thread_safety unsafe — call from the reactor thread or submit as a
 *                reactor task via kith_reactor_submit.
 * @ownership caller — @p event strings and payload are borrowed for the
 *           call and copied on success.
 */
[[nodiscard]] KITH_API int kith_control_publish_event(kith_control_t *ctrl,
                                                      const kith_control_event_t *event);

/*---------------------------------------------------------------------------
 * response builder helpers
 *-------------------------------------------------------------------------*/

/**
 * Write the HTTP status line and a Content-Type header into the response
 * buffer. This is the first call when building a response. Subsequent
 * calls to @c kith_control_response_header append headers, and
 * @c kith_control_response_body writes the body and finalizes the
 * Content-Length.
 *
 * @param resp          Response builder. Must be non-NULL.
 * @param status_code   HTTP status code (e.g. 200, 404, 500).
 * @param content_type  Content-Type header value, NUL-terminated. May be
 *                      NULL to omit the Content-Type header.
 * @return              0 on success, -KITH_EINVAL if @p resp is NULL,
 *                      -KITH_EOVERFLOW if the buffer is too small.
 * @thread_safety unsafe — call only from within a route handler callback.
 * @ownership caller — @p resp and @p content_type are borrowed for the call
 *           only.
 */
[[nodiscard]] KITH_API int kith_control_response_status(kith_control_response_t *resp,
                                                        int status_code,
                                                        const char *content_type);

/**
 * Append a response header. Must be called after
 * @c kith_control_response_status and before
 * @c kith_control_response_body.
 *
 * @param resp   Response builder. Must be non-NULL.
 * @param name   Header name, NUL-terminated. Must be non-NULL.
 * @param value  Header value, NUL-terminated. Must be non-NULL.
 * @return       0 on success, -KITH_EINVAL if args are NULL,
 *               -KITH_EOVERFLOW if the buffer is too small.
 * @thread_safety unsafe — call only from within a route handler callback.
 * @ownership caller — @p resp, @p name, and @p value are borrowed for the call
 *           only.
 */
[[nodiscard]] KITH_API int
kith_control_response_header(kith_control_response_t *resp, const char *name, const char *value);

/**
 * Write the response body and finalize the response. This inserts the
 * Content-Length header, the blank line separating headers from body, and
 * the body bytes. After this call, @p resp->len holds the total response
 * length and the response is ready to be written to the connection.
 *
 * @param resp      Response builder. Must be non-NULL.
 * @param body      Body bytes. May be NULL if @p body_len is 0.
 * @param body_len  Body length in bytes.
 * @return          0 on success, -KITH_EINVAL if @p resp is NULL,
 *                  -KITH_EOVERFLOW if the buffer is too small.
 * @thread_safety unsafe — call only from within a route handler callback.
 * @ownership caller — @p resp and @p body are borrowed for the call only.
 */
[[nodiscard]] KITH_API int
kith_control_response_body(kith_control_response_t *resp, const void *body, uint32_t body_len);

/**
 * Reset a response and render the canonical over-cap rejection into it.
 *
 * Called when a response cannot fit the connection's write buffer: the
 * builder functions' -KITH_EOVERFLOW failure leaves the response unusable,
 * and this replaces it with a complete 500 whose JSON body names the route,
 * the attempted size, and the capacity
 * (`{"error":"response_too_large",...}`) — the sizing condition an operator
 * reads instead of an internal error symbol. A NULL @p ctrl renders the
 * rejection without counting; a NULL @p route omits the route key;
 * @p attempted 0 omits the attempted key. When even the rejection cannot
 * fit, @p resp->len is reset to 0 and -KITH_EOVERFLOW is returned — the
 * connection then closes with nothing on the wire, the same outcome as a
 * route handler returning nonzero.
 *
 * @param ctrl       Control handle whose counter records the rejection. May
 *                   be NULL.
 * @param resp       Response builder to reset and fill. Must be non-NULL.
 * @param route      Request path the oversize response belonged to,
 *                   NUL-terminated, or NULL to omit the route key. The
 *                   text is JSON-escaped; control and non-ASCII bytes are
 *                   dropped.
 * @param attempted  Attempted response body size in bytes, or 0 when
 *                   unknown.
 * @return           0 on success, -KITH_EINVAL if @p resp is NULL,
 *                   -KITH_EOVERFLOW when the rejection itself does not fit
 *                   the buffer (@p resp->len reset to 0).
 * @thread_safety unsafe — call only from within a route handler callback,
 *                like the response builder functions.
 * @ownership caller — @p route is borrowed for the call only.
 */
[[nodiscard]] KITH_API int kith_control_reject_overflow(kith_control_t *ctrl,
                                                        kith_control_response_t *resp,
                                                        const char *route,
                                                        uint32_t attempted);

/** @} */

#endif /* KITH_CONTROL_CONTROL_H */
