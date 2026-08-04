#ifndef KITH_NET_NET_H
#define KITH_NET_NET_H

#include <stddef.h>
#include <stdint.h>

#include "kith/api.h"
#include "kith/proto/proto.h"
#include "kith/types.h"

/**
 * TCP transport and frame extraction.
 *
 * A net handle owns a TCP listener, a connection table, and a per-transport
 * frame pool. Connections wrap a non-blocking socket fd with a power-of-two
 * ring buffer for partial reads, a FIFO output queue for batched writes, and
 * in/out backpressure watermarks. The read path reads available bytes into the
 * ring buffer and extracts complete frames by driving @c kith_proto_decode on
 * the leading bytes (contiguous ring bytes, or a gather copy when the leading
 * frame spans the ring's wrap boundary); decoded frames are delivered to a
 * caller-supplied callback as views into that read staging, valid for the
 * duration of the callback only.
 *
 * The transport does not own an event loop. It exposes the listener fd, each
 * connection's fd, and a desired-events hint (@c kith_net_conn_events) so a
 * reactor (a separate library driving io_uring) can poll the
 * fds and call the read/write/accept primitives in response to readiness. This
 * keeps the transport free of any event-loop dependency.
 *
 * The proto handle is borrowed from the composition root for the lifetime of
 * the net handle; the transport does not own or duplicate the message-type
 * registry. Per-message payload (de)serialization is the caller's concern; the
 * transport moves opaque frame buffers on the write path and delivers decoded
 * proto frame views on the read path.
 *
 * Lifecycle: the composition root creates the net handle, listens on a
 * host:port, and hands the handle to the reactor. The reactor accepts
 * connections, polls their fds, and calls read/write on readiness. Connections
 * are reference-counted so a reactor can hand a connection reference to a
 * worker pool for off-thread processing; the connection is freed when the last
 * reference is released.
 */

/**
 * @defgroup kith_net Transport
 * @{
 */

/**
 * Default configuration values. A @c kith_net_params_t field set to 0 selects
 * the corresponding default at create time. The underlying type is fixed so a
 * default stored in an ABI surface stays a fixed width.
 */
enum kith_net_default : unsigned int
{
    /** Default max_connections (params.max_connections = 0 selects this). */
    KITH_NET_DEFAULT_MAX_CONNECTIONS = 4096u,
    /** Default initial per-connection read buffer capacity (4 KiB). */
    KITH_NET_DEFAULT_READ_BUF_INITIAL = 4u * 1024u,
    /** Default max per-connection read buffer capacity (256 KiB). */
    KITH_NET_DEFAULT_READ_BUF_MAX = 256u * 1024u,
    /**
     * Floor for an explicitly set read_buffer_max: the transport reads in
     * chunks of this size, so a smaller ceiling could not hold even one
     * chunk. kith_net_create rejects smaller values with -KITH_EINVAL.
     */
    KITH_NET_MIN_READ_BUF_MAX = 8u * 1024u,
    /** Default input high watermark (192 KiB); reads pause at this depth. */
    KITH_NET_DEFAULT_IN_HIGH_WATER = 192u * 1024u,
    /** Default input low watermark (96 KiB); reads resume at this depth. */
    KITH_NET_DEFAULT_IN_LOW_WATER = 96u * 1024u,
    /** Default output high watermark (256 KiB); enqueues reject at this depth. */
    KITH_NET_DEFAULT_OUT_HIGH_WATER = 256u * 1024u,
    /** Default output low watermark (128 KiB); writes resume draining. */
    KITH_NET_DEFAULT_OUT_LOW_WATER = 128u * 1024u,
    /**
     * Default per-call output drain cap (64 KiB, two full-budget batch
     * frames); kith_net_conn_write stops past this many bytes written in
     * one call while output remains queued.
     */
    KITH_NET_DEFAULT_OUT_DRAIN_CAP = 64u * 1024u,
};

/**
 * Desired-event bits returned by @c kith_net_conn_events. The reactor polls a
 * connection's fd for the events set here. The underlying type is fixed.
 */
enum kith_net_event : unsigned int
{
    /** The reactor should poll the connection fd for readability. */
    KITH_NET_IN = 0x01u,
    /** The reactor should poll the connection fd for writability. */
    KITH_NET_OUT = 0x02u,
};

/**
 * Opaque transport handle.
 *
 * @ownership callee — created by kith_net_create, destroyed by
 *           kith_net_destroy. Owns the listener fd, the connection table, and
 *           the frame pool. The proto handle is borrowed (not owned).
 */
typedef struct kith_net kith_net_t;

/**
 * Opaque connection handle.
 *
 * @ownership callee — created by kith_net_accept, freed when the last
 *           reference is released. Owns a socket fd, a ring buffer, an output
 *           queue, and backpressure state. The transport owns the connection
 *           table entry; callers interact via references.
 */
typedef struct kith_net_conn kith_net_conn_t;

/**
 * Opaque frame buffer handle (write side). A frame is a contiguous byte buffer
 * backed by the transport's per-class allocation pool. Frames are reference-
 * counted so one frame can be enqueued on many connections for broadcast
 * without per-copy allocation.
 *
 * @ownership callee — created by kith_net_frame_create (refcount 1), released
 *           by kith_net_frame_release. The buffer storage is owned by the frame
 *           and returned to the pool on the final release.
 */
typedef struct kith_net_frame kith_net_frame_t;

/**
 * Creation parameters. Size-versioned: callers set @p size to
 * sizeof(kith_net_params_t) and @p abi_version to KITH_ABI_VERSION at their
 * compile time; the runtime rejects structs from an incompatible generation or
 * an undersized size. A field set to 0 selects the corresponding
 * @c kith_net_default value. Future additive fields occupy the reserved slots
 * so the layout below stays stable across generations.
 */
struct kith_net_params
{
    /** Must be sizeof(kith_net_params_t). */
    uint32_t size;
    /** Must be KITH_ABI_VERSION (from kith/version.h). */
    uint32_t abi_version;

    /** Max simultaneous connections; 0 selects KITH_NET_DEFAULT_MAX_CONNECTIONS. */
    uint32_t max_connections;
    /**
     * Listen backlog; 0 selects SOMAXCONN at runtime (the system maximum for
     * pending unaccepted connections).
     */
    uint32_t listen_backlog;
    /** Initial per-connection read buffer capacity; 0 selects the default. */
    uint32_t read_buffer_initial;
    /**
     * Max per-connection read buffer capacity (grow ceiling); 0 → default.
     * A frame whose declared total exceeds this is rejected with
     * -KITH_EPROTO at header time. When set explicitly, the value must be
     * at least KITH_NET_MIN_READ_BUF_MAX.
     */
    uint32_t read_buffer_max;
    /**
     * Input bytes at which reads pause (high watermark); 0 → default. The
     * pause point lifts to a leading frame's declared total (capped at the
     * read buffer ceiling) so an accepted frame can finish arriving.
     */
    uint32_t in_high_water;
    /** Input bytes at which reads resume (low watermark); 0 → default. */
    uint32_t in_low_water;
    /** Output bytes at which enqueues reject (high watermark); 0 → default. */
    uint32_t out_high_water;
    /** Output bytes at which the queue is considered drained (low); 0 → default. */
    uint32_t out_low_water;
    /**
     * Max bytes kith_net_conn_write pushes per call before stopping; 0
     * selects KITH_NET_DEFAULT_OUT_DRAIN_CAP and UINT32_MAX disables the
     * cap. The first writev of a call always executes, so a normal batch
     * drains in one call; a residual backlog paces across subsequent write
     * calls instead of one unbounded loop on the calling thread.
     */
    uint32_t out_drain_cap;

    /** Reserved for future additive fields. Must be zero-filled. */
    void *reserved[8];
};

/** Alias of struct kith_net_params. */
typedef struct kith_net_params kith_net_params_t;

/**
 * Per-frame callback invoked by @c kith_net_conn_read for each complete frame
 * extracted from the connection's read buffer.
 *
 * The callback runs mid-delivery inside kith_net_conn_read: it must not
 * re-enter kith_net_conn_read on @p conn — a re-entrant read would
 * re-deliver the unconsumed leading frame and corrupt the ring's tail
 * accounting. Closing @p conn from the callback is supported and stops
 * delivery of the remaining buffered frames.
 *
 * @param conn The connection the frame arrived on. The callback may acquire a
 *             reference (kith_net_conn_acquire) or enqueue a response
 *             (kith_net_conn_enqueue) on @p conn.
 * @param frame The decoded proto frame view. @p frame->payload points into the
 *              connection's read staging — contiguous ring bytes, or the
 *              gather buffer when the leading frame spans the ring's wrap
 *              boundary — and is valid ONLY for the duration of the callback.
 *              A callback that needs the payload to outlive the call must copy
 *              it (e.g. into a kith_net_frame_t).
 * @param ctx  The caller context pointer passed to kith_net_conn_read.
 */
typedef void (*kith_net_message_fn)(kith_net_conn_t *conn,
                                    const kith_proto_frame_t *frame,
                                    void *ctx);

/**
 * Build a transport handle from @p params and a borrowed proto handle.
 *
 * @param params  Creation parameters; @c size and @c abi_version must match the
 *                runtime generation. NULL selects all defaults.
 * @param proto   Proto handle borrowed for the transport's lifetime; the
 *                transport calls kith_proto_decode on the read path. Must
 *                outlive the net handle. NULL is an error.
 * @param alloc   Allocator for the handle, its connection table, and every
 *                connection, frame, and buffer the transport allocates, used
 *                again when kith_net_destroy frees them. NULL selects the
 *                default allocator; a supplied allocator is validated (see
 *                kith_allocator_t) and must outlive the handle.
 * @param out_net Receives the new handle on success.
 * @return        0 on success, negative kith_error on failure:
 *                - -KITH_EINVAL if @p out_net or @p proto is NULL, @p alloc
 *                  is missing an operation, or @p params.read_buffer_max is
 *                  set below KITH_NET_MIN_READ_BUF_MAX,
 *                - -KITH_EABIVER if @p params or @p alloc has an
 *                  incompatible abi_version,
 *                - -KITH_ESIZE if @p params or @p alloc has an undersized
 *                  size,
 *                - -KITH_ENOMEM on allocation failure,
 *                - -KITH_ESTATE on mutex/pool initialization failure.
 * @thread_safety unsafe — must not race with another kith_net_create on the
 *                same @p out_net slot.
 * @ownership callee — the returned handle is freed by the caller with
 *           kith_net_destroy.
 */
[[nodiscard]] KITH_API int kith_net_create(const kith_net_params_t *params,
                                           const kith_proto_t *proto,
                                           const kith_allocator_t *alloc,
                                           kith_net_t **out_net);

/**
 * Release a transport handle, closing the listener, closing and releasing every
 * connection, and freeing the frame pool. Passing NULL is a no-op. The borrowed
 * proto handle is not freed.
 *
 * @param net Transport handle. NULL is a no-op.
 * @thread_safety unsafe — no accept/read/write/enqueue may be in flight on any
 *                connection of @p net when this is called.
 * @ownership callee — @p net is consumed and freed by the call.
 */
KITH_API void kith_net_destroy(kith_net_t *net);

/**
 * Bind a TCP listening socket on @p host:@p port and start listening. The
 * socket is non-blocking with close-on-exec set. @p host may be NULL (bind to
 * the wildcard address), a numeric IPv4/IPv6 address, or a host name (resolved
 * via the system resolver). @p port is the TCP port; 0 selects an ephemeral
 * port chosen by the system (queryable via a subsequent accept's connection).
 *
 * May be called at most once per net handle.
 *
 * @param net  Transport handle.
 * @param host Bind address (NULL for wildcard), or a numeric/host name string.
 * @param port TCP port (0 for ephemeral).
 * @return     0 on success, negative kith_error on failure:
 *             - -KITH_EINVAL if @p net is NULL,
 *             - -KITH_ESTATE if already listening,
 *             - -KITH_EIO on resolution, socket, bind, or listen failure.
 * @thread_safety unsafe — call from the composition root before the reactor
 *                starts accepting.
 * @ownership caller — @p host is borrowed for the call only.
 */
[[nodiscard]] KITH_API int kith_net_listen(kith_net_t *net, const char *host, uint16_t port);

/**
 * Return the listener file descriptor, or -1 when not listening. The reactor
 * polls this fd for readability and calls @c kith_net_accept on readiness.
 *
 * @param net Transport handle.
 * @return    The listener file descriptor; -1 when not listening.
 * @thread_safety safe — the fd is fixed after kith_net_listen returns.
 * @ownership caller — @p net is borrowed for the call only.
 */
KITH_API int kith_net_listener_fd(const kith_net_t *net);

/**
 * Accept one pending connection (non-blocking). On success the new connection's
 * fd is non-blocking with close-on-exec and TCP_NODELAY set, and its read
 * buffer and watermarks are initialized from the transport parameters. The
 * connection's initial reference count is 1 (the caller's).
 *
 * @param net      Transport handle.
 * @param out_conn Receives the new connection on success.
 * @return         0 on success, negative kith_error on failure:
 *                 - -KITH_EAGAIN if no pending connection is ready (the reactor
 *                   waits for the next readable event on the listener fd),
 *                 - -KITH_EINVAL if @p net or @p out_conn is NULL,
 *                 - -KITH_EBUSY if the connection table is full,
 *                 - -KITH_EIO on accept failure,
 *                 - -KITH_ENOMEM on allocation failure.
 * @thread_safety unsafe — call from the reactor thread that owns the listener.
 * @ownership callee — the caller releases its reference with
 *           kith_net_conn_release.
 */
[[nodiscard]] KITH_API int kith_net_accept(kith_net_t *net, kith_net_conn_t **out_conn);

/**
 * Return the connection's socket file descriptor, or -1 when the connection is
 * closed. The reactor polls this fd for read/write readiness.
 *
 * @param conn Connection handle.
 * @return     The connection's socket file descriptor; -1 when closed.
 * @thread_safety safe-if (no concurrent close on the same connection — close
 *                sets the fd to -1 without a lock).
 * @ownership caller — @p conn is borrowed for the call only.
 */
KITH_API int kith_net_conn_fd(const kith_net_conn_t *conn);

/**
 * Increment the connection's reference count. Use when handing a connection to
 * a worker that may outlive the reactor's own reference.
 *
 * @param conn Connection handle.
 * @thread_safety safe — atomic refcount.
 * @ownership caller — the caller's reference count is incremented; release it
 *           with kith_net_conn_release.
 */
KITH_API void kith_net_conn_acquire(kith_net_conn_t *conn);

/**
 * Decrement the connection's reference count and free it when the count reaches
 * zero. Passing NULL is a no-op. The connection's socket is closed by
 * @c kith_net_conn_close, not here; release only frees the connection struct
 * and its buffers after the last reference drops.
 *
 * @param conn Connection handle. NULL is a no-op.
 * @thread_safety safe — atomic refcount.
 * @ownership callee — the caller's reference is released; the connection is
 *           freed when the count reaches zero.
 */
KITH_API void kith_net_conn_release(kith_net_conn_t *conn);

/**
 * Close the connection's socket and remove it from the transport's connection
 * table. Idempotent: a no-op on an already-closed or NULL connection. The
 * connection struct is not freed here (references may still exist); pair with
 * @c kith_net_conn_release to drop the caller's reference.
 *
 * After close, the connection's fd is -1 and no further read/write/enqueue
 * calls should be made on it.
 *
 * @param conn Connection handle. NULL or already closed is a no-op.
 * @thread_safety safe-if (closes of different connections, kith_net_accept,
 *                and same-connection read/write/close are externally
 *                serialized — the single-reactor wiring keeps them all on
 *                the reactor thread). The close call itself is idempotent
 *                via an atomic closed flag; the table removal, the fd
 *                field, and the read ring are unsynchronized shared state
 *                behind that condition.
 * @ownership caller — @p conn is borrowed for the call; the socket is closed
 *           but the caller still releases its reference with
 *           kith_net_conn_release.
 */
KITH_API void kith_net_conn_close(kith_net_conn_t *conn);

/**
 * Read available bytes from the connection's socket into its ring buffer, then
 * extract and deliver every complete frame to @p on_message. The read loop
 * drains the socket to EAGAIN/EWOULDBLOCK (suitable for edge-triggered
 * reactors). Delivered frames are views into the connection's read staging,
 * valid only for the duration of each callback. A callback that closes the
 * connection stops delivery of the remaining buffered frames; the read still
 * returns 0. A frame whose declared total exceeds the connection's read
 * buffer ceiling can never complete: the read rejects it with -KITH_EPROTO
 * as soon as its header is buffered and closes the connection.
 *
 * Reads pause at the input high watermark, lifted to a leading frame's
 * declared total (capped at the read buffer ceiling), so an accepted frame
 * can finish arriving.
 *
 * On a fatal return the connection is closed (fd closed, removed from the
 * table); the caller releases its reference.
 *
 * @param conn       Connection handle.
 * @param on_message Per-frame callback. NULL is an error.
 * @param ctx        Caller context passed to the callback.
 * @return           0 on success (socket drained; zero or more frames
 *                   delivered),
 *                   - -KITH_ECONNRESET if the peer closed or reset the
 *                     connection (connection closed),
 *                   - -KITH_EPROTO if a malformed frame was detected or a
 *                     frame's declared total exceeds the read buffer
 *                     ceiling (connection closed),
 *                   - -KITH_EIO on a read error (connection closed),
 *                   - -KITH_ENOMEM if the connection's input buffer
 *                     ceiling is exhausted (connection closed),
 *                   - -KITH_ESTATE if the connection is already closed,
 *                   - -KITH_EINVAL if @p conn or @p on_message is NULL.
 * @thread_safety unsafe — the read path (ring buffer + decode) is not
 *                synchronized; call from one thread (the reactor thread that
 *                owns the connection).
 * @ownership caller — @p ctx is borrowed for the call.
 */
[[nodiscard]] KITH_API int
kith_net_conn_read(kith_net_conn_t *conn, kith_net_message_fn on_message, void *ctx);

/**
 * Allocate a frame buffer of at least @p len bytes from the transport's per-
 * class pool. The frame's used length is initialized to @p len and its
 * reference count to 1. The pool rounds @p len up to the nearest size class;
 * oversize allocations (above the largest class) are served by a direct
 * allocation. Under AddressSanitizer the pool is bypassed for clean leak
 * reporting.
 *
 * @param net Transport handle (owns the pool).
 * @param len Minimum buffer capacity in bytes.
 * @return    A frame with refcount 1, or NULL if @p net is NULL or allocation
 *            fails.
 * @thread_safety safe — the pool is mutex-protected.
 * @ownership callee — the caller releases the frame with
 *           kith_net_frame_release.
 */
KITH_API kith_net_frame_t *kith_net_frame_create(kith_net_t *net, uint32_t len);

/**
 * Return a pointer to the frame's writable byte buffer. The buffer capacity is
 * at least the @p len passed to @c kith_net_frame_create.
 *
 * @param frame Frame buffer handle.
 * @return      A pointer to the frame's writable byte buffer.
 * @thread_safety unsafe — the caller owns the frame and must not write to the
 *                buffer after the frame is enqueued.
 * @ownership callee — the returned pointer aliases the frame's buffer and is
 *           valid while the frame is held; the caller must not free it.
 */
KITH_API void *kith_net_frame_data(kith_net_frame_t *frame);

/**
 * Return the frame's used length in bytes (the count the caller wrote).
 *
 * @param frame Frame buffer handle.
 * @return      The frame's used length in bytes.
 * @thread_safety safe — read-only.
 * @ownership caller — @p frame is borrowed for the call only.
 */
KITH_API uint32_t kith_net_frame_len(const kith_net_frame_t *frame);

/**
 * Set the frame's used length. Call after writing fewer bytes than the frame
 * was created with; the default used length is the create-time @p len.
 *
 * @param frame Frame buffer handle.
 * @param len   New used length in bytes.
 * @thread_safety unsafe — must not race with enqueue or write.
 * @ownership caller — @p frame is borrowed for the call and modified.
 */
KITH_API void kith_net_frame_set_len(kith_net_frame_t *frame, uint32_t len);

/**
 * Increment the frame's reference count. Use when broadcasting one frame to
 * many connections.
 *
 * @param frame Frame buffer handle.
 * @thread_safety safe — atomic refcount.
 * @ownership caller — the caller's reference count is incremented; release it
 *           with kith_net_frame_release.
 */
KITH_API void kith_net_frame_acquire(kith_net_frame_t *frame);

/**
 * Decrement the frame's reference count and return it to the pool when the
 * count reaches zero. Passing NULL is a no-op.
 *
 * @param frame Frame buffer handle. NULL is a no-op.
 * @thread_safety safe — atomic refcount; the pool is mutex-protected.
 * @ownership callee — the caller's reference is released; the frame is returned
 *           to the pool when the count reaches zero.
 */
KITH_API void kith_net_frame_release(kith_net_frame_t *frame);

/**
 * Enqueue a frame on the connection's output queue for a subsequent write. The queue
 * acquires its own reference to @p frame (the caller still owns its reference
 * and must release it separately). Enqueue is rejected when the output queue is
 * at the high watermark (backpressure).
 *
 * @param conn  Connection handle.
 * @param frame Frame to enqueue (refcount incremented on success). NULL is an
 *              error.
 * @return      0 on success, negative kith_error on failure:
 *              - -KITH_EAGAIN if the output queue is at the high watermark
 *                (the caller should defer or drop the frame and retry after the
 *                next write drains the queue),
 *              - -KITH_EINVAL if @p conn or @p frame is NULL,
 *              - -KITH_ESTATE if the connection is already closed at the
 *                check; a close concurrent with the call can still let
 *                the frame enqueue (returning 0) — that frame is never
 *                written and is released when the connection is freed,
 *              - -KITH_ENOMEM on allocation failure.
 * @thread_safety safe — the output queue is protected by a per-connection
 *                mutex.
 * @ownership caller — @p frame is borrowed for the call; the queue acquires its
 *           own reference (the caller still owns its reference and must release
 *           it separately).
 */
[[nodiscard]] KITH_API int kith_net_conn_enqueue(kith_net_conn_t *conn, kith_net_frame_t *frame);

/**
 * Flush the connection's output queue via @c writev, batching contiguous frame
 * regions into one syscall. Frames fully written are released; partially
 * written frames retain their offset for the next call.
 *
 * The drain is bounded by the transport's @c out_drain_cap: once the bytes
 * written this call reach the cap, the call stops with any residual backlog
 * still queued (the cap never prevents the first writev, so progress is
 * guaranteed per call). The residual paces across subsequent calls — with a
 * level-triggered reactor, OUT readiness refires as long as output remains.
 * Each call the cap truncates increments the transport's deferral counter
 * (readable via kith_net_write_deferrals); a stop on kernel backpressure
 * (EAGAIN) is a distinct condition and is not counted.
 *
 * @param conn Connection handle.
 * @return     0 on success (the socket drained to EAGAIN or the queue emptied;
 *             check @c kith_net_conn_events for whether more bytes remain
 *             queued),
 *             - -KITH_ECONNRESET if the peer reset the connection (closed),
 *             - -KITH_EIO on a write error (connection closed),
 *             - -KITH_ESTATE if the connection is closed,
 *             - -KITH_EINVAL if @p conn is NULL.
 * @thread_safety safe — the output queue is protected by a per-connection
 *                mutex.
 * @ownership caller — @p conn is borrowed for the call only.
 */
[[nodiscard]] KITH_API int kith_net_conn_write(kith_net_conn_t *conn);

/**
 * Read the monotonic count of kith_net_conn_write calls that the transport's
 * @c out_drain_cap truncated with output still queued. Each such call wrote
 * its budget and left the residual queued for the next write call, so the
 * count reads as the pacing engagement rate: zero means every drain finished
 * within its pass, a rising count means drains are being paced across
 * reactor iterations. Kernel-backpressure stops are not counted. The counter
 * is monotonic and never reset. The composition root records its delta as
 * @c kith_net_write_deferrals_total.
 *
 * @param net           Transport handle. NULL is an error.
 * @param out_deferrals Receives the monotonic deferral count on success.
 * @return              0 on success, negative kith_error on failure:
 *                      - -KITH_EINVAL if @p net or @p out_deferrals is NULL.
 * @thread_safety safe — the counter is atomic; readable from any thread.
 *                Incremented on the calling thread at truncation time.
 * @ownership caller — @p out_deferrals is the caller's output storage.
 */
[[nodiscard]] KITH_API int kith_net_write_deferrals(const kith_net_t *net, uint64_t *out_deferrals);

/**
 * Read the monotonic count of connections closed because a frame's declared
 * total exceeded the input ring ceiling (@c rb_max). A declared total beyond
 * the ceiling can never complete, so the connection is closed with
 * -KITH_EPROTO instead of buffering an unbounded frame; the count reads as
 * the transport-side malformed-input rejection rate. Frames that arrive and
 * decode (including decode rejections, counted by the proto handle) are not
 * counted here. The counter is monotonic and never reset. The composition
 * root records its delta as @c kith_net_rejections_total; with the proto
 * rejection counter it reconciles the total malformed input a run injected
 * against what the two planes rejected.
 *
 * @param net            Transport handle. NULL is an error.
 * @param out_rejections Receives the monotonic rejection count on success.
 * @return               0 on success, negative kith_error on failure:
 *                       - -KITH_EINVAL if @p net or @p out_rejections is
 *                         NULL.
 * @thread_safety safe — the counter is atomic; readable from any thread.
 *                Incremented on the reactor thread at close time.
 * @ownership caller — @p out_rejections is the caller's output storage.
 */
[[nodiscard]] KITH_API int kith_net_rejections(const kith_net_t *net, uint64_t *out_rejections);

/**
 * Return the desired-event bits for @p conn. The reactor polls the connection's
 * fd for the events set here: @c KITH_NET_IN is clear when reads are paused by
 * backpressure (input ring buffer at/above the pause point — the high
 * watermark, lifted to the leading frame's declared total and capped at the
 * read buffer ceiling), and
 * @c KITH_NET_OUT is set when the output queue is non-empty. Call after each
 * read/write/enqueue to update the reactor's registrations.
 *
 * @param conn Connection handle.
 * @return     The desired-event bits for @p conn.
 * @thread_safety safe — the output queue is locked and the input watermark is
 *                read atomically.
 * @ownership caller — @p conn is borrowed for the call only.
 */
KITH_API unsigned int kith_net_conn_events(const kith_net_conn_t *conn);

/** @} */

#endif /* KITH_NET_NET_H */
