/* Connection lifecycle for the net module: socket setup, non-blocking reads
 * into a ring buffer, frame extraction, and batched writev flushes. Sits
 * below net.c (the public entry) and uses ringbuf.c for partial-read storage
 * and frame.c for frame allocation. */

#include "conn.h"

#include <errno.h>
#include <stdatomic.h>
#include <stdint.h>
#include <string.h>

#include <fcntl.h>
#include <unistd.h>

#include "kith/proto/proto.h"
#include "kith/types.h"

#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/uio.h>

#ifndef IOV_MAX
#define IOV_MAX 1024
#endif

/* Per-read scratch buffer for decoding a frame that spans the ring-buffer wrap
 * boundary. Sized to the largest header + a typical payload; oversized frames
 * fall back to a heap scratch buffer. */
#define KITH_NET_READ_SCRATCH 256u

/* Bytes read per recv() syscall on the read path. Sized to drain a typical
 * socket in one pass. */
#define KITH_NET_READ_CHUNK 8192u

static_assert(KITH_NET_READ_CHUNK == KITH_NET_MIN_READ_BUF_MAX,
              "read chunk must match the read buffer ceiling floor");

/* Max writev iovecs per flush pass. Bounds the on-stack iovec array. */
#define KITH_NET_WRITE_BATCH 64u

static bool set_nonblocking_cloexec(int fd)
{
    int flags = fcntl(fd, F_GETFL);
    if (flags < 0)
    {
        return false;
    }
    if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0)
    {
        return false;
    }
    int fdflags = fcntl(fd, F_GETFD);
    if (fdflags < 0)
    {
        return false;
    }
    if (fcntl(fd, F_SETFD, fdflags | FD_CLOEXEC) < 0)
    {
        return false;
    }
    return true;
}

static bool set_tcp_nodelay(int fd)
{
    int opt = 1;
    return setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt)) == 0;
}

struct kith_net_conn *
kith_conn_create(kith_net_t *net, int fd, uint32_t conn_id, const struct kith_conn_cfg *cfg)
{
    if (net == nullptr || fd < 0 || cfg == nullptr)
    {
        return nullptr;
    }

    if (!set_nonblocking_cloexec(fd))
    {
        return nullptr;
    }
    (void)set_tcp_nodelay(fd);

    const kith_allocator_t *allocator = kith_net_allocator(net);
    struct kith_net_conn *c = kith_alloc_zero(allocator, 1, sizeof(*c));
    if (c == nullptr)
    {
        return nullptr;
    }
    c->allocator = allocator;
    c->net = net;
    c->fd = fd;
    c->conn_id = conn_id;
    c->table_index = UINT32_MAX;
    c->cfg = *cfg;

    if (kith_ringbuf_init(&c->inbuf, allocator, c->cfg.rb_initial, c->cfg.rb_max) != 0)
    {
        kith_free(c->allocator, c);
        return nullptr;
    }
    if (pthread_mutex_init(&c->outq_lock, nullptr) != 0)
    {
        kith_ringbuf_free(&c->inbuf);
        kith_free(c->allocator, c);
        return nullptr;
    }

    atomic_init(&c->read_blocked, false);
    atomic_init(&c->closed, false);
    atomic_init(&c->refcount, 1);
    c->outq_head = nullptr;
    c->outq_tail = nullptr;
    c->outq_bytes = 0u;
    return c;
}

static void outq_drain(struct kith_net_conn *c)
{
    struct kith_net_out_item *item = c->outq_head;
    while (item != nullptr)
    {
        struct kith_net_out_item *next = item->next;
        kith_frame_release(item->frame);
        kith_free(c->allocator, item);
        item = next;
    }
    c->outq_head = nullptr;
    c->outq_tail = nullptr;
    c->outq_bytes = 0u;
}

void kith_conn_destroy(struct kith_net_conn *c)
{
    if (c == nullptr)
    {
        return;
    }
    if (c->fd >= 0)
    {
        (void)close(c->fd);
        c->fd = -1;
    }
    kith_ringbuf_free(&c->inbuf);
    outq_drain(c);
    (void)pthread_mutex_destroy(&c->outq_lock);
    kith_free(c->allocator, c);
}

void kith_net_conn_acquire(kith_net_conn_t *conn)
{
    if (conn == nullptr)
    {
        return;
    }
    (void)atomic_fetch_add_explicit(&conn->refcount, 1, memory_order_relaxed);
}

void kith_net_conn_release(kith_net_conn_t *conn)
{
    if (conn == nullptr)
    {
        return;
    }
    if (atomic_fetch_sub_explicit(&conn->refcount, 1, memory_order_acq_rel) == 1)
    {
        kith_conn_destroy(conn);
    }
}

int kith_net_conn_fd(const kith_net_conn_t *conn)
{
    if (conn == nullptr)
    {
        return -1;
    }
    return conn->fd;
}

void kith_net_conn_close(kith_net_conn_t *conn)
{
    if (conn == nullptr)
    {
        return;
    }
    bool was = atomic_exchange_explicit(&conn->closed, true, memory_order_acq_rel);
    if (was)
    {
        return;
    }

    int fd = conn->fd;
    conn->fd = -1;
    if (fd >= 0)
    {
        (void)close(fd);
    }
    kith_net_t *net = conn->net;
    if (net != nullptr && conn->table_index != UINT32_MAX)
    {
        kith_net_table_remove(net, conn->table_index);
        conn->table_index = UINT32_MAX;
    }
}

/* Update read_blocked based on the input ring-buffer depth and return the
 * desired IN event bit. Called under no lock (read path is single-threaded). */
static unsigned int desired_in_events(const struct kith_net_conn *c)
{
    if (atomic_load_explicit(&c->read_blocked, memory_order_acquire))
    {
        return 0u;
    }
    return KITH_NET_IN;
}

/* Declared wire total of the leading buffered frame, or 0 when the ring
 * holds no complete header. A header spanning the wrap boundary is gathered
 * from the ring before the query. */
static size_t leading_frame_total(const struct kith_net_conn *c)
{
    void *peek_ptr = nullptr;
    const size_t contiguous = kith_ringbuf_peek(&c->inbuf, &peek_ptr);
    if (contiguous >= KITH_PROTO_HDR_SIZE)
    {
        size_t total = 0;
        if (kith_proto_declared_total(peek_ptr, contiguous, &total) != 0)
        {
            return 0u;
        }
        return total;
    }
    if (kith_ringbuf_used(&c->inbuf) < KITH_PROTO_HDR_SIZE)
    {
        return 0u;
    }
    uint8_t hdr[KITH_PROTO_HDR_SIZE];
    (void)kith_ringbuf_copy(&c->inbuf, hdr, sizeof(hdr));
    size_t total = 0;
    if (kith_proto_declared_total(hdr, sizeof(hdr), &total) != 0)
    {
        return 0u;
    }
    return total;
}

/* Decide the outcome of an EAGAIN decode when the leading bytes carry a
 * complete header: an incomplete frame is legitimate, but one whose declared
 * total exceeds the ring ceiling can never complete, so it is rejected
 * instead of held. Frees the gather scratch either way. */
static int finish_partial_frame(const struct kith_net_conn *c,
                                const void *decode_buf,
                                size_t decode_len,
                                uint8_t *heap_scratch)
{
    size_t total = 0;
    const bool overflow = decode_len >= KITH_PROTO_HDR_SIZE &&
                          kith_proto_declared_total(decode_buf, decode_len, &total) == 0 &&
                          total > (size_t)c->cfg.rb_max;
    kith_free(c->allocator, heap_scratch);
    if (overflow)
    {
        kith_net_count_rejection(c->net);
        return kith_error_return(KITH_EPROTO);
    }
    return 0;
}

/* Pause threshold for the pending read: the high watermark, lifted to the
 * leading frame's declared total (capped at the ring ceiling) so an accepted
 * frame can always finish arriving. */
static size_t input_pause_at(const struct kith_net_conn *c)
{
    size_t pause_at = c->cfg.in_high_water;
    const size_t total = leading_frame_total(c);
    if (total > pause_at)
    {
        pause_at = total;
    }
    if (pause_at > (size_t)c->cfg.rb_max)
    {
        pause_at = (size_t)c->cfg.rb_max;
    }
    return pause_at;
}

static int recv_into_ringbuf(struct kith_net_conn *c)
{
    uint8_t chunk[KITH_NET_READ_CHUNK];
    for (;;)
    {
        const ssize_t n = recv(c->fd, chunk, sizeof(chunk), 0);
        if (n < 0)
        {
            if (errno == EAGAIN || errno == EWOULDBLOCK)
            {
                return 0;
            }
            if (errno == EINTR)
            {
                continue;
            }
            return kith_error_return(KITH_EIO);
        }
        if (n == 0)
        {
            return kith_error_return(KITH_ECONNRESET);
        }
        if (kith_ringbuf_write(&c->inbuf, chunk, (size_t)n) != 0)
        {
            /* The read-chunk floor on the ring ceiling guarantees a first
             * chunk lands, so a failure leaves the leading header
             * buffered: a declared total beyond the ceiling is a protocol
             * violation, anything else is input exhaustion. */
            if (leading_frame_total(c) > (size_t)c->cfg.rb_max)
            {
                kith_net_count_rejection(c->net);
                return kith_error_return(KITH_EPROTO);
            }
            return kith_error_return(KITH_ENOMEM);
        }
    }
}

/* Drive the proto decoder against the leading bytes of the ring buffer. When
 * the leading frame spans the wrap boundary, the bytes are gathered into a
 * contiguous scratch buffer first (zero-copy when contiguous, one copy when
 * wrapped). Each decoded frame is delivered to @p on_message; on a malformed
 * frame (-KITH_EPROTO) the connection is closed. Returns 0 on success, a
 * negative kith_error on a fatal condition. */
static int deliver_frames(struct kith_net_conn *c, kith_net_message_fn on_message, void *ctx)
{
    const kith_proto_t *proto = kith_net_proto(c->net);

    for (;;)
    {
        /* A callback that closed the connection ends delivery: no buffered
         * frame is decoded or delivered after close. */
        if (atomic_load_explicit(&c->closed, memory_order_acquire))
        {
            return 0;
        }

        void *peek_ptr = nullptr;
        size_t contiguous = kith_ringbuf_peek(&c->inbuf, &peek_ptr);
        if (contiguous == 0u)
        {
            return 0;
        }

        const uint8_t *decode_buf = (const uint8_t *)peek_ptr;
        size_t decode_len = contiguous;
        uint8_t scratch[KITH_NET_READ_SCRATCH];
        uint8_t *heap_scratch = nullptr;

        /* When the leading bytes do not span the full header, decide from the
         * total used count whether to gather. When they do span a header but
         * the frame wraps, gather into a scratch buffer. The proto decoder
         * reports -KITH_EAGAIN when more bytes are needed; gathering is only
         * required when contiguous < total_used AND a full frame may be
         * present. */
        const size_t total_used = kith_ringbuf_used(&c->inbuf);
        if (contiguous < total_used)
        {
            size_t gather_cap = total_used;
            if (gather_cap > KITH_NET_READ_SCRATCH)
            {
                heap_scratch = kith_alloc(c->allocator, gather_cap);
                if (heap_scratch == nullptr)
                {
                    return kith_error_return(KITH_ENOMEM);
                }
                decode_buf = heap_scratch;
            }
            else
            {
                decode_buf = scratch;
                gather_cap = KITH_NET_READ_SCRATCH;
            }
            decode_len = kith_ringbuf_copy(&c->inbuf, (void *)decode_buf, gather_cap);
        }

        kith_proto_frame_t frame;
        size_t consumed = 0;
        int rc = kith_proto_decode(proto, decode_buf, decode_len, &frame, &consumed);

        if (rc == kith_error_return(KITH_EAGAIN))
        {
            return finish_partial_frame(c, decode_buf, decode_len, heap_scratch);
        }
        if (rc != 0)
        {
            kith_free(c->allocator, heap_scratch);
            return rc; /* -KITH_EPROTO or -KITH_EINVAL */
        }

        /* The decoded payload may alias the gather scratch, so the scratch is
         * released only after the callback returns. */
        on_message((kith_net_conn_t *)c, &frame, ctx);

        kith_free(c->allocator, heap_scratch);
        kith_ringbuf_consume(&c->inbuf, consumed);

        /* Update backpressure after consuming a frame. */
        if (kith_ringbuf_used(&c->inbuf) <= c->cfg.in_low_water)
        {
            atomic_store_explicit(&c->read_blocked, false, memory_order_release);
        }
        (void)kith_ringbuf_maybe_shrink(&c->inbuf);
    }
}

int kith_net_conn_read(kith_net_conn_t *conn, kith_net_message_fn on_message, void *ctx)
{
    if (conn == nullptr || on_message == nullptr)
    {
        return kith_error_return(KITH_EINVAL);
    }
    if (atomic_load_explicit(&conn->closed, memory_order_acquire))
    {
        return kith_error_return(KITH_ESTATE);
    }

    int rr = recv_into_ringbuf(conn);
    if (rr != 0)
    {
        kith_net_conn_close(conn);
        return rr;
    }

    int dr = deliver_frames(conn, on_message, ctx);
    if (dr != 0)
    {
        kith_net_conn_close(conn);
        return dr;
    }

    /* Backpressure: pause reads when the input depth reaches the pause
     * point. Delivery drains every complete frame before this check, so
     * on a live connection the depth stays below the leading frame's
     * declared total and the threshold engages only on a closed
     * connection with buffered undelivered bytes; the ring ceiling, not
     * this pause, bounds live growth. */
    if (kith_ringbuf_used(&conn->inbuf) >= input_pause_at(conn))
    {
        atomic_store_explicit(&conn->read_blocked, true, memory_order_release);
    }
    return 0;
}

int kith_net_conn_enqueue(kith_net_conn_t *conn, kith_net_frame_t *frame)
{
    if (conn == nullptr || frame == nullptr)
    {
        return kith_error_return(KITH_EINVAL);
    }
    if (atomic_load_explicit(&conn->closed, memory_order_acquire))
    {
        return kith_error_return(KITH_ESTATE);
    }

    if (pthread_mutex_lock(&conn->outq_lock) != 0)
    {
        return kith_error_return(KITH_ESTATE);
    }

    int rc = 0;
    if (conn->outq_bytes >= conn->cfg.out_high_water)
    {
        rc = kith_error_return(KITH_EAGAIN);
    }
    else
    {
        struct kith_net_out_item *item = kith_alloc_zero(conn->allocator, 1, sizeof(*item));
        if (item == nullptr)
        {
            rc = kith_error_return(KITH_ENOMEM);
        }
        else
        {
            item->frame = frame;
            item->offset = 0u;
            item->next = nullptr;
            if (conn->outq_tail == nullptr)
            {
                conn->outq_head = item;
            }
            else
            {
                conn->outq_tail->next = item;
            }
            conn->outq_tail = item;
            /* Overflow-safe byte accounting on the enqueue path. */
            size_t next_bytes = conn->outq_bytes + frame->len;
            if (next_bytes < conn->outq_bytes)
            {
                next_bytes = SIZE_MAX;
            }
            conn->outq_bytes = next_bytes;
            /* The queue holds its own reference; the caller still owns theirs. */
            atomic_fetch_add_explicit(&frame->refcount, 1, memory_order_relaxed);
        }
    }

    (void)pthread_mutex_unlock(&conn->outq_lock);
    return rc;
}

/* Build a writev iovec batch from the output queue head, up to
 * KITH_NET_WRITE_BATCH entries. Caller holds outq_lock. Returns the iovec
 * count (0 when the head is fully written or the queue is empty). */
static int build_writev_batch(const struct kith_net_conn *conn, struct iovec *iov)
{
    int iov_count = 0;
    for (struct kith_net_out_item *item = conn->outq_head;
         item != nullptr && iov_count < (int)KITH_NET_WRITE_BATCH;
         item = item->next)
    {
        const uint32_t frame_len = item->frame->len;
        const size_t rem = (frame_len > item->offset) ? (frame_len - item->offset) : 0u;
        if (rem == 0u)
        {
            continue;
        }
        iov[iov_count].iov_base = (void *)(item->frame->data + item->offset);
        iov[iov_count].iov_len = rem;
        iov_count++;
    }
    return iov_count;
}

/* Advance the output queue after a writev of @p written bytes: release frames
 * fully written, retain the offset of a partially-written frame, and decrement
 * the queued-byte counter. Caller holds outq_lock. */
static void advance_outq_after_write(struct kith_net_conn *conn, size_t written)
{
    size_t remaining = written;
    while (conn->outq_head != nullptr && remaining > 0u)
    {
        struct kith_net_out_item *head = conn->outq_head;
        const uint32_t frame_len = head->frame->len;
        const size_t rem = (frame_len > head->offset) ? (frame_len - head->offset) : 0u;
        const size_t sent = (rem < remaining) ? rem : remaining;
        head->offset += sent;
        remaining -= sent;

        if (head->offset >= frame_len)
        {
            conn->outq_head = head->next;
            if (conn->outq_head == nullptr)
            {
                conn->outq_tail = nullptr;
            }
            const size_t dec = (conn->outq_bytes >= frame_len) ? frame_len : conn->outq_bytes;
            conn->outq_bytes -= dec;
            kith_frame_release(head->frame);
            kith_free(conn->allocator, head);
        }
        else
        {
            break;
        }
    }
}

/* Run the writev drain loop for one kith_net_conn_write call, with the
 * outq_lock held. The per-call cap bounds this thread's occupancy without
 * preventing progress: the first writev always executes, so a normal batch
 * still drains in one call, while a multi-batch backlog paces across calls
 * instead of looping unbounded on the caller. Sets @p out_capped when the
 * call stopped on the cap. Returns 0 or -KITH_EIO. */
static int conn_write_drain(struct kith_net_conn *conn, bool *out_capped)
{
    const bool bounded = conn->cfg.out_drain_cap != UINT32_MAX;
    uint64_t written_total = 0u;
    *out_capped = false;

    int rc = 0;
    for (;;)
    {
        if (conn->outq_head == nullptr)
        {
            break;
        }

        struct iovec iov[KITH_NET_WRITE_BATCH];
        int iov_count = build_writev_batch(conn, iov);

        if (iov_count == 0)
        {
            break;
        }

        const ssize_t n = writev(conn->fd, iov, iov_count);
        if (n < 0)
        {
            if (errno == EAGAIN || errno == EWOULDBLOCK)
            {
                break;
            }
            if (errno == EINTR)
            {
                continue;
            }
            return kith_error_return(KITH_EIO);
        }
        if (n == 0)
        {
            break;
        }

        advance_outq_after_write(conn, (size_t)n);
        written_total += (uint64_t)n;
        if (bounded && written_total >= (uint64_t)conn->cfg.out_drain_cap)
        {
            *out_capped = true;
            break;
        }
    }
    return rc;
}

int kith_net_conn_write(kith_net_conn_t *conn)
{
    if (conn == nullptr)
    {
        return kith_error_return(KITH_EINVAL);
    }
    if (atomic_load_explicit(&conn->closed, memory_order_acquire))
    {
        return kith_error_return(KITH_ESTATE);
    }
    if (conn->fd < 0)
    {
        return kith_error_return(KITH_ESTATE);
    }

    if (pthread_mutex_lock(&conn->outq_lock) != 0)
    {
        return kith_error_return(KITH_ESTATE);
    }

    bool capped = false;
    int rc = conn_write_drain(conn, &capped);

    /* A cap stop counts as a deferral only when output actually remains:
     * stopping exactly as the queue empties paced nothing. Kernel-bound
     * stops are a distinct condition and stay uncounted. */
    if (capped && conn->outq_head != nullptr)
    {
        kith_net_count_write_deferral(conn->net);
    }

    (void)pthread_mutex_unlock(&conn->outq_lock);

    if (rc != 0)
    {
        kith_net_conn_close(conn);
        return rc;
    }
    return 0;
}

unsigned int kith_net_conn_events(const kith_net_conn_t *conn)
{
    if (conn == nullptr)
    {
        return 0u;
    }

    unsigned int ev = desired_in_events(conn);

    if (pthread_mutex_lock(&((kith_net_conn_t *)conn)->outq_lock) == 0)
    {
        if (conn->outq_head != nullptr)
        {
            ev |= KITH_NET_OUT;
        }
        (void)pthread_mutex_unlock(&((kith_net_conn_t *)conn)->outq_lock);
    }
    return ev;
}

void *kith_net_frame_data(kith_net_frame_t *frame)
{
    if (frame == nullptr)
    {
        return nullptr;
    }
    return frame->data;
}

uint32_t kith_net_frame_len(const kith_net_frame_t *frame)
{
    if (frame == nullptr)
    {
        return 0u;
    }
    return frame->len;
}

void kith_net_frame_set_len(kith_net_frame_t *frame, uint32_t len)
{
    if (frame == nullptr)
    {
        return;
    }
    frame->len = len;
}

void kith_net_frame_acquire(kith_net_frame_t *frame)
{
    if (frame == nullptr)
    {
        return;
    }
    (void)atomic_fetch_add_explicit(&frame->refcount, 1, memory_order_relaxed);
}
