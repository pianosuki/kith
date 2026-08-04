#pragma once

#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>

#include <pthread.h>

#include "frame.h"
#include "kith/net/net.h"
#include "kith/proto/proto.h"
#include "kith/types.h"
#include "ringbuf.h"

/* Connection-private definitions and transport accessors.
 *
 * The transport (struct kith_net, defined in net.c) owns the connection table
 * and the proto handle. Connections hold an opaque back-pointer to the
 * transport and reach its proto handle and table via the accessors below, so
 * the connection code does not need the transport's layout. */

/* Resolved per-connection configuration, derived once from kith_net_params_t at
 * transport create time and copied into each connection at accept. */
struct kith_conn_cfg
{
    uint32_t rb_initial;
    uint32_t rb_max;
    uint32_t in_high_water;
    uint32_t in_low_water;
    uint32_t out_high_water;
    uint32_t out_low_water;
    /** Per-call write-drain cap (UINT32_MAX disables). */
    uint32_t out_drain_cap;
};

/* One queued outgoing frame. offset tracks bytes already written so a
 * partially-written frame resumes correctly on the next writev pass. */
struct kith_net_out_item
{
    struct kith_net_frame *frame;
    size_t offset;
    struct kith_net_out_item *next;
};

struct kith_net_conn
{
    const kith_allocator_t *allocator;
    kith_net_t *net;
    int fd;
    uint32_t conn_id;
    uint32_t table_index;

    kith_ringbuf_t inbuf;

    pthread_mutex_t outq_lock;
    struct kith_net_out_item *outq_head;
    struct kith_net_out_item *outq_tail;
    size_t outq_bytes;

    _Atomic bool read_blocked;
    _Atomic bool closed;
    _Atomic int refcount;

    struct kith_conn_cfg cfg;
};

/* Allocate and initialize a connection wrapping @p fd. The fd is made
 * non-blocking with close-on-exec and TCP_NODELAY set. Returns NULL on
 * allocation or initialization failure (the caller closes @p fd on failure). */
struct kith_net_conn *
kith_conn_create(kith_net_t *net, int fd, uint32_t conn_id, const struct kith_conn_cfg *cfg);

/* Free a connection with refcount zero: close the fd if open, free the ring
 * buffer, drain and release the output queue, destroy the mutex. */
void kith_conn_destroy(struct kith_net_conn *conn);

/* Transport accessors (defined in net.c). */

/* Return the proto handle borrowed by the transport (for the read-path decode). */
const kith_proto_t *kith_net_proto(const kith_net_t *net);

/* Return the allocator the transport routes every allocation through. */
const kith_allocator_t *kith_net_allocator(const kith_net_t *net);

/* Remove the connection at @p index from the transport's table (swap-remove). */
void kith_net_table_remove(kith_net_t *net, uint32_t index);

/* Count one drain call that the per-call cap truncated with output still
 * queued, on the transport's aggregate deferral counter. */
void kith_net_count_write_deferral(kith_net_t *net);

/* Count one connection closed because a frame's declared total exceeded the
 * input ring ceiling, on the transport's aggregate rejection counter. */
void kith_net_count_rejection(kith_net_t *net);
