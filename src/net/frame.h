#pragma once

#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>

#include <pthread.h>

#include "kith/types.h"

/* Frame buffer pool: a per-transport allocator for variable-size outgoing
 * frame buffers. Frames are reference-counted (atomic) so one frame can be
 * enqueued on many connections for broadcast without per-copy allocation.
 *
 * The pool caches freed frames in per-size-class free lists (power-of-two
 * buckets from 64 bytes to 8 KiB, each capped) to amortize allocation cost on
 * the high-frequency write path. Oversize allocations (above the largest class)
 * are served by a direct malloc and freed on the final release. Under
 * AddressSanitizer the pool is bypassed so freed memory is not recycled,
 * giving clean leak and use-after-free reports. */

/* Size classes (bytes): 64, 128, 256, 512, 1024, 2048, 4096, 8192. */
#define KITH_FRAME_CLASS_COUNT 8u
/* Max cached frames per size class. */
#define KITH_FRAME_POOL_MAX_PER_CLASS 1024u

struct kith_frame_pool
{
    const kith_allocator_t *allocator;
    pthread_mutex_t lock;
    struct kith_net_frame *heads[KITH_FRAME_CLASS_COUNT];
    uint32_t counts[KITH_FRAME_CLASS_COUNT];
};

/* The frame buffer. data[] is the flexible array tail; the allocation is
 * sizeof(struct kith_net_frame) + cap through the pool's allocator, recorded
 * on the frame so release is self-contained (the frame knows where to return
 * and which allocator frees it when it is not recycled). */
struct kith_net_frame
{
    const kith_allocator_t *allocator;
    _Atomic int refcount;
    uint32_t len;
    uint32_t cap;
    struct kith_frame_pool *pool;
    struct kith_net_frame *pool_next;
    uint8_t data[];
};

/* Initialize @p pool with @p allocator routing every frame allocation and
 * free. Returns 0 on success, -1 on mutex init failure. */
int kith_frame_pool_init(struct kith_frame_pool *pool, const kith_allocator_t *allocator);

/* Free every cached frame in @p pool and zero the free lists. */
void kith_frame_pool_destroy(struct kith_frame_pool *pool);

/* Allocate a frame with at least @p len bytes of buffer capacity. The used
 * length is initialized to @p len and the refcount to 1. Returns NULL on
 * allocation failure. */
struct kith_net_frame *kith_frame_create(struct kith_frame_pool *pool, uint32_t len);

/* Decrement the frame's refcount; on zero, return the frame to its pool (or
 * free it when the pool cache is full or under sanitizers). NULL is a no-op. */
void kith_frame_release(struct kith_net_frame *frame);
