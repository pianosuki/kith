#pragma once

#include <stddef.h>
#include <stdint.h>

#include "kith/types.h"

/* Power-of-two ring buffer for partial-read byte accumulation.
 *
 * The buffer size is always a power of two so head/tail counters wrap via a
 * bitmask (size - 1) instead of a modulo. head is the write position, tail is
 * the read position; used = head - tail (wraps naturally on unsigned overflow).
 * The buffer grows by doubling when a write exceeds the free space, up to
 * max_size, and shrinks back toward initial_size when usage drops below a
 * quarter of the current capacity.
 *
 * Not synchronized: the connection read path is single-threaded (one reactor
 * thread per connection). */

typedef struct kith_ringbuf
{
    const kith_allocator_t *allocator;
    uint8_t *buf;
    size_t size;
    size_t mask;
    size_t head;
    size_t tail;
    size_t initial_size;
    size_t max_size;
} kith_ringbuf_t;

/* Initialize @p rb with @p initial_size capacity (rounded up to a power of two)
 * and @p max_size ceiling (also rounded). @p allocator is stored on @p rb and
 * routes the buffer and every grow and shrink; NULL selects the default
 * allocator. Returns 0 on success, -1 on invalid args or allocation failure. */
int kith_ringbuf_init(kith_ringbuf_t *rb,
                      const kith_allocator_t *allocator,
                      size_t initial_size,
                      size_t max_size);

/* Free the backing storage. The struct is not owned (the caller owns it). */
void kith_ringbuf_free(kith_ringbuf_t *rb);

/* Bytes currently stored. */
size_t kith_ringbuf_used(const kith_ringbuf_t *rb);

/* Bytes available for writing before a grow is needed. */
size_t kith_ringbuf_space(const kith_ringbuf_t *rb);

/* Append @p len bytes from @p data, growing the buffer if needed (up to
 * max_size). Returns 0 on success, -1 on invalid args, grow failure, or when
 * @p len exceeds max_size. */
int kith_ringbuf_write(kith_ringbuf_t *rb, const uint8_t *data, size_t len);

/* Return the length of the next contiguous readable region and set @p *out_ptr
 * to point at it. The region may be shorter than the total used bytes when the
 * data wraps; call again after consuming to reach the remainder. Returns 0 when
 * empty or invalid. */
size_t kith_ringbuf_peek(const kith_ringbuf_t *rb, void **out_ptr);

/* Advance the read position by @p len bytes (drop consumed data). A @p len
 * beyond the stored byte count drops what is stored; the tail never passes
 * the head. */
void kith_ringbuf_consume(kith_ringbuf_t *rb, size_t len);

/* Shrink the buffer toward initial_size when usage is below a quarter of the
 * current capacity. Returns 0 on success (including no-op), -1 on failure. */
int kith_ringbuf_maybe_shrink(kith_ringbuf_t *rb);

/* Copy up to @p cap bytes of the leading readable region into @p out_dst,
 * returning the byte count copied. When the readable region wraps, both halves
 * are gathered into the contiguous destination. Returns 0 when empty or invalid.
 * Does not consume; pair with kith_ringbuf_consume to drop copied bytes. */
size_t kith_ringbuf_copy(const kith_ringbuf_t *rb, void *out_dst, size_t cap);
