/* Power-of-two byte ring buffer for the net module: growable up to a max
 * size, wrap-aware read and write, and contiguous-view extraction for frame
 * decoding. Used by conn.c to buffer partial reads off a socket. */

#include "ringbuf.h"

#include <stdint.h>
#include <string.h>

/* Round @p x up to the next power of two. Returns @p x unchanged when it is
 * already a power of two. Returns 1 for @p x == 0 (the smallest usable size). */
static size_t next_pow2(size_t x)
{
    if (x <= 1u)
    {
        return 1u;
    }
    size_t p = 1u;
    while (p < x)
    {
        p <<= 1u;
    }
    return p;
}

int kith_ringbuf_init(kith_ringbuf_t *rb,
                      const kith_allocator_t *allocator,
                      size_t initial_size,
                      size_t max_size)
{
    if (rb == nullptr || initial_size == 0u || max_size == 0u || initial_size > max_size)
    {
        return -1;
    }

    initial_size = next_pow2(initial_size);
    max_size = next_pow2(max_size);
    if (initial_size > max_size)
    {
        return -1;
    }

    rb->allocator = allocator;
    rb->buf = kith_alloc(allocator, initial_size);
    if (rb->buf == nullptr)
    {
        return -1;
    }
    rb->size = initial_size;
    rb->mask = initial_size - 1u;
    rb->head = 0u;
    rb->tail = 0u;
    rb->initial_size = initial_size;
    rb->max_size = max_size;
    return 0;
}

void kith_ringbuf_free(kith_ringbuf_t *rb)
{
    if (rb == nullptr)
    {
        return;
    }
    kith_free(rb->allocator, rb->buf);
    rb->buf = nullptr;
    rb->size = 0u;
    rb->mask = 0u;
}

size_t kith_ringbuf_used(const kith_ringbuf_t *rb)
{
    if (rb == nullptr)
    {
        return 0u;
    }
    return rb->head - rb->tail;
}

size_t kith_ringbuf_space(const kith_ringbuf_t *rb)
{
    if (rb == nullptr)
    {
        return 0u;
    }
    return rb->size - kith_ringbuf_used(rb);
}

/* Linearize the wrapped contents into a freshly allocated power-of-two buffer
 * of @p new_size. Returns 0 on success, -1 on allocation failure or when
 * @p new_size exceeds max_size. Caller ensures @p new_size >= used + need. */
static int ringbuf_realloc(kith_ringbuf_t *rb, size_t new_size)
{
    if (new_size > rb->max_size)
    {
        return -1;
    }

    uint8_t *next = kith_alloc(rb->allocator, new_size);
    if (next == nullptr)
    {
        return -1;
    }

    const size_t used = kith_ringbuf_used(rb);
    const size_t idx = rb->tail & rb->mask;

    if (idx + used <= rb->size)
    {
        memcpy(next, rb->buf + idx, used);
    }
    else
    {
        const size_t first = rb->size - idx;
        memcpy(next, rb->buf + idx, first);
        memcpy(next + first, rb->buf, used - first);
    }

    kith_free(rb->allocator, rb->buf);
    rb->buf = next;
    rb->size = new_size;
    rb->mask = new_size - 1u;
    rb->head = used;
    rb->tail = 0u;
    return 0;
}

int kith_ringbuf_write(kith_ringbuf_t *rb, const uint8_t *data, size_t len)
{
    if (rb == nullptr || rb->buf == nullptr || (data == nullptr && len > 0u))
    {
        return -1;
    }
    if (len == 0u)
    {
        return 0;
    }

    if (kith_ringbuf_space(rb) < len)
    {
        const size_t used = kith_ringbuf_used(rb);
        size_t new_size = rb->size;
        while (new_size < used + len)
        {
            new_size <<= 1u;
        }
        if (ringbuf_realloc(rb, new_size) != 0)
        {
            return -1;
        }
    }

    const size_t idx = rb->head & rb->mask;
    const size_t first = rb->size - idx;

    if (first >= len)
    {
        memcpy(rb->buf + idx, data, len);
    }
    else
    {
        memcpy(rb->buf + idx, data, first);
        memcpy(rb->buf, data + first, len - first);
    }

    rb->head += len;
    return 0;
}

size_t kith_ringbuf_peek(const kith_ringbuf_t *rb, void **out_ptr)
{
    if (rb == nullptr || out_ptr == nullptr || rb->buf == nullptr)
    {
        return 0u;
    }
    const size_t used = kith_ringbuf_used(rb);
    if (used == 0u)
    {
        return 0u;
    }

    const size_t idx = rb->tail & rb->mask;
    size_t contiguous = rb->size - idx;
    if (contiguous > used)
    {
        contiguous = used;
    }

    *out_ptr = rb->buf + idx;
    return contiguous;
}

void kith_ringbuf_consume(kith_ringbuf_t *rb, size_t len)
{
    if (rb == nullptr)
    {
        return;
    }
    /* Clamped to the stored byte count so the tail never passes the head:
     * an unchecked tail wraps the unsigned used-accounting into garbage. */
    const size_t used = kith_ringbuf_used(rb);
    rb->tail += len > used ? used : len;
}

int kith_ringbuf_maybe_shrink(kith_ringbuf_t *rb)
{
    if (rb == nullptr || rb->buf == nullptr)
    {
        return -1;
    }
    if (rb->size <= rb->initial_size)
    {
        return 0;
    }

    const size_t used = kith_ringbuf_used(rb);
    if (used > (rb->size / 4u))
    {
        return 0;
    }

    size_t new_size = rb->initial_size;
    while (new_size < (used * 2u))
    {
        new_size <<= 1u;
    }
    if (new_size >= rb->size)
    {
        return 0;
    }

    return ringbuf_realloc(rb, new_size);
}

size_t kith_ringbuf_copy(const kith_ringbuf_t *rb, void *out_dst, size_t cap)
{
    if (rb == nullptr || out_dst == nullptr || rb->buf == nullptr || cap == 0u)
    {
        return 0u;
    }
    const size_t used = kith_ringbuf_used(rb);
    if (used == 0u)
    {
        return 0u;
    }
    const size_t n = (used < cap) ? used : cap;
    const size_t idx = rb->tail & rb->mask;
    const size_t first = (idx + n <= rb->size) ? n : (rb->size - idx);

    uint8_t *dst = (uint8_t *)out_dst;
    memcpy(dst, rb->buf + idx, first);
    if (first < n)
    {
        memcpy(dst + first, rb->buf, n - first);
    }
    return n;
}
