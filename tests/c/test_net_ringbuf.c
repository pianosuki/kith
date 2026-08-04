/* Grow and shrink ladder of the net module's ring buffer, verified through
 * the observable space accounting: writes double the capacity up to the
 * ceiling, and maybe_shrink returns an over-grown buffer toward its initial
 * size once usage drops below a quarter of capacity. Compiles ringbuf.c
 * directly because the buffer is connection-internal and its symbols are
 * hidden from the shared library. */

#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "ringbuf.h"

static int check_cond(bool ok, int line)
{
    if (!ok)
    {
        (void)fprintf(stderr, "net ringbuf: assertion at line %d failed\n", line);
        return 1;
    }
    return 0;
}

#define CHECK(cond) failures += check_cond((cond), __LINE__)

static uint8_t pattern_byte(size_t i)
{
    return (uint8_t)(i * 31u + 7u);
}

static int test_init_validation(void)
{
    int failures = 0;
    kith_ringbuf_t rb;
    CHECK(kith_ringbuf_init(&rb, nullptr, 0u, 1024u) != 0);
    CHECK(kith_ringbuf_init(&rb, nullptr, 2048u, 1024u) != 0);
    CHECK(kith_ringbuf_init(nullptr, nullptr, 1024u, 1024u) != 0);
    CHECK(kith_ringbuf_init(&rb, nullptr, 1024u, 1024u) == 0);
    CHECK(kith_ringbuf_used(&rb) == 0u);
    CHECK(kith_ringbuf_space(&rb) == 1024u);

    /* A buffer at its initial size does not shrink. */
    CHECK(kith_ringbuf_maybe_shrink(&rb) == 0);
    CHECK(kith_ringbuf_space(&rb) == 1024u);
    kith_ringbuf_free(&rb);

    CHECK(kith_ringbuf_maybe_shrink(nullptr) != 0);
    return failures;
}

static int test_grow_and_shrink_ladder(void)
{
    int failures = 0;
    kith_ringbuf_t rb;
    CHECK(kith_ringbuf_init(&rb, nullptr, 1024u, 65536u) == 0);

    /* Two writes force two doublings: 1024 → 2048 → 4096. */
    uint8_t first[2048];
    memset(first, 0xA5u, sizeof(first));
    CHECK(kith_ringbuf_write(&rb, first, sizeof(first)) == 0);
    CHECK(kith_ringbuf_used(&rb) == 2048u);
    CHECK(kith_ringbuf_space(&rb) == 0u);
    uint8_t byte = 0x5Au;
    CHECK(kith_ringbuf_write(&rb, &byte, 1u) == 0);
    CHECK(kith_ringbuf_used(&rb) == 2049u);
    CHECK(kith_ringbuf_space(&rb) == 2047u);

    /* Consuming everything frees no capacity until the shrink runs. */
    kith_ringbuf_consume(&rb, 2049u);
    CHECK(kith_ringbuf_used(&rb) == 0u);
    CHECK(kith_ringbuf_space(&rb) == 4096u);
    CHECK(kith_ringbuf_maybe_shrink(&rb) == 0);
    CHECK(kith_ringbuf_space(&rb) == 1024u);
    kith_ringbuf_free(&rb);
    return failures;
}

static int test_shrink_guard(void)
{
    int failures = 0;
    kith_ringbuf_t rb;
    CHECK(kith_ringbuf_init(&rb, nullptr, 1024u, 65536u) == 0);

    /* Usage above a quarter of capacity holds the grown size. */
    uint8_t chunk[3000];
    memset(chunk, 0x3Cu, sizeof(chunk));
    CHECK(kith_ringbuf_write(&rb, chunk, sizeof(chunk)) == 0);
    CHECK(kith_ringbuf_space(&rb) == 1096u);
    kith_ringbuf_consume(&rb, 1000u);
    CHECK(kith_ringbuf_used(&rb) == 2000u);
    CHECK(kith_ringbuf_maybe_shrink(&rb) == 0);
    CHECK(kith_ringbuf_space(&rb) == 2096u);
    kith_ringbuf_free(&rb);
    return failures;
}

static int test_wrap_coherency_across_shrink(void)
{
    int failures = 0;
    kith_ringbuf_t rb;
    CHECK(kith_ringbuf_init(&rb, nullptr, 1024u, 65536u) == 0);

    /* Grow to 4096, advance the tail, then append across the physical end
     * without growth: the head sits near the buffer end and the write
     * spans it. Draining to a quarter of capacity leaves the surviving
     * bytes wrapped around the tail. */
    uint8_t fill[3500];
    memset(fill, 0x11u, sizeof(fill));
    CHECK(kith_ringbuf_write(&rb, fill, sizeof(fill)) == 0);
    CHECK(kith_ringbuf_space(&rb) == 596u);
    kith_ringbuf_consume(&rb, 500u);
    uint8_t lead[800];
    memset(lead, 0x44u, sizeof(lead));
    CHECK(kith_ringbuf_write(&rb, lead, sizeof(lead)) == 0);
    CHECK(kith_ringbuf_used(&rb) == 3800u);

    /* Drain to 100 bytes and shrink toward the initial size; the
     * survivors are the tail of the wrapped write. */
    kith_ringbuf_consume(&rb, 3700u);
    CHECK(kith_ringbuf_used(&rb) == 100u);
    CHECK(kith_ringbuf_maybe_shrink(&rb) == 0);
    CHECK(kith_ringbuf_used(&rb) == 100u);
    CHECK(kith_ringbuf_space(&rb) == 924u);
    uint8_t out[100];
    CHECK(kith_ringbuf_copy(&rb, out, sizeof(out)) == 100u);
    CHECK(memcmp(out, lead + 700u, sizeof(out)) == 0);
    kith_ringbuf_consume(&rb, 100u);
    CHECK(kith_ringbuf_used(&rb) == 0u);
    CHECK(kith_ringbuf_space(&rb) == 1024u);
    kith_ringbuf_free(&rb);
    return failures;
}

static int test_ceiling_holds(void)
{
    int failures = 0;
    kith_ringbuf_t rb;
    CHECK(kith_ringbuf_init(&rb, nullptr, 1024u, 4096u) == 0);

    uint8_t big[8192];
    memset(big, 0x77u, sizeof(big));
    CHECK(kith_ringbuf_write(&rb, big, sizeof(big)) != 0);
    CHECK(kith_ringbuf_used(&rb) == 0u);
    CHECK(kith_ringbuf_space(&rb) == 1024u);

    /* A write that fits the ceiling but not the current capacity grows
     * exactly to the next power of two at most. */
    uint8_t exact[4096];
    memset(exact, 0x88u, sizeof(exact));
    CHECK(kith_ringbuf_write(&rb, exact, sizeof(exact)) == 0);
    CHECK(kith_ringbuf_used(&rb) == 4096u);
    CHECK(kith_ringbuf_space(&rb) == 0u);
    kith_ringbuf_free(&rb);
    return failures;
}

static int test_consume_beyond_used_clamps(void)
{
    int failures = 0;
    kith_ringbuf_t rb;
    CHECK(kith_ringbuf_init(&rb, nullptr, 1024u, 4096u) == 0);

    uint8_t data[10];
    for (size_t i = 0; i < sizeof(data); ++i)
    {
        data[i] = pattern_byte(i);
    }
    CHECK(kith_ringbuf_write(&rb, data, sizeof(data)) == 0);
    CHECK(kith_ringbuf_used(&rb) == 10u);

    /* An over-long consume drops exactly what is stored: the tail never
     * passes the head, so the used accounting stays coherent. */
    kith_ringbuf_consume(&rb, 100u);
    CHECK(kith_ringbuf_used(&rb) == 0u);

    /* The buffer keeps working normally after the clamped consume. */
    uint8_t again[5];
    for (size_t i = 0; i < sizeof(again); ++i)
    {
        again[i] = pattern_byte(10u + i);
    }
    CHECK(kith_ringbuf_write(&rb, again, sizeof(again)) == 0);
    CHECK(kith_ringbuf_used(&rb) == 5u);
    uint8_t out[5];
    void *ptr = nullptr;
    CHECK(kith_ringbuf_peek(&rb, &ptr) == 5u);
    memcpy(out, ptr, sizeof(out));
    CHECK(out[0] == pattern_byte(10u) && out[4] == pattern_byte(14u));
    kith_ringbuf_consume(&rb, 5u);
    CHECK(kith_ringbuf_used(&rb) == 0u);
    kith_ringbuf_free(&rb);

    /* NULL is a no-op. */
    kith_ringbuf_consume(nullptr, 1u);
    return failures;
}

int main(void)
{
    int rc = 0;
    rc |= test_init_validation();
    rc |= test_grow_and_shrink_ladder();
    rc |= test_shrink_guard();
    rc |= test_wrap_coherency_across_shrink();
    rc |= test_ceiling_holds();
    rc |= test_consume_beyond_used_clamps();
    if (rc != 0)
    {
        (void)fprintf(stderr, "net ringbuf tests FAILED\n");
    }
    return rc;
}
