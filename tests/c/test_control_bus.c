#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "control/control_internal.h"

// ---------------------------------------------------------------------------
// test harness
// ---------------------------------------------------------------------------

static int check_cond_ext(bool ok, int line)
{
    if (!ok)
    {
        (void)fprintf(stderr, "control bus: assertion at line %d failed\n", line);
        return 1;
    }
    return 0;
}

#define CHECK(cond) failures += check_cond_ext((cond), __LINE__)

// ---------------------------------------------------------------------------
// event bus tests
// ---------------------------------------------------------------------------

static int test_bus_init_free(void)
{
    int failures = 0;
    struct kith_control_event_bus bus;
    control_event_bus_init(&bus, 16u, nullptr);
    CHECK(bus.capacity == 16u);
    CHECK(bus.records != NULL);
    control_event_bus_free(&bus, nullptr);
    CHECK(bus.records == NULL);
    return failures;
}

static int test_bus_publish_drain(void)
{
    int failures = 0;
    struct kith_control_event_bus bus;
    control_event_bus_init(&bus, 16u, nullptr);

    kith_control_event_t ev = {
        .ts_mono_ns = 12345,
        .type = "test_event",
        .correlation_id = NULL,
        .payload = "hello",
        .payload_len = 5,
    };

    int rc = control_event_bus_publish(&bus, &ev);
    CHECK(rc == 0);

    struct kith_control_event_record records[4];
    uint32_t new_cursor = 0;
    uint32_t n = control_event_bus_drain(&bus, 0u, records, 4u, &new_cursor);
    CHECK(n == 1u);
    CHECK(strcmp(records[0].type, "test_event") == 0);
    CHECK(records[0].ts_mono_ns == 12345u);
    CHECK(records[0].payload_len == 5u);
    CHECK(memcmp(records[0].payload, "hello", 5) == 0);

    control_event_bus_free(&bus, nullptr);
    return failures;
}

static int test_bus_full_reclaims(void)
{
    int failures = 0;
    struct kith_control_event_bus bus;
    control_event_bus_init(&bus, 4u, nullptr);

    kith_control_event_t ev = {
        .ts_mono_ns = 1,
        .type = "ev",
        .correlation_id = NULL,
        .payload = NULL,
        .payload_len = 0,
    };

    // capacity is 4 (ring buffer of 4 records, 3 usable before full)
    for (int i = 0; i < 3; i++)
    {
        int rc = control_event_bus_publish(&bus, &ev);
        CHECK(rc == 0);
    }

    // 4th publish fails (ring is full)
    int rc = control_event_bus_publish(&bus, &ev);
    CHECK(rc == -1);

    // The flush path releases the whole ring when no stream is attached:
    // head sits at 3 after the three publishes, and the released tail
    // frees all three slots for new publishes.
    control_event_bus_release(&bus, 3u);
    CHECK(atomic_load_explicit(&bus.tail, memory_order_acquire) == 3u);
    CHECK(control_event_bus_publish(&bus, &ev) == 0);
    CHECK(control_event_bus_publish(&bus, &ev) == 0);
    CHECK(control_event_bus_publish(&bus, &ev) == 0);
    CHECK(control_event_bus_publish(&bus, &ev) == -1);

    control_event_bus_free(&bus, nullptr);
    return failures;
}

static int test_bus_release_guard(void)
{
    int failures = 0;
    struct kith_control_event_bus bus;
    control_event_bus_init(&bus, 4u, nullptr);

    kith_control_event_t ev = {.ts_mono_ns = 1, .type = "ev"};

    CHECK(control_event_bus_publish(&bus, &ev) == 0);

    // A cursor past the head frees records no stream has read.
    control_event_bus_release(&bus, 2u);
    CHECK(atomic_load_explicit(&bus.tail, memory_order_acquire) == 0u);

    // Advancing to the head is honored, and a stale cursor behind the
    // tail (its slots were reclaimed under it) is refused.
    control_event_bus_release(&bus, 1u);
    CHECK(atomic_load_explicit(&bus.tail, memory_order_acquire) == 1u);
    control_event_bus_release(&bus, 0u);
    CHECK(atomic_load_explicit(&bus.tail, memory_order_acquire) == 1u);

    control_event_bus_free(&bus, nullptr);
    return failures;
}

static int test_bus_wrap_reclaims(void)
{
    int failures = 0;
    struct kith_control_event_bus bus;
    control_event_bus_init(&bus, 4u, nullptr);

    // Fill the ring (head=3), release everything, then publish across the
    // wrap boundary: the next publishes land at indices 3, 0, 1 and the
    // records read back through a wrapped cursor keep publish order.
    kith_control_event_t ev = {.type = "ev"};
    for (int i = 0; i < 3; i++)
    {
        ev.ts_mono_ns = (uint64_t)i + 1;
        CHECK(control_event_bus_publish(&bus, &ev) == 0);
    }
    control_event_bus_release(&bus, 3u);

    ev.ts_mono_ns = 10u;
    CHECK(control_event_bus_publish(&bus, &ev) == 0);
    ev.ts_mono_ns = 11u;
    CHECK(control_event_bus_publish(&bus, &ev) == 0);
    ev.ts_mono_ns = 12u;
    CHECK(control_event_bus_publish(&bus, &ev) == 0);
    ev.ts_mono_ns = 13u;
    CHECK(control_event_bus_publish(&bus, &ev) == -1);

    // head sits at 2 after the failed publish; a release to the wrapped
    // cursor 0 frees the record at index 3, and the next publish takes
    // the head slot.
    control_event_bus_release(&bus, 0u);
    CHECK(atomic_load_explicit(&bus.tail, memory_order_acquire) == 0u);
    ev.ts_mono_ns = 13u;
    CHECK(control_event_bus_publish(&bus, &ev) == 0);

    // Drain from the tail through the wrap: publish order 11, 12, 13.
    struct kith_control_event_record records[4];
    uint32_t new_cursor = 0;
    uint32_t n = control_event_bus_drain(&bus, 0u, records, 4u, &new_cursor);
    CHECK(n == 3u);
    CHECK(records[0].ts_mono_ns == 11u);
    CHECK(records[1].ts_mono_ns == 12u);
    CHECK(records[2].ts_mono_ns == 13u);
    CHECK(new_cursor == 3u);

    control_event_bus_free(&bus, nullptr);
    return failures;
}

static int test_bus_drain_empty(void)
{
    int failures = 0;
    struct kith_control_event_bus bus;
    control_event_bus_init(&bus, 16u, nullptr);

    uint32_t new_cursor = 0;
    uint32_t n = control_event_bus_drain(&bus, 0u, NULL, 16u, &new_cursor);
    CHECK(n == 0u);
    CHECK(new_cursor == 0u);

    control_event_bus_free(&bus, nullptr);
    return failures;
}

static int test_bus_correlation(void)
{
    int failures = 0;
    struct kith_control_event_bus bus;
    control_event_bus_init(&bus, 16u, nullptr);

    uint8_t corr[8] = {0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08};
    kith_control_event_t ev = {
        .ts_mono_ns = 99,
        .type = "correlated",
        .correlation_id = corr,
        .payload = NULL,
        .payload_len = 0,
    };

    CHECK(control_event_bus_publish(&bus, &ev) == 0);

    struct kith_control_event_record records[4];
    uint32_t new_cursor = 0;
    uint32_t n = control_event_bus_drain(&bus, 0u, records, 4u, &new_cursor);
    CHECK(n == 1u);
    CHECK(records[0].has_correlation);
    CHECK(memcmp(records[0].correlation_id, corr, 8) == 0);

    control_event_bus_free(&bus, nullptr);
    return failures;
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------
int main(void)
{
    int failures = 0;
    failures += test_bus_init_free();
    failures += test_bus_publish_drain();
    failures += test_bus_full_reclaims();
    failures += test_bus_release_guard();
    failures += test_bus_wrap_reclaims();
    failures += test_bus_drain_empty();
    failures += test_bus_correlation();
    if (failures)
    {
        (void)fprintf(stderr, "control bus: %d failure(s)\n", failures);
    }
    return failures ? 1 : 0;
}
