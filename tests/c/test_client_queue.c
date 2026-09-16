/* Fault-path units for the client-side rings, compiled against
 * src/client/queue.c: argument guards, full-queue and small-output-buffer
 * refusals, ring-index wrap-around, zero-length frames, slot ownership on
 * pop and clear, oldest-record overwrite on the event ring, partial and
 * empty drains, and the payload-length ceiling. */

#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "client/client_internal.h"
#include "kith/types.h"
#include "kith/version.h"

// ---------------------------------------------------------------------------
// test harness
// ---------------------------------------------------------------------------

static int check_cond(bool ok, int line)
{
    if (!ok)
    {
        (void)fprintf(stderr, "client queue: assertion at line %d failed\n", line);
        return 1;
    }
    return 0;
}

#define CHECK(cond) failures += check_cond((cond), __LINE__)

// The fixtures own their slot storage the way a handle's create does; the
// ring helpers receive the default allocator, matching a NULL-alloc create.
static void outq_init(struct client_outq *q, uint32_t cap)
{
    q->slots = calloc(cap, sizeof(*q->slots));
    q->cap = cap;
    q->head = 0u;
    q->tail = 0u;
    q->count = 0u;
}

static void outq_teardown(struct client_outq *q)
{
    client_outq_clear(q, kith_allocator_default());
    free(q->slots);
    q->slots = nullptr;
    q->cap = 0u;
}

struct eventq_fixture
{
    struct kith_client_event_record *records;
    uint32_t cap;
    uint32_t head;
    uint32_t tail;
    uint32_t count;
};

static void eventq_init(struct eventq_fixture *q, uint32_t cap)
{
    q->records = calloc(cap, sizeof(*q->records));
    q->cap = cap;
    q->head = 0u;
    q->tail = 0u;
    q->count = 0u;
}

static void eventq_teardown(struct eventq_fixture *q)
{
    free(q->records);
    q->records = nullptr;
    q->cap = 0u;
    q->head = q->tail = q->count = 0u;
}

static int push_frame(struct client_outq *q, const char *text)
{
    return client_outq_push(
        q, (const uint8_t *)text, (uint32_t)strlen(text), kith_allocator_default());
}

static int pop_frame(struct client_outq *q, uint8_t *out, uint32_t out_cap, uint32_t *out_len)
{
    return client_outq_pop(q, out, out_cap, out_len, kith_allocator_default());
}

// ---------------------------------------------------------------------------
// outbound ring
// ---------------------------------------------------------------------------

// Null descriptors, absent storage, and a frame with length but no data
// pointer are refused everywhere.
static int test_outq_argument_guards(void)
{
    int failures = 0;
    struct client_outq q;
    outq_init(&q, 4u);
    struct client_outq empty = {0};
    uint32_t len = 0u;

    CHECK(client_outq_push(nullptr, (const uint8_t *)"x", 1u, kith_allocator_default()) ==
          kith_error_return(KITH_EINVAL));
    CHECK(client_outq_push(&empty, (const uint8_t *)"x", 1u, kith_allocator_default()) ==
          kith_error_return(KITH_EINVAL));
    CHECK(client_outq_push(&q, nullptr, 1u, kith_allocator_default()) ==
          kith_error_return(KITH_EINVAL));

    CHECK(client_outq_pop(nullptr, (uint8_t *)&len, 4u, &len, kith_allocator_default()) ==
          kith_error_return(KITH_EINVAL));
    CHECK(client_outq_pop(&empty, (uint8_t *)&len, 4u, &len, kith_allocator_default()) ==
          kith_error_return(KITH_EINVAL));
    CHECK(client_outq_pop(&q, nullptr, 4u, &len, kith_allocator_default()) ==
          kith_error_return(KITH_EINVAL));
    CHECK(client_outq_pop(&q, (uint8_t *)&len, 4u, nullptr, kith_allocator_default()) ==
          kith_error_return(KITH_EINVAL));

    /* Clear tolerates a null descriptor and absent storage as silent
     * no-ops. */
    client_outq_clear(nullptr, kith_allocator_default());
    client_outq_clear(&empty, kith_allocator_default());
    CHECK(q.count == 0u);

    outq_teardown(&q);
    return failures;
}

// Pushing past the capacity reports EBUSY and leaves the ring unchanged;
// popping an empty ring reports EAGAIN without touching the outputs.
static int test_outq_full_and_empty(void)
{
    int failures = 0;
    struct client_outq q;
    outq_init(&q, 2u);
    uint8_t sink[64];
    uint32_t len = 99u;

    CHECK(push_frame(&q, "one") == 0);
    CHECK(push_frame(&q, "two") == 0);
    CHECK(push_frame(&q, "three") == kith_error_return(KITH_EBUSY));
    /* The indices live modulo cap: a full two-slot ring has tail wrapped
     * back onto head. */
    CHECK(q.count == 2u && q.head == 0u && q.tail == 0u);

    /* Drain both frames, then confirm the empty ring answers EAGAIN with
     * a zeroed length output. */
    len = 99u;
    CHECK(client_outq_pop(&q, sink, sizeof(sink), &len, kith_allocator_default()) == 0);
    CHECK(client_outq_pop(&q, sink, sizeof(sink), &len, kith_allocator_default()) == 0);
    len = 7u;
    CHECK(client_outq_pop(&q, sink, sizeof(sink), &len, kith_allocator_default()) ==
          kith_error_return(KITH_EAGAIN));
    CHECK(len == 0u);

    outq_teardown(&q);
    return failures;
}

// A pop whose output buffer cannot hold the front frame reports EOVERFLOW
// and leaves the slot allocated for a larger pop.
static int test_outq_small_output_buffer(void)
{
    int failures = 0;
    struct client_outq q;
    outq_init(&q, 2u);
    uint8_t sink[64];
    uint32_t len = 0u;

    CHECK(push_frame(&q, "ten-bytes!") == 0);

    CHECK(pop_frame(&q, sink, 4u, &len) == kith_error_return(KITH_EOVERFLOW));
    CHECK(q.count == 1u);
    CHECK(q.slots[q.head].data != nullptr);
    CHECK(q.slots[q.head].len == 10u);

    CHECK(pop_frame(&q, sink, sizeof(sink), &len) == 0);
    CHECK(len == 10u);
    CHECK(memcmp(sink, "ten-bytes!", 10u) == 0);

    outq_teardown(&q);
    return failures;
}

// Interleaved pushes and pops drive the indices through the modulo
// wrap-around several times while preserving FIFO order and contents.
static int test_outq_wraparound_fifo(void)
{
    int failures = 0;
    struct client_outq q;
    outq_init(&q, 3u);
    uint8_t sink[64];
    uint32_t len = 0u;
    char label[32];

    for (int round = 0; round < 8; round++)
    {
        (void)snprintf(label, sizeof(label), "frame-%d", round);
        CHECK(push_frame(&q, label) == 0);
        if (round >= 2)
        {
            CHECK(pop_frame(&q, sink, sizeof(sink), &len) == 0);
            (void)snprintf(label, sizeof(label), "frame-%d", round - 2);
            CHECK(memcmp(sink, label, strlen(label)) == 0);
        }
    }
    CHECK(q.count == 2u);
    CHECK(q.head < q.cap);

    outq_teardown(&q);
    return failures;
}

// A zero-length frame stores no allocation, pops back with length zero,
// and frees cleanly like any other slot; clear releases every occupied
// slot and resets the indices.
static int test_outq_zero_len_and_clear(void)
{
    int failures = 0;
    struct client_outq q;
    outq_init(&q, 3u);
    uint8_t sink[64];
    uint32_t len = 5u;

    CHECK(client_outq_push(&q, nullptr, 0u, kith_allocator_default()) == 0);
    CHECK(push_frame(&q, "data") == 0);
    CHECK(q.count == 2u);

    CHECK(pop_frame(&q, sink, sizeof(sink), &len) == 0);
    CHECK(len == 0u);
    CHECK(q.slots[0].data == nullptr && q.slots[0].len == 0u);

    outq_teardown(&q);
    CHECK(q.count == 0u && q.head == q.tail);

    outq_init(&q, 3u);
    CHECK(push_frame(&q, "a") == 0);
    CHECK(push_frame(&q, "bb") == 0);
    CHECK(push_frame(&q, "ccc") == 0);
    outq_teardown(&q);
    CHECK(q.count == 0u && q.head == q.tail);
    return failures;
}

// ---------------------------------------------------------------------------
// event ring
// ---------------------------------------------------------------------------

// Argument guards: null storage or state pointers, a zero capacity, and a
// null event are refused; a payload above the inline maximum is refused.
static int test_eventq_argument_guards(void)
{
    int failures = 0;
    struct eventq_fixture q;
    eventq_init(&q, 4u);
    kith_client_event_t event = {.size = sizeof(event),
                                 .abi_version = KITH_ABI_VERSION,
                                 .ts_mono_ns = 1u,
                                 .type_id = 7u,
                                 .payload_len = 2u,
                                 .payload = "hi"};

    CHECK(client_eventq_push(nullptr, 4u, &q.head, &q.tail, &q.count, &event) ==
          kith_error_return(KITH_EINVAL));
    CHECK(client_eventq_push(q.records, 0u, &q.head, &q.tail, &q.count, &event) ==
          kith_error_return(KITH_EINVAL));
    CHECK(client_eventq_push(q.records, 4u, &q.head, &q.tail, &q.count, nullptr) ==
          kith_error_return(KITH_EINVAL));

    kith_client_event_t huge = event;
    huge.payload_len = KITH_CLIENT_EVENT_PAYLOAD_MAX + 1u;
    CHECK(client_eventq_push(q.records, 4u, &q.head, &q.tail, &q.count, &huge) ==
          kith_error_return(KITH_EOVERFLOW));

    kith_client_event_t outs[2] = {{0}};
    size_t out_count = 0u;
    CHECK(client_eventq_drain(nullptr, 4u, &q.head, &q.tail, &q.count, outs, 2u, &out_count) ==
          kith_error_return(KITH_EINVAL));
    CHECK(client_eventq_drain(q.records, 4u, &q.head, &q.tail, &q.count, outs, 0u, &out_count) ==
          kith_error_return(KITH_EINVAL));
    CHECK(client_eventq_drain(q.records, 4u, &q.head, &q.tail, &q.count, nullptr, 2u, &out_count) ==
          kith_error_return(KITH_EINVAL));
    CHECK(client_eventq_drain(q.records, 4u, &q.head, &q.tail, &q.count, outs, 2u, nullptr) ==
          kith_error_return(KITH_EINVAL));

    client_eventq_clear(nullptr, &q.tail, &q.count);
    client_eventq_clear(&q.head, &q.tail, nullptr);

    eventq_teardown(&q);
    return failures;
}

// When the ring is full the oldest record is overwritten in place: four
// publishes into a three-slot ring leave the last three in order.
static int test_eventq_overwrite_oldest(void)
{
    int failures = 0;
    struct eventq_fixture q;
    eventq_init(&q, 3u);

    for (uint16_t i = 1u; i <= 4u; i++)
    {
        kith_client_event_t event = {
            .size = sizeof(event), .abi_version = KITH_ABI_VERSION, .ts_mono_ns = i, .type_id = i};
        CHECK(client_eventq_push(q.records, q.cap, &q.head, &q.tail, &q.count, &event) == 0);
    }
    CHECK(q.count == 3u);

    kith_client_event_t outs[3] = {{0}};
    size_t out_count = 0u;
    CHECK(client_eventq_drain(q.records, q.cap, &q.head, &q.tail, &q.count, outs, 3u, &out_count) ==
          0);
    CHECK(out_count == 3u);
    CHECK(outs[0].type_id == 2u && outs[1].type_id == 3u && outs[2].type_id == 4u);
    CHECK(outs[0].size == sizeof(kith_client_event_t));
    CHECK(outs[0].abi_version == KITH_ABI_VERSION);

    eventq_teardown(&q);
    return failures;
}

// A drain smaller than the ring leaves the remaining records queued in
// order; draining an empty ring reports success with a zero count; clear
// resets all indices.
static int test_eventq_partial_drain_empty_clear(void)
{
    int failures = 0;
    struct eventq_fixture q;
    eventq_init(&q, 4u);

    kith_client_event_t outs[4] = {{0}};
    size_t out_count = 99u;
    CHECK(client_eventq_drain(q.records, q.cap, &q.head, &q.tail, &q.count, outs, 4u, &out_count) ==
          0);
    CHECK(out_count == 0u);

    for (uint16_t i = 1u; i <= 3u; i++)
    {
        kith_client_event_t event = {.size = sizeof(event),
                                     .abi_version = KITH_ABI_VERSION,
                                     .ts_mono_ns = i,
                                     .type_id = i,
                                     .payload_len = 3u,
                                     .payload = "abc"};
        CHECK(client_eventq_push(q.records, q.cap, &q.head, &q.tail, &q.count, &event) == 0);
    }

    out_count = 0u;
    CHECK(client_eventq_drain(q.records, q.cap, &q.head, &q.tail, &q.count, outs, 2u, &out_count) ==
          0);
    CHECK(out_count == 2u);
    CHECK(outs[0].payload_len == 3u && memcmp(outs[0].payload, "abc", 3u) == 0);
    CHECK(q.count == 1u);

    out_count = 0u;
    CHECK(client_eventq_drain(q.records, q.cap, &q.head, &q.tail, &q.count, outs, 4u, &out_count) ==
          0);
    CHECK(out_count == 1u);
    CHECK(outs[0].type_id == 3u);

    client_eventq_clear(&q.head, &q.tail, &q.count);
    CHECK(q.head == 0u && q.tail == 0u && q.count == 0u);

    eventq_teardown(&q);
    return failures;
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------
int main(void)
{
    int failures = 0;
    failures += test_outq_argument_guards();
    failures += test_outq_full_and_empty();
    failures += test_outq_small_output_buffer();
    failures += test_outq_wraparound_fifo();
    failures += test_outq_zero_len_and_clear();
    failures += test_eventq_argument_guards();
    failures += test_eventq_overwrite_oldest();
    failures += test_eventq_partial_drain_empty_clear();
    if (failures)
    {
        (void)fprintf(stderr, "client queue: %d failure(s)\n", failures);
    }
    return failures ? 1 : 0;
}
