#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include "kith/coord/coord.h"
#include "kith/fabric/fabric.h"
#include "kith/types.h"
#include "kith/version.h"

static int check_cond(bool ok, int line)
{
    if (!ok)
    {
        (void)fprintf(stderr, "coord bus: assertion at line %d failed\n", line);
        return 1;
    }
    return 0;
}

#define CHECK(cond) failures += check_cond((cond), __LINE__)

// A NULL out slot is rejected with EINVAL. NULL params selects all defaults
// and builds a working bus with one member (the local instance). destroy
// (NULL) is a no-op.
static int test_bus_create_arg_validation(void)
{
    int failures = 0;
    CHECK(kith_coord_bus_create(nullptr, nullptr, nullptr) == kith_error_return(KITH_EINVAL));

    kith_coord_bus_t *bus = nullptr;
    CHECK(kith_coord_bus_create(nullptr, nullptr, &bus) == 0);
    CHECK(bus != nullptr);
    CHECK(kith_coord_bus_member_count(bus) == 1u);

    kith_coord_bus_destroy(bus);
    kith_coord_bus_destroy(nullptr);
    return failures;
}

// instance_id and member_count reject a NULL handle by returning 0. A
// freshly-created loopback bus has one member whose status matches the
// local instance_id. member_status rejects a NULL handle, a NULL out, and
// an out-of-range index with EINVAL and ERANGE respectively. tick
// refreshes the local member's heartbeat and rejects a NULL handle.
static int test_bus_membership(void)
{
    int failures = 0;
    kith_coord_bus_t *bus = nullptr;
    CHECK(kith_coord_bus_create(nullptr, nullptr, &bus) == 0);

    CHECK(kith_coord_bus_instance_id(nullptr) == 0u);
    CHECK(kith_coord_bus_member_count(nullptr) == 0u);
    CHECK(kith_coord_bus_member_count(bus) == 1u);

    kith_coord_bus_member_status_t st = {0};
    CHECK(kith_coord_bus_member_status(nullptr, 0u, &st) == kith_error_return(KITH_EINVAL));
    CHECK(kith_coord_bus_member_status(bus, 0u, nullptr) == kith_error_return(KITH_EINVAL));
    CHECK(kith_coord_bus_member_status(bus, 1u, &st) == kith_error_return(KITH_ERANGE));
    CHECK(kith_coord_bus_member_status(bus, 0u, &st) == 0);
    CHECK(st.instance_id == kith_coord_bus_instance_id(bus));

    CHECK(kith_coord_bus_tick(nullptr, 1000u) == kith_error_return(KITH_EINVAL));
    CHECK(kith_coord_bus_tick(bus, 5000u) == 0);
    CHECK(kith_coord_bus_member_status(bus, 0u, &st) == 0);
    CHECK(st.heartbeat_ms == 5000u);

    kith_coord_bus_destroy(bus);
    return failures;
}

// subscribe and unsubscribe are refcounted: subscribing the same zone twice
// increments the refcount without creating a second entry. Unsubscribing
// once decrements the refcount but retains the entry; the final unsubscribe
// evicts it. Unsubscribing a zone with no subscription is idempotent.
// NULL handle is rejected with EINVAL.
static int test_bus_subscribe_refcount(void)
{
    int failures = 0;
    kith_coord_bus_t *bus = nullptr;
    CHECK(kith_coord_bus_create(nullptr, nullptr, &bus) == 0);

    CHECK(kith_coord_bus_subscribe(nullptr, 1u) == kith_error_return(KITH_EINVAL));
    CHECK(kith_coord_bus_unsubscribe(nullptr, 1u) == kith_error_return(KITH_EINVAL));

    CHECK(kith_coord_bus_subscribe(bus, 1u) == 0);
    CHECK(kith_coord_bus_subscribe(bus, 1u) == 0);
    CHECK(kith_coord_bus_subscribe(bus, 2u) == 0);

    CHECK(kith_coord_bus_unsubscribe(bus, 1u) == 0);
    CHECK(kith_coord_bus_unsubscribe(bus, 1u) == 0);
    // Idempotent unsubscribe on a zone whose refcount already reached zero.
    CHECK(kith_coord_bus_unsubscribe(bus, 1u) == 0);
    // Idempotent unsubscribe on a never-subscribed zone.
    CHECK(kith_coord_bus_unsubscribe(bus, 99u) == 0);
    CHECK(kith_coord_bus_unsubscribe(bus, 2u) == 0);

    kith_coord_bus_destroy(bus);
    return failures;
}

// publish copies the payload into the bus's internal queue; drain swaps the
// pending queue into the drained queue and copies event headers into the
// caller's buffer. The payload pointer in each event borrows the bus's
// internal storage and is valid until the next drain. Events are returned in
// publish order. A NULL handle is rejected with EINVAL. A zero-payload
// publish stores an event with a NULL payload and payload_len 0.
static int test_bus_publish_drain_roundtrip(void)
{
    int failures = 0;
    kith_coord_bus_t *bus = nullptr;
    CHECK(kith_coord_bus_create(nullptr, nullptr, &bus) == 0);

    CHECK(kith_coord_bus_publish(nullptr, KITH_COORD_BUS_EVENT_REBALANCE, 1u, nullptr, 0u) ==
          kith_error_return(KITH_EINVAL));

    uint32_t payload_a = 0xAABBCCDDu;
    uint32_t payload_b = 0x11223344u;
    CHECK(kith_coord_bus_publish(
              bus, KITH_COORD_BUS_EVENT_REBALANCE, 1u, &payload_a, sizeof(payload_a)) == 0);
    CHECK(kith_coord_bus_publish(bus, KITH_COORD_BUS_EVENT_MEMBERSHIP, 1u, nullptr, 0u) == 0);
    CHECK(kith_coord_bus_publish(
              bus, KITH_COORD_BUS_EVENT_SNAPSHOT_REQUEST, 2u, &payload_b, sizeof(payload_b)) == 0);

    kith_coord_bus_event_t events[4] = {0};
    size_t count = 99u;
    CHECK(kith_coord_bus_drain(bus, events, 4u, &count) == 0);
    CHECK(count == 3u);

    CHECK(events[0].event_type == KITH_COORD_BUS_EVENT_REBALANCE);
    CHECK(events[0].zone == 1u);
    CHECK(events[0].payload_len == sizeof(payload_a));
    if (events[0].payload != nullptr)
    {
        CHECK(memcmp(events[0].payload, &payload_a, sizeof(payload_a)) == 0);
    }
    else
    {
        CHECK(false);
    }

    CHECK(events[1].event_type == KITH_COORD_BUS_EVENT_MEMBERSHIP);
    CHECK(events[1].zone == 1u);
    CHECK(events[1].payload_len == 0u);
    CHECK(events[1].payload == nullptr);

    CHECK(events[2].event_type == KITH_COORD_BUS_EVENT_SNAPSHOT_REQUEST);
    CHECK(events[2].zone == 2u);
    CHECK(events[2].payload_len == sizeof(payload_b));
    if (events[2].payload != nullptr)
    {
        CHECK(memcmp(events[2].payload, &payload_b, sizeof(payload_b)) == 0);
    }
    else
    {
        CHECK(false);
    }

    // A second drain with no new publishes returns zero events.
    count = 99u;
    CHECK(kith_coord_bus_drain(bus, events, 4u, &count) == 0);
    CHECK(count == 0u);

    kith_coord_bus_destroy(bus);
    return failures;
}

// Count-only drain reports the pending count without copying. It still
// swaps the pending queue into the drained queue, so a subsequent copy
// drain on the same pending set returns zero.
static int test_bus_drain_count_only(void)
{
    int failures = 0;
    kith_coord_bus_t *bus = nullptr;
    CHECK(kith_coord_bus_create(nullptr, nullptr, &bus) == 0);

    uint32_t payload_a = 0xAABBCCDDu;
    CHECK(kith_coord_bus_publish(
              bus, KITH_COORD_BUS_EVENT_REBALANCE, 1u, &payload_a, sizeof(payload_a)) == 0);
    CHECK(kith_coord_bus_publish(bus, KITH_COORD_BUS_EVENT_MEMBERSHIP, 1u, nullptr, 0u) == 0);

    kith_coord_bus_event_t events[4] = {0};
    size_t count = 99u;
    CHECK(kith_coord_bus_drain(bus, nullptr, 0u, &count) == 0);
    CHECK(count == 2u);

    // The count-only drain consumed the pending queue.
    count = 99u;
    CHECK(kith_coord_bus_drain(bus, events, 4u, &count) == 0);
    CHECK(count == 0u);

    kith_coord_bus_destroy(bus);
    return failures;
}

// A drain into a buffer too small copies up to max; the events beyond max
// are moved into the drained queue and freed on the next drain, so a
// follow-up drain reports zero pending events.
static int test_bus_drain_small_buffer(void)
{
    int failures = 0;
    kith_coord_bus_t *bus = nullptr;
    CHECK(kith_coord_bus_create(nullptr, nullptr, &bus) == 0);

    uint32_t payload_a = 0xAABBCCDDu;
    uint32_t payload_b = 0x11223344u;
    CHECK(kith_coord_bus_publish(
              bus, KITH_COORD_BUS_EVENT_REBALANCE, 1u, &payload_a, sizeof(payload_a)) == 0);
    CHECK(kith_coord_bus_publish(
              bus, KITH_COORD_BUS_EVENT_MEMBERSHIP, 1u, &payload_b, sizeof(payload_b)) == 0);

    kith_coord_bus_event_t events[4] = {0};
    size_t count = 99u;
    CHECK(kith_coord_bus_drain(bus, events, 1u, &count) == 0);
    CHECK(count == 1u);
    CHECK(events[0].event_type == KITH_COORD_BUS_EVENT_REBALANCE);

    // The second event was moved to drained, not pending; the next drain
    // frees it and reports zero.
    count = 99u;
    CHECK(kith_coord_bus_drain(bus, events, 4u, &count) == 0);
    CHECK(count == 0u);

    kith_coord_bus_destroy(bus);
    return failures;
}

// add_member extends the membership table beyond the single local instance
// the bus creates with. A NULL handle or an instance_id of 0 is rejected
// with EINVAL. Adding an instance_id already present is rejected with
// EEXIST. Each successful add grows member_count and is visible via
// member_status, preserving the local member at index 0.
static int test_bus_add_member(void)
{
    int failures = 0;
    kith_coord_bus_t *bus = nullptr;
    CHECK(kith_coord_bus_create(nullptr, nullptr, &bus) == 0);
    CHECK(kith_coord_bus_member_count(bus) == 1u);

    CHECK(kith_coord_bus_add_member(nullptr, 2u) == kith_error_return(KITH_EINVAL));
    CHECK(kith_coord_bus_add_member(bus, 0u) == kith_error_return(KITH_EINVAL));

    CHECK(kith_coord_bus_add_member(bus, 2u) == 0);
    CHECK(kith_coord_bus_member_count(bus) == 2u);
    CHECK(kith_coord_bus_add_member(bus, 7u) == 0);
    CHECK(kith_coord_bus_member_count(bus) == 3u);

    // The local instance (id 0 by default) stays at index 0; added members
    // follow in insertion order.
    kith_coord_bus_member_status_t st = {0};
    CHECK(kith_coord_bus_member_status(bus, 0u, &st) == 0);
    CHECK(st.instance_id == 0u);
    CHECK(kith_coord_bus_member_status(bus, 1u, &st) == 0);
    CHECK(st.instance_id == 2u);
    CHECK(kith_coord_bus_member_status(bus, 2u, &st) == 0);
    CHECK(st.instance_id == 7u);

    // Duplicate instance_id is rejected without growing the table.
    CHECK(kith_coord_bus_add_member(bus, 2u) == kith_error_return(KITH_EEXIST));
    CHECK(kith_coord_bus_add_member(bus, 7u) == kith_error_return(KITH_EEXIST));
    CHECK(kith_coord_bus_member_count(bus) == 3u);

    kith_coord_bus_destroy(bus);
    return failures;
}

int main(void)
{
    int rc = 0;
    rc |= test_bus_create_arg_validation();
    rc |= test_bus_membership();
    rc |= test_bus_add_member();
    rc |= test_bus_subscribe_refcount();
    rc |= test_bus_publish_drain_roundtrip();
    rc |= test_bus_drain_count_only();
    rc |= test_bus_drain_small_buffer();
    if (rc != 0)
    {
        (void)fprintf(stderr, "coord bus tests FAILED\n");
    }
    return rc;
}
