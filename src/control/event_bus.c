/* Lock-free single-producer/single-consumer ring for control-plane SSE events:
 * publish records an event with optional correlation id, and drain copies from
 * a cursor to a caller buffer. Backs the /events/stream and /logs/stream
 * handlers in handlers.c; the bus is owned by the handle in control.c. */

#include <stdlib.h>
#include <string.h>

#include "control/control_internal.h"

void control_event_bus_init(struct kith_control_event_bus *bus,
                            uint32_t capacity,
                            const kith_allocator_t *alloc)
{
    bus->capacity = capacity;
    bus->records = kith_alloc_zero(alloc, capacity, sizeof(struct kith_control_event_record));
    atomic_init(&bus->head, 0u);
    atomic_init(&bus->tail, 0u);
}

void control_event_bus_free(struct kith_control_event_bus *bus, const kith_allocator_t *alloc)
{
    kith_free(alloc, bus->records);
    bus->records = NULL;
    bus->capacity = 0u;
}

int control_event_bus_publish(struct kith_control_event_bus *bus, const kith_control_event_t *event)
{
    const uint32_t head = atomic_load_explicit(&bus->head, memory_order_relaxed);
    const uint32_t next = (head + 1u) % bus->capacity;
    const uint32_t tail = atomic_load_explicit(&bus->tail, memory_order_acquire);

    if (next == tail)
    {
        return -1;
    }

    struct kith_control_event_record *rec = &bus->records[head];
    rec->ts_mono_ns = event->ts_mono_ns;

    if (event->type != NULL)
    {
        strncpy(rec->type, event->type, CONTROL_EVENT_MAX_TYPE - 1u);
        rec->type[CONTROL_EVENT_MAX_TYPE - 1u] = '\0';
    }
    else
    {
        rec->type[0] = '\0';
    }

    if (event->correlation_id != NULL)
    {
        memcpy(rec->correlation_id, event->correlation_id, 8u);
        rec->has_correlation = true;
    }
    else
    {
        rec->has_correlation = false;
    }

    uint32_t plen = event->payload_len;
    if (plen > CONTROL_EVENT_MAX_PAYLOAD)
    {
        plen = CONTROL_EVENT_MAX_PAYLOAD;
    }
    rec->payload_len = plen;
    if (plen > 0u && event->payload != NULL)
    {
        memcpy(rec->payload, event->payload, plen);
    }

    atomic_store_explicit(&bus->head, next, memory_order_release);
    return 0;
}

uint32_t control_event_bus_drain(struct kith_control_event_bus *bus,
                                 uint32_t cursor,
                                 struct kith_control_event_record *out,
                                 uint32_t out_cap,
                                 uint32_t *out_new_cursor)
{
    const uint32_t head = atomic_load_explicit(&bus->head, memory_order_acquire);
    uint32_t count = 0u;
    uint32_t idx = cursor;

    while (count < out_cap && idx != head)
    {
        if (out != NULL)
        {
            memcpy(&out[count], &bus->records[idx], sizeof(struct kith_control_event_record));
        }
        count++;
        idx = (idx + 1u) % bus->capacity;
    }

    if (out_new_cursor != NULL)
    {
        *out_new_cursor = idx;
    }
    return count;
}

void control_event_bus_release(struct kith_control_event_bus *bus, uint32_t cursor)
{
    const uint32_t capacity = bus->capacity;
    const uint32_t tail = atomic_load_explicit(&bus->tail, memory_order_acquire);
    const uint32_t head = atomic_load_explicit(&bus->head, memory_order_acquire);

    // Advance only within the live window [tail, head]: a cursor behind
    // tail is stale (its slots were reclaimed under it) and one past head
    // frees records no stream has read. The distances are mod
    // capacity, so the comparison survives wraparound.
    const uint32_t live = (head + capacity - tail) % capacity;
    if ((cursor + capacity - tail) % capacity > live)
    {
        return;
    }
    atomic_store_explicit(&bus->tail, cursor, memory_order_release);
}
