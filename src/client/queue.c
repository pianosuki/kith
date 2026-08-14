/* Outbound frame ring and inline-payload event ring used by the client. The
 * outbound ring owns per-slot buffers; the event ring overwrites the oldest
 * record when full. Both are pure data structures driven by client.c, which
 * owns the head/tail/count state. */

#include <string.h>

#include "client/client_internal.h"
#include "kith/types.h"
#include "kith/version.h"

/*---------------------------------------------------------------------------
 * outbound frame ring queue
 *-------------------------------------------------------------------------*/

void client_outq_clear(struct client_outq *q, const kith_allocator_t *alloc)
{
    if (!q || !q->slots || q->cap == 0u)
    {
        return;
    }
    while (q->count > 0u)
    {
        struct kith_client_outbound_slot *slot = &q->slots[q->head];
        kith_free(alloc, slot->data);
        slot->data = nullptr;
        slot->len = 0u;
        q->head = (q->head + 1u) % q->cap;
        q->count -= 1u;
    }
    q->tail = q->head;
}

int client_outq_push(struct client_outq *q,
                     const uint8_t *data,
                     uint32_t len,
                     const kith_allocator_t *alloc)
{
    if (!q || !q->slots || q->cap == 0u)
    {
        return kith_error_return(KITH_EINVAL);
    }
    if (len > 0u && !data)
    {
        return kith_error_return(KITH_EINVAL);
    }
    if (q->count >= q->cap)
    {
        return kith_error_return(KITH_EBUSY);
    }

    uint8_t *copy = nullptr;
    if (len > 0u)
    {
        copy = kith_alloc(alloc, len);
        if (!copy)
        {
            return kith_error_return(KITH_ENOMEM);
        }
        memcpy(copy, data, len);
    }

    struct kith_client_outbound_slot *slot = &q->slots[q->tail];
    slot->data = copy;
    slot->len = len;
    q->tail = (q->tail + 1u) % q->cap;
    q->count += 1u;
    return 0;
}

int client_outq_pop(struct client_outq *q,
                    uint8_t *out_buf,
                    uint32_t out_cap,
                    uint32_t *out_len,
                    const kith_allocator_t *alloc)
{
    if (!q || !q->slots || q->cap == 0u)
    {
        return kith_error_return(KITH_EINVAL);
    }
    if (!out_buf || !out_len)
    {
        return kith_error_return(KITH_EINVAL);
    }
    *out_len = 0u;
    if (q->count == 0u)
    {
        return kith_error_return(KITH_EAGAIN);
    }

    struct kith_client_outbound_slot *slot = &q->slots[q->head];
    if (out_cap < slot->len)
    {
        return kith_error_return(KITH_EOVERFLOW);
    }
    if (slot->len > 0u)
    {
        memcpy(out_buf, slot->data, slot->len);
    }
    *out_len = slot->len;
    kith_free(alloc, slot->data);
    slot->data = nullptr;
    slot->len = 0u;
    q->head = (q->head + 1u) % q->cap;
    q->count -= 1u;
    return 0;
}

/*---------------------------------------------------------------------------
 * event ring (inline-payload records, no per-event allocation)
 *-------------------------------------------------------------------------*/

void client_eventq_clear(uint32_t *head, uint32_t *tail, uint32_t *count)
{
    if (!head || !tail || !count)
    {
        return;
    }
    *head = 0u;
    *tail = 0u;
    *count = 0u;
}

int client_eventq_push(struct kith_client_event_record *records,
                       uint32_t cap,
                       uint32_t *head,
                       uint32_t *tail,
                       uint32_t *count,
                       const kith_client_event_t *event)
{
    if (!records || !head || !tail || !count || cap == 0u)
    {
        return kith_error_return(KITH_EINVAL);
    }
    if (!event)
    {
        return kith_error_return(KITH_EINVAL);
    }
    if (event->payload_len > KITH_CLIENT_EVENT_PAYLOAD_MAX)
    {
        return kith_error_return(KITH_EOVERFLOW);
    }

    // Overwrite the oldest record when the ring is full.
    if (*count >= cap)
    {
        *head = (*head + 1u) % cap;
        *count -= 1u;
    }

    struct kith_client_event_record *rec = &records[*tail];
    rec->ts_mono_ns = event->ts_mono_ns;
    rec->type_id = event->type_id;
    rec->payload_len = event->payload_len;
    if (event->payload_len > 0u)
    {
        memcpy(rec->payload, event->payload, event->payload_len);
    }
    if (event->payload_len < KITH_CLIENT_EVENT_PAYLOAD_MAX)
    {
        memset(rec->payload + event->payload_len,
               0,
               KITH_CLIENT_EVENT_PAYLOAD_MAX - event->payload_len);
    }
    *tail = (*tail + 1u) % cap;
    *count += 1u;
    return 0;
}

int client_eventq_drain(struct kith_client_event_record *records,
                        uint32_t cap,
                        uint32_t *head,
                        const uint32_t *tail,
                        uint32_t *count,
                        kith_client_event_t *out_events,
                        size_t out_cap,
                        size_t *out_count)
{
    if (!records || !head || !tail || !count || cap == 0u)
    {
        return kith_error_return(KITH_EINVAL);
    }
    if (!out_events || !out_count || out_cap == 0u)
    {
        return kith_error_return(KITH_EINVAL);
    }
    *out_count = 0u;
    size_t drained = 0u;
    while (drained < out_cap && *count > 0u)
    {
        struct kith_client_event_record *rec = &records[*head];
        kith_client_event_t *out = &out_events[drained];
        out->size = sizeof(*out);
        out->abi_version = KITH_ABI_VERSION;
        out->ts_mono_ns = rec->ts_mono_ns;
        out->type_id = rec->type_id;
        out->payload_len = rec->payload_len;
        memcpy(out->payload, rec->payload, KITH_CLIENT_EVENT_PAYLOAD_MAX);
        memset(out->reserved, 0, sizeof(out->reserved));
        *head = (*head + 1u) % cap;
        *count -= 1u;
        drained += 1u;
    }
    *out_count = drained;
    return 0;
}
