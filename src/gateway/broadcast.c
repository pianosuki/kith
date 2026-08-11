/* Cell-scoped broadcast queue for the gateway: the submit side accepts a
 * cell key, a message type id, and caller bytes from any thread; the drain
 * runs on the reactor thread inside the tick and fans one encoded frame to
 * every session whose subscription window covers the cell. Siblings:
 * handler.c (message dispatch), delivery.c (composed-stream delivery). */

#include "broadcast.h"

#include <stdbit.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include <pthread.h>

#include "gateway/delivery/delivery.h"
#include "gateway/gateway_internal.h"
#include "gateway/session/session.h"
#include "kith/net/net.h"
#include "kith/proto/proto.h"
#include "kith/types.h"

static_assert(GATEWAY_BROADCAST_QUEUE_CAP <= 64u,
              "the drain matches the batch against each window over one 64-bit mask");

/*---------------------------------------------------------------------------
 * submit
 *-------------------------------------------------------------------------*/

int gateway_broadcast_init(kith_gateway_t *gateway)
{
    if (pthread_mutex_init(&gateway->broadcasts.lock, nullptr) != 0)
    {
        return kith_error_return(KITH_ENOMEM);
    }
    return 0;
}

[[nodiscard]] KITH_API int kith_gateway_broadcast_cell(kith_gateway_t *gateway,
                                                       const kith_fabric_cell_key_t *cell,
                                                       uint16_t type_id,
                                                       const void *payload,
                                                       size_t payload_len)
{
    return gateway_broadcast_submit(gateway, cell, type_id, payload, payload_len);
}

int gateway_broadcast_submit(kith_gateway_t *gateway,
                             const kith_fabric_cell_key_t *cell,
                             uint16_t type_id,
                             const void *payload,
                             size_t payload_len)
{
    if (!gateway || !cell || (!payload && payload_len > 0u))
    {
        return kith_error_return(KITH_EINVAL);
    }
    if (payload_len > UINT32_MAX)
    {
        return kith_error_return(KITH_EOVERFLOW);
    }
    const uint32_t len32 = (uint32_t)payload_len;
    if (kith_proto_encode(gateway->proto, type_id, 0u, 0u, payload, len32, nullptr, 0u) == 0u)
    {
        return kith_error_return(KITH_EPROTO);
    }
    // The copy is taken before the queue check so a filled slot is complete
    // the moment it becomes visible to the drain; a refused submit releases
    // its own copy below.
    void *copy = nullptr;
    if (len32 > 0u)
    {
        copy = kith_alloc(gateway->allocator, len32);
        if (!copy)
        {
            return kith_error_return(KITH_ENOMEM);
        }
        memcpy(copy, payload, len32);
    }
    kith_error_t err = KITH_OK;
    pthread_mutex_lock(&gateway->broadcasts.lock);
    if (gateway->broadcasts.frozen)
    {
        err = KITH_ESTATE;
    }
    else if (gateway->broadcasts.count >= GATEWAY_BROADCAST_QUEUE_CAP)
    {
        atomic_fetch_add_explicit(&gateway->broadcast_refused_total, 1u, memory_order_relaxed);
        err = KITH_EAGAIN;
    }
    else
    {
        struct gateway_broadcast_request *slot =
            &gateway->broadcasts.slots[(gateway->broadcasts.head + gateway->broadcasts.count) %
                                       GATEWAY_BROADCAST_QUEUE_CAP];
        slot->cell = *cell;
        slot->type_id = type_id;
        slot->payload_len = len32;
        slot->payload = copy;
        gateway->broadcasts.count += 1u;
        copy = nullptr;
    }
    pthread_mutex_unlock(&gateway->broadcasts.lock);
    kith_free(gateway->allocator, copy);
    return err == KITH_OK ? 0 : kith_error_return(err);
}

/*---------------------------------------------------------------------------
 * drain
 *-------------------------------------------------------------------------*/

/** Take every pending request out of the queue under one mutex hold. A
 *  frozen queue (teardown in flight) hands over nothing — teardown owns
 *  the release path. @p batch receives the requests in submit order. */
static size_t broadcast_take_locked(kith_gateway_t *gateway,
                                    struct gateway_broadcast_request *batch)
{
    size_t count = 0u;
    pthread_mutex_lock(&gateway->broadcasts.lock);
    if (!gateway->broadcasts.frozen && gateway->broadcasts.count > 0u)
    {
        count = gateway->broadcasts.count;
        for (size_t i = 0u; i < count; ++i)
        {
            batch[i] = gateway->broadcasts
                           .slots[(gateway->broadcasts.head + i) % GATEWAY_BROADCAST_QUEUE_CAP];
        }
        gateway->broadcasts.head = (gateway->broadcasts.head + count) % GATEWAY_BROADCAST_QUEUE_CAP;
        gateway->broadcasts.count = 0u;
    }
    pthread_mutex_unlock(&gateway->broadcasts.lock);
    return count;
}

/** Fan one batch out over the session table: each session's window is read
 *  once under one lock hold and matched against the whole batch, so a
 *  saturated queue costs one lock acquisition per session rather than one
 *  per request. A request's frame is encoded on its first covering session
 *  and reused for the rest; the recipient set is the sessions whose
 *  windows cover the request's cell. Returns the recipient copies that did
 *  not complete, including a request that could not encode (counted once —
 *  no recipient can receive it). */
static uint64_t broadcast_fanout(kith_gateway_t *gateway,
                                 const struct gateway_broadcast_request *batch,
                                 size_t count)
{
    kith_fabric_cell_key_t cells[GATEWAY_BROADCAST_QUEUE_CAP];
    for (size_t i = 0u; i < count; ++i)
    {
        cells[i] = batch[i].cell;
    }
    const struct gateway_session_table *table = &gateway->sessions;
    kith_net_frame_t *frames[GATEWAY_BROADCAST_QUEUE_CAP] = {nullptr};
    bool dead[GATEWAY_BROADCAST_QUEUE_CAP] = {false};
    uint64_t dropped = 0u;
    for (size_t b = 0u; b < table->buckets; ++b)
    {
        struct kith_gateway_session *session = table->slots[b];
        if (!session || !session->conn)
        {
            continue;
        }
        uint64_t mask = gateway_session_window_matches(session, cells, count);
        while (mask != 0u)
        {
            const size_t i = (size_t)stdc_trailing_zeros(mask);
            mask &= mask - 1u;
            if (dead[i])
            {
                continue;
            }
            if (!frames[i])
            {
                frames[i] = gateway_delivery_frame_build(
                    gateway, batch[i].type_id, batch[i].payload, batch[i].payload_len);
                if (!frames[i])
                {
                    dead[i] = true;
                    dropped += 1u;
                    continue;
                }
            }
            if (kith_net_conn_enqueue(session->conn, frames[i]) != 0)
            {
                dropped += 1u;
            }
        }
    }
    for (size_t i = 0u; i < count; ++i)
    {
        if (frames[i])
        {
            kith_net_frame_release(frames[i]);
        }
        kith_free(gateway->allocator, batch[i].payload);
    }
    return dropped;
}

void gateway_broadcast_drain(kith_gateway_t *gateway)
{
    struct gateway_broadcast_request batch[GATEWAY_BROADCAST_QUEUE_CAP];
    const size_t count = broadcast_take_locked(gateway, batch);
    if (count == 0u)
    {
        return;
    }
    const uint64_t dropped = broadcast_fanout(gateway, batch, count);
    if (dropped != 0u)
    {
        atomic_fetch_add_explicit(&gateway->broadcast_dropped_total, dropped, memory_order_relaxed);
    }
}

/*---------------------------------------------------------------------------
 * teardown
 *-------------------------------------------------------------------------*/

void gateway_broadcast_teardown(kith_gateway_t *gateway)
{
    struct gateway_broadcast_request batch[GATEWAY_BROADCAST_QUEUE_CAP];
    size_t count = 0u;
    pthread_mutex_lock(&gateway->broadcasts.lock);
    gateway->broadcasts.frozen = true;
    count = gateway->broadcasts.count;
    for (size_t i = 0u; i < count; ++i)
    {
        batch[i] =
            gateway->broadcasts.slots[(gateway->broadcasts.head + i) % GATEWAY_BROADCAST_QUEUE_CAP];
    }
    gateway->broadcasts.head = 0u;
    gateway->broadcasts.count = 0u;
    pthread_mutex_unlock(&gateway->broadcasts.lock);
    for (size_t i = 0u; i < count; ++i)
    {
        kith_free(gateway->allocator, batch[i].payload);
    }
    if (count != 0u)
    {
        // Accepted requests are never delivered: the recipients are going
        // away with the gateway.
        atomic_fetch_add_explicit(
            &gateway->broadcast_dropped_total, (uint64_t)count, memory_order_relaxed);
    }
}

void gateway_broadcast_fini(kith_gateway_t *gateway)
{
    pthread_mutex_destroy(&gateway->broadcasts.lock);
}
