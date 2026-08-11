/* Replication delivery for the gateway: serializes view subjects into
 * fixed-width payloads and dispatches each session's deliver pass to the
 * delivery strategy bound at session creation. Also owns the shared
 * encode-and-enqueue core that the public kith_gateway_deliver_frame and
 * the built-in strategies use to place frames on a connection. The
 * factory-default "full" strategy lives in strategy_full.c. */

#include "gateway/delivery/delivery.h"

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "gateway/delivery/strategy.h"
#include "kith/types.h"

/*---------------------------------------------------------------------------
 * big-endian byte assembly (portable, no <endian.h> dependency)
 *-------------------------------------------------------------------------*/

static void put_be_u64(uint8_t *p, uint64_t v)
{
    p[0] = (uint8_t)(v >> 56u);
    p[1] = (uint8_t)(v >> 48u);
    p[2] = (uint8_t)(v >> 40u);
    p[3] = (uint8_t)(v >> 32u);
    p[4] = (uint8_t)(v >> 24u);
    p[5] = (uint8_t)(v >> 16u);
    p[6] = (uint8_t)(v >> 8u);
    p[7] = (uint8_t)v;
}

static void put_be_u32(uint8_t *p, uint32_t v)
{
    p[0] = (uint8_t)(v >> 24u);
    p[1] = (uint8_t)(v >> 16u);
    p[2] = (uint8_t)(v >> 8u);
    p[3] = (uint8_t)v;
}

/*---------------------------------------------------------------------------
 * serialization
 *-------------------------------------------------------------------------*/

void gateway_delivery_serialize(const kith_gateway_view_subject_t *s, void *out)
{
    uint8_t *buf = out;
    put_be_u64(buf + 0u, s->actor_id);
    put_be_u64(buf + 8u, (uint64_t)s->pos_x);
    put_be_u64(buf + 16u, (uint64_t)s->pos_y);
    put_be_u64(buf + 24u, (uint64_t)s->pos_z);
    put_be_u64(buf + 32u, (uint64_t)s->vel_x);
    put_be_u64(buf + 40u, (uint64_t)s->vel_y);
    put_be_u64(buf + 48u, (uint64_t)s->vel_z);
    put_be_u32(buf + 56u, s->input_tick);
    put_be_u32(buf + 60u, s->update_seq);
    put_be_u32(buf + 64u, (uint32_t)s->level);
}

void gateway_delivery_serialize_event(uint64_t actor_id, unsigned int kind, void *out)
{
    uint8_t *buf = out;
    put_be_u64(buf + 0u, actor_id);
    memset(buf + 8u, 0, 48u);
    put_be_u32(buf + 56u, 0u);
    put_be_u32(buf + 60u, 0u);
    put_be_u32(buf + 64u, GATEWAY_DELIVERY_EVENT_MARKER | (uint32_t)kind);
}

/*---------------------------------------------------------------------------
 * shared encode + enqueue
 *-------------------------------------------------------------------------*/

kith_net_frame_t *gateway_delivery_frame_build(kith_gateway_t *gateway,
                                               uint16_t type_id,
                                               const void *payload,
                                               size_t payload_len)
{
    size_t frame_len = kith_proto_encode(
        gateway->proto, type_id, 0u, 0u, payload, (uint32_t)payload_len, nullptr, 0u);
    if (frame_len == 0u)
    {
        return nullptr;
    }
    kith_net_frame_t *frame = kith_net_frame_create(gateway->net, (uint32_t)frame_len);
    if (!frame)
    {
        return nullptr;
    }
    void *data = kith_net_frame_data(frame);
    size_t written = kith_proto_encode(
        gateway->proto, type_id, 0u, 0u, payload, (uint32_t)payload_len, data, frame_len);
    if (written == 0u || written > (size_t)kith_net_frame_len(frame))
    {
        kith_net_frame_release(frame);
        return nullptr;
    }
    kith_net_frame_set_len(frame, (uint32_t)written);
    return frame;
}

int gateway_delivery_enqueue(kith_gateway_t *gateway,
                             kith_net_conn_t *conn,
                             uint16_t type_id,
                             const void *payload,
                             size_t payload_len)
{
    if (payload_len > UINT32_MAX)
    {
        return kith_error_return(KITH_EOVERFLOW);
    }
    kith_net_frame_t *frame = gateway_delivery_frame_build(gateway, type_id, payload, payload_len);
    if (!frame)
    {
        // The measure stage distinguishes the two failure classes the
        // caller contract enumerates: a payload that does not encode is
        // -KITH_EPROTO, an allocation failure is -KITH_ENOMEM.
        size_t needed = kith_proto_encode(
            gateway->proto, type_id, 0u, 0u, payload, (uint32_t)payload_len, nullptr, 0u);
        return kith_error_return(needed == 0u ? KITH_EPROTO : KITH_ENOMEM);
    }
    int rc = kith_net_conn_enqueue(conn, frame);
    kith_net_frame_release(frame);
    if (rc != 0)
    {
        return rc;
    }
    return 0;
}

[[nodiscard]] KITH_API int kith_gateway_deliver_frame(kith_gateway_t *gateway,
                                                      kith_net_conn_t *conn,
                                                      uint16_t type_id,
                                                      const void *payload,
                                                      size_t payload_len)
{
    if (!gateway || !conn)
    {
        return kith_error_return(KITH_EINVAL);
    }
    if (payload_len > UINT32_MAX)
    {
        return kith_error_return(KITH_EOVERFLOW);
    }
    return gateway_delivery_enqueue(gateway, conn, type_id, payload, payload_len);
}

/*---------------------------------------------------------------------------
 * dispatch
 *-------------------------------------------------------------------------*/

/** Fold one deliver call's result into the gateway's cumulative delivery
 *  totals and the session's own. Runs on whichever thread executed the
 *  deliver call (inline on the reactor thread; on executor worker threads
 *  otherwise), so every field is a relaxed atomic add — the same
 *  one-tick-skew discipline as the executor's deliver_ns_pending
 *  accumulator. This is the only site that sees both delivery paths,
 *  which is why the totals live here rather than at the call sites; the
 *  per-session fold rides it, so per-session sums partition the gateway
 *  totals (every frame counted on the gateway appears on exactly one
 *  session). The session reference is alive across the fold: the caller
 *  holds it (the reactor's tick pass, or the executor job holding the
 *  dispatch reference through the deliver). */
static void gateway_delivery_totals_fold(kith_gateway_t *gateway,
                                         struct kith_gateway_session *session,
                                         const kith_gateway_delivery_stats_t *stats)
{
    atomic_fetch_add_explicit(
        &gateway->delivery_enqueued_total, stats->enqueued, memory_order_relaxed);
    atomic_fetch_add_explicit(
        &gateway->delivery_dropped_total, stats->dropped, memory_order_relaxed);
    atomic_fetch_add_explicit(
        &gateway->delivery_events_enqueued_total, stats->events_enqueued, memory_order_relaxed);
    atomic_fetch_add_explicit(
        &gateway->delivery_suppressed_total, stats->suppressed, memory_order_relaxed);
    atomic_fetch_add_explicit(
        &session->delivery_enqueued_total, stats->enqueued, memory_order_relaxed);
    atomic_fetch_add_explicit(
        &session->delivery_dropped_total, stats->dropped, memory_order_relaxed);
    atomic_fetch_add_explicit(
        &session->delivery_events_enqueued_total, stats->events_enqueued, memory_order_relaxed);
    atomic_fetch_add_explicit(
        &session->delivery_suppressed_total, stats->suppressed, memory_order_relaxed);
}

[[nodiscard]] KITH_API int kith_gateway_deliver(kith_gateway_t *gateway,
                                                kith_gateway_session_t *session,
                                                uint64_t now_ms,
                                                kith_gateway_delivery_stats_t *out_stats)
{
    if (!gateway || !session)
    {
        return kith_error_return(KITH_EINVAL);
    }
    if (!session->view.has_view)
    {
        return kith_error_return(KITH_ESTATE);
    }
    const kith_gateway_delivery_vtable_t *strategy = session->delivery_strategy;
    if (!strategy || !strategy->deliver)
    {
        return kith_error_return(KITH_ESTATE);
    }
    kith_gateway_delivery_stats_t local_stats;
    kith_gateway_delivery_stats_t *stats = out_stats ? out_stats : &local_stats;
    memset(stats, 0, sizeof(*stats));
    const int vrc = strategy->deliver(session->delivery_state,
                                      gateway,
                                      session,
                                      session->view.subjects,
                                      session->view.subject_count,
                                      now_ms,
                                      stats);
    gateway_delivery_totals_fold(gateway, session, stats);
    return vrc;
}
