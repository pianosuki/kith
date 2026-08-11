/* Factory-default "full" delivery strategy: every deliver pass resends the
 * session's complete composed view set as state-complete absolute records.
 * When the gateway is configured with
 * a non-zero replication_batch_type_id the set is packed into one
 * multi-subject frame (4-byte count header + N records); otherwise each
 * subject is encoded as a separate frame using replication_type_id. A pass
 * with both type ids zero reports -KITH_ESTATE (delivery encoding
 * disabled). Frames rejected by output backpressure are counted as
 * dropped; the next pass self-heals by resending everything.
 *
 * Membership events ride the same frames: the composer's
 * per-rebuild delta is consumed each pass, serialized as marked sentinel
 * records ordered before the state records, and kept in a transactional
 * backlog until successfully enqueued — an event dropped by backpressure
 * retries next pass, since absence is only unambiguous when the exit
 * signal itself gets through. */

#include <stdckdint.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "gateway/delivery/delivery.h"
#include "gateway/delivery/executor.h"
#include "gateway/delivery/strategy.h"
#include "gateway/view/view.h"
#include "kith/types.h"
#include "kith/version.h"

/** Transactional backlog of membership events not yet confirmed enqueued.
 *  Bounded at four times the view budget plus slack; overflow drops the
 *  newest event, never an undelivered exit. */
struct full_state
{
    /** Allocator copied from the session at init; the state and its
     *  backlog allocate and free through it (session_fini receives only
     *  the state, so the copy is what the fini releases through). */
    const kith_allocator_t *allocator;
    struct gateway_view_event *pending;
    size_t pending_count;
    size_t pending_cap;
};

static size_t full_backlog_cap(const kith_gateway_t *gateway)
{
    return (size_t)gateway->view_max_subjects * 4u + 8u;
}

/*---------------------------------------------------------------------------
 * big-endian byte assembly (portable, no <endian.h> dependency)
 *-------------------------------------------------------------------------*/

static void put_be_u16(uint8_t *p, uint16_t v)
{
    p[0] = (uint8_t)(v >> 8u);
    p[1] = (uint8_t)v;
}

/*---------------------------------------------------------------------------
 * per-subject path
 *-------------------------------------------------------------------------*/

static int full_deliver_one(kith_gateway_t *gateway,
                            kith_net_conn_t *conn,
                            const kith_gateway_view_subject_t *s,
                            uint32_t *enqueued,
                            uint32_t *dropped)
{
    uint8_t payload[GATEWAY_DELIVERY_PAYLOAD_SIZE];
    gateway_delivery_serialize(s, payload);
    int rc = gateway_delivery_enqueue(
        gateway, conn, gateway->replication_type_id, payload, GATEWAY_DELIVERY_PAYLOAD_SIZE);
    if (rc != 0)
    {
        *dropped += 1u;
        return 0;
    }
    *enqueued += 1u;
    return 0;
}

/*---------------------------------------------------------------------------
 * batch path
 *-------------------------------------------------------------------------*/

/** Ensure the per-gateway delivery scratch holds at least @p need bytes.
 *  Returns a pointer to the buffer, or NULL on allocation failure. */
static uint8_t *full_batch_scratch(struct gateway_delivery_scratch *scratch, size_t need)
{
    if (need <= scratch->batch_cap)
    {
        return scratch->batch;
    }
    uint8_t *next = kith_realloc(scratch->allocator, scratch->batch, need);
    if (!next)
    {
        return nullptr;
    }
    scratch->batch = next;
    scratch->batch_cap = need;
    return next;
}

/*---------------------------------------------------------------------------
 * membership backlog
 *-------------------------------------------------------------------------*/

/** Append the composer's fresh delta to the transactional backlog. The
 *  backlog is capped: overflow drops the newest event because a missed
 *  entry self-heals on the next state resend, while an undelivered exit
 *  never regenerates and must not be evicted. */
static void full_backlog_absorb(struct full_state *st,
                                kith_gateway_session_t *session,
                                const kith_gateway_t *gateway)
{
    const struct gateway_view_event *delta = nullptr;
    const size_t delta_count = gateway_view_take_membership(session, &delta);
    const size_t cap = full_backlog_cap(gateway);
    for (size_t i = 0u; i < delta_count; ++i)
    {
        if (st->pending_count == cap)
        {
            break;
        }
        if (st->pending_count == st->pending_cap)
        {
            size_t new_cap = st->pending_cap == 0u ? 8u : st->pending_cap * 2u;
            struct gateway_view_event *buf =
                kith_realloc(st->allocator, st->pending, new_cap * sizeof(*buf));
            if (!buf)
            {
                break;
            }
            st->pending = buf;
            st->pending_cap = new_cap;
        }
        st->pending[st->pending_count] = delta[i];
        st->pending_count += 1u;
    }
}

/*---------------------------------------------------------------------------
 * pass execution
 *-------------------------------------------------------------------------*/

/** Batch path: assemble [header][event records][state records] and enqueue
 *  once; the ledger-style commit is all-or-nothing, so the backlog clears
 *  only on success. */
static void full_deliver_batch(struct full_state *st,
                               kith_gateway_t *gateway,
                               kith_gateway_session_t *session,
                               const kith_gateway_view_subject_t *subjects,
                               size_t subject_count,
                               kith_gateway_delivery_stats_t *stats)
{
    if (subject_count == 0u && st->pending_count == 0u)
    {
        return;
    }
    // The calling thread's own scratch: the gateway's on the reactor thread
    // (and inline), a private claim on an executor thread. A
    // failed claim drops the batch as counted backpressure rather than
    // share a buffer across threads.
    struct gateway_delivery_scratch *scratch = gateway_delivery_scratch(gateway);
    if (!scratch)
    {
        stats->dropped += 1u;
        return;
    }
    const size_t total_records = st->pending_count + subject_count;
    size_t need = GATEWAY_DELIVERY_BATCH_HEADER_SIZE;
    if (!ckd_add(&need, need, total_records * GATEWAY_DELIVERY_PAYLOAD_SIZE) && need <= UINT32_MAX)
    {
        uint8_t *payload = full_batch_scratch(scratch, need);
        if (payload)
        {
            const uint16_t count16 = (total_records > 0xffffu) ? 0xffffu : (uint16_t)total_records;
            put_be_u16(payload + 0u, count16);
            put_be_u16(payload + 2u, 0u);
            for (size_t i = 0u; i < st->pending_count; ++i)
            {
                gateway_delivery_serialize_event(st->pending[i].actor_id,
                                                 st->pending[i].kind,
                                                 payload + GATEWAY_DELIVERY_BATCH_HEADER_SIZE +
                                                     i * GATEWAY_DELIVERY_PAYLOAD_SIZE);
            }
            for (size_t i = 0u; i < subject_count; ++i)
            {
                gateway_delivery_serialize(&subjects[i],
                                           payload + GATEWAY_DELIVERY_BATCH_HEADER_SIZE +
                                               (st->pending_count + i) *
                                                   GATEWAY_DELIVERY_PAYLOAD_SIZE);
            }
            const int rc = gateway_delivery_enqueue(
                gateway, session->conn, gateway->replication_batch_type_id, payload, need);
            if (rc == 0)
            {
                stats->events_enqueued = (uint32_t)st->pending_count;
                st->pending_count = 0u;
                stats->enqueued += 1u;
            }
            else
            {
                stats->dropped += 1u;
            }
        }
        else
        {
            stats->dropped += 1u;
        }
    }
}

/** Per-subject path: events commit frame by frame and keep their
 *  unconfirmed prefix in order; every state subject follows as its own
 *  frame. */
static void full_deliver_single(struct full_state *st,
                                kith_gateway_t *gateway,
                                kith_gateway_session_t *session,
                                const kith_gateway_view_subject_t *subjects,
                                size_t subject_count,
                                kith_gateway_delivery_stats_t *stats)
{
    size_t confirmed = 0u;
    for (size_t i = 0u; i < st->pending_count; ++i)
    {
        uint8_t payload[GATEWAY_DELIVERY_PAYLOAD_SIZE];
        gateway_delivery_serialize_event(st->pending[i].actor_id, st->pending[i].kind, payload);
        const int rc = gateway_delivery_enqueue(
            gateway, session->conn, gateway->replication_type_id, payload, sizeof(payload));
        if (rc == 0)
        {
            stats->enqueued += 1u;
            stats->events_enqueued += 1u;
            ++confirmed;
        }
        else
        {
            stats->dropped += 1u;
            break; // FIFO ordering: stop at the first failed event
        }
    }
    if (confirmed > 0u)
    {
        memmove(st->pending,
                st->pending + confirmed,
                (st->pending_count - confirmed) * sizeof(*st->pending));
        st->pending_count -= confirmed;
    }
    for (size_t i = 0u; i < subject_count; ++i)
    {
        (void)full_deliver_one(
            gateway, session->conn, &subjects[i], &stats->enqueued, &stats->dropped);
    }
}

/*---------------------------------------------------------------------------
 * vtable
 *-------------------------------------------------------------------------*/

static int
full_session_init(const kith_gateway_session_t *session, const void *config, void **out_state)
{
    (void)config;
    struct full_state *st = kith_alloc_zero(session->allocator, 1u, sizeof(*st));
    if (!st)
    {
        return kith_error_return(KITH_ENOMEM);
    }
    st->allocator = session->allocator;
    *out_state = st;
    return 0;
}

static void full_session_fini(void *state)
{
    struct full_state *st = state;
    if (st)
    {
        kith_free(st->allocator, st->pending);
        kith_free(st->allocator, st);
    }
}

static int full_deliver(void *state,
                        kith_gateway_t *gateway,
                        kith_gateway_session_t *session,
                        const kith_gateway_view_subject_t *subjects,
                        size_t subject_count,
                        uint64_t now_ms,
                        kith_gateway_delivery_stats_t *out_stats)
{
    (void)now_ms;

    if (gateway->replication_batch_type_id == 0u && gateway->replication_type_id == 0u)
    {
        return kith_error_return(KITH_ESTATE);
    }

    struct full_state *st = state;
    full_backlog_absorb(st, session, gateway);

    kith_gateway_delivery_stats_t totals = {0};
    if (gateway->replication_batch_type_id != 0u)
    {
        full_deliver_batch(st, gateway, session, subjects, subject_count, &totals);
    }
    else
    {
        full_deliver_single(st, gateway, session, subjects, subject_count, &totals);
    }

    if (out_stats)
    {
        *out_stats = totals;
    }
    return 0;
}

static const kith_gateway_delivery_vtable_t kith_delivery_full = {
    .size = sizeof(kith_gateway_delivery_vtable_t),
    .abi_version = KITH_ABI_VERSION,
    .session_init = full_session_init,
    .session_fini = full_session_fini,
    .deliver = full_deliver,
};

const kith_gateway_delivery_vtable_t *gateway_delivery_full_vtable(void)
{
    return &kith_delivery_full;
}
