/* "tiered" delivery strategy: state-complete records, rate-limited
 * scheduling, and record-granular change suppression. Each
 * session keeps a send ledger — the last successfully enqueued record's
 * payload per subject, stored as the exact scalar fields the serializer
 * reads. A pass delivers the self subject every tick and any other subject
 * whose payload differs from its ledger entry once the subject's tier
 * cadence allows a send; a subject silent past the max-gap floor refreshes
 * regardless of change as a paranoia backstop. Membership events ride ahead
 * of state records and are never suppressed; an event unconfirmed by
 * enqueue stays pending in the ledger and retries next pass. The ledger
 * advances only on confirmed enqueue, so a dropped frame leaves the
 * client's believed state untouched.
 *
 * Suppression compares the serializer's input scalars, not serialized
 * bytes: the 64-byte record is a pure function of {actor_id, pos, vel,
 * input_tick, level} (a fully packed layout with no reserved regions), the
 * actor_id is the ledger key, and the composer has already applied its
 * level-dependent conventions to the subject before the strategy sees it.
 * Field equality against the stored payload is therefore byte equality of
 * the wire record — the same predicate as a byte comparison, without
 * building a record for subjects that end up suppressed. Tier transitions
 * re-differ exactly as under byte comparison (a REDUCED→FULL upgrade
 * carries the composer's re-widened input_tick, so the payload differs
 * even though the source artifact did not). */

#include "gateway/delivery/strategy_tiered.h"

#include <stdckdint.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "gateway/delivery/delivery.h"
#include "gateway/delivery/strategy.h"
#include "gateway/view/view.h"
#include "kith/types.h"
#include "kith/version.h"

/** No membership event pending on a ledger entry. */
#define TIERED_EVENT_NONE 0xFFu

static void put_be_u16(uint8_t *p, uint16_t v)
{
    p[0] = (uint8_t)(v >> 8u);
    p[1] = (uint8_t)v;
}

/** One subject's send state, keyed by actor_id in a sorted array (0 is the
 *  crowd aggregate's key). */
struct tiered_entry
{
    /** Subject id (0 = crowd aggregate). Sorted ascending. */
    uint64_t actor_id;
    /** The last successfully enqueued record's payload, stored as the
     *  exact scalar fields the serializer reads. Meaningful only when
     *  @p has_record; an entry without one is due definitionally (the
     *  first frame for a subject is full). The composer's level-dependent
     *  conventions (e.g. REDUCED zeroing input_tick) are already applied
     *  to the subject, so these hold the values as sent. */
    int64_t last_pos_x;
    int64_t last_pos_y;
    int64_t last_pos_z;
    int64_t last_vel_x;
    int64_t last_vel_y;
    int64_t last_vel_z;
    uint32_t last_input_tick;
    uint32_t last_level;
    /** True once a record was enqueued for this subject. */
    bool has_record;
    /** Monotonic ms of the last successful send of anything for this
     *  subject (records and events alike): cadences and the max-gap floor
     *  measure silence from delivered truth. */
    uint64_t last_send_ms;
    /** Pass stamp of the last pass that saw this subject in the view;
     *  entries the view stops referencing are swept once unpending. */
    uint64_t seen_pass;
    /** Pending membership event kind (TIERED_EVENT_NONE when clear). A
     *  newer classification supersedes an undelivered older one: both
     *  departure flavors remove the subject client-side, and an entry
     *  rides the next state resend anyway. */
    unsigned int pending_kind;
    /** True when the subject has left the composed view; the entry
     *  survives only until its pending departure event is enqueued. */
    bool exiting;
};

struct tiered_state
{
    /** Allocator copied from the session at init; the state, its ledger,
     *  and its per-pass scratch allocate and free through it (session_fini
     *  receives only the state, so the copy is what the fini releases
     *  through). */
    const kith_allocator_t *allocator;
    /** Resolved configuration (defaults applied at init). */
    uint32_t full_interval_ms;
    uint32_t reduced_interval_ms;
    uint32_t crowd_interval_ms;
    uint32_t max_gap_ms;
    /** Send ledger sorted ascending by actor_id. */
    struct tiered_entry *entries;
    size_t count;
    size_t cap;
    /** Number of entries carrying an undelivered membership event,
     *  maintained incrementally (absorb increments on a new pending kind,
     *  commit-confirm and removal decrement). */
    size_t pending_events;
    /** Monotonic pass counter; @p seen_pass stamps compare against it. */
    uint64_t pass;
    /** Per-pass decision + assembly scratch (grow-only): inclusion flag
     *  per composed subject, the serialized record of each included
     *  subject, and the final payload buffer. Steady-state passes perform
     *  no allocation. */
    uint8_t *include;
    uint8_t *staging;
    size_t scratch_cap;
    uint8_t *frame;
    size_t frame_cap;
};

/*---------------------------------------------------------------------------
 * ledger helpers
 *-------------------------------------------------------------------------*/

static struct tiered_entry *tiered_entry_get(struct tiered_state *st, uint64_t actor_id)
{
    size_t lo = 0u;
    size_t hi = st->count;
    while (lo < hi)
    {
        const size_t mid = lo + (hi - lo) / 2u;
        if (st->entries[mid].actor_id < actor_id)
        {
            lo = mid + 1u;
        }
        else
        {
            hi = mid;
        }
    }
    if (lo < st->count && st->entries[lo].actor_id == actor_id)
    {
        return &st->entries[lo];
    }
    return nullptr;
}

/** Find the entry for @p actor_id, inserting a zeroed one when absent.
 *  Returns the entry (existing or freshly inserted), or NULL on allocation
 *  failure, so a commit path needs one search instead of two. */
static struct tiered_entry *tiered_entry_ensure(struct tiered_state *st, uint64_t actor_id)
{
    size_t lo = 0u;
    size_t hi = st->count;
    while (lo < hi)
    {
        const size_t mid = lo + (hi - lo) / 2u;
        if (st->entries[mid].actor_id < actor_id)
        {
            lo = mid + 1u;
        }
        else
        {
            hi = mid;
        }
    }
    if (lo < st->count && st->entries[lo].actor_id == actor_id)
    {
        return &st->entries[lo];
    }
    if (st->count == st->cap)
    {
        size_t new_cap = st->cap == 0u ? 16u : st->cap * 2u;
        struct tiered_entry *buf = kith_realloc(st->allocator, st->entries, new_cap * sizeof(*buf));
        if (!buf)
        {
            return nullptr;
        }
        st->entries = buf;
        st->cap = new_cap;
    }
    memmove(st->entries + lo + 1u, st->entries + lo, (st->count - lo) * sizeof(*st->entries));
    memset(&st->entries[lo], 0, sizeof(st->entries[lo]));
    st->entries[lo].actor_id = actor_id;
    st->entries[lo].pending_kind = TIERED_EVENT_NONE;
    st->count += 1u;
    return &st->entries[lo];
}

static void tiered_entry_remove(struct tiered_state *st, size_t index)
{
    memmove(st->entries + index,
            st->entries + index + 1u,
            (st->count - index - 1u) * sizeof(*st->entries));
    st->count -= 1u;
}

/*---------------------------------------------------------------------------
 * config resolution
 *-------------------------------------------------------------------------*/

static int tiered_resolve_config(const void *config, struct tiered_state *st)
{
    st->full_interval_ms = 0u;
    st->reduced_interval_ms = KITH_GATEWAY_TIERED_DEFAULT_REDUCED_INTERVAL_MS;
    st->crowd_interval_ms = KITH_GATEWAY_TIERED_DEFAULT_CROWD_INTERVAL_MS;
    st->max_gap_ms = KITH_GATEWAY_TIERED_DEFAULT_MAX_GAP_MS;
    if (!config)
    {
        return 0;
    }
    const kith_gateway_tiered_config_t *cfg = config;
    if (cfg->size < sizeof(kith_gateway_tiered_config_t))
    {
        return kith_error_return(KITH_ESIZE);
    }
    if (cfg->abi_version != KITH_ABI_VERSION)
    {
        return kith_error_return(KITH_EABIVER);
    }
    if (cfg->full_interval_ms != 0u)
    {
        st->full_interval_ms = cfg->full_interval_ms;
    }
    if (cfg->reduced_interval_ms != 0u)
    {
        st->reduced_interval_ms = cfg->reduced_interval_ms;
    }
    if (cfg->crowd_interval_ms != 0u)
    {
        st->crowd_interval_ms = cfg->crowd_interval_ms;
    }
    if (cfg->max_gap_ms != 0u)
    {
        st->max_gap_ms = cfg->max_gap_ms;
    }
    return 0;
}

static uint32_t tiered_interval_for(const struct tiered_state *st,
                                    kith_fabric_product_level_t level)
{
    switch (level)
    {
        case KITH_FABRIC_LEVEL_FULL:
            return st->full_interval_ms;
        case KITH_FABRIC_LEVEL_REDUCED:
            return st->reduced_interval_ms;
        case KITH_FABRIC_LEVEL_CROWD:
        default:
            return st->crowd_interval_ms;
    }
}

/** Grow the per-pass scratch arrays to hold @p subject_count decisions. */
static int tiered_scratch_ensure(struct tiered_state *st, size_t subject_count)
{
    if (subject_count <= st->scratch_cap)
    {
        return 0;
    }
    const size_t new_cap = subject_count * 2u;
    uint8_t *inc = kith_realloc(st->allocator, st->include, new_cap * sizeof(*inc));
    if (!inc)
    {
        return kith_error_return(KITH_ENOMEM);
    }
    st->include = inc;
    size_t staging_bytes = 0u;
    if (ckd_mul(&staging_bytes, new_cap, GATEWAY_DELIVERY_PAYLOAD_SIZE))
    {
        return kith_error_return(KITH_EOVERFLOW);
    }
    uint8_t *stag = kith_realloc(st->allocator, st->staging, staging_bytes);
    if (!stag)
    {
        return kith_error_return(KITH_ENOMEM);
    }
    st->staging = stag;
    st->scratch_cap = new_cap;
    return 0;
}

/** Decide one non-self subject against its ledger entry: due when it was
 *  never sent, when its payload differs from the stored one once the tier
 *  cadence allows a send, or when silence passed the max-gap floor. Exiting
 *  subjects are never sent as records (their event speaks instead). The
 *  payload comparison reads the subject's scalar fields directly — the
 *  exact inputs of the serializer, with no record built for subjects that
 *  end up suppressed. */
static bool tiered_due(struct tiered_state *st,
                       const kith_gateway_view_subject_t *s,
                       struct tiered_entry *e,
                       uint64_t now_ms)
{
    if (!e || !e->has_record)
    {
        return true;
    }
    if (e->exiting)
    {
        return false;
    }
    const bool cadence_ok = now_ms - e->last_send_ms >= tiered_interval_for(st, s->level);
    // update_seq stays out of the change test: it is delivery certification
    // metadata, not content, and it advances in lockstep with
    // input_tick on every movement publish anyway.
    const bool changed = s->pos_x != e->last_pos_x || s->pos_y != e->last_pos_y ||
                         s->pos_z != e->last_pos_z || s->vel_x != e->last_vel_x ||
                         s->vel_y != e->last_vel_y || s->vel_z != e->last_vel_z ||
                         s->input_tick != e->last_input_tick || (uint32_t)s->level != e->last_level;
    const bool max_gap_ok = now_ms - e->last_send_ms >= st->max_gap_ms;
    return (cadence_ok && changed) || max_gap_ok;
}

/** Remove entries the pass can safely forget: exited subjects whose
 *  departure event was confirmed, and ids the view stopped referencing
 *  without any pending event (a rebind leaves the old self id this way).
 *  Only runs after a pass confirmed everything it decided, so a stamp
 *  mismatch means genuinely gone rather than backpressure-deferred. */
static void tiered_sweep(struct tiered_state *st)
{
    for (size_t i = st->count; i-- > 0u;)
    {
        struct tiered_entry *e = &st->entries[i];
        if (e->pending_kind != TIERED_EVENT_NONE)
        {
            continue;
        }
        if (e->exiting || e->seen_pass != st->pass)
        {
            tiered_entry_remove(st, i);
        }
    }
}

/*---------------------------------------------------------------------------
 * per-pass pipeline
 *-------------------------------------------------------------------------*/

/** Pass A of a batch pass — decide. Marks inclusion against each subject's
 *  ledger entry by payload comparison (the self subject always delivers),
 *  serializes only the included subjects into the
 *  pass-B staging area, stamps presence so the sweep spares referenced
 *  entries, and accumulates suppressed into @p stats. Returns the number
 *  of included subjects. */
static size_t tiered_decide(struct tiered_state *st,
                            const kith_gateway_view_subject_t *subjects,
                            size_t subject_count,
                            uint64_t now_ms,
                            kith_gateway_delivery_stats_t *stats)
{
    size_t included = 0u;
    for (size_t i = 0u; i < subject_count; ++i)
    {
        const bool is_self = i == 0u || subjects[i].subject_class == KITH_GATEWAY_VIEW_CLASS_SELF;
        struct tiered_entry *e = tiered_entry_get(st, subjects[i].actor_id);
        const bool due = is_self || tiered_due(st, &subjects[i], e, now_ms);
        st->include[i] = due ? 1u : 0u;
        if (due)
        {
            gateway_delivery_serialize(&subjects[i],
                                       st->staging + i * GATEWAY_DELIVERY_PAYLOAD_SIZE);
            included += 1u;
        }
        else
        {
            stats->suppressed += 1u;
        }
        // Presence, not inclusion, keeps an entry out of the sweep: a
        // suppressed subject stays referenced by the retained view and
        // must survive; only ids the view stops referencing sweep.
        if (e)
        {
            e->seen_pass = st->pass;
        }
    }
    return included;
}

/** Pass B of a batch pass — assemble [header][event records][included
 *  state records] into @p payload, which the caller sized for exactly
 *  @p total_records. Included records copy from the pass-A staging area,
 *  so only the subject count is needed here. */
static void tiered_assemble(uint8_t *payload,
                            const struct tiered_state *st,
                            size_t subject_count,
                            size_t total_records)
{
    size_t off = GATEWAY_DELIVERY_BATCH_HEADER_SIZE;
    if (st->pending_events > 0u)
    {
        for (size_t i = 0u; i < st->count; ++i)
        {
            const struct tiered_entry *e = &st->entries[i];
            if (e->pending_kind == TIERED_EVENT_NONE)
            {
                continue;
            }
            gateway_delivery_serialize_event(e->actor_id, e->pending_kind, payload + off);
            off += GATEWAY_DELIVERY_PAYLOAD_SIZE;
        }
    }
    for (size_t i = 0u; i < subject_count; ++i)
    {
        if (st->include[i])
        {
            memcpy(payload + off,
                   st->staging + i * GATEWAY_DELIVERY_PAYLOAD_SIZE,
                   GATEWAY_DELIVERY_PAYLOAD_SIZE);
            off += GATEWAY_DELIVERY_PAYLOAD_SIZE;
        }
    }
    const uint16_t count16 = (total_records > 0xffffu) ? 0xffffu : (uint16_t)total_records;
    put_be_u16(payload + 0u, count16);
    put_be_u16(payload + 2u, 0u);
}

/** Commit a successfully enqueued batch: confirm every pending event and
 *  advance every included subject's ledger entry to the payload just sent
 *  (the scalars as the serializer read them). Runs only after the enqueue
 *  succeeded, so a dropped frame leaves the ledger untouched. */
static void tiered_commit(struct tiered_state *st,
                          const kith_gateway_view_subject_t *subjects,
                          size_t subject_count,
                          uint64_t now_ms)
{
    if (st->pending_events > 0u)
    {
        for (size_t i = 0u; i < st->count; ++i)
        {
            struct tiered_entry *e = &st->entries[i];
            if (e->pending_kind != TIERED_EVENT_NONE)
            {
                e->pending_kind = TIERED_EVENT_NONE;
                e->last_send_ms = now_ms;
                st->pending_events -= 1u;
            }
        }
    }
    for (size_t i = 0u; i < subject_count; ++i)
    {
        if (!st->include[i])
        {
            continue;
        }
        // Ensure (not just lookup): a subject delivered on its first send
        // has no entry until its record commits.
        struct tiered_entry *e = tiered_entry_ensure(st, subjects[i].actor_id);
        if (!e)
        {
            continue;
        }
        e->seen_pass = st->pass;
        e->last_pos_x = subjects[i].pos_x;
        e->last_pos_y = subjects[i].pos_y;
        e->last_pos_z = subjects[i].pos_z;
        e->last_vel_x = subjects[i].vel_x;
        e->last_vel_y = subjects[i].vel_y;
        e->last_vel_z = subjects[i].vel_z;
        e->last_input_tick = subjects[i].input_tick;
        e->last_level = (uint32_t)subjects[i].level;
        e->has_record = true;
        e->last_send_ms = now_ms;
    }
}

/** Batch-framed pass: decide inclusion, grow the frame, assemble one
 *  frame of [events][states], enqueue once, commit on success. A pass
 *  with nothing due and no pending events enqueues nothing. */
static void tiered_deliver_batch(struct tiered_state *st,
                                 kith_gateway_t *gateway,
                                 kith_gateway_session_t *session,
                                 const kith_gateway_view_subject_t *subjects,
                                 size_t subject_count,
                                 uint64_t now_ms,
                                 kith_gateway_delivery_stats_t *stats)
{
    if (tiered_scratch_ensure(st, subject_count) != 0)
    {
        memset(stats, 0, sizeof(*stats));
        stats->dropped = 1u;
        return;
    }

    const size_t included = tiered_decide(st, subjects, subject_count, now_ms, stats);
    const size_t pending_count = st->pending_events;
    const size_t total_records = pending_count + included;

    size_t need = GATEWAY_DELIVERY_BATCH_HEADER_SIZE;
    if (!ckd_add(&need, need, total_records * GATEWAY_DELIVERY_PAYLOAD_SIZE) && need <= UINT32_MAX)
    {
        if (need > st->frame_cap)
        {
            uint8_t *buf = kith_realloc(st->allocator, st->frame, need);
            if (!buf)
            {
                stats->dropped += 1u;
                need = 0u;
            }
            else
            {
                st->frame = buf;
                st->frame_cap = need;
            }
        }
    }
    else
    {
        need = 0u;
    }

    if (need != 0u)
    {
        tiered_assemble(st->frame, st, subject_count, total_records);
        const int erc = gateway_delivery_enqueue(
            gateway, session->conn, gateway->replication_batch_type_id, st->frame, need);
        if (erc == 0)
        {
            stats->enqueued += 1u;
            stats->events_enqueued += (uint32_t)pending_count;
            tiered_commit(st, subjects, subject_count, now_ms);
            tiered_sweep(st);
        }
        else
        {
            stats->dropped += 1u;
        }
    }
    else if (total_records == 0u)
    {
        // Nothing due and no pending events: a zero-tick pass enqueues
        // nothing and advances nothing.
    }
    else
    {
        stats->dropped += 1u;
    }
}

/** Send one subject's state record and commit its ledger entry. Returns
 *  false when backpressure stopped the pass (FIFO ordering); suppression
 *  accumulates into @p stats. Exiting subjects never send records — their
 *  pending event speaks instead. */
static bool tiered_send_subject(struct tiered_state *st,
                                kith_gateway_t *gateway,
                                kith_net_conn_t *conn,
                                const kith_gateway_view_subject_t *s,
                                bool is_self,
                                uint64_t now_ms,
                                kith_gateway_delivery_stats_t *stats)
{
    struct tiered_entry *e = tiered_entry_get(st, s->actor_id);
    if (e)
    {
        e->seen_pass = st->pass;
    }
    if (!is_self && !tiered_due(st, s, e, now_ms))
    {
        stats->suppressed += 1u;
        return true;
    }
    uint8_t rec[GATEWAY_DELIVERY_PAYLOAD_SIZE];
    gateway_delivery_serialize(s, rec);
    const int rc =
        gateway_delivery_enqueue(gateway, conn, gateway->replication_type_id, rec, sizeof(rec));
    if (rc != 0)
    {
        stats->dropped += 1u;
        return false;
    }
    stats->enqueued += 1u;
    // Ensure (not just lookup): a subject delivered on its first send has
    // no entry until its record commits.
    e = tiered_entry_ensure(st, s->actor_id);
    if (!e)
    {
        return true;
    }
    e->seen_pass = st->pass;
    e->last_pos_x = s->pos_x;
    e->last_pos_y = s->pos_y;
    e->last_pos_z = s->pos_z;
    e->last_vel_x = s->vel_x;
    e->last_vel_y = s->vel_y;
    e->last_vel_z = s->vel_z;
    e->last_input_tick = s->input_tick;
    e->last_level = (uint32_t)s->level;
    e->has_record = true;
    e->last_send_ms = now_ms;
    return true;
}

/** Per-subject framing: events first, then states, frame by frame. A
 *  failed frame stops the pass (FIFO order on the wire) and commits only
 *  what was confirmed before it. The sweep runs whenever the event loop
 *  completed cleanly — entries referenced by the retained view were
 *  stamped as the subject loop reached them, so an enqueue failure part
 *  way through the loop also sweeps the ids it never visited; a swept
 *  entry's next send is a full record (no ledger), trading one resend
 *  for the early exit. */
static void tiered_deliver_single(struct tiered_state *st,
                                  kith_gateway_t *gateway,
                                  kith_gateway_session_t *session,
                                  const kith_gateway_view_subject_t *subjects,
                                  size_t subject_count,
                                  uint64_t now_ms,
                                  kith_gateway_delivery_stats_t *stats)
{
    bool clean = true;
    for (size_t i = 0u; i < st->count; ++i)
    {
        struct tiered_entry *e = &st->entries[i];
        if (e->pending_kind == TIERED_EVENT_NONE)
        {
            continue;
        }
        uint8_t ev[GATEWAY_DELIVERY_PAYLOAD_SIZE];
        gateway_delivery_serialize_event(e->actor_id, e->pending_kind, ev);
        const int rc = gateway_delivery_enqueue(
            gateway, session->conn, gateway->replication_type_id, ev, sizeof(ev));
        if (rc != 0)
        {
            stats->dropped += 1u;
            clean = false;
            break; // FIFO: stop at the first failed event
        }
        stats->enqueued += 1u;
        stats->events_enqueued += 1u;
        e->pending_kind = TIERED_EVENT_NONE;
        e->last_send_ms = now_ms;
    }
    if (clean)
    {
        for (size_t i = 0u; i < subject_count; ++i)
        {
            const bool is_self =
                i == 0u || subjects[i].subject_class == KITH_GATEWAY_VIEW_CLASS_SELF;
            if (!tiered_send_subject(
                    st, gateway, session->conn, &subjects[i], is_self, now_ms, stats))
            {
                break; // FIFO: stop at the first failed record
            }
        }
        tiered_sweep(st);
    }
}

/*---------------------------------------------------------------------------
 * vtable callbacks
 *-------------------------------------------------------------------------*/

static int
tiered_session_init(const kith_gateway_session_t *session, const void *config, void **out_state)
{
    struct tiered_state *st = kith_alloc_zero(session->allocator, 1u, sizeof(*st));
    if (!st)
    {
        return kith_error_return(KITH_ENOMEM);
    }
    st->allocator = session->allocator;
    int rc = tiered_resolve_config(config, st);
    if (rc != 0)
    {
        kith_free(st->allocator, st);
        return rc;
    }
    *out_state = st;
    return 0;
}

static void tiered_session_fini(void *state)
{
    struct tiered_state *st = state;
    if (st)
    {
        kith_free(st->allocator, st->entries);
        kith_free(st->allocator, st->include);
        kith_free(st->allocator, st->staging);
        kith_free(st->allocator, st->frame);
        kith_free(st->allocator, st);
    }
}

/** Absorb the composer's fresh membership delta into the ledger:
 *  departures mark their entry exiting with a pending event (a newer
 *  classification supersedes an older undelivered one); entries confirm
 *  nothing until their frames enqueue successfully. */
static void tiered_absorb_delta(struct tiered_state *st, kith_gateway_session_t *session)
{
    const struct gateway_view_event *delta = nullptr;
    const size_t delta_count = gateway_view_take_membership(session, &delta);
    for (size_t i = 0u; i < delta_count; ++i)
    {
        const unsigned int kind = delta[i].kind;
        if (kind == (unsigned int)GATEWAY_DELIVERY_EVENT_ENTER ||
            kind == (unsigned int)GATEWAY_DELIVERY_EVENT_CROWD_ENTER)
        {
            // A brand-new subject has no ledger entry yet and is due
            // definitionally; a re-entering subject's entry was removed
            // when its departure event was delivered. Clear a stale
            // exiting flag so the subject cannot stay pinned silent.
            struct tiered_entry *e = tiered_entry_get(st, delta[i].actor_id);
            if (e && e->exiting && e->pending_kind == TIERED_EVENT_NONE)
            {
                e->exiting = false;
            }
            continue;
        }
        struct tiered_entry *e = tiered_entry_ensure(st, delta[i].actor_id);
        if (e)
        {
            if (e->pending_kind == TIERED_EVENT_NONE)
            {
                st->pending_events += 1u;
            }
            e->pending_kind = kind;
            e->exiting = true;
        }
    }
}

static int tiered_deliver(void *state,
                          kith_gateway_t *gateway,
                          kith_gateway_session_t *session,
                          const kith_gateway_view_subject_t *subjects,
                          size_t subject_count,
                          uint64_t now_ms,
                          kith_gateway_delivery_stats_t *out_stats)
{
    if (gateway->replication_batch_type_id == 0u && gateway->replication_type_id == 0u)
    {
        return kith_error_return(KITH_ESTATE);
    }

    struct tiered_state *st = state;
    st->pass += 1u;
    tiered_absorb_delta(st, session);

    kith_gateway_delivery_stats_t totals = {0};
    if (gateway->replication_batch_type_id != 0u)
    {
        tiered_deliver_batch(st, gateway, session, subjects, subject_count, now_ms, &totals);
    }
    else
    {
        tiered_deliver_single(st, gateway, session, subjects, subject_count, now_ms, &totals);
    }

    if (out_stats)
    {
        *out_stats = totals;
    }
    return 0;
}

static const kith_gateway_delivery_vtable_t kith_delivery_tiered = {
    .size = sizeof(kith_gateway_delivery_vtable_t),
    .abi_version = KITH_ABI_VERSION,
    .session_init = tiered_session_init,
    .session_fini = tiered_session_fini,
    .deliver = tiered_deliver,
};

const kith_gateway_delivery_vtable_t *gateway_delivery_tiered_vtable(void)
{
    return &kith_delivery_tiered;
}
