/* Relevance composer for the gateway: streams the candidate artifacts of the
 * cached cells around a subscriber into a bounded max-heap keyed by
 * prior-view stickiness and squared distance, keeps the best candidates up
 * to the configured budget, and emits a crowd aggregate for the overflow.
 * Reads the cache (cache/) and feeds delivery.c. */

#include "gateway/view/view.h"

#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>

#include "gateway/cache/cache.h"
#include "gateway/delivery/delivery.h"
#include "kith/types.h"

/*---------------------------------------------------------------------------
 * compose entry + window totals (internal to the composer)
 *-------------------------------------------------------------------------*/

/** One streaming candidate. Both pointers borrow from the cache and are
 *  valid only while the composer holds the window's cache stripes. */
struct gateway_compose_entry
{
    /** The candidate artifact. */
    const kith_fabric_artifact_t *artifact;
    /** The cell the artifact is cached in (for tier selection). */
    const kith_fabric_cell_key_t *cell;
    /** Squared distance to the subscriber (Q16.16). */
    int64_t dist_sq;
    /** True when the actor was in the prior view (sticky continuity). */
    bool was_prior;
};

/** Window-level facts gathered before the candidate scan: the subscriber's
 *  own artifact and cell, plus the candidate totals the crowd aggregate and
 *  view metadata need. */
struct gateway_window_totals
{
    /** The subscriber's artifact (first occurrence in window order). */
    kith_fabric_artifact_t sub;
    /** The cell the subscriber's artifact is cached in. */
    kith_fabric_cell_key_t sub_cell;
    /** Candidates in the window: every artifact except the subscriber's. */
    size_t candidate_count;
    /** Sum of candidate pos_x across the window. */
    int64_t cand_sum_x;
    /** Sum of candidate pos_y (see cand_sum_x). */
    int64_t cand_sum_y;
    /** Sum of candidate pos_z (see cand_sum_x). */
    int64_t cand_sum_z;
};

/** Position sums of the selected individuals, for the crowd aggregate. */
struct gateway_crowd_input
{
    /** Σ pos_x of the selected individuals. */
    int64_t sel_sum_x;
    /** Σ pos_y of the selected individuals. */
    int64_t sel_sum_y;
    /** Σ pos_z of the selected individuals. */
    int64_t sel_sum_z;
    /** Individuals stored in the view set. */
    size_t individual_count;
    /** Candidates in the window (subscriber excluded). */
    size_t candidate_count;
};

/*---------------------------------------------------------------------------
 * helpers
 *-------------------------------------------------------------------------*/

static int64_t
gateway_view_dist_sq(int64_t x, int64_t y, int64_t z, const kith_fabric_artifact_t *sub)
{
    int64_t dx = x - sub->pos_x;
    int64_t dy = y - sub->pos_y;
    int64_t dz = z - sub->pos_z;
    return dx * dx + dy * dy + dz * dz;
}

static int32_t gateway_cell_chebyshev(const kith_fabric_cell_key_t *a,
                                      const kith_fabric_cell_key_t *b)
{
    int32_t dx = a->cell_x - b->cell_x;
    int32_t dy = a->cell_y - b->cell_y;
    int32_t dz = a->cell_z - b->cell_z;
    if (dx < 0)
    {
        dx = -dx;
    }
    if (dy < 0)
    {
        dy = -dy;
    }
    if (dz < 0)
    {
        dz = -dz;
    }
    int32_t m = dx;
    if (dy > m)
    {
        m = dy;
    }
    if (dz > m)
    {
        m = dz;
    }
    return m;
}

/*---------------------------------------------------------------------------
 * prior-view id set
 *-------------------------------------------------------------------------*/

static uint64_t gateway_view_id_mix(uint64_t id)
{
    uint64_t k = id;
    k ^= k >> 33;
    k *= 0xff51afd7ed558ccdULL;
    k ^= k >> 33;
    return k;
}

static bool gateway_prior_set_contains(const struct gateway_compose_scratch *scratch, uint64_t id)
{
    size_t cap = scratch->prior_set_cap;
    if (cap == 0u)
    {
        return false;
    }
    size_t mask = cap - 1u;
    size_t idx = (size_t)gateway_view_id_mix(id) & mask;
    while (scratch->prior_set[idx] != 0u)
    {
        if (scratch->prior_set[idx] == id)
        {
            return true;
        }
        idx = (idx + 1u) & mask;
    }
    return false;
}

static void gateway_prior_set_insert(struct gateway_compose_scratch *scratch, uint64_t id)
{
    size_t mask = scratch->prior_set_cap - 1u;
    size_t idx = (size_t)gateway_view_id_mix(id) & mask;
    while (scratch->prior_set[idx] != 0u && scratch->prior_set[idx] != id)
    {
        idx = (idx + 1u) & mask;
    }
    scratch->prior_set[idx] = id;
}

static int
gateway_prior_set_build(struct gateway_compose_scratch *scratch, const uint64_t *ids, size_t count)
{
    if (count == 0u)
    {
        // An empty prior view leaves the set logically empty; clear any set
        // left by a prior composition.
        if (scratch->prior_set_cap != 0u)
        {
            memset(scratch->prior_set, 0, scratch->prior_set_cap * sizeof(*scratch->prior_set));
        }
        return 0;
    }
    size_t want = 16u;
    while (want < 2u * count)
    {
        want <<= 1u;
    }
    if (want > scratch->prior_set_cap)
    {
        uint64_t *buf = kith_realloc(scratch->allocator, scratch->prior_set, want * sizeof(*buf));
        if (!buf)
        {
            return kith_error_return(KITH_ENOMEM);
        }
        scratch->prior_set = buf;
        scratch->prior_set_cap = want;
    }
    memset(scratch->prior_set, 0, scratch->prior_set_cap * sizeof(*scratch->prior_set));
    for (size_t i = 0u; i < count; ++i)
    {
        gateway_prior_set_insert(scratch, ids[i]);
    }
    return 0;
}

/*---------------------------------------------------------------------------
 * bounded candidate heap (max-heap: the root is the worst kept candidate)
 *-------------------------------------------------------------------------*/

static bool gateway_entry_better(const struct gateway_compose_entry *a,
                                 const struct gateway_compose_entry *b)
{
    if (a->was_prior != b->was_prior)
    {
        return a->was_prior;
    }
    return a->dist_sq < b->dist_sq;
}

static void gateway_heap_sift_up(struct gateway_compose_entry *heap, size_t i)
{
    while (i > 0u)
    {
        size_t parent = (i - 1u) / 2u;
        if (!gateway_entry_better(&heap[parent], &heap[i]))
        {
            break;
        }
        struct gateway_compose_entry tmp = heap[i];
        heap[i] = heap[parent];
        heap[parent] = tmp;
        i = parent;
    }
}

static void gateway_heap_sift_down(struct gateway_compose_entry *heap, size_t n, size_t i)
{
    while (true)
    {
        size_t worst = i;
        size_t left = 2u * i + 1u;
        size_t right = left + 1u;
        if (left < n && gateway_entry_better(&heap[worst], &heap[left]))
        {
            worst = left;
        }
        if (right < n && gateway_entry_better(&heap[worst], &heap[right]))
        {
            worst = right;
        }
        if (worst == i)
        {
            break;
        }
        struct gateway_compose_entry tmp = heap[i];
        heap[i] = heap[worst];
        heap[worst] = tmp;
        i = worst;
    }
}

static void gateway_heap_sort(struct gateway_compose_entry *heap, size_t n)
{
    for (size_t end = n; end > 1u;)
    {
        end -= 1u;
        struct gateway_compose_entry tmp = heap[0];
        heap[0] = heap[end];
        heap[end] = tmp;
        gateway_heap_sift_down(heap, end, 0u);
    }
}

/*---------------------------------------------------------------------------
 * window snapshot + subscriber location
 *-------------------------------------------------------------------------*/

static int gateway_view_snapshot_window(kith_gateway_session_t *session,
                                        struct gateway_compose_scratch *scratch,
                                        uint64_t *out_window_seq,
                                        size_t *out_count)
{
    pthread_mutex_lock(&session->window_lock);
    size_t count = session->window_count;
    if (count > scratch->window_cap)
    {
        kith_fabric_cell_key_t *buf =
            kith_realloc(scratch->allocator, scratch->window, count * sizeof(*buf));
        if (!buf)
        {
            pthread_mutex_unlock(&session->window_lock);
            return kith_error_return(KITH_ENOMEM);
        }
        scratch->window = buf;
        scratch->window_cap = count;
    }
    if (count != 0u)
    {
        memcpy(scratch->window, session->window, count * sizeof(*scratch->window));
    }
    // Read in the same critical section as the copy so the counter and the
    // array it describes cannot diverge.
    *out_window_seq = session->window_seq;
    pthread_mutex_unlock(&session->window_lock);
    *out_count = count;
    return 0;
}

static size_t gateway_node_lower_bound(const struct gateway_cache_node *n, uint64_t actor_id)
{
    size_t lo = 0u;
    size_t hi = n->artifact_count;
    while (lo < hi)
    {
        size_t mid = lo + (hi - lo) / 2u;
        if (n->artifacts[mid].actor_id < actor_id)
        {
            lo = mid + 1u;
        }
        else
        {
            hi = mid;
        }
    }
    return lo;
}

static bool gateway_view_locate(const struct gateway_cache *c,
                                const kith_fabric_cell_key_t *window,
                                size_t window_count,
                                uint64_t actor_id,
                                struct gateway_window_totals *out)
{
    memset(out, 0, sizeof(*out));
    bool found = false;
    for (size_t w = 0u; w < window_count; ++w)
    {
        const struct gateway_cache_node *n = gateway_cache_find(c, &window[w]);
        if (!n)
        {
            continue;
        }
        size_t lo = gateway_node_lower_bound(n, actor_id);
        size_t occurrences = 0u;
        int64_t sub_sum_x = 0;
        int64_t sub_sum_y = 0;
        int64_t sub_sum_z = 0;
        for (size_t j = lo; j < n->artifact_count && n->artifacts[j].actor_id == actor_id; ++j)
        {
            occurrences += 1u;
            sub_sum_x += n->artifacts[j].pos_x;
            sub_sum_y += n->artifacts[j].pos_y;
            sub_sum_z += n->artifacts[j].pos_z;
        }
        if (occurrences != 0u && !found)
        {
            out->sub = n->artifacts[lo];
            out->sub_cell = n->key;
            found = true;
        }
        out->candidate_count += n->artifact_count - occurrences;
        out->cand_sum_x += n->sum_x - sub_sum_x;
        out->cand_sum_y += n->sum_y - sub_sum_y;
        out->cand_sum_z += n->sum_z - sub_sum_z;
    }
    return found;
}

/*---------------------------------------------------------------------------
 * candidate scan (stream into the bounded heap)
 *-------------------------------------------------------------------------*/

static size_t gateway_view_scan(const struct gateway_cache *c,
                                const kith_fabric_cell_key_t *window,
                                size_t window_count,
                                const struct gateway_window_totals *totals,
                                struct gateway_compose_scratch *scratch,
                                size_t heap_cap)
{
    struct gateway_compose_entry *heap = scratch->heap;
    size_t size = 0u;
    for (size_t w = 0u; w < window_count; ++w)
    {
        const struct gateway_cache_node *node = gateway_cache_find(c, &window[w]);
        if (!node)
        {
            continue;
        }
        for (size_t j = 0u; j < node->artifact_count; ++j)
        {
            const kith_fabric_artifact_t *a = &node->artifacts[j];
            if (a->actor_id == totals->sub.actor_id)
            {
                continue;
            }
            struct gateway_compose_entry e = {
                .artifact = a,
                .cell = &node->key,
                .dist_sq = gateway_view_dist_sq(a->pos_x, a->pos_y, a->pos_z, &totals->sub),
                .was_prior = gateway_prior_set_contains(scratch, a->actor_id),
            };
            if (size < heap_cap)
            {
                heap[size] = e;
                gateway_heap_sift_up(heap, size);
                size += 1u;
            }
            else if (heap_cap != 0u && gateway_entry_better(&e, &heap[0]))
            {
                heap[0] = e;
                gateway_heap_sift_down(heap, heap_cap, 0u);
            }
        }
    }
    return size;
}

/*---------------------------------------------------------------------------
 * select + store
 *-------------------------------------------------------------------------*/

static int gateway_view_heap_ensure(struct gateway_compose_scratch *scratch, size_t cap)
{
    if (cap <= scratch->heap_cap)
    {
        return 0;
    }
    struct gateway_compose_entry *buf =
        kith_realloc(scratch->allocator, scratch->heap, cap * sizeof(*buf));
    if (!buf)
    {
        return kith_error_return(KITH_ENOMEM);
    }
    scratch->heap = buf;
    scratch->heap_cap = cap;
    return 0;
}

static size_t gateway_view_budget(const kith_gateway_t *gateway)
{
    uint32_t max_subjects = gateway->view_max_subjects;
    if (max_subjects == 0u)
    {
        max_subjects = KITH_GATEWAY_DEFAULT_VIEW_MAX_SUBJECTS;
    }
    return (size_t)max_subjects - 1u;
}

/** Crowd-regime exit margin (hysteresis band below the entry threshold).
 *  A configured value wins; the default is one eighth of the budget with a
 *  floor of 2, wide enough that a candidate count jittering at the entry
 *  point cannot flip the aggregate on consecutive compositions. */
static size_t gateway_crowd_exit_margin(const kith_gateway_t *gateway, size_t budget)
{
    if (gateway->crowd_exit_margin != 0u)
    {
        return gateway->crowd_exit_margin;
    }
    size_t margin = budget >> 3;
    return margin < 2u ? 2u : margin;
}

static int gateway_view_ensure_capacity(struct gateway_view_state *v, uint32_t max_subjects)
{
    if (v->subject_cap < (size_t)max_subjects)
    {
        kith_gateway_view_subject_t *buf =
            kith_realloc(v->allocator, v->subjects, (size_t)max_subjects * sizeof(*buf));
        if (!buf)
        {
            return kith_error_return(KITH_ENOMEM);
        }
        v->subjects = buf;
        v->subject_cap = (size_t)max_subjects;
    }
    if (v->prior_cap < (size_t)max_subjects)
    {
        uint64_t *buf =
            kith_realloc(v->allocator, v->prior_ids, (size_t)max_subjects * sizeof(*buf));
        if (!buf)
        {
            return kith_error_return(KITH_ENOMEM);
        }
        v->prior_ids = buf;
        v->prior_cap = (size_t)max_subjects;
    }
    return 0;
}

static void gateway_view_build_self(struct gateway_view_state *v,
                                    const kith_gateway_session_t *session,
                                    const kith_fabric_artifact_t *sub,
                                    bool self_echo_enabled)
{
    memset(&v->subjects[0], 0, sizeof(v->subjects[0]));
    v->subjects[0].actor_id = session->actor_id;
    v->subjects[0].pos_x = sub->pos_x;
    v->subjects[0].pos_y = sub->pos_y;
    v->subjects[0].pos_z = sub->pos_z;
    v->subjects[0].vel_x = sub->vel_x;
    v->subjects[0].vel_y = sub->vel_y;
    v->subjects[0].vel_z = sub->vel_z;
    v->subjects[0].input_tick = sub->input_tick;
    // The counter rides the subject only while stamping is enabled, so a
    // stamping-disabled run keeps every record's seq bytes at zero.
    v->subjects[0].update_seq = self_echo_enabled ? sub->update_seq : 0u;
    v->subjects[0].level = KITH_FABRIC_LEVEL_FULL;
    v->subjects[0].subject_class = KITH_GATEWAY_VIEW_CLASS_SELF;
}

static void gateway_view_assign_tier(kith_gateway_view_subject_t *s,
                                     const struct gateway_compose_entry *cand,
                                     const kith_fabric_cell_key_t *sub_cell)
{
    int32_t cd = gateway_cell_chebyshev(cand->cell, sub_cell);
    if (cd <= 1 && cand->cell->zone == sub_cell->zone)
    {
        s->level = KITH_FABRIC_LEVEL_FULL;
    }
    else
    {
        s->level = KITH_FABRIC_LEVEL_REDUCED;
        s->input_tick = 0u;
        s->update_seq = 0u;
    }
}

static void gateway_view_select_individuals(struct gateway_view_state *v,
                                            const struct gateway_compose_entry *entries,
                                            size_t count,
                                            const kith_fabric_cell_key_t *sub_cell,
                                            uint16_t *tier_count,
                                            uint16_t *class_count,
                                            uint16_t *sticky_count)
{
    for (size_t i = 0u; i < count; ++i)
    {
        const kith_fabric_artifact_t *a = entries[i].artifact;
        kith_gateway_view_subject_t *s = &v->subjects[i + 1u];
        memset(s, 0, sizeof(*s));
        s->actor_id = a->actor_id;
        s->pos_x = a->pos_x;
        s->pos_y = a->pos_y;
        s->pos_z = a->pos_z;
        s->vel_x = a->vel_x;
        s->vel_y = a->vel_y;
        s->vel_z = a->vel_z;
        s->input_tick = a->input_tick;
        s->update_seq = a->update_seq;
        s->subject_class = KITH_GATEWAY_VIEW_CLASS_ACTOR;
        gateway_view_assign_tier(s, &entries[i], sub_cell);
        s->sticky = entries[i].was_prior;
        s->demoted = false;
        tier_count[s->level] += 1u;
        class_count[s->subject_class] += 1u;
        if (s->sticky)
        {
            *sticky_count += 1u;
        }
        if (s->actor_id != 0u && v->prior_count < v->prior_cap)
        {
            v->prior_ids[v->prior_count] = s->actor_id;
            v->prior_count += 1u;
        }
    }
}

static void gateway_view_store_crowd(struct gateway_view_state *v,
                                     const struct gateway_window_totals *totals,
                                     const struct gateway_crowd_input *in,
                                     uint16_t *tier_count,
                                     uint16_t *class_count)
{
    size_t overflow_count = in->candidate_count - in->individual_count;
    kith_gateway_view_subject_t *s = &v->subjects[in->individual_count + 1u];
    memset(s, 0, sizeof(*s));
    s->actor_id = 0u;
    s->pos_x = (totals->cand_sum_x - in->sel_sum_x) / (int64_t)overflow_count;
    s->pos_y = (totals->cand_sum_y - in->sel_sum_y) / (int64_t)overflow_count;
    s->pos_z = (totals->cand_sum_z - in->sel_sum_z) / (int64_t)overflow_count;
    s->subject_class = KITH_GATEWAY_VIEW_CLASS_CROWD;
    s->level = KITH_FABRIC_LEVEL_CROWD;
    s->sticky = false;
    s->demoted = false;
    tier_count[KITH_FABRIC_LEVEL_CROWD] += 1u;
    class_count[KITH_GATEWAY_VIEW_CLASS_CROWD] += 1u;
}

/** Crowd-regime decision under hysteresis. The aggregate is emitted only
 *  when the candidate count exceeds the budget by at least 4: the crowd
 *  occupies one slot that an individual otherwise holds, so a small
 *  overflow is better dropped than crowded (the crowd must represent
 *  enough actors to outweigh the sacrificed individual slot).
 *
 *  The regime is latched: once the aggregate exists it persists until the
 *  candidate count falls below the entry point by the exit margin, keeping
 *  a count hovering at the threshold from flipping the aggregate — and
 *  every actor's membership between individual record and aggregate
 *  silence — on consecutive ticks. */
static bool gateway_view_overflow(const struct gateway_view_state *v,
                                  size_t candidate_count,
                                  size_t budget,
                                  size_t crowd_exit_margin)
{
    const size_t entry_point = budget + 3u;
    const size_t resume_point =
        (entry_point > crowd_exit_margin) ? entry_point - crowd_exit_margin : 0u;
    if (v->crowd_latch)
    {
        return (budget >= 2u) && (candidate_count > resume_point);
    }
    return (candidate_count > entry_point) && (budget >= 2u);
}

/** Position sums over the selected individuals, feeding the crowd
 *  aggregate's mean. */
static void gateway_view_crowd_sums(struct gateway_crowd_input *crowd,
                                    const struct gateway_compose_entry *entries,
                                    size_t individual_count,
                                    size_t candidate_count)
{
    *crowd = (struct gateway_crowd_input){
        .sel_sum_x = 0,
        .sel_sum_y = 0,
        .sel_sum_z = 0,
        .individual_count = individual_count,
        .candidate_count = candidate_count,
    };
    for (size_t i = 0u; i < individual_count; ++i)
    {
        crowd->sel_sum_x += entries[i].artifact->pos_x;
        crowd->sel_sum_y += entries[i].artifact->pos_y;
        crowd->sel_sum_z += entries[i].artifact->pos_z;
    }
}

static int gateway_view_select(kith_gateway_session_t *session,
                               const struct gateway_window_totals *totals,
                               const struct gateway_compose_entry *entries,
                               size_t entry_count,
                               size_t budget,
                               size_t crowd_exit_margin,
                               bool self_echo_enabled,
                               uint64_t now_ms)
{
    struct gateway_view_state *v = &session->view;
    int rc = gateway_view_ensure_capacity(v, (uint32_t)budget + 1u);
    if (rc != 0)
    {
        return rc;
    }
    gateway_view_build_self(v, session, &totals->sub, self_echo_enabled);

    const bool overflow_regime =
        gateway_view_overflow(v, totals->candidate_count, budget, crowd_exit_margin);
    size_t individual_count = entry_count;
    // The scan runs whenever locate succeeds and caps entries at the
    // budget, so on a scanned pass a candidate count above budget - 1
    // fills at least budget - 1 entry slots and the aggregate keeps at
    // least one member. The clamp keeps selection inside the entry array
    // even if a future path ever breaks that antecedent. A latched regime
    // can outlive its own overflow: once every candidate fits the
    // individual slots there is no member left to average, so the regime
    // dissolves instead of emitting and re-enters through the normal entry
    // threshold.
    const bool emit_crowd = overflow_regime && totals->candidate_count > budget - 1u;
    if (emit_crowd)
    {
        individual_count = entry_count < budget - 1u ? entry_count : budget - 1u;
    }

    uint16_t tier_count[KITH_GATEWAY_VIEW_TIER_COUNT];
    uint16_t class_count[KITH_GATEWAY_VIEW_CLASS_COUNT];
    memset(tier_count, 0, sizeof(tier_count));
    memset(class_count, 0, sizeof(class_count));
    tier_count[KITH_FABRIC_LEVEL_FULL] = 1u;
    class_count[KITH_GATEWAY_VIEW_CLASS_SELF] = 1u;
    uint16_t sticky_count = 0u;
    v->prior_count = 0u;

    gateway_view_select_individuals(
        v, entries, individual_count, &totals->sub_cell, tier_count, class_count, &sticky_count);

    size_t total_subjects = individual_count + 1u;
    if (emit_crowd)
    {
        struct gateway_crowd_input crowd;
        gateway_view_crowd_sums(&crowd, entries, individual_count, totals->candidate_count);
        gateway_view_store_crowd(v, totals, &crowd, tier_count, class_count);
        total_subjects += 1u;
    }

    v->subject_count = total_subjects;
    v->has_view = true;
    memset(&v->meta, 0, sizeof(v->meta));
    v->meta.subscriber_actor_id = session->actor_id;
    v->meta.built_at_ms = now_ms;
    v->meta.candidate_count = (uint16_t)totals->candidate_count;
    v->meta.selected_count = (uint16_t)total_subjects;
    for (size_t i = 0u; i < KITH_GATEWAY_VIEW_TIER_COUNT; ++i)
    {
        v->meta.tier_selected_count[i] = tier_count[i];
    }
    for (size_t i = 0u; i < KITH_GATEWAY_VIEW_CLASS_COUNT; ++i)
    {
        v->meta.class_selected_count[i] = class_count[i];
    }
    v->meta.sticky_selected_count = sticky_count;
    v->meta.demoted_selected_count = 0u;
    v->crowd_latch = emit_crowd;
    return 0;
}

/*---------------------------------------------------------------------------
 * compose sub-phase timing
 *-------------------------------------------------------------------------*/

// Accumulate one sub-phase of the composer into its cumulative reactor-side
// total. The brackets are sequential inside gateway_view_compose, so the six
// sub-phases partition the lock-held and pre-lock compute; heap growth and
// the stripe unlock stay untimed (amortized no-op / N uncontended mutex
// releases), so the sum under-attributes rather than over-attributes.
static void gateway_view_phase_add(_Atomic uint64_t *total, uint64_t start)
{
    atomic_fetch_add_explicit(total, gateway_now_ns() - start, memory_order_relaxed);
}

/*---------------------------------------------------------------------------
 * orchestration
 *-------------------------------------------------------------------------*/

/** Stripe indices acquired for one composition's window, held from before
 *  the skip probe until the subjects are stored (or the attempt fails). */
struct gateway_window_locks
{
    size_t indices[GATEWAY_CACHE_STRIPE_COUNT];
    size_t count;
};

// True when the session's identity and configuration inputs still match the
// baseline recorded by its last full composition. Windowed cell content is
// checked separately by gateway_view_skip_match: that probe reads node
// state workers mutate under the cache stripes, so it runs only after those
// stripes are held.
static bool gateway_view_skip_candidate(const kith_gateway_session_t *session,
                                        uint64_t window_seq,
                                        size_t heap_cap)
{
    const struct gateway_view_state *v = &session->view;
    return v->has_view && v->cached_node_seqs != nullptr && v->cached_window_seq == window_seq &&
           v->cached_actor_id == session->actor_id && v->cached_budget == heap_cap;
}

// Probe every window position's cached content sequence against the
// baseline recorded by the session's last full composition. Runs with all
// window stripes held: the probe reads node state that workers free and
// re-create under those same stripes, and a stripe-by-stripe probe misses
// an eviction landing in an already-released stripe. Equality at every
// position means each cell's front buffer is byte-identical to the one
// scored then.
static bool gateway_view_skip_match(const struct gateway_cache *cache,
                                    const kith_fabric_cell_key_t *window,
                                    size_t window_count,
                                    const struct gateway_view_state *v)
{
    for (size_t i = 0u; i < window_count; ++i)
    {
        const struct gateway_cache_node *n = gateway_cache_find(cache, &window[i]);
        uint64_t seq = n ? n->content_seq : 0u;
        if (seq != v->cached_node_seqs[i])
        {
            return false;
        }
    }
    return true;
}

// Record the skip baseline after a successful full composition, while the
// window stripes are still held: the sequences then describe exactly the
// front buffers the subjects were just built from.
static int gateway_view_baseline_record(kith_gateway_session_t *session,
                                        const struct gateway_cache *cache,
                                        const kith_fabric_cell_key_t *window,
                                        size_t window_count,
                                        uint64_t window_seq,
                                        size_t heap_cap)
{
    struct gateway_view_state *v = &session->view;
    if (v->cached_node_seq_cap < window_count)
    {
        uint64_t *buf =
            kith_realloc(v->allocator, v->cached_node_seqs, window_count * sizeof(*buf));
        if (!buf)
        {
            return kith_error_return(KITH_ENOMEM);
        }
        v->cached_node_seqs = buf;
        v->cached_node_seq_cap = window_count;
    }
    for (size_t i = 0u; i < window_count; ++i)
    {
        const struct gateway_cache_node *n = gateway_cache_find(cache, &window[i]);
        v->cached_node_seqs[i] = n ? n->content_seq : 0u;
    }
    v->cached_window_seq = window_seq;
    v->cached_actor_id = session->actor_id;
    v->cached_budget = heap_cap;
    return 0;
}

/** True when @p actor_id appears in any cached window cell's artifacts
 *  (each node's artifacts are sorted ascending by actor_id, so the lookup
 *  is a binary search per cell). Called with the window stripes held. */
static bool gateway_actor_in_window(const struct gateway_cache *cache,
                                    const kith_fabric_cell_key_t *window,
                                    size_t window_count,
                                    uint64_t actor_id)
{
    for (size_t i = 0u; i < window_count; ++i)
    {
        const struct gateway_cache_node *node = gateway_cache_find(cache, &window[i]);
        if (!node || node->artifact_count == 0u)
        {
            continue;
        }
        size_t lo = 0u;
        size_t hi = node->artifact_count;
        while (lo < hi)
        {
            const size_t mid = lo + (hi - lo) / 2u;
            if (node->artifacts[mid].actor_id < actor_id)
            {
                lo = mid + 1u;
            }
            else
            {
                hi = mid;
            }
        }
        if (lo < node->artifact_count && node->artifacts[lo].actor_id == actor_id)
        {
            return true;
        }
    }
    return false;
}

/** Append one membership event to the view's delta set. Returns 0 on
 *  success, -KITH_ENOMEM on growth failure. */
static int
gateway_view_delta_push(struct gateway_view_state *v, uint64_t actor_id, unsigned int kind)
{
    if (v->delta_count == v->delta_cap)
    {
        size_t new_cap = v->delta_cap == 0u ? 8u : v->delta_cap * 2u;
        struct gateway_view_event *buf =
            kith_realloc(v->allocator, v->delta_events, new_cap * sizeof(*buf));
        if (!buf)
        {
            return kith_error_return(KITH_ENOMEM);
        }
        v->delta_events = buf;
        v->delta_cap = new_cap;
    }
    v->delta_events[v->delta_count].actor_id = actor_id;
    v->delta_events[v->delta_count].kind = kind;
    v->delta_count += 1u;
    return 0;
}

/** Regenerate the membership delta for a fresh composition:
 *  entries (selected non-prior actors), departures (prior actors no
 *  longer selected, classified EXIT while still observable in the window's
 *  cached cells and VANISH once gone from all of them), and crowd-aggregate
 *  transitions. Overwrites any unconsumed prior delta: the bound strategy
 *  consumes it once per deliver pass and carries undelivered events in its
 *  own transactional state. Called with the window stripes held, right
 *  after selection stored the new subject set. */
static int gateway_view_membership_delta(kith_gateway_session_t *session,
                                         struct gateway_compose_scratch *scratch,
                                         const struct gateway_cache *cache,
                                         const kith_fabric_cell_key_t *window,
                                         size_t window_count,
                                         const uint64_t *prev_priors,
                                         size_t prev_prior_count,
                                         bool prev_crowd_latch)
{
    struct gateway_view_state *v = &session->view;

    // Rebuild the scratch hash over the NEW ids so each old prior id can be
    // probed in constant time; the scan already finished with the old hash.
    int rc = gateway_prior_set_build(scratch, v->prior_ids, v->prior_count);

    v->delta_count = 0u;
    v->delta_consumed = false;

    // Entries: non-self actors selected without a prior-view presence.
    for (size_t i = 1u; i < v->subject_count && rc == 0; ++i)
    {
        const kith_gateway_view_subject_t *s = &v->subjects[i];
        if (s->subject_class != KITH_GATEWAY_VIEW_CLASS_ACTOR || s->sticky)
        {
            continue;
        }
        rc = gateway_view_delta_push(v, s->actor_id, GATEWAY_DELIVERY_EVENT_ENTER);
    }

    // Departures: prior ids absent from the new selection.
    for (size_t i = 0u; i < prev_prior_count && rc == 0; ++i)
    {
        const uint64_t id = prev_priors[i];
        if (gateway_prior_set_contains(scratch, id))
        {
            continue;
        }
        unsigned int kind = gateway_actor_in_window(cache, window, window_count, id)
                                ? (unsigned int)GATEWAY_DELIVERY_EVENT_EXIT
                                : (unsigned int)GATEWAY_DELIVERY_EVENT_VANISH;
        rc = gateway_view_delta_push(v, id, kind);
    }

    // Crowd transitions ride the hysteresis latch.
    if (rc == 0 && prev_crowd_latch != v->crowd_latch)
    {
        unsigned int kind = v->crowd_latch ? (unsigned int)GATEWAY_DELIVERY_EVENT_CROWD_ENTER
                                           : (unsigned int)GATEWAY_DELIVERY_EVENT_CROWD_EXIT;
        rc = gateway_view_delta_push(v, 0u, kind);
    }
    return rc;
}

/** Snapshot the prior ids before selection overwrites them: the membership
 *  delta diffs this set against the newly selected one. Unlocks the window
 *  stripes and returns -KITH_ENOMEM when growth fails. */
static int gateway_view_prior_snapshot(struct gateway_compose_scratch *scratch,
                                       struct gateway_view_state *v,
                                       const struct gateway_cache *cache,
                                       const struct gateway_window_locks *locks,
                                       size_t *out_count)
{
    if (v->prior_count > scratch->prior_snap_cap)
    {
        const size_t new_cap = v->prior_count * 2u;
        uint64_t *buf =
            kith_realloc(scratch->allocator, scratch->prior_snapshot, new_cap * sizeof(*buf));
        if (!buf)
        {
            gateway_cache_unlock_window(cache, locks->indices, locks->count);
            return kith_error_return(KITH_ENOMEM);
        }
        scratch->prior_snapshot = buf;
        scratch->prior_snap_cap = new_cap;
    }
    if (v->prior_count > 0u)
    {
        // An empty prior set can leave the snapshot buffer unallocated;
        // memcpy is undefined on null pointers even at length 0.
        memcpy(scratch->prior_snapshot, v->prior_ids, v->prior_count * sizeof(*v->prior_ids));
    }
    *out_count = v->prior_count;
    return 0;
}

/** Locate the subscriber in the window and stream every candidate into the
 *  bounded heap. Unlocks the window stripes and returns -KITH_ESTATE when
 *  the subscriber appears in no cached window cell even after repair. The
 *  scan runs on every pass that ultimately locates the subscriber — the
 *  entry array and the totals it produces always describe the same window
 *  state, never a locate retry without its scan. */
static int gateway_view_scan_phase(kith_gateway_t *gateway,
                                   kith_gateway_session_t *session,
                                   const kith_fabric_cell_key_t *window,
                                   size_t window_count,
                                   size_t heap_cap,
                                   struct gateway_window_locks *locks,
                                   struct gateway_window_totals *out_totals,
                                   size_t *out_kept)
{
    uint64_t phase_start = gateway_now_ns();
    bool located =
        gateway_view_locate(&gateway->cache, window, window_count, session->actor_id, out_totals);
    gateway_view_phase_add(&gateway->compose_scan_ns_total, phase_start);
    if (!located)
    {
        // A tracked window cell can lag sim truth by one flush round: a
        // crossing publish that lands after this tick's refresh leaves the
        // destination node holding its previous population (or empty)
        // until the next refresh consumes the pending bump, and the
        // subscriber is then cached in no window cell. Reconcile every
        // tracked window node against current sim truth under the stripes
        // held for this scan — empty nodes gain their first membership,
        // stale nodes drop residents that have already moved on — then
        // retry once, so the pass composes from current truth instead of
        // skipping delivery for the tick. The repair runs only on a
        // failed first locate, so healthy passes never pay for it, and
        // its cost is bounded by the window size.
        bool reconciled = false;
        for (size_t w = 0u; w < window_count; ++w)
        {
            struct gateway_cache_node *zn = gateway_cache_find(&gateway->cache, &window[w]);
            if (!zn)
            {
                continue;
            }
            int populate_rc = gateway_cache_refresh_artifacts(
                &gateway->cache, gateway->fabric, zn, KITH_FABRIC_LEVEL_FULL);
            if (populate_rc == 0)
            {
                reconciled = true;
            }
        }
        if (reconciled)
        {
            located = gateway_view_locate(
                &gateway->cache, window, window_count, session->actor_id, out_totals);
        }
    }
    if (!located)
    {
        atomic_fetch_add_explicit(&gateway->view_locate_failures_total, 1u, memory_order_relaxed);
        gateway_cache_unlock_window(&gateway->cache, locks->indices, locks->count);
        return kith_error_return(KITH_ESTATE);
    }
    phase_start = gateway_now_ns();
    *out_kept = gateway_view_scan(
        &gateway->cache, window, window_count, out_totals, &gateway->compose, heap_cap);
    gateway_view_phase_add(&gateway->compose_scan_ns_total, phase_start);
    return 0;
}

// Rebuild the subject set from the cache: prior-id snapshot, bounded
// candidate heap, heapsort, subject build, then a fresh skip baseline.
// Called with the window stripes held; releases them on every path.
static int gateway_view_compose_rebuild(kith_gateway_t *gateway,
                                        kith_gateway_session_t *session,
                                        const kith_fabric_cell_key_t *window,
                                        size_t window_count,
                                        uint64_t window_seq,
                                        size_t heap_cap,
                                        uint64_t now_ms,
                                        struct gateway_window_locks *locks)
{
    struct gateway_compose_scratch *scratch = &gateway->compose;
    struct gateway_view_state *v = &session->view;
    const bool prev_crowd_latch = v->crowd_latch;

    size_t prev_prior_count = 0u;
    int rc = gateway_view_prior_snapshot(scratch, v, &gateway->cache, locks, &prev_prior_count);
    if (rc != 0)
    {
        return rc;
    }

    uint64_t phase_start = gateway_now_ns();
    rc = gateway_prior_set_build(scratch, v->prior_ids, v->prior_count);
    gateway_view_phase_add(&gateway->compose_prior_ns_total, phase_start);
    if (rc != 0)
    {
        gateway_cache_unlock_window(&gateway->cache, locks->indices, locks->count);
        return rc;
    }
    rc = gateway_view_heap_ensure(scratch, heap_cap);
    if (rc != 0)
    {
        gateway_cache_unlock_window(&gateway->cache, locks->indices, locks->count);
        return rc;
    }

    struct gateway_window_totals totals;
    size_t kept = 0u;
    rc = gateway_view_scan_phase(
        gateway, session, window, window_count, heap_cap, locks, &totals, &kept);
    if (rc != 0)
    {
        return rc;
    }

    phase_start = gateway_now_ns();
    gateway_heap_sort(scratch->heap, kept);
    gateway_view_phase_add(&gateway->compose_sort_ns_total, phase_start);

    phase_start = gateway_now_ns();
    rc = gateway_view_select(session,
                             &totals,
                             scratch->heap,
                             kept,
                             heap_cap,
                             gateway_crowd_exit_margin(gateway, heap_cap),
                             gateway->self_echo_enabled,
                             now_ms);
    gateway_view_phase_add(&gateway->compose_select_ns_total, phase_start);
    if (rc == 0)
    {
        // Stamp accounting rides the rebuild only: a retained-view tick
        // re-delivers the previous subject's counter unchanged, so it
        // writes no new stamp.
        if (gateway->self_echo_enabled)
        {
            _Atomic uint64_t *counter = v->subjects[0].update_seq != 0u
                                            ? &gateway->self_echo_stamps_total
                                            : &gateway->self_echo_fallbacks_total;
            atomic_fetch_add_explicit(counter, 1u, memory_order_relaxed);
        }
        rc = gateway_view_membership_delta(session,
                                           scratch,
                                           &gateway->cache,
                                           window,
                                           window_count,
                                           scratch->prior_snapshot,
                                           prev_prior_count,
                                           prev_crowd_latch);
    }
    if (rc == 0)
    {
        rc = gateway_view_baseline_record(
            session, &gateway->cache, window, window_count, window_seq, heap_cap);
    }
    gateway_cache_unlock_window(&gateway->cache, locks->indices, locks->count);
    return rc;
}

static int
gateway_view_compose(kith_gateway_t *gateway, kith_gateway_session_t *session, uint64_t now_ms)
{
    struct gateway_compose_scratch *scratch = &gateway->compose;
    size_t window_count = 0u;
    uint64_t window_seq = 0u;
    uint64_t phase_start = gateway_now_ns();
    int rc = gateway_view_snapshot_window(session, scratch, &window_seq, &window_count);
    gateway_view_phase_add(&gateway->compose_window_ns_total, phase_start);
    if (rc != 0)
    {
        return rc;
    }
    size_t heap_cap = gateway_view_budget(gateway);

    // The window snapshot is a value copy taken under the per-session lock
    // (no cache stripes held), so a worker-thread window add/remove cannot
    // free a node mid-read. The composer then acquires every distinct stripe
    // the window touches in ascending index order and holds them across the
    // whole scan and subject build: heap entries borrow pointers into the
    // nodes' artifact arrays, so the stripes cannot be released until the
    // subjects are stored. The skip probe reads the same node state and
    // shares this hold — see gateway_view_skip_match.
    struct gateway_window_locks locks;
    phase_start = gateway_now_ns();
    locks.count =
        gateway_cache_lock_window(&gateway->cache, scratch->window, window_count, locks.indices);
    gateway_view_phase_add(&gateway->compose_lock_wait_ns_total, phase_start);

    if (gateway_view_skip_candidate(session, window_seq, heap_cap) &&
        gateway_view_skip_match(&gateway->cache, scratch->window, window_count, &session->view))
    {
        gateway_cache_unlock_window(&gateway->cache, locks.indices, locks.count);
        atomic_fetch_add_explicit(&gateway->compose_skips_total, 1u, memory_order_relaxed);
        return 0;
    }
    return gateway_view_compose_rebuild(
        gateway, session, scratch->window, window_count, window_seq, heap_cap, now_ms, &locks);
}

/*---------------------------------------------------------------------------
 * view state + compose scratch lifecycle
 *-------------------------------------------------------------------------*/

void gateway_view_init(struct gateway_view_state *v, const kith_allocator_t *alloc)
{
    memset(v, 0, sizeof(*v));
    v->allocator = alloc;
}

void gateway_view_fini(struct gateway_view_state *v)
{
    kith_free(v->allocator, v->subjects);
    kith_free(v->allocator, v->prior_ids);
    kith_free(v->allocator, v->cached_node_seqs);
    kith_free(v->allocator, v->delta_events);
    memset(v, 0, sizeof(*v));
}

void gateway_view_scratch_init(struct gateway_compose_scratch *scratch,
                               const kith_allocator_t *alloc)
{
    memset(scratch, 0, sizeof(*scratch));
    scratch->allocator = alloc;
}

void gateway_view_scratch_fini(struct gateway_compose_scratch *scratch)
{
    kith_free(scratch->allocator, scratch->window);
    kith_free(scratch->allocator, scratch->heap);
    kith_free(scratch->allocator, scratch->prior_set);
    kith_free(scratch->allocator, scratch->prior_snapshot);
    memset(scratch, 0, sizeof(*scratch));
}

size_t gateway_view_take_membership(kith_gateway_session_t *session,
                                    const struct gateway_view_event **out_events)
{
    struct gateway_view_state *v = &session->view;
    if (v->delta_consumed)
    {
        *out_events = nullptr;
        return 0u;
    }
    v->delta_consumed = true;
    *out_events = v->delta_events;
    return v->delta_count;
}

/*---------------------------------------------------------------------------
 * public view API
 *-------------------------------------------------------------------------*/

[[nodiscard]] KITH_API int
kith_gateway_view_refresh(kith_gateway_t *gateway, kith_gateway_session_t *session, uint64_t now_ms)
{
    if (!gateway || !session)
    {
        return kith_error_return(KITH_EINVAL);
    }
    if (session->actor_id == 0u)
    {
        return kith_error_return(KITH_ESTATE);
    }
    if (session->view.has_view && session->view.last_refresh_ms != 0u && now_ms != 0u)
    {
        uint64_t delta = now_ms - session->view.last_refresh_ms;
        if (delta < gateway->view_refresh_interval_ms)
        {
            return 0;
        }
    }
    session->view.last_refresh_ms = now_ms;
    return gateway_view_compose(gateway, session, now_ms);
}

[[nodiscard]] KITH_API int kith_gateway_view_snapshot(const kith_gateway_t *gateway,
                                                      const kith_gateway_session_t *session,
                                                      kith_gateway_view_snapshot_t *out_view,
                                                      kith_gateway_view_subject_t *out_subjects,
                                                      size_t max,
                                                      size_t *out_count)
{
    if (!gateway || !session || !out_view)
    {
        return kith_error_return(KITH_EINVAL);
    }
    *out_view = session->view.meta;
    size_t copied = 0u;
    if (out_subjects != nullptr && max > 0u)
    {
        size_t to_copy = session->view.subject_count;
        if (to_copy > max)
        {
            to_copy = max;
        }
        for (size_t i = 0u; i < to_copy; ++i)
        {
            out_subjects[i] = session->view.subjects[i];
        }
        copied = to_copy;
    }
    if (out_count)
    {
        *out_count = copied;
    }
    return 0;
}
