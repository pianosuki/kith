/* Session table for the gateway: open-addressed hash keyed by net connection,
 * session id allocation, insert, lookup, and remove, and the public session
 * accessors. Owned by gateway.c; the receive path uses it to resolve an
 * inbound connection to its session state. */

#include "gateway/session/session.h"

#include <stdatomic.h>
#include <stdckdint.h>
#include <stdlib.h>
#include <string.h>

#include "gateway/cache/cache.h"
#include "gateway/delivery/strategy.h"
#include "gateway/view/view.h"
#include "kith/fabric/fabric.h"
#include "kith/types.h"
#include "kith/util/util.h"

/*---------------------------------------------------------------------------
 * helpers
 *-------------------------------------------------------------------------*/

static uint32_t gateway_session_next_pow2(uint32_t v)
{
    uint32_t r = 1u;
    while (r < v)
    {
        r <<= 1u;
    }
    return r;
}

/*---------------------------------------------------------------------------
 * table lifecycle
 *-------------------------------------------------------------------------*/

int gateway_session_table_init(struct gateway_session_table *t,
                               size_t max_sessions,
                               const kith_allocator_t *alloc)
{
    size_t buckets = (size_t)gateway_session_next_pow2((uint32_t)max_sessions);
    if (buckets < 16u)
    {
        buckets = 16u;
    }
    t->slots = kith_alloc_zero(alloc, buckets, sizeof(*t->slots));
    if (!t->slots)
    {
        return kith_error_return(KITH_ENOMEM);
    }
    t->allocator = alloc;
    t->buckets = buckets;
    t->count = 0u;
    t->max_sessions = max_sessions;
    t->next_session_id = 1u;
    return 0;
}

void gateway_session_table_fini(struct gateway_session_table *t)
{
    if (!t->slots)
    {
        return;
    }
    for (size_t i = 0u; i < t->buckets; ++i)
    {
        if (t->slots[i])
        {
            t->slots[i]->gateway = nullptr;
        }
    }
    kith_free(t->allocator, t->slots);
    t->slots = nullptr;
    t->buckets = 0u;
    t->count = 0u;
}

/*---------------------------------------------------------------------------
 * lookup / insert / remove
 *-------------------------------------------------------------------------*/

struct kith_gateway_session *gateway_session_lookup_by_conn(const struct gateway_session_table *t,
                                                            const kith_net_conn_t *conn)
{
    if (!t->slots || !conn)
    {
        return nullptr;
    }
    size_t mask = gateway_mask(t->buckets);
    size_t i = (size_t)gateway_conn_mix(conn) & mask;
    for (size_t probe = 0u; probe < t->buckets; ++probe)
    {
        size_t idx = (i + probe) & mask;
        struct kith_gateway_session *s = t->slots[idx];
        if (!s)
        {
            return nullptr;
        }
        if (s->conn == conn)
        {
            return s;
        }
    }
    return nullptr;
}

int gateway_session_insert(struct gateway_session_table *t, struct kith_gateway_session *s)
{
    if (t->count >= t->max_sessions)
    {
        return kith_error_return(KITH_EBUSY);
    }
    size_t mask = gateway_mask(t->buckets);
    size_t i = (size_t)gateway_conn_mix(s->conn) & mask;
    for (size_t probe = 0u; probe < t->buckets; ++probe)
    {
        size_t idx = (i + probe) & mask;
        struct kith_gateway_session *existing = t->slots[idx];
        if (!existing)
        {
            t->slots[idx] = s;
            t->count += 1u;
            return 0;
        }
        if (existing->conn == s->conn)
        {
            return kith_error_return(KITH_EEXIST);
        }
    }
    return kith_error_return(KITH_EBUSY);
}

// Cyclic probe distance from @p from back to @p to in a power-of-two table:
// (from - to) mod buckets. Used by the backward-shift deletion to decide
// whether an entry probed past the hole can be pulled back into it.
static inline size_t gateway_session_dist(size_t from, size_t to, size_t mask)
{
    return (from - to) & mask;
}

void gateway_session_remove(struct gateway_session_table *t, const kith_net_conn_t *conn)
{
    if (!t->slots)
    {
        return;
    }
    size_t mask = gateway_mask(t->buckets);
    size_t i = (size_t)gateway_conn_mix(conn) & mask;
    size_t hole = t->buckets; // index of the slot to vacate
    for (size_t probe = 0u; probe < t->buckets; ++probe)
    {
        size_t idx = (i + probe) & mask;
        struct kith_gateway_session *s = t->slots[idx];
        if (!s)
        {
            return; // not present
        }
        if (s->conn == conn)
        {
            hole = idx;
            break;
        }
    }
    if (hole == t->buckets)
    {
        return;
    }

    // Backward-shift deletion (Knuth Algorithm R): nulling the slot severs
    // the probe chain for any session that hashed to an earlier index and
    // probed past this one, leaving it unfindable. That session's own remove
    // on conn-close then fails to locate it, the slot keeps a pointer to
    // freed memory, and the next gateway tick dereferences the dangling
    // pointer. Walk forward from the hole and pull each subsequent entry
    // back into the hole when its natural home lies at or before the hole
    // (cyclically), so every remaining chain stays contiguous. The scan
    // stops at the first empty slot; the count constraint keeps at least one
    // empty slot in practice, and the probe bound guards a degenerate full
    // table.
    size_t j = hole;
    for (size_t probe = 0u; probe < t->buckets; ++probe)
    {
        size_t next = (j + 1u) & mask;
        struct kith_gateway_session *n = t->slots[next];
        if (!n)
        {
            break;
        }
        size_t home = (size_t)gateway_conn_mix(n->conn) & mask;
        if (gateway_session_dist(next, home, mask) >= gateway_session_dist(next, hole, mask))
        {
            t->slots[hole] = n;
            hole = next;
        }
        j = next;
    }
    t->slots[hole] = nullptr;
    if (t->count > 0u)
    {
        t->count -= 1u;
    }
}

/*---------------------------------------------------------------------------
 * refcount
 *-------------------------------------------------------------------------*/

void gateway_session_acquire(struct kith_gateway_session *session)
{
    if (session == nullptr)
    {
        return;
    }
    (void)atomic_fetch_add_explicit(&session->refcount, 1, memory_order_relaxed);
}

void gateway_session_release(struct kith_gateway_session *session)
{
    if (session == nullptr)
    {
        return;
    }
    if (atomic_fetch_sub_explicit(&session->refcount, 1, memory_order_acq_rel) != 1)
    {
        return;
    }
    // Last reference dropped. The session has already been removed from the
    // gateway's session table by kith_gateway_session_destroy (the owner
    // reference path); a worker dropping a dispatch reference here does not
    // touch the table or the gateway, so this teardown is safe off the
    // reactor thread. The connection reference acquired at create is
    // released here, not in destroy, so the bound connection outlives any
    // in-flight dispatch that reads session metadata.
    // View teardown goes through the view module's fini so every composed
    // buffer (subjects, prior ids, and the composer's per-session skip
    // baseline) is retired in one place; do not inline these frees here.
    // The delivery state is retired through its strategy's fini so the
    // strategy owns its own teardown shape; a worker dropping the last
    // dispatch reference runs this after the reactor stopped driving the
    // session, so no deliver pass can race the fini.
    gateway_view_fini(&session->view);
    if (session->delivery_strategy && session->delivery_strategy->session_fini)
    {
        session->delivery_strategy->session_fini(session->delivery_state);
    }
    kith_free(session->allocator, session->window);
    kith_free(session->allocator, session->pending);
    pthread_mutex_destroy(&session->window_lock);
    kith_net_conn_release(session->conn);
    kith_free(session->allocator, session);
}

/*---------------------------------------------------------------------------
 * public session API
 *-------------------------------------------------------------------------*/

/** Discard a session that failed to finish creating: it never entered the
 *  table, so destroy's teardown never runs for it. */
static void session_discard(struct kith_gateway_session *s)
{
    pthread_mutex_destroy(&s->window_lock);
    kith_free(s->allocator, s);
}

/** Bind the delivery strategy before the session becomes visible in the
 *  table: an unknown name fails creation (ENOENT) rather than surfacing
 *  as a per-tick deliver error, and a failing strategy init rolls the
 *  whole create back. The vtable pointer targets the registry's stable
 *  entry storage, so it outlives every subsequent registration. */
static int session_bind_strategy(struct kith_gateway_session *s, kith_gateway_t *gateway)
{
    const kith_gateway_delivery_vtable_t *strategy =
        gateway_delivery_registry_find(&gateway->deliveries, gateway->delivery_strategy_name);
    if (!strategy)
    {
        return kith_error_return(KITH_ENOENT);
    }
    void *strategy_state = nullptr;
    const int drc = strategy->session_init(s, gateway->delivery_config, &strategy_state);
    if (drc != 0)
    {
        return drc;
    }
    s->delivery_strategy = strategy;
    s->delivery_state = strategy_state;
    return 0;
}

/** Zero a fresh session's delivery-total atomics; the fold adds on the
 *  first delivery. */
static void session_init_delivery_totals(struct kith_gateway_session *s)
{
    atomic_init(&s->delivery_enqueued_total, 0u);
    atomic_init(&s->delivery_dropped_total, 0u);
    atomic_init(&s->delivery_events_enqueued_total, 0u);
    atomic_init(&s->delivery_suppressed_total, 0u);
}

[[nodiscard]] KITH_API int kith_gateway_session_create(kith_gateway_t *gateway,
                                                       kith_net_conn_t *conn,
                                                       kith_gateway_session_type_t type,
                                                       uint64_t principal_id,
                                                       const kith_allocator_t *alloc,
                                                       kith_gateway_session_t **out_session)
{
    if (!out_session)
    {
        return kith_error_return(KITH_EINVAL);
    }
    *out_session = nullptr;
    if (!gateway || !conn)
    {
        return kith_error_return(KITH_EINVAL);
    }
    // The session is gateway-attached storage, so a NULL allocator derives
    // the gateway's instance rather than the default; a supplied allocator
    // overrides it. Either way the session carries its own copy: its last
    // reference can drop on a worker thread after the gateway is gone.
    if (alloc != nullptr)
    {
        const kith_error_t alloc_rc = kith_allocator_check(alloc);
        if (alloc_rc != 0)
        {
            return kith_error_return(alloc_rc);
        }
    }
    const kith_allocator_t *allocator = (alloc != nullptr) ? alloc : gateway->allocator;

    struct kith_gateway_session *s = kith_alloc_zero(allocator, 1, sizeof(*s));
    if (!s)
    {
        return kith_error_return(KITH_ENOMEM);
    }
    s->allocator = allocator;
    s->gateway = gateway;
    s->conn = conn;
    s->type = type;
    s->principal_id = principal_id;
    atomic_init(&s->actor_id, 0u);
    atomic_init(&s->refcount, 1);
    atomic_init(&s->delivery_in_flight, false);
    session_init_delivery_totals(s);
    gateway_view_init(&s->view, allocator);
    s->window = nullptr;
    s->window_count = 0u;
    s->window_cap = 0u;
    s->pending = nullptr;
    s->pending_count = 0u;
    s->pending_cap = 0u;
    s->pending_head = 0u;
    if (pthread_mutex_init(&s->window_lock, nullptr) != 0)
    {
        session_discard(s);
        return kith_error_return(KITH_ENOMEM);
    }

    int rc = session_bind_strategy(s, gateway);
    if (rc != 0)
    {
        session_discard(s);
        return rc;
    }

    rc = gateway_session_insert(&gateway->sessions, s);
    if (rc != 0)
    {
        // The session never reaches destroy's teardown, so the strategy
        // state initialized above is retired here.
        if (s->delivery_strategy->session_fini)
        {
            s->delivery_strategy->session_fini(s->delivery_state);
        }
        session_discard(s);
        return rc;
    }
    s->session_id = gateway->sessions.next_session_id;
    gateway->sessions.next_session_id += 1u;
    kith_net_conn_acquire(conn);
    atomic_fetch_add_explicit(&gateway->session_count, 1u, memory_order_release);
    *out_session = s;
    return 0;
}

/*---------------------------------------------------------------------------
 * destroyed-session notification
 *-------------------------------------------------------------------------*/

// A heap-allocated work record carrying one destroyed-session notification
// across the reactor→worker handoff. The identity record is copied because
// the session's memory is released by the owner-reference drop right after
// the notification is submitted, so the record holds no session reference;
// the game context is the registration's user_data, alive for as long as
// the registration stands. The record frees itself after the callback runs.
struct gateway_destroyed_work
{
    /** Allocator copied from the gateway at submit; the record frees
     *  itself through this copy on the worker thread. */
    const kith_allocator_t *allocator;
    kith_gateway_session_destroyed_fn fn;
    void *user_data;
    /** Handler flags copied from the registration slot: the worker task
     *  reads the PYTHON bit to honor the interpreter exit gate. */
    uint32_t flags;
    kith_gateway_session_info_t info;
};

static void gateway_destroyed_worker_task(void *arg)
{
    struct gateway_destroyed_work *work = arg;
    // A python-bound trampoline must not enter a finalizing interpreter: a
    // foreign C thread's entry there has no graceful path. Pool-flagged
    // native C callbacks never enter the interpreter and still run. The
    // refusal is not counted: the process is exiting, and there is nothing
    // left to notify.
    if ((work->flags & KITH_GATEWAY_HANDLER_PYTHON) != 0u && kith_python_finalizing())
    {
        kith_free(work->allocator, work);
        return;
    }
    work->fn(&work->info, work->user_data);
    kith_free(work->allocator, work);
}

// Fire the destroyed-session notification for a session whose teardown is
// complete except for the owner reference. The registration is copied under
// the slot's mutex so a concurrent re-registration never tears the
// dispatch. Pool-bound kinds submit a self-contained work record; a
// submission with no pool attached, or refused by a saturated pool, is a
// dropped notification — the game's disconnect bookkeeping does not run, so
// the drop counts in the gateway's dedicated lifecycle gauge.
static void gateway_session_notify_destroyed(kith_gateway_t *gateway,
                                             struct kith_gateway_session *session)
{
    kith_gateway_session_info_t info;
    (void)kith_gateway_session_info(session, &info);

    kith_gateway_session_destroyed_fn fn;
    void *user_data;
    uint32_t flags;
    pthread_mutex_lock(&gateway->destroyed.lock);
    fn = gateway->destroyed.fn;
    user_data = gateway->destroyed.user_data;
    flags = gateway->destroyed.flags;
    pthread_mutex_unlock(&gateway->destroyed.lock);
    if (fn == nullptr)
    {
        return;
    }

    if (flags == KITH_GATEWAY_HANDLER_NONE)
    {
        fn(&info, user_data);
        return;
    }

    struct gateway_destroyed_work *work = kith_alloc(gateway->allocator, sizeof(*work));
    if (work == nullptr)
    {
        atomic_fetch_add_explicit(&gateway->lifecycle_dropped, 1u, memory_order_relaxed);
        return;
    }
    work->allocator = gateway->allocator;
    work->fn = fn;
    work->user_data = user_data;
    work->flags = flags;
    work->info = info;
    if (gateway->workers == nullptr ||
        kith_worker_submit(gateway->workers, gateway_destroyed_worker_task, work) != 0)
    {
        kith_free(work->allocator, work);
        atomic_fetch_add_explicit(&gateway->lifecycle_dropped, 1u, memory_order_relaxed);
    }
}

KITH_API void kith_gateway_session_destroy(kith_gateway_session_t *session)
{
    if (!session)
    {
        return;
    }
    // Remove from the gateway's session table before dropping the owner
    // reference. The table is reactor-thread-only (no lock), so destroy is
    // called from the reactor thread; once removed, no new dispatch can
    // locate this session and acquire a reference. The owner reference is
    // then dropped via gateway_session_release: if a worker dispatch is in
    // flight (it holds its own reference), the session's memory is freed
    // when that dispatch releases, not here.
    kith_gateway_t *gateway = session->gateway;
    if (gateway != nullptr)
    {
        gateway_session_remove(&gateway->sessions, session->conn);
        atomic_fetch_sub_explicit(&gateway->session_count, 1u, memory_order_release);
    }
    // Detach and drain under window_lock so an in-flight dispatch's window
    // operations serialize against teardown: each one either commits fully
    // against the live gateway or observes the detached state and becomes
    // inert. The remaining cells release their cache refcounts here, so
    // disconnecting without an explicit clear leaves no subscribed nodes
    // behind.
    pthread_mutex_lock(&session->window_lock);
    session->gateway = nullptr;
    for (size_t i = 0u; i < session->window_count; ++i)
    {
        (void)kith_gateway_unsubscribe(gateway, &session->window[i]);
    }
    session->window_count = 0u;
    session->window_seq += 1u;
    // Retained adds die with the session: their cells were never
    // subscribed, so there is nothing to unsubscribe — the mirror gauge
    // drops by the ring's depth and the array itself frees at the final
    // reference release.
    if (gateway != nullptr && session->pending_count > 0u)
    {
        atomic_fetch_sub_explicit(
            &gateway->window_retries_pending, session->pending_count, memory_order_release);
        session->pending_count = 0u;
    }
    pthread_mutex_unlock(&session->window_lock);
    // The notification fires on the captured gateway pointer: the detach
    // above already nulled session->gateway, and the captured pointer is
    // non-NULL exactly when the session was table-attached. The owner
    // reference drops after it, so the notification is the last ordered
    // step of the teardown.
    if (gateway != nullptr)
    {
        gateway_session_notify_destroyed(gateway, session);
    }
    gateway_session_release(session);
}

KITH_API void kith_gateway_session_acquire(kith_gateway_session_t *session)
{
    gateway_session_acquire(session);
}

KITH_API void kith_gateway_session_release(kith_gateway_session_t *session)
{
    gateway_session_release(session);
}

[[nodiscard]] KITH_API int kith_gateway_session_info(const kith_gateway_session_t *session,
                                                     kith_gateway_session_info_t *out_info)
{
    if (!session || !out_info)
    {
        return kith_error_return(KITH_EINVAL);
    }
    out_info->session_id = session->session_id;
    out_info->principal_id = session->principal_id;
    out_info->actor_id = atomic_load_explicit(&session->actor_id, memory_order_acquire);
    out_info->type = session->type;
    out_info->pad = 0u;
    return 0;
}

KITH_API kith_net_conn_t *kith_gateway_session_conn(const kith_gateway_session_t *session)
{
    if (!session)
    {
        return nullptr;
    }
    return session->conn;
}

[[nodiscard]] KITH_API int kith_gateway_session_bind_actor(kith_gateway_session_t *session,
                                                           uint64_t actor_id)
{
    if (!session)
    {
        return kith_error_return(KITH_EINVAL);
    }
    // Store first, then bump the counter under the lock: a composer whose
    // window snapshot follows this unlock reads the new id through the
    // release-acquire edge, while a composition already holding a pre-bump
    // snapshot sees the counter move and rebuilds at its next refresh
    // instead of skipping on a baseline that mixed the two identities.
    // No other state rides the binding edge, so this ordering exists for
    // that invalidation guarantee alone.
    atomic_store_explicit(&session->actor_id, actor_id, memory_order_release);
    pthread_mutex_lock(&session->window_lock);
    session->window_seq += 1u;
    pthread_mutex_unlock(&session->window_lock);
    return 0;
}

/*---------------------------------------------------------------------------
 * per-subscriber subscription window
 *-------------------------------------------------------------------------*/

static bool gateway_window_contains_locked(const kith_gateway_session_t *s,
                                           const kith_fabric_cell_key_t *key)
{
    for (size_t i = 0u; i < s->window_count; ++i)
    {
        const kith_fabric_cell_key_t *k = &s->window[i];
        if (k->zone == key->zone && k->cell_x == key->cell_x && k->cell_y == key->cell_y &&
            k->cell_z == key->cell_z && k->lod == key->lod)
        {
            return true;
        }
    }
    return false;
}

/** Whether @p key is already retained in the pending ring. Caller holds
 *  window_lock; a zero-capacity ring (nothing retained yet) contains
 *  nothing. */
static bool gateway_window_pending_contains_locked(const struct kith_gateway_session *s,
                                                   const kith_fabric_cell_key_t *key)
{
    for (size_t i = 0u; i < s->pending_count; ++i)
    {
        const kith_fabric_cell_key_t *k = &s->pending[(s->pending_head + i) % s->pending_cap];
        if (k->zone == key->zone && k->cell_x == key->cell_x && k->cell_y == key->cell_y &&
            k->cell_z == key->cell_z && k->lod == key->lod)
        {
            return true;
        }
    }
    return false;
}

/** Drop the pending ring's logical entry @p index, preserving the order of
 *  the remaining entries. Caller holds window_lock. */
static void gateway_window_pending_drop_locked(struct kith_gateway_session *s,
                                               kith_gateway_t *gateway,
                                               size_t index)
{
    for (size_t i = index; i + 1u < s->pending_count; ++i)
    {
        const size_t cur = (s->pending_head + i) % s->pending_cap;
        const size_t next = (s->pending_head + i + 1u) % s->pending_cap;
        s->pending[cur] = s->pending[next];
    }
    s->pending_count -= 1u;
    atomic_fetch_sub_explicit(&gateway->window_retries_pending, 1u, memory_order_release);
}

/** Cancel every retained add for @p key. Caller holds window_lock. */
static void gateway_window_pending_purge_key_locked(struct kith_gateway_session *s,
                                                    kith_gateway_t *gateway,
                                                    const kith_fabric_cell_key_t *key)
{
    for (size_t i = 0u; i < s->pending_count;)
    {
        const kith_fabric_cell_key_t *k = &s->pending[(s->pending_head + i) % s->pending_cap];
        if (k->zone == key->zone && k->cell_x == key->cell_x && k->cell_y == key->cell_y &&
            k->cell_z == key->cell_z && k->lod == key->lod)
        {
            gateway_window_pending_drop_locked(s, gateway, i);
        }
        else
        {
            ++i;
        }
    }
}

/** Append @p key at the ring's back, growing the array in logical order
 *  when full so the rotation head stays meaningful. Caller holds
 *  window_lock; the pending gauge is the caller's business — a retry pass
 *  rotates an existing entry without touching it. */
static void gateway_window_pending_push_back_locked(struct kith_gateway_session *s,
                                                    const kith_fabric_cell_key_t *key)
{
    if (s->pending_count == s->pending_cap)
    {
        size_t new_cap = s->pending_cap == 0u ? 8u : s->pending_cap * 2u;
        if (new_cap > GATEWAY_WINDOW_PENDING_MAX)
        {
            new_cap = GATEWAY_WINDOW_PENDING_MAX;
        }
        kith_fabric_cell_key_t *buf = kith_alloc(s->allocator, new_cap * sizeof(*buf));
        if (!buf)
        {
            return;
        }
        for (size_t i = 0u; i < s->pending_count; ++i)
        {
            buf[i] = s->pending[(s->pending_head + i) % s->pending_cap];
        }
        kith_free(s->allocator, s->pending);
        s->pending = buf;
        s->pending_cap = new_cap;
        s->pending_head = 0u;
    }
    s->pending[(s->pending_head + s->pending_count) % s->pending_cap] = *key;
    s->pending_count += 1u;
}

/** Retain a capacity-failed window add for the tick pass's retry. Caller
 *  holds window_lock. Deduplicates against the ring — a caller retrying
 *  its own failed add must not fill the ring with duplicates — and bounds
 *  it at GATEWAY_WINDOW_PENDING_MAX: an add arriving while the ring is
 *  full fails without retention. */
static void gateway_window_retain_locked(struct kith_gateway_session *s,
                                         kith_gateway_t *gateway,
                                         const kith_fabric_cell_key_t *key)
{
    if (gateway_window_pending_contains_locked(s, key))
    {
        return;
    }
    if (s->pending_count >= GATEWAY_WINDOW_PENDING_MAX)
    {
        return;
    }
    gateway_window_pending_push_back_locked(s, key);
    atomic_fetch_add_explicit(&gateway->window_retries_pending, 1u, memory_order_release);
}

/** The window-add core shared by the public add and the retry pass. The
 *  key must not already be in the window (both callers check first): the
 *  window array grows in place, and a subscribe failure rolls the array
 *  back so the window and both shared tables stay unchanged on any
 *  failure. Caller holds window_lock; @p gateway is the session's live
 *  gateway. */
[[nodiscard]] static int gateway_window_add_locked(struct kith_gateway_session *s,
                                                   kith_gateway_t *gateway,
                                                   const kith_fabric_cell_key_t *key)
{
    if (s->window_count == s->window_cap)
    {
        size_t new_cap = s->window_cap == 0u ? 8u : s->window_cap * 2u;
        kith_fabric_cell_key_t *buf = kith_realloc(s->allocator, s->window, new_cap * sizeof(*buf));
        if (!buf)
        {
            return kith_error_return(KITH_ENOMEM);
        }
        s->window = buf;
        s->window_cap = new_cap;
    }
    s->window[s->window_count] = *key;
    s->window_count += 1u;
    int rc = kith_gateway_subscribe(gateway, key);
    if (rc != 0)
    {
        s->window_count -= 1u;
        return rc;
    }
    // Every committed array mutation bumps the counter: the composer's skip
    // baseline pairs (counter, per-position content sequences) with the
    // snapshotted window order.
    s->window_seq += 1u;
    return 0;
}

[[nodiscard]] KITH_API int kith_gateway_session_window_add(kith_gateway_session_t *session,
                                                           const kith_fabric_cell_key_t *key)
{
    if (!session || !key)
    {
        return kith_error_return(KITH_EINVAL);
    }
    pthread_mutex_lock(&session->window_lock);
    // The gateway pointer is read once under window_lock; a NULL snapshot
    // means the session is detached and window mutations are inert — the
    // composer can never reach a detached session, and detach already
    // released every remaining cell subscription.
    kith_gateway_t *gateway = session->gateway;
    if (gateway == nullptr)
    {
        pthread_mutex_unlock(&session->window_lock);
        return 0;
    }
    if (gateway_window_contains_locked(session, key))
    {
        pthread_mutex_unlock(&session->window_lock);
        return 0;
    }
    int rc = gateway_window_add_locked(session, gateway, key);
    if (rc != 0)
    {
        // A capacity failure (-KITH_ENOMEM: the cell's cache stripe or the
        // fabric interest set had no free slot) is retained for the tick
        // pass's retry. The churn that saturates the shared tables also
        // frees slots, so a pass lands the add once a slot frees — a
        // one-shot seed heals without a caller-side retry loop. The caller
        // still sees the honest failure, and window_remove or window_clear
        // cancels the retention. Other codes pass uncounted and unretained.
        if (rc == kith_error_return(KITH_ENOMEM))
        {
            atomic_fetch_add_explicit(&gateway->window_add_failures, 1u, memory_order_relaxed);
            gateway_window_retain_locked(session, gateway, key);
        }
        pthread_mutex_unlock(&session->window_lock);
        return rc;
    }
    pthread_mutex_unlock(&session->window_lock);
    return 0;
}

[[nodiscard]] KITH_API int kith_gateway_session_window_remove(kith_gateway_session_t *session,
                                                              const kith_fabric_cell_key_t *key)
{
    if (!session || !key)
    {
        return kith_error_return(KITH_EINVAL);
    }
    pthread_mutex_lock(&session->window_lock);
    // The gateway pointer is read once under window_lock; a NULL snapshot
    // means the session is detached and the cell drops from the window
    // without touching fabric subscriptions (detach already drained them).
    kith_gateway_t *gateway = session->gateway;
    // A remove cancels a retained add for the cell: the game rescinded the
    // intent, so the retry pass never lands it.
    if (gateway != nullptr)
    {
        gateway_window_pending_purge_key_locked(session, gateway, key);
    }
    for (size_t i = 0u; i < session->window_count; ++i)
    {
        const kith_fabric_cell_key_t *k = &session->window[i];
        if (k->zone == key->zone && k->cell_x == key->cell_x && k->cell_y == key->cell_y &&
            k->cell_z == key->cell_z && k->lod == key->lod)
        {
            session->window[i] = session->window[session->window_count - 1u];
            session->window_count -= 1u;
            session->window_seq += 1u;
            int rc = 0;
            if (gateway != nullptr)
            {
                rc = kith_gateway_unsubscribe(gateway, key);
            }
            pthread_mutex_unlock(&session->window_lock);
            return rc;
        }
    }
    pthread_mutex_unlock(&session->window_lock);
    return 0;
}

KITH_API void kith_gateway_session_window_clear(kith_gateway_session_t *session)
{
    if (!session)
    {
        return;
    }
    pthread_mutex_lock(&session->window_lock);
    // The gateway pointer is read once under window_lock; a NULL snapshot
    // means the session is detached and its cell subscriptions were
    // already released during detach.
    kith_gateway_t *gateway = session->gateway;
    if (gateway != nullptr)
    {
        for (size_t i = 0u; i < session->window_count; ++i)
        {
            (void)kith_gateway_unsubscribe(gateway, &session->window[i]);
        }
    }
    session->window_count = 0u;
    // Cleared unconditionally (even from an already-empty window): a clear
    // is a statement about the window, and one forced recomposition is the
    // cheap, safe reading of it.
    session->window_seq += 1u;
    // A clear cancels every retained add with the window itself.
    if (gateway != nullptr && session->pending_count > 0u)
    {
        atomic_fetch_sub_explicit(
            &gateway->window_retries_pending, session->pending_count, memory_order_release);
        session->pending_count = 0u;
    }
    pthread_mutex_unlock(&session->window_lock);
}

[[nodiscard]] KITH_API int kith_gateway_session_populate(kith_gateway_session_t *session,
                                                         uint64_t actor_id,
                                                         const kith_fabric_cell_key_t *keys,
                                                         size_t count)
{
    if (!session || (keys == nullptr && count != 0u))
    {
        return kith_error_return(KITH_EINVAL);
    }
    pthread_mutex_lock(&session->window_lock);
    // Identity and seed ride one window_lock hold, so the census's
    // bound-without-cells reading and the composer's window snapshot can
    // never observe the populate in progress: a reader inside the hold
    // sees the pre-populate pair, one after the unlock sees the populated
    // pair. The store keeps the bind's ordering — identity first, then
    // the counter bump that forces a composer holding a pre-populate
    // snapshot to rebuild — and the composer's actor reads ride the
    // field's default sequencing, so no stronger edge is owed here.
    atomic_store_explicit(&session->actor_id, actor_id, memory_order_release);
    session->window_seq += 1u;
    kith_gateway_t *gateway = session->gateway;
    int rc = 0;
    if (gateway != nullptr)
    {
        for (size_t i = 0u; i < count; ++i)
        {
            if (gateway_window_contains_locked(session, &keys[i]))
            {
                continue;
            }
            int add_rc = gateway_window_add_locked(session, gateway, &keys[i]);
            if (add_rc != 0)
            {
                if (rc == 0)
                {
                    rc = add_rc;
                }
                // A capacity failure is retained for the tick pass's
                // retry, the per-cell add's contract verbatim; other
                // codes pass uncounted and unretained.
                if (add_rc == kith_error_return(KITH_ENOMEM))
                {
                    atomic_fetch_add_explicit(
                        &gateway->window_add_failures, 1u, memory_order_relaxed);
                    gateway_window_retain_locked(session, gateway, &keys[i]);
                }
            }
        }
    }
    pthread_mutex_unlock(&session->window_lock);
    return rc;
}

[[nodiscard]] KITH_API int kith_gateway_session_window_count(const kith_gateway_session_t *session,
                                                             size_t *out_count)
{
    if (!session || !out_count)
    {
        return kith_error_return(KITH_EINVAL);
    }
    gateway_mutex_lock(&session->window_lock);
    *out_count = session->window_count;
    gateway_mutex_unlock(&session->window_lock);
    return 0;
}

void gateway_window_tick_pass(struct kith_gateway_session *session, size_t *out_without_cells)
{
    *out_without_cells = 0u;
    if (session == nullptr)
    {
        return;
    }
    pthread_mutex_lock(&session->window_lock);
    kith_gateway_t *gateway = session->gateway;
    // The retry pass rotates the ring: every visited entry is popped from
    // the front; a stale retention and a landed add are consumed, and an
    // entry the capacity guards keep returns to the back. Rotation without
    // reordering keeps the ring's logical mapping intact across passes, and
    // the pending gauge moves only when an entry truly leaves the ring.
    if (gateway != nullptr && session->pending_count > 0u)
    {
        for (size_t visited = 0u;
             visited < GATEWAY_WINDOW_RETRY_PASS_CAP && visited < session->pending_count;
             ++visited)
        {
            const kith_fabric_cell_key_t key = session->pending[session->pending_head];
            session->pending_head = (session->pending_head + 1u) % session->pending_cap;
            session->pending_count -= 1u;
            if (gateway_window_contains_locked(session, &key))
            {
                // The caller landed the add itself; the retention is stale.
                atomic_fetch_sub_explicit(
                    &gateway->window_retries_pending, 1u, memory_order_release);
                continue;
            }
            // The O(1) capacity guards keep a saturated pass from paying a
            // full locked probe per retained add: the cache stripe's live
            // count answers the cache leg, and the fabric interest set's
            // size against its contracted bucket count answers the fabric
            // leg. An entry behind either guard returns to the back and
            // retries on a subsequent pass.
            if (gateway_cache_stripe_saturated(&gateway->cache, &key) ||
                kith_fabric_subscription_size(gateway->cache.sub) >=
                    KITH_FABRIC_DEFAULT_BUCKET_COUNT)
            {
                gateway_window_pending_push_back_locked(session, &key);
                continue;
            }
            if (gateway_window_add_locked(session, gateway, &key) == 0)
            {
                atomic_fetch_add_explicit(&gateway->window_retry_adds, 1u, memory_order_relaxed);
                atomic_fetch_sub_explicit(
                    &gateway->window_retries_pending, 1u, memory_order_release);
                continue;
            }
            // A capacity failure that slipped past the guards retries on a
            // subsequent pass; any other code is unreachable from this
            // path and is kept too — the pending gauge makes a stuck entry
            // visible.
            gateway_window_pending_push_back_locked(session, &key);
        }
    }
    // The seed-failure census: a session bound to an actor while tracking
    // no cells composes no view and receives no frames. Read under the
    // same hold as the retry so the reactor's census never races a
    // worker's window mutation.
    if (atomic_load_explicit(&session->actor_id, memory_order_acquire) != 0u &&
        session->window_count == 0u)
    {
        *out_without_cells = 1u;
    }
    pthread_mutex_unlock(&session->window_lock);
}

uint64_t gateway_session_window_matches(struct kith_gateway_session *session,
                                        const kith_fabric_cell_key_t *keys,
                                        size_t key_count)
{
    uint64_t mask = 0u;
    if (session == nullptr || keys == nullptr || key_count == 0u)
    {
        return mask;
    }
    // One hold for the whole batch: a broadcast drain walks every request
    // key against one window read, so a queue of requests costs a session
    // one lock acquisition instead of one per request.
    pthread_mutex_lock(&session->window_lock);
    for (size_t i = 0u; i < key_count; ++i)
    {
        if (gateway_window_contains_locked(session, &keys[i]))
        {
            mask |= (uint64_t)1u << i;
        }
    }
    pthread_mutex_unlock(&session->window_lock);
    return mask;
}

[[nodiscard]] KITH_API uint64_t kith_gateway_session_count(const kith_gateway_t *gateway)
{
    if (gateway == nullptr)
    {
        return 0u;
    }
    return atomic_load_explicit(&gateway->session_count, memory_order_acquire);
}

[[nodiscard]] KITH_API int kith_gateway_session_snapshot(const kith_gateway_t *gateway,
                                                         kith_gateway_session_info_t *out,
                                                         size_t max,
                                                         size_t *out_count)
{
    if (gateway == nullptr || (out == nullptr && max > 0u))
    {
        return kith_error_return(KITH_EINVAL);
    }
    size_t copied = 0u;
    for (size_t i = 0u; i < gateway->sessions.buckets && copied < max; ++i)
    {
        struct kith_gateway_session *entry = gateway->sessions.slots[i];
        if (entry != nullptr)
        {
            (void)kith_gateway_session_info(entry, &out[copied]);
            copied += 1u;
        }
    }
    if (out_count != nullptr)
    {
        *out_count = copied;
    }
    return 0;
}
