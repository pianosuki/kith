#pragma once

#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>
#include <time.h>

#include <pthread.h>

#include "kith/fabric/fabric.h"
#include "kith/gateway/gateway.h"
#include "kith/net/net.h"
#include "kith/proto/proto.h"
#include "kith/types.h"
#include "kith/version.h"
#include "kith/worker/worker.h"

/**
 * Private gateway internals. The gateway library is split across subsystem
 * translation units (session, cache, view, delivery, handler, gateway) that
 * share this header for structure layouts and hash helpers. The structures
 * here are not visible outside the library.
 *
 * The gateway owns six tables: a session table (open-addressed hash keyed
 * by connection pointer for dispatch lookup), a shared cell cache
 * (open-addressed hash keyed by cell locator, refcounted per local
 * subscriber), a handler registration table (direct-indexed array of message
 * type slots, mutex-protected), a destroyed-session notification slot (one
 * registration per gateway, mutex-serialized against the reactor-thread
 * fire), a delivery strategy registry (named vtable presets, populated at
 * create and extended before sessions bind), and the cell-scoped broadcast
 * request queue (fixed-capacity FIFO; submits arrive on any thread, the
 * tick's drain runs on the reactor thread). Per-session view state (the
 * composed scored view set plus prior-view actor ids for sticky continuity)
 * lives on each session, served by one per-gateway compose scratch reused
 * across view refreshes on the reactor thread. The gateway borrows net,
 * fabric, and proto handles from the composition root.
 */

struct gateway_compose_entry;

/*---------------------------------------------------------------------------
 * mutex helpers
 *-------------------------------------------------------------------------*/

/* Internally-synchronized read APIs take const handles: the mutex is guard
 * state, not the data it protects. The cast lives here so the const surface
 * holds at every call site. */
static inline void gateway_mutex_lock(const pthread_mutex_t *m)
{
    pthread_mutex_lock((pthread_mutex_t *)m);
}

static inline void gateway_mutex_unlock(const pthread_mutex_t *m)
{
    pthread_mutex_unlock((pthread_mutex_t *)m);
}

/*---------------------------------------------------------------------------
 * hash helpers
 *-------------------------------------------------------------------------*/

/** Mix a 5-tuple cell key into a 64-bit hash for cache bucket selection.
 *  Mirrors the fabric's cell mix so cache and fabric bucketing align. */
static inline uint64_t
gateway_cell_mix(uint32_t zone, int32_t cx, int32_t cy, int32_t cz, uint8_t lod)
{
    uint64_t k = (uint64_t)zone;
    k = k * 131u + (uint64_t)(uint32_t)cx;
    k = k * 131u + (uint64_t)(uint32_t)cy;
    k = k * 131u + (uint64_t)(uint32_t)cz;
    k = k * 131u + (uint64_t)lod;
    k ^= k >> 33;
    k *= 0xff51afd7ed558ccdULL;
    k ^= k >> 33;
    return k;
}

/** Mix a connection pointer into a 64-bit hash for session-table lookup. */
static inline uint64_t gateway_conn_mix(const void *conn)
{
    uint64_t k = (uint64_t)(uintptr_t)conn;
    k ^= k >> 33;
    k *= 0xff51afd7ed558ccdULL;
    k ^= k >> 33;
    return k;
}

static inline size_t gateway_mask(size_t buckets)
{
    return buckets - 1u;
}

/*---------------------------------------------------------------------------
 * timing
 *-------------------------------------------------------------------------*/

/** Monotonic nanosecond timestamp for phase and sub-phase timing. Mirrors
 *  the reactor's and control plane's CLOCK_MONOTONIC read (a vDSO call on
 *  Linux, so the per-session bracketing in kith_gateway_tick and the
 *  per-sub-phase bracketing in the composer stay cheap). Nanosecond
 *  accumulation avoids per-session rounding loss; the composition root
 *  divides by 1e6 to report ms. Used only to accumulate the per-phase ns
 *  totals. */
static inline uint64_t gateway_now_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

/*---------------------------------------------------------------------------
 * handler registration table
 *-------------------------------------------------------------------------*/

/** One message-type slot in the handler registration table. */
struct gateway_handler_slot
{
    /** Registered callback, or NULL when the slot is free. */
    kith_gateway_msg_handler_fn fn;
    /** Caller context passed verbatim to fn on dispatch. */
    void *user_data;
    /** Registration flags (bitwise OR of kith_gateway_handler_flag).
     *  Pool-bound slots (KITH_GATEWAY_HANDLER_PYTHON or
     *  KITH_GATEWAY_HANDLER_POOL) are submitted to the worker pool when one
     *  is attached; unflagged C slots run inline. */
    uint32_t flags;
};

/** Direct-indexed array of handler slots, one per message type id. */
struct gateway_handler_table
{
    /** Allocator resolved at gateway create; the slot array allocates and
     *  frees through it. */
    const kith_allocator_t *allocator;
    struct gateway_handler_slot *slots;
    size_t capacity;
    pthread_mutex_t lock;
};

/** Initialize a handler table with @p capacity type-id slots. Returns 0
 *  on success, negative kith_error on allocation or mutex failure. */
[[nodiscard]] int gateway_handler_init(struct gateway_handler_table *t,
                                       size_t capacity,
                                       const kith_allocator_t *alloc);

/** Free a handler table's slot array and destroy its mutex. */
void gateway_handler_fini(struct gateway_handler_table *t);

/** Destroyed-session notification slot: one registration per gateway. The
 *  registration moves under the slot's mutex; the fire reads a copy of the
 *  registration so a concurrent re-registration never tears a dispatch. */
struct gateway_destroyed_slot
{
    /** Registered callback, or NULL when the slot is free. */
    kith_gateway_session_destroyed_fn fn;
    /** Caller context passed verbatim to fn on dispatch. */
    void *user_data;
    /** Registration flags (bitwise OR of kith_gateway_handler_flag). */
    uint32_t flags;
    /** Serializes registration against the reactor-thread fire. */
    pthread_mutex_t lock;
};

/*---------------------------------------------------------------------------
 * shared cell cache
 *-------------------------------------------------------------------------*/

/** One refcounted cached cell. Stores the cell product header plus the
 *  full-fidelity artifacts refreshed from the fabric so the relevance
 *  composer can score candidates without re-querying the fabric per
 *  subscriber. */
struct gateway_cache_node
{
    /** Cell locator. */
    kith_fabric_cell_key_t key;
    /** Shared subscription refcount (local subscribers interested). */
    uint32_t refcount;
    /** Authority epoch (stale-product guard). */
    uint32_t authority_epoch;
    /** Latest publish sequence observed in the cache for this cell. */
    uint64_t latest_publish_seq;
    /** Monotonic millisecond timestamp of the last refresh for this cell. */
    uint64_t refreshed_at_ms;
    /** Content sequence of the cached snapshot: minted from the cache-wide
     *  clock at node creation and re-minted on every refresh that replaces
     *  the observable front-buffer content (the staging swap and the
     *  empty-cell clear). A failed or stale refresh mints nothing, so an
     *  unchanged snapshot keeps its sequence. Written under this node's
     *  stripe lock and probed by the composer under the window's stripes;
     *  the composer compares it against the per-window baseline recorded
     *  with the session's last full composition. */
    uint64_t content_seq;
    /** Number of actors in this cell (sourced from the fabric). */
    uint32_t actor_count;
    /** Source product level (full-fidelity from the fabric). */
    kith_fabric_product_level_t product_level;
    /** Slot occupied (probing stops at an unused slot). */
    bool used;
    /** Slot vacated; probing continues past a tombstone. */
    bool deleted;
    /** Full-fidelity artifacts refreshed from the fabric (may be NULL).
     *  Sorted ascending by actor_id at refresh so the composer's
     *  subscriber lookup is a binary search. */
    kith_fabric_artifact_t *artifacts;
    /** Number of artifacts in the artifacts array. */
    size_t artifact_count;
    /** Capacity (rows) of the artifacts array. */
    size_t artifact_cap;
    /** Staging buffer for the next refresh; the back side of a ping-pong
     *  pair with @p artifacts. Grow-only across refreshes so steady-state
     *  refresh performs no allocation, and swapped in only on success so
     *  the prior snapshot survives every failure path. May be NULL. */
    kith_fabric_artifact_t *staging;
    /** Capacity (rows) of the staging array. */
    size_t staging_cap;
    /** Sum of artifact pos_x across the artifacts array (0 when empty).
     *  Accumulated once per refresh so the composer's crowd aggregate
     *  reads O(1) per window cell instead of summing every artifact per
     *  subscriber. */
    int64_t sum_x;
    /** Sum of artifact pos_y (see sum_x). */
    int64_t sum_y;
    /** Sum of artifact pos_z (see sum_x). */
    int64_t sum_z;
};

/** Number of lock stripes sharding the cell cache. A power of two so the
 *  stripe index is a cheap bitmask of the cell hash. Each stripe owns an
 *  independent open-addressed sub-hash and a mutex, so a worker-thread
 *  subscribe/unsubscribe on one cell does not block the reactor composing
 *  or refreshing a cell homed in a different stripe. */
#define GATEWAY_CACHE_STRIPE_COUNT 16u
/** Log2 of GATEWAY_CACHE_STRIPE_COUNT; the in-stripe bucket is taken from
 *  the bits above the stripe index so the stripe and bucket selections do
 *  not correlate. */
#define GATEWAY_CACHE_STRIPE_SHIFT 4u
static_assert(GATEWAY_CACHE_STRIPE_COUNT == (1u << GATEWAY_CACHE_STRIPE_SHIFT),
              "stripe count must be a power of two matching the shift");

/** Per-session ceiling on retained (retryable) window adds. A window add
 *  that fails against transient capacity — the cell cache's stripe or the
 *  fabric interest set had no free slot — is retained here and retried by
 *  the tick pass until it lands; an add arriving while the list is full
 *  fails without retention. */
#define GATEWAY_WINDOW_PENDING_MAX 64u
/** Per-pass ceiling on retained-add retry attempts for one session.
 *  Bounded work keeps a saturated pass from paying a probe per retained
 *  add per session; the rotation head walks the list across passes. */
#define GATEWAY_WINDOW_RETRY_PASS_CAP 8u

/** One shard of the cell cache: an open-addressed hash of refcounted
 *  cached cells plus the mutex serializing structure and artifact access
 *  within the shard. The mutex is cache-line-aligned so concurrent stripes
 *  do not false-share their locks (AGENTS.md §2.1); the alignment
 *  propagates to the struct, padding its size to a cache-line multiple so
 *  adjacent array elements stay on separate lines. */
struct gateway_cache_stripe
{
    /** Serializes find/create/evict and artifact refresh within this
     *  shard. Held by the reactor during refresh (per drained cell) and
     *  view composition (all window cells homed in this shard) and by a
     *  worker during a window add/remove that touches a cell in this
     *  shard. */
    alignas(64) pthread_mutex_t lock;
    /** Open-addressed hash table of cached cells in this shard. */
    struct gateway_cache_node *nodes;
    /** Power-of-two slot count in @p nodes. */
    size_t buckets;
    /** Live (non-tombstone) entries in this shard. */
    size_t count;
};

/** Sharded cell cache: N independent open-addressed sub-hashes keyed by
 *  kith_fabric_cell_key_t, refcounted entries refreshed from a fabric
 *  subscription, and a per-stripe mutex. A cell maps to stripe
 *  gateway_key_hash(key) & (GATEWAY_CACHE_STRIPE_COUNT - 1u); the in-stripe
 *  bucket is (gateway_key_hash(key) >> GATEWAY_CACHE_STRIPE_SHIFT) &
 *  (buckets - 1u). The fabric subscription is shared across shards and is
 *  internally synchronized by the fabric, so it carries no cache-side lock.
 *  Drives view.c (relevance composition) and is owned by gateway.c. */
struct gateway_cache
{
    /** Allocator resolved at gateway create; the stripe tables, the
     *  node artifact buffers, and the refresh drain scratch allocate and
     *  free through it. */
    const kith_allocator_t *allocator;
    /** Per-stripe sub-hashes. */
    struct gateway_cache_stripe stripes[GATEWAY_CACHE_STRIPE_COUNT];
    /** The fabric subscription shared by all local subscribers.
     *  Internally synchronized (kith_fabric_subscription_add/remove/
     *  drain/size are thread-safe per the fabric contract). */
    kith_fabric_subscription_t *sub;
    /** Monotonic millisecond timestamp of the last cache refresh. Reactor
     *  thread only (kith_gateway_cache_refresh runs on the tick path; the
     *  public API is documented @thread_safety unsafe). */
    uint64_t last_refresh_ms;
    /** Value dispenser for node content sequences. A relaxed fetch-add
     *  mints a fresh value at node creation (worker threads create nodes
     *  under the new node's stripe lock) and on every content-replacing
     *  refresh (reactor thread, under the target node's stripe lock);
     *  stripes do not order one another, so the shared counter must be
     *  atomic even though each node's own sequence is lock-protected.
     *  Minted-value uniqueness across node lifetimes is what defeats an
     *  evict/recreate false match in the composer's skip probe. Seeded at
     *  1 so no minted value collides with the 0 "node absent" sentinel in
     *  the per-window baselines. Never persisted, logged, hashed, or
     *  exported: it is in-process skip-comparison state only, and emitting
     *  it would leak nondeterministic scheduling into any replay digest. */
    alignas(64) _Atomic uint64_t content_seq_clock;
};

/*---------------------------------------------------------------------------
 * delivery strategy registry
 *-------------------------------------------------------------------------*/

/** One registered delivery strategy: owned name + copied vtable. */
struct gateway_delivery_entry
{
    /** Owned NUL-terminated strategy name (registry-allocated). */
    char *name;
    /** Copied vtable; stable address for the gateway's lifetime. */
    kith_gateway_delivery_vtable_t vtable;
};

/** Per-gateway registry of delivery strategies, keyed by name (linear
 *  scan; the registry holds a handful of entries). Reactor-thread-only:
 * populated at gateway creation with the built-in presets and extended
 *  via the public registration call before sessions bind it. */
struct gateway_delivery_registry
{
    /** Allocator resolved at gateway create; the entry array and the
     *  strategy name copies allocate and free through it. */
    const kith_allocator_t *allocator;
    /** Registered entries (grow-only array). */
    struct gateway_delivery_entry *entries;
    /** Number of entries in @p entries. */
    size_t count;
    /** Capacity of @p entries. */
    size_t cap;
};

int gateway_delivery_registry_init(struct gateway_delivery_registry *r,
                                   const kith_allocator_t *alloc);
void gateway_delivery_registry_fini(struct gateway_delivery_registry *r);

/** Register @p vtable under an owned copy of @p name. Returns 0 on
 *  success, negative kith_error (EINVAL/ESIZE/EABIVER/EEXIST/ENOMEM)
 *  matching the sim model registry's conventions. */
[[nodiscard]] int gateway_delivery_registry_register(struct gateway_delivery_registry *r,
                                                     const char *name,
                                                     const kith_gateway_delivery_vtable_t *vtable);

/** Look up a strategy by exact name, or NULL when absent. */
const kith_gateway_delivery_vtable_t *
gateway_delivery_registry_find(const struct gateway_delivery_registry *r, const char *name);

/** Register the built-in presets ("full", and further factory presets) on
 *  @p r. Called once during gateway creation. Returns 0 on success,
 *  negative kith_error on allocation failure. */
[[nodiscard]] int gateway_delivery_builtins_register(struct gateway_delivery_registry *r);

/*---------------------------------------------------------------------------
 * per-gateway compose scratch
 *-------------------------------------------------------------------------*/

/** Working buffers for one view composition, reused across refreshes so the
 *  steady-state compose path performs no allocation. Reactor thread only:
 *  view refresh runs on the tick path (kith_gateway_view_refresh is
 *  documented @thread_safety unsafe), so one set of buffers serves every
 *  session's composition on that gateway. The heap entry layout is private
 *  to view.c; only the pointer and capacity are shared here. */
struct gateway_compose_scratch
{
    /** Allocator resolved at gateway create; every growable buffer in
     *  this scratch allocates and frees through it. */
    const kith_allocator_t *allocator;
    /** Copy of the composing session's subscription window, taken under the
     *  session's window lock so the composer can iterate the cells without
     *  holding that lock. Grow-only. */
    kith_fabric_cell_key_t *window;
    /** Capacity of the window buffer. */
    size_t window_cap;
    /** Storage for the bounded candidate heap (view.c entry layout).
     *  Grow-only; capacity equals the per-gateway view budget. */
    struct gateway_compose_entry *heap;
    /** Capacity of the heap buffer. */
    size_t heap_cap;
    /** Open-addressed set of the composing session's prior-view actor ids
     *  (0 marks an empty slot; capacity is a power of two kept at or above
     *  twice the prior-view size, so the load factor never exceeds 0.5).
     *  NULL until the first composition with a non-empty prior view. */
    uint64_t *prior_set;
    /** Capacity of the prior-id set (0 when never allocated). */
    size_t prior_set_cap;
    /** Snapshot of the prior-view ids taken before a rebuild overwrites
     *  them, so membership departures can be diffed after selection.
     *  Grow-only; reactor thread only. */
    uint64_t *prior_snapshot;
    /** Capacity of @p prior_snapshot. */
    size_t prior_snap_cap;
};

/** Working buffer for multi-subject batch delivery, reused across sessions
 *  so the steady-state batch delivery path performs no per-session
 *  allocation. One instance per delivery thread: the gateway's own scratch
 *  serves the reactor thread (and inline delivery), while executor threads
 *  lazily claim private thread-local scratches registered for teardown —
 *  the full strategy's batch fill and the enqueue under it
 *  must never share one buffer across threads. The
 *  @c gateway_delivery_scratch accessor returns the calling thread's
 *  scratch. Holds the serialized batch payload (4-byte count header + N
 *  subject records) before it is copied into a net frame. Grow-only; freed
 *  at gateway destroy (executor scratches after the executor drain). */
struct gateway_delivery_scratch
{
    /** Allocator resolved at gateway create (each executor thread's
     *  claimed copy carries the same instance); the batch buffer
     *  allocates and frees through it. */
    const kith_allocator_t *allocator;
    /** Serialized batch payload buffer (4-byte header + N * 64-byte records).
     *  NULL until the first batch delivery. */
    uint8_t *batch;
    /** Capacity of the batch buffer in bytes. */
    size_t batch_cap;
};

/*---------------------------------------------------------------------------
 * delivery executor
 *-------------------------------------------------------------------------*/

struct kith_gateway_session;

/** Zero-allocation delivery job submitted to the executor, embedded in its
 *  session so a submit performs no allocation. @p gateway is captured at
 *  submit: a session destroyed mid-flight nulls its own @c gateway field,
 *  and the worker must keep driving the delivery against the (still-live)
 *  gateway through the executor drain. @p session is captured at submit;
 *  the reference the executor takes at submit keeps it valid until the job
 *  completes. @p now_ms is the tick's stamp: deliver decisions use the
 *  submit-time stamp, and execution-time re-stamping would desynchronize
 *  the delivery cadence clock from the reactor's. */
struct gateway_delivery_job
{
    /** Owning gateway, captured at submit. */
    kith_gateway_t *gateway;
    /** Owning session, captured at submit; the executor's submit-time
     *  reference keeps it valid until the job completes. */
    struct kith_gateway_session *session;
    /** Tick-supplied millisecond stamp (submit time, not execution time). */
    uint64_t now_ms;
};

/** Registry node for one executor thread's claimed scratch. The node owns
 *  the scratch storage so the executor fini can free them together after
 *  the drain. */
struct gateway_scratch_node
{
    /** Next claimed scratch in the executor's registry. */
    struct gateway_scratch_node *next;
    /** The scratch handed to the strategy; never shared across threads. */
    struct gateway_delivery_scratch scratch;
};

/** Gateway-owned executor that moves the tick's per-session deliver pass
 *  off the reactor thread. Created at gateway creation when
 *  params select it (delivery_worker_count > 0); NULL means inline
 *  delivery and zero executor code on the tick path. All counter writes
 *  are relaxed: each is single-writer (the reactor for submit-side
 *  counters, workers for the pending accumulator) or monotone, and the
 *  composition root reads them only for observability. Drained and freed
 *  first at teardown so in-flight jobs complete before the gateway's own
 *  scratch dies. */
struct gateway_delivery_executor
{
    /** Allocator resolved at gateway create; the executor, its worker
     *  pool, and the claimed thread-local scratches allocate and free
     *  through it. */
    const kith_allocator_t *allocator;
    /** Job pool (the shared worker primitive; a disjoint contract from
     *  the Python-handler pool). */
    kith_worker_t *workers;
    /** Cumulative jobs accepted by the pool. */
    _Atomic uint64_t jobs_submitted_total;
    /** Cumulative deliver passes skipped: the session's previous deliver
     *  was still in flight. */
    _Atomic uint64_t inflight_skips_total;
    /** Cumulative submissions rejected with KITH_EBUSY. */
    _Atomic uint64_t ebusy_skips_total;
    /** Cumulative in-flight sessions that skipped the compose wait after
     *  the pass's budget ran out. */
    _Atomic uint64_t wait_budget_exhausted_total;
    /** Cumulative compose waits that exceeded the pass's budget. */
    _Atomic uint64_t compose_wait_timeouts_total;
    /** Jobs currently in flight (gauge). */
    _Atomic uint64_t inflight_current;
    /** Peak of inflight_current since creation (gauge). */
    _Atomic uint64_t inflight_high_watermark;
    /** Deliver nanoseconds accumulated by workers since the last tick
     *  drain; the tick folds it into deliver_ns_total. */
    _Atomic uint64_t deliver_ns_pending;
    /** Guards the claimed-scratch registry (worker-side enrollment, fini
     *  frees the list after the drain). */
    pthread_mutex_t scratch_lock;
    /** Thread-local scratches claimed by executor threads. */
    struct gateway_scratch_node *claimed_scratches;
};

/*---------------------------------------------------------------------------
 * per-subscriber view state
 *-------------------------------------------------------------------------*/

/** One membership transition produced by a composition:
 *  @p actor_id names the subject (0 for the crowd aggregate) and @p kind
 *  is a gateway_delivery_event_kind. The composer regenerates the delta
 *  set on every full recomposition; the bound delivery strategy consumes
 *  it once per deliver pass and carries any not-yet-delivered events in
 *  its own transactional state. */
struct gateway_view_event
{
    /** Subject the event concerns (0 = crowd aggregate). */
    uint64_t actor_id;
    /** gateway_delivery_event_kind value. */
    unsigned int kind;
};

/** The composed scored view set for one subscriber, plus the prior-view
 *  actor ids used for sticky continuity across refreshes. */
struct gateway_view_state
{
    /** Allocator copied from the owning session at create; every growable
     *  buffer here allocates and frees through it. */
    const kith_allocator_t *allocator;
    /** Selected subjects (the bounded view set). */
    kith_gateway_view_subject_t *subjects;
    /** Number of subjects in the subjects array. */
    size_t subject_count;
    /** Capacity of the subjects array. */
    size_t subject_cap;
    /** View metadata (counts, breakdowns). */
    kith_gateway_view_snapshot_t meta;
    /** Monotonic millisecond timestamp of the last view refresh. */
    uint64_t last_refresh_ms;
    /** True after the first successful refresh. */
    bool has_view;
    /** Actor ids from the prior view (non-self, non-crowd) for continuity. */
    uint64_t *prior_ids;
    /** Number of ids in the prior_ids array. */
    size_t prior_count;
    /** Capacity of the prior_ids array. */
    size_t prior_cap;
    /** Window mutation counter observed by the last full composition.
     *  Equality with the session's current counter means the window array
     *  is byte-identical to the one the baselines below were recorded
     *  against (window_remove swaps the last element into the vacated
     *  slot, so array order can change without the key set changing —
     *  every mutation bumps the counter, which is what keeps these
     *  position-indexed baselines valid). */
    uint64_t cached_window_seq;
    /** Bound actor id observed by the last full composition. A rebind
     *  changes both the self subject and the candidate exclusion filter,
     *  so a mismatch forces a full recomposition. */
    uint64_t cached_actor_id;
    /** Resolved view budget observed by the last full composition. The
     *  budget is fixed at gateway creation today; recording it keeps the
     *  skip correct if that ever stops holding. */
    size_t cached_budget;
    /** Per-window-position content sequences at the last full composition
     *  (0 = no node existed for that position). Aligned with the
     *  snapshotted window order; meaningful only while cached_window_seq
     *  matches the session's current counter. */
    uint64_t *cached_node_seqs;
    /** Capacity of the cached_node_seqs array. */
    size_t cached_node_seq_cap;
    /** Crowd-regime latch from the last full composition: true while the
     *  composed set carries a crowd aggregate. Holds the aggregate across
     *  recompositions until the candidate count falls below the entry
     *  threshold by the exit margin (hysteresis), so a count jittering at
     *  the threshold cannot flip membership on consecutive ticks. */
    bool crowd_latch;
    /** Membership delta of the LAST full recomposition, regenerated (not
     *  accumulated) on each rebuild: entries, crowd transitions, and
     *  departures with their window-observability classification. The
     *  bound strategy consumes it via gateway_view_take_membership; a
     *  rebuild overwrites unconsumed deltas, so storage stays bounded by
     *  one composition's membership churn. */
    struct gateway_view_event *delta_events;
    /** Number of events in @p delta_events. */
    size_t delta_count;
    /** Capacity of @p delta_events. */
    size_t delta_cap;
    /** True after the strategy consumed the current delta (a fresh
     *  rebuild resets it). */
    bool delta_consumed;
};

/*---------------------------------------------------------------------------
 * session
 *-------------------------------------------------------------------------*/

/** A session binds an authenticated identity to a connection and carries
 *  the per-subscriber view state. Sessions are heap-allocated (stable
 *  addresses) and tracked in the gateway's session table by connection
 *  pointer for dispatch lookup. */
struct kith_gateway_session
{
    /** Allocator resolved at create (the supplied instance, or the
     *  gateway's when none was supplied); carried because the session's
     *  last reference can drop on a worker thread after the gateway is
     *  gone, so the session, its window, and its view state release
     *  through this copy. */
    const kith_allocator_t *allocator;
    /** Owning gateway (nulled when the gateway is destroyed first). */
    kith_gateway_t *gateway;
    /** Bound connection (reference acquired on create, released on destroy). */
    kith_net_conn_t *conn;
    /** Gateway-local unique session identifier. */
    uint64_t session_id;
    /** Authenticated principal identifier (opaque key). */
    uint64_t principal_id;
    /** Bound subscriber actor identifier (0 until bind_actor). Atomic
     *  because worker-pool handlers rebind while the reactor-thread
     *  composer reads: plain lvalue accesses are seq_cst atomic loads
     *  on this field, and bind_actor pairs its release store with a
     *  window_seq bump so no skip baseline is recorded across a
     *  concurrent rebind. */
    _Atomic uint64_t actor_id;
    /** Session authentication type. */
    kith_gateway_session_type_t type;
    /** Atomic dispatch refcount. The owner reference (count 1) is held by
     *  the session's creator and dropped by kith_gateway_session_destroy;
     *  each reactor→worker dispatch handoff acquires a reference (the
     *  worker reads session fields after the reactor may have already
     *  closed the connection and dropped the owner reference) and the
     *  worker task releases it. The session's own resources are freed
     *  when the count reaches zero, so an in-flight dispatch keeps the
     *  session alive past the reactor's destroy. Mirrors the connection
     *  refcount (kith_net_conn_acquire/release); the acquire uses
     *  memory_order_relaxed and the release uses memory_order_acq_rel. */
    _Atomic int refcount;
    /** Per-subscriber view state. */
    struct gateway_view_state view;
    /** Per-subscriber subscription window: the cells this subscriber tracks.
     *  The relevance composer iterates only these cells (via the shared cache)
     *  when building the view set, so a cell no local subscriber tracks is
     *  invisible to that subscriber's view. Serialized by window_lock. */
    kith_fabric_cell_key_t *window;
    /** Number of cells in the window array. */
    size_t window_count;
    /** Capacity of the window array. */
    size_t window_cap;
    /** Mutation counter for the window array and the bound actor id,
     *  bumped under window_lock on every add/remove/clear that changes
     *  the array (a no-op add of an already-present cell or a remove of
     *  an absent cell does not) and on every rebind, where it invalidates
     *  skip baselines that would otherwise outlive the identity they were
     *  composed around. The composer snapshots it with the window copy to
     *  detect view-input changes between compositions. Array order is part
     *  of the composed output (first-occurrence subscriber locate,
     *  scan-order heap tie-breaks), so any reorder rides a remove+add pair
     *  and bumps twice — the counter must track mutations, not key-set
     *  differences. */
    uint64_t window_seq;
    /** Serializes window add/remove/clear against the composer's window read
     *  and against concurrent worker-thread mutations (Python handlers run
     *  on a worker pool under free-threaded Python). */
    pthread_mutex_t window_lock;
    /** Retained window adds: cells whose subscribe failed because the
     *  cell cache's stripe or the fabric interest set had no free slot.
     *  The tick pass retries them (bounded per pass, rotation head for
     *  fairness) until capacity frees; window_remove and window_clear
     *  cancel entries; the final release frees the array. A ring over
     *  pending_cap slots: logical entry i lives at (pending_head + i) mod
     *  pending_cap. Serialized by window_lock. */
    kith_fabric_cell_key_t *pending;
    /** Number of retained adds in the pending ring. */
    size_t pending_count;
    /** Capacity of the pending ring allocation. */
    size_t pending_cap;
    /** Ring head: the logical first entry's slot index. Advances by the
     *  pass's attempt count so a saturated stripe does not pin the
     *  queue's front. */
    size_t pending_head;
    /** Delivery strategy bound at creation (points into the gateway
     *  registry's stable entry storage). Never NULL: sessions always bind
     *  a strategy (the factory default when params select none). */
    const kith_gateway_delivery_vtable_t *delivery_strategy;
    /** Strategy-owned per-session encode state (the strategy's session_init
     *  output; NULL for stateless strategies). Created and destroyed
     *  through the bound vtable; the reactor thread drives deliver, and
     *  fini runs when the last session reference drops. */
    void *delivery_state;
    /** Per-session cumulative delivery totals, folded at the same site the
     *  gateway's delivery totals fold (both the inline tick path and the
     *  executor's worker threads), so each is a relaxed atomic add. The
     *  per-session sums partition the gateway totals — every frame counted
     *  on the gateway appears on exactly one session — and the accessor
     *  exposes them as the per-session delivery-health signal. Readable
     *  from any thread while the session reference lives. */
    _Atomic uint64_t delivery_enqueued_total;
    _Atomic uint64_t delivery_dropped_total;
    _Atomic uint64_t delivery_events_enqueued_total;
    _Atomic uint64_t delivery_suppressed_total;
    /** In-flight delivery serialization: true while an executor
     *  job is delivering this session's view. The reactor sets it (release)
     *  before submitting and the worker clears it (release) as the job's
     *  last write to the session, so an acquire load observing false pairs
     *  with every write the deliver made. Executor mode only: inline
     *  delivery never touches this flag. Own cache line: the reactor
     *  polls it in the compose-wait while the worker writes the view. */
    alignas(64) _Atomic bool delivery_in_flight;
    /** Embedded zero-allocation job record the reactor fills at submit and
     *  the executor thread reads on dequeue. Distinct cache line
     *  from the flag: the reactor writes it while a previous job's worker
     *  may still be draining. */
    struct gateway_delivery_job delivery_job;
};

/** Open-addressed hash of session pointers keyed by connection pointer. */
struct gateway_session_table
{
    /** Allocator resolved at gateway create; the slot array allocates and
     *  frees through it. */
    const kith_allocator_t *allocator;
    struct kith_gateway_session **slots;
    size_t buckets;
    size_t count;
    size_t max_sessions;
    /** Monotonic session-id counter. */
    uint64_t next_session_id;
};

/** Fixed depth of the cell-scoped broadcast request queue. A submit past
 *  the depth is refused with -KITH_EAGAIN and counted; the depth is
 *  bounded work per drain, and the refusal count is the evidence path for
 *  any future retuning. Must not exceed 64: a drain matches the batch
 *  against each session's window in one pass over a 64-bit mask. */
#define GATEWAY_BROADCAST_QUEUE_CAP 64u

/** One queued cell-scoped broadcast request. */
struct gateway_broadcast_request
{
    /** Cell the broadcast is scoped to; copied at submit. */
    kith_fabric_cell_key_t cell;
    /** Message type id for the delivered frame. */
    uint16_t type_id;
    /** Caller payload length in bytes. */
    uint32_t payload_len;
    /** Caller bytes copied at submit through the gateway allocator;
     *  released at drain or teardown. NULL when payload_len is 0. */
    void *payload;
};

/** FIFO of pending cell-scoped broadcast requests. Submits run on any
 *  thread; the drain runs on the reactor thread inside the tick, ahead of
 *  the session pass. The fixed-depth array is embedded (no allocation);
 *  payload copies ride the gateway's allocator. Serialized by lock. */
struct gateway_broadcast_queue
{
    pthread_mutex_t lock;
    /** Ring storage: logical entry i lives at (head + i) mod
     *  GATEWAY_BROADCAST_QUEUE_CAP. */
    struct gateway_broadcast_request slots[GATEWAY_BROADCAST_QUEUE_CAP];
    size_t head;
    size_t count;
    /** Set under lock at teardown before any pending request is released;
     *  a submit that observes it refuses -KITH_ESTATE instead of queueing
     *  a request the gateway will never drain. */
    bool frozen;
};

/*---------------------------------------------------------------------------
 * gateway handle
 *-------------------------------------------------------------------------*/

struct kith_gateway
{
    /** Allocator resolved at create; the handle, its tables, scratches,
     *  registry, executor, and every session and strategy buffer grown
     *  through it allocate and free through this instance. */
    const kith_allocator_t *allocator;
    /** Borrowed transport handle. */
    kith_net_t *net;
    /** Borrowed fabric handle. */
    kith_fabric_t *fabric;
    /** Borrowed proto handle. */
    kith_proto_t *proto;
    /** Borrowed worker pool for Python-bound handler dispatch.
     *  NULL when no pool is attached (C handlers run inline on the reactor
     *  thread, which is always correct for plain C function pointers). */
    kith_worker_t *workers;
    /** Monotonic count of Python-bound handler dispatches dropped because the
     *  attached worker pool was exhausted (kith_worker_submit returned
     *  EBUSY). Incremented on the reactor thread at drop time as a single
     *  atomic increment — the drop hot path performs no metrics-library
     *  work; the composition root reads the delta and records it as
     *  kith_gateway_dispatch_dropped_total. Dropping is honest backpressure:
     *  the reactor never falls back to running a Python-bound handler inline.
     *  C handlers
     *  (no PYTHON flag) and Python handlers with no pool attached keep the
     *  inline dispatch path and do not increment this counter. */
    _Atomic uint64_t dispatch_dropped;
    /** Monotonic count of destroyed-session notifications dropped because
     *  no worker pool was attached or the attached pool refused the
     *  submit. Kept separate from dispatch_dropped: a dropped dispatch is
     *  backpressure, a dropped lifecycle notification is a lost disconnect
     *  notice — the game's disconnect bookkeeping silently did not run and
     *  per-session state leaks. Incremented on the reactor thread at drop
     *  time as a single atomic increment; the composition root reads the
     *  delta and records it as kith_gateway_lifecycle_dropped_total. A
     *  Python-bound callback refused at the worker because the interpreter
     *  is finalizing is not counted (the process is exiting). */
    _Atomic uint64_t lifecycle_dropped;
    /** Monotonic count of session window adds that failed against
     *  transient capacity: the cell cache's stripe or the fabric interest
     *  set had no free slot (-KITH_ENOMEM). Incremented at the failing add
     *  on the calling thread (window adds are safe from any thread); other
     *  failure codes pass uncounted (-KITH_EINVAL is a caller error,
     *  -KITH_ESTATE names a stripe mutex that failed at gateway create).
     *  The failed add is retained for the tick pass's retry, so this is
     *  the churn-coupled flood signal the gauges are read against, not a
     *  terminal count. Recorded as
     *  kith_gateway_window_add_failures_total. */
    _Atomic uint64_t window_add_failures;
    /** Monotonic count of retained window adds landed by the tick pass
     *  after capacity freed. Incremented on the reactor thread at landing;
     *  recorded as kith_gateway_window_retry_adds_total. Read against
     *  window_add_failures and the pending gauge it separates a flood that
     *  is healing (failures climbing, retries landing, pending draining)
     *  from one that is stuck (failures climbing, no landings). */
    _Atomic uint64_t window_retry_adds;
    /** Mirror gauge of retained (retryable) window adds across all
     *  sessions, updated at the same window_lock-held sites that mutate
     *  the pending rings: a retention adds one, a landing, a cancel, or a
     *  teardown drain removes one. Recorded as
     *  kith_gateway_window_retries_pending. Pegged at its ceiling with no
     *  landings is the permanent-geometry signature; rising and draining
     *  is the transient the retry heals. */
    _Atomic uint64_t window_retries_pending;
    /** Mirror gauge of sessions bound to an actor while tracking no
     *  subscription window — the seed-failure signature ("subscribed to
     *  nothing"). Computed on the reactor thread during the tick's session
     *  pass (each visit reads its session's window under window_lock) and
     *  recorded as kith_gateway_sessions_without_cells. A zero-delivered
     *  predicate would also count sessions whose window is healthy and
     *  whose view is legitimately empty (the composer excludes the
     *  subscriber's own actor), so the census reads the window itself. */
    _Atomic uint64_t sessions_without_cells;
    /** Mirror gauge of the session table's count, updated at the same
     *  reactor-thread create and destroy sites that mutate the table. The
     *  table itself is reactor-thread-only (no lock), so off-reactor
     *  readers take the gauge instead; the acquire/release pair keeps the
     *  mirror exact at every table mutation. */
    _Atomic uint64_t session_count;
    /** Monotonic cumulative nanoseconds spent in the cache-refresh phase of
     *  @c kith_gateway_tick, accumulated on the reactor thread. The
     *  composition root reads the per-tick delta and records it as
     *  @c kith_gateway_refresh_ns_total (ms = ns / 1e6). The first
     *  plane-level timing counter; mirrors the @c dispatch_dropped shape
     *  (reactor writes, composition root records the delta off the hot
     *  path). Nanosecond accumulation avoids per-session rounding loss. */
    _Atomic uint64_t refresh_ns_total;
    /** Monotonic cumulative nanoseconds spent in the view-compose phase
     *  (the per-session @c kith_gateway_view_refresh loop) of
     *  @c kith_gateway_tick. Recorded as @c kith_gateway_compose_ns_total. */
    _Atomic uint64_t compose_ns_total;
    /** Monotonic cumulative nanoseconds spent in the delivery phase (the
     *  per-session @c kith_gateway_deliver loop) of @c kith_gateway_tick.
     *  Recorded as @c kith_gateway_deliver_ns_total. */
    _Atomic uint64_t deliver_ns_total;
    /** Monotonic cumulative nanoseconds the compose path spent copying the
     *  composing session's subscription window under the window lock.
     *  Reactor thread only; recorded as
     *  @c kith_gateway_compose_window_ns_total. */
    _Atomic uint64_t compose_window_ns_total;
    /** Monotonic cumulative nanoseconds the compose path spent rebuilding
     *  the prior-view id set. Reactor thread only; recorded as
     *  @c kith_gateway_compose_prior_ns_total. */
    _Atomic uint64_t compose_prior_ns_total;
    /** Monotonic cumulative nanoseconds the compose path spent acquiring
     *  the window's cache stripes (mutex acquisition including contention
     *  against worker-held stripes, plus the ascending-order dedup walk).
     *  Reactor-side acquisition only: a worker waiting on a stripe the
     *  reactor holds is not visible to this counter. Recorded as
     *  @c kith_gateway_compose_lock_wait_ns_total. */
    _Atomic uint64_t compose_lock_wait_ns_total;
    /** Monotonic cumulative nanoseconds the compose path spent locating the
     *  subscriber in the window and streaming candidate artifacts into the
     *  bounded heap. Reactor thread only; recorded as
     *  @c kith_gateway_compose_scan_ns_total. */
    _Atomic uint64_t compose_scan_ns_total;
    /** Monotonic cumulative nanoseconds the compose path spent heapsorting
     *  the kept candidates. Reactor thread only; recorded as
     *  @c kith_gateway_compose_sort_ns_total. */
    _Atomic uint64_t compose_sort_ns_total;
    /** Monotonic cumulative nanoseconds the compose path spent building the
     *  view set (subjects, tiers, crowd aggregate, metadata). Reactor thread
     *  only; recorded as @c kith_gateway_compose_select_ns_total
     * . */
    _Atomic uint64_t compose_select_ns_total;
    /** Monotonic count of compositions skipped because neither the
     *  session's window, its bound actor, the view budget, nor any
     *  windowed cell's cached content changed since the session's last
     *  full composition: the retained view set is already exactly what a
     *  recomposition would produce. Incremented on the reactor thread at
     *  skip time; recorded as @c kith_gateway_compose_skips_total
     * . */
    _Atomic uint64_t compose_skips_total;
    /** Monotonic count of compositions deferred by the per-tick compose
     *  budget: @c kith_gateway_tick consumed compose_budget_us within the
     *  tick's session pass and a session that already holds a retained
     *  view set would otherwise have recomposed. The session delivers its
     *  retained set that tick (cadence unchanged) and is served first on a
     *  subsequent tick by the rotating start index; sessions without a retained
     *  view set are never deferred (they must compose so their delivery
     *  cadence starts). Incremented on the reactor thread at deferral
     *  time; recorded as @c kith_gateway_compose_deferrals_total
     * . Sibling to @c compose_skips_total: skips mean "nothing
     *  changed", deferrals mean "the tick ran out of budget". */
    _Atomic uint64_t compose_deferrals_total;
    /** Monotonic count of view compositions that failed to locate the
     *  subscriber inside its own subscription window: the scan phase found
     *  no cached window cell holding the session's bound actor id, so the
     *  composer returned -KITH_ESTATE and that session delivered nothing
     *  this tick. A healthy system never fails to locate (the bound
     *  actor's crossings keep its cells subscribed), so any nonzero
     *  reading marks a subscription-window ownership defect rather than
     *  load. Incremented on the reactor thread at failure time; recorded
     *  as @c kith_gateway_view_locate_failures_total. */
    _Atomic uint64_t view_locate_failures_total;
    /** Monotonic count of @c kith_gateway_dispatch calls. Incremented on the
     *  reactor thread as a relaxed atomic for every dispatched frame; recorded
     *  as @c kith_gateway_dispatches_total. Sibling to @c dispatch_dropped:
     *  drops are a subset of this total. */
    _Atomic uint64_t dispatch_total;
    /** Monotonic count of self rebuilds whose subject carried the actor's
     *  publisher-minted update_seq while stamping is enabled.
     *  Reactor thread only; recorded as @c kith_gateway_self_echo_stamps_total. */
    _Atomic uint64_t self_echo_stamps_total;
    /** Monotonic count of self rebuilds whose source artifact carried no
     *  minted update_seq while stamping is enabled — a publisher that never
     *  mints, or an actor that has never applied movement.
     *  Reactor thread only; recorded as
     *  @c kith_gateway_self_echo_fallbacks_total. */
    _Atomic uint64_t self_echo_fallbacks_total;
    /** Monotonic cumulative frames successfully enqueued on session
     *  connections, folded inside @c kith_gateway_deliver on whichever
     *  thread executed the call (reactor thread inline; executor worker
     *  threads under the executor — one-tick skew, same discipline as
     *  @c deliver_ns_pending). Recorded as
     *  @c kith_gateway_delivery_frames_enqueued_total. Sibling
     *  to @c delivery_dropped_total: drops are the enqueue-side loss the
     *  selection metrics are read against. */
    _Atomic uint64_t delivery_enqueued_total;
    /** Monotonic cumulative frames dropped due to output backpressure,
     *  folded at the same site as @c delivery_enqueued_total. Recorded as
     *  @c kith_gateway_delivery_dropped_total. A nonzero
     *  reading means frames never left for the client — the backpressure
     *  half of the selected-clients diagnosis. */
    _Atomic uint64_t delivery_dropped_total;
    /** Monotonic cumulative membership-event frames successfully
     *  enqueued, folded at the same site. Recorded as
     *  @c kith_gateway_delivery_event_frames_enqueued_total. */
    _Atomic uint64_t delivery_events_enqueued_total;
    /** Monotonic cumulative subjects skipped by change suppression,
     *  folded at the same site. Recorded as
     *  @c kith_gateway_delivery_suppressed_total. */
    _Atomic uint64_t delivery_suppressed_total;
    /** Monotonic count of delivery-pass visits of sessions holding a
     *  view. Incremented on the reactor thread at the delivery section of
     *  @c gateway_tick_visit_session — including sessions whose executor
     *  submission is subsequently skipped (in-flight or EBUSY; those
     *  skips are separately counted by the executor counters), so
     *  visits − skips approximates actually-delivered sessions. Recorded
     *  as @c kith_gateway_view_visits_total. */
    _Atomic uint64_t view_visits_total;
    /** Monotonic sum of visited sessions' candidate counts (the scan
     *  yield). Accumulated on the reactor thread at the same visit site;
     *  recorded as @c kith_gateway_view_candidate_total.
     *  Paired with @c view_selected_total: the ratio is the live
     *  selection density. */
    _Atomic uint64_t view_candidate_total;
    /** Monotonic sum of visited sessions' selected counts (the post-budget
     *  yield). Accumulated at the same site; recorded as
     *  @c kith_gateway_view_selected_total. */
    _Atomic uint64_t view_selected_total;
    /** Peak candidate count observed at any visit (gauge, CAS-max,
     *  relaxed). Recorded as @c kith_gateway_view_candidate_high_watermark
     * ; approaching the view budget is the cap-binding
     *  evidence. */
    _Atomic uint64_t view_candidate_high_watermark;
    /** Peak selected count observed at any visit (gauge, CAS-max,
     *  relaxed). Recorded as
     *  @c kith_gateway_view_selected_high_watermark. */
    _Atomic uint64_t view_selected_high_watermark;
    /** Session table (conn → session map). */
    struct gateway_session_table sessions;
    /** Cell-scoped broadcast request queue (see
     *  kith_gateway_broadcast_cell). */
    struct gateway_broadcast_queue broadcasts;
    /** Monotonic count of broadcast submits refused because the queue was
     *  full. Recorded as kith_gateway_broadcast_refused_total. */
    _Atomic uint64_t broadcast_refused_total;
    /** Monotonic count of broadcast deliveries not completed: a recipient
     *  whose connection queue was full or had closed, plus a queued
     *  request whose frame could not encode (counted once) and requests
     *  pending at teardown (one each). Recorded as
     *  kith_gateway_broadcast_dropped_total. */
    _Atomic uint64_t broadcast_dropped_total;
    /** Shared cell cache + fabric subscription. */
    struct gateway_cache cache;
    /** Handler registration table. */
    struct gateway_handler_table handlers;
    /** Destroyed-session notification slot (one registration per
     *  gateway). Its mutex initializes in gateway_core_init; teardown
     *  unregisters before any other fini so a session destroyed during
     *  teardown delivers nothing. */
    struct gateway_destroyed_slot destroyed;
    /** View-composition scratch, reused across every session's refresh. */
    struct gateway_compose_scratch compose;
    /** Batch-delivery scratch, reused across every session's delivery. */
    struct gateway_delivery_scratch delivery;
    /** Delivery executor; NULL selects inline delivery and
     *  keeps every executor code path off the tick. */
    struct gateway_delivery_executor *delivery_executor;
    /** Resolved max simultaneous sessions. */
    uint32_t max_sessions;
    /** Resolved max subjects per subscriber view set. */
    uint32_t view_max_subjects;
    /** Resolved view refresh interval in milliseconds. */
    uint32_t view_refresh_interval_ms;
    /** Resolved per-tick composition-pass budget in microseconds
     *  (compose_budget_us; UINT32_MAX disables bounding). Consulted by
     *  @c kith_gateway_tick's session pass. */
    uint32_t compose_budget_us;
    /** Resolved per-pass compose-wait budget in microseconds
     *  (delivery_wait_budget_us). Consulted by the tick's session pass in
     *  executor mode only. */
    uint32_t delivery_wait_budget_us;
    /** Resolved cache refresh interval in milliseconds. */
    uint32_t cache_refresh_interval_ms;
    /** Crowd-regime exit margin in candidates (0 selects the composer
     *  default: one eighth of the budget, floor 2). Consulted by the
     *  composer's crowd latch. */
    uint32_t crowd_exit_margin;
    /** Self-echo coverage stamping. When true, every rebuilt
     *  self subject carries its actor's publisher-minted update_seq and
     *  advances the stamp or fallback counter. */
    bool self_echo_enabled;
    /** Rotating start index for the tick's session pass (reactor thread
     *  only). Each pass begins where the previous one deferred its first
     *  session, so deferred work leads the next sweep and no session
     *  starves behind an always-hot prefix; with nothing deferred it
     *  advances one slot for plain round-robin. */
    size_t sched_cursor;
    /** Message type id for per-subject replication frames (0 disables). */
    uint16_t replication_type_id;
    /** Message type id for multi-subject batch replication frames (0
     *  disables batch delivery; when non-zero, replication_type_id is
     *  ignored and delivery packs the full view set into one frame). */
    uint16_t replication_batch_type_id;
    /** Delivery strategy registry (built-ins registered at creation). */
    struct gateway_delivery_registry deliveries;
    /** Resolved default strategy name sessions bind at creation (owned
     *  copy of params.delivery_strategy, "full" when unset). */
    char *delivery_strategy_name;
    /** Owned copy of params.delivery_config, passed to each session's
     *  strategy session_init. NULL when no configuration was supplied. */
    void *delivery_config;
    /** Byte length of @p delivery_config (0 when none). */
    size_t delivery_config_size;
};
