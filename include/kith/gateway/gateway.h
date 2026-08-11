#ifndef KITH_GATEWAY_GATEWAY_H
#define KITH_GATEWAY_GATEWAY_H

#include <stddef.h>
#include <stdint.h>

#include "kith/api.h"
#include "kith/fabric/fabric.h"
#include "kith/net/net.h"
#include "kith/proto/proto.h"
#include "kith/types.h"
#include "kith/version.h"

/**
 * Opaque worker pool handle (defined in kith/worker/worker.h). Forward-
 * declared here so @c kith_gateway_attach_worker_pool and
 * @c kith_gateway_register_handler_flags can take a pool pointer without
 * forcing every consumer of the gateway header to also include the worker
 * header. The pool bridges Python-bound handlers off the reactor thread.
 */
typedef struct kith_worker kith_worker_t;

/**
 * @defgroup kith_gateway Gateway
 * @{
 */

/**
 * Client sockets, session lifecycle, shared cell cache, relevance
 * composer, and bounded delivery.
 *
 * A gateway handle owns the session table (connection to session map),
 * the shared cell cache (refcounted per-cell snapshots sourced from the
 * fabric), the per-subscriber relevance composer (bounded scored view sets),
 * the handler registration table (message type to callback map), and the
 * delivery path (encodes and enqueues the already-bounded send set on
 * each session's connection).
 *
 * A session is the gateway's stateful record for one connected peer —
 * identity, subscriptions, composed view set, delivery state — and rides
 * exactly one connection: the transport-level socket state the net module
 * owns (its C-identifier shorthand is @c conn).
 *
 * The composition root creates the handle with borrowed net, fabric, and
 * proto handles. The gateway subscribes to cells on the fabric (once per
 * cell for all local subscribers, refcounted), drains pending cell products
 * each tick into the shared cache, composes a per-subscriber scored view set
 * from the cache, and delivers the bounded set to each session's socket.
 *
 * @section gateway_plane Plane contract
 *
 * The gateway owns the Gateway plane (see @c docs/architecture/planes.md).
 * It holds no authoritative actor state (the sim owns that); it does not
 * rescan raw zone state each tick (it drains the fabric's pending
 * products); it does not recompute AOI from scratch per connection (it
 * composes from the shared cache). A subscriber's send set is always
 * budgeted, even under no load. Full-fidelity replication is a privilege
 * of relevance, not the default right of every visible actor.
 *
 * @section gateway_cache Shared cell cache
 *
 * The cache holds one refcounted snapshot per subscribed cell. When a
 * subscriber's subscription window expands to include a cell, the gateway
 * subscribes that cell on the fabric (or increments its refcount if
 * already subscribed by another local subscriber). When the last subscriber
 * unsubscribes a cell, the gateway unsubscribes it on the fabric and
 * evicts the cache entry. The cache is refreshed each tick by draining
 * the fabric's pending product headers and re-snapshotting changed cells.
 *
 * @section gateway_view Relevance composer
 *
 * The relevance composer builds a per-subscriber scored view set from the
 * shared cache. Each tick (or at a configured refresh interval), it
 * collects candidate actors from the cells near the subscriber, scores them
 * by squared distance to the subscriber, classifies them by subject class
 * (self, actor, crowd), selects a representation tier (full, reduced,
 * crowd) per subject, and bounds the total to a configured maximum.
 * Sticky continuity preserves prior-view members that still qualify,
 * preventing membership flicker across ticks.
 *
 * @section gateway_delivery Delivery
 *
 * Delivery encodes the bounded view set as actor state frames and
 * enqueues them on the session's connection. When
 * @c kith_gateway_params_t.replication_batch_type_id is non-zero, the full
 * view set is packed into one multi-subject frame per refresh (a 4-byte
 * count header followed by N serialized subject records); otherwise each
 * subject is encoded as a separate frame using the configured
 * @c replication_type_id. The reactor flushes the connection's output queue
 * separately via @c kith_net_conn_write; the gateway only composes and
 * enqueues.
 *
 * The encoding and scheduling policy behind that enqueue is a delivery
 * strategy: a size-versioned vtable registered by name and
 * selected through @c kith_gateway_params_t.delivery_strategy. The factory
 * default @c full resends every selected subject every tick; further
 * presets ship alongside it, and games register their own strategies
 * within the plane invariants and the delivery-strategy correctness
 * rules.
 *
 * @section gateway_handlers Handler registration table
 *
 * The handler registration table replaces a hardcoded dispatch switch.
 * The composition root or game code registers a callback for each
 * message type id. When a decoded frame arrives on a connection, the
 * gateway dispatches it: it looks up the session bound to the connection,
 * looks up the handler registered for the frame's type id, and invokes
 * the handler with the payload and session. Python handlers register
 * via a ctypes callback wrapper; the reactor posts Python-bound handlers
 * to a worker pool (size 1 under the GIL, size N under free-threaded
 * Python).
 *
 * The handle is owned by the composition root and passed by pointer;
 * there is no global accessor. All state lives on the handle. The
 * gateway is driven from one thread (the reactor thread); the session
 * table and view composer are reactor-thread-only state, while the
 * cache serializes its cross-thread entry points under per-cell stripe
 * locks and each session's subscription window under a per-session
 * mutex. The handler table is serialized by a per-handle mutex so
 * concurrent register and dispatch calls do not interleave. Delivery
 * runs on executor threads
 * only when the delivery executor is configured; the session
 * table and composer stay reactor-thread-only in every mode.
 */

/**
 * Format invariants. The underlying type is fixed so a constant stored in
 * an ABI surface stays a fixed width.
 */
enum kith_gateway_format : unsigned int
{
    /** Number of representation tiers (full, reduced, crowd). */
    KITH_GATEWAY_VIEW_TIER_COUNT = 3u,
    /** Number of subject classes (self, actor, crowd). */
    KITH_GATEWAY_VIEW_CLASS_COUNT = 3u,
};

/**
 * Default configuration values. A @c kith_gateway_params_t field set to 0
 * selects the corresponding default at create time. The underlying type
 * is fixed.
 */
enum kith_gateway_default : unsigned int
{
    /** Default max simultaneous sessions (params.max_sessions = 0 → this). */
    KITH_GATEWAY_DEFAULT_MAX_SESSIONS = 4096u,
    /** Default shared cell-cache hash bucket count. */
    KITH_GATEWAY_DEFAULT_CACHE_BUCKETS = 4096u,
    /** Default per-subscriber view-store hash bucket count. */
    KITH_GATEWAY_DEFAULT_VIEW_BUCKETS = 4096u,
    /** Default max subjects per subscriber's view set. */
    KITH_GATEWAY_DEFAULT_VIEW_MAX_SUBJECTS = 512u,
    /** Default view refresh interval in milliseconds. */
    KITH_GATEWAY_DEFAULT_VIEW_REFRESH_MS = 100u,
    /** Default cache refresh interval in milliseconds. */
    KITH_GATEWAY_DEFAULT_CACHE_REFRESH_MS = 100u,
    /** Default handler registration table capacity (message type slots). */
    KITH_GATEWAY_DEFAULT_HANDLER_TABLE_SIZE = 256u,
    /**
     * Default per-tick composition-pass budget in microseconds
     * (params.compose_budget_us = 0 → this). Half the 20 Hz gate tick.
     */
    KITH_GATEWAY_DEFAULT_COMPOSE_BUDGET_US = 25000u,
    /**
     * Default per-pass compose-wait budget in microseconds
     * (params.delivery_wait_budget_us = 0 → this). Below the 20 Hz tick
     * interval by a wide margin: the wait is the one reactor-block point
     * the delivery executor introduces, and the default bounds
     * a fully in-flight pass's stall to a sixth of one tick.
     */
    KITH_GATEWAY_DEFAULT_DELIVERY_WAIT_BUDGET_US = 8000u,
};

/**
 * Session authentication type. Defines the authenticated identity class,
 * not game behavior. A subscriber session is created after credential
 * verification; an app or service session is created after OAuth or
 * internal token exchange. The framework carries no permission model;
 * authorization is the caller's concern.
 */
enum kith_gateway_session_type : unsigned int
{
    /** Authenticated subscriber session, created after credential verification. */
    KITH_GATEWAY_SESSION_SUBSCRIBER = 0u,
    /** Authenticated application session, created after an OAuth exchange. */
    KITH_GATEWAY_SESSION_APP = 1u,
    /** Authenticated internal service session, created after an internal token exchange. */
    KITH_GATEWAY_SESSION_SERVICE = 2u,
};

/** Alias of enum kith_gateway_session_type. */
typedef enum kith_gateway_session_type kith_gateway_session_type_t;

/**
 * View subject classification. The relevance composer assigns each
 * candidate actor to one class before scoring and budgeting. The self
 * subject is the subscriber's own actor (always selected at full fidelity).
 * Actor subjects are individual nearby actors. Crowd subjects are
 * aggregate representations of dense cells.
 */
enum kith_gateway_view_class : unsigned int
{
    /** The subscriber's own actor (always selected at full fidelity). */
    KITH_GATEWAY_VIEW_CLASS_SELF = 0u,
    /** An individual nearby actor. */
    KITH_GATEWAY_VIEW_CLASS_ACTOR = 1u,
    /** An aggregate representation of a dense cell. */
    KITH_GATEWAY_VIEW_CLASS_CROWD = 2u,
};

/** Alias of enum kith_gateway_view_class. */
typedef enum kith_gateway_view_class kith_gateway_view_class_t;

/**
 * Opaque gateway handle.
 *
 * @ownership callee — created by kith_gateway_create, destroyed by
 *           kith_gateway_destroy. Owns the session table, shared cell
 *           cache, per-subscriber view store, handler registration table,
 *           and an internal aoi spatial index. Borrows the net, fabric,
 *           and proto handles (not owned; the caller destroys them
 *           separately).
 */
typedef struct kith_gateway kith_gateway_t;

/**
 * Opaque session handle. A session binds an authenticated identity to a
 * network connection and carries the per-subscriber view state used by the
 * relevance composer and delivery path.
 *
 * @ownership callee — created by kith_gateway_session_create, destroyed
 *           by kith_gateway_session_destroy. Acquires a reference to the
 *           bound connection on creation and releases it on destroy.
 */
typedef struct kith_gateway_session kith_gateway_session_t;

/**
 * Gateway handle creation parameters. Size-versioned: callers set @p size
 * to sizeof(kith_gateway_params_t) and @p abi_version to KITH_ABI_VERSION
 * at their compile time; the runtime rejects structs from an incompatible
 * generation or an undersized size. A field set to 0 selects the
 * corresponding @c kith_gateway_default value. Additive fields are appended
 * ahead of the reserved array within a generation; the reserved slots stay
 * available for pointer-valued extensions.
 */
struct kith_gateway_params
{
    /** Must be sizeof(kith_gateway_params_t). */
    uint32_t size;
    /** Must be KITH_ABI_VERSION (from kith/version.h). */
    uint32_t abi_version;

    /** Max simultaneous sessions; 0 selects KITH_GATEWAY_DEFAULT_MAX_SESSIONS. */
    uint32_t max_sessions;
    /** Shared cell-cache hash bucket count; 0 selects the default. */
    uint32_t cache_bucket_count;
    /** Per-subscriber view-store hash bucket count; 0 selects the default. */
    uint32_t view_bucket_count;
    /** Max subjects per subscriber's view set; 0 → default. */
    uint32_t view_max_subjects;
    /** View refresh interval in ms; 0 selects KITH_GATEWAY_DEFAULT_VIEW_REFRESH_MS. */
    uint32_t view_refresh_interval_ms;
    /** Cache refresh interval in ms; 0 selects KITH_GATEWAY_DEFAULT_CACHE_REFRESH_MS. */
    uint32_t cache_refresh_interval_ms;
    /** Handler table capacity (message type slots); 0 → default. */
    uint32_t handler_table_size;
    /**
     * Message type id used for actor state replication frames produced by
     * @c kith_gateway_deliver when @c replication_batch_type_id is 0. The
     * caller registers this type via @c kith_proto_register_type_id before
     * creating the gateway. 0 disables per-subject delivery encoding. When
     * both this and @c replication_batch_type_id are 0, @c kith_gateway_deliver
     * returns -KITH_ESTATE; the caller composes and enqueues frames itself.
     */
    uint16_t replication_type_id;
    /**
     * Message type id used for multi-subject batch replication frames
     * produced by @c kith_gateway_deliver. When non-zero, the gateway packs
     * the session's full composed view-subject set into one frame per
     * refresh (a 4-byte header + N serialized subject records) instead of
     * one frame per subject, reducing the reactor's per-refresh frame
     * enqueue volume from O(N x K) to O(N). The caller registers this type
     * via @c kith_proto_register_type_id before creating the gateway. 0
     * selects per-subject delivery via @c replication_type_id. When
     * non-zero, @c replication_type_id is ignored.
     */
    uint16_t replication_batch_type_id;
    /**
     * Soft per-tick budget in microseconds for the composition pass inside
     * @c kith_gateway_tick. Once the pass has consumed this much wall time,
     * sessions that already hold a composed view set skip recomposition for
     * that tick (the retained subject set is delivered as usual, so every
     * session's per-tick frame cadence is unchanged); a rotating start
     * index serves deferred sessions first on the next tick, so deferral
     * costs bounded staleness rather than skipped frames. The pass's first
     * eligible composition always runs regardless of the clock, which
     * bounds a deferred view's staleness to one pass per remaining session;
     * sessions without a retained view set compose unconditionally
     * (deferring them would leave nothing to deliver). The bound converts
     * a dirty-tick composition spike into spread-out work: the worst tick's
     * gateway cost becomes the budget plus the unconditional deliver pass
     * instead of an unbounded multi-hundred-millisecond stall.
     * 0 selects KITH_GATEWAY_DEFAULT_COMPOSE_BUDGET_US; UINT32_MAX disables
     * bounding.
     */
    uint32_t compose_budget_us;
    /**
     * Delivery strategy name. Resolved against the gateway's
     * strategy registry when each session is created; unknown names fail
     * @c kith_gateway_session_create with -KITH_ENOENT. NULL selects
     * @c "full", the factory default that resends every selected subject
     * every tick. The string is copied at gateway creation; the caller may
     * release it after the call. Strategies registered after gateway
     * creation (via @c kith_gateway_register_delivery) are selectable by
     * sessions created afterwards.
     */
    const char *delivery_strategy;
    /**
     * Strategy configuration passed verbatim to the selected strategy's
     * @c session_init callback. Must point to a size-versioned struct whose
     * leading @c size field the strategy interprets; NULL passes no
     * configuration (strategies apply their documented defaults). Borrowed
     * at gateway creation: the gateway copies @c size bytes and owns the
     * copy, so the caller may release the original after the call.
     */
    const void *delivery_config;
    /**
     * Crowd-regime exit margin in candidates. The composer's crowd
     * aggregate engages when candidates exceed the budget by 4; once
     * engaged it persists until candidates fall below that entry point by
     * this margin (hysteresis), so a count hovering at the threshold does
     * not flip the aggregate — and every actor's membership between
     * individual record and aggregate — on consecutive ticks. 0 selects
     * the composer default of one eighth of the view budget (floor 2).
     */
    uint32_t crowd_exit_margin;
    /**
     * Delivery executor thread count. 0 (the default) runs
     * delivery inline on the reactor thread; every executor code path is
     * bypassed. A non-zero
     * value moves the tick's per-session deliver pass onto a gateway-owned
     * executor with this many threads: the tick thread submits one job per
     * session instead of running the delivery strategy inline. Jobs never
     * touch Python, the fabric, the sim plane, or the reactor's ring.
     * Values above 64 are clamped to 64 by the underlying executor
     * primitive.
     */
    uint32_t delivery_worker_count;
    /**
     * Per-pass compose-wait budget in microseconds, consumed only when a
     * delivery executor is configured. Before composing a
     * session whose previous deliver is still in flight, the tick waits
     * for the in-flight job to finish; each such wait draws from one
     * budget shared by the whole pass (not one per wait), so a saturated
     * pass stalls the reactor by at most this much regardless of how many
     * sessions are in flight. Once the budget is exhausted, subsequent
     * in-flight sessions skip the wait immediately (counted separately).
     * On expiry the composition is skipped for the tick — the session
     * delivers its retained view, and staleness is bounded by the 1 s
     * max-gap backstop. Keep the value below one tick interval; 0 selects
     * KITH_GATEWAY_DEFAULT_DELIVERY_WAIT_BUDGET_US.
     */
    uint32_t delivery_wait_budget_us;
    /**
     * Disable self-echo coverage stamping. When false (the zero-init
     * default), every composed self subject carries its actor's
     * publisher-minted update_seq, and each stamped (or unminted, counted
     * as a fallback) self composition advances the corresponding
     * phase-stats counters. Setting the flag leaves the subject's
     * update_seq at 0 and advances nothing — the pre-stamping wire shape,
     * for comparability runs.
     */
    bool self_echo_disabled;

    /** Reserved for future additive fields. Must be zero-filled. */
    void *reserved[8];
};

/** Alias of struct kith_gateway_params. */
typedef struct kith_gateway_params kith_gateway_params_t;

/**
 * Session metadata. Exposed-layout value type. Returned by
 * @c kith_gateway_session_info.
 */
struct kith_gateway_session_info
{
    /** Gateway-local unique session identifier. */
    uint64_t session_id;
    /** Authenticated principal identifier (caller-supplied, opaque key). */
    uint64_t principal_id;
    /** Bound subscriber actor identifier (0 until @c kith_gateway_session_bind_actor). */
    uint64_t actor_id;
    /** Session authentication type. */
    kith_gateway_session_type_t type;
    /** Padding for alignment. */
    uint32_t pad;
};

/** Alias of struct kith_gateway_session_info. */
typedef struct kith_gateway_session_info kith_gateway_session_info_t;

/**
 * One cached cell's snapshot. Exposed-layout value type. Returned by
 * @c kith_gateway_cache_snapshot_cell. The snapshot reflects the cell's
 * state as of the last cache refresh; the actor_count and publish_seq
 * are sourced from the fabric's cell product header.
 */
struct kith_gateway_cell_snapshot
{
    /** Cell locator. */
    kith_fabric_cell_key_t key;
    /** Authority epoch (stale-product guard). */
    uint32_t authority_epoch;
    /** Latest publish sequence observed in the cache for this cell. */
    uint64_t latest_publish_seq;
    /** Monotonic millisecond timestamp of the last cache refresh for this cell. */
    uint64_t refreshed_at_ms;
    /** Shared subscription refcount (number of local subscribers interested). */
    uint32_t refcount;
    /** Number of actors in this cell (sourced from the fabric). */
    uint32_t actor_count;
    /** Source product level (full-fidelity from the fabric). */
    kith_fabric_product_level_t product_level;
    /** Padding for alignment. */
    uint32_t pad;
};

/** Alias of struct kith_gateway_cell_snapshot. */
typedef struct kith_gateway_cell_snapshot kith_gateway_cell_snapshot_t;

/**
 * Shared cell-cache statistics. Exposed-layout value type. Returned by
 * @c kith_gateway_cache_stats.
 */
struct kith_gateway_cache_stats
{
    /** Number of cells currently cached. */
    uint32_t cell_count;
    /** Number of cells currently subscribed on the fabric. */
    uint32_t subscribed_cell_count;
    /** Monotonic millisecond timestamp of the last cache refresh. */
    uint64_t last_refresh_ms;
};

/** Alias of struct kith_gateway_cache_stats. */
typedef struct kith_gateway_cache_stats kith_gateway_cache_stats_t;

/**
 * Reactor-path phase statistics. Exposed-layout value type. Returned by
 * @c kith_gateway_phase_stats. Each field is a monotonic cumulative total
 * that the composition root samples per tick and publishes as a counter
 * delta, so a @c /metrics scrape yields the per-phase ns/sec (ms after
 * /1e6) and the dispatch rate the per-tick
 * capacity model reconciles against the tick budget.
 */
struct kith_gateway_phase_stats
{
    /** Cumulative nanoseconds spent in @c kith_gateway_cache_refresh. */
    uint64_t refresh_ns_total;
    /** Cumulative nanoseconds spent in @c kith_gateway_view_refresh across
     *  every session (the compose phase of the tick). */
    uint64_t compose_ns_total;
    /** Cumulative nanoseconds spent in @c kith_gateway_deliver across every
     *  session (the delivery phase of the tick). */
    uint64_t deliver_ns_total;
    /** Cumulative count of @c kith_gateway_dispatch calls (every dispatched
     *  frame, regardless of handler kind). Monotonic; never reset. */
    uint64_t dispatch_total;
    /** Cumulative count of self compositions whose subject carried the
     *  actor's publisher-minted update_seq. Monotonic; advanced
     *  unless @c kith_gateway_params_t.self_echo_disabled is set. */
    uint64_t self_echo_stamps_total;
    /** Cumulative count of self compositions whose source artifact carried
     *  no minted update_seq (a publisher that never mints, or an actor
     *  that has never applied movement) — the case where the subscriber's
     *  certification falls back to event-counting. */
    uint64_t self_echo_fallbacks_total;
};

/** Alias of struct kith_gateway_phase_stats. */
typedef struct kith_gateway_phase_stats kith_gateway_phase_stats_t;

/**
 * View-compose sub-phase statistics. Exposed-layout value type. Returned by
 * @c kith_gateway_compose_stats. Each field is a monotonic cumulative total
 * that the composition root samples per tick and publishes as a counter
 * delta, so a @c /metrics scrape yields the per-sub-phase ns/sec (ms after
 * /1e6) inside the compose phase the per-tick
 * capacity model reconciles against the tick budget.
 *
 * The six sub-phases partition most of the compose phase, not all of it:
 * heap growth and the stripe unlock are untimed and error paths return
 * early, so the sum of the fields is at most @c compose_ns_total and the
 * remainder mixes those slivers with failed compositions rather than
 * isolating unattributed compute.
 *
 * @warning @c lock_wait_ns_total measures the reactor thread acquiring the
 *          window's cache stripes (contention against worker-held stripes
 *          included). A worker waiting on a stripe the reactor holds is not
 *          visible here; a small value does not mean worker-side lock waits
 *          are absent.
 */
struct kith_gateway_compose_stats
{
    /** Cumulative nanoseconds spent copying the composing session's
     *  subscription window under the window lock. */
    uint64_t window_ns_total;
    /** Cumulative nanoseconds spent rebuilding the prior-view id set. */
    uint64_t prior_ns_total;
    /** Cumulative nanoseconds spent acquiring the window's cache stripes.
     *  Reactor-side acquisition only — see the warning above. */
    uint64_t lock_wait_ns_total;
    /** Cumulative nanoseconds spent locating the subscriber in the window
     *  and streaming candidate artifacts into the bounded heap. */
    uint64_t scan_ns_total;
    /** Cumulative nanoseconds spent heapsorting the kept candidates. */
    uint64_t sort_ns_total;
    /** Cumulative nanoseconds spent building the view set (subjects,
     *  tiers, crowd aggregate, metadata). */
    uint64_t select_ns_total;
};

/** Alias of struct kith_gateway_compose_stats. */
typedef struct kith_gateway_compose_stats kith_gateway_compose_stats_t;

/**
 * Delivery-executor statistics. Exposed-layout value type. Returned by
 * @c kith_gateway_delivery_executor_stats. The counter set is frozen:
 * fields are appended only by an ABI-breaking change, never additively.
 * The five _total fields are monotonic cumulative counts the
 * composition root samples per tick and publishes as counter deltas; the
 * two inflight fields are gauges
 * recorded as-is.
 *
 * @c inflight_skips_total counts the per-session delivery cadence gaps
 * under the executor; @c compose_wait_timeouts_total plus
 * @c wait_budget_exhausted_total decompose the compose-wait causes of
 * those gaps (an expired per-pass wait budget, and in-flight sessions
 * observed after the budget ran out, respectively).
 */
struct kith_gateway_delivery_executor_stats
{
    /** Cumulative jobs submitted to the delivery executor. */
    uint64_t jobs_submitted_total;
    /** Cumulative per-session deliver passes skipped because the
     *  session's previous deliver was still in flight. */
    uint64_t inflight_skips_total;
    /** Cumulative submissions rejected because the executor's task queue
     *  was full (KITH_EBUSY); the reactor never falls back to inline
     *  delivery for these. */
    uint64_t ebusy_skips_total;
    /** Cumulative in-flight sessions that skipped the compose wait
     *  because the pass's wait budget was already exhausted. */
    uint64_t wait_budget_exhausted_total;
    /** Cumulative compose waits that exceeded the pass's remaining wait
     *  budget (the composition was skipped for that tick). */
    uint64_t compose_wait_timeouts_total;
    /** Deliver jobs currently in flight (gauge). */
    uint64_t inflight_current;
    /** Peak @c inflight_current observed since gateway creation (gauge). */
    uint64_t inflight_high_watermark;
};

/** Alias of struct kith_gateway_delivery_executor_stats. */
typedef struct kith_gateway_delivery_executor_stats kith_gateway_delivery_executor_stats_t;

/**
 * One subject in a subscriber's composed view set. Exposed-layout value type.
 * Returned by @c kith_gateway_view_snapshot as a copy array (no shared
 * ownership). Positions and velocities are Q16.16 fixed-point, matching
 * the sim and fabric artifact representation.
 */
struct kith_gateway_view_subject
{
    /** Opaque actor identifier (caller-supplied, treated as a key). */
    uint64_t actor_id;
    /** Position X in Q16.16 fixed-point. */
    int64_t pos_x;
    /** Position Y in Q16.16 fixed-point. */
    int64_t pos_y;
    /** Position Z in Q16.16 fixed-point (0 for 2D models). */
    int64_t pos_z;
    /** Velocity X in Q16.16 fixed-point. */
    int64_t vel_x;
    /** Velocity Y in Q16.16 fixed-point. */
    int64_t vel_y;
    /** Velocity Z in Q16.16 fixed-point (0 for 2D models). */
    int64_t vel_z;
    /** Last input tick applied when this subject's state was sourced. */
    uint32_t input_tick;
    /** The subject actor's publisher-minted movement-application counter at
     *  source time (0 when the publisher never minted it). Zeroed on
     *  reduced-tier subjects alongside input_tick. The self subject's
     *  value certifies movement coverage to the subscriber. */
    uint32_t update_seq;
    /** Selected representation tier for this subject. */
    kith_fabric_product_level_t level;
    /** Subject classification assigned by the relevance composer. */
    kith_gateway_view_class_t subject_class;
    /** True when retained from the prior view for continuity (no flicker). */
    bool sticky;
    /** True when reduced from a higher tier by the budget bound. Always
     *  false this release — no composer sets it; the field is reserved. */
    bool demoted;
    /** Padding for alignment. */
    uint8_t pad[2];
};

/** Alias of struct kith_gateway_view_subject. */
typedef struct kith_gateway_view_subject kith_gateway_view_subject_t;

/**
 * A subscriber's composed view set metadata. Exposed-layout value type.
 * Returned by @c kith_gateway_view_snapshot alongside the subject array.
 * The counts describe the composition: how many candidates were
 * considered, how many were selected, and the per-tier, per-class, and
 * continuity breakdowns.
 */
struct kith_gateway_view_snapshot
{
    /** The subscriber's actor identifier. */
    uint64_t subscriber_actor_id;
    /** Monotonic millisecond timestamp of the last full composition. A
     *  skipped recomcomposition (nothing relevant changed; see
     *  @c kith_gateway_compose_skips) retains the prior metadata, so this
     *  reflects the composition that produced the current subject set. */
    uint64_t built_at_ms;
    /** Number of candidate actors considered before budgeting. */
    uint16_t candidate_count;
    /** Number of actors selected into the view set. */
    uint16_t selected_count;
    /** Per-tier selected counts (indexed by kith_fabric_product_level). */
    uint16_t tier_selected_count[KITH_GATEWAY_VIEW_TIER_COUNT];
    /** Per-class selected counts (indexed by kith_gateway_view_class). */
    uint16_t class_selected_count[KITH_GATEWAY_VIEW_CLASS_COUNT];
    /** Subjects retained from the prior view (continuity). */
    uint16_t sticky_selected_count;
    /** Subjects demoted from a higher tier by the budget bound. Always 0
     *  this release — no composer sets it; the field is reserved (see the
     *  subject's demoted field). */
    uint16_t demoted_selected_count;
    /** Padding for alignment. */
    uint32_t pad;
};

/** Alias of struct kith_gateway_view_snapshot. */
typedef struct kith_gateway_view_snapshot kith_gateway_view_snapshot_t;

/**
 * Delivery result for one session. Exposed-layout value type. Returned by
 * @c kith_gateway_deliver.
 */
struct kith_gateway_delivery_stats
{
    /** Frames successfully enqueued on the session's connection. */
    uint32_t enqueued;
    /** Frames dropped due to output backpressure (high watermark). */
    uint32_t dropped;
    /** Membership-event frames successfully enqueued this pass (a subset
     *  of @p enqueued under batch framing, where one frame carries events
     *  and state records together). */
    uint32_t events_enqueued;
    /** Subjects skipped by change suppression (strategies that resend
     *  everything report 0). */
    uint32_t suppressed;
    /** Padding for alignment. */
    uint64_t pad;
};

/** Alias of struct kith_gateway_delivery_stats. */
typedef struct kith_gateway_delivery_stats kith_gateway_delivery_stats_t;

/**
 * Cumulative delivery totals. Exposed-layout value type. Returned by
 * @c kith_gateway_delivery_totals and, per session, by
 * @c kith_gateway_session_delivery_totals. The field set is frozen:
 * fields are appended only by an ABI-breaking change, never additively.
 *
 * Unlike the per-call @c kith_gateway_delivery_stats_t, these accumulate
 * every deliver call across the gateway's lifetime — both the inline tick
 * path and on the delivery executor's worker threads — so
 * they are 64-bit: the per-call 32-bit shape would wrap its suppressed
 * field in minutes at saturation. The composition root samples the struct
 * per tick and publishes the deltas as counters. Per session, the same
 * fields accumulate that session's own deliver calls and their sums
 * partition the gateway totals (every frame counted on the gateway
 * appears on exactly one session).
 */
struct kith_gateway_delivery_totals
{
    /** Cumulative frames successfully enqueued on session connections. */
    uint64_t enqueued;
    /** Cumulative frames dropped due to output backpressure. */
    uint64_t dropped;
    /** Cumulative membership-event frames successfully enqueued (a
     *  subset of @p enqueued under batch framing). */
    uint64_t events_enqueued;
    /** Cumulative subjects skipped by change suppression. */
    uint64_t suppressed;
};

/** Alias of struct kith_gateway_delivery_totals. */
typedef struct kith_gateway_delivery_totals kith_gateway_delivery_totals_t;

/**
 * Cumulative view-composition population totals. Exposed-layout value
 * type. Returned by @c kith_gateway_view_totals. The field set is frozen:
 * fields are appended only by an ABI-breaking change, never additively.
 *
 * Aggregated at each session's delivery-pass visit: @p visits counts
 * sessions that held a view when the pass reached them — including
 * sessions whose executor submission was subsequently skipped (in-flight
 * or EBUSY; those skips are separately counted by the executor counters),
 * so @p visits minus the executor skips approximates actually-delivered
 * sessions. The candidate and selected sums carry each visited session's
 * current view metadata (the composition that produced the live subject
 * set — a skipped recomposition retains the prior metadata), which makes
 * the ratio selected/candidate the live selection density and the
 * high-watermarks the cap-binding evidence.
 */
struct kith_gateway_view_totals
{
    /** Cumulative delivery-pass visits of sessions holding a view. */
    uint64_t visits;
    /** Sum of visited sessions' candidate counts. */
    uint64_t candidate_total;
    /** Sum of visited sessions' selected counts. */
    uint64_t selected_total;
    /** Peak candidate count observed at any visit (gauge). */
    uint64_t candidate_high_watermark;
    /** Peak selected count observed at any visit (gauge). */
    uint64_t selected_high_watermark;
};

/** Alias of struct kith_gateway_view_totals. */
typedef struct kith_gateway_view_totals kith_gateway_view_totals_t;

/**
 * Delivery strategy vtable. Size-versioned: implementers set
 * @p size to sizeof(kith_gateway_delivery_vtable_t) and @p abi_version to
 * KITH_ABI_VERSION at their compile time. Registered on a gateway handle
 * via @c kith_gateway_register_delivery and selected per session through
 * @c kith_gateway_params_t.delivery_strategy.
 *
 * A strategy owns the encoding and scheduling policy between the composed
 * view set and the connection enqueue. Strategies execute inline in the
 * gateway tick path (the reactor thread) when no delivery executor is
 * configured; with one (@c delivery_worker_count > 0) the
 * @c deliver callback runs on executor threads —
 * every callback is invoked with no other callback of the same session in
 * flight (enforced by the per-session in-flight serialization), and
 * @c session_init/@c session_fini stay on the reactor thread. Strategies
 * deliver the budgeted send set and follow the strategy correctness
 * rules: the send ledger is the last successfully enqueued
 * state, a dropped frame leaves the ledger untouched, and no product
 * history is requested from the fabric.
 *
 * The factory default @c full is the reference implementation.
 */
struct kith_gateway_delivery_vtable
{
    /** Must be sizeof(kith_gateway_delivery_vtable_t). */
    uint32_t size;
    /** Must be KITH_ABI_VERSION (from kith/version.h). */
    uint32_t abi_version;

    /**
     * Allocate the strategy's per-session encode state. Called once when a
     * session binds the strategy (at kith_gateway_session_create), before
     * the first delivery. @p config is the @c delivery_config pointer the
     * gateway copied at creation (NULL when none was supplied); the
     * strategy interprets it as the size-versioned struct it documented.
     *
     * @param session    The session being initialized. Borrowed for the
     *                   call; the pointer stays valid for the session's
     *                   lifetime.
     * @param config     Strategy configuration, or NULL.
     * @param out_state  Receives the strategy's state pointer (may be NULL
     *                   for stateless strategies).
     * @return           0 on success, negative kith_error to fail session
     *                   creation.
     */
    int (*session_init)(const kith_gateway_session_t *session,
                        const void *config,
                        void **out_state);
    /**
     * Release the per-session state returned by @c session_init. May be
     * NULL when the strategy is stateless. Invoked when the session's last
     * reference drops; never concurrently with any other callback of the
     * same session.
     */
    void (*session_fini)(void *state);
    /**
     * Deliver one pass for the session. Invoked once per tick by
     * @c kith_gateway_tick and on every explicit @c kith_gateway_deliver
     * call, with @p subjects pointing at the session's composed view copy
     * (stable for the call; mutated by the next composition, not during
     * delivery). The strategy enqueues frames itself, typically via
     * @c kith_gateway_deliver_frame, and reports counts through
     * @p out_stats (may be NULL; zeroed before the call).
     *
     * @return 0 on success (including a pass that intentionally enqueues
     *         nothing), negative kith_error on failure.
     */
    int (*deliver)(void *state,
                   kith_gateway_t *gateway,
                   kith_gateway_session_t *session,
                   const kith_gateway_view_subject_t *subjects,
                   size_t subject_count,
                   uint64_t now_ms,
                   kith_gateway_delivery_stats_t *out_stats);

    /** Reserved for future additive fields. Must be zero-filled. */
    void *reserved[8];
};

/** Alias of struct kith_gateway_delivery_vtable. */
typedef struct kith_gateway_delivery_vtable kith_gateway_delivery_vtable_t;

/**
 * Configuration for the built-in @c tiered delivery strategy, supplied via
 * @c kith_gateway_params_t.delivery_config when
 * @c delivery_strategy selects @c "tiered". Size-versioned: callers set
 * @p size to sizeof(kith_gateway_tiered_config_t) and @p abi_version to
 * KITH_ABI_VERSION. A field set to 0 selects the documented default. The
 * strategy copies the image at gateway create; the caller may release the
 * struct once kith_gateway_create returns.
 *
 * The tiered strategy sends a subject only when its serialized record
 * differs from what the subscriber was last successfully sent, and only
 * when the subject's tier cadence allows the send; the self subject is
 * exempt and updates every pass. A subject that has been silent for
 * @p max_gap_ms is refreshed regardless of change as a paranoia backstop;
 * correctness never rests on it.
 */
struct kith_gateway_tiered_config
{
    /** Must be sizeof(kith_gateway_tiered_config_t). */
    uint32_t size;
    /** Must be KITH_ABI_VERSION (from kith/version.h). */
    uint32_t abi_version;

    /** Minimum milliseconds between sends of a FULL-tier subject.
     *  0 sends every pass. */
    uint32_t full_interval_ms;
    /** Minimum milliseconds between sends of a REDUCED-tier subject.
     *  0 selects KITH_GATEWAY_TIERED_DEFAULT_REDUCED_INTERVAL_MS. */
    uint32_t reduced_interval_ms;
    /** Minimum milliseconds between sends of a CROWD-tier subject.
     *  0 selects KITH_GATEWAY_TIERED_DEFAULT_CROWD_INTERVAL_MS. */
    uint32_t crowd_interval_ms;
    /** Maximum silence before any subject is refreshed regardless of
     *  change. 0 selects KITH_GATEWAY_TIERED_DEFAULT_MAX_GAP_MS. */
    uint32_t max_gap_ms;

    /** Reserved for future additive fields. Must be zero-filled. */
    void *reserved[8];
};

/** Alias of struct kith_gateway_tiered_config. */
typedef struct kith_gateway_tiered_config kith_gateway_tiered_config_t;

/**
 * Default configuration values for the built-in @c tiered delivery
 * strategy. The underlying type is fixed.
 */
enum kith_gateway_tiered_default : unsigned int
{
    /** Default minimum interval between sends of a REDUCED-tier subject. */
    KITH_GATEWAY_TIERED_DEFAULT_REDUCED_INTERVAL_MS = 200u,
    /** Default minimum interval between sends of a CROWD-tier subject. */
    KITH_GATEWAY_TIERED_DEFAULT_CROWD_INTERVAL_MS = 500u,
    /** Default maximum silence before an unconditional refresh. */
    KITH_GATEWAY_TIERED_DEFAULT_MAX_GAP_MS = 1000u,
};

/**
 * Register a delivery strategy under @p name on @p gateway's registry.
 * Sessions created after a successful registration resolve
 * @c delivery_strategy against the extended registry; already-created
 * sessions keep their bound strategy.
 *
 * The built-in presets are registered at gateway creation and cannot be
 * replaced: registering a name that is already taken fails with
 * -KITH_EEXIST.
 *
 * @param gateway  Gateway handle. NULL is an error.
 * @param name     NUL-terminated strategy name, non-NULL, non-empty.
 *                 Copied at registration; the caller may free @p name
 *                 after the call.
 * @param vtable   Strategy vtable. The @c size field must be at least
 *                 sizeof(kith_gateway_delivery_vtable_t) and the
 *                 @c abi_version field must match KITH_ABI_VERSION.
 *                 Borrowed for the call only and copied on success.
 * @return         0 on success, negative kith_error on failure:
 *                 - -KITH_EINVAL if @p gateway, @p name, or @p vtable is
 *                   NULL, or @p name is empty,
 *                 - -KITH_ESIZE if @p vtable->size is undersized,
 *                 - -KITH_EABIVER if @p vtable->abi_version is
 *                   incompatible or a required callback is missing,
 *                 - -KITH_EEXIST if @p name is already registered,
 *                 - -KITH_ENOMEM on allocation failure.
 * @thread_safety unsafe — the strategy registry is not synchronized.
 * @ownership caller — @p name and @p vtable are borrowed for the call only.
 */
[[nodiscard]] KITH_API int kith_gateway_register_delivery(
    kith_gateway_t *gateway, const char *name, const kith_gateway_delivery_vtable_t *vtable);

/**
 * Message handler callback. Registered via @c kith_gateway_register_handler
 * and invoked by @c kith_gateway_dispatch when a decoded frame of the
 * registered type arrives on a session's connection.
 *
 * An unflagged handler runs on the reactor thread (the thread that called
 * @c kith_gateway_dispatch). Handlers registered with a pool-bound flag
 * (@c KITH_GATEWAY_HANDLER_PYTHON or @c KITH_GATEWAY_HANDLER_POOL) are
 * posted to the attached worker pool and run on a worker thread: Python
 * trampolines keep the interpreter off the reactor, and
 * pool-flagged native C handlers keep long C work off it.
 *
 * @param msg_type     The message type id from the decoded frame header.
 * @param payload      Payload bytes, or NULL when @p payload_len is 0.
 *                     Borrows the connection's ring buffer for the call
 *                     only; copy if the data must outlive the callback.
 * @param payload_len  Payload length in bytes (not counting the
 *                     correlation trailer, which the codec stripped).
 * @param session      The session the frame arrived on. Borrowed for the
 *                     call; the callback may acquire a reference or
 *                     enqueue a response.
 * @param user_data    Caller context passed to @c kith_gateway_register_handler.
 */
typedef void (*kith_gateway_msg_handler_fn)(uint16_t msg_type,
                                            const void *payload,
                                            uint32_t payload_len,
                                            kith_gateway_session_t *session,
                                            void *user_data);

/**
 * Build a gateway handle from @p params and borrowed net, fabric, and
 * proto handles.
 *
 * @param params       Creation parameters; @c size and @c abi_version must
 *                     match the runtime generation. NULL selects all
 *                     defaults.
 * @param net          Borrowed transport handle. The gateway accepts
 *                     connections, creates frames, and enqueues on
 *                     connections via this handle. Must outlive the
 *                     gateway. NULL is an error.
 * @param fabric       Borrowed fabric handle. The gateway subscribes to
 *                     cells, drains pending products, and snapshots cells
 *                     via this handle. Must outlive the gateway. NULL is
 *                     an error.
 * @param proto        Borrowed proto handle. The gateway encodes actor
 *                     state replication frames via this handle. Must
 *                     outlive the gateway. NULL is an error.
 * @param alloc        Allocator for the new handle, its session table,
 *                     shared cell cache, handler table, view and delivery
 *                     scratch, delivery registry and executor, and every
 *                     session, view, cache, strategy, and dispatch buffer
 *                     grown through them, used again when
 *                     kith_gateway_destroy frees them. NULL selects the
 *                     default allocator; a supplied allocator is validated
 *                     (see kith_allocator_t) and must outlive the handle,
 *                     every session created from it, and any dispatch work
 *                     record still draining on a worker pool.
 * @param out_gateway  Receives the new handle on success.
 * @return             0 on success, negative kith_error on failure:
 *                     - -KITH_EINVAL if @p out_gateway, @p net, @p fabric,
 *                       or @p proto is NULL, or @p alloc is missing an
 *                       operation,
 *                     - -KITH_EABIVER if @p params or @p alloc has an
 *                       incompatible abi_version,
 *                     - -KITH_ESIZE if @p params or @p alloc has an
 *                       undersized size,
 *                     - -KITH_ENOMEM on allocation failure,
 *                     - -KITH_ESTATE on internal index initialization
 *                       failure.
 * @thread_safety unsafe — must not race with another
 *                kith_gateway_create on the same @p out_gateway slot.
 * @ownership callee — the returned handle is freed by the caller with
 *           kith_gateway_destroy. @p net, @p fabric, and @p proto are
 *           borrowed, not owned.
 */
[[nodiscard]] KITH_API int kith_gateway_create(const kith_gateway_params_t *params,
                                               kith_net_t *net,
                                               kith_fabric_t *fabric,
                                               kith_proto_t *proto,
                                               const kith_allocator_t *alloc,
                                               kith_gateway_t **out_gateway);

/**
 * Release a gateway handle, its session table, shared cell cache, view
 * store, handler table, and internal spatial index. Passing NULL is a
 * no-op. The borrowed net, fabric, and proto handles are not freed (the
 * caller owns them and destroys them separately). Sessions created via
 * @c kith_gateway_session_create are not freed here (the caller owns them
 * and must call @c kith_gateway_session_destroy).
 *
 * @param gateway Gateway handle. NULL is a no-op.
 * @thread_safety unsafe — no session/cache/view/deliver/dispatch may be
 *                in flight on @p gateway when this is called.
 * @ownership callee — @p gateway is consumed and freed by the call.
 */
KITH_API void kith_gateway_destroy(kith_gateway_t *gateway);

/**
 * Create a session bound to @p conn. The session carries the authenticated
 * @p principal_id and @p type. The gateway acquires a reference to @p conn
 * (via @c kith_net_conn_acquire) so the connection stays alive for the
 * session's lifetime; the reference is released on
 * @c kith_gateway_session_destroy.
 *
 * @param gateway     Gateway handle. NULL is an error.
 * @param conn        Connection the session binds to. NULL is an error.
 *                    Borrowed; the gateway acquires its own reference.
 * @param type        Session authentication type.
 * @param principal_id Authenticated principal identifier (opaque key).
 * @param alloc       Allocator for the new session, its view state,
 *                    subscription window, and the delivery strategy state
 *                    bound at creation, used again when the session's last
 *                    reference releases them. NULL derives the gateway's
 *                    allocator (the session is gateway-attached storage);
 *                    a supplied allocator is validated (see
 *                    kith_allocator_t) and must outlive the session — a
 *                    worker dispatch reference can release it after the
 *                    gateway is gone.
 * @param out_session Receives the new session on success.
 * @return            0 on success, negative kith_error on failure:
 *                    - -KITH_EINVAL if @p gateway, @p conn, or
 *                      @p out_session is NULL, or @p alloc is missing an
 *                      operation,
 *                    - -KITH_EABIVER if @p alloc has an incompatible
 *                      abi_version,
 *                    - -KITH_ESIZE if @p alloc has an undersized size,
 *                    - -KITH_ENOENT if the configured delivery strategy
 *                      name is not registered,
 *                    - -KITH_EBUSY if the session table is full,
 *                    - -KITH_EEXIST if a session is already bound to
 *                      @p conn,
 *                    - the bound strategy's @c session_init error when it
 *                      rejects the configured delivery strategy config
 *                      (e.g. -KITH_ESIZE or -KITH_EABIVER),
 *                    - -KITH_ENOMEM on allocation failure.
 * @thread_safety unsafe — the session table is not synchronized.
 * @ownership callee — the caller destroys the session with
 *           kith_gateway_session_destroy.
 */
[[nodiscard]] KITH_API int kith_gateway_session_create(kith_gateway_t *gateway,
                                                       kith_net_conn_t *conn,
                                                       kith_gateway_session_type_t type,
                                                       uint64_t principal_id,
                                                       const kith_allocator_t *alloc,
                                                       kith_gateway_session_t **out_session);

/**
 * Destroy a session and release its connection reference. Passing NULL is
 * a no-op. The session is removed from the gateway's session table and its
 * owner reference is dropped; the bound connection is not closed here (the
 * caller or the reactor closes it via @c kith_net_conn_close), and the
 * connection reference acquired at creation is released when the last
 * session reference drops.
 *
 * The session is refcounted across the reactor→worker dispatch handoff: a
 * worker dispatch in flight on @p session holds its own reference, so
 * calling this while a dispatch is in flight is safe — the session's memory
 * and its connection reference are freed when that dispatch's reference
 * releases, not here. The session is removed from the table immediately so
 * no new dispatch can locate it.
 *
 * @param session Session handle. NULL is a no-op.
 * @thread_safety safe-if called from the reactor thread — the session table
 *                is not synchronized (reactor-thread-only). Safe to call
 *                while a worker dispatch is in flight on @p session: the
 *                session is freed when the last reference drops.
 * @ownership callee — @p session is consumed; the session is freed when the
 *           last reference drops (immediately when no dispatch is in flight,
 *           otherwise when the in-flight dispatch releases).
 */
KITH_API void kith_gateway_session_destroy(kith_gateway_session_t *session);

/**
 * Pin a session with an additional reference. The session's memory stays
 * allocated until the matching @c kith_gateway_session_release, even after
 * the reactor destroys the session. Acquire is legal only where a
 * reference is already held: the dispatch reference inside a handler, or
 * the owner reference on the reactor thread. The dispatch reference dies
 * when the handler returns — an acquire made after that (from a session
 * pointer stored in a previous dispatch) is undefined behavior on a
 * potentially-freed session. A pinned session outlives the reactor's
 * destroy: its send follows the discrete-send contract (a closed
 * connection is refused in the lifecycle family), and its identity record
 * stays readable until the release.
 *
 * @param session Session handle. NULL is a no-op.
 * @thread_safety safe-if the caller already holds a reference to @p session
 *                (a dispatch reference inside a handler, or the owner
 *                reference on the reactor thread) — the increment is
 *                atomic, but a stale pointer is undefined.
 * @ownership caller — @p session is borrowed for the call only; the pinned
 *           reference it leaves behind is released with
 *           @c kith_gateway_session_release.
 */
KITH_API void kith_gateway_session_acquire(kith_gateway_session_t *session);

/**
 * Release a reference acquired with @c kith_gateway_session_acquire. The
 * session's memory is freed when the last reference drops. The teardown
 * touches no gateway or session-table state, so this is safe on any
 * thread. Passing NULL is a no-op.
 *
 * @param session Session handle. NULL is a no-op.
 * @thread_safety safe — atomic refcount decrement; the last release frees
 *                the session and its connection reference.
 * @ownership callee — the pinned reference is consumed by the call; the
 *           session's memory frees when the last reference drops.
 */
KITH_API void kith_gateway_session_release(kith_gateway_session_t *session);

/**
 * Copy the session's metadata into @p out_info. The metadata fields
 * (session id, principal id, and type) are immutable after creation; the
 * bound actor id is written atomically by
 * @c kith_gateway_session_bind_actor and may change between successive
 * calls while a handler rebinds the session. The session is refcounted,
 * so a caller holding a reference (the reactor's owner reference, or a
 * dispatch reference acquired across the reactor→worker handoff) may read
 * them concurrently from a worker thread.
 *
 * @param session  Session handle. NULL is an error.
 * @param out_info Receives the metadata on success.
 * @return         0 on success, negative kith_error on failure:
 *                 - -KITH_EINVAL if @p session or @p out_info is NULL.
 * @thread_safety safe — the session is refcounted and every field read
 *                here is either immutable after creation or updated only
 *                through atomics (the bound actor id). A caller holding a
 *                reference may read concurrently; the session stays alive
 *                until the last reference drops.
 * @ownership caller — @p out_info is the caller's output storage.
 */
[[nodiscard]] KITH_API int kith_gateway_session_info(const kith_gateway_session_t *session,
                                                     kith_gateway_session_info_t *out_info);

/**
 * Return the connection the session is bound to, or NULL when @p session
 * is NULL. The pointer is fixed at session creation and stays valid for
 * the session's lifetime — the session holds a connection reference until
 * its last reference drops — whether or not the connection has since been
 * closed: closedness is not observable through this return. The
 * destroyed-session callback (see
 * @c kith_gateway_register_session_destroyed_handler) is the session
 * death signal. The reactor polls the connection's fd for read/write
 * readiness and calls read/write on readiness.
 *
 * @param session Session handle.
 * @return        The connection the session is bound to; NULL when @p session is NULL.
 * @thread_safety safe — the connection pointer is fixed after session
 *                creation.
 * @ownership caller — the returned connection is borrowed from the session;
 *           the caller must not destroy it.
 */
KITH_API kith_net_conn_t *kith_gateway_session_conn(const kith_gateway_session_t *session);

/**
 * Bind the session to a subscriber actor identifier. Called after the subscriber
 * selects an actor (the actor id is not known at session creation).
 * The relevance composer uses the bound actor id to locate the subscriber's
 * position in the shared cache and compose the view set around it. Until
 * a binding is set, @c kith_gateway_view_refresh returns -KITH_ESTATE.
 *
 * Rebinding publishes atomically: refreshes and info reads running
 * concurrently with this call are race-free. A rebind landing while a
 * refresh composes forces that session's view to be rebuilt at the next
 * refresh, so at most one composed frame mixes the previous and the new
 * identity.
 *
 * @param session  Session handle. NULL is an error.
 * @param actor_id Subscriber actor identifier to bind.
 * @return         0 on success, negative kith_error on failure:
 *                 - -KITH_EINVAL if @p session is NULL.
 * @thread_safety safe — the bind publishes the actor id atomically and
 *                is race-free with concurrent refreshes and info reads.
 *                The call acquires no reference: the caller must hold one
 *                (the reactor's owner reference or a dispatch reference)
 *                that keeps the session alive for the duration.
 * @ownership caller — @p session is borrowed for the call only.
 */
[[nodiscard]] KITH_API int kith_gateway_session_bind_actor(kith_gateway_session_t *session,
                                                           uint64_t actor_id);

/**
 * Add a cell to the session's subscription window. The gateway subscribes
 * the cell on the fabric via the shared cache (incrementing the cache's
 * refcount if another local subscriber already tracks the cell). The
 * relevance composer considers only the cells in a subscriber's window
 * when building its view set, so a cell no subscriber tracks locally is
 * invisible to that subscriber's view. Idempotent: adding a cell already
 * in the window is a no-op.
 *
 * Cells live in fixed-size tables created with the gateway: the shared
 * cache stripes (sized by @c kith_gateway_params_t.cache_bucket_count)
 * and the gateway's fabric subscription (a fixed-capacity interest set,
 * see @c kith_fabric_subscription_add). A cell needing a new table slot
 * fails with -KITH_ENOMEM when either its cache stripe or the gateway's
 * fabric subscription has no free slot. A failed add leaves the window
 * and both tables unchanged and is RETAINED: the tick pass retries it
 * (bounded work per pass) until a slot frees and the cell is subscribed,
 * so a one-shot seed heals without a caller-side retry loop. @c
 * kith_gateway_session_window_remove and @c kith_gateway_session_window_clear
 * cancel a retained add; a caller retry remains legal (the add is
 * idempotent). Retention is bounded per session — an add arriving while
 * the retention list is full fails without retention — and every capacity
 * failure increments @c kith_gateway_window_add_failures, every
 * retry-pass landing increments @c kith_gateway_window_retry_adds.
 * Removing cells frees slots and remains the operator's capacity lever.
 *
 * @param session  Session handle. NULL is an error.
 * @param key      Cell locator to add. NULL is an error.
 * @return         0 on success, negative kith_error on failure:
 *                 - -KITH_EINVAL if @p session or @p key is NULL,
 *                 - -KITH_ENOMEM when the session's window array cannot
 *                   grow, or when either the cell's cache stripe or the
 *                   gateway's fabric subscription has no free slot (the
 *                   add is retained and retried in both cases),
 * @thread_safety safe — the window is serialized by a per-session mutex.
 *                May be called from a worker thread while the reactor
 *                composes the view set.
 * @ownership caller — @p key is borrowed for the call only.
 */
[[nodiscard]] KITH_API int kith_gateway_session_window_add(kith_gateway_session_t *session,
                                                           const kith_fabric_cell_key_t *key);

/**
 * Remove a cell from the session's subscription window. The shared cache's
 * refcount for the cell is decremented and, when no local subscriber tracks
 * it, the cell is unsubscribed on the fabric and evicted from the cache.
 * Idempotent: removing a cell not in the window is a no-op. A remove also
 * cancels a retained add for the cell (see
 * @c kith_gateway_session_window_add), so the retry never lands a
 * rescinded intent.
 *
 * @param session  Session handle. NULL is an error.
 * @param key      Cell locator to remove. NULL is an error.
 * @return         0 on success, negative kith_error on failure:
 *                 - -KITH_EINVAL if @p session or @p key is NULL.
 * @thread_safety safe — the window is serialized by a per-session mutex.
 * @ownership caller — @p key is borrowed for the call only.
 */
[[nodiscard]] KITH_API int kith_gateway_session_window_remove(kith_gateway_session_t *session,
                                                              const kith_fabric_cell_key_t *key);

/**
 * Remove every cell from the session's subscription window. The shared
 * cache's refcount is decremented for each cell; cells with no remaining
 * local subscriber are unsubscribed on the fabric and evicted from the
 * cache. Every retained add (see @c kith_gateway_session_window_add) is
 * cancelled with the window. Passing NULL is a no-op.
 *
 * @param session Session handle. NULL is a no-op.
 * @thread_safety safe — the window is serialized by a per-session mutex.
 * @ownership caller — @p session is borrowed for the call only.
 */
KITH_API void kith_gateway_session_window_clear(kith_gateway_session_t *session);

/**
 * Bind the session to a subscriber actor and seed its subscription window
 * in one atomic step. The delivery startup contract's identity and
 * subscription halves land together: under one hold of the session's
 * window mutex the actor id publishes, then each cell joins the window
 * with the per-cell add semantics of @c kith_gateway_session_window_add
 * (idempotent, subscription via the shared cache, a recomposition counter
 * bump per committed add). No observer can read the gap between the two
 * halves: the seed-failure census
 * (@c kith_gateway_sessions_without_cells) and the relevance composer
 * both snapshot the session under the same mutex, so the session is
 * either unpopulated or populated — the bound-without-cells state is
 * reachable only as a seed failure (every cell retained) or through a
 * bind-only populate.
 *
 * A capacity failure (-KITH_ENOMEM) is not fatal: the bind stays, landed
 * cells stay, and every failed add is retained and retried by the tick
 * pass until a slot frees (see @c kith_gateway_session_window_add), so
 * the window converges to the requested set. Window removal or clear
 * cancels a retained add. The call reports no per-cell breakdown;
 * per-cell precision is the per-cell add call's job.
 *
 * @param session  Session handle. NULL is an error.
 * @param actor_id Subscriber actor identifier to bind.
 * @param keys     Cell locators to seed, borrowed for the call only. NULL
 *                 is an error when @p count is non-zero; NULL with a zero
 *                 count is a bind-only populate.
 * @param count    Number of cells in @p keys; 0 binds without seeding.
 * @return         0 on success, negative kith_error on failure:
 *                 - -KITH_EINVAL if @p session is NULL or @p keys is NULL
 *                   with a non-zero @p count,
 *                 - -KITH_ENOMEM when a cell's cache stripe or the
 *                   gateway's fabric subscription has no free slot, or the
 *                   window array cannot grow (landed cells stay, failed
 *                   adds are retained and retried in all cases).
 * @thread_safety safe — the bind publishes the actor id atomically and
 *                the seed is serialized by the per-session window mutex,
 *                both under one critical section; may be called from a
 *                worker thread while the reactor composes the view set.
 *                The call acquires no reference: the caller must hold one
 *                (the reactor's owner reference or a dispatch reference)
 *                that keeps the session alive for the duration.
 * @ownership caller — @p keys is borrowed for the call only.
 */
[[nodiscard]] KITH_API int kith_gateway_session_populate(kith_gateway_session_t *session,
                                                         uint64_t actor_id,
                                                         const kith_fabric_cell_key_t *keys,
                                                         size_t count);

/**
 * Copy the number of cells in the session's subscription window into
 * @p out_count.
 *
 * @param session   Session handle. NULL is an error.
 * @param out_count Receives the window size on success.
 * @return          0 on success, negative kith_error on failure:
 *                  - -KITH_EINVAL if @p session or @p out_count is NULL.
 * @thread_safety safe — the window is serialized by a per-session mutex.
 * @ownership caller — @p out_count is the caller's output storage.
 */
[[nodiscard]] KITH_API int kith_gateway_session_window_count(const kith_gateway_session_t *session,
                                                             size_t *out_count);

/**
 * Report the number of sessions currently in the gateway's session table.
 * The value is a mirror gauge of the table, updated at the same
 * reactor-thread create and destroy sites, so it reads consistently from
 * any thread without touching the reactor-thread-only table itself. A
 * NULL gateway reports 0.
 *
 * @param gateway Gateway handle.
 * @return        The live session count.
 * @thread_safety safe — the mirror gauge is an atomic read.
 * @ownership caller — @p gateway is borrowed for the call only.
 */
[[nodiscard]] KITH_API uint64_t kith_gateway_session_count(const kith_gateway_t *gateway);

/**
 * Copy the identity record of every live session into @p out. The entries
 * are copied in no particular order and @p out_count receives the number
 * copied; a @p max smaller than the live count (compare
 * @c kith_gateway_session_count) copies the first @p max entries and
 * leaves the rest uncopied.
 *
 * @param gateway   Gateway handle. NULL is an error.
 * @param out       Output buffer with room for @p max records. May be NULL
 *                  only when @p max is 0.
 * @param max       Maximum number of records to copy.
 * @param out_count Receives the number of records copied. May be NULL.
 * @return          0 on success, negative kith_error on failure:
 *                  - -KITH_EINVAL if @p gateway is NULL, or @p out is NULL
 *                    with @p max greater than 0.
 * @thread_safety unsafe — the session table is not synchronized
 *                (reactor-thread-only), like
 *                @c kith_gateway_session_create.
 * @ownership caller — @p out is the caller's output storage.
 */
[[nodiscard]] KITH_API int kith_gateway_session_snapshot(const kith_gateway_t *gateway,
                                                         kith_gateway_session_info_t *out,
                                                         size_t max,
                                                         size_t *out_count);

/**
 * Register a message handler for @p msg_type. When @c kith_gateway_dispatch
 * receives a frame of this type, it invokes @p fn with @p user_data and
 * the session bound to the frame's connection. Registering a handler for
 * a type that already has one replaces the prior registration.
 *
 * @param gateway   Gateway handle. NULL is an error.
 * @param msg_type  Message type id (must match a type registered with the
 *                  proto handle).
 * @param fn        Handler callback. NULL is an error.
 * @param user_data Caller context passed verbatim to @p fn on dispatch.
 * @return          0 on success, negative kith_error on failure:
 *                  - -KITH_EINVAL if @p gateway or @p fn is NULL or
 *                    @p msg_type is out of range,
 *                  - -KITH_ENOMEM on allocation failure.
 * @thread_safety safe — the handler table is serialized by a per-handle
 *                mutex.
 * @ownership caller — @p user_data is borrowed for the registration's
 *           lifetime; the caller must keep it valid until the handler is
 *           unregistered or the gateway is destroyed.
 */
[[nodiscard]] KITH_API int kith_gateway_register_handler(kith_gateway_t *gateway,
                                                         uint16_t msg_type,
                                                         kith_gateway_msg_handler_fn fn,
                                                         void *user_data);

/**
 * Message handler registration flags. The underlying type is fixed.
 */
enum kith_gateway_handler_flag : unsigned int
{
    /** Default: the handler is a C function pointer and runs inline on the
     *  reactor thread (no pool hop). */
    KITH_GATEWAY_HANDLER_NONE = 0u,
    /** The handler is a Python-bound ctypes trampoline. It is submitted to
     *  the worker pool attached via @c kith_gateway_attach_worker_pool
     *  instead of invoked inline, keeping the reactor thread out of the
     *  Python interpreter. With no pool attached the dispatch is dropped
     *  and counted — the reactor never runs a pool-bound handler inline. */
    KITH_GATEWAY_HANDLER_PYTHON = 1u,
    /** The handler is a native C function pointer dispatched through the
     *  attached worker pool instead of invoked inline on the reactor thread.
     *  The dispatch crosses the reactor→worker handoff with the same
     *  machinery as @c KITH_GATEWAY_HANDLER_PYTHON (payload copy, session
     *  reference, drop-and-count on pool exhaustion) while the handler body
     *  stays out of the Python interpreter entirely.
     *  With no pool attached the dispatch is dropped and counted — the
     *  reactor never runs a pool-bound handler inline. */
    KITH_GATEWAY_HANDLER_POOL = 2u,
};

/** Alias of enum kith_gateway_handler_flag. */
typedef enum kith_gateway_handler_flag kith_gateway_handler_flag_t;

/**
 * Variant of @c kith_gateway_register_handler that accepts a flags bitmask.
 * @c KITH_GATEWAY_HANDLER_PYTHON marks the handler as a Python-bound ctypes
 * trampoline so @c kith_gateway_dispatch routes it through the attached
 * worker pool instead of invoking it on the reactor thread.
 * @c KITH_GATEWAY_HANDLER_POOL marks a native C handler for the same
 * pool-bound dispatch. A handler is one or the other — setting
 * both flags is rejected.
 *
 * @param gateway   Gateway handle. NULL is an error.
 * @param msg_type  Message type id.
 * @param fn        Handler callback. NULL is an error.
 * @param user_data Caller context passed verbatim to @p fn on dispatch.
 * @param flags     Bitwise OR of @c kith_gateway_handler_flag values.
 *                  @c KITH_GATEWAY_HANDLER_PYTHON and
 *                  @c KITH_GATEWAY_HANDLER_POOL together are an error.
 * @return          0 on success, negative kith_error on failure:
 *                  - -KITH_EINVAL if @p gateway or @p fn is NULL,
 *                    @p msg_type is out of range, or @p flags sets both
 *                    @c KITH_GATEWAY_HANDLER_PYTHON and
 *                    @c KITH_GATEWAY_HANDLER_POOL,
 *                  - -KITH_ENOMEM on allocation failure.
 * @thread_safety safe — the handler table is serialized by a per-handle
 *                mutex.
 * @ownership caller — @p user_data is borrowed for the registration's lifetime;
 *           the caller must keep it valid until the handler is unregistered or
 *           the gateway is destroyed.
 */
[[nodiscard]] KITH_API int kith_gateway_register_handler_flags(kith_gateway_t *gateway,
                                                               uint16_t msg_type,
                                                               kith_gateway_msg_handler_fn fn,
                                                               void *user_data,
                                                               uint32_t flags);

/**
 * Attach a worker pool for pool-bound handler dispatch.
 *
 * When a non-NULL @p pool is attached, handlers registered with the
 * @c KITH_GATEWAY_HANDLER_PYTHON or the @c KITH_GATEWAY_HANDLER_POOL flag
 * are submitted to @p pool by @c kith_gateway_dispatch instead of invoked
 * inline on the reactor thread; a worker runs the handler (which may apply
 * input to the sim, re-broadcast via the fabric, etc.) and returns. Gateway
 * handlers are fire-and-forget (the replication stream carries the response,
 * not the handler return), so no reactor-flush completion is needed — the
 * worker just runs the callback.
 *
 * Unflagged C handlers (the default) always run inline on the reactor
 * thread — they are plain C function pointers, so the pool hop would only
 * add latency. Python-bound handlers never enter Python on the reactor
 * (the reactor never enters Python); pool-flagged C handlers leave the
 * reactor for the same reason a long C handler would stall it.
 *
 * Pass NULL to detach (handlers revert to inline dispatch). The pool is
 * borrowed for the gateway handle's lifetime; the caller owns it.
 *
 * @param gateway Gateway handle. Must be non-NULL.
 * @param pool    Worker pool handle, or NULL to detach.
 * @return        0 on success, negative kith_error on failure:
 *                - -KITH_EINVAL if @p gateway is NULL.
 * @thread_safety unsafe — call from the composition root before the reactor
 *                starts (the dispatch path reads @p pool without a lock).
 * @ownership caller — @p pool is borrowed for the call only; the caller
 *           destroys it after @c kith_gateway_destroy.
 */
[[nodiscard]] KITH_API int kith_gateway_attach_worker_pool(kith_gateway_t *gateway,
                                                           kith_worker_t *pool);

/**
 * Unregister the message handler for @p msg_type. After this call,
 * @c kith_gateway_dispatch returns -KITH_ENOENT for frames of @p msg_type.
 * Idempotent: unregistering a type with no registered handler returns 0.
 *
 * @param gateway  Gateway handle. NULL is an error.
 * @param msg_type Message type id to unregister.
 * @return         0 on success (even if no handler was registered),
 *                 - -KITH_EINVAL if @p gateway is NULL.
 * @thread_safety safe — the handler table is serialized by a per-handle
 *                mutex.
 * @ownership caller — @p gateway is borrowed for the call only.
 */
[[nodiscard]] KITH_API int kith_gateway_unregister_handler(kith_gateway_t *gateway,
                                                           uint16_t msg_type);

/**
 * Destroyed-session callback. The gateway invokes it with the destroyed
 * session's identity record when it destroys the session; @p info is
 * borrowed for the call's duration. The session itself is not passed: it
 * is detached and dying, and the pool-dispatched kinds run after the
 * teardown has returned, so no reference to it can be carried here.
 *
 * @param info      The destroyed session's identity record.
 * @param user_data The context passed at registration.
 */
typedef void (*kith_gateway_session_destroyed_fn)(const kith_gateway_session_info_t *info,
                                                  void *user_data);

/**
 * Register the destroyed-session callback. The gateway invokes it once
 * per session destroyed while the gateway handle is alive — after the
 * session is detached from the session table and its subscriptions, and
 * before the owner reference drops — with the session's identity record.
 * The callback must not destroy the session (it is not passed) and must
 * not rely on notification ordering across sessions. Registering
 * replaces any previous registration;
 * @c kith_gateway_unregister_session_destroyed_handler detaches. Sessions
 * that outlive the gateway (the caller destroys them after teardown)
 * deliver nothing.
 *
 * The unflagged form runs the callback inline on the reactor thread
 * inside the destroy call; see
 * @c kith_gateway_register_session_destroyed_handler_flags for the
 * pool-dispatched forms.
 *
 * @param gateway   Gateway handle. NULL is an error.
 * @param fn        Destroyed-session callback. NULL is an error.
 * @param user_data Caller context passed verbatim to @p fn on dispatch.
 * @return          0 on success, negative kith_error on failure:
 *                  - -KITH_EINVAL if @p gateway or @p fn is NULL.
 * @thread_safety safe — the registration slot is serialized by a mutex.
 * @ownership caller — @p user_data is borrowed for the registration's
 *           lifetime; the caller must keep it valid until the handler is
 *           unregistered or the gateway is destroyed.
 */
[[nodiscard]] KITH_API int kith_gateway_register_session_destroyed_handler(
    kith_gateway_t *gateway, kith_gateway_session_destroyed_fn fn, void *user_data);

/**
 * Register the destroyed-session callback with dispatch flags. The flags
 * select where the callback runs:
 *
 * - @c KITH_GATEWAY_HANDLER_NONE (the default): inline on the reactor
 *   thread, inside the destroy call.
 * - @c KITH_GATEWAY_HANDLER_PYTHON: the callback is a Python-bound ctypes
 *   trampoline; it is submitted to the attached worker pool and runs on a
 *   worker, like a Python-bound message handler. A notification that
 *   arrives while the interpreter is finalizing is refused at the worker
 *   (the process is exiting; nothing to clean into).
 * - @c KITH_GATEWAY_HANDLER_POOL: the callback is a native C function
 *   pointer dispatched through the attached worker pool.
 *
 * A notification bound for a pool (either flag) with no pool attached, or
 * refused by a saturated pool, is dropped and counted — see
 * @c kith_gateway_lifecycle_drops. A dropped notification means the
 * game's disconnect bookkeeping for that session did not run. Setting
 * both dispatch flags is rejected.
 *
 * @param gateway   Gateway handle. NULL is an error.
 * @param fn        Destroyed-session callback. NULL is an error.
 * @param user_data Caller context passed verbatim to @p fn on dispatch.
 * @param flags     Bitwise OR of @c kith_gateway_handler_flag values.
 * @return          0 on success, negative kith_error on failure:
 *                  - -KITH_EINVAL if @p gateway or @p fn is NULL, or both
 *                    dispatch flags are set.
 * @thread_safety safe — the registration slot is serialized by a mutex.
 * @ownership caller — @p user_data is borrowed for the registration's
 *           lifetime; the caller must keep it valid until the handler is
 *           unregistered or the gateway is destroyed.
 */
[[nodiscard]] KITH_API int kith_gateway_register_session_destroyed_handler_flags(
    kith_gateway_t *gateway, kith_gateway_session_destroyed_fn fn, void *user_data, uint32_t flags);

/**
 * Unregister the destroyed-session callback. Idempotent: unregistering
 * with no registration returns 0. Sessions destroyed after this call
 * deliver no notification.
 *
 * @param gateway Gateway handle. NULL is an error.
 * @return        0 on success (even if no handler was registered),
 *                - -KITH_EINVAL if @p gateway is NULL.
 * @thread_safety safe — the registration slot is serialized by a mutex.
 * @ownership caller — @p gateway is borrowed for the call only.
 */
[[nodiscard]] KITH_API int
kith_gateway_unregister_session_destroyed_handler(kith_gateway_t *gateway);

/**
 * Dispatch a decoded frame to the handler registered for its type. Looks
 * up the session bound to @p conn, extracts the message type and payload
 * from @p frame, and invokes the registered handler. If no session is
 * bound to @p conn, returns -KITH_ENOENT. If no handler is registered for
 * the frame's type, returns -KITH_ENOENT.
 *
 * The handler runs on the calling thread unless it is pool-bound: handlers
 * registered with @c KITH_GATEWAY_HANDLER_PYTHON or
 * @c KITH_GATEWAY_HANDLER_POOL are posted to the attached worker pool by
 * this call (payload copied, session referenced), so they run on a worker
 * thread, not the reactor thread. Unflagged C handlers may enqueue a
 * response on @p conn directly.
 *
 * @param gateway Gateway handle. NULL is an error.
 * @param conn    Connection the frame arrived on. NULL is an error.
 * @param frame   Decoded proto frame view. NULL is an error. The
 *                @c payload field borrows the connection's ring buffer
 *                and is valid for the call only.
 * @return        0 on success (handler found and invoked),
 *                negative kith_error on failure:
 *                - -KITH_EINVAL if @p gateway, @p conn, or @p frame is
 *                  NULL,
 *                - -KITH_ENOENT if no session is bound to @p conn or no
 *                  handler is registered for the frame's type id,
 *                  - -KITH_EBUSY when a pool-bound handler (registered with
 *                    @c KITH_GATEWAY_HANDLER_PYTHON or
 *                    @c KITH_GATEWAY_HANDLER_POOL) cannot dispatch off the
 *                    reactor thread: the pool's task queue is exhausted, or
 *                    no worker pool is attached. The dispatch is dropped and
 *                    counted via @c kith_gateway_dispatch_drops; the reactor
 *                    never runs a pool-bound handler inline. Unflagged C
 *                    handlers run inline and never return EBUSY,
 *                  - -KITH_ENOMEM when the pool-dispatch work record cannot
 *                    be allocated.
 * @thread_safety safe — the handler table is serialized by a per-handle
 *                mutex. The session lookup is not synchronized; call from
 *                the reactor thread that owns the connection.
 * @ownership caller — @p frame is borrowed for the call only.
 */
[[nodiscard]] KITH_API int kith_gateway_dispatch(kith_gateway_t *gateway,
                                                 kith_net_conn_t *conn,
                                                 const kith_proto_frame_t *frame);

/**
 * Read the monotonic count of pool-bound handler dispatches dropped due to
 * worker-pool exhaustion. A drop occurs when @c kith_gateway_dispatch is
 * called for a handler registered with @c KITH_GATEWAY_HANDLER_PYTHON or
 * @c KITH_GATEWAY_HANDLER_POOL, a worker pool is attached, and
 * @c kith_worker_submit returns EBUSY; the dispatch is dropped instead of
 * running the handler on the reactor thread. The
 * counter is monotonic and never reset. The composition
 * root records its delta as @c kith_gateway_dispatch_dropped_total.
 *
 * @param gateway   Gateway handle. NULL is an error.
 * @param out_drops Receives the monotonic drop count on success.
 * @return          0 on success, negative kith_error on failure:
 *                  - -KITH_EINVAL if @p gateway or @p out_drops is NULL.
 * @thread_safety safe — the counter is atomic; readable from any thread.
 *                Incremented on the reactor thread at drop time.
 * @ownership caller — @p out_drops is the caller's output storage.
 */
[[nodiscard]] KITH_API int kith_gateway_dispatch_drops(const kith_gateway_t *gateway,
                                                       uint64_t *out_drops);

/**
 * Report the count of destroyed-session notifications dropped because no
 * worker pool was attached or the attached pool refused the submit. A
 * dropped notification means the game's disconnect bookkeeping for that
 * session did not run, so the count is the operator's signal that
 * per-session state is leaking; the composition root records the counted
 * delta as kith_gateway_lifecycle_dropped_total. A Python-bound callback
 * refused at the worker because the interpreter is finalizing is not
 * counted: the process is exiting, and there is nothing left to notify.
 *
 * @param gateway   Gateway handle. NULL is an error.
 * @param out_drops Receives the monotonic dropped-notification count.
 * @return          0 on success, negative kith_error on failure:
 *                  - -KITH_EINVAL if @p gateway or @p out_drops is NULL.
 * @thread_safety safe — the counter is atomic; readable from any thread.
 *                Incremented on the reactor thread at drop time.
 * @ownership caller — @p out_drops is the caller's output storage.
 */
[[nodiscard]] KITH_API int kith_gateway_lifecycle_drops(const kith_gateway_t *gateway,
                                                        uint64_t *out_drops);

/**
 * Report the count of session window adds that failed against transient
 * capacity: the cell's cache stripe or the gateway's fabric interest set
 * had no free slot (-KITH_ENOMEM). The churn that saturates the shared
 * tables also frees slots, so a nonzero reading is the read-the-gauges-
 * first flood signal, not by itself a saturation verdict — a failing add
 * is retained and retried by the tick pass (see
 * @c kith_gateway_session_window_add). Other failure codes pass uncounted
 * (-KITH_EINVAL is a caller error; -KITH_ESTATE names a stripe mutex that
 * failed at gateway create). Read against
 * @c kith_gateway_window_retry_adds and
 * @c kith_gateway_window_retries_pending: failures climbing with retries
 * landing and pending draining is a flood that is healing, failures with
 * no landings is capacity that is not freeing. The composition root
 * records the delta as kith_gateway_window_add_failures_total.
 *
 * @param gateway      Gateway handle. NULL is an error.
 * @param out_failures Receives the monotonic failure count on success.
 * @return             0 on success, negative kith_error on failure:
 *                     - -KITH_EINVAL if @p gateway or @p out_failures is
 *                       NULL.
 * @thread_safety safe — the counter is atomic; readable from any thread.
 *                Incremented at the failing add on the calling thread.
 * @ownership caller — @p out_failures is the caller's output storage.
 */
[[nodiscard]] KITH_API int kith_gateway_window_add_failures(const kith_gateway_t *gateway,
                                                            uint64_t *out_failures);

/**
 * Report the count of retained window adds the tick pass's retry landed
 * after capacity freed. Read against
 * @c kith_gateway_window_add_failures and
 * @c kith_gateway_window_retries_pending: landings with a draining
 * pending gauge are the healing half of the flood story. The composition
 * root records the delta as kith_gateway_window_retry_adds_total.
 *
 * @param gateway       Gateway handle. NULL is an error.
 * @param out_retry_adds Receives the monotonic landing count on success.
 * @return              0 on success, negative kith_error on failure:
 *                      - -KITH_EINVAL if @p gateway or @p out_retry_adds
 *                        is NULL.
 * @thread_safety safe — the counter is atomic; readable from any thread.
 *                Incremented on the reactor thread at landing.
 * @ownership caller — @p out_retry_adds is the caller's output storage.
 */
[[nodiscard]] KITH_API int kith_gateway_window_retry_adds(const kith_gateway_t *gateway,
                                                          uint64_t *out_retry_adds);

/**
 * Report the current depth of the window-add retry queue: the retained
 * adds across all sessions waiting for a free slot. A gauge, not a
 * counter — read it as the instant's queue depth. Pegged at its
 * per-session ceiling with no landings is the permanent-geometry
 * signature (capacity never frees; widen @c cache_bucket_count or the
 * cell geometry); rising and draining is the transient the retry heals.
 * The composition root records it as
 * kith_gateway_window_retries_pending.
 *
 * @param gateway     Gateway handle. NULL is an error.
 * @param out_pending Receives the queue depth on success.
 * @return            0 on success, negative kith_error on failure:
 *                    - -KITH_EINVAL if @p gateway or @p out_pending is
 *                      NULL.
 * @thread_safety safe — the gauge is atomic; readable from any thread.
 *                Updated at the window-lock-held retention, landing, and
 *                cancel sites.
 * @ownership caller — @p out_pending is the caller's output storage.
 */
[[nodiscard]] KITH_API int kith_gateway_window_retries_pending(const kith_gateway_t *gateway,
                                                               uint64_t *out_pending);

/**
 * Report the count of sessions bound to an actor while tracking no
 * subscription window: the seed-failure signature ("subscribed to
 * nothing" — no cell, no view, no frames). A gauge recomputed on the
 * reactor thread during each tick's session pass; the reading is exact as
 * of the last pass. A healthy population reads zero: the populate step
 * (see @c kith_gateway_session_populate) binds and seeds under one hold
 * of the session's window mutex, so the state is reachable only as a seed
 * failure — every cell retained, awaiting the retry pass — or through a
 * bind-only populate. A session legitimately alone in empty cells is not
 * counted (its window is healthy — the composer excludes the
 * subscriber's own actor, so a zero-delivered predicate would misread
 * that state). The composition root records it as
 * kith_gateway_sessions_without_cells.
 *
 * @param gateway      Gateway handle. NULL is an error.
 * @param out_sessions Receives the census on success.
 * @return             0 on success, negative kith_error on failure:
 *                     - -KITH_EINVAL if @p gateway or @p out_sessions is
 *                       NULL.
 * @thread_safety safe — the gauge is atomic; readable from any thread.
 *                Written on the reactor thread at the pass boundary.
 * @ownership caller — @p out_sessions is the caller's output storage.
 */
[[nodiscard]] KITH_API int kith_gateway_sessions_without_cells(const kith_gateway_t *gateway,
                                                               uint64_t *out_sessions);

/**
 * Subscribe to a cell on the fabric and cache its snapshot. The
 * subscription is refcounted: if the cell is already subscribed (by
 * another local subscriber's subscription window), the refcount is
 * incremented and no new fabric subscription is created. A newly created
 * entry snapshots the cell's current artifacts synchronously, so the first
 * composition after subscribe resolves its locator without waiting for a
 * refresh; a refcount-hit entry holding no artifacts yet is reconciled the
 * same way. Population reads current sim truth for the cell, so a caller
 * that publishes its artifact to the sim before subscribing is covered on
 * this call; publishing after subscribing defers visibility to the
 * next product drain. Product metadata (@c authority_epoch,
 * @c latest_publish_seq, @c actor_count, @c refreshed_at_ms) reaches
 * entries through drained product headers and arrives with the next
 * refresh after a publish. If an initial snapshot fails, the entry stays
 * empty until the cell's next subscribe re-reconciles it or a drained
 * product covers it.
 *
 * @param gateway Gateway handle. NULL is an error.
 * @param key     Cell locator. NULL is an error.
 * @return        0 on success, negative kith_error on failure:
 *                - -KITH_EINVAL if @p gateway or @p key is NULL,
 *                - -KITH_ENOMEM on allocation failure.
 * @thread_safety safe — the target cell's stripe lock serializes subscribe,
 *                unsubscribe, and refresh against the shared cache.
 * @ownership caller — @p key is borrowed for the call only.
 */
[[nodiscard]] KITH_API int kith_gateway_subscribe(kith_gateway_t *gateway,
                                                  const kith_fabric_cell_key_t *key);

/**
 * Unsubscribe a cell. The refcount is decremented; when it reaches zero,
 * the cell is unsubscribed on the fabric and the cache entry is evicted.
 * Idempotent: unsubscribing a cell with no cache entry returns 0.
 *
 * @param gateway Gateway handle. NULL is an error.
 * @param key     Cell locator. NULL is an error.
 * @return        0 on success (even if the cell was not subscribed),
 *                - -KITH_EINVAL if @p gateway or @p key is NULL.
 * @thread_safety safe — the target cell's stripe lock serializes
 *                subscribe, unsubscribe, and refresh against the shared
 *                cache.
 * @ownership caller — @p key is borrowed for the call only.
 */
[[nodiscard]] KITH_API int kith_gateway_unsubscribe(kith_gateway_t *gateway,
                                                    const kith_fabric_cell_key_t *key);

/**
 * Refresh the shared cell cache. Drains pending cell product headers from
 * the fabric subscription (via @c kith_fabric_drain) and re-snapshots
 * cells whose products changed since the last refresh. Cells with no
 * pending changes retain their cached snapshot. The refresh is gated by
 * the configured refresh interval: calls within the interval are no-ops.
 *
 * @param gateway Gateway handle. NULL is an error.
 * @param now_ms  Current monotonic time in milliseconds (from
 *                @c kith_reactor_now_ms or @c clock_gettime).
 * @return        0 on success (including interval-gated no-op),
 *                negative kith_error on failure:
 *                - -KITH_EINVAL if @p gateway is NULL.
 * @thread_safety unsafe — call from the reactor thread.
 * @ownership caller — @p gateway is borrowed for the call only.
 */
[[nodiscard]] KITH_API int kith_gateway_cache_refresh(kith_gateway_t *gateway, uint64_t now_ms);

/**
 * Snapshot one cached cell. The cell is located by @p key; the snapshot
 * reflects the cell's state as of the last @c kith_gateway_cache_refresh.
 *
 * @param gateway Gateway handle. NULL is an error.
 * @param key     Cell locator. NULL is an error.
 * @param out     Receives the cell snapshot on success.
 * @return        0 on success, negative kith_error on failure:
 *                - -KITH_EINVAL if @p gateway, @p key, or @p out is NULL,
 *                - -KITH_ENOENT if the cell is not cached.
 * @thread_safety unsafe.
 * @ownership caller — @p out is the caller's output storage.
 */
[[nodiscard]] KITH_API int kith_gateway_cache_snapshot_cell(const kith_gateway_t *gateway,
                                                            const kith_fabric_cell_key_t *key,
                                                            kith_gateway_cell_snapshot_t *out);

/**
 * Copy shared cell-cache statistics into @p out_stats.
 *
 * @param gateway   Gateway handle. NULL is an error.
 * @param out_stats Receives the statistics on success.
 * @return          0 on success, negative kith_error on failure:
 *                  - -KITH_EINVAL if @p gateway or @p out_stats is NULL.
 * @thread_safety unsafe.
 * @ownership caller — @p out_stats is the caller's output storage.
 */
[[nodiscard]] KITH_API int kith_gateway_cache_stats(const kith_gateway_t *gateway,
                                                    kith_gateway_cache_stats_t *out_stats);

/**
 * Copy reactor-path phase statistics into @p out_stats. The fields are
 * monotonic cumulative totals (nanoseconds in each tick phase, dispatch
 * counts, and the self-echo coverage counters) read atomically, so a caller
 * on any thread observes a
 * coherent-enough snapshot for rate computation. The composition root
 * samples this per tick and publishes the deltas as counters.
 *
 * @param gateway   Gateway handle. NULL is an error.
 * @param out_stats Receives the statistics on success.
 * @return          0 on success, negative kith_error on failure:
 *                  - -KITH_EINVAL if @p gateway or @p out_stats is NULL.
 * @thread_safety safe — each field is read as a relaxed atomic load; the
 *                totals are written only on the reactor thread.
 * @ownership caller — @p out_stats is the caller's output storage.
 */
[[nodiscard]] KITH_API int kith_gateway_phase_stats(const kith_gateway_t *gateway,
                                                    kith_gateway_phase_stats_t *out_stats);

/**
 * Copy view-compose sub-phase statistics into @p out_stats. The fields are
 * monotonic cumulative totals (nanoseconds per compose sub-phase) read
 * atomically, so a caller on any thread observes a coherent-enough snapshot
 * for rate computation. The composition root samples this per tick and
 * records the deltas as counters; the sum of
 * the fields bounds the compose phase from below (see the struct doc).
 *
 * @param gateway   Gateway handle. NULL is an error.
 * @param out_stats Receives the statistics on success.
 * @return          0 on success, negative kith_error on failure:
 *                  - -KITH_EINVAL if @p gateway or @p out_stats is NULL.
 * @thread_safety safe — each field is read as a relaxed atomic load; the
 *                totals are written only on the reactor thread.
 * @ownership caller — @p out_stats is the caller's output storage.
 */
[[nodiscard]] KITH_API int kith_gateway_compose_stats(const kith_gateway_t *gateway,
                                                      kith_gateway_compose_stats_t *out_stats);

/**
 * Read the monotonic count of view compositions skipped because nothing
 * relevant changed. A skip occurs when @c kith_gateway_view_refresh finds,
 * for a session that already has a composed view set, that the session's
 * subscription window, bound actor id, view budget, and every windowed
 * cell's cached content are unchanged since that session's last full
 * composition; the retained subject set is already exactly what a
 * recomposition would produce, so the composer keeps it and counts the
 * skip. The counter is monotonic and never reset. The composition root
 * records its delta as @c kith_gateway_compose_skips_total.
 *
 * @param gateway   Gateway handle. NULL is an error.
 * @param out_skips Receives the monotonic skip count on success.
 * @return          0 on success, negative kith_error on failure:
 *                  - -KITH_EINVAL if @p gateway or @p out_skips is NULL.
 * @thread_safety safe — the counter is atomic; readable from any thread.
 *                Incremented on the reactor thread at skip time.
 * @ownership caller — @p out_skips is the caller's output storage.
 */
[[nodiscard]] KITH_API int kith_gateway_compose_skips(const kith_gateway_t *gateway,
                                                      uint64_t *out_skips);

/**
 * Read the monotonic count of view compositions deferred by the per-tick
 * compose budget. A deferral occurs when @c kith_gateway_tick has consumed
 * @c kith_gateway_params_t.compose_budget_us within a tick's composition
 * pass and a session that already holds a composed view set would otherwise
 * recompose: the session keeps its retained subject set for that tick
 * (delivery is unaffected) and the pass's rotating start index serves it
 * first on a subsequent tick. Sessions without a retained view set are never
 * deferred. The counter is monotonic and never reset. The composition root
 * records its delta as @c kith_gateway_compose_deferrals_total.
 * Alongside the skip counter it separates
 * "nothing changed" from "the tick ran out of budget", which is how a run
 * distinguishes a clean world from an overloaded one.
 *
 * @param gateway        Gateway handle. NULL is an error.
 * @param out_deferrals  Receives the monotonic deferral count on success.
 * @return               0 on success, negative kith_error on failure:
 *                       - -KITH_EINVAL if @p gateway or @p out_deferrals
 *                         is NULL.
 * @thread_safety safe — the counter is atomic; readable from any thread.
 *                Incremented on the reactor thread at deferral time.
 * @ownership caller — @p out_deferrals is the caller's output storage.
 */
[[nodiscard]] KITH_API int kith_gateway_compose_deferrals(const kith_gateway_t *gateway,
                                                          uint64_t *out_deferrals);

/**
 * Read the monotonic count of view compositions that failed to locate the
 * subscriber inside its own subscription window. A locate failure occurs
 * when @c kith_gateway_view_refresh's scan phase finds no cached cell in
 * the session's window holding the session's bound actor id: the composer
 * returns -KITH_ESTATE for that session and it delivers nothing this tick.
 * A nonzero reading marks a session whose window does not track its bound
 * actor's cell: a subscription-window ownership defect (a window left
 * seeded at the login position while its actor moved away is the
 * signature) or window adds failing against a full cache stripe or a
 * full fabric interest set (see @c kith_gateway_session_window_add and
 * the getting-started guide's window capacity section). The counter is
 * monotonic and never reset. The composition root records its delta as
 * @c kith_gateway_view_locate_failures_total; alongside the deferral
 * counter it separates
 * "the tick ran out of budget" from "this session cannot be served further".
 *
 * @param gateway          Gateway handle. NULL is an error.
 * @param out_locate_failures  Receives the monotonic failure count on
 *                             success.
 * @return                 0 on success, negative kith_error on failure:
 *                         - -KITH_EINVAL if @p gateway or
 *                           @p out_locate_failures is NULL.
 * @thread_safety safe — the counter is atomic; readable from any thread.
 *                Incremented on the reactor thread at failure time.
 * @ownership caller — @p out_locate_failures is the caller's output storage.
 */
[[nodiscard]] KITH_API int kith_gateway_view_locate_failures(const kith_gateway_t *gateway,
                                                             uint64_t *out_locate_failures);

/**
 * Read the delivery executor's statistics. The counters decompose the
 * executor's per-session cadence behavior: @c inflight_skips_total counts
 * the per-session deliver passes skipped because the session's
 * previous deliver was still in flight, and the wait-budget fields
 * decompose the compose-wait causes. With no executor
 * configured (@c delivery_worker_count == 0 at creation) the call fails
 * with -KITH_ESTATE: there are no executor counters to read, and the
 * inline path reports its behavior through the phase and deferral
 * counters instead.
 *
 * @param gateway  Gateway handle. NULL is an error.
 * @param out_stats Receives the statistics on success.
 * @return          0 on success, negative kith_error on failure:
 *                  - -KITH_EINVAL if @p gateway or @p out_stats is NULL,
 *                  - -KITH_ESTATE if no delivery executor is configured.
 * @thread_safety safe — the counters are atomic; readable from any thread.
 * @ownership caller — @p out_stats is the caller's output storage.
 */
[[nodiscard]] KITH_API int
kith_gateway_delivery_executor_stats(const kith_gateway_t *gateway,
                                     kith_gateway_delivery_executor_stats_t *out_stats);

/**
 * Read the gateway's cumulative delivery totals. Unlike the executor
 * statistics these accumulate on both delivery paths — inline and
 * executor — because the fold lives inside
 * @c kith_gateway_deliver itself, so they are meaningful on every
 * configuration and answer the backpressure question directly:
 * @c dropped is the enqueue-side loss the selection metrics must be read
 * against.
 *
 * @param gateway     Gateway handle. NULL is an error.
 * @param out_totals  Receives the cumulative totals on success.
 * @return            0 on success, negative kith_error on failure:
 *                    - -KITH_EINVAL if @p gateway or @p out_totals is
 *                      NULL.
 * @thread_safety safe — the counters are atomic; readable from any
 *                thread. Folded relaxed on whichever thread executed the
 *                deliver call, so a just-returned deliver may not be
 *                visible yet (same one-tick skew as the deliver phase
 *                accumulator under the executor).
 * @ownership caller — @p out_totals is the caller's output storage.
 */
[[nodiscard]] KITH_API int kith_gateway_delivery_totals(const kith_gateway_t *gateway,
                                                        kith_gateway_delivery_totals_t *out_totals);

/**
 * Read one session's cumulative delivery totals: the frames enqueued,
 * dropped by output backpressure, membership-event frames enqueued, and
 * subjects suppressed by change suppression across every deliver call of
 * that session's lifetime — the per-session delivery-health signal.
 * Per-session sums partition the gateway totals (see
 * @c kith_gateway_delivery_totals): every frame counted on the gateway
 * appears on exactly one session, so the per-session read is the
 * session-scoped half of the same story. The session identity record
 * (@c kith_gateway_session_info) stays identity-only; delivery health
 * rides this accessor.
 *
 * @param session     Session handle. NULL is an error.
 * @param out_totals  Receives the session's cumulative totals on success.
 * @return            0 on success, negative kith_error on failure:
 *                    - -KITH_EINVAL if @p session or @p out_totals is
 *                      NULL.
 * @thread_safety safe — the counters are atomic; readable from any
 *                thread while the session reference lives. Folded
 *                relaxed on whichever thread executed the deliver call,
 *                so a just-returned deliver may not be visible yet (same
 *                one-tick skew as the deliver phase accumulator under
 *                the executor).
 * @ownership caller — @p out_totals is the caller's output storage.
 */
[[nodiscard]] KITH_API int
kith_gateway_session_delivery_totals(const kith_gateway_session_t *session,
                                     kith_gateway_delivery_totals_t *out_totals);

/**
 * Read the gateway's cumulative view-composition population totals. The
 * sums and the high-watermarks are the gateway-wide aggregate of the
 * per-session view metadata: selection density (selected/candidate) and
 * candidate-cap binding, which the per-session snapshot alone cannot
 * show at population scale. @p visits counts delivery-pass visits of
 * sessions holding a view, including sessions whose executor submission
 * was subsequently skipped — see the struct documentation for the exact
 * denominator contract.
 *
 * @param gateway     Gateway handle. NULL is an error.
 * @param out_totals  Receives the cumulative totals on success.
 * @return            0 on success, negative kith_error on failure:
 *                    - -KITH_EINVAL if @p gateway or @p out_totals is
 *                      NULL.
 * @thread_safety safe — the counters are atomic; readable from any
 *                thread. Accumulated relaxed on the reactor thread at
 *                each visit.
 * @ownership caller — @p out_totals is the caller's output storage.
 */
[[nodiscard]] KITH_API int kith_gateway_view_totals(const kith_gateway_t *gateway,
                                                    kith_gateway_view_totals_t *out_totals);

/**
 * Compose or refresh a subscriber's view set from the shared cache. Collects
 * candidate actors from the cells near the subscriber's position (looked up
 * by the session's bound actor id), scores them by squared distance,
 * classifies them by subject class, selects a representation tier per
 * subject, and bounds the total to the configured maximum. Sticky
 * continuity preserves prior-view members that still qualify.
 *
 * The refresh is gated by the configured refresh interval: calls within
 * the interval are no-ops (the prior view set is retained). The session
 * must have a bound actor id (@c kith_gateway_session_bind_actor);
 * otherwise -KITH_ESTATE is returned.
 *
 * Beyond the interval gate, a session whose composition inputs are all
 * unchanged since its last full composition — the subscription window, the
 * bound actor id, the view budget, and every windowed cell's cached
 * content — keeps its retained subject set and skips the recomposition
 * (counted by @c kith_gateway_compose_skips). A skipped refresh is still a
 * 0 return; the view metadata continues to describe the last full
 * composition.
 *
 * @param gateway Gateway handle. NULL is an error.
 * @param session Session handle. NULL is an error.
 * @param now_ms  Current monotonic time in milliseconds.
 * @return        0 on success (including interval-gated no-op),
 *                negative kith_error on failure:
 *                - -KITH_EINVAL if @p gateway or @p session is NULL,
 *                - -KITH_ESTATE if the session has no bound actor id,
 *                - -KITH_ENOMEM on allocation failure.
 * @thread_safety unsafe — call from the reactor thread.
 * @ownership caller — @p gateway and @p session are borrowed for the call only.
 */
[[nodiscard]] KITH_API int kith_gateway_view_refresh(kith_gateway_t *gateway,
                                                     kith_gateway_session_t *session,
                                                     uint64_t now_ms);

/**
 * Snapshot a subscriber's composed view set. Copies the view metadata into
 * @p out_view and the subject array into @p out_subjects (up to @p max).
 * The subjects are the actors selected into the view set by the last
 * @c kith_gateway_view_refresh, with their positions, velocities,
 * selected representation tier, and classification.
 *
 * @param gateway      Gateway handle. NULL is an error.
 * @param session      Session handle. NULL is an error.
 * @param out_view     Receives the view metadata on success.
 * @param out_subjects Output buffer for subjects. May be NULL when
 *                     @p max is 0 (only counts).
 * @param max          Maximum number of subjects to copy.
 * @param out_count    Receives the number of subjects copied into
 *                     @p out_subjects. May be NULL.
 * @return             0 on success, negative kith_error on failure:
 *                     - -KITH_EINVAL if @p gateway, @p session, or
 *                       @p out_view is NULL.
 * @thread_safety unsafe.
 * @ownership caller — @p out_view and @p out_subjects are the caller's
 *           output storage.
 */
[[nodiscard]] KITH_API int kith_gateway_view_snapshot(const kith_gateway_t *gateway,
                                                      const kith_gateway_session_t *session,
                                                      kith_gateway_view_snapshot_t *out_view,
                                                      kith_gateway_view_subject_t *out_subjects,
                                                      size_t max,
                                                      size_t *out_count);

/**
 * Encode @p payload as one frame of @p type_id on the gateway's proto
 * handle and enqueue it on @p conn. The support surface delivery
 * strategies use to place frames on a session's connection without
 * touching the borrowed net and proto handles directly.
 *
 * On output backpressure (the connection's high watermark) or allocation
 * failure the frame is released and the error is returned; the caller
 * decides retry semantics. A dropped frame leaves any caller-side send
 * ledger untouched (the transactional-commit rule).
 *
 * The encode-and-enqueue path reads only create-fixed gateway and proto
 * state plus mutex-protected pools and queues; the delivery executor
 * exercises it from its own threads whenever the deliver pass runs there.
 * What the caller must supply is the connection's lifetime: a reference
 * that pins @p conn across the call.
 *
 * @param gateway     Gateway handle supplying the proto handle. NULL is an
 *                    error.
 * @param conn        Connection to enqueue on. NULL is an error.
 * @param type_id     Registered proto message type id for the frame.
 * @param payload     Frame payload bytes. NULL when @p payload_len is 0.
 * @param payload_len Payload length in bytes.
 * @return            0 on success, negative kith_error on failure:
 *                    - -KITH_EINVAL if @p gateway or @p conn is NULL,
 *                    - -KITH_EOVERFLOW if @p payload_len exceeds
 *                      UINT32_MAX,
 *                    - -KITH_ESTATE if @p conn is already closed at the
 *                      enqueue's check; a close concurrent with the call
 *                      can still let the frame enqueue (returning 0) —
 *                      that frame is never written and is released when
 *                      the connection is freed,
 *                    - -KITH_EPROTO if the payload does not encode,
 *                    - -KITH_ENOMEM if the frame cannot be allocated,
 *                    - -KITH_EAGAIN on output backpressure.
 * @thread_safety safe-if (the caller holds a live reference to the session
 *                bound to @p conn — the dispatch reference a
 *                pool-dispatched handler runs under, or a session
 *                reference the caller already holds — or a connection
 *                reference on @p conn via @c kith_net_conn_acquire, or
 *                calls from the reactor thread). A conn not pinned by a
 *                session or connection reference may be released
 *                concurrently by session teardown; calling from any other
 *                context is unsafe.
 * @ownership caller — @p payload is copied into the enqueued frame; the
 *           caller retains ownership.
 */
[[nodiscard]] KITH_API int kith_gateway_deliver_frame(kith_gateway_t *gateway,
                                                      kith_net_conn_t *conn,
                                                      uint16_t type_id,
                                                      const void *payload,
                                                      size_t payload_len);

/**
 * Submit a cell-scoped broadcast: on the gateway's next tick pass the
 * reactor delivers @p payload as one frame of @p type_id to every session
 * whose subscription window covers @p cell. The recipient set is derived,
 * never submitted — window membership is the gateway-side image of the
 * cell's subscription set — so a broadcast reaches tier-suppressed
 * subscribers too and no caller-side recipient roster forms.
 *
 * The request rides a bounded FIFO queue: the payload is copied at submit
 * and checked against the proto handle's maximum frame payload, so the
 * queue never holds a request that cannot encode, and a submit past the
 * queue's depth is refused with -KITH_EAGAIN and counted. Delivery is one
 * tick deep and best-effort: the frame is encoded once and fanned through
 * the connection queues on a shared refcounted frame; a recipient whose
 * connection queue is full or whose connection has closed is counted as a
 * drop, a request with zero covering recipients completes silently, and
 * requests pending at gateway teardown are counted as drops. The drain
 * runs ahead of the tick's session pass, so a broadcast frame rides a
 * connection ahead of that pass's composed frames — the same ordering
 * edge membership events ride — and is not subject to the composition
 * budget.
 *
 * The frame is an ordinary message frame — caller type id, caller
 * payload, no correlation trailer — so both peers register @p type_id out
 * of band and the payload format is the game's own. The composed
 * replication stream, its presets, and the replication record are
 * untouched.
 *
 * @param gateway     Gateway handle. NULL is an error.
 * @param cell        Cell the broadcast is scoped to. NULL is an error.
 * @param type_id     Message type id for the delivered frame.
 * @param payload     Caller bytes copied at submit. NULL when
 *                    @p payload_len is 0.
 * @param payload_len Payload length in bytes.
 * @return            0 on success, negative kith_error on failure:
 *                    - -KITH_EINVAL if @p gateway or @p cell is NULL, or
 *                      @p payload is NULL with a non-zero @p payload_len,
 *                    - -KITH_EOVERFLOW if @p payload_len exceeds
 *                      UINT32_MAX,
 *                    - -KITH_EPROTO if the payload does not encode under
 *                      the proto handle's maximum frame payload,
 *                    - -KITH_EAGAIN if the request queue is full (counted
 *                      via @c kith_gateway_broadcast_refusals),
 *                    - -KITH_ESTATE if the gateway is shutting down.
 * @thread_safety safe — the request queue is internally synchronized;
 *                submission from a worker-pool handler is expected use.
 * @ownership caller — @p payload is copied at submit; the caller retains
 *           ownership.
 */
[[nodiscard]] KITH_API int kith_gateway_broadcast_cell(kith_gateway_t *gateway,
                                                       const kith_fabric_cell_key_t *cell,
                                                       uint16_t type_id,
                                                       const void *payload,
                                                       size_t payload_len);

/**
 * Read the monotonic count of broadcast submits refused because the
 * request queue was full (-KITH_EAGAIN from
 * @c kith_gateway_broadcast_cell). The count is the saturation signal for
 * the broadcast surface: bursts past the queue's depth are refused rather
 * than buffered, and a game learning from this counter paces its event
 * fanout or widens its cell scoping. The counter is monotonic and never
 * reset. The composition root records its delta as
 * kith_gateway_broadcast_refused_total.
 *
 * @param gateway      Gateway handle. NULL is an error.
 * @param out_refusals Receives the monotonic refusal count on success.
 * @return             0 on success, negative kith_error on failure:
 *                     - -KITH_EINVAL if @p gateway or @p out_refusals is
 *                       NULL.
 * @thread_safety safe — the counter is atomic; readable from any thread.
 * @ownership caller — @p out_refusals is the caller's output storage.
 */
[[nodiscard]] KITH_API int kith_gateway_broadcast_refusals(const kith_gateway_t *gateway,
                                                           uint64_t *out_refusals);

/**
 * Read the monotonic count of broadcast deliveries not completed:
 * recipients whose connection queue was full or whose connection had
 * closed, requests whose frame could not encode (counted once), and
 * requests still pending at gateway teardown (one each). A nonzero
 * reading means cell-local event fanout lost recipients; a request with
 * zero covering recipients is a complete no-op and never counts. The
 * counter is monotonic and never reset. The composition root records its
 * delta as kith_gateway_broadcast_dropped_total.
 *
 * @param gateway   Gateway handle. NULL is an error.
 * @param out_drops Receives the monotonic drop count on success.
 * @return          0 on success, negative kith_error on failure:
 *                  - -KITH_EINVAL if @p gateway or @p out_drops is NULL.
 * @thread_safety safe — the counter is atomic; readable from any thread.
 * @ownership caller — @p out_drops is the caller's output storage.
 */
[[nodiscard]] KITH_API int kith_gateway_broadcast_drops(const kith_gateway_t *gateway,
                                                        uint64_t *out_drops);

/**
 * Deliver the session's composed view set to its connection. Delegates to
 * the delivery strategy bound at session creation
 * (@c kith_gateway_params_t.delivery_strategy, default @c full). Under the
 * factory-default @c full strategy: when @c replication_batch_type_id is
 * non-zero, serializes the full view set into one multi-subject frame
 * (4-byte count header + N subject records) and enqueues it as a single
 * frame; otherwise encodes each subject as a separate actor state frame
 * (using the configured @c replication_type_id) and enqueues each on the
 * session's connection (via @c kith_net_conn_enqueue). Frames rejected by
 * output backpressure are counted as dropped.
 *
 * The reactor flushes the connection's output queue separately via
 * @c kith_net_conn_write; this function only composes and enqueues.
 *
 * @param gateway   Gateway handle. NULL is an error.
 * @param session   Session handle. NULL is an error.
 * @param now_ms    Current monotonic time in milliseconds.
 * @param out_stats Receives delivery counts on success. May be NULL.
 * @return          0 on success, negative kith_error on failure:
 *                  - -KITH_EINVAL if @p gateway or @p session is NULL,
 *                  - -KITH_ESTATE if both @c replication_type_id and
 *                    @c replication_batch_type_id are 0 (delivery encoding
 *                    disabled) or the session has no composed view set.
 * @thread_safety unsafe — call from the reactor thread. The gateway's own
 *                tick-driven deliveries run on executor threads when the
 *                delivery executor is configured; direct calls
 *                stay reactor-thread-only and never concurrently with an
 *                in-flight executor job for the same session.
 * @ownership caller — @p out_stats is the caller's output storage.
 */
[[nodiscard]] KITH_API int kith_gateway_deliver(kith_gateway_t *gateway,
                                                kith_gateway_session_t *session,
                                                uint64_t now_ms,
                                                kith_gateway_delivery_stats_t *out_stats);

/**
 * Drive one gateway tick: refresh the shared cell cache against the fabric,
 * drain pending cell-scoped broadcasts (see
 * @c kith_gateway_broadcast_cell), then for every bound session refresh
 * its view set and deliver the composed view as replication frames on its
 * connection. This is the per-tick entry the reactor calls once per tick;
 * it folds cache maintenance, event fanout, view composition, and
 * delivery into one reactor-thread pass so a composition root does not
 * drive them separately.
 *
 * The cache refresh is gated by the configured cache refresh interval;
 * each session's view refresh is gated by its configured view refresh
 * interval. Sessions without a bound actor id are skipped (their view
 * refresh returns -KITH_ESTATE, which is not an error here). A per-session
 * view or delivery failure is recorded and the pass continues so one bad
 * session does not stop delivery to the rest; the first recorded error is
 * returned.
 *
 * The composition pass runs under a soft per-tick budget
 * (@c kith_gateway_params_t.compose_budget_us): once consumed, sessions
 * that already hold a composed view set defer recomposition to a subsequent
 * tick (counted by @c kith_gateway_compose_deferrals) while still receiving
 * their retained subject set this tick — delivery cadence per session is
 * unchanged when delivery runs inline or keeps up with the executor, and
 * a rotating start index serves deferred sessions first on
 * the next tick. The pass's first eligible composition always runs;
 * sessions without a retained view set compose regardless. Under the
 * delivery executor, a session whose previous deliver is still in flight
 * composes only within the per-pass wait budget
 * (@c kith_gateway_params_t.delivery_wait_budget_us); an expired budget
 * skips that session's composition and its delivery for the tick — a
 * bounded cadence gap, staleness bounded by the 1 s max-gap backstop and
 * the gap rate visible through @c kith_gateway_delivery_executor_stats.
 *
 * @param gateway Gateway handle. NULL is an error.
 * @param now_ms  Current monotonic time in milliseconds.
 * @return        0 on success, negative kith_error on failure:
 *                - -KITH_EINVAL if @p gateway is NULL,
 *                - the first per-session non-ESTATE failure otherwise.
 * @thread_safety unsafe — call from the reactor thread.
 * @ownership caller — @p gateway is borrowed for the call only.
 */
[[nodiscard]] KITH_API int kith_gateway_tick(kith_gateway_t *gateway, uint64_t now_ms);

/** @} */

#endif /* KITH_GATEWAY_GATEWAY_H */
