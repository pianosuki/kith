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
 * enqueues them on the session's connection. The message type id used
 * for replication frames is configured at create time
 * (@c kith_gateway_params_t.replication_type_id). The reactor flushes
 * the connection's output queue separately via @c kith_net_conn_write;
 * the gateway only composes and enqueues.
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
 * table, cache, and view composer are not synchronized. The handler
 * table is serialized by a per-handle mutex so concurrent register and
 * dispatch calls do not interleave.
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
    /** Default max subjects per subscriber's view set (ADR-0005 budget). */
    KITH_GATEWAY_DEFAULT_VIEW_MAX_SUBJECTS = 512u,
    /** Default view refresh interval in milliseconds. */
    KITH_GATEWAY_DEFAULT_VIEW_REFRESH_MS = 100u,
    /** Default cache refresh interval in milliseconds. */
    KITH_GATEWAY_DEFAULT_CACHE_REFRESH_MS = 100u,
    /** Default handler registration table capacity (message type slots). */
    KITH_GATEWAY_DEFAULT_HANDLER_TABLE_SIZE = 256u,
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
    /** Authenticated subscriber session (requires credential verification). */
    KITH_GATEWAY_SESSION_SUBSCRIBER = 0u,
    /** Authenticated application session (requires an OAuth client). */
    KITH_GATEWAY_SESSION_APP = 1u,
    /** Authenticated internal service session. */
    KITH_GATEWAY_SESSION_SERVICE = 2u,
};

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
 * corresponding @c kith_gateway_default value. Additive fields occupy the
 * reserved slots so the layout below stays stable across generations.
 */
struct kith_gateway_params
{
    uint32_t size;
    uint32_t abi_version;

    /** Max simultaneous sessions; 0 selects KITH_GATEWAY_DEFAULT_MAX_SESSIONS. */
    uint32_t max_sessions;
    /** Shared cell-cache hash bucket count; 0 selects the default. */
    uint32_t cache_bucket_count;
    /** Per-subscriber view-store hash bucket count; 0 selects the default. */
    uint32_t view_bucket_count;
    /** Max subjects per subscriber's view set (ADR-0005 budget); 0 → default. */
    uint32_t view_max_subjects;
    /** View refresh interval in ms; 0 selects KITH_GATEWAY_DEFAULT_VIEW_REFRESH_MS. */
    uint32_t view_refresh_interval_ms;
    /** Cache refresh interval in ms; 0 selects KITH_GATEWAY_DEFAULT_CACHE_REFRESH_MS. */
    uint32_t cache_refresh_interval_ms;
    /** Handler table capacity (message type slots); 0 → default. */
    uint32_t handler_table_size;
    /**
     * Message type id used for actor state replication frames produced by
     * @c kith_gateway_deliver. The caller registers this type via
     * @c kith_proto_register_type_id before creating the gateway. 0
     * disables delivery encoding (@c kith_gateway_deliver returns
     * -KITH_ESTATE); the caller composes and enqueues frames itself.
     */
    uint16_t replication_type_id;
    /** Padding for alignment. */
    uint16_t pad;

    void *reserved[8];
};

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

typedef struct kith_gateway_cache_stats kith_gateway_cache_stats_t;

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
    /** Selected representation tier for this subject. */
    kith_fabric_product_level_t level;
    /** Subject classification assigned by the relevance composer. */
    kith_gateway_view_class_t subject_class;
    /** True when retained from the prior view for continuity (no flicker). */
    bool sticky;
    /** True when reduced from a higher tier by the budget bound. */
    bool demoted;
    /** Padding for alignment. */
    uint8_t pad[2];
};

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
    /** Monotonic millisecond timestamp of the last view refresh. */
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
    /** Subjects demoted from a higher tier by the budget bound. */
    uint16_t demoted_selected_count;
    /** Padding for alignment. */
    uint32_t pad;
};

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
    /** Padding for alignment. */
    uint64_t pad;
};

typedef struct kith_gateway_delivery_stats kith_gateway_delivery_stats_t;

/**
 * Message handler callback. Registered via @c kith_gateway_register_handler
 * and invoked by @c kith_gateway_dispatch when a decoded frame of the
 * registered type arrives on a session's connection.
 *
 * The callback runs on the reactor thread (the thread that called
 * @c kith_gateway_dispatch). C handlers may enqueue a response on @p
 * session's connection directly. Python-bound handlers are posted to a
 * worker pool by the caller and do not run on the reactor thread.
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
 * @param out_gateway  Receives the new handle on success.
 * @return             0 on success, negative kith_error on failure:
 *                     - -KITH_EINVAL if @p out_gateway, @p net, @p fabric,
 *                       or @p proto is NULL, or @p params has an
 *                       incompatible abi_version or an undersized size,
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
                                               kith_gateway_t **out_gateway);

/**
 * Release a gateway handle, its session table, shared cell cache, view
 * store, handler table, and internal spatial index. Passing NULL is a
 * no-op. The borrowed net, fabric, and proto handles are not freed (the
 * caller owns them and destroys them separately). Sessions created via
 * @c kith_gateway_session_create are not freed here (the caller owns them
 * and must call @c kith_gateway_session_destroy).
 *
 * @thread_safety unsafe — no session/cache/view/deliver/dispatch may be
 *                in flight on @p gateway when this is called.
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
 * @param out_session Receives the new session on success.
 * @return            0 on success, negative kith_error on failure:
 *                    - -KITH_EINVAL if @p gateway, @p conn, or
 *                      @p out_session is NULL,
 *                    - -KITH_EBUSY if the session table is full,
 *                    - -KITH_ENOMEM on allocation failure.
 * @thread_safety unsafe — the session table is not synchronized.
 * @ownership callee — the caller destroys the session with
 *           kith_gateway_session_destroy.
 */
[[nodiscard]] KITH_API int kith_gateway_session_create(kith_gateway_t *gateway,
                                                       kith_net_conn_t *conn,
                                                       kith_gateway_session_type_t type,
                                                       uint64_t principal_id,
                                                       kith_gateway_session_t **out_session);

/**
 * Destroy a session and release its connection reference. Passing NULL is
 * a no-op. The session is removed from the gateway's session table and
 * its view state is freed. The bound connection is not closed here (the
 * caller or the reactor closes it via @c kith_net_conn_close); the
 * session only releases the reference it acquired at creation.
 *
 * @thread_safety unsafe — no dispatch/deliver/view-refresh may be in
 *                flight on @p session.
 */
KITH_API void kith_gateway_session_destroy(kith_gateway_session_t *session);

/**
 * Copy the session's metadata into @p out_info.
 *
 * @param session  Session handle. NULL is an error.
 * @param out_info Receives the metadata on success.
 * @return         0 on success, negative kith_error on failure:
 *                 - -KITH_EINVAL if @p session or @p out_info is NULL.
 * @thread_safety unsafe.
 * @ownership caller — @p out_info is the caller's output storage.
 */
[[nodiscard]] KITH_API int kith_gateway_session_info(const kith_gateway_session_t *session,
                                                     kith_gateway_session_info_t *out_info);

/**
 * Return the connection the session is bound to, or NULL when @p session
 * is NULL or the connection has been closed. The reactor polls the
 * connection's fd for read/write readiness and calls read/write on
 * readiness.
 *
 * @thread_safety safe — the connection pointer is fixed after session
 *                creation.
 */
KITH_API kith_net_conn_t *kith_gateway_session_conn(const kith_gateway_session_t *session);

/**
 * Bind the session to a subscriber actor identifier. Called after the subscriber
 * selects an actor (the actor id is not known at session creation).
 * The relevance composer uses the bound actor id to locate the subscriber's
 * position in the shared cache and compose the view set around it. Until
 * a binding is set, @c kith_gateway_view_refresh returns -KITH_ESTATE.
 *
 * @param session  Session handle. NULL is an error.
 * @param actor_id Subscriber actor identifier to bind.
 * @return         0 on success, negative kith_error on failure:
 *                 - -KITH_EINVAL if @p session is NULL.
 * @thread_safety unsafe.
 */
[[nodiscard]] KITH_API int kith_gateway_session_bind_actor(kith_gateway_session_t *session,
                                                           uint64_t actor_id);

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
 *                  - -KITH_EINVAL if @p gateway or @p fn is NULL,
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
 */
[[nodiscard]] KITH_API int kith_gateway_unregister_handler(kith_gateway_t *gateway,
                                                           uint16_t msg_type);

/**
 * Dispatch a decoded frame to the handler registered for its type. Looks
 * up the session bound to @p conn, extracts the message type and payload
 * from @p frame, and invokes the registered handler. If no session is
 * bound to @p conn, returns -KITH_ENOENT. If no handler is registered for
 * the frame's type, returns -KITH_ENOENT.
 *
 * The handler runs on the calling thread. C handlers may enqueue a
 * response on @p conn directly. Python-bound handlers are posted to a
 * worker pool by the caller (the caller wraps the handler in a task
 * submitted via @c kith_reactor_submit).
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
 *                  handler is registered for the frame's type id.
 * @thread_safety safe — the handler table is serialized by a per-handle
 *                mutex. The session lookup is not synchronized; call from
 *                the reactor thread that owns the connection.
 * @ownership caller — @p frame is borrowed for the call only.
 */
[[nodiscard]] KITH_API int kith_gateway_dispatch(kith_gateway_t *gateway,
                                                 kith_net_conn_t *conn,
                                                 const kith_proto_frame_t *frame);

/**
 * Subscribe to a cell on the fabric and cache its snapshot. The
 * subscription is refcounted: if the cell is already subscribed (by
 * another local subscriber's subscription window), the refcount is
 * incremented and no new fabric subscription is created. The cell's
 * snapshot is refreshed on the next @c kith_gateway_cache_refresh.
 *
 * @param gateway Gateway handle. NULL is an error.
 * @param key     Cell locator. NULL is an error.
 * @return        0 on success, negative kith_error on failure:
 *                - -KITH_EINVAL if @p gateway or @p key is NULL,
 *                - -KITH_ENOMEM on allocation failure.
 * @thread_safety unsafe — the cache is not synchronized.
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
 * @thread_safety unsafe.
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
 * @param gateway Gateway handle. NULL is an error.
 * @param session Session handle. NULL is an error.
 * @param now_ms  Current monotonic time in milliseconds.
 * @return        0 on success (including interval-gated no-op),
 *                negative kith_error on failure:
 *                - -KITH_EINVAL if @p gateway or @p session is NULL,
 *                - -KITH_ESTATE if the session has no bound actor id,
 *                - -KITH_ENOMEM on allocation failure.
 * @thread_safety unsafe — call from the reactor thread.
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
 * Deliver the session's composed view set to its connection. For each
 * subject in the view set, encodes an actor state frame (using the
 * configured @c replication_type_id and the borrowed proto handle) and
 * enqueues it on the session's connection (via @c kith_net_conn_enqueue).
 * Frames rejected by output backpressure are counted as dropped.
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
 *                  - -KITH_ESTATE if @p replication_type_id is 0 (delivery
 *                    encoding disabled) or the session has no composed
 *                    view set.
 * @thread_safety unsafe — call from the reactor thread.
 * @ownership caller — @p out_stats is the caller's output storage.
 */
[[nodiscard]] KITH_API int kith_gateway_deliver(kith_gateway_t *gateway,
                                                kith_gateway_session_t *session,
                                                uint64_t now_ms,
                                                kith_gateway_delivery_stats_t *out_stats);

#endif /* KITH_GATEWAY_GATEWAY_H */
