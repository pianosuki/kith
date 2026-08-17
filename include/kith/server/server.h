#ifndef KITH_SERVER_SERVER_H
#define KITH_SERVER_SERVER_H

#include <stdint.h>

#include "kith/api.h"
#include "kith/config/config.h"
#include "kith/control/control.h"
#include "kith/db/db.h"
#include "kith/gateway/gateway.h"
#include "kith/sim/sim.h"
#include "kith/types.h"
#include "kith/version.h"

/**
 * Generic MMO server composition root.
 *
 * A server handle is the single wiring point that creates, connects, and
 * drives the five runtime planes (Sim, Fabric, Gateway, Coord, Control)
 * according to a configured topology. It owns every plane handle it creates
 * and releases them in reverse order at destroy time. The handle carries no
 * game-specific concepts: it dispatches via the handler table, loads the
 * configured simulation model, and resolves named queries through the query
 * registry. Game logic lives in user code (Python extensions or C shared
 * libraries) and plugs into the plane contracts the server wires together.
 *
 * @section server_lifecycle Lifecycle
 *
 * The handle moves through four states: @c CREATED (wired but not yet
 * running), @c RUNNING (the tick loop is driving the reactor and stepping
 * the planes), @c DRAINING (shutdown requested, in-flight work completing),
 * and @c STOPPED (the run loop has exited; the handle awaits destruction).
 * @c kith_server_run blocks until shutdown is requested and the drain
 * completes, then returns. @c kith_server_destroy releases the handle and
 * every owned plane handle; it must not be called while @c kith_server_run
 * is in flight.
 *
 * @section server_topology Topology
 *
 * The topology field selects how the planes are wired. In the embedded
 * topology every plane runs in one process, the coord is a no-op stub, and
 * the fabric uses in-memory storage. In the distributed topology the planes
 * may run in the same or in separate processes, connected through the coord
 * bus, and the fabric backing store is selected by configuration. Switching
 * topology is a configuration change, not a code change; a module written
 * against a plane's contract runs unmodified in either topology.
 *
 * @section server_config Configuration
 *
 * The size-versioned @c kith_server_params_t carries the high-level
 * tunables (topology, instance identity, listen endpoint, tick rate, Python
 * worker pool size). A borrowed @c kith_config_t source fills the listen
 * endpoint and tick-rate fields the caller leaves unset: the wiring reads
 * the @c tick_hz, @c listen_port, and @c listen_host keys by name at create
 * time, and an explicit params field wins over the key. A field still unset
 * after both selects a default: @c tick_hz and @c python_worker_count take
 * their @c kith_server_default values, while @c listen_port selects an
 * OS-assigned ephemeral port (the @c KITH_SERVER_DEFAULT_LISTEN_PORT value
 * is a suggested fixed port a caller passes explicitly rather than one the
 * wiring applies automatically). Per-plane internals (bucket counts,
 * thresholds, dwell times, backing store selection) are wiring defaults and
 * are not configuration surface. The actual bound port is read back with
 * @c kith_server_listen_port.
 */

/**
 * @defgroup kith_server Server
 * @{
 */

/**
 * Default configuration values. A @c kith_server_params_t field set to 0
 * selects the corresponding default at create time. The underlying type is
 * fixed.
 */
enum kith_server_default : unsigned int
{
    /** Default simulation tick rate in Hz (config.tick_hz = 0 selects this). */
    KITH_SERVER_DEFAULT_TICK_HZ = 20u,
    /** Suggested gateway listen port. A config listen_port of 0 selects an
     *  OS-assigned ephemeral port; pass this value explicitly to bind it. */
    KITH_SERVER_DEFAULT_LISTEN_PORT = 7777u,
    /** Default Python handler worker pool size (config.python_worker_count = 0
     *  selects this). Size 1 under the GIL; raise under free-threaded Python. */
    KITH_SERVER_DEFAULT_PYTHON_WORKERS = 1u,
};

/**
 * Runtime topology. Selects how the composition root wires the planes. A
 * field set to 0 selects the distributed topology (the default).
 */
enum kith_server_topology : unsigned int
{
    /** Distributed topology: planes scale independently across instances. */
    KITH_SERVER_TOPOLOGY_DISTRIBUTED = 0u,
    /** Embedded topology: every plane runs in one process; coord is a stub. */
    KITH_SERVER_TOPOLOGY_EMBEDDED = 1u,
};

/** Alias of enum kith_server_topology. */
typedef enum kith_server_topology kith_server_topology_t;

/**
 * Server lifecycle state. Observed via @c kith_server_status. The run loop
 * transitions CREATED -> RUNNING -> DRAINING -> STOPPED; @c kith_server_destroy
 * is valid once the handle is STOPPED (or CREATED, when run was never entered).
 */
enum kith_server_status : unsigned int
{
    /** Wired but the run loop has not been entered. */
    KITH_SERVER_STATUS_CREATED = 0u,
    /** The run loop is driving the reactor and stepping the planes. */
    KITH_SERVER_STATUS_RUNNING = 1u,
    /** Shutdown requested; in-flight work is draining. */
    KITH_SERVER_STATUS_DRAINING = 2u,
    /** The run loop has exited; the handle awaits destruction. */
    KITH_SERVER_STATUS_STOPPED = 3u,
};

/** Alias of enum kith_server_status. */
typedef enum kith_server_status kith_server_status_t;

/**
 * Tick handler registration flags. The underlying type is fixed.
 *
 * @c KITH_SERVER_HANDLER_PYTHON marks the callback as a Python-bound ctypes
 * trampoline so the tick dispatch routes it through the server's worker
 * pool instead of invoking it on the reactor thread. A callback
 * registered without the flag is a plain C function pointer and runs inline
 * on the reactor thread.
 */
enum kith_server_handler_flag : unsigned int
{
    /** Default: the callback is a C function pointer and runs inline on the
     *  reactor thread (no pool hop). */
    KITH_SERVER_HANDLER_NONE = 0u,
    /** The callback is a Python-bound ctypes trampoline. It is submitted to
     *  the worker pool attached to the server instead of invoked on the
     *  reactor thread, keeping the reactor thread out of the Python
     *  interpreter. The reactor never runs a Python-bound tick callback
     *  inline: with no pool attached, or when the pool's queue is
     *  exhausted, the callback is dropped and counted via
     *  @c kith_server_tick_drops. */
    KITH_SERVER_HANDLER_PYTHON = 1u,
};

/** Alias of enum kith_server_handler_flag. */
typedef enum kith_server_handler_flag kith_server_handler_flag_t;

/**
 * Per-tick game-logic callback. Invoked once per normal tick with the
 * server's monotonic tick counter @p tick (the tick-boundary time base)
 * and the @p user_data pointer supplied at
 * registration. The callback runs on a worker thread when registered with
 * @c KITH_SERVER_HANDLER_PYTHON and a pool is attached, or inline on the
 * reactor thread for a C callback (no flag).
 *
 * @param tick      The monotonic tick index for this tick (1-based; advances
 *                  once per normal tick, independent of whether a callback
 *                  is registered). The shutdown drain tick does not dispatch
 *                  the callback.
 * @param user_data The caller context supplied at registration.
 */
typedef void (*kith_server_tick_fn)(uint64_t tick, void *user_data);

/**
 * Poll callback for a run loop on the embedding's main thread. Invoked
 * synchronously by the run-loop thread once per tick, before the tick's
 * plane work, at a safe point where re-entering an embedding runtime is
 * permitted (the bounded main-thread exception: a Python facade pumps
 * pending signals here so a main-thread run loop receives signal
 * delivery). The
 * callback carries no game logic and no payload.
 *
 * @param user_data The caller context supplied at registration.
 * @return          0 to continue the run loop, nonzero to request the same
 *                  graceful shutdown @c kith_server_shutdown performs.
 */
typedef int (*kith_server_poll_fn)(void *user_data);

/**
 * Opaque server handle.
 *
 * @ownership callee — created by kith_server_create, destroyed by
 *           kith_server_destroy. Owns every plane handle the wiring creates
 *           (sim, fabric, gateway, coord, control, and the shared
 *           infrastructure handles they borrow). The borrowed @c config
 *           source passed at create time is not owned; the caller destroys
 *           it separately after the server handle is destroyed.
 */
typedef struct kith_server kith_server_t;

/**
 * Server creation parameters. Size-versioned: callers set @p size to
 * sizeof(kith_server_params_t) and @p abi_version to KITH_ABI_VERSION at
 * their compile time; the runtime rejects structs from an incompatible
 * generation or an undersized size. A field set to 0 selects the
 * corresponding @c kith_server_default value. Future additive fields occupy
 * the reserved slots so the layout of the fields below stays stable across
 * generations.
 */
struct kith_server_params
{
    /** Must be sizeof(kith_server_params_t). */
    uint32_t size;
    /** Must be KITH_ABI_VERSION (from kith/version.h). */
    uint32_t abi_version;

    /**
     * Borrowed typed configuration source, or NULL. The wiring reads the
     * tick_hz, listen_port, and listen_host keys by name at create time for
     * fields the caller leaves unset; an explicit params field wins. The
     * snapshot must remain valid until the server handle is destroyed. Not
     * owned by the handle.
     */
    const kith_config_t *config;
    /** Runtime topology; 0 selects the distributed topology. */
    kith_server_topology_t topology;
    /** Local instance ID (0 = embedded single-instance). */
    uint32_t instance_id;
    /**
     * Gateway listen host, or NULL to bind all interfaces. A numeric IPv4/
     * IPv6 address binds that address; a host name is resolved via the system
     * resolver and an unresolvable name fails the create. When NULL, a
     * listen_host key in the config source supplies the bind host. Borrowed
     * for the create call; the wiring passes it to the transport listener.
     */
    const char *listen_host;
    /** Gateway listen port; 0 selects an OS-assigned ephemeral port. */
    uint16_t listen_port;
    /** Simulation tick rate in Hz; 0 selects the default. */
    uint16_t tick_hz;
    /** Python handler worker pool size; 0 selects the default. */
    uint32_t python_worker_count;
    /**
     * Gateway handler-table capacity (number of message-type slots). The
     * gateway dispatches by direct index, so this must exceed the largest
     * message type id the game registers. 0 selects the gateway default
     * (256), which covers framework types but not user types
     * (@c KITH_PROTO_TYPE_USER_BASE = 1000); a game using user types sets
     * this to cover its highest type id.
     */
    uint32_t handler_table_size;
    /**
     * Message type id used for actor state replication frames the gateway
     * encodes via @c kith_gateway_deliver when @c replication_batch_type_id
     * is 0. The caller registers this type on the borrowed proto handle
     * (obtained via @c kith_server_proto) before entering the run loop,
     * alongside the game's other wire types. 0 disables per-subject delivery
     * encoding. When both this and @c replication_batch_type_id are 0, the
     * caller composes and enframes replication itself.
     */
    uint16_t replication_type_id;
    /**
     * Message type id used for multi-subject batch replication frames the
     * gateway encodes via @c kith_gateway_deliver. When non-zero, the gateway
     * packs the session's full composed view-subject set into one frame per
     * refresh instead of one frame per subject. The caller registers this
     * type on the borrowed proto handle before entering the run loop. 0
     * selects per-subject delivery via @c replication_type_id. When non-zero,
     * @c replication_type_id is ignored.
     */
    uint16_t replication_batch_type_id;
    /**
     * Registered delivery strategy name the gateway resolves for every
     * session it creates (for example @c "full" or @c "tiered"), or NULL to
     * select the factory default. Borrowed for the call only; the wiring
     * copies the name at create time. An unknown name fails session
     * creation, not server creation, so a misspelling surfaces on the first
     * client connect.
     */
    const char *delivery_strategy;
    /**
     * Strategy configuration image passed through to the selected delivery
     * strategy, or NULL to select the strategy's documented defaults. The
     * image is a size-versioned struct whose leading @c uint32 declares its
     * own length; its schema is strategy-specific (@c "tiered" reads a
     * @c kith_gateway_tiered_config_t). Borrowed for the call only; the
     * wiring copies the image at create time and the gateway copies it again
     * for its own lifetime.
     */
    const void *delivery_config;
    /**
     * Delivery executor thread count forwarded to the gateway
     * (kith_gateway_params_t.delivery_worker_count). 0 keeps
     * gateway delivery inline on the reactor thread.
     */
    uint32_t delivery_worker_count;
    /**
     * Per-pass compose-wait budget in microseconds forwarded to the gateway
     * (kith_gateway_params_t.delivery_wait_budget_us). 0 selects
     * the gateway default, clamped by the server wiring to half the tick
     * interval when the gateway default would breach the below-one-tick
     * rule (tick rates above 62 Hz); an explicit value passes through
     * unclamped. Consulted only when @p delivery_worker_count is
     * non-zero.
     */
    uint32_t delivery_wait_budget_us;
    /**
     * Disable self-echo coverage stamping. The composition root
     * enables stamping on the gateway it creates by default; setting this
     * flag keeps every replicated record's counter bytes at zero and
     * advances neither stamp counter — the pre-stamping wire shape, for
     * comparability runs.
     */
    bool self_echo_disabled;
    /**
     * Control-plane per-connection response write buffer capacity in bytes,
     * forwarded to the control plane
     * (kith_control_params_t.write_buffer_cap). 0 selects the control
     * plane's default (262144); a route response larger than the capacity
     * answers the canonical counted rejection, so size the capacity for the
     * largest listing a control route returns.
     */
    uint32_t control_write_buffer_cap;
    /**
     * Per-subscriber view-set subject capacity forwarded to the gateway
     * (kith_gateway_params_t.view_max_subjects). 0 selects the gateway
     * default (512); the subscribing subject occupies one slot of the
     * set, and candidates past the budget drop from the delivered
     * individual set — the view candidate and selected high-watermark
     * gauges are the approaching-the-cap evidence. Size the capacity for
     * the densest view a subscriber's window is expected to cover.
     */
    uint32_t view_max_subjects;
    /**
     * View compose-and-deliver interval in milliseconds forwarded to the
     * gateway (kith_gateway_params_t.view_refresh_interval_ms). 0 selects
     * the tick interval — the composition root's derivation, because a
     * refresh interval wider than the tick leaves inputs published inside
     * the gap undelivered until the next refresh. An explicit value
     * passes through unclamped and owns that trade.
     */
    uint32_t view_refresh_interval_ms;
    /**
     * Cell-cache refresh interval in milliseconds forwarded to the gateway
     * (kith_gateway_params_t.cache_refresh_interval_ms). 0 selects the
     * tick interval; an explicit value passes through unclamped.
     */
    uint32_t cache_refresh_interval_ms;

    /** Reserved for future additive fields. Must be zero-filled. */
    void *reserved[8];
};

/** Alias of struct kith_server_params. */
typedef struct kith_server_params kith_server_params_t;

/**
 * Build a server handle from @p config. The wiring creates every plane
 * handle, connects them according to @p config.topology, reads detailed
 * per-plane tuning from @p config.config (when non-NULL), and registers the
 * built-in simulation models. The handle starts in the @c CREATED state;
 * @c kith_server_run transitions it to @c RUNNING.
 *
 * @param config     Creation parameters; @c size and @c abi_version must
 *                   match the runtime generation. NULL selects all defaults
 *                   (distributed topology, an OS-assigned ephemeral listen
 *                   port, default tick rate, no per-plane config source).
 * @param alloc      Allocator for the new handle's own allocations — the
 *                   handle, the delivery strategy copy, the delivery
 *                   configuration image, the connection bridge's table and
 *                   slots, and tick-dispatch work records — used again when
 *                   kith_server_destroy frees them. The plane handles the
 *                   wiring builds are constructed through their own public
 *                   create calls with the default allocator. NULL selects
 *                   the default allocator; a supplied allocator is validated
 *                   (see kith_allocator_t) and must outlive the handle.
 * @param out_server Receives the new handle on success.
 * @return           0 on success, negative kith_error on failure:
 *                   - -KITH_EINVAL if @p out_server is NULL, or @p alloc is
 *                     missing an operation,
 *                   - -KITH_EABIVER if @p config or @p alloc has an
 *                     incompatible abi_version,
 *                   - -KITH_ESIZE if @p config or @p alloc has an
 *                     undersized size,
 *                   - -KITH_ENOMEM on allocation failure,
 *                   - -KITH_EIO if a configured backing store or listen
 *                     endpoint cannot be opened.
 * @thread_safety unsafe — must not race with another kith_server_create on
 *                the same @p out_server slot.
 * @ownership callee — the returned handle is freed by the caller with
 *           kith_server_destroy. @p config.config is borrowed, not owned.
 */
[[nodiscard]] KITH_API int kith_server_create(const kith_server_params_t *config,
                                              const kith_allocator_t *alloc,
                                              kith_server_t **out_server);

/**
 * Release a server handle and every plane handle the wiring owns, in reverse
 * creation order. Passing NULL is a no-op. Must not be called while
 * @c kith_server_run is in flight on @p server; call @c kith_server_shutdown
 * first and wait for @c kith_server_run to return. The borrowed configuration
 * source passed at create time is not freed (the caller owns it and destroys
 * it separately, after this call).
 *
 * The contract is enforced at runtime: a @c kith_server_destroy call made
 * while the run loop is in flight aborts the process (after logging at
 * @c KITH_LOG_LEVEL_ERROR) rather than tearing down the reactor and plane
 * handles out from under the run-loop thread, which is undefined behavior.
 *
 * @param server Server handle. NULL is a no-op.
 * @thread_safety unsafe — no run may be in flight on @p server when this is
 *                called; a violation aborts the process.
 * @ownership callee — @p server is consumed and freed by the call.
 */
KITH_API void kith_server_destroy(kith_server_t *server);

/**
 * Enter the server run loop. Drives the reactor and the tick-maintenance
 * cadence at the configured tick rate (coordination-bus drain, coordination
 * tick, gateway refresh and delivery); the simulation step itself belongs to
 * game code's registered tick callback. The call blocks until
 * @c kith_server_shutdown is requested (from another thread or a signal
 * handler) and the drain completes, then transitions the handle to
 * @c STOPPED and returns.
 *
 * @param server Server handle. NULL is an error.
 * @return       0 on clean shutdown, negative kith_error on failure:
 *               - -KITH_EINVAL if @p server is NULL,
 *               - -KITH_ESTATE if the handle is not in the @c CREATED or
 *                 @c STOPPED state (run already in flight),
 *               - -KITH_ENOMEM if the reactor cannot arm the next tick
 *                 mid-run (event-ring exhaustion); the loop drains, stops,
 *                 and run reports the code,
 *               - -KITH_EIO on a reactor or plane failure that aborts the
 *                 loop before shutdown completes.
 * @thread_safety unsafe — exactly one run loop may be active on @p server at
 *                a time.
 * @ownership caller — @p server is borrowed for the call; the caller
 *           destroys it with kith_server_destroy after run returns.
 */
[[nodiscard]] KITH_API int kith_server_run(kith_server_t *server);

/**
 * Request graceful shutdown. Sets an atomic flag observed by the run loop;
 * the loop stops accepting new work, drains in-flight work within the
 * configured drain window, then @c kith_server_run returns. Safe to call
 * from a thread other than the run-loop thread or from a signal handler.
 * Idempotent: calling on a handle that is already draining or stopped
 * returns 0. Calling on a handle that has never been run transitions it
 * directly to @c STOPPED.
 *
 * @param server Server handle. NULL is an error.
 * @return       0 on success (including when already draining or stopped),
 *               - -KITH_EINVAL if @p server is NULL.
 * @thread_safety safe — may be called concurrently with @c kith_server_run
 *                and from a signal handler.
 * @ownership caller — @p server is borrowed for the call only.
 */
[[nodiscard]] KITH_API int kith_server_shutdown(kith_server_t *server);

/**
 * Return the handle's current lifecycle state. The value reflects the most
 * recent transition observed by the run loop.
 *
 * @param server Server handle.
 * @return       the @c kith_server_status value, or
 *               @c KITH_SERVER_STATUS_CREATED when @p server is NULL.
 * @thread_safety safe
 * @ownership caller — @p server is borrowed for the call only.
 */
KITH_API kith_server_status_t kith_server_status(const kith_server_t *server);

/**
 * Return the actual TCP port the gateway listener is bound to. When the
 * config @c listen_port is 0 (or @p config is NULL), the OS assigns an
 * ephemeral port at bind time; this accessor reads it back via
 * @c getsockname so a caller that bound ephemerally can advertise or connect
 * to the real endpoint. The value is fixed once the handle reaches the
 * @c CREATED state.
 *
 * @param server Server handle.
 * @return       the bound port in host byte order, or 0 when @p server is NULL
 *               or the listener is not bound.
 * @thread_safety safe — the listener fd and bound port are fixed after
 *                server creation.
 * @ownership caller — @p server is borrowed for the call only.
 */
KITH_API uint16_t kith_server_listen_port(const kith_server_t *server);

/**
 * Return the actual TCP port the control-plane listener is bound to. When
 * the control plane library is linked into the build, the wiring starts it
 * with an OS-assigned ephemeral port (params port 0); this accessor reads
 * the bound port back so a caller can reach the control plane's HTTP
 * routes. Returns 0 when @p server is NULL, the control plane library is
 * not linked into the build, or the control plane is not started.
 *
 * @param server Server handle.
 * @return       the bound control-plane port in host byte order, or 0 when
 *               unbound or absent.
 * @thread_safety safe — the listener fd and bound port are fixed after
 *                server creation.
 * @ownership caller — @p server is borrowed for the call only.
 */
KITH_API uint16_t kith_server_control_port(const kith_server_t *server);

/**
 * Register a single per-tick game-logic callback. The reactor invokes @p fn
 * once per normal tick, advancing a monotonic tick counter and dispatching
 * the callback to the worker pool
 * as a single task when @p flags carries @c KITH_SERVER_HANDLER_PYTHON,
 * or inline on the reactor thread for a C callback (no flag). The shutdown
 * drain tick does not dispatch the callback. Replaces any prior
 * registration.
 *
 * When @c KITH_SERVER_HANDLER_PYTHON is set, the tick callback is
 * dispatched to the worker pool as a single task. With no pool attached,
 * or when the pool's task queue is exhausted, the callback is dropped and
 * counted via @c kith_server_tick_drops; the reactor never runs a
 * Python-bound tick callback inline.
 *
 * @param server    Server handle. NULL is an error.
 * @param fn        Tick callback. NULL is an error.
 * @param user_data Caller context passed verbatim to @p fn on each tick.
 * @param flags     Bitmask of @c kith_server_handler_flag values.
 * @return          0 on success, negative kith_error on failure:
 *                  - -KITH_EINVAL if @p server or @p fn is NULL.
 * @thread_safety unsafe — must not be called concurrently with
 *                @c kith_server_run (the reactor reads the registration
 *                once per tick). Register before @c kith_server_run.
 * @ownership caller — @p user_data is borrowed for the registration's
 *           lifetime; the caller must keep it valid until the callback is
 *           unregistered or the server is destroyed.
 */
[[nodiscard]] KITH_API int kith_server_register_tick_handler(kith_server_t *server,
                                                             kith_server_tick_fn fn,
                                                             void *user_data,
                                                             kith_server_handler_flag_t flags);

/**
 * Unregister the per-tick callback. After this call the reactor stops
 * dispatching the callback each tick. Idempotent: unregistering when no
 * callback is registered returns 0. A tick callback already submitted to
 * the worker pool before this call returns still runs to completion (it
 * holds its own callback snapshot).
 *
 * @param server Server handle. NULL is an error.
 * @return       0 on success (even when no callback was registered),
 *               - -KITH_EINVAL if @p server is NULL.
 * @thread_safety unsafe — must not be called concurrently with
 *                @c kith_server_run.
 * @ownership caller — @p server is borrowed for the call only.
 */
[[nodiscard]] KITH_API int kith_server_unregister_tick_handler(kith_server_t *server);

/**
 * Register the synchronous poll observer. The run-loop thread invokes @p fn
 * once per tick, before the tick's plane work, at a safe point where
 * re-entering an embedding runtime is permitted (the bounded main-thread
 * exception: a Python facade pumps pending signals here so a run loop on the
 * interpreter's main thread receives signal delivery — CPython runs signal
 * handlers and raises KeyboardInterrupt only in the main thread's eval loop,
 * which sits inside the blocking run call). A nonzero return requests the
 * same graceful shutdown @c kith_server_shutdown performs; the drain starts
 * on the current tick. Replaces any prior registration; @p fn NULL clears
 * it.
 *
 * @param server    Server handle. NULL is an error.
 * @param fn        Poll callback, or NULL to clear the registration.
 * @param user_data Caller context passed verbatim to @p fn on each poll.
 * @return          0 on success, negative kith_error on failure:
 *                  - -KITH_EINVAL if @p server is NULL.
 * @thread_safety unsafe — must not be called concurrently with
 *                @c kith_server_run. Register before @c kith_server_run.
 * @ownership caller — @p user_data is borrowed for the registration's
 *           lifetime; the caller must keep it valid until the observer is
 *           cleared or the server is destroyed.
 */
[[nodiscard]] KITH_API int
kith_server_register_poll_observer(kith_server_t *server, kith_server_poll_fn fn, void *user_data);

/**
 * Read the monotonic count of per-tick callbacks dropped due to worker-pool
 * exhaustion. A drop occurs when the tick dispatch is called for a callback
 * registered with @c KITH_SERVER_HANDLER_PYTHON, a worker pool is attached,
 * and @c kith_worker_submit returns EBUSY; the callback is dropped instead
 * of running it on the reactor thread. The counter is monotonic
 * and never reset. The composition root records its delta as
 * @c kith_server_tick_dropped_total.
 *
 * @param server   Server handle. NULL is an error.
 * @param out_drops Receives the monotonic drop count on success.
 * @return          0 on success, negative kith_error on failure:
 *                  - -KITH_EINVAL if @p server or @p out_drops is NULL.
 * @thread_safety safe — the counter is atomic; readable from any thread.
 *                Incremented on the reactor thread at drop time.
 * @ownership caller — @p out_drops is the caller's output storage.
 */
[[nodiscard]] KITH_API int kith_server_tick_drops(const kith_server_t *server, uint64_t *out_drops);

/** @} */

/**
 * @defgroup kith_server_planes Borrowed plane accessors
 * @ingroup kith_server
 * @{
 *
 * The composition root owns every plane handle the wiring creates. These
 * accessors hand out borrowed references to those handles so game code can
 * register handlers, models, queries, and routes against the plane
 * contracts without re-composing the planes itself. Every returned handle
 * stays owned by @p server: the caller uses it and must never destroy it
 * (the server releases each plane in reverse creation order at destroy
 * time). A NULL @p server yields a NULL handle.
 *
 * The handles are fixed for the server's lifetime once it reaches the
 * @c CREATED state. Registering against a borrowed handle before entering
 * @c kith_server_run is the intended sequence: the gateway only decodes
 * frames during the run loop, so types registered on the borrowed proto
 * before run are visible to the decoder.
 */

/**
 * Return the borrowed gateway plane handle, or NULL when @p server is NULL.
 *
 * @param server Server handle.
 * @return Borrowed gateway handle, or NULL.
 * @thread_safety safe — the handle is fixed after server creation.
 * @ownership caller — borrowed; the caller must not destroy the handle.
 */
KITH_API kith_gateway_t *kith_server_gateway(kith_server_t *server);

/**
 * Return the borrowed simulation plane handle, or NULL when @p server is
 * NULL.
 *
 * @param server Server handle.
 * @return Borrowed sim handle, or NULL.
 * @thread_safety safe — the handle is fixed after server creation.
 * @ownership caller — borrowed; the caller must not destroy the handle.
 */
KITH_API kith_sim_t *kith_server_sim(kith_server_t *server);

/**
 * Return the borrowed fabric plane handle, or NULL when @p server is NULL.
 *
 * @param server Server handle.
 * @return Borrowed fabric handle, or NULL.
 * @thread_safety safe — the handle is fixed after server creation.
 * @ownership caller — borrowed; the caller must not destroy the handle.
 */
KITH_API kith_fabric_t *kith_server_fabric(kith_server_t *server);

/**
 * Return the borrowed proto handle, or NULL when @p server is NULL. The
 * caller registers wire message types on this handle before the run loop so
 * the gateway's decoder accepts frames of those types.
 *
 * @param server Server handle.
 * @return Borrowed proto handle, or NULL.
 * @thread_safety safe — the handle is fixed after server creation.
 * @ownership caller — borrowed; the caller must not destroy the handle.
 */
KITH_API kith_proto_t *kith_server_proto(kith_server_t *server);

/**
 * Return the borrowed control plane handle, or NULL when @p server is NULL
 * or the control plane library is not linked into the build. The caller
 * registers HTTP routes on this handle.
 *
 * @param server Server handle.
 * @return Borrowed control handle, or NULL when not configured.
 * @thread_safety safe — the handle is fixed after server creation.
 * @ownership caller — borrowed; the caller must not destroy the handle.
 */
KITH_API kith_control_t *kith_server_control(kith_server_t *server);

/**
 * Return the borrowed persistence pool handle, or NULL. No shipped wiring
 * attaches a pool to the server, so this returns NULL and the caller that
 * needs persistence builds its own db handle and supplies the connection
 * parameters out of band, as the Postgres example does. The caller
 * registers named parameterized queries on a handle it obtains that way.
 *
 * @param server Server handle.
 * @return Borrowed db handle, or NULL when no pool is configured.
 * @thread_safety safe — the handle is fixed after server creation.
 * @ownership caller — borrowed; the caller must not destroy the handle.
 */
KITH_API kith_db_t *kith_server_db(kith_server_t *server);

/** @} */

#endif /* KITH_SERVER_SERVER_H */
