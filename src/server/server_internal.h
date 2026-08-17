#pragma once

#include <stdatomic.h>
#include <stdint.h>

#include <pthread.h>

#include "kith/config/config.h"
#include "kith/coord/coord.h"
#include "kith/fabric/fabric.h"
#include "kith/gateway/gateway.h"
#include "kith/logger/logger.h"
#include "kith/metrics/metrics.h"
#include "kith/net/net.h"
#include "kith/proto/proto.h"
#include "kith/reactor/reactor.h"
#include "kith/server/server.h"
#include "kith/sim/sim.h"
#include "kith/types.h"
#include "kith/version.h"
#include "kith/worker/worker.h"
#include "server/wire.h"

/**
 * Private server internals. The server library is split across two
 * translation units (server and wiring) that share this header for the
 * kith_server struct layout and the wiring contract. The structures here
 * are not visible outside the library.
 *
 * The server is the composition root: it owns every plane handle the
 * wiring creates (logger, metrics, proto, reactor, net, sim, fabric,
 * gateway, coord bus, coord, and the control plane) and releases them in
 * reverse creation order at destroy time. The borrowed kith_config_t
 * source passed at create time fills the documented server-level keys
 * (tick_hz, listen_port, listen_host) for unset fields and is not owned.
 *
 * The run loop is reactor-driven: kith_server_run schedules a periodic
 * tick on the reactor's timer wheel and enters kith_reactor_run. The tick
 * callback performs plane maintenance (coord bus drain, coord tick,
 * gateway cache refresh) and re-schedules itself. kith_server_shutdown
 * sets an atomic flag observed by the tick callback, which drains, stops
 * the reactor, and lets kith_reactor_run return.
 */

/*---------------------------------------------------------------------------
 * server handle
 *-------------------------------------------------------------------------*/

struct kith_server
{
    /** Allocator the handle was created with; every allocation the server
     *  performs over its lifetime — including teardown — routes through
     *  this one instance. Borrowed: must outlive the handle. */
    const kith_allocator_t *allocator;

    /** Owned infrastructure handles, in creation order. Destroyed in
     *  reverse by server_wiring_destroy. */
    kith_logger_t *logger;
    kith_metrics_t *metrics;
    kith_proto_t *proto;
    kith_reactor_t *reactor;
    /** Worker pool for Python-bound handler dispatch. Owned by
     *  the server; created before the gateway and control planes so they
     *  can borrow it. Size 1 under standard Python, N under free-threaded
     *  Python (python_worker_count config). NULL only if creation failed. */
    kith_worker_t *workers;
    kith_net_t *net;
    kith_sim_t *sim;
    kith_fabric_t *fabric;
    kith_gateway_t *gateway;
    /** NULL in the embedded topology (coord owns every cell). */
    kith_coord_bus_t *coord_bus;
    kith_coord_t *coord;
    /** NULL when the control plane library is not linked into the build. */
    kith_control_t *control;
    /** NULL when no persistence pool is configured. */
    kith_db_t *db;
    /** Wire driver: binds the transport listener and per-connection
     *  read/write/dispatch to the reactor and the gateway. Inline storage;
     *  its connection table is heap-allocated within server_wire. */
    struct server_wire wire;

    /** Borrowed typed configuration source, or NULL. Not owned. */
    const kith_config_t *config;

    /** Resolved high-level tunables. */
    kith_server_topology_t topology;
    uint32_t instance_id;
    uint16_t listen_port;
    uint16_t tick_hz;
    /** Message type id forwarded to the gateway as replication_type_id. */
    uint16_t replication_type_id;
    /** Message type id forwarded to the gateway as
     *  replication_batch_type_id (0 = per-subject delivery). */
    uint16_t replication_batch_type_id;
    /** Owned copy of params.delivery_strategy, forwarded to the gateway as
     *  delivery_strategy at create time (NULL = factory default). The
     *  gateway copies the name again for its own lifetime; this copy only
     *  has to outlive server_create_gateway. */
    char *delivery_strategy;
    /** Owned copy of params.delivery_config (NULL = strategy defaults). The
     *  image is a size-versioned struct whose leading uint32 declares its
     *  length; the copy validates that declaration before duplicating. */
    void *delivery_config;
    /** Gateway handler-table capacity forwarded at create time; 0 → default. */
    uint32_t handler_table_size;
    /** Delivery executor thread count forwarded to the gateway at create
     *  time (0 = inline delivery). */
    uint32_t delivery_worker_count;
    /** Per-pass compose-wait budget forwarded to the gateway at create
     *  time; 0 selects the gateway default. Consulted only when an
     *  executor is configured. */
    uint32_t delivery_wait_budget_us;
    /** Self-echo coverage stamping disable flag forwarded to the gateway at
     *  create time: the composition root enables stamping unless
     *  this is set. */
    bool self_echo_disabled;
    uint32_t python_worker_count;
    /** Control-plane per-connection response write buffer capacity forwarded
     *  to the control plane at create time (0 = control plane default). */
    uint32_t control_write_buffer_cap;
    /** Per-subscriber view-set subject capacity forwarded to the gateway at
     *  create time (0 = gateway default). */
    uint32_t view_max_subjects;
    /** View compose-and-deliver interval forwarded to the gateway at create
     *  time; 0 selects the tick interval (server_gateway_params). */
    uint32_t view_refresh_interval_ms;
    /** Cell-cache refresh interval forwarded to the gateway at create time;
     *  0 selects the tick interval (server_gateway_params). */
    uint32_t cache_refresh_interval_ms;
    /** Tick interval in milliseconds, derived as 1000 / tick_hz. */
    uint32_t tick_interval_ms;
    /** Last value read from kith_gateway_dispatch_drops; the tick path
     *  records the per-tick delta as kith_gateway_dispatch_dropped_total.
     *  The gateway
     *  owns the monotonic counter (incremented on its reactor-side drop
     *  hot path); the composition root owns the metrics handle and records
     *  the delta here on the tick path, off the drop hot path. */
    uint64_t gateway_dispatch_dropped_last;
    /** Last value read from kith_gateway_lifecycle_drops; the tick path
     *  records the per-tick delta as kith_gateway_lifecycle_dropped_total.
     *  The gateway owns the monotonic counter (incremented on its
     *  reactor-side drop path when a destroyed-session notification cannot
     *  reach a pool); the composition root owns the metrics handle and
     *  records the delta here on the tick path, off the drop hot path. */
    uint64_t gateway_lifecycle_dropped_last;
    /** Last value read from kith_gateway_broadcast_refusals; the tick path
     *  records the per-tick delta as
     *  kith_gateway_broadcast_refused_total. The gateway owns the
     *  monotonic counter (incremented when a broadcast submit finds the
     *  request queue full); the composition root owns the metrics handle
     *  and records the delta here on the tick path, off the submit path. */
    uint64_t gateway_broadcast_refused_last;
    /** Last value read from kith_gateway_broadcast_drops; the tick path
     *  records the per-tick delta as
     *  kith_gateway_broadcast_dropped_total. The gateway owns the
     *  monotonic counter (incremented when a broadcast fanout cannot
     *  complete a recipient's copy); the composition root owns the metrics
     *  handle and records the delta here on the tick path, off the fanout
     *  path. */
    uint64_t gateway_broadcast_dropped_last;
    /** Last value read from kith_proto_rejections; the tick path records
     *  the per-tick delta as kith_proto_rejections_total. Proto owns the
     *  monotonic counter (incremented on the decoding thread at rejection
     *  time); the composition root owns the metrics handle and records the
     *  delta here on the tick path, off the decode hot path. */
    uint64_t proto_rejections_last;
    /** Last value read from kith_net_rejections; the tick path records the
     *  per-tick delta as kith_net_rejections_total. Net owns the monotonic
     *  counter (incremented on the reactor thread at rb_max close time);
     *  the composition root owns the metrics handle and records the delta
     *  here on the tick path. With the proto counter the pair reconciles
     *  the run's total malformed input against what the two planes
     *  rejected. */
    uint64_t net_rejections_last;
    /** Last value read from kith_gateway_window_add_failures; the tick
     *  path records the per-tick delta as
     *  kith_gateway_window_add_failures_total. The gateway owns the
     *  monotonic counter (incremented at the failing window add on the
     *  calling thread); the composition root owns the metrics handle and
     *  records the delta here on the tick path, off the add hot path. */
    uint64_t gateway_window_add_failures_last;
    /** Last value read from kith_gateway_window_retry_adds; the tick path
     *  records the per-tick delta as kith_gateway_window_retry_adds_total.
     *  The gateway owns the monotonic counter (incremented on the reactor
     *  thread when a retained add lands); the composition root owns the
     *  metrics handle and records the delta here on the tick path. With
     *  the failure counter and the retry-pending gauge it separates a
     *  healing flood from a stuck one. */
    uint64_t gateway_window_retry_adds_last;
    /** Last value read from kith_python_handler_exceptions; the tick path
     *  records the per-tick delta as
     *  kith_python_handler_exceptions_total. Util owns the process-global
     *  monotonic counter (the Python bridge's handler guards increment it
     *  on worker threads); the composition root owns the metrics handle
     *  and records the delta here on the tick path. The count spans the
     *  tick, message, session-destroyed, and control-route handler
     *  seams. */
    uint64_t python_handler_exceptions_last;

    /** Last samples of the gateway's reactor-path phase totals; the tick
     *  path records the per-tick deltas. The gateway owns the monotonic cumulative
     *  counters (ns in each tick phase + the dispatch count); the composition
     *  root owns the metrics handle and records the deltas here on the tick
     *  path, off the hot path. Mirrors @c
     *  gateway_dispatch_dropped_last for the timing and dispatch-rate
     *  terms of the per-tick capacity model. */
    uint64_t gateway_refresh_ns_last;
    uint64_t gateway_compose_ns_last;
    uint64_t gateway_deliver_ns_last;
    uint64_t gateway_dispatch_total_last;
    /** Last samples of the gateway's compose sub-phase totals; the tick
     *  path records the per-tick deltas. Mirrors the phase totals above for the
     *  sub-phases inside the compose phase (window copy, prior-set rebuild,
     *  stripe acquisition, scan, heapsort, select); the sum of the deltas
     *  bounds the compose delta from below (heap growth, the stripe unlock,
     *  and failed compositions stay unrecorded). */
    uint64_t gateway_compose_window_ns_last;
    uint64_t gateway_compose_prior_ns_last;
    uint64_t gateway_compose_lock_wait_ns_last;
    uint64_t gateway_compose_scan_ns_last;
    uint64_t gateway_compose_sort_ns_last;
    uint64_t gateway_compose_select_ns_last;
    /** Last executor-stats samples for the delivery-executor delta
     *  counters: jobs submitted, in-flight deliver-pass skips,
     *  EBUSY skips, wait-budget-exhausted skips, and compose-wait
     *  timeouts. The composition root reads the executor's stats DTO once
     *  per tick and records each counter's delta here on the tick path;
     *  when no executor is configured the DTO read fails with ESTATE and
     *  nothing is recorded. */
    uint64_t gateway_delivery_jobs_last;
    uint64_t gateway_delivery_inflight_skips_last;
    uint64_t gateway_delivery_ebusy_skips_last;
    uint64_t gateway_delivery_budget_exhausted_last;
    uint64_t gateway_delivery_wait_timeouts_last;
    /** Last samples of the gateway's cumulative delivery and view
     *  population totals; the tick path records their per-tick deltas. The
     *  gateway
     *  folds the delivery totals inside kith_gateway_deliver (inline and
     *  executor paths alike) and accumulates the view totals at each
     *  delivery-pass visit; the composition root reads both DTOs once per
     *  tick and records each counter's delta here on the tick path.
     *  The two view high-watermarks are gauges, recorded
     *  as-is, so they carry no last-sample state. */
    uint64_t gateway_delivery_enqueued_last;
    uint64_t gateway_delivery_dropped_last;
    uint64_t gateway_delivery_events_last;
    uint64_t gateway_delivery_suppressed_last;
    uint64_t gateway_view_visits_last;
    uint64_t gateway_view_candidate_last;
    uint64_t gateway_view_selected_last;
    /** Last value read from kith_gateway_compose_skips; the tick path
     *  records the per-tick delta as kith_gateway_compose_skips_total.
     *  The gateway
     *  owns the monotonic counter (incremented on the reactor thread when
     *  a session's retained view set is reused); the composition root
     *  records the delta here on the tick path. */
    uint64_t gateway_compose_skips_last;
    /** Last value read from kith_gateway_compose_deferrals; the tick path
     *  records the per-tick delta as kith_gateway_compose_deferrals_total. Sibling
     *  to compose_skips_last: skips separate "nothing changed" from
     *  deferrals' "the tick ran out of budget". */
    uint64_t gateway_compose_deferrals_last;
    /** Last value read from kith_gateway_view_locate_failures; the tick
     *  path records the per-tick delta as
     *  kith_gateway_view_locate_failures_total. The gateway owns the
     *  monotonic counter (incremented on the reactor thread when a scan
     *  phase cannot locate a session's bound actor in its window); the
     *  composition root records the delta here on the tick path.
     *  A healthy system records zero for this series forever:
     *  any nonzero reading is a subscription-window ownership defect. */
    uint64_t gateway_view_locate_failures_last;
    /** Last sample of the fabric's monotonic publish counter; the tick path
     *  records the per-tick delta as @c kith_fabric_publishes_total (the
     *  publishes/sec term of the capacity model). */
    uint64_t fabric_publish_last;
    /** Last sample of the wire driver's connection write-drain total; the
     *  tick path records the per-tick delta as @c kith_net_write_ns_total.
     *  The
     *  gateway phase counters stop at the in-memory enqueue; this is the
     *  socket-drain term that runs outside them on the reactor thread. */
    uint64_t net_write_ns_last;
    /** Last value read from kith_net_write_deferrals; the tick path records
     *  the per-tick delta as @c kith_net_write_deferrals_total. Counts drain
     *  calls the per-call cap truncated with output still queued (paced
     *  across reactor iterations); kernel-bound stops stay uncounted. */
    uint64_t net_write_deferrals_last;
    /** Last samples of the worker pool's lifetime task totals; the tick
     *  path records the per-tick deltas as kith_worker_tasks_submitted_total and
     *  kith_worker_tasks_completed_total. The pool owns the monotonic
     *  counters (relaxed atomics on its submit and completion paths); the
     *  composition root records the deltas here on the tick path.
     *  Submitted counts dispatches the gateway and control
     *  planes accepted (EBUSY rejections excluded); completed counts
     *  applies — the accepted-rate vs apply-rate pair the gateway dispatch
     *  counters reconcile against. */
    uint64_t worker_tasks_submitted_last;
    uint64_t worker_tasks_completed_last;

    /** Per-tick game-logic callback registration. @c tick_fn is NULL when
     *  no callback is registered. The
     *  reactor reads the triple under @c tick_lock once per normal tick and
     *  dispatches @c tick_fn: to the worker pool as a single task when
     *  @c tick_flags carries @c KITH_SERVER_HANDLER_PYTHON, or
     *  inline on the reactor for a C callback. @c tick_count is the
     *  monotonic tick counter advanced once per normal tick (independent of
     *  registration) and passed to the callback; the shutdown drain tick
     *  neither advances it nor dispatches. @c tick_dropped counts
     *  worker-pool-exhaustion drops (the tick analogue of the gateway's
     *  @c dispatch_dropped); @c tick_dropped_last backs the per-tick delta
     *  recorded as @c kith_server_tick_dropped_total. */
    kith_server_tick_fn tick_fn;
    void *tick_user_data;
    kith_server_handler_flag_t tick_flags;
    uint64_t tick_count;
    _Atomic uint64_t tick_dropped;
    uint64_t tick_dropped_last;
    pthread_mutex_t tick_lock;
    /** Error the tick callback stashes when the next-tick reactor
     *  schedule fails: plain int, set by the tick callback on the
     *  reactor thread and read by kith_server_run on the same thread
     *  after kith_reactor_run returns, so no atomic is needed. Zeroed
     *  by the zero-allocating create and reset at each run entry so a
     *  re-run never observes a stale code. */
    int tick_schedule_rc;

    /** Synchronous poll observer: invoked by
     *  the run-loop thread once per tick, before plane work, so an embedding
     *  whose run loop occupies the interpreter's main thread can pump
     *  pending signals. Registered before run; read on the run-loop thread
     *  only. A nonzero return sets shutdown_requested. */
    kith_server_poll_fn poll_fn;
    void *poll_user_data;

    /** Set by kith_server_shutdown (any thread) and observed by the tick
     *  callback on the reactor thread. */
    atomic_bool shutdown_requested;
    /** Current lifecycle state (CREATED/RUNNING/DRAINING/STOPPED). */
    _Atomic kith_server_status_t status;
    /** Set by kith_server_run for the duration of the blocking run loop and
     *  cleared on every return path before the function exits. Read by
     *  kith_server_destroy to enforce the @thread_safety contract: a destroy
     *  while a run is in flight aborts the process rather than tearing down
     *  the reactor and plane handles out from under the run-loop thread. The
     *  flag guards a single in-flight run; concurrent kith_server_run callers
     *  remain @thread_safety unsafe per the public contract. */
    atomic_bool run_in_flight;
};

/*---------------------------------------------------------------------------
 * wiring contract (wiring.c)
 *-------------------------------------------------------------------------*/

/**
 * Resolve the high-level tunables from @p config onto @p s: fill the
 * tick_hz and listen_port fields the caller left unset from the borrowed
 * config source's documented keys, substitute defaults for the remaining
 * zero fields, and derive the tick interval. Returns 0 on success or a
 * negative kith_error when a present key fails its parse or range check.
 * Called by kith_server_create before the wiring builds the plane handles.
 */
[[nodiscard]] int server_resolve_config(struct kith_server *s, const kith_server_params_t *config);

/**
 * Compose the gateway params from the resolved server state: replication
 * type ids, the delivery strategy and its config image, the handler-table
 * capacity, tick-aligned refresh intervals, the half-tick compose budget,
 * the delivery executor shape, and the compose-wait budget (0 resolves to
 * the gateway default, clamped by the wiring to half the tick interval
 * where that default would breach the below-one-tick rule). Called by
 * server_create_gateway.
 */
kith_gateway_params_t server_gateway_params(struct kith_server *s);

/**
 * Create every plane handle the composition root owns, in dependency order,
 * and connect them per the resolved topology. @p config supplies the
 * delivery strategy name and configuration image, which are copied onto the
 * server before any plane is created so the caller's memory only has to
 * stay valid for the enclosing create call. On failure, every partially
 * created handle is released in reverse order before returning, so the
 * server struct is left clean for the caller to free.
 *
 * @return 0 on success, negative kith_error on failure.
 */
[[nodiscard]] int server_wiring_create(struct kith_server *s, const kith_server_params_t *config);

/**
 * Release every plane handle the server owns, in reverse creation order.
 * Borrowed handles (the config source) are not freed. Passing a server
 * whose wiring never succeeded is safe (each handle is NULL-checked).
 */
void server_wiring_destroy(struct kith_server *s);

/*---------------------------------------------------------------------------
 * tick contract (wiring.c)
 *-------------------------------------------------------------------------*/

/**
 * Periodic plane maintenance invoked once per tick on the reactor thread:
 * drain the coord bus and dispatch rebalance contracts to the coord, run
 * the coord split/merge evaluation, maintain bus membership, and refresh
 * the shared gateway cell cache. The sim step itself is game code's
 * concern (game code owns the actor array and calls kith_sim_model_step
 * from a registered handler or reactor callback); the server drives the
 * tick cadence and the plane maintenance that keeps the fabric, coord,
 * and gateway coherent.
 *
 * @return 0 on success, negative kith_error on failure.
 */
[[nodiscard]] int server_tick_planes(struct kith_server *s, uint64_t now_ms);

/**
 * Record the interpreter's handler-exception delta into the server's
 * metrics registry. The counter is process-global (util owns the atomic;
 * the Python bridge's handler guards increment it on worker threads); the
 * composition root owns the metrics handle and folds the delta on the
 * tick path. Exposed for the wiring internals test, which drives the
 * recorder against a real metrics handle and asserts the rendered
 * counter.
 *
 * @param s  The server whose metrics handle records the delta.
 */
void server_record_python_handler_exceptions(struct kith_server *s);

/** Dispatch the registered per-tick game-logic callback for tick @p tick.
 *  Called once per normal tick on the reactor thread after
 *  @c server_tick_planes. A @c KITH_SERVER_HANDLER_PYTHON callback is
 *  submitted to the server's worker pool as a single task; a C callback
 *  runs inline on the reactor. On pool exhaustion the callback is dropped
 *  and counted in @c s->tick_dropped. No-op when no callback is registered.
 *  Not called on the shutdown drain tick. */
void server_dispatch_tick_hook(struct kith_server *s, uint64_t tick);

/** Re-arm every wire connection's reactor registration to match the
 *  connection's desired event bits after the gateway's per-tick delivery
 *  pass. Called on the reactor thread once per tick after the gateway
 *  delivers replication frames, so a connection with freshly enqueued
 *  output is registered for OUT and drained on the next reactor poll.
 *
 *  @return 0 on success, negative kith_error on failure.
 */
[[nodiscard]] int server_tick_arm(struct kith_server *s);
