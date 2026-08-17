/* Per-plane construction for the server: resolves config defaults, then
 * creates and wires the logger, metrics, reactor, net, proto, fabric, sim,
 * coord, gateway, control, and worker handles in dependency order, with
 * NULL-safe teardown on failure. Called from server.c. */

#include <stdlib.h>
#include <string.h>

#include "kith/util/util.h"
#include "server/server_internal.h"

/*---------------------------------------------------------------------------
 * config resolution
 *-------------------------------------------------------------------------*/

static int server_resolve_defaults(struct kith_server *s)
{
    /* A config key fills a field the caller left unset; an explicit params
     * field wins over the key. A key's value is the field's value, so a 0
     * tick rate or port from the source takes the same default-or-ephemeral
     * path as a 0 from the params. A listen_port still unset passes through
     * to the transport, which binds an OS-assigned ephemeral port (no fixed
     * default is applied here); the actual port is read back with
     * kith_server_listen_port. */
    if (s->tick_hz == 0u && s->config != nullptr)
    {
        uint16_t from_config = 0u;
        int rc = kith_config_u16(s->config, "tick_hz", 0u, UINT16_MAX, &from_config);
        if (rc != 0 && rc != kith_error_return(KITH_ENOENT))
        {
            return rc;
        }
        s->tick_hz = from_config;
    }
    if (s->tick_hz == 0u)
    {
        s->tick_hz = (uint16_t)KITH_SERVER_DEFAULT_TICK_HZ;
    }
    if (s->listen_port == 0u && s->config != nullptr)
    {
        uint16_t from_config = 0u;
        int rc = kith_config_u16(s->config, "listen_port", 0u, UINT16_MAX, &from_config);
        if (rc != 0 && rc != kith_error_return(KITH_ENOENT))
        {
            return rc;
        }
        s->listen_port = from_config;
    }
    if (s->python_worker_count == 0u)
    {
        s->python_worker_count = KITH_SERVER_DEFAULT_PYTHON_WORKERS;
    }
    s->tick_interval_ms = 1000u / s->tick_hz;
    return 0;
}

int server_resolve_config(struct kith_server *s, const kith_server_params_t *config)
{
    if (config == nullptr)
    {
        s->topology = KITH_SERVER_TOPOLOGY_DISTRIBUTED;
        s->instance_id = 0u;
        s->listen_port = 0u;
        s->tick_hz = 0u;
        s->replication_type_id = 0u;
        s->replication_batch_type_id = 0u;
        s->handler_table_size = 0u;
        s->delivery_worker_count = 0u;
        s->delivery_wait_budget_us = 0u;
        s->self_echo_disabled = false;
        s->python_worker_count = 0u;
        s->control_write_buffer_cap = 0u;
        s->view_max_subjects = 0u;
        s->view_refresh_interval_ms = 0u;
        s->cache_refresh_interval_ms = 0u;
        s->config = nullptr;
        return server_resolve_defaults(s);
    }

    s->topology = config->topology;
    s->instance_id = config->instance_id;
    s->listen_port = config->listen_port;
    s->tick_hz = config->tick_hz;
    s->replication_type_id = config->replication_type_id;
    s->replication_batch_type_id = config->replication_batch_type_id;
    s->handler_table_size = config->handler_table_size;
    s->delivery_worker_count = config->delivery_worker_count;
    s->delivery_wait_budget_us = config->delivery_wait_budget_us;
    s->self_echo_disabled = config->self_echo_disabled;
    s->python_worker_count = config->python_worker_count;
    s->control_write_buffer_cap = config->control_write_buffer_cap;
    s->view_max_subjects = config->view_max_subjects;
    s->view_refresh_interval_ms = config->view_refresh_interval_ms;
    s->cache_refresh_interval_ms = config->cache_refresh_interval_ms;
    s->config = config->config;
    return server_resolve_defaults(s);
}

/** Copy the delivery strategy name and configuration image from the
 *  caller's params onto stable storage. The gateway re-copies both for its
 *  own lifetime at gateway create; these copies only have to outlive that
 *  call, but owning them here keeps the params contract "borrowed for the
 *  call" while every teardown path frees through one ladder. The image is a
 *  size-versioned struct whose leading uint32 declares its own length; the
 *  declaration gets the same sanity range the gateway applies before it
 *  trusts the extent. */
static int server_delivery_copy(struct kith_server *s, const kith_server_params_t *config)
{
    if (config->delivery_strategy != nullptr)
    {
        s->delivery_strategy = kith_strdup(s->allocator, config->delivery_strategy);
        if (s->delivery_strategy == nullptr)
        {
            return kith_error_return(KITH_ENOMEM);
        }
    }
    if (config->delivery_config != nullptr)
    {
        const uint32_t declared = *(const uint32_t *)config->delivery_config;
        if (declared == 0u || declared > 65536u)
        {
            return kith_error_return(KITH_EINVAL);
        }
        s->delivery_config = kith_alloc(s->allocator, declared);
        if (s->delivery_config == nullptr)
        {
            return kith_error_return(KITH_ENOMEM);
        }
        memcpy(s->delivery_config, config->delivery_config, declared);
    }
    return 0;
}

/*---------------------------------------------------------------------------
 * per-plane creation helpers
 *
 * Each helper creates one plane handle, stores it on the server struct, and
 * returns 0 on success or a negative kith_error on failure. The main
 * server_wiring_create calls them in dependency order and tears down on
 * failure via server_wiring_destroy, which NULL-checks every slot.
 *-------------------------------------------------------------------------*/

static kith_logger_params_t server_logger_params(void)
{
    kith_logger_params_t p;
    memset(&p, 0, sizeof(p));
    p.size = sizeof(p);
    p.abi_version = KITH_ABI_VERSION;
    return p;
}

static int server_create_logger(struct kith_server *s)
{
    kith_logger_params_t p = server_logger_params();
    return kith_logger_create(&p, nullptr, &s->logger);
}

static kith_metrics_params_t server_metrics_params(void)
{
    kith_metrics_params_t p;
    memset(&p, 0, sizeof(p));
    p.size = sizeof(p);
    p.abi_version = KITH_ABI_VERSION;
    return p;
}

static int server_create_metrics(struct kith_server *s)
{
    kith_metrics_params_t p = server_metrics_params();
    return kith_metrics_create(&p, nullptr, &s->metrics);
}

static kith_proto_params_t server_proto_params(void)
{
    kith_proto_params_t p;
    memset(&p, 0, sizeof(p));
    p.size = sizeof(p);
    p.abi_version = KITH_ABI_VERSION;
    return p;
}

static int server_create_proto(struct kith_server *s)
{
    kith_proto_params_t p = server_proto_params();
    return kith_proto_create(&p, nullptr, &s->proto);
}

static kith_reactor_params_t server_reactor_params(void)
{
    kith_reactor_params_t p;
    memset(&p, 0, sizeof(p));
    p.size = sizeof(p);
    p.abi_version = KITH_ABI_VERSION;
    return p;
}

static int server_create_reactor(struct kith_server *s)
{
    kith_reactor_params_t p = server_reactor_params();
    return kith_reactor_create(&p, nullptr, &s->reactor);
}

static kith_worker_params_t server_worker_params(struct kith_server *s)
{
    kith_worker_params_t p;
    memset(&p, 0, sizeof(p));
    p.size = sizeof(p);
    p.abi_version = KITH_ABI_VERSION;
    p.worker_count = s->python_worker_count;
    return p;
}

static int server_create_workers(struct kith_server *s)
{
    kith_worker_params_t p = server_worker_params(s);
    return kith_worker_create(&p, nullptr, &s->workers);
}

// Attach the worker pool to the planes that borrow it. Called after every
// plane handle exists (the gateway and control planes read the pool without
// a lock from their dispatch paths, so the attach happens here on the
// composition root's thread before the reactor starts).
static void server_attach_workers(struct kith_server *s)
{
    if (s->workers == nullptr)
    {
        return;
    }
    (void)kith_gateway_attach_worker_pool(s->gateway, s->workers);
#ifdef KITH_CONTROL_PLANE_ENABLED
    (void)kith_control_attach_worker_pool(s->control, s->workers);
#endif
}

// Metric name for the gateway's dispatch-drop counter. The gateway plane
// exposes the count as an accessor; the composition root records it under
// the kith_<plane>_ namespace.
static const char gateway_dispatch_dropped_metric[] = "kith_gateway_dispatch_dropped_total";

// Metric name for the gateway's dropped-lifecycle-notification counter. A
// dropped destroyed-session notification means the game's disconnect
// bookkeeping did not run for that session, so the gauge reads as
// per-session state leakage, not backpressure — hence its own counter
// rather than the dispatch-drop gauge.
static const char gateway_lifecycle_dropped_metric[] = "kith_gateway_lifecycle_dropped_total";

// Metric names for the cell-scoped broadcast counters. Refusals count
// submits the bounded request queue turned away (saturation);
// drops count deliveries that did not complete (a recipient whose queue
// was full or whose connection closed, a request that did not encode,
// requests pending at teardown). The split separates the game pacing
// signal from the delivery-loss signal.
static const char gateway_broadcast_refused_metric[] = "kith_gateway_broadcast_refused_total";
static const char gateway_broadcast_dropped_metric[] = "kith_gateway_broadcast_dropped_total";

// Metric names for the decode-path rejection counters. Proto counts frames
// its decode entry point rejected; net counts connections closed because a
// frame's declared total exceeded the input ring ceiling. The two surfaces
// are mutually exclusive (a frame either reaches decode or is rejected at
// the ring boundary), so their sum reconciles the run's total malformed
// input.
static const char proto_rejections_metric[] = "kith_proto_rejections_total";
static const char net_rejections_metric[] = "kith_net_rejections_total";

// Metric name for the interpreter's handler-exception counter. The count
// is process-global (util owns the atomic; the Python bridge's handler
// guards increment it from the ctypes trampolines), so the name reflects
// the source plane — the interpreter — and not the server that renders it.
// Tick, message, session-destroyed, and control-route handlers all fold
// into the one counter.
static const char python_handler_exceptions_metric[] = "kith_python_handler_exceptions_total";

// Record the interpreter's handler-exception delta into the metrics
// registry, the same tick-path shape as the dispatch-drop recorder: the
// Python bridge increments the process-global atomic at the guard on a
// worker thread; the metrics-library call happens here on the tick path,
// off the raise path. The count is interpreter-scoped, so a process
// running several server handles shares the delta (each records its own
// advance of the same global); single-server processes — the embedded
// default, and every distributed cluster member — attribute exactly. Only
// records when exceptions occurred in the interval, so a quiet tick takes
// no metrics lock.
void server_record_python_handler_exceptions(struct kith_server *s)
{
    if (s->metrics == nullptr)
    {
        return;
    }
    uint64_t now = kith_python_handler_exceptions();
    uint64_t delta = now - s->python_handler_exceptions_last;
    if (delta == 0u)
    {
        return;
    }
    s->python_handler_exceptions_last = now;
    (void)kith_metrics_counter_add(
        s->metrics, python_handler_exceptions_metric, nullptr, 0u, delta);
}

// Record the gateway's monotonic dispatch-drop delta into the metrics
// registry. The gateway increments an atomic on its reactor-side drop path
// (a Python-bound handler never runs inline, so saturation drops the
// dispatch); the metrics-library call happens here on the tick
// path, off the drop hot path. Only records when drops occurred in the
// interval, so a quiet tick takes no metrics lock.
static void server_record_gateway_dispatch_drops(struct kith_server *s)
{
    if (s->metrics == nullptr || s->gateway == nullptr)
    {
        return;
    }
    uint64_t now = 0u;
    if (kith_gateway_dispatch_drops(s->gateway, &now) != 0)
    {
        return;
    }
    uint64_t delta = now - s->gateway_dispatch_dropped_last;
    if (delta == 0u)
    {
        return;
    }
    s->gateway_dispatch_dropped_last = now;
    (void)kith_metrics_counter_add(s->metrics, gateway_dispatch_dropped_metric, nullptr, 0u, delta);
}

// Record the gateway's dropped-lifecycle-notification delta into the
// metrics registry, the same tick-path shape as the dispatch-drop recorder:
// the gateway increments an atomic on its reactor-side drop path (no pool
// attached, or a saturated pool refusing a destroyed-session notification);
// the metrics-library call happens here on the tick path, off the drop hot
// path. Only records when drops occurred in the interval, so a quiet tick
// takes no metrics lock.
static void server_record_gateway_lifecycle_drops(struct kith_server *s)
{
    if (s->metrics == nullptr || s->gateway == nullptr)
    {
        return;
    }
    uint64_t now = 0u;
    if (kith_gateway_lifecycle_drops(s->gateway, &now) != 0)
    {
        return;
    }
    uint64_t delta = now - s->gateway_lifecycle_dropped_last;
    if (delta == 0u)
    {
        return;
    }
    s->gateway_lifecycle_dropped_last = now;
    (void)kith_metrics_counter_add(
        s->metrics, gateway_lifecycle_dropped_metric, nullptr, 0u, delta);
}

// Record the cell-scoped broadcast submit-refusal delta into the metrics
// registry, the same tick-path shape as the dispatch-drop recorder: the
// gateway increments the atomic when a submit finds the request queue full
// (any submitting thread); the metrics-library call happens here on the
// tick path. Only records when refusals occurred in the interval, so a
// quiet tick takes no metrics lock.
static void server_record_gateway_broadcast_refusals(struct kith_server *s)
{
    if (s->metrics == nullptr || s->gateway == nullptr)
    {
        return;
    }
    uint64_t now = 0u;
    if (kith_gateway_broadcast_refusals(s->gateway, &now) != 0)
    {
        return;
    }
    uint64_t delta = now - s->gateway_broadcast_refused_last;
    if (delta == 0u)
    {
        return;
    }
    s->gateway_broadcast_refused_last = now;
    (void)kith_metrics_counter_add(
        s->metrics, gateway_broadcast_refused_metric, nullptr, 0u, delta);
}

// Record the cell-scoped broadcast delivery-drop delta into the metrics
// registry, the same tick-path shape: the gateway increments the atomic
// when a fanout cannot complete a recipient's copy (full or closed
// connection queue, unencodable request, teardown-pending request); the
// metrics-library call happens here on the tick path. Only records when
// drops occurred in the interval, so a quiet tick takes no metrics lock.
static void server_record_gateway_broadcast_drops(struct kith_server *s)
{
    if (s->metrics == nullptr || s->gateway == nullptr)
    {
        return;
    }
    uint64_t now = 0u;
    if (kith_gateway_broadcast_drops(s->gateway, &now) != 0)
    {
        return;
    }
    uint64_t delta = now - s->gateway_broadcast_dropped_last;
    if (delta == 0u)
    {
        return;
    }
    s->gateway_broadcast_dropped_last = now;
    (void)kith_metrics_counter_add(
        s->metrics, gateway_broadcast_dropped_metric, nullptr, 0u, delta);
}

static const char gateway_window_add_failures_metric[] = "kith_gateway_window_add_failures_total";

// Record the gateway's window-add capacity-failure delta into the metrics
// registry, the same tick-path shape as the dispatch-drop recorder: the
// gateway increments an atomic at the failing add on the calling thread,
// the metrics-library call happens here on the tick path, and a quiet
// interval takes no metrics lock. A failing add is retained for the retry
// pass, so this counter is the flood signal the gauges are read against,
// not a terminal count.
static void server_record_gateway_window_failures(struct kith_server *s)
{
    if (s->metrics == nullptr || s->gateway == nullptr)
    {
        return;
    }
    uint64_t now = 0u;
    if (kith_gateway_window_add_failures(s->gateway, &now) != 0)
    {
        return;
    }
    uint64_t delta = now - s->gateway_window_add_failures_last;
    if (delta == 0u)
    {
        return;
    }
    s->gateway_window_add_failures_last = now;
    (void)kith_metrics_counter_add(
        s->metrics, gateway_window_add_failures_metric, nullptr, 0u, delta);
}

static const char gateway_window_retry_adds_metric[] = "kith_gateway_window_retry_adds_total";
static const char gateway_window_retries_pending_metric[] = "kith_gateway_window_retries_pending";
static const char gateway_sessions_without_cells_metric[] = "kith_gateway_sessions_without_cells";

// Record the retry backstop's landing delta and publish its two state
// gauges: the retry queue's depth and the bound-but-windowless session
// census. Against the failure counter the trio separates a healing flood
// (failures climbing, landings landing, pending draining, census falling)
// from a stuck one (no landings, pending pegged, census climbing); the
// gauges are absolute reads, so they publish every tick like the
// executor's inflight gauge.
static void server_record_gateway_window_retries(struct kith_server *s)
{
    if (s->metrics == nullptr || s->gateway == nullptr)
    {
        return;
    }
    uint64_t now = 0u;
    if (kith_gateway_window_retry_adds(s->gateway, &now) != 0)
    {
        return;
    }
    uint64_t delta = now - s->gateway_window_retry_adds_last;
    if (delta != 0u)
    {
        s->gateway_window_retry_adds_last = now;
        (void)kith_metrics_counter_add(
            s->metrics, gateway_window_retry_adds_metric, nullptr, 0u, delta);
    }
    uint64_t pending = 0u;
    if (kith_gateway_window_retries_pending(s->gateway, &pending) != 0)
    {
        return;
    }
    (void)kith_metrics_gauge_set(
        s->metrics, gateway_window_retries_pending_metric, nullptr, 0u, (int64_t)pending);
    uint64_t without_cells = 0u;
    if (kith_gateway_sessions_without_cells(s->gateway, &without_cells) != 0)
    {
        return;
    }
    (void)kith_metrics_gauge_set(
        s->metrics, gateway_sessions_without_cells_metric, nullptr, 0u, (int64_t)without_cells);
}

// Record the proto decode-rejection delta into the metrics registry, the
// same tick-path shape as the gateway dispatch-drop recorder: the plane
// increments an atomic on its rejection hot path, the metrics-library call
// happens here off that path, and a quiet interval takes no metrics lock.
static void server_record_proto_rejections(struct kith_server *s)
{
    if (s->metrics == nullptr || s->proto == nullptr)
    {
        return;
    }
    uint64_t now = 0u;
    if (kith_proto_rejections(s->proto, &now) != 0)
    {
        return;
    }
    uint64_t delta = now - s->proto_rejections_last;
    if (delta == 0u)
    {
        return;
    }
    s->proto_rejections_last = now;
    (void)kith_metrics_counter_add(s->metrics, proto_rejections_metric, nullptr, 0u, delta);
}

// Record the transport's rb_max-close rejection delta, the same tick-path
// shape as the proto recorder. A frame rejected here never reaches the
// proto decode entry point, so the two counters never double-count one
// frame.
static void server_record_net_rejections(struct kith_server *s)
{
    if (s->metrics == nullptr || s->net == nullptr)
    {
        return;
    }
    uint64_t now = 0u;
    if (kith_net_rejections(s->net, &now) != 0)
    {
        return;
    }
    uint64_t delta = now - s->net_rejections_last;
    if (delta == 0u)
    {
        return;
    }
    s->net_rejections_last = now;
    (void)kith_metrics_counter_add(s->metrics, net_rejections_metric, nullptr, 0u, delta);
}

// Metric name for the server's per-tick callback drop counter. The server
// owns both the tick hook and the metrics handle; the drop is an atomic
// increment on the reactor's dispatch hot path and the per-tick delta is
// recorded here, off the hot path, mirroring the gateway dispatch-drop
// precedent.
static const char server_tick_dropped_metric[] = "kith_server_tick_dropped_total";

// Record the server's monotonic per-tick callback drop delta into the
// metrics registry. The tick dispatch increments an atomic on its
// reactor-side drop path (a Python-bound tick callback is never run inline
// on the reactor, so pool exhaustion drops the tick); the
// metrics-library call happens here on the tick path, off the drop hot
// path. Only records when drops occurred in the interval.
static void server_record_tick_drops(struct kith_server *s)
{
    if (s->metrics == nullptr)
    {
        return;
    }
    uint64_t now = atomic_load_explicit(&s->tick_dropped, memory_order_acquire);
    uint64_t delta = now - s->tick_dropped_last;
    if (delta == 0u)
    {
        return;
    }
    s->tick_dropped_last = now;
    (void)kith_metrics_counter_add(s->metrics, server_tick_dropped_metric, nullptr, 0u, delta);
}

// Metric names for the gateway's reactor-path phase totals. The gateway
// exposes the cumulative ns spent in each tick phase (refresh, compose,
// deliver) and the cumulative dispatch count as accessors; the composition
// root records the per-tick deltas here, so a /metrics scrape yields the
// per-phase ns/sec (ms after /1e6)
// and the dispatch rate the per-tick capacity model reconciles against the
// tick budget.
static const char gateway_refresh_ns_metric[] = "kith_gateway_refresh_ns_total";
static const char gateway_compose_ns_metric[] = "kith_gateway_compose_ns_total";
static const char gateway_deliver_ns_metric[] = "kith_gateway_deliver_ns_total";
static const char gateway_dispatches_metric[] = "kith_gateway_dispatches_total";
static const char fabric_publishes_metric[] = "kith_fabric_publishes_total";

// Metric names for the gateway's compose sub-phase totals: the ns the
// compose phase spends copying the window, rebuilding the prior-view id
// set, acquiring the window's cache stripes, scanning candidates into the
// bounded heap, heapsorting, and building the view set. Recorded like the
// phase totals above; the sum of the deltas bounds the compose delta from
// below rather than partitioning it (untimed slivers + error paths).
static const char gateway_compose_window_metric[] = "kith_gateway_compose_window_ns_total";
static const char gateway_compose_prior_metric[] = "kith_gateway_compose_prior_ns_total";
static const char gateway_compose_lock_wait_metric[] = "kith_gateway_compose_lock_wait_ns_total";
static const char gateway_compose_scan_metric[] = "kith_gateway_compose_scan_ns_total";
static const char gateway_compose_sort_metric[] = "kith_gateway_compose_sort_ns_total";
static const char gateway_compose_select_metric[] = "kith_gateway_compose_select_ns_total";

// Metric name for the gateway's compose-skip counter: recompositions skipped
// because a session's window, bound actor, view budget, and every windowed
// cell's cached content are unchanged since its last full composition.
static const char gateway_compose_skips_metric[] = "kith_gateway_compose_skips_total";

// Record the gateway's compose-skip delta into the metrics registry. The
// composer increments an atomic on the reactor thread whenever a session's
// retained view set is reused without a recomposition; the metrics-library
// call happens here on the tick path, off the hot path. Only records when
// skips occurred in the interval, so a fully-invalidated tick takes no
// metrics lock for this series.
static void server_record_gateway_compose_skips(struct kith_server *s)
{
    if (s->metrics == nullptr || s->gateway == nullptr)
    {
        return;
    }
    uint64_t now = 0u;
    if (kith_gateway_compose_skips(s->gateway, &now) != 0)
    {
        return;
    }
    uint64_t delta = now - s->gateway_compose_skips_last;
    if (delta == 0u)
    {
        return;
    }
    s->gateway_compose_skips_last = now;
    (void)kith_metrics_counter_add(s->metrics, gateway_compose_skips_metric, nullptr, 0u, delta);
}

// Metric name for the gateway's compose-deferral counter: recompositions a
// tick's session pass did not run because the pass had already consumed its
// per-tick compose budget. The deferred session delivers its retained view
// set that tick, so the series reads as bounded view staleness rather than
// missed frames; alongside the skip counter it separates "nothing changed"
// from "the tick ran out of budget".
static const char gateway_compose_deferrals_metric[] = "kith_gateway_compose_deferrals_total";

// Record the gateway's compose-deferral delta into the metrics registry.
// The gateway increments an atomic on the reactor thread when the tick's
// session pass defers a composed session past the budget; the metrics-
// library call happens here on the tick path, off the hot path. Only
// records when deferrals occurred in the interval.
static void server_record_gateway_compose_deferrals(struct kith_server *s)
{
    if (s->metrics == nullptr || s->gateway == nullptr)
    {
        return;
    }
    uint64_t now = 0u;
    if (kith_gateway_compose_deferrals(s->gateway, &now) != 0)
    {
        return;
    }
    uint64_t delta = now - s->gateway_compose_deferrals_last;
    if (delta == 0u)
    {
        return;
    }
    s->gateway_compose_deferrals_last = now;
    (void)kith_metrics_counter_add(
        s->metrics, gateway_compose_deferrals_metric, nullptr, 0u, delta);
}

// Metric name for the gateway's view locate-failure counter: compositions
// whose scan phase found no cached window cell holding the session's bound
// actor id, so the session delivered nothing that tick. A healthy system
// records zero for this series forever; any nonzero reading marks a
// subscription-window ownership defect.
static const char gateway_view_locate_failures_metric[] = "kith_gateway_view_locate_failures_total";

// Record the gateway's view locate-failure delta into the metrics registry.
// The gateway increments an atomic on the reactor thread when a scan phase
// fails to locate the subscriber; the metrics-library call happens here on
// the tick path, off the hot path. Only records when failures occurred in
// the interval, so a healthy tick takes no metrics lock for this series.
static void server_record_gateway_view_locate_failures(struct kith_server *s)
{
    if (s->metrics == nullptr || s->gateway == nullptr)
    {
        return;
    }
    uint64_t now = 0u;
    if (kith_gateway_view_locate_failures(s->gateway, &now) != 0)
    {
        return;
    }
    uint64_t delta = now - s->gateway_view_locate_failures_last;
    if (delta == 0u)
    {
        return;
    }
    s->gateway_view_locate_failures_last = now;
    (void)kith_metrics_counter_add(
        s->metrics, gateway_view_locate_failures_metric, nullptr, 0u, delta);
}

// Metric names for the delivery executor's counters. The five
// counters record per-tick deltas; the two inflight fields are gauges
// recorded as-is. Inflight skips count the per-session delivery
// cadence gaps under the executor, and the wait-budget counter pair
// decomposes its compose-side causes.
static const char gateway_delivery_jobs_metric[] = "kith_gateway_delivery_jobs_submitted_total";
static const char gateway_delivery_inflight_skips_metric[] =
    "kith_gateway_delivery_inflight_skips_total";
static const char gateway_delivery_ebusy_skips_metric[] = "kith_gateway_delivery_ebusy_skips_total";
static const char gateway_delivery_budget_exhausted_metric[] =
    "kith_gateway_delivery_wait_budget_exhausted_total";
static const char gateway_delivery_wait_timeouts_metric[] =
    "kith_gateway_compose_wait_timeouts_total";
static const char gateway_delivery_inflight_current_metric[] =
    "kith_gateway_delivery_inflight_current";
static const char gateway_delivery_inflight_high_watermark_metric[] =
    "kith_gateway_delivery_inflight_high_watermark";

// Record the delivery executor's stats into the metrics registry. The
// gateway exposes one stats DTO for the whole executor; the composition
// root reads it once per tick and records the five counter deltas plus
// the two gauges. With no executor configured the read fails with ESTATE
// and nothing is recorded — the inline path reports through the phase and
// deferral counters instead.
static void server_record_gateway_delivery_executor(struct kith_server *s)
{
    if (s->metrics == nullptr || s->gateway == nullptr)
    {
        return;
    }
    kith_gateway_delivery_executor_stats_t stats = {0};
    if (kith_gateway_delivery_executor_stats(s->gateway, &stats) != 0)
    {
        return;
    }
    const uint64_t jobs_delta = stats.jobs_submitted_total - s->gateway_delivery_jobs_last;
    const uint64_t inflight_delta =
        stats.inflight_skips_total - s->gateway_delivery_inflight_skips_last;
    const uint64_t ebusy_delta = stats.ebusy_skips_total - s->gateway_delivery_ebusy_skips_last;
    const uint64_t exhausted_delta =
        stats.wait_budget_exhausted_total - s->gateway_delivery_budget_exhausted_last;
    const uint64_t timeouts_delta =
        stats.compose_wait_timeouts_total - s->gateway_delivery_wait_timeouts_last;
    s->gateway_delivery_jobs_last = stats.jobs_submitted_total;
    s->gateway_delivery_inflight_skips_last = stats.inflight_skips_total;
    s->gateway_delivery_ebusy_skips_last = stats.ebusy_skips_total;
    s->gateway_delivery_budget_exhausted_last = stats.wait_budget_exhausted_total;
    s->gateway_delivery_wait_timeouts_last = stats.compose_wait_timeouts_total;
    if (jobs_delta != 0u)
    {
        (void)kith_metrics_counter_add(
            s->metrics, gateway_delivery_jobs_metric, nullptr, 0u, jobs_delta);
    }
    if (inflight_delta != 0u)
    {
        (void)kith_metrics_counter_add(
            s->metrics, gateway_delivery_inflight_skips_metric, nullptr, 0u, inflight_delta);
    }
    if (ebusy_delta != 0u)
    {
        (void)kith_metrics_counter_add(
            s->metrics, gateway_delivery_ebusy_skips_metric, nullptr, 0u, ebusy_delta);
    }
    if (exhausted_delta != 0u)
    {
        (void)kith_metrics_counter_add(
            s->metrics, gateway_delivery_budget_exhausted_metric, nullptr, 0u, exhausted_delta);
    }
    if (timeouts_delta != 0u)
    {
        (void)kith_metrics_counter_add(
            s->metrics, gateway_delivery_wait_timeouts_metric, nullptr, 0u, timeouts_delta);
    }
    (void)kith_metrics_gauge_set(s->metrics,
                                 gateway_delivery_inflight_current_metric,
                                 nullptr,
                                 0u,
                                 (int64_t)stats.inflight_current);
    (void)kith_metrics_gauge_set(s->metrics,
                                 gateway_delivery_inflight_high_watermark_metric,
                                 nullptr,
                                 0u,
                                 (int64_t)stats.inflight_high_watermark);
}

static const char gateway_delivery_frames_enqueued_metric[] =
    "kith_gateway_delivery_frames_enqueued_total";
static const char gateway_delivery_dropped_metric[] = "kith_gateway_delivery_dropped_total";
static const char gateway_delivery_event_frames_enqueued_metric[] =
    "kith_gateway_delivery_event_frames_enqueued_total";
static const char gateway_delivery_suppressed_metric[] = "kith_gateway_delivery_suppressed_total";

// Record the gateway's cumulative delivery totals into the metrics
// registry. The gateway folds them inside kith_gateway_deliver — the one
// site both the inline and the executor path share — so the DTO read
// always succeeds; the composition root records each counter's delta per
// tick. The dropped counter is the enqueue-side loss the selection
// metrics are read against; the suppressed counter quantifies the live
// suppression-lever behavior.
static void server_record_gateway_delivery_totals(struct kith_server *s)
{
    if (s->metrics == nullptr || s->gateway == nullptr)
    {
        return;
    }
    kith_gateway_delivery_totals_t totals = {0};
    if (kith_gateway_delivery_totals(s->gateway, &totals) != 0)
    {
        return;
    }
    const uint64_t enqueued_delta = totals.enqueued - s->gateway_delivery_enqueued_last;
    const uint64_t dropped_delta = totals.dropped - s->gateway_delivery_dropped_last;
    const uint64_t events_delta = totals.events_enqueued - s->gateway_delivery_events_last;
    const uint64_t suppressed_delta = totals.suppressed - s->gateway_delivery_suppressed_last;
    s->gateway_delivery_enqueued_last = totals.enqueued;
    s->gateway_delivery_dropped_last = totals.dropped;
    s->gateway_delivery_events_last = totals.events_enqueued;
    s->gateway_delivery_suppressed_last = totals.suppressed;
    if (enqueued_delta != 0u)
    {
        (void)kith_metrics_counter_add(
            s->metrics, gateway_delivery_frames_enqueued_metric, nullptr, 0u, enqueued_delta);
    }
    if (dropped_delta != 0u)
    {
        (void)kith_metrics_counter_add(
            s->metrics, gateway_delivery_dropped_metric, nullptr, 0u, dropped_delta);
    }
    if (events_delta != 0u)
    {
        (void)kith_metrics_counter_add(
            s->metrics, gateway_delivery_event_frames_enqueued_metric, nullptr, 0u, events_delta);
    }
    if (suppressed_delta != 0u)
    {
        (void)kith_metrics_counter_add(
            s->metrics, gateway_delivery_suppressed_metric, nullptr, 0u, suppressed_delta);
    }
}

static const char gateway_view_visits_metric[] = "kith_gateway_view_visits_total";
static const char gateway_view_candidate_metric[] = "kith_gateway_view_candidate_total";
static const char gateway_view_selected_metric[] = "kith_gateway_view_selected_total";
static const char gateway_view_candidate_high_watermark_metric[] =
    "kith_gateway_view_candidate_high_watermark";
static const char gateway_view_selected_high_watermark_metric[] =
    "kith_gateway_view_selected_high_watermark";

// Record the gateway's cumulative view-composition population totals into
// the metrics registry: delivery-pass visits of view-holding sessions and
// the sums of their live view metadata, plus the two peak gauges. The
// ratio of the candidate and selected deltas is the window's selection
// density; a candidate high-watermark at the view budget is the
// cap-binding evidence.
static void server_record_gateway_view_totals(struct kith_server *s)
{
    if (s->metrics == nullptr || s->gateway == nullptr)
    {
        return;
    }
    kith_gateway_view_totals_t totals = {0};
    if (kith_gateway_view_totals(s->gateway, &totals) != 0)
    {
        return;
    }
    const uint64_t visits_delta = totals.visits - s->gateway_view_visits_last;
    const uint64_t candidate_delta = totals.candidate_total - s->gateway_view_candidate_last;
    const uint64_t selected_delta = totals.selected_total - s->gateway_view_selected_last;
    s->gateway_view_visits_last = totals.visits;
    s->gateway_view_candidate_last = totals.candidate_total;
    s->gateway_view_selected_last = totals.selected_total;
    if (visits_delta != 0u)
    {
        (void)kith_metrics_counter_add(
            s->metrics, gateway_view_visits_metric, nullptr, 0u, visits_delta);
    }
    if (candidate_delta != 0u)
    {
        (void)kith_metrics_counter_add(
            s->metrics, gateway_view_candidate_metric, nullptr, 0u, candidate_delta);
    }
    if (selected_delta != 0u)
    {
        (void)kith_metrics_counter_add(
            s->metrics, gateway_view_selected_metric, nullptr, 0u, selected_delta);
    }
    (void)kith_metrics_gauge_set(s->metrics,
                                 gateway_view_candidate_high_watermark_metric,
                                 nullptr,
                                 0u,
                                 (int64_t)totals.candidate_high_watermark);
    (void)kith_metrics_gauge_set(s->metrics,
                                 gateway_view_selected_high_watermark_metric,
                                 nullptr,
                                 0u,
                                 (int64_t)totals.selected_high_watermark);
}

// Record the gateway's reactor-path phase deltas into the metrics registry.
// The gateway accumulates per-phase ns and the dispatch count as relaxed
// atomics on the reactor thread (and the dispatch-count increment on the
// reactor dispatch path); the metrics-library calls happen here on the tick
// path, off the hot path. Only records when a phase advanced, so a quiet
// tick takes no metrics lock for that phase.
static void server_record_gateway_phases(struct kith_server *s)
{
    if (s->metrics == nullptr || s->gateway == nullptr)
    {
        return;
    }
    kith_gateway_phase_stats_t stats;
    if (kith_gateway_phase_stats(s->gateway, &stats) != 0)
    {
        return;
    }
    uint64_t refresh_delta = stats.refresh_ns_total - s->gateway_refresh_ns_last;
    if (refresh_delta != 0u)
    {
        s->gateway_refresh_ns_last = stats.refresh_ns_total;
        (void)kith_metrics_counter_add(
            s->metrics, gateway_refresh_ns_metric, nullptr, 0u, refresh_delta);
    }
    uint64_t compose_delta = stats.compose_ns_total - s->gateway_compose_ns_last;
    if (compose_delta != 0u)
    {
        s->gateway_compose_ns_last = stats.compose_ns_total;
        (void)kith_metrics_counter_add(
            s->metrics, gateway_compose_ns_metric, nullptr, 0u, compose_delta);
    }
    uint64_t deliver_delta = stats.deliver_ns_total - s->gateway_deliver_ns_last;
    if (deliver_delta != 0u)
    {
        s->gateway_deliver_ns_last = stats.deliver_ns_total;
        (void)kith_metrics_counter_add(
            s->metrics, gateway_deliver_ns_metric, nullptr, 0u, deliver_delta);
    }
    uint64_t dispatch_delta = stats.dispatch_total - s->gateway_dispatch_total_last;
    if (dispatch_delta != 0u)
    {
        s->gateway_dispatch_total_last = stats.dispatch_total;
        (void)kith_metrics_counter_add(
            s->metrics, gateway_dispatches_metric, nullptr, 0u, dispatch_delta);
    }
}

// Record the gateway's compose sub-phase deltas into the metrics registry.
// The composer accumulates per-sub-phase ns as relaxed atomics on the
// reactor thread while the stripes are held and before/after it; the
// metrics-library calls happen here on the tick path, off the hot path.
// The lock-wait series is reactor-side acquisition only — a worker waiting
// on a stripe the reactor holds is not visible in it.
static void server_record_gateway_compose_phases(struct kith_server *s)
{
    if (s->metrics == nullptr || s->gateway == nullptr)
    {
        return;
    }
    kith_gateway_compose_stats_t stats;
    if (kith_gateway_compose_stats(s->gateway, &stats) != 0)
    {
        return;
    }
    uint64_t window_delta = stats.window_ns_total - s->gateway_compose_window_ns_last;
    if (window_delta != 0u)
    {
        s->gateway_compose_window_ns_last = stats.window_ns_total;
        (void)kith_metrics_counter_add(
            s->metrics, gateway_compose_window_metric, nullptr, 0u, window_delta);
    }
    uint64_t prior_delta = stats.prior_ns_total - s->gateway_compose_prior_ns_last;
    if (prior_delta != 0u)
    {
        s->gateway_compose_prior_ns_last = stats.prior_ns_total;
        (void)kith_metrics_counter_add(
            s->metrics, gateway_compose_prior_metric, nullptr, 0u, prior_delta);
    }
    uint64_t lock_delta = stats.lock_wait_ns_total - s->gateway_compose_lock_wait_ns_last;
    if (lock_delta != 0u)
    {
        s->gateway_compose_lock_wait_ns_last = stats.lock_wait_ns_total;
        (void)kith_metrics_counter_add(
            s->metrics, gateway_compose_lock_wait_metric, nullptr, 0u, lock_delta);
    }
    uint64_t scan_delta = stats.scan_ns_total - s->gateway_compose_scan_ns_last;
    if (scan_delta != 0u)
    {
        s->gateway_compose_scan_ns_last = stats.scan_ns_total;
        (void)kith_metrics_counter_add(
            s->metrics, gateway_compose_scan_metric, nullptr, 0u, scan_delta);
    }
    uint64_t sort_delta = stats.sort_ns_total - s->gateway_compose_sort_ns_last;
    if (sort_delta != 0u)
    {
        s->gateway_compose_sort_ns_last = stats.sort_ns_total;
        (void)kith_metrics_counter_add(
            s->metrics, gateway_compose_sort_metric, nullptr, 0u, sort_delta);
    }
    uint64_t select_delta = stats.select_ns_total - s->gateway_compose_select_ns_last;
    if (select_delta != 0u)
    {
        s->gateway_compose_select_ns_last = stats.select_ns_total;
        (void)kith_metrics_counter_add(
            s->metrics, gateway_compose_select_metric, nullptr, 0u, select_delta);
    }
}

// Record the fabric's monotonic publish delta into the metrics registry. The
// fabric increments a relaxed atomic on every successful publish (on the
// publishing thread — a worker under free-threaded Python); the metrics-
// library call happens here on the tick path, off the publish hot path.
// Only records when publishes occurred in the interval.
static void server_record_fabric_publishes(struct kith_server *s)
{
    if (s->metrics == nullptr || s->fabric == nullptr)
    {
        return;
    }
    uint64_t now = kith_fabric_publish_total(s->fabric);
    uint64_t delta = now - s->fabric_publish_last;
    if (delta == 0u)
    {
        return;
    }
    s->fabric_publish_last = now;
    (void)kith_metrics_counter_add(s->metrics, fabric_publishes_metric, nullptr, 0u, delta);
}

// Metric names for the worker pool's lifetime task totals. Submitted
// counts dispatches the gateway and control planes accepted (EBUSY
// rejections excluded); completed counts applies. The pair reconciles the
// gateway dispatch counters: attempts minus drops equals pool completions
// when the pool carries no other steady work.
static const char worker_tasks_submitted_metric[] = "kith_worker_tasks_submitted_total";
static const char worker_tasks_completed_metric[] = "kith_worker_tasks_completed_total";

// Record the worker pool's task-total deltas into the metrics registry. The
// pool increments the atomics on its submit and completion paths; the
// metrics-library call happens here on the tick path, off both hot paths.
// Only records when a total advanced, so a quiet tick takes no metrics lock.
static void server_record_worker_tasks(struct kith_server *s)
{
    if (s->metrics == nullptr || s->workers == nullptr)
    {
        return;
    }
    uint64_t submitted = kith_worker_tasks_submitted(s->workers);
    uint64_t delta = submitted - s->worker_tasks_submitted_last;
    if (delta != 0u)
    {
        s->worker_tasks_submitted_last = submitted;
        (void)kith_metrics_counter_add(
            s->metrics, worker_tasks_submitted_metric, nullptr, 0u, delta);
    }
    uint64_t completed = kith_worker_tasks_completed(s->workers);
    delta = completed - s->worker_tasks_completed_last;
    if (delta != 0u)
    {
        s->worker_tasks_completed_last = completed;
        (void)kith_metrics_counter_add(
            s->metrics, worker_tasks_completed_metric, nullptr, 0u, delta);
    }
}

// Metric name for the server's connection write-drain total: the cumulative
// ns the reactor thread spent draining connection output queues to their
// sockets (the kith_net_conn_write call site in the wire's readiness
// handler). The gateway phase counters bracket refresh/compose/deliver but
// stop at the in-memory enqueue; this series measures the transport drain
// that runs outside them on the same thread, so a /metrics scrape yields the
// full reactor-side delivery cost the per-tick capacity model reconciles
// against the tick budget.
static const char net_write_ns_metric[] = "kith_net_write_ns_total";

// Record the wire driver's write-drain delta into the metrics registry. The
// driver accumulates the bracketed ns as a relaxed atomic on the reactor
// thread; the metrics-library call happens here on the tick path, off the
// drain hot path. Only records when the total advanced, so an idle tick takes
// no metrics lock for this series.
static void server_record_net_write_ns(struct kith_server *s)
{
    if (s->metrics == nullptr)
    {
        return;
    }
    uint64_t now = atomic_load_explicit(&s->wire.write_ns_total, memory_order_relaxed);
    uint64_t delta = now - s->net_write_ns_last;
    if (delta == 0u)
    {
        return;
    }
    s->net_write_ns_last = now;
    (void)kith_metrics_counter_add(s->metrics, net_write_ns_metric, nullptr, 0u, delta);
}

// Metric name for the transport's write-deferral counter: kith_net_conn_write
// calls the per-call drain cap stopped with output still queued. The residual
// paces across subsequent readiness events, so the series reads as the pacing
// engagement rate; zero means every drain finished within its pass.
static const char net_write_deferrals_metric[] = "kith_net_write_deferrals_total";

// Record the transport's write-deferral delta into the metrics registry. The
// transport increments an atomic on the write-calling thread when the per-call
// cap truncates a drain; the metrics-library call happens here on the tick
// path, off the drain hot path. Only records when deferrals occurred in the
// interval.
static void server_record_net_write_deferrals(struct kith_server *s)
{
    if (s->metrics == nullptr || s->net == nullptr)
    {
        return;
    }
    uint64_t now = 0u;
    if (kith_net_write_deferrals(s->net, &now) != 0)
    {
        return;
    }
    uint64_t delta = now - s->net_write_deferrals_last;
    if (delta == 0u)
    {
        return;
    }
    s->net_write_deferrals_last = now;
    (void)kith_metrics_counter_add(s->metrics, net_write_deferrals_metric, nullptr, 0u, delta);
}

static kith_net_params_t server_net_params(void)
{
    kith_net_params_t p;
    memset(&p, 0, sizeof(p));
    p.size = sizeof(p);
    p.abi_version = KITH_ABI_VERSION;
    return p;
}

static int server_create_net(struct kith_server *s, const char *listen_host)
{
    kith_net_params_t p = server_net_params();
    int rc = kith_net_create(&p, s->proto, nullptr, &s->net);
    if (rc != 0)
    {
        return rc;
    }
    return kith_net_listen(s->net, listen_host, s->listen_port);
}

/** The gateway bind host: the caller's params value when set, else the
 *  listen_host key from the borrowed config source when present and
 *  non-empty, else NULL for the wildcard bind. The string aliases the
 *  config snapshot, which outlives the create call. */
static const char *server_resolve_listen_host(const struct kith_server *s,
                                              const kith_server_params_t *config)
{
    if (config->listen_host != nullptr)
    {
        return config->listen_host;
    }
    if (s->config == nullptr)
    {
        return nullptr;
    }
    const char *from_config = nullptr;
    if (kith_config_string(s->config, "listen_host", &from_config) != 0)
    {
        return nullptr;
    }
    return (*from_config != '\0') ? from_config : nullptr;
}

static kith_sim_params_t server_sim_params(struct kith_server *s)
{
    kith_sim_params_t p;
    memset(&p, 0, sizeof(p));
    p.size = sizeof(p);
    p.abi_version = KITH_ABI_VERSION;
    p.tick_hz = s->tick_hz;
    return p;
}

static int server_create_sim(struct kith_server *s)
{
    kith_sim_params_t p = server_sim_params(s);
    return kith_sim_create(&p, nullptr, &s->sim);
}

static kith_fabric_params_t server_fabric_params(void)
{
    kith_fabric_params_t p;
    memset(&p, 0, sizeof(p));
    p.size = sizeof(p);
    p.abi_version = KITH_ABI_VERSION;
    return p;
}

static int server_create_fabric(struct kith_server *s)
{
    kith_fabric_params_t p = server_fabric_params();
    return kith_fabric_create(&p, s->sim, nullptr, &s->fabric);
}

kith_gateway_params_t server_gateway_params(struct kith_server *s)
{
    kith_gateway_params_t p;
    memset(&p, 0, sizeof(p));
    p.size = sizeof(p);
    p.abi_version = KITH_ABI_VERSION;
    p.replication_type_id = s->replication_type_id;
    p.replication_batch_type_id = s->replication_batch_type_id;
    p.delivery_strategy = s->delivery_strategy;
    p.delivery_config = s->delivery_config;
    p.handler_table_size = s->handler_table_size;
    // Per-subscriber view-set capacity: forwarded raw, the gateway resolves
    // a 0 to its own default.
    p.view_max_subjects = s->view_max_subjects;
    // Align cache and view refresh with the simulation tick. The gateway
    // params default to a fixed 100 ms interval, but the server tick fires
    // at tick_interval_ms (50 ms at the 20 Hz default); a refresh interval
    // wider than the tick drops every input that lands inside the gap, so a
    // move published between two ticks is only observed on the tick after
    // the next. The caller's 0 selects this derivation; an explicit value
    // passes through unclamped and owns the wider-than-tick staleness it
    // implies. Tying the intervals to the tick makes the gateway compose
    // once per tick, matching the sim step cadence the handler publishes
    // at.
    p.view_refresh_interval_ms =
        s->view_refresh_interval_ms != 0u ? s->view_refresh_interval_ms : s->tick_interval_ms;
    p.cache_refresh_interval_ms =
        s->cache_refresh_interval_ms != 0u ? s->cache_refresh_interval_ms : s->tick_interval_ms;
    // Bound the composition pass to half the tick interval: a dirty tick
    // whose invalidations make many sessions recompose costs more reactor
    // time than one tick budget, and those ticks arrive back to back. Past
    // the budget composed sessions deliver their retained view set (frame
    // cadence per session is unchanged) and are served first next tick, so
    // overload degrades view freshness — bounded by rotation — instead of
    // delivery punctuality.
    p.compose_budget_us = s->tick_interval_ms * 500u;
    // Delivery executor: 0 keeps delivery inline on the reactor
    // thread; a non-zero count moves the tick's deliver pass onto
    // executor threads with the per-pass compose-wait budget alongside.
    p.delivery_worker_count = s->delivery_worker_count;
    // The gateway resolves a 0 wait budget to its 8000 us default, which
    // breaches the field's own below-one-tick rule once the tick interval
    // drops under 8 ms. Resolve the 0 path here to min(default, half the
    // tick — matching compose_budget_us above): rates at or under 62 Hz
    // keep the default, higher rates clamp, and a 0 ms integer interval
    // (tick_hz above 1000) resolves to 0, which the gateway's own 0
    // resolution then backs up to the default. An explicit caller value
    // passes through unclamped.
    p.delivery_wait_budget_us = s->delivery_wait_budget_us;
    if (p.delivery_wait_budget_us == 0u)
    {
        const uint32_t half_tick_us = s->tick_interval_ms * 500u;
        p.delivery_wait_budget_us = half_tick_us < KITH_GATEWAY_DEFAULT_DELIVERY_WAIT_BUDGET_US
                                        ? half_tick_us
                                        : KITH_GATEWAY_DEFAULT_DELIVERY_WAIT_BUDGET_US;
    }
    // Self-echo coverage stamping is on by default at the
    // composition root; the params flag exists for comparability runs.
    p.self_echo_disabled = s->self_echo_disabled;
    return p;
}

static int server_create_gateway(struct kith_server *s)
{
    kith_gateway_params_t p = server_gateway_params(s);
    return kith_gateway_create(&p, s->net, s->fabric, s->proto, nullptr, &s->gateway);
}

static kith_coord_params_t server_coord_params(struct kith_server *s)
{
    kith_coord_params_t p;
    memset(&p, 0, sizeof(p));
    p.size = sizeof(p);
    p.abi_version = KITH_ABI_VERSION;
    p.instance_id = s->instance_id;
    return p;
}

static int server_create_coord(struct kith_server *s)
{
    if (s->topology == KITH_SERVER_TOPOLOGY_EMBEDDED)
    {
        kith_coord_params_t p = server_coord_params(s);
        return kith_coord_create(&p, nullptr, nullptr, &s->coord);
    }

    kith_coord_bus_params_t bp;
    memset(&bp, 0, sizeof(bp));
    bp.size = sizeof(bp);
    bp.abi_version = KITH_ABI_VERSION;
    bp.instance_id = s->instance_id;
    bp.transport = KITH_COORD_BUS_TRANSPORT_LOOPBACK;
    int rc = kith_coord_bus_create(&bp, nullptr, &s->coord_bus);
    if (rc != 0)
    {
        return rc;
    }
    kith_coord_params_t p = server_coord_params(s);
    rc = kith_coord_create(&p, s->coord_bus, nullptr, &s->coord);
    if (rc != 0)
    {
        kith_coord_bus_destroy(s->coord_bus);
        s->coord_bus = nullptr;
    }
    return rc;
}

#ifdef KITH_CONTROL_PLANE_ENABLED

static kith_control_params_t server_control_params(struct kith_server *s)
{
    kith_control_params_t p;
    memset(&p, 0, sizeof(p));
    p.size = sizeof(p);
    p.abi_version = KITH_ABI_VERSION;
    p.write_buffer_cap = s->control_write_buffer_cap;
    return p;
}

static int server_create_control(struct kith_server *s)
{
    kith_control_params_t p = server_control_params(s);
    int rc = kith_control_create(&p, s->reactor, s->logger, s->metrics, nullptr, &s->control);
    if (rc != 0)
    {
        return rc;
    }
    return kith_control_start(s->control);
}

#endif /* KITH_CONTROL_PLANE_ENABLED */

/*---------------------------------------------------------------------------
 * wiring create / destroy
 *-------------------------------------------------------------------------*/

/** Create the services that connect the core planes to the outside world:
 *  the coordination plane, the optional control plane, the handler worker
 *  pool (attached to the planes that dispatch through it), and the wire
 *  driver that binds the transport. Each stage returns through the shared
 *  destroy path on failure. */
[[nodiscard]] static int server_wiring_connect_services(struct kith_server *s)
{
    int rc = server_create_coord(s);
    if (rc != 0)
    {
        return rc;
    }
#ifdef KITH_CONTROL_PLANE_ENABLED
    rc = server_create_control(s);
    if (rc != 0)
    {
        return rc;
    }
#endif
    rc = server_create_workers(s);
    if (rc != 0)
    {
        return rc;
    }
    server_attach_workers(s);
    return server_wire_create(&s->wire, s->allocator, s->reactor, s->net, s->gateway);
}

[[nodiscard]] int server_wiring_create(struct kith_server *s, const kith_server_params_t *config)
{
    int rc = server_delivery_copy(s, config);
    if (rc != 0)
    {
        return rc;
    }
    rc = server_create_logger(s);
    if (rc != 0)
    {
        return rc;
    }
    rc = server_create_metrics(s);
    if (rc != 0)
    {
        return rc;
    }
    rc = server_create_proto(s);
    if (rc != 0)
    {
        return rc;
    }
    rc = server_create_reactor(s);
    if (rc != 0)
    {
        return rc;
    }
    rc = server_create_net(s, server_resolve_listen_host(s, config));
    if (rc != 0)
    {
        return rc;
    }
    rc = server_create_sim(s);
    if (rc != 0)
    {
        return rc;
    }
    rc = server_create_fabric(s);
    if (rc != 0)
    {
        return rc;
    }
    rc = server_create_gateway(s);
    if (rc != 0)
    {
        return rc;
    }
    return server_wiring_connect_services(s);
}

void server_wiring_destroy(struct kith_server *s)
{
    // The worker pool is destroyed first: kith_worker_destroy joins every
    // worker thread after draining pending tasks, so no worker is running
    // a Python-bound handler when the planes it dispatched into are torn
    // down below. The planes borrowed the pool; the borrow ends here.
    kith_worker_destroy(s->workers);
    s->workers = nullptr;
    // The wire driver is next: it closes every connection (deregistering fds
    // from the reactor) and destroys the gateway sessions it created, while
    // the reactor, net, and gateway are still live to receive those calls.
    // Workers are already joined, so no in-flight dispatch references a
    // session being destroyed here.
    server_wire_destroy(&s->wire);
#ifdef KITH_CONTROL_PLANE_ENABLED
    kith_control_destroy(s->control);
    s->control = nullptr;
#endif
    kith_coord_destroy(s->coord);
    s->coord = nullptr;
    kith_coord_bus_destroy(s->coord_bus);
    s->coord_bus = nullptr;
    kith_gateway_destroy(s->gateway);
    s->gateway = nullptr;
    kith_fabric_destroy(s->fabric);
    s->fabric = nullptr;
    kith_sim_destroy(s->sim);
    s->sim = nullptr;
    kith_net_destroy(s->net);
    s->net = nullptr;
    kith_reactor_destroy(s->reactor);
    s->reactor = nullptr;
    kith_proto_destroy(s->proto);
    s->proto = nullptr;
    kith_metrics_destroy(s->metrics);
    s->metrics = nullptr;
    kith_logger_destroy(s->logger);
    s->logger = nullptr;
    // The delivery strategy name and configuration image are plain
    // allocations owned by the wiring; the gateway holds its own copies, so
    // freeing them here is safe at any point in the teardown.
    kith_free(s->allocator, s->delivery_strategy);
    s->delivery_strategy = nullptr;
    kith_free(s->allocator, s->delivery_config);
    s->delivery_config = nullptr;
    // The tick-handler registration mutex is the last piece of wiring state;
    // it is initialized in kith_server_create before server_wiring_create
    // and torn down here so the create-failure path and kith_server_destroy
    // both balance it.
    (void)pthread_mutex_destroy(&s->tick_lock);
}

/*---------------------------------------------------------------------------
 * tick: periodic plane maintenance
 *-------------------------------------------------------------------------*/

/** Drain the coord bus and dispatch rebalance contracts to the coord.
 *  No-op in the embedded topology (coord_bus is NULL). */
static int server_drain_coord_bus(struct kith_server *s)
{
    if (s->coord_bus == nullptr)
    {
        return 0;
    }

    kith_coord_bus_event_t events[16];
    size_t drained = 0;
    int rc = kith_coord_bus_drain(s->coord_bus, events, 16, &drained);
    if (rc != 0)
    {
        return rc;
    }
    for (size_t i = 0; i < drained; ++i)
    {
        if (events[i].event_type != KITH_COORD_BUS_EVENT_REBALANCE)
        {
            continue;
        }
        if (events[i].payload == nullptr ||
            events[i].payload_len < sizeof(kith_coord_rebalance_contract_t))
        {
            continue;
        }
        const kith_coord_rebalance_contract_t *contract = events[i].payload;
        rc = kith_coord_on_rebalance(s->coord, contract);
        if (rc != 0)
        {
            return rc;
        }
    }
    return 0;
}

[[nodiscard]] int server_tick_planes(struct kith_server *s, uint64_t now_ms)
{
    int rc = server_drain_coord_bus(s);
    if (rc != 0)
    {
        return rc;
    }
    rc = kith_coord_tick(s->coord, now_ms);
    if (rc != 0)
    {
        return rc;
    }
    if (s->coord_bus != nullptr)
    {
        rc = kith_coord_bus_tick(s->coord_bus, now_ms);
        if (rc != 0)
        {
            return rc;
        }
    }
    rc = kith_gateway_tick(s->gateway, now_ms);
    // Record the gateway's dispatch-drop delta after the gateway tick. The
    // gateway exposes the monotonic drop count as an accessor and the
    // composition root owns the metrics handle, so the metrics-library call
    // happens here on the tick path rather than on the gateway's reactor-side
    // drop hot path. Drops accumulate continuously between ticks;
    // a tick that returns early rolls its delta into the next recording.
    server_record_gateway_dispatch_drops(s);
    server_record_gateway_lifecycle_drops(s);
    server_record_gateway_broadcast_refusals(s);
    server_record_gateway_broadcast_drops(s);
    // Record the window-capacity story on the same tick path: the
    // failure-counter delta, the retry backstop's landings, and the two
    // state gauges (retry queue depth, seedless-session census). Together
    // they separate a healing flood from a stuck one.
    server_record_gateway_window_failures(s);
    server_record_gateway_window_retries(s);
    // Record the decode-path rejection deltas on the same tick path: proto
    // counts frames its decode entry point rejected, net counts connections
    // closed at the input ring ceiling. The pair never counts one frame
    // twice, so their sum reconciles the run's total malformed input.
    server_record_proto_rejections(s);
    server_record_net_rejections(s);
    // Record the tick-callback drop delta on the same tick path. The tick
    // dispatch (called after server_tick_planes returns) increments the
    // atomic on its drop hot path; the delta recorded here captures the
    // prior tick's drops (the current tick's dispatch has not run yet), so a
    // drop rolls one tick forward into the next recording.
    server_record_tick_drops(s);
    // Record the interpreter's handler-exception delta on the same tick
    // path. The Python bridge's handler guards increment the process-global
    // counter on worker threads while handlers run; the delta recorded here
    // captures the raises that happened before this recording.
    server_record_python_handler_exceptions(s);
    // Record the worker pool's task-total deltas on the same tick path. The
    // pool's submitted total counts dispatches the gateway and control
    // planes accepted (EBUSY rejections excluded); completed counts applies.
    // Together with the gateway dispatch counters they reconcile the
    // movement-input path end to end.
    server_record_worker_tasks(s);
    // Record the reactor-path phase and fabric-publish deltas. The gateway
    // accumulates per-phase ns (refresh/compose/deliver) and the dispatch
    // count; the fabric accumulates the publish count. Both are sampled off
    // the hot path here and feed the per-tick capacity model.
    server_record_gateway_phases(s);
    server_record_gateway_compose_phases(s);
    server_record_gateway_compose_skips(s);
    server_record_gateway_compose_deferrals(s);
    server_record_gateway_view_locate_failures(s);
    server_record_gateway_delivery_executor(s);
    server_record_gateway_delivery_totals(s);
    server_record_gateway_view_totals(s);
    server_record_fabric_publishes(s);
    server_record_net_write_ns(s);
    server_record_net_write_deferrals(s);
    return rc;
}

[[nodiscard]] int server_tick_arm(struct kith_server *s)
{
    server_wire_arm(&s->wire);
    return 0;
}

/*---------------------------------------------------------------------------
 * per-tick game-logic callback dispatch
 *-------------------------------------------------------------------------*/

// Heap-allocated work record for a single tick callback. The reactor
// allocates one per tick and submits it to the worker pool; the worker
// invokes the callback and frees the record. Carrying fn/tick/user_data by
// value decouples a running callback from a concurrent unregister or
// re-register (two ticks may be in flight on the pool if a callback is
// slow), with no use-after-free. The record carries the allocator it was
// allocated with, so the pool-side free routes through it independently of
// the server handle's lifetime.
struct server_tick_work
{
    const kith_allocator_t *allocator;
    kith_server_tick_fn fn;
    uint64_t tick;
    void *user_data;
};

static void server_tick_worker_task(void *arg)
{
    struct server_tick_work *work = arg;
    // Every task on this path carries a python-bound callback. A foreign C
    // thread entering a finalizing interpreter has no graceful path there:
    // refuse the entry and release the record.
    if (kith_python_finalizing())
    {
        kith_free(work->allocator, work);
        return;
    }
    work->fn(work->tick, work->user_data);
    kith_free(work->allocator, work);
}

void server_dispatch_tick_hook(struct kith_server *s, uint64_t tick)
{
    kith_server_tick_fn fn;
    void *user_data;
    kith_server_handler_flag_t flags;
    pthread_mutex_lock(&s->tick_lock);
    fn = s->tick_fn;
    user_data = s->tick_user_data;
    flags = s->tick_flags;
    pthread_mutex_unlock(&s->tick_lock);
    if (fn == nullptr)
    {
        return;
    }
    if ((flags & KITH_SERVER_HANDLER_PYTHON) != 0u)
    {
        // A Python-bound tick callback never runs on the reactor thread:
        // with no pool attached, or when the pool's queue is exhausted,
        // the callback is dropped and counted. server_create always builds
        // a pool, so the no-pool case is only reachable to an embedder
        // that bypassed it.
        if (s->workers == nullptr)
        {
            atomic_fetch_add_explicit(&s->tick_dropped, 1u, memory_order_relaxed);
            return;
        }
        struct server_tick_work *work = kith_alloc(s->allocator, sizeof(*work));
        if (work == nullptr)
        {
            atomic_fetch_add_explicit(&s->tick_dropped, 1u, memory_order_relaxed);
            return;
        }
        work->allocator = s->allocator;
        work->fn = fn;
        work->tick = tick;
        work->user_data = user_data;
        if (kith_worker_submit(s->workers, server_tick_worker_task, work) != 0)
        {
            kith_free(s->allocator, work);
            atomic_fetch_add_explicit(&s->tick_dropped, 1u, memory_order_relaxed);
        }
        return;
    }
    // C callback (no KITH_SERVER_HANDLER_PYTHON flag): dispatches inline.
    // A C callback never enters the interpreter, so the reactor's
    // never-blocks-on-Python invariant holds.
    fn(tick, user_data);
}
