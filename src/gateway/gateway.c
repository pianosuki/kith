/* Public handle and lifecycle for the gateway: params resolution, create and
 * destroy, and the per-tick drive that refreshes the cache, composes views,
 * and delivers replication frames. Owns the cache (cache/), session table
 * (session/), view composer (view/), and handler table (handler.c). */

#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>

#include "gateway/broadcast.h"
#include "gateway/cache/cache.h"
#include "gateway/delivery/executor.h"
#include "gateway/delivery/strategy.h"
#include "gateway/gateway_internal.h"
#include "gateway/session/session.h"
#include "gateway/view/view.h"
#include "kith/types.h"
#include "kith/version.h"

/*---------------------------------------------------------------------------
 * params resolution
 *-------------------------------------------------------------------------*/

static void gateway_resolve_params(kith_gateway_params_t *out)
{
    if (out->max_sessions == 0u)
    {
        out->max_sessions = KITH_GATEWAY_DEFAULT_MAX_SESSIONS;
    }
    if (out->cache_bucket_count == 0u)
    {
        out->cache_bucket_count = KITH_GATEWAY_DEFAULT_CACHE_BUCKETS;
    }
    if (out->view_bucket_count == 0u)
    {
        out->view_bucket_count = KITH_GATEWAY_DEFAULT_VIEW_BUCKETS;
    }
    if (out->view_max_subjects == 0u)
    {
        out->view_max_subjects = KITH_GATEWAY_DEFAULT_VIEW_MAX_SUBJECTS;
    }
    if (out->view_refresh_interval_ms == 0u)
    {
        out->view_refresh_interval_ms = KITH_GATEWAY_DEFAULT_VIEW_REFRESH_MS;
    }
    if (out->cache_refresh_interval_ms == 0u)
    {
        out->cache_refresh_interval_ms = KITH_GATEWAY_DEFAULT_CACHE_REFRESH_MS;
    }
    if (out->handler_table_size == 0u)
    {
        out->handler_table_size = KITH_GATEWAY_DEFAULT_HANDLER_TABLE_SIZE;
    }
    if (out->compose_budget_us == 0u)
    {
        out->compose_budget_us = KITH_GATEWAY_DEFAULT_COMPOSE_BUDGET_US;
    }
    if (out->delivery_wait_budget_us == 0u)
    {
        out->delivery_wait_budget_us = KITH_GATEWAY_DEFAULT_DELIVERY_WAIT_BUDGET_US;
    }
}

static bool gateway_params_validate(const kith_gateway_params_t *params, kith_error_t *out_err)
{
    if (params->size < sizeof(*params))
    {
        *out_err = KITH_ESIZE;
        return false;
    }
    if (params->abi_version != KITH_ABI_VERSION)
    {
        *out_err = KITH_EABIVER;
        return false;
    }
    *out_err = KITH_OK;
    return true;
}

/** Validate and default-fill @p params into @p out. A NULL params selects
 *  every default; otherwise size and abi_version are checked first and the
 *  caller's struct is copied, so subsequent caller-side mutation cannot race
 *  creation. Returns a wrapped error code, or 0. */
static int gateway_params_prepare(const kith_gateway_params_t *params, kith_gateway_params_t *out)
{
    if (params)
    {
        kith_error_t err = KITH_OK;
        if (!gateway_params_validate(params, &err))
        {
            return kith_error_return(err);
        }
        *out = *params;
    }
    else
    {
        memset(out, 0, sizeof(*out));
        out->size = sizeof(*out);
        out->abi_version = KITH_ABI_VERSION;
    }
    gateway_resolve_params(out);
    return 0;
}

/** Bind the resolved parameters onto a freshly allocated handle and start
 *  the pre-registry subsystems: session table, cache, handler table, and
 *  the compose scratch. Each stage cleans up its predecessors on failure;
 *  the handle itself stays the caller's to free. */
static int gateway_core_init(kith_gateway_t *g,
                             kith_net_t *net,
                             kith_fabric_t *fabric,
                             kith_proto_t *proto,
                             const kith_gateway_params_t *resolved)
{
    g->net = net;
    g->fabric = fabric;
    g->proto = proto;
    g->max_sessions = resolved->max_sessions;
    g->view_max_subjects = resolved->view_max_subjects;
    g->view_refresh_interval_ms = resolved->view_refresh_interval_ms;
    g->compose_budget_us = resolved->compose_budget_us;
    g->delivery_wait_budget_us = resolved->delivery_wait_budget_us;
    g->self_echo_enabled = !resolved->self_echo_disabled;
    g->cache_refresh_interval_ms = resolved->cache_refresh_interval_ms;
    g->crowd_exit_margin = resolved->crowd_exit_margin;
    g->replication_type_id = resolved->replication_type_id;
    g->replication_batch_type_id = resolved->replication_batch_type_id;

    int rc = gateway_broadcast_init(g);
    if (rc != 0)
    {
        return rc;
    }
    rc = gateway_session_table_init(&g->sessions, g->max_sessions, g->allocator);
    if (rc != 0)
    {
        gateway_broadcast_fini(g);
        return rc;
    }
    rc = gateway_cache_init(&g->cache, fabric, resolved->cache_bucket_count, g->allocator);
    if (rc != 0)
    {
        gateway_session_table_fini(&g->sessions);
        gateway_broadcast_fini(g);
        return rc;
    }
    rc = gateway_handler_init(&g->handlers, resolved->handler_table_size, g->allocator);
    if (rc != 0)
    {
        gateway_cache_fini(&g->cache);
        gateway_session_table_fini(&g->sessions);
        gateway_broadcast_fini(g);
        return rc;
    }
    if (pthread_mutex_init(&g->destroyed.lock, nullptr) != 0)
    {
        gateway_handler_fini(&g->handlers);
        gateway_cache_fini(&g->cache);
        gateway_session_table_fini(&g->sessions);
        gateway_broadcast_fini(g);
        return kith_error_return(KITH_ENOMEM);
    }
    gateway_view_scratch_init(&g->compose, g->allocator);
    g->delivery.allocator = g->allocator;
    g->delivery.batch = nullptr;
    g->delivery.batch_cap = 0u;
    g->delivery_executor = nullptr;
    if (resolved->delivery_worker_count > 0u)
    {
        // task_capacity = max_sessions is tight: one in-flight job per
        // session at most makes steady-state exhaustion impossible, and
        // a smaller capacity puts EBUSY on the steady-state path.
        rc = gateway_delivery_executor_create(
            resolved->delivery_worker_count, g->max_sessions, g->allocator, &g->delivery_executor);
        if (rc != 0)
        {
            pthread_mutex_destroy(&g->destroyed.lock);
            gateway_handler_fini(&g->handlers);
            gateway_cache_fini(&g->cache);
            gateway_session_table_fini(&g->sessions);
            gateway_broadcast_fini(g);
            return rc;
        }
    }
    return 0;
}

/** Copy the strategy configuration blob for the gateway's lifetime. The
 *  blob is a size-versioned struct whose leading uint32 declares its own
 *  length; the declared length is trusted for the read extent (callers
 *  construct it exactly as they construct any size-versioned struct),
 *  with a sanity range so a zero or absurd declaration fails loudly
 *  instead of copying garbage a strategy misreads at bind time. */
static int gateway_delivery_config_copy(kith_gateway_t *g, const void *config)
{
    if (!config)
    {
        return 0;
    }
    const uint32_t declared = *(const uint32_t *)config;
    if (declared == 0u || declared > 65536u)
    {
        return kith_error_return(KITH_EINVAL);
    }
    void *copy = kith_alloc(g->allocator, declared);
    if (!copy)
    {
        return kith_error_return(KITH_ENOMEM);
    }
    memcpy(copy, config, declared);
    g->delivery_config = copy;
    g->delivery_config_size = declared;
    return 0;
}

/** Start the delivery layer: strategy registry with built-ins registered,
 *  then the configured strategy name (empty selects "full") and the
 *  configuration blob copied onto stable storage so sessions bind against
 *  them for the gateway's life. Never cleans up on failure — the caller
 *  tears the whole handle down through one path, which keeps registry fini
 *  single-owner. */
static int
gateway_delivery_setup(kith_gateway_t *g, const char *strategy_name, const void *delivery_config)
{
    int rc = gateway_delivery_registry_init(&g->deliveries, g->allocator);
    if (rc == 0)
    {
        rc = gateway_delivery_builtins_register(&g->deliveries);
    }
    if (rc != 0)
    {
        return rc;
    }

    if (!strategy_name || strategy_name[0] == '\0')
    {
        strategy_name = "full";
    }
    g->delivery_strategy_name = kith_strdup(g->allocator, strategy_name);
    if (!g->delivery_strategy_name)
    {
        return kith_error_return(KITH_ENOMEM);
    }
    rc = gateway_delivery_config_copy(g, delivery_config);
    if (rc != 0)
    {
        kith_free(g->allocator, g->delivery_strategy_name);
        g->delivery_strategy_name = nullptr;
    }
    return rc;
}

/** Release every resource a fully or partially constructed handle owns.
 *  Shared by destroy and the late create-failure paths so each subsystem
 *  has exactly one fini site per object. The executor drains first:
 *  queued and running jobs (each holding a session reference) complete
 *  while every structure they touch is still alive, and their claimed
 *  scratches are freed before the gateway's own delivery scratch below. */
static void gateway_handle_teardown(kith_gateway_t *g)
{
    // Unregister the notification first: a session destroyed after this
    // point delivers nothing, and the slot's mutex retires with the handle
    // below, after the session table detaches its live sessions.
    pthread_mutex_lock(&g->destroyed.lock);
    g->destroyed.fn = nullptr;
    g->destroyed.user_data = nullptr;
    g->destroyed.flags = 0u;
    pthread_mutex_unlock(&g->destroyed.lock);
    // Freeze the broadcast queue and release its pending payloads before
    // the recipient sessions go away; a submit in flight
    // observes the freeze and refuses.
    gateway_broadcast_teardown(g);
    gateway_delivery_executor_fini(g->delivery_executor);
    gateway_view_scratch_fini(&g->compose);
    kith_free(g->delivery.allocator, g->delivery.batch);
    kith_free(g->allocator, g->delivery_strategy_name);
    kith_free(g->allocator, g->delivery_config);
    gateway_delivery_registry_fini(&g->deliveries);
    gateway_handler_fini(&g->handlers);
    gateway_cache_fini(&g->cache);
    gateway_session_table_fini(&g->sessions);
    pthread_mutex_destroy(&g->destroyed.lock);
    gateway_broadcast_fini(g);
    kith_free(g->allocator, g);
}

[[nodiscard]] KITH_API int kith_gateway_create(const kith_gateway_params_t *params,
                                               kith_net_t *net,
                                               kith_fabric_t *fabric,
                                               kith_proto_t *proto,
                                               const kith_allocator_t *alloc,
                                               kith_gateway_t **out_gateway)
{
    if (!out_gateway)
    {
        return kith_error_return(KITH_EINVAL);
    }
    *out_gateway = nullptr;
    if (!net || !fabric || !proto)
    {
        return kith_error_return(KITH_EINVAL);
    }

    kith_gateway_params_t resolved;
    int rc = gateway_params_prepare(params, &resolved);
    if (rc != 0)
    {
        return rc;
    }
    if (alloc != nullptr)
    {
        const kith_error_t alloc_rc = kith_allocator_check(alloc);
        if (alloc_rc != 0)
        {
            return kith_error_return(alloc_rc);
        }
    }
    const kith_allocator_t *allocator = (alloc != nullptr) ? alloc : kith_allocator_default();

    kith_gateway_t *g = kith_alloc_zero(allocator, 1, sizeof(*g));
    if (!g)
    {
        return kith_error_return(KITH_ENOMEM);
    }
    g->allocator = allocator;
    rc = gateway_core_init(g, net, fabric, proto, &resolved);
    if (rc != 0)
    {
        kith_free(allocator, g);
        return rc;
    }
    rc = gateway_delivery_setup(g, resolved.delivery_strategy, resolved.delivery_config);
    if (rc != 0)
    {
        gateway_handle_teardown(g);
        return rc;
    }

    *out_gateway = g;
    return 0;
}

KITH_API void kith_gateway_destroy(kith_gateway_t *gateway)
{
    if (!gateway)
    {
        return;
    }
    gateway_handle_teardown(gateway);
}

/*---------------------------------------------------------------------------
 * strategy registration
 *-------------------------------------------------------------------------*/

[[nodiscard]] KITH_API int kith_gateway_register_delivery(
    kith_gateway_t *gateway, const char *name, const kith_gateway_delivery_vtable_t *vtable)
{
    if (!gateway)
    {
        return kith_error_return(KITH_EINVAL);
    }
    return gateway_delivery_registry_register(&gateway->deliveries, name, vtable);
}

/*---------------------------------------------------------------------------
 * per-tick orchestration
 *-------------------------------------------------------------------------*/

/** True when @p session's composition runs in this tick's pass. A session
 *  without a retained view set always composes — deferring it would leave
 *  nothing for delivery to enqueue and the client would see no frame this
 *  tick. The pass's first eligible composition also always runs whatever
 *  the clock says (the head-of-pass guarantee): it bounds how long a
 *  deferred view stays stale to one pass per remaining session instead of
 *  letting a budget consumed by the phases before the pass starve every
 *  session forever. Past that, once the pass has consumed the compose
 *  budget, sessions with a retained view set defer (@p exhausted latches
 *  so subsequent sessions skip the clock read); the rotating start index serves
 *  them first next tick. @p composed counts the pass's compositions so far;
 *  @p pass_start_ns and @p budget_ns bracket the current pass. */
static bool gateway_tick_should_compose(const struct kith_gateway_session *s,
                                        uint64_t pass_start_ns,
                                        uint64_t budget_ns,
                                        bool *exhausted,
                                        const uint32_t *composed)
{
    if (!s->view.has_view)
    {
        return true;
    }
    if (*composed == 0u)
    {
        return true;
    }
    if (*exhausted)
    {
        return false;
    }
    if (gateway_now_ns() - pass_start_ns <= budget_ns)
    {
        return true;
    }
    *exhausted = true;
    return false;
}

/** Executor-mode compose gate: a session whose previous deliver is still
 *  in flight waits for it before its view may be recomposed, drawing from
 *  the pass's remaining wait budget (the deadline encodes it — waits are
 *  sequential on this thread, so deadline − now is what is left). Returns
 *  true when the composition may proceed. On expiry returns false and
 *  counts the cause: a wait that drew budget and ran it out is a wait
 *  timeout; an in-flight session arriving after the budget was already
 *  spent skips the wait instantly. Both leave the session its retained
 *  view; neither consumes the head-of-pass guarantee (the caller does not
 *  increment its composition count on false). Reactor thread only. */
static bool gateway_tick_compose_wait(struct gateway_delivery_executor *executor,
                                      struct kith_gateway_session *s,
                                      uint64_t wait_deadline_ns)
{
    if (!atomic_load_explicit(&s->delivery_in_flight, memory_order_acquire))
    {
        return true;
    }
    if (gateway_now_ns() >= wait_deadline_ns)
    {
        atomic_fetch_add_explicit(&executor->wait_budget_exhausted_total, 1u, memory_order_relaxed);
        return false;
    }
    if (!gateway_delivery_executor_wait_idle(s, wait_deadline_ns))
    {
        atomic_fetch_add_explicit(&executor->compose_wait_timeouts_total, 1u, memory_order_relaxed);
        return false;
    }
    return true;
}

/** Accumulate one session's view metadata into the gateway's cumulative
 *  view totals at its delivery-pass visit. Reactor thread only (the
 *  delivery section of the tick pass), so the adds are relaxed atomics
 *  purely to keep the cross-thread accessors well-defined; the
 *  high-watermarks CAS-max because a monotone peak has no single-writer
 *  shape. Called for sessions holding a view — a viewless session has no
 *  metadata to sum (its zeros would dilute the density ratio, not
 *  measure it). */
static void gateway_tick_view_totals_fold(kith_gateway_t *gateway,
                                          const struct kith_gateway_session *s)
{
    atomic_fetch_add_explicit(&gateway->view_visits_total, 1u, memory_order_relaxed);
    const uint64_t candidates = s->view.meta.candidate_count;
    const uint64_t selected = s->view.meta.selected_count;
    atomic_fetch_add_explicit(&gateway->view_candidate_total, candidates, memory_order_relaxed);
    atomic_fetch_add_explicit(&gateway->view_selected_total, selected, memory_order_relaxed);
    uint64_t prev =
        atomic_load_explicit(&gateway->view_candidate_high_watermark, memory_order_relaxed);
    while (candidates > prev &&
           !atomic_compare_exchange_weak_explicit(&gateway->view_candidate_high_watermark,
                                                  &prev,
                                                  candidates,
                                                  memory_order_relaxed,
                                                  memory_order_relaxed))
    {
        // prev refreshed by the failed CAS; loop until the peak holds.
    }
    prev = atomic_load_explicit(&gateway->view_selected_high_watermark, memory_order_relaxed);
    while (selected > prev &&
           !atomic_compare_exchange_weak_explicit(&gateway->view_selected_high_watermark,
                                                  &prev,
                                                  selected,
                                                  memory_order_relaxed,
                                                  memory_order_relaxed))
    {
        // prev refreshed by the failed CAS; loop until the peak holds.
    }
}

/** Deliver one pass for the session: submitted to the executor in executor
 *  mode (in-flight skips and queue exhaustion are counted there and are
 *  never an inline fallback nor an error), inline otherwise with the phase
 *  timing accumulated into @p out_deliver_ns. Returns the inline deliver's
 *  non-ESTATE error for first_err propagation; executor submissions always
 *  report 0 (the worker's deliver code is discarded). */
static int gateway_tick_deliver_session(kith_gateway_t *gateway,
                                        struct gateway_delivery_executor *executor,
                                        struct kith_gateway_session *s,
                                        uint64_t now_ms,
                                        uint64_t *out_deliver_ns)
{
    if (s->view.has_view)
    {
        gateway_tick_view_totals_fold(gateway, s);
    }
    if (executor)
    {
        gateway_delivery_executor_deliver(gateway, executor, s, now_ms);
        return 0;
    }
    uint64_t deliver_start = gateway_now_ns();
    int drc = kith_gateway_deliver(gateway, s, now_ms, nullptr);
    *out_deliver_ns += gateway_now_ns() - deliver_start;
    if (drc != 0 && drc != kith_error_return(KITH_ESTATE))
    {
        return drc;
    }
    return 0;
}

/** Refresh one session's view inside the tick's pass and accumulate the
 *  compose timing. Returns true when the pass skips the session entirely
 *  (the refresh reported ESTATE or a propagated error); @p out_err
 *  receives the error to propagate as the pass's first failure (0 when
 *  the skip is the benign ESTATE). */
static bool gateway_tick_compose_session(kith_gateway_t *gateway,
                                         struct kith_gateway_session *s,
                                         uint64_t now_ms,
                                         uint64_t *out_compose_ns,
                                         int *out_err)
{
    uint64_t compose_start = gateway_now_ns();
    int vrc = kith_gateway_view_refresh(gateway, s, now_ms);
    *out_compose_ns += gateway_now_ns() - compose_start;
    if (vrc == 0)
    {
        return false;
    }
    if (vrc != kith_error_return(KITH_ESTATE) && *out_err == 0)
    {
        *out_err = vrc;
    }
    return true;
}
/** Mutable state threaded through one pass's session visits. */
struct gateway_tick_pass
{
    /** The delivery executor, or NULL for inline delivery. */
    struct gateway_delivery_executor *executor;
    /** When the pass's compose-wait budget expires (monotonic ns). */
    uint64_t wait_deadline_ns;
    /** Latched once the compose budget ran out this pass. */
    bool budget_exhausted;
    /** Compositions so far this pass (head-of-pass guarantee). */
    uint32_t composed;
    /** Set at the first deferred session; drives the rotation cursor. */
    bool deferred_any;
    /** Start index for the next pass. */
    size_t next_start;
    /** Accumulated compose phase nanoseconds. */
    uint64_t compose_ns;
    /** Accumulated deliver phase nanoseconds (inline only; executor jobs
     *  report through the pending accumulator). */
    uint64_t deliver_ns;
    /** Bound sessions whose window stayed empty through this pass (the
     *  seed-failure census; published as the pass-end mirror gauge). */
    uint32_t without_cells;
};

/** Visit one session in the tick's pass: compose (the compose budget and,
 *  in executor mode, the per-pass wait budget permitting) and deliver
 *  (inline or submitted). Returns the error to propagate as the pass's
 *  first failure (0 when none). Reactor thread only. */
static int gateway_tick_visit_session(kith_gateway_t *gateway,
                                      struct gateway_tick_pass *pass,
                                      uint64_t pass_start_ns,
                                      uint64_t budget_ns,
                                      struct kith_gateway_session *s,
                                      size_t bucket_index,
                                      uint64_t now_ms)
{
    // The retry-and-census pass runs before composition so a window add
    // landed here participates in this pass's compose: the subscribe-time
    // reconcile populates the cell and the view scan finds it immediately.
    size_t without_cells = 0u;
    gateway_window_tick_pass(s, &without_cells);
    pass->without_cells += (uint32_t)without_cells;
    if (gateway_tick_should_compose(
            s, pass_start_ns, budget_ns, &pass->budget_exhausted, &pass->composed))
    {
        // Inline mode never evaluates the wait (executor is NULL), so its
        // path reads no executor state at all.
        if (pass->executor && !gateway_tick_compose_wait(pass->executor, s, pass->wait_deadline_ns))
        {
            // The in-flight deliver outlived the pass's wait budget: the
            // session keeps its retained set (delivered below) and leads
            // the next pass; the head-of-pass guarantee passes to the next
            // eligible session because composed stays at its current count
            // here.
            if (!pass->deferred_any)
            {
                pass->deferred_any = true;
                pass->next_start = bucket_index;
            }
        }
        else
        {
            ++pass->composed;
            int compose_err = 0;
            if (gateway_tick_compose_session(gateway, s, now_ms, &pass->compose_ns, &compose_err))
            {
                return compose_err;
            }
        }
    }
    else
    {
        if (!pass->deferred_any)
        {
            pass->deferred_any = true;
            pass->next_start = bucket_index;
        }
        atomic_fetch_add_explicit(&gateway->compose_deferrals_total, 1u, memory_order_relaxed);
    }
    return gateway_tick_deliver_session(gateway, pass->executor, s, now_ms, &pass->deliver_ns);
}

/** Run the budgeted per-session compose-and-deliver pass over the session
 *  table and accumulate @p out_compose_ns / @p out_deliver_ns. The pass
 *  starts at the gateway's rotating cursor; a session past the compose
 *  budget delivers its retained view set unchanged, and the first session
 *  a pass defers leads the next pass. Returns the first per-session
 *  non-ESTATE failure, or 0. Reactor thread only. */
static int gateway_tick_session_pass(kith_gateway_t *gateway,
                                     uint64_t now_ms,
                                     uint64_t *out_compose_ns,
                                     uint64_t *out_deliver_ns)
{
    const struct gateway_session_table *t = &gateway->sessions;
    const size_t buckets = t->buckets;
    if (buckets == 0u)
    {
        *out_compose_ns = 0u;
        *out_deliver_ns = 0u;
        return 0;
    }
    // The composition pass runs under a soft wall-time budget: a dirty tick
    // whose invalidations make every session recompose costs far more than
    // one tick's worth of reactor time, and those ticks arrive back to back,
    // which is how a bursty fabric publish wave turns into consecutive late
    // delivery ticks. Past the budget a session with a retained view set
    // delivers that set unchanged instead of recomposing — frame cadence per
    // session is untouched when delivery runs inline or keeps up, the view
    // converges once its turn comes around. Under the delivery executor
    // a session whose previous deliver is still in flight
    // composes only within the pass's wait budget: composing overwrites
    // the view the worker is reading. An expired budget skips that
    // session's composition and delivery for the tick — a bounded cadence
    // gap, staleness bounded by the 1 s max-gap backstop.
    const uint64_t pass_start_ns = gateway_now_ns();
    const uint64_t budget_ns = (uint64_t)gateway->compose_budget_us * 1000u;
    // One wait deadline for the whole pass, not one per wait: a per-wait
    // bound amplifies to N times the bound of reactor stall under
    // multi-session saturation. The first in-flight compose may consume
    // the entire budget (an accepted fairness loss); once the
    // deadline passes, the remaining in-flight sessions skip the wait
    // instantly.
    struct gateway_tick_pass pass = {
        .executor = gateway->delivery_executor,
        .wait_deadline_ns = pass_start_ns + (uint64_t)gateway->delivery_wait_budget_us * 1000u,
        .budget_exhausted = false,
        .composed = 0u,
        .deferred_any = false,
        // Next pass starts at the first session THIS pass deferred, so
        // deferred work leads the sweep; with nothing deferred the start
        // advances one slot for plain round-robin.
        .next_start = (gateway->sched_cursor % buckets + 1u) % buckets,
        .compose_ns = 0u,
        .deliver_ns = 0u,
        .without_cells = 0u,
    };
    int first_err = 0;
    const size_t start = gateway->sched_cursor % buckets;
    for (size_t n = 0u; n < buckets; ++n)
    {
        const size_t i = (start + n) % buckets;
        struct kith_gateway_session *s = t->slots[i];
        if (!s)
        {
            continue;
        }
        int err =
            gateway_tick_visit_session(gateway, &pass, pass_start_ns, budget_ns, s, i, now_ms);
        if (err != 0 && first_err == 0)
        {
            first_err = err;
        }
    }
    gateway->sched_cursor = pass.next_start;
    // Publish the seed-failure census: the pass is the one reactor-thread
    // walk that already touches every session's window state, so the
    // mirror gauge is exact as of this pass.
    atomic_store_explicit(
        &gateway->sessions_without_cells, pass.without_cells, memory_order_release);
    *out_compose_ns = pass.compose_ns;
    *out_deliver_ns = pass.deliver_ns;
    return first_err;
}

[[nodiscard]] KITH_API int kith_gateway_tick(kith_gateway_t *gateway, uint64_t now_ms)
{
    if (!gateway)
    {
        return kith_error_return(KITH_EINVAL);
    }
    // Bracket each tick phase against CLOCK_MONOTONIC and accumulate the
    // per-phase ns into reactor-thread locals, then publish the totals as
    // relaxed atomic adds once per tick. The composition root samples the
    // cumulative atomics and records the deltas, so a
    // /metrics scrape yields the per-phase ns/sec (ms after /1e6) the
    // capacity model reconciles against the tick budget. The per-session
    // bracketing keeps the loop's interleaving (compose, deliver, next
    // session) unchanged — the measurement is additive and perturbs no
    // behavior.
    uint64_t refresh_start = gateway_now_ns();
    int rc = kith_gateway_cache_refresh(gateway, now_ms);
    uint64_t refresh_ns = gateway_now_ns() - refresh_start;
    if (rc != 0)
    {
        atomic_fetch_add_explicit(&gateway->refresh_ns_total, refresh_ns, memory_order_relaxed);
        return rc;
    }
    // Pending broadcasts drain ahead of the session pass, so a request
    // submitted since the last tick rides the connection ahead of this
    // pass's composed frames, and event fanout never waits behind a
    // compose deferral.
    gateway_broadcast_drain(gateway);
    uint64_t compose_ns = 0u;
    uint64_t deliver_ns = 0u;
    int first_err = gateway_tick_session_pass(gateway, now_ms, &compose_ns, &deliver_ns);
    if (gateway->delivery_executor)
    {
        // Fold the executor's worker-accumulated deliver time into this
        // tick's phase total: monotone overall, with a one-tick skew for
        // jobs finishing after the drain.
        deliver_ns += atomic_exchange_explicit(
            &gateway->delivery_executor->deliver_ns_pending, 0u, memory_order_relaxed);
    }
    atomic_fetch_add_explicit(&gateway->refresh_ns_total, refresh_ns, memory_order_relaxed);
    atomic_fetch_add_explicit(&gateway->compose_ns_total, compose_ns, memory_order_relaxed);
    atomic_fetch_add_explicit(&gateway->deliver_ns_total, deliver_ns, memory_order_relaxed);
    return first_err;
}

/*---------------------------------------------------------------------------
 * statistics accessors
 *-------------------------------------------------------------------------*/

[[nodiscard]] KITH_API int kith_gateway_phase_stats(const kith_gateway_t *gateway,
                                                    kith_gateway_phase_stats_t *out_stats)
{
    if (!gateway || !out_stats)
    {
        return kith_error_return(KITH_EINVAL);
    }
    out_stats->refresh_ns_total =
        atomic_load_explicit(&gateway->refresh_ns_total, memory_order_relaxed);
    out_stats->compose_ns_total =
        atomic_load_explicit(&gateway->compose_ns_total, memory_order_relaxed);
    out_stats->deliver_ns_total =
        atomic_load_explicit(&gateway->deliver_ns_total, memory_order_relaxed);
    out_stats->dispatch_total =
        atomic_load_explicit(&gateway->dispatch_total, memory_order_relaxed);
    out_stats->self_echo_stamps_total =
        atomic_load_explicit(&gateway->self_echo_stamps_total, memory_order_relaxed);
    out_stats->self_echo_fallbacks_total =
        atomic_load_explicit(&gateway->self_echo_fallbacks_total, memory_order_relaxed);
    return 0;
}

[[nodiscard]] KITH_API int kith_gateway_compose_stats(const kith_gateway_t *gateway,
                                                      kith_gateway_compose_stats_t *out_stats)
{
    if (!gateway || !out_stats)
    {
        return kith_error_return(KITH_EINVAL);
    }
    out_stats->window_ns_total =
        atomic_load_explicit(&gateway->compose_window_ns_total, memory_order_relaxed);
    out_stats->prior_ns_total =
        atomic_load_explicit(&gateway->compose_prior_ns_total, memory_order_relaxed);
    out_stats->lock_wait_ns_total =
        atomic_load_explicit(&gateway->compose_lock_wait_ns_total, memory_order_relaxed);
    out_stats->scan_ns_total =
        atomic_load_explicit(&gateway->compose_scan_ns_total, memory_order_relaxed);
    out_stats->sort_ns_total =
        atomic_load_explicit(&gateway->compose_sort_ns_total, memory_order_relaxed);
    out_stats->select_ns_total =
        atomic_load_explicit(&gateway->compose_select_ns_total, memory_order_relaxed);
    return 0;
}

[[nodiscard]] KITH_API int kith_gateway_compose_skips(const kith_gateway_t *gateway,
                                                      uint64_t *out_skips)
{
    if (!gateway || !out_skips)
    {
        return kith_error_return(KITH_EINVAL);
    }
    *out_skips = atomic_load_explicit(&gateway->compose_skips_total, memory_order_relaxed);
    return 0;
}

[[nodiscard]] KITH_API int kith_gateway_compose_deferrals(const kith_gateway_t *gateway,
                                                          uint64_t *out_deferrals)
{
    if (!gateway || !out_deferrals)
    {
        return kith_error_return(KITH_EINVAL);
    }
    *out_deferrals = atomic_load_explicit(&gateway->compose_deferrals_total, memory_order_relaxed);
    return 0;
}

[[nodiscard]] KITH_API int kith_gateway_broadcast_refusals(const kith_gateway_t *gateway,
                                                           uint64_t *out_refusals)
{
    if (!gateway || !out_refusals)
    {
        return kith_error_return(KITH_EINVAL);
    }
    *out_refusals = atomic_load_explicit(&gateway->broadcast_refused_total, memory_order_relaxed);
    return 0;
}

[[nodiscard]] KITH_API int kith_gateway_broadcast_drops(const kith_gateway_t *gateway,
                                                        uint64_t *out_drops)
{
    if (!gateway || !out_drops)
    {
        return kith_error_return(KITH_EINVAL);
    }
    *out_drops = atomic_load_explicit(&gateway->broadcast_dropped_total, memory_order_relaxed);
    return 0;
}

[[nodiscard]] KITH_API int kith_gateway_view_locate_failures(const kith_gateway_t *gateway,
                                                             uint64_t *out_locate_failures)
{
    if (!gateway || !out_locate_failures)
    {
        return kith_error_return(KITH_EINVAL);
    }
    *out_locate_failures =
        atomic_load_explicit(&gateway->view_locate_failures_total, memory_order_relaxed);
    return 0;
}

[[nodiscard]] KITH_API int kith_gateway_window_add_failures(const kith_gateway_t *gateway,
                                                            uint64_t *out_failures)
{
    if (!gateway || !out_failures)
    {
        return kith_error_return(KITH_EINVAL);
    }
    *out_failures = atomic_load_explicit(&gateway->window_add_failures, memory_order_acquire);
    return 0;
}

[[nodiscard]] KITH_API int kith_gateway_window_retry_adds(const kith_gateway_t *gateway,
                                                          uint64_t *out_retry_adds)
{
    if (!gateway || !out_retry_adds)
    {
        return kith_error_return(KITH_EINVAL);
    }
    *out_retry_adds = atomic_load_explicit(&gateway->window_retry_adds, memory_order_acquire);
    return 0;
}

[[nodiscard]] KITH_API int kith_gateway_window_retries_pending(const kith_gateway_t *gateway,
                                                               uint64_t *out_pending)
{
    if (!gateway || !out_pending)
    {
        return kith_error_return(KITH_EINVAL);
    }
    *out_pending = atomic_load_explicit(&gateway->window_retries_pending, memory_order_acquire);
    return 0;
}

[[nodiscard]] KITH_API int kith_gateway_sessions_without_cells(const kith_gateway_t *gateway,
                                                               uint64_t *out_sessions)
{
    if (!gateway || !out_sessions)
    {
        return kith_error_return(KITH_EINVAL);
    }
    *out_sessions = atomic_load_explicit(&gateway->sessions_without_cells, memory_order_acquire);
    return 0;
}

[[nodiscard]] KITH_API int
kith_gateway_delivery_executor_stats(const kith_gateway_t *gateway,
                                     kith_gateway_delivery_executor_stats_t *out_stats)
{
    if (!gateway || !out_stats)
    {
        return kith_error_return(KITH_EINVAL);
    }
    if (!gateway->delivery_executor)
    {
        return kith_error_return(KITH_ESTATE);
    }
    gateway_delivery_executor_stats_fill(gateway->delivery_executor, out_stats);
    return 0;
}

[[nodiscard]] KITH_API int kith_gateway_delivery_totals(const kith_gateway_t *gateway,
                                                        kith_gateway_delivery_totals_t *out_totals)
{
    if (!gateway || !out_totals)
    {
        return kith_error_return(KITH_EINVAL);
    }
    out_totals->enqueued =
        atomic_load_explicit(&gateway->delivery_enqueued_total, memory_order_relaxed);
    out_totals->dropped =
        atomic_load_explicit(&gateway->delivery_dropped_total, memory_order_relaxed);
    out_totals->events_enqueued =
        atomic_load_explicit(&gateway->delivery_events_enqueued_total, memory_order_relaxed);
    out_totals->suppressed =
        atomic_load_explicit(&gateway->delivery_suppressed_total, memory_order_relaxed);
    return 0;
}

[[nodiscard]] KITH_API int
kith_gateway_session_delivery_totals(const kith_gateway_session_t *session,
                                     kith_gateway_delivery_totals_t *out_totals)
{
    if (!session || !out_totals)
    {
        return kith_error_return(KITH_EINVAL);
    }
    out_totals->enqueued =
        atomic_load_explicit(&session->delivery_enqueued_total, memory_order_relaxed);
    out_totals->dropped =
        atomic_load_explicit(&session->delivery_dropped_total, memory_order_relaxed);
    out_totals->events_enqueued =
        atomic_load_explicit(&session->delivery_events_enqueued_total, memory_order_relaxed);
    out_totals->suppressed =
        atomic_load_explicit(&session->delivery_suppressed_total, memory_order_relaxed);
    return 0;
}

[[nodiscard]] KITH_API int kith_gateway_view_totals(const kith_gateway_t *gateway,
                                                    kith_gateway_view_totals_t *out_totals)
{
    if (!gateway || !out_totals)
    {
        return kith_error_return(KITH_EINVAL);
    }
    out_totals->visits = atomic_load_explicit(&gateway->view_visits_total, memory_order_relaxed);
    out_totals->candidate_total =
        atomic_load_explicit(&gateway->view_candidate_total, memory_order_relaxed);
    out_totals->selected_total =
        atomic_load_explicit(&gateway->view_selected_total, memory_order_relaxed);
    out_totals->candidate_high_watermark =
        atomic_load_explicit(&gateway->view_candidate_high_watermark, memory_order_relaxed);
    out_totals->selected_high_watermark =
        atomic_load_explicit(&gateway->view_selected_high_watermark, memory_order_relaxed);
    return 0;
}
