/* The delivery executor: off-thread delivery, per-pass compose
 * waits, the wait-idle branch exits, in-flight skips, counted EBUSY,
 * drain-on-destroy teardown, the deliver_ns skew contract, thread-local
 * scratch byte-correctness, and tiered wire parity under the executor.
 * A registered wrapper strategy
 * forwards to a built-in vtable with test-held blocking and captured
 * stats, so every handshake is observed deterministically. */

#include <signal.h>
#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <pthread.h>
#include <unistd.h>

#include "gateway/delivery/delivery.h"
#include "gateway/delivery/executor.h"
#include "gateway/delivery/strategy.h"
#include "gateway/delivery/strategy_tiered.h"
#include "gateway/gateway_internal.h"
#include "kith/fabric/fabric.h"
#include "kith/gateway/gateway.h"
#include "kith/net/net.h"
#include "kith/proto/proto.h"
#include "kith/sim/sim.h"
#include "kith/types.h"
#include "kith/version.h"

#include <netinet/in.h>
#include <sys/socket.h>

static int check_cond(bool ok, int line)
{
    if (!ok)
    {
        (void)fprintf(stderr, "gateway executor: assertion at line %d failed\n", line);
        return 1;
    }
    return 0;
}

#define CHECK(cond) failures += check_cond((cond), __LINE__)

static void sleep_us(uint64_t us)
{
    struct timespec pause = {.tv_sec = (time_t)(us / 1000000u),
                             .tv_nsec = (long)(us % 1000000u) * 1000L};
    nanosleep(&pause, nullptr);
}

static uint64_t now_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

/** Read the executor's counters through the public accessor. */
static kith_gateway_delivery_executor_stats_t ex_stats(kith_gateway_t *gw)
{
    kith_gateway_delivery_executor_stats_t out = {0};
    (void)kith_gateway_delivery_executor_stats(gw, &out);
    return out;
}

/** Spin until the atomic reaches @p target (bounded at 10 s). */
static bool wait_u64_ge(const _Atomic uint64_t *counter, uint64_t target)
{
    const uint64_t deadline = now_ns() + 10000000000ull;
    while (atomic_load_explicit(counter, memory_order_acquire) < target)
    {
        if (now_ns() >= deadline)
        {
            return false;
        }
        sleep_us(100u);
    }
    return true;
}

/*---------------------------------------------------------------------------
 * wrapper strategy: forwarding vtable with test hooks
 *-------------------------------------------------------------------------*/

/** Shared test hooks. One instance per executable; each test resets it.
 *  Delivery stats live per session in tw_state, so concurrent workers
 *  never share the write. The global mirror in workers==1 fixtures pairs
 *  with the completed counter: the worker stores the mirror before the
 *  release add, readers load completed with acquire first. */
struct tw_ctx
{
    const kith_gateway_delivery_vtable_t *real;
    _Atomic uint64_t deliver_entered;
    _Atomic uint64_t completed;
    _Atomic bool hold;
    _Atomic uintptr_t last_deliver_tid;
    uint32_t probe_sleep_ms;
    /** Delivery-worker count of the active fixture (1 or 2); set before
     *  the gateway creates its pool. */
    uint32_t workers;
    kith_gateway_delivery_stats_t last_stats;
};

static struct tw_ctx g_tw = {.real = nullptr,
                             .deliver_entered = 0,
                             .completed = 0,
                             .hold = false,
                             .last_deliver_tid = 0,
                             .probe_sleep_ms = 0u,
                             .workers = 1u,
                             .last_stats = {0}};

static void tw_reset(const kith_gateway_delivery_vtable_t *real, bool hold)
{
    g_tw.real = real;
    atomic_store_explicit(&g_tw.deliver_entered, 0u, memory_order_relaxed);
    atomic_store_explicit(&g_tw.completed, 0u, memory_order_relaxed);
    atomic_store_explicit(&g_tw.hold, hold, memory_order_release);
    atomic_store_explicit(&g_tw.last_deliver_tid, 0u, memory_order_relaxed);
    g_tw.probe_sleep_ms = 0u;
    memset(&g_tw.last_stats, 0, sizeof(g_tw.last_stats));
}

/** The wrapper's per-session state: the real strategy's state plus the
 *  wrapper's own allocation, so forwarding keeps the real per-session
 *  ledger the built-in strategy expects. The executor serializes a
 *  session's in-flight deliveries, so this state's stats field is written
 *  by at most one worker at a time. */
struct tw_state
{
    void *real_state;
    kith_gateway_delivery_stats_t last_stats;
};

static int
tw_session_init(const kith_gateway_session_t *session, const void *config, void **out_state)
{
    struct tw_state *st = calloc(1u, sizeof(*st));
    if (!st)
    {
        return kith_error_return(KITH_ENOMEM);
    }
    int rc = g_tw.real->session_init(session, config, &st->real_state);
    if (rc != 0)
    {
        free(st);
        return rc;
    }
    *out_state = st;
    return 0;
}

static void tw_session_fini(void *state)
{
    struct tw_state *st = state;
    if (st)
    {
        if (g_tw.real->session_fini)
        {
            g_tw.real->session_fini(st->real_state);
        }
        free(st);
    }
}

static int tw_deliver(void *state,
                      kith_gateway_t *gateway,
                      kith_gateway_session_t *session,
                      const kith_gateway_view_subject_t *subjects,
                      size_t subject_count,
                      uint64_t now_ms,
                      kith_gateway_delivery_stats_t *out_stats)
{
    struct tw_state *st = state;
    atomic_store_explicit(&g_tw.last_deliver_tid, (uintptr_t)pthread_self(), memory_order_release);
    atomic_fetch_add_explicit(&g_tw.deliver_entered, 1u, memory_order_acq_rel);
    if (g_tw.probe_sleep_ms > 0u)
    {
        sleep_us((uint64_t)g_tw.probe_sleep_ms * 1000u);
    }
    // The hold is the test's brake on the worker. It self-releases after
    // 10 s so a failing assertion degrades to a failure, not a hang.
    if (atomic_load_explicit(&g_tw.hold, memory_order_acquire))
    {
        const uint64_t deadline = now_ns() + 10000000000ull;
        while (atomic_load_explicit(&g_tw.hold, memory_order_acquire))
        {
            if (now_ns() >= deadline)
            {
                break;
            }
            sleep_us(100u);
        }
    }
    int rc = g_tw.real->deliver(
        st->real_state, gateway, session, subjects, subject_count, now_ms, &st->last_stats);
    if (out_stats)
    {
        *out_stats = st->last_stats;
    }
    // Single-worker fixtures mirror the stats into the shared context for
    // their readers; the one pool thread serializes the mirror, and the
    // completed add below orders it before an acquire reader.
    if (g_tw.workers == 1u)
    {
        g_tw.last_stats = st->last_stats;
    }
    atomic_fetch_add_explicit(&g_tw.completed, 1u, memory_order_acq_rel);
    return rc;
}

static const kith_gateway_delivery_vtable_t tw_vtable = {
    .size = sizeof(tw_vtable),
    .abi_version = KITH_ABI_VERSION,
    .session_init = tw_session_init,
    .session_fini = tw_session_fini,
    .deliver = tw_deliver,
};

/*---------------------------------------------------------------------------
 * fixture
 *-------------------------------------------------------------------------*/

struct fixture
{
    kith_proto_t *proto;
    kith_net_t *net;
    kith_sim_t *sim;
    kith_fabric_t *fabric;
    kith_gateway_t *gw;
    int client_fd;
    kith_net_conn_t *conn;
    uint16_t replication_type_id;
};

/** Shared construction: batch framing, the wrapper strategy, and an
 *  executor of @p workers threads with @p wait_budget_us. @p tiered_cfg
 *  (may be NULL) is forwarded as the delivery config for the tiered
 *  parity test. @p max_sessions sizes the session table and, with it, the
 *  executor's task capacity. */
static int fixture_init_ex(struct fixture *fx,
                           uint32_t workers,
                           uint32_t wait_budget_us,
                           uint32_t max_sessions,
                           const void *tiered_cfg)
{
    memset(fx, 0, sizeof(*fx));
    fx->client_fd = -1;
    if (kith_proto_create(nullptr, nullptr, &fx->proto) != 0)
    {
        return -1;
    }
    fx->replication_type_id = KITH_PROTO_TYPE_USER_BASE;
    if (kith_proto_register_type_id(fx->proto, "replication", fx->replication_type_id) != 0)
    {
        return -1;
    }
    if (kith_net_create(nullptr, fx->proto, nullptr, &fx->net) != 0)
    {
        return -1;
    }
    if (kith_sim_create(nullptr, nullptr, &fx->sim) != 0)
    {
        return -1;
    }
    if (kith_fabric_create(nullptr, fx->sim, nullptr, &fx->fabric) != 0)
    {
        return -1;
    }
    kith_gateway_params_t params = {0};
    params.size = sizeof(params);
    params.abi_version = KITH_ABI_VERSION;
    params.replication_batch_type_id = fx->replication_type_id;
    params.view_refresh_interval_ms = 10u;
    params.cache_refresh_interval_ms = 10u;
    params.max_sessions = max_sessions;
    params.delivery_strategy = "test_wrapper";
    params.delivery_config = tiered_cfg;
    params.delivery_worker_count = workers;
    params.delivery_wait_budget_us = wait_budget_us;
    // Set before the gateway creates its pool: the worker threads read it
    // only after pthread_create publishes it.
    g_tw.workers = workers;
    if (kith_gateway_create(&params, fx->net, fx->fabric, fx->proto, nullptr, &fx->gw) != 0)
    {
        return -1;
    }
    if (kith_gateway_register_delivery(fx->gw, "test_wrapper", &tw_vtable) != 0)
    {
        return -1;
    }
    if (kith_net_listen(fx->net, "127.0.0.1", 0) != 0)
    {
        return -1;
    }
    return 0;
}

static void fixture_fini(struct fixture *fx)
{
    if (fx->client_fd >= 0)
    {
        (void)close(fx->client_fd);
    }
    if (fx->conn)
    {
        kith_net_conn_close(fx->conn);
        kith_net_conn_release(fx->conn);
    }
    kith_gateway_destroy(fx->gw);
    kith_fabric_destroy(fx->fabric);
    kith_sim_destroy(fx->sim);
    kith_net_destroy(fx->net);
    kith_proto_destroy(fx->proto);
}

static int connect_to_listener(kith_net_t *net)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0)
    {
        return -1;
    }
    struct sockaddr_in addr = {0};
    socklen_t addr_len = sizeof(addr);
    if (getsockname(kith_net_listener_fd(net), (struct sockaddr *)&addr, &addr_len) != 0)
    {
        (void)close(fd);
        return -1;
    }
    if (connect(fd, (struct sockaddr *)&addr, addr_len) != 0)
    {
        (void)close(fd);
        return -1;
    }
    return fd;
}

static kith_fabric_cell_key_t
make_key(uint32_t zone, int32_t cx, int32_t cy, int32_t cz, uint8_t lod)
{
    kith_fabric_cell_key_t k = {0};
    k.zone = zone;
    k.cell_x = cx;
    k.cell_y = cy;
    k.cell_z = cz;
    k.lod = lod;
    return k;
}

static kith_sim_artifact_key_t make_skey(const kith_fabric_cell_key_t *fk, uint32_t epoch)
{
    kith_sim_artifact_key_t k = {0};
    k.zone = fk->zone;
    k.cell_x = fk->cell_x;
    k.cell_y = fk->cell_y;
    k.cell_z = fk->cell_z;
    k.lod = fk->lod;
    k.authority_epoch = epoch;
    return k;
}

static kith_sim_actor_t make_actor(uint64_t id, int64_t x, uint32_t tick)
{
    kith_sim_actor_t a = {0};
    a.id = id;
    a.pos_x = x;
    a.input_tick = tick;
    return a;
}

/** Publish the self artifact at the home cell, bind and window the
 *  session, refresh the cache and the view, so the session holds a
 *  composed view set ready for delivery. The connection is owned by the
 *  fixture (closed and released by fixture_fini). */
static int
make_composed_session(struct fixture *fx, kith_gateway_session_t **out_session, uint64_t now_ms)
{
    *out_session = nullptr;
    int fd = connect_to_listener(fx->net);
    if (fd < 0)
    {
        return -1;
    }
    kith_net_conn_t *conn = nullptr;
    if (kith_net_accept(fx->net, &conn) != 0)
    {
        (void)close(fd);
        return -1;
    }
    fx->client_fd = fd;
    fx->conn = conn;
    kith_gateway_session_t *s = nullptr;
    if (kith_gateway_session_create(
            fx->gw, conn, KITH_GATEWAY_SESSION_SUBSCRIBER, 1u, nullptr, &s) != 0)
    {
        return -1;
    }
    *out_session = s;
    if (kith_gateway_session_bind_actor(s, 1u) != 0)
    {
        return -1;
    }
    kith_fabric_cell_key_t home = make_key(7u, 0, 0, 0, 0u);
    if (kith_gateway_session_window_add(s, &home) != 0)
    {
        return -1;
    }
    kith_sim_artifact_key_t sk = make_skey(&home, 1u);
    kith_sim_actor_t sub = make_actor(1u, 0, 0u);
    if (kith_sim_publish_artifact(fx->sim, &sk, &sub, nullptr) != 0)
    {
        return -1;
    }
    // A pair of neighbors so the view carries more than the self subject.
    for (uint64_t n = 2u; n < 4u; ++n)
    {
        kith_sim_actor_t a = make_actor(n, (int64_t)(n * 100u), 0u);
        if (kith_sim_publish_artifact(fx->sim, &sk, &a, nullptr) != 0)
        {
            return -1;
        }
    }
    (void)fx->client_fd;
    if (kith_fabric_publish(fx->fabric, &home, 1u, nullptr) != 0)
    {
        return -1;
    }
    if (kith_gateway_cache_refresh(fx->gw, now_ms) != 0)
    {
        return -1;
    }
    if (kith_gateway_view_refresh(fx->gw, s, now_ms + 100u) != 0)
    {
        return -1;
    }
    return 0;
}

/*---------------------------------------------------------------------------
 * helper thread
 *-------------------------------------------------------------------------*/

/** Release the test hold after a short delay, from a helper thread, so a
 *  blocked deliver finishes while the reactor thread is inside the pass. */
static void *tw_release_hold_later(void *arg)
{
    (void)arg;
    sleep_us(1000u);
    atomic_store_explicit(&g_tw.hold, false, memory_order_release);
    return nullptr;
}

/*---------------------------------------------------------------------------
 * tests
 *-------------------------------------------------------------------------*/

// Executor-driven delivery runs off the tick thread: the wrapper records
// the calling thread, and after one tick it is not the caller's. The
// public stats accessor reads the executor counters and reports ESTATE in
// inline mode.
static int test_executor_off_thread(void)
{
    int failures = 0;
    tw_reset(gateway_delivery_full_vtable(), false);
    struct fixture fx;
    CHECK(fixture_init_ex(&fx, 1u, 8000u, 4096u, nullptr) == 0);

    kith_gateway_delivery_executor_stats_t stats = {0};
    CHECK(kith_gateway_delivery_executor_stats(fx.gw, &stats) == 0);
    CHECK(stats.jobs_submitted_total == 0u);
    CHECK(stats.inflight_current == 0u);

    kith_gateway_session_t *s = nullptr;
    CHECK(make_composed_session(&fx, &s, 1000u) == 0);

    const uintptr_t main_tid = (uintptr_t)pthread_self();
    CHECK(kith_gateway_tick(fx.gw, 1200u) == 0);
    CHECK(wait_u64_ge(&g_tw.completed, 1u));
    CHECK(atomic_load_explicit(&g_tw.last_deliver_tid, memory_order_acquire) != main_tid);

    CHECK(kith_gateway_delivery_executor_stats(fx.gw, &stats) == 0);
    CHECK(stats.jobs_submitted_total == 1u);
    CHECK(stats.inflight_skips_total == 0u);
    CHECK(stats.ebusy_skips_total == 0u);
    CHECK(stats.wait_budget_exhausted_total == 0u);
    CHECK(stats.compose_wait_timeouts_total == 0u);
    CHECK(stats.inflight_high_watermark == 1u);

    // Inline mode reports ESTATE: there is no executor to read.
    struct fixture inl;
    CHECK(fixture_init_ex(&inl, 0u, 8000u, 4096u, nullptr) == 0);
    CHECK(kith_gateway_delivery_executor_stats(inl.gw, &stats) == kith_error_return(KITH_ESTATE));
    fixture_fini(&inl);

    kith_gateway_session_destroy(s);
    fixture_fini(&fx);
    return failures;
}

/* The executor's in-flight accounting is sampled across delivery windows:
 * a held first window guarantees samples while the job is in flight, and
 * fast windows follow. The gauge is unsigned, so an accounting underflow
 * reads as a huge value — the per-sample bound catches it live, and the
 * high watermark, which absorbs any wrapped value, keeps the settled
 * ceiling even for windows too fast to sample. The worker decrements the
 * gauge after the strategy call returns, so each settled read settles on
 * the executor's per-session idle signal first: its acquire load orders
 * the worker's decrement before the gauge read. */
static int test_executor_accounting_invariants(void)
{
    int failures = 0;
    tw_reset(gateway_delivery_full_vtable(), true);
    struct fixture fx;
    CHECK(fixture_init_ex(&fx, 1u, 8000u, 4096u, nullptr) == 0);

    kith_gateway_session_t *s = nullptr;
    CHECK(make_composed_session(&fx, &s, 1000u) == 0);

    kith_gateway_delivery_executor_stats_t prev = {0};
    CHECK(kith_gateway_delivery_executor_stats(fx.gw, &prev) == 0);

    /* Held window: the job sits in flight while the driver samples it. */
    CHECK(kith_gateway_tick(fx.gw, 1200u) == 0);
    CHECK(wait_u64_ge(&g_tw.deliver_entered, 1u));
    for (unsigned i = 0u; i < 8u; ++i)
    {
        kith_gateway_delivery_executor_stats_t held = {0};
        CHECK(kith_gateway_delivery_executor_stats(fx.gw, &held) == 0);
        CHECK(held.inflight_current == 1u);
        CHECK(held.inflight_high_watermark == 1u);
        sleep_us(100u);
    }
    atomic_store_explicit(&g_tw.hold, false, memory_order_release);
    CHECK(wait_u64_ge(&g_tw.completed, 1u));
    CHECK(gateway_delivery_executor_wait_idle(s, now_ns() + 10000000000ull));
    CHECK(kith_gateway_delivery_executor_stats(fx.gw, &prev) == 0);
    CHECK(prev.inflight_current == 0u);

    /* Fast windows: the bound and the monotone watermark hold on every
     * caught sample, and the settled gauge returns to zero each round. */
    for (unsigned round = 0u; round < 32u; ++round)
    {
        CHECK(kith_gateway_tick(fx.gw, 1300u + round) == 0);
        const uint64_t deadline = now_ns() + 10000000000ull;
        while (atomic_load_explicit(&g_tw.completed, memory_order_acquire) < round + 2u)
        {
            kith_gateway_delivery_executor_stats_t sample = {0};
            CHECK(kith_gateway_delivery_executor_stats(fx.gw, &sample) == 0);
            CHECK(sample.inflight_current <= sample.jobs_submitted_total);
            CHECK(sample.inflight_high_watermark >= prev.inflight_high_watermark);
            prev = sample;
            CHECK(now_ns() < deadline);
            sleep_us(100u);
        }
        CHECK(gateway_delivery_executor_wait_idle(s, now_ns() + 10000000000ull));
        kith_gateway_delivery_executor_stats_t settled = {0};
        CHECK(kith_gateway_delivery_executor_stats(fx.gw, &settled) == 0);
        CHECK(settled.inflight_current == 0u);
        CHECK(settled.inflight_high_watermark <= settled.jobs_submitted_total);
    }

    kith_gateway_session_destroy(s);
    fixture_fini(&fx);
    return failures;
}

// A blocked deliver released by a helper thread inside the pass budget:
// the compose-wait succeeds, the compose runs, and no timeout counter
// moves. The delivery section sees the flag clear and submits a second
// job — the fresh view delivers.
static int test_executor_compose_wait_success(void)
{
    int failures = 0;
    tw_reset(gateway_delivery_full_vtable(), true);
    struct fixture fx;
    CHECK(fixture_init_ex(&fx, 1u, 8000u, 4096u, nullptr) == 0);

    kith_gateway_session_t *s = nullptr;
    CHECK(make_composed_session(&fx, &s, 1000u) == 0);

    // Tick one leaves a blocked deliver in flight.
    CHECK(kith_gateway_tick(fx.gw, 1200u) == 0);
    CHECK(wait_u64_ge(&g_tw.deliver_entered, 1u));

    pthread_t releaser;
    CHECK(pthread_create(&releaser, nullptr, tw_release_hold_later, nullptr) == 0);
    // The helper releases after ~1 ms; the pass budget is 8 ms, so the
    // wait succeeds well inside it.

    CHECK(kith_gateway_tick(fx.gw, 1500u) == 0);
    CHECK(ex_stats(fx.gw).compose_wait_timeouts_total == 0u);
    CHECK(ex_stats(fx.gw).wait_budget_exhausted_total == 0u);
    CHECK(ex_stats(fx.gw).inflight_skips_total == 0u);
    CHECK(wait_u64_ge(&g_tw.completed, 2u));
    CHECK(ex_stats(fx.gw).jobs_submitted_total == 2u);

    (void)pthread_join(releaser, nullptr);
    kith_gateway_session_destroy(s);
    fixture_fini(&fx);
    return failures;
}

// A budget smaller than the hold: the blocked visit skips the
// composition (the retained snapshot stays) and counts one cause — a
// visit inside the deadline draws the wait to a timeout, a visit after
// the spent deadline skips as exhausted — and the delivery section
// counts the in-flight skip instead of submitting a second concurrent
// deliver.
static int test_executor_compose_wait_timeout(void)
{
    int failures = 0;
    tw_reset(gateway_delivery_full_vtable(), true);
    struct fixture fx;
    CHECK(fixture_init_ex(&fx, 1u, 50u, 4096u, nullptr) == 0);

    kith_gateway_session_t *s = nullptr;
    CHECK(make_composed_session(&fx, &s, 1000u) == 0);

    kith_gateway_view_snapshot_t before = {0};
    CHECK(kith_gateway_view_snapshot(fx.gw, s, &before, nullptr, 0u, nullptr) == 0);

    CHECK(kith_gateway_tick(fx.gw, 1200u) == 0);
    CHECK(wait_u64_ge(&g_tw.deliver_entered, 1u));
    CHECK(ex_stats(fx.gw).compose_wait_timeouts_total == 0u);

    // The next tick's compose waits 50 us against a held deliver and
    // expires: the composition is skipped and the pass moves on.
    CHECK(kith_gateway_tick(fx.gw, 1500u) == 0);
    const kith_gateway_delivery_executor_stats_t stats = ex_stats(fx.gw);
    CHECK(stats.wait_budget_exhausted_total + stats.compose_wait_timeouts_total == 1u);
    CHECK(stats.inflight_skips_total == 1u);
    CHECK(atomic_load_explicit(&g_tw.deliver_entered, memory_order_acquire) == 1u);

    // The retained view was not overwritten by a recomposition.
    kith_gateway_view_snapshot_t after = {0};
    CHECK(kith_gateway_view_snapshot(fx.gw, s, &after, nullptr, 0u, nullptr) == 0);
    CHECK(after.built_at_ms == before.built_at_ms);

    atomic_store_explicit(&g_tw.hold, false, memory_order_release);
    CHECK(wait_u64_ge(&g_tw.completed, 1u));
    kith_gateway_session_destroy(s);
    fixture_fini(&fx);
    return failures;
}

// The wait-idle branch exits, off the pass's clock: against a held
// deliver, a deadline inside the hold returns false (the timeout exit)
// and a deadline outlasting the helper's release returns true once the
// job completes and the in-flight flag clears (the idle exit).
static int test_executor_wait_idle_branches(void)
{
    int failures = 0;
    tw_reset(gateway_delivery_full_vtable(), true);
    struct fixture fx;
    CHECK(fixture_init_ex(&fx, 1u, 8000u, 4096u, nullptr) == 0);

    kith_gateway_session_t *s = nullptr;
    CHECK(make_composed_session(&fx, &s, 1000u) == 0);

    CHECK(kith_gateway_tick(fx.gw, 1200u) == 0);
    CHECK(wait_u64_ge(&g_tw.deliver_entered, 1u));

    CHECK(!gateway_delivery_executor_wait_idle(s, now_ns() + 2000000ull));

    pthread_t releaser;
    CHECK(pthread_create(&releaser, nullptr, tw_release_hold_later, nullptr) == 0);
    CHECK(gateway_delivery_executor_wait_idle(s, now_ns() + 10000000000ull));
    CHECK(wait_u64_ge(&g_tw.completed, 1u));
    (void)pthread_join(releaser, nullptr);

    kith_gateway_session_destroy(s);
    fixture_fini(&fx);
    return failures;
}

// Budget-spent in-flight skips: with a 1 us pass budget and a held
// deliver, each in-flight visit skips the wait and the composition — a
// visit inside the deadline draws it to a timeout, one after the spent
// deadline skips as exhausted, at most one timeout per pass (one
// deadline for the whole pass) — and the delivery section counts the
// skip instead of submitting a second concurrent deliver. The
// compose-skip counter stays flat across the tick: nothing changes
// between the ticks, so even a wrongly-run compose skip-matches the
// retained view and moves the counter the probe reads.
static int test_executor_budget_deferred_skips(void)
{
    int failures = 0;
    tw_reset(gateway_delivery_full_vtable(), true);
    struct fixture fx;
    CHECK(fixture_init_ex(&fx, 1u, 1u, 4096u, nullptr) == 0);

    kith_gateway_session_t *a = nullptr;
    kith_gateway_session_t *b = nullptr;
    CHECK(make_composed_session(&fx, &a, 1000u) == 0);
    CHECK(make_composed_session(&fx, &b, 1000u) == 0);

    CHECK(kith_gateway_tick(fx.gw, 1200u) == 0);
    CHECK(wait_u64_ge(&g_tw.deliver_entered, 1u));

    uint64_t compose_skips_before = 0u;
    CHECK(kith_gateway_compose_skips(fx.gw, &compose_skips_before) == 0);
    CHECK(kith_gateway_tick(fx.gw, 1500u) == 0);
    const kith_gateway_delivery_executor_stats_t stats = ex_stats(fx.gw);
    CHECK(stats.wait_budget_exhausted_total + stats.compose_wait_timeouts_total == 2u);
    CHECK(stats.compose_wait_timeouts_total <= 1u);
    CHECK(stats.inflight_skips_total == 2u);

    uint64_t compose_skips_after = 0u;
    CHECK(kith_gateway_compose_skips(fx.gw, &compose_skips_after) == 0);
    CHECK(compose_skips_after == compose_skips_before);
    CHECK(atomic_load_explicit(&g_tw.deliver_entered, memory_order_acquire) == 1u);

    atomic_store_explicit(&g_tw.hold, false, memory_order_release);
    CHECK(wait_u64_ge(&g_tw.completed, 2u));
    kith_gateway_session_destroy(a);
    kith_gateway_session_destroy(b);
    fixture_fini(&fx);
    return failures;
}

// Per-pass budget, not per-wait: one deadline bounds the whole pass, so
// at most one in-flight visit draws a wait (to the deadline, a timeout)
// and the rest skip as exhausted — never one wait per session. The pass
// costs one budget plus slack, not sessions_n budgets.
static int test_executor_wait_budget_amplification(void)
{
    int failures = 0;
    const uint32_t sessions_n = 4u;
    tw_reset(gateway_delivery_full_vtable(), true);
    struct fixture fx;
    CHECK(fixture_init_ex(&fx, 2u, 20000u, 4096u, nullptr) == 0);

    kith_gateway_session_t *ss[4];
    for (uint32_t i = 0; i < sessions_n; ++i)
    {
        ss[i] = nullptr;
        CHECK(make_composed_session(&fx, &ss[i], 1000u) == 0);
    }

    CHECK(kith_gateway_tick(fx.gw, 1200u) == 0);
    // Both workers hold a blocked deliver before the measured tick, so
    // every session is in flight (the flag is set at submit) when it
    // visits.
    CHECK(wait_u64_ge(&g_tw.deliver_entered, 2u));

    const uint64_t tick_start = now_ns();
    CHECK(kith_gateway_tick(fx.gw, 1500u) == 0);
    const uint64_t elapsed_ns = now_ns() - tick_start;

    // At most one wait runs (the shared deadline plus one poll
    // interval); every other visit skips instantly, so the tick costs
    // one budget plus slack. The ceiling sits far above that slack but
    // far below a per-session-budget amplification, with margin for a
    // loaded host.
    CHECK(elapsed_ns < 80000000ull);
    const kith_gateway_delivery_executor_stats_t stats = ex_stats(fx.gw);
    CHECK(stats.compose_wait_timeouts_total <= 1u);
    CHECK(stats.wait_budget_exhausted_total + stats.compose_wait_timeouts_total ==
          (uint64_t)sessions_n);
    CHECK(stats.inflight_skips_total == sessions_n);
    CHECK(atomic_load_explicit(&g_tw.deliver_entered, memory_order_acquire) == 2u);

    atomic_store_explicit(&g_tw.hold, false, memory_order_release);
    CHECK(wait_u64_ge(&g_tw.completed, (uint64_t)sessions_n));
    for (uint32_t i = 0; i < sessions_n; ++i)
    {
        kith_gateway_session_destroy(ss[i]);
    }
    fixture_fini(&fx);
    return failures;
}

// EBUSY is a counted skip, never an inline fallback: with both task nodes
// held by blocked jobs, a whitebox resubmit against a manually cleared
// flag fails, leaves the flag false, restores the refcount, and never
// re-enters deliver on the reactor thread.
static int test_executor_ebusy_counted_skip(void)
{
    int failures = 0;
    tw_reset(gateway_delivery_full_vtable(), true);
    struct fixture fx;
    // max_sessions = 2 sizes the executor's task capacity to 2 nodes.
    CHECK(fixture_init_ex(&fx, 2u, 8000u, 2u, nullptr) == 0);

    kith_gateway_session_t *a = nullptr;
    kith_gateway_session_t *b = nullptr;
    CHECK(make_composed_session(&fx, &a, 1000u) == 0);
    CHECK(make_composed_session(&fx, &b, 1000u) == 0);

    CHECK(kith_gateway_tick(fx.gw, 1200u) == 0);
    // Both workers grab both nodes and block: the free list is empty.
    CHECK(wait_u64_ge(&g_tw.deliver_entered, 2u));
    CHECK(ex_stats(fx.gw).jobs_submitted_total == 2u);
    // The in-flight jobs each hold a submit reference on top of the owner's.
    const int refcount_before = atomic_load_explicit(&a->refcount, memory_order_relaxed);

    struct gateway_delivery_executor *executor = fx.gw->delivery_executor;
    CHECK(executor != nullptr);
    atomic_store_explicit(&a->delivery_in_flight, false, memory_order_release);
    gateway_delivery_executor_deliver(fx.gw, executor, a, 1500u);

    CHECK(ex_stats(fx.gw).ebusy_skips_total == 1u);
    CHECK(atomic_load_explicit(&a->delivery_in_flight, memory_order_acquire) == false);
    CHECK(atomic_load_explicit(&a->refcount, memory_order_relaxed) == refcount_before);
    CHECK(atomic_load_explicit(&g_tw.deliver_entered, memory_order_acquire) == 2u);

    atomic_store_explicit(&g_tw.hold, false, memory_order_release);
    CHECK(wait_u64_ge(&g_tw.completed, 2u));
    kith_gateway_session_destroy(a);
    kith_gateway_session_destroy(b);
    fixture_fini(&fx);
    return failures;
}

// Teardown drains an in-flight job: destroy blocks until the held deliver
// completes, then returns.
static int test_executor_teardown_drains(void)
{
    int failures = 0;
    tw_reset(gateway_delivery_full_vtable(), true);
    struct fixture fx;
    CHECK(fixture_init_ex(&fx, 1u, 8000u, 4096u, nullptr) == 0);

    kith_gateway_session_t *s = nullptr;
    CHECK(make_composed_session(&fx, &s, 1000u) == 0);
    CHECK(kith_gateway_tick(fx.gw, 1200u) == 0);
    CHECK(wait_u64_ge(&g_tw.deliver_entered, 1u));

    pthread_t releaser;
    CHECK(pthread_create(&releaser, nullptr, tw_release_hold_later, nullptr) == 0);
    // The owner reference drops here while the job still holds its submit
    // reference: the session memory survives until the job completes, and
    // a mid-drain release frees a detached session — the executor's
    // drain-on-destroy teardown path.
    kith_gateway_session_destroy(s);
    // Destroy drains the executor before its own teardown: it returns
    // only after the held job finished.
    kith_gateway_destroy(fx.gw);
    fx.gw = nullptr;
    CHECK(atomic_load_explicit(&g_tw.completed, memory_order_acquire) == 1u);
    (void)pthread_join(releaser, nullptr);

    fixture_fini(&fx);
    return failures;
}

// deliver_ns accounting: a probe sleep inside the worker's deliver does
// not appear in deliver_ns_total at the tick that submitted it (the job
// was still running at that tick's drain) and appears by the next tick
// end — the one-tick skew contract.
static int test_executor_deliver_ns_skew(void)
{
    int failures = 0;
    tw_reset(gateway_delivery_full_vtable(), true);
    g_tw.probe_sleep_ms = 5u;
    struct fixture fx;
    CHECK(fixture_init_ex(&fx, 1u, 8000u, 4096u, nullptr) == 0);

    kith_gateway_session_t *s = nullptr;
    CHECK(make_composed_session(&fx, &s, 1000u) == 0);

    kith_gateway_phase_stats_t phase = {0};
    CHECK(kith_gateway_phase_stats(fx.gw, &phase) == 0);
    const uint64_t deliver_before = phase.deliver_ns_total;

    CHECK(kith_gateway_tick(fx.gw, 1200u) == 0);

    pthread_t releaser;
    CHECK(pthread_create(&releaser, nullptr, tw_release_hold_later, nullptr) == 0);
    CHECK(wait_u64_ge(&g_tw.completed, 1u));
    // The job finished, but no tick end has drained its pending time.
    CHECK(kith_gateway_phase_stats(fx.gw, &phase) == 0);
    CHECK(phase.deliver_ns_total == deliver_before);

    CHECK(kith_gateway_tick(fx.gw, 1500u) == 0);
    CHECK(kith_gateway_phase_stats(fx.gw, &phase) == 0);
    CHECK(phase.deliver_ns_total - deliver_before >= 5000000ull);

    (void)pthread_join(releaser, nullptr);
    kith_gateway_session_destroy(s);
    fixture_fini(&fx);
    return failures;
}

/** Drain one batch frame from @p conn (writing the outq first) and return
 *  the record bytes through @p out_records (@p out_cap bytes). Returns
 *  the record count, or -1 on any framing error. */
static int capture_frame(kith_net_conn_t *conn, int client_fd, uint8_t *out_records, size_t out_cap)
{
    if (kith_net_conn_write(conn) != 0)
    {
        return -1;
    }
    uint8_t header[14u];
    size_t got = 0u;
    while (got < sizeof(header))
    {
        ssize_t r = recv(client_fd, header + got, sizeof(header) - got, 0);
        if (r <= 0)
        {
            return -1;
        }
        got += (size_t)r;
    }
    const uint16_t count = (uint16_t)((uint16_t)header[10] << 8u | header[11]);
    const size_t rest = (size_t)count * GATEWAY_DELIVERY_PAYLOAD_SIZE;
    if (rest > out_cap)
    {
        return -1;
    }
    size_t recs = 0u;
    while (recs < rest)
    {
        ssize_t r = recv(client_fd, out_records + recs, rest - recs, 0);
        if (r <= 0)
        {
            return -1;
        }
        recs += (size_t)r;
    }
    return count;
}

/** Capture one batch frame from @p s's connection and check every
 *  serialized view subject appears byte-identical on the wire. Returns
 *  the failure count. */
static int check_frame_matches_view(kith_gateway_t *gw,
                                    kith_gateway_session_t *s,
                                    kith_net_conn_t *conn,
                                    int client_fd)
{
    int failures = 0;
    kith_gateway_view_snapshot_t snap = {0};
    kith_gateway_view_subject_t subjects[16u];
    size_t count = 0u;
    uint8_t wire[16u * GATEWAY_DELIVERY_PAYLOAD_SIZE];
    const int records = capture_frame(conn, client_fd, wire, sizeof(wire));
    CHECK(records >= 0);
    CHECK(kith_gateway_view_snapshot(gw, s, &snap, subjects, 16u, &count) == 0);
    for (size_t j = 0; j < count; ++j)
    {
        uint8_t rec[GATEWAY_DELIVERY_PAYLOAD_SIZE];
        gateway_delivery_serialize(&subjects[j], rec);
        bool on_wire = false;
        for (int r = 0; r < records; ++r)
        {
            if (memcmp(wire + (size_t)r * GATEWAY_DELIVERY_PAYLOAD_SIZE,
                       rec,
                       GATEWAY_DELIVERY_PAYLOAD_SIZE) == 0)
            {
                on_wire = true;
                break;
            }
        }
        CHECK(on_wire);
    }
    return failures;
}

// Two concurrent full-strategy delivers under a two-thread executor stay
// byte-correct: each thread's private scratch keeps the batch assembly
// isolated, so both frames match the serialize() of their session's view.
static int test_executor_concurrent_full_bytes(void)
{
    int failures = 0;
    tw_reset(gateway_delivery_full_vtable(), false);
    struct fixture fx;
    CHECK(fixture_init_ex(&fx, 2u, 8000u, 4096u, nullptr) == 0);

    kith_gateway_session_t *a = nullptr;
    kith_gateway_session_t *b = nullptr;
    CHECK(make_composed_session(&fx, &a, 1000u) == 0);
    // Session a's connection, kept alongside session b's so both frames
    // can be captured independently.
    kith_net_conn_t *conn_a = fx.conn;
    const int fd_a = fx.client_fd;
    int fd_b = -1;
    kith_net_conn_t *conn_b = nullptr;
    // A second session on its own cell so both deliver real payloads.
    {
        fd_b = connect_to_listener(fx.net);
        CHECK(fd_b >= 0);
        CHECK(kith_net_accept(fx.net, &conn_b) == 0);
        fx.conn = conn_b;
        fx.client_fd = fd_b;
        CHECK(kith_gateway_session_create(
                  fx.gw, conn_b, KITH_GATEWAY_SESSION_SUBSCRIBER, 2u, nullptr, &b) == 0);
        CHECK(kith_gateway_session_bind_actor(b, 2u) == 0);
        kith_fabric_cell_key_t home = make_key(9u, 0, 0, 0, 0u);
        CHECK(kith_gateway_session_window_add(b, &home) == 0);
        kith_sim_artifact_key_t sk = make_skey(&home, 1u);
        kith_sim_actor_t sub = make_actor(2u, 0, 0u);
        CHECK(kith_sim_publish_artifact(fx.sim, &sk, &sub, nullptr) == 0);
        for (uint64_t n = 3u; n < 5u; ++n)
        {
            kith_sim_actor_t nb = make_actor(n, (int64_t)(n * 77u), 0u);
            CHECK(kith_sim_publish_artifact(fx.sim, &sk, &nb, nullptr) == 0);
        }
        CHECK(kith_fabric_publish(fx.fabric, &home, 1u, nullptr) == 0);
        CHECK(kith_gateway_cache_refresh(fx.gw, 1000u) == 0);
        CHECK(kith_gateway_view_refresh(fx.gw, b, 1100u) == 0);
    }
    CHECK(kith_gateway_tick(fx.gw, 1200u) == 0);
    CHECK(wait_u64_ge(&g_tw.completed, 2u));

    failures += check_frame_matches_view(fx.gw, a, conn_a, fd_a);
    failures += check_frame_matches_view(fx.gw, b, conn_b, fd_b);
    kith_gateway_session_destroy(a);
    kith_gateway_session_destroy(b);
    kith_net_conn_close(conn_a);
    kith_net_conn_release(conn_a);
    fixture_fini(&fx);
    return failures;
}
/*---------------------------------------------------------------------------
 * tiered property equivalence under the executor
 *-------------------------------------------------------------------------*/

static uint64_t ex_next_random(uint64_t *state)
{
    *state ^= *state << 13u;
    *state ^= *state >> 7u;
    *state ^= *state << 17u;
    return *state;
}

struct ex_oracle
{
    uint8_t last[8u][GATEWAY_DELIVERY_PAYLOAD_SIZE];
    bool has[8u];
};

/** The tiered byte-compare oracle: a record is due exactly
 *  when it is the self subject, was never enqueued, or differs from the
 *  last enqueued bytes. */
static int ex_assert_wire(const kith_gateway_view_subject_t *subjects,
                          size_t subject_count,
                          const uint8_t *wire,
                          uint16_t count,
                          struct ex_oracle *oracle)
{
    int failures = 0;
    for (size_t j = 0; j < subject_count; ++j)
    {
        const bool is_self = j == 0u || subjects[j].subject_class == KITH_GATEWAY_VIEW_CLASS_SELF;
        uint8_t rec[GATEWAY_DELIVERY_PAYLOAD_SIZE];
        gateway_delivery_serialize(&subjects[j], rec);
        const uint64_t id = subjects[j].actor_id;
        const bool ref_due = is_self || !oracle->has[id] ||
                             memcmp(rec, oracle->last[id], GATEWAY_DELIVERY_PAYLOAD_SIZE) != 0;
        bool on_wire = false;
        for (uint16_t r = 0; r < count; ++r)
        {
            const uint8_t *w = wire + (size_t)r * GATEWAY_DELIVERY_PAYLOAD_SIZE;
            const uint64_t wid = (uint64_t)w[0] << 56u | (uint64_t)w[1] << 48u |
                                 (uint64_t)w[2] << 40u | (uint64_t)w[3] << 32u |
                                 (uint64_t)w[4] << 24u | (uint64_t)w[5] << 16u |
                                 (uint64_t)w[6] << 8u | (uint64_t)w[7];
            if (wid == id)
            {
                CHECK(memcmp(w, rec, GATEWAY_DELIVERY_PAYLOAD_SIZE) == 0);
                on_wire = true;
            }
        }
        CHECK(ref_due == on_wire);
        if (ref_due)
        {
            memcpy(oracle->last[id], rec, GATEWAY_DELIVERY_PAYLOAD_SIZE);
            oracle->has[id] = true;
        }
    }
    return failures;
}

/** Publish the property world: the self artifact at the home cell, six
 *  neighbors across the three tracked cells, and one fabric publish per
 *  cell. @p cells receives the three cell keys (function-scoped storage). */
static int ex_property_world(struct fixture *fx,
                             kith_sim_actor_t *actors,
                             const kith_fabric_cell_key_t *cells[3],
                             kith_fabric_cell_key_t *home,
                             const kith_fabric_cell_key_t *near,
                             const kith_fabric_cell_key_t *far_cell)
{
    cells[0] = home;
    cells[1] = near;
    cells[2] = far_cell;
    uint64_t actor_id = 2u;
    for (int c = 0; c < 3; ++c)
    {
        for (int n = 0; n < 2; ++n)
        {
            actors[actor_id - 2u] = make_actor(actor_id, (int64_t)(n + 1) << 15, 0u);
            actors[actor_id - 2u].pos_y = (int64_t)((unsigned)(n * 11)) << 16;
            kith_sim_artifact_key_t sk = make_skey(cells[c], 1u);
            if (kith_sim_publish_artifact(fx->sim, &sk, &actors[actor_id - 2u], nullptr) != 0)
            {
                return -1;
            }
            actor_id += 1u;
        }
    }
    for (int c = 0; c < 3; ++c)
    {
        if (kith_fabric_publish(fx->fabric, cells[c], 1u, nullptr) != 0)
        {
            return -1;
        }
    }
    return 0;
}

/** Apply one round's random moves to a subset of the six neighbors and
 *  publish the touched cells. Returns 0 on success. */
static int ex_round_moves(struct fixture *fx,
                          kith_sim_actor_t *actors,
                          const kith_fabric_cell_key_t *cells[3],
                          uint64_t *rng,
                          uint64_t now_ms)
{
    bool dirty[3] = {false, false, false};
    for (uint64_t a = 2u; a < 8u; ++a)
    {
        if ((ex_next_random(rng) & 3u) != 0u)
        {
            continue;
        }
        int c;
        if (a <= 3u)
        {
            c = 0;
        }
        else if (a <= 5u)
        {
            c = 1;
        }
        else
        {
            c = 2;
        }
        actors[a - 2u].pos_x = (int64_t)(ex_next_random(rng) & 0xFFFFu);
        actors[a - 2u].pos_y = (int64_t)(ex_next_random(rng) & 0xFFu) << 8;
        actors[a - 2u].vel_x = (int64_t)(ex_next_random(rng) & 0x3FFu) - 512;
        actors[a - 2u].input_tick = (uint32_t)(now_ms / 100u);
        kith_sim_artifact_key_t sk = make_skey(cells[c], 1u);
        if (kith_sim_publish_artifact(fx->sim, &sk, &actors[a - 2u], nullptr) != 0)
        {
            return -1;
        }
        dirty[c] = true;
    }
    for (int c = 0; c < 3; ++c)
    {
        if (dirty[c] && kith_fabric_publish(fx->fabric, cells[c], 1u, nullptr) != 0)
        {
            return -1;
        }
    }
    return 0;
}

// The tiered property test under the executor: same seed, same oracle,
// same 40 randomized rounds as the inline tiered suite, but every deliver
// pass is submitted to the executor and the wire is asserted after the
// job completes. Byte-parity here is parity with inline in its strongest
// form.
static int test_executor_tiered_property(void)
{
    int failures = 0;
    kith_gateway_tiered_config_t cfg = {0};
    cfg.size = sizeof(cfg);
    cfg.abi_version = KITH_ABI_VERSION;
    cfg.full_interval_ms = 0u;
    cfg.reduced_interval_ms = 0u;
    cfg.max_gap_ms = 3600000u;

    tw_reset(gateway_delivery_tiered_vtable(), false);
    struct fixture fx;
    CHECK(fixture_init_ex(&fx, 1u, 8000u, 4096u, &cfg) == 0);

    kith_gateway_session_t *s = nullptr;
    CHECK(make_composed_session(&fx, &s, 1000u) == 0);
    // Extend the window with two more cells (the property rounds move
    // neighbors across all three).
    kith_fabric_cell_key_t home = make_key(7u, 0, 0, 0, 0u);
    kith_fabric_cell_key_t near = make_key(7u, 1, 0, 0, 0u);
    kith_fabric_cell_key_t far_cell = make_key(7u, 3, 0, 0, 0u);
    CHECK(kith_gateway_session_window_add(s, &near) == 0);
    CHECK(kith_gateway_session_window_add(s, &far_cell) == 0);
    // Neighbors across the three cells, ids dense from 2.
    kith_sim_actor_t actors[6] = {0};
    const kith_fabric_cell_key_t *cells[3] = {nullptr, nullptr, nullptr};
    CHECK(ex_property_world(&fx, actors, cells, &home, &near, &far_cell) == 0);

    struct ex_oracle oracle = {0};
    uint64_t now_ms = 2000u;
    uint64_t rng = 0x5DEECE66DULL;
    for (int round = 1; round <= 40; ++round)
    {
        // The tiered cadence defaults gate reduced-tier subjects at a
        // 200 ms floor; the inline property suite steps the clock 200 ms
        // per deliver so the byte-compare oracle alone decides dues.
        CHECK(ex_round_moves(&fx, actors, cells, &rng, now_ms) == 0);
        now_ms += 200u;
        CHECK(kith_gateway_tick(fx.gw, now_ms) == 0);
        CHECK(wait_u64_ge(&g_tw.completed, (uint64_t)round));

        // The tick recomposed with the fresh cache; the delivered view
        // equals the snapshot read after completion.
        kith_gateway_view_snapshot_t snap = {0};
        kith_gateway_view_subject_t subjects[16u];
        size_t count = 0u;
        CHECK(kith_gateway_view_snapshot(fx.gw, s, &snap, subjects, 16u, &count) == 0);

        uint8_t wire[16u * GATEWAY_DELIVERY_PAYLOAD_SIZE] = {0};
        const int records = capture_frame(fx.conn, fx.client_fd, wire, sizeof(wire));
        CHECK(records >= 0);
        CHECK(g_tw.last_stats.events_enqueued == 0u);
        CHECK((size_t)records == count - (size_t)g_tw.last_stats.suppressed);
        failures += ex_assert_wire(subjects, count, wire, (uint16_t)records, &oracle);
    }

    kith_gateway_session_destroy(s);
    fixture_fini(&fx);
    return failures;
}

int main(void)
{
    (void)signal(SIGPIPE, SIG_IGN);
    int rc = 0;
    rc |= test_executor_off_thread();
    rc |= test_executor_accounting_invariants();
    rc |= test_executor_compose_wait_success();
    rc |= test_executor_compose_wait_timeout();
    rc |= test_executor_wait_idle_branches();
    rc |= test_executor_budget_deferred_skips();
    rc |= test_executor_wait_budget_amplification();
    rc |= test_executor_ebusy_counted_skip();
    rc |= test_executor_teardown_drains();
    rc |= test_executor_deliver_ns_skew();
    rc |= test_executor_concurrent_full_bytes();
    rc |= test_executor_tiered_property();
    if (rc == 0)
    {
        (void)printf("gateway executor: all tests passed\n");
    }
    return rc;
}
