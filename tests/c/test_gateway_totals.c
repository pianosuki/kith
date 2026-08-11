/* The cumulative delivery and view totals: the fold inside
 * kith_gateway_deliver captures both the inline and the executor path
 * (the only site that sees both), the visit-side view totals count
 * delivery-pass visits of sessions holding a view and sum their live
 * view metadata, and the accessors guard their arguments. A registered
 * probe strategy reports fixed per-call stats so the accumulation is
 * asserted exactly, independent of any built-in strategy's behavior. */

#include <signal.h>
#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <unistd.h>

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
        (void)fprintf(stderr, "gateway totals: assertion at line %d failed\n", line);
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

/** Spin until the delivery totals reach @p target enqueued (bounded at
 *  10 s): the executor folds land on worker threads, so a just-returned
 *  tick may lag one pass (the deliver_ns_pending skew discipline). */
static bool wait_enqueued_ge(const kith_gateway_t *gw, uint64_t target)
{
    const uint64_t deadline = now_ns() + 10000000000ull;
    for (;;)
    {
        kith_gateway_delivery_totals_t totals = {0};
        if (kith_gateway_delivery_totals(gw, &totals) == 0 && totals.enqueued >= target)
        {
            return true;
        }
        if (now_ns() >= deadline)
        {
            return false;
        }
        sleep_us(100u);
    }
}

/*---------------------------------------------------------------------------
 * probe strategy: fixed per-call stats, no I/O
 *-------------------------------------------------------------------------*/

#define PROBE_ENQUEUED   2u
#define PROBE_DROPPED    1u
#define PROBE_EVENTS     1u
#define PROBE_SUPPRESSED 3u

static int
probe_session_init(const kith_gateway_session_t *session, const void *config, void **out_state)
{
    (void)session;
    (void)config;
    *out_state = nullptr;
    return 0;
}

static int probe_deliver(void *state,
                         kith_gateway_t *gateway,
                         kith_gateway_session_t *session,
                         const kith_gateway_view_subject_t *subjects,
                         size_t subject_count,
                         uint64_t now_ms,
                         kith_gateway_delivery_stats_t *out_stats)
{
    (void)state;
    (void)gateway;
    (void)session;
    (void)subjects;
    (void)subject_count;
    (void)now_ms;
    out_stats->enqueued = PROBE_ENQUEUED;
    out_stats->dropped = PROBE_DROPPED;
    out_stats->events_enqueued = PROBE_EVENTS;
    out_stats->suppressed = PROBE_SUPPRESSED;
    return 0;
}

static const kith_gateway_delivery_vtable_t probe_vtable = {
    .size = sizeof(probe_vtable),
    .abi_version = KITH_ABI_VERSION,
    .session_init = probe_session_init,
    .session_fini = nullptr,
    .deliver = probe_deliver,
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
};

static int fixture_init(struct fixture *fx, uint32_t workers)
{
    memset(fx, 0, sizeof(*fx));
    fx->client_fd = -1;
    if (kith_proto_create(nullptr, nullptr, &fx->proto) != 0)
    {
        return -1;
    }
    const uint16_t replication_type_id = KITH_PROTO_TYPE_USER_BASE;
    if (kith_proto_register_type_id(fx->proto, "replication", replication_type_id) != 0)
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
    params.replication_batch_type_id = replication_type_id;
    params.view_refresh_interval_ms = 10u;
    params.cache_refresh_interval_ms = 10u;
    params.max_sessions = 8u;
    params.delivery_strategy = "totals_probe";
    params.delivery_worker_count = workers;
    if (kith_gateway_create(&params, fx->net, fx->fabric, fx->proto, nullptr, &fx->gw) != 0)
    {
        return -1;
    }
    if (kith_gateway_register_delivery(fx->gw, "totals_probe", &probe_vtable) != 0)
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

/** Connect, create a subscriber session, bind, window the home cell,
 *  publish the self artifact plus two neighbors, refresh cache and view
 *  so the session holds a composed view set. */
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
    for (uint64_t n = 2u; n < 4u; ++n)
    {
        kith_sim_actor_t a = make_actor(n, (int64_t)(n * 100u), 0u);
        if (kith_sim_publish_artifact(fx->sim, &sk, &a, nullptr) != 0)
        {
            return -1;
        }
    }
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
 * tests
 *-------------------------------------------------------------------------*/

// A fresh gateway reports all-zero totals; the accessors reject NULL and
// never fail with ESTATE (unlike the executor stats, the totals exist on
// every configuration).
static int test_totals_zero_and_guards(void)
{
    int failures = 0;
    struct fixture fx;
    CHECK(fixture_init(&fx, 0u) == 0);

    kith_gateway_delivery_totals_t d = {0};
    kith_gateway_view_totals_t v = {0};
    CHECK(kith_gateway_delivery_totals(fx.gw, &d) == 0);
    CHECK(kith_gateway_view_totals(fx.gw, &v) == 0);
    CHECK(d.enqueued == 0u && d.dropped == 0u && d.events_enqueued == 0u && d.suppressed == 0u);
    CHECK(v.visits == 0u && v.candidate_total == 0u && v.selected_total == 0u &&
          v.candidate_high_watermark == 0u && v.selected_high_watermark == 0u);
    CHECK(kith_gateway_delivery_totals(nullptr, &d) == kith_error_return(KITH_EINVAL));
    CHECK(kith_gateway_delivery_totals(fx.gw, nullptr) == kith_error_return(KITH_EINVAL));
    CHECK(kith_gateway_view_totals(nullptr, &v) == kith_error_return(KITH_EINVAL));
    CHECK(kith_gateway_view_totals(fx.gw, nullptr) == kith_error_return(KITH_EINVAL));

    fixture_fini(&fx);
    return failures;
}

// The inline tick path folds once per session per pass, and an explicit
// deliver folds too — whether the caller passed an out_stats buffer or
// NULL (the fold owns its local buffer when the caller passes NULL).
static int test_delivery_totals_inline_fold(void)
{
    int failures = 0;
    struct fixture fx;
    CHECK(fixture_init(&fx, 0u) == 0);

    kith_gateway_session_t *s = nullptr;
    CHECK(make_composed_session(&fx, &s, 1000u) == 0);
    CHECK(kith_gateway_tick(fx.gw, 1200u) == 0);
    CHECK(kith_gateway_tick(fx.gw, 1300u) == 0);

    kith_gateway_delivery_totals_t d = {0};
    CHECK(kith_gateway_delivery_totals(fx.gw, &d) == 0);
    CHECK(d.enqueued == 2ull * PROBE_ENQUEUED);
    CHECK(d.dropped == 2ull * PROBE_DROPPED);
    CHECK(d.events_enqueued == 2ull * PROBE_EVENTS);
    CHECK(d.suppressed == 2ull * PROBE_SUPPRESSED);

    // Explicit deliver with the caller's buffer: the reported stats and
    // the folded totals agree.
    kith_gateway_delivery_stats_t per_call = {0};
    CHECK(kith_gateway_deliver(fx.gw, s, 1400u, &per_call) == 0);
    CHECK(per_call.enqueued == PROBE_ENQUEUED && per_call.dropped == PROBE_DROPPED);
    CHECK(kith_gateway_delivery_totals(fx.gw, &d) == 0);
    CHECK(d.enqueued == 3ull * PROBE_ENQUEUED);
    CHECK(d.suppressed == 3ull * PROBE_SUPPRESSED);

    // Explicit deliver with NULL out_stats folds from the internal buffer.
    CHECK(kith_gateway_deliver(fx.gw, s, 1500u, nullptr) == 0);
    CHECK(kith_gateway_delivery_totals(fx.gw, &d) == 0);
    CHECK(d.enqueued == 4ull * PROBE_ENQUEUED);

    kith_gateway_session_destroy(s);
    fixture_fini(&fx);
    return failures;
}

// The executor path folds on worker threads; the totals converge to the
// inline result for the same deliver count (bounded wait for the fold).
static int test_delivery_totals_executor_fold(void)
{
    int failures = 0;
    struct fixture fx;
    CHECK(fixture_init(&fx, 1u) == 0);

    kith_gateway_session_t *s = nullptr;
    CHECK(make_composed_session(&fx, &s, 1000u) == 0);
    CHECK(kith_gateway_tick(fx.gw, 1200u) == 0);
    CHECK(kith_gateway_tick(fx.gw, 1300u) == 0);
    CHECK(wait_enqueued_ge(fx.gw, 2ull * PROBE_ENQUEUED));

    kith_gateway_delivery_totals_t d = {0};
    CHECK(kith_gateway_delivery_totals(fx.gw, &d) == 0);
    CHECK(d.enqueued == 2ull * PROBE_ENQUEUED);
    CHECK(d.dropped == 2ull * PROBE_DROPPED);
    CHECK(d.events_enqueued == 2ull * PROBE_EVENTS);
    CHECK(d.suppressed == 2ull * PROBE_SUPPRESSED);

    // The executor's fold lands on the session's counters too.
    kith_gateway_delivery_totals_t sd = {0};
    CHECK(kith_gateway_session_delivery_totals(s, &sd) == 0);
    CHECK(sd.enqueued == 2ull * PROBE_ENQUEUED);
    CHECK(sd.dropped == 2ull * PROBE_DROPPED);
    CHECK(sd.events_enqueued == 2ull * PROBE_EVENTS);
    CHECK(sd.suppressed == 2ull * PROBE_SUPPRESSED);

    kith_gateway_session_destroy(s);
    fixture_fini(&fx);
    return failures;
}

// Per-session delivery totals: the same probe stats fold onto the
// session's own counters across the tick path and explicit delivers, the
// single session's sums equal the gateway totals (the partition never
// exceeds it), and the accessor guards its arguments.
static int test_session_delivery_totals_partition(void)
{
    int failures = 0;
    struct fixture fx;
    CHECK(fixture_init(&fx, 0u) == 0);

    kith_gateway_session_t *s = nullptr;
    CHECK(make_composed_session(&fx, &s, 1000u) == 0);
    CHECK(kith_gateway_tick(fx.gw, 1200u) == 0);
    CHECK(kith_gateway_tick(fx.gw, 1300u) == 0);
    CHECK(kith_gateway_deliver(fx.gw, s, 1400u, nullptr) == 0);

    kith_gateway_delivery_totals_t sd = {0};
    CHECK(kith_gateway_session_delivery_totals(s, &sd) == 0);
    CHECK(sd.enqueued == 3ull * PROBE_ENQUEUED);
    CHECK(sd.dropped == 3ull * PROBE_DROPPED);
    CHECK(sd.events_enqueued == 3ull * PROBE_EVENTS);
    CHECK(sd.suppressed == 3ull * PROBE_SUPPRESSED);

    kith_gateway_delivery_totals_t d = {0};
    CHECK(kith_gateway_delivery_totals(fx.gw, &d) == 0);
    CHECK(sd.enqueued <= d.enqueued);
    CHECK(sd.dropped <= d.dropped);
    CHECK(sd.events_enqueued <= d.events_enqueued);
    CHECK(sd.suppressed <= d.suppressed);
    CHECK(sd.enqueued == d.enqueued);
    CHECK(sd.suppressed == d.suppressed);

    CHECK(kith_gateway_session_delivery_totals(nullptr, &sd) == kith_error_return(KITH_EINVAL));
    CHECK(kith_gateway_session_delivery_totals(s, nullptr) == kith_error_return(KITH_EINVAL));

    kith_gateway_session_destroy(s);
    fixture_fini(&fx);
    return failures;
}

// The view totals count one visit per pass per view-holding session and
// sum its live metadata; a session without a view never visits; the
// high-watermarks track the observed peaks.
static int test_view_totals_visits(void)
{
    int failures = 0;
    struct fixture fx;
    CHECK(fixture_init(&fx, 0u) == 0);

    // A connected session with no bound actor and no view: the pass must
    // not count it. Its connection stays in test-local storage — the
    // fixture tracks only the composed session's.
    int viewless_fd = connect_to_listener(fx.net);
    CHECK(viewless_fd >= 0);
    kith_net_conn_t *viewless_conn = nullptr;
    CHECK(kith_net_accept(fx.net, &viewless_conn) == 0);
    kith_gateway_session_t *viewless = nullptr;
    CHECK(kith_gateway_session_create(
              fx.gw, viewless_conn, KITH_GATEWAY_SESSION_SUBSCRIBER, 1u, nullptr, &viewless) == 0);

    kith_gateway_session_t *s = nullptr;
    CHECK(make_composed_session(&fx, &s, 1000u) == 0);

    kith_gateway_view_snapshot_t snap = {0};
    kith_gateway_view_subject_t subjects[16u];
    size_t count = 0u;
    CHECK(kith_gateway_view_snapshot(fx.gw, s, &snap, subjects, 16u, &count) == 0);
    CHECK(snap.candidate_count > 0u && snap.selected_count > 0u);

    CHECK(kith_gateway_tick(fx.gw, 1200u) == 0);
    CHECK(kith_gateway_tick(fx.gw, 1300u) == 0);

    kith_gateway_view_totals_t v = {0};
    CHECK(kith_gateway_view_totals(fx.gw, &v) == 0);
    CHECK(v.visits == 2u);
    CHECK(v.candidate_total == 2u * (uint64_t)snap.candidate_count);
    CHECK(v.selected_total == 2u * (uint64_t)snap.selected_count);
    CHECK(v.candidate_high_watermark == (uint64_t)snap.candidate_count);
    CHECK(v.selected_high_watermark == (uint64_t)snap.selected_count);

    kith_gateway_session_destroy(viewless);
    kith_gateway_session_destroy(s);
    kith_net_conn_close(viewless_conn);
    kith_net_conn_release(viewless_conn);
    (void)close(viewless_fd);
    fixture_fini(&fx);
    return failures;
}

int main(void)
{
    (void)signal(SIGPIPE, SIG_IGN);
    int rc = 0;
    rc |= test_totals_zero_and_guards();
    rc |= test_delivery_totals_inline_fold();
    rc |= test_delivery_totals_executor_fold();
    rc |= test_session_delivery_totals_partition();
    rc |= test_view_totals_visits();
    if (rc == 0)
    {
        (void)printf("gateway totals: all tests passed\n");
    }
    return rc;
}
