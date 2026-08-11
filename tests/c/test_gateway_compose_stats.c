#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include <unistd.h>

#include "kith/fabric/fabric.h"
#include "kith/gateway/gateway.h"
#include "kith/net/net.h"
#include "kith/proto/proto.h"
#include "kith/sim/sim.h"
#include "kith/types.h"

#include <netinet/in.h>
#include <sys/socket.h>

static int check_cond(bool ok, int line)
{
    if (!ok)
    {
        (void)fprintf(stderr, "gateway compose stats: assertion at line %d failed\n", line);
        return 1;
    }
    return 0;
}

#define CHECK(cond) failures += check_cond((cond), __LINE__)

struct fixture
{
    kith_proto_t *proto;
    kith_net_t *net;
    kith_sim_t *sim;
    kith_fabric_t *fabric;
    kith_gateway_t *gw;
};

static int fixture_init(struct fixture *fx)
{
    memset(fx, 0, sizeof(*fx));
    if (kith_proto_create(nullptr, nullptr, &fx->proto) != 0)
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
    if (kith_gateway_create(nullptr, fx->net, fx->fabric, fx->proto, nullptr, &fx->gw) != 0)
    {
        return -1;
    }
    if (kith_net_listen(fx->net, "127.0.0.1", 0) != 0)
    {
        return -1;
    }
    return 0;
}

static void fixture_fini(const struct fixture *fx)
{
    kith_gateway_destroy(fx->gw);
    kith_fabric_destroy(fx->fabric);
    kith_sim_destroy(fx->sim);
    kith_net_destroy(fx->net);
    kith_proto_destroy(fx->proto);
}

static uint64_t compose_stats_sum(const kith_gateway_compose_stats_t *s)
{
    return s->window_ns_total + s->prior_ns_total + s->lock_wait_ns_total + s->scan_ns_total +
           s->sort_ns_total + s->select_ns_total;
}

static bool compose_stats_monotonic(const kith_gateway_compose_stats_t *a,
                                    const kith_gateway_compose_stats_t *b)
{
    return b->window_ns_total >= a->window_ns_total && b->prior_ns_total >= a->prior_ns_total &&
           b->lock_wait_ns_total >= a->lock_wait_ns_total && b->scan_ns_total >= a->scan_ns_total &&
           b->sort_ns_total >= a->sort_ns_total && b->select_ns_total >= a->select_ns_total;
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

static int make_conn(kith_net_t *net, kith_net_conn_t **out_conn)
{
    *out_conn = nullptr;
    int client_fd = connect_to_listener(net);
    if (client_fd < 0)
    {
        return -1;
    }
    int rc = kith_net_accept(net, out_conn);
    (void)close(client_fd);
    return rc;
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

static kith_sim_actor_t make_actor(uint64_t id, int64_t x, int64_t y, uint32_t tick)
{
    kith_sim_actor_t a = {0};
    a.id = id;
    a.pos_x = x;
    a.pos_y = y;
    a.input_tick = tick;
    return a;
}

// One subscriber and two nearby actors cached in one cell, with a bound
// session whose window covers the cell — the minimal state for a
// composition to produce a view set.
static int
compose_setup(struct fixture *fx, kith_net_conn_t **out_conn, kith_gateway_session_t **out_session)
{
    int failures = 0;
    kith_fabric_cell_key_t cell = make_key(3u, 0, 0, 0, 0u);

    // Subscribe before publishing: the cache drain picks up products that
    // follow the window's fabric subscription.
    CHECK(make_conn(fx->net, out_conn) == 0);
    CHECK(kith_gateway_session_create(
              fx->gw, *out_conn, KITH_GATEWAY_SESSION_SUBSCRIBER, 1u, nullptr, out_session) == 0);
    CHECK(kith_gateway_session_bind_actor(*out_session, 100u) == 0);
    CHECK(kith_gateway_session_window_add(*out_session, &cell) == 0);

    kith_sim_artifact_key_t sk = make_skey(&cell, 1u);
    kith_sim_actor_t sub = make_actor(100u, 0, 0, 7u);
    CHECK(kith_sim_publish_artifact(fx->sim, &sk, &sub, nullptr) == 0);
    kith_sim_actor_t near_a = make_actor(1u, 1LL << KITH_SIM_FIX_SHIFT, 0, 0u);
    CHECK(kith_sim_publish_artifact(fx->sim, &sk, &near_a, nullptr) == 0);
    kith_sim_actor_t near_b = make_actor(2u, 2LL << KITH_SIM_FIX_SHIFT, 0, 0u);
    CHECK(kith_sim_publish_artifact(fx->sim, &sk, &near_b, nullptr) == 0);
    CHECK(kith_fabric_publish(fx->fabric, &cell, 1u, nullptr) == 0);

    CHECK(kith_gateway_cache_refresh(fx->gw, 100u) == 0);
    return failures;
}

// The accessor rejects a NULL gateway or NULL out-stats with EINVAL. A fresh
// gateway reports every sub-phase total as zero (nothing has composed yet),
// and the skip counter starts at zero (nothing has been skipped yet).
static int test_compose_stats_arg_validation(void)
{
    int failures = 0;
    struct fixture fx;
    CHECK(fixture_init(&fx) == 0);

    kith_gateway_compose_stats_t stats = {0};
    CHECK(kith_gateway_compose_stats(nullptr, &stats) == kith_error_return(KITH_EINVAL));
    CHECK(kith_gateway_compose_stats(fx.gw, nullptr) == kith_error_return(KITH_EINVAL));

    CHECK(kith_gateway_compose_stats(fx.gw, &stats) == 0);
    CHECK(stats.window_ns_total == 0u);
    CHECK(stats.prior_ns_total == 0u);
    CHECK(stats.lock_wait_ns_total == 0u);
    CHECK(stats.scan_ns_total == 0u);
    CHECK(stats.sort_ns_total == 0u);
    CHECK(stats.select_ns_total == 0u);

    uint64_t skips = 99u;
    CHECK(kith_gateway_compose_skips(nullptr, &skips) == kith_error_return(KITH_EINVAL));
    CHECK(kith_gateway_compose_skips(fx.gw, nullptr) == kith_error_return(KITH_EINVAL));
    CHECK(kith_gateway_compose_skips(fx.gw, &skips) == 0);
    CHECK(skips == 0u);

    fixture_fini(&fx);
    return failures;
}

// The sub-phase brackets live in the composer itself, so a direct
// kith_gateway_view_refresh advances them even outside kith_gateway_tick —
// unlike the parent compose phase total, whose bracket lives in the tick
// loop and therefore stays untouched here.
static int test_compose_stats_advance_outside_tick(void)
{
    int failures = 0;
    struct fixture fx;
    CHECK(fixture_init(&fx) == 0);

    kith_net_conn_t *conn = nullptr;
    kith_gateway_session_t *sess = nullptr;
    CHECK(compose_setup(&fx, &conn, &sess) == 0);

    kith_gateway_compose_stats_t s0 = {0};
    kith_gateway_phase_stats_t p0 = {0};
    CHECK(kith_gateway_compose_stats(fx.gw, &s0) == 0);
    CHECK(kith_gateway_phase_stats(fx.gw, &p0) == 0);

    CHECK(kith_gateway_view_refresh(fx.gw, sess, 200u) == 0);

    kith_gateway_compose_stats_t s1 = {0};
    kith_gateway_phase_stats_t p1 = {0};
    CHECK(kith_gateway_compose_stats(fx.gw, &s1) == 0);
    CHECK(kith_gateway_phase_stats(fx.gw, &p1) == 0);
    CHECK(compose_stats_monotonic(&s0, &s1));
    CHECK(compose_stats_sum(&s1) > compose_stats_sum(&s0));
    CHECK(p1.compose_ns_total == p0.compose_ns_total);

    // A refresh inside the view interval composes nothing: no advance.
    CHECK(kith_gateway_view_refresh(fx.gw, sess, 250u) == 0);
    kith_gateway_compose_stats_t s2 = {0};
    CHECK(kith_gateway_compose_stats(fx.gw, &s2) == 0);
    CHECK(compose_stats_sum(&s2) == compose_stats_sum(&s1));

    kith_gateway_session_destroy(sess);
    kith_net_conn_close(conn);
    kith_net_conn_release(conn);
    fixture_fini(&fx);
    return failures;
}

// A tick that drives the per-session compose loop advances every sub-phase
// total monotonically, moves at least one of them strictly forward, and
// stays within the parent compose phase total (the six bracket most of the
// compose interior; heap growth, the unlock, and error paths stay untimed).
static int test_compose_stats_monotonic_across_ticks(void)
{
    int failures = 0;
    struct fixture fx;
    CHECK(fixture_init(&fx) == 0);

    kith_net_conn_t *conn = nullptr;
    kith_gateway_session_t *sess = nullptr;
    CHECK(compose_setup(&fx, &conn, &sess) == 0);

    kith_gateway_compose_stats_t s0 = {0};
    kith_gateway_phase_stats_t p0 = {0};
    CHECK(kith_gateway_compose_stats(fx.gw, &s0) == 0);
    CHECK(kith_gateway_phase_stats(fx.gw, &p0) == 0);

    // First tick past the cache-refresh interval: the pending cell drains,
    // then the session's first composition runs on the tick path.
    CHECK(kith_gateway_tick(fx.gw, 300u) == 0);
    kith_gateway_compose_stats_t s1 = {0};
    kith_gateway_phase_stats_t p1 = {0};
    CHECK(kith_gateway_compose_stats(fx.gw, &s1) == 0);
    CHECK(kith_gateway_phase_stats(fx.gw, &p1) == 0);
    CHECK(p1.compose_ns_total > p0.compose_ns_total);
    CHECK(compose_stats_monotonic(&s0, &s1));
    CHECK(compose_stats_sum(&s1) > compose_stats_sum(&s0));
    CHECK(compose_stats_sum(&s1) <= p1.compose_ns_total - p0.compose_ns_total);

    // A second tick well past the view interval keeps every total monotonic.
    CHECK(kith_gateway_tick(fx.gw, 500u) == 0);
    kith_gateway_compose_stats_t s2 = {0};
    CHECK(kith_gateway_compose_stats(fx.gw, &s2) == 0);
    CHECK(compose_stats_monotonic(&s1, &s2));

    kith_gateway_session_destroy(sess);
    kith_net_conn_close(conn);
    kith_net_conn_release(conn);
    fixture_fini(&fx);
    return failures;
}

int main(void)
{
    int rc = 0;
    rc |= test_compose_stats_arg_validation();
    rc |= test_compose_stats_advance_outside_tick();
    rc |= test_compose_stats_monotonic_across_ticks();
    if (rc != 0)
    {
        (void)fprintf(stderr, "gateway compose stats tests FAILED\n");
    }
    return rc;
}
