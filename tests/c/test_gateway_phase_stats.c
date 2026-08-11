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
#include "kith/version.h"

#include <netinet/in.h>
#include <sys/socket.h>

static int check_cond(bool ok, int line)
{
    if (!ok)
    {
        (void)fprintf(stderr, "gateway phase stats: assertion at line %d failed\n", line);
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

static void capture_fn(uint16_t msg_type,
                       const void *payload,
                       uint32_t payload_len,
                       kith_gateway_session_t *session,
                       void *user_data)
{
    (void)msg_type;
    (void)payload;
    (void)payload_len;
    (void)session;
    int *calls = user_data;
    *calls += 1;
}

// The accessor rejects a NULL gateway or NULL out-stats with EINVAL. A fresh
// gateway reports every cumulative total as zero (no ticks, no dispatches).
static int test_phase_stats_arg_validation(void)
{
    int failures = 0;
    struct fixture fx;
    CHECK(fixture_init(&fx) == 0);

    kith_gateway_phase_stats_t stats = {0};
    CHECK(kith_gateway_phase_stats(nullptr, &stats) == kith_error_return(KITH_EINVAL));
    CHECK(kith_gateway_phase_stats(fx.gw, nullptr) == kith_error_return(KITH_EINVAL));

    CHECK(kith_gateway_phase_stats(fx.gw, &stats) == 0);
    CHECK(stats.refresh_ns_total == 0u);
    CHECK(stats.compose_ns_total == 0u);
    CHECK(stats.deliver_ns_total == 0u);
    CHECK(stats.dispatch_total == 0u);

    fixture_fini(&fx);
    return failures;
}

// The fabric publish counter is zero on a fresh handle and increments by
// exactly one per successful kith_fabric_publish (a publish requires a sim
// artifact in the cell first; the counter counts the publish, not the
// artifact).
static int test_fabric_publish_total_increments(void)
{
    int failures = 0;
    struct fixture fx;
    CHECK(fixture_init(&fx) == 0);

    CHECK(kith_fabric_publish_total(fx.fabric) == 0u);
    CHECK(kith_fabric_publish_total(nullptr) == 0u);

    kith_fabric_cell_key_t k = make_key(7u, 0, 0, 0, 0u);
    kith_sim_artifact_key_t sk = make_skey(&k, 1u);
    kith_sim_actor_t a = make_actor(1u, 0, 0, 0u);
    CHECK(kith_sim_publish_artifact(fx.sim, &sk, &a, nullptr) == 0);
    CHECK(kith_fabric_publish(fx.fabric, &k, 1u, nullptr) == 0);
    CHECK(kith_fabric_publish_total(fx.fabric) == 1u);

    // A second publish (update of the existing cell) increments again.
    kith_sim_actor_t a2 = make_actor(1u, 1LL << KITH_SIM_FIX_SHIFT, 0, 1u);
    CHECK(kith_sim_publish_artifact(fx.sim, &sk, &a2, nullptr) == 0);
    CHECK(kith_fabric_publish(fx.fabric, &k, 1u, nullptr) == 0);
    CHECK(kith_fabric_publish_total(fx.fabric) == 2u);

    fixture_fini(&fx);
    return failures;
}

// Across ticks the per-phase ns totals are monotonic non-decreasing, and a
// tick that performs a real cache refresh advances refresh_ns_total. The
// phase counters are accumulated only inside kith_gateway_tick (the bracket
// lives there, not in the individual phase functions), so a direct
// kith_gateway_cache_refresh call does not move them.
static int test_phase_stats_monotonic_across_ticks(void)
{
    int failures = 0;
    struct fixture fx;
    CHECK(fixture_init(&fx) == 0);

    kith_fabric_cell_key_t k = make_key(1u, 0, 0, 0, 0u);
    CHECK(kith_gateway_subscribe(fx.gw, &k) == 0);
    kith_sim_artifact_key_t sk = make_skey(&k, 1u);
    kith_sim_actor_t a = make_actor(1u, 0, 0, 0u);
    CHECK(kith_sim_publish_artifact(fx.sim, &sk, &a, nullptr) == 0);
    CHECK(kith_fabric_publish(fx.fabric, &k, 1u, nullptr) == 0);

    kith_gateway_phase_stats_t s0 = {0};
    CHECK(kith_gateway_phase_stats(fx.gw, &s0) == 0);

    // First tick: last_refresh_ms is 0 so the interval gate does not skip;
    // the pending cell is drained and snapshotted, advancing refresh_ns.
    CHECK(kith_gateway_tick(fx.gw, 100u) == 0);
    kith_gateway_phase_stats_t s1 = {0};
    CHECK(kith_gateway_phase_stats(fx.gw, &s1) == 0);
    CHECK(s1.refresh_ns_total > s0.refresh_ns_total);
    CHECK(s1.compose_ns_total >= s0.compose_ns_total);
    CHECK(s1.deliver_ns_total >= s0.deliver_ns_total);
    CHECK(s1.dispatch_total == 0u);

    // A second tick well past the refresh interval keeps the totals
    // monotonic (a gated no-op tick adds zero to every phase).
    CHECK(kith_gateway_tick(fx.gw, 100000u) == 0);
    kith_gateway_phase_stats_t s2 = {0};
    CHECK(kith_gateway_phase_stats(fx.gw, &s2) == 0);
    CHECK(s2.refresh_ns_total >= s1.refresh_ns_total);
    CHECK(s2.compose_ns_total >= s1.compose_ns_total);
    CHECK(s2.deliver_ns_total >= s1.deliver_ns_total);
    CHECK(s2.dispatch_total == 0u);

    fixture_fini(&fx);
    return failures;
}

// dispatch_total counts every kith_gateway_dispatch that reaches a handler.
// A dispatch with no session (ENOENT) or no handler (ENOENT) does not
// increment it; a dispatch that invokes a handler increments by exactly
// one.
static int test_dispatch_total_increments(void)
{
    int failures = 0;
    struct fixture fx;
    CHECK(fixture_init(&fx) == 0);

    kith_net_conn_t *conn = nullptr;
    CHECK(make_conn(fx.net, &conn) == 0);

    // No session bound: ENOENT, no increment.
    kith_proto_frame_t frame = {0};
    frame.type_id = 10u;
    CHECK(kith_gateway_dispatch(fx.gw, conn, &frame) == kith_error_return(KITH_ENOENT));
    kith_gateway_phase_stats_t s = {0};
    CHECK(kith_gateway_phase_stats(fx.gw, &s) == 0);
    CHECK(s.dispatch_total == 0u);

    kith_gateway_session_t *sess = nullptr;
    CHECK(kith_gateway_session_create(
              fx.gw, conn, KITH_GATEWAY_SESSION_SUBSCRIBER, 1u, nullptr, &sess) == 0);

    // Session bound, no handler for type 10: ENOENT, still no increment.
    CHECK(kith_gateway_dispatch(fx.gw, conn, &frame) == kith_error_return(KITH_ENOENT));
    CHECK(kith_gateway_phase_stats(fx.gw, &s) == 0);
    CHECK(s.dispatch_total == 0u);

    int calls = 0;
    CHECK(kith_gateway_register_handler(fx.gw, 10u, capture_fn, &calls) == 0);

    // Two dispatches that reach the handler: dispatch_total advances 0 -> 1 -> 2.
    CHECK(kith_gateway_dispatch(fx.gw, conn, &frame) == 0);
    CHECK(kith_gateway_phase_stats(fx.gw, &s) == 0);
    CHECK(s.dispatch_total == 1u);
    CHECK(kith_gateway_dispatch(fx.gw, conn, &frame) == 0);
    CHECK(kith_gateway_phase_stats(fx.gw, &s) == 0);
    CHECK(s.dispatch_total == 2u);
    CHECK(calls == 2);

    kith_gateway_session_destroy(sess);
    kith_net_conn_close(conn);
    kith_net_conn_release(conn);
    fixture_fini(&fx);
    return failures;
}

int main(void)
{
    int rc = 0;
    rc |= test_phase_stats_arg_validation();
    rc |= test_fabric_publish_total_increments();
    rc |= test_phase_stats_monotonic_across_ticks();
    rc |= test_dispatch_total_increments();
    if (rc != 0)
    {
        (void)fprintf(stderr, "gateway phase stats tests FAILED\n");
    }
    return rc;
}
