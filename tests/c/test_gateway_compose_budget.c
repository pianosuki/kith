/* Per-tick compose-budget tests: the tick's composition pass runs under a
 * soft wall-time budget, and once it is consumed sessions that already hold
 * a retained view set defer recomposition to a subsequent tick while still
 * receiving their retained set every tick. Drives dirty-tick storms over
 * multi-session worlds and asserts deferral fires, delivery cadence is
 * preserved for deferred sessions, deferred views converge under the
 * rotating start index, the disabled sentinel never defers, and a session
 * with no retained view composes even past the budget. */

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
        (void)fprintf(stderr, "gateway compose budget: assertion at line %d failed\n", line);
        return 1;
    }
    return 0;
}

#define CHECK(cond) failures += check_cond((cond), __LINE__)

// Sessions per world. Small enough that the whole suite stays fast, large
// enough that a one-microsecond budget cannot cover every recomposition.
#define N_SESSIONS 6u

struct fixture
{
    kith_proto_t *proto;
    kith_net_t *net;
    kith_sim_t *sim;
    kith_fabric_t *fabric;
    kith_gateway_t *gw;
    kith_net_conn_t *conns[N_SESSIONS];
    kith_gateway_session_t *sessions[N_SESSIONS];
};

static kith_fabric_cell_key_t cell_for(uint32_t i)
{
    kith_fabric_cell_key_t k = {0};
    k.zone = 3u;
    k.cell_x = (int32_t)i;
    k.lod = 0u;
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

static int fixture_init(struct fixture *fx, uint32_t compose_budget_us)
{
    memset(fx, 0, sizeof(*fx));
    if (kith_proto_create(nullptr, nullptr, &fx->proto) != 0)
    {
        return -1;
    }
    if (kith_proto_register_type_id(fx->proto, "test.budget.batch", 60u) != 0)
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
    params.view_refresh_interval_ms = 1u;
    params.cache_refresh_interval_ms = 1u;
    params.compose_budget_us = compose_budget_us;
    params.replication_batch_type_id = 60u;
    if (kith_gateway_create(&params, fx->net, fx->fabric, fx->proto, nullptr, &fx->gw) != 0)
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
    for (size_t i = 0u; i < N_SESSIONS; ++i)
    {
        if (fx->sessions[i] != nullptr)
        {
            kith_gateway_session_destroy(fx->sessions[i]);
        }
        if (fx->conns[i] != nullptr)
        {
            kith_net_conn_close(fx->conns[i]);
            kith_net_conn_release(fx->conns[i]);
        }
    }
    kith_gateway_destroy(fx->gw);
    kith_fabric_destroy(fx->fabric);
    kith_sim_destroy(fx->sim);
    kith_net_destroy(fx->net);
    kith_proto_destroy(fx->proto);
}

static int make_conn(kith_net_t *net, kith_net_conn_t **out_conn)
{
    *out_conn = nullptr;
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0)
    {
        return -1;
    }
    struct sockaddr_in addr = {0};
    socklen_t addr_len = sizeof(addr);
    if (getsockname(kith_net_listener_fd(net), (struct sockaddr *)&addr, &addr_len) != 0 ||
        connect(fd, (struct sockaddr *)&addr, addr_len) != 0)
    {
        (void)close(fd);
        return -1;
    }
    int rc = kith_net_accept(net, out_conn);
    (void)close(fd);
    return rc;
}

// Populate cell @p i: subscriber actor plus two nearby candidates.
static int populate_cell(struct fixture *fx, uint32_t i)
{
    kith_fabric_cell_key_t cell = cell_for(i);
    kith_sim_artifact_key_t sk = {0};
    sk.zone = cell.zone;
    sk.cell_x = cell.cell_x;
    sk.lod = cell.lod;
    sk.authority_epoch = 1u;
    kith_sim_actor_t sub = make_actor(1000u + i, 0, 100u);
    if (kith_sim_publish_artifact(fx->sim, &sk, &sub, nullptr) != 0)
    {
        return -1;
    }
    kith_sim_actor_t a = make_actor(10u + i, 1LL << KITH_SIM_FIX_SHIFT, 0u);
    if (kith_sim_publish_artifact(fx->sim, &sk, &a, nullptr) != 0)
    {
        return -1;
    }
    kith_sim_actor_t b = make_actor(20u + i, 2LL << KITH_SIM_FIX_SHIFT, 0u);
    if (kith_sim_publish_artifact(fx->sim, &sk, &b, nullptr) != 0)
    {
        return -1;
    }
    return kith_fabric_publish(fx->fabric, &cell, 1u, nullptr);
}

// Create @p count sessions bound to their own populated cells and drive one
// full pass so every session holds a composed view set.
static int build_world(struct fixture *fx)
{
    for (uint32_t i = 0u; i < N_SESSIONS; ++i)
    {
        if (make_conn(fx->net, &fx->conns[i]) != 0)
        {
            return -1;
        }
        if (kith_gateway_session_create(fx->gw,
                                        fx->conns[i],
                                        KITH_GATEWAY_SESSION_SUBSCRIBER,
                                        1u + i,
                                        nullptr,
                                        &fx->sessions[i]) != 0)
        {
            return -1;
        }
        if (kith_gateway_session_bind_actor(fx->sessions[i], 1000u + i) != 0)
        {
            return -1;
        }
        kith_fabric_cell_key_t cell = cell_for(i);
        if (kith_gateway_session_window_add(fx->sessions[i], &cell) != 0)
        {
            return -1;
        }
        if (populate_cell(fx, i) != 0)
        {
            return -1;
        }
    }
    // One tick drains the published products into the cache; the second
    // composes every session's view against them.
    if (kith_gateway_tick(fx->gw, 100u) != 0)
    {
        return -1;
    }
    return kith_gateway_tick(fx->gw, 200u);
}

// Republish every cell so the next cache refresh mints fresh content
// sequences and invalidates every session's skip baseline.
static int dirty_every_cell(struct fixture *fx)
{
    for (uint32_t i = 0u; i < N_SESSIONS; ++i)
    {
        kith_fabric_cell_key_t cell = cell_for(i);
        kith_sim_artifact_key_t sk = {0};
        sk.zone = cell.zone;
        sk.cell_x = cell.cell_x;
        sk.lod = cell.lod;
        sk.authority_epoch = 1u;
        kith_sim_actor_t moved = make_actor(20u + i, 9LL << KITH_SIM_FIX_SHIFT, 300u);
        if (kith_sim_publish_artifact(fx->sim, &sk, &moved, nullptr) != 0)
        {
            return -1;
        }
        if (kith_fabric_publish(fx->fabric, &cell, 2u, nullptr) != 0)
        {
            return -1;
        }
    }
    return 0;
}

static bool view_is_fresh(struct fixture *fx, size_t i, uint64_t min_built_at)
{
    kith_gateway_view_snapshot_t meta = {0};
    if (kith_gateway_view_snapshot(fx->gw, fx->sessions[i], &meta, nullptr, 0u, nullptr) != 0)
    {
        return false;
    }
    return meta.built_at_ms >= min_built_at && meta.selected_count > 0u;
}

static bool all_views_fresh(struct fixture *fx, uint64_t min_built_at)
{
    for (size_t i = 0u; i < N_SESSIONS; ++i)
    {
        if (!view_is_fresh(fx, i, min_built_at))
        {
            return false;
        }
    }
    return true;
}

static bool every_conn_has_queued_output(struct fixture *fx)
{
    for (size_t i = 0u; i < N_SESSIONS; ++i)
    {
        if ((kith_net_conn_events(fx->conns[i]) & KITH_NET_OUT) == 0u)
        {
            return false;
        }
    }
    return true;
}

// A one-microsecond budget under an all-cells-dirty storm defers some
// compositions, yet every session still receives its frame this tick, and
// the deferred views converge within N ticks because each tick's rotating
// start index hands the budget to the sessions deferred longest ago.
static int test_budget_defers_and_converges(void)
{
    int failures = 0;
    struct fixture fx;
    CHECK(fixture_init(&fx, 1u) == 0);
    CHECK(build_world(&fx) == 0);

    uint64_t skips_before = 0u;
    uint64_t deferrals_before = 0u;
    CHECK(kith_gateway_compose_skips(fx.gw, &skips_before) == 0);
    CHECK(kith_gateway_compose_deferrals(fx.gw, &deferrals_before) == 0);

    CHECK(dirty_every_cell(&fx) == 0);
    CHECK(kith_gateway_tick(fx.gw, 400u) == 0);

    uint64_t deferrals_now = 0u;
    CHECK(kith_gateway_compose_deferrals(fx.gw, &deferrals_now) == 0);
    CHECK(deferrals_now > 0u);
    CHECK(deferrals_now > deferrals_before);

    // Delivery cadence held: even deferred sessions had their retained view
    // set enqueued this tick.
    CHECK(every_conn_has_queued_output(&fx));

    // Convergence: keep ticking; each pass makes at least one composition
    // (the head-of-pass guarantee) and the rotating start index cycles the
    // head through the table, so every deferred session recomposes within a
    // bounded number of passes. Two passes per session covers the rotation.
    bool converged = false;
    for (uint32_t t = 0u; t < 2u * N_SESSIONS && !converged; ++t)
    {
        CHECK(kith_gateway_tick(fx.gw, 500u + t) == 0);
        converged = all_views_fresh(&fx, 400u);
    }
    CHECK(converged);

    fixture_fini(&fx);
    return failures;
}

// The disabled sentinel never defers: after the same storm every view is
// fresh on the first post-storm tick and the deferral counter stays zero.
static int test_disabled_budget_never_defers(void)
{
    int failures = 0;
    struct fixture fx;
    CHECK(fixture_init(&fx, UINT32_MAX) == 0);
    CHECK(build_world(&fx) == 0);

    CHECK(dirty_every_cell(&fx) == 0);
    CHECK(kith_gateway_tick(fx.gw, 400u) == 0);

    uint64_t deferrals = 1u;
    CHECK(kith_gateway_compose_deferrals(fx.gw, &deferrals) == 0);
    CHECK(deferrals == 0u);
    CHECK(all_views_fresh(&fx, 400u));

    fixture_fini(&fx);
    return failures;
}

// A session with no retained view set composes even when the budget is long
// exhausted: without this guarantee its connection receives no frame
// until it happens to win the budget, which is exactly the arrival gap the
// budget exists to prevent.
static int test_never_composed_session_composes(void)
{
    int failures = 0;
    struct fixture fx;
    CHECK(fixture_init(&fx, 1u) == 0);
    CHECK(build_world(&fx) == 0);

    // A fresh session over the same dirty world: bound actor, window, but no
    // composition has ever run for it.
    kith_net_conn_t *conn = nullptr;
    CHECK(make_conn(fx.net, &conn) == 0);
    kith_gateway_session_t *fresh = nullptr;
    CHECK(kith_gateway_session_create(
              fx.gw, conn, KITH_GATEWAY_SESSION_SUBSCRIBER, 42u, nullptr, &fresh) == 0);
    CHECK(kith_gateway_session_bind_actor(fresh, 4242u) == 0);
    kith_fabric_cell_key_t cell = cell_for(0u);
    CHECK(kith_gateway_session_window_add(fresh, &cell) == 0);

    CHECK(dirty_every_cell(&fx) == 0);

    // The bound actor must exist in the windowed cell for the composer to
    // locate the subscriber; publish it after the dirt pass so its publish
    // sequence stays above the one dirty_every_cell used.
    kith_sim_artifact_key_t sk = {0};
    sk.zone = cell.zone;
    sk.cell_x = cell.cell_x;
    sk.lod = cell.lod;
    sk.authority_epoch = 1u;
    kith_sim_actor_t self = make_actor(4242u, 0, 350u);
    CHECK(kith_sim_publish_artifact(fx.sim, &sk, &self, nullptr) == 0);
    CHECK(kith_fabric_publish(fx.fabric, &cell, 3u, nullptr) == 0);

    CHECK(kith_gateway_tick(fx.gw, 400u) == 0);

    kith_gateway_view_snapshot_t meta = {0};
    CHECK(kith_gateway_view_snapshot(fx.gw, fresh, &meta, nullptr, 0u, nullptr) == 0);
    CHECK(meta.selected_count > 0u);

    kith_gateway_session_destroy(fresh);
    kith_net_conn_close(conn);
    kith_net_conn_release(conn);
    fixture_fini(&fx);
    return failures;
}

static int test_accessor_null_rejection(void)
{
    int failures = 0;
    struct fixture fx;
    CHECK(fixture_init(&fx, UINT32_MAX) == 0);

    uint64_t out = 0u;
    CHECK(kith_gateway_compose_deferrals(nullptr, &out) == kith_error_return(KITH_EINVAL));
    CHECK(kith_gateway_compose_deferrals(fx.gw, nullptr) == kith_error_return(KITH_EINVAL));

    fixture_fini(&fx);
    return failures;
}

int main(void)
{
    int rc = 0;
    rc |= test_accessor_null_rejection();
    rc |= test_budget_defers_and_converges();
    rc |= test_disabled_budget_never_defers();
    rc |= test_never_composed_session_composes();
    if (rc != 0)
    {
        (void)fprintf(stderr, "gateway compose budget tests FAILED\n");
    }
    return rc;
}
