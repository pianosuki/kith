#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include <unistd.h>

#include "gateway/delivery/delivery.h"
#include "gateway/view/view.h"
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
        (void)fprintf(stderr, "gateway view: assertion at line %d failed\n", line);
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

// Every view entry point rejects a NULL handle (and NULL sub-argument where
// the function takes one) with EINVAL. view_refresh on a session with no
// bound actor id returns ESTATE.
static int test_view_arg_validation(void)
{
    int failures = 0;
    struct fixture fx;
    CHECK(fixture_init(&fx) == 0);

    kith_net_conn_t *conn = nullptr;
    CHECK(make_conn(fx.net, &conn) == 0);
    kith_gateway_session_t *s = nullptr;
    CHECK(kith_gateway_session_create(
              fx.gw, conn, KITH_GATEWAY_SESSION_SUBSCRIBER, 1u, nullptr, &s) == 0);

    CHECK(kith_gateway_view_refresh(nullptr, s, 0u) == kith_error_return(KITH_EINVAL));
    CHECK(kith_gateway_view_refresh(fx.gw, nullptr, 0u) == kith_error_return(KITH_EINVAL));
    // No bound actor id: ESTATE.
    CHECK(kith_gateway_view_refresh(fx.gw, s, 100u) == kith_error_return(KITH_ESTATE));

    kith_gateway_view_snapshot_t meta = {0};
    CHECK(kith_gateway_view_snapshot(nullptr, s, &meta, nullptr, 0u, nullptr) ==
          kith_error_return(KITH_EINVAL));
    CHECK(kith_gateway_view_snapshot(fx.gw, nullptr, &meta, nullptr, 0u, nullptr) ==
          kith_error_return(KITH_EINVAL));
    CHECK(kith_gateway_view_snapshot(fx.gw, s, nullptr, nullptr, 0u, nullptr) ==
          kith_error_return(KITH_EINVAL));

    kith_gateway_session_destroy(s);
    kith_net_conn_close(conn);
    kith_net_conn_release(conn);
    fixture_fini(&fx);
    return failures;
}

// With one subscriber actor in the cache and a few nearby actors, view_refresh
// composes a view set whose first subject is the subscriber (self, full
// fidelity) and whose remaining subjects are the nearby actors sorted by
// squared distance. The self subject carries the subscriber's actor id and
// full-fidelity position/velocity.
static int test_view_self_and_nearby(void)
{
    int failures = 0;
    struct fixture fx;
    CHECK(fixture_init(&fx) == 0);

    kith_fabric_cell_key_t cell = make_key(1u, 0, 0, 0, 0u);

    kith_net_conn_t *conn = nullptr;
    CHECK(make_conn(fx.net, &conn) == 0);
    kith_gateway_session_t *s = nullptr;
    CHECK(kith_gateway_session_create(
              fx.gw, conn, KITH_GATEWAY_SESSION_SUBSCRIBER, 1u, nullptr, &s) == 0);
    CHECK(kith_gateway_session_bind_actor(s, 100u) == 0);
    CHECK(kith_gateway_session_window_add(s, &cell) == 0);

    // Subscriber at (0,0); three nearby actors at increasing distance.
    kith_sim_artifact_key_t sk = make_skey(&cell, 1u);
    kith_sim_actor_t sub = make_actor(100u, 0, 0, 7u);
    CHECK(kith_sim_publish_artifact(fx.sim, &sk, &sub, nullptr) == 0);
    kith_sim_actor_t near = make_actor(1u, 1LL << KITH_SIM_FIX_SHIFT, 0, 0u);
    CHECK(kith_sim_publish_artifact(fx.sim, &sk, &near, nullptr) == 0);
    kith_sim_actor_t mid = make_actor(2u, 2LL << KITH_SIM_FIX_SHIFT, 0, 0u);
    CHECK(kith_sim_publish_artifact(fx.sim, &sk, &mid, nullptr) == 0);
    kith_sim_actor_t far = make_actor(3u, 4LL << KITH_SIM_FIX_SHIFT, 0, 0u);
    CHECK(kith_sim_publish_artifact(fx.sim, &sk, &far, nullptr) == 0);
    CHECK(kith_fabric_publish(fx.fabric, &cell, 1u, nullptr) == 0);
    CHECK(kith_gateway_cache_refresh(fx.gw, 100u) == 0);

    CHECK(kith_gateway_view_refresh(fx.gw, s, 200u) == 0);

    kith_gateway_view_snapshot_t meta = {0};
    kith_gateway_view_subject_t out[8] = {0};
    size_t n = 99u;
    CHECK(kith_gateway_view_snapshot(fx.gw, s, &meta, out, 8u, &n) == 0);
    CHECK(n == 4u);
    // Self is first.
    CHECK(out[0].actor_id == 100u);
    CHECK(out[0].subject_class == KITH_GATEWAY_VIEW_CLASS_SELF);
    CHECK(out[0].level == KITH_FABRIC_LEVEL_FULL);
    CHECK(out[0].pos_x == 0);
    CHECK(out[0].input_tick == 7u);
    // Remaining subjects sorted by distance: 1, 2, 4.
    CHECK(out[1].actor_id == 1u);
    CHECK(out[1].subject_class == KITH_GATEWAY_VIEW_CLASS_ACTOR);
    CHECK(out[2].actor_id == 2u);
    CHECK(out[3].actor_id == 3u);
    // Same cell as subscriber → full tier.
    CHECK(out[1].level == KITH_FABRIC_LEVEL_FULL);

    CHECK(meta.selected_count == 4u);
    CHECK(meta.candidate_count == 3u);
    CHECK(meta.class_selected_count[KITH_GATEWAY_VIEW_CLASS_SELF] == 1u);
    CHECK(meta.class_selected_count[KITH_GATEWAY_VIEW_CLASS_ACTOR] == 3u);
    CHECK(meta.tier_selected_count[KITH_FABRIC_LEVEL_FULL] == 4u);
    CHECK(meta.subscriber_actor_id == 100u);
    CHECK(meta.built_at_ms == 200u);

    kith_gateway_session_destroy(s);
    kith_net_conn_close(conn);
    kith_net_conn_release(conn);
    fixture_fini(&fx);
    return failures;
}

// Self-echo coverage stamping: with stamping enabled, a rebuilt
// self subject carries the actor's publisher-minted update_seq and advances
// exactly one stamp counter per rebuild; an unminted source advances the
// fallback counter; an unchanged-content rebuild is skipped and counts
// nothing; with stamping disabled the subject's counter stays zero and
// neither counter moves.
static int fixture_init_stamping(struct fixture *fx, bool enabled)
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
    kith_gateway_params_t params = {0};
    params.size = sizeof(params);
    params.abi_version = KITH_ABI_VERSION;
    params.self_echo_disabled = !enabled;
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

// The composed self subject carries the actor's publisher-minted
// update_seq, a full composition advances the stamp counter once, and a
// skipped rebuild stamps nothing.
static int test_view_self_echo_stamping(void)
{
    int failures = 0;
    struct fixture fx;
    CHECK(fixture_init_stamping(&fx, true) == 0);

    kith_fabric_cell_key_t cell = make_key(1u, 0, 0, 0, 0u);

    kith_net_conn_t *conn = nullptr;
    CHECK(make_conn(fx.net, &conn) == 0);
    kith_gateway_session_t *s = nullptr;
    CHECK(kith_gateway_session_create(
              fx.gw, conn, KITH_GATEWAY_SESSION_SUBSCRIBER, 1u, nullptr, &s) == 0);
    CHECK(kith_gateway_session_bind_actor(s, 100u) == 0);
    CHECK(kith_gateway_session_window_add(s, &cell) == 0);

    kith_sim_artifact_key_t sk = make_skey(&cell, 1u);
    kith_sim_actor_t sub = make_actor(100u, 0, 0, 7u);
    sub.update_seq = 5u;
    CHECK(kith_sim_publish_artifact(fx.sim, &sk, &sub, nullptr) == 0);
    CHECK(kith_fabric_publish(fx.fabric, &cell, 1u, nullptr) == 0);
    CHECK(kith_gateway_cache_refresh(fx.gw, 100u) == 0);

    CHECK(kith_gateway_view_refresh(fx.gw, s, 200u) == 0);

    kith_gateway_view_snapshot_t meta = {0};
    kith_gateway_view_subject_t out[8] = {0};
    size_t n = 99u;
    CHECK(kith_gateway_view_snapshot(fx.gw, s, &meta, out, 8u, &n) == 0);
    CHECK(n == 1u);
    CHECK(out[0].update_seq == 5u);

    kith_gateway_phase_stats_t stats = {0};
    CHECK(kith_gateway_phase_stats(fx.gw, &stats) == 0);
    CHECK(stats.self_echo_stamps_total == 1u);
    CHECK(stats.self_echo_fallbacks_total == 0u);

    // An unchanged rebuild is a skip: no second stamp.
    CHECK(kith_gateway_view_refresh(fx.gw, s, 300u) == 0);
    CHECK(kith_gateway_phase_stats(fx.gw, &stats) == 0);
    CHECK(stats.self_echo_stamps_total == 1u);

    // A session bound to an actor that never minted is a fallback.
    kith_sim_actor_t unminted = make_actor(101u, 3LL << KITH_SIM_FIX_SHIFT, 0, 9u);
    CHECK(kith_sim_publish_artifact(fx.sim, &sk, &unminted, nullptr) == 0);
    CHECK(kith_fabric_publish(fx.fabric, &cell, 1u, nullptr) == 0);
    CHECK(kith_gateway_cache_refresh(fx.gw, 400u) == 0);

    kith_net_conn_t *conn2 = nullptr;
    CHECK(make_conn(fx.net, &conn2) == 0);
    kith_gateway_session_t *s2 = nullptr;
    CHECK(kith_gateway_session_create(
              fx.gw, conn2, KITH_GATEWAY_SESSION_SUBSCRIBER, 1u, nullptr, &s2) == 0);
    CHECK(kith_gateway_session_bind_actor(s2, 101u) == 0);
    CHECK(kith_gateway_session_window_add(s2, &cell) == 0);
    CHECK(kith_gateway_view_refresh(fx.gw, s2, 500u) == 0);

    kith_gateway_view_subject_t out2[8] = {0};
    n = 99u;
    CHECK(kith_gateway_view_snapshot(fx.gw, s2, &meta, out2, 8u, &n) == 0);
    CHECK(n == 2u);
    CHECK(out2[0].update_seq == 0u);

    CHECK(kith_gateway_phase_stats(fx.gw, &stats) == 0);
    CHECK(stats.self_echo_stamps_total == 1u);
    CHECK(stats.self_echo_fallbacks_total == 1u);

    kith_gateway_session_destroy(s2);
    kith_net_conn_close(conn2);
    kith_net_conn_release(conn2);
    kith_gateway_session_destroy(s);
    kith_net_conn_close(conn);
    kith_net_conn_release(conn);
    fixture_fini(&fx);
    return failures;
}

// Disabled stamping leaves the subject's counter at zero and advances
// nothing, even for a minted source.
static int test_view_self_echo_stamping_disabled(void)
{
    int failures = 0;
    struct fixture off;
    CHECK(fixture_init_stamping(&off, false) == 0);
    kith_fabric_cell_key_t cell2 = make_key(2u, 0, 0, 0, 0u);
    kith_net_conn_t *conn3 = nullptr;
    CHECK(make_conn(off.net, &conn3) == 0);
    kith_gateway_session_t *s3 = nullptr;
    CHECK(kith_gateway_session_create(
              off.gw, conn3, KITH_GATEWAY_SESSION_SUBSCRIBER, 1u, nullptr, &s3) == 0);
    CHECK(kith_gateway_session_bind_actor(s3, 100u) == 0);
    CHECK(kith_gateway_session_window_add(s3, &cell2) == 0);

    kith_sim_artifact_key_t sk2 = make_skey(&cell2, 1u);
    kith_sim_actor_t sub = make_actor(100u, 0, 0, 7u);
    sub.update_seq = 5u;
    CHECK(kith_sim_publish_artifact(off.sim, &sk2, &sub, nullptr) == 0);
    CHECK(kith_fabric_publish(off.fabric, &cell2, 1u, nullptr) == 0);
    CHECK(kith_gateway_cache_refresh(off.gw, 100u) == 0);
    CHECK(kith_gateway_view_refresh(off.gw, s3, 200u) == 0);

    kith_gateway_view_snapshot_t meta = {0};
    kith_gateway_view_subject_t out3[8] = {0};
    size_t n = 99u;
    CHECK(kith_gateway_view_snapshot(off.gw, s3, &meta, out3, 8u, &n) == 0);
    CHECK(n == 1u);
    CHECK(out3[0].update_seq == 0u);
    kith_gateway_phase_stats_t stats = {0};
    CHECK(kith_gateway_phase_stats(off.gw, &stats) == 0);
    CHECK(stats.self_echo_stamps_total == 0u);
    CHECK(stats.self_echo_fallbacks_total == 0u);

    kith_gateway_session_destroy(s3);
    kith_net_conn_close(conn3);
    kith_net_conn_release(conn3);
    fixture_fini(&off);
    return failures;
}

static int fixture_init_budget_core(struct fixture *fx,
                                    uint32_t max_subjects,
                                    uint32_t refresh_ms,
                                    uint32_t crowd_exit_margin)
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
    kith_gateway_params_t params = {0};
    params.size = sizeof(params);
    params.abi_version = KITH_ABI_VERSION;
    params.view_max_subjects = max_subjects;
    params.view_refresh_interval_ms = refresh_ms;
    params.crowd_exit_margin = crowd_exit_margin;
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

static int fixture_init_budget(struct fixture *fx, uint32_t max_subjects, uint32_t refresh_ms)
{
    return fixture_init_budget_core(fx, max_subjects, refresh_ms, 0u);
}

// The view set is bounded to the configured max_subjects. With a tight budget
// and more candidates than the budget, only the closest candidates are
// selected (the self subject always occupies one slot, so the actor budget is
// max_subjects - 1).
static int test_view_budget_bound(void)
{
    int failures = 0;
    struct fixture fx;
    // Recreate the gateway with a small view_max_subjects.
    CHECK(fixture_init_budget(&fx, 3u, KITH_GATEWAY_DEFAULT_VIEW_REFRESH_MS) == 0);

    kith_fabric_cell_key_t cell = make_key(2u, 0, 0, 0, 0u);

    kith_net_conn_t *conn = nullptr;
    CHECK(make_conn(fx.net, &conn) == 0);
    kith_gateway_session_t *s = nullptr;
    CHECK(kith_gateway_session_create(
              fx.gw, conn, KITH_GATEWAY_SESSION_SUBSCRIBER, 1u, nullptr, &s) == 0);
    CHECK(kith_gateway_session_bind_actor(s, 200u) == 0);
    CHECK(kith_gateway_session_window_add(s, &cell) == 0);

    kith_sim_artifact_key_t sk = make_skey(&cell, 1u);
    kith_sim_actor_t sub = make_actor(200u, 0, 0, 0u);
    CHECK(kith_sim_publish_artifact(fx.sim, &sk, &sub, nullptr) == 0);
    // Five candidates at increasing distance.
    for (uint64_t id = 1u; id <= 5u; ++id)
    {
        kith_sim_actor_t a = make_actor(id, (int64_t)id << KITH_SIM_FIX_SHIFT, 0, 0u);
        CHECK(kith_sim_publish_artifact(fx.sim, &sk, &a, nullptr) == 0);
    }
    CHECK(kith_fabric_publish(fx.fabric, &cell, 1u, nullptr) == 0);
    CHECK(kith_gateway_cache_refresh(fx.gw, 100u) == 0);

    CHECK(kith_gateway_view_refresh(fx.gw, s, 200u) == 0);

    kith_gateway_view_snapshot_t meta = {0};
    kith_gateway_view_subject_t out[8] = {0};
    size_t n = 99u;
    CHECK(kith_gateway_view_snapshot(fx.gw, s, &meta, out, 8u, &n) == 0);
    // Budget = 3: self + the 2 closest candidates (actors 1 and 2).
    CHECK(n == 3u);
    CHECK(out[0].actor_id == 200u);
    CHECK(out[1].actor_id == 1u);
    CHECK(out[2].actor_id == 2u);
    CHECK(meta.selected_count == 3u);
    CHECK(meta.candidate_count == 5u);

    kith_gateway_session_destroy(s);
    kith_net_conn_close(conn);
    kith_net_conn_release(conn);
    fixture_fini(&fx);
    return failures;
}

static kith_fabric_product_level_t
tier_of(const kith_gateway_view_subject_t *out, size_t n, uint64_t actor_id)
{
    for (size_t i = 0u; i < n; ++i)
    {
        if (out[i].actor_id == actor_id)
        {
            return out[i].level;
        }
    }
    return KITH_FABRIC_LEVEL_FULL;
}

static uint32_t tick_of(const kith_gateway_view_subject_t *out, size_t n, uint64_t actor_id)
{
    for (size_t i = 0u; i < n; ++i)
    {
        if (out[i].actor_id == actor_id)
        {
            return out[i].input_tick;
        }
    }
    return 0u;
}

/** Advance one rebuild cycle past both refresh intervals (+1000 ms to the
 *  cache clock, +1000 ms to the view clock) and optionally snapshot the
 *  recomposed view into @p out_meta / @p out / @p out_count. Returns 0 on
 *  success. */
static int view_step(struct fixture *fx,
                     kith_gateway_session_t *s,
                     uint64_t *io_now,
                     kith_gateway_view_snapshot_t *out_meta,
                     kith_gateway_view_subject_t *out,
                     size_t *out_count)
{
    *io_now += 1000u;
    if (kith_gateway_cache_refresh(fx->gw, *io_now) != 0)
    {
        return -1;
    }
    *io_now += 1000u;
    if (kith_gateway_view_refresh(fx->gw, s, *io_now) != 0)
    {
        return -1;
    }
    if (out_meta)
    {
        return kith_gateway_view_snapshot(fx->gw, s, out_meta, out, 8u, out_count);
    }
    return 0;
}

/** Consume the session's fresh membership delta and require exactly three
 *  ENTER events, for fixture ids 10, 20, and 30. Returns the number of
 *  failed checks. */
static int delta_expect_enters(kith_gateway_session_t *s)
{
    const struct gateway_view_event *ev = nullptr;
    const size_t n = gateway_view_take_membership(s, &ev);
    if (n != 3u)
    {
        return 1;
    }
    return !(ev[0].actor_id == 10u && ev[0].kind == (unsigned)GATEWAY_DELIVERY_EVENT_ENTER &&
             ev[1].actor_id == 20u && ev[1].kind == (unsigned)GATEWAY_DELIVERY_EVENT_ENTER &&
             ev[2].actor_id == 30u && ev[2].kind == (unsigned)GATEWAY_DELIVERY_EVENT_ENTER);
}

/** Consume the session's fresh membership delta and require exactly one
 *  event: @p kind for @p actor_id. Returns the number of failed checks. */
static int delta_expect_one(kith_gateway_session_t *s, uint64_t actor_id, unsigned kind)
{
    const struct gateway_view_event *ev = nullptr;
    const size_t n = gateway_view_take_membership(s, &ev);
    if (n != 1u || ev[0].actor_id != actor_id || ev[0].kind != kind)
    {
        return 1;
    }
    return 0;
}

// Count the transitions of one kind among one composition's membership
// events and drain the queue; ENTER noise from members changing class is
// ignored.
static int crowd_transitions(kith_gateway_session_t *s, unsigned int kind)
{
    const struct gateway_view_event *ev = nullptr;
    const size_t n = gateway_view_take_membership(s, &ev);
    size_t hits = 0u;
    for (size_t i = 0u; i < n; ++i)
    {
        if (ev[i].kind == kind)
        {
            hits += 1u;
        }
    }
    return (int)hits;
}

// A subject in a cell adjacent (Chebyshev distance 1) to the subscriber's
// cell within the same zone is selected at full fidelity; a subject in a cell
// farther away (Chebyshev distance >= 2) is demoted to reduced fidelity and
// its input tick is cleared. A subject in a cell of a different zone (even at
// Chebyshev distance 0) is reduced.
static int test_view_tier_selection(void)
{
    int failures = 0;
    struct fixture fx;
    CHECK(fixture_init(&fx) == 0);

    // Subscriber in cell (zone 1, 0,0,0). Place the subscriber and one actor
    // in the same cell, one in the adjacent cell (zone 1, 1,0,0), one in a
    // far cell (zone 1, 3,0,0), and one in a different-zone cell at (2,0,0).
    kith_fabric_cell_key_t home = make_key(1u, 0, 0, 0, 0u);
    kith_fabric_cell_key_t adj = make_key(1u, 1, 0, 0, 0u);
    kith_fabric_cell_key_t far = make_key(1u, 3, 0, 0, 0u);
    kith_fabric_cell_key_t other_zone = make_key(2u, 0, 0, 0, 0u);

    kith_net_conn_t *conn = nullptr;
    CHECK(make_conn(fx.net, &conn) == 0);
    kith_gateway_session_t *s = nullptr;
    CHECK(kith_gateway_session_create(
              fx.gw, conn, KITH_GATEWAY_SESSION_SUBSCRIBER, 1u, nullptr, &s) == 0);
    CHECK(kith_gateway_session_bind_actor(s, 500u) == 0);
    CHECK(kith_gateway_session_window_add(s, &home) == 0);
    CHECK(kith_gateway_session_window_add(s, &adj) == 0);
    CHECK(kith_gateway_session_window_add(s, &far) == 0);
    CHECK(kith_gateway_session_window_add(s, &other_zone) == 0);

    kith_sim_actor_t sub = make_actor(500u, 0, 0, 0u);
    kith_sim_artifact_key_t sk_home = make_skey(&home, 1u);
    CHECK(kith_sim_publish_artifact(fx.sim, &sk_home, &sub, nullptr) == 0);

    kith_sim_actor_t a_home = make_actor(1u, 0, 0, 11u);
    CHECK(kith_sim_publish_artifact(fx.sim, &sk_home, &a_home, nullptr) == 0);
    kith_sim_artifact_key_t sk_adj = make_skey(&adj, 1u);
    kith_sim_actor_t a_adj = make_actor(2u, 0, 0, 22u);
    CHECK(kith_sim_publish_artifact(fx.sim, &sk_adj, &a_adj, nullptr) == 0);
    kith_sim_artifact_key_t sk_far = make_skey(&far, 1u);
    kith_sim_actor_t a_far = make_actor(3u, 0, 0, 33u);
    CHECK(kith_sim_publish_artifact(fx.sim, &sk_far, &a_far, nullptr) == 0);
    kith_sim_artifact_key_t sk_other = make_skey(&other_zone, 1u);
    kith_sim_actor_t a_other = make_actor(4u, 0, 0, 44u);
    CHECK(kith_sim_publish_artifact(fx.sim, &sk_other, &a_other, nullptr) == 0);

    CHECK(kith_fabric_publish(fx.fabric, &home, 1u, nullptr) == 0);
    CHECK(kith_fabric_publish(fx.fabric, &adj, 1u, nullptr) == 0);
    CHECK(kith_fabric_publish(fx.fabric, &far, 1u, nullptr) == 0);
    CHECK(kith_fabric_publish(fx.fabric, &other_zone, 1u, nullptr) == 0);
    CHECK(kith_gateway_cache_refresh(fx.gw, 100u) == 0);

    CHECK(kith_gateway_view_refresh(fx.gw, s, 200u) == 0);

    kith_gateway_view_snapshot_t meta = {0};
    kith_gateway_view_subject_t out[8] = {0};
    size_t n = 0u;
    CHECK(kith_gateway_view_snapshot(fx.gw, s, &meta, out, 8u, &n) == 0);
    // self + home actor + adjacent actor + far actor + other-zone actor = 5.
    CHECK(n == 5u);
    CHECK(out[0].actor_id == 500u);
    CHECK(out[0].level == KITH_FABRIC_LEVEL_FULL);

    CHECK(tier_of(out, n, 1u) == KITH_FABRIC_LEVEL_FULL);
    CHECK(tick_of(out, n, 1u) == 11u);
    CHECK(tier_of(out, n, 2u) == KITH_FABRIC_LEVEL_FULL);
    CHECK(tier_of(out, n, 3u) == KITH_FABRIC_LEVEL_REDUCED);
    CHECK(tick_of(out, n, 3u) == 0u);
    CHECK(tier_of(out, n, 4u) == KITH_FABRIC_LEVEL_REDUCED);

    kith_gateway_session_destroy(s);
    kith_net_conn_close(conn);
    kith_net_conn_release(conn);
    fixture_fini(&fx);
    return failures;
}

// A cell whose actor count exceeds the view budget produces a crowd aggregate
// for the overflow: the closest candidates are selected individually up to
// budget - 1, and one crowd subject (actor_id 0, CROWD class and level)
// represents the remaining actors at their centroid. With a budget of 5
// (self + 4 slots) and 13 candidates, the closest 3 actors are selected
// individually and the remaining 10 form the crowd (reserving one slot).
static int test_view_crowd_aggregate(void)
{
    int failures = 0;
    struct fixture fx;
    // Budget of 5: self + 3 individuals + 1 crowd = 5.
    CHECK(fixture_init_budget(&fx, 5u, KITH_GATEWAY_DEFAULT_VIEW_REFRESH_MS) == 0);

    kith_fabric_cell_key_t dense = make_key(3u, 0, 0, 0, 0u);
    kith_fabric_cell_key_t home = make_key(3u, 1, 0, 0, 0u);

    kith_net_conn_t *conn = nullptr;
    CHECK(make_conn(fx.net, &conn) == 0);
    kith_gateway_session_t *s = nullptr;
    CHECK(kith_gateway_session_create(
              fx.gw, conn, KITH_GATEWAY_SESSION_SUBSCRIBER, 1u, nullptr, &s) == 0);
    CHECK(kith_gateway_session_bind_actor(s, 900u) == 0);
    CHECK(kith_gateway_session_window_add(s, &dense) == 0);
    CHECK(kith_gateway_session_window_add(s, &home) == 0);

    // Subscriber in the adjacent home cell.
    kith_sim_actor_t sub = make_actor(900u, 0, 0, 0u);
    kith_sim_artifact_key_t sk_home = make_skey(&home, 1u);
    CHECK(kith_sim_publish_artifact(fx.sim, &sk_home, &sub, nullptr) == 0);

    // 13 actors in the dense cell at positions 1..13.
    kith_sim_artifact_key_t sk_dense = make_skey(&dense, 1u);
    for (uint64_t id = 1u; id <= 13u; ++id)
    {
        kith_sim_actor_t a = make_actor(id, (int64_t)id << KITH_SIM_FIX_SHIFT, 0, 0u);
        CHECK(kith_sim_publish_artifact(fx.sim, &sk_dense, &a, nullptr) == 0);
    }
    CHECK(kith_fabric_publish(fx.fabric, &dense, 1u, nullptr) == 0);
    CHECK(kith_fabric_publish(fx.fabric, &home, 1u, nullptr) == 0);
    CHECK(kith_gateway_cache_refresh(fx.gw, 100u) == 0);

    CHECK(kith_gateway_view_refresh(fx.gw, s, 200u) == 0);

    kith_gateway_view_snapshot_t meta = {0};
    kith_gateway_view_subject_t out[8] = {0};
    size_t n = 0u;
    CHECK(kith_gateway_view_snapshot(fx.gw, s, &meta, out, 8u, &n) == 0);
    // self + 3 closest individuals + 1 crowd = 5.
    CHECK(n == 5u);
    CHECK(out[0].actor_id == 900u);
    CHECK(out[1].actor_id == 1u);
    CHECK(out[1].subject_class == KITH_GATEWAY_VIEW_CLASS_ACTOR);
    CHECK(out[2].actor_id == 2u);
    CHECK(out[3].actor_id == 3u);
    // Crowd subject for overflow (actors 4-13).
    CHECK(out[4].actor_id == 0u);
    CHECK(out[4].subject_class == KITH_GATEWAY_VIEW_CLASS_CROWD);
    CHECK(out[4].level == KITH_FABRIC_LEVEL_CROWD);
    // Centroid of actors 4-13: sum = 85, count = 10.
    int64_t expected_crowd_x = 0;
    for (uint64_t id = 4u; id <= 13u; ++id)
    {
        expected_crowd_x += (int64_t)id << KITH_SIM_FIX_SHIFT;
    }
    expected_crowd_x /= 10;
    CHECK(out[4].pos_x == expected_crowd_x);
    CHECK(meta.class_selected_count[KITH_GATEWAY_VIEW_CLASS_CROWD] == 1u);
    CHECK(meta.class_selected_count[KITH_GATEWAY_VIEW_CLASS_ACTOR] == 3u);
    CHECK(meta.selected_count == 5u);
    CHECK(meta.candidate_count == 13u);

    kith_gateway_session_destroy(s);
    kith_net_conn_close(conn);
    kith_net_conn_release(conn);
    fixture_fini(&fx);
    return failures;
}

// A dense cell whose actor count is within the view budget produces individual
// subjects for every actor — no crowd aggregate, whatever the cell's raw
// count. With the default budget (512) and 23 candidates, all 23 are
// delivered individually.
static int test_view_dense_cell_within_budget(void)
{
    int failures = 0;
    struct fixture fx;
    CHECK(fixture_init(&fx) == 0);

    kith_fabric_cell_key_t cell = make_key(6u, 0, 0, 0, 0u);

    kith_net_conn_t *conn = nullptr;
    CHECK(make_conn(fx.net, &conn) == 0);
    kith_gateway_session_t *s = nullptr;
    CHECK(kith_gateway_session_create(
              fx.gw, conn, KITH_GATEWAY_SESSION_SUBSCRIBER, 1u, nullptr, &s) == 0);
    CHECK(kith_gateway_session_bind_actor(s, 1000u) == 0);
    CHECK(kith_gateway_session_window_add(s, &cell) == 0);

    kith_sim_artifact_key_t sk = make_skey(&cell, 1u);
    kith_sim_actor_t sub = make_actor(1000u, 0, 0, 0u);
    CHECK(kith_sim_publish_artifact(fx.sim, &sk, &sub, nullptr) == 0);
    // 23 other actors in the same cell.
    for (uint64_t id = 1u; id <= 23u; ++id)
    {
        kith_sim_actor_t a = make_actor(id, (int64_t)id << KITH_SIM_FIX_SHIFT, 0, 0u);
        CHECK(kith_sim_publish_artifact(fx.sim, &sk, &a, nullptr) == 0);
    }
    CHECK(kith_fabric_publish(fx.fabric, &cell, 1u, nullptr) == 0);
    CHECK(kith_gateway_cache_refresh(fx.gw, 100u) == 0);

    CHECK(kith_gateway_view_refresh(fx.gw, s, 200u) == 0);

    kith_gateway_view_snapshot_t meta = {0};
    kith_gateway_view_subject_t out[32] = {0};
    size_t n = 0u;
    CHECK(kith_gateway_view_snapshot(fx.gw, s, &meta, out, 32u, &n) == 0);
    // self + 23 individual actors = 24 subjects. No crowd.
    CHECK(n == 24u);
    CHECK(out[0].actor_id == 1000u);
    CHECK(out[0].subject_class == KITH_GATEWAY_VIEW_CLASS_SELF);
    for (size_t i = 1u; i < n; ++i)
    {
        CHECK(out[i].subject_class == KITH_GATEWAY_VIEW_CLASS_ACTOR);
        CHECK(out[i].actor_id != 0u);
    }
    CHECK(meta.class_selected_count[KITH_GATEWAY_VIEW_CLASS_CROWD] == 0u);
    CHECK(meta.class_selected_count[KITH_GATEWAY_VIEW_CLASS_ACTOR] == 23u);
    CHECK(meta.candidate_count == 23u);

    kith_gateway_session_destroy(s);
    kith_net_conn_close(conn);
    kith_net_conn_release(conn);
    fixture_fini(&fx);
    return failures;
}

// A subject retained from the prior view (still a candidate) is marked sticky
// and sorts ahead of non-sticky candidates at the same distance band. After
// a second refresh with a tight budget, prior members are retained first.
static int test_view_sticky_continuity(void)
{
    int failures = 0;
    struct fixture fx;
    // Recreate with a budget of 3 (self + 2 actors) and a 1 ms refresh
    // interval so the second refresh is not gated out.
    CHECK(fixture_init_budget(&fx, 3u, 1u) == 0);

    kith_fabric_cell_key_t cell = make_key(4u, 0, 0, 0, 0u);

    kith_net_conn_t *conn = nullptr;
    CHECK(make_conn(fx.net, &conn) == 0);
    kith_gateway_session_t *s = nullptr;
    CHECK(kith_gateway_session_create(
              fx.gw, conn, KITH_GATEWAY_SESSION_SUBSCRIBER, 1u, nullptr, &s) == 0);
    CHECK(kith_gateway_session_bind_actor(s, 700u) == 0);
    CHECK(kith_gateway_session_window_add(s, &cell) == 0);

    kith_sim_artifact_key_t sk = make_skey(&cell, 1u);
    kith_sim_actor_t sub = make_actor(700u, 0, 0, 0u);
    CHECK(kith_sim_publish_artifact(fx.sim, &sk, &sub, nullptr) == 0);
    // Two near actors (selected first).
    kith_sim_actor_t a1 = make_actor(10u, 1LL << KITH_SIM_FIX_SHIFT, 0, 0u);
    CHECK(kith_sim_publish_artifact(fx.sim, &sk, &a1, nullptr) == 0);
    kith_sim_actor_t a2 = make_actor(20u, 2LL << KITH_SIM_FIX_SHIFT, 0, 0u);
    CHECK(kith_sim_publish_artifact(fx.sim, &sk, &a2, nullptr) == 0);
    CHECK(kith_fabric_publish(fx.fabric, &cell, 1u, nullptr) == 0);
    CHECK(kith_gateway_cache_refresh(fx.gw, 100u) == 0);

    CHECK(kith_gateway_view_refresh(fx.gw, s, 200u) == 0);
    kith_gateway_view_snapshot_t meta1 = {0};
    kith_gateway_view_subject_t out1[8] = {0};
    size_t n1 = 0u;
    CHECK(kith_gateway_view_snapshot(fx.gw, s, &meta1, out1, 8u, &n1) == 0);
    CHECK(n1 == 3u);
    CHECK(out1[1].actor_id == 10u);
    CHECK(out1[2].actor_id == 20u);
    CHECK(meta1.sticky_selected_count == 0u);

    // Add a closer actor (id 5 at distance 0 via same cell position) that
    // displaces actor 20 under pure distance ordering, but actor 10 and
    // 20 are now prior members and sort ahead of non-prior candidates.
    kith_sim_actor_t closer = make_actor(5u, 0, 0, 0u);
    CHECK(kith_sim_publish_artifact(fx.sim, &sk, &closer, nullptr) == 0);
    CHECK(kith_fabric_publish(fx.fabric, &cell, 1u, nullptr) == 0);
    CHECK(kith_gateway_cache_refresh(fx.gw, 300u) == 0);
    CHECK(kith_gateway_view_refresh(fx.gw, s, 400u) == 0);

    kith_gateway_view_snapshot_t meta2 = {0};
    kith_gateway_view_subject_t out2[8] = {0};
    size_t n2 = 0u;
    CHECK(kith_gateway_view_snapshot(fx.gw, s, &meta2, out2, 8u, &n2) == 0);
    CHECK(n2 == 3u);
    // The two prior members (10, 20) are retained first; the closer new
    // candidate (5) is not selected because the budget is exhausted.
    CHECK(out2[1].actor_id == 10u);
    CHECK(out2[2].actor_id == 20u);
    CHECK(out2[1].sticky == true);
    CHECK(out2[2].sticky == true);
    CHECK(meta2.sticky_selected_count == 2u);

    kith_gateway_session_destroy(s);
    kith_net_conn_close(conn);
    kith_net_conn_release(conn);
    fixture_fini(&fx);
    return failures;
}

// view_refresh is gated by the configured interval: a second call within the
// interval is a no-op and retains the prior view set.
static int test_view_refresh_interval(void)
{
    int failures = 0;
    struct fixture fx;
    CHECK(fixture_init(&fx) == 0);

    kith_fabric_cell_key_t cell = make_key(5u, 0, 0, 0, 0u);

    kith_net_conn_t *conn = nullptr;
    CHECK(make_conn(fx.net, &conn) == 0);
    kith_gateway_session_t *s = nullptr;
    CHECK(kith_gateway_session_create(
              fx.gw, conn, KITH_GATEWAY_SESSION_SUBSCRIBER, 1u, nullptr, &s) == 0);
    CHECK(kith_gateway_session_bind_actor(s, 800u) == 0);
    CHECK(kith_gateway_session_window_add(s, &cell) == 0);

    kith_sim_artifact_key_t sk = make_skey(&cell, 1u);
    kith_sim_actor_t sub = make_actor(800u, 0, 0, 0u);
    CHECK(kith_sim_publish_artifact(fx.sim, &sk, &sub, nullptr) == 0);
    kith_sim_actor_t a1 = make_actor(1u, 1LL << KITH_SIM_FIX_SHIFT, 0, 0u);
    CHECK(kith_sim_publish_artifact(fx.sim, &sk, &a1, nullptr) == 0);
    CHECK(kith_fabric_publish(fx.fabric, &cell, 1u, nullptr) == 0);
    CHECK(kith_gateway_cache_refresh(fx.gw, 100u) == 0);

    CHECK(kith_gateway_view_refresh(fx.gw, s, 200u) == 0);
    kith_gateway_view_snapshot_t meta1 = {0};
    CHECK(kith_gateway_view_snapshot(fx.gw, s, &meta1, nullptr, 0u, nullptr) == 0);
    CHECK(meta1.built_at_ms == 200u);

    // Within the 100 ms interval: no-op, prior view retained.
    CHECK(kith_gateway_view_refresh(fx.gw, s, 250u) == 0);
    kith_gateway_view_snapshot_t meta2 = {0};
    CHECK(kith_gateway_view_snapshot(fx.gw, s, &meta2, nullptr, 0u, nullptr) == 0);
    CHECK(meta2.built_at_ms == 200u);

    kith_gateway_session_destroy(s);
    kith_net_conn_close(conn);
    kith_net_conn_release(conn);
    fixture_fini(&fx);
    return failures;
}

// With a 1 ms refresh interval and an unchanged world, the second refresh
// past the interval skips the recomposition: the skip counter advances by
// one and the retained subject set is byte-identical to the first
// composition, metadata included.
static int test_view_skip_when_unchanged(void)
{
    int failures = 0;
    struct fixture fx;
    CHECK(fixture_init_budget(&fx, 8u, 1u) == 0);

    kith_fabric_cell_key_t cell = make_key(7u, 0, 0, 0, 0u);

    kith_net_conn_t *conn = nullptr;
    CHECK(make_conn(fx.net, &conn) == 0);
    kith_gateway_session_t *s = nullptr;
    CHECK(kith_gateway_session_create(
              fx.gw, conn, KITH_GATEWAY_SESSION_SUBSCRIBER, 1u, nullptr, &s) == 0);
    CHECK(kith_gateway_session_bind_actor(s, 1100u) == 0);
    CHECK(kith_gateway_session_window_add(s, &cell) == 0);

    kith_sim_artifact_key_t sk = make_skey(&cell, 1u);
    kith_sim_actor_t sub = make_actor(1100u, 0, 0, 3u);
    CHECK(kith_sim_publish_artifact(fx.sim, &sk, &sub, nullptr) == 0);
    for (uint64_t id = 1u; id <= 3u; ++id)
    {
        kith_sim_actor_t a = make_actor(id, (int64_t)id << KITH_SIM_FIX_SHIFT, 0, 0u);
        CHECK(kith_sim_publish_artifact(fx.sim, &sk, &a, nullptr) == 0);
    }
    CHECK(kith_fabric_publish(fx.fabric, &cell, 1u, nullptr) == 0);
    CHECK(kith_gateway_cache_refresh(fx.gw, 100u) == 0);

    CHECK(kith_gateway_view_refresh(fx.gw, s, 200u) == 0);
    kith_gateway_view_snapshot_t meta1 = {0};
    kith_gateway_view_subject_t out1[8] = {0};
    size_t n1 = 0u;
    CHECK(kith_gateway_view_snapshot(fx.gw, s, &meta1, out1, 8u, &n1) == 0);
    CHECK(n1 == 4u);
    CHECK(meta1.built_at_ms == 200u);

    uint64_t skips0 = 0u;
    CHECK(kith_gateway_compose_skips(fx.gw, &skips0) == 0);

    // Nothing changed: the next refresh past the interval is a skip.
    CHECK(kith_gateway_view_refresh(fx.gw, s, 300u) == 0);
    uint64_t skips1 = 0u;
    CHECK(kith_gateway_compose_skips(fx.gw, &skips1) == 0);
    CHECK(skips1 == skips0 + 1u);
    kith_gateway_view_snapshot_t meta2 = {0};
    kith_gateway_view_subject_t out2[8] = {0};
    size_t n2 = 0u;
    CHECK(kith_gateway_view_snapshot(fx.gw, s, &meta2, out2, 8u, &n2) == 0);
    CHECK(n2 == n1);
    CHECK(memcmp(out1, out2, n1 * sizeof(*out1)) == 0);
    CHECK(meta2.built_at_ms == meta1.built_at_ms);
    CHECK(meta2.candidate_count == meta1.candidate_count);
    CHECK(meta2.selected_count == meta1.selected_count);

    kith_gateway_session_destroy(s);
    kith_net_conn_close(conn);
    kith_net_conn_release(conn);
    fixture_fini(&fx);
    return failures;
}

// A publish into a windowed cell replaces that node's cached content and
// advances its sequence: the skip baseline is invalidated and the next
// refresh fully recomposes, picking up the new actor without counting a
// skip.
static int test_view_skip_invalidated_by_publish(void)
{
    int failures = 0;
    struct fixture fx;
    CHECK(fixture_init_budget(&fx, 8u, 1u) == 0);

    kith_fabric_cell_key_t cell = make_key(11u, 0, 0, 0, 0u);

    kith_net_conn_t *conn = nullptr;
    CHECK(make_conn(fx.net, &conn) == 0);
    kith_gateway_session_t *s = nullptr;
    CHECK(kith_gateway_session_create(
              fx.gw, conn, KITH_GATEWAY_SESSION_SUBSCRIBER, 1u, nullptr, &s) == 0);
    CHECK(kith_gateway_session_bind_actor(s, 1100u) == 0);
    CHECK(kith_gateway_session_window_add(s, &cell) == 0);

    kith_sim_artifact_key_t sk = make_skey(&cell, 1u);
    kith_sim_actor_t sub = make_actor(1100u, 0, 0, 3u);
    CHECK(kith_sim_publish_artifact(fx.sim, &sk, &sub, nullptr) == 0);
    for (uint64_t id = 1u; id <= 3u; ++id)
    {
        kith_sim_actor_t a = make_actor(id, (int64_t)id << KITH_SIM_FIX_SHIFT, 0, 0u);
        CHECK(kith_sim_publish_artifact(fx.sim, &sk, &a, nullptr) == 0);
    }
    CHECK(kith_fabric_publish(fx.fabric, &cell, 1u, nullptr) == 0);
    CHECK(kith_gateway_cache_refresh(fx.gw, 100u) == 0);

    CHECK(kith_gateway_view_refresh(fx.gw, s, 200u) == 0);
    uint64_t skips0 = 0u;
    CHECK(kith_gateway_compose_skips(fx.gw, &skips0) == 0);

    kith_sim_actor_t late = make_actor(4u, 4LL << KITH_SIM_FIX_SHIFT, 0, 0u);
    CHECK(kith_sim_publish_artifact(fx.sim, &sk, &late, nullptr) == 0);
    CHECK(kith_fabric_publish(fx.fabric, &cell, 1u, nullptr) == 0);
    CHECK(kith_gateway_cache_refresh(fx.gw, 300u) == 0);
    CHECK(kith_gateway_view_refresh(fx.gw, s, 400u) == 0);
    uint64_t skips1 = 0u;
    CHECK(kith_gateway_compose_skips(fx.gw, &skips1) == 0);
    CHECK(skips1 == skips0);
    kith_gateway_view_snapshot_t meta = {0};
    kith_gateway_view_subject_t out[8] = {0};
    size_t n = 0u;
    CHECK(kith_gateway_view_snapshot(fx.gw, s, &meta, out, 8u, &n) == 0);
    CHECK(n == 5u);
    CHECK(out[0].actor_id == 1100u);
    CHECK(out[4].actor_id == 4u);
    CHECK(meta.built_at_ms == 400u);

    kith_gateway_session_destroy(s);
    kith_net_conn_close(conn);
    kith_net_conn_release(conn);
    fixture_fini(&fx);
    return failures;
}

// A window mutation invalidates the skip baseline even when every surviving
// cell's content is unchanged: after removing the adjacent cell from the
// window, the next refresh fully recomposes over the shrunken window.
static int test_view_skip_invalidated_by_window(void)
{
    int failures = 0;
    struct fixture fx;
    CHECK(fixture_init_budget(&fx, 8u, 1u) == 0);

    kith_fabric_cell_key_t home = make_key(8u, 0, 0, 0, 0u);
    kith_fabric_cell_key_t adj = make_key(8u, 1, 0, 0, 0u);

    kith_net_conn_t *conn = nullptr;
    CHECK(make_conn(fx.net, &conn) == 0);
    kith_gateway_session_t *s = nullptr;
    CHECK(kith_gateway_session_create(
              fx.gw, conn, KITH_GATEWAY_SESSION_SUBSCRIBER, 1u, nullptr, &s) == 0);
    CHECK(kith_gateway_session_bind_actor(s, 1200u) == 0);
    CHECK(kith_gateway_session_window_add(s, &home) == 0);
    CHECK(kith_gateway_session_window_add(s, &adj) == 0);

    kith_sim_artifact_key_t sk_home = make_skey(&home, 1u);
    kith_sim_actor_t sub = make_actor(1200u, 0, 0, 0u);
    CHECK(kith_sim_publish_artifact(fx.sim, &sk_home, &sub, nullptr) == 0);
    kith_sim_artifact_key_t sk_adj = make_skey(&adj, 1u);
    kith_sim_actor_t near = make_actor(1u, 0, 0, 0u);
    CHECK(kith_sim_publish_artifact(fx.sim, &sk_adj, &near, nullptr) == 0);
    CHECK(kith_fabric_publish(fx.fabric, &home, 1u, nullptr) == 0);
    CHECK(kith_fabric_publish(fx.fabric, &adj, 1u, nullptr) == 0);
    CHECK(kith_gateway_cache_refresh(fx.gw, 100u) == 0);

    CHECK(kith_gateway_view_refresh(fx.gw, s, 200u) == 0);
    uint64_t skips0 = 0u;
    CHECK(kith_gateway_compose_skips(fx.gw, &skips0) == 0);
    CHECK(kith_gateway_view_refresh(fx.gw, s, 250u) == 0);
    uint64_t skips1 = 0u;
    CHECK(kith_gateway_compose_skips(fx.gw, &skips1) == 0);
    CHECK(skips1 == skips0 + 1u);

    // Removing the adjacent cell bumps the window counter and evicts its
    // (sole-subscriber) node; the next refresh must recompose fully.
    CHECK(kith_gateway_session_window_remove(s, &adj) == 0);
    CHECK(kith_gateway_view_refresh(fx.gw, s, 300u) == 0);
    uint64_t skips2 = 0u;
    CHECK(kith_gateway_compose_skips(fx.gw, &skips2) == 0);
    CHECK(skips2 == skips1);
    kith_gateway_view_snapshot_t meta = {0};
    kith_gateway_view_subject_t out[8] = {0};
    size_t n = 0u;
    CHECK(kith_gateway_view_snapshot(fx.gw, s, &meta, out, 8u, &n) == 0);
    CHECK(n == 1u);
    CHECK(out[0].actor_id == 1200u);
    CHECK(out[0].subject_class == KITH_GATEWAY_VIEW_CLASS_SELF);

    kith_gateway_session_destroy(s);
    kith_net_conn_close(conn);
    kith_net_conn_release(conn);
    fixture_fini(&fx);
    return failures;
}

// Rebinding the session's actor id invalidates the skip baseline: both the
// self subject and the candidate exclusion filter derive from it. The next
// refresh recomposes fully with the new identity as the self subject.
static int test_view_skip_invalidated_by_rebind(void)
{
    int failures = 0;
    struct fixture fx;
    CHECK(fixture_init_budget(&fx, 8u, 1u) == 0);

    kith_fabric_cell_key_t cell = make_key(9u, 0, 0, 0, 0u);

    kith_net_conn_t *conn = nullptr;
    CHECK(make_conn(fx.net, &conn) == 0);
    kith_gateway_session_t *s = nullptr;
    CHECK(kith_gateway_session_create(
              fx.gw, conn, KITH_GATEWAY_SESSION_SUBSCRIBER, 1u, nullptr, &s) == 0);
    CHECK(kith_gateway_session_bind_actor(s, 1300u) == 0);
    CHECK(kith_gateway_session_window_add(s, &cell) == 0);

    kith_sim_artifact_key_t sk = make_skey(&cell, 1u);
    kith_sim_actor_t a = make_actor(1300u, 0, 0, 0u);
    CHECK(kith_sim_publish_artifact(fx.sim, &sk, &a, nullptr) == 0);
    kith_sim_actor_t b = make_actor(1301u, 5LL << KITH_SIM_FIX_SHIFT, 0, 0u);
    CHECK(kith_sim_publish_artifact(fx.sim, &sk, &b, nullptr) == 0);
    kith_sim_actor_t c = make_actor(1u, 1LL << KITH_SIM_FIX_SHIFT, 0, 0u);
    CHECK(kith_sim_publish_artifact(fx.sim, &sk, &c, nullptr) == 0);
    CHECK(kith_fabric_publish(fx.fabric, &cell, 1u, nullptr) == 0);
    CHECK(kith_gateway_cache_refresh(fx.gw, 100u) == 0);

    CHECK(kith_gateway_view_refresh(fx.gw, s, 200u) == 0);
    uint64_t skips0 = 0u;
    CHECK(kith_gateway_compose_skips(fx.gw, &skips0) == 0);
    CHECK(kith_gateway_view_refresh(fx.gw, s, 250u) == 0);
    uint64_t skips1 = 0u;
    CHECK(kith_gateway_compose_skips(fx.gw, &skips1) == 0);
    CHECK(skips1 == skips0 + 1u);

    CHECK(kith_gateway_session_bind_actor(s, 1301u) == 0);
    CHECK(kith_gateway_view_refresh(fx.gw, s, 300u) == 0);
    uint64_t skips2 = 0u;
    CHECK(kith_gateway_compose_skips(fx.gw, &skips2) == 0);
    CHECK(skips2 == skips1);
    kith_gateway_view_snapshot_t meta = {0};
    kith_gateway_view_subject_t out[8] = {0};
    size_t n = 0u;
    CHECK(kith_gateway_view_snapshot(fx.gw, s, &meta, out, 8u, &n) == 0);
    CHECK(n == 3u);
    CHECK(out[0].actor_id == 1301u);
    CHECK(out[0].subject_class == KITH_GATEWAY_VIEW_CLASS_SELF);
    CHECK(meta.subscriber_actor_id == 1301u);

    kith_gateway_session_destroy(s);
    kith_net_conn_close(conn);
    kith_net_conn_release(conn);
    fixture_fini(&fx);
    return failures;
}

// Evicting a windowed cell's node behind the session's back (via the public
// refcount API) and recreating it must not produce a false skip: the
// recreated node mints a fresh content sequence, so the recorded baseline
// cannot match it even though the repopulated rows are identical.
static int test_view_skip_after_evict_recreate(void)
{
    int failures = 0;
    struct fixture fx;
    CHECK(fixture_init_budget(&fx, 8u, 1u) == 0);

    kith_fabric_cell_key_t cell = make_key(10u, 0, 0, 0, 0u);

    kith_net_conn_t *conn = nullptr;
    CHECK(make_conn(fx.net, &conn) == 0);
    kith_gateway_session_t *s = nullptr;
    CHECK(kith_gateway_session_create(
              fx.gw, conn, KITH_GATEWAY_SESSION_SUBSCRIBER, 1u, nullptr, &s) == 0);
    CHECK(kith_gateway_session_bind_actor(s, 1400u) == 0);
    CHECK(kith_gateway_session_window_add(s, &cell) == 0);

    kith_sim_artifact_key_t sk = make_skey(&cell, 1u);
    kith_sim_actor_t sub = make_actor(1400u, 0, 0, 0u);
    CHECK(kith_sim_publish_artifact(fx.sim, &sk, &sub, nullptr) == 0);
    for (uint64_t id = 1u; id <= 2u; ++id)
    {
        kith_sim_actor_t a = make_actor(id, (int64_t)id << KITH_SIM_FIX_SHIFT, 0, 0u);
        CHECK(kith_sim_publish_artifact(fx.sim, &sk, &a, nullptr) == 0);
    }
    CHECK(kith_fabric_publish(fx.fabric, &cell, 1u, nullptr) == 0);
    CHECK(kith_gateway_cache_refresh(fx.gw, 100u) == 0);

    CHECK(kith_gateway_view_refresh(fx.gw, s, 200u) == 0);
    uint64_t skips0 = 0u;
    CHECK(kith_gateway_compose_skips(fx.gw, &skips0) == 0);
    CHECK(kith_gateway_view_refresh(fx.gw, s, 240u) == 0);
    uint64_t skips1 = 0u;
    CHECK(kith_gateway_compose_skips(fx.gw, &skips1) == 0);
    CHECK(skips1 == skips0 + 1u);

    // The window still covers the cell and the session's refcount was one:
    // unsubscribe evicts the node; subscribe creates a fresh one.
    CHECK(kith_gateway_unsubscribe(fx.gw, &cell) == 0);
    CHECK(kith_gateway_subscribe(fx.gw, &cell) == 0);
    CHECK(kith_fabric_publish(fx.fabric, &cell, 1u, nullptr) == 0);
    CHECK(kith_gateway_cache_refresh(fx.gw, 340u) == 0);

    CHECK(kith_gateway_view_refresh(fx.gw, s, 380u) == 0);
    uint64_t skips2 = 0u;
    CHECK(kith_gateway_compose_skips(fx.gw, &skips2) == 0);
    CHECK(skips2 == skips1);
    kith_gateway_view_snapshot_t meta = {0};
    kith_gateway_view_subject_t out[8] = {0};
    size_t n = 0u;
    CHECK(kith_gateway_view_snapshot(fx.gw, s, &meta, out, 8u, &n) == 0);
    CHECK(n == 3u);
    CHECK(out[0].actor_id == 1400u);
    CHECK(out[1].actor_id == 1u);
    CHECK(out[2].actor_id == 2u);

    // With the fresh baseline recorded, an unchanged refresh skips again.
    CHECK(kith_gateway_view_refresh(fx.gw, s, 420u) == 0);
    uint64_t skips3 = 0u;
    CHECK(kith_gateway_compose_skips(fx.gw, &skips3) == 0);
    CHECK(skips3 == skips2 + 1u);

    kith_gateway_session_destroy(s);
    kith_net_conn_close(conn);
    kith_net_conn_release(conn);
    fixture_fini(&fx);
    return failures;
}

// The crowd regime is latched with hysteresis: once the aggregate exists,
// a candidate count that dips below the entry point (but not below the
// exit point) keeps the aggregate instead of flipping it, and only a drop
// past the exit margin dissolves it. With max_subjects 5 the composer
// budget is 4, entry is candidates > 7, and the default exit margin of 2
// keeps the aggregate while candidates > 5.
static int test_view_crowd_hysteresis(void)
{
    int failures = 0;
    struct fixture fx;
    CHECK(fixture_init_budget(&fx, 5u, KITH_GATEWAY_DEFAULT_VIEW_REFRESH_MS) == 0);

    kith_fabric_cell_key_t dense = make_key(4u, 0, 0, 0, 0u);
    kith_fabric_cell_key_t home = make_key(4u, 1, 0, 0, 0u);

    kith_net_conn_t *conn = nullptr;
    CHECK(make_conn(fx.net, &conn) == 0);
    kith_gateway_session_t *s = nullptr;
    CHECK(kith_gateway_session_create(
              fx.gw, conn, KITH_GATEWAY_SESSION_SUBSCRIBER, 1u, nullptr, &s) == 0);
    CHECK(kith_gateway_session_bind_actor(s, 950u) == 0);
    CHECK(kith_gateway_session_window_add(s, &dense) == 0);
    CHECK(kith_gateway_session_window_add(s, &home) == 0);

    kith_sim_actor_t sub = make_actor(950u, 0, 0, 0u);
    kith_sim_artifact_key_t sk_home = make_skey(&home, 1u);
    CHECK(kith_sim_publish_artifact(fx.sim, &sk_home, &sub, nullptr) == 0);

    // 8 actors in the dense cell: one past the entry point.
    kith_sim_artifact_key_t sk_dense = make_skey(&dense, 1u);
    for (uint64_t id = 1u; id <= 8u; ++id)
    {
        kith_sim_actor_t a = make_actor(id, (int64_t)(id + 10u) << KITH_SIM_FIX_SHIFT, 0, 0u);
        CHECK(kith_sim_publish_artifact(fx.sim, &sk_dense, &a, nullptr) == 0);
    }
    CHECK(kith_fabric_publish(fx.fabric, &dense, 1u, nullptr) == 0);
    CHECK(kith_fabric_publish(fx.fabric, &home, 1u, nullptr) == 0);

    // Each step advances the clock past both refresh intervals so every
    // cache refresh and view refresh actually runs. The first composition
    // runs its cache refresh at 100 and view refresh at 1100.
    kith_gateway_view_snapshot_t meta = {0};
    kith_gateway_view_subject_t out[8] = {0};
    size_t n = 0u;
    uint64_t now_ms = 100u;
    CHECK(kith_gateway_cache_refresh(fx.gw, now_ms) == 0);
    now_ms += 1000u;
    CHECK(kith_gateway_view_refresh(fx.gw, s, now_ms) == 0);
    CHECK(kith_gateway_view_snapshot(fx.gw, s, &meta, out, 8u, &n) == 0);
    CHECK(meta.candidate_count == 8u);
    CHECK(meta.class_selected_count[KITH_GATEWAY_VIEW_CLASS_CROWD] == 1u);

    // Drop to exactly the entry point (8 -> 7): unlatched this turns the
    // aggregate off; latched it stays on.
    CHECK(kith_sim_remove_artifact(fx.sim, 8u) == 0);
    CHECK(kith_fabric_publish(fx.fabric, &dense, 1u, nullptr) == 0);
    CHECK(view_step(&fx, s, &now_ms, &meta, out, &n) == 0);
    CHECK(meta.candidate_count == 7u);
    CHECK(meta.class_selected_count[KITH_GATEWAY_VIEW_CLASS_CROWD] == 1u);

    // Drop inside the band (7 -> 6): still latched.
    CHECK(kith_sim_remove_artifact(fx.sim, 7u) == 0);
    CHECK(kith_fabric_publish(fx.fabric, &dense, 1u, nullptr) == 0);
    CHECK(view_step(&fx, s, &now_ms, &meta, out, &n) == 0);
    CHECK(meta.candidate_count == 6u);
    CHECK(meta.class_selected_count[KITH_GATEWAY_VIEW_CLASS_CROWD] == 1u);

    // Drop past the resume point (6 -> 5): the aggregate dissolves and the
    // freed crowd slot is refilled with an individual.
    CHECK(kith_sim_remove_artifact(fx.sim, 6u) == 0);
    CHECK(kith_fabric_publish(fx.fabric, &dense, 1u, nullptr) == 0);
    CHECK(view_step(&fx, s, &now_ms, &meta, out, &n) == 0);
    CHECK(meta.candidate_count == 5u);
    CHECK(meta.class_selected_count[KITH_GATEWAY_VIEW_CLASS_CROWD] == 0u);
    CHECK(meta.class_selected_count[KITH_GATEWAY_VIEW_CLASS_ACTOR] == 4u);
    CHECK(meta.selected_count == 5u); // self + budget individuals

    kith_gateway_session_destroy(s);
    kith_net_conn_close(conn);
    kith_net_conn_release(conn);
    fixture_fini(&fx);
    return failures;
}

// Publish actors [lo, hi] into one cell artifact at staggered x so distance
// ordering follows id.
static int
crowd_publish_range(struct fixture *fx, const kith_sim_artifact_key_t *sk, uint64_t lo, uint64_t hi)
{
    for (uint64_t id = lo; id <= hi; ++id)
    {
        kith_sim_actor_t a = make_actor(id, (int64_t)(id + 10u) << KITH_SIM_FIX_SHIFT, 0, 0u);
        if (kith_sim_publish_artifact(fx->sim, sk, &a, nullptr) != 0)
        {
            return -1;
        }
    }
    return 0;
}

// Remove actors [lo, hi] from the simulation source.
static int crowd_remove_range(struct fixture *fx, uint64_t lo, uint64_t hi)
{
    for (uint64_t id = lo; id <= hi; ++id)
    {
        if (kith_sim_remove_artifact(fx->sim, id) != 0)
        {
            return -1;
        }
    }
    return 0;
}

// Advance both clocks past their refresh intervals and recompose.
static int crowd_recompose(struct fixture *fx, kith_gateway_session_t *s, uint64_t *io_now)
{
    *io_now += 2000u;
    if (kith_gateway_cache_refresh(fx->gw, *io_now) != 0)
    {
        return -1;
    }
    *io_now += 1000u;
    return kith_gateway_view_refresh(fx->gw, s, *io_now);
}

// Shared latch setup for the band-collapse and band-recovery tests: composer
// budget 15, configured exit margin 10 (entry above 18, resume at 8), and 19
// candidates so the aggregate latches on. Returns the bound session, or NULL
// on setup failure.
static kith_gateway_session_t *crowd_latch_fixture(struct fixture *fx,
                                                   kith_net_conn_t **out_conn,
                                                   kith_fabric_cell_key_t *out_dense,
                                                   kith_sim_artifact_key_t *out_sk,
                                                   uint64_t *io_now)
{
    memset(fx, 0, sizeof(*fx));
    if (fixture_init_budget_core(fx, 16u, KITH_GATEWAY_DEFAULT_VIEW_REFRESH_MS, 10u) != 0)
    {
        return nullptr;
    }
    *out_dense = make_key(6u, 0, 0, 0, 0u);
    if (make_conn(fx->net, out_conn) != 0)
    {
        return nullptr;
    }
    kith_gateway_session_t *s = nullptr;
    if (kith_gateway_session_create(
            fx->gw, *out_conn, KITH_GATEWAY_SESSION_SUBSCRIBER, 1u, nullptr, &s) != 0 ||
        kith_gateway_session_bind_actor(s, 850u) != 0 ||
        kith_gateway_session_window_add(s, out_dense) != 0)
    {
        return nullptr;
    }
    kith_sim_actor_t sub = make_actor(850u, 0, 0, 0u);
    *out_sk = make_skey(out_dense, 1u);
    if (kith_sim_publish_artifact(fx->sim, out_sk, &sub, nullptr) != 0 ||
        crowd_publish_range(fx, out_sk, 1u, 19u) != 0 ||
        kith_fabric_publish(fx->fabric, out_dense, 1u, nullptr) != 0)
    {
        return nullptr;
    }
    *io_now = 100u;
    if (kith_gateway_cache_refresh(fx->gw, *io_now) != 0 || crowd_recompose(fx, s, io_now) != 0)
    {
        return nullptr;
    }
    return s;
}

// A latched crowd whose candidate count collapses into the hysteresis band
// leaves every candidate individually selectable while the regime still reads
// overflow. Emitting there divides the aggregate mean by an empty overflow
// (and reads past the entry array just below that boundary), so the regime
// dissolves instead: no crowd subject, every candidate delivered as an
// individual, exactly one exit transition, no flapping deeper in the band.
static int test_view_crowd_band_collapse(void)
{
    int failures = 0;
    struct fixture fx;
    kith_net_conn_t *conn = nullptr;
    kith_fabric_cell_key_t dense;
    kith_sim_artifact_key_t sk_dense;
    uint64_t now_ms = 0u;
    kith_gateway_session_t *s = crowd_latch_fixture(&fx, &conn, &dense, &sk_dense, &now_ms);
    CHECK(s != nullptr);
    if (s == nullptr)
    {
        return failures;
    }

    kith_gateway_view_snapshot_t meta = {0};
    kith_gateway_view_subject_t out[24] = {0};
    size_t n = 0u;
    CHECK(kith_gateway_view_snapshot(fx.gw, s, &meta, out, 24u, &n) == 0);
    CHECK(meta.class_selected_count[KITH_GATEWAY_VIEW_CLASS_CROWD] == 1u);

    // Collapse to exactly composer budget - 1: emitting here divided by an
    // empty overflow before the guard.
    CHECK(crowd_remove_range(&fx, 15u, 19u) == 0);
    CHECK(kith_fabric_publish(fx.fabric, &dense, 1u, nullptr) == 0);
    CHECK(crowd_recompose(&fx, s, &now_ms) == 0);
    CHECK(crowd_transitions(s, (unsigned)GATEWAY_DELIVERY_EVENT_CROWD_EXIT) == 1);
    CHECK(kith_gateway_view_snapshot(fx.gw, s, &meta, out, 24u, &n) == 0);
    CHECK(meta.candidate_count == 14u);
    CHECK(meta.class_selected_count[KITH_GATEWAY_VIEW_CLASS_CROWD] == 0u);
    CHECK(meta.class_selected_count[KITH_GATEWAY_VIEW_CLASS_ACTOR] == 14u);
    CHECK(meta.selected_count == 15u); // self + all candidates as individuals

    // One deeper into the band: selection read past the entry array here.
    CHECK(crowd_remove_range(&fx, 14u, 14u) == 0);
    CHECK(kith_fabric_publish(fx.fabric, &dense, 1u, nullptr) == 0);
    CHECK(crowd_recompose(&fx, s, &now_ms) == 0);
    CHECK(crowd_transitions(s, (unsigned)GATEWAY_DELIVERY_EVENT_CROWD_EXIT) == 0);
    CHECK(crowd_transitions(s, (unsigned)GATEWAY_DELIVERY_EVENT_CROWD_ENTER) == 0);
    CHECK(kith_gateway_view_snapshot(fx.gw, s, &meta, out, 24u, &n) == 0);
    CHECK(meta.candidate_count == 13u);
    CHECK(meta.class_selected_count[KITH_GATEWAY_VIEW_CLASS_CROWD] == 0u);
    CHECK(meta.class_selected_count[KITH_GATEWAY_VIEW_CLASS_ACTOR] == 13u);
    CHECK(meta.selected_count == 14u);

    kith_gateway_session_destroy(s);
    kith_net_conn_close(conn);
    kith_net_conn_release(conn);
    fixture_fini(&fx);
    return failures;
}

// After a band-collapse dissolution the regime re-enters only through the
// normal entry threshold: repopulating to the entry boundary keeps the
// aggregate off, and one candidate past it re-engages it with a single enter
// transition.
static int test_view_crowd_band_recovery(void)
{
    int failures = 0;
    struct fixture fx;
    kith_net_conn_t *conn = nullptr;
    kith_fabric_cell_key_t dense;
    kith_sim_artifact_key_t sk_dense;
    uint64_t now_ms = 0u;
    kith_gateway_session_t *s = crowd_latch_fixture(&fx, &conn, &dense, &sk_dense, &now_ms);
    CHECK(s != nullptr);
    if (s == nullptr)
    {
        return failures;
    }

    // Collapse into the band: 13 candidates remain and dissolve the regime.
    CHECK(crowd_remove_range(&fx, 14u, 19u) == 0);
    CHECK(kith_fabric_publish(fx.fabric, &dense, 1u, nullptr) == 0);
    CHECK(crowd_recompose(&fx, s, &now_ms) == 0);
    CHECK(crowd_transitions(s, (unsigned)GATEWAY_DELIVERY_EVENT_CROWD_EXIT) == 1);

    // Back to the entry boundary (18): still dissolved, no flapping.
    CHECK(crowd_publish_range(&fx, &sk_dense, 14u, 18u) == 0);
    CHECK(kith_fabric_publish(fx.fabric, &dense, 1u, nullptr) == 0);
    CHECK(crowd_recompose(&fx, s, &now_ms) == 0);
    CHECK(crowd_transitions(s, (unsigned)GATEWAY_DELIVERY_EVENT_CROWD_EXIT) == 0);
    CHECK(crowd_transitions(s, (unsigned)GATEWAY_DELIVERY_EVENT_CROWD_ENTER) == 0);
    kith_gateway_view_snapshot_t meta = {0};
    kith_gateway_view_subject_t out[24] = {0};
    size_t n = 0u;
    CHECK(kith_gateway_view_snapshot(fx.gw, s, &meta, out, 24u, &n) == 0);
    CHECK(meta.candidate_count == 18u);
    CHECK(meta.class_selected_count[KITH_GATEWAY_VIEW_CLASS_CROWD] == 0u);

    // One past the boundary re-engages through the entry threshold.
    CHECK(crowd_publish_range(&fx, &sk_dense, 19u, 19u) == 0);
    CHECK(kith_fabric_publish(fx.fabric, &dense, 1u, nullptr) == 0);
    CHECK(crowd_recompose(&fx, s, &now_ms) == 0);
    CHECK(crowd_transitions(s, (unsigned)GATEWAY_DELIVERY_EVENT_CROWD_ENTER) == 1);
    CHECK(kith_gateway_view_snapshot(fx.gw, s, &meta, out, 24u, &n) == 0);
    CHECK(meta.candidate_count == 19u);
    CHECK(meta.class_selected_count[KITH_GATEWAY_VIEW_CLASS_CROWD] == 1u);

    kith_gateway_session_destroy(s);
    kith_net_conn_close(conn);
    kith_net_conn_release(conn);
    fixture_fini(&fx);
    return failures;
}

// Membership deltas regenerate per recomposition and are consumed once:
// entries on the first composition, nothing when nothing changes, crowd
// transitions on latch flips, and a vanish when a prior member disappears
// from every cached window cell.
static int test_view_membership_delta(void)
{
    int failures = 0;
    struct fixture fx;
    CHECK(fixture_init_budget(&fx, 5u, KITH_GATEWAY_DEFAULT_VIEW_REFRESH_MS) == 0);

    kith_fabric_cell_key_t cell = make_key(5u, 0, 0, 0, 0u);
    kith_net_conn_t *conn = nullptr;
    CHECK(make_conn(fx.net, &conn) == 0);
    kith_gateway_session_t *s = nullptr;
    CHECK(kith_gateway_session_create(
              fx.gw, conn, KITH_GATEWAY_SESSION_SUBSCRIBER, 1u, nullptr, &s) == 0);
    CHECK(kith_gateway_session_bind_actor(s, 900u) == 0);
    CHECK(kith_gateway_session_window_add(s, &cell) == 0);

    kith_sim_artifact_key_t sk = make_skey(&cell, 1u);
    kith_sim_actor_t sub = make_actor(900u, 0, 0, 0u);
    CHECK(kith_sim_publish_artifact(fx.sim, &sk, &sub, nullptr) == 0);
    kith_sim_actor_t a = make_actor(10u, 1LL << KITH_SIM_FIX_SHIFT, 0, 0u);
    CHECK(kith_sim_publish_artifact(fx.sim, &sk, &a, nullptr) == 0);
    kith_sim_actor_t b = make_actor(20u, 2LL << KITH_SIM_FIX_SHIFT, 0, 0u);
    CHECK(kith_sim_publish_artifact(fx.sim, &sk, &b, nullptr) == 0);
    kith_sim_actor_t c = make_actor(30u, 3LL << KITH_SIM_FIX_SHIFT, 0, 0u);
    CHECK(kith_sim_publish_artifact(fx.sim, &sk, &c, nullptr) == 0);
    CHECK(kith_fabric_publish(fx.fabric, &cell, 1u, nullptr) == 0);

    uint64_t now_ms = 100u;
    CHECK(kith_gateway_cache_refresh(fx.gw, now_ms) == 0);
    now_ms += 1000u;
    CHECK(kith_gateway_view_refresh(fx.gw, s, now_ms) == 0);

    const struct gateway_view_event *ev = nullptr;
    CHECK(delta_expect_enters(s) == 0);
    // Consumed: the next take returns nothing until a fresh rebuild.
    CHECK(gateway_view_take_membership(s, &ev) == 0u);

    // A rebuild with unchanged content produces an empty delta...
    CHECK(kith_fabric_publish(fx.fabric, &cell, 1u, nullptr) == 0);
    CHECK(view_step(&fx, s, &now_ms, nullptr, nullptr, nullptr) == 0);
    CHECK(gateway_view_take_membership(s, &ev) == 0u);

    // ...and crossing the entry point engages the crowd exactly once.
    for (uint64_t id = 40u; id <= 69u; ++id)
    {
        kith_sim_actor_t far_actor =
            make_actor(id, (int64_t)(id * 10u) << KITH_SIM_FIX_SHIFT, 0, 0u);
        CHECK(kith_sim_publish_artifact(fx.sim, &sk, &far_actor, nullptr) == 0);
    }
    CHECK(kith_fabric_publish(fx.fabric, &cell, 1u, nullptr) == 0);
    CHECK(view_step(&fx, s, &now_ms, nullptr, nullptr, nullptr) == 0);
    CHECK(delta_expect_one(s, 0u, (unsigned)GATEWAY_DELIVERY_EVENT_CROWD_ENTER) == 0);

    // Dissolving the crowd (candidates back under the resume point) emits
    // the exit transition once.
    for (uint64_t id = 40u; id <= 69u; ++id)
    {
        CHECK(kith_sim_remove_artifact(fx.sim, id) == 0);
    }
    CHECK(kith_fabric_publish(fx.fabric, &cell, 1u, nullptr) == 0);
    CHECK(view_step(&fx, s, &now_ms, nullptr, nullptr, nullptr) == 0);
    CHECK(delta_expect_one(s, 0u, (unsigned)GATEWAY_DELIVERY_EVENT_CROWD_EXIT) == 0);

    // A prior member removed at the source departs as VANISH (its artifact
    // is gone from every cached window cell).
    CHECK(kith_sim_remove_artifact(fx.sim, 10u) == 0);
    CHECK(kith_fabric_publish(fx.fabric, &cell, 1u, nullptr) == 0);
    CHECK(view_step(&fx, s, &now_ms, nullptr, nullptr, nullptr) == 0);
    CHECK(delta_expect_one(s, 10u, (unsigned)GATEWAY_DELIVERY_EVENT_VANISH) == 0);

    kith_gateway_session_destroy(s);
    kith_net_conn_close(conn);
    kith_net_conn_release(conn);
    fixture_fini(&fx);
    return failures;
}

// The locate-failure counter starts at zero, rejects NULL arguments with
// EINVAL, and increments exactly once per refresh whose scan phase cannot
// find the session's bound actor id inside its own window cells. A bound
// actor that IS cached keeps the counter untouched.
static int test_view_locate_failures_counter(void)
{
    int failures = 0;
    struct fixture fx;
    CHECK(fixture_init(&fx) == 0);

    uint64_t failures_count = 99u;
    CHECK(kith_gateway_view_locate_failures(nullptr, &failures_count) ==
          kith_error_return(KITH_EINVAL));
    CHECK(kith_gateway_view_locate_failures(fx.gw, nullptr) == kith_error_return(KITH_EINVAL));
    CHECK(kith_gateway_view_locate_failures(fx.gw, &failures_count) == 0);
    CHECK(failures_count == 0u);

    // Bound actor 500 owns a window whose only cell caches a different
    // population: the subscriber is located in no window cell.
    kith_fabric_cell_key_t cell = make_key(2u, 0, 0, 0, 0u);
    kith_net_conn_t *conn = nullptr;
    CHECK(make_conn(fx.net, &conn) == 0);
    kith_gateway_session_t *s = nullptr;
    CHECK(kith_gateway_session_create(
              fx.gw, conn, KITH_GATEWAY_SESSION_SUBSCRIBER, 1u, nullptr, &s) == 0);
    CHECK(kith_gateway_session_bind_actor(s, 500u) == 0);
    CHECK(kith_gateway_session_window_add(s, &cell) == 0);

    kith_sim_artifact_key_t sk = make_skey(&cell, 1u);
    kith_sim_actor_t other = make_actor(11u, 0, 0, 3u);
    CHECK(kith_sim_publish_artifact(fx.sim, &sk, &other, nullptr) == 0);
    CHECK(kith_fabric_publish(fx.fabric, &cell, 1u, nullptr) == 0);
    CHECK(kith_gateway_cache_refresh(fx.gw, 100u) == 0);

    CHECK(kith_gateway_view_refresh(fx.gw, s, 200u) == kith_error_return(KITH_ESTATE));
    CHECK(kith_gateway_view_locate_failures(fx.gw, &failures_count) == 0);
    CHECK(failures_count == 1u);

    // Publish the subscriber into its window cell: the next refresh locates
    // it and the counter stays at one (monotonic; failures never reset).
    kith_sim_actor_t sub = make_actor(500u, 0, 0, 4u);
    CHECK(kith_sim_publish_artifact(fx.sim, &sk, &sub, nullptr) == 0);
    CHECK(kith_fabric_publish(fx.fabric, &cell, 2u, nullptr) == 0);
    CHECK(kith_gateway_cache_refresh(fx.gw, 300u) == 0);
    CHECK(kith_gateway_view_refresh(fx.gw, s, 400u) == 0);
    CHECK(kith_gateway_view_locate_failures(fx.gw, &failures_count) == 0);
    CHECK(failures_count == 1u);

    kith_gateway_session_destroy(s);
    kith_net_conn_close(conn);
    kith_net_conn_release(conn);
    fixture_fini(&fx);
    return failures;
}

// A freshly subscribed cell resolves its locator on the very first compose.
// The node created by subscribe snapshots the cell's current artifacts
// synchronously, so view_refresh succeeds with NO intervening
// cache_refresh — including when the subscriber's artifact landed on the
// sim and was published while the cell was still unsubscribed — and every
// subsequent refreshed recompose keeps locating it.
static int test_view_locate_after_subscribe(void)
{
    int failures = 0;
    struct fixture fx;
    CHECK(fixture_init(&fx) == 0);

    kith_fabric_cell_key_t cell = make_key(6u, 0, 0, 0, 0u);

    kith_net_conn_t *conn = nullptr;
    CHECK(make_conn(fx.net, &conn) == 0);
    kith_gateway_session_t *s = nullptr;
    CHECK(kith_gateway_session_create(
              fx.gw, conn, KITH_GATEWAY_SESSION_SUBSCRIBER, 1u, nullptr, &s) == 0);
    CHECK(kith_gateway_session_bind_actor(s, 300u) == 0);

    // State lands while the cell is unsubscribed: the crossing shape.
    kith_sim_artifact_key_t sk = make_skey(&cell, 1u);
    kith_sim_actor_t sub = make_actor(300u, 0, 0, 3u);
    CHECK(kith_sim_publish_artifact(fx.sim, &sk, &sub, nullptr) == 0);
    CHECK(kith_fabric_publish(fx.fabric, &cell, 1u, nullptr) == 0);

    CHECK(kith_gateway_session_window_add(s, &cell) == 0);
    // No cache_refresh between subscribe and compose.
    CHECK(kith_gateway_view_refresh(fx.gw, s, 100u) == 0);

    kith_gateway_view_snapshot_t meta = {0};
    kith_gateway_view_subject_t out[4] = {0};
    size_t n = 99u;
    CHECK(kith_gateway_view_snapshot(fx.gw, s, &meta, out, 4u, &n) == 0);
    CHECK(n == 1u);
    CHECK(out[0].actor_id == 300u);
    CHECK(out[0].subject_class == KITH_GATEWAY_VIEW_CLASS_SELF);
    CHECK(meta.subscriber_actor_id == 300u);

    uint64_t failures_count = 99u;
    CHECK(kith_gateway_view_locate_failures(fx.gw, &failures_count) == 0);
    CHECK(failures_count == 0u);

    // Publish echoes re-dirty the cell; each restocked recompose locates
    // the subscriber and the counter stays untouched.
    for (uint32_t pass = 0u; pass < 3u; ++pass)
    {
        CHECK(kith_fabric_publish(fx.fabric, &cell, 1u, nullptr) == 0);
        CHECK(kith_gateway_cache_refresh(fx.gw, 250u + 200u * pass) == 0);
        CHECK(kith_gateway_view_refresh(fx.gw, s, 260u + 200u * pass) == 0);
    }
    CHECK(kith_gateway_view_locate_failures(fx.gw, &failures_count) == 0);
    CHECK(failures_count == 0u);

    kith_gateway_session_destroy(s);
    kith_net_conn_close(conn);
    kith_net_conn_release(conn);
    fixture_fini(&fx);
    return failures;
}

// Subscribing an empty cell is a valid state: the entry exists with a zero
// product count and the composer reports ESTATE until content arrives. A
// publish marks the tracked cell pending, one refresh fills the
// entry, and the same compose then resolves its locator.
static int test_view_empty_cell_heals_via_pending(void)
{
    int failures = 0;
    struct fixture fx;
    CHECK(fixture_init(&fx) == 0);

    kith_fabric_cell_key_t cell = make_key(7u, 0, 0, 0, 0u);

    kith_net_conn_t *conn = nullptr;
    CHECK(make_conn(fx.net, &conn) == 0);
    kith_gateway_session_t *s = nullptr;
    CHECK(kith_gateway_session_create(
              fx.gw, conn, KITH_GATEWAY_SESSION_SUBSCRIBER, 1u, nullptr, &s) == 0);
    CHECK(kith_gateway_session_bind_actor(s, 400u) == 0);
    CHECK(kith_gateway_session_window_add(s, &cell) == 0);

    kith_gateway_cell_snapshot_t snap = {0};
    CHECK(kith_gateway_cache_snapshot_cell(fx.gw, &cell, &snap) == 0);
    CHECK(snap.actor_count == 0u);

    uint64_t failures_count = 99u;
    CHECK(kith_gateway_view_locate_failures(fx.gw, &failures_count) == 0);
    CHECK(failures_count == 0u);
    CHECK(kith_gateway_view_refresh(fx.gw, s, 100u) == kith_error_return(KITH_ESTATE));
    CHECK(kith_gateway_view_locate_failures(fx.gw, &failures_count) == 0);
    CHECK(failures_count == 1u);

    // Heal through the normal refresh channel: publish raises pending on
    // the tracked cell, refresh fills the entry, locate succeeds.
    kith_sim_artifact_key_t sk = make_skey(&cell, 1u);
    kith_sim_actor_t sub = make_actor(400u, 0, 0, 5u);
    CHECK(kith_sim_publish_artifact(fx.sim, &sk, &sub, nullptr) == 0);
    CHECK(kith_fabric_publish(fx.fabric, &cell, 1u, nullptr) == 0);
    CHECK(kith_gateway_cache_refresh(fx.gw, 150u) == 0);
    CHECK(kith_gateway_view_refresh(fx.gw, s, 160u) == 0);
    CHECK(kith_gateway_view_locate_failures(fx.gw, &failures_count) == 0);
    CHECK(failures_count == 1u);

    kith_gateway_session_destroy(s);
    kith_net_conn_close(conn);
    kith_net_conn_release(conn);
    fixture_fini(&fx);
    return failures;
}

// An evicted entry (last refcount dropped) recreated by a subsequent
// subscribe re-snapshots synchronously: the next compose resolves its
// locator with no intervening refresh, and the counter never moves.
static int test_view_recreate_populates_on_subscribe(void)
{
    int failures = 0;
    struct fixture fx;
    CHECK(fixture_init(&fx) == 0);

    kith_fabric_cell_key_t cell = make_key(8u, 0, 0, 0, 0u);

    kith_net_conn_t *conn = nullptr;
    CHECK(make_conn(fx.net, &conn) == 0);
    kith_gateway_session_t *s = nullptr;
    CHECK(kith_gateway_session_create(
              fx.gw, conn, KITH_GATEWAY_SESSION_SUBSCRIBER, 1u, nullptr, &s) == 0);
    CHECK(kith_gateway_session_bind_actor(s, 600u) == 0);

    kith_sim_artifact_key_t sk = make_skey(&cell, 1u);
    kith_sim_actor_t sub = make_actor(600u, 0, 0, 3u);
    CHECK(kith_sim_publish_artifact(fx.sim, &sk, &sub, nullptr) == 0);
    CHECK(kith_fabric_publish(fx.fabric, &cell, 1u, nullptr) == 0);

    CHECK(kith_gateway_session_window_add(s, &cell) == 0);
    CHECK(kith_gateway_view_refresh(fx.gw, s, 100u) == 0);

    // Full unsubscribe evicts the entry; subscribing again recreates it.
    CHECK(kith_gateway_session_window_remove(s, &cell) == 0);
    CHECK(kith_gateway_session_window_add(s, &cell) == 0);
    CHECK(kith_gateway_view_refresh(fx.gw, s, 300u) == 0);

    kith_gateway_view_snapshot_t meta = {0};
    kith_gateway_view_subject_t out[4] = {0};
    size_t n = 99u;
    CHECK(kith_gateway_view_snapshot(fx.gw, s, &meta, out, 4u, &n) == 0);
    CHECK(n == 1u);
    CHECK(out[0].actor_id == 600u);

    uint64_t failures_count = 99u;
    CHECK(kith_gateway_view_locate_failures(fx.gw, &failures_count) == 0);
    CHECK(failures_count == 0u);

    kith_gateway_session_destroy(s);
    kith_net_conn_close(conn);
    kith_net_conn_release(conn);
    fixture_fini(&fx);
    return failures;
}

// A tracked cell that was subscribed while genuinely empty gains sim
// membership through the subscriber's own boundary crossing without any
// cache event of its own: no product header covers it yet, so the drain
// never fills the node. The composer's scan-phase repair reconciles such
// nodes against sim truth on a failed locate, so the crossing input is
// observable on the very pass it applies, with the failure counter
// untouched.
static int test_view_locate_repairs_uncovered_cell(void)
{
    int failures = 0;
    struct fixture fx;
    CHECK(fixture_init(&fx) == 0);

    kith_fabric_cell_key_t cell = make_key(9u, 0, 0, 0, 0u);
    kith_net_conn_t *conn = nullptr;
    CHECK(make_conn(fx.net, &conn) == 0);
    kith_gateway_session_t *s = nullptr;
    CHECK(kith_gateway_session_create(
              fx.gw, conn, KITH_GATEWAY_SESSION_SUBSCRIBER, 1u, nullptr, &s) == 0);
    CHECK(kith_gateway_session_bind_actor(s, 700u) == 0);
    CHECK(kith_gateway_session_window_add(s, &cell) == 0);

    // Sim truth lands after the node exists and no fabric publish or
    // refresh runs: exactly the crossing-shaped blind state.
    kith_sim_artifact_key_t sk = make_skey(&cell, 1u);
    kith_sim_actor_t sub = make_actor(700u, 0, 0, 3u);
    CHECK(kith_sim_publish_artifact(fx.sim, &sk, &sub, nullptr) == 0);

    CHECK(kith_gateway_view_refresh(fx.gw, s, 100u) == 0);

    kith_gateway_view_snapshot_t meta = {0};
    kith_gateway_view_subject_t out[4] = {0};
    size_t n = 99u;
    CHECK(kith_gateway_view_snapshot(fx.gw, s, &meta, out, 4u, &n) == 0);
    CHECK(n == 1u);
    CHECK(out[0].actor_id == 700u);
    CHECK(out[0].subject_class == KITH_GATEWAY_VIEW_CLASS_SELF);

    uint64_t failures_count = 99u;
    CHECK(kith_gateway_view_locate_failures(fx.gw, &failures_count) == 0);
    CHECK(failures_count == 0u);

    kith_gateway_session_destroy(s);
    kith_net_conn_close(conn);
    kith_net_conn_release(conn);
    fixture_fini(&fx);
    return failures;
}

// The scan-phase reconcile reads the entire cell, not just the locating
// actor: two members published before interest materialized are captured
// together, and product metadata stays owned by the drain path even after
// a successful populate.
static int test_view_reconcile_captures_whole_cell(void)
{
    int failures = 0;
    struct fixture fx;
    CHECK(fixture_init(&fx) == 0);

    kith_fabric_cell_key_t cell = make_key(10u, 0, 0, 0, 0u);
    kith_net_conn_t *conn_a = nullptr;
    CHECK(make_conn(fx.net, &conn_a) == 0);
    kith_gateway_session_t *sa = nullptr;
    CHECK(kith_gateway_session_create(
              fx.gw, conn_a, KITH_GATEWAY_SESSION_SUBSCRIBER, 1u, nullptr, &sa) == 0);
    CHECK(kith_gateway_session_bind_actor(sa, 800u) == 0);
    CHECK(kith_gateway_session_window_add(sa, &cell) == 0);

    // Two members land on the sim behind the already-tracked cold node;
    // neither a fabric publish nor a refresh covers them.
    kith_sim_artifact_key_t sk = make_skey(&cell, 1u);
    kith_sim_actor_t other = make_actor(801u, 0, 1LL << KITH_SIM_FIX_SHIFT, 4u);
    kith_sim_actor_t self_a = make_actor(800u, 0, 0, 5u);
    CHECK(kith_sim_publish_artifact(fx.sim, &sk, &other, nullptr) == 0);
    CHECK(kith_sim_publish_artifact(fx.sim, &sk, &self_a, nullptr) == 0);

    // New interest in the same cell reconciles the cold node at
    // subscribe time and captures both members.
    kith_net_conn_t *conn_b = nullptr;
    CHECK(make_conn(fx.net, &conn_b) == 0);
    kith_gateway_session_t *sb = nullptr;
    CHECK(kith_gateway_session_create(
              fx.gw, conn_b, KITH_GATEWAY_SESSION_SUBSCRIBER, 2u, nullptr, &sb) == 0);
    CHECK(kith_gateway_session_bind_actor(sb, 801u) == 0);
    CHECK(kith_gateway_session_window_add(sb, &cell) == 0);

    CHECK(kith_gateway_view_refresh(fx.gw, sb, 200u) == 0);
    kith_gateway_view_snapshot_t meta = {0};
    kith_gateway_view_subject_t out[4] = {0};
    size_t n = 99u;
    CHECK(kith_gateway_view_snapshot(fx.gw, sb, &meta, out, 4u, &n) == 0);
    CHECK(n == 2u);
    CHECK(out[0].actor_id == 801u);
    CHECK(out[1].actor_id == 800u);

    uint64_t failures_count = 99u;
    CHECK(kith_gateway_view_locate_failures(fx.gw, &failures_count) == 0);
    CHECK(failures_count == 0u);

    kith_gateway_session_destroy(sa);
    kith_gateway_session_destroy(sb);
    kith_net_conn_close(conn_a);
    kith_net_conn_release(conn_a);
    kith_net_conn_close(conn_b);
    kith_net_conn_release(conn_b);
    fixture_fini(&fx);
    return failures;
}

// A scan-phase repair that rescues a failed locate must also run the
// candidate scan before selection: selection reads the repaired pass's
// entry array directly rather than trusting a candidate count, so the
// entries it walks and the totals it reports always describe the same
// reconciled window.
static int test_view_repair_rescans_before_select(void)
{
    int failures = 0;
    struct fixture fx;
    CHECK(fixture_init_budget_core(&fx, 16u, KITH_GATEWAY_DEFAULT_VIEW_REFRESH_MS, 10u) == 0);

    // Subscriber cell and dense neighbor cell: both tracked while
    // genuinely empty, then sim truth lands behind them with no fabric
    // publish and no refresh — the crossing-shaped blind state for both.
    kith_fabric_cell_key_t home = make_key(2u, 0, 0, 0, 0u);
    kith_fabric_cell_key_t dense = make_key(3u, 0, 0, 0, 0u);
    kith_net_conn_t *conn = nullptr;
    CHECK(make_conn(fx.net, &conn) == 0);
    kith_gateway_session_t *s = nullptr;
    CHECK(kith_gateway_session_create(
              fx.gw, conn, KITH_GATEWAY_SESSION_SUBSCRIBER, 1u, nullptr, &s) == 0);
    CHECK(kith_gateway_session_bind_actor(s, 850u) == 0);
    CHECK(kith_gateway_session_window_add(s, &home) == 0);
    CHECK(kith_gateway_session_window_add(s, &dense) == 0);

    kith_sim_actor_t sub = make_actor(850u, 0, 0, 0u);
    kith_sim_artifact_key_t sk_home = make_skey(&home, 1u);
    kith_sim_artifact_key_t sk_dense = make_skey(&dense, 1u);
    CHECK(kith_sim_publish_artifact(fx.sim, &sk_home, &sub, nullptr) == 0);
    // Nineteen neighbors: past the budget - 1 emit threshold, so the
    // repaired pass exercises selection with a full candidate count.
    CHECK(crowd_publish_range(&fx, &sk_dense, 1u, 19u) == 0);

    // One refresh: locate fails, the repair reconciles both cold nodes,
    // the retry locates the subscriber, and the re-run scan feeds
    // selection from the same reconciled window state.
    CHECK(kith_gateway_view_refresh(fx.gw, s, 100u) == 0);

    kith_gateway_view_snapshot_t meta = {0};
    kith_gateway_view_subject_t out[32] = {0};
    size_t n = 0u;
    CHECK(kith_gateway_view_snapshot(fx.gw, s, &meta, out, 32u, &n) == 0);
    CHECK(n == 16u); // self + 14 individuals + 1 crowd aggregate
    CHECK(meta.candidate_count == 19u);
    CHECK(meta.selected_count == 16u);
    CHECK(meta.class_selected_count[KITH_GATEWAY_VIEW_CLASS_SELF] == 1u);
    CHECK(meta.class_selected_count[KITH_GATEWAY_VIEW_CLASS_ACTOR] == 14u);
    CHECK(meta.class_selected_count[KITH_GATEWAY_VIEW_CLASS_CROWD] == 1u);
    CHECK(out[0].actor_id == 850u);
    for (size_t i = 1u; i < 15u; ++i)
    {
        CHECK(out[i].subject_class == KITH_GATEWAY_VIEW_CLASS_ACTOR);
        CHECK(out[i].actor_id >= 1u && out[i].actor_id <= 19u);
    }

    CHECK(crowd_transitions(s, (unsigned)GATEWAY_DELIVERY_EVENT_CROWD_ENTER) == 1);

    uint64_t failures_count = 99u;
    CHECK(kith_gateway_view_locate_failures(fx.gw, &failures_count) == 0);
    CHECK(failures_count == 0u); // the repair rescued the locate

    kith_gateway_session_destroy(s);
    kith_net_conn_close(conn);
    kith_net_conn_release(conn);
    fixture_fini(&fx);
    return failures;
}

// A stale-but-populated window node also reconciles in the scan-phase
// repair. The subscriber crossed into a cell whose cache node still holds
// the previous population — the crossing publish lands after this tick's
// refresh — so the subscriber is cached in no window cell and locate
// fails. The repair refreshes populated nodes against sim truth too, the
// retry locates the subscriber, and the failure counter stays at zero.
static int test_view_repair_reconciles_stale_populated_node(void)
{
    int failures = 0;
    struct fixture fx;
    CHECK(fixture_init(&fx) == 0);

    kith_fabric_cell_key_t cell = make_key(9u, 0, 0, 0, 0u);
    kith_net_conn_t *conn = nullptr;
    CHECK(make_conn(fx.net, &conn) == 0);
    kith_gateway_session_t *s = nullptr;
    CHECK(kith_gateway_session_create(
              fx.gw, conn, KITH_GATEWAY_SESSION_SUBSCRIBER, 1u, nullptr, &s) == 0);
    CHECK(kith_gateway_session_bind_actor(s, 900u) == 0);
    CHECK(kith_gateway_session_window_add(s, &cell) == 0);

    // The window node populates through the normal pipeline with a
    // previous resident; the subscriber is absent from the cache.
    kith_sim_artifact_key_t sk = make_skey(&cell, 1u);
    kith_sim_actor_t other = make_actor(11u, 0, 0, 2u);
    CHECK(kith_sim_publish_artifact(fx.sim, &sk, &other, nullptr) == 0);
    CHECK(kith_fabric_publish(fx.fabric, &cell, 1u, nullptr) == 0);
    CHECK(kith_gateway_cache_refresh(fx.gw, 100u) == 0);

    // The subscriber's state lands in the same cell while the node stays
    // stale: no fabric publish and no refresh — the intra-tick race shape.
    kith_sim_actor_t sub = make_actor(900u, 0, 0, 4u);
    CHECK(kith_sim_publish_artifact(fx.sim, &sk, &sub, nullptr) == 0);

    // Locate fails on the stale population; the repair refreshes the
    // populated node against sim truth and the retry locates.
    CHECK(kith_gateway_view_refresh(fx.gw, s, 200u) == 0);

    kith_gateway_view_snapshot_t meta = {0};
    kith_gateway_view_subject_t out[4] = {0};
    size_t n = 0u;
    CHECK(kith_gateway_view_snapshot(fx.gw, s, &meta, out, 4u, &n) == 0);
    CHECK(n == 2u);
    CHECK(out[0].actor_id == 900u);
    CHECK(out[0].subject_class == KITH_GATEWAY_VIEW_CLASS_SELF);
    CHECK(out[1].actor_id == 11u);
    CHECK(out[1].subject_class == KITH_GATEWAY_VIEW_CLASS_ACTOR);
    CHECK(meta.candidate_count == 1u);

    uint64_t failures_count = 99u;
    CHECK(kith_gateway_view_locate_failures(fx.gw, &failures_count) == 0);
    CHECK(failures_count == 0u); // the repair rescued the locate

    kith_gateway_session_destroy(s);
    kith_net_conn_close(conn);
    kith_net_conn_release(conn);
    fixture_fini(&fx);
    return failures;
}

int main(void)
{
    int rc = 0;
    rc |= test_view_arg_validation();
    rc |= test_view_self_and_nearby();
    rc |= test_view_self_echo_stamping();
    rc |= test_view_self_echo_stamping_disabled();
    rc |= test_view_budget_bound();
    rc |= test_view_tier_selection();
    rc |= test_view_crowd_aggregate();
    rc |= test_view_crowd_hysteresis();
    rc |= test_view_crowd_band_collapse();
    rc |= test_view_crowd_band_recovery();
    rc |= test_view_membership_delta();
    rc |= test_view_dense_cell_within_budget();
    rc |= test_view_sticky_continuity();
    rc |= test_view_refresh_interval();
    rc |= test_view_skip_when_unchanged();
    rc |= test_view_skip_invalidated_by_publish();
    rc |= test_view_skip_invalidated_by_window();
    rc |= test_view_skip_invalidated_by_rebind();
    rc |= test_view_skip_after_evict_recreate();
    rc |= test_view_locate_failures_counter();
    rc |= test_view_locate_after_subscribe();
    rc |= test_view_empty_cell_heals_via_pending();
    rc |= test_view_recreate_populates_on_subscribe();
    rc |= test_view_locate_repairs_uncovered_cell();
    rc |= test_view_repair_rescans_before_select();
    rc |= test_view_repair_reconciles_stale_populated_node();
    rc |= test_view_reconcile_captures_whole_cell();
    if (rc != 0)
    {
        (void)fprintf(stderr, "gateway view tests FAILED\n");
    }
    return rc;
}
