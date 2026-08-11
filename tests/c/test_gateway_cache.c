#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include "kith/fabric/fabric.h"
#include "kith/gateway/gateway.h"
#include "kith/net/net.h"
#include "kith/proto/proto.h"
#include "kith/sim/sim.h"
#include "kith/types.h"
#include "kith/version.h"

static int check_cond(bool ok, int line)
{
    if (!ok)
    {
        (void)fprintf(stderr, "gateway cache: assertion at line %d failed\n", line);
        return 1;
    }
    return 0;
}

#define CHECK(cond) failures += check_cond((cond), __LINE__)

// Twice the 256-cell ceiling of a 16-bucket cache, so a fill run crosses
// the capacity boundary with room to spare.
enum
{
    CAPACITY_PROBE_CELLS = 512
};

struct fixture
{
    kith_proto_t *proto;
    kith_net_t *net;
    kith_sim_t *sim;
    kith_fabric_t *fabric;
    kith_gateway_t *gw;
};

static int fixture_init_buckets(struct fixture *fx, uint32_t cache_bucket_count)
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
    params.cache_bucket_count = cache_bucket_count;
    if (kith_gateway_create(&params, fx->net, fx->fabric, fx->proto, nullptr, &fx->gw) != 0)
    {
        return -1;
    }
    return 0;
}

static int fixture_init(struct fixture *fx)
{
    return fixture_init_buckets(fx, 0u);
}

static void fixture_fini(const struct fixture *fx)
{
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

static kith_sim_actor_t make_actor(uint64_t id, int64_t x, int64_t y, uint32_t tick)
{
    kith_sim_actor_t a = {0};
    a.id = id;
    a.pos_x = x;
    a.pos_y = y;
    a.input_tick = tick;
    return a;
}

// Every cache entry point rejects a NULL handle (and NULL key/out where the
// function takes one) with EINVAL. snapshot_cell on an unsubscribed cell
// returns ENOENT.
static int test_cache_arg_validation(void)
{
    int failures = 0;
    struct fixture fx;
    CHECK(fixture_init(&fx) == 0);

    kith_fabric_cell_key_t k = make_key(1u, 0, 0, 0, 0u);

    CHECK(kith_gateway_subscribe(nullptr, &k) == kith_error_return(KITH_EINVAL));
    CHECK(kith_gateway_subscribe(fx.gw, nullptr) == kith_error_return(KITH_EINVAL));
    CHECK(kith_gateway_unsubscribe(nullptr, &k) == kith_error_return(KITH_EINVAL));
    CHECK(kith_gateway_unsubscribe(fx.gw, nullptr) == kith_error_return(KITH_EINVAL));
    CHECK(kith_gateway_cache_refresh(nullptr, 0u) == kith_error_return(KITH_EINVAL));

    kith_gateway_cell_snapshot_t snap = {0};
    CHECK(kith_gateway_cache_snapshot_cell(nullptr, &k, &snap) == kith_error_return(KITH_EINVAL));
    CHECK(kith_gateway_cache_snapshot_cell(fx.gw, nullptr, &snap) ==
          kith_error_return(KITH_EINVAL));
    CHECK(kith_gateway_cache_snapshot_cell(fx.gw, &k, nullptr) == kith_error_return(KITH_EINVAL));
    CHECK(kith_gateway_cache_snapshot_cell(fx.gw, &k, &snap) == kith_error_return(KITH_ENOENT));

    kith_gateway_cache_stats_t stats = {0};
    CHECK(kith_gateway_cache_stats(nullptr, &stats) == kith_error_return(KITH_EINVAL));
    CHECK(kith_gateway_cache_stats(fx.gw, nullptr) == kith_error_return(KITH_EINVAL));

    fixture_fini(&fx);
    return failures;
}

// Subscribing a cell twice increments the refcount without creating a second
// cache entry or a second fabric subscription. Unsubscribing once decrements
// the refcount but retains the entry; the final unsubscribe evicts the entry
// and removes the fabric subscription. Unsubscribing an unsubscribed cell is
// idempotent.
static int test_subscribe_refcount(void)
{
    int failures = 0;
    struct fixture fx;
    CHECK(fixture_init(&fx) == 0);

    kith_fabric_cell_key_t k = make_key(1u, 0, 0, 0, 0u);

    CHECK(kith_gateway_subscribe(fx.gw, &k) == 0);
    kith_gateway_cache_stats_t stats = {0};
    CHECK(kith_gateway_cache_stats(fx.gw, &stats) == 0);
    CHECK(stats.cell_count == 1u);
    CHECK(stats.subscribed_cell_count == 1u);

    kith_gateway_cell_snapshot_t snap = {0};
    CHECK(kith_gateway_cache_snapshot_cell(fx.gw, &k, &snap) == 0);
    CHECK(snap.refcount == 1u);

    // Second subscribe: refcount becomes 2, no new entry or subscription.
    CHECK(kith_gateway_subscribe(fx.gw, &k) == 0);
    CHECK(kith_gateway_cache_stats(fx.gw, &stats) == 0);
    CHECK(stats.cell_count == 1u);
    CHECK(stats.subscribed_cell_count == 1u);
    CHECK(kith_gateway_cache_snapshot_cell(fx.gw, &k, &snap) == 0);
    CHECK(snap.refcount == 2u);

    // First unsubscribe: refcount drops to 1, entry retained.
    CHECK(kith_gateway_unsubscribe(fx.gw, &k) == 0);
    CHECK(kith_gateway_cache_stats(fx.gw, &stats) == 0);
    CHECK(stats.cell_count == 1u);
    CHECK(stats.subscribed_cell_count == 1u);
    CHECK(kith_gateway_cache_snapshot_cell(fx.gw, &k, &snap) == 0);
    CHECK(snap.refcount == 1u);

    // Final unsubscribe: entry evicted, fabric subscription removed.
    CHECK(kith_gateway_unsubscribe(fx.gw, &k) == 0);
    CHECK(kith_gateway_cache_stats(fx.gw, &stats) == 0);
    CHECK(stats.cell_count == 0u);
    CHECK(stats.subscribed_cell_count == 0u);
    CHECK(kith_gateway_cache_snapshot_cell(fx.gw, &k, &snap) == kith_error_return(KITH_ENOENT));

    // Idempotent unsubscribe on a never-subscribed cell.
    CHECK(kith_gateway_unsubscribe(fx.gw, &k) == 0);

    fixture_fini(&fx);
    return failures;
}

// cache_refresh drains pending fabric products and re-snapshots the changed
// cells. A subscribed cell with no pending publish stays at its prior
// snapshot. After a publish, refresh updates the authority epoch, publish
// sequence, actor count, and refresh timestamp.
static int test_cache_refresh_updates(void)
{
    int failures = 0;
    struct fixture fx;
    CHECK(fixture_init(&fx) == 0);

    kith_fabric_cell_key_t k = make_key(2u, 0, 0, 0, 0u);
    CHECK(kith_gateway_subscribe(fx.gw, &k) == 0);

    // Publish an actor into the cell on the sim, then publish the cell on the
    // fabric so the gateway's subscription is marked pending.
    kith_sim_artifact_key_t sk = make_skey(&k, 1u);
    kith_sim_actor_t a = make_actor(7u, 1LL << KITH_SIM_FIX_SHIFT, 2LL << KITH_SIM_FIX_SHIFT, 11u);
    CHECK(kith_sim_publish_artifact(fx.sim, &sk, &a, nullptr) == 0);
    uint64_t seq = 0u;
    CHECK(kith_fabric_publish(fx.fabric, &k, 1u, &seq) == 0);
    CHECK(seq == 1u);

    CHECK(kith_gateway_cache_refresh(fx.gw, 100u) == 0);

    kith_gateway_cell_snapshot_t snap = {0};
    CHECK(kith_gateway_cache_snapshot_cell(fx.gw, &k, &snap) == 0);
    CHECK(snap.authority_epoch == 1u);
    CHECK(snap.latest_publish_seq == 1u);
    CHECK(snap.actor_count == 1u);
    CHECK(snap.refreshed_at_ms == 100u);
    CHECK(snap.refcount == 1u);

    kith_gateway_cache_stats_t stats = {0};
    CHECK(kith_gateway_cache_stats(fx.gw, &stats) == 0);
    CHECK(stats.last_refresh_ms == 100u);

    fixture_fini(&fx);
    return failures;
}

// Refresh is gated by the configured interval: a second call within the
// interval is a no-op (a freshly published product stays pending). A call
// past the interval drains the pending product.
static int test_cache_refresh_interval(void)
{
    int failures = 0;
    struct fixture fx;
    CHECK(fixture_init(&fx) == 0);

    kith_fabric_cell_key_t k = make_key(3u, 0, 0, 0, 0u);
    CHECK(kith_gateway_subscribe(fx.gw, &k) == 0);

    kith_sim_artifact_key_t sk = make_skey(&k, 1u);
    kith_sim_actor_t a = make_actor(1u, 0, 0, 0u);
    CHECK(kith_sim_publish_artifact(fx.sim, &sk, &a, nullptr) == 0);
    CHECK(kith_fabric_publish(fx.fabric, &k, 1u, nullptr) == 0);

    CHECK(kith_gateway_cache_refresh(fx.gw, 100u) == 0);
    kith_gateway_cell_snapshot_t snap = {0};
    CHECK(kith_gateway_cache_snapshot_cell(fx.gw, &k, &snap) == 0);
    CHECK(snap.actor_count == 1u);
    CHECK(snap.refreshed_at_ms == 100u);

    // Publish a second actor; refresh within the 100 ms interval is a no-op.
    kith_sim_actor_t a2 = make_actor(2u, 0, 0, 0u);
    CHECK(kith_sim_publish_artifact(fx.sim, &sk, &a2, nullptr) == 0);
    CHECK(kith_fabric_publish(fx.fabric, &k, 1u, nullptr) == 0);
    CHECK(kith_gateway_cache_refresh(fx.gw, 150u) == 0);
    CHECK(kith_gateway_cache_snapshot_cell(fx.gw, &k, &snap) == 0);
    CHECK(snap.actor_count == 1u);
    CHECK(snap.refreshed_at_ms == 100u);

    // Past the interval, the pending product is drained.
    CHECK(kith_gateway_cache_refresh(fx.gw, 201u) == 0);
    CHECK(kith_gateway_cache_snapshot_cell(fx.gw, &k, &snap) == 0);
    CHECK(snap.actor_count == 2u);
    CHECK(snap.refreshed_at_ms == 201u);

    fixture_fini(&fx);
    return failures;
}

// A cell that transitions to empty (its last actor removed and the cell
// re-published) clears its cached snapshot on refresh: the legitimately
// empty cell must report actor_count 0 and a zeroed crowd aggregate, not a
// stale snapshot. This locks the count==0 clearing path that the
// stage-then-swap refresh retains for the populated case.
static int test_cache_refresh_clears_emptied_cell(void)
{
    int failures = 0;
    struct fixture fx;
    CHECK(fixture_init(&fx) == 0);

    kith_fabric_cell_key_t k = make_key(4u, 0, 0, 0, 0u);
    CHECK(kith_gateway_subscribe(fx.gw, &k) == 0);

    // Populate the cell, then refresh so the cache holds one artifact.
    kith_sim_artifact_key_t sk = make_skey(&k, 1u);
    kith_sim_actor_t a = make_actor(9u, 1LL << KITH_SIM_FIX_SHIFT, 0, 7u);
    CHECK(kith_sim_publish_artifact(fx.sim, &sk, &a, nullptr) == 0);
    CHECK(kith_fabric_publish(fx.fabric, &k, 1u, nullptr) == 0);
    CHECK(kith_gateway_cache_refresh(fx.gw, 100u) == 0);
    kith_gateway_cell_snapshot_t snap = {0};
    CHECK(kith_gateway_cache_snapshot_cell(fx.gw, &k, &snap) == 0);
    CHECK(snap.actor_count == 1u);
    CHECK(snap.refreshed_at_ms == 100u);

    // Remove the actor, re-publish the now-empty cell, and refresh.
    CHECK(kith_sim_remove_artifact(fx.sim, 9u) == 0);
    CHECK(kith_fabric_publish(fx.fabric, &k, 1u, nullptr) == 0);
    CHECK(kith_gateway_cache_refresh(fx.gw, 200u) == 0);
    CHECK(kith_gateway_cache_snapshot_cell(fx.gw, &k, &snap) == 0);
    CHECK(snap.actor_count == 0u);
    CHECK(snap.refreshed_at_ms == 200u);

    fixture_fini(&fx);
    return failures;
}

// Subscribe consecutive cells in @p zone until the first failure. Returns
// the landing count and the failed cell's x coordinate through
// @p out_fail_x (-1 when every subscribe landed).
static size_t subscribe_run(kith_gateway_t *gw, uint32_t zone, int32_t *out_fail_x)
{
    size_t landed = 0u;
    *out_fail_x = -1;
    for (int32_t x = 0; x < (int32_t)CAPACITY_PROBE_CELLS; ++x)
    {
        kith_fabric_cell_key_t k = make_key(zone, x, 0, 0, 0u);
        if (kith_gateway_subscribe(gw, &k) != 0)
        {
            *out_fail_x = x;
            break;
        }
        ++landed;
    }
    return landed;
}

// Cache capacity binds identically before and after a churn cycle. A
// 16-bucket cache gives every stripe 16 slots (256 cells in total), so a
// fill past that fails with ENOMEM once a stripe saturates, the cache and
// fabric counts stay equal to the landing count, a full evict empties both,
// and a refill of the same cells binds at the same ceiling through the
// reclaimed slots — every reclaimed cell is findable and refcounted, so
// nothing is subscribed without a live cache node.
static int test_cache_capacity_binds_after_churn(void)
{
    int failures = 0;
    struct fixture fx;
    CHECK(fixture_init_buckets(&fx, 16u) == 0);

    int32_t fail_x = -1;
    size_t landed = subscribe_run(fx.gw, 7u, &fail_x);
    CHECK(fail_x >= 0);
    CHECK(landed > 0u);
    CHECK(landed <= 256u);

    kith_gateway_cache_stats_t stats = {0};
    CHECK(kith_gateway_cache_stats(fx.gw, &stats) == 0);
    CHECK(stats.cell_count == landed);
    CHECK(stats.subscribed_cell_count == landed);

    for (uint32_t x = 0u; x < landed; ++x)
    {
        kith_fabric_cell_key_t k = make_key(7u, (int32_t)x, 0, 0, 0u);
        CHECK(kith_gateway_unsubscribe(fx.gw, &k) == 0);
    }
    CHECK(kith_gateway_cache_stats(fx.gw, &stats) == 0);
    CHECK(stats.cell_count == 0u);
    CHECK(stats.subscribed_cell_count == 0u);

    int32_t refill_fail_x = -1;
    size_t refilled = subscribe_run(fx.gw, 7u, &refill_fail_x);
    CHECK(refill_fail_x == fail_x);
    CHECK(refilled == landed);
    CHECK(kith_gateway_cache_stats(fx.gw, &stats) == 0);
    CHECK(stats.cell_count == refilled);
    CHECK(stats.subscribed_cell_count == refilled);

    // A reclaimed cell is a live node: findable, refcounted, and removable.
    kith_fabric_cell_key_t first_key = make_key(7u, 0, 0, 0, 0u);
    kith_gateway_cell_snapshot_t snap = {0};
    CHECK(kith_gateway_cache_snapshot_cell(fx.gw, &first_key, &snap) == 0);
    CHECK(snap.refcount == 1u);
    CHECK(kith_gateway_unsubscribe(fx.gw, &first_key) == 0);

    fixture_fini(&fx);
    return failures;
}

int main(void)
{
    int rc = 0;
    rc |= test_cache_arg_validation();
    rc |= test_subscribe_refcount();
    rc |= test_cache_refresh_updates();
    rc |= test_cache_refresh_interval();
    rc |= test_cache_refresh_clears_emptied_cell();
    rc |= test_cache_capacity_binds_after_churn();
    if (rc != 0)
    {
        (void)fprintf(stderr, "gateway cache tests FAILED\n");
    }
    return rc;
}
