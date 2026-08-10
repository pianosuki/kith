#include <stddef.h>
#include <stdint.h>
#include <stdio.h>

#include "kith/fabric/fabric.h"
#include "kith/sim/sim.h"
#include "kith/types.h"

/* Cell-store churn past the initial capacity. The insert path grows a shard
 * when live-plus-tombstone load crosses the growth threshold, so publish
 * lands a readable product for every distinct key across create/remove
 * churn, and every published cell stays queryable below and above each
 * growth point. */

static int check_cond(bool ok, int line)
{
    if (!ok)
    {
        (void)fprintf(stderr, "fabric cell store: assertion at line %d failed\n", line);
        return 1;
    }
    return 0;
}

#define CHECK(cond) failures += check_cond((cond), __LINE__)

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

static int check_publish_range(kith_fabric_t *f, uint32_t zone, int32_t begin, int32_t end)
{
    int failures = 0;
    for (int32_t x = begin; x < end; ++x)
    {
        kith_fabric_cell_key_t k = make_key(zone, x, 0, 0, 0u);
        if (kith_fabric_publish(f, &k, 1u, nullptr) != 0)
        {
            (void)fprintf(stderr, "fabric cell store: publish of cell_x %d failed\n", x);
            failures += 1;
        }
    }
    return failures;
}

static int check_remove_range(kith_fabric_t *f, uint32_t zone, int32_t begin, int32_t end)
{
    int failures = 0;
    for (int32_t x = begin; x < end; ++x)
    {
        kith_fabric_cell_key_t k = make_key(zone, x, 0, 0, 0u);
        if (kith_fabric_remove_cell(f, &k) != 0)
        {
            (void)fprintf(stderr, "fabric cell store: remove of cell_x %d failed\n", x);
            failures += 1;
        }
    }
    return failures;
}

// Every cell in the range is queryable with the publish epoch and a fresh
// sequence number.
static int check_readable_range(kith_fabric_t *f, uint32_t zone, int32_t begin, int32_t end)
{
    int failures = 0;
    for (int32_t x = begin; x < end; ++x)
    {
        kith_fabric_cell_key_t k = make_key(zone, x, 0, 0, 0u);
        kith_fabric_cell_product_t p = {0};
        if (kith_fabric_cell_product(f, &k, &p) != 0 || p.authority_epoch != 1u ||
            p.publish_seq != 1u)
        {
            (void)fprintf(stderr, "fabric cell store: cell_x %d unreadable or stale\n", x);
            failures += 1;
        }
    }
    return failures;
}

// Publish, tombstone, and republish enough distinct keys in one zone to
// cross the growth threshold of one shard (3/4 of the initial bucket
// count), then keep churning. Every published-and-not-removed cell stays
// queryable, and a subscription sampling one live cell from each wave
// receives all of them on the final drain.
static int test_cell_store_churn_grow(void)
{
    int failures = 0;
    kith_sim_t *sim = nullptr;
    CHECK(kith_sim_create(nullptr, nullptr, &sim) == 0);
    kith_fabric_t *f = nullptr;
    CHECK(kith_fabric_create(nullptr, sim, nullptr, &f) == 0);

    // The samples are subscribed before their first publish so the pending
    // marks accumulate through the whole run.
    kith_fabric_subscription_t *sub = nullptr;
    CHECK(kith_fabric_create_subscription(f, &sub) == 0);
    kith_fabric_cell_key_t sample_a = make_key(7u, 1500, 0, 0, 0u);
    kith_fabric_cell_key_t sample_b = make_key(7u, 5000, 0, 0, 0u);
    kith_fabric_cell_key_t sample_c = make_key(7u, 8500, 0, 0, 0u);
    CHECK(kith_fabric_subscription_add(sub, &sample_a) == 0);
    CHECK(kith_fabric_subscription_add(sub, &sample_b) == 0);
    CHECK(kith_fabric_subscription_add(sub, &sample_c) == 0);

    // Wave 1: fill below the growth threshold, then tombstone half of it —
    // the vacated slots stay occupied for probing.
    failures += check_publish_range(f, 7u, 0, 2048);
    CHECK(kith_fabric_product_count(f) == 2048u);
    failures += check_remove_range(f, 7u, 0, 1024);
    CHECK(kith_fabric_product_count(f) == 1024u);

    // Wave 2: live-plus-tombstone load crosses 3/4 of the initial bucket
    // count mid-batch, growing the shard.
    failures += check_publish_range(f, 7u, 4096, 6144);
    CHECK(kith_fabric_product_count(f) == 3072u);

    // Wave 3: another tombstone-and-refill round on the grown table.
    failures += check_remove_range(f, 7u, 4096, 4608);
    CHECK(kith_fabric_product_count(f) == 2560u);
    failures += check_publish_range(f, 7u, 8192, 8704);
    CHECK(kith_fabric_product_count(f) == 3072u);

    // Every published-and-not-removed cell is queryable with its publish
    // epoch: the sweep covers keys below and above every growth point.
    failures += check_readable_range(f, 7u, 1024, 2048);
    failures += check_readable_range(f, 7u, 4608, 6144);
    failures += check_readable_range(f, 7u, 8192, 8704);

    // The sampled cells deliver on the final drain, and the queue empties.
    kith_fabric_cell_product_t out[8] = {0};
    size_t n = 0u;
    CHECK(kith_fabric_drain(f, sub, out, 8u, &n) == 0);
    CHECK(n == 3u);
    bool seen_a = false;
    bool seen_b = false;
    bool seen_c = false;
    for (size_t i = 0u; i < n; ++i)
    {
        seen_a = seen_a || (out[i].key.cell_x == sample_a.cell_x);
        seen_b = seen_b || (out[i].key.cell_x == sample_b.cell_x);
        seen_c = seen_c || (out[i].key.cell_x == sample_c.cell_x);
    }
    CHECK(seen_a);
    CHECK(seen_b);
    CHECK(seen_c);
    size_t total = 99u;
    CHECK(kith_fabric_drain(f, sub, nullptr, 0u, &total) == 0);
    CHECK(total == 0u);

    kith_fabric_subscription_destroy(sub);
    kith_fabric_destroy(f);
    kith_sim_destroy(sim);
    return failures;
}

// A removed cell's slot is reusable with fresh identity: re-publishing the
// same key after removal restarts the sequence at one, the product is
// queryable again, and the re-publish is delivered to a subscriber.
static int test_cell_store_recreate_after_remove(void)
{
    int failures = 0;
    kith_sim_t *sim = nullptr;
    CHECK(kith_sim_create(nullptr, nullptr, &sim) == 0);
    kith_fabric_t *f = nullptr;
    CHECK(kith_fabric_create(nullptr, sim, nullptr, &f) == 0);

    kith_fabric_subscription_t *sub = nullptr;
    CHECK(kith_fabric_create_subscription(f, &sub) == 0);
    kith_fabric_cell_key_t k = make_key(9u, 3, 4, 5, 2u);
    CHECK(kith_fabric_subscription_add(sub, &k) == 0);

    CHECK(kith_fabric_publish(f, &k, 1u, nullptr) == 0);
    uint64_t seq = 0u;
    CHECK(kith_fabric_publish(f, &k, 1u, &seq) == 0);
    CHECK(seq == 2u);
    CHECK(kith_fabric_remove_cell(f, &k) == 0);
    kith_fabric_cell_product_t gone = {0};
    CHECK(kith_fabric_cell_product(f, &k, &gone) == kith_error_return(KITH_ENOENT));

    CHECK(kith_fabric_publish(f, &k, 1u, &seq) == 0);
    CHECK(seq == 1u);
    kith_fabric_cell_product_t p = {0};
    CHECK(kith_fabric_cell_product(f, &k, &p) == 0);
    CHECK(p.authority_epoch == 1u);
    CHECK(p.publish_seq == 1u);

    kith_fabric_cell_product_t out[4] = {0};
    size_t n = 0u;
    CHECK(kith_fabric_drain(f, sub, out, 4u, &n) == 0);
    CHECK(n == 1u);
    CHECK(out[0].key.cell_x == 3);
    CHECK(out[0].publish_seq == 1u);

    kith_fabric_subscription_destroy(sub);
    kith_fabric_destroy(f);
    kith_sim_destroy(sim);
    return failures;
}

int main(void)
{
    int rc = 0;
    rc |= test_cell_store_churn_grow();
    rc |= test_cell_store_recreate_after_remove();
    if (rc != 0)
    {
        (void)fprintf(stderr, "fabric cell store tests FAILED\n");
    }
    return rc;
}
