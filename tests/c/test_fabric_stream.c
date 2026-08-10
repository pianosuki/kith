#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include "kith/fabric/fabric.h"
#include "kith/sim/sim.h"
#include "kith/types.h"

static int check_cond(bool ok, int line)
{
    if (!ok)
    {
        (void)fprintf(stderr, "fabric stream: assertion at line %d failed\n", line);
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

// The first publish for a cell assigns publish_seq = 1; the cell appears in
// the stream and product_count reflects it. cell_product returns the header
// with the authority_epoch the caller supplied.
static int test_publish_basic(void)
{
    int failures = 0;
    kith_sim_t *sim = nullptr;
    CHECK(kith_sim_create(nullptr, nullptr, &sim) == 0);
    kith_fabric_t *f = nullptr;
    CHECK(kith_fabric_create(nullptr, sim, nullptr, &f) == 0);

    kith_fabric_cell_key_t k = make_key(1u, 0, 0, 0, 0u);
    uint64_t seq = 999u;
    CHECK(kith_fabric_publish(f, &k, 7u, &seq) == 0);
    CHECK(seq == 1u);
    CHECK(kith_fabric_product_count(f) == 1u);

    kith_fabric_cell_product_t p = {0};
    CHECK(kith_fabric_cell_product(f, &k, &p) == 0);
    CHECK(p.key.zone == 1u);
    CHECK(p.key.cell_x == 0);
    CHECK(p.authority_epoch == 7u);
    CHECK(p.publish_seq == 1u);

    // A cell with no product returns ENOENT.
    kith_fabric_cell_key_t k_empty = make_key(1u, 99, 99, 0, 0u);
    CHECK(kith_fabric_cell_product(f, &k_empty, &p) == kith_error_return(KITH_ENOENT));

    kith_fabric_destroy(f);
    kith_sim_destroy(sim);
    return failures;
}

// Publishing the same cell at the same authority_epoch refreshes the product:
// publish_seq increments, the cell count stays at 1, and cell_product returns
// the refreshed sequence.
static int test_publish_refresh_same_epoch(void)
{
    int failures = 0;
    kith_sim_t *sim = nullptr;
    CHECK(kith_sim_create(nullptr, nullptr, &sim) == 0);
    kith_fabric_t *f = nullptr;
    CHECK(kith_fabric_create(nullptr, sim, nullptr, &f) == 0);

    kith_fabric_cell_key_t k = make_key(2u, 1, 1, 0, 0u);
    uint64_t seq1 = 0u;
    CHECK(kith_fabric_publish(f, &k, 5u, &seq1) == 0);
    CHECK(seq1 == 1u);

    uint64_t seq2 = 0u;
    CHECK(kith_fabric_publish(f, &k, 5u, &seq2) == 0);
    CHECK(seq2 == 2u);
    CHECK(kith_fabric_product_count(f) == 1u);

    kith_fabric_cell_product_t p = {0};
    CHECK(kith_fabric_cell_product(f, &k, &p) == 0);
    CHECK(p.publish_seq == 2u);
    CHECK(p.authority_epoch == 5u);

    kith_fabric_destroy(f);
    kith_sim_destroy(sim);
    return failures;
}

// Publishing at a newer authority_epoch transitions authority: publish_seq
// increments and the stored epoch advances. Publishing at an older epoch is
// rejected with EPERM and does not mutate the stored product.
static int test_publish_epoch_guard(void)
{
    int failures = 0;
    kith_sim_t *sim = nullptr;
    CHECK(kith_sim_create(nullptr, nullptr, &sim) == 0);
    kith_fabric_t *f = nullptr;
    CHECK(kith_fabric_create(nullptr, sim, nullptr, &f) == 0);

    kith_fabric_cell_key_t k = make_key(3u, 0, 0, 0, 0u);
    uint64_t seq1 = 0u;
    CHECK(kith_fabric_publish(f, &k, 10u, &seq1) == 0);
    CHECK(seq1 == 1u);

    // Older epoch: rejected, stored product unchanged.
    uint64_t seq_stale = 999u;
    CHECK(kith_fabric_publish(f, &k, 9u, &seq_stale) == kith_error_return(KITH_EPERM));
    kith_fabric_cell_product_t p = {0};
    CHECK(kith_fabric_cell_product(f, &k, &p) == 0);
    CHECK(p.authority_epoch == 10u);
    CHECK(p.publish_seq == 1u);

    // Newer epoch: authority transition, seq increments, epoch advances.
    uint64_t seq3 = 0u;
    CHECK(kith_fabric_publish(f, &k, 11u, &seq3) == 0);
    CHECK(seq3 == 2u);
    CHECK(kith_fabric_cell_product(f, &k, &p) == 0);
    CHECK(p.authority_epoch == 11u);
    CHECK(p.publish_seq == 2u);

    // Equal epoch: refresh again (covered separately, but assert here too).
    uint64_t seq4 = 0u;
    CHECK(kith_fabric_publish(f, &k, 11u, &seq4) == 0);
    CHECK(seq4 == 3u);

    kith_fabric_destroy(f);
    kith_sim_destroy(sim);
    return failures;
}

// snapshot_zone_cells returns one product header per cell in a zone at a
// given lod. Cells at other lods are filtered out. The lod field on the
// returned headers matches the requested lod.
static int test_snapshot_zone_cells(void)
{
    int failures = 0;
    kith_sim_t *sim = nullptr;
    CHECK(kith_sim_create(nullptr, nullptr, &sim) == 0);
    kith_fabric_t *f = nullptr;
    CHECK(kith_fabric_create(nullptr, sim, nullptr, &f) == 0);

    // Two cells in zone 6 at lod 0, one cell at lod 1.
    kith_fabric_cell_key_t k_a = make_key(6u, 0, 0, 0, 0u);
    kith_fabric_cell_key_t k_b = make_key(6u, 1, 0, 0, 0u);
    kith_fabric_cell_key_t k_c = make_key(6u, 0, 0, 0, 1u);
    CHECK(kith_fabric_publish(f, &k_a, 1u, nullptr) == 0);
    CHECK(kith_fabric_publish(f, &k_b, 1u, nullptr) == 0);
    CHECK(kith_fabric_publish(f, &k_c, 1u, nullptr) == 0);
    CHECK(kith_fabric_product_count(f) == 3u);

    kith_fabric_cell_product_t products[8] = {0};
    size_t n = 0u;
    CHECK(kith_fabric_snapshot_zone_cells(f, 6u, 0u, products, 8u, &n) == 0);
    CHECK(n == 2u);

    // Order is hash-bucket order, not insertion order, so locate by content.
    const kith_fabric_cell_product_t *pa = nullptr;
    const kith_fabric_cell_product_t *pb = nullptr;
    for (size_t i = 0u; i < n; ++i)
    {
        if (products[i].key.cell_x == 0)
        {
            pa = &products[i];
        }
        else if (products[i].key.cell_x == 1)
        {
            pb = &products[i];
        }
    }
    CHECK(pa != nullptr);
    CHECK(pb != nullptr);
    if (pa != nullptr)
    {
        CHECK(pa->key.lod == 0u);
        CHECK(pa->publish_seq == 1u);
    }
    if (pb != nullptr)
    {
        CHECK(pb->key.lod == 0u);
    }

    // lod 1 has exactly one cell.
    kith_fabric_cell_product_t lod1[8] = {0};
    size_t n1 = 0u;
    CHECK(kith_fabric_snapshot_zone_cells(f, 6u, 1u, lod1, 8u, &n1) == 0);
    CHECK(n1 == 1u);
    CHECK(lod1[0].key.lod == 1u);

    // A zone with no products reports zero cells (counting mode).
    size_t n_empty = 99u;
    CHECK(kith_fabric_snapshot_zone_cells(f, 99u, 0u, nullptr, 0u, &n_empty) == 0);
    CHECK(n_empty == 0u);

    kith_fabric_destroy(f);
    kith_sim_destroy(sim);
    return failures;
}

// remove_cell drops a cell from the stream: product_count decrements,
// cell_product returns ENOENT, and snapshot_zone_cells does not list it.
// Removing a cell that does not exist is a no-op success.
static int test_remove_cell(void)
{
    int failures = 0;
    kith_sim_t *sim = nullptr;
    CHECK(kith_sim_create(nullptr, nullptr, &sim) == 0);
    kith_fabric_t *f = nullptr;
    CHECK(kith_fabric_create(nullptr, sim, nullptr, &f) == 0);

    kith_fabric_cell_key_t k = make_key(7u, 2, 3, 0, 0u);
    CHECK(kith_fabric_publish(f, &k, 1u, nullptr) == 0);
    CHECK(kith_fabric_product_count(f) == 1u);

    // Removing a non-existent cell is idempotent.
    kith_fabric_cell_key_t k_other = make_key(7u, 9, 9, 0, 0u);
    CHECK(kith_fabric_remove_cell(f, &k_other) == 0);
    CHECK(kith_fabric_product_count(f) == 1u);

    CHECK(kith_fabric_remove_cell(f, &k) == 0);
    CHECK(kith_fabric_product_count(f) == 0u);

    kith_fabric_cell_product_t p = {0};
    CHECK(kith_fabric_cell_product(f, &k, &p) == kith_error_return(KITH_ENOENT));

    // The slot is tombstoned, so republishing reuses it and the count goes
    // back to 1 (the tombstone does not inflate the live count).
    CHECK(kith_fabric_publish(f, &k, 1u, nullptr) == 0);
    CHECK(kith_fabric_product_count(f) == 1u);

    kith_fabric_destroy(f);
    kith_sim_destroy(sim);
    return failures;
}

// remove_zone drops every cell in one zone. Two zones that share a shard
// (zone 0 and zone KITH_FABRIC_CELL_SHARDS both mod to shard 0) exercise
// the per-zone filter inside one shard: removing zone 0 leaves the cells in
// the colliding zone intact.
static int test_remove_zone(void)
{
    int failures = 0;
    kith_sim_t *sim = nullptr;
    CHECK(kith_sim_create(nullptr, nullptr, &sim) == 0);
    kith_fabric_t *f = nullptr;
    CHECK(kith_fabric_create(nullptr, sim, nullptr, &f) == 0);

    kith_fabric_cell_key_t k_z0 = make_key(0u, 0, 0, 0, 0u);
    kith_fabric_cell_key_t k_z_collide = make_key(KITH_FABRIC_CELL_SHARDS, 0, 0, 0, 0u);
    kith_fabric_cell_key_t k_z1 = make_key(1u, 0, 0, 0, 0u);
    CHECK(kith_fabric_publish(f, &k_z0, 1u, nullptr) == 0);
    CHECK(kith_fabric_publish(f, &k_z_collide, 1u, nullptr) == 0);
    CHECK(kith_fabric_publish(f, &k_z1, 1u, nullptr) == 0);
    CHECK(kith_fabric_product_count(f) == 3u);

    CHECK(kith_fabric_remove_zone(f, 0u) == 0);
    CHECK(kith_fabric_product_count(f) == 2u);

    kith_fabric_cell_product_t p = {0};
    CHECK(kith_fabric_cell_product(f, &k_z0, &p) == kith_error_return(KITH_ENOENT));
    CHECK(kith_fabric_cell_product(f, &k_z_collide, &p) == 0);
    CHECK(kith_fabric_cell_product(f, &k_z1, &p) == 0);

    // Removing a zone with no products is a no-op success.
    CHECK(kith_fabric_remove_zone(f, 99u) == 0);
    CHECK(kith_fabric_product_count(f) == 2u);

    kith_fabric_destroy(f);
    kith_sim_destroy(sim);
    return failures;
}

int main(void)
{
    int rc = 0;
    rc |= test_publish_basic();
    rc |= test_publish_refresh_same_epoch();
    rc |= test_publish_epoch_guard();
    rc |= test_snapshot_zone_cells();
    rc |= test_remove_cell();
    rc |= test_remove_zone();
    if (rc != 0)
    {
        (void)fprintf(stderr, "fabric stream tests FAILED\n");
    }
    return rc;
}
