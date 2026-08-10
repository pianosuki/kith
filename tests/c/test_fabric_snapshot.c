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
        (void)fprintf(stderr, "fabric snapshot: assertion at line %d failed\n", line);
        return 1;
    }
    return 0;
}

#define CHECK(cond) failures += check_cond((cond), __LINE__)

static kith_fabric_cell_key_t
make_fkey(uint32_t zone, int32_t cx, int32_t cy, int32_t cz, uint8_t lod)
{
    kith_fabric_cell_key_t k = {0};
    k.zone = zone;
    k.cell_x = cx;
    k.cell_y = cy;
    k.cell_z = cz;
    k.lod = lod;
    return k;
}

static kith_sim_artifact_key_t
make_skey(uint32_t zone, int32_t cx, int32_t cy, int32_t cz, uint8_t lod, uint32_t epoch)
{
    kith_sim_artifact_key_t k = {0};
    k.zone = zone;
    k.cell_x = cx;
    k.cell_y = cy;
    k.cell_z = cz;
    k.lod = lod;
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

// The publisher-minted update_seq rides the full rendering verbatim and is
// zeroed in the reduced rendering alongside the input tick: reduced and
// crowd renderings do not vouch for per-actor coverage.
static int test_snapshot_update_seq_tiering(void)
{
    int failures = 0;
    kith_sim_t *sim = nullptr;
    CHECK(kith_sim_create(nullptr, nullptr, &sim) == 0);
    kith_fabric_t *f = nullptr;
    CHECK(kith_fabric_create(nullptr, sim, nullptr, &f) == 0);

    kith_sim_artifact_key_t sk = make_skey(5u, 0, 0, 0, 0u, 1u);
    kith_sim_actor_t a = make_actor(13u, 0, 0, 7u);
    a.update_seq = 4u;
    CHECK(kith_sim_publish_artifact(sim, &sk, &a, nullptr) == 0);

    kith_fabric_cell_key_t fk = make_fkey(5u, 0, 0, 0, 0u);
    CHECK(kith_fabric_publish(f, &fk, 1u, nullptr) == 0);

    kith_fabric_artifact_t out[4] = {0};
    size_t n = 0u;
    CHECK(kith_fabric_snapshot_cell(f, &fk, KITH_FABRIC_LEVEL_FULL, out, 4u, &n) == 0);
    CHECK(n == 1u);
    CHECK(out[0].update_seq == 4u);

    CHECK(kith_fabric_snapshot_cell(f, &fk, KITH_FABRIC_LEVEL_REDUCED, out, 4u, &n) == 0);
    CHECK(n == 1u);
    CHECK(out[0].update_seq == 0u);

    kith_fabric_destroy(f);
    kith_sim_destroy(sim);
    return failures;
}

// The full tier copies every field from the sim artifact: position,
// velocity, and the input tick. The fabric borrows the sim handle and
// queries its artifact store at snapshot time, so a sim publish between two
// fabric snapshots is visible in the second call.
static int test_snapshot_full(void)
{
    int failures = 0;
    kith_sim_t *sim = nullptr;
    CHECK(kith_sim_create(nullptr, nullptr, &sim) == 0);
    kith_fabric_t *f = nullptr;
    CHECK(kith_fabric_create(nullptr, sim, nullptr, &f) == 0);

    kith_sim_artifact_key_t sk = make_skey(1u, 0, 0, 0, 0u, 1u);
    kith_sim_actor_t a = make_actor(7u, 1LL << KITH_SIM_FIX_SHIFT, 2LL << KITH_SIM_FIX_SHIFT, 11u);
    CHECK(kith_sim_publish_artifact(sim, &sk, &a, nullptr) == 0);

    // The fabric cell must exist for cell_product, but snapshot_cell queries
    // the sim directly and renders whatever the sim holds for the locator.
    kith_fabric_cell_key_t fk = make_fkey(1u, 0, 0, 0, 0u);
    CHECK(kith_fabric_publish(f, &fk, 1u, nullptr) == 0);

    kith_fabric_artifact_t out[4] = {0};
    size_t n = 99u;
    CHECK(kith_fabric_snapshot_cell(f, &fk, KITH_FABRIC_LEVEL_FULL, out, 4u, &n) == 0);
    CHECK(n == 1u);
    CHECK(out[0].actor_id == 7u);
    CHECK(out[0].pos_x == a.pos_x);
    CHECK(out[0].pos_y == a.pos_y);
    CHECK(out[0].input_tick == 11u);
    CHECK(out[0].update_seq == 0u);
    CHECK(out[0].product_level == KITH_FABRIC_LEVEL_FULL);

    kith_fabric_destroy(f);
    kith_sim_destroy(sim);
    return failures;
}

// The reduced tier zeroes the input tick (the field that varies most rapidly
// and is least relevant to a far subscriber) while keeping position and
// velocity. The product_level field on the returned artifact is REDUCED.
static int test_snapshot_reduced(void)
{
    int failures = 0;
    kith_sim_t *sim = nullptr;
    CHECK(kith_sim_create(nullptr, nullptr, &sim) == 0);
    kith_fabric_t *f = nullptr;
    CHECK(kith_fabric_create(nullptr, sim, nullptr, &f) == 0);

    kith_sim_artifact_key_t sk = make_skey(2u, 0, 0, 0, 0u, 1u);
    kith_sim_actor_t a = make_actor(11u, 3LL << KITH_SIM_FIX_SHIFT, 4LL << KITH_SIM_FIX_SHIFT, 42u);
    CHECK(kith_sim_publish_artifact(sim, &sk, &a, nullptr) == 0);

    kith_fabric_cell_key_t fk = make_fkey(2u, 0, 0, 0, 0u);
    CHECK(kith_fabric_publish(f, &fk, 1u, nullptr) == 0);

    kith_fabric_artifact_t out[4] = {0};
    size_t n = 0u;
    CHECK(kith_fabric_snapshot_cell(f, &fk, KITH_FABRIC_LEVEL_REDUCED, out, 4u, &n) == 0);
    CHECK(n == 1u);
    CHECK(out[0].actor_id == 11u);
    CHECK(out[0].pos_x == a.pos_x);
    CHECK(out[0].input_tick == 0u);
    CHECK(out[0].update_seq == 0u);
    CHECK(out[0].product_level == KITH_FABRIC_LEVEL_REDUCED);

    kith_fabric_destroy(f);
    kith_sim_destroy(sim);
    return failures;
}

// The crowd tier collapses every actor in a cell into one centroid artifact
// with actor_id = 0 and product_level = CROWD. An empty cell returns zero
// artifacts. The centroid is the mean of the per-axis positions (sum then
// divide), not the sum of per-actor truncated means.
static int test_snapshot_crowd(void)
{
    int failures = 0;
    kith_sim_t *sim = nullptr;
    CHECK(kith_sim_create(nullptr, nullptr, &sim) == 0);
    kith_fabric_t *f = nullptr;
    CHECK(kith_fabric_create(nullptr, sim, nullptr, &f) == 0);

    // Empty cell: crowd returns zero artifacts.
    kith_fabric_cell_key_t fk_empty = make_fkey(3u, 0, 0, 0, 0u);
    kith_fabric_artifact_t out_empty[1] = {0};
    size_t n_empty = 99u;
    CHECK(kith_fabric_snapshot_cell(
              f, &fk_empty, KITH_FABRIC_LEVEL_CROWD, out_empty, 1u, &n_empty) == 0);
    CHECK(n_empty == 0u);

    // Three actors at positions 10, 20, 30 (in Q16.16 units). The centroid
    // is 20 (sum 60 / count 3), not 19 (sum of per-actor truncated means).
    kith_sim_artifact_key_t sk = make_skey(3u, 0, 0, 0, 0u, 1u);
    kith_sim_actor_t a1 = make_actor(1u, 10LL << KITH_SIM_FIX_SHIFT, 0, 0u);
    kith_sim_actor_t a2 = make_actor(2u, 20LL << KITH_SIM_FIX_SHIFT, 0, 0u);
    kith_sim_actor_t a3 = make_actor(3u, 30LL << KITH_SIM_FIX_SHIFT, 0, 0u);
    CHECK(kith_sim_publish_artifact(sim, &sk, &a1, nullptr) == 0);
    CHECK(kith_sim_publish_artifact(sim, &sk, &a2, nullptr) == 0);
    CHECK(kith_sim_publish_artifact(sim, &sk, &a3, nullptr) == 0);

    kith_fabric_cell_key_t fk = make_fkey(3u, 0, 0, 0, 0u);
    CHECK(kith_fabric_publish(f, &fk, 1u, nullptr) == 0);

    kith_fabric_artifact_t out[4] = {0};
    size_t n = 0u;
    CHECK(kith_fabric_snapshot_cell(f, &fk, KITH_FABRIC_LEVEL_CROWD, out, 4u, &n) == 0);
    CHECK(n == 1u);
    CHECK(out[0].actor_id == 0u);
    CHECK(out[0].pos_x == 20LL << KITH_SIM_FIX_SHIFT);
    CHECK(out[0].product_level == KITH_FABRIC_LEVEL_CROWD);

    // Counting mode (out=NULL, max=0) reports 1 for a non-empty cell and does
    // not fill any output slot.
    size_t total = 99u;
    CHECK(kith_fabric_snapshot_cell(f, &fk, KITH_FABRIC_LEVEL_CROWD, nullptr, 0u, &total) == 0);
    CHECK(total == 1u);

    // A population whose per-axis sum exceeds the int64 range still yields
    // the exact mean: four actors at 4e18 raw sum to 1.6e19, and the
    // centroid is 4e18, not the wrapped sum's quotient.
    kith_sim_artifact_key_t sk_wide = make_skey(6u, 0, 0, 0, 0u, 1u);
    kith_sim_actor_t a11 = make_actor(11u, 4'000'000'000'000'000'000, 0, 1u);
    kith_sim_actor_t a12 = make_actor(12u, 4'000'000'000'000'000'000, 0, 1u);
    kith_sim_actor_t a13 = make_actor(13u, 4'000'000'000'000'000'000, 0, 1u);
    kith_sim_actor_t a14 = make_actor(14u, 4'000'000'000'000'000'000, 0, 1u);
    CHECK(kith_sim_publish_artifact(sim, &sk_wide, &a11, nullptr) == 0);
    CHECK(kith_sim_publish_artifact(sim, &sk_wide, &a12, nullptr) == 0);
    CHECK(kith_sim_publish_artifact(sim, &sk_wide, &a13, nullptr) == 0);
    CHECK(kith_sim_publish_artifact(sim, &sk_wide, &a14, nullptr) == 0);
    kith_fabric_cell_key_t fk_wide = make_fkey(6u, 0, 0, 0, 0u);
    CHECK(kith_fabric_publish(f, &fk_wide, 1u, nullptr) == 0);
    kith_fabric_artifact_t out_wide[1] = {0};
    size_t n_wide = 0u;
    CHECK(kith_fabric_snapshot_cell(f, &fk_wide, KITH_FABRIC_LEVEL_CROWD, out_wide, 1u, &n_wide) ==
          0);
    CHECK(n_wide == 1u);
    CHECK(out_wide[0].pos_x == 4'000'000'000'000'000'000);

    kith_fabric_destroy(f);
    kith_sim_destroy(sim);
    return failures;
}

// snapshot_cell returns the cell's full artifact set in an unspecified
// order that is deterministic for a given sequence of store mutations.
// Three actors published in sequence into one cell must all come back
// exactly once, and two snapshots of an unchanged store must agree byte
// for byte. The fabric artifact does not carry publish_seq; the actor_id
// published at each sequence identifies it (actor 3 = seq 3).
static int test_snapshot_ordering(void)
{
    int failures = 0;
    kith_sim_t *sim = nullptr;
    CHECK(kith_sim_create(nullptr, nullptr, &sim) == 0);
    kith_fabric_t *f = nullptr;
    CHECK(kith_fabric_create(nullptr, sim, nullptr, &f) == 0);

    kith_sim_artifact_key_t sk = make_skey(4u, 0, 0, 0, 0u, 1u);
    for (uint64_t id = 1u; id <= 3u; ++id)
    {
        kith_sim_actor_t a = make_actor(id, (int64_t)id << KITH_SIM_FIX_SHIFT, 0, 0u);
        CHECK(kith_sim_publish_artifact(sim, &sk, &a, nullptr) == 0);
    }

    kith_fabric_cell_key_t fk = make_fkey(4u, 0, 0, 0, 0u);
    CHECK(kith_fabric_publish(f, &fk, 1u, nullptr) == 0);

    kith_fabric_artifact_t out[8] = {0};
    size_t n = 0u;
    CHECK(kith_fabric_snapshot_cell(f, &fk, KITH_FABRIC_LEVEL_FULL, out, 8u, &n) == 0);
    CHECK(n == 3u);
    // The set is complete with no duplicates; position in the array is
    // unspecified.
    uint64_t expected[3] = {1u, 2u, 3u};
    for (size_t e = 0u; e < 3u; ++e)
    {
        size_t hits = 0u;
        for (size_t i = 0u; i < n; ++i)
        {
            hits += (out[i].actor_id == expected[e]) ? 1u : 0u;
        }
        CHECK(hits == 1u);
    }

    // Deterministic: a second snapshot of the unchanged store matches byte
    // for byte.
    kith_fabric_artifact_t again[8] = {0};
    size_t n2 = 0u;
    CHECK(kith_fabric_snapshot_cell(f, &fk, KITH_FABRIC_LEVEL_FULL, again, 8u, &n2) == 0);
    CHECK(n2 == n);
    CHECK(memcmp(out, again, n * sizeof(*out)) == 0);

    // Counting mode (out=NULL, max=0) reports the full cell size.
    size_t total = 99u;
    CHECK(kith_fabric_snapshot_cell(f, &fk, KITH_FABRIC_LEVEL_FULL, nullptr, 0u, &total) == 0);
    CHECK(total == 3u);

    kith_fabric_destroy(f);
    kith_sim_destroy(sim);
    return failures;
}

// A buffer smaller than the cell reports -KITH_ERANGE with the required
// count in out_count (not the number of entries filled), fills the first
// max entries with distinct valid artifacts from the cell, and a retry at
// the required size succeeds. The fabric renders from its grow-only sim-row
// scratch, so the retry also exercises the warm-scratch path.
static int test_snapshot_truncation_reports_needed_size(void)
{
    int failures = 0;
    kith_sim_t *sim = nullptr;
    CHECK(kith_sim_create(nullptr, nullptr, &sim) == 0);
    kith_fabric_t *f = nullptr;
    CHECK(kith_fabric_create(nullptr, sim, nullptr, &f) == 0);

    kith_sim_artifact_key_t sk = make_skey(9u, 0, 0, 0, 0u, 1u);
    for (uint64_t id = 1u; id <= 5u; ++id)
    {
        kith_sim_actor_t a = make_actor(id, (int64_t)id << KITH_SIM_FIX_SHIFT, 0, 0u);
        CHECK(kith_sim_publish_artifact(sim, &sk, &a, nullptr) == 0);
    }
    kith_fabric_cell_key_t fk = make_fkey(9u, 0, 0, 0, 0u);
    CHECK(kith_fabric_publish(f, &fk, 1u, nullptr) == 0);

    kith_fabric_artifact_t small[2] = {0};
    size_t m = 0u;
    CHECK(kith_fabric_snapshot_cell(f, &fk, KITH_FABRIC_LEVEL_FULL, small, 2u, &m) ==
          kith_error_return(KITH_ERANGE));
    CHECK(m == 5u);
    // The filled prefix holds two distinct artifacts of the cell.
    CHECK(small[0].actor_id != small[1].actor_id);
    CHECK(small[0].actor_id >= 1u && small[0].actor_id <= 5u);
    CHECK(small[1].actor_id >= 1u && small[1].actor_id <= 5u);

    // Retry at the reported size succeeds and returns everything.
    kith_fabric_artifact_t full[5] = {0};
    size_t n = 0u;
    CHECK(kith_fabric_snapshot_cell(f, &fk, KITH_FABRIC_LEVEL_FULL, full, m, &n) == 0);
    CHECK(n == m);

    // The count-probe form is unaffected by the truncation contract.
    size_t total = 0u;
    CHECK(kith_fabric_snapshot_cell(f, &fk, KITH_FABRIC_LEVEL_FULL, nullptr, 0u, &total) == 0);
    CHECK(total == 5u);

    kith_fabric_destroy(f);
    kith_sim_destroy(sim);
    return failures;
}

int main(void)
{
    int rc = 0;
    rc |= test_snapshot_full();
    rc |= test_snapshot_reduced();
    rc |= test_snapshot_update_seq_tiering();
    rc |= test_snapshot_crowd();
    rc |= test_snapshot_ordering();
    rc |= test_snapshot_truncation_reports_needed_size();
    if (rc != 0)
    {
        (void)fprintf(stderr, "fabric snapshot tests FAILED\n");
    }
    return rc;
}
