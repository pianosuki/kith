#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <threads.h>
#include <time.h>

#include <pthread.h>

#include "kith/sim/sim.h"
#include "kith/types.h"
#include "kith/version.h"

static int check_cond(bool ok, int line)
{
    if (!ok)
    {
        (void)fprintf(stderr, "sim artifact: assertion at line %d failed\n", line);
        return 1;
    }
    return 0;
}

#define CHECK(cond) failures += check_cond((cond), __LINE__)

// Build a cell locator key. publish_seq and authority_epoch are ignored by
// the store on input; the store assigns publish_seq itself.
static kith_sim_artifact_key_t
make_key(uint32_t zone, int32_t cx, int32_t cy, int32_t cz, uint8_t lod, uint32_t epoch)
{
    kith_sim_artifact_key_t k = {0};
    k.zone = zone;
    k.cell_x = cx;
    k.cell_y = cy;
    k.cell_z = cz;
    k.lod = lod;
    k.authority_epoch = epoch;
    k.publish_seq = 0u;
    return k;
}

static kith_sim_actor_t make_actor(uint64_t id, int64_t x, int64_t y)
{
    kith_sim_actor_t a = {0};
    a.id = id;
    a.pos_x = x;
    a.pos_y = y;
    return a;
}

// True when @p id appears among the first @p len artifacts in @p arr.
static bool has_actor(const kith_sim_artifact_t *arr, size_t len, uint64_t id)
{
    for (size_t i = 0u; i < len; ++i)
    {
        if (arr[i].actor_id == id)
        {
            return true;
        }
    }
    return false;
}

// Publish one actor; the store assigns the first publish_seq for the cell,
// artifact_count goes to 1, and snapshot_cell returns the one artifact.
static int test_publish_basic(void)
{
    int failures = 0;
    kith_sim_t *sim = nullptr;
    CHECK(kith_sim_create(nullptr, nullptr, &sim) == 0);

    kith_sim_artifact_key_t k = make_key(1u, 0, 0, 0, 0u, 1u);
    kith_sim_actor_t a = make_actor(7u, 1LL << KITH_SIM_FIX_SHIFT, 2LL << KITH_SIM_FIX_SHIFT);

    uint64_t seq = 999u;
    CHECK(kith_sim_publish_artifact(sim, &k, &a, &seq) == 0);
    CHECK(seq == 1u);
    CHECK(kith_sim_artifact_count(sim) == 1u);

    kith_sim_artifact_t out[4] = {0};
    size_t n = 99u;
    CHECK(kith_sim_snapshot_cell(sim, &k, out, 4u, &n) == 0);
    CHECK(n == 1u);
    CHECK(out[0].actor_id == 7u);
    CHECK(out[0].pos_x == a.pos_x);
    CHECK(out[0].pos_y == a.pos_y);
    CHECK(out[0].key.publish_seq == 1u);

    kith_sim_destroy(sim);
    return failures;
}

// The publisher-minted update_seq rides the actor into the published
// artifact verbatim; an actor that never minted carries 0.
static int test_publish_update_seq_carried(void)
{
    int failures = 0;
    kith_sim_t *sim = nullptr;
    CHECK(kith_sim_create(nullptr, nullptr, &sim) == 0);

    kith_sim_artifact_key_t k = make_key(4u, 0, 0, 0, 0u, 1u);
    kith_sim_actor_t minted = make_actor(31u, 0, 0);
    minted.update_seq = 41u;
    kith_sim_actor_t unminted = make_actor(32u, 0, 0);

    CHECK(kith_sim_publish_artifact(sim, &k, &minted, nullptr) == 0);
    CHECK(kith_sim_publish_artifact(sim, &k, &unminted, nullptr) == 0);

    kith_sim_artifact_t out[4] = {0};
    size_t n = 0u;
    CHECK(kith_sim_snapshot_cell(sim, &k, out, 4u, &n) == 0);
    CHECK(n == 2u);
    for (size_t i = 0u; i < n; ++i)
    {
        if (out[i].actor_id == 31u)
        {
            CHECK(out[i].update_seq == 41u);
        }
        else
        {
            CHECK(out[i].actor_id == 32u);
            CHECK(out[i].update_seq == 0u);
        }
    }

    kith_sim_destroy(sim);
    return failures;
}

// Publishing the same actor again supersedes the existing artifact in place:
// the store's total count stays at 1, the cell's publish_seq increments, and
// the snapshot reflects the updated position.
static int test_publish_supersession_in_place(void)
{
    int failures = 0;
    kith_sim_t *sim = nullptr;
    CHECK(kith_sim_create(nullptr, nullptr, &sim) == 0);

    kith_sim_artifact_key_t k = make_key(2u, 1, 1, 0, 0u, 1u);
    kith_sim_actor_t a = make_actor(11u, 0, 0);

    uint64_t seq1 = 0u;
    CHECK(kith_sim_publish_artifact(sim, &k, &a, &seq1) == 0);
    CHECK(seq1 == 1u);

    // Move the actor to a new position and republish into the same cell.
    a.pos_x = 5LL << KITH_SIM_FIX_SHIFT;
    a.pos_y = 6LL << KITH_SIM_FIX_SHIFT;
    uint64_t seq2 = 0u;
    CHECK(kith_sim_publish_artifact(sim, &k, &a, &seq2) == 0);
    CHECK(seq2 == 2u);
    CHECK(kith_sim_artifact_count(sim) == 1u);

    kith_sim_artifact_t out[4] = {0};
    size_t n = 0u;
    CHECK(kith_sim_snapshot_cell(sim, &k, out, 4u, &n) == 0);
    CHECK(n == 1u);
    CHECK(out[0].key.publish_seq == 2u);
    CHECK(out[0].pos_x == a.pos_x);
    CHECK(out[0].pos_y == a.pos_y);

    kith_sim_destroy(sim);
    return failures;
}

// Publishing the same actor into a different cell supersedes in place at the
// actor level: the old cell loses the artifact, the new cell gains it, and
// the total count stays at 1.
static int test_publish_moves_cell(void)
{
    int failures = 0;
    kith_sim_t *sim = nullptr;
    CHECK(kith_sim_create(nullptr, nullptr, &sim) == 0);

    kith_sim_artifact_key_t k_a = make_key(3u, 0, 0, 0, 0u, 1u);
    kith_sim_artifact_key_t k_b = make_key(3u, 1, 0, 0, 0u, 1u);
    kith_sim_actor_t a = make_actor(21u, 0, 0);

    uint64_t seq = 0u;
    CHECK(kith_sim_publish_artifact(sim, &k_a, &a, &seq) == 0);
    CHECK(seq == 1u);

    // Republish into cell B. The actor moves cells; total stays at 1.
    CHECK(kith_sim_publish_artifact(sim, &k_b, &a, &seq) == 0);
    CHECK(seq == 1u);
    CHECK(kith_sim_artifact_count(sim) == 1u);

    size_t n_a = 99u;
    CHECK(kith_sim_snapshot_cell(sim, &k_a, nullptr, 0u, &n_a) == 0);
    CHECK(n_a == 0u);
    size_t n_b = 99u;
    CHECK(kith_sim_snapshot_cell(sim, &k_b, nullptr, 0u, &n_b) == 0);
    CHECK(n_b == 1u);

    kith_sim_destroy(sim);
    return failures;
}

// remove_artifact drops every artifact for one actor across all cells. The
// total count reflects the removal, and a subsequent snapshot of the cell
// is empty.
static int test_remove_actor(void)
{
    int failures = 0;
    kith_sim_t *sim = nullptr;
    CHECK(kith_sim_create(nullptr, nullptr, &sim) == 0);

    kith_sim_artifact_key_t k = make_key(4u, 0, 0, 0, 0u, 1u);
    kith_sim_actor_t a1 = make_actor(31u, 0, 0);
    kith_sim_actor_t a2 = make_actor(32u, 0, 0);
    CHECK(kith_sim_publish_artifact(sim, &k, &a1, nullptr) == 0);
    CHECK(kith_sim_publish_artifact(sim, &k, &a2, nullptr) == 0);
    CHECK(kith_sim_artifact_count(sim) == 2u);

    CHECK(kith_sim_remove_artifact(sim, 31u) == 0);
    CHECK(kith_sim_artifact_count(sim) == 1u);

    kith_sim_artifact_t out[4] = {0};
    size_t n = 0u;
    CHECK(kith_sim_snapshot_cell(sim, &k, out, 4u, &n) == 0);
    CHECK(n == 1u);
    CHECK(out[0].actor_id == 32u);

    // Removing an actor that has no artifacts is a no-op success.
    CHECK(kith_sim_remove_artifact(sim, 9999u) == 0);
    CHECK(kith_sim_artifact_count(sim) == 1u);

    kith_sim_destroy(sim);
    return failures;
}

// remove_zone drops every artifact whose key.zone matches, leaving artifacts
// in other zones intact. The shard index is zone % SHARDS, so two zones that
// share a shard exercise the per-zone filter inside one shard.
static int test_remove_zone(void)
{
    int failures = 0;
    kith_sim_t *sim = nullptr;
    CHECK(kith_sim_create(nullptr, nullptr, &sim) == 0);

    // Zones 0 and KITH_SIM_ARTIFACT_SHARDS share a shard (both mod to 0).
    kith_sim_artifact_key_t k_z0 = make_key(0u, 0, 0, 0, 0u, 1u);
    kith_sim_artifact_key_t k_z_shard = make_key(KITH_SIM_ARTIFACT_SHARDS, 0, 0, 0, 0u, 1u);
    kith_sim_artifact_key_t k_z1 = make_key(1u, 0, 0, 0, 0u, 1u);
    kith_sim_actor_t a = make_actor(1u, 0, 0);
    CHECK(kith_sim_publish_artifact(sim, &k_z0, &a, nullptr) == 0);
    a.id = 2u;
    CHECK(kith_sim_publish_artifact(sim, &k_z_shard, &a, nullptr) == 0);
    a.id = 3u;
    CHECK(kith_sim_publish_artifact(sim, &k_z1, &a, nullptr) == 0);
    CHECK(kith_sim_artifact_count(sim) == 3u);

    // Remove zone 0; the artifact in zone SHARDS (same shard) must survive,
    // as must the artifact in zone 1.
    CHECK(kith_sim_remove_zone(sim, 0u) == 0);
    CHECK(kith_sim_artifact_count(sim) == 2u);

    size_t n0 = 99u;
    CHECK(kith_sim_snapshot_cell(sim, &k_z0, nullptr, 0u, &n0) == 0);
    CHECK(n0 == 0u);
    size_t n_shard = 99u;
    CHECK(kith_sim_snapshot_cell(sim, &k_z_shard, nullptr, 0u, &n_shard) == 0);
    CHECK(n_shard == 1u);
    size_t n1 = 99u;
    CHECK(kith_sim_snapshot_cell(sim, &k_z1, nullptr, 0u, &n1) == 0);
    CHECK(n1 == 1u);

    kith_sim_destroy(sim);
    return failures;
}

// snapshot_cell returns the cell's full artifact set in an unspecified
// order that is deterministic for a given sequence of store mutations.
// Three actors in one cell must all come back exactly once, and two
// snapshots of an unchanged store must agree byte for byte.
static int test_snapshot_cell_ordering(void)
{
    int failures = 0;
    kith_sim_t *sim = nullptr;
    CHECK(kith_sim_create(nullptr, nullptr, &sim) == 0);

    kith_sim_artifact_key_t k = make_key(5u, 0, 0, 0, 0u, 1u);
    for (uint64_t id = 1u; id <= 3u; ++id)
    {
        kith_sim_actor_t a = make_actor(id, (int64_t)id << KITH_SIM_FIX_SHIFT, 0);
        CHECK(kith_sim_publish_artifact(sim, &k, &a, nullptr) == 0);
    }
    CHECK(kith_sim_artifact_count(sim) == 3u);

    kith_sim_artifact_t out[8] = {0};
    size_t n = 0u;
    CHECK(kith_sim_snapshot_cell(sim, &k, out, 8u, &n) == 0);
    CHECK(n == 3u);
    // The set is complete with no duplicates; position in the array is
    // unspecified.
    uint64_t expected[3] = {1u, 2u, 3u};
    for (size_t e = 0u; e < 3u; ++e)
    {
        size_t hits = 0u;
        for (size_t i = 0u; i < n; ++i)
        {
            hits += (out[i].key.publish_seq == expected[e]) ? 1u : 0u;
        }
        CHECK(hits == 1u);
    }

    // Deterministic: a second snapshot of the unchanged store matches
    // byte for byte.
    kith_sim_artifact_t again[8] = {0};
    size_t n2 = 0u;
    CHECK(kith_sim_snapshot_cell(sim, &k, again, 8u, &n2) == 0);
    CHECK(n2 == n);
    CHECK(memcmp(out, again, n * sizeof(*out)) == 0);

    // Counting mode (out=NULL, max=0) reports the full cell size.
    size_t total = 99u;
    CHECK(kith_sim_snapshot_cell(sim, &k, nullptr, 0u, &total) == 0);
    CHECK(total == 3u);

    kith_sim_destroy(sim);
    return failures;
}

// A buffer smaller than the cell reports -KITH_ERANGE with the required
// count in out_count (not the number of entries filled), fills the first
// max entries with distinct valid artifacts from the cell, and a retry at
// the required size succeeds. This is the contract that closes the silent
// count-then-copy truncation: a caller sizing its buffer from out_count can
// always recover the full snapshot.
static int test_snapshot_truncation_reports_needed_size(void)
{
    int failures = 0;
    kith_sim_t *sim = nullptr;
    CHECK(kith_sim_create(nullptr, nullptr, &sim) == 0);

    kith_sim_artifact_key_t k = make_key(7u, 0, 0, 0, 0u, 1u);
    for (uint64_t id = 1u; id <= 5u; ++id)
    {
        kith_sim_actor_t a = make_actor(id, (int64_t)id << KITH_SIM_FIX_SHIFT, 0);
        CHECK(kith_sim_publish_artifact(sim, &k, &a, nullptr) == 0);
    }

    kith_sim_artifact_t small[2] = {0};
    size_t m = 0u;
    CHECK(kith_sim_snapshot_cell(sim, &k, small, 2u, &m) == kith_error_return(KITH_ERANGE));
    CHECK(m == 5u);
    // The filled prefix holds two distinct artifacts of the cell.
    CHECK(small[0].key.publish_seq != small[1].key.publish_seq);
    CHECK(small[0].actor_id >= 1u && small[0].actor_id <= 5u);
    CHECK(small[1].actor_id >= 1u && small[1].actor_id <= 5u);

    // Retry at the reported size succeeds and returns everything.
    kith_sim_artifact_t full[5] = {0};
    size_t n = 0u;
    CHECK(kith_sim_snapshot_cell(sim, &k, full, m, &n) == 0);
    CHECK(n == m);

    // The count-probe form is unaffected by the truncation contract.
    size_t total = 0u;
    CHECK(kith_sim_snapshot_cell(sim, &k, nullptr, 0u, &total) == 0);
    CHECK(total == 5u);

    kith_sim_destroy(sim);
    return failures;
}

// snapshot_zone_cells returns one product header per cell in a zone at a
// given lod, with actor_count and latest_publish_seq reflecting the
// artifacts published.
static int test_snapshot_zone_cells(void)
{
    int failures = 0;
    kith_sim_t *sim = nullptr;
    CHECK(kith_sim_create(nullptr, nullptr, &sim) == 0);

    // Two cells in zone 6 at lod 0, plus one cell at lod 1 (filtered out).
    // Use distinct actors to avoid supersession (which moves an actor
    // from one cell to another and skews the counts).
    kith_sim_artifact_key_t k_a = make_key(6u, 0, 0, 0, 0u, 1u);
    kith_sim_artifact_key_t k_b = make_key(6u, 1, 0, 0, 0u, 1u);
    kith_sim_artifact_key_t k_c = make_key(6u, 0, 0, 0, 1u, 1u);
    kith_sim_actor_t a1 = make_actor(1u, 0, 0);
    kith_sim_actor_t a2 = make_actor(2u, 0, 0);
    kith_sim_actor_t a3 = make_actor(3u, 0, 0);
    kith_sim_actor_t a4 = make_actor(4u, 0, 0);
    CHECK(kith_sim_publish_artifact(sim, &k_a, &a1, nullptr) == 0);
    CHECK(kith_sim_publish_artifact(sim, &k_a, &a2, nullptr) == 0);
    CHECK(kith_sim_publish_artifact(sim, &k_b, &a3, nullptr) == 0);
    CHECK(kith_sim_publish_artifact(sim, &k_c, &a4, nullptr) == 0);

    kith_sim_cell_product_t products[8] = {0};
    size_t n = 0u;
    CHECK(kith_sim_snapshot_zone_cells(sim, 6u, 0u, products, 8u, &n) == 0);
    CHECK(n == 2u);

    // Find each cell by its coordinates (the order is hash-bucket order,
    // not insertion order, so locate by content).
    const kith_sim_cell_product_t *pa = nullptr;
    const kith_sim_cell_product_t *pb = nullptr;
    for (size_t i = 0u; i < n; ++i)
    {
        if (products[i].cell_x == 0 && products[i].cell_y == 0)
        {
            pa = &products[i];
        }
        else if (products[i].cell_x == 1 && products[i].cell_y == 0)
        {
            pb = &products[i];
        }
    }
    CHECK(pa != nullptr);
    CHECK(pb != nullptr);
    if (pa != nullptr)
    {
        CHECK(pa->actor_count == 2u);
        CHECK(pa->latest_publish_seq == 2u);
    }
    if (pb != nullptr)
    {
        CHECK(pb->actor_count == 1u);
        CHECK(pb->latest_publish_seq == 1u);
    }

    // lod 1 has exactly one cell.
    kith_sim_cell_product_t lod1[8] = {0};
    size_t n1 = 0u;
    CHECK(kith_sim_snapshot_zone_cells(sim, 6u, 1u, lod1, 8u, &n1) == 0);
    CHECK(n1 == 1u);
    CHECK(lod1[0].actor_count == 1u);

    // A zone with no artifacts reports zero cells.
    size_t n_empty = 99u;
    CHECK(kith_sim_snapshot_zone_cells(sim, 99u, 0u, nullptr, 0u, &n_empty) == 0);
    CHECK(n_empty == 0u);

    kith_sim_destroy(sim);
    return failures;
}

// cell_product returns the header for one cell. An empty cell returns
// ENOENT.
static int test_cell_product(void)
{
    int failures = 0;
    kith_sim_t *sim = nullptr;
    CHECK(kith_sim_create(nullptr, nullptr, &sim) == 0);

    kith_sim_artifact_key_t k = make_key(7u, 2, 3, 0, 0u, 5u);
    kith_sim_actor_t a = make_actor(1u, 0, 0);
    CHECK(kith_sim_publish_artifact(sim, &k, &a, nullptr) == 0);

    kith_sim_cell_product_t p = {0};
    CHECK(kith_sim_cell_product(sim, &k, &p) == 0);
    CHECK(p.zone == 7u);
    CHECK(p.cell_x == 2);
    CHECK(p.cell_y == 3);
    CHECK(p.actor_count == 1u);
    CHECK(p.latest_publish_seq == 1u);
    CHECK(p.authority_epoch == 5u);

    // Empty cell -> ENOENT.
    kith_sim_artifact_key_t k_empty = make_key(7u, 99, 99, 0, 0u, 1u);
    CHECK(kith_sim_cell_product(sim, &k_empty, &p) == kith_error_return(KITH_ENOENT));

    kith_sim_destroy(sim);
    return failures;
}

// NULL-handle and NULL-key validation: every public artifact entry point
// rejects a NULL sim or key with EINVAL. artifact_count on NULL returns 0.
static int test_arg_validation(void)
{
    int failures = 0;
    kith_sim_t *sim = nullptr;
    CHECK(kith_sim_create(nullptr, nullptr, &sim) == 0);

    kith_sim_artifact_key_t k = make_key(1u, 0, 0, 0, 0u, 1u);
    kith_sim_actor_t a = make_actor(1u, 0, 0);

    CHECK(kith_sim_publish_artifact(nullptr, &k, &a, nullptr) == kith_error_return(KITH_EINVAL));
    CHECK(kith_sim_publish_artifact(sim, nullptr, &a, nullptr) == kith_error_return(KITH_EINVAL));
    CHECK(kith_sim_publish_artifact(sim, &k, nullptr, nullptr) == kith_error_return(KITH_EINVAL));
    CHECK(kith_sim_remove_artifact(nullptr, 1u) == kith_error_return(KITH_EINVAL));
    CHECK(kith_sim_remove_zone(nullptr, 1u) == kith_error_return(KITH_EINVAL));
    CHECK(kith_sim_snapshot_cell(nullptr, &k, nullptr, 0u, nullptr) ==
          kith_error_return(KITH_EINVAL));
    CHECK(kith_sim_snapshot_cell(sim, nullptr, nullptr, 0u, nullptr) ==
          kith_error_return(KITH_EINVAL));
    CHECK(kith_sim_snapshot_zone_cells(nullptr, 1u, 0u, nullptr, 0u, nullptr) ==
          kith_error_return(KITH_EINVAL));
    CHECK(kith_sim_cell_product(nullptr, &k, nullptr) == kith_error_return(KITH_EINVAL));
    CHECK(kith_sim_cell_product(sim, nullptr, nullptr) == kith_error_return(KITH_EINVAL));
    CHECK(kith_sim_cell_product(sim, &k, nullptr) == kith_error_return(KITH_EINVAL));
    CHECK(kith_sim_artifact_count(nullptr) == 0u);

    kith_sim_destroy(sim);
    return failures;
}

// The artifact store grows its per-shard tables on a 3/4 load-factor
// threshold instead of failing with ENOMEM when a fixed cap fills. A tiny
// initial bucket count exercises the grow/rehash path: publishing more
// distinct actors and cells than the initial capacity holds succeeds, and
// movement churn that leaves cell tombstones does not exhaust the cell
// table before the next grow rehashes the tombstones away.
static int test_grow_rehash(void)
{
    int failures = 0;
    kith_sim_params_t p = {
        .size = sizeof(p),
        .abi_version = KITH_ABI_VERSION,
        .artifact_bucket_count = 16u,
    };
    kith_sim_t *sim = nullptr;
    CHECK(kith_sim_create(&p, nullptr, &sim) == 0);

    // Publish 64 distinct actors in 64 distinct cells in one zone (all in
    // one shard). The initial 16-bucket shard must grow several times; a
    // fixed-cap store returns ENOMEM once the cell table fills.
    const uint32_t zone = 1u;
    const size_t n = 64u;
    for (size_t i = 0u; i < n; ++i)
    {
        kith_sim_artifact_key_t k = make_key(zone, (int32_t)i, 0, 0, 0u, 1u);
        kith_sim_actor_t a = make_actor((uint64_t)(i + 1u), (int64_t)i << KITH_SIM_FIX_SHIFT, 0);
        CHECK(kith_sim_publish_artifact(sim, &k, &a, nullptr) == 0);
    }
    CHECK(kith_sim_artifact_count(sim) == n);

    // Every cell is retrievable and holds exactly one artifact.
    for (size_t i = 0u; i < n; ++i)
    {
        kith_sim_artifact_key_t k = make_key(zone, (int32_t)i, 0, 0, 0u, 1u);
        size_t cnt = 99u;
        CHECK(kith_sim_snapshot_cell(sim, &k, nullptr, 0u, &cnt) == 0);
        CHECK(cnt == 1u);
    }

    // Move half the actors to fresh cells, leaving tombstones in their old
    // cells. The total count is unchanged (moves, not inserts).
    for (size_t i = 0u; i < n / 2u; ++i)
    {
        kith_sim_artifact_key_t k = make_key(zone, (int32_t)i, 1, 0, 0u, 1u);
        kith_sim_actor_t a = make_actor((uint64_t)(i + 1u), 0, 0);
        CHECK(kith_sim_publish_artifact(sim, &k, &a, nullptr) == 0);
    }
    CHECK(kith_sim_artifact_count(sim) == n);

    // Publish a second wave of new actors into new cells to drive another
    // grow while tombstones are present. The grow rehashes the cell table
    // and drops tombstones; no ENOMEM and the count doubles.
    for (size_t i = 0u; i < n; ++i)
    {
        kith_sim_artifact_key_t k = make_key(zone, (int32_t)i, 2, 0, 0u, 1u);
        kith_sim_actor_t a = make_actor((uint64_t)(n + i + 1u), 0, 0);
        CHECK(kith_sim_publish_artifact(sim, &k, &a, nullptr) == 0);
    }
    CHECK(kith_sim_artifact_count(sim) == 2u * n);

    kith_sim_destroy(sim);
    return failures;
}

// remove_actor followed by a republish of the same actor must not leak the
// actor hash slot: the republish reclaims the exact tombstone the remove
// created, so the shard's actor table stays constant-size across many churn
// cycles. A stale live entry per remove+republish cycle accumulates under
// movement churn, and the grower turns that into unbounded memory growth.
static int test_remove_republish_no_leak(void)
{
    int failures = 0;
    kith_sim_params_t p = {
        .size = sizeof(p),
        .abi_version = KITH_ABI_VERSION,
        .artifact_bucket_count = 256u,
    };
    kith_sim_t *sim = nullptr;
    CHECK(kith_sim_create(&p, nullptr, &sim) == 0);

    const uint32_t zone = 1u;
    const uint64_t actor = 42u;
    // Many more churn cycles than the initial bucket count: if each cycle
    // leaked a slot, the shard grows repeatedly to keep up.
    const size_t cycles = 2000u;
    for (size_t i = 0u; i < cycles; ++i)
    {
        kith_sim_artifact_key_t k = make_key(zone, (int32_t)(i % 32), 0, 0, 0u, 1u);
        kith_sim_actor_t a = make_actor(actor, (int64_t)i << KITH_SIM_FIX_SHIFT, 0);
        CHECK(kith_sim_publish_artifact(sim, &k, &a, nullptr) == 0);
        CHECK(kith_sim_remove_artifact(sim, actor) == 0);
    }
    // Net zero: every publish was paired with a remove.
    CHECK(kith_sim_artifact_count(sim) == 0u);

    // A final publish leaves exactly one artifact, and it is retrievable.
    kith_sim_artifact_key_t kf = make_key(zone, 7, 0, 0, 0u, 1u);
    kith_sim_actor_t af = make_actor(actor, 0, 0);
    CHECK(kith_sim_publish_artifact(sim, &kf, &af, nullptr) == 0);
    CHECK(kith_sim_artifact_count(sim) == 1u);
    size_t nf = 99u;
    CHECK(kith_sim_snapshot_cell(sim, &kf, nullptr, 0u, &nf) == 0);
    CHECK(nf == 1u);

    kith_sim_destroy(sim);
    return failures;
}

// The per-cell member index must survive a swap-remove whose moved artifact
// lives in a DIFFERENT cell than the removed one. Two cells A and B each hold
// two actors; removing an actor from A swaps B's last actor into the vacated
// dense slot, and B's member entry for that actor must be fixed to its new
// index or B's snapshot misses it. A subsequent cross-cell move and
// another remove exercise the same fixup paths together.
static int test_member_index_swap_remove_cross_cell(void)
{
    int failures = 0;
    kith_sim_t *sim = nullptr;
    CHECK(kith_sim_create(nullptr, nullptr, &sim) == 0);

    kith_sim_artifact_key_t k_a = make_key(2u, 0, 0, 0, 0u, 1u);
    kith_sim_artifact_key_t k_b = make_key(2u, 1, 0, 0, 0u, 1u);
    kith_sim_actor_t a1 = make_actor(101u, 0, 0);
    kith_sim_actor_t a2 = make_actor(102u, 0, 0);
    kith_sim_actor_t a3 = make_actor(103u, 0, 0);
    kith_sim_actor_t a4 = make_actor(104u, 0, 0);
    CHECK(kith_sim_publish_artifact(sim, &k_a, &a1, nullptr) == 0);
    CHECK(kith_sim_publish_artifact(sim, &k_a, &a2, nullptr) == 0);
    CHECK(kith_sim_publish_artifact(sim, &k_b, &a3, nullptr) == 0);
    CHECK(kith_sim_publish_artifact(sim, &k_b, &a4, nullptr) == 0);
    CHECK(kith_sim_artifact_count(sim) == 4u);

    kith_sim_artifact_t out[8] = {0};
    size_t n = 0u;
    CHECK(kith_sim_snapshot_cell(sim, &k_a, out, 8u, &n) == 0);
    CHECK(n == 2u);
    CHECK(has_actor(out, n, 101u));
    CHECK(has_actor(out, n, 102u));
    CHECK(kith_sim_snapshot_cell(sim, &k_b, out, 8u, &n) == 0);
    CHECK(n == 2u);
    CHECK(has_actor(out, n, 103u));
    CHECK(has_actor(out, n, 104u));

    // Remove a1 from A. The dense swap moves a4 (B's last actor) into a1's
    // old slot; B's member index must still find a4 there.
    CHECK(kith_sim_remove_artifact(sim, 101u) == 0);
    CHECK(kith_sim_artifact_count(sim) == 3u);
    CHECK(kith_sim_snapshot_cell(sim, &k_a, out, 8u, &n) == 0);
    CHECK(n == 1u);
    CHECK(has_actor(out, n, 102u));
    CHECK(kith_sim_snapshot_cell(sim, &k_b, out, 8u, &n) == 0);
    CHECK(n == 2u);
    CHECK(has_actor(out, n, 103u));
    CHECK(has_actor(out, n, 104u));

    // Move a2 from A to B, then remove a4 (still in B) so the swap-remove
    // fixup runs again within B after the move relinked a2 into B.
    kith_sim_actor_t a2b = make_actor(102u, 1, 0);
    CHECK(kith_sim_publish_artifact(sim, &k_b, &a2b, nullptr) == 0);
    CHECK(kith_sim_snapshot_cell(sim, &k_a, out, 8u, &n) == 0);
    CHECK(n == 0u);
    CHECK(kith_sim_snapshot_cell(sim, &k_b, out, 8u, &n) == 0);
    CHECK(n == 3u);
    CHECK(has_actor(out, n, 103u));
    CHECK(has_actor(out, n, 104u));
    CHECK(has_actor(out, n, 102u));

    CHECK(kith_sim_remove_artifact(sim, 104u) == 0);
    CHECK(kith_sim_artifact_count(sim) == 2u);
    CHECK(kith_sim_snapshot_cell(sim, &k_b, out, 8u, &n) == 0);
    CHECK(n == 2u);
    CHECK(has_actor(out, n, 103u));
    CHECK(has_actor(out, n, 102u));

    kith_sim_destroy(sim);
    return failures;
}

// A cross-cell publish relocates the actor's artifact atomically under the
// store lock: a concurrent observer summing the zone's per-cell occupancies
// in one locked snapshot must always count exactly one artifact for the
// churning actor — never zero (the absence window a remove+republish
// pairing opens between its two calls) and never two (the duplicate a
// non-atomic move leaves behind).
struct relocation_state
{
    kith_sim_t *sim;
    _Atomic bool stop;
    _Atomic long violations;
};

static void *relocation_writer(void *arg)
{
    struct relocation_state *st = arg;
    kith_sim_artifact_key_t k_a = make_key(9u, 0, 0, 0, 0u, 1u);
    kith_sim_artifact_key_t k_b = make_key(9u, 1, 0, 0, 0u, 1u);
    kith_sim_actor_t a = make_actor(77u, 0, 0);
    size_t i = 0u;
    while (!atomic_load_explicit(&st->stop, memory_order_relaxed))
    {
        kith_sim_artifact_key_t const *k = (i % 2u == 0u) ? &k_a : &k_b;
        if (kith_sim_publish_artifact(st->sim, k, &a, nullptr) != 0)
        {
            atomic_fetch_add(&st->violations, 1L);
        }
        i += 1u;
    }
    return nullptr;
}

static void *relocation_reader(void *arg)
{
    struct relocation_state *st = arg;
    kith_sim_cell_product_t cells[8] = {0};
    while (!atomic_load_explicit(&st->stop, memory_order_relaxed))
    {
        size_t n = 0u;
        if (kith_sim_snapshot_zone_cells(st->sim, 9u, 0u, cells, 8u, &n) != 0)
        {
            atomic_fetch_add(&st->violations, 1L);
            continue;
        }
        uint64_t occupancy = 0u;
        for (size_t i = 0u; i < n; ++i)
        {
            occupancy += cells[i].actor_count;
        }
        if (occupancy != 1u)
        {
            atomic_fetch_add(&st->violations, 1L);
        }
    }
    return nullptr;
}

static int test_publish_relocation_never_absent(void)
{
    int failures = 0;
    kith_sim_t *sim = nullptr;
    CHECK(kith_sim_create(nullptr, nullptr, &sim) == 0);

    kith_sim_artifact_key_t k_a = make_key(9u, 0, 0, 0, 0u, 1u);
    kith_sim_actor_t a = make_actor(77u, 0, 0);
    CHECK(kith_sim_publish_artifact(sim, &k_a, &a, nullptr) == 0);

    struct relocation_state st = {0};
    st.sim = sim;
    atomic_init(&st.stop, false);
    atomic_init(&st.violations, 0L);

    pthread_t writer;
    pthread_t reader;
    CHECK(pthread_create(&writer, nullptr, relocation_writer, &st) == 0);
    CHECK(pthread_create(&reader, nullptr, relocation_reader, &st) == 0);
    struct timespec spin = {0, 50'000'000L};
    (void)thrd_sleep(&spin, nullptr);
    atomic_store_explicit(&st.stop, true, memory_order_relaxed);
    CHECK(pthread_join(writer, nullptr) == 0);
    CHECK(pthread_join(reader, nullptr) == 0);

    CHECK(atomic_load(&st.violations) == 0L);
    CHECK(kith_sim_artifact_count(sim) == 1u);

    kith_sim_destroy(sim);
    return failures;
}

int main(void)
{
    int rc = 0;
    rc |= test_arg_validation();
    rc |= test_publish_basic();
    rc |= test_publish_update_seq_carried();
    rc |= test_publish_supersession_in_place();
    rc |= test_publish_moves_cell();
    rc |= test_remove_actor();
    rc |= test_remove_zone();
    rc |= test_snapshot_cell_ordering();
    rc |= test_snapshot_truncation_reports_needed_size();
    rc |= test_snapshot_zone_cells();
    rc |= test_cell_product();
    rc |= test_grow_rehash();
    rc |= test_remove_republish_no_leak();
    rc |= test_member_index_swap_remove_cross_cell();
    rc |= test_publish_relocation_never_absent();
    if (rc != 0)
    {
        (void)fprintf(stderr, "sim artifact tests FAILED\n");
    }
    return rc;
}
