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
        (void)fprintf(stderr, "fabric subscription: assertion at line %d failed\n", line);
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

// A subscription is a per-gateway interest set. Adding a cell grows the
// size; adding the same cell again is idempotent. Removing a cell shrinks
// the size; removing a cell that is not subscribed is idempotent.
static int test_subscription_add_remove(void)
{
    int failures = 0;
    kith_sim_t *sim = nullptr;
    CHECK(kith_sim_create(nullptr, nullptr, &sim) == 0);
    kith_fabric_t *f = nullptr;
    CHECK(kith_fabric_create(nullptr, sim, nullptr, &f) == 0);

    kith_fabric_subscription_t *sub = nullptr;
    CHECK(kith_fabric_create_subscription(f, &sub) == 0);
    CHECK(sub != nullptr);
    CHECK(kith_fabric_subscription_size(sub) == 0u);

    kith_fabric_cell_key_t k_a = make_key(1u, 0, 0, 0, 0u);
    kith_fabric_cell_key_t k_b = make_key(1u, 1, 0, 0, 0u);
    CHECK(kith_fabric_subscription_add(sub, &k_a) == 0);
    CHECK(kith_fabric_subscription_size(sub) == 1u);
    CHECK(kith_fabric_subscription_add(sub, &k_b) == 0);
    CHECK(kith_fabric_subscription_size(sub) == 2u);

    // Adding the same cell again is idempotent.
    CHECK(kith_fabric_subscription_add(sub, &k_a) == 0);
    CHECK(kith_fabric_subscription_size(sub) == 2u);

    // Removing a cell that is not subscribed is idempotent.
    kith_fabric_cell_key_t k_other = make_key(1u, 99, 99, 0, 0u);
    CHECK(kith_fabric_subscription_remove(sub, &k_other) == 0);
    CHECK(kith_fabric_subscription_size(sub) == 2u);

    CHECK(kith_fabric_subscription_remove(sub, &k_a) == 0);
    CHECK(kith_fabric_subscription_size(sub) == 1u);
    CHECK(kith_fabric_subscription_remove(sub, &k_a) == 0);
    CHECK(kith_fabric_subscription_size(sub) == 1u);

    // Re-adding after removal works (the tombstone is reused, not leaked).
    CHECK(kith_fabric_subscription_add(sub, &k_a) == 0);
    CHECK(kith_fabric_subscription_size(sub) == 2u);

    kith_fabric_subscription_destroy(sub);
    kith_fabric_destroy(f);
    kith_sim_destroy(sim);
    return failures;
}

// When a cell is published, every subscription containing that cell is
// marked pending. drain returns the pending product headers in publish_seq
// order (newest first) and clears the pending flag for the returned cells.
// A second drain with no intervening publish returns zero.
static int test_drain_fanout(void)
{
    int failures = 0;
    kith_sim_t *sim = nullptr;
    CHECK(kith_sim_create(nullptr, nullptr, &sim) == 0);
    kith_fabric_t *f = nullptr;
    CHECK(kith_fabric_create(nullptr, sim, nullptr, &f) == 0);

    kith_fabric_subscription_t *sub = nullptr;
    CHECK(kith_fabric_create_subscription(f, &sub) == 0);
    kith_fabric_cell_key_t k_a = make_key(1u, 0, 0, 0, 0u);
    kith_fabric_cell_key_t k_b = make_key(1u, 1, 0, 0, 0u);
    CHECK(kith_fabric_subscription_add(sub, &k_a) == 0);
    CHECK(kith_fabric_subscription_add(sub, &k_b) == 0);
    // k_other is not subscribed; publishing it must not mark the subscription.
    kith_fabric_cell_key_t k_other = make_key(1u, 9, 9, 0, 0u);

    // Publish k_a twice and k_b once; k_other once (not subscribed).
    uint64_t seq_a1 = 0u;
    CHECK(kith_fabric_publish(f, &k_a, 1u, &seq_a1) == 0);
    CHECK(seq_a1 == 1u);
    uint64_t seq_a2 = 0u;
    CHECK(kith_fabric_publish(f, &k_a, 1u, &seq_a2) == 0);
    CHECK(seq_a2 == 2u);
    uint64_t seq_b = 0u;
    CHECK(kith_fabric_publish(f, &k_b, 1u, &seq_b) == 0);
    CHECK(seq_b == 1u);
    CHECK(kith_fabric_publish(f, &k_other, 1u, nullptr) == 0);

    // Drain returns the two subscribed pending cells (k_a, k_b), newest-first.
    kith_fabric_cell_product_t out[8] = {0};
    size_t n = 0u;
    CHECK(kith_fabric_drain(f, sub, out, 8u, &n) == 0);
    CHECK(n == 2u);
    CHECK(out[0].publish_seq >= out[1].publish_seq);
    // The newer product (k_a, seq 2) sorts before the older (k_b, seq 1).
    CHECK(out[0].key.cell_x == 0);
    CHECK(out[0].publish_seq == 2u);
    CHECK(out[1].key.cell_x == 1);
    CHECK(out[1].publish_seq == 1u);

    // A second drain with no intervening publish returns zero.
    size_t n2 = 99u;
    CHECK(kith_fabric_drain(f, sub, nullptr, 0u, &n2) == 0);
    CHECK(n2 == 0u);

    kith_fabric_subscription_destroy(sub);
    kith_fabric_destroy(f);
    kith_sim_destroy(sim);
    return failures;
}

// Every cell's publish_seq starts at 1, so products from different cells
// can hold equal sequences. The drain orders that tie group by ascending
// cell key, so the returned order is a stated contract rather than an
// artifact of whichever sort the libc ships. Each key field decides one
// adjacent pair in the expected order (zone, cell_x, cell_y, cell_z, lod).
static int test_drain_seq_tie_order(void)
{
    int failures = 0;
    kith_sim_t *sim = nullptr;
    CHECK(kith_sim_create(nullptr, nullptr, &sim) == 0);
    kith_fabric_t *f = nullptr;
    CHECK(kith_fabric_create(nullptr, sim, nullptr, &f) == 0);

    kith_fabric_subscription_t *sub = nullptr;
    CHECK(kith_fabric_create_subscription(f, &sub) == 0);

    kith_fabric_cell_key_t keys[] = {
        make_key(1u, 0, 0, 0, 0u),
        make_key(1u, 0, 0, 0, 2u),
        make_key(1u, -3, 0, 0, 0u),
        make_key(1u, -3, 5, 0, 0u),
        make_key(1u, -3, 5, 7, 0u),
        make_key(2u, 0, 0, 0, 0u),
    };
    for (size_t i = 0u; i < sizeof(keys) / sizeof(keys[0]); ++i)
    {
        CHECK(kith_fabric_subscription_add(sub, &keys[i]) == 0);
        uint64_t seq = 0u;
        CHECK(kith_fabric_publish(f, &keys[i], 1u, &seq) == 0);
        CHECK(seq == 1u);
    }

    // The tie group comes back key-ascending: the signed negative cell_x
    // group first, then lod ascending inside one cell, then the higher zone.
    kith_fabric_cell_product_t out[8] = {0};
    size_t n = 0u;
    CHECK(kith_fabric_drain(f, sub, out, 8u, &n) == 0);
    CHECK(n == 6u);
    static const size_t expected[] = {2u, 3u, 4u, 0u, 1u, 5u};
    for (size_t i = 0u; i < sizeof(expected) / sizeof(expected[0]); ++i)
    {
        CHECK(out[i].publish_seq == 1u);
        CHECK(out[i].key.zone == keys[expected[i]].zone);
        CHECK(out[i].key.cell_x == keys[expected[i]].cell_x);
        CHECK(out[i].key.cell_y == keys[expected[i]].cell_y);
        CHECK(out[i].key.cell_z == keys[expected[i]].cell_z);
        CHECK(out[i].key.lod == keys[expected[i]].lod);
    }

    kith_fabric_subscription_destroy(sub);
    kith_fabric_destroy(f);
    kith_sim_destroy(sim);
    return failures;
}

// Counting mode (out=NULL, max=0) reports the number of pending products
// without clearing them. A subsequent copy-mode drain still receives every
// pending product. Copy-mode drain then clears the pending flag.
static int test_drain_counting_mode(void)
{
    int failures = 0;
    kith_sim_t *sim = nullptr;
    CHECK(kith_sim_create(nullptr, nullptr, &sim) == 0);
    kith_fabric_t *f = nullptr;
    CHECK(kith_fabric_create(nullptr, sim, nullptr, &f) == 0);

    kith_fabric_subscription_t *sub = nullptr;
    CHECK(kith_fabric_create_subscription(f, &sub) == 0);
    kith_fabric_cell_key_t k_a = make_key(2u, 0, 0, 0, 0u);
    kith_fabric_cell_key_t k_b = make_key(2u, 1, 0, 0, 0u);
    CHECK(kith_fabric_subscription_add(sub, &k_a) == 0);
    CHECK(kith_fabric_subscription_add(sub, &k_b) == 0);
    CHECK(kith_fabric_publish(f, &k_a, 1u, nullptr) == 0);
    CHECK(kith_fabric_publish(f, &k_b, 1u, nullptr) == 0);

    // Counting mode does not clear pending.
    size_t total = 99u;
    CHECK(kith_fabric_drain(f, sub, nullptr, 0u, &total) == 0);
    CHECK(total == 2u);
    size_t total2 = 99u;
    CHECK(kith_fabric_drain(f, sub, nullptr, 0u, &total2) == 0);
    CHECK(total2 == 2u);

    // Copy-mode drain receives every pending product and clears them.
    kith_fabric_cell_product_t out[8] = {0};
    size_t n = 0u;
    CHECK(kith_fabric_drain(f, sub, out, 8u, &n) == 0);
    CHECK(n == 2u);

    // After copy-mode drain, counting mode reports zero.
    size_t total3 = 99u;
    CHECK(kith_fabric_drain(f, sub, nullptr, 0u, &total3) == 0);
    CHECK(total3 == 0u);

    kith_fabric_subscription_destroy(sub);
    kith_fabric_destroy(f);
    kith_sim_destroy(sim);
    return failures;
}

// A truncated drain returns the first N pending products (still
// newest-first) and leaves every undelivered cell pending: a counting call
// after the truncated drain reports the remainder, a full drain then
// delivers it, and only then does the queue empty.
static int test_drain_truncated(void)
{
    int failures = 0;
    kith_sim_t *sim = nullptr;
    CHECK(kith_sim_create(nullptr, nullptr, &sim) == 0);
    kith_fabric_t *f = nullptr;
    CHECK(kith_fabric_create(nullptr, sim, nullptr, &f) == 0);

    kith_fabric_subscription_t *sub = nullptr;
    CHECK(kith_fabric_create_subscription(f, &sub) == 0);
    kith_fabric_cell_key_t k_a = make_key(3u, 0, 0, 0, 0u);
    kith_fabric_cell_key_t k_b = make_key(3u, 1, 0, 0, 0u);
    CHECK(kith_fabric_subscription_add(sub, &k_a) == 0);
    CHECK(kith_fabric_subscription_add(sub, &k_b) == 0);
    CHECK(kith_fabric_publish(f, &k_a, 1u, nullptr) == 0);
    CHECK(kith_fabric_publish(f, &k_b, 1u, nullptr) == 0);

    // Truncated copy-mode drain: one slot, two pending.
    kith_fabric_cell_product_t out[1] = {0};
    size_t n = 0u;
    CHECK(kith_fabric_drain(f, sub, out, 1u, &n) == 0);
    CHECK(n == 1u);

    // The undelivered cell stays pending: counting mode reports one.
    size_t remaining = 99u;
    CHECK(kith_fabric_drain(f, sub, nullptr, 0u, &remaining) == 0);
    CHECK(remaining == 1u);

    // The next drain delivers the remainder.
    kith_fabric_cell_product_t out2[1] = {0};
    size_t n2 = 0u;
    CHECK(kith_fabric_drain(f, sub, out2, 1u, &n2) == 0);
    CHECK(n2 == 1u);
    CHECK(out2[0].key.cell_x != out[0].key.cell_x);

    // The queue is now empty.
    size_t total = 99u;
    CHECK(kith_fabric_drain(f, sub, nullptr, 0u, &total) == 0);
    CHECK(total == 0u);

    kith_fabric_subscription_destroy(sub);
    kith_fabric_destroy(f);
    kith_sim_destroy(sim);
    return failures;
}

// Fanout is per-subscription: two subscriptions on the same fabric, one
// subscribed to a published cell and one not, receive independent pending
// sets. Destroying a subscription removes it from the fabric's table; the
// fabric does not fan out to a destroyed subscription.
static int test_drain_multiple_subscriptions(void)
{
    int failures = 0;
    kith_sim_t *sim = nullptr;
    CHECK(kith_sim_create(nullptr, nullptr, &sim) == 0);
    kith_fabric_t *f = nullptr;
    CHECK(kith_fabric_create(nullptr, sim, nullptr, &f) == 0);

    kith_fabric_subscription_t *sub_a = nullptr;
    kith_fabric_subscription_t *sub_b = nullptr;
    CHECK(kith_fabric_create_subscription(f, &sub_a) == 0);
    CHECK(kith_fabric_create_subscription(f, &sub_b) == 0);

    kith_fabric_cell_key_t k = make_key(4u, 0, 0, 0, 0u);
    CHECK(kith_fabric_subscription_add(sub_a, &k) == 0);
    // sub_b is intentionally empty: publish must not mark it pending.

    CHECK(kith_fabric_publish(f, &k, 1u, nullptr) == 0);

    kith_fabric_cell_product_t out_a[4] = {0};
    size_t n_a = 99u;
    CHECK(kith_fabric_drain(f, sub_a, out_a, 4u, &n_a) == 0);
    CHECK(n_a == 1u);

    kith_fabric_cell_product_t out_b[4] = {0};
    size_t n_b = 99u;
    CHECK(kith_fabric_drain(f, sub_b, out_b, 4u, &n_b) == 0);
    CHECK(n_b == 0u);

    // After destroying sub_a, the fabric holds no reference to it: a fresh
    // subscription on the same cell receives pending from a new publish.
    kith_fabric_subscription_destroy(sub_a);
    kith_fabric_subscription_destroy(sub_b);

    kith_fabric_subscription_t *sub_c = nullptr;
    CHECK(kith_fabric_create_subscription(f, &sub_c) == 0);
    CHECK(kith_fabric_subscription_add(sub_c, &k) == 0);
    CHECK(kith_fabric_publish(f, &k, 1u, nullptr) == 0);
    kith_fabric_cell_product_t out_c[4] = {0};
    size_t n_c = 99u;
    CHECK(kith_fabric_drain(f, sub_c, out_c, 4u, &n_c) == 0);
    CHECK(n_c == 1u);
    kith_fabric_subscription_destroy(sub_c);

    kith_fabric_destroy(f);
    kith_sim_destroy(sim);
    return failures;
}

// A cell that was subscribed, published, then removed from the stream is
// still pending on the subscription. drain clears the pending flag but
// returns no header for it (the cell product is gone). Counting mode
// reports the pending cell; copy-mode drain reports only the cells that
// still exist.
static int test_drain_removed_cell(void)
{
    int failures = 0;
    kith_sim_t *sim = nullptr;
    CHECK(kith_sim_create(nullptr, nullptr, &sim) == 0);
    kith_fabric_t *f = nullptr;
    CHECK(kith_fabric_create(nullptr, sim, nullptr, &f) == 0);

    kith_fabric_subscription_t *sub = nullptr;
    CHECK(kith_fabric_create_subscription(f, &sub) == 0);
    kith_fabric_cell_key_t k = make_key(5u, 0, 0, 0, 0u);
    CHECK(kith_fabric_subscription_add(sub, &k) == 0);
    CHECK(kith_fabric_publish(f, &k, 1u, nullptr) == 0);
    CHECK(kith_fabric_remove_cell(f, &k) == 0);

    // Counting mode: the pending flag is still set.
    size_t total = 99u;
    CHECK(kith_fabric_drain(f, sub, nullptr, 0u, &total) == 0);
    CHECK(total == 1u);

    // Copy mode: pending is cleared, but no header is returned (cell gone).
    kith_fabric_cell_product_t out[4] = {0};
    size_t n = 99u;
    CHECK(kith_fabric_drain(f, sub, out, 4u, &n) == 0);
    CHECK(n == 0u);

    // After drain, the pending flag is cleared.
    size_t total2 = 99u;
    CHECK(kith_fabric_drain(f, sub, nullptr, 0u, &total2) == 0);
    CHECK(total2 == 0u);

    kith_fabric_subscription_destroy(sub);
    kith_fabric_destroy(f);
    kith_sim_destroy(sim);
    return failures;
}

int main(void)
{
    int rc = 0;
    rc |= test_subscription_add_remove();
    rc |= test_drain_fanout();
    rc |= test_drain_seq_tie_order();
    rc |= test_drain_counting_mode();
    rc |= test_drain_truncated();
    rc |= test_drain_multiple_subscriptions();
    rc |= test_drain_removed_cell();
    if (rc != 0)
    {
        (void)fprintf(stderr, "fabric subscription tests FAILED\n");
    }
    return rc;
}
