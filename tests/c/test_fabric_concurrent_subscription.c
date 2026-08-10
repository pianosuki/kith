#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <pthread.h>

#include "kith/fabric/fabric.h"
#include "kith/sim/sim.h"
#include "kith/types.h"

/* Concurrent subscription mutation, drain, and publish. The fabric's
 * subscription table and cell store are shared between a reactor-thread
 * drainer and a worker-thread publisher; this test drives all three paths
 * at once and asserts that no thread crashes and the final interest-set
 * size is consistent with the net add/remove balance. */

static int check_cond(bool ok, int line)
{
    if (!ok)
    {
        (void)fprintf(stderr, "fabric concurrent: assertion at line %d failed\n", line);
        return 1;
    }
    return 0;
}

#define CHECK(cond) failures += check_cond((cond), __LINE__)

#define CONCURRENT_ITERATIONS 20000u
#define CONCURRENT_CELLS      16u

struct concurrent_state
{
    kith_fabric_t *fabric;
    kith_fabric_subscription_t *sub;
    kith_fabric_cell_key_t keys[CONCURRENT_CELLS];
    bool publisher_adds;
};

static void fill_keys(kith_fabric_cell_key_t *keys)
{
    for (size_t i = 0u; i < CONCURRENT_CELLS; ++i)
    {
        kith_fabric_cell_key_t k = {0};
        k.zone = 1u;
        k.cell_x = (int32_t)i;
        k.cell_y = 0;
        k.cell_z = 0;
        k.lod = 0u;
        keys[i] = k;
    }
}

static void *adder_thread(void *arg)
{
    struct concurrent_state *st = arg;
    for (size_t i = 0u; i < CONCURRENT_ITERATIONS; ++i)
    {
        const kith_fabric_cell_key_t *k = &st->keys[i % CONCURRENT_CELLS];
        (void)kith_fabric_subscription_add(st->sub, k);
        (void)kith_fabric_subscription_remove(st->sub, k);
    }
    return nullptr;
}

static void *drainer_thread(void *arg)
{
    struct concurrent_state *st = arg;
    for (size_t i = 0u; i < CONCURRENT_ITERATIONS; ++i)
    {
        size_t total = 0u;
        (void)kith_fabric_drain(st->fabric, st->sub, nullptr, 0u, &total);
        if ((i & 0x3ffu) == 0u)
        {
            kith_fabric_cell_product_t out[CONCURRENT_CELLS];
            size_t n = 0u;
            (void)kith_fabric_drain(st->fabric, st->sub, out, CONCURRENT_CELLS, &n);
        }
    }
    return nullptr;
}

static void *publisher_thread(void *arg)
{
    struct concurrent_state *st = arg;
    for (size_t i = 0u; i < CONCURRENT_ITERATIONS; ++i)
    {
        const kith_fabric_cell_key_t *k = &st->keys[i % CONCURRENT_CELLS];
        (void)kith_fabric_publish(st->fabric, k, 1u, nullptr);
    }
    return nullptr;
}

static int test_concurrent_add_drain_publish(void)
{
    int failures = 0;
    kith_sim_t *sim = nullptr;
    CHECK(kith_sim_create(nullptr, nullptr, &sim) == 0);
    kith_fabric_t *f = nullptr;
    CHECK(kith_fabric_create(nullptr, sim, nullptr, &f) == 0);

    kith_fabric_subscription_t *sub = nullptr;
    CHECK(kith_fabric_create_subscription(f, &sub) == 0);

    struct concurrent_state st = {0};
    st.fabric = f;
    st.sub = sub;
    fill_keys(st.keys);

    pthread_t adder;
    pthread_t drainer;
    pthread_t publisher;
    int rc = pthread_create(&adder, nullptr, adder_thread, &st);
    CHECK(rc == 0);
    rc = pthread_create(&drainer, nullptr, drainer_thread, &st);
    CHECK(rc == 0);
    rc = pthread_create(&publisher, nullptr, publisher_thread, &st);
    CHECK(rc == 0);

    void *adder_status = nullptr;
    void *drainer_status = nullptr;
    void *publisher_status = nullptr;
    (void)pthread_join(adder, &adder_status);
    (void)pthread_join(drainer, &drainer_status);
    (void)pthread_join(publisher, &publisher_status);
    CHECK(adder_status == nullptr);
    CHECK(drainer_status == nullptr);
    CHECK(publisher_status == nullptr);

    // The adder alternates add then remove on every cell each iteration, so
    // at steady state every cell has been removed last; the net interest set
    // is empty. The size is internally synchronized, so reading it after the
    // joins is consistent.
    CHECK(kith_fabric_subscription_size(sub) == 0u);

    kith_fabric_subscription_destroy(sub);
    kith_fabric_destroy(f);
    kith_sim_destroy(sim);
    return failures;
}

// A second subscription on the same fabric, created and destroyed on one
// thread while another thread publishes and drains the first subscription,
// must not race the subscription table against fanout. The destroyed
// subscription matches every published cell, so fanout contends the destroy
// on the fabric lock for the whole loop instead of one cell in sixteen —
// wider contention overlap over the same serialized interleaving.
static int test_concurrent_subscription_lifecycle(void)
{
    int failures = 0;
    kith_sim_t *sim = nullptr;
    CHECK(kith_sim_create(nullptr, nullptr, &sim) == 0);
    kith_fabric_t *f = nullptr;
    CHECK(kith_fabric_create(nullptr, sim, nullptr, &f) == 0);

    kith_fabric_subscription_t *sub = nullptr;
    CHECK(kith_fabric_create_subscription(f, &sub) == 0);
    kith_fabric_cell_key_t keys[CONCURRENT_CELLS];
    fill_keys(keys);
    for (size_t i = 0u; i < CONCURRENT_CELLS; ++i)
    {
        CHECK(kith_fabric_subscription_add(sub, &keys[i]) == 0);
    }

    struct concurrent_state st = {0};
    st.fabric = f;
    st.sub = sub;
    fill_keys(st.keys);

    pthread_t publisher;
    pthread_t drainer;
    int rc = pthread_create(&publisher, nullptr, publisher_thread, &st);
    CHECK(rc == 0);
    rc = pthread_create(&drainer, nullptr, drainer_thread, &st);
    CHECK(rc == 0);

    for (size_t i = 0u; i < (CONCURRENT_ITERATIONS / 4u); ++i)
    {
        kith_fabric_subscription_t *extra = nullptr;
        CHECK(kith_fabric_create_subscription(f, &extra) == 0);
        for (size_t j = 0u; j < CONCURRENT_CELLS; ++j)
        {
            CHECK(kith_fabric_subscription_add(extra, &keys[j]) == 0);
        }
        kith_fabric_subscription_destroy(extra);
    }

    void *publisher_status = nullptr;
    void *drainer_status = nullptr;
    (void)pthread_join(publisher, &publisher_status);
    (void)pthread_join(drainer, &drainer_status);
    CHECK(publisher_status == nullptr);
    CHECK(drainer_status == nullptr);

    CHECK(kith_fabric_subscription_size(sub) == CONCURRENT_CELLS);

    kith_fabric_subscription_destroy(sub);
    kith_fabric_destroy(f);
    kith_sim_destroy(sim);
    return failures;
}

int main(void)
{
    int rc = 0;
    rc |= test_concurrent_add_drain_publish();
    rc |= test_concurrent_subscription_lifecycle();
    if (rc != 0)
    {
        (void)fprintf(stderr, "fabric concurrent subscription tests FAILED\n");
    }
    return rc;
}
