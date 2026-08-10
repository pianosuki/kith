#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include "kith/fabric/fabric.h"
#include "kith/sim/sim.h"
#include "kith/types.h"
#include "kith/version.h"

static int check_cond(bool ok, int line)
{
    if (!ok)
    {
        (void)fprintf(stderr, "fabric create: assertion at line %d failed\n", line);
        return 1;
    }
    return 0;
}

#define CHECK(cond) failures += check_cond((cond), __LINE__)

// A fabric handle requires a borrowed sim handle; passing NULL for the sim
// is rejected with EINVAL. Passing NULL for the out slot is also EINVAL.
static int test_create_arg_validation(void)
{
    int failures = 0;
    kith_sim_t *sim = nullptr;
    CHECK(kith_sim_create(nullptr, nullptr, &sim) == 0);

    kith_fabric_t *f = nullptr;
    CHECK(kith_fabric_create(nullptr, nullptr, nullptr, &f) == kith_error_return(KITH_EINVAL));
    CHECK(kith_fabric_create(nullptr, sim, nullptr, nullptr) == kith_error_return(KITH_EINVAL));

    // NULL params selects defaults; the handle builds and the stream is empty.
    CHECK(kith_fabric_create(nullptr, sim, nullptr, &f) == 0);
    CHECK(f != nullptr);
    CHECK(kith_fabric_product_count(f) == 0u);

    kith_fabric_destroy(f);
    kith_fabric_destroy(nullptr);

    kith_sim_destroy(sim);
    return failures;
}

// Params are size-versioned: an incompatible abi_version or an undersized
// size is rejected. The caller fills size and abi_version at its compile
// time; the runtime checks them at create.
static int test_create_params_validation(void)
{
    int failures = 0;
    kith_sim_t *sim = nullptr;
    CHECK(kith_sim_create(nullptr, nullptr, &sim) == 0);

    kith_fabric_params_t params = {0};
    params.size = sizeof(params);
    params.abi_version = KITH_ABI_VERSION;
    params.cell_bucket_count = 0u;

    kith_fabric_t *f = nullptr;
    CHECK(kith_fabric_create(&params, sim, nullptr, &f) == 0);
    CHECK(f != nullptr);
    kith_fabric_destroy(f);

    // Wrong abi_version.
    params.abi_version = KITH_ABI_VERSION
    +1u;
    f = nullptr;
    CHECK(kith_fabric_create(&params, sim, nullptr, &f) == kith_error_return(KITH_EABIVER));
    CHECK(f == nullptr);

    // Undersized size.
    params.abi_version = KITH_ABI_VERSION;
    params.size = sizeof(params) - 1u;
    f = nullptr;
    CHECK(kith_fabric_create(&params, sim, nullptr, &f) == kith_error_return(KITH_ESIZE));
    CHECK(f == nullptr);

    kith_sim_destroy(sim);
    return failures;
}

// A small bucket count is rounded up to the next power of two with a floor,
// so a fabric with a tiny bucket count still publishes without overflowing
// the shard array (the cell store relies on power-of-two masking).
static int test_create_small_bucket_count(void)
{
    int failures = 0;
    kith_sim_t *sim = nullptr;
    CHECK(kith_sim_create(nullptr, nullptr, &sim) == 0);

    kith_fabric_params_t params = {0};
    params.size = sizeof(params);
    params.abi_version = KITH_ABI_VERSION;
    params.cell_bucket_count = 1u;
    kith_fabric_t *f = nullptr;
    CHECK(kith_fabric_create(&params, sim, nullptr, &f) == 0);

    // Publish several cells into one zone; the shard mask handles the
    // sub-bucket probing regardless of the small bucket count.
    for (int32_t cx = 0; cx < 8; ++cx)
    {
        kith_fabric_cell_key_t k = {0};
        k.zone = 1u;
        k.cell_x = cx;
        CHECK(kith_fabric_publish(f, &k, 1u, nullptr) == 0);
    }
    CHECK(kith_fabric_product_count(f) == 8u);

    kith_fabric_destroy(f);
    kith_sim_destroy(sim);
    return failures;
}

// Every public entry point rejects a NULL fabric (or NULL sub / key where
// the function takes one) with EINVAL. product_count and subscription_size
// on NULL return 0.
static int test_public_arg_validation(void)
{
    int failures = 0;
    kith_sim_t *sim = nullptr;
    CHECK(kith_sim_create(nullptr, nullptr, &sim) == 0);
    kith_fabric_t *f = nullptr;
    CHECK(kith_fabric_create(nullptr, sim, nullptr, &f) == 0);

    kith_fabric_cell_key_t k = {0};
    k.zone = 1u;

    CHECK(kith_fabric_publish(nullptr, &k, 1u, nullptr) == kith_error_return(KITH_EINVAL));
    CHECK(kith_fabric_publish(f, nullptr, 1u, nullptr) == kith_error_return(KITH_EINVAL));
    CHECK(kith_fabric_remove_cell(nullptr, &k) == kith_error_return(KITH_EINVAL));
    CHECK(kith_fabric_remove_cell(f, nullptr) == kith_error_return(KITH_EINVAL));
    CHECK(kith_fabric_remove_zone(nullptr, 1u) == kith_error_return(KITH_EINVAL));
    CHECK(kith_fabric_snapshot_cell(nullptr, &k, KITH_FABRIC_LEVEL_FULL, nullptr, 0u, nullptr) ==
          kith_error_return(KITH_EINVAL));
    CHECK(kith_fabric_snapshot_cell(f, nullptr, KITH_FABRIC_LEVEL_FULL, nullptr, 0u, nullptr) ==
          kith_error_return(KITH_EINVAL));
    CHECK(kith_fabric_snapshot_zone_cells(nullptr, 1u, 0u, nullptr, 0u, nullptr) ==
          kith_error_return(KITH_EINVAL));
    CHECK(kith_fabric_cell_product(nullptr, &k, nullptr) == kith_error_return(KITH_EINVAL));
    CHECK(kith_fabric_cell_product(f, nullptr, nullptr) == kith_error_return(KITH_EINVAL));
    CHECK(kith_fabric_cell_product(f, &k, nullptr) == kith_error_return(KITH_EINVAL));
    CHECK(kith_fabric_product_count(nullptr) == 0u);
    CHECK(kith_fabric_create_subscription(nullptr, nullptr) == kith_error_return(KITH_EINVAL));
    kith_fabric_subscription_t *sub = nullptr;
    CHECK(kith_fabric_create_subscription(f, nullptr) == kith_error_return(KITH_EINVAL));
    CHECK(kith_fabric_create_subscription(nullptr, &sub) == kith_error_return(KITH_EINVAL));
    CHECK(kith_fabric_subscription_add(nullptr, &k) == kith_error_return(KITH_EINVAL));
    CHECK(kith_fabric_subscription_remove(nullptr, &k) == kith_error_return(KITH_EINVAL));
    CHECK(kith_fabric_subscription_size(nullptr) == 0u);
    CHECK(kith_fabric_drain(nullptr, nullptr, nullptr, 0u, nullptr) ==
          kith_error_return(KITH_EINVAL));

    kith_fabric_destroy(f);
    kith_sim_destroy(sim);
    return failures;
}

int main(void)
{
    int rc = 0;
    rc |= test_create_arg_validation();
    rc |= test_create_params_validation();
    rc |= test_create_small_bucket_count();
    rc |= test_public_arg_validation();
    if (rc != 0)
    {
        (void)fprintf(stderr, "fabric create tests FAILED\n");
    }
    return rc;
}
