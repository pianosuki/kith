#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include "kith/coord/coord.h"
#include "kith/fabric/fabric.h"
#include "kith/types.h"
#include "kith/version.h"

static int check_cond(bool ok, int line)
{
    if (!ok)
    {
        (void)fprintf(stderr, "coord create: assertion at line %d failed\n", line);
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

// A NULL out slot is rejected with EINVAL. NULL params selects all defaults
// and builds a working embedded-mode handle (bus NULL). destroy(NULL) is a
// no-op. A non-NULL bus is borrowed and not freed by coord_destroy.
static int test_create_arg_validation(void)
{
    int failures = 0;
    CHECK(kith_coord_create(nullptr, nullptr, nullptr, nullptr) == kith_error_return(KITH_EINVAL));

    kith_coord_t *c = nullptr;
    CHECK(kith_coord_create(nullptr, nullptr, nullptr, &c) == 0);
    CHECK(c != nullptr);
    CHECK(kith_coord_cell_count(c) == 0u);

    kith_coord_destroy(c);
    kith_coord_destroy(nullptr);

    // With a borrowed bus, the coord holds it but does not own it.
    kith_coord_bus_t *bus = nullptr;
    CHECK(kith_coord_bus_create(nullptr, nullptr, &bus) == 0);
    CHECK(bus != nullptr);

    c = nullptr;
    CHECK(kith_coord_create(nullptr, bus, nullptr, &c) == 0);
    CHECK(c != nullptr);
    kith_coord_destroy(c);
    kith_coord_bus_destroy(bus);
    return failures;
}

// Params are size-versioned: an incompatible abi_version is rejected with
// EABIVER and an undersized size with ESIZE. A params struct with every
// field zero selects the defaults and builds a working handle.
static int test_create_params_validation(void)
{
    int failures = 0;
    kith_coord_params_t params = {0};
    params.size = sizeof(params);
    params.abi_version = KITH_ABI_VERSION;

    kith_coord_t *c = nullptr;
    CHECK(kith_coord_create(&params, nullptr, nullptr, &c) == 0);
    CHECK(c != nullptr);
    kith_coord_destroy(c);

    // Wrong abi_version.
    params.abi_version = KITH_ABI_VERSION
    +1u;
    c = nullptr;
    CHECK(kith_coord_create(&params, nullptr, nullptr, &c) == kith_error_return(KITH_EABIVER));
    CHECK(c == nullptr);

    // Undersized size.
    params.abi_version = KITH_ABI_VERSION;
    params.size = sizeof(params) - 1u;
    c = nullptr;
    CHECK(kith_coord_create(&params, nullptr, nullptr, &c) == kith_error_return(KITH_ESIZE));
    CHECK(c == nullptr);

    // Bus params are size-versioned too.
    kith_coord_bus_params_t bparams = {0};
    bparams.size = sizeof(bparams);
    bparams.abi_version = KITH_ABI_VERSION;

    kith_coord_bus_t *bus = nullptr;
    CHECK(kith_coord_bus_create(&bparams, nullptr, &bus) == 0);
    CHECK(bus != nullptr);
    kith_coord_bus_destroy(bus);

    bparams.abi_version = KITH_ABI_VERSION
    +1u;
    bus = nullptr;
    CHECK(kith_coord_bus_create(&bparams, nullptr, &bus) == kith_error_return(KITH_EABIVER));
    CHECK(bus == nullptr);

    bparams.abi_version = KITH_ABI_VERSION;
    bparams.size = sizeof(bparams) - 1u;
    bus = nullptr;
    CHECK(kith_coord_bus_create(&bparams, nullptr, &bus) == kith_error_return(KITH_ESIZE));
    CHECK(bus == nullptr);
    return failures;
}

// In embedded mode (bus NULL), every cell's authority is the local
// instance_id (default 0) with epoch 0. A custom instance_id is reflected
// in the hash-fallback authority.
static int test_create_embedded_authority(void)
{
    int failures = 0;
    kith_coord_params_t params = {0};
    params.size = sizeof(params);
    params.abi_version = KITH_ABI_VERSION;
    params.instance_id = 7u;

    kith_coord_t *c = nullptr;
    CHECK(kith_coord_create(&params, nullptr, nullptr, &c) == 0);
    CHECK(c != nullptr);

    kith_fabric_cell_key_t k = make_key(1u, 3, 4, 0, 0u);
    kith_coord_authority_t auth = {0};
    CHECK(kith_coord_authority(c, &k, &auth) == 0);
    CHECK(auth.instance_id == 7u);
    CHECK(auth.authority_epoch == 0u);

    // The bus with a custom instance_id reports it back.
    kith_coord_bus_params_t bparams = {0};
    bparams.size = sizeof(bparams);
    bparams.abi_version = KITH_ABI_VERSION;
    bparams.instance_id = 9u;

    kith_coord_bus_t *bus = nullptr;
    CHECK(kith_coord_bus_create(&bparams, nullptr, &bus) == 0);
    CHECK(kith_coord_bus_instance_id(bus) == 9u);
    CHECK(kith_coord_bus_member_count(bus) == 1u);
    kith_coord_bus_destroy(bus);

    kith_coord_destroy(c);
    return failures;
}

int main(void)
{
    int rc = 0;
    rc |= test_create_arg_validation();
    rc |= test_create_params_validation();
    rc |= test_create_embedded_authority();
    if (rc != 0)
    {
        (void)fprintf(stderr, "coord create tests FAILED\n");
    }
    return rc;
}
