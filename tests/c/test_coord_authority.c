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
        (void)fprintf(stderr, "coord authority: assertion at line %d failed\n", line);
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

// Every public authority entry point rejects a NULL handle (and NULL key or
// out where the function takes one) with EINVAL. cell_count and
// owned_cell_count on NULL return 0.
static int test_authority_arg_validation(void)
{
    int failures = 0;
    kith_coord_t *c = nullptr;
    CHECK(kith_coord_create(nullptr, nullptr, nullptr, &c) == 0);

    kith_fabric_cell_key_t k = make_key(1u, 0, 0, 0, 0u);
    kith_coord_authority_t auth = {0};

    CHECK(kith_coord_authority(nullptr, &k, &auth) == kith_error_return(KITH_EINVAL));
    CHECK(kith_coord_authority(c, nullptr, &auth) == kith_error_return(KITH_EINVAL));
    CHECK(kith_coord_authority(c, &k, nullptr) == kith_error_return(KITH_EINVAL));

    CHECK(kith_coord_set_authority(nullptr, &k, 1u) == kith_error_return(KITH_EINVAL));
    CHECK(kith_coord_set_authority(c, nullptr, 1u) == kith_error_return(KITH_EINVAL));

    CHECK(kith_coord_clear_authority(nullptr, &k) == kith_error_return(KITH_EINVAL));
    CHECK(kith_coord_clear_authority(c, nullptr) == kith_error_return(KITH_EINVAL));

    CHECK(kith_coord_cell_count(nullptr) == 0u);
    CHECK(kith_coord_owned_cell_count(nullptr, 1u) == 0u);

    CHECK(kith_coord_snapshot_zone(nullptr, 1u, 0u, nullptr, 0u, nullptr) ==
          kith_error_return(KITH_EINVAL));

    kith_coord_destroy(c);
    return failures;
}

// set_authority installs an override and bumps the authority epoch.
// authority returns the override's instance_id and epoch. A second
// set_authority on the same cell replaces the override with a new epoch.
// clear_authority removes the override (revert to hash-fallback) and is
// idempotent on a cell with no override.
static int test_set_clear_authority(void)
{
    int failures = 0;
    kith_coord_t *c = nullptr;
    CHECK(kith_coord_create(nullptr, nullptr, nullptr, &c) == 0);

    kith_fabric_cell_key_t k = make_key(2u, 1, 2, 0, 0u);
    kith_coord_authority_t auth = {0};

    // No override yet: hash-fallback (local instance_id 0, epoch 0).
    CHECK(kith_coord_authority(c, &k, &auth) == 0);
    CHECK(auth.instance_id == 0u);
    CHECK(auth.authority_epoch == 0u);
    CHECK(kith_coord_cell_count(c) == 0u);

    // Set an override to instance 5.
    CHECK(kith_coord_set_authority(c, &k, 5u) == 0);
    CHECK(kith_coord_authority(c, &k, &auth) == 0);
    CHECK(auth.instance_id == 5u);
    CHECK(auth.authority_epoch == 1u);
    CHECK(kith_coord_cell_count(c) == 1u);
    CHECK(kith_coord_owned_cell_count(c, 5u) == 1u);
    CHECK(kith_coord_owned_cell_count(c, 0u) == 0u);

    // Replace the override to instance 6; epoch bumps again.
    CHECK(kith_coord_set_authority(c, &k, 6u) == 0);
    CHECK(kith_coord_authority(c, &k, &auth) == 0);
    CHECK(auth.instance_id == 6u);
    CHECK(auth.authority_epoch == 2u);
    CHECK(kith_coord_cell_count(c) == 1u);
    CHECK(kith_coord_owned_cell_count(c, 6u) == 1u);

    // Clear the override; authority reverts to hash-fallback with epoch 0.
    CHECK(kith_coord_clear_authority(c, &k) == 0);
    CHECK(kith_coord_authority(c, &k, &auth) == 0);
    CHECK(auth.instance_id == 0u);
    CHECK(auth.authority_epoch == 0u);
    CHECK(kith_coord_cell_count(c) == 0u);
    CHECK(kith_coord_owned_cell_count(c, 6u) == 0u);

    // Idempotent clear on a cell with no override.
    CHECK(kith_coord_clear_authority(c, &k) == 0);

    kith_coord_destroy(c);
    return failures;
}

// Multiple cells in different zones and lods are tracked independently.
// owned_cell_count tallies only the cells overridden to that instance.
static int test_multiple_cells(void)
{
    int failures = 0;
    kith_coord_t *c = nullptr;
    CHECK(kith_coord_create(nullptr, nullptr, nullptr, &c) == 0);

    kith_fabric_cell_key_t k1 = make_key(1u, 0, 0, 0, 0u);
    kith_fabric_cell_key_t k2 = make_key(1u, 1, 1, 0, 0u);
    kith_fabric_cell_key_t k3 = make_key(2u, 0, 0, 0, 1u);

    CHECK(kith_coord_set_authority(c, &k1, 3u) == 0);
    CHECK(kith_coord_set_authority(c, &k2, 3u) == 0);
    CHECK(kith_coord_set_authority(c, &k3, 4u) == 0);
    CHECK(kith_coord_cell_count(c) == 3u);
    CHECK(kith_coord_owned_cell_count(c, 3u) == 2u);
    CHECK(kith_coord_owned_cell_count(c, 4u) == 1u);
    CHECK(kith_coord_owned_cell_count(c, 9u) == 0u);

    // Clearing one cell leaves the others intact.
    CHECK(kith_coord_clear_authority(c, &k1) == 0);
    CHECK(kith_coord_cell_count(c) == 2u);
    CHECK(kith_coord_owned_cell_count(c, 3u) == 1u);
    CHECK(kith_coord_owned_cell_count(c, 4u) == 1u);

    kith_coord_destroy(c);
    return failures;
}

// snapshot_zone copies the overridden cells in one zone at one lod into
// the caller's buffer. A NULL buffer with max 0 only counts. A buffer too
// small copies up to max and reports the copied count via out_count.
static int test_snapshot_zone(void)
{
    int failures = 0;
    kith_coord_t *c = nullptr;
    CHECK(kith_coord_create(nullptr, nullptr, nullptr, &c) == 0);

    kith_fabric_cell_key_t k1 = make_key(5u, 0, 0, 0, 0u);
    kith_fabric_cell_key_t k2 = make_key(5u, 1, 1, 0, 0u);
    kith_fabric_cell_key_t k3 = make_key(5u, 2, 2, 0, 1u);
    kith_fabric_cell_key_t k4 = make_key(6u, 0, 0, 0, 0u);

    CHECK(kith_coord_set_authority(c, &k1, 1u) == 0);
    CHECK(kith_coord_set_authority(c, &k2, 2u) == 0);
    CHECK(kith_coord_set_authority(c, &k3, 3u) == 0);
    CHECK(kith_coord_set_authority(c, &k4, 4u) == 0);

    // Count-only: zone 5, lod 0 has 2 entries (k1, k2).
    size_t count = 99u;
    CHECK(kith_coord_snapshot_zone(c, 5u, 0u, nullptr, 0u, &count) == 0);
    CHECK(count == 2u);

    // Copy into a buffer.
    kith_coord_cell_entry_t entries[8] = {0};
    count = 99u;
    CHECK(kith_coord_snapshot_zone(c, 5u, 0u, entries, 8u, &count) == 0);
    CHECK(count == 2u);

    // The entries are for zone 5, lod 0, with the set instance_ids.
    bool found1 = false;
    bool found2 = false;
    for (size_t i = 0u; i < count; ++i)
    {
        CHECK(entries[i].key.zone == 5u);
        CHECK(entries[i].key.lod == 0u);
        if (entries[i].key.cell_x == 0 && entries[i].key.cell_y == 0)
        {
            CHECK(entries[i].instance_id == 1u);
            found1 = true;
        }
        if (entries[i].key.cell_x == 1 && entries[i].key.cell_y == 1)
        {
            CHECK(entries[i].instance_id == 2u);
            found2 = true;
        }
    }
    CHECK(found1);
    CHECK(found2);

    // A buffer too small copies up to max; out_count reports copied.
    kith_coord_cell_entry_t small[1] = {0};
    size_t copied = 99u;
    CHECK(kith_coord_snapshot_zone(c, 5u, 0u, small, 1u, &copied) == 0);
    CHECK(copied == 1u);

    // Zone 6, lod 0 has 1 entry (k4).
    count = 99u;
    CHECK(kith_coord_snapshot_zone(c, 6u, 0u, nullptr, 0u, &count) == 0);
    CHECK(count == 1u);

    // An empty zone reports zero.
    count = 99u;
    CHECK(kith_coord_snapshot_zone(c, 99u, 0u, nullptr, 0u, &count) == 0);
    CHECK(count == 0u);

    kith_coord_destroy(c);
    return failures;
}

int main(void)
{
    int rc = 0;
    rc |= test_authority_arg_validation();
    rc |= test_set_clear_authority();
    rc |= test_multiple_cells();
    rc |= test_snapshot_zone();
    if (rc != 0)
    {
        (void)fprintf(stderr, "coord authority tests FAILED\n");
    }
    return rc;
}
