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
        (void)fprintf(stderr, "coord split: assertion at line %d failed\n", line);
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

// report_density rejects a NULL coord or key with EINVAL. A valid report
// records the actor count for the cell without changing the ownership
// table. tick in embedded mode (bus NULL or one member) is a no-op that
// returns 0 without evaluating thresholds.
static int test_report_density_and_tick(void)
{
    int failures = 0;
    kith_coord_t *c = nullptr;
    CHECK(kith_coord_create(nullptr, nullptr, nullptr, &c) == 0);

    kith_fabric_cell_key_t k = make_key(1u, 0, 0, 0, 0u);

    CHECK(kith_coord_report_density(nullptr, &k, 100u, 1000u) == kith_error_return(KITH_EINVAL));
    CHECK(kith_coord_report_density(c, nullptr, 100u, 1000u) == kith_error_return(KITH_EINVAL));

    // A valid report does not create an ownership override.
    CHECK(kith_coord_report_density(c, &k, 100u, 1000u) == 0);
    CHECK(kith_coord_cell_count(c) == 0u);

    // tick in embedded mode is a no-op (no bus, no peers to split to).
    CHECK(kith_coord_tick(nullptr, 1000u) == kith_error_return(KITH_EINVAL));
    CHECK(kith_coord_tick(c, 2000u) == 0);
    CHECK(kith_coord_cell_count(c) == 0u);

    kith_coord_destroy(c);
    return failures;
}

// on_rebalance applies an incoming contract directly. A contract with a
// non-zero target sets the override to the target instance with the
// contract's epoch. A contract with target 0 clears the override (merge).
// NULL coord or contract is rejected with EINVAL.
static int test_on_rebalance(void)
{
    int failures = 0;
    kith_coord_t *c = nullptr;
    CHECK(kith_coord_create(nullptr, nullptr, nullptr, &c) == 0);

    kith_fabric_cell_key_t k = make_key(2u, 3, 4, 0, 0u);

    kith_coord_rebalance_contract_t contract = {0};
    contract.key = k;
    CHECK(kith_coord_on_rebalance(nullptr, &contract) == kith_error_return(KITH_EINVAL));
    CHECK(kith_coord_on_rebalance(c, nullptr) == kith_error_return(KITH_EINVAL));

    // A split contract: target 8, epoch 42.
    kith_coord_rebalance_contract_t split = {0};
    split.key = k;
    split.source_instance_id = 0u;
    split.target_instance_id = 8u;
    split.authority_epoch = 42u;

    CHECK(kith_coord_on_rebalance(c, &split) == 0);
    kith_coord_authority_t auth = {0};
    CHECK(kith_coord_authority(c, &k, &auth) == 0);
    CHECK(auth.instance_id == 8u);
    CHECK(auth.authority_epoch == 42u);
    CHECK(kith_coord_cell_count(c) == 1u);

    // A second split contract replaces the override with a new epoch.
    kith_coord_rebalance_contract_t split2 = {0};
    split2.key = k;
    split2.source_instance_id = 8u;
    split2.target_instance_id = 9u;
    split2.authority_epoch = 43u;

    CHECK(kith_coord_on_rebalance(c, &split2) == 0);
    CHECK(kith_coord_authority(c, &k, &auth) == 0);
    CHECK(auth.instance_id == 9u);
    CHECK(auth.authority_epoch == 43u);
    CHECK(kith_coord_cell_count(c) == 1u);

    // A merge contract: target 0, epoch 44 clears the override.
    kith_coord_rebalance_contract_t merge = {0};
    merge.key = k;
    merge.source_instance_id = 9u;
    merge.target_instance_id = 0u;
    merge.authority_epoch = 44u;

    CHECK(kith_coord_on_rebalance(c, &merge) == 0);
    CHECK(kith_coord_authority(c, &k, &auth) == 0);
    CHECK(auth.instance_id == 0u);
    CHECK(auth.authority_epoch == 0u);
    CHECK(kith_coord_cell_count(c) == 0u);

    // A merge on a cell with no override is a no-op (idempotent).
    CHECK(kith_coord_on_rebalance(c, &merge) == 0);
    CHECK(kith_coord_cell_count(c) == 0u);

    kith_coord_destroy(c);
    return failures;
}

// tick with a borrowed bus of one member (loopback) is still a no-op:
// the split/merge evaluator only fires when there is more than one member
// to split to. report_density still records entries safely.
static int test_tick_with_loopback_bus(void)
{
    int failures = 0;
    kith_coord_bus_t *bus = nullptr;
    CHECK(kith_coord_bus_create(nullptr, nullptr, &bus) == 0);
    CHECK(kith_coord_bus_member_count(bus) == 1u);

    kith_coord_t *c = nullptr;
    CHECK(kith_coord_create(nullptr, bus, nullptr, &c) == 0);

    kith_fabric_cell_key_t k = make_key(1u, 0, 0, 0, 0u);
    // Report a density well above the split threshold; tick stays a no-op
    // because the loopback bus has one member.
    CHECK(kith_coord_report_density(c, &k, 999u, 0u) == 0);
    CHECK(kith_coord_tick(c, 100000u) == 0);
    CHECK(kith_coord_cell_count(c) == 0u);

    kith_coord_destroy(c);
    kith_coord_bus_destroy(bus);
    return failures;
}

// Two coord handles borrowing one loopback bus form a real multi-member
// cluster in-process once add_member extends the membership table. The bus
// local instance is 2; coord B (instance 2) is the bus owner. The added
// member 1 is coord A. With two members at equal load, the split target
// picker selects members[0] (instance 2), which is not coord A's own
// instance, so the split fires. Returns 0 on success.
static int
build_two_member_cluster(kith_coord_bus_t **out_bus, kith_coord_t **out_a, kith_coord_t **out_b)
{
    *out_bus = nullptr;
    *out_a = nullptr;
    *out_b = nullptr;

    kith_coord_bus_params_t bparams = {0};
    bparams.size = sizeof(bparams);
    bparams.abi_version = KITH_ABI_VERSION;
    bparams.instance_id = 2u;
    if (kith_coord_bus_create(&bparams, nullptr, out_bus) != 0)
    {
        return 1;
    }
    if (kith_coord_bus_add_member(*out_bus, 1u) != 0 || kith_coord_bus_member_count(*out_bus) != 2u)
    {
        kith_coord_bus_destroy(*out_bus);
        *out_bus = nullptr;
        return 1;
    }

    kith_coord_params_t params = {0};
    params.size = sizeof(params);
    params.abi_version = KITH_ABI_VERSION;
    params.split_threshold = 10u;
    params.merge_threshold = 5u;
    params.split_min_dwell_ms = 100u;
    params.merge_min_dwell_ms = 100u;
    params.density_stride = 1u;

    params.instance_id = 1u;
    if (kith_coord_create(&params, *out_bus, nullptr, out_a) != 0)
    {
        kith_coord_bus_destroy(*out_bus);
        *out_bus = nullptr;
        return 1;
    }
    params.instance_id = 2u;
    if (kith_coord_create(&params, *out_bus, nullptr, out_b) != 0)
    {
        kith_coord_destroy(*out_a);
        *out_a = nullptr;
        kith_coord_bus_destroy(*out_bus);
        *out_bus = nullptr;
        return 1;
    }
    return 0;
}

// Drain one rebalance event from the bus and apply it to coord. Returns 0
// on success. Asserts the event is a rebalance carrying a contract.
static int drain_and_apply_rebalance(kith_coord_bus_t *bus, kith_coord_t *coord)
{
    kith_coord_bus_event_t events[1] = {0};
    size_t count = 0u;
    if (kith_coord_bus_drain(bus, events, 1u, &count) != 0 || count != 1u)
    {
        return 1;
    }
    if (events[0].event_type != KITH_COORD_BUS_EVENT_REBALANCE ||
        events[0].payload_len != sizeof(kith_coord_rebalance_contract_t))
    {
        return 1;
    }
    kith_coord_rebalance_contract_t contract = {0};
    memcpy(&contract, events[0].payload, sizeof(contract));
    return kith_coord_on_rebalance(coord, &contract) == 0 ? 0 : 1;
}

// The density-driven evaluator fires on the overloaded coord: it picks the
// least-loaded peer as the split target, sets the override, bumps the
// authority epoch, and broadcasts a rebalance contract. The peer drains the
// bus and applies the contract, so both coords converge on the new
// authority.
static int test_density_driven_split_fires_across_coords(void)
{
    int failures = 0;
    kith_coord_bus_t *bus = nullptr;
    kith_coord_t *a = nullptr;
    kith_coord_t *b = nullptr;
    CHECK(build_two_member_cluster(&bus, &a, &b) == 0);

    kith_fabric_cell_key_t k = make_key(1u, 0, 0, 0, 0u);

    // Hash-fallback for this cell with members [2, 1]: (0+0+1) % 2 = 1,
    // mapping to members[1] = instance 1 (coord A owns it pre-split).
    kith_coord_authority_t auth = {0};
    CHECK(kith_coord_authority(a, &k, &auth) == 0);
    CHECK(auth.instance_id == 1u);
    CHECK(auth.authority_epoch == 0u);

    // Report density above the split threshold; advance past the dwell. The
    // report timestamp must be non-zero: report_density arms split_since_ms
    // only when it is zero, and tick treats a zero split_since_ms as "not
    // armed."
    CHECK(kith_coord_report_density(a, &k, 100u, 1000u) == 0);
    CHECK(kith_coord_tick(a, 2000u) == 0);

    // Coord A applied the split: the override now points at instance 2.
    CHECK(kith_coord_authority(a, &k, &auth) == 0);
    CHECK(auth.instance_id == 2u);
    CHECK(auth.authority_epoch == 1u);

    // Coord B drains the broadcast rebalance and applies it, converging on
    // the same authority as coord A.
    CHECK(drain_and_apply_rebalance(bus, b) == 0);
    CHECK(kith_coord_authority(b, &k, &auth) == 0);
    CHECK(auth.instance_id == 2u);
    CHECK(auth.authority_epoch == 1u);

    kith_coord_destroy(a);
    kith_coord_destroy(b);
    kith_coord_bus_destroy(bus);
    return failures;
}

// After a split, a density drop below the merge threshold fires the merge
// path: coord A clears its override and broadcasts a target-0 contract;
// coord B applies it and reverts to hash-fallback.
static int test_density_driven_merge_reverts_across_coords(void)
{
    int failures = 0;
    kith_coord_bus_t *bus = nullptr;
    kith_coord_t *a = nullptr;
    kith_coord_t *b = nullptr;
    CHECK(build_two_member_cluster(&bus, &a, &b) == 0);

    kith_fabric_cell_key_t k = make_key(1u, 0, 0, 0, 0u);

    // Seed an override on coord A by driving a density-driven split, then
    // drain the rebalance so the bus queue is empty before the merge.
    CHECK(kith_coord_report_density(a, &k, 100u, 1000u) == 0);
    CHECK(kith_coord_tick(a, 2000u) == 0);
    kith_coord_authority_t auth = {0};
    CHECK(kith_coord_authority(a, &k, &auth) == 0);
    CHECK(auth.instance_id == 2u);
    CHECK(drain_and_apply_rebalance(bus, b) == 0);

    // Density drops below the merge threshold; advance past the merge
    // dwell. Coord A clears its override and broadcasts a target-0 contract.
    CHECK(kith_coord_report_density(a, &k, 0u, 3000u) == 0);
    CHECK(kith_coord_tick(a, 4000u) == 0);
    CHECK(kith_coord_authority(a, &k, &auth) == 0);
    CHECK(auth.instance_id == 1u);
    CHECK(auth.authority_epoch == 0u);

    // Coord B applies the target-0 contract and reverts to hash-fallback.
    CHECK(drain_and_apply_rebalance(bus, b) == 0);
    CHECK(kith_coord_authority(b, &k, &auth) == 0);
    CHECK(auth.instance_id == 1u);
    CHECK(auth.authority_epoch == 0u);

    kith_coord_destroy(a);
    kith_coord_destroy(b);
    kith_coord_bus_destroy(bus);
    return failures;
}

// The bus-owning coord can split too: the target picker ranks only the
// members other than the evaluating coord, so the owner (members[0] here)
// targets the least-loaded other member instead of being rejected as a
// self-target. The local member's owned cell count is refreshed from its
// own table on the same tick.
static int test_density_driven_split_fires_on_bus_owner(void)
{
    int failures = 0;
    kith_coord_bus_t *bus = nullptr;
    kith_coord_t *a = nullptr;
    kith_coord_t *b = nullptr;
    CHECK(build_two_member_cluster(&bus, &a, &b) == 0);

    kith_fabric_cell_key_t k = make_key(7u, 0, 0, 0, 0u);

    // Report density above the split threshold on the bus owner (instance
    // 2) and advance past the dwell.
    CHECK(kith_coord_report_density(b, &k, 100u, 1000u) == 0);
    CHECK(kith_coord_tick(b, 2000u) == 0);

    // The override points at the OTHER member (instance 1), never at self.
    kith_coord_authority_t auth = {0};
    CHECK(kith_coord_authority(b, &k, &auth) == 0);
    CHECK(auth.instance_id == 1u);
    CHECK(auth.authority_epoch == 1u);

    // Coord A applies the broadcast and converges on the same authority.
    CHECK(drain_and_apply_rebalance(bus, a) == 0);
    CHECK(kith_coord_authority(a, &k, &auth) == 0);
    CHECK(auth.instance_id == 1u);
    CHECK(auth.authority_epoch == 1u);

    // A's own tick refreshes its member entry from its table: instance 1
    // now owns the cell the split moved to it.
    CHECK(kith_coord_tick(a, 2100u) == 0);
    kith_coord_bus_member_status_t status = {0};
    CHECK(kith_coord_bus_member_status(bus, 1u, &status) == 0);
    CHECK(status.instance_id == 1u);
    CHECK(status.owned_cell_count == 1u);

    kith_coord_destroy(a);
    kith_coord_destroy(b);
    kith_coord_bus_destroy(bus);
    return failures;
}

int main(void)
{
    int rc = 0;
    rc |= test_report_density_and_tick();
    rc |= test_on_rebalance();
    rc |= test_tick_with_loopback_bus();
    rc |= test_density_driven_split_fires_across_coords();
    rc |= test_density_driven_merge_reverts_across_coords();
    rc |= test_density_driven_split_fires_on_bus_owner();
    if (rc != 0)
    {
        (void)fprintf(stderr, "coord split tests FAILED\n");
    }
    return rc;
}
