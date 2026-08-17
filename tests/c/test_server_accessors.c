#include <stddef.h>
#include <stdint.h>
#include <stdio.h>

#include "kith/server/server.h"
#include "kith/types.h"
#include "kith/version.h"

static int check_cond(bool ok, int line)
{
    if (!ok)
    {
        (void)fprintf(stderr, "server accessors: assertion at line %d failed\n", line);
        return 1;
    }
    return 0;
}

#define CHECK(cond) failures += check_cond((cond), __LINE__)

// A NULL server yields a NULL handle from every accessor (the borrowed
// contract: the caller uses the handle and never destroys it, and a missing
// server reports its absence as NULL rather than crashing).
static int test_null_server_accessors(void)
{
    int failures = 0;
    CHECK(kith_server_gateway(nullptr) == nullptr);
    CHECK(kith_server_sim(nullptr) == nullptr);
    CHECK(kith_server_fabric(nullptr) == nullptr);
    CHECK(kith_server_proto(nullptr) == nullptr);
    CHECK(kith_server_control(nullptr) == nullptr);
    CHECK(kith_server_db(nullptr) == nullptr);
    return failures;
}

// A freshly-created server wires every plane handle. The accessors return
// non-NULL borrowed references for the planes the default topology brings
// up (gateway, sim, fabric, proto, control). The db accessor returns NULL
// when no persistence pool is configured (the default). The handles stay
// fixed for the server's lifetime; the caller never destroys them. The
// default listen_port (0) binds an OS-assigned ephemeral port, so this test
// does not contend with the other server tests under parallel ctest.
static int test_default_topology_exposes_planes(void)
{
    int failures = 0;
    kith_server_params_t params = {0};
    params.size = sizeof(params);
    params.abi_version = KITH_ABI_VERSION;

    kith_server_t *s = nullptr;
    CHECK(kith_server_create(&params, nullptr, &s) == 0);
    CHECK(s != nullptr);

    CHECK(kith_server_gateway(s) != nullptr);
    CHECK(kith_server_sim(s) != nullptr);
    CHECK(kith_server_fabric(s) != nullptr);
    CHECK(kith_server_proto(s) != nullptr);
    CHECK(kith_server_control(s) != nullptr);
    // No persistence pool is configured by the default wiring.
    CHECK(kith_server_db(s) == nullptr);

    // NULL server reports 0; a created server started the control plane on
    // an OS-assigned ephemeral port, so the accessor reports a real port.
    CHECK(kith_server_control_port(nullptr) == 0u);
    CHECK(kith_server_control_port(s) > 0u);

    kith_server_destroy(s);
    return failures;
}

// The embedded topology wires the same planes as the distributed topology
// (the coord owns every cell; the fabric uses in-memory storage). Every
// accessor behaves the same as the distributed case. The default listen_port
// (0) binds an OS-assigned ephemeral port, so this test does not contend
// with its siblings under parallel ctest.
static int test_embedded_topology_exposes_planes(void)
{
    int failures = 0;
    kith_server_params_t params = {0};
    params.size = sizeof(params);
    params.abi_version = KITH_ABI_VERSION;
    params.topology = KITH_SERVER_TOPOLOGY_EMBEDDED;

    kith_server_t *s = nullptr;
    CHECK(kith_server_create(&params, nullptr, &s) == 0);
    CHECK(s != nullptr);

    CHECK(kith_server_gateway(s) != nullptr);
    CHECK(kith_server_sim(s) != nullptr);
    CHECK(kith_server_fabric(s) != nullptr);
    CHECK(kith_server_proto(s) != nullptr);
    CHECK(kith_server_control(s) != nullptr);
    CHECK(kith_server_db(s) == nullptr);

    CHECK(kith_server_control_port(s) > 0u);

    kith_server_destroy(s);
    return failures;
}

// Registering a wire type on the borrowed proto handle before the run loop
// lets the gateway decoder accept frames of that type. The accessors hand
// out the proto the gateway borrows, so a type registered on the borrowed
// proto is visible to the decoder.
static int test_borrowed_proto_registers_type(void)
{
    int failures = 0;
    kith_server_params_t params = {0};
    params.size = sizeof(params);
    params.abi_version = KITH_ABI_VERSION;

    kith_server_t *s = nullptr;
    CHECK(kith_server_create(&params, nullptr, &s) == 0);

    kith_proto_t *proto = kith_server_proto(s);
    CHECK(proto != nullptr);
    // A user type id at the reserved base registers without error.
    CHECK(kith_proto_register_type_id(proto, "login", 1000u) == 0);

    uint16_t out_id = 0u;
    CHECK(kith_proto_lookup_type(proto, "login", &out_id) == 0);
    CHECK(out_id == 1000u);

    kith_server_destroy(s);
    return failures;
}

int main(void)
{
    int rc = 0;
    rc |= test_null_server_accessors();
    rc |= test_default_topology_exposes_planes();
    rc |= test_embedded_topology_exposes_planes();
    rc |= test_borrowed_proto_registers_type();
    if (rc != 0)
    {
        (void)fprintf(stderr, "server accessor tests FAILED\n");
    }
    return rc;
}
