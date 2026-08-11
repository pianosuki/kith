#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include "kith/fabric/fabric.h"
#include "kith/gateway/gateway.h"
#include "kith/net/net.h"
#include "kith/proto/proto.h"
#include "kith/sim/sim.h"
#include "kith/types.h"
#include "kith/version.h"

static int check_cond(bool ok, int line)
{
    if (!ok)
    {
        (void)fprintf(stderr, "gateway create: assertion at line %d failed\n", line);
        return 1;
    }
    return 0;
}

#define CHECK(cond) failures += check_cond((cond), __LINE__)

static void handler_nop(uint16_t msg_type,
                        const void *payload,
                        uint32_t payload_len,
                        kith_gateway_session_t *session,
                        void *user_data)
{
    (void)msg_type;
    (void)payload;
    (void)payload_len;
    (void)session;
    (void)user_data;
}

// A NULL out slot is rejected with EINVAL. NULL net, fabric, or proto is
// rejected with EINVAL even when the other handles are valid. NULL params
// selects all defaults and builds a working handle. destroy(NULL) is a no-op.
static int test_create_arg_validation(void)
{
    int failures = 0;
    kith_proto_t *proto = nullptr;
    CHECK(kith_proto_create(nullptr, nullptr, &proto) == 0);
    kith_net_t *net = nullptr;
    CHECK(kith_net_create(nullptr, proto, nullptr, &net) == 0);
    kith_sim_t *sim = nullptr;
    CHECK(kith_sim_create(nullptr, nullptr, &sim) == 0);
    kith_fabric_t *fabric = nullptr;
    CHECK(kith_fabric_create(nullptr, sim, nullptr, &fabric) == 0);

    CHECK(kith_gateway_create(nullptr, net, fabric, proto, nullptr, nullptr) ==
          kith_error_return(KITH_EINVAL));

    kith_gateway_t *g = nullptr;
    CHECK(kith_gateway_create(nullptr, nullptr, fabric, proto, nullptr, &g) ==
          kith_error_return(KITH_EINVAL));
    CHECK(g == nullptr);
    CHECK(kith_gateway_create(nullptr, net, nullptr, proto, nullptr, &g) ==
          kith_error_return(KITH_EINVAL));
    CHECK(g == nullptr);
    CHECK(kith_gateway_create(nullptr, net, fabric, nullptr, nullptr, &g) ==
          kith_error_return(KITH_EINVAL));
    CHECK(g == nullptr);

    CHECK(kith_gateway_create(nullptr, net, fabric, proto, nullptr, &g) == 0);
    CHECK(g != nullptr);

    kith_gateway_destroy(g);
    kith_gateway_destroy(nullptr);

    kith_fabric_destroy(fabric);
    kith_sim_destroy(sim);
    kith_net_destroy(net);
    kith_proto_destroy(proto);
    return failures;
}

// Params are size-versioned: an incompatible abi_version is rejected with
// EABIVER and an undersized size with ESIZE. A params struct with every
// field zero selects the defaults and builds a working handle.
static int test_create_params_validation(void)
{
    int failures = 0;
    kith_proto_t *proto = nullptr;
    CHECK(kith_proto_create(nullptr, nullptr, &proto) == 0);
    kith_net_t *net = nullptr;
    CHECK(kith_net_create(nullptr, proto, nullptr, &net) == 0);
    kith_sim_t *sim = nullptr;
    CHECK(kith_sim_create(nullptr, nullptr, &sim) == 0);
    kith_fabric_t *fabric = nullptr;
    CHECK(kith_fabric_create(nullptr, sim, nullptr, &fabric) == 0);

    kith_gateway_params_t params = {0};
    params.size = sizeof(params);
    params.abi_version = KITH_ABI_VERSION;

    kith_gateway_t *g = nullptr;
    CHECK(kith_gateway_create(&params, net, fabric, proto, nullptr, &g) == 0);
    CHECK(g != nullptr);
    kith_gateway_destroy(g);

    // Wrong abi_version.
    params.abi_version = KITH_ABI_VERSION
    +1u;
    g = nullptr;
    CHECK(kith_gateway_create(&params, net, fabric, proto, nullptr, &g) ==
          kith_error_return(KITH_EABIVER));
    CHECK(g == nullptr);

    // Undersized size.
    params.abi_version = KITH_ABI_VERSION;
    params.size = sizeof(params) - 1u;
    g = nullptr;
    CHECK(kith_gateway_create(&params, net, fabric, proto, nullptr, &g) ==
          kith_error_return(KITH_ESIZE));
    CHECK(g == nullptr);

    kith_fabric_destroy(fabric);
    kith_sim_destroy(sim);
    kith_net_destroy(net);
    kith_proto_destroy(proto);
    return failures;
}

// A handler table capacity smaller than the largest registered message type
// id is honored: registering a type id at or beyond the capacity is rejected
// with EINVAL. A custom capacity tight to the used ids keeps the table small.
static int test_create_handler_table_size(void)
{
    int failures = 0;
    kith_proto_t *proto = nullptr;
    CHECK(kith_proto_create(nullptr, nullptr, &proto) == 0);
    kith_net_t *net = nullptr;
    CHECK(kith_net_create(nullptr, proto, nullptr, &net) == 0);
    kith_sim_t *sim = nullptr;
    CHECK(kith_sim_create(nullptr, nullptr, &sim) == 0);
    kith_fabric_t *fabric = nullptr;
    CHECK(kith_fabric_create(nullptr, sim, nullptr, &fabric) == 0);

    kith_gateway_params_t params = {0};
    params.size = sizeof(params);
    params.abi_version = KITH_ABI_VERSION;
    params.handler_table_size = 8u;

    kith_gateway_t *g = nullptr;
    CHECK(kith_gateway_create(&params, net, fabric, proto, nullptr, &g) == 0);
    CHECK(g != nullptr);

    // Type id 7 is within the 8-slot table; type id 8 is not.
    CHECK(kith_gateway_register_handler(g, 7u, handler_nop, nullptr) == 0);
    CHECK(kith_gateway_register_handler(g, 8u, handler_nop, nullptr) ==
          kith_error_return(KITH_EINVAL));

    kith_gateway_destroy(g);
    kith_fabric_destroy(fabric);
    kith_sim_destroy(sim);
    kith_net_destroy(net);
    kith_proto_destroy(proto);
    return failures;
}

int main(void)
{
    int rc = 0;
    rc |= test_create_arg_validation();
    rc |= test_create_params_validation();
    rc |= test_create_handler_table_size();
    if (rc != 0)
    {
        (void)fprintf(stderr, "gateway create tests FAILED\n");
    }
    return rc;
}
