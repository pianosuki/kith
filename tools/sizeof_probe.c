/*---------------------------------------------------------------------------
 * sizeof probe
 *
 * Prints sizeof() for every size-versioned public struct, one
 * "<typedef-name> <bytes>" line per struct on stdout. The Python parity
 * test (tests/python/test_generated_sizeof_parity.py) runs this binary
 * and asserts each ctypes binding's sizeof against the C compiler's:
 * the drift checker proves bindings == headers, and this probe is the
 * only reader of the C-compiler-layout half. Header-only — no library
 * linkage, the sizes come from the public headers alone.
 *
 * The struct list must equal the size-versioned set the bindings carry;
 * the parity test fails both directions of a mismatch, so a new
 * size-versioned struct lands here in the same commit as its binding.
 *-------------------------------------------------------------------------*/

#include <stdio.h>

#include "kith/aoi/aoi.h"
#include "kith/client/client.h"
#include "kith/config/config.h"
#include "kith/control/control.h"
#include "kith/coord/coord.h"
#include "kith/db/db.h"
#include "kith/fabric/fabric.h"
#include "kith/gateway/gateway.h"
#include "kith/logger/logger.h"
#include "kith/metrics/metrics.h"
#include "kith/net/net.h"
#include "kith/proto/proto.h"
#include "kith/reactor/reactor.h"
#include "kith/server/server.h"
#include "kith/sim/sim.h"
#include "kith/state/state.h"
#include "kith/types.h"
#include "kith/util/rng.h"
#include "kith/util/tick_clock.h"
#include "kith/worker/worker.h"

int main(void)
{
    printf("kith_allocator_t %zu\n", sizeof(kith_allocator_t));
    printf("kith_aoi_params_t %zu\n", sizeof(kith_aoi_params_t));
    printf("kith_client_bootstrap_status_t %zu\n", sizeof(kith_client_bootstrap_status_t));
    printf("kith_client_event_t %zu\n", sizeof(kith_client_event_t));
    printf("kith_client_params_t %zu\n", sizeof(kith_client_params_t));
    printf("kith_client_runtime_status_t %zu\n", sizeof(kith_client_runtime_status_t));
    printf("kith_config_params_t %zu\n", sizeof(kith_config_params_t));
    printf("kith_control_params_t %zu\n", sizeof(kith_control_params_t));
    printf("kith_coord_bus_params_t %zu\n", sizeof(kith_coord_bus_params_t));
    printf("kith_coord_params_t %zu\n", sizeof(kith_coord_params_t));
    printf("kith_db_params_t %zu\n", sizeof(kith_db_params_t));
    printf("kith_fabric_params_t %zu\n", sizeof(kith_fabric_params_t));
    printf("kith_gateway_delivery_vtable_t %zu\n", sizeof(kith_gateway_delivery_vtable_t));
    printf("kith_gateway_params_t %zu\n", sizeof(kith_gateway_params_t));
    printf("kith_gateway_tiered_config_t %zu\n", sizeof(kith_gateway_tiered_config_t));
    printf("kith_logger_params_t %zu\n", sizeof(kith_logger_params_t));
    printf("kith_metrics_params_t %zu\n", sizeof(kith_metrics_params_t));
    printf("kith_net_params_t %zu\n", sizeof(kith_net_params_t));
    printf("kith_proto_params_t %zu\n", sizeof(kith_proto_params_t));
    printf("kith_reactor_params_t %zu\n", sizeof(kith_reactor_params_t));
    printf("kith_rng_params_t %zu\n", sizeof(kith_rng_params_t));
    printf("kith_server_params_t %zu\n", sizeof(kith_server_params_t));
    printf("kith_sim_config_t %zu\n", sizeof(kith_sim_config_t));
    printf("kith_sim_model_vtable_t %zu\n", sizeof(kith_sim_model_vtable_t));
    printf("kith_sim_params_t %zu\n", sizeof(kith_sim_params_t));
    printf("kith_state_params_t %zu\n", sizeof(kith_state_params_t));
    printf("kith_tick_clock_params_t %zu\n", sizeof(kith_tick_clock_params_t));
    printf("kith_worker_params_t %zu\n", sizeof(kith_worker_params_t));
    return 0;
}
