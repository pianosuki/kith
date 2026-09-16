/* Allocator contract tests: the per-object allocator seam, exercised
 * through the util, config, logger, metrics, proto, net, reactor, worker,
 * state, db, aoi, sim, fabric, gateway, coord, control, client, and server
 * constructors. A counting/failing allocator sweeps each create path's
 * ENOMEM branch — the Nth allocation attempt fails until the
 * path is proven to unwind cleanly and report -KITH_ENOMEM, then succeeds
 * on the attempt after the path's last allocation. Every successful
 * allocation must be released through the allocator before the call
 * returns: a partial unwind or a foreign free leaves the outstanding count
 * nonzero and fails the test even before the sanitizer build looks at it.
 *
 * The per-path expected attempt counts are load-bearing: a create path
 * that starts allocating one more or one fewer time moves where the
 * success attempt lands. The sweep discovers the count by iteration, and
 * the equality check after the success run keeps that adaptation honest.
 * Config's sweep fixtures use distinct environment-variable namespaces,
 * set up once outside the sweep loop and torn down after, so libc's own
 * allocations and leftover state cannot move a count between runs. */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <unistd.h>

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
#include "kith/util/rng.h"
#include "kith/util/tick_clock.h"
#include "kith/version.h"
#include "kith/worker/worker.h"

#include <netinet/in.h>
#include <sys/socket.h>

/* Sentinel address handed to create calls as the out slot, so a failure
 * path that forgets to clear it is observable. Never dereferenced. */
static uint64_t sentinel;

/* A create path that never succeeds within this many allocation attempts
 * has a permanent failure or an unbounded allocation loop. The sim create
 * is the deepest shape in the suite: its handle, registry, name copies,
 * store, and sixteen shard tables (six apiece) make 101 attempts. */
#define SWEEP_MAX_ATTEMPTS 128U

/* Scaffold failures inside sweep run functions use this positive code so
 * they cannot be mistaken for the negated KITH_ENOMEM a failing
 * allocation must produce. */
#define SWEEP_SCAFFOLD_ERROR 100

typedef struct alloc_tally
{
    unsigned attempts;     /* alloc, alloc_zero, and realloc calls */
    unsigned frees;
    unsigned outstanding;  /* successful allocations not yet freed */
    unsigned fail_attempt; /* the attempt number to fail; 0 disables */
} alloc_tally_t;

static alloc_tally_t g_tally;

static void tally_reset(unsigned fail_attempt)
{
    g_tally.attempts = 0U;
    g_tally.frees = 0U;
    g_tally.outstanding = 0U;
    g_tally.fail_attempt = fail_attempt;
}

static void *tally_alloc(void *ctx, size_t size)
{
    alloc_tally_t *tally = ctx;
    tally->attempts++;
    if (tally->attempts == tally->fail_attempt)
    {
        return nullptr;
    }
    tally->outstanding++;
    return malloc(size);
}

static void *tally_alloc_zero(void *ctx, size_t count, size_t size)
{
    alloc_tally_t *tally = ctx;
    tally->attempts++;
    if (tally->attempts == tally->fail_attempt)
    {
        return nullptr;
    }
    tally->outstanding++;
    return calloc(count, size);
}

static void *tally_realloc(void *ctx, void *ptr, size_t size)
{
    alloc_tally_t *tally = ctx;
    tally->attempts++;
    if (tally->attempts == tally->fail_attempt)
    {
        return nullptr;
    }
    if (ptr != nullptr)
    {
        // realloc releases the replaced block itself, so the tally drops it
        // here: a failed attempt leaves the original block owned, and a
        // successful in-place growth keeps one outstanding slot.
        tally->outstanding--;
    }
    tally->outstanding++;
    return realloc(ptr, size);
}

static void tally_free(void *ctx, void *ptr)
{
    alloc_tally_t *tally = ctx;
    tally->frees++;
    tally->outstanding--;
    free(ptr);
}

static const kith_allocator_t g_tally_allocator = {
    .size = sizeof(kith_allocator_t),
    .abi_version = KITH_ABI_VERSION,
    .user_data = &g_tally,
    .alloc = tally_alloc,
    .alloc_zero = tally_alloc_zero,
    .realloc = tally_realloc,
    .free = tally_free,
    .reserved = {nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr},
};

static kith_rng_params_t valid_rng_params(uint64_t seed)
{
    return (kith_rng_params_t)
    {
        .size = sizeof(kith_rng_params_t), .abi_version = KITH_ABI_VERSION
        , .seed = seed
    };
}

static kith_tick_clock_params_t valid_clock_params(uint32_t tick_hz)
{
    return (kith_tick_clock_params_t)
    {
        .size = sizeof(kith_tick_clock_params_t), .abi_version = KITH_ABI_VERSION
        , .initial_tick = 0, .tick_hz = tick_hz
    };
}

static kith_config_params_t valid_config_params(const char *file_path, const char *env_prefix)
{
    return (kith_config_params_t)
    {
        .size = sizeof(kith_config_params_t), .abi_version = KITH_ABI_VERSION
        , .env_prefix = env_prefix, .file_path = file_path
    };
}

static kith_logger_params_t valid_logger_params(const char *name)
{
    return (kith_logger_params_t)
    {
        .size = sizeof(kith_logger_params_t), .abi_version = KITH_ABI_VERSION
        , .name = name
    };
}

static kith_metrics_params_t valid_metrics_params(const char *prefix)
{
    return (kith_metrics_params_t)
    {
        .size = sizeof(kith_metrics_params_t), .abi_version = KITH_ABI_VERSION
        , .prefix = prefix,
    };
}

static kith_reactor_params_t valid_reactor_params(void)
{
    return (kith_reactor_params_t)
    {
        .size = sizeof(kith_reactor_params_t), .abi_version = KITH_ABI_VERSION
        , .max_fds = 16U, .task_capacity = 2U,
    };
}

static kith_worker_params_t valid_worker_params(void)
{
    return (kith_worker_params_t)
    {
        .size = sizeof(kith_worker_params_t), .abi_version = KITH_ABI_VERSION
        , .worker_count = 1U, .task_capacity = 4U,
    };
}

static kith_state_params_t valid_state_params(void)
{
    return (kith_state_params_t)
    {
        .size = sizeof(kith_state_params_t), .abi_version = KITH_ABI_VERSION
        , .timeout_ms = 1000U,
    };
}

static kith_db_params_t valid_db_params(void)
{
    return (kith_db_params_t)
    {
        .size = sizeof(kith_db_params_t), .abi_version = KITH_ABI_VERSION
        , .host = "127.0.0.1", .port = 5432U, .connect_timeout_ms = 2000U,
    };
}

static kith_aoi_params_t valid_aoi_params(void)
{
    return (kith_aoi_params_t)
    {
        .size = sizeof(kith_aoi_params_t), .abi_version = KITH_ABI_VERSION
        ,
    };
}

/* A small shard table (16 buckets) keeps the sim create's 96 shard-table
 * allocations cheap without changing their count. */
static kith_sim_params_t valid_sim_params(void)
{
    return (kith_sim_params_t)
    {
        .size = sizeof(kith_sim_params_t), .abi_version = KITH_ABI_VERSION
        , .artifact_bucket_count = 16u
    };
}

static kith_sim_actor_t valid_sim_actor(uint64_t id)
{
    return (kith_sim_actor_t){.id = id};
}

static kith_sim_artifact_key_t valid_sim_key(int32_t cz)
{
    return (kith_sim_artifact_key_t){
        .zone = 0u,
        .cell_x = 0,
        .cell_y = 0,
        .cell_z = cz,
        .lod = 0u,
    };
}

static kith_fabric_params_t valid_fabric_params(void)
{
    return (kith_fabric_params_t)
    {
        .size = sizeof(kith_fabric_params_t), .abi_version = KITH_ABI_VERSION
        ,
    };
}

/* One indexed object at @p x_units world units on the X axis (Q16.16),
 * radius zero, so the default 16-unit cell grid buckets it by the unit
 * index: units below 16 share cell 0, 32 lands in cell 2. */
static kith_aoi_object_t valid_aoi_object(uint64_t id, int64_t x_units)
{
    return (kith_aoi_object_t){
        .id = id,
        .pos_x = x_units << KITH_AOI_FIX_SHIFT,
    };
}

/* Requires kith_rng_create to reject @p alloc with @p want_code and to
 * leave the out slot cleared. */
static int rng_rejects_allocator(const kith_allocator_t *alloc, kith_error_t want_code)
{
    kith_rng_params_t params = valid_rng_params(1U);
    kith_rng_t *rng = (kith_rng_t *)&sentinel;
    if (kith_rng_create(&params, alloc, &rng) != kith_error_return(want_code))
    {
        return 1;
    }
    return rng != nullptr;
}

/* Requires kith_rng_create to succeed with @p alloc; releases the handle. */
static int rng_accepts_allocator(const kith_allocator_t *alloc)
{
    kith_rng_params_t params = valid_rng_params(1U);
    kith_rng_t *rng = nullptr;
    if (kith_rng_create(&params, alloc, &rng) != 0)
    {
        return 1;
    }
    kith_rng_destroy(rng);
    return 0;
}

/* Requires kith_config_create to reject @p alloc with @p want_code and to
 * leave the out slot cleared. */
static int config_rejects_allocator(const kith_allocator_t *alloc, kith_error_t want_code)
{
    kith_config_t *cfg = (kith_config_t *)&sentinel;
    if (kith_config_create(nullptr, alloc, &cfg) != kith_error_return(want_code))
    {
        return 1;
    }
    return cfg != nullptr;
}

/* Requires kith_config_create to succeed with @p alloc; releases the handle. */
static int config_accepts_allocator(const kith_allocator_t *alloc)
{
    kith_config_t *cfg = nullptr;
    if (kith_config_create(nullptr, alloc, &cfg) != 0)
    {
        return 1;
    }
    kith_config_destroy(cfg);
    return 0;
}

/* Requires kith_logger_create to reject @p alloc with @p want_code and to
 * leave the out slot cleared. */
static int logger_rejects_allocator(const kith_allocator_t *alloc, kith_error_t want_code)
{
    kith_logger_params_t params = valid_logger_params("reject");
    kith_logger_t *logger = (kith_logger_t *)&sentinel;
    if (kith_logger_create(&params, alloc, &logger) != kith_error_return(want_code))
    {
        return 1;
    }
    return logger != nullptr;
}

/* Requires kith_logger_create to succeed with @p alloc; releases the handle. */
static int logger_accepts_allocator(const kith_allocator_t *alloc)
{
    kith_logger_params_t params = valid_logger_params("accept");
    kith_logger_t *logger = nullptr;
    if (kith_logger_create(&params, alloc, &logger) != 0)
    {
        return 1;
    }
    kith_logger_destroy(logger);
    return 0;
}

/* Requires kith_metrics_create to reject @p alloc with @p want_code and to
 * leave the out slot cleared. */
static int metrics_rejects_allocator(const kith_allocator_t *alloc, kith_error_t want_code)
{
    kith_metrics_t *metrics = (kith_metrics_t *)&sentinel;
    if (kith_metrics_create(nullptr, alloc, &metrics) != kith_error_return(want_code))
    {
        return 1;
    }
    return metrics != nullptr;
}

/* Requires kith_metrics_create to succeed with @p alloc; releases the handle. */
static int metrics_accepts_allocator(const kith_allocator_t *alloc)
{
    kith_metrics_t *metrics = nullptr;
    if (kith_metrics_create(nullptr, alloc, &metrics) != 0)
    {
        return 1;
    }
    kith_metrics_destroy(metrics);
    return 0;
}

/* Requires kith_proto_create to reject @p alloc with @p want_code and to
 * clear the out slot. */
static int proto_rejects_allocator(const kith_allocator_t *alloc, kith_error_t want_code)
{
    kith_proto_t *proto = (kith_proto_t *)&sentinel;
    if (kith_proto_create(nullptr, alloc, &proto) != kith_error_return(want_code))
    {
        return 1;
    }
    return proto != nullptr;
}

/* Requires kith_proto_create to succeed with @p alloc; releases the handle. */
static int proto_accepts_allocator(const kith_allocator_t *alloc)
{
    kith_proto_t *proto = nullptr;
    if (kith_proto_create(nullptr, alloc, &proto) != 0)
    {
        return 1;
    }
    kith_proto_destroy(proto);
    return 0;
}

/* Requires kith_net_create to reject @p alloc with @p want_code and to
 * clear the out slot. The borrowed proto handle is built with the default
 * allocator so the tally observes only the net path. */
static int net_rejects_allocator(const kith_allocator_t *alloc, kith_error_t want_code)
{
    kith_proto_t *proto = nullptr;
    if (kith_proto_create(nullptr, nullptr, &proto) != 0)
    {
        return 1;
    }
    kith_net_t *net = (kith_net_t *)&sentinel;
    const int rc = kith_net_create(nullptr, proto, alloc, &net);
    kith_proto_destroy(proto);
    if (rc != kith_error_return(want_code))
    {
        return 1;
    }
    return net != nullptr;
}

/* Requires kith_net_create to succeed with @p alloc; releases the handle. */
static int net_accepts_allocator(const kith_allocator_t *alloc)
{
    kith_proto_t *proto = nullptr;
    if (kith_proto_create(nullptr, nullptr, &proto) != 0)
    {
        return 1;
    }
    kith_net_t *net = nullptr;
    const int rc = kith_net_create(nullptr, proto, alloc, &net);
    kith_proto_destroy(proto);
    if (rc != 0)
    {
        return 1;
    }
    kith_net_destroy(net);
    return 0;
}

/* Timer/task callback for the reactor routing pin and its ENOMEM tooth: no
 * run loop is driven, so it never fires; it only needs a callable address. */
static void reactor_noop_task(void *ctx)
{
    (void)ctx;
}

/* Requires kith_reactor_create to reject @p alloc with @p want_code and to
 * clear the out slot. No scaffold: the reactor takes no borrowed handles. */
static int reactor_rejects_allocator(const kith_allocator_t *alloc, kith_error_t want_code)
{
    kith_reactor_t *reactor = (kith_reactor_t *)&sentinel;
    const int rc = kith_reactor_create(nullptr, alloc, &reactor);
    if (rc != kith_error_return(want_code))
    {
        return 1;
    }
    return reactor != nullptr;
}

/* Requires kith_reactor_create to succeed with @p alloc; releases the handle. */
static int reactor_accepts_allocator(const kith_allocator_t *alloc)
{
    kith_reactor_t *reactor = nullptr;
    const int rc = kith_reactor_create(nullptr, alloc, &reactor);
    if (rc != 0)
    {
        return 1;
    }
    kith_reactor_destroy(reactor);
    return 0;
}

/* Ready-hop callback for the worker routing pin and its drop tooth: no
 * pool work is observed here, it only needs a callable address. */
static void worker_noop_ready(int fd, unsigned int events, void *ctx)
{
    (void)fd;
    (void)events;
    (void)ctx;
}

/* Requires kith_worker_create to reject @p alloc with @p want_code and to
 * clear the out slot. No scaffold: the pool takes no borrowed handles. */
static int worker_rejects_allocator(const kith_allocator_t *alloc, kith_error_t want_code)
{
    kith_worker_t *pool = (kith_worker_t *)&sentinel;
    if (kith_worker_create(nullptr, alloc, &pool) != kith_error_return(want_code))
    {
        return 1;
    }
    return pool != nullptr;
}

/* Requires kith_worker_create to succeed with @p alloc; releases the handle. */
static int worker_accepts_allocator(const kith_allocator_t *alloc)
{
    kith_worker_t *pool = nullptr;
    if (kith_worker_create(nullptr, alloc, &pool) != 0)
    {
        return 1;
    }
    kith_worker_destroy(pool);
    return 0;
}

/* Reply callback for the state routing pin and its ENOMEM tooth: the pinned
 * paths fail before any submission, so no reply ever fires. */
static void state_noop_reply(kith_state_reply_t *reply)
{
    (void)reply;
}

/* Task callback that fills the reactor's task queue for the state
 * submit-failure tooth: it never runs, it only needs a callable address. */
static void state_noop_task(void *ctx)
{
    (void)ctx;
}

/* Requires kith_state_create to reject @p alloc with @p want_code and to
 * clear the out slot. The borrowed reactor is built with the default
 * allocator so the tally observes only the state path. */
static int state_rejects_allocator(const kith_allocator_t *alloc, kith_error_t want_code)
{
    kith_reactor_t *reactor = nullptr;
    if (kith_reactor_create(nullptr, nullptr, &reactor) != 0)
    {
        return 1;
    }
    kith_state_params_t params = valid_state_params();
    kith_state_t *state = (kith_state_t *)&sentinel;
    const int rc = kith_state_create(&params, reactor, alloc, &state);
    kith_reactor_destroy(reactor);
    if (rc != kith_error_return(want_code))
    {
        return 1;
    }
    return state != nullptr;
}

/* Requires kith_state_create to succeed with @p alloc; releases the handle
 * before the reactor it borrowed. */
static int state_accepts_allocator(const kith_allocator_t *alloc)
{
    kith_reactor_t *reactor = nullptr;
    if (kith_reactor_create(nullptr, nullptr, &reactor) != 0)
    {
        return 1;
    }
    kith_state_params_t params = valid_state_params();
    kith_state_t *state = nullptr;
    if (kith_state_create(&params, reactor, alloc, &state) != 0)
    {
        kith_reactor_destroy(reactor);
        return 1;
    }
    kith_state_destroy(state);
    kith_reactor_destroy(reactor);
    return 0;
}

/* Reply callback for the db routing pin and its ENOMEM tooth: the pinned
 * paths fail or stall before any submission, so no reply ever fires. */
static void db_noop_reply(kith_db_reply_t *reply)
{
    (void)reply;
}

/* Task callback that fills the reactor's task queue for the db
 * submit-failure tooth: it never runs, it only needs a callable address. */
static void db_noop_task(void *ctx)
{
    (void)ctx;
}

/* One scaffold reactor serves every db stage: the io_uring backend pins its
 * ring pages against the process's memlock budget and the kernel reclaims
 * them lazily after close, so per-call scaffolds multiply the suite's ring
 * churn past what one run may hold. Created with the default allocator on
 * first use, released once after the last db stage. */
static kith_reactor_t *g_db_scaffold;

static kith_reactor_t *db_scaffold_reactor(void)
{
    if (g_db_scaffold == nullptr && kith_reactor_create(nullptr, nullptr, &g_db_scaffold) != 0)
    {
        return nullptr;
    }
    return g_db_scaffold;
}

static void db_scaffold_release(void)
{
    kith_reactor_destroy(g_db_scaffold);
    g_db_scaffold = nullptr;
}

/* Requires kith_db_create to reject @p alloc with @p want_code and to
 * clear the out slot. The scaffold reactor carries the default allocator,
 * so the tally observes only the db path. */
static int db_rejects_allocator(const kith_allocator_t *alloc, kith_error_t want_code)
{
    kith_reactor_t *reactor = db_scaffold_reactor();
    if (reactor == nullptr)
    {
        return 1;
    }
    kith_db_params_t params = valid_db_params();
    kith_db_t *db = (kith_db_t *)&sentinel;
    const int rc = kith_db_create(&params, reactor, alloc, &db);
    if (rc != kith_error_return(want_code))
    {
        return 1;
    }
    return db != nullptr;
}

/* Requires kith_db_create to succeed with @p alloc; releases the handle.
 * The scaffold reactor outlives the db stage group. */
static int db_accepts_allocator(const kith_allocator_t *alloc)
{
    kith_reactor_t *reactor = db_scaffold_reactor();
    if (reactor == nullptr)
    {
        return 1;
    }
    kith_db_params_t params = valid_db_params();
    kith_db_t *db = nullptr;
    if (kith_db_create(&params, reactor, alloc, &db) != 0)
    {
        return 1;
    }
    kith_db_destroy(db);
    return 0;
}

/* Requires kith_aoi_create to reject @p alloc with @p want_code and to
 * clear the out slot. */
static int aoi_rejects_allocator(const kith_allocator_t *alloc, kith_error_t want_code)
{
    kith_aoi_params_t params = valid_aoi_params();
    kith_aoi_t *aoi = (kith_aoi_t *)&sentinel;
    const int rc = kith_aoi_create(&params, alloc, &aoi);
    if (rc != kith_error_return(want_code))
    {
        return 1;
    }
    return aoi != nullptr;
}

/* Requires kith_aoi_create to succeed with @p alloc; releases the handle. */
static int aoi_accepts_allocator(const kith_allocator_t *alloc)
{
    kith_aoi_params_t params = valid_aoi_params();
    kith_aoi_t *aoi = nullptr;
    if (kith_aoi_create(&params, alloc, &aoi) != 0)
    {
        return 1;
    }
    kith_aoi_destroy(aoi);
    return 0;
}

/* Requires kith_sim_create to reject @p alloc with @p want_code and to
 * clear the out slot. */
static int sim_rejects_allocator(const kith_allocator_t *alloc, kith_error_t want_code)
{
    kith_sim_params_t params = valid_sim_params();
    kith_sim_t *sim = (kith_sim_t *)&sentinel;
    const int rc = kith_sim_create(&params, alloc, &sim);
    if (rc != kith_error_return(want_code))
    {
        return 1;
    }
    return sim != nullptr;
}

/* Requires kith_sim_create to succeed with @p alloc; releases the handle. */
static int sim_accepts_allocator(const kith_allocator_t *alloc)
{
    kith_sim_params_t params = valid_sim_params();
    kith_sim_t *sim = nullptr;
    if (kith_sim_create(&params, alloc, &sim) != 0)
    {
        return 1;
    }
    kith_sim_destroy(sim);
    return 0;
}

/* Requires kith_fabric_create to reject @p alloc with @p want_code and to
 * clear the out slot. The sim scaffold uses the default allocator, so only
 * the fabric create touches the tally. */
static int fabric_rejects_allocator(const kith_allocator_t *alloc, kith_error_t want_code)
{
    kith_sim_t *sim = nullptr;
    if (kith_sim_create(nullptr, nullptr, &sim) != 0)
    {
        return 1;
    }
    kith_fabric_params_t params = valid_fabric_params();
    kith_fabric_t *fabric = (kith_fabric_t *)&sentinel;
    const int rc = kith_fabric_create(&params, sim, alloc, &fabric);
    kith_sim_destroy(sim);
    if (rc != kith_error_return(want_code))
    {
        return 1;
    }
    return fabric != nullptr;
}

/* Requires kith_fabric_create to succeed with @p alloc; releases the handle. */
static int fabric_accepts_allocator(const kith_allocator_t *alloc)
{
    kith_sim_t *sim = nullptr;
    if (kith_sim_create(nullptr, nullptr, &sim) != 0)
    {
        return 1;
    }
    kith_fabric_params_t params = valid_fabric_params();
    kith_fabric_t *fabric = nullptr;
    int rc = 1;
    if (kith_fabric_create(&params, sim, alloc, &fabric) == 0)
    {
        kith_fabric_destroy(fabric);
        rc = 0;
    }
    kith_sim_destroy(sim);
    return rc;
}

/*---------------------------------------------------------------------------
 * gateway
 *-------------------------------------------------------------------------*/

static kith_gateway_params_t valid_gateway_params(void)
{
    return (kith_gateway_params_t)
    {
        .size = sizeof(kith_gateway_params_t), .abi_version = KITH_ABI_VERSION
        ,
    };
}

/* An executor-configured variant: the create path additionally spends the
 * executor struct and the gateway-owned worker pool (pool struct, task
 * nodes, thread array) through the same allocator. */
static kith_gateway_params_t executor_gateway_params(void)
{
    kith_gateway_params_t p = valid_gateway_params();
    p.delivery_worker_count = 1U;
    return p;
}

/* The gateway section's borrowed scaffold: proto, net, sim, and fabric
 * under the default allocator, so only the gateway under test (and the
 * sessions created from it) touch the tally. */
struct gateway_scaffold
{
    kith_proto_t *proto;
    kith_net_t *net;
    kith_sim_t *sim;
    kith_fabric_t *fabric;
};

static void gateway_scaffold_fini(const struct gateway_scaffold *fx)
{
    kith_fabric_destroy(fx->fabric);
    kith_sim_destroy(fx->sim);
    kith_net_destroy(fx->net);
    kith_proto_destroy(fx->proto);
}

static int gateway_scaffold_init(struct gateway_scaffold *fx)
{
    memset(fx, 0, sizeof(*fx));
    if (kith_proto_create(nullptr, nullptr, &fx->proto) != 0)
    {
        return 1;
    }
    if (kith_net_create(nullptr, fx->proto, nullptr, &fx->net) != 0)
    {
        gateway_scaffold_fini(fx);
        return 1;
    }
    if (kith_sim_create(nullptr, nullptr, &fx->sim) != 0)
    {
        gateway_scaffold_fini(fx);
        return 1;
    }
    if (kith_fabric_create(nullptr, fx->sim, nullptr, &fx->fabric) != 0)
    {
        gateway_scaffold_fini(fx);
        return 1;
    }
    return 0;
}

/* Requires kith_gateway_create to reject @p alloc with @p want_code and to
 * leave the out slot cleared. */
static int gateway_rejects_allocator(const kith_allocator_t *alloc, kith_error_t want_code)
{
    struct gateway_scaffold fx;
    if (gateway_scaffold_init(&fx) != 0)
    {
        gateway_scaffold_fini(&fx);
        return 1;
    }
    kith_gateway_t *gw = (kith_gateway_t *)&sentinel;
    const int rc = kith_gateway_create(nullptr, fx.net, fx.fabric, fx.proto, alloc, &gw);
    gateway_scaffold_fini(&fx);
    if (rc != kith_error_return(want_code))
    {
        return 1;
    }
    return gw != nullptr;
}

/* Requires kith_gateway_create to succeed with @p alloc; releases the
 * handle. */
static int gateway_accepts_allocator(const kith_allocator_t *alloc)
{
    struct gateway_scaffold fx;
    if (gateway_scaffold_init(&fx) != 0)
    {
        gateway_scaffold_fini(&fx);
        return 1;
    }
    kith_gateway_t *gw = nullptr;
    int rc = 1;
    if (kith_gateway_create(nullptr, fx.net, fx.fabric, fx.proto, alloc, &gw) == 0)
    {
        kith_gateway_destroy(gw);
        rc = 0;
    }
    gateway_scaffold_fini(&fx);
    return rc;
}

static int gateway_allocator_validation(void)
{
    if (gateway_rejects_allocator(
            &(kith_allocator_t) { .size = 8U, .abi_version = KITH_ABI_VERSION }, KITH_ESIZE))
    {
        return 1;
    }
    if (gateway_rejects_allocator(
            &(kith_allocator_t) {
                .size = sizeof(kith_allocator_t), .abi_version = KITH_ABI_VERSION
                +1U
            },
            KITH_EABIVER))
    {
        return 2;
    }
    if (gateway_rejects_allocator(
            &(kith_allocator_t) {
                .size = sizeof(kith_allocator_t), .abi_version = KITH_ABI_VERSION
                , .alloc = tally_alloc
            },
            KITH_EINVAL))
    {
        return 3;
    }
    if (gateway_accepts_allocator(&g_tally_allocator))
    {
        return 4;
    }
    return 0;
}

/* Requires kith_coord_create to reject @p alloc with @p want_code and to
 * clear the out slot. NULL params and bus select the embedded defaults, so
 * only the allocator under test is exercised. */
static int coord_create_rejects_allocator(const kith_allocator_t *alloc, kith_error_t want_code)
{
    kith_coord_t *coord = (kith_coord_t *)&sentinel;
    const int rc = kith_coord_create(nullptr, nullptr, alloc, &coord);
    if (rc != kith_error_return(want_code))
    {
        return 1;
    }
    return coord != nullptr;
}

/* Requires kith_coord_create to succeed with @p alloc; releases the handle. */
static int coord_create_accepts_allocator(const kith_allocator_t *alloc)
{
    kith_coord_t *coord = nullptr;
    if (kith_coord_create(nullptr, nullptr, alloc, &coord) != 0)
    {
        return 1;
    }
    kith_coord_destroy(coord);
    return 0;
}

/* Requires kith_coord_bus_create to reject @p alloc with @p want_code and
 * to clear the out slot. */
static int coord_bus_rejects_allocator(const kith_allocator_t *alloc, kith_error_t want_code)
{
    kith_coord_bus_t *bus = (kith_coord_bus_t *)&sentinel;
    const int rc = kith_coord_bus_create(nullptr, alloc, &bus);
    if (rc != kith_error_return(want_code))
    {
        return 1;
    }
    return bus != nullptr;
}

/* Requires kith_coord_bus_create to succeed with @p alloc; releases the
 * handle. */
static int coord_bus_accepts_allocator(const kith_allocator_t *alloc)
{
    kith_coord_bus_t *bus = nullptr;
    if (kith_coord_bus_create(nullptr, alloc, &bus) != 0)
    {
        return 1;
    }
    kith_coord_bus_destroy(bus);
    return 0;
}

/* Both coord constructors validate a supplied allocator: the handle create
 * and the bus create reject the same malformed instances with the same
 * codes (1-4 and 5-8 respectively). */
static int coord_allocator_validation(void)
{
    if (coord_create_rejects_allocator(
            &(kith_allocator_t) { .size = 8U, .abi_version = KITH_ABI_VERSION }, KITH_ESIZE))
    {
        return 1;
    }
    if (coord_create_rejects_allocator(
            &(kith_allocator_t) {
                .size = sizeof(kith_allocator_t), .abi_version = KITH_ABI_VERSION
                +1U
            },
            KITH_EABIVER))
    {
        return 2;
    }
    if (coord_create_rejects_allocator(
            &(kith_allocator_t) {
                .size = sizeof(kith_allocator_t), .abi_version = KITH_ABI_VERSION
                , .alloc = tally_alloc
            },
            KITH_EINVAL))
    {
        return 3;
    }
    if (coord_create_accepts_allocator(&g_tally_allocator))
    {
        return 4;
    }
    if (coord_bus_rejects_allocator(
            &(kith_allocator_t) { .size = 8U, .abi_version = KITH_ABI_VERSION }, KITH_ESIZE))
    {
        return 5;
    }
    if (coord_bus_rejects_allocator(
            &(kith_allocator_t) {
                .size = sizeof(kith_allocator_t), .abi_version = KITH_ABI_VERSION
                +1U
            },
            KITH_EABIVER))
    {
        return 6;
    }
    if (coord_bus_rejects_allocator(
            &(kith_allocator_t) {
                .size = sizeof(kith_allocator_t), .abi_version = KITH_ABI_VERSION
                , .alloc = tally_alloc
            },
            KITH_EINVAL))
    {
        return 7;
    }
    if (coord_bus_accepts_allocator(&g_tally_allocator))
    {
        return 8;
    }
    return 0;
}

/* The control plane's constructors need a live reactor to validate
 * against; one scaffold is created lazily and shared across the section's
 * arms (the db section's precedent — ring pages pin against the memlock
 * budget, and per-arm reactors churn that budget for no coverage). */
static kith_reactor_t *g_control_scaffold;

static kith_reactor_t *control_scaffold_reactor(void)
{
    if (g_control_scaffold == nullptr &&
        kith_reactor_create(nullptr, nullptr, &g_control_scaffold) != 0)
    {
        return nullptr;
    }
    return g_control_scaffold;
}

/* Requires kith_control_create to reject @p alloc with @p want_code and to
 * clear the out slot. NULL params selects the size-versioned defaults, so
 * only the allocator under test is exercised. */
static int control_create_rejects_allocator(const kith_allocator_t *alloc, kith_error_t want_code)
{
    kith_reactor_t *reactor = control_scaffold_reactor();
    if (reactor == nullptr)
    {
        return 9;
    }
    kith_control_t *ctrl = (kith_control_t *)&sentinel;
    const int rc = kith_control_create(nullptr, reactor, nullptr, nullptr, alloc, &ctrl);
    if (rc != kith_error_return(want_code))
    {
        return 1;
    }
    return ctrl != nullptr;
}

/* Requires kith_control_create to succeed with @p alloc; releases the
 * handle. */
static int control_create_accepts_allocator(const kith_allocator_t *alloc)
{
    kith_reactor_t *reactor = control_scaffold_reactor();
    if (reactor == nullptr)
    {
        return 9;
    }
    kith_control_t *ctrl = nullptr;
    if (kith_control_create(nullptr, reactor, nullptr, nullptr, alloc, &ctrl) != 0)
    {
        return 1;
    }
    kith_control_destroy(ctrl);
    return 0;
}

/* The control constructor validates a supplied allocator: size, abi
 * version, and operation presence are rejected with the same ladder the
 * params carry. */
static int control_allocator_validation(void)
{
    if (control_create_rejects_allocator(
            &(kith_allocator_t) { .size = 8U, .abi_version = KITH_ABI_VERSION }, KITH_ESIZE))
    {
        return 1;
    }
    if (control_create_rejects_allocator(
            &(kith_allocator_t) {
                .size = sizeof(kith_allocator_t), .abi_version = KITH_ABI_VERSION
                +1U
            },
            KITH_EABIVER))
    {
        return 2;
    }
    if (control_create_rejects_allocator(
            &(kith_allocator_t) {
                .size = sizeof(kith_allocator_t), .abi_version = KITH_ABI_VERSION
                , .alloc = tally_alloc
            },
            KITH_EINVAL))
    {
        return 3;
    }
    if (control_create_accepts_allocator(&g_tally_allocator))
    {
        return 4;
    }
    return 0;
}

/* The client constructor validates against a borrowed proto handle; one
 * scaffold is created lazily and shared across the section's arms (the
 * control section's shared-reactor precedent — the proto carries the
 * default allocator, so scaffold creation is invisible to the tally). */
static kith_proto_t *g_client_scaffold;

static kith_proto_t *client_scaffold_proto(void)
{
    if (g_client_scaffold == nullptr)
    {
        kith_proto_params_t params = {.size = sizeof(kith_proto_params_t),
                                      .abi_version = KITH_ABI_VERSION};
        if (kith_proto_create(&params, nullptr, &g_client_scaffold) != 0)
        {
            return nullptr;
        }
    }
    return g_client_scaffold;
}

/* Requires kith_client_create to reject @p alloc with @p want_code and to
 * clear the out slot. NULL params selects the size-versioned defaults, so
 * only the allocator under test is exercised. */
static int client_create_rejects_allocator(const kith_allocator_t *alloc, kith_error_t want_code)
{
    kith_proto_t *proto = client_scaffold_proto();
    if (proto == nullptr)
    {
        return 9;
    }
    kith_client_t *client = (kith_client_t *)&sentinel;
    const int rc = kith_client_create(nullptr, proto, nullptr, alloc, &client);
    if (rc != kith_error_return(want_code))
    {
        return 1;
    }
    return client != nullptr;
}

/* Requires kith_client_create to succeed with @p alloc; releases the
 * handle. */
static int client_create_accepts_allocator(const kith_allocator_t *alloc)
{
    kith_proto_t *proto = client_scaffold_proto();
    if (proto == nullptr)
    {
        return 9;
    }
    kith_client_t *client = nullptr;
    const int rc = kith_client_create(nullptr, proto, nullptr, alloc, &client);
    if (rc != 0)
    {
        return 1;
    }
    kith_client_destroy(client);
    return 0;
}

/* The client constructor validates a supplied allocator: size, abi
 * version, and operation presence are rejected with the same ladder the
 * params carry. */
static int client_allocator_validation(void)
{
    if (client_create_rejects_allocator(
            &(kith_allocator_t) { .size = 8U, .abi_version = KITH_ABI_VERSION }, KITH_ESIZE))
    {
        return 1;
    }
    if (client_create_rejects_allocator(
            &(kith_allocator_t) {
                .size = sizeof(kith_allocator_t), .abi_version = KITH_ABI_VERSION
                +1U
            },
            KITH_EABIVER))
    {
        return 2;
    }
    if (client_create_rejects_allocator(
            &(kith_allocator_t) {
                .size = sizeof(kith_allocator_t), .abi_version = KITH_ABI_VERSION
                , .alloc = tally_alloc
            },
            KITH_EINVAL))
    {
        return 3;
    }
    if (client_create_accepts_allocator(&g_tally_allocator))
    {
        return 4;
    }
    return 0;
}

/* The routing pin's pass: handle, outbound slot table, event ring, and
 * step table at create; two submits each spend the encode scratch and the
 * queued slot copy; the first pop and the destroy-time clear release the
 * copies — both free routes of the outbound ring in one flow. */
static int test_client_allocator_routing(void)
{
    kith_proto_t *proto = client_scaffold_proto();
    if (proto == nullptr)
    {
        return 9;
    }
    kith_client_t *routed = nullptr;
    tally_reset(0U);
    if (kith_client_create(nullptr, proto, nullptr, &g_tally_allocator, &routed) != 0)
    {
        return 1;
    }
    const kith_client_bootstrap_step_t steps[1] = {{.await_type_id = 200u}};
    if (kith_client_configure_bootstrap(routed, steps, 1) != 0)
    {
        kith_client_destroy(routed);
        return 2;
    }
    const uint8_t payload[4] = {1u, 2u, 3u, 4u};
    const kith_client_command_t command = {
        .type_id = 100u,
        .flags = 0u,
        .correlation_id = 0u,
        .payload = payload,
        .payload_len = sizeof(payload),
    };
    uint8_t sink[128];
    uint32_t len = 0u;
    if (kith_client_submit_interactive(routed, &command, nullptr) != 0 ||
        kith_client_pop_outbound(routed, sink, sizeof(sink), &len) != 0 || len == 0u)
    {
        kith_client_destroy(routed);
        return 3;
    }
    /* The second submit stays queued: destroy clears the occupied slot,
     * exercising the outbound ring's other free route. */
    if (kith_client_submit_interactive(routed, &command, nullptr) != 0)
    {
        kith_client_destroy(routed);
        return 4;
    }
    kith_client_destroy(routed);
    if (g_tally.attempts != 8U || g_tally.frees != 8U || g_tally.outstanding != 0U)
    {
        return 5;
    }
    return 0;
}

/* The server constructor takes no borrowed handles, so a NULL params build
 * reaches the allocator validation before any plane is constructed. The
 * delivery fields ride a tiered configuration image so the strategy and
 * image copies are part of the covered path. */
static kith_server_params_t valid_server_params(void)
{
    static kith_gateway_tiered_config_t tiered;
    tiered.size = sizeof(tiered);
    tiered.abi_version = KITH_ABI_VERSION;
    kith_server_params_t params = {0};
    params.size = sizeof(params);
    params.abi_version = KITH_ABI_VERSION;
    params.topology = KITH_SERVER_TOPOLOGY_EMBEDDED;
    params.delivery_strategy = "tiered";
    params.delivery_config = &tiered;
    return params;
}

/* Requires kith_server_create to reject @p alloc with @p want_code and to
 * clear the out slot. NULL params selects the size-versioned defaults, so
 * only the allocator under test is exercised. */
static int server_create_rejects_allocator(const kith_allocator_t *alloc, kith_error_t want_code)
{
    kith_server_t *server = (kith_server_t *)&sentinel;
    const int rc = kith_server_create(nullptr, alloc, &server);
    if (rc != kith_error_return(want_code))
    {
        return 1;
    }
    return server != nullptr;
}

/* Requires kith_server_create to succeed with @p alloc; releases the
 * handle. */
static int server_create_accepts_allocator(const kith_allocator_t *alloc)
{
    kith_server_t *server = nullptr;
    const int rc = kith_server_create(nullptr, alloc, &server);
    if (rc != 0)
    {
        return 1;
    }
    kith_server_destroy(server);
    return 0;
}

/* The server constructor validates a supplied allocator: size, abi
 * version, and operation presence are rejected with the same ladder the
 * params carry. */
static int server_allocator_validation(void)
{
    if (server_create_rejects_allocator(
            &(kith_allocator_t) { .size = 8U, .abi_version = KITH_ABI_VERSION }, KITH_ESIZE))
    {
        return 1;
    }
    if (server_create_rejects_allocator(
            &(kith_allocator_t) {
                .size = sizeof(kith_allocator_t), .abi_version = KITH_ABI_VERSION
                +1U
            },
            KITH_EABIVER))
    {
        return 2;
    }
    if (server_create_rejects_allocator(
            &(kith_allocator_t) {
                .size = sizeof(kith_allocator_t), .abi_version = KITH_ABI_VERSION
                , .alloc = tally_alloc
            },
            KITH_EINVAL))
    {
        return 3;
    }
    if (server_create_accepts_allocator(&g_tally_allocator))
    {
        return 4;
    }
    return 0;
}

/* The pin's tick handler: requests shutdown from the pool thread so the
 * run loop drains and returns; the dispatch that queued this callback is
 * the pin's one runtime allocation. */
static void server_pin_tick(uint64_t tick, void *user_data)
{
    (void)tick;
    (void)kith_server_shutdown((kith_server_t *)user_data);
}

/* The routing pin's pass: handle, delivery strategy copy, delivery
 * configuration image, and the wire's connection table at create; the tick
 * dispatch adds one work record on the reactor thread, freed by the pool
 * task after the callback runs. Destroy joins the pool before it frees the
 * planes, so the task-side free is always counted by the time the tally is
 * read. */
static int test_server_allocator_routing(void)
{
    kith_server_params_t params = valid_server_params();
    kith_server_t *routed = nullptr;
    int rc = kith_error_return(KITH_EIO);
    for (unsigned attempt = 0u; attempt < 120u && rc == kith_error_return(KITH_EIO); attempt++)
    {
        tally_reset(0U);
        rc = kith_server_create(&params, &g_tally_allocator, &routed);
        if (rc != 0 && rc != kith_error_return(KITH_EIO))
        {
            return 1;
        }
        if (rc != 0)
        {
            struct timespec pause = {.tv_sec = 0, .tv_nsec = 50L * 1000 * 1000};
            (void)nanosleep(&pause, nullptr);
        }
    }
    if (rc != 0)
    {
        return 1;
    }
    if (kith_server_register_tick_handler(
            routed, server_pin_tick, routed, KITH_SERVER_HANDLER_PYTHON) != 0)
    {
        kith_server_destroy(routed);
        return 2;
    }
    if (kith_server_run(routed) != 0)
    {
        kith_server_destroy(routed);
        return 3;
    }
    kith_server_destroy(routed);
    if (g_tally.attempts != 5U || g_tally.frees != 5U || g_tally.outstanding != 0U)
    {
        return 4;
    }
    return 0;
}

static int test_allocator_contract(void)
{
    const kith_allocator_t *def = kith_allocator_default();
    if (def->size != sizeof(kith_allocator_t) || def->abi_version != KITH_ABI_VERSION ||
        def->alloc == nullptr || def->alloc_zero == nullptr || def->realloc == nullptr ||
        def->free == nullptr)
    {
        return 1;
    }
    if (kith_allocator_check(def) != 0)
    {
        return 2;
    }
    /* A NULL alloc argument and the default instance are interchangeable:
     * both builds of the same handle draw identically. */
    kith_rng_params_t params = valid_rng_params(0x5EEDULL);
    kith_rng_t *via_null = nullptr;
    kith_rng_t *via_default = nullptr;
    if (kith_rng_create(&params, nullptr, &via_null) != 0 ||
        kith_rng_create(&params, kith_allocator_default(), &via_default) != 0)
    {
        return 3;
    }
    if (kith_rng_next(via_null) != kith_rng_next(via_default))
    {
        return 4;
    }
    kith_rng_destroy(via_null);
    kith_rng_destroy(via_default);
    /* The default instance round-trips all four operations. */
    unsigned char *buf = kith_alloc(def, 32U);
    if (buf == nullptr)
    {
        return 5;
    }
    for (unsigned i = 0; i < 32U; i++)
    {
        buf[i] = (unsigned char)i;
    }
    unsigned char *zeroed = kith_alloc_zero(def, 8U, 16U);
    if (zeroed == nullptr)
    {
        kith_free(def, buf);
        return 6;
    }
    for (unsigned i = 0; i < 8U * 16U; i++)
    {
        if (zeroed[i] != 0)
        {
            kith_free(def, zeroed);
            kith_free(def, buf);
            return 6;
        }
    }
    unsigned char *grown = kith_realloc(def, buf, 64U);
    if (grown == nullptr || grown[0] != 0U || grown[31] != 31U)
    {
        kith_free(def, zeroed);
        kith_free(def, grown);
        return 7;
    }
    kith_free(def, zeroed);
    kith_free(def, grown);
    kith_free(def, nullptr);
    return 0;
}

/* One reject case per allocator-contract clause plus one accept, per
 * module: an undersized instance, a foreign abi_version, a missing
 * operation, and a hand-built complete allocator passing the same
 * validation. Returns 1-4 for the failing clause, 0 when the module's
 * create accepts and rejects exactly as the contract requires. */
static int rng_allocator_validation(void)
{
    if (rng_rejects_allocator(
            &(kith_allocator_t) { .size = 8U, .abi_version = KITH_ABI_VERSION }, KITH_ESIZE))
    {
        return 1;
    }
    if (rng_rejects_allocator(
            &(kith_allocator_t) {
                .size = sizeof(kith_allocator_t), .abi_version = KITH_ABI_VERSION
                +1U
            },
            KITH_EABIVER))
    {
        return 2;
    }
    if (rng_rejects_allocator(
            &(kith_allocator_t) {
                .size = sizeof(kith_allocator_t), .abi_version = KITH_ABI_VERSION
                , .alloc = tally_alloc
            },
            KITH_EINVAL))
    {
        return 3;
    }
    if (rng_accepts_allocator(&g_tally_allocator))
    {
        return 4;
    }
    return 0;
}

static int config_allocator_validation(void)
{
    if (config_rejects_allocator(
            &(kith_allocator_t) { .size = 8U, .abi_version = KITH_ABI_VERSION }, KITH_ESIZE))
    {
        return 1;
    }
    if (config_rejects_allocator(
            &(kith_allocator_t) {
                .size = sizeof(kith_allocator_t), .abi_version = KITH_ABI_VERSION
                +1U
            },
            KITH_EABIVER))
    {
        return 2;
    }
    if (config_rejects_allocator(
            &(kith_allocator_t) {
                .size = sizeof(kith_allocator_t), .abi_version = KITH_ABI_VERSION
                , .alloc = tally_alloc
            },
            KITH_EINVAL))
    {
        return 3;
    }
    if (config_accepts_allocator(&g_tally_allocator))
    {
        return 4;
    }
    return 0;
}

static int logger_allocator_validation(void)
{
    if (logger_rejects_allocator(
            &(kith_allocator_t) { .size = 8U, .abi_version = KITH_ABI_VERSION }, KITH_ESIZE))
    {
        return 1;
    }
    if (logger_rejects_allocator(
            &(kith_allocator_t) {
                .size = sizeof(kith_allocator_t), .abi_version = KITH_ABI_VERSION
                +1U
            },
            KITH_EABIVER))
    {
        return 2;
    }
    if (logger_rejects_allocator(
            &(kith_allocator_t) {
                .size = sizeof(kith_allocator_t), .abi_version = KITH_ABI_VERSION
                , .alloc = tally_alloc
            },
            KITH_EINVAL))
    {
        return 3;
    }
    if (logger_accepts_allocator(&g_tally_allocator))
    {
        return 4;
    }
    return 0;
}

static int metrics_allocator_validation(void)
{
    if (metrics_rejects_allocator(
            &(kith_allocator_t) { .size = 8U, .abi_version = KITH_ABI_VERSION }, KITH_ESIZE))
    {
        return 1;
    }
    if (metrics_rejects_allocator(
            &(kith_allocator_t) {
                .size = sizeof(kith_allocator_t), .abi_version = KITH_ABI_VERSION
                +1U
            },
            KITH_EABIVER))
    {
        return 2;
    }
    if (metrics_rejects_allocator(
            &(kith_allocator_t) {
                .size = sizeof(kith_allocator_t), .abi_version = KITH_ABI_VERSION
                , .alloc = tally_alloc
            },
            KITH_EINVAL))
    {
        return 3;
    }
    if (metrics_accepts_allocator(&g_tally_allocator))
    {
        return 4;
    }
    return 0;
}

static int proto_allocator_validation(void)
{
    if (proto_rejects_allocator(
            &(kith_allocator_t) { .size = 8U, .abi_version = KITH_ABI_VERSION }, KITH_ESIZE))
    {
        return 1;
    }
    if (proto_rejects_allocator(
            &(kith_allocator_t) {
                .size = sizeof(kith_allocator_t), .abi_version = KITH_ABI_VERSION
                +1U
            },
            KITH_EABIVER))
    {
        return 2;
    }
    if (proto_rejects_allocator(
            &(kith_allocator_t) {
                .size = sizeof(kith_allocator_t), .abi_version = KITH_ABI_VERSION
                , .alloc = tally_alloc
            },
            KITH_EINVAL))
    {
        return 3;
    }
    if (proto_accepts_allocator(&g_tally_allocator))
    {
        return 4;
    }
    return 0;
}

static int net_allocator_validation(void)
{
    if (net_rejects_allocator(
            &(kith_allocator_t) { .size = 8U, .abi_version = KITH_ABI_VERSION }, KITH_ESIZE))
    {
        return 1;
    }
    if (net_rejects_allocator(
            &(kith_allocator_t) {
                .size = sizeof(kith_allocator_t), .abi_version = KITH_ABI_VERSION
                +1U
            },
            KITH_EABIVER))
    {
        return 2;
    }
    if (net_rejects_allocator(
            &(kith_allocator_t) {
                .size = sizeof(kith_allocator_t), .abi_version = KITH_ABI_VERSION
                , .alloc = tally_alloc
            },
            KITH_EINVAL))
    {
        return 3;
    }
    if (net_accepts_allocator(&g_tally_allocator))
    {
        return 4;
    }
    return 0;
}

static int reactor_allocator_validation(void)
{
    if (reactor_rejects_allocator(
            &(kith_allocator_t) { .size = 8U, .abi_version = KITH_ABI_VERSION }, KITH_ESIZE))
    {
        return 1;
    }
    if (reactor_rejects_allocator(
            &(kith_allocator_t) {
                .size = sizeof(kith_allocator_t), .abi_version = KITH_ABI_VERSION
                +1U
            },
            KITH_EABIVER))
    {
        return 2;
    }
    if (reactor_rejects_allocator(
            &(kith_allocator_t) {
                .size = sizeof(kith_allocator_t), .abi_version = KITH_ABI_VERSION
                , .alloc = tally_alloc
            },
            KITH_EINVAL))
    {
        return 3;
    }
    if (reactor_accepts_allocator(&g_tally_allocator))
    {
        return 4;
    }
    return 0;
}

static int worker_allocator_validation(void)
{
    if (worker_rejects_allocator(
            &(kith_allocator_t) { .size = 8U, .abi_version = KITH_ABI_VERSION }, KITH_ESIZE))
    {
        return 1;
    }
    if (worker_rejects_allocator(
            &(kith_allocator_t) {
                .size = sizeof(kith_allocator_t), .abi_version = KITH_ABI_VERSION
                +1U
            },
            KITH_EABIVER))
    {
        return 2;
    }
    if (worker_rejects_allocator(
            &(kith_allocator_t) {
                .size = sizeof(kith_allocator_t), .abi_version = KITH_ABI_VERSION
                , .alloc = tally_alloc
            },
            KITH_EINVAL))
    {
        return 3;
    }
    if (worker_accepts_allocator(&g_tally_allocator))
    {
        return 4;
    }
    return 0;
}

static int state_allocator_validation(void)
{
    if (state_rejects_allocator(
            &(kith_allocator_t) { .size = 8U, .abi_version = KITH_ABI_VERSION }, KITH_ESIZE))
    {
        return 1;
    }
    if (state_rejects_allocator(
            &(kith_allocator_t) {
                .size = sizeof(kith_allocator_t), .abi_version = KITH_ABI_VERSION
                +1U
            },
            KITH_EABIVER))
    {
        return 2;
    }
    if (state_rejects_allocator(
            &(kith_allocator_t) {
                .size = sizeof(kith_allocator_t), .abi_version = KITH_ABI_VERSION
                , .alloc = tally_alloc
            },
            KITH_EINVAL))
    {
        return 3;
    }
    if (state_accepts_allocator(&g_tally_allocator))
    {
        return 4;
    }
    return 0;
}

static int db_allocator_validation(void)
{
    if (db_rejects_allocator(
            &(kith_allocator_t) { .size = 8U, .abi_version = KITH_ABI_VERSION }, KITH_ESIZE))
    {
        return 1;
    }
    if (db_rejects_allocator(
            &(kith_allocator_t) {
                .size = sizeof(kith_allocator_t), .abi_version = KITH_ABI_VERSION
                +1U
            },
            KITH_EABIVER))
    {
        return 2;
    }
    if (db_rejects_allocator(
            &(kith_allocator_t) {
                .size = sizeof(kith_allocator_t), .abi_version = KITH_ABI_VERSION
                , .alloc = tally_alloc
            },
            KITH_EINVAL))
    {
        return 3;
    }
    if (db_accepts_allocator(&g_tally_allocator))
    {
        return 4;
    }
    return 0;
}

static int aoi_allocator_validation(void)
{
    if (aoi_rejects_allocator(
            &(kith_allocator_t) { .size = 8U, .abi_version = KITH_ABI_VERSION }, KITH_ESIZE))
    {
        return 1;
    }
    if (aoi_rejects_allocator(
            &(kith_allocator_t) {
                .size = sizeof(kith_allocator_t), .abi_version = KITH_ABI_VERSION
                +1U
            },
            KITH_EABIVER))
    {
        return 2;
    }
    if (aoi_rejects_allocator(
            &(kith_allocator_t) {
                .size = sizeof(kith_allocator_t), .abi_version = KITH_ABI_VERSION
                , .alloc = tally_alloc
            },
            KITH_EINVAL))
    {
        return 3;
    }
    if (aoi_accepts_allocator(&g_tally_allocator))
    {
        return 4;
    }
    return 0;
}

static int sim_allocator_validation(void)
{
    if (sim_rejects_allocator(
            &(kith_allocator_t) { .size = 8U, .abi_version = KITH_ABI_VERSION }, KITH_ESIZE))
    {
        return 1;
    }
    if (sim_rejects_allocator(
            &(kith_allocator_t) {
                .size = sizeof(kith_allocator_t), .abi_version = KITH_ABI_VERSION
                +1U
            },
            KITH_EABIVER))
    {
        return 2;
    }
    if (sim_rejects_allocator(
            &(kith_allocator_t) {
                .size = sizeof(kith_allocator_t), .abi_version = KITH_ABI_VERSION
                , .alloc = tally_alloc
            },
            KITH_EINVAL))
    {
        return 3;
    }
    if (sim_accepts_allocator(&g_tally_allocator))
    {
        return 4;
    }
    return 0;
}

static int fabric_allocator_validation(void)
{
    if (fabric_rejects_allocator(
            &(kith_allocator_t) { .size = 8U, .abi_version = KITH_ABI_VERSION }, KITH_ESIZE))
    {
        return 1;
    }
    if (fabric_rejects_allocator(
            &(kith_allocator_t) {
                .size = sizeof(kith_allocator_t), .abi_version = KITH_ABI_VERSION
                +1U
            },
            KITH_EABIVER))
    {
        return 2;
    }
    if (fabric_rejects_allocator(
            &(kith_allocator_t) {
                .size = sizeof(kith_allocator_t), .abi_version = KITH_ABI_VERSION
                , .alloc = tally_alloc
            },
            KITH_EINVAL))
    {
        return 3;
    }
    if (fabric_accepts_allocator(&g_tally_allocator))
    {
        return 4;
    }
    return 0;
}

/* The eight latest module validations, composed with their offsets: the
 * composite return keeps the failing module recoverable from the exit
 * code (40 aoi, 44 sim, 48 fabric, 52 gateway, 56 coord, 60 control,
 * 64 client, 68 server). */
static int test_late_allocator_validations(void)
{
    int rc = aoi_allocator_validation();
    if (rc != 0)
    {
        return 40 + rc;
    }
    rc = sim_allocator_validation();
    if (rc != 0)
    {
        return 44 + rc;
    }
    rc = fabric_allocator_validation();
    if (rc != 0)
    {
        return 48 + rc;
    }
    rc = gateway_allocator_validation();
    if (rc != 0)
    {
        return 52 + rc;
    }
    rc = coord_allocator_validation();
    if (rc != 0)
    {
        return 56 + rc;
    }
    rc = control_allocator_validation();
    if (rc != 0)
    {
        return 60 + rc;
    }
    rc = client_allocator_validation();
    if (rc != 0)
    {
        return 64 + rc;
    }
    rc = server_allocator_validation();
    if (rc != 0)
    {
        return 68 + rc;
    }
    return 0;
}

static int test_allocator_validation(void)
{
    int rc = rng_allocator_validation();
    if (rc != 0)
    {
        return rc;
    }
    rc = config_allocator_validation();
    if (rc != 0)
    {
        return 4 + rc;
    }
    rc = logger_allocator_validation();
    if (rc != 0)
    {
        return 8 + rc;
    }
    rc = metrics_allocator_validation();
    if (rc != 0)
    {
        return 12 + rc;
    }
    rc = proto_allocator_validation();
    if (rc != 0)
    {
        return 16 + rc;
    }
    rc = net_allocator_validation();
    if (rc != 0)
    {
        return 20 + rc;
    }
    rc = reactor_allocator_validation();
    if (rc != 0)
    {
        return 24 + rc;
    }
    rc = worker_allocator_validation();
    if (rc != 0)
    {
        return 28 + rc;
    }
    rc = state_allocator_validation();
    if (rc != 0)
    {
        return 32 + rc;
    }
    rc = db_allocator_validation();
    if (rc != 0)
    {
        return 36 + rc;
    }
    return test_late_allocator_validations();
}

static int test_custom_allocator_routing(void)
{
    kith_rng_params_t params = valid_rng_params(0x12345678ULL);
    kith_rng_t *routed = nullptr;
    kith_rng_t *plain = nullptr;
    tally_reset(0U);
    if (kith_rng_create(&params, &g_tally_allocator, &routed) != 0 ||
        kith_rng_create(&params, nullptr, &plain) != 0)
    {
        return 1;
    }
    /* One allocation at create; the draw sequences agree because routing
     * must not touch the algorithm. */
    if (g_tally.attempts != 1U || kith_rng_next(routed) != kith_rng_next(plain))
    {
        kith_rng_destroy(plain);
        kith_rng_destroy(routed);
        return 2;
    }
    kith_rng_destroy(plain);
    kith_rng_destroy(routed);
    if (g_tally.frees != 1U)
    {
        return 3;
    }
    return 0;
}

static int test_config_allocator_routing(void)
{
    if (setenv("KITH_ALLOC_ROUTE_alpha", "one", 1) != 0)
    {
        return 1;
    }
    kith_config_params_t params = valid_config_params(nullptr, "KITH_ALLOC_ROUTE_");
    kith_config_t *routed = nullptr;
    tally_reset(0U);
    const int rc = kith_config_create(&params, &g_tally_allocator, &routed);
    (void)unsetenv("KITH_ALLOC_ROUTE_alpha");
    if (rc != 0)
    {
        return 2;
    }
    /* Routing must not disturb the lookup the allocator built. The counts
     * pin the route: one attempt each for the handle, the keybuf, the
     * entry-table growth, and the key and value copies, mirrored by the
     * same number of frees through the stored instance at destroy. */
    const char *s = "";
    if (kith_config_string(routed, "alpha", &s) != 0 || strcmp(s, "one") != 0)
    {
        kith_config_destroy(routed);
        return 3;
    }
    kith_config_destroy(routed);
    if (g_tally.attempts != 5U || g_tally.frees != 5U)
    {
        return 4;
    }
    return 0;
}

static int test_logger_allocator_routing(void)
{
    static char jsonl_path[] = "/tmp/kith_alloc_route.jsonl";
    kith_logger_params_t params = valid_logger_params("route");
    params.jsonl_path = jsonl_path;
    kith_logger_t *routed = nullptr;
    tally_reset(0U);
    const int rc = kith_logger_create(&params, &g_tally_allocator, &routed);
    if (rc != 0)
    {
        (void)remove(jsonl_path);
        return 1;
    }
    /* Routing must not disturb the record: one ERROR entry writes cleanly
     * through the stored instance. The counts pin the route — one attempt
     * each for the handle and the name copy at create, one for the record
     * buffer on the write, mirrored by the same number of frees through the
     * stored instance at destroy. */
    const int log_rc =
        kith_logger_log(routed, KITH_LOG_LEVEL_ERROR, "t.c", 1U, nullptr, 0U, "routed entry");
    kith_logger_destroy(routed);
    (void)remove(jsonl_path);
    if (log_rc != 0 || g_tally.attempts != 3U || g_tally.frees != 3U || g_tally.outstanding != 0U)
    {
        return 2;
    }
    return 0;
}

static int test_metrics_allocator_routing(void)
{
    static const kith_metrics_label_t labels_tcp[] = {{"transport", "tcp"}};
    kith_metrics_params_t params = valid_metrics_params("kith_");
    kith_metrics_t *routed = nullptr;
    tally_reset(0U);
    if (kith_metrics_create(&params, &g_tally_allocator, &routed) != 0)
    {
        return 1;
    }
    /* Routing must not disturb the record: the counter observation and both
     * renders see the same registry. The counts pin the route — the handle
     * and the prefix copy at create, the series-array growth, the series
     * name, the label array, and the label key and value on the first
     * observation, one order array for the Prometheus render and order plus
     * group keys for the OTLP render, mirrored by the same number of frees
     * through the stored instance at destroy. */
    if (kith_metrics_counter_add(routed, "route_total", labels_tcp, 1U, 1U) != 0)
    {
        kith_metrics_destroy(routed);
        return 2;
    }
    if (kith_metrics_render_prometheus(routed, nullptr, 0U) == 0U ||
        kith_metrics_export_otlp(routed, nullptr, 0U) == 0U)
    {
        kith_metrics_destroy(routed);
        return 3;
    }
    kith_metrics_destroy(routed);
    if (g_tally.attempts != 10U || g_tally.frees != 10U || g_tally.outstanding != 0U)
    {
        return 4;
    }
    return 0;
}

static int test_proto_allocator_routing(void)
{
    kith_proto_t *routed = nullptr;
    tally_reset(0U);
    if (kith_proto_create(nullptr, &g_tally_allocator, &routed) != 0)
    {
        return 1;
    }
    /* Register exactly the first capacity window (16 entries), so the type
     * array's only growth is the empty-to-allocated one. The counts pin the
     * route — the handle at create, the type array, and one name copy per
     * registration, mirrored by the same number of frees through the stored
     * instance at destroy. */
    for (uint16_t i = 0; i < 16U; ++i)
    {
        char name[16];
        (void)snprintf(name, sizeof(name), "route_%u", (unsigned)i);
        if (kith_proto_register_type_id(routed, name, (uint16_t)(1000U + i)) != 0)
        {
            kith_proto_destroy(routed);
            return 2;
        }
    }
    uint16_t id = 0;
    if (kith_proto_lookup_type(routed, "route_7", &id) != 0 || id != 1007U)
    {
        kith_proto_destroy(routed);
        return 3;
    }
    kith_proto_destroy(routed);
    if (g_tally.attempts != 18U || g_tally.frees != 18U || g_tally.outstanding != 0U)
    {
        return 4;
    }
    return 0;
}

static int test_net_allocator_routing(void)
{
    kith_proto_t *proto = nullptr;
    if (kith_proto_create(nullptr, nullptr, &proto) != 0)
    {
        return 1;
    }
    kith_net_t *routed = nullptr;
    tally_reset(0U);
    if (kith_net_create(nullptr, proto, &g_tally_allocator, &routed) != 0)
    {
        kith_proto_destroy(proto);
        return 1;
    }
    /* Routing must not disturb the transport: one frame allocates, carries
     * bytes, and releases cleanly. The counts pin the route — the handle
     * and the connection table at create plus one frame from the pool's
     * empty class, mirrored by the same number of frees through the stored
     * instance (the recycled frame at pool destroy, or at its release where
     * the sanitizer build bypasses the pool). */
    kith_net_frame_t *frame = kith_net_frame_create(routed, 16U);
    if (frame == nullptr)
    {
        kith_net_destroy(routed);
        kith_proto_destroy(proto);
        return 2;
    }
    (void)memset(kith_net_frame_data(frame), 0xAB, kith_net_frame_len(frame));
    kith_net_frame_release(frame);
    kith_net_destroy(routed);
    kith_proto_destroy(proto);
    if (g_tally.attempts != 3U || g_tally.frees != 3U || g_tally.outstanding != 0U)
    {
        return 3;
    }
    return 0;
}

static int test_reactor_allocator_routing(void)
{
    kith_reactor_params_t params = valid_reactor_params();
    kith_reactor_t *routed = nullptr;
    tally_reset(0U);
    if (kith_reactor_create(&params, &g_tally_allocator, &routed) != 0)
    {
        return 1;
    }
    /* Routing must not disturb the loop: one timer arms on the wheel and
     * destroy releases it with everything create built. The counts pin the
     * route — handle, fd table, two task nodes, backend state, and the
     * backend's fd table at create, plus the timer node — mirrored by the
     * same number of frees through the stored instance. */
    if (kith_reactor_schedule(routed, 1U, reactor_noop_task, nullptr) != 0)
    {
        kith_reactor_destroy(routed);
        return 2;
    }
    kith_reactor_destroy(routed);
    if (g_tally.attempts != 7U || g_tally.frees != 7U || g_tally.outstanding != 0U)
    {
        return 3;
    }
    return 0;
}

static int test_worker_allocator_routing(void)
{
    kith_worker_params_t params = valid_worker_params();
    kith_worker_t *routed = nullptr;
    kith_worker_bridge_t *bridge = nullptr;
    tally_reset(0U);
    if (kith_worker_create(&params, &g_tally_allocator, &routed) != 0)
    {
        return 1;
    }
    if (kith_worker_bridge_create_ready(routed, worker_noop_ready, nullptr, &bridge) != 0)
    {
        kith_worker_destroy(routed);
        return 2;
    }
    /* Routing must not disturb the pool: one fire submits a work record and
     * destroy drains it before releasing what create built. The counts pin
     * the route — handle, node pool, thread array, bridge, and the work
     * record — mirrored by the same number of frees through the stored
     * instance. The record's free runs on a worker thread during destroy's
     * drain, so the pool is destroyed before the bridge: the join orders
     * that free ahead of the bridge's, keeping the tally race-free. */
    kith_worker_bridge_ready_cb(7, 1U, bridge);
    kith_worker_destroy(routed);
    kith_worker_bridge_destroy(bridge);
    if (g_tally.attempts != 5U || g_tally.frees != 5U || g_tally.outstanding != 0U)
    {
        return 3;
    }
    return 0;
}

static int test_state_allocator_routing(void)
{
    kith_reactor_t *reactor = nullptr;
    if (kith_reactor_create(nullptr, nullptr, &reactor) != 0)
    {
        return 1;
    }
    kith_state_params_t params = valid_state_params();
    kith_state_t *routed = nullptr;
    tally_reset(0U);
    if (kith_state_create(&params, reactor, &g_tally_allocator, &routed) != 0)
    {
        kith_reactor_destroy(reactor);
        return 1;
    }
    /* Routing must not disturb the store: create and destroy are the whole
     * observable surface without a live Redis — the hiredis context, its
     * buffers, and replies are hiredis-allocated and stay outside the tally.
     * The counts pin the route: one attempt for the handle, mirrored by one
     * free through the stored instance at destroy. */
    kith_state_destroy(routed);
    kith_reactor_destroy(reactor);
    if (g_tally.attempts != 1U || g_tally.frees != 1U || g_tally.outstanding != 0U)
    {
        return 2;
    }
    return 0;
}

static int test_db_allocator_routing(void)
{
    kith_reactor_t *reactor = db_scaffold_reactor();
    if (reactor == nullptr)
    {
        return 1;
    }
    kith_db_params_t params = valid_db_params();
    kith_db_t *routed = nullptr;
    tally_reset(0U);
    if (kith_db_create(&params, reactor, &g_tally_allocator, &routed) != 0)
    {
        return 1;
    }
    /* Routing must not disturb the pool: the registry reads and writes are
     * the whole observable surface without a live Postgres — no loop is
     * driven. The counts pin the route — the handle, the host copy, and
     * the connection table at create (no password: the sweep covers that
     * copy), the registry entry, its name, and its SQL on the
     * registration, mirrored by the same number of frees through the
     * stored instance at destroy. */
    if (kith_db_register_query(routed, "route_q", "SELECT $1", 1U) != 0)
    {
        kith_db_destroy(routed);
        return 2;
    }
    uint32_t n_params = 0U;
    if (kith_db_lookup_query(routed, "route_q", &n_params) != 0 || n_params != 1U)
    {
        kith_db_destroy(routed);
        return 3;
    }
    kith_db_destroy(routed);
    if (g_tally.attempts != 6U || g_tally.frees != 6U || g_tally.outstanding != 0U)
    {
        return 4;
    }
    return 0;
}

static int test_aoi_allocator_routing(void)
{
    kith_aoi_params_t params = valid_aoi_params();
    kith_aoi_t *routed = nullptr;
    tally_reset(0U);
    if (kith_aoi_create(&params, &g_tally_allocator, &routed) != 0)
    {
        return 1;
    }
    /* Routing must not disturb the index: five objects share one cell (the
     * first member array, then its in-place growth past the initial
     * capacity), and one update moves an object to a fresh cell (a third
     * member array). The counts pin the route — the handle and the two
     * indexes at create plus the three member arrays, mirrored at destroy
     * by five frees through the stored instance: the in-place growth
     * replaced its block without a free call, so six attempts release as
     * five frees. */
    for (uint64_t i = 1u; i <= 5u; ++i)
    {
        kith_aoi_object_t obj = valid_aoi_object(i, (int64_t)i - 1u);
        if (kith_aoi_insert(routed, &obj) != 0)
        {
            kith_aoi_destroy(routed);
            return 2;
        }
    }
    kith_aoi_object_t moved = valid_aoi_object(1u, 32);
    if (kith_aoi_update(routed, &moved) != 0)
    {
        kith_aoi_destroy(routed);
        return 3;
    }
    kith_aoi_destroy(routed);
    if (g_tally.attempts != 6U || g_tally.frees != 5U || g_tally.outstanding != 0U)
    {
        return 4;
    }
    return 0;
}

static int test_sim_allocator_routing(void)
{
    kith_sim_params_t params = valid_sim_params();
    kith_sim_t *routed = nullptr;
    tally_reset(0U);
    if (kith_sim_create(&params, &g_tally_allocator, &routed) != 0)
    {
        return 1;
    }
    /* Routing must not disturb the store. Five actors share one cell (the
     * first member array, then its in-place growth past the initial
     * capacity), one publish moves an actor to a fresh cell, seven more
     * actors take cells of their own, and the thirteenth distinct actor
     * trips the shard grow (five fresh tables plus the dense-array
     * realloc). The counts pin the route: 101 at create (handle, registry,
     * two name copies, store, sixteen shards times six tables) plus ten
     * member arrays and the grow's six attempts, mirrored at destroy by
     * 116 frees — the in-place member growth and the grow's dense realloc
     * each released their replaced block without a free call, and the grow
     * released the five displaced tables. */
    kith_sim_actor_t actor = valid_sim_actor(1u);
    kith_sim_artifact_key_t key = valid_sim_key(0);
    if (kith_sim_publish_artifact(routed, &key, &actor, nullptr) != 0)
    {
        kith_sim_destroy(routed);
        return 2;
    }
    for (uint64_t i = 2u; i <= 5u; ++i)
    {
        actor = valid_sim_actor(i);
        if (kith_sim_publish_artifact(routed, &key, &actor, nullptr) != 0)
        {
            kith_sim_destroy(routed);
            return 2;
        }
    }
    actor = valid_sim_actor(1u);
    key.cell_z = 1;
    if (kith_sim_publish_artifact(routed, &key, &actor, nullptr) != 0)
    {
        kith_sim_destroy(routed);
        return 3;
    }
    for (uint64_t i = 6u; i <= 12u; ++i)
    {
        actor = valid_sim_actor(i);
        key.cell_z = (int32_t)i;
        if (kith_sim_publish_artifact(routed, &key, &actor, nullptr) != 0)
        {
            kith_sim_destroy(routed);
            return 2;
        }
    }
    actor = valid_sim_actor(13u);
    key.cell_z = 13;
    if (kith_sim_publish_artifact(routed, &key, &actor, nullptr) != 0)
    {
        kith_sim_destroy(routed);
        return 2;
    }
    kith_sim_destroy(routed);
    if (g_tally.attempts != 118U || g_tally.frees != 116U || g_tally.outstanding != 0U)
    {
        return 4;
    }
    return 0;
}

/* The routing pin's sim scaffold: one artifact in cell_z 0 and three in
 * cell_z 1 (default allocator, so only the fabric touches the tally); the
 * second snapshot outgrows the scratch the first one sized. Returns NULL
 * when the scaffold cannot be built. */
static kith_sim_t *fabric_routing_scaffold_sim(void)
{
    kith_sim_t *sim = nullptr;
    if (kith_sim_create(nullptr, nullptr, &sim) != 0)
    {
        return nullptr;
    }
    kith_sim_actor_t actor = valid_sim_actor(1u);
    kith_sim_artifact_key_t sim_key = valid_sim_key(0);
    if (kith_sim_publish_artifact(sim, &sim_key, &actor, nullptr) != 0)
    {
        kith_sim_destroy(sim);
        return nullptr;
    }
    for (uint64_t i = 2u; i <= 4u; ++i)
    {
        actor = valid_sim_actor(i);
        sim_key = valid_sim_key(1);
        if (kith_sim_publish_artifact(sim, &sim_key, &actor, nullptr) != 0)
        {
            kith_sim_destroy(sim);
            return nullptr;
        }
    }
    return sim;
}

/* The pin's two subscriptions: the first grows the subscription table, and
 * cell_z 1 joins its interest set. Returns nonzero on the first failure
 * with the subscriptions released. */
static int fabric_routing_subscribe(kith_fabric_t *routed,
                                    kith_fabric_subscription_t **sub_a,
                                    kith_fabric_subscription_t **sub_b)
{
    if (kith_fabric_create_subscription(routed, sub_a) != 0 ||
        kith_fabric_create_subscription(routed, sub_b) != 0)
    {
        return 1;
    }
    const kith_fabric_cell_key_t key = {
        .zone = 0u, .cell_x = 0, .cell_y = 0, .cell_z = 1, .lod = 0u};
    return kith_fabric_subscription_add(*sub_a, &key) != 0;
}

/* One snapshot of cell_z @p cell_z rendered at full level, requiring @p
 * want artifacts in the caller's four-slot buffer. */
static int fabric_routing_snapshot_expect(kith_fabric_t *routed, int32_t cell_z, size_t want)
{
    const kith_fabric_cell_key_t key = {
        .zone = 0u, .cell_x = 0, .cell_y = 0, .cell_z = cell_z, .lod = 0u};
    kith_fabric_artifact_t arts[4];
    size_t count = 0u;
    if (kith_fabric_snapshot_cell(routed, &key, KITH_FABRIC_LEVEL_FULL, arts, 4u, &count) != 0 ||
        count != want)
    {
        return 1;
    }
    return 0;
}

/* The pin's settle steps: the subscribed cell drains (one pending product)
 * and then leaves the store (one product remains). Returns nonzero on the
 * first failure. */
static int fabric_routing_settle(kith_fabric_t *routed, kith_fabric_subscription_t *sub_a)
{
    size_t count = 0u;
    if (kith_fabric_drain(routed, sub_a, nullptr, 0u, &count) != 0 || count != 1u)
    {
        return 1;
    }
    const kith_fabric_cell_key_t key = {
        .zone = 0u, .cell_x = 0, .cell_y = 0, .cell_z = 1, .lod = 0u};
    if (kith_fabric_remove_cell(routed, &key) != 0 || kith_fabric_product_count(routed) != 1u)
    {
        return 2;
    }
    return 0;
}

static int test_fabric_allocator_routing(void)
{
    kith_sim_t *sim = fabric_routing_scaffold_sim();
    if (sim == nullptr)
    {
        return 1;
    }
    kith_fabric_params_t params = valid_fabric_params();
    kith_fabric_t *routed = nullptr;
    tally_reset(0U);
    if (kith_fabric_create(&params, sim, &g_tally_allocator, &routed) != 0)
    {
        kith_sim_destroy(sim);
        return 1;
    }
    /* Routing must not disturb the stream: two subscriptions join (the
     * first also grows the subscription table), two publishes take cells
     * of their own (cell nodes are array slots, never allocations) and
     * mark the subscribed cell pending, the snapshots size and then
     * regrow the scratch, and a remove tombstones without allocating. The
     * counts pin the route — the handle and the sixteen shard tables at
     * create, the table and the two subscriptions at their creates, the
     * two scratch allocations — mirrored at destroy by 23 frees through
     * the stored instance: the scratch regrowth released its replaced
     * block without a free call, so 24 attempts release as 23 frees. */
    kith_fabric_subscription_t *sub_a = nullptr;
    kith_fabric_subscription_t *sub_b = nullptr;
    int step = fabric_routing_subscribe(routed, &sub_a, &sub_b);
    if (step == 0)
    {
        const kith_fabric_cell_key_t key = {
            .zone = 0u, .cell_x = 0, .cell_y = 0, .cell_z = 1, .lod = 0u};
        step = kith_fabric_publish(routed, &key, 1u, nullptr) != 0;
        const kith_fabric_cell_key_t other = {
            .zone = 0u, .cell_x = 0, .cell_y = 0, .cell_z = 0, .lod = 0u};
        if (step == 0)
        {
            step = kith_fabric_publish(routed, &other, 1u, nullptr) != 0;
        }
        if (step == 0)
        {
            step = fabric_routing_snapshot_expect(routed, 0, 1u) * 2;
        }
        if (step == 0)
        {
            step = fabric_routing_snapshot_expect(routed, 1, 3u) * 2;
        }
        if (step == 0)
        {
            const int settled = fabric_routing_settle(routed, sub_a);
            step = settled != 0 ? 4 + settled : 0;
        }
    }
    kith_fabric_destroy(routed);
    kith_sim_destroy(sim);
    if (step != 0 || g_tally.attempts != 24U || g_tally.frees != 23U || g_tally.outstanding != 0U)
    {
        return step != 0 ? step : 7;
    }
    return 0;
}

/* One published artifact per cell (subscriber seed in cell_z 0, three
 * candidates in cell_z 1), so the pin's compose locates the subscriber and
 * scores neighbors. Default allocator; only the gateway touches the tally. */

static kith_sim_t *gateway_routing_scaffold_sim(void)
{
    kith_sim_t *sim = nullptr;
    if (kith_sim_create(nullptr, nullptr, &sim) != 0)
    {
        return nullptr;
    }
    kith_sim_actor_t actor = valid_sim_actor(1u);
    kith_sim_artifact_key_t sim_key = valid_sim_key(0);
    if (kith_sim_publish_artifact(sim, &sim_key, &actor, nullptr) != 0)
    {
        kith_sim_destroy(sim);
        return nullptr;
    }
    for (uint64_t i = 2u; i <= 4u; ++i)
    {
        actor = valid_sim_actor(i);
        sim_key = valid_sim_key(1);
        if (kith_sim_publish_artifact(sim, &sim_key, &actor, nullptr) != 0)
        {
            kith_sim_destroy(sim);
            return nullptr;
        }
    }
    return sim;
}

/* Loopback connection for session paths: connect to the net's ephemeral
 * listener, accept it, and drop the client fd. Returns nonzero on failure
 * with *out_conn left NULL. */
static int gateway_test_conn(kith_net_t *net, kith_net_conn_t **out_conn)
{
    *out_conn = nullptr;
    int client_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (client_fd < 0)
    {
        return 1;
    }
    struct sockaddr_in addr = {0};
    socklen_t addr_len = sizeof(addr);
    if (getsockname(kith_net_listener_fd(net), (struct sockaddr *)&addr, &addr_len) != 0 ||
        connect(client_fd, (struct sockaddr *)&addr, addr_len) != 0)
    {
        (void)close(client_fd);
        return 1;
    }
    const int rc = kith_net_accept(net, out_conn);
    (void)close(client_fd);
    return rc != 0;
}

/* The routing pin's scaffold: populated sim (three published actors),
 * fabric, batch-typed proto, listening net, and the gateway under the
 * tally. Returns nullptr with the scaffold torn down if any step fails. */
static kith_gateway_t *routing_scaffold_ready(struct gateway_scaffold *fx)
{
    memset(fx, 0, sizeof(*fx));
    if (kith_proto_create(nullptr, nullptr, &fx->proto) != 0 ||
        kith_net_create(nullptr, fx->proto, nullptr, &fx->net) != 0)
    {
        gateway_scaffold_fini(fx);
        return nullptr;
    }
    fx->sim = gateway_routing_scaffold_sim();
    if (fx->sim == nullptr || kith_fabric_create(nullptr, fx->sim, nullptr, &fx->fabric) != 0 ||
        kith_proto_register_type_id(fx->proto, "route_batch", 1000U) != 0 ||
        kith_net_listen(fx->net, "127.0.0.1", 0) != 0)
    {
        gateway_scaffold_fini(fx);
        return nullptr;
    }
    kith_gateway_params_t params = valid_gateway_params();
    params.replication_batch_type_id = 1000U;
    kith_gateway_t *gw = nullptr;
    tally_reset(0U);
    if (kith_gateway_create(&params, fx->net, fx->fabric, fx->proto, &g_tally_allocator, &gw) != 0)
    {
        gateway_scaffold_fini(fx);
        return nullptr;
    }
    return gw;
}

/* The pin's full pass: create, session (deriving the gateway's instance),
 * window adds, one publish, one tick (cache refresh, compose, deliver), and
 * the destroy unwind — every attempt and free through the gateway's stored
 * instance. Returns nonzero on the first broken step. */
static int test_gateway_allocator_routing(void)
{
    struct gateway_scaffold fx;
    kith_gateway_t *gw = routing_scaffold_ready(&fx);
    if (gw == nullptr)
    {
        return 1;
    }
    /* Routing must not disturb the pipeline. The counts pin the route:
     * create spends the handle, the session table, sixteen cache stripe
     * tables, the handler table, the registry's growth and two name
     * copies, and the default strategy name (23); the session carries the
     * derived instance and spends its struct plus the full strategy's
     * state (2); the first window add sizes its window array and the cold
     * staging buffer of the cell it subscribes, the second sizes the other
     * cell's staging (3 more); the tick drains both published cells into a
     * products scratch and re-arms each cell's swapped-out staging side
     * (3), composes (window copy, heap, subjects, prior ids, the prior-set
     * rebuild over the three entries, delta events, baseline — 7) and
     * delivers the batch (event backlog, batch payload — 2). Every growth
     * starts from an empty buffer, so 40 attempts release as 40 frees. */
    int step = 0;
    kith_net_conn_t *conn = nullptr;
    kith_gateway_session_t *session = nullptr;
    const kith_fabric_cell_key_t key0 = {
        .zone = 0u, .cell_x = 0, .cell_y = 0, .cell_z = 0, .lod = 0u};
    const kith_fabric_cell_key_t key1 = {
        .zone = 0u, .cell_x = 0, .cell_y = 0, .cell_z = 1, .lod = 0u};
    if (gateway_test_conn(fx.net, &conn) != 0 ||
        kith_gateway_session_create(
            gw, conn, KITH_GATEWAY_SESSION_SUBSCRIBER, 1u, nullptr, &session) != 0)
    {
        step = 1;
    }
    if (step == 0 && (kith_gateway_session_bind_actor(session, 1u) != 0 ||
                      kith_gateway_session_window_add(session, &key0) != 0 ||
                      kith_gateway_session_window_add(session, &key1) != 0))
    {
        step = 2;
    }
    if (step == 0 && (kith_fabric_publish(fx.fabric, &key0, 1u, nullptr) != 0 ||
                      kith_fabric_publish(fx.fabric, &key1, 1u, nullptr) != 0))
    {
        step = 3;
    }
    if (step == 0 && kith_gateway_tick(gw, 1000U) != 0)
    {
        step = 4;
    }
    if (session != nullptr)
    {
        kith_gateway_session_destroy(session);
    }
    kith_gateway_destroy(gw);
    if (conn != nullptr)
    {
        kith_net_conn_close(conn);
        kith_net_conn_release(conn);
    }
    gateway_scaffold_fini(&fx);
    if (step != 0 || g_tally.attempts != 40U || g_tally.frees != 40U || g_tally.outstanding != 0U)
    {
        return step != 0 ? step : 7;
    }
    return 0;
}

/* The coord pin's pass: bus and coord under the tally, one authority
 * override, one density report, a subscription, a remote member, and two
 * payload publishes with a drain between. Returns nonzero on the first
 * broken step. */
static int test_coord_allocator_routing(void)
{
    kith_coord_bus_t *bus = nullptr;
    kith_coord_t *coord = nullptr;
    tally_reset(0U);
    if (kith_coord_bus_create(nullptr, &g_tally_allocator, &bus) != 0 ||
        kith_coord_create(nullptr, bus, &g_tally_allocator, &coord) != 0)
    {
        kith_coord_destroy(coord);
        kith_coord_bus_destroy(bus);
        return 1;
    }
    /* Routing must not disturb the flow: authority and density inserts are
     * in-table slots, never allocations, and the added member rides create's
     * eight-slot table. The counts pin the route — the bus handle and its
     * initial member (2), the coord handle, sixteen shard tables, and the
     * density table (18), the first subscription's table growth, and each
     * publish's pending ring plus payload (2 + 2). No growth realloc
     * replaces a live block (both pending rings grow from NULL), so 25
     * attempts release as 25 frees. */
    int step = 0;
    const kith_fabric_cell_key_t key = {
        .zone = 1u, .cell_x = 2, .cell_y = 3, .cell_z = 4, .lod = 0u};
    const uint8_t payload[8] = {1u, 2u, 3u, 4u, 5u, 6u, 7u, 8u};
    if (kith_coord_set_authority(coord, &key, 2u) != 0 ||
        kith_coord_report_density(coord, &key, 5u, 1000u) != 0)
    {
        step = 1;
    }
    if (step == 0 &&
        (kith_coord_bus_subscribe(bus, 7u) != 0 || kith_coord_bus_add_member(bus, 3u) != 0))
    {
        step = 2;
    }
    if (step == 0 && kith_coord_bus_publish(
                         bus, KITH_COORD_BUS_EVENT_REBALANCE, 7u, payload, sizeof(payload)) != 0)
    {
        step = 3;
    }
    kith_coord_bus_event_t drained[2] = {0};
    size_t drained_count = 0u;
    if (step == 0 && (kith_coord_bus_drain(bus, drained, 2u, &drained_count) != 0 ||
                      drained_count != 1u || drained[0].payload_len != sizeof(payload)))
    {
        step = 4;
    }
    if (step == 0 && kith_coord_bus_publish(
                         bus, KITH_COORD_BUS_EVENT_REBALANCE, 7u, payload, sizeof(payload)) != 0)
    {
        step = 5;
    }
    kith_coord_destroy(coord);
    kith_coord_bus_destroy(bus);
    if (step != 0 || g_tally.attempts != 25U || g_tally.frees != 25U || g_tally.outstanding != 0U)
    {
        return step != 0 ? step : 7;
    }
    return 0;
}

/* The control pin's pass: handle, connection table, and event-bus records
 * under the tally, one published event riding the create-time ring.
 * Returns nonzero on the first broken step. */
static int test_control_allocator_routing(void)
{
    kith_reactor_t *reactor = control_scaffold_reactor();
    if (reactor == nullptr)
    {
        return 1;
    }
    kith_control_t *routed = nullptr;
    tally_reset(0U);
    if (kith_control_create(nullptr, reactor, nullptr, nullptr, &g_tally_allocator, &routed) != 0)
    {
        return 1;
    }
    /* Routing must not disturb the plane: one event publishes into the
     * create-time ring. The counts pin the route — handle, connection
     * table, and bus records at create — mirrored by the same number of
     * frees through the stored instance at destroy. The ring is
     * fixed-capacity: publish is an in-slot copy, never an allocation. */
    const kith_control_event_t event = {
        .ts_mono_ns = 1U,
        .type = "pin",
        .correlation_id = nullptr,
        .payload = nullptr,
        .payload_len = 0U,
    };
    if (kith_control_publish_event(routed, &event) != 0)
    {
        kith_control_destroy(routed);
        return 2;
    }
    kith_control_destroy(routed);
    if (g_tally.attempts != 3U || g_tally.frees != 3U || g_tally.outstanding != 0U)
    {
        return 3;
    }
    return 0;
}

/* Sweep one create call: fail the Nth attempt for N = 1, 2, ... and require
 * -KITH_ENOMEM, a cleared out slot, and a balanced unwind (every allocation
 * released through the allocator) each time, until the attempt after the
 * path's last allocation succeeds. On success, verifies the path made
 * exactly N-1 allocation attempts and reports that count through
 * @p out_attempts. */
typedef int (*create_fn)(void);

static int sweep_create(create_fn run, unsigned *out_attempts)
{
    for (unsigned n = 1U; n <= SWEEP_MAX_ATTEMPTS; n++)
    {
        tally_reset(n);
        const int rc = run();
        if (rc == 0)
        {
            if (g_tally.attempts != n - 1U || g_tally.outstanding != 0U)
            {
                return 1;
            }
            *out_attempts = n - 1U;
            return 0;
        }
        if (rc != kith_error_return(KITH_ENOMEM))
        {
            return 2;
        }
        if (g_tally.outstanding != 0U)
        {
            return 3;
        }
    }
    return 4;
}

static int run_rng_create(void)
{
    kith_rng_params_t params = valid_rng_params(42U);
    kith_rng_t *rng = (kith_rng_t *)&sentinel;
    const int rc = kith_rng_create(&params, &g_tally_allocator, &rng);
    if (rc == 0)
    {
        kith_rng_destroy(rng);
    }
    return rc;
}

static int run_rng_create_stream(void)
{
    kith_rng_params_t params = valid_rng_params(42U);
    kith_rng_t *parent = nullptr;
    if (kith_rng_create(&params, nullptr, &parent) != 0)
    {
        return SWEEP_SCAFFOLD_ERROR;
    }
    kith_rng_t *rng = (kith_rng_t *)&sentinel;
    const int rc = kith_rng_create_stream(parent, 7U, &g_tally_allocator, &rng);
    kith_rng_destroy(parent);
    if (rc != 0)
    {
        if (rng != nullptr)
        {
            return SWEEP_SCAFFOLD_ERROR;
        }
        return rc;
    }
    kith_rng_destroy(rng);
    return 0;
}

static int run_tick_clock_create(void)
{
    kith_tick_clock_params_t params = valid_clock_params(20U);
    kith_tick_clock_t *clock = (kith_tick_clock_t *)&sentinel;
    const int rc = kith_tick_clock_create(&params, &g_tally_allocator, &clock);
    if (rc == 0)
    {
        kith_tick_clock_destroy(clock);
    }
    return rc;
}

static int run_config_create_env(void)
{
    kith_config_params_t params = valid_config_params(nullptr, "KITH_ALLOC_SWEEP_");
    kith_config_t *cfg = (kith_config_t *)&sentinel;
    const int rc = kith_config_create(&params, &g_tally_allocator, &cfg);
    if (rc == 0)
    {
        kith_config_destroy(cfg);
    }
    return rc;
}

static int run_config_create_file_env(void)
{
    kith_config_params_t params =
        valid_config_params("/tmp/kith_alloc_sweep.conf", "KITH_ALLOC_SWEEP2_");
    kith_config_t *cfg = (kith_config_t *)&sentinel;
    const int rc = kith_config_create(&params, &g_tally_allocator, &cfg);
    if (rc == 0)
    {
        kith_config_destroy(cfg);
    }
    return rc;
}

static int run_logger_create(void)
{
    kith_logger_params_t params = valid_logger_params("sweep");
    kith_logger_t *logger = (kith_logger_t *)&sentinel;
    const int rc = kith_logger_create(&params, &g_tally_allocator, &logger);
    if (rc == 0)
    {
        kith_logger_destroy(logger);
    }
    return rc;
}

static int run_metrics_create(void)
{
    kith_metrics_params_t params = valid_metrics_params("kith_");
    kith_metrics_t *metrics = (kith_metrics_t *)&sentinel;
    const int rc = kith_metrics_create(&params, &g_tally_allocator, &metrics);
    if (rc == 0)
    {
        kith_metrics_destroy(metrics);
    }
    return rc;
}

static int run_proto_create(void)
{
    kith_proto_t *proto = (kith_proto_t *)&sentinel;
    const int rc = kith_proto_create(nullptr, &g_tally_allocator, &proto);
    if (rc == 0)
    {
        kith_proto_destroy(proto);
    }
    return rc;
}

static int run_net_create(void)
{
    kith_proto_t *proto = nullptr;
    if (kith_proto_create(nullptr, nullptr, &proto) != 0)
    {
        return SWEEP_SCAFFOLD_ERROR;
    }
    kith_net_t *net = (kith_net_t *)&sentinel;
    const int rc = kith_net_create(nullptr, proto, &g_tally_allocator, &net);
    kith_proto_destroy(proto);
    if (rc == 0)
    {
        kith_net_destroy(net);
    }
    return rc;
}

static int run_reactor_create(void)
{
    kith_reactor_params_t params = valid_reactor_params();
    kith_reactor_t *reactor = (kith_reactor_t *)&sentinel;
    const int rc = kith_reactor_create(&params, &g_tally_allocator, &reactor);
    if (rc == 0)
    {
        kith_reactor_destroy(reactor);
    }
    return rc;
}

static int run_worker_create(void)
{
    kith_worker_params_t params = valid_worker_params();
    kith_worker_t *pool = (kith_worker_t *)&sentinel;
    const int rc = kith_worker_create(&params, &g_tally_allocator, &pool);
    if (rc == 0)
    {
        kith_worker_destroy(pool);
    }
    return rc;
}

static int run_state_create(void)
{
    kith_reactor_t *reactor = nullptr;
    if (kith_reactor_create(nullptr, nullptr, &reactor) != 0)
    {
        return SWEEP_SCAFFOLD_ERROR;
    }
    kith_state_params_t params = valid_state_params();
    kith_state_t *state = (kith_state_t *)&sentinel;
    const int rc = kith_state_create(&params, reactor, &g_tally_allocator, &state);
    if (rc == 0)
    {
        kith_state_destroy(state);
    }
    kith_reactor_destroy(reactor);
    return rc;
}

static int run_db_create(void)
{
    kith_reactor_t *reactor = db_scaffold_reactor();
    if (reactor == nullptr)
    {
        return SWEEP_SCAFFOLD_ERROR;
    }
    kith_db_params_t params = valid_db_params();
    params.password = "sweep-secret"; // pulls the password copy into the path
    kith_db_t *db = (kith_db_t *)&sentinel;
    const int rc = kith_db_create(&params, reactor, &g_tally_allocator, &db);
    if (rc == 0)
    {
        kith_db_destroy(db);
    }
    return rc;
}

static int run_aoi_create(void)
{
    kith_aoi_params_t params = valid_aoi_params();
    kith_aoi_t *aoi = (kith_aoi_t *)&sentinel;
    const int rc = kith_aoi_create(&params, &g_tally_allocator, &aoi);
    if (rc == 0)
    {
        kith_aoi_destroy(aoi);
    }
    return rc;
}

static int run_sim_create(void)
{
    kith_sim_params_t params = valid_sim_params();
    kith_sim_t *sim = (kith_sim_t *)&sentinel;
    const int rc = kith_sim_create(&params, &g_tally_allocator, &sim);
    if (rc == 0)
    {
        kith_sim_destroy(sim);
    }
    return rc;
}

static int run_sim_create_model(void)
{
    kith_sim_t *sim = nullptr;
    if (kith_sim_create(nullptr, nullptr, &sim) != 0)
    {
        return SWEEP_SCAFFOLD_ERROR;
    }
    kith_sim_model_t *model = (kith_sim_model_t *)&sentinel;
    const int rc = kith_sim_create_model(sim, "tile2d", nullptr, &g_tally_allocator, &model);
    if (rc == 0)
    {
        kith_sim_model_destroy(model);
    }
    kith_sim_destroy(sim);
    return rc;
}

static int run_fabric_create(void)
{
    kith_sim_t *sim = nullptr;
    if (kith_sim_create(nullptr, nullptr, &sim) != 0)
    {
        return SWEEP_SCAFFOLD_ERROR;
    }
    kith_fabric_params_t params = valid_fabric_params();
    kith_fabric_t *fabric = (kith_fabric_t *)&sentinel;
    const int rc = kith_fabric_create(&params, sim, &g_tally_allocator, &fabric);
    if (rc == 0)
    {
        kith_fabric_destroy(fabric);
    }
    kith_sim_destroy(sim);
    return rc;
}

static int run_gateway_create(void)
{
    struct gateway_scaffold fx;
    if (gateway_scaffold_init(&fx) != 0)
    {
        gateway_scaffold_fini(&fx);
        return SWEEP_SCAFFOLD_ERROR;
    }
    kith_gateway_t *gw = (kith_gateway_t *)&sentinel;
    const int rc =
        kith_gateway_create(nullptr, fx.net, fx.fabric, fx.proto, &g_tally_allocator, &gw);
    if (rc == 0)
    {
        kith_gateway_destroy(gw);
    }
    gateway_scaffold_fini(&fx);
    return rc;
}

static int run_gateway_create_executor(void)
{
    struct gateway_scaffold fx;
    if (gateway_scaffold_init(&fx) != 0)
    {
        gateway_scaffold_fini(&fx);
        return SWEEP_SCAFFOLD_ERROR;
    }
    kith_gateway_params_t params = executor_gateway_params();
    kith_gateway_t *gw = (kith_gateway_t *)&sentinel;
    const int rc =
        kith_gateway_create(&params, fx.net, fx.fabric, fx.proto, &g_tally_allocator, &gw);
    if (rc == 0)
    {
        kith_gateway_destroy(gw);
    }
    gateway_scaffold_fini(&fx);
    return rc;
}

static int run_gateway_session_create(void)
{
    struct gateway_scaffold fx;
    if (gateway_scaffold_init(&fx) != 0 || kith_net_listen(fx.net, "127.0.0.1", 0) != 0)
    {
        gateway_scaffold_fini(&fx);
        return SWEEP_SCAFFOLD_ERROR;
    }
    kith_gateway_t *gw = nullptr;
    if (kith_gateway_create(nullptr, fx.net, fx.fabric, fx.proto, nullptr, &gw) != 0)
    {
        gateway_scaffold_fini(&fx);
        return SWEEP_SCAFFOLD_ERROR;
    }
    kith_net_conn_t *conn = nullptr;
    if (gateway_test_conn(fx.net, &conn) != 0)
    {
        kith_gateway_destroy(gw);
        gateway_scaffold_fini(&fx);
        return SWEEP_SCAFFOLD_ERROR;
    }
    kith_gateway_session_t *session = (kith_gateway_session_t *)&sentinel;
    const int rc = kith_gateway_session_create(
        gw, conn, KITH_GATEWAY_SESSION_SUBSCRIBER, 1u, &g_tally_allocator, &session);
    if (rc == 0)
    {
        kith_gateway_session_destroy(session);
    }
    kith_net_conn_close(conn);
    kith_net_conn_release(conn);
    kith_gateway_destroy(gw);
    gateway_scaffold_fini(&fx);
    return rc;
}

static int run_coord_create(void)
{
    kith_coord_t *coord = (kith_coord_t *)&sentinel;
    const int rc = kith_coord_create(nullptr, nullptr, &g_tally_allocator, &coord);
    if (rc == 0)
    {
        kith_coord_destroy(coord);
    }
    return rc;
}

static int run_coord_bus_create(void)
{
    kith_coord_bus_t *bus = (kith_coord_bus_t *)&sentinel;
    const int rc = kith_coord_bus_create(nullptr, &g_tally_allocator, &bus);
    if (rc == 0)
    {
        kith_coord_bus_destroy(bus);
    }
    return rc;
}

static int run_control_create(void)
{
    kith_reactor_t *reactor = control_scaffold_reactor();
    if (reactor == nullptr)
    {
        return SWEEP_SCAFFOLD_ERROR;
    }
    kith_control_t *ctrl = (kith_control_t *)&sentinel;
    const int rc =
        kith_control_create(nullptr, reactor, nullptr, nullptr, &g_tally_allocator, &ctrl);
    if (rc == 0)
    {
        kith_control_destroy(ctrl);
    }
    return rc;
}

static int run_client_create(void)
{
    kith_proto_t *proto = client_scaffold_proto();
    if (proto == nullptr)
    {
        return SWEEP_SCAFFOLD_ERROR;
    }
    kith_client_t *client = (kith_client_t *)&sentinel;
    const int rc = kith_client_create(nullptr, proto, nullptr, &g_tally_allocator, &client);
    if (rc == 0)
    {
        kith_client_destroy(client);
    }
    return rc;
}

static int run_server_create(void)
{
    kith_server_params_t params = valid_server_params();
    kith_server_t *server = (kith_server_t *)&sentinel;
    int rc = kith_server_create(&params, &g_tally_allocator, &server);
    for (unsigned attempt = 0u; attempt < 120u && rc == kith_error_return(KITH_EIO); attempt++)
    {
        // The io_uring ring reclaims its pages against the memlock budget
        // lazily after close, so a back-to-back create can transiently
        // report EIO; the re-arm keeps the injected failure ordinal
        // unchanged for the retry.
        tally_reset(g_tally.fail_attempt);
        struct timespec pause = {.tv_sec = 0, .tv_nsec = 50L * 1000 * 1000};
        (void)nanosleep(&pause, nullptr);
        rc = kith_server_create(&params, &g_tally_allocator, &server);
    }
    if (rc == 0)
    {
        kith_server_destroy(server);
    }
    return rc;
}

static const char *write_sweep_file(const char *body)
{
    static char path[] = "/tmp/kith_alloc_sweep.conf";
    FILE *f = fopen(path, "wb");
    if (f == nullptr || fputs(body, f) == EOF || fclose(f) != 0)
    {
        return nullptr;
    }
    return path;
}

static int test_enomem_sweep_util(void)
{
    unsigned attempts = 0;
    if (sweep_create(run_rng_create, &attempts) != 0 || attempts != 1U)
    {
        return 1;
    }
    if (sweep_create(run_rng_create_stream, &attempts) != 0 || attempts != 1U)
    {
        return 2;
    }
    if (sweep_create(run_tick_clock_create, &attempts) != 0 || attempts != 1U)
    {
        return 3;
    }
    return 0;
}

static int test_enomem_sweep_config(void)
{
    unsigned attempts = 0;
    if (setenv("KITH_ALLOC_SWEEP_alpha", "one", 1) != 0 ||
        setenv("KITH_ALLOC_SWEEP_beta", "two", 1) != 0 ||
        setenv("KITH_ALLOC_SWEEP_gamma", "three", 1) != 0)
    {
        return 1;
    }
    int rc = sweep_create(run_config_create_env, &attempts);
    (void)unsetenv("KITH_ALLOC_SWEEP_alpha");
    (void)unsetenv("KITH_ALLOC_SWEEP_beta");
    (void)unsetenv("KITH_ALLOC_SWEEP_gamma");
    /* Three prefixed entries drive the handle, one keybuf per entry, the
     * entry-table growth on the first insert only, and the key and value
     * copies: 1 + (1 + 1 + 1 + 1) + (1 + 1 + 1) + (1 + 1 + 1) attempts. */
    if (rc != 0 || attempts != 11U)
    {
        return 2;
    }

    static const char body[] = "alpha = file\nbeta = two\n";
    const char *path = write_sweep_file(body);
    if (path == nullptr || setenv("KITH_ALLOC_SWEEP2_alpha", "env", 1) != 0)
    {
        return 3;
    }
    rc = sweep_create(run_config_create_file_env, &attempts);
    (void)unsetenv("KITH_ALLOC_SWEEP2_alpha");
    (void)remove(path);
    /* The file buffer, two insert-path copies, and the override copy stack
     * onto the handle; the old value frees only after the override copy
     * succeeds, so a failed copy leaves it to the destroy unwind. qsort
     * sorts in place and allocates nothing, so it stays out of the count. */
    if (rc != 0 || attempts != 9U)
    {
        return 4;
    }
    return 0;
}

/* The JSONL record buffer is the logger's one allocation on the write path.
 * The logger is created with the tally allocator (create spends the handle
 * and the name copy), then the third attempt — the record buffer — is made
 * to fail: the log call must report -KITH_ENOMEM while the handle's own
 * storage stays outstanding for the destroy unwind, which must release it
 * through the stored instance. */
static int test_logger_write_enomem(void)
{
    static char jsonl_path[] = "/tmp/kith_alloc_sweep.jsonl";
    kith_logger_params_t params = valid_logger_params("sweep");
    params.jsonl_path = jsonl_path;
    kith_logger_t *logger = nullptr;
    tally_reset(0U);
    if (kith_logger_create(&params, &g_tally_allocator, &logger) != 0)
    {
        return 1;
    }
    g_tally.fail_attempt = 3U;
    const int rc = kith_logger_log(logger, KITH_LOG_LEVEL_ERROR, "t.c", 1U, nullptr, 0U, "boom");
    g_tally.fail_attempt = 0U;
    (void)remove(jsonl_path);
    if (rc != kith_error_return(KITH_ENOMEM) || g_tally.attempts != 3U)
    {
        kith_logger_destroy(logger);
        return 2;
    }
    kith_logger_destroy(logger);
    if (g_tally.frees != 2U || g_tally.outstanding != 0U)
    {
        return 3;
    }
    return 0;
}

static int test_enomem_sweep_logger(void)
{
    unsigned attempts = 0;
    /* The create path spends the handle and the name copy; a failed name
     * copy unwinds through kith_logger_destroy with the handle released
     * through the stored instance. */
    if (sweep_create(run_logger_create, &attempts) != 0 || attempts != 2U)
    {
        return 1;
    }
    return test_logger_write_enomem();
}

/* The first observation of a series is the metrics write path. The registry
 * is created with the tally allocator (the handle is its only create-path
 * allocation without a prefix), then the label-value duplicate — the last
 * of the observation path's five allocations — is made to fail:
 * counter_add must report -KITH_ENOMEM while the copy_labels unwind and the
 * series_create unwind release the partial series through the stored
 * instance, and destroy releases the rest. */
static int test_metrics_observe_enomem(void)
{
    static const kith_metrics_label_t labels_tcp[] = {{"transport", "tcp"}};
    kith_metrics_t *metrics = nullptr;
    tally_reset(0U);
    if (kith_metrics_create(nullptr, &g_tally_allocator, &metrics) != 0)
    {
        return 1;
    }
    g_tally.fail_attempt = 6U;
    const int rc = kith_metrics_counter_add(metrics, "obs_total", labels_tcp, 1U, 1U);
    g_tally.fail_attempt = 0U;
    if (rc != kith_error_return(KITH_ENOMEM) || g_tally.attempts != 6U || g_tally.outstanding != 2U)
    {
        kith_metrics_destroy(metrics);
        return 2;
    }
    kith_metrics_destroy(metrics);
    if (g_tally.frees != 5U || g_tally.outstanding != 0U)
    {
        return 3;
    }
    return 0;
}

/* Both renders allocate their scratch order arrays inside the locked render
 * call, and their snprintf-style length contract has no error channel: an
 * allocation failure reports 0. The Prometheus render's order allocation is
 * made to fail outright; the OTLP render's order allocation fails with its
 * group keys succeeding, which must free the keys through the stored
 * instance. The registry's own storage stays outstanding for the destroy
 * unwind. */
static int test_metrics_render_enomem(void)
{
    static const kith_metrics_label_t labels_tcp[] = {{"transport", "tcp"}};
    kith_metrics_t *metrics = nullptr;
    tally_reset(0U);
    if (kith_metrics_create(nullptr, &g_tally_allocator, &metrics) != 0 ||
        kith_metrics_counter_add(metrics, "render_total", labels_tcp, 1U, 1U) != 0)
    {
        kith_metrics_destroy(metrics);
        return 1;
    }
    g_tally.fail_attempt = 7U;
    const size_t prom = kith_metrics_render_prometheus(metrics, nullptr, 0U);
    g_tally.fail_attempt = 8U;
    const size_t otlp = kith_metrics_export_otlp(metrics, nullptr, 0U);
    g_tally.fail_attempt = 0U;
    if (prom != 0U || otlp != 0U)
    {
        kith_metrics_destroy(metrics);
        return 2;
    }
    kith_metrics_destroy(metrics);
    if (g_tally.attempts != 9U || g_tally.frees != 7U || g_tally.outstanding != 0U)
    {
        return 3;
    }
    return 0;
}

static int test_enomem_sweep_metrics(void)
{
    unsigned attempts = 0;
    /* The create path spends the handle and the prefix copy; a failed
     * prefix copy unwinds through kith_metrics_destroy with the handle
     * released through the stored instance. */
    if (sweep_create(run_metrics_create, &attempts) != 0 || attempts != 2U)
    {
        return 1;
    }
    int rc = test_metrics_observe_enomem();
    if (rc != 0)
    {
        return 2 + rc;
    }
    return test_metrics_render_enomem();
}

/* The first registration grows the type array before it copies the name.
 * Failing the growth must report -KITH_ENOMEM with the registry unchanged,
 * leaving a handle destroy releases fully through the stored instance. */
static int test_proto_register_grow_enomem(void)
{
    kith_proto_t *proto = nullptr;
    tally_reset(0U);
    if (kith_proto_create(nullptr, &g_tally_allocator, &proto) != 0)
    {
        return 1;
    }
    g_tally.fail_attempt = 2U;
    const int rc = kith_proto_register_type_id(proto, "grow_fail", 1000U);
    g_tally.fail_attempt = 0U;
    if (rc != kith_error_return(KITH_ENOMEM))
    {
        kith_proto_destroy(proto);
        return 2;
    }
    kith_proto_destroy(proto);
    if (g_tally.attempts != 2U || g_tally.frees != 1U || g_tally.outstanding != 0U)
    {
        return 3;
    }
    return 0;
}

/* The name copy is the last allocation of the registration path. Failing
 * it after the growth succeeded must report -KITH_ENOMEM and leave the
 * grown-but-empty array for destroy to release through the stored
 * instance. */
static int test_proto_register_name_enomem(void)
{
    kith_proto_t *proto = nullptr;
    tally_reset(0U);
    if (kith_proto_create(nullptr, &g_tally_allocator, &proto) != 0)
    {
        return 1;
    }
    g_tally.fail_attempt = 3U;
    const int rc = kith_proto_register_type_id(proto, "name_fail", 1000U);
    g_tally.fail_attempt = 0U;
    if (rc != kith_error_return(KITH_ENOMEM))
    {
        kith_proto_destroy(proto);
        return 2;
    }
    kith_proto_destroy(proto);
    if (g_tally.attempts != 3U || g_tally.frees != 2U || g_tally.outstanding != 0U)
    {
        return 3;
    }
    return 0;
}

static int test_enomem_sweep_proto(void)
{
    unsigned attempts = 0;
    /* The create path spends only the handle; a failed mutex
     * initialization releases it through the stored instance. */
    if (sweep_create(run_proto_create, &attempts) != 0 || attempts != 1U)
    {
        return 1;
    }
    int rc = test_proto_register_grow_enomem();
    if (rc != 0)
    {
        return 2 + rc;
    }
    return test_proto_register_name_enomem();
}

/* A loopback client for @p net's listener: -1 on any setup failure. */
static int net_sweep_connect(kith_net_t *net)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0)
    {
        return -1;
    }
    struct sockaddr_in addr = {0};
    socklen_t addr_len = sizeof(addr);
    if (getsockname(kith_net_listener_fd(net), (struct sockaddr *)&addr, &addr_len) != 0 ||
        connect(fd, (struct sockaddr *)&addr, addr_len) != 0)
    {
        (void)close(fd);
        return -1;
    }
    return fd;
}

/* One accept failure per run, mirroring the create sweep's single-fail
 * model: the tally arms before the transport setup and the run counts from
 * the first allocation, so with @p fail_attempt at 3 the connection struct
 * allocation fails and at 4 the struct survives and its ring buffer fails,
 * releasing the struct through the stored instance. accept must report
 * -KITH_ENOMEM with the out slot cleared and the pending fd closed; the
 * destroy balances the tally. */
static int net_accept_enomem_at(unsigned fail_attempt, kith_proto_t *proto)
{
    tally_reset(fail_attempt);
    kith_net_t *net = nullptr;
    if (kith_net_create(nullptr, proto, &g_tally_allocator, &net) != 0 ||
        kith_net_listen(net, "127.0.0.1", 0) != 0)
    {
        kith_net_destroy(net);
        return SWEEP_SCAFFOLD_ERROR;
    }
    int client_fd = net_sweep_connect(net);
    if (client_fd < 0)
    {
        kith_net_destroy(net);
        return SWEEP_SCAFFOLD_ERROR;
    }
    kith_net_conn_t *conn = (kith_net_conn_t *)&sentinel;
    const int rc = kith_net_accept(net, &conn);
    (void)close(client_fd);
    kith_net_destroy(net);
    if (rc != kith_error_return(KITH_ENOMEM) || conn != nullptr)
    {
        return 1;
    }
    if (g_tally.attempts != fail_attempt || g_tally.frees != fail_attempt - 1U ||
        g_tally.outstanding != 0U)
    {
        return 2;
    }
    return 0;
}

static int test_net_accept_enomem(void)
{
    kith_proto_t *proto = nullptr;
    if (kith_proto_create(nullptr, nullptr, &proto) != 0)
    {
        return 1;
    }
    int rc = net_accept_enomem_at(3U, proto);
    if (rc == 0)
    {
        rc = net_accept_enomem_at(4U, proto);
    }
    kith_proto_destroy(proto);
    return rc;
}

/* The output-queue entry is the enqueue path's only allocation. Failing it
 * must report -KITH_ENOMEM with the frame's reference count untouched; the
 * caller's release recycles (or, under sanitizers, frees) the frame and the
 * cleanup balances the tally. The tally runs from the transport setup, so
 * the setup spends the handle, the connection table, the connection, its
 * ring buffer, and the frame — five attempts — and the sixth is the queue
 * entry. */
static int test_net_enqueue_enomem(void)
{
    int rc = -1;
    kith_proto_t *proto = nullptr;
    kith_net_t *net = nullptr;
    kith_net_conn_t *conn = nullptr;
    kith_net_frame_t *frame = nullptr;
    int client_fd = -1;
    tally_reset(0U);
    if (kith_proto_create(nullptr, nullptr, &proto) != 0 ||
        kith_net_create(nullptr, proto, &g_tally_allocator, &net) != 0 ||
        kith_net_listen(net, "127.0.0.1", 0) != 0)
    {
        goto scaffold_fail;
    }
    client_fd = net_sweep_connect(net);
    if (client_fd < 0 || kith_net_accept(net, &conn) != 0)
    {
        goto scaffold_fail;
    }
    frame = kith_net_frame_create(net, 16U);
    if (frame == nullptr)
    {
        goto scaffold_fail;
    }
    g_tally.fail_attempt = 6U;
    rc = kith_net_conn_enqueue(conn, frame);
    g_tally.fail_attempt = 0U;
    if (rc != kith_error_return(KITH_ENOMEM) || g_tally.attempts != 6U || g_tally.outstanding != 5U)
    {
        goto scaffold_fail;
    }
    kith_net_frame_release(frame);
    kith_net_conn_close(conn);
    kith_net_conn_release(conn);
    (void)close(client_fd);
    kith_net_destroy(net);
    kith_proto_destroy(proto);
    /* The recycled frame frees at pool destroy; under sanitizers, at its
     * release. Either way every block returns. */
    if (g_tally.frees != 5U || g_tally.outstanding != 0U)
    {
        return 1;
    }
    return 0;

scaffold_fail:
    g_tally.fail_attempt = 0U;
    tally_reset(0U);
    if (frame != nullptr)
    {
        kith_net_frame_release(frame);
    }
    if (conn != nullptr)
    {
        kith_net_conn_close(conn);
        kith_net_conn_release(conn);
    }
    if (client_fd >= 0)
    {
        (void)close(client_fd);
    }
    kith_net_destroy(net);
    kith_proto_destroy(proto);
    return SWEEP_SCAFFOLD_ERROR;
}

static int test_enomem_sweep_net(void)
{
    unsigned attempts = 0;
    /* The create path spends the handle and the connection table; a failed
     * table allocation releases the handle through the stored instance. */
    if (sweep_create(run_net_create, &attempts) != 0 || attempts != 2U)
    {
        return 1;
    }
    int rc = test_net_accept_enomem();
    if (rc != 0)
    {
        return 2 + rc;
    }
    return test_net_enqueue_enomem();
}

static int test_reactor_schedule_enomem(void)
{
    kith_reactor_params_t params = valid_reactor_params();
    kith_reactor_t *reactor = nullptr;
    /* Create consumes the handle, fd table, two task nodes, and the
     * backend's two blocks; the timer node is the 7th attempt and fails,
     * leaving the six held blocks untouched until destroy releases them. */
    tally_reset(7U);
    if (kith_reactor_create(&params, &g_tally_allocator, &reactor) != 0)
    {
        return 1;
    }
    const int rc = kith_reactor_schedule(reactor, 1U, reactor_noop_task, nullptr);
    if (rc != kith_error_return(KITH_ENOMEM) || g_tally.attempts != 7U || g_tally.outstanding != 6U)
    {
        kith_reactor_destroy(reactor);
        return 2;
    }
    kith_reactor_destroy(reactor);
    if (g_tally.frees != 6U || g_tally.outstanding != 0U)
    {
        return 3;
    }
    return 0;
}

static int test_enomem_sweep_reactor(void)
{
    unsigned attempts = 0;
    /* The create path spends the handle, fd table, two task nodes, and the
     * backend's two blocks; each failure unwinds through the stored
     * instance. */
    if (sweep_create(run_reactor_create, &attempts) != 0 || attempts != 6U)
    {
        return 1;
    }
    return test_reactor_schedule_enomem();
}

static int test_worker_bridge_create_enomem(void)
{
    kith_worker_params_t params = valid_worker_params();
    kith_worker_t *pool = nullptr;
    kith_worker_bridge_t *bridge = (kith_worker_bridge_t *)&sentinel;
    /* Create consumes the handle, the node pool, and the thread array; the
     * bridge's own block is the 4th attempt and fails, leaving the three
     * held blocks untouched until destroy releases them. */
    tally_reset(4U);
    if (kith_worker_create(&params, &g_tally_allocator, &pool) != 0)
    {
        return 1;
    }
    const int rc = kith_worker_bridge_create_ready(pool, worker_noop_ready, nullptr, &bridge);
    if (rc != kith_error_return(KITH_ENOMEM) || bridge != nullptr || g_tally.attempts != 4U ||
        g_tally.outstanding != 3U)
    {
        kith_worker_destroy(pool);
        return 2;
    }
    kith_worker_destroy(pool);
    if (g_tally.frees != 3U || g_tally.outstanding != 0U)
    {
        return 3;
    }
    return 0;
}

static int test_worker_fire_drop_enomem(void)
{
    kith_worker_params_t params = valid_worker_params();
    kith_worker_t *pool = nullptr;
    kith_worker_bridge_t *bridge = nullptr;
    /* Create plus the bridge consume four attempts; the fire's work record
     * is the 5th and fails, which the hop drops silently — nothing reaches
     * the queue, so no task counter moves and no worker thread runs. */
    tally_reset(5U);
    if (kith_worker_create(&params, &g_tally_allocator, &pool) != 0)
    {
        return 1;
    }
    if (kith_worker_bridge_create_ready(pool, worker_noop_ready, nullptr, &bridge) != 0)
    {
        kith_worker_destroy(pool);
        return 2;
    }
    kith_worker_bridge_ready_cb(7, 1U, bridge);
    if (kith_worker_tasks_submitted(pool) != 0U || g_tally.attempts != 5U ||
        g_tally.outstanding != 4U)
    {
        kith_worker_destroy(pool);
        kith_worker_bridge_destroy(bridge);
        return 3;
    }
    kith_worker_destroy(pool);
    kith_worker_bridge_destroy(bridge);
    if (g_tally.frees != 4U || g_tally.outstanding != 0U)
    {
        return 4;
    }
    return 0;
}

static int test_enomem_sweep_worker(void)
{
    unsigned attempts = 0;
    /* The create path spends the handle, the node pool, and the thread
     * array; each failure unwinds through the stored instance. */
    if (sweep_create(run_worker_create, &attempts) != 0 || attempts != 3U)
    {
        return 1;
    }
    const int rc = test_worker_bridge_create_enomem();
    if (rc != 0)
    {
        return 10 + rc;
    }
    return test_worker_fire_drop_enomem();
}

/* The command block is the operations path's one allocation. The store is
 * created with the tally allocator (the handle is its only create-path
 * allocation), then the second attempt — the command block — is made to
 * fail: the operation must report -KITH_ENOMEM while the handle's own
 * storage stays outstanding for the destroy unwind, which must release it
 * through the stored instance. */
static int test_state_command_alloc_enomem(void)
{
    kith_reactor_t *reactor = nullptr;
    if (kith_reactor_create(nullptr, nullptr, &reactor) != 0)
    {
        return SWEEP_SCAFFOLD_ERROR;
    }
    kith_state_params_t params = valid_state_params();
    kith_state_t *state = nullptr;
    tally_reset(0U);
    if (kith_state_create(&params, reactor, &g_tally_allocator, &state) != 0)
    {
        kith_reactor_destroy(reactor);
        return SWEEP_SCAFFOLD_ERROR;
    }
    g_tally.fail_attempt = 2U;
    const int rc = kith_state_set(state, "k", 1U, "v", 1U, state_noop_reply, nullptr);
    g_tally.fail_attempt = 0U;
    if (rc != kith_error_return(KITH_ENOMEM) || g_tally.attempts != 2U || g_tally.frees != 0U ||
        g_tally.outstanding != 1U)
    {
        kith_state_destroy(state);
        kith_reactor_destroy(reactor);
        return 1;
    }
    kith_state_destroy(state);
    kith_reactor_destroy(reactor);
    if (g_tally.frees != 1U || g_tally.outstanding != 0U)
    {
        return 2;
    }
    return 0;
}

/* The submit-failure path frees the command block on the caller's thread:
 * with the reactor's task queue filled, the operation must report
 * -KITH_EBUSY and release the block through the stored instance. The
 * reactor-thread free sites (the submission task and the hiredis reply
 * callback) share the same stored-instance expression; the live-Redis runs
 * of the ops suite exercise them — the sweep never drives the loop, because
 * driving the async adapter against a dead endpoint trips hiredis. */
static int test_state_command_submit_busy(void)
{
    kith_reactor_params_t reactor_params = valid_reactor_params();
    kith_reactor_t *reactor = nullptr;
    if (kith_reactor_create(&reactor_params, nullptr, &reactor) != 0)
    {
        return SWEEP_SCAFFOLD_ERROR;
    }
    kith_state_params_t params = valid_state_params();
    kith_state_t *state = nullptr;
    tally_reset(0U);
    if (kith_state_create(&params, reactor, &g_tally_allocator, &state) != 0)
    {
        kith_reactor_destroy(reactor);
        return SWEEP_SCAFFOLD_ERROR;
    }
    for (unsigned i = 0; i < 2U; ++i)
    {
        if (kith_reactor_submit(reactor, state_noop_task, nullptr) != 0)
        {
            kith_state_destroy(state);
            kith_reactor_destroy(reactor);
            return SWEEP_SCAFFOLD_ERROR;
        }
    }
    const int rc = kith_state_set(state, "k", 1U, "v", 1U, state_noop_reply, nullptr);
    if (rc != kith_error_return(KITH_EBUSY) || g_tally.attempts != 2U || g_tally.frees != 1U ||
        g_tally.outstanding != 1U)
    {
        kith_state_destroy(state);
        kith_reactor_destroy(reactor);
        return 1;
    }
    kith_state_destroy(state);
    kith_reactor_destroy(reactor);
    if (g_tally.frees != 2U || g_tally.outstanding != 0U)
    {
        return 2;
    }
    return 0;
}

static int test_enomem_sweep_state(void)
{
    unsigned attempts = 0;
    /* The create path spends only the handle; a failed handle allocation
     * releases nothing and the out slot stays cleared. */
    if (sweep_create(run_state_create, &attempts) != 0 || attempts != 1U)
    {
        return 1;
    }
    int rc = test_state_command_alloc_enomem();
    if (rc != 0)
    {
        return 10 + rc;
    }
    return test_state_command_submit_busy();
}

/* The command block is the operations path's first allocation. The pool is
 * created with the tally allocator (the handle, the host copy, and the
 * connection table are its only create-path allocations without a
 * password), the one-parameter
 * query is registered (the entry, its name, and its SQL), then each of the
 * three command-path allocations is made to fail in turn: the block leaves
 * nothing to free, the parameter-array failure frees the block directly
 * (its copy list is still empty), and the parameter-copy failure unwinds
 * through cmd_free — the array and the block, through the stored
 * instance. Every arm must report -KITH_ENOMEM while the pool's own
 * storage stays outstanding for the destroy unwind. */
static int test_db_command_alloc_enomem(void)
{
    kith_reactor_t *reactor = db_scaffold_reactor();
    if (reactor == nullptr)
    {
        return SWEEP_SCAFFOLD_ERROR;
    }
    kith_db_params_t params = valid_db_params();
    kith_db_t *db = nullptr;
    tally_reset(0U);
    if (kith_db_create(&params, reactor, &g_tally_allocator, &db) != 0 ||
        kith_db_register_query(db, "q", "SELECT $1", 1U) != 0)
    {
        kith_db_destroy(db);
        return SWEEP_SCAFFOLD_ERROR;
    }
    const char *param_values[] = {"v"};
    /* One arm per command-path allocation, keyed on the cumulative attempt
     * the failure lands on: the block (7th), the parameter array (9th,
     * after the block succeeds), the parameter copy (12th, after both).
     * Expected frees: 0, then the directly freed block, then the array and
     * the block through cmd_free. */
    static const struct
    {
        unsigned fail_attempt;
        unsigned expected_attempts;
        unsigned expected_frees;
    } arms[] = {
        {7U, 7U, 0U},
        {9U, 9U, 1U},
        {12U, 12U, 3U},
    };
    for (size_t arm = 0; arm < sizeof(arms) / sizeof(arms[0]); arm++)
    {
        g_tally.fail_attempt = arms[arm].fail_attempt;
        const int rc = kith_db_exec(db, "q", param_values, 1U, db_noop_reply, nullptr);
        g_tally.fail_attempt = 0U;
        if (rc != kith_error_return(KITH_ENOMEM) ||
            g_tally.attempts != arms[arm].expected_attempts ||
            g_tally.frees != arms[arm].expected_frees || g_tally.outstanding != 6U)
        {
            kith_db_destroy(db);
            return 1;
        }
    }
    kith_db_destroy(db);
    if (g_tally.frees != 9U || g_tally.outstanding != 0U)
    {
        return 2;
    }
    return 0;
}

/* The submit-failure path frees the command block on the caller's thread:
 * with the reactor's task queue filled, the operation must report
 * -KITH_EBUSY and release the block, its parameter array, and its
 * parameter copy through the stored instance. The queue fills until a
 * submission fails, so the tooth does not depend on the default capacity.
 * The reactor-thread free sites (the submission task and the libpq reply
 * paths) share the same stored-instance expression; the ops suite
 * exercises them — its dead-endpoint connect failures and live-Postgres
 * runs both drive the loop — while the sweep never drives it: every exec
 * failure the arms produce happens before submission. */
static int test_db_command_submit_busy(void)
{
    kith_reactor_t *reactor = db_scaffold_reactor();
    if (reactor == nullptr)
    {
        return SWEEP_SCAFFOLD_ERROR;
    }
    kith_db_params_t params = valid_db_params();
    kith_db_t *db = nullptr;
    tally_reset(0U);
    if (kith_db_create(&params, reactor, &g_tally_allocator, &db) != 0 ||
        kith_db_register_query(db, "q", "SELECT $1", 1U) != 0)
    {
        kith_db_destroy(db);
        return SWEEP_SCAFFOLD_ERROR;
    }
    while (kith_reactor_submit(reactor, db_noop_task, nullptr) == 0)
    {
    }
    const char *param_values[] = {"v"};
    const int rc = kith_db_exec(db, "q", param_values, 1U, db_noop_reply, nullptr);
    if (rc != kith_error_return(KITH_EBUSY) || g_tally.attempts != 9U || g_tally.frees != 3U ||
        g_tally.outstanding != 6U)
    {
        kith_db_destroy(db);
        return 1;
    }
    kith_db_destroy(db);
    if (g_tally.frees != 9U || g_tally.outstanding != 0U)
    {
        return 2;
    }
    return 0;
}

/* The raw-SQL command path carries one more allocation than the registry
 * path: the statement text is copied at call time because the submission
 * task may drain after the caller's buffer is gone. The pool and registry
 * setup spend the same six attempts as the registry test, then each of
 * the four command-path allocations fails in turn: the block leaves
 * nothing to free, the array failure frees the block directly, the
 * parameter-copy failure unwinds through cmd_free (array and block), and
 * the SQL-copy failure unwinds through cmd_free (copy, array, and block).
 * Arms key on the cumulative attempt; expected frees accumulate across
 * arms. Every arm must report -KITH_ENOMEM while the pool's own storage
 * stays outstanding for the destroy unwind. */
static int test_db_execsql_alloc_enomem(void)
{
    kith_reactor_t *reactor = db_scaffold_reactor();
    if (reactor == nullptr)
    {
        return SWEEP_SCAFFOLD_ERROR;
    }
    kith_db_params_t params = valid_db_params();
    kith_db_t *db = nullptr;
    tally_reset(0U);
    if (kith_db_create(&params, reactor, &g_tally_allocator, &db) != 0 ||
        kith_db_register_query(db, "q", "SELECT $1", 1U) != 0)
    {
        kith_db_destroy(db);
        return SWEEP_SCAFFOLD_ERROR;
    }
    const char *param_values[] = {"v"};
    static const struct
    {
        unsigned fail_attempt;
        unsigned expected_attempts;
        unsigned expected_frees;
    } arms[] = {
        {7U, 7U, 0U},
        {9U, 9U, 1U},
        {12U, 12U, 3U},
        {16U, 16U, 6U},
    };
    for (size_t arm = 0; arm < sizeof(arms) / sizeof(arms[0]); arm++)
    {
        g_tally.fail_attempt = arms[arm].fail_attempt;
        const int rc = kith_db_exec_sql(db, "SELECT $1", param_values, 1U, db_noop_reply, nullptr);
        g_tally.fail_attempt = 0U;
        if (rc != kith_error_return(KITH_ENOMEM) ||
            g_tally.attempts != arms[arm].expected_attempts ||
            g_tally.frees != arms[arm].expected_frees || g_tally.outstanding != 6U)
        {
            kith_db_destroy(db);
            return 1;
        }
    }
    kith_db_destroy(db);
    if (g_tally.frees != 12U || g_tally.outstanding != 0U)
    {
        return 2;
    }
    return 0;
}

/* The raw-SQL submit-failure path frees the command block on the caller's
 * thread: with the reactor's task queue filled, the statement must report
 * -KITH_EBUSY and release the block, its parameter array, its parameter
 * copy, and its SQL copy through the stored instance — one more block
 * member than the registry path's three. */
static int test_db_execsql_submit_busy(void)
{
    kith_reactor_t *reactor = db_scaffold_reactor();
    if (reactor == nullptr)
    {
        return SWEEP_SCAFFOLD_ERROR;
    }
    kith_db_params_t params = valid_db_params();
    kith_db_t *db = nullptr;
    tally_reset(0U);
    if (kith_db_create(&params, reactor, &g_tally_allocator, &db) != 0 ||
        kith_db_register_query(db, "q", "SELECT $1", 1U) != 0)
    {
        kith_db_destroy(db);
        return SWEEP_SCAFFOLD_ERROR;
    }
    while (kith_reactor_submit(reactor, db_noop_task, nullptr) == 0)
    {
    }
    const char *param_values[] = {"v"};
    const int rc = kith_db_exec_sql(db, "SELECT $1", param_values, 1U, db_noop_reply, nullptr);
    if (rc != kith_error_return(KITH_EBUSY) || g_tally.attempts != 10U || g_tally.frees != 4U ||
        g_tally.outstanding != 6U)
    {
        kith_db_destroy(db);
        return 1;
    }
    kith_db_destroy(db);
    if (g_tally.frees != 10U || g_tally.outstanding != 0U)
    {
        return 2;
    }
    return 0;
}

static int test_enomem_sweep_db(void)
{
    unsigned attempts = 0;
    /* The create path spends the handle, the host copy, the password
     * copy, and the connection table; each failure unwinds through the
     * stored instance. */
    if (sweep_create(run_db_create, &attempts) != 0 || attempts != 4U)
    {
        db_scaffold_release();
        return 1;
    }
    int rc = test_db_command_alloc_enomem();
    if (rc == 0)
    {
        rc = test_db_command_submit_busy();
    }
    else
    {
        rc = 10 + rc;
    }
    if (rc != 0)
    {
        db_scaffold_release();
        return rc;
    }
    rc = test_db_execsql_alloc_enomem();
    if (rc == 0)
    {
        rc = test_db_execsql_submit_busy();
        if (rc != 0)
        {
            rc = 40 + rc;
        }
    }
    else
    {
        rc = 20 + rc;
    }
    db_scaffold_release();
    return rc;
}

/* The first member-array growth is an insert's only allocation: the cell
 * exists before the array does (cells are array slots, never reclaimed), so
 * a failed growth leaves nothing to unwind — the index reports -KITH_ENOMEM
 * with no object stored, and destroy releases only the create-path
 * storage. */
static int test_aoi_insert_grow_enomem(void)
{
    kith_aoi_params_t params = valid_aoi_params();
    kith_aoi_t *aoi = nullptr;
    tally_reset(0U);
    if (kith_aoi_create(&params, &g_tally_allocator, &aoi) != 0)
    {
        return SWEEP_SCAFFOLD_ERROR;
    }
    kith_aoi_object_t obj = valid_aoi_object(1u, 0);
    g_tally.fail_attempt = 4U;
    const int rc = kith_aoi_insert(aoi, &obj);
    g_tally.fail_attempt = 0U;
    if (rc != kith_error_return(KITH_ENOMEM) || g_tally.attempts != 4U || g_tally.frees != 0U ||
        g_tally.outstanding != 3U || kith_aoi_size(aoi) != 0u)
    {
        kith_aoi_destroy(aoi);
        return 1;
    }
    kith_aoi_destroy(aoi);
    if (g_tally.frees != 3U || g_tally.outstanding != 0U)
    {
        return 2;
    }
    return 0;
}

/* Moving an object to a fresh cell grows that cell's member array before
 * the detach; a failed growth must leave the object in its old cell —
 * lookup still reports the old position — with nothing of the move
 * outstanding. */
static int test_aoi_update_move_grow_enomem(void)
{
    kith_aoi_params_t params = valid_aoi_params();
    kith_aoi_t *aoi = nullptr;
    tally_reset(0U);
    if (kith_aoi_create(&params, &g_tally_allocator, &aoi) != 0)
    {
        return SWEEP_SCAFFOLD_ERROR;
    }
    kith_aoi_object_t obj = valid_aoi_object(1u, 0);
    if (kith_aoi_insert(aoi, &obj) != 0)
    {
        kith_aoi_destroy(aoi);
        return SWEEP_SCAFFOLD_ERROR;
    }
    kith_aoi_object_t moved = valid_aoi_object(1u, 32);
    g_tally.fail_attempt = 5U;
    const int rc = kith_aoi_update(aoi, &moved);
    g_tally.fail_attempt = 0U;
    if (rc != kith_error_return(KITH_ENOMEM) || g_tally.attempts != 5U || g_tally.frees != 0U ||
        g_tally.outstanding != 4U)
    {
        kith_aoi_destroy(aoi);
        return 1;
    }
    kith_aoi_object_t out = {0};
    if (kith_aoi_lookup(aoi, 1u, &out) != 0 || out.pos_x != obj.pos_x)
    {
        kith_aoi_destroy(aoi);
        return 2;
    }
    kith_aoi_destroy(aoi);
    if (g_tally.frees != 4U || g_tally.outstanding != 0U)
    {
        return 3;
    }
    return 0;
}

/* An in-place growth reallocates a block the allocator already owns: a
 * failed attempt must leave that block and its members untouched, so the
 * stored objects survive and the destroy balances the tally. */
static int test_aoi_insert_grow_inplace_enomem(void)
{
    kith_aoi_params_t params = valid_aoi_params();
    kith_aoi_t *aoi = nullptr;
    tally_reset(0U);
    if (kith_aoi_create(&params, &g_tally_allocator, &aoi) != 0)
    {
        return SWEEP_SCAFFOLD_ERROR;
    }
    for (uint64_t i = 1u; i <= 4u; ++i)
    {
        kith_aoi_object_t obj = valid_aoi_object(i, (int64_t)i - 1u);
        if (kith_aoi_insert(aoi, &obj) != 0)
        {
            kith_aoi_destroy(aoi);
            return SWEEP_SCAFFOLD_ERROR;
        }
    }
    kith_aoi_object_t fifth = valid_aoi_object(5u, 4);
    g_tally.fail_attempt = 5U;
    const int rc = kith_aoi_insert(aoi, &fifth);
    g_tally.fail_attempt = 0U;
    if (rc != kith_error_return(KITH_ENOMEM) || g_tally.attempts != 5U || g_tally.frees != 0U ||
        g_tally.outstanding != 4U || kith_aoi_size(aoi) != 4u)
    {
        kith_aoi_destroy(aoi);
        return 1;
    }
    for (uint64_t i = 1u; i <= 4u; ++i)
    {
        kith_aoi_object_t out = {0};
        if (kith_aoi_lookup(aoi, i, &out) != 0 || out.id != i)
        {
            kith_aoi_destroy(aoi);
            return 2;
        }
    }
    kith_aoi_destroy(aoi);
    if (g_tally.frees != 4U || g_tally.outstanding != 0U)
    {
        return 3;
    }
    return 0;
}

static int test_enomem_sweep_aoi(void)
{
    unsigned attempts = 0;
    /* The create path spends the handle, the object index, and the cell
     * index; each failure unwinds through the stored instance. */
    if (sweep_create(run_aoi_create, &attempts) != 0 || attempts != 3U)
    {
        return 1;
    }
    int rc = test_aoi_insert_grow_enomem();
    if (rc == 0)
    {
        rc = test_aoi_update_move_grow_enomem();
    }
    if (rc == 0)
    {
        rc = test_aoi_insert_grow_inplace_enomem();
    }
    if (rc != 0)
    {
        return 10 + rc;
    }
    return 0;
}

/* A no-op vtable for registry-level tests: every required callback present
 * (registration validates their presence), none ever invoked. */
static int
sim_noop_init(const kith_sim_config_t *cfg, const kith_allocator_t *alloc, kith_sim_model_t **out)
{
    (void)cfg;
    (void)alloc;
    *out = nullptr;
    return kith_error_return(KITH_EINVAL);
}

static void sim_noop_destroy(kith_sim_model_t *model)
{
    (void)model;
}

static int
sim_noop_step(kith_sim_model_t *model, kith_sim_actor_t *actors, size_t count, uint32_t dt_ms)
{
    (void)model;
    (void)actors;
    (void)count;
    (void)dt_ms;
    return kith_error_return(KITH_EINVAL);
}

static int sim_noop_apply_input(kith_sim_model_t *model,
                                kith_sim_actor_t *actor,
                                const kith_sim_input_t *input)
{
    (void)model;
    (void)actor;
    (void)input;
    return kith_error_return(KITH_EINVAL);
}

static int sim_noop_load_behavior(kith_sim_model_t *model, const char *path)
{
    (void)model;
    (void)path;
    return kith_error_return(KITH_EINVAL);
}

static kith_sim_model_vtable_t valid_sim_vtable(void)
{
    return (kith_sim_model_vtable_t)
    {
        .size = sizeof(kith_sim_model_vtable_t), .abi_version = KITH_ABI_VERSION
        , .init = sim_noop_init, .destroy = sim_noop_destroy, .step = sim_noop_step,
            .apply_input = sim_noop_apply_input, .load_behavior = sim_noop_load_behavior,
    };
}

/* The registry has no public lookup, so registration state is probed
 * through create: a registered name reaches the no-op init (EINVAL), an
 * unregistered one fails with ENOENT. */
static bool sim_model_registered(kith_sim_t *sim, const char *name)
{
    kith_sim_model_t *model = (kith_sim_model_t *)&sentinel;
    return kith_sim_create_model(sim, name, nullptr, nullptr, &model) !=
           kith_error_return(KITH_ENOENT);
}

/* The member-array growth on a publish's insert path is that publish's only
 * allocation. A failed growth rolls the actor-hash insert back (the store
 * reports the same artifact count) with nothing outstanding; the create-path
 * storage is all that remains until destroy releases it. */
static int test_sim_publish_member_grow_enomem(void)
{
    kith_sim_params_t params = valid_sim_params();
    kith_sim_t *sim = nullptr;
    tally_reset(0U);
    if (kith_sim_create(&params, &g_tally_allocator, &sim) != 0)
    {
        return SWEEP_SCAFFOLD_ERROR;
    }
    kith_sim_actor_t actor = valid_sim_actor(1u);
    kith_sim_artifact_key_t key = valid_sim_key(0);
    if (kith_sim_publish_artifact(sim, &key, &actor, nullptr) != 0)
    {
        kith_sim_destroy(sim);
        return SWEEP_SCAFFOLD_ERROR;
    }
    /* A fresh cell: its member array's first growth is this publish's only
     * allocation (the second actor in the same cell fits the array the
     * first publish already grew). */
    actor = valid_sim_actor(2u);
    key = valid_sim_key(1);
    g_tally.fail_attempt = 103U;
    const int rc = kith_sim_publish_artifact(sim, &key, &actor, nullptr);
    g_tally.fail_attempt = 0U;
    if (rc != kith_error_return(KITH_ENOMEM) || g_tally.attempts != 103U || g_tally.frees != 0U ||
        g_tally.outstanding != 102U || kith_sim_artifact_count(sim) != 1u)
    {
        kith_sim_destroy(sim);
        return 1;
    }
    kith_sim_destroy(sim);
    if (g_tally.frees != 102U || g_tally.outstanding != 0U)
    {
        return 2;
    }
    return 0;
}

/* The shard grow is staged: five fresh hash tables, then the dense-array
 * realloc, then the displaced tables' release. A failure inside either
 * allocation step must leave the shard at its prior capacity with the live
 * entries intact — the artifact count and the tally both hold — and the
 * next publish grows through cleanly. */
static int test_sim_shard_grow_enomem(void)
{
    kith_sim_params_t params = valid_sim_params();
    kith_sim_t *sim = nullptr;
    tally_reset(0U);
    if (kith_sim_create(&params, &g_tally_allocator, &sim) != 0)
    {
        return SWEEP_SCAFFOLD_ERROR;
    }
    kith_sim_actor_t actor = {0};
    kith_sim_artifact_key_t key = {0};
    for (uint64_t i = 1u; i <= 12u; ++i)
    {
        actor = valid_sim_actor(i);
        key = valid_sim_key((int32_t)i);
        if (kith_sim_publish_artifact(sim, &key, &actor, nullptr) != 0)
        {
            kith_sim_destroy(sim);
            return SWEEP_SCAFFOLD_ERROR;
        }
    }
    actor = valid_sim_actor(13u);
    key = valid_sim_key(13);
    g_tally.fail_attempt = 114U;
    int rc = kith_sim_publish_artifact(sim, &key, &actor, nullptr);
    g_tally.fail_attempt = 0U;
    /* The grow's five table callocs all run before the NULL check, so a
     * first-calloc failure still spends five attempts and releases the four
     * live blocks through the allocator. */
    if (rc != kith_error_return(KITH_ENOMEM) || g_tally.attempts != 118U || g_tally.frees != 4U ||
        g_tally.outstanding != 113U || kith_sim_artifact_count(sim) != 12u)
    {
        kith_sim_destroy(sim);
        return 1;
    }
    g_tally.fail_attempt = 119U;
    rc = kith_sim_publish_artifact(sim, &key, &actor, nullptr);
    g_tally.fail_attempt = 0U;
    if (rc != kith_error_return(KITH_ENOMEM) || g_tally.attempts != 123U || g_tally.frees != 8U ||
        g_tally.outstanding != 113U || kith_sim_artifact_count(sim) != 12u)
    {
        kith_sim_destroy(sim);
        return 2;
    }
    g_tally.fail_attempt = 129U;
    rc = kith_sim_publish_artifact(sim, &key, &actor, nullptr);
    g_tally.fail_attempt = 0U;
    /* The realloc branch fails after the five tables are live: its unwind
     * releases all five, and the dense array (with the member lists the
     * rehash has not yet touched) stays at the prior capacity. */
    if (rc != kith_error_return(KITH_ENOMEM) || g_tally.attempts != 129U || g_tally.frees != 13U ||
        g_tally.outstanding != 113U || kith_sim_artifact_count(sim) != 12u)
    {
        kith_sim_destroy(sim);
        return 2;
    }
    if (kith_sim_publish_artifact(sim, &key, &actor, nullptr) != 0 ||
        kith_sim_artifact_count(sim) != 13u)
    {
        kith_sim_destroy(sim);
        return 3;
    }
    kith_sim_destroy(sim);
    if (g_tally.frees != 132U || g_tally.outstanding != 0U)
    {
        return 4;
    }
    return 0;
}

/* The registry grows its entry array before copying the new name. A failure
 * in either step leaves the registry's entries unchanged — the rejected name
 * is absent by lookup — and the growth completes on the next registration. */
static int test_sim_register_model_grow_enomem(void)
{
    kith_sim_params_t params = valid_sim_params();
    kith_sim_t *sim = nullptr;
    tally_reset(0U);
    if (kith_sim_create(&params, &g_tally_allocator, &sim) != 0)
    {
        return SWEEP_SCAFFOLD_ERROR;
    }
    kith_sim_model_vtable_t vtable = valid_sim_vtable();
    for (uint64_t i = 0u; i < 6u; ++i)
    {
        char name[8];
        (void)snprintf(name, sizeof(name), "m%llu", (unsigned long long)i);
        if (kith_sim_register_model(sim, name, &vtable) != 0)
        {
            kith_sim_destroy(sim);
            return SWEEP_SCAFFOLD_ERROR;
        }
    }
    g_tally.fail_attempt = 108U;
    int rc = kith_sim_register_model(sim, "m6", &vtable);
    g_tally.fail_attempt = 0U;
    if (rc != kith_error_return(KITH_ENOMEM) || g_tally.attempts != 108U || g_tally.frees != 0U ||
        g_tally.outstanding != 107U || sim_model_registered(sim, "m6"))
    {
        kith_sim_destroy(sim);
        return 1;
    }
    g_tally.fail_attempt = 109U;
    rc = kith_sim_register_model(sim, "m6", &vtable);
    g_tally.fail_attempt = 0U;
    if (rc != kith_error_return(KITH_ENOMEM) || g_tally.attempts != 109U || g_tally.frees != 0U ||
        g_tally.outstanding != 107U || sim_model_registered(sim, "m6"))
    {
        kith_sim_destroy(sim);
        return 2;
    }
    if (kith_sim_register_model(sim, "m6", &vtable) != 0 || !sim_model_registered(sim, "m6"))
    {
        kith_sim_destroy(sim);
        return 3;
    }
    kith_sim_destroy(sim);
    if (g_tally.frees != 108U || g_tally.outstanding != 0U)
    {
        return 4;
    }
    return 0;
}

static int test_enomem_sweep_sim(void)
{
    unsigned attempts = 0;
    /* The create path spends the handle, the registry, the two built-in
     * name copies, the store, and sixteen shards of six tables each — 101
     * attempts; each failure unwinds through the stored instance. The model
     * path (driven through the built-in tile2d init) spends the state, the
     * pending map, and the model handle. */
    if (sweep_create(run_sim_create, &attempts) != 0 || attempts != 101U)
    {
        return 1;
    }
    if (sweep_create(run_sim_create_model, &attempts) != 0 || attempts != 3U)
    {
        return 2;
    }
    int rc = test_sim_publish_member_grow_enomem();
    if (rc == 0)
    {
        rc = test_sim_shard_grow_enomem();
    }
    if (rc == 0)
    {
        rc = test_sim_register_model_grow_enomem();
    }
    if (rc != 0)
    {
        return 10 + rc;
    }
    return 0;
}

/* The shard grow is one calloc of the new table followed by an in-memory
 * rehash: the old table stays intact until the new one is populated, so a
 * failed growth leaves the shard at its prior capacity with the live cells
 * intact — the product count and the tally both hold — and the next
 * publish grows through cleanly. A small table (16 buckets, threshold 12)
 * keeps the trigger at the thirteenth cell of one shard. */
static int test_fabric_shard_grow_enomem(void)
{
    kith_sim_t *sim = nullptr;
    if (kith_sim_create(nullptr, nullptr, &sim) != 0)
    {
        return SWEEP_SCAFFOLD_ERROR;
    }
    kith_fabric_params_t params = valid_fabric_params();
    params.cell_bucket_count = 16u;
    kith_fabric_t *fabric = nullptr;
    tally_reset(0U);
    if (kith_fabric_create(&params, sim, &g_tally_allocator, &fabric) != 0)
    {
        kith_sim_destroy(sim);
        return SWEEP_SCAFFOLD_ERROR;
    }
    kith_fabric_cell_key_t key = {.zone = 0u, .cell_x = 0, .cell_y = 0, .cell_z = 0, .lod = 0u};
    for (uint32_t i = 1u; i <= 12u; ++i)
    {
        key.cell_z = (int32_t)i;
        if (kith_fabric_publish(fabric, &key, 1u, nullptr) != 0)
        {
            kith_fabric_destroy(fabric);
            kith_sim_destroy(sim);
            return SWEEP_SCAFFOLD_ERROR;
        }
    }
    key.cell_z = 13;
    g_tally.fail_attempt = 18U;
    const int rc = kith_fabric_publish(fabric, &key, 1u, nullptr);
    g_tally.fail_attempt = 0U;
    if (rc != kith_error_return(KITH_ENOMEM) || g_tally.attempts != 18U || g_tally.frees != 0U ||
        g_tally.outstanding != 17U || kith_fabric_product_count(fabric) != 12u)
    {
        kith_fabric_destroy(fabric);
        kith_sim_destroy(sim);
        return 1;
    }
    if (kith_fabric_publish(fabric, &key, 1u, nullptr) != 0 ||
        kith_fabric_product_count(fabric) != 13u)
    {
        kith_fabric_destroy(fabric);
        kith_sim_destroy(sim);
        return 2;
    }
    kith_fabric_destroy(fabric);
    kith_sim_destroy(sim);
    if (g_tally.attempts != 19U || g_tally.frees != 18U || g_tally.outstanding != 0U)
    {
        return 3;
    }
    return 0;
}

/* The subscription path has three allocations, all routed through the
 * fabric's stored instance: the subscription handle, its interest set, and
 * — on the first subscription only — the fabric's subscription table.
 * Failing each in turn leaves the fabric intact (its create-path storage
 * still outstanding) with the out slot cleared; a clean pair afterwards
 * registers, and the destroy releases everything through the stored
 * instances. */
static int test_fabric_subscription_enomem(void)
{
    kith_sim_t *sim = nullptr;
    if (kith_sim_create(nullptr, nullptr, &sim) != 0)
    {
        return SWEEP_SCAFFOLD_ERROR;
    }
    kith_fabric_params_t params = valid_fabric_params();
    kith_fabric_t *fabric = nullptr;
    tally_reset(0U);
    if (kith_fabric_create(&params, sim, &g_tally_allocator, &fabric) != 0)
    {
        kith_sim_destroy(sim);
        return SWEEP_SCAFFOLD_ERROR;
    }
    /* One arm per path allocation. The attempt counter carries across
     * calls: a run that fails on the interest set has already spent one
     * successful handle attempt, and a run that fails on the table has
     * spent both prior allocations. The handle fails on the 18th attempt,
     * the interest set on the 20th, the table on the 23rd. The tally
     * accumulates across arms — expected frees run 0, then the handle,
     * then the interest set and the handle. */
    static const struct
    {
        unsigned fail_attempt;
        unsigned expected_frees;
    } arms[] = {
        {18U, 0U},
        {20U, 1U},
        {23U, 3U},
    };
    for (size_t arm = 0; arm < sizeof(arms) / sizeof(arms[0]); arm++)
    {
        kith_fabric_subscription_t *sub = (kith_fabric_subscription_t *)&sentinel;
        g_tally.fail_attempt = arms[arm].fail_attempt;
        const int rc = kith_fabric_create_subscription(fabric, &sub);
        g_tally.fail_attempt = 0U;
        if (rc != kith_error_return(KITH_ENOMEM) || sub != nullptr ||
            g_tally.attempts != arms[arm].fail_attempt ||
            g_tally.frees != arms[arm].expected_frees || g_tally.outstanding != 17U)
        {
            kith_fabric_destroy(fabric);
            kith_sim_destroy(sim);
            return 1;
        }
    }
    kith_fabric_subscription_t *sub_a = nullptr;
    kith_fabric_subscription_t *sub_b = nullptr;
    if (kith_fabric_create_subscription(fabric, &sub_a) != 0 ||
        kith_fabric_create_subscription(fabric, &sub_b) != 0)
    {
        kith_fabric_destroy(fabric);
        kith_sim_destroy(sim);
        return 2;
    }
    kith_fabric_destroy(fabric);
    kith_sim_destroy(sim);
    if (g_tally.attempts != 28U || g_tally.frees != 25U || g_tally.outstanding != 0U)
    {
        return 3;
    }
    return 0;
}

/* The snapshot scratch is the snapshot path's one allocation: the count
 * probe (out == NULL) never allocates, the copy path sizes the scratch
 * from the probe and copies on the next attempt. Failing the sizing must
 * report -KITH_ENOMEM with the fabric's own storage untouched; the retry
 * succeeds and the destroy balances the tally. */
static int test_fabric_snapshot_scratch_enomem(void)
{
    kith_sim_t *sim = nullptr;
    if (kith_sim_create(nullptr, nullptr, &sim) != 0)
    {
        return SWEEP_SCAFFOLD_ERROR;
    }
    kith_sim_actor_t actor = valid_sim_actor(1u);
    kith_sim_artifact_key_t sim_key = valid_sim_key(0);
    if (kith_sim_publish_artifact(sim, &sim_key, &actor, nullptr) != 0)
    {
        kith_sim_destroy(sim);
        return SWEEP_SCAFFOLD_ERROR;
    }
    kith_fabric_params_t params = valid_fabric_params();
    kith_fabric_t *fabric = nullptr;
    tally_reset(0U);
    if (kith_fabric_create(&params, sim, &g_tally_allocator, &fabric) != 0)
    {
        kith_sim_destroy(sim);
        return SWEEP_SCAFFOLD_ERROR;
    }
    kith_fabric_cell_key_t key = {.zone = 0u, .cell_x = 0, .cell_y = 0, .cell_z = 0, .lod = 0u};
    size_t count = 0u;
    g_tally.fail_attempt = 18U;
    const int probe =
        kith_fabric_snapshot_cell(fabric, &key, KITH_FABRIC_LEVEL_FULL, nullptr, 0u, &count);
    if (probe != 0 || count != 1u)
    {
        g_tally.fail_attempt = 0U;
        kith_fabric_destroy(fabric);
        kith_sim_destroy(sim);
        return 1;
    }
    kith_fabric_artifact_t arts[4];
    const int rc =
        kith_fabric_snapshot_cell(fabric, &key, KITH_FABRIC_LEVEL_FULL, arts, 4u, &count);
    g_tally.fail_attempt = 0U;
    if (rc != kith_error_return(KITH_ENOMEM) || g_tally.attempts != 18U || g_tally.frees != 0U ||
        g_tally.outstanding != 17U)
    {
        kith_fabric_destroy(fabric);
        kith_sim_destroy(sim);
        return 2;
    }
    if (kith_fabric_snapshot_cell(fabric, &key, KITH_FABRIC_LEVEL_FULL, arts, 4u, &count) != 0 ||
        count != 1u)
    {
        kith_fabric_destroy(fabric);
        kith_sim_destroy(sim);
        return 3;
    }
    kith_fabric_destroy(fabric);
    kith_sim_destroy(sim);
    if (g_tally.attempts != 19U || g_tally.frees != 18U || g_tally.outstanding != 0U)
    {
        return 4;
    }
    return 0;
}

static int test_enomem_sweep_fabric(void)
{
    unsigned attempts = 0;
    /* The create path spends the handle and the sixteen shard tables; each
     * failure unwinds through the stored instance. */
    if (sweep_create(run_fabric_create, &attempts) != 0 || attempts != 17U)
    {
        return 1;
    }
    int rc = test_fabric_shard_grow_enomem();
    if (rc == 0)
    {
        rc = test_fabric_subscription_enomem();
    }
    if (rc == 0)
    {
        rc = test_fabric_snapshot_scratch_enomem();
    }
    if (rc != 0)
    {
        return 10 + rc;
    }
    return 0;
}

/* The window array is the session-side growth path: the first window add
 * sizes the window (8 cells) before subscribing its cell. Failing the
 * sizing must report -KITH_ENOMEM with the window still empty; the failed
 * add is retained for the tick pass's retry, whose ring allocation rides
 * the same allocator and succeeds, so the failed add leaves one live
 * allocation beyond the session's own. The retry subscribes cleanly and
 * the destroy balances. */
static int test_gateway_window_grow_enomem(void)
{
    struct gateway_scaffold fx;
    if (gateway_scaffold_init(&fx) != 0 || kith_net_listen(fx.net, "127.0.0.1", 0) != 0)
    {
        gateway_scaffold_fini(&fx);
        return SWEEP_SCAFFOLD_ERROR;
    }
    kith_gateway_t *gw = nullptr;
    kith_net_conn_t *conn = nullptr;
    kith_gateway_session_t *session = nullptr;
    int rc = SWEEP_SCAFFOLD_ERROR;
    tally_reset(0U);
    if (kith_gateway_create(nullptr, fx.net, fx.fabric, fx.proto, &g_tally_allocator, &gw) == 0 &&
        gateway_test_conn(fx.net, &conn) == 0 &&
        kith_gateway_session_create(
            gw, conn, KITH_GATEWAY_SESSION_SUBSCRIBER, 1u, nullptr, &session) == 0)
    {
        rc = 0;
    }
    if (rc != 0)
    {
        if (session != nullptr)
        {
            kith_gateway_session_destroy(session);
        }
        if (gw != nullptr)
        {
            kith_gateway_destroy(gw);
        }
        if (conn != nullptr)
        {
            kith_net_conn_close(conn);
            kith_net_conn_release(conn);
        }
        gateway_scaffold_fini(&fx);
        return SWEEP_SCAFFOLD_ERROR;
    }
    /* Create spends 23 attempts and the session 2 more (all outstanding);
     * the window sizing is the next attempt, and the retention's ring
     * allocation follows it. */
    const kith_fabric_cell_key_t key = {
        .zone = 0u, .cell_x = 0, .cell_y = 0, .cell_z = 0, .lod = 0u};
    g_tally.fail_attempt = 26U;
    const int grow_rc = kith_gateway_session_window_add(session, &key);
    g_tally.fail_attempt = 0U;
    size_t count = 0u;
    if (grow_rc != kith_error_return(KITH_ENOMEM) || g_tally.attempts != 27U ||
        g_tally.outstanding != 26U || kith_gateway_session_window_count(session, &count) != 0 ||
        count != 0u)
    {
        rc = 1;
    }
    else if (kith_gateway_session_window_add(session, &key) != 0 || g_tally.attempts != 28U)
    {
        rc = 2;
    }
    else if (kith_gateway_session_window_remove(session, &key) != 0)
    {
        rc = 3;
    }
    kith_gateway_session_destroy(session);
    kith_gateway_destroy(gw);
    kith_net_conn_close(conn);
    kith_net_conn_release(conn);
    gateway_scaffold_fini(&fx);
    if (rc == 0 && (g_tally.attempts != 28U || g_tally.frees != 27U || g_tally.outstanding != 0U))
    {
        rc = 4;
    }
    return rc;
}

static int
route_fail_session_init(const kith_gateway_session_t *session, const void *config, void **out_state)
{
    (void)session;
    (void)config;
    (void)out_state;
    return 0;
}

static int route_fail_deliver(void *state,
                              kith_gateway_t *gateway,
                              kith_gateway_session_t *session,
                              const kith_gateway_view_subject_t *subjects,
                              size_t subject_count,
                              uint64_t now_ms,
                              kith_gateway_delivery_stats_t *out_stats)
{
    (void)state;
    (void)gateway;
    (void)session;
    (void)subjects;
    (void)subject_count;
    (void)now_ms;
    (void)out_stats;
    return 0;
}

/* The delivery registry is the gateway-side grow path reached through the
 * public registration call. Two unarmed registrations fill the registry
 * to capacity so the armed third exercises the growth: failing the
 * growth leaves the four earlier registrations untouched, and failing
 * the name copy after a successful growth leaves the grown array for the
 * destroy unwind. Both release everything through the stored instance;
 * the growth's replaced block is released by realloc itself, so both
 * arms settle at 25 outstanding allocations. */
static int test_gateway_register_enomem(void)
{
    static const kith_gateway_delivery_vtable_t route_fail_vtable = {
        .size = sizeof(kith_gateway_delivery_vtable_t),
        .abi_version = KITH_ABI_VERSION,
        .session_init = route_fail_session_init,
        .session_fini = nullptr,
        .deliver = route_fail_deliver,
    };
    /* Attempts 1–25 are create plus the two fills; the armed
     * registration's entry-array growth is 26, its name copy 27. */
    static const unsigned fail_attempts[] = {26U, 27U};
    for (size_t arm = 0; arm < sizeof(fail_attempts) / sizeof(fail_attempts[0]); arm++)
    {
        struct gateway_scaffold fx;
        if (gateway_scaffold_init(&fx) != 0)
        {
            gateway_scaffold_fini(&fx);
            return SWEEP_SCAFFOLD_ERROR;
        }
        kith_gateway_t *gw = nullptr;
        tally_reset(0U);
        if (kith_gateway_create(nullptr, fx.net, fx.fabric, fx.proto, &g_tally_allocator, &gw) !=
                0 ||
            kith_gateway_register_delivery(gw, "route_a", &route_fail_vtable) != 0 ||
            kith_gateway_register_delivery(gw, "route_b", &route_fail_vtable) != 0)
        {
            kith_gateway_destroy(gw);
            gateway_scaffold_fini(&fx);
            return SWEEP_SCAFFOLD_ERROR;
        }
        g_tally.fail_attempt = fail_attempts[arm];
        const int rc = kith_gateway_register_delivery(gw, "route_fail", &route_fail_vtable);
        g_tally.fail_attempt = 0U;
        if (rc != kith_error_return(KITH_ENOMEM) || g_tally.attempts != fail_attempts[arm] ||
            g_tally.outstanding != 25U)
        {
            kith_gateway_destroy(gw);
            gateway_scaffold_fini(&fx);
            return 1;
        }
        kith_gateway_destroy(gw);
        gateway_scaffold_fini(&fx);
        if (g_tally.frees != 25U || g_tally.outstanding != 0U)
        {
            return 2;
        }
    }
    return 0;
}

/* The dispatch work record is the pool-bound dispatch path's one
 * allocation. Failing it must report -KITH_ENOMEM before the session
 * reference is acquired and before the drop counter moves (drops count
 * submit exhaustion, not allocation failure); the destroy balances. */
static void route_fail_handler(uint16_t msg_type,
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

/* Armed setup for the dispatch-work arm: gateway, connected session, and
 * a one-worker pool with a pool-routed handler. Returns 0 with every out
 * param set; nonzero leaves everything torn down. */
static int dispatch_arm_setup(struct gateway_scaffold *fx,
                              kith_gateway_t **out_gw,
                              kith_net_conn_t **out_conn,
                              kith_gateway_session_t **out_session,
                              kith_worker_t **out_pool)
{
    *out_gw = nullptr;
    *out_conn = nullptr;
    *out_session = nullptr;
    *out_pool = nullptr;
    if (gateway_scaffold_init(fx) != 0 || kith_net_listen(fx->net, "127.0.0.1", 0) != 0)
    {
        gateway_scaffold_fini(fx);
        return 1;
    }
    int rc = 1;
    tally_reset(0U);
    if (kith_gateway_create(nullptr, fx->net, fx->fabric, fx->proto, &g_tally_allocator, out_gw) ==
            0 &&
        gateway_test_conn(fx->net, out_conn) == 0 &&
        kith_gateway_session_create(
            *out_gw, *out_conn, KITH_GATEWAY_SESSION_SUBSCRIBER, 1u, nullptr, out_session) == 0 &&
        kith_worker_create(
            &(kith_worker_params_t) {
                .size = sizeof(kith_worker_params_t), .abi_version = KITH_ABI_VERSION
                , .worker_count = 1U, .task_capacity = 4U,
            },
            nullptr,
            out_pool) == 0 &&
        kith_gateway_attach_worker_pool(*out_gw, *out_pool) == 0 &&
        kith_gateway_register_handler_flags(
            *out_gw, 5U, route_fail_handler, nullptr, KITH_GATEWAY_HANDLER_POOL) == 0)
    {
        rc = 0;
    }
    if (rc != 0)
    {
        if (*out_pool != nullptr)
        {
            kith_worker_destroy(*out_pool);
        }
        if (*out_session != nullptr)
        {
            kith_gateway_session_destroy(*out_session);
        }
        if (*out_gw != nullptr)
        {
            kith_gateway_destroy(*out_gw);
        }
        if (*out_conn != nullptr)
        {
            kith_net_conn_close(*out_conn);
            kith_net_conn_release(*out_conn);
        }
        gateway_scaffold_fini(fx);
    }
    return rc;
}

static int test_gateway_dispatch_work_enomem(void)
{
    struct gateway_scaffold fx;
    kith_gateway_t *gw = nullptr;
    kith_net_conn_t *conn = nullptr;
    kith_gateway_session_t *session = nullptr;
    kith_worker_t *pool = nullptr;
    if (dispatch_arm_setup(&fx, &gw, &conn, &session, &pool) != 0)
    {
        return SWEEP_SCAFFOLD_ERROR;
    }
    int rc = 0;
    const uint8_t payload[4] = {0u, 0u, 0u, 0u};
    const kith_proto_frame_t frame = {
        .type_id = 5U,
        .flags = 0u,
        .has_correlation = false,
        .correlation_id = 0u,
        .payload = payload,
        .payload_len = sizeof(payload),
    };
    /* Create and session spend 25 attempts, all outstanding; the work
     * record is the next attempt. */
    g_tally.fail_attempt = 26U;
    const int dispatch_rc = kith_gateway_dispatch(gw, conn, &frame);
    g_tally.fail_attempt = 0U;
    uint64_t drops = 0u;
    if (dispatch_rc != kith_error_return(KITH_ENOMEM) || g_tally.attempts != 26U ||
        g_tally.outstanding != 25U || kith_gateway_dispatch_drops(gw, &drops) != 0 || drops != 0u)
    {
        rc = 1;
    }
    kith_gateway_session_destroy(session);
    kith_gateway_destroy(gw);
    kith_worker_destroy(pool);
    kith_net_conn_close(conn);
    kith_net_conn_release(conn);
    gateway_scaffold_fini(&fx);
    if (rc == 0 && (g_tally.attempts != 26U || g_tally.frees != 25U || g_tally.outstanding != 0U))
    {
        rc = 2;
    }
    return rc;
}

static int test_enomem_sweep_gateway(void)
{
    unsigned attempts = 0;
    /* The create path spends the handle, the session table, sixteen cache
     * stripe tables, the handler table, the registry's growth and two name
     * copies, and the default strategy name; with the delivery executor
     * configured, the executor struct and the gateway-owned worker pool
     * (pool struct, task nodes, thread array) ride the same allocator.
     * Each failure unwinds through the stored instance. The session create
     * spends the session struct and the full strategy's state through the
     * session's own allocator. */
    if (sweep_create(run_gateway_create, &attempts) != 0 || attempts != 23U)
    {
        return 1;
    }
    if (sweep_create(run_gateway_create_executor, &attempts) != 0 || attempts != 27U)
    {
        return 2;
    }
    if (sweep_create(run_gateway_session_create, &attempts) != 0 || attempts != 2U)
    {
        return 3;
    }
    int rc = test_gateway_window_grow_enomem();
    if (rc == 0)
    {
        rc = test_gateway_register_enomem();
    }
    if (rc == 0)
    {
        rc = test_gateway_dispatch_work_enomem();
    }
    if (rc != 0)
    {
        return 10 + rc;
    }
    return 0;
}

/* The membership table starts at eight slots; the ninth add grows it. The
 * failed growth releases nothing (the tally counts the attempt, the
 * membership keeps eight entries), and the retry succeeds. The growth's
 * realloc replaces the live table internally, so four attempts close as two
 * frees. */
static int test_coord_bus_member_grow_enomem(void)
{
    kith_coord_bus_t *bus = nullptr;
    tally_reset(0U);
    if (kith_coord_bus_create(nullptr, &g_tally_allocator, &bus) != 0)
    {
        return SWEEP_SCAFFOLD_ERROR;
    }
    for (uint32_t id = 2u; id <= 8u; ++id)
    {
        if (kith_coord_bus_add_member(bus, id) != 0)
        {
            kith_coord_bus_destroy(bus);
            return SWEEP_SCAFFOLD_ERROR;
        }
    }
    g_tally.fail_attempt = 3U;
    const int rc = kith_coord_bus_add_member(bus, 9u);
    g_tally.fail_attempt = 0U;
    if (rc != kith_error_return(KITH_ENOMEM) || g_tally.attempts != 3U || g_tally.frees != 0U ||
        g_tally.outstanding != 2U || kith_coord_bus_member_count(bus) != 8u)
    {
        kith_coord_bus_destroy(bus);
        return 1;
    }
    if (kith_coord_bus_add_member(bus, 9u) != 0 || kith_coord_bus_member_count(bus) != 9u)
    {
        kith_coord_bus_destroy(bus);
        return 2;
    }
    kith_coord_bus_destroy(bus);
    if (g_tally.attempts != 4U || g_tally.frees != 2U || g_tally.outstanding != 0U)
    {
        return 3;
    }
    return 0;
}

/* The subscription table grows on the first subscribe. The failed growth
 * leaves no subscription; the retry subscribes and the unsubscribe removes
 * the entry without releasing the table. */
static int test_coord_bus_subscribe_grow_enomem(void)
{
    kith_coord_bus_t *bus = nullptr;
    tally_reset(0U);
    if (kith_coord_bus_create(nullptr, &g_tally_allocator, &bus) != 0)
    {
        return SWEEP_SCAFFOLD_ERROR;
    }
    g_tally.fail_attempt = 3U;
    const int rc = kith_coord_bus_subscribe(bus, 7u);
    g_tally.fail_attempt = 0U;
    if (rc != kith_error_return(KITH_ENOMEM) || g_tally.attempts != 3U || g_tally.frees != 0U ||
        g_tally.outstanding != 2U)
    {
        kith_coord_bus_destroy(bus);
        return 1;
    }
    if (kith_coord_bus_subscribe(bus, 7u) != 0 || kith_coord_bus_unsubscribe(bus, 7u) != 0)
    {
        kith_coord_bus_destroy(bus);
        return 2;
    }
    kith_coord_bus_destroy(bus);
    if (g_tally.attempts != 4U || g_tally.frees != 3U || g_tally.outstanding != 0U)
    {
        return 3;
    }
    return 0;
}

/* One arm per publish allocation: the pending ring and the payload copy.
 * The ring's failure publishes nothing; the payload's failure leaves the
 * staged slot uncounted (drain sees no event). */
static int test_coord_bus_publish_enomem(void)
{
    const uint8_t payload[8] = {1u, 2u, 3u, 4u, 5u, 6u, 7u, 8u};
    /* Ring failure. */
    kith_coord_bus_t *bus = nullptr;
    tally_reset(0U);
    if (kith_coord_bus_create(nullptr, &g_tally_allocator, &bus) != 0)
    {
        return SWEEP_SCAFFOLD_ERROR;
    }
    g_tally.fail_attempt = 3U;
    int rc =
        kith_coord_bus_publish(bus, KITH_COORD_BUS_EVENT_REBALANCE, 7u, payload, sizeof(payload));
    g_tally.fail_attempt = 0U;
    size_t count = 0u;
    if (rc != kith_error_return(KITH_ENOMEM) || g_tally.attempts != 3U || g_tally.frees != 0U ||
        g_tally.outstanding != 2U || kith_coord_bus_drain(bus, nullptr, 0u, &count) != 0 ||
        count != 0u)
    {
        kith_coord_bus_destroy(bus);
        return 1;
    }
    kith_coord_bus_destroy(bus);
    if (g_tally.attempts != 3U || g_tally.frees != 2U || g_tally.outstanding != 0U)
    {
        return 2;
    }
    /* Payload failure: the ring is outstanding, the slot is uncounted. */
    bus = nullptr;
    tally_reset(0U);
    if (kith_coord_bus_create(nullptr, &g_tally_allocator, &bus) != 0)
    {
        return SWEEP_SCAFFOLD_ERROR;
    }
    g_tally.fail_attempt = 4U;
    rc = kith_coord_bus_publish(bus, KITH_COORD_BUS_EVENT_REBALANCE, 7u, payload, sizeof(payload));
    g_tally.fail_attempt = 0U;
    if (rc != kith_error_return(KITH_ENOMEM) || g_tally.attempts != 4U || g_tally.frees != 0U ||
        g_tally.outstanding != 3U || kith_coord_bus_drain(bus, nullptr, 0u, &count) != 0 ||
        count != 0u)
    {
        kith_coord_bus_destroy(bus);
        return 3;
    }
    kith_coord_bus_destroy(bus);
    if (g_tally.attempts != 4U || g_tally.frees != 3U || g_tally.outstanding != 0U)
    {
        return 4;
    }
    return 0;
}

/* A clean publish/drain/publish/drain pass with the swap between: the
 * drained ring and its payload release on the second drain, and the
 * swapped-in side (plus the handle and the membership table) on destroy. */
static int test_coord_bus_publish_swap_enomem(void)
{
    const uint8_t payload[8] = {1u, 2u, 3u, 4u, 5u, 6u, 7u, 8u};
    kith_coord_bus_t *bus = nullptr;
    tally_reset(0U);
    if (kith_coord_bus_create(nullptr, &g_tally_allocator, &bus) != 0)
    {
        return SWEEP_SCAFFOLD_ERROR;
    }
    kith_coord_bus_event_t events[2] = {{0}};
    size_t count = 0u;
    if (kith_coord_bus_publish(bus, KITH_COORD_BUS_EVENT_REBALANCE, 7u, payload, sizeof(payload)) !=
        0)
    {
        kith_coord_bus_destroy(bus);
        return SWEEP_SCAFFOLD_ERROR;
    }
    if (kith_coord_bus_drain(bus, events, 2u, &count) != 0 || count != 1u ||
        events[0].payload_len != sizeof(payload) ||
        memcmp(events[0].payload, payload, sizeof(payload)) != 0)
    {
        kith_coord_bus_destroy(bus);
        return 1;
    }
    if (kith_coord_bus_publish(bus, KITH_COORD_BUS_EVENT_REBALANCE, 7u, payload, sizeof(payload)) !=
            0 ||
        kith_coord_bus_drain(bus, events, 2u, &count) != 0 || count != 1u)
    {
        kith_coord_bus_destroy(bus);
        return 2;
    }
    kith_coord_bus_destroy(bus);
    if (g_tally.attempts != 6U || g_tally.frees != 6U || g_tally.outstanding != 0U)
    {
        return 3;
    }
    return 0;
}

static int test_enomem_sweep_coord(void)
{
    unsigned attempts = 0;
    /* The create path spends the handle, the sixteen shard tables, and the
     * density table; the bus create spends the handle and the initial
     * member. Each failure unwinds through the stored instance. */
    if (sweep_create(run_coord_create, &attempts) != 0 || attempts != 18U)
    {
        return 1;
    }
    if (sweep_create(run_coord_bus_create, &attempts) != 0 || attempts != 2U)
    {
        return 2;
    }
    int rc = test_coord_bus_member_grow_enomem();
    if (rc == 0)
    {
        rc = test_coord_bus_subscribe_grow_enomem();
    }
    if (rc == 0)
    {
        rc = test_coord_bus_publish_enomem();
    }
    if (rc == 0)
    {
        rc = test_coord_bus_publish_swap_enomem();
    }
    if (rc != 0)
    {
        return 10 + rc;
    }
    return 0;
}

static int test_enomem_sweep_control(void)
{
    unsigned attempts = 0;
    /* The create path spends the handle, the connection table, and the
     * event-bus records; each failure unwinds through the stored
     * instance. The per-connection buffers allocate and free through it
     * too, covered at the conn level by test_control_conn. */
    if (sweep_create(run_control_create, &attempts) != 0 || attempts != 3U)
    {
        return 1;
    }
    return 0;
}

/* The step-table copy is the configure path's only allocation (the fourth
 * attempt after create's three); its failure leaves the handle intact and
 * the retry succeeds. */
static int test_client_configure_enomem(void)
{
    kith_proto_t *proto = client_scaffold_proto();
    if (proto == nullptr)
    {
        return SWEEP_SCAFFOLD_ERROR;
    }
    kith_client_t *client = nullptr;
    tally_reset(0U);
    if (kith_client_create(nullptr, proto, nullptr, &g_tally_allocator, &client) != 0)
    {
        return SWEEP_SCAFFOLD_ERROR;
    }
    const kith_client_bootstrap_step_t steps[1] = {{.await_type_id = 200u}};
    g_tally.fail_attempt = 4U;
    const int rc = kith_client_configure_bootstrap(client, steps, 1);
    g_tally.fail_attempt = 0U;
    if (rc != kith_error_return(KITH_ENOMEM) || g_tally.attempts != 4U || g_tally.frees != 0U ||
        g_tally.outstanding != 3U)
    {
        kith_client_destroy(client);
        return 1;
    }
    if (kith_client_configure_bootstrap(client, steps, 1) != 0)
    {
        kith_client_destroy(client);
        return 2;
    }
    kith_client_destroy(client);
    if (g_tally.attempts != 5U || g_tally.frees != 4U || g_tally.outstanding != 0U)
    {
        return 3;
    }
    return 0;
}

/* One arm per submit allocation: the encode scratch and the queued slot
 * copy. A scratch failure publishes nothing; a copy failure releases the
 * scratch and leaves the ring empty; the success run pops its copy and
 * destroy balances. */
static int test_client_submit_enomem(void)
{
    kith_proto_t *proto = client_scaffold_proto();
    if (proto == nullptr)
    {
        return SWEEP_SCAFFOLD_ERROR;
    }
    const uint8_t payload[4] = {1u, 2u, 3u, 4u};
    const kith_client_command_t command = {
        .type_id = 100u,
        .flags = 0u,
        .correlation_id = 0u,
        .payload = payload,
        .payload_len = sizeof(payload),
    };
    uint8_t sink[128];
    uint32_t len = 0u;
    kith_client_t *client = nullptr;
    tally_reset(0U);
    if (kith_client_create(nullptr, proto, nullptr, &g_tally_allocator, &client) != 0)
    {
        return SWEEP_SCAFFOLD_ERROR;
    }
    /* Scratch failure. */
    g_tally.fail_attempt = 4U;
    int rc = kith_client_submit_interactive(client, &command, nullptr);
    g_tally.fail_attempt = 0U;
    if (rc != kith_error_return(KITH_ENOMEM) || g_tally.attempts != 4U || g_tally.frees != 0U ||
        g_tally.outstanding != 3U)
    {
        kith_client_destroy(client);
        return 1;
    }
    kith_client_destroy(client);
    if (g_tally.attempts != 4U || g_tally.frees != 3U || g_tally.outstanding != 0U)
    {
        return 2;
    }
    /* Copy failure: the scratch is spent and released, the ring stays
     * empty. */
    client = nullptr;
    tally_reset(0U);
    if (kith_client_create(nullptr, proto, nullptr, &g_tally_allocator, &client) != 0)
    {
        return SWEEP_SCAFFOLD_ERROR;
    }
    g_tally.fail_attempt = 5U;
    rc = kith_client_submit_interactive(client, &command, nullptr);
    g_tally.fail_attempt = 0U;
    if (rc != kith_error_return(KITH_ENOMEM) || g_tally.attempts != 5U || g_tally.frees != 1U ||
        g_tally.outstanding != 3U)
    {
        kith_client_destroy(client);
        return 3;
    }
    /* The retry succeeds; pop releases the copy, destroy the rest. */
    if (kith_client_submit_interactive(client, &command, nullptr) != 0 ||
        kith_client_pop_outbound(client, sink, sizeof(sink), &len) != 0 || len == 0u)
    {
        kith_client_destroy(client);
        return 4;
    }
    kith_client_destroy(client);
    if (g_tally.attempts != 7U || g_tally.frees != 6U || g_tally.outstanding != 0U)
    {
        return 5;
    }
    return 0;
}

static int test_enomem_sweep_client(void)
{
    unsigned attempts = 0;
    /* The create path spends the handle, the outbound slot table, and the
     * event ring; each failure unwinds through the stored instance. */
    if (sweep_create(run_client_create, &attempts) != 0 || attempts != 3U)
    {
        return 1;
    }
    int rc = test_client_configure_enomem();
    if (rc == 0)
    {
        rc = test_client_submit_enomem();
    }
    if (rc != 0)
    {
        return 10 + rc;
    }
    return 0;
}

static int test_enomem_sweep_server(void)
{
    unsigned attempts = 0;
    /* The create path spends the handle, the delivery strategy copy, the
     * delivery configuration image, and the wire's connection table; each
     * failure unwinds through the stored instance. */
    if (sweep_create(run_server_create, &attempts) != 0 || attempts != 4U)
    {
        return 1;
    }
    return 0;
}

int main(void)
{
    int rc = test_allocator_contract();
    if (rc == 0)
    {
        rc = 10 * test_allocator_validation();
    }
    /* One stage per module section; each weight spaces the composite exit
     * codes so the failing stage is recoverable from the exit status. */
    static const struct
    {
        int weight;
        int (*run)(void);
    } stages[] = {
        {20, test_custom_allocator_routing},   {25, test_config_allocator_routing},
        {30, test_enomem_sweep_util},          {40, test_enomem_sweep_config},
        {45, test_logger_allocator_routing},   {50, test_enomem_sweep_logger},
        {55, test_metrics_allocator_routing},  {60, test_enomem_sweep_metrics},
        {65, test_proto_allocator_routing},    {70, test_enomem_sweep_proto},
        {75, test_net_allocator_routing},      {80, test_enomem_sweep_net},
        {85, test_reactor_allocator_routing},  {90, test_enomem_sweep_reactor},
        {95, test_worker_allocator_routing},   {100, test_enomem_sweep_worker},
        {105, test_state_allocator_routing},   {110, test_enomem_sweep_state},
        {115, test_db_allocator_routing},      {120, test_enomem_sweep_db},
        {125, test_aoi_allocator_routing},     {130, test_enomem_sweep_aoi},
        {135, test_sim_allocator_routing},     {140, test_enomem_sweep_sim},
        {145, test_fabric_allocator_routing},  {150, test_enomem_sweep_fabric},
        {155, test_gateway_allocator_routing}, {160, test_enomem_sweep_gateway},
        {165, test_coord_allocator_routing},   {170, test_enomem_sweep_coord},
        {175, test_control_allocator_routing}, {180, test_enomem_sweep_control},
        {185, test_client_allocator_routing},  {190, test_enomem_sweep_client},
        {195, test_server_allocator_routing},  {200, test_enomem_sweep_server},
    };
    for (size_t i = 0; rc == 0 && i < sizeof(stages) / sizeof(stages[0]); ++i)
    {
        rc = stages[i].weight * stages[i].run();
    }
    return rc;
}
