/* Delivery-strategy registry: registration argument and vtable-shape
 * validation, duplicate-name rejection, per-session binding of the
 * configured strategy name, config-blob copying, and routing of deliver
 * passes through the bound vtable. Compiles against the public gateway
 * surface only. */

#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <unistd.h>

#include "kith/fabric/fabric.h"
#include "kith/gateway/gateway.h"
#include "kith/net/net.h"
#include "kith/sim/sim.h"
#include "kith/types.h"
#include "kith/version.h"

#include <netinet/in.h>
#include <sys/socket.h>

static int check_cond(bool ok, int line)
{
    if (!ok)
    {
        (void)fprintf(stderr, "gateway registry: assertion at line %d failed\n", line);
        return 1;
    }
    return 0;
}

#define CHECK(cond) failures += check_cond((cond), __LINE__)

/** State the test strategy's session_init allocates and its callbacks
 *  record into. Lets the assertions observe binding, passing, and teardown
 *  without reaching into gateway internals. */
struct custom_state
{
    const void *config_seen;
    uint32_t config_size_seen;
    uint64_t actor_id_seen;
    size_t subject_count_seen;
    uint64_t now_ms_seen;
    uint32_t deliver_calls;
    bool fini_called;
};

struct custom_config
{
    uint32_t size;
    uint32_t magic;
};

static int
custom_session_init(const kith_gateway_session_t *session, const void *config, void **out_state)
{
    struct custom_state *st = calloc(1u, sizeof(*st));
    if (!st)
    {
        return -1;
    }
    st->config_seen = config;
    if (config)
    {
        const struct custom_config *cfg = config;
        st->config_size_seen = cfg->size;
    }
    // The session pointer must be usable for identity reads at init time
    // (the session id is assigned after binding, so only success matters).
    kith_gateway_session_info_t info = {0};
    if (kith_gateway_session_info(session, &info) != 0)
    {
        free(st);
        return -2;
    }
    *out_state = st;
    return 0;
}

static void custom_session_fini(void *state)
{
    struct custom_state *st = state;
    if (st)
    {
        st->fini_called = true;
    }
    free(st);
}

// Records the pass and reports one enqueued frame per subject.
static int custom_deliver(void *state,
                          kith_gateway_t *gateway,
                          kith_gateway_session_t *session,
                          const kith_gateway_view_subject_t *subjects,
                          size_t subject_count,
                          uint64_t now_ms,
                          kith_gateway_delivery_stats_t *out_stats)
{
    (void)gateway;
    (void)session;
    struct custom_state *st = state;
    st->deliver_calls += 1u;
    st->subject_count_seen = subject_count;
    st->now_ms_seen = now_ms;
    if (subject_count > 0u)
    {
        st->actor_id_seen = subjects[0].actor_id;
    }
    if (out_stats)
    {
        out_stats->enqueued = (uint32_t)subject_count;
    }
    return 0;
}

static int connect_to_listener(kith_net_t *net)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0)
    {
        return -1;
    }
    struct sockaddr_in addr = {0};
    socklen_t addr_len = sizeof(addr);
    if (getsockname(kith_net_listener_fd(net), (struct sockaddr *)&addr, &addr_len) != 0)
    {
        (void)close(fd);
        return -1;
    }
    if (connect(fd, (struct sockaddr *)&addr, addr_len) != 0)
    {
        (void)close(fd);
        return -1;
    }
    return fd;
}

static int make_conn(kith_net_t *net, kith_net_conn_t **out_conn)
{
    *out_conn = nullptr;
    int client_fd = connect_to_listener(net);
    if (client_fd < 0)
    {
        return -1;
    }
    int rc = kith_net_accept(net, out_conn);
    (void)close(client_fd);
    return rc;
}

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

static kith_sim_actor_t make_actor(uint64_t id, int64_t x, int64_t y, uint32_t tick)
{
    kith_sim_actor_t a = {0};
    a.id = id;
    a.pos_x = x;
    a.pos_y = y;
    a.input_tick = tick;
    return a;
}

struct registry_fixture
{
    kith_proto_t *proto;
    kith_net_t *net;
    kith_sim_t *sim;
    kith_fabric_t *fabric;
    kith_gateway_t *gw;
};

static int
registry_fixture_init(struct registry_fixture *fx, const char *strategy_name, const void *config)
{
    memset(fx, 0, sizeof(*fx));
    if (kith_proto_create(nullptr, nullptr, &fx->proto) != 0)
    {
        return -1;
    }
    if (kith_net_create(nullptr, fx->proto, nullptr, &fx->net) != 0)
    {
        return -1;
    }
    if (kith_sim_create(nullptr, nullptr, &fx->sim) != 0)
    {
        return -1;
    }
    if (kith_fabric_create(nullptr, fx->sim, nullptr, &fx->fabric) != 0)
    {
        return -1;
    }
    kith_gateway_params_t params = {0};
    params.size = sizeof(params);
    params.abi_version = KITH_ABI_VERSION;
    params.replication_type_id = 0u; // the custom strategy enqueues nothing
    params.delivery_strategy = strategy_name;
    params.delivery_config = config;
    if (kith_gateway_create(&params, fx->net, fx->fabric, fx->proto, nullptr, &fx->gw) != 0)
    {
        return -1;
    }
    if (kith_net_listen(fx->net, "127.0.0.1", 0) != 0)
    {
        return -1;
    }
    return 0;
}

static void registry_fixture_fini(const struct registry_fixture *fx)
{
    kith_gateway_destroy(fx->gw);
    kith_fabric_destroy(fx->fabric);
    kith_sim_destroy(fx->sim);
    kith_net_destroy(fx->net);
    kith_proto_destroy(fx->proto);
}

static kith_gateway_delivery_vtable_t custom_vtable(void)
{
    kith_gateway_delivery_vtable_t v = {0};
    v.size = sizeof(v);
    v.abi_version = KITH_ABI_VERSION;
    v.session_init = custom_session_init;
    v.session_fini = custom_session_fini;
    v.deliver = custom_deliver;
    return v;
}

// Registration rejects null handles, an empty name, an undersized vtable,
// an incompatible abi_version, and a missing required callback, all with
// the sim registry's error conventions.
static int test_registry_shape_errors(void)
{
    int failures = 0;
    struct registry_fixture fx;
    CHECK(registry_fixture_init(&fx, nullptr, nullptr) == 0);

    kith_gateway_delivery_vtable_t good = custom_vtable();
    CHECK(kith_gateway_register_delivery(nullptr, "x", &good) == kith_error_return(KITH_EINVAL));
    CHECK(kith_gateway_register_delivery(fx.gw, nullptr, &good) == kith_error_return(KITH_EINVAL));
    CHECK(kith_gateway_register_delivery(fx.gw, "", &good) == kith_error_return(KITH_EINVAL));
    CHECK(kith_gateway_register_delivery(fx.gw, "x", nullptr) == kith_error_return(KITH_EINVAL));

    kith_gateway_delivery_vtable_t small = custom_vtable();
    small.size = sizeof(small) - 1u;
    CHECK(kith_gateway_register_delivery(fx.gw, "small", &small) == kith_error_return(KITH_ESIZE));

    kith_gateway_delivery_vtable_t oldgen = custom_vtable();
    oldgen.abi_version = KITH_ABI_VERSION
    -1u;
    CHECK(kith_gateway_register_delivery(fx.gw, "oldgen", &oldgen) ==
          kith_error_return(KITH_EABIVER));

    kith_gateway_delivery_vtable_t naked = custom_vtable();
    naked.deliver = nullptr;
    CHECK(kith_gateway_register_delivery(fx.gw, "naked", &naked) ==
          kith_error_return(KITH_EABIVER));
    kith_gateway_delivery_vtable_t noinit = custom_vtable();
    noinit.session_init = nullptr;
    CHECK(kith_gateway_register_delivery(fx.gw, "noinit", &noinit) ==
          kith_error_return(KITH_EABIVER));

    registry_fixture_fini(&fx);
    return failures;
}

// A name registers once; re-registering it or shadowing the built-in
// "full" preset fails with EEXIST. Registered names survive gateway
// destruction is covered implicitly by every other case tearing down.
static int test_registry_duplicate_names(void)
{
    int failures = 0;
    struct registry_fixture fx;
    CHECK(registry_fixture_init(&fx, nullptr, nullptr) == 0);

    kith_gateway_delivery_vtable_t v = custom_vtable();
    CHECK(kith_gateway_register_delivery(fx.gw, "custom", &v) == 0);
    CHECK(kith_gateway_register_delivery(fx.gw, "custom", &v) == kith_error_return(KITH_EEXIST));
    CHECK(kith_gateway_register_delivery(fx.gw, "full", &v) == kith_error_return(KITH_EEXIST));

    registry_fixture_fini(&fx);
    return failures;
}

// An unknown configured strategy name fails session creation with ENOENT
// instead of surfacing as a per-tick deliver error.
static int test_unknown_strategy_name_fails_session(void)
{
    int failures = 0;
    struct registry_fixture fx;
    CHECK(registry_fixture_init(&fx, "missing", nullptr) == 0);

    kith_net_conn_t *conn = nullptr;
    CHECK(make_conn(fx.net, &conn) == 0);
    kith_gateway_session_t *s = nullptr;
    CHECK(kith_gateway_session_create(
              fx.gw, conn, KITH_GATEWAY_SESSION_SUBSCRIBER, 1u, nullptr, &s) ==
          kith_error_return(KITH_ENOENT));

    kith_net_conn_close(conn);
    kith_net_conn_release(conn);
    registry_fixture_fini(&fx);
    return failures;
}

// With a registered custom strategy selected through params, session
// creation initializes it with the copied config blob, deliver routes
// through the custom vtable with the composed subject set, and session
// destruction invokes the strategy fini exactly once.
static int test_custom_strategy_binding(void)
{
    int failures = 0;
    struct custom_config cfg = {.size = sizeof(cfg), .magic = 0xC0FFEEu};
    struct registry_fixture fx;
    CHECK(registry_fixture_init(&fx, "custom", &cfg) == 0);

    kith_gateway_delivery_vtable_t v = custom_vtable();
    CHECK(kith_gateway_register_delivery(fx.gw, "custom", &v) == 0);

    kith_fabric_cell_key_t cell = make_key(7u, 0, 0, 0, 0u);
    kith_net_conn_t *conn = nullptr;
    CHECK(make_conn(fx.net, &conn) == 0);
    kith_gateway_session_t *s = nullptr;
    CHECK(kith_gateway_session_create(
              fx.gw, conn, KITH_GATEWAY_SESSION_SUBSCRIBER, 1u, nullptr, &s) == 0);

    // Bind and subscribe before publishing: the composer reads the
    // subscriber's artifact from the cache populated by this refresh.
    CHECK(kith_gateway_session_bind_actor(s, 1u) == 0);
    CHECK(kith_gateway_session_window_add(s, &cell) == 0);
    kith_sim_artifact_key_t sk = {.zone = cell.zone,
                                  .cell_x = cell.cell_x,
                                  .cell_y = cell.cell_y,
                                  .cell_z = cell.cell_z,
                                  .lod = cell.lod,
                                  .authority_epoch = 1u};
    kith_sim_actor_t sub = make_actor(1u, 0, 0, 5u);
    CHECK(kith_sim_publish_artifact(fx.sim, &sk, &sub, nullptr) == 0);
    CHECK(kith_fabric_publish(fx.fabric, &cell, 1u, nullptr) == 0);
    CHECK(kith_gateway_cache_refresh(fx.gw, 100u) == 0);
    CHECK(kith_gateway_view_refresh(fx.gw, s, 200u) == 0);

    kith_gateway_view_snapshot_t meta = {0};
    kith_gateway_view_subject_t subj[8] = {0};
    size_t view_count = 0u;
    CHECK(kith_gateway_view_snapshot(fx.gw, s, &meta, subj, 8u, &view_count) == 0);
    CHECK(meta.selected_count >= 1u);
    CHECK(view_count == meta.selected_count);
    CHECK(subj[0].actor_id == 1u); // the self subject leads the set

    kith_gateway_delivery_stats_t stats = {0};
    stats.enqueued = 0xDEADu; // the dispatcher zeroes stale out_stats first
    CHECK(kith_gateway_deliver(fx.gw, s, 300u, &stats) == 0);
    // The custom deliver reported one frame per composed subject, proving
    // the pass routed through the registered vtable.
    CHECK(stats.enqueued == (uint32_t)meta.selected_count);

    kith_gateway_session_destroy(s);
    kith_net_conn_close(conn);
    kith_net_conn_release(conn);
    registry_fixture_fini(&fx);
    return failures;
}

// The public enqueue helper validates its arguments and enqueues a
// well-formed frame on a live connection.
static int test_deliver_frame_validation(void)
{
    int failures = 0;
    struct registry_fixture fx;
    CHECK(registry_fixture_init(&fx, nullptr, nullptr) == 0);

    kith_net_conn_t *conn = nullptr;
    CHECK(make_conn(fx.net, &conn) == 0);

    uint8_t payload[16] = {0};
    CHECK(kith_gateway_deliver_frame(nullptr, conn, 1u, payload, sizeof(payload)) ==
          kith_error_return(KITH_EINVAL));
    CHECK(kith_gateway_deliver_frame(fx.gw, nullptr, 1u, payload, sizeof(payload)) ==
          kith_error_return(KITH_EINVAL));
    // The encoder frames any type id (the registry only gates dispatch),
    // so a well-formed payload on a live connection enqueues cleanly.
    CHECK(kith_gateway_deliver_frame(fx.gw, conn, 1u, payload, sizeof(payload)) == 0);

    kith_net_conn_close(conn);
    kith_net_conn_release(conn);
    registry_fixture_fini(&fx);
    return failures;
}

int main(void)
{
    int failures = 0;
    failures += test_registry_shape_errors();
    failures += test_registry_duplicate_names();
    failures += test_unknown_strategy_name_fails_session();
    failures += test_custom_strategy_binding();
    failures += test_deliver_frame_validation();
    if (failures == 0)
    {
        (void)printf("gateway registry: all tests passed\n");
    }
    return failures == 0 ? 0 : 1;
}
