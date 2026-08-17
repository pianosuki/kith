/* Interpreter exit gate tests: while kith_python_finalizing is set, every
 * interpreter-hosted callback dispatch is refused. The server's tick task
 * drops the python-bound callback, the gateway's dispatch task drops a
 * PYTHON-flagged handler but still runs a POOL-flagged native C handler,
 * and the store itself normalizes any nonzero value to the refused state.
 * The refused dispatches are observable because each pool is joined before
 * its counters are read (kith_worker_destroy drains in-flight tasks).
 * Every scenario restores the gate to 0 — the store is process-global and
 * ctest runs the C suite serially, so a leaked set gate poisons the
 * rest of the run. */

#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

#include <pthread.h>
#include <unistd.h>

#include "kith/fabric/fabric.h"
#include "kith/gateway/gateway.h"
#include "kith/net/net.h"
#include "kith/proto/proto.h"
#include "kith/server/server.h"
#include "kith/sim/sim.h"
#include "kith/types.h"
#include "kith/util/util.h"
#include "kith/version.h"
#include "kith/worker/worker.h"

#include <netinet/in.h>
#include <sys/socket.h>

static int check_cond(bool ok, int line)
{
    if (!ok)
    {
        (void)fprintf(stderr, "python exit gate: assertion at line %d failed\n", line);
        return 1;
    }
    return 0;
}

#define CHECK(cond) failures += check_cond((cond), __LINE__)

// ---------------------------------------------------------------------------
// fixtures
// ---------------------------------------------------------------------------

static int make_server(kith_server_t **out)
{
    kith_server_params_t params = {0};
    params.size = sizeof(params);
    params.abi_version = KITH_ABI_VERSION;
    return kith_server_create(&params, nullptr, out);
}

// The gate blocks the tick callback's own shutdown request, so the run is
// stopped from this helper thread after a short window; the run then drains
// and returns.
struct stop_run_ctx
{
    kith_server_t *server;
    unsigned int wait_ms;
};

static void *stop_run_thread(void *arg)
{
    struct stop_run_ctx *ctx = arg;
    struct timespec ts = {
        .tv_sec = (time_t)(ctx->wait_ms / 1000u),
        .tv_nsec = (long)(ctx->wait_ms % 1000u) * 1000000L,
    };
    (void)nanosleep(&ts, nullptr);
    (void)kith_server_shutdown(ctx->server);
    return nullptr;
}

struct tick_ctx
{
    kith_server_t *server; // non-NULL when the callback requests shutdown
    _Atomic uint32_t calls;
};

static void tick_capture_fn(uint64_t tick, void *user_data)
{
    (void)tick;
    struct tick_ctx *c = user_data;
    atomic_fetch_add_explicit(&c->calls, 1u, memory_order_relaxed);
}

static void tick_shutdown_fn(uint64_t tick, void *user_data)
{
    (void)tick;
    struct tick_ctx *c = user_data;
    uint32_t n = atomic_fetch_add_explicit(&c->calls, 1u, memory_order_relaxed) + 1u;
    if (n >= 3u)
    {
        (void)kith_server_shutdown(c->server);
    }
}

struct gate_fixture
{
    kith_proto_t *proto;
    kith_net_t *net;
    kith_sim_t *sim;
    kith_fabric_t *fabric;
    kith_gateway_t *gw;
};

static int gate_fixture_init(struct gate_fixture *fx)
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
    if (kith_gateway_create(nullptr, fx->net, fx->fabric, fx->proto, nullptr, &fx->gw) != 0)
    {
        return -1;
    }
    if (kith_net_listen(fx->net, "127.0.0.1", 0) != 0)
    {
        return -1;
    }
    return 0;
}

static void gate_fixture_fini(const struct gate_fixture *fx)
{
    kith_gateway_destroy(fx->gw);
    kith_fabric_destroy(fx->fabric);
    kith_sim_destroy(fx->sim);
    kith_net_destroy(fx->net);
    kith_proto_destroy(fx->proto);
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

struct handler_capture
{
    _Atomic uint32_t calls;
};

static void handler_capture_fn(uint16_t msg_type,
                               const void *payload,
                               uint32_t payload_len,
                               kith_gateway_session_t *session,
                               void *user_data)
{
    (void)msg_type;
    (void)payload;
    (void)payload_len;
    (void)session;
    struct handler_capture *c = user_data;
    atomic_fetch_add_explicit(&c->calls, 1u, memory_order_relaxed);
}

static kith_worker_t *make_pool(uint32_t worker_count)
{
    kith_worker_t *pool = nullptr;
    kith_worker_params_t wparams = {
        .size = sizeof(kith_worker_params_t),
        .abi_version = KITH_ABI_VERSION,
        .worker_count = worker_count,
        .task_capacity = 64u,
    };
    if (kith_worker_create(&wparams, nullptr, &pool) != 0)
    {
        return nullptr;
    }
    return pool;
}

// ---------------------------------------------------------------------------
// scenarios
// ---------------------------------------------------------------------------

// The store normalizes any nonzero argument to the refused state and 0 to
// the allowed state.
static int test_gate_store_contract(void)
{
    int failures = 0;
    kith_python_finalizing_set(0);
    CHECK(kith_python_finalizing() == 0);
    kith_python_finalizing_set(1);
    CHECK(kith_python_finalizing() == 1);
    kith_python_finalizing_set(7);
    CHECK(kith_python_finalizing() == 1);
    kith_python_finalizing_set(0);
    CHECK(kith_python_finalizing() == 0);
    return failures;
}

// With the gate set, a python-bound tick callback is never invoked: the
// worker task refuses the entry and releases the work record. The run is
// stopped from a helper thread: the gated callback never runs, so its
// own shutdown request never fires.
static int test_tick_refused_while_gate_set(void)
{
    int failures = 0;
    kith_server_t *s = nullptr;
    CHECK(make_server(&s) == 0);

    struct tick_ctx cap = {0};
    atomic_init(&cap.calls, 0u);
    CHECK(kith_server_register_tick_handler(s, tick_capture_fn, &cap, KITH_SERVER_HANDLER_PYTHON) ==
          0);

    kith_python_finalizing_set(1);
    pthread_t stopper;
    struct stop_run_ctx stop_ctx = {.server = s, .wait_ms = 300u};
    CHECK(pthread_create(&stopper, nullptr, stop_run_thread, &stop_ctx) == 0);
    CHECK(kith_server_run(s) == 0);
    CHECK(pthread_join(stopper, nullptr) == 0);
    kith_python_finalizing_set(0);

    // Destroy joins the worker pool, so the refusal (if any) is complete
    // before the capture is read.
    kith_server_destroy(s);
    CHECK(atomic_load_explicit(&cap.calls, memory_order_acquire) == 0u);
    return failures;
}

// With the gate clear, the same registration dispatches: the callback runs
// on the worker and its own shutdown request drains the run.
static int test_tick_dispatched_after_gate_clear(void)
{
    int failures = 0;
    kith_server_t *s = nullptr;
    CHECK(make_server(&s) == 0);

    struct tick_ctx ctx = {0};
    ctx.server = s;
    atomic_init(&ctx.calls, 0u);
    CHECK(kith_server_register_tick_handler(
              s, tick_shutdown_fn, &ctx, KITH_SERVER_HANDLER_PYTHON) == 0);

    CHECK(kith_server_run(s) == 0);
    kith_server_destroy(s);
    CHECK(atomic_load_explicit(&ctx.calls, memory_order_acquire) >= 3u);
    return failures;
}

// A PYTHON-flagged gateway handler is refused while the gate is set; the
// same registration dispatches once the gate clears.
static int test_gateway_python_refusal(void)
{
    int failures = 0;
    struct gate_fixture fx;
    CHECK(gate_fixture_init(&fx) == 0);

    kith_net_conn_t *conn = nullptr;
    CHECK(make_conn(fx.net, &conn) == 0);
    kith_gateway_session_t *session = nullptr;
    CHECK(kith_gateway_session_create(
              fx.gw, conn, KITH_GATEWAY_SESSION_SUBSCRIBER, 1u, nullptr, &session) == 0);

    struct handler_capture cap = {0};
    atomic_init(&cap.calls, 0u);
    CHECK(kith_gateway_register_handler_flags(
              fx.gw, 30u, handler_capture_fn, &cap, KITH_GATEWAY_HANDLER_PYTHON) == 0);

    kith_proto_frame_t frame = {0};
    frame.type_id = 30u;
    frame.payload = nullptr;
    frame.payload_len = 0u;

    kith_worker_t *pool = make_pool(1u);
    CHECK(pool != nullptr);
    CHECK(kith_gateway_attach_worker_pool(fx.gw, pool) == 0);

    kith_python_finalizing_set(1);
    CHECK(kith_gateway_dispatch(fx.gw, conn, &frame) == 0);
    kith_worker_destroy(pool);
    CHECK(atomic_load_explicit(&cap.calls, memory_order_acquire) == 0u);
    kith_python_finalizing_set(0);

    pool = make_pool(1u);
    CHECK(pool != nullptr);
    CHECK(kith_gateway_attach_worker_pool(fx.gw, pool) == 0);
    CHECK(kith_gateway_dispatch(fx.gw, conn, &frame) == 0);
    kith_worker_destroy(pool);
    CHECK(atomic_load_explicit(&cap.calls, memory_order_acquire) == 1u);

    kith_gateway_session_destroy(session);
    kith_net_conn_close(conn);
    kith_net_conn_release(conn);
    gate_fixture_fini(&fx);
    return failures;
}

// A POOL-flagged native C handler never enters the interpreter and still
// runs while the gate is set.
static int test_gateway_pool_native_dispatches_with_gate_set(void)
{
    int failures = 0;
    struct gate_fixture fx;
    CHECK(gate_fixture_init(&fx) == 0);

    kith_net_conn_t *conn = nullptr;
    CHECK(make_conn(fx.net, &conn) == 0);
    kith_gateway_session_t *session = nullptr;
    CHECK(kith_gateway_session_create(
              fx.gw, conn, KITH_GATEWAY_SESSION_SUBSCRIBER, 1u, nullptr, &session) == 0);

    struct handler_capture cap = {0};
    atomic_init(&cap.calls, 0u);
    CHECK(kith_gateway_register_handler_flags(
              fx.gw, 31u, handler_capture_fn, &cap, KITH_GATEWAY_HANDLER_POOL) == 0);

    kith_proto_frame_t frame = {0};
    frame.type_id = 31u;

    kith_worker_t *pool = make_pool(1u);
    CHECK(pool != nullptr);
    CHECK(kith_gateway_attach_worker_pool(fx.gw, pool) == 0);

    kith_python_finalizing_set(1);
    CHECK(kith_gateway_dispatch(fx.gw, conn, &frame) == 0);
    kith_worker_destroy(pool);
    CHECK(atomic_load_explicit(&cap.calls, memory_order_acquire) == 1u);
    kith_python_finalizing_set(0);

    kith_gateway_session_destroy(session);
    kith_net_conn_close(conn);
    kith_net_conn_release(conn);
    gate_fixture_fini(&fx);
    return failures;
}

int main(void)
{
    int failures = 0;
    kith_python_finalizing_set(0);
    failures += test_gate_store_contract();
    failures += test_tick_refused_while_gate_set();
    failures += test_tick_dispatched_after_gate_clear();
    failures += test_gateway_python_refusal();
    failures += test_gateway_pool_native_dispatches_with_gate_set();
    kith_python_finalizing_set(0);
    if (failures != 0)
    {
        (void)fprintf(stderr, "python exit gate: %d assertion(s) failed\n", failures);
    }
    return failures == 0 ? 0 : 1;
}
