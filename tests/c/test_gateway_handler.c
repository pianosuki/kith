#include <stdatomic.h>
#include <stddef.h>
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
#include "kith/sim/sim.h"
#include "kith/types.h"
#include "kith/version.h"
#include "kith/worker/worker.h"

#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/time.h>

static int check_cond(bool ok, int line)
{
    if (!ok)
    {
        (void)fprintf(stderr, "gateway handler: assertion at line %d failed\n", line);
        return 1;
    }
    return 0;
}

#define CHECK(cond) failures += check_cond((cond), __LINE__)

struct fixture
{
    kith_proto_t *proto;
    kith_net_t *net;
    kith_sim_t *sim;
    kith_fabric_t *fabric;
    kith_gateway_t *gw;
};

static int fixture_init(struct fixture *fx)
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

static void fixture_fini(const struct fixture *fx)
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
    uint16_t msg_type;
    const void *payload;
    uint32_t payload_len;
    kith_gateway_session_t *session;
    void *user_data;
    int calls;
};

static void handler_capture_fn(uint16_t msg_type,
                               const void *payload,
                               uint32_t payload_len,
                               kith_gateway_session_t *session,
                               void *user_data)
{
    struct handler_capture *c = user_data;
    c->msg_type = msg_type;
    c->payload = payload;
    c->payload_len = payload_len;
    c->session = session;
    c->user_data = user_data;
    c->calls += 1;
}

static int
make_frame_view(kith_proto_frame_t *out, uint16_t type_id, const void *payload, uint32_t len)
{
    memset(out, 0, sizeof(*out));
    out->type_id = type_id;
    out->payload = payload;
    out->payload_len = len;
    return 0;
}

// Every handler entry point rejects a NULL handle (and NULL fn for register)
// with EINVAL. register at or beyond the table capacity is EINVAL. dispatch
// with a NULL gateway, conn, or frame is EINVAL.
static int test_handler_arg_validation(void)
{
    int failures = 0;
    struct fixture fx;
    CHECK(fixture_init(&fx) == 0);

    CHECK(kith_gateway_register_handler(nullptr, 1u, handler_capture_fn, nullptr) ==
          kith_error_return(KITH_EINVAL));
    CHECK(kith_gateway_register_handler(fx.gw, 1u, nullptr, nullptr) ==
          kith_error_return(KITH_EINVAL));
    // Default handler_table_size is 256; type id 256 is out of range.
    CHECK(kith_gateway_register_handler(fx.gw, 256u, handler_capture_fn, nullptr) ==
          kith_error_return(KITH_EINVAL));
    CHECK(kith_gateway_unregister_handler(nullptr, 1u) == kith_error_return(KITH_EINVAL));

    kith_net_conn_t *conn = nullptr;
    CHECK(make_conn(fx.net, &conn) == 0);
    kith_proto_frame_t frame = {0};
    CHECK(kith_gateway_dispatch(nullptr, conn, &frame) == kith_error_return(KITH_EINVAL));
    CHECK(kith_gateway_dispatch(fx.gw, nullptr, &frame) == kith_error_return(KITH_EINVAL));
    CHECK(kith_gateway_dispatch(fx.gw, conn, nullptr) == kith_error_return(KITH_EINVAL));

    kith_net_conn_close(conn);
    kith_net_conn_release(conn);
    fixture_fini(&fx);
    return failures;
}

// dispatch with no session bound to the connection returns ENOENT. With a
// session bound but no handler registered for the frame's type id, dispatch
// returns ENOENT.
static int test_dispatch_missing_lookup(void)
{
    int failures = 0;
    struct fixture fx;
    CHECK(fixture_init(&fx) == 0);

    kith_net_conn_t *conn = nullptr;
    CHECK(make_conn(fx.net, &conn) == 0);

    kith_proto_frame_t frame = {0};
    make_frame_view(&frame, 10u, nullptr, 0u);

    // No session bound.
    CHECK(kith_gateway_dispatch(fx.gw, conn, &frame) == kith_error_return(KITH_ENOENT));

    kith_gateway_session_t *s = nullptr;
    CHECK(kith_gateway_session_create(
              fx.gw, conn, KITH_GATEWAY_SESSION_SUBSCRIBER, 1u, nullptr, &s) == 0);

    // Session bound, no handler for type 10.
    CHECK(kith_gateway_dispatch(fx.gw, conn, &frame) == kith_error_return(KITH_ENOENT));

    kith_gateway_session_destroy(s);
    kith_net_conn_close(conn);
    kith_net_conn_release(conn);
    fixture_fini(&fx);
    return failures;
}

// Registering a handler for a type that already has one replaces the prior
// registration. dispatch invokes the registered handler with the frame's
// message type, payload, payload length, the session bound to the
// connection, and the caller's user_data.
static int test_dispatch_invokes_handler(void)
{
    int failures = 0;
    struct fixture fx;
    CHECK(fixture_init(&fx) == 0);

    kith_net_conn_t *conn = nullptr;
    CHECK(make_conn(fx.net, &conn) == 0);
    kith_gateway_session_t *s = nullptr;
    CHECK(kith_gateway_session_create(
              fx.gw, conn, KITH_GATEWAY_SESSION_SUBSCRIBER, 42u, nullptr, &s) == 0);

    struct handler_capture cap_a = {0};
    struct handler_capture cap_b = {0};
    CHECK(kith_gateway_register_handler(fx.gw, 20u, handler_capture_fn, &cap_a) == 0);
    // Replacement: registering type 20 again swaps the callback and user_data.
    CHECK(kith_gateway_register_handler(fx.gw, 20u, handler_capture_fn, &cap_b) == 0);

    const uint8_t payload[] = {0xabu, 0xcdu, 0xefu};
    kith_proto_frame_t frame = {0};
    make_frame_view(&frame, 20u, payload, (uint32_t)sizeof(payload));

    CHECK(kith_gateway_dispatch(fx.gw, conn, &frame) == 0);
    CHECK(cap_a.calls == 0);
    CHECK(cap_b.calls == 1);
    CHECK(cap_b.msg_type == 20u);
    CHECK(cap_b.payload == payload);
    CHECK(cap_b.payload_len == sizeof(payload));
    CHECK(cap_b.session == s);
    CHECK(cap_b.user_data == &cap_b);

    // Unregistering makes dispatch return ENOENT for that type.
    CHECK(kith_gateway_unregister_handler(fx.gw, 20u) == 0);
    CHECK(kith_gateway_dispatch(fx.gw, conn, &frame) == kith_error_return(KITH_ENOENT));
    // Unregistering again is idempotent.
    CHECK(kith_gateway_unregister_handler(fx.gw, 20u) == 0);

    kith_gateway_session_destroy(s);
    kith_net_conn_close(conn);
    kith_net_conn_release(conn);
    fixture_fini(&fx);
    return failures;
}

// Registering a handler with both pool-bound flags is rejected: a handler
// is a Python-bound trampoline or a pool-dispatched C function, never
// both. The single-flag registrations stay valid.
static int test_register_pool_python_rejected(void)
{
    int failures = 0;
    struct fixture fx;
    CHECK(fixture_init(&fx) == 0);

    const uint32_t both = KITH_GATEWAY_HANDLER_PYTHON | KITH_GATEWAY_HANDLER_POOL;
    CHECK(kith_gateway_register_handler_flags(fx.gw, 50u, handler_capture_fn, nullptr, both) ==
          kith_error_return(KITH_EINVAL));
    // Neither registration happened; the slot stays free.
    kith_net_conn_t *conn = nullptr;
    CHECK(make_conn(fx.net, &conn) == 0);
    kith_proto_frame_t frame = {0};
    make_frame_view(&frame, 50u, nullptr, 0u);
    CHECK(kith_gateway_dispatch(fx.gw, conn, &frame) == kith_error_return(KITH_ENOENT));

    kith_net_conn_close(conn);
    kith_net_conn_release(conn);
    fixture_fini(&fx);
    return failures;
}

struct dispatch_pool_ctx
{
    _Atomic uint32_t calls;
    _Atomic uint32_t mismatch;
    uint64_t expected_session_id;
    uint64_t expected_actor_id;
};

// A pool-flagged dispatch handler run on a worker pool. Reads the session
// metadata (the access path that use-after-freed before the session refcount),
// sleeps briefly to widen the window in which the reactor thread can destroy
// the session underneath the worker, then records whether the read was intact.
static void dispatch_pool_handler(uint16_t msg_type,
                                  const void *payload,
                                  uint32_t payload_len,
                                  kith_gateway_session_t *session,
                                  void *user_data)
{
    (void)msg_type;
    (void)payload;
    (void)payload_len;
    struct dispatch_pool_ctx *c = user_data;
    kith_gateway_session_info_t info = {0};
    if (kith_gateway_session_info(session, &info) != 0 ||
        info.session_id != c->expected_session_id || info.actor_id != c->expected_actor_id)
    {
        atomic_fetch_add_explicit(&c->mismatch, 1u, memory_order_relaxed);
    }
    usleep(2000);
    atomic_fetch_add_explicit(&c->calls, 1u, memory_order_release);
}

// A pool-bound handler (PYTHON trampoline stand-in or POOL-flagged native C
// handler — the dispatch path is shared) dispatched through a worker pool
// holds a dispatch reference on the session (acquired in
// kith_gateway_dispatch, released at the end of the worker task). The session
// must stay alive for the dispatch duration even when the reactor thread
// closes the connection and calls kith_gateway_session_destroy before the
// worker runs the handler. This test dispatches a batch of frames to a pool,
// then destroys the session while the workers are still executing, and
// asserts every handler invocation reads the session's metadata intact (the
// session id it was created with). Under ASan or valgrind a missing
// dispatch reference surfaces as a heap-use-after-free in
// kith_gateway_session_info; under the normal suite this asserts the
// structural contract (no stale/garbage metadata read, no crash,
// no leak). Parameterized over both pool-bound flags so the POOL
// flag inherits the same session-lifetime contract.
static int test_dispatch_pool_flag_session_outlives_destroy(uint32_t flags)
{
    int failures = 0;
    struct fixture fx;
    CHECK(fixture_init(&fx) == 0);

    kith_worker_t *pool = nullptr;
    kith_worker_params_t wparams = {
        .size = sizeof(kith_worker_params_t),
        .abi_version = KITH_ABI_VERSION,
        .worker_count = 4u,
        .task_capacity = 256u,
    };
    CHECK(kith_worker_create(&wparams, nullptr, &pool) == 0);
    CHECK(kith_gateway_attach_worker_pool(fx.gw, pool) == 0);

    kith_net_conn_t *conn = nullptr;
    CHECK(make_conn(fx.net, &conn) == 0);
    kith_gateway_session_t *s = nullptr;
    CHECK(kith_gateway_session_create(fx.gw, conn, KITH_GATEWAY_SESSION_APP, 7u, nullptr, &s) == 0);
    CHECK(kith_gateway_session_bind_actor(s, 77u) == 0);
    kith_gateway_session_info_t seed = {0};
    CHECK(kith_gateway_session_info(s, &seed) == 0);

    struct dispatch_pool_ctx ctx = {0};
    ctx.expected_session_id = seed.session_id;
    ctx.expected_actor_id = 77u;

    CHECK(kith_gateway_register_handler_flags(fx.gw, 30u, dispatch_pool_handler, &ctx, flags) == 0);

    kith_proto_frame_t frame = {0};
    make_frame_view(&frame, 30u, nullptr, 0u);
    const uint32_t n = 64u;
    for (uint32_t i = 0u; i < n; i++)
    {
        CHECK(kith_gateway_dispatch(fx.gw, conn, &frame) == 0);
    }

    // Destroy the session while the workers are still executing. The owner
    // reference drops here; the in-flight dispatch references keep the session
    // alive until each worker task releases.
    kith_gateway_session_destroy(s);

    // Join the pool: every worker task finishes (releasing its dispatch
    // reference) before destroy returns. The last release frees the session.
    kith_worker_destroy(pool);

    CHECK(atomic_load_explicit(&ctx.calls, memory_order_acquire) == n);
    CHECK(atomic_load_explicit(&ctx.mismatch, memory_order_relaxed) == 0u);

    kith_net_conn_close(conn);
    kith_net_conn_release(conn);
    fixture_fini(&fx);
    return failures;
}

// Reactor-boundary conformance: when the worker pool is
// saturated, a
// pool-bound handler is dropped (and counted) rather than run inline on
// the reactor thread. This test drives the reactor-side dispatch path from
// the main thread (the reactor thread in the harness), registers a slow
// pool-flagged handler, and floods the pool far past its capacity. The
// assertions: no handler invocation runs on the reactor thread
// (every invocation runs on a worker), the reactor stays responsive (the
// dispatch loop finishes in bounded wall time instead of serializing behind
// the slow handler), and the drop counter accounts for every rejected
// dispatch.
struct saturation_ctx
{
    pthread_t reactor_tid;
    _Atomic uint32_t reactor_entered;    // handler ran on the reactor thread
    _Atomic uint32_t worker_invocations; // handler ran on a worker thread
};

// Slow pool-flagged handler. Records whether it is running on the reactor
// thread (the violation under test) before sleeping, then counts the
// invocation. The pool-bound flags only route the handler through the
// worker pool; the handler body is plain C, standing in for the ctypes
// trampoline (PYTHON) or the native pool-dispatched handler (POOL).
static void saturation_handler(uint16_t msg_type,
                               const void *payload,
                               uint32_t payload_len,
                               kith_gateway_session_t *session,
                               void *user_data)
{
    (void)msg_type;
    (void)payload;
    (void)payload_len;
    (void)session;
    struct saturation_ctx *c = user_data;
    if (pthread_equal(pthread_self(), c->reactor_tid))
    {
        atomic_fetch_add_explicit(&c->reactor_entered, 1u, memory_order_relaxed);
    }
    // Sleep before counting so an inline dispatch on the reactor thread
    // serializes the dispatch loop (the liveness bound catches that).
    usleep(5 * 1000);
    atomic_fetch_add_explicit(&c->worker_invocations, 1u, memory_order_release);
}

static uint64_t monotonic_ms(void)
{
    struct timespec ts = {0};
    (void)clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000u + (uint64_t)ts.tv_nsec / 1000000u;
}

// Flood @p gateway with @p n dispatches of @p frame. Counts accepted
// (rc 0) and dropped (rc EBUSY) dispatches; any other rc sets *out_bad so
// the caller can fail the test. Returns the elapsed wall time in ms.
static uint64_t flood_dispatch(kith_gateway_t *gateway,
                               kith_net_conn_t *conn,
                               const kith_proto_frame_t *frame,
                               uint32_t n,
                               uint32_t *out_submitted,
                               uint32_t *out_dropped,
                               bool *out_bad)
{
    uint32_t submitted = 0u;
    uint32_t dropped = 0u;
    uint64_t start_ms = monotonic_ms();
    for (uint32_t i = 0u; i < n; i++)
    {
        int rc = kith_gateway_dispatch(gateway, conn, frame);
        if (rc == 0)
        {
            submitted += 1u;
        }
        else if (rc == kith_error_return(KITH_EBUSY))
        {
            dropped += 1u;
        }
        else
        {
            *out_bad = true;
        }
    }
    *out_submitted = submitted;
    *out_dropped = dropped;
    return monotonic_ms() - start_ms;
}

static int test_dispatch_pool_flag_drops_on_pool_saturation(uint32_t flags)
{
    int failures = 0;
    struct fixture fx;
    CHECK(fixture_init(&fx) == 0);

    // Small pool so saturation is immediate: one worker, eight task nodes.
    kith_worker_t *pool = nullptr;
    kith_worker_params_t wparams = {
        .size = sizeof(kith_worker_params_t),
        .abi_version = KITH_ABI_VERSION,
        .worker_count = 1u,
        .task_capacity = 8u,
    };
    CHECK(kith_worker_create(&wparams, nullptr, &pool) == 0);
    CHECK(kith_gateway_attach_worker_pool(fx.gw, pool) == 0);

    kith_net_conn_t *conn = nullptr;
    CHECK(make_conn(fx.net, &conn) == 0);
    kith_gateway_session_t *s = nullptr;
    CHECK(kith_gateway_session_create(fx.gw, conn, KITH_GATEWAY_SESSION_APP, 9u, nullptr, &s) == 0);

    struct saturation_ctx ctx = {0};
    ctx.reactor_tid = pthread_self();
    CHECK(kith_gateway_register_handler_flags(fx.gw, 40u, saturation_handler, &ctx, flags) == 0);

    kith_proto_frame_t frame = {0};
    make_frame_view(&frame, 40u, nullptr, 0u);

    // The single worker drains ~200 tasks/sec (5 ms each); a tight dispatch
    // loop submits far faster, so most dispatches hit EBUSY.
    const uint32_t n = 4000u;
    uint32_t submitted = 0u;
    uint32_t dropped = 0u;
    bool bad = false;
    uint64_t elapsed_ms = flood_dispatch(fx.gw, conn, &frame, n, &submitted, &dropped, &bad);
    CHECK(!bad);

    // Destroy the pool: every successfully submitted task runs to completion
    // (the worker drains before destroy returns), so worker_invocations
    // equals the number of accepted dispatches.
    kith_gateway_session_destroy(s);
    kith_worker_destroy(pool);

    uint32_t worker_invocations =
        atomic_load_explicit(&ctx.worker_invocations, memory_order_acquire);
    uint32_t reactor_entered = atomic_load_explicit(&ctx.reactor_entered, memory_order_relaxed);
    uint64_t counted_drops = 0u;
    CHECK(kith_gateway_dispatch_drops(fx.gw, &counted_drops) == 0);
    CHECK(kith_gateway_dispatch_drops(nullptr, &counted_drops) == kith_error_return(KITH_EINVAL));

    CHECK(dropped > 0u);                       // saturation actually happened
    CHECK(submitted == worker_invocations);    // every accept ran on a worker
    CHECK(submitted + dropped == n);           // every dispatch ran or dropped
    CHECK((uint64_t)dropped == counted_drops); // counter matches rejected
    CHECK(reactor_entered == 0u);              // no reactor-inline Python
    // Reactor liveness: the loop finished in bounded wall time rather than
    // serializing behind n inline 5 ms calls (~20 s). 4 s separates the two
    // with margin for a loaded host.
    CHECK(elapsed_ms < 4000u);

    kith_net_conn_close(conn);
    kith_net_conn_release(conn);
    fixture_fini(&fx);
    return failures;
}

// A pool-bound handler with NO pool attached is dropped and counted — the
// dispatch cannot run off the reactor thread and never runs inline, the
// same contract as queue exhaustion. No worker is involved and the handler
// never executes.
static int test_dispatch_pool_flag_dropped_without_pool(void)
{
    int failures = 0;
    struct fixture fx;
    CHECK(fixture_init(&fx) == 0);

    kith_net_conn_t *conn = nullptr;
    CHECK(make_conn(fx.net, &conn) == 0);
    kith_gateway_session_t *s = nullptr;
    CHECK(kith_gateway_session_create(fx.gw, conn, KITH_GATEWAY_SESSION_APP, 11u, nullptr, &s) ==
          0);

    struct saturation_ctx ctx = {0};
    ctx.reactor_tid = pthread_self();
    CHECK(kith_gateway_register_handler_flags(
              fx.gw, 60u, saturation_handler, &ctx, KITH_GATEWAY_HANDLER_POOL) == 0);

    kith_proto_frame_t frame = {0};
    make_frame_view(&frame, 60u, nullptr, 0u);
    // One dispatch is enough for the shape: dropped with the exhaustion
    // error, counted, never run.
    CHECK(kith_gateway_dispatch(fx.gw, conn, &frame) == kith_error_return(KITH_EBUSY));

    uint64_t counted_drops = 0u;
    CHECK(kith_gateway_dispatch_drops(fx.gw, &counted_drops) == 0);

    CHECK(atomic_load_explicit(&ctx.reactor_entered, memory_order_relaxed) == 0u);
    CHECK(atomic_load_explicit(&ctx.worker_invocations, memory_order_acquire) == 0u);
    CHECK(counted_drops == 1u);

    kith_gateway_session_destroy(s);
    kith_net_conn_close(conn);
    kith_net_conn_release(conn);
    fixture_fini(&fx);
    return failures;
}

// A pool-dispatched handler that places one discrete frame on the
// dispatched session's connection: the cross-thread shape the deliver
// contract sanctions. The dispatch reference pins the session — and
// through it the connection object — for the handler's duration, so the
// encode-and-enqueue path runs on a worker thread exactly as the delivery
// executor runs it.
struct deliver_ctx
{
    kith_gateway_t *gateway;
    uint16_t type_id;
    const uint8_t *payload;
    uint32_t payload_len;
    _Atomic int rc;
    _Atomic uint32_t calls;
};

static void deliver_pool_handler(uint16_t msg_type,
                                 const void *payload,
                                 uint32_t payload_len,
                                 kith_gateway_session_t *session,
                                 void *user_data)
{
    (void)msg_type;
    (void)payload;
    (void)payload_len;
    struct deliver_ctx *c = user_data;
    kith_net_conn_t *conn = kith_gateway_session_conn(session);
    atomic_store_explicit(
        &c->rc,
        kith_gateway_deliver_frame(c->gateway, conn, c->type_id, c->payload, c->payload_len),
        memory_order_relaxed);
    atomic_fetch_add_explicit(&c->calls, 1u, memory_order_release);
}

// Fixture up to the attached pool and a bound session, with the client fd
// kept open so a test can read the bytes the gateway delivers on the wire.
static int deliver_fixture_init(struct fixture *fx,
                                kith_worker_t **out_pool,
                                kith_net_conn_t **out_conn,
                                int *out_client_fd,
                                kith_gateway_session_t **out_session)
{
    *out_pool = nullptr;
    *out_conn = nullptr;
    *out_session = nullptr;
    *out_client_fd = -1;
    if (fixture_init(fx) != 0)
    {
        return -1;
    }
    kith_worker_params_t wparams = {
        .size = sizeof(kith_worker_params_t),
        .abi_version = KITH_ABI_VERSION,
        .worker_count = 1u,
        .task_capacity = 16u,
    };
    if (kith_worker_create(&wparams, nullptr, out_pool) != 0 ||
        kith_gateway_attach_worker_pool(fx->gw, *out_pool) != 0)
    {
        kith_worker_destroy(*out_pool);
        return -1;
    }
    *out_client_fd = connect_to_listener(fx->net);
    if (*out_client_fd < 0)
    {
        return -1;
    }
    if (kith_net_accept(fx->net, out_conn) != 0)
    {
        (void)close(*out_client_fd);
        *out_client_fd = -1;
        return -1;
    }
    return kith_gateway_session_create(
        fx->gw, *out_conn, KITH_GATEWAY_SESSION_APP, 21u, nullptr, out_session);
}

// The worker-side send lands on the wire: dispatch runs the pool-flagged
// handler on a worker thread, the handler sends through the dispatched
// session, and the flushed connection carries the encoded frame to the
// peer socket (10-byte header: magic, version, flags 0, type, length).
static int test_pool_handler_delivers_cross_thread(void)
{
    int failures = 0;
    struct fixture fx;
    kith_worker_t *pool = nullptr;
    kith_net_conn_t *conn = nullptr;
    kith_gateway_session_t *s = nullptr;
    int client_fd = -1;
    CHECK(deliver_fixture_init(&fx, &pool, &conn, &client_fd, &s) == 0);

    static const uint8_t reply[] = {0xdeu, 0xadu, 0xbeu, 0xefu};
    struct deliver_ctx ctx = {0};
    ctx.gateway = fx.gw;
    ctx.type_id = 41u;
    ctx.payload = reply;
    ctx.payload_len = (uint32_t)sizeof(reply);

    CHECK(kith_gateway_register_handler_flags(
              fx.gw, 40u, deliver_pool_handler, &ctx, KITH_GATEWAY_HANDLER_POOL) == 0);

    kith_proto_frame_t frame = {0};
    make_frame_view(&frame, 40u, nullptr, 0u);
    CHECK(kith_gateway_dispatch(fx.gw, conn, &frame) == 0);

    // Destroying the pool joins it: the handler has run and released its
    // dispatch reference before the destroy returns.
    kith_worker_destroy(pool);
    pool = nullptr;

    CHECK(atomic_load_explicit(&ctx.calls, memory_order_acquire) == 1u);
    CHECK(atomic_load_explicit(&ctx.rc, memory_order_relaxed) == 0);

    CHECK(kith_net_conn_write(conn) == 0);
    const struct timeval tv = {.tv_sec = 5, .tv_usec = 0};
    CHECK(setsockopt(client_fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) == 0);
    uint8_t hdr[10];
    const ssize_t got = recv(client_fd, hdr, sizeof(hdr), MSG_WAITALL);
    CHECK(got == (ssize_t)sizeof(hdr));
    if (got == (ssize_t)sizeof(hdr))
    {
        CHECK(hdr[0] == KITH_PROTO_MAGIC0);
        CHECK(hdr[1] == KITH_PROTO_MAGIC1);
        CHECK(hdr[2] == KITH_PROTO_VERSION);
        CHECK(hdr[3] == 0u);
        const uint16_t wire_type = (uint16_t)(((uint32_t)hdr[4] << 8u) | hdr[5]);
        CHECK(wire_type == ctx.type_id);
        const uint32_t wire_len = ((uint32_t)hdr[6] << 24u) | ((uint32_t)hdr[7] << 16u) |
                                  ((uint32_t)hdr[8] << 8u) | hdr[9];
        CHECK(wire_len == ctx.payload_len);
        uint8_t body[16];
        const ssize_t body_got = recv(client_fd, body, ctx.payload_len, MSG_WAITALL);
        CHECK(body_got == (ssize_t)ctx.payload_len);
        CHECK(memcmp(body, reply, sizeof(reply)) == 0);
    }

    kith_gateway_session_destroy(s);
    kith_net_conn_close(conn);
    kith_net_conn_release(conn);
    (void)close(client_fd);
    fixture_fini(&fx);
    return failures;
}

// Backpressure refuses the worker-side send: the queue prefilled from the
// calling thread sits at its high watermark, and the handler's one-off
// frame comes back EAGAIN instead of being queued or dropped silently.
static int test_pool_handler_deliver_backpressure(void)
{
    int failures = 0;
    struct fixture fx;
    kith_worker_t *pool = nullptr;
    kith_net_conn_t *conn = nullptr;
    kith_gateway_session_t *s = nullptr;
    int client_fd = -1;
    CHECK(deliver_fixture_init(&fx, &pool, &conn, &client_fd, &s) == 0);

    static const uint8_t filler[] = {1u, 2u, 3u, 4u};
    int rc = 0;
    do
    {
        rc = kith_gateway_deliver_frame(fx.gw, conn, 41u, filler, (uint32_t)sizeof(filler));
    } while (rc == 0);
    CHECK(rc == kith_error_return(KITH_EAGAIN));

    struct deliver_ctx ctx = {0};
    ctx.gateway = fx.gw;
    ctx.type_id = 41u;
    ctx.payload = filler;
    ctx.payload_len = (uint32_t)sizeof(filler);
    CHECK(kith_gateway_register_handler_flags(
              fx.gw, 40u, deliver_pool_handler, &ctx, KITH_GATEWAY_HANDLER_POOL) == 0);

    kith_proto_frame_t frame = {0};
    make_frame_view(&frame, 40u, nullptr, 0u);
    CHECK(kith_gateway_dispatch(fx.gw, conn, &frame) == 0);
    kith_worker_destroy(pool);
    pool = nullptr;

    CHECK(atomic_load_explicit(&ctx.calls, memory_order_acquire) == 1u);
    CHECK(atomic_load_explicit(&ctx.rc, memory_order_relaxed) == kith_error_return(KITH_EAGAIN));

    kith_gateway_session_destroy(s);
    kith_net_conn_close(conn);
    kith_net_conn_release(conn);
    (void)close(client_fd);
    fixture_fini(&fx);
    return failures;
}

// A connection closed before the dispatch is refused, not dereferenced:
// the dispatch reference keeps the session alive across the worker handoff,
// the session's connection reference keeps the conn object allocated, and
// the enqueue's closed check returns ESTATE — the closed half of the
// contract, exercised cross-thread.
static int test_pool_handler_deliver_closed_conn(void)
{
    int failures = 0;
    struct fixture fx;
    kith_worker_t *pool = nullptr;
    kith_net_conn_t *conn = nullptr;
    kith_gateway_session_t *s = nullptr;
    int client_fd = -1;
    CHECK(deliver_fixture_init(&fx, &pool, &conn, &client_fd, &s) == 0);

    static const uint8_t payload[] = {7u, 7u};
    struct deliver_ctx ctx = {0};
    ctx.gateway = fx.gw;
    ctx.type_id = 41u;
    ctx.payload = payload;
    ctx.payload_len = (uint32_t)sizeof(payload);
    CHECK(kith_gateway_register_handler_flags(
              fx.gw, 40u, deliver_pool_handler, &ctx, KITH_GATEWAY_HANDLER_POOL) == 0);

    kith_net_conn_close(conn);

    kith_proto_frame_t frame = {0};
    make_frame_view(&frame, 40u, nullptr, 0u);
    CHECK(kith_gateway_dispatch(fx.gw, conn, &frame) == 0);
    kith_worker_destroy(pool);
    pool = nullptr;

    CHECK(atomic_load_explicit(&ctx.calls, memory_order_acquire) == 1u);
    CHECK(atomic_load_explicit(&ctx.rc, memory_order_relaxed) == kith_error_return(KITH_ESTATE));

    kith_gateway_session_destroy(s);
    kith_net_conn_release(conn);
    (void)close(client_fd);
    fixture_fini(&fx);
    return failures;
}

int main(void)
{
    int rc = 0;
    rc |= test_handler_arg_validation();
    rc |= test_dispatch_missing_lookup();
    rc |= test_dispatch_invokes_handler();
    rc |= test_register_pool_python_rejected();
    rc |= test_dispatch_pool_flag_session_outlives_destroy(KITH_GATEWAY_HANDLER_PYTHON);
    rc |= test_dispatch_pool_flag_session_outlives_destroy(KITH_GATEWAY_HANDLER_POOL);
    rc |= test_dispatch_pool_flag_drops_on_pool_saturation(KITH_GATEWAY_HANDLER_PYTHON);
    rc |= test_dispatch_pool_flag_drops_on_pool_saturation(KITH_GATEWAY_HANDLER_POOL);
    rc |= test_dispatch_pool_flag_dropped_without_pool();
    rc |= test_pool_handler_delivers_cross_thread();
    rc |= test_pool_handler_deliver_backpressure();
    rc |= test_pool_handler_deliver_closed_conn();
    if (rc != 0)
    {
        (void)fprintf(stderr, "gateway handler tests FAILED\n");
    }
    return rc;
}
