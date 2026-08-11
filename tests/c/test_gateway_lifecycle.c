/* Session lifecycle surface tests: the destroyed-session callback (inline
 * and pool-dispatched kinds, drop accounting, exit-gate refusal), the
 * acquire/release pin, the session-count mirror gauge, and the identity
 * snapshot. Owns its fixture; siblings live in test_gateway_*.c. */

#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <pthread.h>
#include <unistd.h>

#include "kith/fabric/fabric.h"
#include "kith/gateway/gateway.h"
#include "kith/net/net.h"
#include "kith/proto/proto.h"
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
        (void)fprintf(stderr, "gateway lifecycle: assertion at line %d failed\n", line);
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
    if (kith_proto_create(nullptr, nullptr, &fx->proto) != 0 ||
        kith_net_create(nullptr, fx->proto, nullptr, &fx->net) != 0 ||
        kith_sim_create(nullptr, nullptr, &fx->sim) != 0 ||
        kith_fabric_create(nullptr, fx->sim, nullptr, &fx->fabric) != 0 ||
        kith_gateway_create(nullptr, fx->net, fx->fabric, fx->proto, nullptr, &fx->gw) != 0 ||
        kith_net_listen(fx->net, "127.0.0.1", 0) != 0)
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

// A session bound to a fresh loopback connection; the caller keeps the
// client fd alive for the connection's lifetime and closes both in
// session_conn_teardown.
static int make_session(struct fixture *fx,
                        uint64_t principal,
                        int *out_client_fd,
                        kith_net_conn_t **out_conn,
                        kith_gateway_session_t **out_session)
{
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
        fx->gw, *out_conn, KITH_GATEWAY_SESSION_APP, principal, nullptr, out_session);
}

/*---------------------------------------------------------------------------
 * capture contexts
 *-------------------------------------------------------------------------*/

struct destroyed_capture
{
    _Atomic int calls;
    /** The gateway whose gauges the callback reads off-reactor (pool kind). */
    kith_gateway_t *gw;
    uint64_t session_id;
    uint64_t principal_id;
    uint64_t actor_id;
    kith_gateway_session_type_t type;
    /** The session-count gauge read inside the callback. */
    uint64_t observed_count;
};

static void destroyed_capture_fn(const kith_gateway_session_info_t *info, void *user_data)
{
    struct destroyed_capture *cap = user_data;
    cap->session_id = info->session_id;
    cap->principal_id = info->principal_id;
    cap->actor_id = info->actor_id;
    cap->type = info->type;
    cap->observed_count = kith_gateway_session_count(cap->gw);
    atomic_fetch_add_explicit(&cap->calls, 1, memory_order_release);
}

// A counting allocator over the default instance, tracking outstanding
// allocations so the acquire pin can prove the session's memory releases
// exactly at the last release.
struct outstanding_tally
{
    _Atomic int outstanding;
};

static void *tally_alloc(void *ctx, size_t size)
{
    struct outstanding_tally *tally = ctx;
    atomic_fetch_add_explicit(&tally->outstanding, 1, memory_order_relaxed);
    return malloc(size);
}

static void *tally_alloc_zero(void *ctx, size_t count, size_t size)
{
    struct outstanding_tally *tally = ctx;
    atomic_fetch_add_explicit(&tally->outstanding, 1, memory_order_relaxed);
    return calloc(count, size);
}

static void *tally_realloc(void *ctx, void *ptr, size_t size)
{
    (void)ctx;
    return realloc(ptr, size);
}

static void tally_free(void *ctx, void *ptr)
{
    struct outstanding_tally *tally = ctx;
    if (ptr != NULL)
    {
        atomic_fetch_sub_explicit(&tally->outstanding, 1, memory_order_relaxed);
    }
    free(ptr);
}

/*---------------------------------------------------------------------------
 * tests
 *-------------------------------------------------------------------------*/

// The unflagged callback runs inline inside the destroy call, receives the
// destroyed session's identity (id, principal, bound actor), and observes
// the post-remove table: the count gauge has already dropped the session.
static int test_destroyed_callback_inline_reports_identity(void)
{
    int failures = 0;
    struct fixture fx;
    struct destroyed_capture cap = {0};
    atomic_init(&cap.calls, 0);
    CHECK(fixture_init(&fx) == 0);
    cap.gw = fx.gw;
    CHECK(kith_gateway_register_session_destroyed_handler(fx.gw, destroyed_capture_fn, &cap) == 0);

    int client_fd = -1;
    kith_net_conn_t *conn = NULL;
    kith_gateway_session_t *session = NULL;
    CHECK(make_session(&fx, 77u, &client_fd, &conn, &session) == 0);
    CHECK(kith_gateway_session_bind_actor(session, 4242u) == 0);

    kith_gateway_session_info_t info = {0};
    CHECK(kith_gateway_session_info(session, &info) == 0);
    CHECK(kith_gateway_session_count(fx.gw) == 1u);

    kith_gateway_session_destroy(session);
    CHECK(atomic_load_explicit(&cap.calls, memory_order_acquire) == 1);
    CHECK(cap.session_id == info.session_id);
    CHECK(cap.principal_id == 77u);
    CHECK(cap.actor_id == 4242u);
    CHECK(cap.type == KITH_GATEWAY_SESSION_APP);
    // The fire point is after the table remove: the in-callback gauge read
    // (the mirror, read from the reactor thread here) already dropped the
    // destroyed session.
    CHECK(cap.observed_count == 0u);
    CHECK(kith_gateway_session_count(fx.gw) == 0u);

    kith_net_conn_close(conn);
    kith_net_conn_release(conn);
    (void)close(client_fd);
    fixture_fini(&fx);
    return failures;
}

// The pool-flagged callback is dispatched to the attached pool and runs
// there with the same identity; the pool join (destroy) is the wait.
static int test_destroyed_callback_pool_dispatched(void)
{
    int failures = 0;
    struct fixture fx;
    struct destroyed_capture cap = {0};
    atomic_init(&cap.calls, 0);
    CHECK(fixture_init(&fx) == 0);
    cap.gw = fx.gw;

    kith_worker_params_t wparams = {
        .size = sizeof(kith_worker_params_t),
        .abi_version = KITH_ABI_VERSION,
        .worker_count = 1u,
        .task_capacity = 16u,
    };
    kith_worker_t *pool = NULL;
    CHECK(kith_worker_create(&wparams, nullptr, &pool) == 0);
    CHECK(kith_gateway_attach_worker_pool(fx.gw, pool) == 0);
    CHECK(kith_gateway_register_session_destroyed_handler_flags(
              fx.gw, destroyed_capture_fn, &cap, KITH_GATEWAY_HANDLER_POOL) == 0);

    int client_fd = -1;
    kith_net_conn_t *conn = NULL;
    kith_gateway_session_t *session = NULL;
    CHECK(make_session(&fx, 91u, &client_fd, &conn, &session) == 0);
    kith_gateway_session_info_t info = {0};
    CHECK(kith_gateway_session_info(session, &info) == 0);

    kith_gateway_session_destroy(session);
    // Destroy returns before the worker necessarily runs the callback; the
    // join drains the queued notification and is the test's synchronization
    // point.
    kith_worker_destroy(pool);
    CHECK(atomic_load_explicit(&cap.calls, memory_order_acquire) == 1);
    CHECK(cap.session_id == info.session_id);
    CHECK(cap.principal_id == 91u);
    // The off-reactor gauge read inside the callback: the destroyed session
    // is already out of the table, so the mirror reads 0 from a pool
    // worker.
    CHECK(cap.observed_count == 0u);
    CHECK(kith_gateway_session_count(fx.gw) == 0u);
    uint64_t drops = 1u;
    CHECK(kith_gateway_lifecycle_drops(fx.gw, &drops) == 0);
    CHECK(drops == 0u);

    kith_net_conn_close(conn);
    kith_net_conn_release(conn);
    (void)close(client_fd);
    fixture_fini(&fx);
    return failures;
}

// A pool-bound notification with no pool attached is dropped and counted:
// the game's disconnect bookkeeping for that session does not run, and the
// lifecycle gauge — not the dispatch-drop gauge — carries the drop.
static int test_destroyed_callback_no_pool_counts_drop(void)
{
    int failures = 0;
    struct fixture fx;
    struct destroyed_capture cap = {0};
    atomic_init(&cap.calls, 0);
    CHECK(fixture_init(&fx) == 0);
    CHECK(kith_gateway_register_session_destroyed_handler_flags(
              fx.gw, destroyed_capture_fn, &cap, KITH_GATEWAY_HANDLER_POOL) == 0);

    int client_fd = -1;
    kith_net_conn_t *conn = NULL;
    kith_gateway_session_t *session = NULL;
    CHECK(make_session(&fx, 15u, &client_fd, &conn, &session) == 0);
    kith_gateway_session_destroy(session);
    CHECK(atomic_load_explicit(&cap.calls, memory_order_acquire) == 0);
    uint64_t drops = 0u;
    CHECK(kith_gateway_lifecycle_drops(fx.gw, &drops) == 0);
    CHECK(drops == 1u);
    uint64_t dispatch_drops = 1u;
    CHECK(kith_gateway_dispatch_drops(fx.gw, &dispatch_drops) == 0);
    CHECK(dispatch_drops == 0u);

    kith_net_conn_close(conn);
    kith_net_conn_release(conn);
    (void)close(client_fd);
    fixture_fini(&fx);
    return failures;
}

// A Python-bound notification submitted while the interpreter exit gate is
// armed is refused at the worker and not counted: the process is exiting,
// and there is nothing left to notify. The zero drop count distinguishes
// the in-task refusal from a submit-time drop.
static int test_destroyed_callback_exit_gate_refusal(void)
{
    int failures = 0;
    struct fixture fx;
    struct destroyed_capture cap = {0};
    atomic_init(&cap.calls, 0);
    CHECK(fixture_init(&fx) == 0);
    cap.gw = fx.gw;

    kith_worker_params_t wparams = {
        .size = sizeof(kith_worker_params_t),
        .abi_version = KITH_ABI_VERSION,
        .worker_count = 1u,
        .task_capacity = 16u,
    };
    kith_worker_t *pool = NULL;
    CHECK(kith_worker_create(&wparams, nullptr, &pool) == 0);
    CHECK(kith_gateway_attach_worker_pool(fx.gw, pool) == 0);
    CHECK(kith_gateway_register_session_destroyed_handler_flags(
              fx.gw, destroyed_capture_fn, &cap, KITH_GATEWAY_HANDLER_PYTHON) == 0);

    int client_fd = -1;
    kith_net_conn_t *conn = NULL;
    kith_gateway_session_t *session = NULL;
    CHECK(make_session(&fx, 33u, &client_fd, &conn, &session) == 0);

    kith_python_finalizing_set(1);
    kith_gateway_session_destroy(session);
    kith_worker_destroy(pool);
    CHECK(atomic_load_explicit(&cap.calls, memory_order_acquire) == 0);
    uint64_t drops = 1u;
    CHECK(kith_gateway_lifecycle_drops(fx.gw, &drops) == 0);
    CHECK(drops == 0u);
    kith_python_finalizing_set(0);

    kith_net_conn_close(conn);
    kith_net_conn_release(conn);
    (void)close(client_fd);
    fixture_fini(&fx);
    return failures;
}

// A pinned session survives the reactor's destroy: the memory (tracked by
// the outstanding-allocation tally) releases exactly at the caller's
// release, and the identity record stays readable in between.
static int test_acquire_pins_session_past_destroy(void)
{
    int failures = 0;
    struct fixture fx;
    CHECK(fixture_init(&fx) == 0);

    struct outstanding_tally tally = {0};
    atomic_init(&tally.outstanding, 0);
    const kith_allocator_t tally_allocator = {
        .size = sizeof(kith_allocator_t),
        .abi_version = KITH_ABI_VERSION,
        .user_data = &tally,
        .alloc = tally_alloc,
        .alloc_zero = tally_alloc_zero,
        .realloc = tally_realloc,
        .free = tally_free,
        .reserved = {nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr},
    };

    int client_fd = -1;
    kith_net_conn_t *conn = NULL;
    kith_gateway_session_t *session = NULL;
    client_fd = connect_to_listener(fx.net);
    CHECK(client_fd >= 0);
    CHECK(kith_net_accept(fx.net, &conn) == 0);
    CHECK(kith_gateway_session_create(
              fx.gw, conn, KITH_GATEWAY_SESSION_APP, 5u, &tally_allocator, &session) == 0);
    const int outstanding_after_create =
        atomic_load_explicit(&tally.outstanding, memory_order_relaxed);
    CHECK(outstanding_after_create > 0);

    // The pin is taken in an owner-reference context (this thread created
    // the session), then the reactor's destroy runs underneath it.
    kith_gateway_session_acquire(session);
    kith_gateway_session_info_t pinned_info = {0};
    kith_gateway_session_destroy(session);
    CHECK(kith_gateway_session_info(session, &pinned_info) == 0);
    CHECK(pinned_info.principal_id == 5u);
    CHECK(atomic_load_explicit(&tally.outstanding, memory_order_relaxed) ==
          outstanding_after_create);
    CHECK(kith_gateway_session_count(fx.gw) == 0u);

    kith_gateway_session_release(session);
    CHECK(atomic_load_explicit(&tally.outstanding, memory_order_relaxed) == 0);

    kith_net_conn_close(conn);
    kith_net_conn_release(conn);
    (void)close(client_fd);
    fixture_fini(&fx);
    return failures;
}

// The count mirror tracks the table across create and destroy; the
// snapshot copies live identity records with truncation reported through
// the copied count.
static int test_count_mirror_and_snapshot(void)
{
    int failures = 0;
    struct fixture fx;
    CHECK(fixture_init(&fx) == 0);

    int fds[3] = {-1, -1, -1};
    kith_net_conn_t *conns[3] = {NULL, NULL, NULL};
    kith_gateway_session_t *sessions[3] = {NULL, NULL, NULL};
    for (size_t i = 0u; i < 3u; ++i)
    {
        CHECK(make_session(&fx, 100u + (uint64_t)i, &fds[i], &conns[i], &sessions[i]) == 0);
    }
    CHECK(kith_gateway_session_count(fx.gw) == 3u);
    // The accessor's NULL tolerance: a NULL gateway reports 0.
    CHECK(kith_gateway_session_count(NULL) == 0u);

    kith_gateway_session_info_t out[4] = {0};
    size_t copied = 0u;
    CHECK(kith_gateway_session_snapshot(fx.gw, out, 4u, &copied) == 0);
    CHECK(copied == 3u);
    for (size_t i = 0u; i < 3u; ++i)
    {
        bool found = false;
        for (size_t j = 0u; j < copied; ++j)
        {
            found = found || out[j].principal_id == 100u + (uint64_t)i;
        }
        CHECK(found);
    }

    // Truncation: two slots copy two records; the mirror still reports the
    // full live count.
    memset(out, 0, sizeof(out));
    copied = 99u;
    CHECK(kith_gateway_session_snapshot(fx.gw, out, 2u, &copied) == 0);
    CHECK(copied == 2u);
    CHECK(kith_gateway_session_count(fx.gw) == 3u);

    // NULL out with max 0 counts nothing and stays legal; a NULL out with
    // a nonzero max is an error.
    copied = 99u;
    CHECK(kith_gateway_session_snapshot(fx.gw, NULL, 0u, &copied) == 0);
    CHECK(copied == 0u);
    CHECK(kith_gateway_session_snapshot(fx.gw, NULL, 1u, &copied) ==
          kith_error_return(KITH_EINVAL));

    kith_gateway_session_destroy(sessions[0]);
    CHECK(kith_gateway_session_count(fx.gw) == 2u);
    for (size_t i = 0u; i < 3u; ++i)
    {
        if (i != 0u)
        {
            kith_gateway_session_destroy(sessions[i]);
        }
        kith_net_conn_close(conns[i]);
        kith_net_conn_release(conns[i]);
        (void)close(fds[i]);
    }
    CHECK(kith_gateway_session_count(fx.gw) == 0u);
    fixture_fini(&fx);
    return failures;
}

// Unregistering detaches the notification; unregister is idempotent and
// the both-dispatch-flags registration is rejected.
static int test_unregister_and_registration_contract(void)
{
    int failures = 0;
    struct fixture fx;
    struct destroyed_capture cap = {0};
    atomic_init(&cap.calls, 0);
    CHECK(fixture_init(&fx) == 0);
    CHECK(kith_gateway_register_session_destroyed_handler(fx.gw, destroyed_capture_fn, &cap) == 0);
    CHECK(kith_gateway_unregister_session_destroyed_handler(fx.gw) == 0);
    CHECK(kith_gateway_unregister_session_destroyed_handler(fx.gw) == 0);
    const uint32_t both_flags =
        (uint32_t)KITH_GATEWAY_HANDLER_PYTHON | (uint32_t)KITH_GATEWAY_HANDLER_POOL;
    CHECK(kith_gateway_register_session_destroyed_handler_flags(
              fx.gw, destroyed_capture_fn, &cap, both_flags) == kith_error_return(KITH_EINVAL));
    CHECK(kith_gateway_register_session_destroyed_handler(fx.gw, NULL, &cap) ==
          kith_error_return(KITH_EINVAL));
    CHECK(kith_gateway_register_session_destroyed_handler(NULL, destroyed_capture_fn, &cap) ==
          kith_error_return(KITH_EINVAL));
    CHECK(kith_gateway_unregister_session_destroyed_handler(NULL) ==
          kith_error_return(KITH_EINVAL));

    int client_fd = -1;
    kith_net_conn_t *conn = NULL;
    kith_gateway_session_t *session = NULL;
    CHECK(make_session(&fx, 7u, &client_fd, &conn, &session) == 0);
    kith_gateway_session_destroy(session);
    CHECK(atomic_load_explicit(&cap.calls, memory_order_acquire) == 0);

    kith_net_conn_close(conn);
    kith_net_conn_release(conn);
    (void)close(client_fd);
    fixture_fini(&fx);
    return failures;
}

// The bound connection pointer is fixed for the session's lifetime: it
// stays valid and equal after the connection is closed, and closedness is
// not observable through the accessor.
static int test_session_conn_pointer_survives_close(void)
{
    int failures = 0;
    struct fixture fx;
    CHECK(fixture_init(&fx) == 0);
    int client_fd = -1;
    kith_net_conn_t *conn = NULL;
    kith_gateway_session_t *session = NULL;
    CHECK(make_session(&fx, 9u, &client_fd, &conn, &session) == 0);
    CHECK(kith_gateway_session_conn(session) == conn);
    kith_net_conn_close(conn);
    CHECK(kith_gateway_session_conn(session) == conn);
    kith_gateway_session_destroy(session);
    kith_net_conn_release(conn);
    (void)close(client_fd);
    fixture_fini(&fx);
    return failures;
}

int main(void)
{
    int failures = 0;
    failures += test_destroyed_callback_inline_reports_identity();
    failures += test_destroyed_callback_pool_dispatched();
    failures += test_destroyed_callback_no_pool_counts_drop();
    failures += test_destroyed_callback_exit_gate_refusal();
    failures += test_acquire_pins_session_past_destroy();
    failures += test_count_mirror_and_snapshot();
    failures += test_unregister_and_registration_contract();
    failures += test_session_conn_pointer_survives_close();
    if (failures == 0)
    {
        (void)printf("gateway lifecycle: all tests passed\n");
    }
    return failures == 0 ? 0 : 1;
}
