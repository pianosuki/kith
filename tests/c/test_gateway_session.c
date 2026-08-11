#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <pthread.h>
#include <unistd.h>

#include "gateway/gateway_internal.h"
#include "gateway/session/session.h"
#include "kith/fabric/fabric.h"
#include "kith/gateway/gateway.h"
#include "kith/net/net.h"
#include "kith/proto/proto.h"
#include "kith/sim/sim.h"
#include "kith/types.h"
#include "kith/version.h"

#include <netinet/in.h>
#include <sys/socket.h>

static int check_cond(bool ok, int line)
{
    if (!ok)
    {
        (void)fprintf(stderr, "gateway session: assertion at line %d failed\n", line);
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
    // Listen once on an ephemeral port; make_conn connects and accepts.
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

static kith_sim_artifact_key_t make_skey(const kith_fabric_cell_key_t *fk, uint32_t epoch)
{
    kith_sim_artifact_key_t k = {0};
    k.zone = fk->zone;
    k.cell_x = fk->cell_x;
    k.cell_y = fk->cell_y;
    k.cell_z = fk->cell_z;
    k.lod = fk->lod;
    k.authority_epoch = epoch;
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

// The window entry points reject NULL handles and NULL out-arguments with
// EINVAL. window_clear(NULL) is a no-op. A fresh session has an empty
// window (count 0).
static int test_session_arg_validation(void)
{
    int failures = 0;
    struct fixture fx;
    CHECK(fixture_init(&fx) == 0);

    kith_net_conn_t *conn = nullptr;
    CHECK(make_conn(fx.net, &conn) == 0);

    CHECK(kith_gateway_session_create(
              nullptr, conn, KITH_GATEWAY_SESSION_SUBSCRIBER, 1u, nullptr, nullptr) ==
          kith_error_return(KITH_EINVAL));
    kith_gateway_session_t *s = nullptr;
    CHECK(kith_gateway_session_create(
              fx.gw, nullptr, KITH_GATEWAY_SESSION_SUBSCRIBER, 1u, nullptr, &s) ==
          kith_error_return(KITH_EINVAL));
    CHECK(s == nullptr);
    CHECK(kith_gateway_session_create(
              fx.gw, conn, KITH_GATEWAY_SESSION_SUBSCRIBER, 1u, nullptr, nullptr) ==
          kith_error_return(KITH_EINVAL));

    CHECK(kith_gateway_session_info(nullptr, nullptr) == kith_error_return(KITH_EINVAL));
    CHECK(kith_gateway_session_conn(nullptr) == nullptr);
    CHECK(kith_gateway_session_bind_actor(nullptr, 1u) == kith_error_return(KITH_EINVAL));

    kith_gateway_session_destroy(nullptr);

    kith_net_conn_close(conn);
    kith_net_conn_release(conn);
    fixture_fini(&fx);
    return failures;
}

// A created session carries the principal id and type, assigns a monotonic
// session id, and binds to the connection. session_conn returns the bound
// connection. bind_actor sets the subscriber actor id (visible via info).
static int test_session_lifecycle(void)
{
    int failures = 0;
    struct fixture fx;
    CHECK(fixture_init(&fx) == 0);

    kith_net_conn_t *conn = nullptr;
    CHECK(make_conn(fx.net, &conn) == 0);

    kith_gateway_session_t *s = nullptr;
    CHECK(kith_gateway_session_create(fx.gw, conn, KITH_GATEWAY_SESSION_APP, 1234u, nullptr, &s) ==
          0);
    CHECK(s != nullptr);

    kith_gateway_session_info_t info = {0};
    CHECK(kith_gateway_session_info(s, &info) == 0);
    CHECK(info.principal_id == 1234u);
    CHECK(info.type == KITH_GATEWAY_SESSION_APP);
    CHECK(info.actor_id == 0u);
    CHECK(info.session_id != 0u);

    CHECK(kith_gateway_session_conn(s) == conn);

    CHECK(kith_gateway_session_bind_actor(s, 99u) == 0);
    CHECK(kith_gateway_session_info(s, &info) == 0);
    CHECK(info.actor_id == 99u);

    kith_gateway_session_destroy(s);

    kith_net_conn_close(conn);
    kith_net_conn_release(conn);
    fixture_fini(&fx);
    return failures;
}

/*---------------------------------------------------------------------------
 * bind_actor concurrency
 *-------------------------------------------------------------------------*/

// Writer threads rebind the session's actor id through the public entry
// point while one composer thread refreshes the view and reader threads
// sample session info — the production shape of one reactor-side composer
// against worker-pool binders. Every observed actor id must be 0 (the
// initial unbound value) or one of the ids the writers bind; this binary
// can only assert that value domain, so the thread-sanitizer lane is the
// arbiter for the access pair itself (a non-atomic write reports there).
#define BIND_THREADS 4u
#define BIND_IDS     4u
#define BIND_ITERS   10000u

struct bind_hammer_state
{
    kith_gateway_t *gw;
    kith_gateway_session_t *session;
};

// Per-thread argument and result slot: threads record failures here and
// return NULL, keeping integer results out of the pthread pointer return.
struct bind_thread_args
{
    struct bind_hammer_state hammer;
    uintptr_t failures;
};

static void *bind_writer_thread(void *arg)
{
    struct bind_thread_args *ta = arg;
    for (uint32_t i = 0u; i < BIND_ITERS; ++i)
    {
        uint64_t id = (uint64_t)(i % BIND_IDS) + 1u;
        if (kith_gateway_session_bind_actor(ta->hammer.session, id) != 0)
        {
            ta->failures += 1u;
        }
    }
    return nullptr;
}

static void *bind_reader_thread(void *arg)
{
    struct bind_thread_args *ta = arg;
    kith_gateway_session_info_t info;
    for (uint32_t i = 0u; i < BIND_ITERS; ++i)
    {
        if (kith_gateway_session_info(ta->hammer.session, &info) != 0)
        {
            ta->failures += 1u;
            continue;
        }
        if (info.actor_id > BIND_IDS)
        {
            ta->failures += 1u;
        }
    }
    return nullptr;
}

static void *bind_composer_thread(void *arg)
{
    struct bind_thread_args *ta = arg;
    for (uint32_t i = 0u; i < BIND_ITERS; ++i)
    {
        // An empty window never locates a subscriber, so the refresh ends
        // in ESTATE once past the unbound guard; both outcomes are legal.
        int rc = kith_gateway_view_refresh(ta->hammer.gw, ta->hammer.session, (uint64_t)i + 1u);
        if (rc != 0 && rc != kith_error_return(KITH_ESTATE))
        {
            ta->failures += 1u;
        }
    }
    return nullptr;
}

// Rebinding races an active composer and concurrent info readers without
// corrupting the bound identity: all threads report zero failures and the
// quiescent session carries one of the bound ids.
static int test_session_bind_actor_concurrent_refresh(void)
{
    int failures = 0;
    struct fixture fx;
    CHECK(fixture_init(&fx) == 0);

    kith_net_conn_t *conn = nullptr;
    CHECK(make_conn(fx.net, &conn) == 0);

    kith_gateway_session_t *s = nullptr;
    CHECK(kith_gateway_session_create(
              fx.gw, conn, KITH_GATEWAY_SESSION_SUBSCRIBER, 1u, nullptr, &s) == 0);

    struct bind_thread_args composer_args = {{fx.gw, s}, 0};
    struct bind_thread_args writer_args[BIND_THREADS];
    struct bind_thread_args reader_args[BIND_THREADS];

    pthread_t composer;
    CHECK(pthread_create(&composer, nullptr, bind_composer_thread, &composer_args) == 0);
    pthread_t writers[BIND_THREADS];
    pthread_t readers[BIND_THREADS];
    for (uint32_t i = 0u; i < BIND_THREADS; ++i)
    {
        writer_args[i].hammer = (struct bind_hammer_state){fx.gw, s};
        writer_args[i].failures = 0;
        reader_args[i].hammer = (struct bind_hammer_state){fx.gw, s};
        reader_args[i].failures = 0;
        CHECK(pthread_create(&writers[i], nullptr, bind_writer_thread, &writer_args[i]) == 0);
        CHECK(pthread_create(&readers[i], nullptr, bind_reader_thread, &reader_args[i]) == 0);
    }

    (void)pthread_join(composer, nullptr);
    failures += (int)composer_args.failures;
    for (uint32_t i = 0u; i < BIND_THREADS; ++i)
    {
        (void)pthread_join(writers[i], nullptr);
        failures += (int)writer_args[i].failures;
        (void)pthread_join(readers[i], nullptr);
        failures += (int)reader_args[i].failures;
    }

    kith_gateway_session_info_t info;
    CHECK(kith_gateway_session_info(s, &info) == 0);
    CHECK(info.actor_id >= 1u && info.actor_id <= BIND_IDS);

    kith_gateway_session_destroy(s);

    kith_net_conn_close(conn);
    kith_net_conn_release(conn);
    fixture_fini(&fx);
    return failures;
}

// The bound actor id member must stay _Atomic-qualified: the direct atomic
// accesses below fail to compile against a plain field, so the unlocked
// write is a compile-time rejection.
static int test_session_bind_actor_member_is_atomic(void)
{
    int failures = 0;
    struct fixture fx;
    CHECK(fixture_init(&fx) == 0);

    kith_net_conn_t *conn = nullptr;
    CHECK(make_conn(fx.net, &conn) == 0);

    kith_gateway_session_t *s = nullptr;
    CHECK(kith_gateway_session_create(
              fx.gw, conn, KITH_GATEWAY_SESSION_SUBSCRIBER, 1u, nullptr, &s) == 0);

    atomic_store_explicit(&s->actor_id, 77u, memory_order_release);
    CHECK(atomic_load_explicit(&s->actor_id, memory_order_acquire) == 77u);

    kith_gateway_session_info_t info;
    CHECK(kith_gateway_session_info(s, &info) == 0);
    CHECK(info.actor_id == 77u);

    kith_gateway_session_destroy(s);

    kith_net_conn_close(conn);
    kith_net_conn_release(conn);
    fixture_fini(&fx);
    return failures;
}

// A second session created on the same gateway receives the next session id.
// Destroying a session frees its slot, so a fresh session can be created
// afterward on a new connection.
static int test_session_id_monotonic(void)
{
    int failures = 0;
    struct fixture fx;
    CHECK(fixture_init(&fx) == 0);

    kith_net_conn_t *c1 = nullptr;
    CHECK(make_conn(fx.net, &c1) == 0);
    kith_net_conn_t *c2 = nullptr;
    CHECK(make_conn(fx.net, &c2) == 0);

    kith_gateway_session_t *s1 = nullptr;
    CHECK(kith_gateway_session_create(
              fx.gw, c1, KITH_GATEWAY_SESSION_SUBSCRIBER, 1u, nullptr, &s1) == 0);
    kith_gateway_session_t *s2 = nullptr;
    CHECK(kith_gateway_session_create(
              fx.gw, c2, KITH_GATEWAY_SESSION_SUBSCRIBER, 2u, nullptr, &s2) == 0);

    kith_gateway_session_info_t i1 = {0};
    kith_gateway_session_info_t i2 = {0};
    CHECK(kith_gateway_session_info(s1, &i1) == 0);
    CHECK(kith_gateway_session_info(s2, &i2) == 0);
    CHECK(i2.session_id == i1.session_id + 1u);

    kith_gateway_session_destroy(s1);
    kith_gateway_session_destroy(s2);

    kith_net_conn_close(c1);
    kith_net_conn_release(c1);
    kith_net_conn_close(c2);
    kith_net_conn_release(c2);
    fixture_fini(&fx);
    return failures;
}

// Binding two sessions to the same connection is rejected with EEXIST. A
// session table sized to one slot rejects a second concurrent session with
// EBUSY.
static int test_session_create_conflicts(void)
{
    int failures = 0;
    struct fixture fx;
    CHECK(fixture_init(&fx) == 0);

    kith_net_conn_t *c1 = nullptr;
    CHECK(make_conn(fx.net, &c1) == 0);
    kith_net_conn_t *c2 = nullptr;
    CHECK(make_conn(fx.net, &c2) == 0);

    kith_gateway_session_t *s1 = nullptr;
    CHECK(kith_gateway_session_create(
              fx.gw, c1, KITH_GATEWAY_SESSION_SUBSCRIBER, 1u, nullptr, &s1) == 0);

    // Same connection again: EEXIST.
    kith_gateway_session_t *dup = nullptr;
    CHECK(kith_gateway_session_create(
              fx.gw, c1, KITH_GATEWAY_SESSION_SUBSCRIBER, 1u, nullptr, &dup) ==
          kith_error_return(KITH_EEXIST));
    CHECK(dup == nullptr);

    kith_gateway_session_destroy(s1);
    s1 = nullptr;

    // A one-slot table fills after the first session; a second concurrent
    // session on a distinct connection is rejected with EBUSY.
    kith_gateway_destroy(fx.gw);
    fx.gw = nullptr;
    kith_gateway_params_t params = {0};
    params.size = sizeof(params);
    params.abi_version = KITH_ABI_VERSION;
    params.max_sessions = 1u;
    CHECK(kith_gateway_create(&params, fx.net, fx.fabric, fx.proto, nullptr, &fx.gw) == 0);

    CHECK(kith_gateway_session_create(
              fx.gw, c1, KITH_GATEWAY_SESSION_SUBSCRIBER, 1u, nullptr, &s1) == 0);
    kith_gateway_session_t *s2 = nullptr;
    CHECK(
        kith_gateway_session_create(fx.gw, c2, KITH_GATEWAY_SESSION_SUBSCRIBER, 2u, nullptr, &s2) ==
        kith_error_return(KITH_EBUSY));
    CHECK(s2 == nullptr);

    kith_gateway_session_destroy(s1);

    kith_net_conn_close(c1);
    kith_net_conn_release(c1);
    kith_net_conn_close(c2);
    kith_net_conn_release(c2);
    fixture_fini(&fx);
    return failures;
}

// The window entry points reject NULL handles and NULL out-arguments with
// EINVAL. window_clear(NULL) is a no-op. A fresh session has an empty
// window (count 0).
static int test_session_window_arg_validation(void)
{
    int failures = 0;
    struct fixture fx;
    CHECK(fixture_init(&fx) == 0);

    kith_net_conn_t *conn = nullptr;
    CHECK(make_conn(fx.net, &conn) == 0);
    kith_gateway_session_t *s = nullptr;
    CHECK(kith_gateway_session_create(
              fx.gw, conn, KITH_GATEWAY_SESSION_SUBSCRIBER, 1u, nullptr, &s) == 0);

    kith_fabric_cell_key_t k = make_key(1u, 0, 0, 0, 0u);
    CHECK(kith_gateway_session_window_add(nullptr, &k) == kith_error_return(KITH_EINVAL));
    CHECK(kith_gateway_session_window_add(s, nullptr) == kith_error_return(KITH_EINVAL));
    CHECK(kith_gateway_session_window_remove(nullptr, &k) == kith_error_return(KITH_EINVAL));
    CHECK(kith_gateway_session_window_remove(s, nullptr) == kith_error_return(KITH_EINVAL));
    CHECK(kith_gateway_session_window_count(nullptr, nullptr) == kith_error_return(KITH_EINVAL));
    size_t count = 99u;
    CHECK(kith_gateway_session_window_count(s, nullptr) == kith_error_return(KITH_EINVAL));
    CHECK(kith_gateway_session_window_count(nullptr, &count) == kith_error_return(KITH_EINVAL));
    CHECK(kith_gateway_session_window_count(s, &count) == 0);
    CHECK(count == 0u);

    kith_gateway_session_window_clear(nullptr);

    kith_gateway_session_destroy(s);
    kith_net_conn_close(conn);
    kith_net_conn_release(conn);
    fixture_fini(&fx);
    return failures;
}

// add/remove/clear drive the shared cache refcount: adding a cell subscribes
// it (cache stats reflect one subscribed cell), adding the same cell twice is
// idempotent, removing decrements, and clear drops the window to zero.
static int test_session_window_refcount(void)
{
    int failures = 0;
    struct fixture fx;
    CHECK(fixture_init(&fx) == 0);

    kith_net_conn_t *conn = nullptr;
    CHECK(make_conn(fx.net, &conn) == 0);
    kith_gateway_session_t *s = nullptr;
    CHECK(kith_gateway_session_create(
              fx.gw, conn, KITH_GATEWAY_SESSION_SUBSCRIBER, 1u, nullptr, &s) == 0);

    kith_fabric_cell_key_t a = make_key(1u, 0, 0, 0, 0u);
    kith_fabric_cell_key_t b = make_key(1u, 1, 0, 0, 0u);
    CHECK(kith_gateway_session_window_add(s, &a) == 0);
    CHECK(kith_gateway_session_window_add(s, &b) == 0);
    size_t count = 0u;
    CHECK(kith_gateway_session_window_count(s, &count) == 0);
    CHECK(count == 2u);

    kith_gateway_cache_stats_t stats = {0};
    CHECK(kith_gateway_cache_stats(fx.gw, &stats) == 0);
    CHECK(stats.subscribed_cell_count == 2u);

    // Idempotent re-add: window size and subscription count unchanged.
    CHECK(kith_gateway_session_window_add(s, &a) == 0);
    CHECK(kith_gateway_session_window_count(s, &count) == 0);
    CHECK(count == 2u);
    CHECK(kith_gateway_cache_stats(fx.gw, &stats) == 0);
    CHECK(stats.subscribed_cell_count == 2u);

    // Remove one: window shrinks, subscription drops to one.
    CHECK(kith_gateway_session_window_remove(s, &a) == 0);
    CHECK(kith_gateway_session_window_count(s, &count) == 0);
    CHECK(count == 1u);
    CHECK(kith_gateway_cache_stats(fx.gw, &stats) == 0);
    CHECK(stats.subscribed_cell_count == 1u);

    // Remove a cell not in the window: no-op.
    CHECK(kith_gateway_session_window_remove(s, &a) == 0);
    CHECK(kith_gateway_session_window_count(s, &count) == 0);
    CHECK(count == 1u);

    kith_gateway_session_window_clear(s);
    CHECK(kith_gateway_session_window_count(s, &count) == 0);
    CHECK(count == 0u);
    CHECK(kith_gateway_cache_stats(fx.gw, &stats) == 0);
    CHECK(stats.subscribed_cell_count == 0u);

    kith_gateway_session_destroy(s);
    kith_net_conn_close(conn);
    kith_net_conn_release(conn);
    fixture_fini(&fx);
    return failures;
}

// Refresh a session's view and return whether a given actor is visible in
// the snapshot. Asserts the snapshot succeeded and carries the expected
// subject count before testing membership.
static bool session_view_has_actor(kith_gateway_t *gw,
                                   kith_gateway_session_t *session,
                                   uint64_t now_ms,
                                   size_t expected_count,
                                   uint64_t actor_id)
{
    if (kith_gateway_view_refresh(gw, session, now_ms) != 0)
    {
        return false;
    }
    kith_gateway_view_snapshot_t meta = {0};
    kith_gateway_view_subject_t out[8] = {0};
    size_t n = 0u;
    if (kith_gateway_view_snapshot(gw, session, &meta, out, 8u, &n) != 0)
    {
        return false;
    }
    if (n != expected_count)
    {
        return false;
    }
    for (size_t i = 0u; i < n; ++i)
    {
        if (out[i].actor_id == actor_id)
        {
            return true;
        }
    }
    return false;
}

// Emulate the dispatch handoff around teardown: a worker reference is
// held across destroy, then window operations run against the detached
// session. Each returns 0, mutates nothing on the fabric side, and the
// cells subscribed before detach were drained by destroy itself.
static int test_session_window_ops_after_detach(void)
{
    int failures = 0;
    struct fixture fx;
    CHECK(fixture_init(&fx) == 0);

    kith_net_conn_t *conn = nullptr;
    CHECK(make_conn(fx.net, &conn) == 0);
    kith_gateway_session_t *s = nullptr;
    CHECK(kith_gateway_session_create(
              fx.gw, conn, KITH_GATEWAY_SESSION_SUBSCRIBER, 1u, nullptr, &s) == 0);

    kith_fabric_cell_key_t a = make_key(1u, 0, 0, 0, 0u);
    kith_fabric_cell_key_t b = make_key(1u, 1, 0, 0, 0u);
    CHECK(kith_gateway_session_window_add(s, &a) == 0);
    CHECK(kith_gateway_session_window_add(s, &b) == 0);

    // The worker's dispatch reference: the session survives destroy while
    // this reference is outstanding.
    gateway_session_acquire(s);
    kith_gateway_session_destroy(s);

    // Post-detach window operations are defined no-ops: no raise shape,
    // no fabric access.
    CHECK(kith_gateway_session_window_add(s, &b) == 0);
    CHECK(kith_gateway_session_window_remove(s, &b) == 0);
    kith_gateway_session_window_clear(s);
    size_t count = 9u;
    CHECK(kith_gateway_session_window_count(s, &count) == 0);
    CHECK(count == 0u);

    // Detach drained both subscriptions; nothing stays refcounted behind.
    kith_gateway_cache_stats_t stats = {0};
    CHECK(kith_gateway_cache_stats(fx.gw, &stats) == 0);
    CHECK(stats.subscribed_cell_count == 0u);

    gateway_session_release(s);
    kith_net_conn_close(conn);
    kith_net_conn_release(conn);
    fixture_fini(&fx);
    return failures;
}

// Disconnect without an explicit clear: destroy drains every remaining
// cell subscription, so cache nodes do not outlive their subscribers.
// Populate binds and seeds in one call: the actor id and the window land
// together, duplicate cells in the seed list are idempotent, a rebind
// leaves the window untouched, and a bind-only populate (zero cells) is
// legal. NULL keys with a non-zero count is a caller error that touches
// neither identity nor window.
static int test_session_populate_binds_and_seeds(void)
{
    int failures = 0;
    struct fixture fx;
    CHECK(fixture_init(&fx) == 0);

    kith_net_conn_t *conn = nullptr;
    CHECK(make_conn(fx.net, &conn) == 0);
    kith_gateway_session_t *s = nullptr;
    CHECK(kith_gateway_session_create(
              fx.gw, conn, KITH_GATEWAY_SESSION_SUBSCRIBER, 1u, nullptr, &s) == 0);

    kith_fabric_cell_key_t a = make_key(1u, 0, 0, 0, 0u);
    kith_fabric_cell_key_t b = make_key(1u, 1, 0, 0, 0u);
    kith_fabric_cell_key_t seed[3] = {a, b, a};
    CHECK(kith_gateway_session_populate(s, 42u, seed, 3u) == 0);

    kith_gateway_session_info_t info = {0};
    CHECK(kith_gateway_session_info(s, &info) == 0);
    CHECK(info.actor_id == 42u);
    size_t count = 0u;
    CHECK(kith_gateway_session_window_count(s, &count) == 0);
    CHECK(count == 2u);
    kith_gateway_cache_stats_t stats = {0};
    CHECK(kith_gateway_cache_stats(fx.gw, &stats) == 0);
    CHECK(stats.subscribed_cell_count == 2u);

    // A bind-only populate rebinds without touching the window.
    CHECK(kith_gateway_session_populate(s, 43u, nullptr, 0u) == 0);
    CHECK(kith_gateway_session_info(s, &info) == 0);
    CHECK(info.actor_id == 43u);
    CHECK(kith_gateway_session_window_count(s, &count) == 0);
    CHECK(count == 2u);

    // Caller errors: NULL session, and NULL keys with a non-zero count.
    CHECK(kith_gateway_session_populate(nullptr, 44u, seed, 1u) == kith_error_return(KITH_EINVAL));
    CHECK(kith_gateway_session_populate(s, 44u, nullptr, 1u) == kith_error_return(KITH_EINVAL));
    CHECK(kith_gateway_session_info(s, &info) == 0);
    CHECK(info.actor_id == 43u);
    CHECK(kith_gateway_session_window_count(s, &count) == 0);
    CHECK(count == 2u);

    kith_gateway_session_destroy(s);
    kith_net_conn_close(conn);
    kith_net_conn_release(conn);
    fixture_fini(&fx);
    return failures;
}

// Populate against a detached session (a reference held across destroy):
// the bind still publishes — identity reads survive detach — and the seed
// is an inert no-op, mirroring the detached window add. No subscription
// reappears behind the drain.
static int test_session_populate_after_detach(void)
{
    int failures = 0;
    struct fixture fx;
    CHECK(fixture_init(&fx) == 0);

    kith_net_conn_t *conn = nullptr;
    CHECK(make_conn(fx.net, &conn) == 0);
    kith_gateway_session_t *s = nullptr;
    CHECK(kith_gateway_session_create(
              fx.gw, conn, KITH_GATEWAY_SESSION_SUBSCRIBER, 1u, nullptr, &s) == 0);

    gateway_session_acquire(s);
    kith_gateway_session_destroy(s);

    kith_fabric_cell_key_t a = make_key(1u, 0, 0, 0, 0u);
    CHECK(kith_gateway_session_populate(s, 77u, &a, 1u) == 0);
    kith_gateway_session_info_t info = {0};
    CHECK(kith_gateway_session_info(s, &info) == 0);
    CHECK(info.actor_id == 77u);
    size_t count = 9u;
    CHECK(kith_gateway_session_window_count(s, &count) == 0);
    CHECK(count == 0u);
    kith_gateway_cache_stats_t stats = {0};
    CHECK(kith_gateway_cache_stats(fx.gw, &stats) == 0);
    CHECK(stats.subscribed_cell_count == 0u);

    gateway_session_release(s);
    kith_net_conn_close(conn);
    kith_net_conn_release(conn);
    fixture_fini(&fx);
    return failures;
}

static int test_session_destroy_drains_cell_refcounts(void)
{
    int failures = 0;
    struct fixture fx;
    CHECK(fixture_init(&fx) == 0);

    kith_net_conn_t *conn = nullptr;
    CHECK(make_conn(fx.net, &conn) == 0);
    kith_gateway_session_t *s = nullptr;
    CHECK(kith_gateway_session_create(
              fx.gw, conn, KITH_GATEWAY_SESSION_SUBSCRIBER, 1u, nullptr, &s) == 0);

    for (size_t i = 0u; i < 4u; ++i)
    {
        kith_fabric_cell_key_t k = make_key(1u, (int32_t)i, 0, 0, 0u);
        CHECK(kith_gateway_session_window_add(s, &k) == 0);
    }
    kith_gateway_cache_stats_t stats = {0};
    CHECK(kith_gateway_cache_stats(fx.gw, &stats) == 0);
    CHECK(stats.subscribed_cell_count == 4u);

    kith_gateway_session_destroy(s);
    CHECK(kith_gateway_cache_stats(fx.gw, &stats) == 0);
    CHECK(stats.subscribed_cell_count == 0u);

    kith_net_conn_close(conn);
    kith_net_conn_release(conn);
    fixture_fini(&fx);
    return failures;
}

// The per-subscriber window scopes the relevance composer: two sessions with
// different windows see different candidate sets. An actor published into a
// cell tracked only by session A is visible to A and invisible to B; adding
// the cell to B's window makes it visible to B too.
static int test_session_window_scopes_view(void)
{
    int failures = 0;
    struct fixture fx;
    CHECK(fixture_init(&fx) == 0);

    kith_fabric_cell_key_t cell_a = make_key(1u, 0, 0, 0, 0u);
    kith_fabric_cell_key_t cell_b = make_key(1u, 5, 0, 0, 0u);

    kith_net_conn_t *conn_a = nullptr;
    CHECK(make_conn(fx.net, &conn_a) == 0);
    kith_gateway_session_t *sa = nullptr;
    CHECK(kith_gateway_session_create(
              fx.gw, conn_a, KITH_GATEWAY_SESSION_SUBSCRIBER, 1u, nullptr, &sa) == 0);
    CHECK(kith_gateway_session_bind_actor(sa, 10u) == 0);
    CHECK(kith_gateway_session_window_add(sa, &cell_a) == 0);
    CHECK(kith_gateway_session_window_add(sa, &cell_b) == 0);

    kith_net_conn_t *conn_b = nullptr;
    CHECK(make_conn(fx.net, &conn_b) == 0);
    kith_gateway_session_t *sb = nullptr;
    CHECK(kith_gateway_session_create(
              fx.gw, conn_b, KITH_GATEWAY_SESSION_SUBSCRIBER, 2u, nullptr, &sb) == 0);
    CHECK(kith_gateway_session_bind_actor(sb, 20u) == 0);
    // sb tracks only its own cell; cell_b is outside its window.
    CHECK(kith_gateway_session_window_add(sb, &cell_a) == 0);

    // Subscriber a (id 10) in cell_a; subscriber b (id 20) in cell_a; an
    // extra actor (id 30) in cell_b.
    kith_sim_artifact_key_t ska = make_skey(&cell_a, 1u);
    kith_sim_actor_t aa = make_actor(10u, 0, 0, 0u);
    CHECK(kith_sim_publish_artifact(fx.sim, &ska, &aa, nullptr) == 0);
    kith_sim_actor_t ab = make_actor(20u, 0, 0, 0u);
    CHECK(kith_sim_publish_artifact(fx.sim, &ska, &ab, nullptr) == 0);
    kith_sim_artifact_key_t skb = make_skey(&cell_b, 1u);
    kith_sim_actor_t extra = make_actor(30u, 0, 0, 0u);
    CHECK(kith_sim_publish_artifact(fx.sim, &skb, &extra, nullptr) == 0);
    CHECK(kith_fabric_publish(fx.fabric, &cell_a, 1u, nullptr) == 0);
    CHECK(kith_fabric_publish(fx.fabric, &cell_b, 1u, nullptr) == 0);
    CHECK(kith_gateway_cache_refresh(fx.gw, 100u) == 0);

    // a tracks both cells: its view (3 subjects) includes the extra actor.
    CHECK(session_view_has_actor(fx.gw, sa, 200u, 3u, 30u));
    // b tracks only cell_a: its view (2 subjects) excludes the extra actor.
    CHECK(!session_view_has_actor(fx.gw, sb, 300u, 2u, 30u));
    // Adding cell_b to b's window makes the extra actor visible to b.
    CHECK(kith_gateway_session_window_add(sb, &cell_b) == 0);
    CHECK(session_view_has_actor(fx.gw, sb, 400u, 3u, 30u));

    kith_gateway_session_destroy(sa);
    kith_gateway_session_destroy(sb);
    kith_net_conn_close(conn_a);
    kith_net_conn_release(conn_a);
    kith_net_conn_close(conn_b);
    kith_net_conn_release(conn_b);
    fixture_fini(&fx);
    return failures;
}

// The session table is an open-addressed hash keyed by connection pointer.
// Removing a session whose slot sits in the middle of a probe chain must not
// leave a null gap, or a subsequent lookup for a session that hashed to an
// earlier slot and probed past the gap stops at the gap and fails to find
// it. That session is then never removed by its own conn-close path, its
// slot keeps a pointer to freed memory, and the next gateway tick
// dereferences the dangling pointer. Backward-shift deletion pulls
// subsequent chain entries back into the gap so every remaining chain
// stays contiguous.
//
// The table primitives key on the connection pointer only (hash it and
// compare it for equality); they never dereference the connection, so mock
// sessions carrying crafted connection values drive the table directly and
// deterministically force collisions without a network.
enum
{
    TABLE_BUCKETS = 16u,
    TABLE_CHAIN = 8u
};

// Fabricate a connection pointer from an integer address. The session table
// hashes the pointer value (gateway_conn_mix) and compares pointers for
// equality; it never dereferences the connection, so a crafted address is a
// valid key.
static kith_net_conn_t *mock_conn_from_addr(uintptr_t addr)
{
    return (kith_net_conn_t *)addr; // NOLINT(performance-no-int-to-ptr)
}

// Fill @p conns with @p chain pointers whose hash home is @p home, so the
// inserts build a single probe chain. Returns the count found.
static size_t collect_colliding_conns(size_t home, size_t chain, kith_net_conn_t **conns)
{
    size_t const mask = TABLE_BUCKETS - 1u;
    size_t found = 0u;
    for (uintptr_t addr = 0x1000u; addr < 0x100000u && found < chain; addr += 0x10u)
    {
        kith_net_conn_t *c = mock_conn_from_addr(addr);
        if (((size_t)gateway_conn_mix(c) & mask) == home)
        {
            conns[found] = c;
            found += 1u;
        }
    }
    return found;
}

// Allocate @p chain zeroed sessions and bind each to the matching conn.
static int
build_mock_sessions(kith_net_conn_t **conns, struct kith_gateway_session **sessions, size_t chain)
{
    for (size_t i = 0u; i < chain; ++i)
    {
        sessions[i] = calloc(1, sizeof(*sessions[i]));
        if (!sessions[i])
        {
            return 1;
        }
        sessions[i]->conn = conns[i];
    }
    return 0;
}

// Assert every conn in @p conns (except index @p skip) still resolves to its
// session after a removal.
static int assert_remaining_findable(struct gateway_session_table *t,
                                     kith_net_conn_t **conns,
                                     struct kith_gateway_session **sessions,
                                     size_t chain,
                                     size_t skip)
{
    int failures = 0;
    for (size_t i = 0u; i < chain; ++i)
    {
        if (i == skip)
        {
            continue;
        }
        CHECK(gateway_session_lookup_by_conn(t, conns[i]) == sessions[i]);
    }
    return failures;
}

// Remove a session whose slot sits in the middle of a probe chain and assert
// the chain is repaired (every other session stays findable), then drain the
// table and assert no slot retains a stranded pointer.
static int test_session_table_remove_preserves_chain(void)
{
    int failures = 0;
    size_t const mask = TABLE_BUCKETS - 1u;
    kith_net_conn_t *conns[TABLE_CHAIN] = {0};
    size_t home = (size_t)gateway_conn_mix(mock_conn_from_addr(0x1000u)) & mask;
    CHECK(collect_colliding_conns(home, TABLE_CHAIN, conns) == TABLE_CHAIN);

    struct gateway_session_table t;
    CHECK(gateway_session_table_init(&t, TABLE_BUCKETS, nullptr) == 0);
    struct kith_gateway_session *sessions[TABLE_CHAIN] = {0};
    CHECK(build_mock_sessions(conns, sessions, TABLE_CHAIN) == 0);
    for (size_t i = 0u; i < TABLE_CHAIN; ++i)
    {
        CHECK(gateway_session_insert(&t, sessions[i]) == 0);
    }
    CHECK(t.count == TABLE_CHAIN);

    // Every session is findable before removal.
    for (size_t i = 0u; i < TABLE_CHAIN; ++i)
    {
        CHECK(gateway_session_lookup_by_conn(&t, conns[i]) == sessions[i]);
    }

    // Remove a session in the middle of the chain: the backward-shift re-links
    // the chain, so sessions probed past the removed slot still resolve.
    size_t const victim = 3u;
    gateway_session_remove(&t, conns[victim]);
    CHECK(t.count == TABLE_CHAIN - 1u);
    CHECK(gateway_session_lookup_by_conn(&t, conns[victim]) == nullptr);
    // Every other session is still findable: the chain re-links around the
    // removed slot instead of severing at it.
    failures += assert_remaining_findable(&t, conns, sessions, TABLE_CHAIN, victim);

    // Draining one-by-one in arbitrary order empties every slot (no stranded
    // pointers, no count drift) and leaves the table ready for fresh inserts.
    for (size_t i = 0u; i < TABLE_CHAIN; ++i)
    {
        if (i == victim)
        {
            continue;
        }
        gateway_session_remove(&t, conns[i]);
    }
    CHECK(t.count == 0u);
    for (size_t i = 0u; i < t.buckets; ++i)
    {
        CHECK(t.slots[i] == nullptr);
    }

    for (size_t i = 0u; i < TABLE_CHAIN; ++i)
    {
        free(sessions[i]);
    }
    gateway_session_table_fini(&t);
    return failures;
}

// A probe chain that wraps past the end of the table back to slot 0 is
// repaired the same way as a non-wrapping chain.
static int test_session_table_remove_repairs_wrapped_chain(void)
{
    int failures = 0;
    kith_net_conn_t *conns[TABLE_CHAIN] = {0};
    // Home at the last slot so CHAIN entries occupy [home, 0, 1, ...].
    size_t const wrap_home = TABLE_BUCKETS - 1u;
    CHECK(collect_colliding_conns(wrap_home, TABLE_CHAIN, conns) == TABLE_CHAIN);

    struct gateway_session_table t;
    CHECK(gateway_session_table_init(&t, TABLE_BUCKETS, nullptr) == 0);
    struct kith_gateway_session *sessions[TABLE_CHAIN] = {0};
    CHECK(build_mock_sessions(conns, sessions, TABLE_CHAIN) == 0);
    for (size_t i = 0u; i < TABLE_CHAIN; ++i)
    {
        CHECK(gateway_session_insert(&t, sessions[i]) == 0);
    }
    CHECK(t.count == TABLE_CHAIN);

    // Remove the entry whose home is the wrap point (first in the chain).
    gateway_session_remove(&t, conns[0]);
    CHECK(t.count == TABLE_CHAIN - 1u);
    failures += assert_remaining_findable(&t, conns, sessions, TABLE_CHAIN, 0u);

    for (size_t i = 1u; i < TABLE_CHAIN; ++i)
    {
        gateway_session_remove(&t, conns[i]);
    }
    CHECK(t.count == 0u);

    for (size_t i = 0u; i < TABLE_CHAIN; ++i)
    {
        free(sessions[i]);
    }
    gateway_session_table_fini(&t);
    return failures;
}

int main(void)
{
    int rc = 0;
    rc |= test_session_arg_validation();
    rc |= test_session_lifecycle();
    rc |= test_session_bind_actor_concurrent_refresh();
    rc |= test_session_bind_actor_member_is_atomic();
    rc |= test_session_id_monotonic();
    rc |= test_session_create_conflicts();
    rc |= test_session_window_arg_validation();
    rc |= test_session_window_refcount();
    rc |= test_session_populate_binds_and_seeds();
    rc |= test_session_window_ops_after_detach();
    rc |= test_session_populate_after_detach();
    rc |= test_session_destroy_drains_cell_refcounts();
    rc |= test_session_window_scopes_view();
    rc |= test_session_table_remove_preserves_chain();
    rc |= test_session_table_remove_repairs_wrapped_chain();
    if (rc != 0)
    {
        (void)fprintf(stderr, "gateway session tests FAILED\n");
    }
    return rc;
}
