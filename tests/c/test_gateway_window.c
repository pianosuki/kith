/* Subscription-window capacity tests: the window-add failure counter, the
 * retained-add retry backstop (retain, cancel, dedupe, cap, heal), the
 * retry-queue and seedless-session gauges, and the backstop's
 * cross-thread churn shape. Owns its fixture; siblings live in
 * test_gateway_*.c. */

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
#include "kith/version.h"

#include <netinet/in.h>
#include <sys/socket.h>

static int check_cond(bool ok, int line)
{
    if (!ok)
    {
        (void)fprintf(stderr, "gateway window: assertion at line %d failed\n", line);
        return 1;
    }
    return 0;
}

#define CHECK(cond) failures += check_cond((cond), __LINE__)

// A 16-bucket cache splits across the 16 stripes below the 16-slot
// per-stripe floor, so every stripe carries exactly 16 slots: the cache
// holds at most 256 live cells, which keeps the saturation loops in these
// tests small and their landing counts hash-independent.
#define TEST_CACHE_BUCKETS 16u

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
        kith_fabric_create(nullptr, fx->sim, nullptr, &fx->fabric) != 0)
    {
        return -1;
    }
    kith_gateway_params_t params = {0};
    params.size = sizeof(params);
    params.abi_version = KITH_ABI_VERSION;
    params.cache_bucket_count = TEST_CACHE_BUCKETS;
    if (kith_gateway_create(&params, fx->net, fx->fabric, fx->proto, nullptr, &fx->gw) != 0 ||
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
// client fd alive for the connection's lifetime.
static int make_session(struct fixture *fx,
                        uint64_t principal,
                        int *out_client_fd,
                        kith_gateway_session_t **out_session)
{
    *out_client_fd = connect_to_listener(fx->net);
    if (*out_client_fd < 0)
    {
        return -1;
    }
    kith_net_conn_t *conn = nullptr;
    if (kith_net_accept(fx->net, &conn) != 0)
    {
        (void)close(*out_client_fd);
        *out_client_fd = -1;
        return -1;
    }
    return kith_gateway_session_create(
        fx->gw, conn, KITH_GATEWAY_SESSION_APP, principal, nullptr, out_session);
}

static kith_fabric_cell_key_t cell_key(uint32_t zone, int32_t x)
{
    kith_fabric_cell_key_t key = {0};
    key.zone = zone;
    key.cell_x = x;
    return key;
}

static uint64_t gw_failures(kith_gateway_t *gw)
{
    uint64_t value = 0u;
    if (kith_gateway_window_add_failures(gw, &value) != 0)
    {
        return UINT64_MAX;
    }
    return value;
}

static uint64_t gw_retry_adds(kith_gateway_t *gw)
{
    uint64_t value = 0u;
    if (kith_gateway_window_retry_adds(gw, &value) != 0)
    {
        return UINT64_MAX;
    }
    return value;
}

static uint64_t gw_pending(kith_gateway_t *gw)
{
    uint64_t value = 0u;
    if (kith_gateway_window_retries_pending(gw, &value) != 0)
    {
        return UINT64_MAX;
    }
    return value;
}

static uint64_t gw_without_cells(kith_gateway_t *gw)
{
    uint64_t value = 0u;
    if (kith_gateway_sessions_without_cells(gw, &value) != 0)
    {
        return UINT64_MAX;
    }
    return value;
}

static size_t window_count(kith_gateway_session_t *session)
{
    size_t count = 0u;
    if (kith_gateway_session_window_count(session, &count) != 0)
    {
        return SIZE_MAX;
    }
    return count;
}

// Add consecutive cells in @p zone, recording every landed index, until
// @p max_cells adds are attempted or — with @p stop_on_failure — the
// first capacity failure. Returns the first failed add's return code (0
// when every add landed) and the failed cell's x coordinate through
// @p out_first_failed_x (-1 when none failed).
static int add_cells_track_landed(kith_gateway_session_t *session,
                                  uint32_t zone,
                                  uint32_t max_cells,
                                  bool stop_on_failure,
                                  uint32_t *out_landed_x,
                                  size_t *out_landed_count,
                                  int32_t *out_first_failed_x)
{
    *out_landed_count = 0u;
    *out_first_failed_x = -1;
    for (uint32_t i = 0u; i < max_cells; ++i)
    {
        kith_fabric_cell_key_t key = cell_key(zone, (int32_t)i);
        int rc = kith_gateway_session_window_add(session, &key);
        if (rc != 0)
        {
            if (*out_first_failed_x < 0)
            {
                *out_first_failed_x = (int32_t)i;
            }
            if (stop_on_failure)
            {
                return rc;
            }
            continue;
        }
        out_landed_x[(*out_landed_count)++] = i;
    }
    return (*out_first_failed_x >= 0) ? kith_error_return(KITH_ENOMEM) : 0;
}

static int remove_landed(kith_gateway_session_t *session,
                         uint32_t zone,
                         const uint32_t *landed_x,
                         size_t landed_count)
{
    int failures = 0;
    for (size_t i = 0u; i < landed_count; ++i)
    {
        kith_fabric_cell_key_t key = cell_key(zone, (int32_t)landed_x[i]);
        CHECK(kith_gateway_session_window_remove(session, &key) == 0);
    }
    return failures;
}

// The first capacity failure is counted, retained, and healed by the retry
// pass once the landed cells free their stripes: the retained cell is the
// only window member after the drain, the landing increments the retry
// counter exactly once, and a re-add of the landed cell is an idempotent
// no-op that counts no new failure.
static int test_failure_counted_retained_and_healed(void)
{
    struct fixture fx;
    if (fixture_init(&fx) != 0)
    {
        return 1;
    }
    int failures = 0;
    int client_fd = -1;
    kith_gateway_session_t *session = nullptr;
    CHECK(make_session(&fx, 1001u, &client_fd, &session) == 0);

    static uint32_t landed_x[512];
    size_t landed_count = 0u;
    int32_t first_failed_x = -1;
    CHECK(
        add_cells_track_landed(session, 7u, 512u, true, landed_x, &landed_count, &first_failed_x) ==
        kith_error_return(KITH_ENOMEM));
    CHECK(first_failed_x >= 0);
    CHECK(landed_count > 0u);
    CHECK(gw_failures(fx.gw) == 1u);
    CHECK(gw_pending(fx.gw) == 1u);
    CHECK(window_count(session) == landed_count);

    const kith_fabric_cell_key_t failed_key = cell_key(7u, first_failed_x);
    failures += remove_landed(session, 7u, landed_x, landed_count);
    CHECK(window_count(session) == 0u);
    // Removing other cells never cancels another cell's retention.
    CHECK(gw_pending(fx.gw) == 1u);

    CHECK(kith_gateway_tick(fx.gw, 1000u) == 0);
    CHECK(gw_retry_adds(fx.gw) == 1u);
    CHECK(gw_pending(fx.gw) == 0u);
    CHECK(window_count(session) == 1u);
    // The window now holds exactly the retained cell.
    CHECK(kith_gateway_session_window_add(session, &failed_key) == 0);
    CHECK(gw_failures(fx.gw) == 1u);
    CHECK(window_count(session) == 1u);

    kith_gateway_session_destroy(session);
    (void)close(client_fd);
    fixture_fini(&fx);
    return failures;
}

// A remove of a retained cell cancels the retention (the retry must not
// land a rescinded intent), and a window clear cancels every retention.
static int test_remove_and_clear_cancel_retained(void)
{
    struct fixture fx;
    if (fixture_init(&fx) != 0)
    {
        return 1;
    }
    int failures = 0;
    int client_fd = -1;
    kith_gateway_session_t *session = nullptr;
    CHECK(make_session(&fx, 1002u, &client_fd, &session) == 0);

    static uint32_t landed_x[512];
    size_t landed_count = 0u;
    int32_t first_failed_x = -1;
    CHECK(add_cells_track_landed(
              session, 11u, 512u, true, landed_x, &landed_count, &first_failed_x) ==
          kith_error_return(KITH_ENOMEM));
    CHECK(gw_pending(fx.gw) == 1u);
    const kith_fabric_cell_key_t failed_key = cell_key(11u, first_failed_x);

    // The failed cell is not a window member; the remove is idempotent for
    // the window and cancels the retention.
    CHECK(kith_gateway_session_window_remove(session, &failed_key) == 0);
    CHECK(gw_pending(fx.gw) == 0u);
    failures += remove_landed(session, 11u, landed_x, landed_count);
    CHECK(kith_gateway_tick(fx.gw, 1000u) == 0);
    CHECK(gw_retry_adds(fx.gw) == 0u);
    CHECK(window_count(session) == 0u);

    // A clear cancels the retention with the window.
    landed_count = 0u;
    first_failed_x = -1;
    const int rc12 =
        add_cells_track_landed(session, 12u, 512u, true, landed_x, &landed_count, &first_failed_x);
    CHECK(rc12 == kith_error_return(KITH_ENOMEM));
    CHECK(gw_pending(fx.gw) == 1u);
    kith_gateway_session_window_clear(session);
    CHECK(gw_pending(fx.gw) == 0u);
    CHECK(kith_gateway_tick(fx.gw, 1001u) == 0);
    CHECK(gw_retry_adds(fx.gw) == 0u);

    kith_gateway_session_destroy(session);
    (void)close(client_fd);
    fixture_fini(&fx);
    return failures;
}

// The pending ring deduplicates caller retries of the same failed cell,
// pins at its ceiling (an add arriving while the ring is full fails
// without retention), and drains through the retry pass at its bounded
// per-pass rate once every stripe frees.
static int test_pending_ring_dedupes_caps_and_drains(void)
{
    struct fixture fx;
    if (fixture_init(&fx) != 0)
    {
        return 1;
    }
    int failures = 0;
    int client_fd = -1;
    kith_gateway_session_t *session = nullptr;
    CHECK(make_session(&fx, 1003u, &client_fd, &session) == 0);

    static uint32_t landed_x[2000];
    size_t landed_count = 0u;
    int32_t first_failed_x = -1;
    CHECK(add_cells_track_landed(
              session, 21u, 2000u, false, landed_x, &landed_count, &first_failed_x) ==
          kith_error_return(KITH_ENOMEM));
    // At most 256 slots exist, so well past that every add fails and the
    // ring pins at its ceiling.
    const uint64_t base_failures = gw_failures(fx.gw);
    CHECK(base_failures > 256u);
    CHECK(gw_pending(fx.gw) == 64u);

    // Caller retries of an already-retained cell count as failures but
    // never duplicate the entry.
    for (int n = 0; n < 3; ++n)
    {
        kith_fabric_cell_key_t key = cell_key(21u, first_failed_x);
        CHECK(kith_gateway_session_window_add(session, &key) == kith_error_return(KITH_ENOMEM));
    }
    CHECK(gw_failures(fx.gw) == base_failures + 3u);
    CHECK(gw_pending(fx.gw) == 64u);

    // Free every stripe and drain: the pass lands its bounded per-pass
    // share, the rotation head walks the ring, and the whole retention
    // drains without starving.
    failures += remove_landed(session, 21u, landed_x, landed_count);
    const uint64_t retries_before = gw_retry_adds(fx.gw);
    for (int t = 0; t < 24 && gw_pending(fx.gw) > 0u; ++t)
    {
        CHECK(kith_gateway_tick(fx.gw, 2000u + (uint64_t)t) == 0);
    }
    CHECK(gw_pending(fx.gw) == 0u);
    CHECK(gw_retry_adds(fx.gw) - retries_before == 64u);
    CHECK(window_count(session) == 64u);

    kith_gateway_session_destroy(session);
    (void)close(client_fd);
    fixture_fini(&fx);
    return failures;
}

// The seed-failure census counts a bound session whose window stayed
// empty across the pass and clears once the seed lands; an unbound
// session contributes nothing.
static int test_census_tracks_bound_seedless_sessions(void)
{
    struct fixture fx;
    if (fixture_init(&fx) != 0)
    {
        return 1;
    }
    int failures = 0;
    int fd_a = -1;
    int fd_b = -1;
    kith_gateway_session_t *bound_session = nullptr;
    kith_gateway_session_t *plain_session = nullptr;
    CHECK(make_session(&fx, 2001u, &fd_a, &bound_session) == 0);
    CHECK(make_session(&fx, 2002u, &fd_b, &plain_session) == 0);

    CHECK(kith_gateway_tick(fx.gw, 3000u) == 0);
    CHECK(gw_without_cells(fx.gw) == 0u);

    CHECK(kith_gateway_session_bind_actor(bound_session, 4242u) == 0);
    CHECK(kith_gateway_tick(fx.gw, 3001u) == 0);
    CHECK(gw_without_cells(fx.gw) == 1u);

    // The seed lands directly (capacity is available) and the census
    // clears; the unbound session never contributes.
    kith_fabric_cell_key_t key = cell_key(31u, 5);
    CHECK(kith_gateway_session_window_add(bound_session, &key) == 0);
    CHECK(kith_gateway_tick(fx.gw, 3002u) == 0);
    CHECK(gw_without_cells(fx.gw) == 0u);

    kith_gateway_session_destroy(bound_session);
    kith_gateway_session_destroy(plain_session);
    (void)close(fd_a);
    (void)close(fd_b);
    fixture_fini(&fx);
    return failures;
}

// Populate under a saturated cache pins the census contract end to end: a
// healthy populate is census-invisible (bound with cells), a populate
// whose cells all miss the saturated cache rebinds, keeps the landed
// window, and reports the capacity failure with every cell retained, and
// a fully failed populate leaves its session bound without cells — the
// seed-failure state, distinct from any populate in progress. Freeing
// capacity heals the retained seeds through the retry pass and the census
// returns to zero. The fully failed populate runs on its own session so
// the census phase needs no window removal — a removal frees the
// very slots the pending seed needs and heals inside the observation tick.
// Fill the cache to its 256-cell capacity at churn depth so every stripe
// is full and any new cell fails its subscribe. Asserts the fill hit the
// capacity bound.
static int saturate_cache(kith_gateway_session_t *filler)
{
    int failures = 0;
    static uint32_t landed_x[2000];
    size_t landed_count = 0u;
    int32_t first_failed_x = -1;
    CHECK(add_cells_track_landed(
              filler, 62u, 2000u, false, landed_x, &landed_count, &first_failed_x) ==
          kith_error_return(KITH_ENOMEM));
    CHECK(landed_count <= 256u);
    return failures;
}

// Populate two sessions against the saturated cache, eight fresh cells
// each. @p partial carries two landed cells: the rebind publishes, every
// cell retains on the session's own ring, the landed window stays, and
// the call reports the capacity failure. @p failed carries none: the bind
// publishes and all eight cells retain with the window left empty. The
// pending gauge carries the fill's capped 64 plus both sessions' retained
// seeds; the retry pass skips them all while the stripes stay full.
static int populate_fails_retained(kith_gateway_t *gw,
                                   kith_gateway_session_t *partial,
                                   kith_gateway_session_t *failed)
{
    int failures = 0;
    const uint64_t failures_before = gw_failures(gw);
    kith_fabric_cell_key_t more[8];
    for (size_t i = 0u; i < 8u; ++i)
    {
        more[i] = cell_key(63u, (int32_t)i);
    }
    CHECK(kith_gateway_session_populate(partial, 4243u, more, 8u) ==
          kith_error_return(KITH_ENOMEM));
    CHECK(window_count(partial) == 2u);
    CHECK(gw_failures(gw) == failures_before + 8u);

    kith_fabric_cell_key_t fresh[8];
    for (size_t i = 0u; i < 8u; ++i)
    {
        fresh[i] = cell_key(64u, (int32_t)i);
    }
    CHECK(kith_gateway_session_populate(failed, 4244u, fresh, 8u) ==
          kith_error_return(KITH_ENOMEM));
    CHECK(window_count(failed) == 0u);
    CHECK(gw_pending(gw) == 80u);
    return failures;
}

static int test_populate_failure_is_census_seed_failure(void)
{
    struct fixture fx;
    if (fixture_init(&fx) != 0)
    {
        return 1;
    }
    int failures = 0;
    int fd_p1 = -1;
    int fd_p2 = -1;
    int fd_f = -1;
    kith_gateway_session_t *partial = nullptr;
    kith_gateway_session_t *failed = nullptr;
    kith_gateway_session_t *filler = nullptr;
    CHECK(make_session(&fx, 6001u, &fd_p1, &partial) == 0);
    CHECK(make_session(&fx, 6002u, &fd_p2, &failed) == 0);
    CHECK(make_session(&fx, 6003u, &fd_f, &filler) == 0);

    // Healthy populate: identity and window land in one call and the
    // census stays at zero.
    kith_fabric_cell_key_t seed[2] = {cell_key(61u, 0), cell_key(61u, 1)};
    CHECK(kith_gateway_session_populate(partial, 4242u, seed, 2u) == 0);
    CHECK(window_count(partial) == 2u);
    CHECK(kith_gateway_tick(fx.gw, 6000u) == 0);
    CHECK(gw_without_cells(fx.gw) == 0u);

    failures += saturate_cache(filler);
    failures += populate_fails_retained(fx.gw, partial, failed);

    // Read the census under saturation: the fully failed populate's
    // session is bound without cells — the seed-failure state, distinct
    // from any populate in progress.
    CHECK(kith_gateway_tick(fx.gw, 6001u) == 0);
    CHECK(gw_without_cells(fx.gw) == 1u);
    CHECK(window_count(partial) == 2u);
    CHECK(window_count(failed) == 0u);

    // Freeing capacity heals the retained seeds: the retry pass is
    // work-bounded per tick, and a ring whose every entry can land drains
    // geometrically (each pass lands half of what remains), so the tick
    // runs until the pending gauge empties. The census returns to zero
    // with both sessions' windows reseeded.
    kith_gateway_session_destroy(filler);
    for (int t = 0; t < 8 && gw_pending(fx.gw) > 0u; ++t)
    {
        CHECK(kith_gateway_tick(fx.gw, 6002u + (uint64_t)t) == 0);
    }
    CHECK(window_count(partial) == 10u);
    CHECK(window_count(failed) == 8u);
    CHECK(gw_pending(fx.gw) == 0u);
    CHECK(gw_without_cells(fx.gw) == 0u);

    kith_gateway_session_destroy(partial);
    kith_gateway_session_destroy(failed);
    (void)close(fd_p1);
    (void)close(fd_p2);
    (void)close(fd_f);
    fixture_fini(&fx);
    return failures;
}

// Destroying a session drains its retention from the shared gauge.
static int test_destroy_drains_pending(void)
{
    struct fixture fx;
    if (fixture_init(&fx) != 0)
    {
        return 1;
    }
    int failures = 0;
    int client_fd = -1;
    kith_gateway_session_t *session = nullptr;
    CHECK(make_session(&fx, 3001u, &client_fd, &session) == 0);

    static uint32_t landed_x[512];
    size_t landed_count = 0u;
    int32_t first_failed_x = -1;
    CHECK(add_cells_track_landed(
              session, 41u, 512u, true, landed_x, &landed_count, &first_failed_x) ==
          kith_error_return(KITH_ENOMEM));
    CHECK(gw_pending(fx.gw) == 1u);
    kith_gateway_session_destroy(session);
    CHECK(gw_pending(fx.gw) == 0u);
    (void)close(client_fd);
    fixture_fini(&fx);
    return failures;
}

struct churn_args
{
    kith_gateway_session_t *session;
    uint32_t zone;
    int iterations;
};

// Worker-thread churn over two cells against a fully saturated cache:
// every add fails and retains (or dedupes), every remove cancels — the
// cross-thread shape the sanitizer lanes exercise.
static void *churn_worker(void *arg)
{
    struct churn_args *churn = arg;
    for (int i = 0; i < churn->iterations; ++i)
    {
        const int32_t x = (int32_t)(i % 2);
        kith_fabric_cell_key_t key = cell_key(churn->zone, x);
        (void)kith_gateway_session_window_add(churn->session, &key);
        (void)kith_gateway_session_window_remove(churn->session, &key);
    }
    return nullptr;
}

// The backstop's cross-thread shape: a worker thread retains and cancels
// while the reactor thread retries through the tick pass. Serialized by
// the per-session window lock end to end; the shared gauges stay exact.
static int test_worker_churn_threadsafe(void)
{
    struct fixture fx;
    if (fixture_init(&fx) != 0)
    {
        return 1;
    }
    int failures = 0;
    int client_fd = -1;
    kith_gateway_session_t *session = nullptr;
    CHECK(make_session(&fx, 4001u, &client_fd, &session) == 0);

    static uint32_t landed_x[2000];
    size_t landed_count = 0u;
    int32_t first_failed_x = -1;
    CHECK(add_cells_track_landed(
              session, 51u, 2000u, false, landed_x, &landed_count, &first_failed_x) ==
          kith_error_return(KITH_ENOMEM));
    // Every stripe is full at this depth, so the churn cells' adds fail
    // and retain instead of landing silently.
    CHECK(landed_count <= 256u);

    pthread_t worker;
    struct churn_args churn = {.session = session, .zone = 52u, .iterations = 400};
    CHECK(pthread_create(&worker, nullptr, churn_worker, &churn) == 0);
    for (int t = 0; t < 200; ++t)
    {
        CHECK(kith_gateway_tick(fx.gw, 4000u + (uint64_t)t) == 0);
        (void)usleep(1000);
    }
    CHECK(pthread_join(worker, nullptr) == 0);

    // The churn cells are fully retracted: the worker's removes cancelled
    // every retention and left no window member behind.
    for (int32_t x = 0; x < 2; ++x)
    {
        kith_fabric_cell_key_t key = cell_key(52u, x);
        CHECK(kith_gateway_session_window_remove(session, &key) == 0);
    }
    CHECK(gw_retry_adds(fx.gw) <= gw_failures(fx.gw));
    CHECK(gw_pending(fx.gw) <= 64u);

    kith_gateway_session_destroy(session);
    (void)close(client_fd);
    fixture_fini(&fx);
    return failures;
}

int main(void)
{
    int failures = 0;
    failures += test_failure_counted_retained_and_healed();
    failures += test_remove_and_clear_cancel_retained();
    failures += test_pending_ring_dedupes_caps_and_drains();
    failures += test_census_tracks_bound_seedless_sessions();
    failures += test_populate_failure_is_census_seed_failure();
    failures += test_destroy_drains_pending();
    failures += test_worker_churn_threadsafe();
    if (failures == 0)
    {
        (void)fprintf(stderr, "gateway window: all tests passed\n");
    }
    return failures;
}
