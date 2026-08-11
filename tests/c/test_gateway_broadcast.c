/* Cell-scoped broadcast queue pins: submit validation, queue saturation,
 * window-derived fanout, FIFO ordering, ordering ahead of the tick's
 * composed frames, drop accounting, teardown release, and the
 * submit-vs-drain race shape the sanitizer lane exercises. */

#include <errno.h>
#include <signal.h>
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
#include <sys/time.h>

static int check_cond(bool ok, int line)
{
    if (!ok)
    {
        (void)fprintf(stderr, "gateway broadcast: assertion at line %d failed\n", line);
        return 1;
    }
    return 0;
}

#define CHECK(cond) failures += check_cond((cond), __LINE__)

static const uint16_t event_type_a = 1100u;
static const uint16_t event_type_b = 1101u;

/*---------------------------------------------------------------------------
 * tally allocator
 *-------------------------------------------------------------------------*/

typedef struct alloc_tally
{
    unsigned attempts;
    unsigned frees;
    unsigned outstanding;
} alloc_tally_t;

static alloc_tally_t g_broadcast_tally;

static void broadcast_tally_reset(void)
{
    g_broadcast_tally.attempts = 0u;
    g_broadcast_tally.frees = 0u;
    g_broadcast_tally.outstanding = 0u;
}

static void *tally_alloc(void *ctx, size_t size)
{
    alloc_tally_t *tally = ctx;
    tally->attempts++;
    tally->outstanding++;
    return malloc(size);
}

static void *tally_alloc_zero(void *ctx, size_t count, size_t size)
{
    alloc_tally_t *tally = ctx;
    tally->attempts++;
    tally->outstanding++;
    return calloc(count, size);
}

static void *tally_realloc(void *ctx, void *ptr, size_t size)
{
    alloc_tally_t *tally = ctx;
    tally->attempts++;
    if (ptr != nullptr)
    {
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
    .user_data = &g_broadcast_tally,
    .alloc = tally_alloc,
    .alloc_zero = tally_alloc_zero,
    .realloc = tally_realloc,
    .free = tally_free,
    .reserved = {nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr},
};

/*---------------------------------------------------------------------------
 * fixture
 *-------------------------------------------------------------------------*/

struct fixture
{
    kith_proto_t *proto;
    kith_net_t *net;
    kith_sim_t *sim;
    kith_fabric_t *fabric;
    kith_gateway_t *gw;
};

/** Stack construction: proto with the replication and event types
 *  registered, net/sim/fabric, a gateway with per-subject replication
 *  framing enabled, and a loopback listener. @p alloc routes through the
 *  gateway (NULL selects the default allocator). */
static int fixture_init_alloc(struct fixture *fx, const kith_allocator_t *alloc)
{
    memset(fx, 0, sizeof(*fx));
    if (kith_proto_create(nullptr, nullptr, &fx->proto) != 0)
    {
        return -1;
    }
    if (kith_proto_register_type_id(fx->proto, "replication", KITH_PROTO_TYPE_USER_BASE) != 0 ||
        kith_proto_register_type_id(fx->proto, "event_a", event_type_a) != 0 ||
        kith_proto_register_type_id(fx->proto, "event_b", event_type_b) != 0)
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
    params.replication_type_id = KITH_PROTO_TYPE_USER_BASE;
    if (kith_gateway_create(&params, fx->net, fx->fabric, fx->proto, alloc, &fx->gw) != 0)
    {
        return -1;
    }
    if (kith_net_listen(fx->net, "127.0.0.1", 0) != 0)
    {
        return -1;
    }
    return 0;
}

static int fixture_init(struct fixture *fx)
{
    return fixture_init_alloc(fx, nullptr);
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

/** One loopback session with its peer socket kept open: connect, accept,
 *  create, bind, seed @p cells into the window. The client fd stays open so
 *  a test can read the frames the gateway delivers on the wire. */
static int make_session(const struct fixture *fx,
                        const kith_fabric_cell_key_t *cells,
                        size_t cell_count,
                        kith_net_conn_t **out_conn,
                        int *out_client_fd,
                        kith_gateway_session_t **out_session)
{
    *out_conn = nullptr;
    *out_session = nullptr;
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
    if (kith_gateway_session_create(
            fx->gw, *out_conn, KITH_GATEWAY_SESSION_SUBSCRIBER, 1u, nullptr, out_session) != 0)
    {
        kith_net_conn_close(*out_conn);
        kith_net_conn_release(*out_conn);
        (void)close(*out_client_fd);
        *out_client_fd = -1;
        return -1;
    }
    if (kith_gateway_session_bind_actor(*out_session, 100u) != 0)
    {
        return -1;
    }
    for (size_t i = 0u; i < cell_count; ++i)
    {
        if (kith_gateway_session_window_add(*out_session, &cells[i]) != 0)
        {
            return -1;
        }
    }
    return 0;
}

static void session_teardown(kith_gateway_session_t *s, kith_net_conn_t *conn, int client_fd)
{
    kith_gateway_session_destroy(s);
    kith_net_conn_close(conn);
    kith_net_conn_release(conn);
    (void)close(client_fd);
}

static int read_full(int fd, void *buf, size_t n)
{
    uint8_t *p = buf;
    size_t got = 0u;
    while (got < n)
    {
        ssize_t r = recv(fd, p + got, n - got, 0);
        if (r < 0 && errno == EINTR)
        {
            continue;
        }
        if (r <= 0)
        {
            return -1;
        }
        got += (size_t)r;
    }
    return 0;
}

/** Read one proto frame from the peer socket; the header fields land in
 *  @p out_type / @p out_len, the payload in @p out_body. */
static int
read_frame(int fd, uint16_t *out_type, uint32_t *out_len, void *out_body, size_t body_cap)
{
    uint8_t hdr[10];
    if (read_full(fd, hdr, sizeof(hdr)) != 0)
    {
        return -1;
    }
    if (hdr[0] != KITH_PROTO_MAGIC0 || hdr[1] != KITH_PROTO_MAGIC1 ||
        hdr[2] != KITH_PROTO_VERSION || hdr[3] != 0u)
    {
        return -1;
    }
    *out_type = (uint16_t)((uint32_t)hdr[4] << 8u | hdr[5]);
    *out_len = (uint32_t)hdr[6] << 24u | (uint32_t)hdr[7] << 16u | (uint32_t)hdr[8] << 8u | hdr[9];
    if (*out_len > body_cap || read_full(fd, out_body, *out_len) != 0)
    {
        return -1;
    }
    return 0;
}

static void peer_set_timeout(int client_fd, long seconds, long usec)
{
    const struct timeval tv = {.tv_sec = seconds, .tv_usec = usec};
    (void)setsockopt(client_fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
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

/*---------------------------------------------------------------------------
 * tests
 *-------------------------------------------------------------------------*/

// Every submit rejection path: NULL handles, a NULL payload with a
// non-zero length, a payload past UINT32_MAX, and a payload past the
// codec's maximum frame payload. The queue accepts a valid submit after
// every rejected one.
static int test_broadcast_submit_validation(void)
{
    int failures = 0;
    struct fixture fx;
    CHECK(fixture_init(&fx) == 0);

    kith_fabric_cell_key_t cell = make_key(1u, 0, 0, 0, 0u);
    static const uint8_t body[] = {1u, 2u, 3u};

    CHECK(kith_gateway_broadcast_cell(nullptr, &cell, event_type_a, body, sizeof(body)) ==
          kith_error_return(KITH_EINVAL));
    CHECK(kith_gateway_broadcast_cell(fx.gw, nullptr, event_type_a, body, sizeof(body)) ==
          kith_error_return(KITH_EINVAL));
    CHECK(kith_gateway_broadcast_cell(fx.gw, &cell, event_type_a, nullptr, 1u) ==
          kith_error_return(KITH_EINVAL));
    CHECK(kith_gateway_broadcast_cell(fx.gw, &cell, event_type_a, body, (size_t)UINT32_MAX + 1u) ==
          kith_error_return(KITH_EOVERFLOW));
    // Past the codec's maximum frame payload the measure stage answers 0
    // and the submit refuses without queueing.
    uint8_t *oversize = malloc((size_t)KITH_PROTO_DEFAULT_MAX_PAYLOAD + 1u);
    CHECK(oversize != nullptr);
    CHECK(kith_gateway_broadcast_cell(
              fx.gw, &cell, event_type_a, oversize, (size_t)KITH_PROTO_DEFAULT_MAX_PAYLOAD + 1u) ==
          kith_error_return(KITH_EPROTO));
    free(oversize);

    // A valid submit still queues and delivers after the rejected ones.
    kith_net_conn_t *conn = nullptr;
    kith_gateway_session_t *s = nullptr;
    int client_fd = -1;
    CHECK(make_session(&fx, &cell, 1u, &conn, &client_fd, &s) == 0);
    CHECK(kith_gateway_broadcast_cell(fx.gw, &cell, event_type_a, body, sizeof(body)) == 0);
    CHECK(kith_gateway_tick(fx.gw, 100u) == 0);
    CHECK(kith_net_conn_write(conn) == 0);
    uint16_t type = 0u;
    uint32_t len = 0u;
    uint8_t got[16] = {0};
    peer_set_timeout(client_fd, 5, 0);
    CHECK(read_frame(client_fd, &type, &len, got, sizeof(got)) == 0);
    CHECK(type == event_type_a);
    CHECK(len == sizeof(body));
    CHECK(memcmp(got, body, sizeof(body)) == 0);

    session_teardown(s, conn, client_fd);
    fixture_fini(&fx);
    return failures;
}

// A submit past the queue's fixed depth is refused with EAGAIN and counted;
// the drain releases the accepted requests and the queue accepts again.
static int test_broadcast_refused_when_full(void)
{
    int failures = 0;
    struct fixture fx;
    CHECK(fixture_init(&fx) == 0);

    kith_fabric_cell_key_t cell = make_key(1u, 0, 0, 0, 0u);
    static const uint8_t body[] = {9u};
    for (unsigned i = 0u; i < 64u; ++i)
    {
        CHECK(kith_gateway_broadcast_cell(fx.gw, &cell, event_type_a, body, sizeof(body)) == 0);
    }
    CHECK(kith_gateway_broadcast_cell(fx.gw, &cell, event_type_a, body, sizeof(body)) ==
          kith_error_return(KITH_EAGAIN));
    uint64_t refusals = 0u;
    CHECK(kith_gateway_broadcast_refusals(fx.gw, &refusals) == 0);
    CHECK(refusals == 1u);

    // The drain releases every accepted request; with no sessions nothing
    // is delivered and nothing drops.
    CHECK(kith_gateway_tick(fx.gw, 100u) == 0);
    uint64_t drops = 0u;
    CHECK(kith_gateway_broadcast_drops(fx.gw, &drops) == 0);
    CHECK(drops == 0u);
    CHECK(kith_gateway_broadcast_cell(fx.gw, &cell, event_type_a, body, sizeof(body)) == 0);

    fixture_fini(&fx);
    return failures;
}

// The fanout's recipient set is the sessions whose windows cover the cell:
// two covering sessions each receive exactly one frame with the submitted
// bytes; a session covering only another cell receives nothing; the
// counters stay at zero.
static int test_broadcast_fans_out_to_window(void)
{
    int failures = 0;
    struct fixture fx;
    CHECK(fixture_init(&fx) == 0);

    kith_fabric_cell_key_t cell_a = make_key(1u, 0, 0, 0, 0u);
    kith_fabric_cell_key_t cell_b = make_key(1u, 1, 0, 0, 0u);

    kith_net_conn_t *conn_a = nullptr;
    kith_gateway_session_t *s_a = nullptr;
    int fd_a = -1;
    CHECK(make_session(&fx, &cell_a, 1u, &conn_a, &fd_a, &s_a) == 0);
    kith_net_conn_t *conn_ab = nullptr;
    kith_gateway_session_t *s_ab = nullptr;
    int fd_ab = -1;
    const kith_fabric_cell_key_t both[2] = {cell_a, cell_b};
    CHECK(make_session(&fx, both, 2u, &conn_ab, &fd_ab, &s_ab) == 0);
    kith_net_conn_t *conn_b = nullptr;
    kith_gateway_session_t *s_b = nullptr;
    int fd_b = -1;
    CHECK(make_session(&fx, &cell_b, 1u, &conn_b, &fd_b, &s_b) == 0);

    static const uint8_t text[] = "cell a broadcast";
    CHECK(kith_gateway_broadcast_cell(fx.gw, &cell_a, event_type_a, text, sizeof(text)) == 0);
    CHECK(kith_gateway_tick(fx.gw, 100u) == 0);
    CHECK(kith_net_conn_write(conn_a) == 0);
    CHECK(kith_net_conn_write(conn_ab) == 0);

    uint16_t type = 0u;
    uint32_t len = 0u;
    uint8_t got[64] = {0};
    peer_set_timeout(fd_a, 5, 0);
    peer_set_timeout(fd_ab, 5, 0);
    CHECK(read_frame(fd_a, &type, &len, got, sizeof(got)) == 0);
    CHECK(type == event_type_a);
    CHECK(len == sizeof(text));
    CHECK(memcmp(got, text, sizeof(text)) == 0);
    memset(got, 0, sizeof(got));
    CHECK(read_frame(fd_ab, &type, &len, got, sizeof(got)) == 0);
    CHECK(type == event_type_a);
    CHECK(len == sizeof(text));
    CHECK(memcmp(got, text, sizeof(text)) == 0);
    // The session not covering the cell receives nothing: a read with a
    // short timeout fails instead of yielding a frame.
    peer_set_timeout(fd_b, 0, 200000);
    uint8_t probe[4] = {0};
    CHECK(read_full(fd_b, probe, sizeof(probe)) != 0);

    uint64_t refusals = 0u;
    uint64_t drops = 0u;
    CHECK(kith_gateway_broadcast_refusals(fx.gw, &refusals) == 0);
    CHECK(kith_gateway_broadcast_drops(fx.gw, &drops) == 0);
    CHECK(refusals == 0u);
    CHECK(drops == 0u);

    // A second broadcast to the other cell reaches its own recipient set.
    CHECK(kith_gateway_broadcast_cell(fx.gw, &cell_b, event_type_b, text, sizeof(text)) == 0);
    CHECK(kith_gateway_tick(fx.gw, 300u) == 0);
    CHECK(kith_net_conn_write(conn_ab) == 0);
    CHECK(kith_net_conn_write(conn_b) == 0);
    CHECK(read_frame(fd_ab, &type, &len, got, sizeof(got)) == 0);
    CHECK(type == event_type_b);
    CHECK(read_frame(fd_b, &type, &len, got, sizeof(got)) == 0);
    CHECK(type == event_type_b);

    session_teardown(s_a, conn_a, fd_a);
    session_teardown(s_ab, conn_ab, fd_ab);
    session_teardown(s_b, conn_b, fd_b);
    fixture_fini(&fx);
    return failures;
}

// Requests drain in submit order: two broadcasts to the same cell arrive as
// two frames on the covering session, first submitted first delivered.
static int test_broadcast_fifo_order(void)
{
    int failures = 0;
    struct fixture fx;
    CHECK(fixture_init(&fx) == 0);

    kith_fabric_cell_key_t cell = make_key(2u, 0, 0, 0, 0u);
    kith_net_conn_t *conn = nullptr;
    kith_gateway_session_t *s = nullptr;
    int client_fd = -1;
    CHECK(make_session(&fx, &cell, 1u, &conn, &client_fd, &s) == 0);

    static const uint8_t first[] = {0x11u, 0x22u};
    static const uint8_t second[] = {0x33u};
    CHECK(kith_gateway_broadcast_cell(fx.gw, &cell, event_type_a, first, sizeof(first)) == 0);
    CHECK(kith_gateway_broadcast_cell(fx.gw, &cell, event_type_b, second, sizeof(second)) == 0);
    CHECK(kith_gateway_tick(fx.gw, 100u) == 0);
    CHECK(kith_net_conn_write(conn) == 0);

    uint16_t type = 0u;
    uint32_t len = 0u;
    uint8_t got[16] = {0};
    peer_set_timeout(client_fd, 5, 0);
    CHECK(read_frame(client_fd, &type, &len, got, sizeof(got)) == 0);
    CHECK(type == event_type_a);
    CHECK(len == sizeof(first));
    CHECK(memcmp(got, first, sizeof(first)) == 0);
    CHECK(read_frame(client_fd, &type, &len, got, sizeof(got)) == 0);
    CHECK(type == event_type_b);
    CHECK(len == sizeof(second));
    CHECK(memcmp(got, second, sizeof(second)) == 0);

    session_teardown(s, conn, client_fd);
    fixture_fini(&fx);
    return failures;
}

// A queued broadcast rides ahead of the same tick's composed frames: the
// drain runs before the session pass, so the covering session's connection
// carries the broadcast frame first, then the replication frames.
static int test_broadcast_precedes_composed(void)
{
    int failures = 0;
    struct fixture fx;
    CHECK(fixture_init(&fx) == 0);

    kith_fabric_cell_key_t cell = make_key(3u, 0, 0, 0, 0u);
    kith_net_conn_t *conn = nullptr;
    kith_gateway_session_t *s = nullptr;
    int client_fd = -1;
    CHECK(make_session(&fx, &cell, 1u, &conn, &client_fd, &s) == 0);

    // Give the view set content: the subscriber's own artifact (the
    // composer locates the bound actor in the covered cell) plus one other
    // actor, so the session pass composes and delivers replication frames.
    kith_sim_artifact_key_t sk = {0};
    sk.zone = cell.zone;
    sk.cell_x = cell.cell_x;
    sk.cell_y = cell.cell_y;
    sk.cell_z = cell.cell_z;
    sk.lod = cell.lod;
    sk.authority_epoch = 1u;
    kith_sim_actor_t self_actor = {0};
    self_actor.id = 100u;
    CHECK(kith_sim_publish_artifact(fx.sim, &sk, &self_actor, nullptr) == 0);
    kith_sim_actor_t other = {0};
    other.id = 7u;
    other.pos_x = 1LL << KITH_SIM_FIX_SHIFT;
    CHECK(kith_sim_publish_artifact(fx.sim, &sk, &other, nullptr) == 0);
    CHECK(kith_fabric_publish(fx.fabric, &cell, 1u, nullptr) == 0);
    CHECK(kith_gateway_tick(fx.gw, 100u) == 0);
    CHECK(kith_net_conn_write(conn) == 0);
    // The first tick delivered the composed set (entry events + state
    // records); read and discard it so the ordering read below starts
    // clean.
    peer_set_timeout(client_fd, 0, 200000);
    uint8_t discard[512];
    for (;;)
    {
        uint16_t type = 0u;
        uint32_t len = 0u;
        if (read_frame(client_fd, &type, &len, discard, sizeof(discard)) != 0)
        {
            break;
        }
    }

    // A content change makes the next pass recompose and deliver; the
    // broadcast queued before that pass must precede its frames. The
    // artifact key's authority epoch rides the cell product's epoch, so
    // the re-publish steps both.
    kith_sim_actor_t moved = other;
    moved.pos_x = 2LL << KITH_SIM_FIX_SHIFT;
    kith_sim_artifact_key_t sk_next = sk;
    sk_next.authority_epoch = 2u;
    CHECK(kith_sim_publish_artifact(fx.sim, &sk_next, &moved, nullptr) == 0);
    CHECK(kith_fabric_publish(fx.fabric, &cell, 2u, nullptr) == 0);
    static const uint8_t text[] = "ahead of state";
    CHECK(kith_gateway_broadcast_cell(fx.gw, &cell, event_type_a, text, sizeof(text)) == 0);
    CHECK(kith_gateway_tick(fx.gw, 300u) == 0);
    CHECK(kith_net_conn_write(conn) == 0);

    uint16_t type = 0u;
    uint32_t len = 0u;
    // Room for the composed pass's 68-byte replication records, not just
    // the broadcast's event payload.
    uint8_t got[96] = {0};
    peer_set_timeout(client_fd, 5, 0);
    CHECK(read_frame(client_fd, &type, &len, got, sizeof(got)) == 0);
    CHECK(type == event_type_a);
    CHECK(len == sizeof(text));
    CHECK(memcmp(got, text, sizeof(text)) == 0);
    CHECK(read_frame(client_fd, &type, &len, got, sizeof(got)) == 0);
    CHECK(type == KITH_PROTO_TYPE_USER_BASE);

    session_teardown(s, conn, client_fd);
    fixture_fini(&fx);
    return failures;
}

// A covering session whose connection queue is at its high watermark loses
// its copy (counted), and the fanout still completes for the other covering
// session. A broadcast to a cell no session covers completes silently.
static int test_broadcast_recipient_drop_counted(void)
{
    int failures = 0;
    struct fixture fx;
    CHECK(fixture_init(&fx) == 0);

    kith_fabric_cell_key_t cell = make_key(4u, 0, 0, 0, 0u);
    kith_fabric_cell_key_t vacant = make_key(4u, 9, 9, 9, 0u);

    kith_net_conn_t *conn_a = nullptr;
    kith_gateway_session_t *s_a = nullptr;
    int fd_a = -1;
    CHECK(make_session(&fx, &cell, 1u, &conn_a, &fd_a, &s_a) == 0);
    kith_net_conn_t *conn_b = nullptr;
    kith_gateway_session_t *s_b = nullptr;
    int fd_b = -1;
    CHECK(make_session(&fx, &cell, 1u, &conn_b, &fd_b, &s_b) == 0);

    // Saturate the first session's output queue from the calling thread.
    static const uint8_t filler[] = {1u, 2u, 3u, 4u};
    int rc = 0;
    do
    {
        rc = kith_gateway_deliver_frame(fx.gw, conn_a, event_type_a, filler, sizeof(filler));
    } while (rc == 0);
    CHECK(rc == kith_error_return(KITH_EAGAIN));

    static const uint8_t text[] = "drops counted";
    CHECK(kith_gateway_broadcast_cell(fx.gw, &cell, event_type_a, text, sizeof(text)) == 0);
    // A second request scoped to a cell nobody covers: the fanout is a
    // no-op, not a drop.
    CHECK(kith_gateway_broadcast_cell(fx.gw, &vacant, event_type_a, text, sizeof(text)) == 0);
    CHECK(kith_gateway_tick(fx.gw, 100u) == 0);
    CHECK(kith_net_conn_write(conn_b) == 0);

    uint64_t drops = 0u;
    uint64_t refusals = 0u;
    CHECK(kith_gateway_broadcast_drops(fx.gw, &drops) == 0);
    CHECK(kith_gateway_broadcast_refusals(fx.gw, &refusals) == 0);
    CHECK(drops == 1u);
    CHECK(refusals == 0u);

    // The saturated session's copy is gone; the unsaturated one's copy
    // arrived.
    peer_set_timeout(fd_a, 0, 200000);
    uint8_t probe[4] = {0};
    CHECK(read_full(fd_a, probe, sizeof(probe)) != 0);
    uint16_t type = 0u;
    uint32_t len = 0u;
    uint8_t got[64] = {0};
    CHECK(read_frame(fd_b, &type, &len, got, sizeof(got)) == 0);
    CHECK(type == event_type_a);
    CHECK(len == sizeof(text));
    CHECK(memcmp(got, text, sizeof(text)) == 0);

    session_teardown(s_a, conn_a, fd_a);
    session_teardown(s_b, conn_b, fd_b);
    fixture_fini(&fx);
    return failures;
}

// Gateway teardown releases every queued payload through the handle's
// allocator: a tally allocator shows zero outstanding blocks after a
// destroy with requests still queued. The teardown's drop counting shares
// the recipient-drop increment; the freeze that makes a late submit refuse
// with -KITH_ESTATE sits inside the destroy sequence and has no
// observably-safe external probe, so it stays a code-reviewed contract.
static int test_broadcast_teardown_releases_payloads(void)
{
    int failures = 0;
    broadcast_tally_reset();

    struct fixture fx;
    CHECK(fixture_init_alloc(&fx, &g_tally_allocator) == 0);

    kith_fabric_cell_key_t cell = make_key(5u, 0, 0, 0, 0u);
    kith_net_conn_t *conn = nullptr;
    kith_gateway_session_t *s = nullptr;
    int client_fd = -1;
    CHECK(make_session(&fx, &cell, 1u, &conn, &client_fd, &s) == 0);
    static const uint8_t body[] = {0xAu, 0xBu};
    CHECK(kith_gateway_broadcast_cell(fx.gw, &cell, event_type_a, body, sizeof(body)) == 0);
    CHECK(kith_gateway_broadcast_cell(fx.gw, &cell, event_type_a, nullptr, 0u) == 0);
    const unsigned payload_blocks = g_broadcast_tally.outstanding;
    CHECK(payload_blocks > 0u);

    // Destroy with both requests pending: the payloads release through the
    // tally allocator and every accepted request counts as a drop.
    session_teardown(s, conn, client_fd);
    fixture_fini(&fx);
    CHECK(g_broadcast_tally.outstanding == 0u);
    CHECK(g_broadcast_tally.frees >= payload_blocks);

    return failures;
}

/*---------------------------------------------------------------------------
 * submit-vs-drain race
 *-------------------------------------------------------------------------*/

struct broadcast_storm_ctx
{
    kith_gateway_t *gateway;
    const kith_fabric_cell_key_t *cell;
    _Atomic unsigned stop;
    _Atomic unsigned attempts;
    _Atomic unsigned refusals;
    uint8_t body[8];
};

static void *broadcast_storm(void *arg)
{
    struct broadcast_storm_ctx *ctx = arg;
    while (atomic_load_explicit(&ctx->stop, memory_order_relaxed) == 0u)
    {
        atomic_fetch_add_explicit(&ctx->attempts, 1u, memory_order_relaxed);
        const int rc = kith_gateway_broadcast_cell(
            ctx->gateway, ctx->cell, event_type_a, ctx->body, sizeof(ctx->body));
        if (rc != 0)
        {
            atomic_fetch_add_explicit(&ctx->refusals, 1u, memory_order_relaxed);
        }
    }
    return nullptr;
}

static int
run_ticks(kith_gateway_t *gw, kith_net_conn_t *conn, unsigned count, unsigned tick_budget_us)
{
    int failures = 0;
    for (unsigned tick = 0u; tick < count; ++tick)
    {
        CHECK(kith_gateway_tick(gw, tick_budget_us) == 0);
        CHECK(kith_net_conn_write(conn) == 0);
    }
    return failures;
}

// Submits from a worker thread race the reactor's drains: every attempt is
// accounted exactly once as either a delivered frame or a counted refusal.
// The interleaving is timing-dependent by construction — the assertion is
// the accounting identity, not a delivery count.
static int test_broadcast_submit_drain_race(void)
{
    int failures = 0;
    struct fixture fx;
    CHECK(fixture_init(&fx) == 0);

    kith_fabric_cell_key_t cell = make_key(6u, 0, 0, 0, 0u);
    kith_net_conn_t *conn = nullptr;
    kith_gateway_session_t *s = nullptr;
    int client_fd = -1;
    CHECK(make_session(&fx, &cell, 1u, &conn, &client_fd, &s) == 0);

    struct broadcast_storm_ctx ctx;
    memset(&ctx, 0, sizeof(ctx));
    ctx.gateway = fx.gw;
    ctx.cell = &cell;
    atomic_init(&ctx.stop, 0u);
    atomic_init(&ctx.attempts, 0u);
    atomic_init(&ctx.refusals, 0u);
    for (size_t i = 0u; i < sizeof(ctx.body); ++i)
    {
        ctx.body[i] = (uint8_t)(i + 1u);
    }

    pthread_t storm;
    CHECK(pthread_create(&storm, nullptr, broadcast_storm, &ctx) == 0);
    // The identity is vacuous if the storm never ran: the wait holds
    // the ticks open until the storm's first counted attempt, with the
    // bound sized to a working scheduler rather than a fast one.
    for (unsigned wait = 0u;
         atomic_load_explicit(&ctx.attempts, memory_order_relaxed) == 0u && wait < 500u;
         ++wait)
    {
        failures += run_ticks(fx.gw, conn, 1u, 100u);
    }
    failures += run_ticks(fx.gw, conn, 8u, 100u);
    atomic_store_explicit(&ctx.stop, 1u, memory_order_relaxed);
    CHECK(pthread_join(storm, nullptr) == 0);
    // The queue holds at most one full batch; a bounded number of extra
    // ticks empties it.
    failures += run_ticks(fx.gw, conn, 8u, 200u);

    unsigned delivered = 0u;
    uint16_t type = 0u;
    uint32_t len = 0u;
    uint8_t got[64] = {0};
    peer_set_timeout(client_fd, 0, 200000);
    while (read_frame(client_fd, &type, &len, got, sizeof(got)) == 0)
    {
        CHECK(type == event_type_a);
        CHECK(len == sizeof(ctx.body));
        delivered += 1u;
        if (delivered > 2000u)
        {
            break;
        }
    }
    const unsigned attempts = atomic_load_explicit(&ctx.attempts, memory_order_relaxed);
    const unsigned refusals = atomic_load_explicit(&ctx.refusals, memory_order_relaxed);
    uint64_t dropped = 0u;
    CHECK(kith_gateway_broadcast_drops(fx.gw, &dropped) == 0);
    CHECK(attempts > 0u);
    CHECK((uint64_t)delivered + refusals + dropped == attempts);

    session_teardown(s, conn, client_fd);
    fixture_fini(&fx);
    return failures;
}

int main(void)
{
    (void)signal(SIGPIPE, SIG_IGN);
    int rc = 0;
    rc |= test_broadcast_submit_validation();
    rc |= test_broadcast_refused_when_full();
    rc |= test_broadcast_fans_out_to_window();
    rc |= test_broadcast_fifo_order();
    rc |= test_broadcast_precedes_composed();
    rc |= test_broadcast_recipient_drop_counted();
    rc |= test_broadcast_teardown_releases_payloads();
    rc |= test_broadcast_submit_drain_race();
    if (rc != 0)
    {
        (void)fprintf(stderr, "gateway broadcast tests FAILED\n");
    }
    return rc;
}
