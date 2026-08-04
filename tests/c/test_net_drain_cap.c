/* Per-call output drain cap for the transport: kith_net_conn_write stops
 * once the bytes written this call reach the transport's out_drain_cap (the
 * first writev always executes), leaving any residual queued for the next
 * call, and counts each cap-truncated call on the transport's deferral
 * counter. Kernel-backpressure stops (EAGAIN) and cap stops that empty the
 * queue exactly are distinct conditions and stay uncounted. Drives real
 * loopback connections with pinned socket-buffer sizes so the pipe capacity
 * sits strictly between the cap and the backlog, separating the three stop
 * regimes deterministically. */

#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include <unistd.h>

#include "kith/net/net.h"
#include "kith/proto/proto.h"
#include "kith/types.h"
#include "kith/version.h"

#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/time.h>

static int check_cond(bool ok, int line)
{
    if (!ok)
    {
        (void)fprintf(stderr, "net drain cap: assertion at line %d failed\n", line);
        return 1;
    }
    return 0;
}

#define CHECK(cond) failures += check_cond((cond), __LINE__)

/* Millisecond sleep, for the accept poll loop. */
static void sleep_ms(unsigned int ms)
{
    struct timeval tv = {.tv_sec = (time_t)(ms / 1000u),
                         .tv_usec = (suseconds_t)((ms % 1000u) * 1000u)};
    (void)select(0, nullptr, nullptr, nullptr, &tv);
}

/* One loopback connection through a fresh transport: listener + connected
 * client + accepted server-side connection. @p client_rcvbuf sets the
 * client's receive buffer (0 = kernel default) BEFORE the connect, so the
 * pipe capacity is pinned from the first byte. */
struct drain_fixture
{
    kith_proto_t *proto;
    kith_net_t *net;
    int client_fd;
    kith_net_conn_t *conn;
};

static void drain_fixture_destroy(struct drain_fixture *fx);

static int
drain_fixture_init(struct drain_fixture *fx, const kith_net_params_t *params, int client_rcvbuf)
{
    memset(fx, 0, sizeof(*fx));
    fx->client_fd = -1;
    if (kith_proto_create(nullptr, nullptr, &fx->proto) != 0)
    {
        return -1;
    }
    if (kith_net_create(params, fx->proto, nullptr, &fx->net) != 0)
    {
        return -1;
    }
    if (kith_net_listen(fx->net, "127.0.0.1", 0) != 0)
    {
        return -1;
    }

    fx->client_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fx->client_fd < 0)
    {
        return -1;
    }
    if (client_rcvbuf > 0)
    {
        (void)setsockopt(
            fx->client_fd, SOL_SOCKET, SO_RCVBUF, &client_rcvbuf, sizeof(client_rcvbuf));
    }

    struct sockaddr_in addr = {0};
    socklen_t addr_len = sizeof(addr);
    if (getsockname(kith_net_listener_fd(fx->net), (struct sockaddr *)&addr, &addr_len) != 0)
    {
        return -1;
    }
    if (connect(fx->client_fd, (struct sockaddr *)&addr, sizeof(addr)) != 0)
    {
        return -1;
    }
    for (unsigned int i = 0u; i < 1000u && fx->conn == nullptr; ++i)
    {
        int rc = kith_net_accept(fx->net, &fx->conn);
        if (rc == 0)
        {
            break;
        }
        if (rc != kith_error_return(KITH_EAGAIN))
        {
            return -1;
        }
        sleep_ms(2u);
    }
    return (fx->conn != nullptr) ? 0 : -1;
}

static void drain_fixture_destroy(struct drain_fixture *fx)
{
    /* Close first (drops the connection from the transport's table), then
     * release the accepted reference — mirroring the wire driver's teardown
     * order, since the table holds its own reference until close. */
    kith_net_conn_close(fx->conn);
    kith_net_conn_release(fx->conn);
    kith_net_destroy(fx->net);
    kith_proto_destroy(fx->proto);
    if (fx->client_fd >= 0)
    {
        (void)close(fx->client_fd);
        fx->client_fd = -1;
    }
}

/* Enqueue @p len bytes as one frame on the connection. */
static int enqueue_bytes(kith_net_conn_t *conn, kith_net_t *net, uint32_t len, uint8_t fill)
{
    kith_net_frame_t *frame = kith_net_frame_create(net, len);
    if (frame == nullptr)
    {
        return -1;
    }
    (void)memset(kith_net_frame_data(frame), (int)fill, len);
    kith_net_frame_set_len(frame, len);
    int rc = kith_net_conn_enqueue(conn, frame);
    kith_net_frame_release(frame);
    return rc;
}

static uint64_t deferrals(const kith_net_t *net)
{
    uint64_t n = 0u;
    if (kith_net_write_deferrals(net, &n) != 0)
    {
        return UINT64_MAX;
    }
    return n;
}

static bool out_pending(const kith_net_conn_t *conn)
{
    return (kith_net_conn_events(conn) & KITH_NET_OUT) != 0u;
}

/* Size a socket buffer; the kernel doubles the value. */
static void set_sockbuf(int fd, int optname, int bytes)
{
    (void)setsockopt(fd, SOL_SOCKET, optname, &bytes, sizeof(bytes));
}

/* Default params: a single full-budget batch frame (32,772 B) is below the
 * 64 KiB default cap, so one write call drains it fully and counts no
 * deferral — the cap binds only past two queued batches. */
static int test_default_cap_leaves_single_batch_untouched(void)
{
    int failures = 0;
    struct drain_fixture fx;
    CHECK(drain_fixture_init(&fx, nullptr, 0) == 0);
    CHECK(enqueue_bytes(fx.conn, fx.net, 32u * 1024u + 4u, 0xAB) == 0);
    CHECK(out_pending(fx.conn));
    CHECK(kith_net_conn_write(fx.conn) == 0);
    CHECK(!out_pending(fx.conn));
    CHECK(deferrals(fx.net) == 0u);
    drain_fixture_destroy(&fx);
    return failures;
}

/* Pure cap stop: pipe capacity well above the cap, backlog well above the
 * pipe. The first write call reaches the cap with output still queued and
 * without the kernel ever returning EAGAIN — one deferral, residual still
 * queued. Repeated calls then drain fully; only the truncated calls count. */
static int test_cap_truncation_counts_and_paces(void)
{
    int failures = 0;
    kith_net_params_t params = {0};
    params.size = sizeof(params);
    params.abi_version = KITH_ABI_VERSION;
    params.out_high_water = 4u * 1024u * 1024u; /* hold a 512 KiB backlog */
    params.out_drain_cap = 8u * 1024u;

    struct drain_fixture fx;
    CHECK(drain_fixture_init(&fx, &params, 64 * 1024) == 0);
    set_sockbuf(kith_net_conn_fd(fx.conn), SO_SNDBUF, 64 * 1024);

    for (int i = 0; i < 128; i++)
    {
        CHECK(enqueue_bytes(fx.conn, fx.net, 4u * 1024u, (uint8_t)i) == 0);
    }
    CHECK(out_pending(fx.conn));
    CHECK(deferrals(fx.net) == 0u);

    /* First call: the pipe (>= 128 KiB) accepts far more than the 8 KiB cap
     * before the kernel is full, so the stop is the cap's, not EAGAIN's. */
    CHECK(kith_net_conn_write(fx.conn) == 0);
    CHECK(out_pending(fx.conn));
    const uint64_t first = deferrals(fx.net);
    CHECK(first == 1u);

    /* The client reads while the server keeps writing: every call makes
     * progress, truncated calls keep counting, and the queue empties. */
    uint8_t sink[16u * 1024u];
    uint64_t last = first;
    for (int round = 0; round < 4000 && out_pending(fx.conn); round++)
    {
        (void)recv(fx.client_fd, sink, sizeof(sink), 0);
        CHECK(kith_net_conn_write(fx.conn) == 0);
        const uint64_t now = deferrals(fx.net);
        CHECK(now >= last);
        last = now;
    }
    CHECK(!out_pending(fx.conn));
    CHECK(last >= 1u);

    /* An empty queue counts nothing further. */
    CHECK(kith_net_conn_write(fx.conn) == 0);
    CHECK(deferrals(fx.net) == last);
    drain_fixture_destroy(&fx);
    return failures;
}

/* Pure kernel-bound stop: the pipe (tiny sndbuf) fills below the cap, so
 * the call ends on EAGAIN with fewer bytes written than the cap allows —
 * a distinct condition that counts no deferral, however often it repeats. */
static int test_eagain_stop_is_not_counted(void)
{
    int failures = 0;
    kith_net_params_t params = {0};
    params.size = sizeof(params);
    params.abi_version = KITH_ABI_VERSION;
    params.out_high_water = 4u * 1024u * 1024u;
    params.out_drain_cap = 512u * 1024u;

    struct drain_fixture fx;
    CHECK(drain_fixture_init(&fx, &params, 4 * 1024) == 0);
    set_sockbuf(kith_net_conn_fd(fx.conn), SO_SNDBUF, 4 * 1024);

    for (int i = 0; i < 64; i++)
    {
        CHECK(enqueue_bytes(fx.conn, fx.net, 4u * 1024u, (uint8_t)i) == 0);
    }
    CHECK(out_pending(fx.conn));

    /* The client never reads: the pipe fills on the first call and every
     * call ends on EAGAIN, under the 512 KiB cap, counting nothing. */
    for (int round = 0; round < 4; round++)
    {
        CHECK(kith_net_conn_write(fx.conn) == 0);
        CHECK(deferrals(fx.net) == 0u);
        CHECK(out_pending(fx.conn));
    }
    drain_fixture_destroy(&fx);
    return failures;
}

/* A cap stop that empties the queue exactly paced nothing: one frame sized
 * to the cap drains fully in the first (ungated) writev, and the counter
 * stays at zero. */
static int test_cap_stop_with_empty_queue_not_counted(void)
{
    int failures = 0;
    kith_net_params_t params = {0};
    params.size = sizeof(params);
    params.abi_version = KITH_ABI_VERSION;
    params.out_drain_cap = 64u * 1024u;

    struct drain_fixture fx;
    CHECK(drain_fixture_init(&fx, &params, 512 * 1024) == 0);
    set_sockbuf(kith_net_conn_fd(fx.conn), SO_SNDBUF, 512 * 1024);

    CHECK(enqueue_bytes(fx.conn, fx.net, 64u * 1024u, 0xCD) == 0);
    CHECK(out_pending(fx.conn));
    CHECK(kith_net_conn_write(fx.conn) == 0);
    CHECK(!out_pending(fx.conn));
    CHECK(deferrals(fx.net) == 0u);
    drain_fixture_destroy(&fx);
    return failures;
}

/* UINT32_MAX disables the cap: a multi-batch backlog drains within one
 * call (the pipe holds it all) and no deferral is ever counted. */
static int test_disabled_cap_drains_unbounded(void)
{
    int failures = 0;
    kith_net_params_t params = {0};
    params.size = sizeof(params);
    params.abi_version = KITH_ABI_VERSION;
    params.out_high_water = 4u * 1024u * 1024u;
    params.out_drain_cap = UINT32_MAX;

    struct drain_fixture fx;
    CHECK(drain_fixture_init(&fx, &params, 1024 * 1024) == 0);
    set_sockbuf(kith_net_conn_fd(fx.conn), SO_SNDBUF, 1024 * 1024);

    for (int i = 0; i < 256; i++)
    {
        CHECK(enqueue_bytes(fx.conn, fx.net, 4u * 1024u, (uint8_t)i) == 0);
    }
    CHECK(out_pending(fx.conn));
    CHECK(kith_net_conn_write(fx.conn) == 0);
    CHECK(!out_pending(fx.conn));
    CHECK(deferrals(fx.net) == 0u);
    drain_fixture_destroy(&fx);
    return failures;
}

/* Accessor contract: NULL handles are rejected, and a fresh transport
 * reads zero. */
static int test_accessor_contract(void)
{
    int failures = 0;
    uint64_t out = 0u;
    CHECK(kith_net_write_deferrals(nullptr, &out) == kith_error_return(KITH_EINVAL));

    struct drain_fixture fx;
    CHECK(drain_fixture_init(&fx, nullptr, 0) == 0);
    CHECK(kith_net_write_deferrals(fx.net, nullptr) == kith_error_return(KITH_EINVAL));
    CHECK(kith_net_write_deferrals(fx.net, &out) == 0);
    CHECK(out == 0u);
    drain_fixture_destroy(&fx);
    return failures;
}

int main(void)
{
    int rc = 0;
    rc |= test_default_cap_leaves_single_batch_untouched();
    rc |= test_cap_truncation_counts_and_paces();
    rc |= test_eagain_stop_is_not_counted();
    rc |= test_cap_stop_with_empty_queue_not_counted();
    rc |= test_disabled_cap_drains_unbounded();
    rc |= test_accessor_contract();
    if (rc != 0)
    {
        (void)fprintf(stderr, "net drain cap tests FAILED\n");
    }
    return rc;
}
