#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <pthread.h>
#include <unistd.h>

#include "kith/net/net.h"
#include "kith/proto/proto.h"
#include "kith/types.h"
#include "kith/version.h"

#include <netinet/in.h>
#include <sys/socket.h>

static int check_cond(bool ok, int line)
{
    if (!ok)
    {
        (void)fprintf(stderr, "net transport: assertion at line %d failed\n", line);
        return 1;
    }
    return 0;
}

#define CHECK(cond) failures += check_cond((cond), __LINE__)

/*---------------------------------------------------------------------------
 * create / params validation
 *-------------------------------------------------------------------------*/

static int test_create_validation(void)
{
    int failures = 0;
    kith_proto_t *proto = nullptr;
    CHECK(kith_proto_create(nullptr, nullptr, &proto) == 0);

    CHECK(kith_net_create(nullptr, nullptr, nullptr, nullptr) == kith_error_return(KITH_EINVAL));
    CHECK(kith_net_create(nullptr, proto, nullptr, nullptr) == kith_error_return(KITH_EINVAL));

    kith_net_params_t bad_abi = {
        .size = sizeof(kith_net_params_t),
        .abi_version = KITH_ABI_VERSION + 1u,
    };
    kith_net_t *net = nullptr;
    CHECK(kith_net_create(&bad_abi, proto, nullptr, &net) == kith_error_return(KITH_EABIVER));
    CHECK(net == nullptr);

    kith_net_params_t small = {
        .size = 8u,
        .abi_version = KITH_ABI_VERSION,
    };
    CHECK(kith_net_create(&small, proto, nullptr, &net) == kith_error_return(KITH_ESIZE));
    CHECK(net == nullptr);

    /* An explicit read_buffer_max below the read-chunk floor is rejected:
     * a smaller ceiling cannot hold even one read chunk. The floor
     * value itself is accepted. */
    kith_net_params_t tiny_max = {
        .size = sizeof(kith_net_params_t),
        .abi_version = KITH_ABI_VERSION,
        .read_buffer_max = 1024u,
    };
    CHECK(kith_net_create(&tiny_max, proto, nullptr, &net) == kith_error_return(KITH_EINVAL));
    CHECK(net == nullptr);
    kith_net_params_t floor_max = {
        .size = sizeof(kith_net_params_t),
        .abi_version = KITH_ABI_VERSION,
        .read_buffer_max = KITH_NET_MIN_READ_BUF_MAX,
    };
    CHECK(kith_net_create(&floor_max, proto, nullptr, &net) == 0);
    kith_net_destroy(net);
    net = nullptr;

    kith_net_destroy(nullptr);
    kith_proto_destroy(proto);
    return failures;
}

static int test_listen_and_fd(void)
{
    int failures = 0;
    kith_proto_t *proto = nullptr;
    CHECK(kith_proto_create(nullptr, nullptr, &proto) == 0);
    kith_net_t *net = nullptr;
    CHECK(kith_net_create(nullptr, proto, nullptr, &net) == 0);

    CHECK(kith_net_listener_fd(net) == -1);
    CHECK(kith_net_listen(net, "127.0.0.1", 0) == 0);
    CHECK(kith_net_listener_fd(net) >= 0);

    /* Double listen is rejected. */
    CHECK(kith_net_listen(net, "127.0.0.1", 0) == kith_error_return(KITH_ESTATE));

    /* NULL host binds to wildcard. */
    kith_net_t *net2 = nullptr;
    CHECK(kith_net_create(nullptr, proto, nullptr, &net2) == 0);
    CHECK(kith_net_listen(net2, nullptr, 0) == 0);

    kith_net_destroy(net);
    kith_net_destroy(net2);
    kith_proto_destroy(proto);
    return failures;
}

/*---------------------------------------------------------------------------
 * wildcard bind conflicts
 *-------------------------------------------------------------------------*/

static int test_listen_wildcard_conflict(void)
{
    int failures = 0;
    kith_proto_t *proto = nullptr;
    CHECK(kith_proto_create(nullptr, nullptr, &proto) == 0);

    /* Hold the port with a plain IPv4 wildcard listener — the stale-server
     * shape. A kith wildcard listen must fail loudly on either family, not
     * silently substitute the family the conflict does not cover. */
    int holder = socket(AF_INET, SOCK_STREAM, 0);
    CHECK(holder >= 0);
    struct sockaddr_in held;
    memset(&held, 0, sizeof(held));
    held.sin_family = AF_INET;
    held.sin_addr.s_addr = htonl(INADDR_ANY);
    held.sin_port = 0;
    CHECK(bind(holder, (struct sockaddr *)&held, sizeof(held)) == 0);
    CHECK(listen(holder, 8) == 0);
    socklen_t held_len = sizeof(held);
    CHECK(getsockname(holder, (struct sockaddr *)&held, &held_len) == 0);
    uint16_t port = ntohs(held.sin_port);
    CHECK(port != 0);

    kith_net_t *net = nullptr;
    CHECK(kith_net_create(nullptr, proto, nullptr, &net) == 0);
    CHECK(kith_net_listener_fd(net) == -1);
    CHECK(kith_net_listen(net, nullptr, port) == kith_error_return(KITH_EIO));
    CHECK(kith_net_listener_fd(net) == -1);
    CHECK(kith_net_listen(net, "*", port) == kith_error_return(KITH_EIO));
    CHECK(kith_net_listener_fd(net) == -1);

    /* The conflict released, the wildcard listen succeeds again — on one
     * dual-stack socket (v6only off) when the system offers IPv6, else the
     * IPv4 wildcard. */
    CHECK(close(holder) == 0);
    CHECK(kith_net_listen(net, nullptr, port) == 0);
    int fd = kith_net_listener_fd(net);
    CHECK(fd >= 0);
    struct sockaddr_storage bound;
    memset(&bound, 0, sizeof(bound));
    socklen_t bound_len = sizeof(bound);
    CHECK(getsockname(fd, (struct sockaddr *)&bound, &bound_len) == 0);
    if (bound.ss_family == AF_INET6)
    {
        int v6only = -1;
        socklen_t v6only_len = sizeof(v6only);
        CHECK(getsockopt(fd, IPPROTO_IPV6, IPV6_V6ONLY, &v6only, &v6only_len) == 0);
        CHECK(v6only == 0);
    }

    kith_net_destroy(net);
    kith_proto_destroy(proto);
    return failures;
}

/*---------------------------------------------------------------------------
 * listen_backlog param
 *-------------------------------------------------------------------------*/

static int test_listen_backlog_param(void)
{
    int failures = 0;
    kith_proto_t *proto = nullptr;
    CHECK(kith_proto_create(nullptr, nullptr, &proto) == 0);

    /* The kernel clamps backlog to /proc/sys/net/core/somaxconn, so the
     * value is not readable back from the socket. The contract is that an
     * explicit backlog is honored (listen succeeds and the socket enters the
     * listening state); 0 selects SOMAXCONN at runtime per the field docs. */
    kith_net_params_t params = {
        .size = sizeof(kith_net_params_t),
        .abi_version = KITH_ABI_VERSION,
        .listen_backlog = 16u,
    };
    kith_net_t *net = nullptr;
    CHECK(kith_net_create(&params, proto, nullptr, &net) == 0);
    CHECK(kith_net_listen(net, "127.0.0.1", 0) == 0);
    int fd = kith_net_listener_fd(net);
    CHECK(fd >= 0);
    int accepting = 0;
    socklen_t optlen = sizeof(accepting);
    CHECK(getsockopt(fd, SOL_SOCKET, SO_ACCEPTCONN, &accepting, &optlen) == 0);
    CHECK(accepting != 0);
    kith_net_destroy(net);

    kith_net_params_t zero_params = {
        .size = sizeof(kith_net_params_t),
        .abi_version = KITH_ABI_VERSION,
        .listen_backlog = 0u,
    };
    kith_net_t *net2 = nullptr;
    CHECK(kith_net_create(&zero_params, proto, nullptr, &net2) == 0);
    CHECK(kith_net_listen(net2, "127.0.0.1", 0) == 0);
    int fd2 = kith_net_listener_fd(net2);
    CHECK(fd2 >= 0);
    int accepting2 = 0;
    socklen_t optlen2 = sizeof(accepting2);
    CHECK(getsockopt(fd2, SOL_SOCKET, SO_ACCEPTCONN, &accepting2, &optlen2) == 0);
    CHECK(accepting2 != 0);
    kith_net_destroy(net2);

    kith_proto_destroy(proto);
    return failures;
}

/*---------------------------------------------------------------------------
 * read / write loopback
 *-------------------------------------------------------------------------*/

/* A captured frame delivered to the read callback. */
struct captured
{
    kith_proto_frame_t frame;
    bool received;
};

static void on_message_captured(kith_net_conn_t *conn, const kith_proto_frame_t *frame, void *ctx)
{
    struct captured *cap = (struct captured *)ctx;
    cap->frame = *frame;
    cap->received = true;
    (void)conn;
}

struct client_args
{
    int fd;
    const uint8_t *bytes;
    size_t len;
    int rc;
};

static void *client_thread(void *arg)
{
    struct client_args *a = (struct client_args *)arg;
    size_t off = 0u;
    while (off < a->len)
    {
        ssize_t n = send(a->fd, a->bytes + off, a->len - off, 0);
        if (n <= 0)
        {
            a->rc = -1;
            return nullptr;
        }
        off += (size_t)n;
    }
    a->rc = 0;
    return nullptr;
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

static int test_accept_read_frame(void)
{
    int failures = 0;
    kith_proto_t *proto = nullptr;
    CHECK(kith_proto_create(nullptr, nullptr, &proto) == 0);
    CHECK(kith_proto_register_type_id(proto, "ping", 1000u) == 0);
    kith_net_t *net = nullptr;
    CHECK(kith_net_create(nullptr, proto, nullptr, &net) == 0);
    CHECK(kith_net_listen(net, "127.0.0.1", 0) == 0);

    int client_fd = connect_to_listener(net);
    CHECK(client_fd >= 0);

    /* Encode a frame and send it from a client thread. */
    uint8_t wire[64];
    const uint8_t payload[] = {0x01u, 0x02u, 0x03u};
    const size_t n = kith_proto_encode(proto, 1000u, 0u, 0ull, payload, 3u, wire, sizeof(wire));
    CHECK(n == KITH_PROTO_HDR_SIZE + 3u);

    struct client_args args = {.fd = client_fd, .bytes = wire, .len = n, .rc = -99};
    pthread_t tid;
    CHECK(pthread_create(&tid, nullptr, client_thread, &args) == 0);

    /* Accept the server-side connection. */
    kith_net_conn_t *conn = nullptr;
    int accept_rc = kith_net_accept(net, &conn);
    CHECK(accept_rc == 0);
    CHECK(conn != nullptr);
    CHECK(kith_net_conn_fd(conn) >= 0);

    /* Read the frame; the callback captures the decoded view. The client
     * thread sends asynchronously, so loop on read until a frame arrives or
     * the peer closes. */
    struct captured cap = {.received = false};
    struct timespec ts = {.tv_sec = 0, .tv_nsec = 5L * 1000 * 1000};
    for (int i = 0; i < 100 && !cap.received; ++i)
    {
        int read_rc = kith_net_conn_read(conn, on_message_captured, &cap);
        if (read_rc != 0)
        {
            CHECK(read_rc == 0);
            break;
        }
        if (!cap.received)
        {
            nanosleep(&ts, nullptr);
        }
    }
    CHECK(cap.received);
    CHECK(cap.frame.type_id == 1000u);
    CHECK(cap.frame.payload_len == 3u);
    if (cap.frame.payload_len == 3u && cap.frame.payload != nullptr)
    {
        CHECK(memcmp(cap.frame.payload, payload, 3u) == 0);
    }

    (void)pthread_join(tid, nullptr);
    (void)close(client_fd);
    kith_net_conn_close(conn);
    kith_net_conn_release(conn);
    kith_net_destroy(net);
    kith_proto_destroy(proto);
    return failures;
}

/* Send a frame split across two TCP segments to exercise the EAGAIN partial-
 * read path (the ring buffer accumulates, decode returns EAGAIN, then the
 * second read completes the frame). */
static int test_partial_read(void)
{
    int failures = 0;
    kith_proto_t *proto = nullptr;
    CHECK(kith_proto_create(nullptr, nullptr, &proto) == 0);
    CHECK(kith_proto_register_type_id(proto, "move", 1001u) == 0);
    kith_net_t *net = nullptr;
    CHECK(kith_net_create(nullptr, proto, nullptr, &net) == 0);
    CHECK(kith_net_listen(net, "127.0.0.1", 0) == 0);

    int client_fd = connect_to_listener(net);
    CHECK(client_fd >= 0);

    uint8_t wire[64];
    const size_t n = kith_proto_encode(proto, 1001u, 0u, 0ull, nullptr, 0u, wire, sizeof(wire));
    CHECK(n == KITH_PROTO_HDR_SIZE);

    /* Send the header in two halves with a delay between them. */
    CHECK(send(client_fd, wire, 4u, 0) == 4);
    struct timespec ts = {.tv_sec = 0, .tv_nsec = 20L * 1000 * 1000};
    nanosleep(&ts, nullptr);
    CHECK(send(client_fd, wire + 4u, n - 4u, 0) == (ssize_t)(n - 4u));

    kith_net_conn_t *conn = nullptr;
    CHECK(kith_net_accept(net, &conn) == 0);

    struct captured cap = {.received = false};
    /* First read sees 4 bytes (incomplete header) → EAGAIN internally, no
     * callback. The call returns 0 (socket drained). */
    int rc = kith_net_conn_read(conn, on_message_captured, &cap);
    CHECK(rc == 0);
    /* The frame may already be complete if both segments were coalesced; read
     * again to drain the second segment if not. */
    if (!cap.received)
    {
        nanosleep(&ts, nullptr);
        rc = kith_net_conn_read(conn, on_message_captured, &cap);
        CHECK(rc == 0);
    }
    CHECK(cap.received);
    CHECK(cap.frame.type_id == 1001u);
    CHECK(cap.frame.payload_len == 0u);

    (void)close(client_fd);
    kith_net_conn_close(conn);
    kith_net_conn_release(conn);
    kith_net_destroy(net);
    kith_proto_destroy(proto);
    return failures;
}

/*---------------------------------------------------------------------------
 * wrapped-frame gather
 *-------------------------------------------------------------------------*/

/* Deterministic ring positioning: the ring is 8192 bytes, so the positioning
 * frames park head at known offsets and the wrapped frames straddle the
 * physical end. Every phase is fully consumed before the next send — the recv
 * loop drains the socket before any frame is consumed, so overlapping sends
 * overflow the ring and close the connection. */
#define GATHER_RING_SIZE 8192u

static uint8_t pattern_byte(size_t i)
{
    return (uint8_t)(i * 31u + 7u);
}

static int send_all(int fd, const uint8_t *bytes, size_t len)
{
    size_t off = 0u;
    while (off < len)
    {
        ssize_t n = send(fd, bytes + off, len - off, 0);
        if (n <= 0)
        {
            return -1;
        }
        off += (size_t)n;
    }
    return 0;
}

/* A captured delivery. The payload comparison runs inside the callback: the
 * view is valid only for the callback's duration, so comparing after the read
 * returns reads freed staging on the gather paths. */
struct gather_ctx
{
    const uint8_t *expect;
    size_t expect_len;
    uint16_t target_type;
    size_t delivered;
    bool payload_ok;
};

static void on_message_gather(kith_net_conn_t *conn, const kith_proto_frame_t *frame, void *ctx)
{
    struct gather_ctx *g = (struct gather_ctx *)ctx;
    g->delivered += 1u;
    if (frame->type_id == g->target_type)
    {
        g->payload_ok = frame->payload_len == g->expect_len &&
                        memcmp(frame->payload, g->expect, g->expect_len) == 0;
    }
    (void)conn;
}

static int read_until_delivered(kith_net_conn_t *conn, void *ctx, size_t target)
{
    struct timespec ts = {.tv_sec = 0, .tv_nsec = 5L * 1000 * 1000};
    for (int i = 0; i < 200; ++i)
    {
        int rc = kith_net_conn_read(conn, on_message_gather, ctx);
        if (rc != 0)
        {
            return -1;
        }
        if (((struct gather_ctx *)ctx)->delivered >= target)
        {
            return 0;
        }
        nanosleep(&ts, nullptr);
    }
    return -1;
}

/* Encode one frame carrying @p payload and send it on @p fd. */
static int gather_send_frame(kith_proto_t *proto,
                             int fd,
                             uint16_t type_id,
                             const uint8_t *payload,
                             size_t payload_len,
                             uint8_t *wire,
                             size_t wire_cap)
{
    size_t len =
        kith_proto_encode(proto, type_id, 0u, 0ull, payload, (uint32_t)payload_len, wire, wire_cap);
    if (len != wire_cap || send_all(fd, wire, len) != 0)
    {
        return -1;
    }
    return 0;
}

/* Park head at 7368 with one fully consumed positioning frame, then deliver a
 * 910-byte frame that wraps 824+86 with total_used above the 256-byte stack
 * scratch — the heap gather branch. */
static int test_wrapped_frame_heap_gather(void)
{
    int failures = 0;
    kith_proto_t *proto = nullptr;
    CHECK(kith_proto_create(nullptr, nullptr, &proto) == 0);
    CHECK(kith_proto_register_type_id(proto, "fill", 1003u) == 0);
    CHECK(kith_proto_register_type_id(proto, "big", 1005u) == 0);

    kith_net_params_t params = {
        .size = sizeof(kith_net_params_t),
        .abi_version = KITH_ABI_VERSION,
        .read_buffer_initial = GATHER_RING_SIZE,
        .read_buffer_max = GATHER_RING_SIZE,
    };
    kith_net_t *net = nullptr;
    CHECK(kith_net_create(&params, proto, nullptr, &net) == 0);
    CHECK(kith_net_listen(net, "127.0.0.1", 0) == 0);

    int client_fd = connect_to_listener(net);
    CHECK(client_fd >= 0);

    kith_net_conn_t *conn = nullptr;
    CHECK(kith_net_accept(net, &conn) == 0);

    struct gather_ctx g = {.expect = nullptr,
                           .expect_len = 0u,
                           .target_type = 0u,
                           .delivered = 0u,
                           .payload_ok = false};

    uint8_t fill_payload[7358u];
    memset(fill_payload, 0xA5u, sizeof(fill_payload));
    uint8_t fill_wire[KITH_PROTO_HDR_SIZE + sizeof(fill_payload)];
    CHECK(gather_send_frame(proto,
                            client_fd,
                            1003u,
                            fill_payload,
                            sizeof(fill_payload),
                            fill_wire,
                            sizeof(fill_wire)) == 0);
    CHECK(read_until_delivered(conn, &g, 1u) == 0);

    uint8_t big_payload[900u];
    for (size_t i = 0; i < sizeof(big_payload); ++i)
    {
        big_payload[i] = pattern_byte(i);
    }
    uint8_t big_wire[KITH_PROTO_HDR_SIZE + sizeof(big_payload)];
    CHECK(gather_send_frame(proto,
                            client_fd,
                            1005u,
                            big_payload,
                            sizeof(big_payload),
                            big_wire,
                            sizeof(big_wire)) == 0);
    g.target_type = 1005u;
    g.expect = big_payload;
    g.expect_len = sizeof(big_payload);
    CHECK(read_until_delivered(conn, &g, 2u) == 0);
    CHECK(g.payload_ok);

    (void)close(client_fd);
    kith_net_conn_close(conn);
    kith_net_conn_release(conn);
    kith_net_destroy(net);
    kith_proto_destroy(proto);
    return failures;
}

/* Park head at 8190 with one fully consumed positioning frame, then deliver a
 * 20-byte frame that wraps 2+18 within the 256-byte stack scratch — the stack
 * gather branch. */
static int test_wrapped_frame_stack_gather(void)
{
    int failures = 0;
    kith_proto_t *proto = nullptr;
    CHECK(kith_proto_create(nullptr, nullptr, &proto) == 0);
    CHECK(kith_proto_register_type_id(proto, "fill", 1003u) == 0);
    CHECK(kith_proto_register_type_id(proto, "small", 1006u) == 0);

    kith_net_params_t params = {
        .size = sizeof(kith_net_params_t),
        .abi_version = KITH_ABI_VERSION,
        .read_buffer_initial = GATHER_RING_SIZE,
        .read_buffer_max = GATHER_RING_SIZE,
    };
    kith_net_t *net = nullptr;
    CHECK(kith_net_create(&params, proto, nullptr, &net) == 0);
    CHECK(kith_net_listen(net, "127.0.0.1", 0) == 0);

    int client_fd = connect_to_listener(net);
    CHECK(client_fd >= 0);

    kith_net_conn_t *conn = nullptr;
    CHECK(kith_net_accept(net, &conn) == 0);

    struct gather_ctx g = {.expect = nullptr,
                           .expect_len = 0u,
                           .target_type = 0u,
                           .delivered = 0u,
                           .payload_ok = false};

    uint8_t fill_payload[8180u];
    memset(fill_payload, 0x5Au, sizeof(fill_payload));
    uint8_t fill_wire[KITH_PROTO_HDR_SIZE + sizeof(fill_payload)];
    CHECK(gather_send_frame(proto,
                            client_fd,
                            1003u,
                            fill_payload,
                            sizeof(fill_payload),
                            fill_wire,
                            sizeof(fill_wire)) == 0);
    CHECK(read_until_delivered(conn, &g, 1u) == 0);

    uint8_t small_payload[10u];
    for (size_t i = 0; i < sizeof(small_payload); ++i)
    {
        small_payload[i] = pattern_byte(i);
    }
    uint8_t small_wire[KITH_PROTO_HDR_SIZE + sizeof(small_payload)];
    CHECK(gather_send_frame(proto,
                            client_fd,
                            1006u,
                            small_payload,
                            sizeof(small_payload),
                            small_wire,
                            sizeof(small_wire)) == 0);
    g.target_type = 1006u;
    g.expect = small_payload;
    g.expect_len = sizeof(small_payload);
    CHECK(read_until_delivered(conn, &g, 2u) == 0);
    CHECK(g.payload_ok);

    (void)close(client_fd);
    kith_net_conn_close(conn);
    kith_net_conn_release(conn);
    kith_net_destroy(net);
    kith_proto_destroy(proto);
    return failures;
}

/*---------------------------------------------------------------------------
 * close stops delivery
 *-------------------------------------------------------------------------*/

static void
on_message_close_first(kith_net_conn_t *conn, const kith_proto_frame_t *frame, void *ctx)
{
    size_t *count = (size_t *)ctx;
    *count += 1u;
    if (*count == 1u)
    {
        /* The policy-reject move: close from inside the callback. */
        kith_net_conn_close(conn);
    }
    (void)frame;
}

static int test_close_stops_delivery(void)
{
    int failures = 0;
    kith_proto_t *proto = nullptr;
    CHECK(kith_proto_create(nullptr, nullptr, &proto) == 0);
    CHECK(kith_proto_register_type_id(proto, "once", 1007u) == 0);
    kith_net_t *net = nullptr;
    CHECK(kith_net_create(nullptr, proto, nullptr, &net) == 0);
    CHECK(kith_net_listen(net, "127.0.0.1", 0) == 0);

    int client_fd = connect_to_listener(net);
    CHECK(client_fd >= 0);

    kith_net_conn_t *conn = nullptr;
    CHECK(kith_net_accept(net, &conn) == 0);

    /* Two frames sent together; the callback closes on the first delivery,
     * so the second buffered frame must not be delivered. */
    const uint8_t payload[4u] = {1u, 2u, 3u, 4u};
    uint8_t one_wire[KITH_PROTO_HDR_SIZE + sizeof(payload)];
    size_t one = kith_proto_encode(
        proto, 1007u, 0u, 0ull, payload, sizeof(payload), one_wire, sizeof(one_wire));
    CHECK(one == sizeof(one_wire));
    uint8_t two_wire[2u * sizeof(one_wire)];
    memcpy(two_wire, one_wire, one);
    memcpy(two_wire + one, one_wire, one);
    CHECK(send_all(client_fd, two_wire, sizeof(two_wire)) == 0);

    size_t delivered = 0u;
    struct timespec ts = {.tv_sec = 0, .tv_nsec = 5L * 1000 * 1000};
    int rc = -1;
    for (int i = 0; i < 200 && delivered == 0u; ++i)
    {
        rc = kith_net_conn_read(conn, on_message_close_first, &delivered);
        if (delivered == 0u)
        {
            CHECK(rc == 0);
            nanosleep(&ts, nullptr);
        }
    }
    CHECK(delivered == 1u);
    CHECK(rc == 0);
    CHECK(kith_net_conn_fd(conn) == -1);

    (void)close(client_fd);
    kith_net_conn_release(conn);
    kith_net_destroy(net);
    kith_proto_destroy(proto);
    return failures;
}

/*---------------------------------------------------------------------------
 * declared-total bound
 *-------------------------------------------------------------------------*/

/* Build a frame header declaring @p wire_len payload bytes with no body:
 * the declared total, not the bytes actually sent, is what the ceiling
 * bound sees. */
static size_t make_declared_header(uint8_t *out, uint16_t type_id, uint32_t wire_len)
{
    out[0] = (uint8_t)KITH_PROTO_MAGIC0;
    out[1] = (uint8_t)KITH_PROTO_MAGIC1;
    out[2] = (uint8_t)KITH_PROTO_VERSION;
    out[3] = 0u; /* flags */
    out[4] = (uint8_t)(type_id >> 8u);
    out[5] = (uint8_t)type_id;
    out[6] = (uint8_t)(wire_len >> 24u);
    out[7] = (uint8_t)(wire_len >> 16u);
    out[8] = (uint8_t)(wire_len >> 8u);
    out[9] = (uint8_t)wire_len;
    return KITH_PROTO_HDR_SIZE;
}

/* A header declaring a total beyond the read buffer ceiling is rejected at
 * header time. The proto codec's own payload bound (1 MiB) accepts the
 * header, but the frame can never complete inside the ring, so the read
 * fails with -KITH_EPROTO and the connection closes. */
static int test_oversize_header_rejected(void)
{
    int failures = 0;
    kith_proto_t *proto = nullptr;
    CHECK(kith_proto_create(nullptr, nullptr, &proto) == 0);
    CHECK(kith_proto_register_type_id(proto, "big", 1008u) == 0);

    kith_net_params_t params = {
        .size = sizeof(kith_net_params_t),
        .abi_version = KITH_ABI_VERSION,
        .read_buffer_initial = KITH_NET_MIN_READ_BUF_MAX,
        .read_buffer_max = KITH_NET_MIN_READ_BUF_MAX,
    };
    kith_net_t *net = nullptr;
    CHECK(kith_net_create(&params, proto, nullptr, &net) == 0);
    CHECK(kith_net_listen(net, "127.0.0.1", 0) == 0);

    int client_fd = connect_to_listener(net);
    CHECK(client_fd >= 0);

    kith_net_conn_t *conn = nullptr;
    CHECK(kith_net_accept(net, &conn) == 0);

    /* Declared total 9010 exceeds the 8192 ceiling and sits within the
     * codec's 1 MiB bound, so only the net layer rejects it. */
    uint8_t hdr[KITH_PROTO_HDR_SIZE];
    CHECK(make_declared_header(hdr, 1008u, 9000u) == KITH_PROTO_HDR_SIZE);
    CHECK(send_all(client_fd, hdr, sizeof(hdr)) == 0);

    uint64_t rejections = 0u;
    CHECK(kith_net_rejections(net, &rejections) == 0);
    CHECK(rejections == 0u);

    struct gather_ctx g = {.expect = nullptr,
                           .expect_len = 0u,
                           .target_type = 1008u,
                           .delivered = 0u,
                           .payload_ok = false};
    CHECK(kith_net_conn_read(conn, on_message_gather, &g) == kith_error_return(KITH_EPROTO));
    CHECK(kith_net_conn_fd(conn) == -1);
    CHECK(g.delivered == 0u);
    CHECK(kith_net_rejections(net, &rejections) == 0);
    CHECK(rejections == 1u);

    (void)close(client_fd);
    kith_net_conn_close(conn);
    kith_net_conn_release(conn);
    kith_net_destroy(net);
    kith_proto_destroy(proto);
    return failures;
}

/* A burst whose bytes exceed the ceiling within one read pass closes on
 * the ring-full failure. The read-chunk floor guarantees the leading
 * header is buffered by then, so the oversize class reaches the caller as
 * -KITH_EPROTO rather than the input-exhaustion class. */

/* Build one oversized burst: a header declaring @p type_id's payload as
 * @p len minus the header size, then pattern bytes to @p len. Returns a
 * malloc'd buffer the caller frees, or NULL on allocation. */
static uint8_t *make_oversize_burst(uint16_t type_id, size_t len)
{
    uint8_t *burst = malloc(len);
    if (burst == nullptr)
    {
        return nullptr;
    }
    if (make_declared_header(burst, type_id, (uint32_t)(len - KITH_PROTO_HDR_SIZE)) !=
        KITH_PROTO_HDR_SIZE)
    {
        free(burst);
        return nullptr;
    }
    for (size_t i = KITH_PROTO_HDR_SIZE; i < len; ++i)
    {
        burst[i] = pattern_byte(i);
    }
    return burst;
}

static int test_oversize_burst_classified_eproto(void)
{
    int failures = 0;
    kith_proto_t *proto = nullptr;
    CHECK(kith_proto_create(nullptr, nullptr, &proto) == 0);
    CHECK(kith_proto_register_type_id(proto, "big", 1009u) == 0);

    kith_net_params_t params = {
        .size = sizeof(kith_net_params_t),
        .abi_version = KITH_ABI_VERSION,
        .read_buffer_initial = 16384u,
        .read_buffer_max = 16384u,
    };
    kith_net_t *net = nullptr;
    CHECK(kith_net_create(&params, proto, nullptr, &net) == 0);
    CHECK(kith_net_listen(net, "127.0.0.1", 0) == 0);

    int client_fd = connect_to_listener(net);
    CHECK(client_fd >= 0);

    kith_net_conn_t *conn = nullptr;
    CHECK(kith_net_accept(net, &conn) == 0);

    /* 20000 bytes: a header declaring 19990 payload bytes plus the body.
     * The recv loop takes 8192-byte chunks, the ring caps at 16384, and
     * the burst exceeds both, so the pass fails with the header buffered.
     * A partially-arrived burst instead rejects at header time; either
     * path fails with -KITH_EPROTO. */
    enum
    {
        BURST_LEN = 20000
    };
    uint8_t *burst = make_oversize_burst(1009u, BURST_LEN);
    CHECK(burst != nullptr);
    if (burst == nullptr)
    {
        /* CHECK already counted the failure. */
        return failures;
    }

    struct client_args args = {.fd = client_fd, .bytes = burst, .len = BURST_LEN, .rc = -99};
    pthread_t tid;
    CHECK(pthread_create(&tid, nullptr, client_thread, &args) == 0);

    struct gather_ctx g = {.expect = nullptr,
                           .expect_len = 0u,
                           .target_type = 1009u,
                           .delivered = 0u,
                           .payload_ok = false};
    /* The read loop gives the sender time to land bytes: as soon as the
     * header is buffered the pass fails, whether at header time or on the
     * ring-full write. */
    struct timespec ts = {.tv_sec = 0, .tv_nsec = 5L * 1000 * 1000};
    int rc = 0;
    for (int i = 0; i < 200 && rc == 0; ++i)
    {
        rc = kith_net_conn_read(conn, on_message_gather, &g);
        if (rc == 0)
        {
            nanosleep(&ts, nullptr);
        }
    }
    CHECK(rc == kith_error_return(KITH_EPROTO));
    CHECK(kith_net_conn_fd(conn) == -1);
    CHECK(g.delivered == 0u);
    /* Either rejection path (header time or ring-full) closes the
     * connection exactly once and counts exactly once. */
    uint64_t rejections = 0u;
    CHECK(kith_net_rejections(net, &rejections) == 0);
    CHECK(rejections == 1u);

    (void)pthread_join(tid, nullptr);
    free(burst);
    (void)close(client_fd);
    kith_net_conn_close(conn);
    kith_net_conn_release(conn);
    kith_net_destroy(net);
    kith_proto_destroy(proto);
    return failures;
}

/* A declared frame larger than the high watermark must still complete:
 * the pause point lifts to the declared total, so IN stays requested
 * while the frame trickles in and delivery happens once the tail
 * arrives. */
static int test_frame_aware_pause_completes_large_frame(void)
{
    int failures = 0;
    kith_proto_t *proto = nullptr;
    CHECK(kith_proto_create(nullptr, nullptr, &proto) == 0);
    CHECK(kith_proto_register_type_id(proto, "big", 1005u) == 0);

    kith_net_params_t params = {
        .size = sizeof(kith_net_params_t),
        .abi_version = KITH_ABI_VERSION,
        .read_buffer_initial = 1024u,
        .read_buffer_max = 8192u,
        .in_high_water = 1024u,
        .in_low_water = 512u,
    };
    kith_net_t *net = nullptr;
    CHECK(kith_net_create(&params, proto, nullptr, &net) == 0);
    CHECK(kith_net_listen(net, "127.0.0.1", 0) == 0);

    int client_fd = connect_to_listener(net);
    CHECK(client_fd >= 0);

    kith_net_conn_t *conn = nullptr;
    CHECK(kith_net_accept(net, &conn) == 0);

    /* One declared-2048 frame (10-byte header + 2038 payload). The first
     * send alone crosses the 1024 watermark while the frame is still
     * incomplete. */
    enum
    {
        FRAME_PAYLOAD = 2038,
        PART1_PAYLOAD = 1090
    };
    uint8_t expected[FRAME_PAYLOAD];
    for (size_t i = 0; i < sizeof(expected); ++i)
    {
        expected[i] = pattern_byte(i);
    }
    uint8_t part1[KITH_PROTO_HDR_SIZE + PART1_PAYLOAD];
    CHECK(make_declared_header(part1, 1005u, FRAME_PAYLOAD) == KITH_PROTO_HDR_SIZE);
    memcpy(part1 + KITH_PROTO_HDR_SIZE, expected, PART1_PAYLOAD);
    uint8_t part2[FRAME_PAYLOAD - PART1_PAYLOAD];
    memcpy(part2, expected + PART1_PAYLOAD, sizeof(part2));

    CHECK(send_all(client_fd, part1, sizeof(part1)) == 0);

    struct gather_ctx g = {.expect = expected,
                           .expect_len = sizeof(expected),
                           .target_type = 1005u,
                           .delivered = 0u,
                           .payload_ok = false};
    struct timespec ts = {.tv_sec = 0, .tv_nsec = 5L * 1000 * 1000};
    for (int i = 0; i < 10; ++i)
    {
        /* The pause stays lifted while the oversized frame is incomplete:
         * IN remains requested at a depth above the watermark. */
        CHECK((kith_net_conn_events(conn) & KITH_NET_IN) != 0u);
        CHECK(kith_net_conn_read(conn, on_message_gather, &g) == 0);
        nanosleep(&ts, nullptr);
    }
    CHECK(g.delivered == 0u);

    CHECK(send_all(client_fd, part2, sizeof(part2)) == 0);
    for (int i = 0; i < 200 && g.delivered == 0u; ++i)
    {
        CHECK((kith_net_conn_events(conn) & KITH_NET_IN) != 0u);
        CHECK(kith_net_conn_read(conn, on_message_gather, &g) == 0);
        nanosleep(&ts, nullptr);
    }
    CHECK(g.delivered == 1u);
    CHECK(g.payload_ok);

    (void)close(client_fd);
    kith_net_conn_close(conn);
    kith_net_conn_release(conn);
    kith_net_destroy(net);
    kith_proto_destroy(proto);
    return failures;
}

/*---------------------------------------------------------------------------
 * write path
 *-------------------------------------------------------------------------*/

static int test_enqueue_and_write(void)
{
    int failures = 0;
    kith_proto_t *proto = nullptr;
    CHECK(kith_proto_create(nullptr, nullptr, &proto) == 0);
    CHECK(kith_proto_register_type_id(proto, "pong", 1002u) == 0);
    kith_net_t *net = nullptr;
    CHECK(kith_net_create(nullptr, proto, nullptr, &net) == 0);
    CHECK(kith_net_listen(net, "127.0.0.1", 0) == 0);

    int client_fd = connect_to_listener(net);
    CHECK(client_fd >= 0);

    kith_net_conn_t *conn = nullptr;
    CHECK(kith_net_accept(net, &conn) == 0);

    /* Build a frame carrying an encoded proto frame as its payload. */
    uint8_t wire[64];
    const size_t wire_len =
        kith_proto_encode(proto, 1002u, 0u, 0ull, nullptr, 0u, wire, sizeof(wire));
    CHECK(wire_len == KITH_PROTO_HDR_SIZE);

    kith_net_frame_t *f = kith_net_frame_create(net, (uint32_t)wire_len);
    CHECK(f != nullptr);
    memcpy(kith_net_frame_data(f), wire, wire_len);
    kith_net_frame_set_len(f, (uint32_t)wire_len);

    /* OUT event is set before enqueue (queue empty → not set), set after. */
    CHECK((kith_net_conn_events(conn) & KITH_NET_OUT) == 0u);
    CHECK(kith_net_conn_enqueue(conn, f) == 0);
    CHECK((kith_net_conn_events(conn) & KITH_NET_OUT) != 0u);

    /* The caller still owns its reference; release it (the queue holds its own). */
    kith_net_frame_release(f);

    CHECK(kith_net_conn_write(conn) == 0);

    /* Drain the client side. */
    uint8_t recv_buf[64];
    ssize_t got = recv(client_fd, recv_buf, sizeof(recv_buf), 0);
    CHECK(got == (ssize_t)wire_len);
    if (got == (ssize_t)wire_len)
    {
        CHECK(memcmp(recv_buf, wire, wire_len) == 0);
    }

    /* Queue drained → OUT event clears. */
    CHECK((kith_net_conn_events(conn) & KITH_NET_OUT) == 0u);

    (void)close(client_fd);
    kith_net_conn_close(conn);
    kith_net_conn_release(conn);
    kith_net_destroy(net);
    kith_proto_destroy(proto);
    return failures;
}

/*---------------------------------------------------------------------------
 * close / lifecycle
 *-------------------------------------------------------------------------*/

static int test_conn_close_and_release(void)
{
    int failures = 0;
    kith_proto_t *proto = nullptr;
    CHECK(kith_proto_create(nullptr, nullptr, &proto) == 0);
    kith_net_t *net = nullptr;
    CHECK(kith_net_create(nullptr, proto, nullptr, &net) == 0);
    CHECK(kith_net_listen(net, "127.0.0.1", 0) == 0);

    int client_fd = connect_to_listener(net);
    CHECK(client_fd >= 0);

    kith_net_conn_t *conn = nullptr;
    CHECK(kith_net_accept(net, &conn) == 0);
    CHECK(kith_net_conn_fd(conn) >= 0);

    /* Close is idempotent. */
    kith_net_conn_close(conn);
    kith_net_conn_close(conn);
    CHECK(kith_net_conn_fd(conn) == -1);

    /* Acquire/release pairing around close. */
    kith_net_conn_acquire(conn);
    kith_net_conn_release(conn);
    kith_net_conn_release(conn);

    /* NULL safety. */
    kith_net_conn_close(nullptr);
    kith_net_conn_acquire(nullptr);
    kith_net_conn_release(nullptr);
    CHECK(kith_net_conn_fd(nullptr) == -1);

    (void)close(client_fd);
    kith_net_destroy(net);
    kith_proto_destroy(proto);
    return failures;
}

static int test_accept_eagain_when_idle(void)
{
    int failures = 0;
    kith_proto_t *proto = nullptr;
    CHECK(kith_proto_create(nullptr, nullptr, &proto) == 0);
    kith_net_t *net = nullptr;
    CHECK(kith_net_create(nullptr, proto, nullptr, &net) == 0);
    CHECK(kith_net_listen(net, "127.0.0.1", 0) == 0);

    kith_net_conn_t *conn = nullptr;
    /* No pending connection → EAGAIN. */
    CHECK(kith_net_accept(net, &conn) == kith_error_return(KITH_EAGAIN));
    CHECK(conn == nullptr);

    kith_net_destroy(net);
    kith_proto_destroy(proto);
    return failures;
}

int main(void)
{
    int rc = 0;
    rc |= test_create_validation();
    rc |= test_listen_and_fd();
    rc |= test_listen_wildcard_conflict();
    rc |= test_listen_backlog_param();
    rc |= test_accept_read_frame();
    rc |= test_partial_read();
    rc |= test_wrapped_frame_heap_gather();
    rc |= test_wrapped_frame_stack_gather();
    rc |= test_close_stops_delivery();
    rc |= test_oversize_header_rejected();
    rc |= test_oversize_burst_classified_eproto();
    rc |= test_frame_aware_pause_completes_large_frame();
    rc |= test_enqueue_and_write();
    rc |= test_conn_close_and_release();
    rc |= test_accept_eagain_when_idle();
    if (rc != 0)
    {
        (void)fprintf(stderr, "net transport tests FAILED\n");
    }
    return rc;
}
