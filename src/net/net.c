/* Public handle and listener for the net module: params resolution, create
 * and destroy, the listening socket, accept, and the connection table. Owns
 * the frame pool (frame.c) and the per-connection state (conn.c, ringbuf.c);
 * the public contract is include/kith/net/net.h. */

#include "kith/net/net.h"

#include <errno.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include <netdb.h>
#include <unistd.h>

#include "conn.h"
#include "frame.h"
#include "kith/proto/proto.h"
#include "kith/types.h"
#include "kith/version.h"

#include <netinet/in.h>
#include <sys/socket.h>

struct kith_net
{
    const kith_allocator_t *allocator;
    const kith_proto_t *proto;
    int listen_fd;
    bool listening;
    uint32_t listen_backlog;

    struct kith_conn_cfg conn_cfg;
    uint32_t next_conn_id;

    /* Connection table (swap-remove, dense array of pointers). */
    struct kith_net_conn **conns;
    uint32_t conn_count;
    uint32_t conn_cap;

    /* Drain calls the per-call cap truncated with output still queued.
     * Incremented on the write-calling thread; read by the stats accessor. */
    _Atomic uint64_t write_deferrals_total;

    /* Connections closed because a frame's declared total exceeded the
     * input ring ceiling (rb_max). Incremented on the reactor thread at
     * close time; read by the stats accessor. With the proto decode
     * rejection counter it reconciles the total malformed input a run
     * injected against what the two planes rejected. */
    _Atomic uint64_t rejections;

    struct kith_frame_pool frame_pool;
};

static int resolve_cfg_field(uint32_t value, unsigned int fallback)
{
    return (value == 0u) ? (int)fallback : (int)value;
}

static void build_conn_cfg(const kith_net_params_t *params, struct kith_conn_cfg *out)
{
    out->rb_initial = (uint32_t)resolve_cfg_field(
        (params != nullptr) ? params->read_buffer_initial : 0u, KITH_NET_DEFAULT_READ_BUF_INITIAL);
    out->rb_max = (uint32_t)resolve_cfg_field((params != nullptr) ? params->read_buffer_max : 0u,
                                              KITH_NET_DEFAULT_READ_BUF_MAX);
    out->in_high_water = (uint32_t)resolve_cfg_field(
        (params != nullptr) ? params->in_high_water : 0u, KITH_NET_DEFAULT_IN_HIGH_WATER);
    out->in_low_water = (uint32_t)resolve_cfg_field((params != nullptr) ? params->in_low_water : 0u,
                                                    KITH_NET_DEFAULT_IN_LOW_WATER);
    out->out_high_water = (uint32_t)resolve_cfg_field(
        (params != nullptr) ? params->out_high_water : 0u, KITH_NET_DEFAULT_OUT_HIGH_WATER);
    out->out_low_water = (uint32_t)resolve_cfg_field(
        (params != nullptr) ? params->out_low_water : 0u, KITH_NET_DEFAULT_OUT_LOW_WATER);
    /* UINT32_MAX passes through as "cap disabled"; only 0 selects the
     * default, so the disable value survives resolution. */
    out->out_drain_cap = ((params != nullptr && params->out_drain_cap != 0u)
                              ? params->out_drain_cap
                              : (uint32_t)KITH_NET_DEFAULT_OUT_DRAIN_CAP);
}

int kith_net_create(const kith_net_params_t *params,
                    const kith_proto_t *proto,
                    const kith_allocator_t *alloc,
                    kith_net_t **out_net)
{
    if (out_net == nullptr || proto == nullptr)
    {
        return kith_error_return(KITH_EINVAL);
    }
    *out_net = nullptr;

    if (params != nullptr)
    {
        if (params->abi_version != KITH_ABI_VERSION)
        {
            return kith_error_return(KITH_EABIVER);
        }
        if (params->size < sizeof(*params))
        {
            return kith_error_return(KITH_ESIZE);
        }
        if (params->read_buffer_max != 0u && params->read_buffer_max < KITH_NET_MIN_READ_BUF_MAX)
        {
            return kith_error_return(KITH_EINVAL);
        }
    }
    if (alloc != nullptr)
    {
        const kith_error_t alloc_rc = kith_allocator_check(alloc);
        if (alloc_rc != 0)
        {
            return kith_error_return(alloc_rc);
        }
    }

    const uint32_t max_conns = (uint32_t)resolve_cfg_field(
        (params != nullptr) ? params->max_connections : 0u, KITH_NET_DEFAULT_MAX_CONNECTIONS);

    const kith_allocator_t *allocator = alloc != nullptr ? alloc : kith_allocator_default();
    kith_net_t *net = kith_alloc(allocator, sizeof(*net));
    if (net == nullptr)
    {
        return kith_error_return(KITH_ENOMEM);
    }
    net->allocator = allocator;
    net->proto = proto;
    net->listen_fd = -1;
    net->listening = false;
    net->listen_backlog =
        (uint32_t)resolve_cfg_field((params != nullptr) ? params->listen_backlog : 0u, SOMAXCONN);
    net->next_conn_id = 1u;
    net->conn_count = 0u;
    net->conn_cap = max_conns;
    atomic_init(&net->write_deferrals_total, 0u);
    atomic_init(&net->rejections, 0u);
    net->conns = kith_alloc_zero(allocator, max_conns, sizeof(*net->conns));
    if (net->conns == nullptr)
    {
        kith_free(allocator, net);
        return kith_error_return(KITH_ENOMEM);
    }
    build_conn_cfg(params, &net->conn_cfg);

    if (kith_frame_pool_init(&net->frame_pool, allocator) != 0)
    {
        kith_free(allocator, net->conns);
        kith_free(allocator, net);
        return kith_error_return(KITH_ESTATE);
    }

    *out_net = net;
    return 0;
}

void kith_net_destroy(kith_net_t *net)
{
    if (net == nullptr)
    {
        return;
    }
    if (net->listen_fd >= 0)
    {
        (void)close(net->listen_fd);
        net->listen_fd = -1;
    }
    /* Sweep from the tail: each close removes the last table index, so no
     * removal swaps another connection into a slot the loop has already
     * passed. Close forces the fd closed and clears the table_index so the
     * table_remove path is not re-entered; then release drops the
     * transport's reference. */
    for (uint32_t i = net->conn_count; i > 0u; --i)
    {
        struct kith_net_conn *c = net->conns[i - 1u];
        if (c != nullptr)
        {
            kith_net_conn_close(c);
            kith_net_conn_release(c);
            net->conns[i - 1u] = nullptr;
        }
    }
    kith_free(net->allocator, net->conns);
    kith_frame_pool_destroy(&net->frame_pool);
    kith_free(net->allocator, net);
}

int kith_net_listener_fd(const kith_net_t *net)
{
    if (net == nullptr)
    {
        return -1;
    }
    return net->listen_fd;
}

const kith_proto_t *kith_net_proto(const kith_net_t *net)
{
    if (net == nullptr)
    {
        return nullptr;
    }
    return net->proto;
}

const kith_allocator_t *kith_net_allocator(const kith_net_t *net)
{
    if (net == nullptr)
    {
        return nullptr;
    }
    return net->allocator;
}

/* True when @p host asks for every interface (the embedded-server
 * default). Such a listen binds one socket that must serve every family
 * equally, so it never substitutes a narrower family after a bind
 * conflict. */
static bool listen_host_is_wildcard(const char *host)
{
    return host == nullptr || host[0] == '\0' || strcmp(host, "*") == 0;
}

/* Create a non-blocking, close-on-exec listening socket on @p host:@p port.
 *
 * A wildcard host binds one dual-stack socket (AF_INET6 with IPV6_V6ONLY
 * off, falling back to the IPv4 wildcard only on an IPv6-less system).
 * The v6only option is what makes the bind conflict with a stale listener
 * on either family, so its failure fails the listen. An explicit host
 * walks its own resolved candidates in order. Returns the fd on success,
 * -1 on failure (errno set). */
static int make_listener(const char *host, uint16_t port, int backlog)
{
    char port_str[16];
    (void)snprintf(port_str, sizeof(port_str), "%u", (unsigned int)port);

    struct addrinfo hints;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;

    struct addrinfo *res = nullptr;
    int gai = getaddrinfo(host, port_str, &hints, &res);
    if (gai != 0)
    {
        return -1;
    }

    const bool wildcard = listen_host_is_wildcard(host);
    struct addrinfo *chosen = nullptr;
    if (wildcard)
    {
        for (struct addrinfo *ai = res; ai != nullptr; ai = ai->ai_next)
        {
            if (ai->ai_family == AF_INET6)
            {
                chosen = ai;
                break;
            }
        }
        if (chosen == nullptr)
        {
            chosen = res;
        }
    }

    int fd = -1;
    for (struct addrinfo *ai = chosen != nullptr ? chosen : res; ai != nullptr;
         ai = wildcard ? nullptr : ai->ai_next)
    {
        fd = socket(ai->ai_family, ai->ai_socktype | SOCK_NONBLOCK | SOCK_CLOEXEC, ai->ai_protocol);
        if (fd < 0)
        {
            continue;
        }

        int opt = 1;
        if (ai->ai_family == AF_INET6)
        {
            int v6only = wildcard ? 0 : 1;
            if (setsockopt(fd, IPPROTO_IPV6, IPV6_V6ONLY, &v6only, sizeof(v6only)) != 0)
            {
                (void)close(fd);
                fd = -1;
                break;
            }
        }
        (void)setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

        if (bind(fd, ai->ai_addr, ai->ai_addrlen) == 0 && listen(fd, backlog) == 0)
        {
            break;
        }
        (void)close(fd);
        fd = -1;
    }
    /* The addrinfo chain is owned by the system resolver, not by the
     * transport's allocator. */
    freeaddrinfo(res);
    return fd;
}

int kith_net_listen(kith_net_t *net, const char *host, uint16_t port)
{
    if (net == nullptr)
    {
        return kith_error_return(KITH_EINVAL);
    }
    if (net->listening)
    {
        return kith_error_return(KITH_ESTATE);
    }

    int backlog = (int)net->listen_backlog;
    int fd = make_listener(host, port, backlog);
    if (fd < 0)
    {
        return kith_error_return(KITH_EIO);
    }

    net->listen_fd = fd;
    net->listening = true;
    return 0;
}

void kith_net_table_remove(kith_net_t *net, uint32_t index)
{
    if (net == nullptr || index >= net->conn_count)
    {
        return;
    }
    const uint32_t last = net->conn_count - 1u;
    if (index != last)
    {
        net->conns[index] = net->conns[last];
        net->conns[index]->table_index = index;
    }
    net->conns[last] = nullptr;
    net->conn_count--;
}

void kith_net_count_write_deferral(kith_net_t *net)
{
    if (net == nullptr)
    {
        return;
    }
    atomic_fetch_add_explicit(&net->write_deferrals_total, 1u, memory_order_relaxed);
}

int kith_net_write_deferrals(const kith_net_t *net, uint64_t *out_deferrals)
{
    if (net == nullptr || out_deferrals == nullptr)
    {
        return kith_error_return(KITH_EINVAL);
    }
    *out_deferrals = atomic_load_explicit(&net->write_deferrals_total, memory_order_relaxed);
    return 0;
}

int kith_net_rejections(const kith_net_t *net, uint64_t *out_rejections)
{
    if (net == nullptr || out_rejections == nullptr)
    {
        return kith_error_return(KITH_EINVAL);
    }
    *out_rejections = atomic_load_explicit(&net->rejections, memory_order_relaxed);
    return 0;
}

void kith_net_count_rejection(kith_net_t *net)
{
    if (net == nullptr)
    {
        return;
    }
    atomic_fetch_add_explicit(&net->rejections, 1u, memory_order_relaxed);
}

int kith_net_accept(kith_net_t *net, kith_net_conn_t **out_conn)
{
    if (net == nullptr || out_conn == nullptr)
    {
        return kith_error_return(KITH_EINVAL);
    }
    *out_conn = nullptr;
    if (!net->listening)
    {
        return kith_error_return(KITH_ESTATE);
    }

    int fd = accept4(net->listen_fd, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
    if (fd < 0)
    {
        if (errno == EAGAIN || errno == EWOULDBLOCK)
        {
            return kith_error_return(KITH_EAGAIN);
        }
        if (errno == EINTR)
        {
            return kith_error_return(KITH_EAGAIN);
        }
        return kith_error_return(KITH_EIO);
    }

    if (net->conn_count >= net->conn_cap)
    {
        (void)close(fd);
        return kith_error_return(KITH_EBUSY);
    }

    uint32_t conn_id = net->next_conn_id++;
    uint32_t index = net->conn_count;
    struct kith_net_conn *c = kith_conn_create(net, fd, conn_id, &net->conn_cfg);
    if (c == nullptr)
    {
        (void)close(fd);
        return kith_error_return(KITH_ENOMEM);
    }
    c->table_index = index;
    net->conns[index] = c;
    net->conn_count++;

    *out_conn = c;
    return 0;
}

kith_net_frame_t *kith_net_frame_create(kith_net_t *net, uint32_t len)
{
    if (net == nullptr)
    {
        return nullptr;
    }
    return kith_frame_create(&net->frame_pool, len);
}

void kith_net_frame_release(kith_net_frame_t *frame)
{
    kith_frame_release(frame);
}
