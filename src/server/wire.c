/* Net-to-gateway bridge for the server: registers the net listener with the
 * reactor, tracks accepted connections, decodes inbound frames, and
 * dispatches them through the gateway. Owned by the server handle (server.c);
 * created and torn down by wiring.c. */

#include "server/wire.h"

#include <stdint.h>
#include <stdlib.h>
#include <time.h>

#include "kith/types.h"

/*---------------------------------------------------------------------------
 * forward declarations
 *-------------------------------------------------------------------------*/

static void server_wire_listener_handler(int fd, unsigned int events, void *ctx);
static void server_wire_conn_handler(int fd, unsigned int events, void *ctx);
static void
server_wire_on_message(kith_net_conn_t *conn, const kith_proto_frame_t *frame, void *ctx);
static void server_wire_conn_close(struct server_wire_conn *wc);

/*---------------------------------------------------------------------------
 * timing
 *-------------------------------------------------------------------------*/

/** Monotonic nanosecond timestamp for the write-drain bracket. Mirrors the
 *  gateway's and reactor's CLOCK_MONOTONIC read (a vDSO call on Linux), so
 *  bracketing each readiness-driven kith_net_conn_write stays cheap on the
 *  reactor thread. */
static uint64_t server_wire_now_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

/*---------------------------------------------------------------------------
 * create / destroy
 *-------------------------------------------------------------------------*/

[[nodiscard]] int server_wire_create(struct server_wire *wire,
                                     const kith_allocator_t *alloc,
                                     kith_reactor_t *reactor,
                                     kith_net_t *net,
                                     kith_gateway_t *gateway)
{
    // The allocator is stored before any allocation: every routed site
    // below reads it through the driver.
    wire->allocator = alloc;
    wire->reactor = reactor;
    wire->net = net;
    wire->gateway = gateway;
    wire->conns = nullptr;
    wire->conn_count = 0u;
    wire->conn_cap = 0u;
    wire->free_wc = nullptr;
    wire->listener_fd = -1;
    atomic_store_explicit(&wire->write_ns_total, 0u, memory_order_relaxed);

    wire->conns = kith_alloc_zero(wire->allocator, 16u, sizeof(*wire->conns));
    if (!wire->conns)
    {
        return kith_error_return(KITH_ENOMEM);
    }
    wire->conn_cap = 16u;

    int lfd = kith_net_listener_fd(net);
    if (lfd < 0)
    {
        kith_free(wire->allocator, wire->conns);
        wire->conns = nullptr;
        wire->conn_cap = 0u;
        return kith_error_return(KITH_ESTATE);
    }
    int rc = kith_reactor_add(reactor, lfd, KITH_REACTOR_IN, server_wire_listener_handler, wire);
    if (rc != 0)
    {
        kith_free(wire->allocator, wire->conns);
        wire->conns = nullptr;
        wire->conn_cap = 0u;
        return rc;
    }
    wire->listener_fd = lfd;
    return 0;
}

void server_wire_destroy(struct server_wire *wire)
{
    if (wire->listener_fd >= 0)
    {
        (void)kith_reactor_del(wire->reactor, wire->listener_fd);
        wire->listener_fd = -1;
    }
    if (!wire->conns)
    {
        return;
    }
    for (size_t i = 0u; i < wire->conn_count; ++i)
    {
        struct server_wire_conn *wc = wire->conns[i];
        if (wc && !wc->closed)
        {
            wc->wire = nullptr;
            server_wire_conn_close(wc);
        }
        kith_free(wire->allocator, wc);
    }
    kith_free(wire->allocator, wire->conns);
    wire->conns = nullptr;
    wire->conn_count = 0u;
    wire->conn_cap = 0u;
    wire->free_wc = nullptr;
}

/*---------------------------------------------------------------------------
 * connection table
 *-------------------------------------------------------------------------*/

static struct server_wire_conn *server_wire_conn_alloc(struct server_wire *wire,
                                                       kith_net_conn_t *conn,
                                                       kith_gateway_session_t *session,
                                                       int fd)
{
    // A closed slot serves the new connection: the table only grows when no
    // closed slot remains, so its size tracks peak concurrency rather than
    // the cumulative accept count.
    struct server_wire_conn *wc = wire->free_wc;
    if (wc != nullptr)
    {
        wire->free_wc = wc->next_free;
        wc->next_free = nullptr;
        wc->wire = wire;
        wc->conn = conn;
        wc->session = session;
        wc->fd = fd;
        atomic_store_explicit(&wc->closed, false, memory_order_release);
        return wc;
    }
    if (wire->conn_count == wire->conn_cap)
    {
        size_t new_cap = wire->conn_cap * 2u;
        _Atomic(struct server_wire_conn *) *buf =
            kith_realloc(wire->allocator, wire->conns, new_cap * sizeof(*buf));
        if (!buf)
        {
            return nullptr;
        }
        wire->conns = buf;
        wire->conn_cap = new_cap;
    }
    wc = kith_alloc_zero(wire->allocator, 1, sizeof(*wc));
    if (!wc)
    {
        return nullptr;
    }
    wc->wire = wire;
    wc->conn = conn;
    wc->session = session;
    wc->fd = fd;
    atomic_store_explicit(&wc->closed, false, memory_order_release);
    atomic_store_explicit(&wire->conns[wire->conn_count], wc, memory_order_relaxed);
    atomic_fetch_add_explicit(&wire->conn_count, 1u, memory_order_release);
    return wc;
}

static void server_wire_conn_close(struct server_wire_conn *wc)
{
    if (!wc || wc->closed)
    {
        return;
    }
    if (wc->fd >= 0 && wc->wire)
    {
        (void)kith_reactor_del(wc->wire->reactor, wc->fd);
    }
    wc->fd = -1;
    if (wc->session)
    {
        kith_gateway_session_destroy(wc->session);
        wc->session = nullptr;
    }
    if (wc->conn)
    {
        kith_net_conn_close(wc->conn);
        kith_net_conn_release(wc->conn);
        wc->conn = nullptr;
    }
    // The slot returns to the reuse list. Destroy-driven closes skip this:
    // their caller nulls wire first and frees the entry right after.
    if (wc->wire)
    {
        wc->next_free = wc->wire->free_wc;
        wc->wire->free_wc = wc;
    }
    // The release store is last: an observer's acquire load that sees the
    // close also sees every teardown write above.
    atomic_store_explicit(&wc->closed, true, memory_order_release);
}

/*---------------------------------------------------------------------------
 * accept
 *-------------------------------------------------------------------------*/

static void server_wire_handle_accept(struct server_wire *wire)
{
    for (;;)
    {
        kith_net_conn_t *conn = nullptr;
        int rc = kith_net_accept(wire->net, &conn);
        if (rc == kith_error_return(KITH_EAGAIN))
        {
            return;
        }
        if (rc != 0)
        {
            // A full table (EBUSY) or accept failure: stop draining this
            // batch; the next readable event retries.
            return;
        }
        kith_gateway_session_t *session = nullptr;
        rc = kith_gateway_session_create(
            wire->gateway, conn, KITH_GATEWAY_SESSION_SUBSCRIBER, 0u, nullptr, &session);
        if (rc != 0)
        {
            kith_net_conn_close(conn);
            kith_net_conn_release(conn);
            continue;
        }
        int fd = kith_net_conn_fd(conn);
        struct server_wire_conn *wc = server_wire_conn_alloc(wire, conn, session, fd);
        if (!wc)
        {
            kith_gateway_session_destroy(session);
            kith_net_conn_close(conn);
            kith_net_conn_release(conn);
            continue;
        }
        // The connection's initial reference (refcount 1 from accept) is the
        // driver's; the session acquired its own. Register for level-triggered
        // IN; the per-tick arm pass adds OUT while output is queued.
        (void)kith_reactor_add(wire->reactor, fd, KITH_REACTOR_IN, server_wire_conn_handler, wc);
    }
}

/*---------------------------------------------------------------------------
 * per-connection read / dispatch / write
 *-------------------------------------------------------------------------*/

static void
server_wire_on_message(kith_net_conn_t *conn, const kith_proto_frame_t *frame, void *ctx)
{
    struct server_wire_conn *wc = ctx;
    if (!wc || !wc->wire || wc->closed)
    {
        return;
    }
    (void)kith_gateway_dispatch(wc->wire->gateway, conn, frame);
}

static void server_wire_conn_handler(int fd, unsigned int events, void *ctx)
{
    (void)fd;
    struct server_wire_conn *wc = ctx;
    if (!wc || !wc->wire || wc->closed)
    {
        return;
    }
    struct server_wire *wire = wc->wire;

    if ((events & (KITH_REACTOR_HUP | KITH_REACTOR_ERR)) != 0u)
    {
        server_wire_conn_close(wc);
        return;
    }

    if ((events & KITH_REACTOR_IN) != 0u)
    {
        int rc = kith_net_conn_read(wc->conn, server_wire_on_message, wc);
        if (rc == kith_error_return(KITH_ECONNRESET) || rc == kith_error_return(KITH_EIO) ||
            rc == kith_error_return(KITH_EPROTO))
        {
            server_wire_conn_close(wc);
            return;
        }
    }

    if (wc->closed)
    {
        return;
    }

    if ((events & KITH_REACTOR_OUT) != 0u)
    {
        uint64_t write_start = server_wire_now_ns();
        int rc = kith_net_conn_write(wc->conn);
        atomic_fetch_add_explicit(
            &wire->write_ns_total, server_wire_now_ns() - write_start, memory_order_relaxed);
        if (rc == kith_error_return(KITH_ECONNRESET) || rc == kith_error_return(KITH_EIO))
        {
            server_wire_conn_close(wc);
            return;
        }
    }

    // Re-arm to match the connection's desired events (adds OUT while output
    // is queued after a delivery tick; drops OUT once the queue drains).
    unsigned int want = KITH_REACTOR_IN;
    if ((kith_net_conn_events(wc->conn) & KITH_NET_OUT) != 0u)
    {
        want |= KITH_REACTOR_OUT;
    }
    (void)kith_reactor_mod(wire->reactor, wc->fd, want);
}

static void server_wire_listener_handler(int fd, unsigned int events, void *ctx)
{
    (void)fd;
    (void)events;
    struct server_wire *wire = ctx;
    if (!wire || !wire->conns)
    {
        return;
    }
    server_wire_handle_accept(wire);
}

/*---------------------------------------------------------------------------
 * per-tick arm
 *-------------------------------------------------------------------------*/

void server_wire_arm(struct server_wire *wire)
{
    if (!wire || !wire->conns)
    {
        return;
    }
    for (size_t i = 0u; i < wire->conn_count; ++i)
    {
        struct server_wire_conn *wc = wire->conns[i];
        if (!wc || wc->closed || wc->fd < 0)
        {
            continue;
        }
        unsigned int want = KITH_REACTOR_IN;
        if ((kith_net_conn_events(wc->conn) & KITH_NET_OUT) != 0u)
        {
            want |= KITH_REACTOR_OUT;
        }
        (void)kith_reactor_mod(wire->reactor, wc->fd, want);
    }
}
