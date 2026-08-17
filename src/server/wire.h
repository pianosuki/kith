#pragma once

#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>

#include "kith/gateway/gateway.h"
#include "kith/net/net.h"
#include "kith/reactor/reactor.h"
#include "kith/types.h"

/**
 * Wire driver: wires the transport listener and per-connection read/write to the
 * reactor and the gateway, so an inbound frame is decoded and dispatched to the
 * gateway's handler table and a bound session receives its replication stream.
 *
 * The driver is reactor-driven: the listener fd is registered once at create
 * time, and each accepted connection is registered for level-triggered IN; the
 * per-tick arm pass adds OUT while a connection's output queue is non-empty
 * (replication frames enqueued by the gateway's per-tick delivery). The gateway
 * owns the session table (keyed by connection); the driver holds the session
 * pointer it created per connection so it can destroy it on close.
 *
 * All driver entry points run on the reactor thread (the listener and connection
 * readiness callbacks) or are called from the composition root before the reactor
 * starts (create) or after it stops (destroy). The one cross-thread surface is
 * observation: the slot table and each slot's closed flag are atomically
 * qualified so an observer thread can poll them without a data race; every write
 * stays on the reactor thread. The structures here are not visible outside the
 * server library.
 */

/** One accepted wire connection and its bound gateway session. */
struct server_wire_conn
{
    /** Owning driver (nulled on destroy so a stale reactor callback no-ops). */
    struct server_wire *wire;
    /** Gateway session bound to this connection (destroyed on close). */
    kith_gateway_session_t *session;
    /** The transport connection (released on close). */
    kith_net_conn_t *conn;
    /** Cached socket fd (read once at accept; -1 after close). */
    int fd;
    /** True once the connection has been closed and its context is stale.
     *  The accept-path reset and the close are release stores on the
     *  reactor thread: an observer's acquire load that sees the flag also
     *  sees the slot's initialized (reset) or torn-down (close) state. */
    _Atomic bool closed;
    /** Next closed slot in the driver's reuse list (meaningful only while
     *  closed). */
    struct server_wire_conn *next_free;
};

/** The wire driver: listener + connection table bound to a reactor. */
struct server_wire
{
    /** Allocator the owning server was created with; set once at create
     *  before any allocation, and used for the connection table's whole
     *  lifetime (create, runtime growth, teardown). Borrowed: the server
     *  handle outlives the driver. */
    const kith_allocator_t *allocator;
    kith_reactor_t *reactor;
    kith_net_t *net;
    kith_gateway_t *gateway;
    /** Connection slots; a slot is allocated once and reused for every
     *  accept that lands on it, so the table holds one entry per slot ever
     *  used (bounded by peak concurrency). Closed entries stay here with
     *  closed set until their slot is reused or the driver is destroyed, so
     *  a stale reactor dispatch always finds a no-op or a live context.
     *  Slots are stored relaxed on the reactor thread; the release add to
     *  conn_count publishes a newly stored slot to an observer thread. */
    _Atomic(struct server_wire_conn *) *conns;
    /** Published with a release add on the reactor thread when a slot is
     *  appended: an acquire load observing the new count also sees that
     *  slot and everything initialized before it. Slot reuse keeps the
     *  count unchanged, so a reused slot's per-cycle publication rides the
     *  slot's closed flag. */
    _Atomic size_t conn_count;
    size_t conn_cap;
    /** Closed slots available for reuse by an accept. */
    struct server_wire_conn *free_wc;
    /** Listener fd, deregistered on destroy. -1 when not registered. */
    int listener_fd;
    /** Monotonic cumulative nanoseconds the reactor thread spent draining
     *  connection output queues to their sockets (the kith_net_conn_write
     *  call site in the connection readiness handler). The gateway phase
     *  counters stop at the in-memory enqueue; this measures the transport
     *  drain that runs outside them on the same thread. Written with
     *  relaxed atomic adds on the reactor thread; the composition root
     *  records the per-tick delta as kith_net_write_ns_total. */
    _Atomic uint64_t write_ns_total;
};

/** Build the driver, register the transport listener with @p reactor, and
 *  return the handle. @p alloc is stored on the driver and funds the
 *  connection table and every slot across the driver's lifetime. Returns 0
 *  on success, negative kith_error on failure. The caller destroys the
 *  driver with server_wire_destroy after the reactor stops. */
[[nodiscard]] int server_wire_create(struct server_wire *wire,
                                     const kith_allocator_t *alloc,
                                     kith_reactor_t *reactor,
                                     kith_net_t *net,
                                     kith_gateway_t *gateway);

/** Close every connection, deregister the listener, and free the connection
 *  table. Passing a driver whose create never succeeded is safe. */
void server_wire_destroy(struct server_wire *wire);

/** Re-arm each connection's reactor registration to match its desired event
 *  bits after a tick's delivery pass. Called on the reactor thread once per
 *  tick after the gateway delivers replication frames. */
void server_wire_arm(struct server_wire *wire);
