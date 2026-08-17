/* Connection write-drain accounting for the wire driver: the connection
 * readiness handler accumulates the nanoseconds spent inside
 * kith_net_conn_write into the driver's write_ns_total counter. Drives a
 * real loopback connection through a running reactor (accept, output-queue
 * enqueue, OUT re-arm, writev drain) and asserts the counter stays zero
 * until a drain happens and advances once one does. */

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
#include "kith/reactor/reactor.h"
#include "kith/sim/sim.h"
#include "kith/types.h"
#include "kith/version.h"
#include "server/wire.h"

#include <netinet/in.h>
#include <sys/socket.h>

static int check_cond(bool ok, int line)
{
    if (!ok)
    {
        (void)fprintf(stderr, "server wire write: assertion at line %d failed\n", line);
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
    kith_reactor_t *reactor;
};

static int fixture_init(struct fixture *fx)
{
    memset(fx, 0, sizeof(*fx));
    if (kith_proto_create(nullptr, nullptr, &fx->proto) != 0)
    {
        return -1;
    }
    // The client sends one frame of this type to trigger the read event that
    // re-arms OUT; decode rejects an unregistered type id and closes the
    // connection instead.
    if (kith_proto_register_type_id(fx->proto, "test.write_drain", 1u) != 0)
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
    if (kith_reactor_create(nullptr, nullptr, &fx->reactor) != 0)
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
    kith_reactor_destroy(fx->reactor);
    kith_gateway_destroy(fx->gw);
    kith_fabric_destroy(fx->fabric);
    kith_sim_destroy(fx->sim);
    kith_net_destroy(fx->net);
    kith_proto_destroy(fx->proto);
}

struct reactor_arg
{
    kith_reactor_t *reactor;
};

static void *reactor_thread_fn(void *raw)
{
    struct reactor_arg *arg = (struct reactor_arg *)raw;
    (void)kith_reactor_run(arg->reactor);
    return nullptr;
}

// Poll the drain counter until it reaches @p min_ns or @p ms milliseconds
// elapse, so the reactor thread has room to deliver the events without
// busy-spinning the test thread.
static bool wait_for_ns_total(struct server_wire *wire, uint64_t min_ns, unsigned int ms)
{
    for (unsigned int elapsed = 0u; elapsed < ms; elapsed += 2u)
    {
        if (atomic_load_explicit(&wire->write_ns_total, memory_order_relaxed) >= min_ns)
        {
            return true;
        }
        struct timespec ts = {.tv_sec = 0, .tv_nsec = 2L * 1000 * 1000};
        (void)nanosleep(&ts, nullptr);
    }
    return false;
}

// Wait for the driver to accept one connection. The acquire load of the
// count pairs with the driver's release add: observing the slot also
// observes its initialized state, so a true return orders the caller's
// enqueue after the accept path.
static bool wait_accepted(struct server_wire *wire)
{
    for (unsigned int i = 0u; i < 1000u; ++i)
    {
        if (atomic_load_explicit(&wire->conn_count, memory_order_acquire) == 1u &&
            atomic_load_explicit(&wire->conns[0], memory_order_relaxed) != nullptr &&
            !atomic_load_explicit(&wire->conns[0]->closed, memory_order_relaxed))
        {
            return true;
        }
        struct timespec ts = {.tv_sec = 0, .tv_nsec = 2L * 1000 * 1000};
        (void)nanosleep(&ts, nullptr);
    }
    return false;
}

static int test_write_drain_counter(void)
{
    int failures = 0;
    struct fixture fx;
    CHECK(fixture_init(&fx) == 0);

    struct server_wire wire;
    memset(&wire, 0, sizeof(wire));
    CHECK(server_wire_create(&wire, nullptr, fx.reactor, fx.net, fx.gw) == 0);
    CHECK(atomic_load_explicit(&wire.write_ns_total, memory_order_relaxed) == 0u);

    struct reactor_arg arg = {.reactor = fx.reactor};
    pthread_t tid;
    int prc = pthread_create(&tid, nullptr, reactor_thread_fn, &arg);
    CHECK(prc == 0);

    // Connect and wait for the driver to accept the connection.
    int client_fd = socket(AF_INET, SOCK_STREAM, 0);
    CHECK(client_fd >= 0);
    if (client_fd >= 0)
    {
        struct sockaddr_in addr = {0};
        socklen_t addr_len = sizeof(addr);
        CHECK(getsockname(kith_net_listener_fd(fx.net), (struct sockaddr *)&addr, &addr_len) == 0);
        CHECK(connect(client_fd, (struct sockaddr *)&addr, addr_len) == 0);
        bool accepted = wait_accepted(&wire);
        CHECK(accepted);

        if (accepted)
        {
            // Enqueue output on the accepted connection. Nothing drains until
            // the readiness handler sees OUT, which only the handler's own
            // re-arm pass adds (after any event on this fd).
            kith_net_frame_t *frame = kith_net_frame_create(fx.net, 16u);
            CHECK(frame != nullptr);
            if (frame != nullptr)
            {
                (void)memset(kith_net_frame_data(frame), 0xAB, 16u);
                kith_net_frame_set_len(frame, 16u);
                CHECK(kith_net_conn_enqueue(wire.conns[0]->conn, frame) == 0);
                kith_net_frame_release(frame);
            }

            // Send one valid frame from the client so the read event fires,
            // the handler re-arms OUT (output is queued), and the next OUT
            // readiness drains the queue through kith_net_conn_write.
            uint8_t payload[8] = {0};
            size_t frame_len =
                kith_proto_encode(fx.proto, 1u, 0u, 0u, payload, sizeof(payload), nullptr, 0u);
            CHECK(frame_len > 0u);
            uint8_t buf[64] = {0};
            CHECK(frame_len <= sizeof(buf));
            size_t written =
                kith_proto_encode(fx.proto, 1u, 0u, 0u, payload, sizeof(payload), buf, sizeof(buf));
            CHECK(written == frame_len);
            CHECK((size_t)send(client_fd, buf, written, 0) == written);

            CHECK(wait_for_ns_total(&wire, 1u, 2000u));
            CHECK(!wire.conns[0]->closed);
        }
        (void)close(client_fd);
    }

    kith_reactor_stop(fx.reactor);
    if (prc == 0)
    {
        (void)pthread_join(tid, nullptr);
    }

    server_wire_destroy(&wire);
    fixture_fini(&fx);
    return failures;
}

int main(void)
{
    int rc = 0;
    rc |= test_write_drain_counter();
    if (rc != 0)
    {
        (void)fprintf(stderr, "server wire write tests FAILED\n");
    }
    return rc;
}
