/* Connection-table reuse under churn for the wire driver: a closed slot is
 * recycled for the next accept, so the table holds one entry per slot ever
 * used (bounded by peak concurrency) instead of one per cumulative accept.
 * Drives 64 connect/disconnect cycles through a running reactor and asserts
 * the table stays at one entry, then proves a post-churn connection still
 * drains output end to end through the reused slot. */

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
        (void)fprintf(stderr, "server wire churn: assertion at line %d failed\n", line);
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
    // The final round-trip sends one frame of this type to trigger the read
    // event that re-arms OUT on the reused slot.
    if (kith_proto_register_type_id(fx->proto, "test.churn", 1u) != 0)
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

static void sleep_ms(unsigned int ms)
{
    struct timespec ts = {.tv_sec = 0, .tv_nsec = (long)ms * 1000L * 1000L};
    (void)nanosleep(&ts, nullptr);
}

static int connect_client(const struct fixture *fx)
{
    int client_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (client_fd < 0)
    {
        return -1;
    }
    struct sockaddr_in addr = {0};
    socklen_t addr_len = sizeof(addr);
    if (getsockname(kith_net_listener_fd(fx->net), (struct sockaddr *)&addr, &addr_len) != 0 ||
        connect(client_fd, (struct sockaddr *)&addr, addr_len) != 0)
    {
        (void)close(client_fd);
        return -1;
    }
    return client_fd;
}

// The table holds exactly one slot: live between the accept and the close of
// the current cycle, closed after it. A second slot means the closed entry
// was not recycled. The closed load is acquire: on a reused slot the count
// never changes, so the flag's release/acquire pair is the per-cycle edge
// that publishes the slot's re-initialized state.
static bool wait_slot_state(struct server_wire *wire, bool live)
{
    for (unsigned int i = 0u; i < 1000u; ++i)
    {
        if (atomic_load_explicit(&wire->conn_count, memory_order_acquire) == 1u &&
            atomic_load_explicit(&wire->conns[0], memory_order_relaxed) != nullptr &&
            atomic_load_explicit(&wire->conns[0]->closed, memory_order_acquire) != live)
        {
            return true;
        }
        sleep_ms(2u);
    }
    return false;
}

static bool wait_for_ns_total(struct server_wire *wire, uint64_t min_ns, unsigned int ms)
{
    for (unsigned int elapsed = 0u; elapsed < ms; elapsed += 2u)
    {
        if (atomic_load_explicit(&wire->write_ns_total, memory_order_relaxed) >= min_ns)
        {
            return true;
        }
        sleep_ms(2u);
    }
    return false;
}

static int test_churn_reuses_slots(void)
{
    int failures = 0;
    struct fixture fx;
    CHECK(fixture_init(&fx) == 0);

    struct server_wire wire;
    memset(&wire, 0, sizeof(wire));
    CHECK(server_wire_create(&wire, nullptr, fx.reactor, fx.net, fx.gw) == 0);

    struct reactor_arg arg = {.reactor = fx.reactor};
    pthread_t tid;
    int prc = pthread_create(&tid, nullptr, reactor_thread_fn, &arg);
    CHECK(prc == 0);

    // Connect and disconnect 64 times: every cycle must land on the same
    // single slot, and the driver must still accept on a closed-but-recycled
    // table.
    for (unsigned int cycle = 0u; cycle < 64u; ++cycle)
    {
        int client_fd = connect_client(&fx);
        CHECK(client_fd >= 0);
        if (client_fd < 0)
        {
            break;
        }
        CHECK(wait_slot_state(&wire, true));
        CHECK(wire.conn_count == 1u);
        (void)close(client_fd);
        CHECK(wait_slot_state(&wire, false));
        CHECK(wire.conn_count == 1u);
    }

    // A connection accepted after the churn is fully functional: output
    // enqueued on the reused slot drains to the socket once the client's
    // frame triggers the read event that re-arms OUT.
    int client_fd = connect_client(&fx);
    CHECK(client_fd >= 0);
    if (client_fd >= 0)
    {
        CHECK(wait_slot_state(&wire, true));
        kith_net_frame_t *frame = kith_net_frame_create(fx.net, 16u);
        CHECK(frame != nullptr);
        if (frame != nullptr)
        {
            (void)memset(kith_net_frame_data(frame), 0xAB, 16u);
            kith_net_frame_set_len(frame, 16u);
            CHECK(kith_net_conn_enqueue(wire.conns[0]->conn, frame) == 0);
            kith_net_frame_release(frame);
        }

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
        CHECK(wire.conn_count == 1u);
    }
    (void)close(client_fd);

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
    rc |= test_churn_reuses_slots();
    if (rc != 0)
    {
        (void)fprintf(stderr, "server wire churn tests FAILED\n");
    }
    return rc;
}
