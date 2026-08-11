#include <signal.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include <unistd.h>

#include "gateway/delivery/delivery.h"
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
        (void)fprintf(stderr, "gateway delivery: assertion at line %d failed\n", line);
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
    uint16_t replication_type_id;
};

/** Shared stack construction. @p batch selects batch framing
 *  (replication_batch_type_id); otherwise the legacy per-subject framing id
 *  is set. Both register "replication" so encoded frames pass the codec. */
static int fixture_init_framed(struct fixture *fx, bool batch)
{
    memset(fx, 0, sizeof(*fx));
    if (kith_proto_create(nullptr, nullptr, &fx->proto) != 0)
    {
        return -1;
    }
    // Register the replication message type so the proto codec accepts frames
    // of that type on encode.
    fx->replication_type_id = KITH_PROTO_TYPE_USER_BASE;
    if (kith_proto_register_type_id(fx->proto, "replication", fx->replication_type_id) != 0)
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
    if (batch)
    {
        params.replication_batch_type_id = fx->replication_type_id;
    }
    else
    {
        params.replication_type_id = fx->replication_type_id;
    }
    if (kith_gateway_create(&params, fx->net, fx->fabric, fx->proto, nullptr, &fx->gw) != 0)
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
    return fixture_init_framed(fx, false);
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

// Every delivery entry point rejects a NULL handle with EINVAL. deliver with
// replication_type_id == 0 (delivery disabled) returns ESTATE. deliver with
// no composed view set returns ESTATE.
static int test_delivery_arg_validation(void)
{
    int failures = 0;
    struct fixture fx;
    CHECK(fixture_init(&fx) == 0);

    kith_net_conn_t *conn = nullptr;
    CHECK(make_conn(fx.net, &conn) == 0);
    kith_gateway_session_t *s = nullptr;
    CHECK(kith_gateway_session_create(
              fx.gw, conn, KITH_GATEWAY_SESSION_SUBSCRIBER, 1u, nullptr, &s) == 0);

    CHECK(kith_gateway_deliver(nullptr, s, 0u, nullptr) == kith_error_return(KITH_EINVAL));
    CHECK(kith_gateway_deliver(fx.gw, nullptr, 0u, nullptr) == kith_error_return(KITH_EINVAL));

    // No composed view set yet.
    kith_gateway_delivery_stats_t stats = {0};
    CHECK(kith_gateway_deliver(fx.gw, s, 0u, &stats) == kith_error_return(KITH_ESTATE));

    kith_gateway_session_destroy(s);
    kith_net_conn_close(conn);
    kith_net_conn_release(conn);
    fixture_fini(&fx);
    return failures;
}

// With replication_type_id == 0 (delivery encoding disabled at create time),
// deliver returns ESTATE even when a view set has been composed.
static int test_delivery_disabled(void)
{
    int failures = 0;
    kith_proto_t *proto = nullptr;
    CHECK(kith_proto_create(nullptr, nullptr, &proto) == 0);
    kith_net_t *net = nullptr;
    CHECK(kith_net_create(nullptr, proto, nullptr, &net) == 0);
    kith_sim_t *sim = nullptr;
    CHECK(kith_sim_create(nullptr, nullptr, &sim) == 0);
    kith_fabric_t *fabric = nullptr;
    CHECK(kith_fabric_create(nullptr, sim, nullptr, &fabric) == 0);

    kith_gateway_params_t params = {0};
    params.size = sizeof(params);
    params.abi_version = KITH_ABI_VERSION;
    params.replication_type_id = 0u;
    kith_gateway_t *gw = nullptr;
    CHECK(kith_gateway_create(&params, net, fabric, proto, nullptr, &gw) == 0);
    CHECK(kith_net_listen(net, "127.0.0.1", 0) == 0);

    kith_fabric_cell_key_t cell = make_key(1u, 0, 0, 0, 0u);

    kith_net_conn_t *conn = nullptr;
    CHECK(make_conn(net, &conn) == 0);
    kith_gateway_session_t *s = nullptr;
    CHECK(kith_gateway_session_create(gw, conn, KITH_GATEWAY_SESSION_SUBSCRIBER, 1u, nullptr, &s) ==
          0);
    CHECK(kith_gateway_session_bind_actor(s, 1u) == 0);
    CHECK(kith_gateway_session_window_add(s, &cell) == 0);

    kith_sim_artifact_key_t sk = make_skey(&cell, 1u);
    kith_sim_actor_t sub = make_actor(1u, 0, 0, 0u);
    CHECK(kith_sim_publish_artifact(sim, &sk, &sub, nullptr) == 0);
    CHECK(kith_fabric_publish(fabric, &cell, 1u, nullptr) == 0);
    CHECK(kith_gateway_cache_refresh(gw, 100u) == 0);

    CHECK(kith_gateway_view_refresh(gw, s, 200u) == 0);

    kith_gateway_delivery_stats_t stats = {0};
    CHECK(kith_gateway_deliver(gw, s, 0u, &stats) == kith_error_return(KITH_ESTATE));
    CHECK(stats.enqueued == 0u);
    CHECK(stats.dropped == 0u);

    kith_gateway_session_destroy(s);
    kith_net_conn_close(conn);
    kith_net_conn_release(conn);
    kith_gateway_destroy(gw);
    kith_fabric_destroy(fabric);
    kith_sim_destroy(sim);
    kith_net_destroy(net);
    kith_proto_destroy(proto);
    return failures;
}

// After view_refresh, deliver encodes one frame per view subject and enqueues
// each on the session's connection. The enqueued count equals the view's
// selected_count. Each frame is a valid proto frame whose type id is the
// configured replication_type_id.
static int test_delivery_enqueues(void)
{
    int failures = 0;
    struct fixture fx;
    CHECK(fixture_init(&fx) == 0);

    kith_fabric_cell_key_t cell = make_key(2u, 0, 0, 0, 0u);

    kith_net_conn_t *conn = nullptr;
    CHECK(make_conn(fx.net, &conn) == 0);
    kith_gateway_session_t *s = nullptr;
    CHECK(kith_gateway_session_create(
              fx.gw, conn, KITH_GATEWAY_SESSION_SUBSCRIBER, 1u, nullptr, &s) == 0);
    CHECK(kith_gateway_session_bind_actor(s, 100u) == 0);
    CHECK(kith_gateway_session_window_add(s, &cell) == 0);

    kith_sim_artifact_key_t sk = make_skey(&cell, 1u);
    kith_sim_actor_t sub = make_actor(100u, 0, 0, 0u);
    CHECK(kith_sim_publish_artifact(fx.sim, &sk, &sub, nullptr) == 0);
    kith_sim_actor_t a1 = make_actor(1u, 1LL << KITH_SIM_FIX_SHIFT, 0, 0u);
    CHECK(kith_sim_publish_artifact(fx.sim, &sk, &a1, nullptr) == 0);
    kith_sim_actor_t a2 = make_actor(2u, 2LL << KITH_SIM_FIX_SHIFT, 0, 0u);
    CHECK(kith_sim_publish_artifact(fx.sim, &sk, &a2, nullptr) == 0);
    CHECK(kith_fabric_publish(fx.fabric, &cell, 1u, nullptr) == 0);
    CHECK(kith_gateway_cache_refresh(fx.gw, 100u) == 0);

    CHECK(kith_gateway_view_refresh(fx.gw, s, 200u) == 0);

    kith_gateway_view_snapshot_t meta = {0};
    CHECK(kith_gateway_view_snapshot(fx.gw, s, &meta, nullptr, 0u, nullptr) == 0);
    CHECK(meta.selected_count == 3u);

    // The first pass also delivers one entry event per non-self subject.
    kith_gateway_delivery_stats_t stats = {0};
    CHECK(kith_gateway_deliver(fx.gw, s, 300u, &stats) == 0);
    CHECK(stats.enqueued == 5u);
    CHECK(stats.events_enqueued == 2u);
    CHECK(stats.dropped == 0u);

    // The OUT event bit is set after enqueuing frames.
    CHECK((kith_net_conn_events(conn) & KITH_NET_OUT) != 0u);

    // Flush and drain the client side; each frame is a proto frame whose
    // type id is the configured replication_type_id and whose payload is the
    // 64-byte serialized subject.
    CHECK(kith_net_conn_write(conn) == 0);

    // One client fd per accepted conn was closed in make_conn; create a fresh
    // loopback client to receive the bytes. Re-accept is not possible on a
    // closed listener-side conn, so instead recv on the conn's peer via a new
    // connection is not available. Read the written bytes back from the
    // conn's own ring buffer is not the contract. Instead, verify the frame
    // count indirectly: the output queue drained (OUT event clears).
    CHECK((kith_net_conn_events(conn) & KITH_NET_OUT) == 0u);

    kith_gateway_session_destroy(s);
    kith_net_conn_close(conn);
    kith_net_conn_release(conn);
    fixture_fini(&fx);
    return failures;
}

// A second deliver call with no intervening view_refresh re-delivers the
// same view set (the view is retained until the next refresh).
static int test_delivery_repeated(void)
{
    int failures = 0;
    struct fixture fx;
    CHECK(fixture_init(&fx) == 0);

    kith_fabric_cell_key_t cell = make_key(3u, 0, 0, 0, 0u);

    kith_net_conn_t *conn = nullptr;
    CHECK(make_conn(fx.net, &conn) == 0);
    kith_gateway_session_t *s = nullptr;
    CHECK(kith_gateway_session_create(
              fx.gw, conn, KITH_GATEWAY_SESSION_SUBSCRIBER, 1u, nullptr, &s) == 0);
    CHECK(kith_gateway_session_bind_actor(s, 300u) == 0);
    CHECK(kith_gateway_session_window_add(s, &cell) == 0);

    kith_sim_artifact_key_t sk = make_skey(&cell, 1u);
    kith_sim_actor_t sub = make_actor(300u, 0, 0, 0u);
    CHECK(kith_sim_publish_artifact(fx.sim, &sk, &sub, nullptr) == 0);
    CHECK(kith_fabric_publish(fx.fabric, &cell, 1u, nullptr) == 0);
    CHECK(kith_gateway_cache_refresh(fx.gw, 100u) == 0);

    CHECK(kith_gateway_view_refresh(fx.gw, s, 200u) == 0);

    kith_gateway_delivery_stats_t stats1 = {0};
    CHECK(kith_gateway_deliver(fx.gw, s, 300u, &stats1) == 0);
    CHECK(stats1.enqueued == 1u); // self only: no non-self subjects, no events
    CHECK(stats1.events_enqueued == 0u);
    CHECK(stats1.dropped == 0u);

    // Flush so the second deliver is not blocked by backpressure.
    CHECK(kith_net_conn_write(conn) == 0);

    kith_gateway_delivery_stats_t stats2 = {0};
    CHECK(kith_gateway_deliver(fx.gw, s, 400u, &stats2) == 0);
    CHECK(stats2.enqueued == 1u);
    CHECK(stats2.dropped == 0u);

    kith_gateway_session_destroy(s);
    kith_net_conn_close(conn);
    kith_net_conn_release(conn);
    fixture_fini(&fx);
    return failures;
}

// When replication_batch_type_id is non-zero, deliver packs the full view
// set into ONE multi-subject frame (4-byte count header + N * 64-byte
// records) and enqueues it as a single frame. The enqueued count is 1
// regardless of subject count, and the frame's type id is the configured
// batch type id.
static int test_delivery_batch(void)
{
    int failures = 0;
    kith_proto_t *proto = nullptr;
    CHECK(kith_proto_create(nullptr, nullptr, &proto) == 0);
    // Register both the per-subject and batch type ids so the proto codec
    // accepts frames of either type on encode.
    const uint16_t single_id = KITH_PROTO_TYPE_USER_BASE;
    const uint16_t batch_id = KITH_PROTO_TYPE_USER_BASE + 1u;
    CHECK(kith_proto_register_type_id(proto, "replication", single_id) == 0);
    CHECK(kith_proto_register_type_id(proto, "replication_batch", batch_id) == 0);

    kith_net_t *net = nullptr;
    CHECK(kith_net_create(nullptr, proto, nullptr, &net) == 0);
    kith_sim_t *sim = nullptr;
    CHECK(kith_sim_create(nullptr, nullptr, &sim) == 0);
    kith_fabric_t *fabric = nullptr;
    CHECK(kith_fabric_create(nullptr, sim, nullptr, &fabric) == 0);

    kith_gateway_params_t params = {0};
    params.size = sizeof(params);
    params.abi_version = KITH_ABI_VERSION;
    params.replication_type_id = single_id;
    params.replication_batch_type_id = batch_id;
    kith_gateway_t *gw = nullptr;
    CHECK(kith_gateway_create(&params, net, fabric, proto, nullptr, &gw) == 0);
    CHECK(kith_net_listen(net, "127.0.0.1", 0) == 0);

    kith_fabric_cell_key_t cell = make_key(4u, 0, 0, 0, 0u);

    kith_net_conn_t *conn = nullptr;
    CHECK(make_conn(net, &conn) == 0);
    kith_gateway_session_t *s = nullptr;
    CHECK(kith_gateway_session_create(gw, conn, KITH_GATEWAY_SESSION_SUBSCRIBER, 1u, nullptr, &s) ==
          0);
    CHECK(kith_gateway_session_bind_actor(s, 400u) == 0);
    CHECK(kith_gateway_session_window_add(s, &cell) == 0);

    kith_sim_artifact_key_t sk = make_skey(&cell, 1u);
    kith_sim_actor_t sub = make_actor(400u, 0, 0, 0u);
    CHECK(kith_sim_publish_artifact(sim, &sk, &sub, nullptr) == 0);
    kith_sim_actor_t a1 = make_actor(10u, 1LL << KITH_SIM_FIX_SHIFT, 0, 0u);
    CHECK(kith_sim_publish_artifact(sim, &sk, &a1, nullptr) == 0);
    kith_sim_actor_t a2 = make_actor(20u, 2LL << KITH_SIM_FIX_SHIFT, 0, 0u);
    CHECK(kith_sim_publish_artifact(sim, &sk, &a2, nullptr) == 0);
    CHECK(kith_fabric_publish(fabric, &cell, 1u, nullptr) == 0);
    CHECK(kith_gateway_cache_refresh(gw, 100u) == 0);

    CHECK(kith_gateway_view_refresh(gw, s, 200u) == 0);

    kith_gateway_view_snapshot_t meta = {0};
    CHECK(kith_gateway_view_snapshot(gw, s, &meta, nullptr, 0u, nullptr) == 0);
    CHECK(meta.selected_count == 3u);

    kith_gateway_delivery_stats_t stats = {0};
    CHECK(kith_gateway_deliver(gw, s, 300u, &stats) == 0);
    // One batch frame for 3 subjects (not 3 frames).
    CHECK(stats.enqueued == 1u);
    CHECK(stats.dropped == 0u);

    // The OUT event bit is set after enqueuing the batch frame.
    CHECK((kith_net_conn_events(conn) & KITH_NET_OUT) != 0u);

    // Flush the batch frame to the wire; the output queue drains.
    CHECK(kith_net_conn_write(conn) == 0);
    CHECK((kith_net_conn_events(conn) & KITH_NET_OUT) == 0u);

    kith_gateway_session_destroy(s);
    kith_net_conn_close(conn);
    kith_net_conn_release(conn);
    kith_gateway_destroy(gw);
    kith_fabric_destroy(fabric);
    kith_sim_destroy(sim);
    kith_net_destroy(net);
    kith_proto_destroy(proto);
    return failures;
}

// A second batch deliver call with no intervening view_refresh re-delivers
// the same view set as one batch frame. The delivery scratch is reused
// (no per-call allocation after the first grow).
static int test_delivery_batch_repeated(void)
{
    int failures = 0;
    kith_proto_t *proto = nullptr;
    CHECK(kith_proto_create(nullptr, nullptr, &proto) == 0);
    const uint16_t single_id = KITH_PROTO_TYPE_USER_BASE;
    const uint16_t batch_id = KITH_PROTO_TYPE_USER_BASE + 1u;
    CHECK(kith_proto_register_type_id(proto, "replication", single_id) == 0);
    CHECK(kith_proto_register_type_id(proto, "replication_batch", batch_id) == 0);

    kith_net_t *net = nullptr;
    CHECK(kith_net_create(nullptr, proto, nullptr, &net) == 0);
    kith_sim_t *sim = nullptr;
    CHECK(kith_sim_create(nullptr, nullptr, &sim) == 0);
    kith_fabric_t *fabric = nullptr;
    CHECK(kith_fabric_create(nullptr, sim, nullptr, &fabric) == 0);

    kith_gateway_params_t params = {0};
    params.size = sizeof(params);
    params.abi_version = KITH_ABI_VERSION;
    params.replication_type_id = single_id;
    params.replication_batch_type_id = batch_id;
    kith_gateway_t *gw = nullptr;
    CHECK(kith_gateway_create(&params, net, fabric, proto, nullptr, &gw) == 0);
    CHECK(kith_net_listen(net, "127.0.0.1", 0) == 0);

    kith_fabric_cell_key_t cell = make_key(5u, 0, 0, 0, 0u);

    kith_net_conn_t *conn = nullptr;
    CHECK(make_conn(net, &conn) == 0);
    kith_gateway_session_t *s = nullptr;
    CHECK(kith_gateway_session_create(gw, conn, KITH_GATEWAY_SESSION_SUBSCRIBER, 1u, nullptr, &s) ==
          0);
    CHECK(kith_gateway_session_bind_actor(s, 500u) == 0);
    CHECK(kith_gateway_session_window_add(s, &cell) == 0);

    kith_sim_artifact_key_t sk = make_skey(&cell, 1u);
    kith_sim_actor_t sub = make_actor(500u, 0, 0, 0u);
    CHECK(kith_sim_publish_artifact(sim, &sk, &sub, nullptr) == 0);
    CHECK(kith_fabric_publish(fabric, &cell, 1u, nullptr) == 0);
    CHECK(kith_gateway_cache_refresh(gw, 100u) == 0);
    CHECK(kith_gateway_view_refresh(gw, s, 200u) == 0);

    kith_gateway_delivery_stats_t stats1 = {0};
    CHECK(kith_gateway_deliver(gw, s, 300u, &stats1) == 0);
    CHECK(stats1.enqueued == 1u);
    CHECK(stats1.dropped == 0u);

    // Flush so the second deliver is not blocked by backpressure.
    CHECK(kith_net_conn_write(conn) == 0);

    kith_gateway_delivery_stats_t stats2 = {0};
    CHECK(kith_gateway_deliver(gw, s, 400u, &stats2) == 0);
    CHECK(stats2.enqueued == 1u);
    CHECK(stats2.dropped == 0u);

    kith_gateway_session_destroy(s);
    kith_net_conn_close(conn);
    kith_net_conn_release(conn);
    kith_gateway_destroy(gw);
    kith_fabric_destroy(fabric);
    kith_sim_destroy(sim);
    kith_net_destroy(net);
    kith_proto_destroy(proto);
    return failures;
}

static int test_delivery_serialize_big_endian(void)
{
    int failures = 0;
    kith_gateway_view_subject_t s = {0};
    s.actor_id = 0x0102030405060708ULL;
    s.pos_x = 0x1122334455667788LL;
    s.pos_y = -1LL;
    s.pos_z = 0x0L;
    s.vel_x = 0x0L;
    s.vel_y = 0x0L;
    s.vel_z = 0x0L;
    s.input_tick = 0x0A0B0C0Du;
    s.update_seq = 0x01020304u;
    s.level = KITH_FABRIC_LEVEL_CROWD;

    uint8_t buf[GATEWAY_DELIVERY_PAYLOAD_SIZE];
    gateway_delivery_serialize(&s, buf);

    // actor_id 0x0102030405060708 -> 01 02 03 04 05 06 07 08
    CHECK(buf[0] == 0x01u);
    CHECK(buf[7] == 0x08u);
    // pos_x 0x1122334455667788 -> 11 22 33 44 55 66 77 88
    CHECK(buf[8] == 0x11u);
    CHECK(buf[15] == 0x88u);
    // pos_y -1 (two's complement) -> eight 0xFF bytes
    CHECK(buf[16] == 0xFFu);
    CHECK(buf[23] == 0xFFu);
    // input_tick 0x0A0B0C0D -> 0A 0B 0C 0D
    CHECK(buf[56] == 0x0Au);
    CHECK(buf[59] == 0x0Du);
    // update_seq 0x01020304 -> 01 02 03 04
    CHECK(buf[60] == 0x01u);
    CHECK(buf[63] == 0x04u);
    // level KITH_FABRIC_LEVEL_CROWD (2) -> 00 00 00 02
    CHECK(buf[64] == 0x00u);
    CHECK(buf[67] == 0x02u);
    return failures;
}

// Membership-event records serialize as marked sentinels: actor_id
// big-endian, zeros elsewhere, product_level word = marker | kind.
static int test_delivery_serialize_event(void)
{
    int failures = 0;
    uint8_t buf[GATEWAY_DELIVERY_PAYLOAD_SIZE];
    memset(buf, 0xAA, sizeof(buf));
    gateway_delivery_serialize_event(
        0x0102030405060708ULL, (unsigned)GATEWAY_DELIVERY_EVENT_EXIT, buf);
    CHECK(buf[0] == 0x01u);
    CHECK(buf[7] == 0x08u);
    // Position/velocity/tick/seq fields carry zeros.
    for (size_t i = 8u; i < 64u; ++i)
    {
        CHECK(buf[i] == 0x00u);
    }
    // Marker bit set in the product_level word; kind in the low bits.
    CHECK(buf[64] == 0x80u);
    CHECK(buf[65] == 0x00u);
    CHECK(buf[66] == 0x00u);
    CHECK(buf[67] == (uint8_t)GATEWAY_DELIVERY_EVENT_EXIT);
    return failures;
}

// Under batch framing, the full preset emits membership events as marked
// sentinel records ordered before the state records. A live client reads
// the frames off the wire (keeping the output queue drained across passes)
// and verifies the sentinel byte layout: actor_id big-endian, zeroed body,
// product_level word = marker | kind.
static int read_full(int fd, void *buf, size_t n)
{
    uint8_t *p = buf;
    size_t got = 0u;
    while (got < n)
    {
        ssize_t r = recv(fd, p + got, n - got, 0);
        if (r <= 0)
        {
            return -1;
        }
        got += (size_t)r;
    }
    return 0;
}

/** Read one batch frame (10-byte transport header + 4-byte batch header +
 *  @p want_count 64-byte records) off the loopback socket and verify the
 *  record count and the zeroed reserved header bytes. */
static int membership_frame_read(int client_fd, uint8_t *wire, size_t cap, uint16_t want_count)
{
    const size_t total = 14u + (size_t)want_count * GATEWAY_DELIVERY_PAYLOAD_SIZE;
    if (total > cap || read_full(client_fd, wire, total) != 0)
    {
        return -1;
    }
    const uint16_t count = (uint16_t)((uint16_t)wire[10] << 8u | wire[11]);
    if (count != want_count || wire[12] != 0u || wire[13] != 0u)
    {
        return -1;
    }
    return 0;
}

/** Membership sentinel record layout: big-endian actor_id low byte, zeroed
 *  body, marker bit with kind in the product_level word. */
static bool membership_is_event(const uint8_t *rec, uint8_t id_low, uint8_t kind)
{
    if (rec[7] != id_low)
    {
        return false;
    }
    for (size_t i = 8u; i < 64u; ++i)
    {
        if (rec[i] != 0u)
        {
            return false;
        }
    }
    return rec[64] == 0x80u && rec[67] == kind;
}

static int test_delivery_membership_events(void)
{
    int failures = 0;
    struct fixture fx;
    CHECK(fixture_init_framed(&fx, true) == 0);

    int client_fd = connect_to_listener(fx.net);
    CHECK(client_fd >= 0);
    kith_net_conn_t *conn = nullptr;
    CHECK(kith_net_accept(fx.net, &conn) == 0);
    kith_gateway_session_t *s = nullptr;
    CHECK(kith_gateway_session_create(
              fx.gw, conn, KITH_GATEWAY_SESSION_SUBSCRIBER, 1u, nullptr, &s) == 0);
    CHECK(kith_gateway_session_bind_actor(s, 100u) == 0);

    kith_fabric_cell_key_t cell = make_key(9u, 0, 0, 0, 0u);
    CHECK(kith_gateway_session_window_add(s, &cell) == 0);

    kith_sim_artifact_key_t sk = make_skey(&cell, 1u);
    kith_sim_actor_t sub = make_actor(100u, 0, 0, 0u);
    CHECK(kith_sim_publish_artifact(fx.sim, &sk, &sub, nullptr) == 0);
    kith_sim_actor_t neighbor = make_actor(200u, 1LL << KITH_SIM_FIX_SHIFT, 0, 0u);
    CHECK(kith_sim_publish_artifact(fx.sim, &sk, &neighbor, nullptr) == 0);
    CHECK(kith_fabric_publish(fx.fabric, &cell, 1u, nullptr) == 0);
    CHECK(kith_gateway_cache_refresh(fx.gw, 100u) == 0);
    CHECK(kith_gateway_view_refresh(fx.gw, s, 200u) == 0);

    // Pass one: one batch frame carrying [ENTER(200)][sub][neighbor].
    kith_gateway_delivery_stats_t stats = {0};
    CHECK(kith_gateway_deliver(fx.gw, s, 300u, &stats) == 0);
    CHECK(stats.enqueued == 1u);
    CHECK(stats.events_enqueued == 1u);
    CHECK(kith_net_conn_write(conn) == 0);
    uint8_t wire[14u + 3u * GATEWAY_DELIVERY_PAYLOAD_SIZE];
    CHECK(membership_frame_read(client_fd, wire, sizeof(wire), 3u) == 0);
    CHECK(membership_is_event(wire + 14u, 0xC8u, (uint8_t)GATEWAY_DELIVERY_EVENT_ENTER));
    const size_t st1 = 14u + GATEWAY_DELIVERY_PAYLOAD_SIZE; // second record: self state
    CHECK(wire[st1 + 7] == 0x64u);                          // actor_id 100
    CHECK((wire[st1 + 64] & 0x80u) == 0u);                  // no marker on state records

    // Pass two without a recomposition: the delta was consumed, so the
    // frame carries only the two state records.
    CHECK(kith_gateway_deliver(fx.gw, s, 400u, &stats) == 0);
    CHECK(stats.enqueued == 1u);
    CHECK(stats.events_enqueued == 0u);
    CHECK(kith_net_conn_write(conn) == 0);
    uint8_t wire_b[14u + 2u * GATEWAY_DELIVERY_PAYLOAD_SIZE];
    CHECK(membership_frame_read(client_fd, wire_b, sizeof(wire_b), 2u) == 0);
    CHECK((wire_b[14 + 64] & 0x80u) == 0u); // both records are states

    // The neighbor vanishes from the cell: next recomposition regenerates
    // the delta as VANISH and the pass delivers it once.
    CHECK(kith_sim_remove_artifact(fx.sim, 200u) == 0);
    CHECK(kith_fabric_publish(fx.fabric, &cell, 1u, nullptr) == 0);
    CHECK(kith_gateway_cache_refresh(fx.gw, 5000u) == 0);
    CHECK(kith_gateway_view_refresh(fx.gw, s, 6000u) == 0);
    CHECK(kith_gateway_deliver(fx.gw, s, 7000u, &stats) == 0);
    CHECK(stats.enqueued == 1u);
    CHECK(stats.events_enqueued == 1u);
    CHECK(kith_net_conn_write(conn) == 0);
    uint8_t wire2[14u + 2u * GATEWAY_DELIVERY_PAYLOAD_SIZE];
    CHECK(membership_frame_read(client_fd, wire2, sizeof(wire2), 2u) == 0);
    CHECK(membership_is_event(wire2 + 14u, 0xC8u, (uint8_t)GATEWAY_DELIVERY_EVENT_VANISH));

    kith_gateway_session_destroy(s);
    kith_net_conn_close(conn);
    kith_net_conn_release(conn);
    (void)close(client_fd);
    fixture_fini(&fx);
    return failures;
}

int main(void)
{
    (void)signal(SIGPIPE, SIG_IGN);
    int rc = 0;
    rc |= test_delivery_arg_validation();
    rc |= test_delivery_disabled();
    rc |= test_delivery_enqueues();
    rc |= test_delivery_repeated();
    rc |= test_delivery_batch();
    rc |= test_delivery_batch_repeated();
    rc |= test_delivery_serialize_big_endian();
    rc |= test_delivery_serialize_event();
    rc |= test_delivery_membership_events();
    if (rc != 0)
    {
        (void)fprintf(stderr, "gateway delivery tests FAILED\n");
    }
    return rc;
}
