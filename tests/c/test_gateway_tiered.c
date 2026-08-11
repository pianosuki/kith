/* The "tiered" delivery strategy: first-send semantics, record-granular
 * suppression against the per-session ledger, change-triggered sends,
 * per-tier cadence gating, the max-gap floor, and membership-event
 * delivery while states stay suppressed. Drives cache/view/deliver
 * manually over a loopback connection so every timestamp is controlled. */

#include <signal.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <unistd.h>

#include "gateway/delivery/delivery.h"
#include "kith/fabric/fabric.h"
#include "kith/gateway/gateway.h"
#include "kith/net/net.h"
#include "kith/sim/sim.h"
#include "kith/types.h"
#include "kith/version.h"

#include <netinet/in.h>
#include <sys/socket.h>

static int check_cond(bool ok, int line)
{
    if (!ok)
    {
        (void)fprintf(stderr, "gateway tiered: assertion at line %d failed\n", line);
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
    int client_fd;
    kith_net_conn_t *conn;
};

static int fixture_init(struct fixture *fx, const kith_gateway_tiered_config_t *config)
{
    memset(fx, 0, sizeof(*fx));
    fx->client_fd = -1;
    if (kith_proto_create(nullptr, nullptr, &fx->proto) != 0)
    {
        return -1;
    }
    const uint16_t batch_type = KITH_PROTO_TYPE_USER_BASE;
    if (kith_proto_register_type_id(fx->proto, "replication", batch_type) != 0)
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
    params.replication_batch_type_id = batch_type;
    params.view_refresh_interval_ms = 10u;
    params.cache_refresh_interval_ms = 10u;
    params.delivery_strategy = "tiered";
    params.delivery_config = config;
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

static void fixture_fini(struct fixture *fx)
{
    if (fx->client_fd >= 0)
    {
        (void)close(fx->client_fd);
    }
    if (fx->conn)
    {
        kith_net_conn_close(fx->conn);
        kith_net_conn_release(fx->conn);
    }
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

/** Deliver one pass and drain its single batch frame off the wire.
 *  Returns the frame's record count through @p out_count. When
 *  @p out_records is non-NULL it receives up to @p out_cap raw record
 *  bytes from the frame body. */
static int deliver_and_capture(struct fixture *fx,
                               kith_gateway_session_t *s,
                               uint64_t now_ms,
                               uint32_t *out_events,
                               uint32_t *out_suppressed,
                               uint16_t *out_count,
                               uint8_t *out_records,
                               size_t out_cap)
{
    kith_gateway_delivery_stats_t stats = {0};
    const int rc = kith_gateway_deliver(fx->gw, s, now_ms, &stats);
    if (rc != 0 || stats.enqueued != 1u)
    {
        return -1;
    }
    if (kith_net_conn_write(fx->conn) != 0)
    {
        return -1;
    }
    uint8_t header[14u];
    if (read_full(fx->client_fd, header, sizeof(header)) != 0)
    {
        return -1;
    }
    const uint16_t count = (uint16_t)((uint16_t)header[10] << 8u | header[11]);
    const size_t rest = (size_t)count * GATEWAY_DELIVERY_PAYLOAD_SIZE;
    if (rest > 0u)
    {
        uint8_t records[32u * 64u];
        if (rest > sizeof(records) || read_full(fx->client_fd, records, rest) != 0)
        {
            return -1;
        }
        if (out_records != nullptr)
        {
            if (rest > out_cap)
            {
                return -1;
            }
            memcpy(out_records, records, rest);
        }
    }
    if (out_events)
    {
        *out_events = stats.events_enqueued;
    }
    if (out_suppressed)
    {
        *out_suppressed = stats.suppressed;
    }
    if (out_count)
    {
        *out_count = count;
    }
    return 0;
}

/** Deliver one pass and drain its single batch frame off the wire.
 *  Returns the frame's record count through @p out_count. */
static int deliver_and_drain(struct fixture *fx,
                             kith_gateway_session_t *s,
                             uint64_t now_ms,
                             uint32_t *out_events,
                             uint32_t *out_suppressed,
                             uint16_t *out_count)
{
    return deliver_and_capture(fx, s, now_ms, out_events, out_suppressed, out_count, nullptr, 0u);
}

/** Advance one controlled tick: jump the clock by @p cache_pre_ms and
 *  refresh the cache, jump a further 100 ms and refresh the view, then
 *  deliver one pass and drain its frame off the wire. Returns 0 on
 *  success. */
static int tiered_step(struct fixture *fx,
                       kith_gateway_session_t *s,
                       uint64_t cache_pre_ms,
                       uint64_t *io_now,
                       uint32_t *out_events,
                       uint32_t *out_suppressed,
                       uint16_t *out_count)
{
    *io_now += cache_pre_ms;
    if (kith_gateway_cache_refresh(fx->gw, *io_now) != 0)
    {
        return -1;
    }
    *io_now += 100u;
    if (kith_gateway_view_refresh(fx->gw, s, *io_now) != 0)
    {
        return -1;
    }
    return deliver_and_drain(fx, s, *io_now, out_events, out_suppressed, out_count);
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

static kith_sim_actor_t make_actor(uint64_t id, int64_t x, uint32_t tick)
{
    kith_sim_actor_t a = {0};
    a.id = id;
    a.pos_x = x;
    a.input_tick = tick;
    return a;
}

// First pass sends everything (states plus their enter events); an
// immediately following pass over unchanged content suppresses every
// non-self subject and still delivers the self record; moving the
// neighbor makes exactly that subject due again.
static int test_tiered_first_send_then_suppression(void)
{
    int failures = 0;
    struct fixture fx;
    CHECK(fixture_init(&fx, nullptr) == 0);

    fx.client_fd = connect_to_listener(fx.net);
    CHECK(fx.client_fd >= 0);
    CHECK(kith_net_accept(fx.net, &fx.conn) == 0);

    kith_gateway_session_t *s = nullptr;
    CHECK(kith_gateway_session_create(
              fx.gw, fx.conn, KITH_GATEWAY_SESSION_SUBSCRIBER, 1u, nullptr, &s) == 0);
    CHECK(kith_gateway_session_bind_actor(s, 1u) == 0);

    kith_fabric_cell_key_t home = make_key(6u, 0, 0, 0, 0u);
    kith_fabric_cell_key_t near = make_key(6u, 1, 0, 0, 0u);
    CHECK(kith_gateway_session_window_add(s, &home) == 0);
    CHECK(kith_gateway_session_window_add(s, &near) == 0);

    kith_sim_artifact_key_t sk_home = make_skey(&home, 1u);
    kith_sim_artifact_key_t sk_near = make_skey(&near, 1u);
    kith_sim_actor_t sub = make_actor(1u, 0, 5u);
    CHECK(kith_sim_publish_artifact(fx.sim, &sk_home, &sub, nullptr) == 0);
    kith_sim_actor_t neighbor = make_actor(2u, 7LL << 16, 5u);
    CHECK(kith_sim_publish_artifact(fx.sim, &sk_near, &neighbor, nullptr) == 0);
    CHECK(kith_fabric_publish(fx.fabric, &home, 1u, nullptr) == 0);
    CHECK(kith_fabric_publish(fx.fabric, &near, 1u, nullptr) == 0);

    uint64_t now_ms = 1000u;
    CHECK(kith_gateway_cache_refresh(fx.gw, now_ms) == 0);
    now_ms += 100u;
    CHECK(kith_gateway_view_refresh(fx.gw, s, now_ms) == 0);

    // Pass one: [sub][neighbor]. Tiered emits no enter sentinels — a
    // record arriving for an id the client never saw IS the entry signal
    // under suppression; only departures disambiguate silence.
    uint16_t count = 0u;
    uint32_t events = 0u;
    uint32_t suppressed = 0u;
    CHECK(deliver_and_drain(&fx, s, now_ms, &events, &suppressed, &count) == 0);
    CHECK(count == 2u);
    CHECK(events == 0u);
    CHECK(suppressed == 0u);

    // Pass two over unchanged content: only the self record delivers.
    CHECK(kith_fabric_publish(fx.fabric, &near, 1u, nullptr) == 0);
    CHECK(tiered_step(&fx, s, 100u, &now_ms, &events, &suppressed, &count) == 0);
    CHECK(count == 1u);
    CHECK(events == 0u);
    CHECK(suppressed == 1u);

    // Moving the neighbor changes its record: exactly it becomes due
    // alongside the always-delivered self.
    neighbor.pos_x = 9LL << 16;
    CHECK(kith_sim_publish_artifact(fx.sim, &sk_near, &neighbor, nullptr) == 0);
    CHECK(kith_fabric_publish(fx.fabric, &near, 1u, nullptr) == 0);
    CHECK(tiered_step(&fx, s, 100u, &now_ms, &events, &suppressed, &count) == 0);
    CHECK(count == 2u);
    CHECK(events == 0u);
    CHECK(suppressed == 0u);

    // Removing the neighbor regenerates the delta as VANISH; the pass
    // delivers the event even though nothing else changed.
    CHECK(kith_sim_remove_artifact(fx.sim, 2u) == 0);
    CHECK(kith_fabric_publish(fx.fabric, &near, 1u, nullptr) == 0);
    CHECK(tiered_step(&fx, s, 100u, &now_ms, &events, &suppressed, &count) == 0);
    CHECK(count == 2u); // [VANISH(2)][self]
    CHECK(events == 1u);

    kith_gateway_session_destroy(s);
    fixture_fini(&fx);
    return failures;
}

// A REDUCED-tier neighbor obeys its cadence: a changed record inside the
// interval stays suppressed; the same change after the interval delivers.
// The max-gap floor refreshes a frozen subject regardless of change.
static int test_tiered_reduced_cadence_and_max_gap(void)
{
    int failures = 0;
    kith_gateway_tiered_config_t cfg = {0};
    cfg.size = sizeof(cfg);
    cfg.abi_version = KITH_ABI_VERSION;
    cfg.reduced_interval_ms = 500u;
    cfg.max_gap_ms = 3600000u;

    struct fixture fx;
    CHECK(fixture_init(&fx, &cfg) == 0);

    fx.client_fd = connect_to_listener(fx.net);
    CHECK(fx.client_fd >= 0);
    CHECK(kith_net_accept(fx.net, &fx.conn) == 0);

    kith_gateway_session_t *s = nullptr;
    CHECK(kith_gateway_session_create(
              fx.gw, fx.conn, KITH_GATEWAY_SESSION_SUBSCRIBER, 1u, nullptr, &s) == 0);
    CHECK(kith_gateway_session_bind_actor(s, 1u) == 0);

    // Two cells away: REDUCED tier for the neighbor.
    kith_fabric_cell_key_t home = make_key(7u, 0, 0, 0, 0u);
    kith_fabric_cell_key_t far_cell = make_key(7u, 3, 0, 0, 0u);
    CHECK(kith_gateway_session_window_add(s, &home) == 0);
    CHECK(kith_gateway_session_window_add(s, &far_cell) == 0);

    kith_sim_artifact_key_t sk_home = make_skey(&home, 1u);
    kith_sim_artifact_key_t sk_far = make_skey(&far_cell, 1u);
    kith_sim_actor_t sub = make_actor(1u, 0, 5u);
    CHECK(kith_sim_publish_artifact(fx.sim, &sk_home, &sub, nullptr) == 0);
    kith_sim_actor_t neighbor = make_actor(2u, 30LL << 16, 5u);
    CHECK(kith_sim_publish_artifact(fx.sim, &sk_far, &neighbor, nullptr) == 0);
    CHECK(kith_fabric_publish(fx.fabric, &home, 1u, nullptr) == 0);
    CHECK(kith_fabric_publish(fx.fabric, &far_cell, 1u, nullptr) == 0);

    uint64_t now_ms = 10000u;
    CHECK(kith_gateway_cache_refresh(fx.gw, now_ms) == 0);
    now_ms += 100u;
    CHECK(kith_gateway_view_refresh(fx.gw, s, now_ms) == 0);

    uint16_t count = 0u;
    uint32_t events = 0u;
    uint32_t suppressed = 0u;
    // First pass sends both subjects (REDUCED record zeroes input_tick).
    CHECK(deliver_and_drain(&fx, s, now_ms, &events, &suppressed, &count) == 0);
    CHECK(count == 2u);
    CHECK(events == 0u);

    // Changed well inside the 500ms cadence: suppressed despite the move.
    neighbor.pos_x = 31LL << 16;
    CHECK(kith_sim_publish_artifact(fx.sim, &sk_far, &neighbor, nullptr) == 0);
    CHECK(kith_fabric_publish(fx.fabric, &far_cell, 1u, nullptr) == 0);
    CHECK(tiered_step(&fx, s, 200u, &now_ms, &events, &suppressed, &count) == 0);
    CHECK(count == 1u);
    CHECK(suppressed == 1u);

    // The same change after the cadence window delivers.
    neighbor.pos_x = 32LL << 16;
    CHECK(kith_sim_publish_artifact(fx.sim, &sk_far, &neighbor, nullptr) == 0);
    CHECK(kith_fabric_publish(fx.fabric, &far_cell, 1u, nullptr) == 0);
    CHECK(tiered_step(&fx, s, 400u, &now_ms, &events, &suppressed, &count) == 0);
    CHECK(count == 2u);
    CHECK(suppressed == 0u);

    kith_gateway_session_destroy(s);
    fixture_fini(&fx);
    return failures;
}

// An undersized or wrong-generation config blob fails session creation at
// strategy init instead of surfacing mid-tick.
static int test_tiered_config_validation(void)
{
    int failures = 0;
    kith_gateway_tiered_config_t cfg = {0};
    cfg.size = sizeof(cfg) - 1u; // undersized
    cfg.abi_version = KITH_ABI_VERSION;

    struct fixture fx;
    CHECK(fixture_init(&fx, &cfg) == 0);
    fx.client_fd = connect_to_listener(fx.net);
    CHECK(fx.client_fd >= 0);
    CHECK(kith_net_accept(fx.net, &fx.conn) == 0);
    kith_gateway_session_t *s = nullptr;
    CHECK(kith_gateway_session_create(
              fx.gw, fx.conn, KITH_GATEWAY_SESSION_SUBSCRIBER, 1u, nullptr, &s) ==
          kith_error_return(KITH_ESIZE));
    fixture_fini(&fx);

    struct fixture fx2;
    cfg.size = sizeof(cfg);
    cfg.abi_version = KITH_ABI_VERSION
    +1u;
    CHECK(fixture_init(&fx2, &cfg) == 0);
    fx2.client_fd = connect_to_listener(fx2.net);
    CHECK(fx2.client_fd >= 0);
    CHECK(kith_net_accept(fx2.net, &fx2.conn) == 0);
    kith_gateway_session_t *s2 = nullptr;
    CHECK(kith_gateway_session_create(
              fx2.gw, fx2.conn, KITH_GATEWAY_SESSION_SUBSCRIBER, 1u, nullptr, &s2) ==
          kith_error_return(KITH_EABIVER));
    fixture_fini(&fx2);
    return failures;
}

// The max-gap floor refreshes a subject whose content never changes once
// its silence exceeds the configured bound; below the bound an unchanged
// subject stays suppressed no matter how many cadence windows elapse.
static int test_tiered_max_gap_floor(void)
{
    int failures = 0;
    kith_gateway_tiered_config_t cfg = {0};
    cfg.size = sizeof(cfg);
    cfg.abi_version = KITH_ABI_VERSION;
    cfg.reduced_interval_ms = 100u;
    cfg.max_gap_ms = 1000u;

    struct fixture fx;
    CHECK(fixture_init(&fx, &cfg) == 0);

    fx.client_fd = connect_to_listener(fx.net);
    CHECK(fx.client_fd >= 0);
    CHECK(kith_net_accept(fx.net, &fx.conn) == 0);

    kith_gateway_session_t *s = nullptr;
    CHECK(kith_gateway_session_create(
              fx.gw, fx.conn, KITH_GATEWAY_SESSION_SUBSCRIBER, 1u, nullptr, &s) == 0);
    CHECK(kith_gateway_session_bind_actor(s, 1u) == 0);

    kith_fabric_cell_key_t home = make_key(8u, 0, 0, 0, 0u);
    kith_fabric_cell_key_t far_cell = make_key(8u, 3, 0, 0, 0u);
    CHECK(kith_gateway_session_window_add(s, &home) == 0);
    CHECK(kith_gateway_session_window_add(s, &far_cell) == 0);

    kith_sim_artifact_key_t sk_home = make_skey(&home, 1u);
    kith_sim_artifact_key_t sk_far = make_skey(&far_cell, 1u);
    kith_sim_actor_t sub = make_actor(1u, 0, 5u);
    CHECK(kith_sim_publish_artifact(fx.sim, &sk_home, &sub, nullptr) == 0);
    kith_sim_actor_t neighbor = make_actor(2u, 30LL << 16, 5u);
    CHECK(kith_sim_publish_artifact(fx.sim, &sk_far, &neighbor, nullptr) == 0);
    CHECK(kith_fabric_publish(fx.fabric, &home, 1u, nullptr) == 0);
    CHECK(kith_fabric_publish(fx.fabric, &far_cell, 1u, nullptr) == 0);

    uint64_t now_ms = 100000u;
    CHECK(kith_gateway_cache_refresh(fx.gw, now_ms) == 0);
    now_ms += 100u;
    CHECK(kith_gateway_view_refresh(fx.gw, s, now_ms) == 0);

    uint16_t count = 0u;
    uint32_t events = 0u;
    uint32_t suppressed = 0u;
    CHECK(deliver_and_drain(&fx, s, now_ms, &events, &suppressed, &count) == 0);
    CHECK(count == 2u);

    // Frozen but repeatedly re-touched past several cadence windows:
    // unchanged records stay suppressed while under the gap bound...
    for (int round = 0; round < 3; ++round)
    {
        CHECK(kith_fabric_publish(fx.fabric, &far_cell, 1u, nullptr) == 0);
        CHECK(tiered_step(&fx, s, 150u, &now_ms, &events, &suppressed, &count) == 0);
        CHECK(count == 1u);
        CHECK(suppressed == 1u);
    }

    // ...until silence crosses the bound, which forces a refresh.
    CHECK(tiered_step(&fx, s, 800u, &now_ms, &events, &suppressed, &count) == 0);
    CHECK(count == 2u);
    CHECK(suppressed == 0u);

    kith_gateway_session_destroy(s);
    fixture_fini(&fx);
    return failures;
}

// A subject whose own payload never changes stays suppressed even while
// its cell-mate moves: the ledger comparison is per subject, not per cell.
static int test_tiered_stationary_cellmate_stays_suppressed(void)
{
    int failures = 0;
    struct fixture fx;
    CHECK(fixture_init(&fx, nullptr) == 0);

    fx.client_fd = connect_to_listener(fx.net);
    CHECK(fx.client_fd >= 0);
    CHECK(kith_net_accept(fx.net, &fx.conn) == 0);

    kith_gateway_session_t *s = nullptr;
    CHECK(kith_gateway_session_create(
              fx.gw, fx.conn, KITH_GATEWAY_SESSION_SUBSCRIBER, 1u, nullptr, &s) == 0);
    CHECK(kith_gateway_session_bind_actor(s, 1u) == 0);

    kith_fabric_cell_key_t home = make_key(10u, 0, 0, 0, 0u);
    kith_fabric_cell_key_t near = make_key(10u, 1, 0, 0, 0u);
    CHECK(kith_gateway_session_window_add(s, &home) == 0);
    CHECK(kith_gateway_session_window_add(s, &near) == 0);

    kith_sim_artifact_key_t sk_home = make_skey(&home, 1u);
    kith_sim_artifact_key_t sk_near = make_skey(&near, 1u);
    kith_sim_actor_t sub = make_actor(1u, 0, 5u);
    kith_sim_actor_t cellmate = make_actor(2u, 3LL << 16, 5u);
    kith_sim_actor_t mover = make_actor(3u, 7LL << 16, 5u);
    CHECK(kith_sim_publish_artifact(fx.sim, &sk_home, &sub, nullptr) == 0);
    CHECK(kith_sim_publish_artifact(fx.sim, &sk_home, &cellmate, nullptr) == 0);
    CHECK(kith_sim_publish_artifact(fx.sim, &sk_near, &mover, nullptr) == 0);
    CHECK(kith_fabric_publish(fx.fabric, &home, 1u, nullptr) == 0);
    CHECK(kith_fabric_publish(fx.fabric, &near, 1u, nullptr) == 0);

    uint64_t now_ms = 1000u;
    CHECK(kith_gateway_cache_refresh(fx.gw, now_ms) == 0);
    now_ms += 100u;
    CHECK(kith_gateway_view_refresh(fx.gw, s, now_ms) == 0);

    uint16_t count = 0u;
    uint32_t events = 0u;
    uint32_t suppressed = 0u;
    CHECK(deliver_and_drain(&fx, s, now_ms, &events, &suppressed, &count) == 0);
    CHECK(count == 3u);

    // The mover changes; the cellmate's own payload does not. Only the
    // mover and the self subject deliver.
    mover.pos_x = 9LL << 16;
    mover.input_tick = 6u;
    CHECK(kith_sim_publish_artifact(fx.sim, &sk_near, &mover, nullptr) == 0);
    CHECK(kith_fabric_publish(fx.fabric, &near, 1u, nullptr) == 0);
    CHECK(tiered_step(&fx, s, 100u, &now_ms, &events, &suppressed, &count) == 0);
    CHECK(count == 2u);
    CHECK(suppressed == 1u);

    kith_gateway_session_destroy(s);
    fixture_fini(&fx);
    return failures;
}

// A REDUCED→FULL tier upgrade re-differs the record even though the
// source artifact did not change: the subscriber's own move re-widens the
// neighbor's input_tick under the composer's level rules.
static int test_tiered_tier_upgrade_rediffers_unchanged_artifact(void)
{
    int failures = 0;
    kith_gateway_tiered_config_t cfg = {0};
    cfg.size = sizeof(cfg);
    cfg.abi_version = KITH_ABI_VERSION;
    cfg.reduced_interval_ms = 3600000u;
    cfg.max_gap_ms = 3600000u;

    struct fixture fx;
    CHECK(fixture_init(&fx, &cfg) == 0);

    fx.client_fd = connect_to_listener(fx.net);
    CHECK(fx.client_fd >= 0);
    CHECK(kith_net_accept(fx.net, &fx.conn) == 0);

    kith_gateway_session_t *s = nullptr;
    CHECK(kith_gateway_session_create(
              fx.gw, fx.conn, KITH_GATEWAY_SESSION_SUBSCRIBER, 1u, nullptr, &s) == 0);
    CHECK(kith_gateway_session_bind_actor(s, 1u) == 0);

    kith_fabric_cell_key_t home = make_key(9u, 0, 0, 0, 0u);
    kith_fabric_cell_key_t mid = make_key(9u, 2, 0, 0, 0u);
    kith_fabric_cell_key_t far_cell = make_key(9u, 3, 0, 0, 0u);
    CHECK(kith_gateway_session_window_add(s, &home) == 0);
    CHECK(kith_gateway_session_window_add(s, &mid) == 0);
    CHECK(kith_gateway_session_window_add(s, &far_cell) == 0);

    kith_sim_artifact_key_t sk_home = make_skey(&home, 1u);
    kith_sim_artifact_key_t sk_mid = make_skey(&mid, 1u);
    kith_sim_artifact_key_t sk_far = make_skey(&far_cell, 1u);
    kith_sim_actor_t sub = make_actor(1u, 0, 5u);
    kith_sim_actor_t neighbor = make_actor(2u, 30LL << 16, 5u);
    CHECK(kith_sim_publish_artifact(fx.sim, &sk_home, &sub, nullptr) == 0);
    CHECK(kith_sim_publish_artifact(fx.sim, &sk_far, &neighbor, nullptr) == 0);
    CHECK(kith_fabric_publish(fx.fabric, &home, 1u, nullptr) == 0);
    CHECK(kith_fabric_publish(fx.fabric, &far_cell, 1u, nullptr) == 0);

    uint64_t now_ms = 10000u;
    CHECK(kith_gateway_cache_refresh(fx.gw, now_ms) == 0);
    now_ms += 100u;
    CHECK(kith_gateway_view_refresh(fx.gw, s, now_ms) == 0);

    uint16_t count = 0u;
    uint32_t events = 0u;
    uint32_t suppressed = 0u;
    CHECK(deliver_and_drain(&fx, s, now_ms, &events, &suppressed, &count) == 0);
    CHECK(count == 2u);

    // The subscriber moves one cell closer; the neighbor's artifact is
    // untouched but its tier upgrades REDUCED→FULL, which re-widens the
    // serialized input_tick — the record must re-deliver.
    CHECK(kith_sim_remove_artifact(fx.sim, 1u) == 0);
    sub.pos_x = 20LL << 16;
    sub.input_tick = 9u;
    CHECK(kith_sim_publish_artifact(fx.sim, &sk_mid, &sub, nullptr) == 0);
    CHECK(kith_fabric_publish(fx.fabric, &home, 1u, nullptr) == 0);
    CHECK(kith_fabric_publish(fx.fabric, &mid, 1u, nullptr) == 0);
    CHECK(tiered_step(&fx, s, 100u, &now_ms, &events, &suppressed, &count) == 0);
    CHECK(count == 2u);
    CHECK(suppressed == 0u);

    kith_gateway_session_destroy(s);
    fixture_fini(&fx);
    return failures;
}

// Rebinding the session to a new actor removes the old self's ledger
// entry even though no departure event exists for it (self is excluded
// from the membership delta), so the id can re-enter as a neighbor and
// deliver from a clean ledger.
static int test_tiered_rebind_sweeps_old_self_entry(void)
{
    int failures = 0;
    struct fixture fx;
    CHECK(fixture_init(&fx, nullptr) == 0);

    fx.client_fd = connect_to_listener(fx.net);
    CHECK(fx.client_fd >= 0);
    CHECK(kith_net_accept(fx.net, &fx.conn) == 0);

    kith_gateway_session_t *s = nullptr;
    CHECK(kith_gateway_session_create(
              fx.gw, fx.conn, KITH_GATEWAY_SESSION_SUBSCRIBER, 1u, nullptr, &s) == 0);
    CHECK(kith_gateway_session_bind_actor(s, 1u) == 0);

    kith_fabric_cell_key_t home = make_key(11u, 0, 0, 0, 0u);
    kith_fabric_cell_key_t near = make_key(11u, 1, 0, 0, 0u);
    CHECK(kith_gateway_session_window_add(s, &home) == 0);
    CHECK(kith_gateway_session_window_add(s, &near) == 0);

    kith_sim_artifact_key_t sk_home = make_skey(&home, 1u);
    kith_sim_artifact_key_t sk_near = make_skey(&near, 1u);
    kith_sim_actor_t sub = make_actor(1u, 0, 5u);
    kith_sim_actor_t neighbor = make_actor(2u, 7LL << 16, 5u);
    CHECK(kith_sim_publish_artifact(fx.sim, &sk_home, &sub, nullptr) == 0);
    CHECK(kith_sim_publish_artifact(fx.sim, &sk_near, &neighbor, nullptr) == 0);
    CHECK(kith_fabric_publish(fx.fabric, &home, 1u, nullptr) == 0);
    CHECK(kith_fabric_publish(fx.fabric, &near, 1u, nullptr) == 0);

    uint64_t now_ms = 1000u;
    CHECK(kith_gateway_cache_refresh(fx.gw, now_ms) == 0);
    now_ms += 100u;
    CHECK(kith_gateway_view_refresh(fx.gw, s, now_ms) == 0);

    uint16_t count = 0u;
    uint32_t events = 0u;
    uint32_t suppressed = 0u;
    CHECK(deliver_and_drain(&fx, s, now_ms, &events, &suppressed, &count) == 0);
    CHECK(count == 2u);

    // Rebind to a new actor published in the home cell; the old self id
    // leaves the world first so the rebuilt view holds exactly the new
    // self and the unchanged neighbor (which stays suppressed).
    kith_sim_actor_t fresh = make_actor(3u, 2LL << 16, 6u);
    CHECK(kith_sim_publish_artifact(fx.sim, &sk_home, &fresh, nullptr) == 0);
    CHECK(kith_sim_remove_artifact(fx.sim, 1u) == 0);
    CHECK(kith_fabric_publish(fx.fabric, &home, 1u, nullptr) == 0);
    CHECK(kith_gateway_session_bind_actor(s, 3u) == 0);
    CHECK(tiered_step(&fx, s, 100u, &now_ms, &events, &suppressed, &count) == 0);
    CHECK(events == 0u);
    CHECK(count == 1u);
    CHECK(suppressed == 1u);

    // The old self id re-enters as a neighbor carrying exactly the payload
    // its stale ledger entry holds: a swept entry is due on first send and
    // the record delivers.
    kith_sim_actor_t returning = make_actor(1u, 0, 5u);
    CHECK(kith_sim_publish_artifact(fx.sim, &sk_near, &returning, nullptr) == 0);
    CHECK(kith_fabric_publish(fx.fabric, &near, 1u, nullptr) == 0);
    CHECK(tiered_step(&fx, s, 100u, &now_ms, &events, &suppressed, &count) == 0);
    CHECK(count == 2u);
    CHECK(suppressed == 1u);

    kith_gateway_session_destroy(s);
    fixture_fini(&fx);
    return failures;
}

// Randomized property pin: for arbitrary payload streams the strategy's
// send decisions must equal the documented byte-compare oracle (the
// 64-byte record is a pure function of the payload scalars, so field
// equality and byte equality decide identically). Each round moves a
// random subset of the neighbors, then the wire frame is checked against
// a per-actor last-sent byte ledger applying the documented rule.
static uint64_t tiered_test_next_random(uint64_t *state)
{
    *state ^= *state << 13u;
    *state ^= *state >> 7u;
    *state ^= *state << 17u;
    return *state;
}

/** The byte-compare oracle's per-actor state: last enqueued record. */
struct tiered_oracle
{
    uint8_t last[8u][GATEWAY_DELIVERY_PAYLOAD_SIZE];
    bool has[8u];
};

/** Assert the delivered wire frame carries exactly the oracle-due records
 *  with byte-identical payloads, and advance the oracle's last-sent bytes. */
static int property_assert_wire(const kith_gateway_view_subject_t *subjects,
                                size_t subject_count,
                                const uint8_t *wire,
                                uint16_t count,
                                struct tiered_oracle *oracle)
{
    int failures = 0;
    for (size_t j = 0; j < subject_count; ++j)
    {
        const bool is_self = j == 0u || subjects[j].subject_class == KITH_GATEWAY_VIEW_CLASS_SELF;
        uint8_t rec[GATEWAY_DELIVERY_PAYLOAD_SIZE];
        gateway_delivery_serialize(&subjects[j], rec);
        const uint64_t id = subjects[j].actor_id;
        const bool ref_due = is_self || !oracle->has[id] ||
                             memcmp(rec, oracle->last[id], GATEWAY_DELIVERY_PAYLOAD_SIZE) != 0;
        bool on_wire = false;
        for (uint16_t r = 0; r < count; ++r)
        {
            const uint8_t *w = wire + (size_t)r * GATEWAY_DELIVERY_PAYLOAD_SIZE;
            const uint64_t wid = (uint64_t)w[0] << 56u | (uint64_t)w[1] << 48u |
                                 (uint64_t)w[2] << 40u | (uint64_t)w[3] << 32u |
                                 (uint64_t)w[4] << 24u | (uint64_t)w[5] << 16u |
                                 (uint64_t)w[6] << 8u | (uint64_t)w[7];
            if (wid == id)
            {
                CHECK(memcmp(w, rec, GATEWAY_DELIVERY_PAYLOAD_SIZE) == 0);
                on_wire = true;
            }
        }
        CHECK(ref_due == on_wire);
        if (ref_due)
        {
            memcpy(oracle->last[id], rec, GATEWAY_DELIVERY_PAYLOAD_SIZE);
            oracle->has[id] = true;
        }
    }
    return failures;
}

/** Run one randomized round: move a random subset of the six neighbors,
 *  publish + refresh, deliver, and assert the wire frame carries exactly
 *  the oracle-due records with byte-identical payloads. */
static int property_round(struct fixture *fx,
                          kith_gateway_session_t *s,
                          uint64_t *io_now,
                          uint64_t *rng,
                          const kith_fabric_cell_key_t *cells[3],
                          kith_sim_actor_t *actors,
                          struct tiered_oracle *oracle)
{
    int failures = 0;
    bool dirty[3] = {false, false, false};
    for (uint64_t a = 2u; a < 8u; ++a)
    {
        if ((tiered_test_next_random(rng) & 3u) != 0u)
        {
            continue;
        }
        int c;
        if (a <= 3u)
        {
            c = 0;
        }
        else if (a <= 5u)
        {
            c = 1;
        }
        else
        {
            c = 2;
        }
        actors[a - 2u].pos_x = (int64_t)(tiered_test_next_random(rng) & 0xFFFFu);
        actors[a - 2u].pos_y = (int64_t)(tiered_test_next_random(rng) & 0xFFu) << 8;
        actors[a - 2u].vel_x = (int64_t)(tiered_test_next_random(rng) & 0x3FFu) - 512;
        actors[a - 2u].input_tick = (uint32_t)(*io_now / 100u);
        kith_sim_artifact_key_t sk = make_skey(cells[c], 1u);
        CHECK(kith_sim_publish_artifact(fx->sim, &sk, &actors[a - 2u], nullptr) == 0);
        dirty[c] = true;
    }
    for (int c = 0; c < 3; ++c)
    {
        if (dirty[c])
        {
            CHECK(kith_fabric_publish(fx->fabric, cells[c], 1u, nullptr) == 0);
        }
    }
    *io_now += 100u;
    CHECK(kith_gateway_cache_refresh(fx->gw, *io_now) == 0);
    *io_now += 100u;
    CHECK(kith_gateway_view_refresh(fx->gw, s, *io_now) == 0);

    kith_gateway_view_snapshot_t snap = {0};
    kith_gateway_view_subject_t subjects[16u];
    size_t subject_count = 0u;
    uint32_t events = 0u;
    uint32_t suppressed = 0u;
    uint16_t count = 0u;
    uint8_t wire[8u * GATEWAY_DELIVERY_PAYLOAD_SIZE];
    CHECK(kith_gateway_view_snapshot(fx->gw, s, &snap, subjects, 16u, &subject_count) == 0);
    CHECK(deliver_and_capture(fx, s, *io_now, &events, &suppressed, &count, wire, sizeof(wire)) ==
          0);
    CHECK(events == 0u);
    CHECK((size_t)count == subject_count - (size_t)suppressed);
    failures += property_assert_wire(subjects, subject_count, wire, count, oracle);
    return failures;
}

static int test_tiered_payload_equivalence_randomized(void)
{
    int failures = 0;
    kith_gateway_tiered_config_t cfg = {0};
    cfg.size = sizeof(cfg);
    cfg.abi_version = KITH_ABI_VERSION;
    cfg.full_interval_ms = 0u;
    cfg.reduced_interval_ms = 0u;
    cfg.max_gap_ms = 3600000u;

    struct fixture fx;
    CHECK(fixture_init(&fx, &cfg) == 0);

    fx.client_fd = connect_to_listener(fx.net);
    CHECK(fx.client_fd >= 0);
    CHECK(kith_net_accept(fx.net, &fx.conn) == 0);

    kith_gateway_session_t *s = nullptr;
    CHECK(kith_gateway_session_create(
              fx.gw, fx.conn, KITH_GATEWAY_SESSION_SUBSCRIBER, 1u, nullptr, &s) == 0);
    CHECK(kith_gateway_session_bind_actor(s, 1u) == 0);

    kith_fabric_cell_key_t home = make_key(12u, 0, 0, 0, 0u);
    kith_fabric_cell_key_t near = make_key(12u, 1, 0, 0, 0u);
    kith_fabric_cell_key_t far_cell = make_key(12u, 3, 0, 0, 0u);
    CHECK(kith_gateway_session_window_add(s, &home) == 0);
    CHECK(kith_gateway_session_window_add(s, &near) == 0);
    CHECK(kith_gateway_session_window_add(s, &far_cell) == 0);

    kith_sim_actor_t sub = make_actor(1u, 0, 5u);
    kith_sim_artifact_key_t sk_self = make_skey(&home, 1u);
    CHECK(kith_sim_publish_artifact(fx.sim, &sk_self, &sub, nullptr) == 0);
    // Six neighbors across the tracked cells; ids are dense from 2.
    kith_sim_actor_t actors[6] = {0};
    const kith_fabric_cell_key_t *cells[3] = {&home, &near, &far_cell};
    const int per_cell[3] = {2, 2, 2};
    uint64_t actor_id = 2u;
    for (int c = 0; c < 3; ++c)
    {
        for (int n = 0; n < per_cell[c]; ++n)
        {
            actors[actor_id - 2u] = make_actor(actor_id, (int64_t)(n + 1) << 15, 5u);
            actors[actor_id - 2u].pos_y = (int64_t)((unsigned)(n * 11)) << 16;
            kith_sim_artifact_key_t sk = make_skey(cells[c], 1u);
            CHECK(kith_sim_publish_artifact(fx.sim, &sk, &actors[actor_id - 2u], nullptr) == 0);
            actor_id += 1u;
        }
    }
    CHECK(kith_fabric_publish(fx.fabric, &home, 1u, nullptr) == 0);
    CHECK(kith_fabric_publish(fx.fabric, &near, 1u, nullptr) == 0);
    CHECK(kith_fabric_publish(fx.fabric, &far_cell, 1u, nullptr) == 0);
    uint64_t now_ms = 1000u;
    CHECK(kith_gateway_cache_refresh(fx.gw, now_ms) == 0);
    now_ms += 100u;
    CHECK(kith_gateway_view_refresh(fx.gw, s, now_ms) == 0);

    struct tiered_oracle oracle = {0};
    uint64_t rng = 0x5DEECE66DULL;
    for (int round = 0; round < 40; ++round)
    {
        CHECK(property_round(&fx, s, &now_ms, &rng, cells, actors, &oracle) == 0);
    }

    kith_gateway_session_destroy(s);
    fixture_fini(&fx);
    return failures;
}

int main(void)
{
    (void)signal(SIGPIPE, SIG_IGN);
    int rc = 0;
    rc |= test_tiered_first_send_then_suppression();
    rc |= test_tiered_reduced_cadence_and_max_gap();
    rc |= test_tiered_config_validation();
    rc |= test_tiered_max_gap_floor();
    rc |= test_tiered_stationary_cellmate_stays_suppressed();
    rc |= test_tiered_tier_upgrade_rediffers_unchanged_artifact();
    rc |= test_tiered_rebind_sweeps_old_self_entry();
    rc |= test_tiered_payload_equivalence_randomized();
    if (rc == 0)
    {
        (void)printf("gateway tiered: all tests passed\n");
    }
    return rc;
}
