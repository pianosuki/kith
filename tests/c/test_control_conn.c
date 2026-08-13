/* Fault-path units for the control plane connection manager, compiled
 * against the module's internal translation units: connection-table
 * exhaustion, close/flush-task deferral while a worker dispatch is in
 * flight, reactor-event fault handling (EOF, read-overflow guard, ERR,
 * HUP), partial-write, interrupted-write retention, and EPIPE draining,
 * SSE flush delivery with cursor
 * advance plus the oversized-line skip, stalled-socket parking with
 * subscription-cursor rewind, and accept draining against a
 * full connection table. The control handle is assembled by hand so the
 * buffer caps are fixed per scenario before any slot allocation. */

#include <errno.h>
#include <signal.h>
#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <fcntl.h>
#include <poll.h>
#include <unistd.h>

#include "control/control_internal.h"
#include "kith/reactor/reactor.h"
#include "kith/types.h"
#include "kith/util/util.h"

#include <netinet/in.h>
#include <sys/socket.h>

// ---------------------------------------------------------------------------
// test harness
// ---------------------------------------------------------------------------

static kith_reactor_t *g_reactor;

static int check_cond(bool ok, int line)
{
    if (!ok)
    {
        (void)fprintf(stderr, "control conn: assertion at line %d failed\n", line);
        return 1;
    }
    return 0;
}

#define CHECK(cond) failures += check_cond((cond), __LINE__)

/* Assemble a control handle without the public constructor: the caps are
 * scenario inputs and must be stored before control_conn_alloc snapshots
 * them into per-slot allocations. The reactor is real but never run; the
 * deregister/reschedule paths reach it directly. */
static struct kith_control *ctrl_build(uint32_t max_conns, uint32_t read_cap, uint32_t write_cap)
{
    struct kith_control *ctrl = calloc(1, sizeof(*ctrl));
    if (ctrl == nullptr)
    {
        return nullptr;
    }
    ctrl->reactor = g_reactor;
    ctrl->logger = nullptr;
    ctrl->metrics = nullptr;
    ctrl->workers = nullptr;
    ctrl->listener_fd = -1;
    ctrl->host = "";
    ctrl->port = 0u;
    ctrl->max_connections = max_conns;
    ctrl->read_buffer_cap = read_cap;
    ctrl->write_buffer_cap = write_cap;
    ctrl->sse_flush_ms = 50u;
    ctrl->conn_count = 0u;
    ctrl->conns = calloc(max_conns, sizeof(struct kith_control_conn));
    if (ctrl->conns == nullptr)
    {
        free(ctrl);
        return nullptr;
    }
    control_event_bus_init(&ctrl->event_bus, 16u, nullptr);
    return ctrl;
}

/* Release every occupied slot's buffers plus the handle. Slots holding an
 * open fd were closed by the scenario first; slots with fd < 0 keep their
 * allocations because control_conn_close keys its free/recycle work off an
 * open descriptor, so teardown frees each slot directly. */
static void ctrl_teardown(struct kith_control *ctrl)
{
    for (uint32_t i = 0u; i < ctrl->conn_count; i++)
    {
        struct kith_control_conn *conn = &ctrl->conns[i];
        free(conn->read_buf);
        free(conn->write_buf);
        free(conn->header_scratch);
    }
    control_event_bus_free(&ctrl->event_bus, nullptr);
    free(ctrl->conns);
    free(ctrl);
}

/* One connection slot backed by a socketpair: slot 0 of a two-slot handle,
 * descriptor registered as the conn side and set non-blocking like the
 * accept path leaves it. */
struct conn_fixture
{
    struct kith_control *ctrl;
    struct kith_control_conn *conn;
    int sv[2];
};

static int fixture_open(struct conn_fixture *fx, uint32_t read_cap, uint32_t write_cap)
{
    fx->ctrl = nullptr;
    fx->conn = nullptr;
    fx->sv[0] = -1;
    fx->sv[1] = -1;
    if (socketpair(AF_UNIX, SOCK_STREAM, 0, fx->sv) != 0)
    {
        return -1;
    }
    int flags = fcntl(fx->sv[0], F_GETFL, 0);
    if (flags < 0 || fcntl(fx->sv[0], F_SETFL, flags | O_NONBLOCK) < 0)
    {
        (void)close(fx->sv[0]);
        (void)close(fx->sv[1]);
        fx->sv[0] = fx->sv[1] = -1;
        return -1;
    }
    fx->ctrl = ctrl_build(2u, read_cap, write_cap);
    if (fx->ctrl == nullptr)
    {
        (void)close(fx->sv[0]);
        (void)close(fx->sv[1]);
        fx->sv[0] = fx->sv[1] = -1;
        return -1;
    }
    fx->conn = control_conn_alloc(fx->ctrl);
    if (fx->conn == nullptr)
    {
        ctrl_teardown(fx->ctrl);
        fx->ctrl = nullptr;
        (void)close(fx->sv[0]);
        (void)close(fx->sv[1]);
        fx->sv[0] = fx->sv[1] = -1;
        return -1;
    }
    fx->conn->fd = fx->sv[0];
    fx->conn->state = KITH_CONTROL_CONN_READING;
    return 0;
}

static void fixture_close(struct conn_fixture *fx)
{
    if (fx->ctrl != nullptr)
    {
        ctrl_teardown(fx->ctrl);
    }
    if (fx->sv[0] >= 0)
    {
        (void)close(fx->sv[0]);
    }
    if (fx->sv[1] >= 0)
    {
        (void)close(fx->sv[1]);
    }
    fx->ctrl = nullptr;
    fx->conn = nullptr;
    fx->sv[0] = fx->sv[1] = -1;
}

/* The host reports EINTR from socket syscalls with no signal delivery. The
 * read path treats a transient read error as leave-open-and-rearm, so a
 * synthesized event landing on one leaves the slot open; re-driving the
 * event mirrors the edge re-arm and converges on the real outcome. */
#define EVENT_DRIVE_ATTEMPTS 64

static void fill_pattern(uint8_t *buf, uint32_t len)
{
    for (uint32_t i = 0u; i < len; i++)
    {
        buf[i] = (uint8_t)(i & 0xFFu);
    }
}

/* Receive everything queued on @p fd without blocking and verify each byte
 * against the fill_pattern stream. Returns 0 when the transfer matches,
 * -1 on a mismatch, overrun past @p limit, or a socket error. Sets @p out_total
 * to the number of bytes drained. */
static int drain_and_verify(int fd, uint32_t *out_total, uint32_t limit)
{
    uint8_t chunk[4096];
    uint32_t total = 0u;
    while (true)
    {
        ssize_t n = recv(fd, chunk, sizeof(chunk), MSG_DONTWAIT);
        if (n < 0)
        {
            if (errno == EINTR)
            {
                continue;
            }
            if (errno == EAGAIN || errno == EWOULDBLOCK)
            {
                break;
            }
            return -1;
        }
        if (n == 0)
        {
            break;
        }
        for (ssize_t i = 0; i < n; i++)
        {
            if (chunk[i] != (uint8_t)((total + (uint32_t)i) & 0xFFu))
            {
                return -1;
            }
        }
        total += (uint32_t)n;
        if (total > limit)
        {
            return -1;
        }
    }
    *out_total = total;
    return 0;
}

static int count_occurrences(const char *hay, const char *needle)
{
    int count = 0;
    const char *cursor = hay;
    while ((cursor = strstr(cursor, needle)) != nullptr)
    {
        count++;
        cursor++;
    }
    return count;
}

/* conn.c compiles into this target as a redirected object: its drain call
 * lands on the shim below. One armed fault (EINTR or EWOULDBLOCK) is
 * delivered verbatim; every other invocation reaches the real descriptor,
 * retrying through host-injected EINTRs so an armed fault stays the only
 * variable in a scenario. */
static atomic_int g_inject_eintr;
static atomic_int g_inject_eagain;

/* conn.o's redirected write resolves against this symbol at link time;
 * the cross-object reference is invisible to linkage analysis, so the
 * internal-linkage lint is suppressed at both spellings. */
ssize_t kith_test_write(int fd, const void *buf, size_t count); // NOLINT(misc-use-internal-linkage)
ssize_t kith_test_write(int fd, const void *buf, size_t count)  // NOLINT(misc-use-internal-linkage)
{
    if (atomic_exchange(&g_inject_eagain, 0) != 0)
    {
        errno = EWOULDBLOCK;
        return -1;
    }
    if (atomic_exchange(&g_inject_eintr, 0) != 0)
    {
        errno = EINTR;
        return -1;
    }
    ssize_t n;
    do
    {
        n = write(fd, buf, count);
    } while (n < 0 && errno == EINTR);
    return n;
}

// ---------------------------------------------------------------------------
// scenarios
// ---------------------------------------------------------------------------

/* Drain everything queued on @p fd without blocking into @p data. */
static size_t recv_queued(int fd, char *data, size_t cap)
{
    size_t have = 0u;
    while (have + 1u < cap)
    {
        ssize_t n = recv(fd, data + have, cap - 1u - have, MSG_DONTWAIT);
        if (n <= 0)
        {
            if (n < 0 && errno == EINTR)
            {
                continue;
            }
            break;
        }
        have += (size_t)n;
    }
    data[have] = '\0';
    return have;
}

// Allocating past max_connections returns NULL with earlier slots intact;
// each slot snapshots the handle caps and grows conn_count to cover it.
static int test_alloc_table_full(void)
{
    int failures = 0;
    struct kith_control *ctrl = ctrl_build(2u, 4096u, 2048u);
    CHECK(ctrl != nullptr);
    if (ctrl == nullptr)
    {
        return failures;
    }

    struct kith_control_conn *c0 = control_conn_alloc(ctrl);
    CHECK(c0 == &ctrl->conns[0]);
    CHECK(c0->fd == -1);
    CHECK(c0->in_use);
    CHECK(c0->state == KITH_CONTROL_CONN_READING);
    CHECK(c0->read_buf != nullptr && c0->read_cap == 4096u);
    CHECK(c0->write_buf != nullptr && c0->write_cap == 2048u);
    CHECK(c0->resp.buf == c0->write_buf && c0->resp.cap == 2048u && c0->resp.len == 0u);
    CHECK(c0->header_scratch != nullptr);
    CHECK(c0->header_scratch_cap ==
          CONTROL_MAX_HEADERS * (CONTROL_MAX_HEADER_NAME + CONTROL_MAX_HEADER_VALUE));
    CHECK(ctrl->conn_count == 1u);

    struct kith_control_conn *c1 = control_conn_alloc(ctrl);
    CHECK(c1 == &ctrl->conns[1]);
    CHECK(ctrl->conn_count == 2u);

    struct kith_control_conn *c2 = control_conn_alloc(ctrl);
    CHECK(c2 == nullptr);
    CHECK(ctrl->conn_count == 2u);

    ctrl_teardown(ctrl);
    return failures;
}

// ---------------------------------------------------------------------------
// allocator routing + failure injection
// ---------------------------------------------------------------------------
// A counting/failing allocator proves the conn buffers route through the
// handle's instance: one accepted slot is three allocations, close is three
// frees, and a failed attempt unwinds the partial slot back to free.
typedef struct conn_tally
{
    unsigned attempts;
    unsigned frees;
    unsigned outstanding;
    unsigned fail_attempt;
} conn_tally_t;

static conn_tally_t g_conn_tally;

static void conn_tally_reset(unsigned fail_attempt)
{
    g_conn_tally.attempts = 0U;
    g_conn_tally.frees = 0U;
    g_conn_tally.outstanding = 0U;
    g_conn_tally.fail_attempt = fail_attempt;
}

static void *conn_tally_alloc(void *ctx, size_t size)
{
    conn_tally_t *tally = ctx;
    tally->attempts++;
    if (tally->attempts == tally->fail_attempt)
    {
        return nullptr;
    }
    tally->outstanding++;
    return malloc(size);
}

static void *conn_tally_alloc_zero(void *ctx, size_t count, size_t size)
{
    conn_tally_t *tally = ctx;
    tally->attempts++;
    if (tally->attempts == tally->fail_attempt)
    {
        return nullptr;
    }
    tally->outstanding++;
    return calloc(count, size);
}

static void *conn_tally_realloc(void *ctx, void *ptr, size_t size)
{
    conn_tally_t *tally = ctx;
    tally->attempts++;
    (void)ptr;
    if (tally->attempts == tally->fail_attempt)
    {
        return nullptr;
    }
    return realloc(ptr, size);
}

static void conn_tally_free(void *ctx, void *ptr)
{
    conn_tally_t *tally = ctx;
    tally->frees++;
    tally->outstanding--;
    free(ptr);
}

static const kith_allocator_t g_conn_tally_allocator = {
    .size = sizeof(kith_allocator_t),
    .abi_version = KITH_ABI_VERSION,
    .user_data = &g_conn_tally,
    .alloc = conn_tally_alloc,
    .alloc_zero = conn_tally_alloc_zero,
    .realloc = conn_tally_realloc,
    .free = conn_tally_free,
    .reserved = {nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr},
};

// An accepted slot allocates its read buffer, write buffer, and header
// scratch through the handle's allocator, and close releases all three
// through the same instance. The slot holds an open descriptor for the
// close: the fd guard short-circuits the free work otherwise.
static int test_alloc_routes_through_handle_allocator(void)
{
    int failures = 0;
    int sv[2] = {-1, -1};
    struct kith_control *ctrl = ctrl_build(2u, 4096u, 2048u);
    CHECK(ctrl != nullptr);
    CHECK(socketpair(AF_UNIX, SOCK_STREAM, 0, sv) == 0);
    if (ctrl == nullptr || sv[0] < 0)
    {
        if (sv[0] >= 0)
        {
            (void)close(sv[0]);
        }
        if (sv[1] >= 0)
        {
            (void)close(sv[1]);
        }
        if (ctrl != nullptr)
        {
            ctrl_teardown(ctrl);
        }
        return failures;
    }
    ctrl->allocator = &g_conn_tally_allocator;

    conn_tally_reset(0U);
    struct kith_control_conn *conn = control_conn_alloc(ctrl);
    CHECK(conn != nullptr);
    if (conn == nullptr)
    {
        ctrl_teardown(ctrl);
        (void)close(sv[1]);
        return failures;
    }
    CHECK(g_conn_tally.attempts == 3U);
    CHECK(g_conn_tally.frees == 0U);
    CHECK(g_conn_tally.outstanding == 3U);

    conn->fd = sv[0];
    control_conn_close(ctrl, conn);
    CHECK(g_conn_tally.frees == 3U);
    CHECK(g_conn_tally.outstanding == 0U);

    (void)close(sv[1]);
    ctrl_teardown(ctrl);
    return failures;
}

// A failed buffer allocation unwinds the partial slot: the failing attempt
// releases what the earlier attempts allocated, the slot stays free with
// its high-water count untouched, and the caller sees NULL.
static int test_alloc_enomem_unwinds(void)
{
    int failures = 0;
    for (unsigned int fail_at = 1U; fail_at <= 3U; fail_at++)
    {
        struct kith_control *ctrl = ctrl_build(2u, 4096u, 2048u);
        CHECK(ctrl != nullptr);
        if (ctrl == nullptr)
        {
            return failures;
        }
        ctrl->allocator = &g_conn_tally_allocator;

        conn_tally_reset(fail_at);
        struct kith_control_conn *conn = control_conn_alloc(ctrl);
        CHECK(conn == nullptr);
        CHECK(g_conn_tally.attempts == fail_at);
        CHECK(g_conn_tally.frees == fail_at - 1U);
        CHECK(g_conn_tally.outstanding == 0U);
        CHECK(ctrl->conns[0].in_use == false);
        CHECK(ctrl->conn_count == 0U);

        ctrl_teardown(ctrl);
    }
    return failures;
}

// Closing an open connection frees the slot; closing it again is a no-op
// (the fd guard short-circuits before any free or recycle work repeats).
static int test_double_close_noop(void)
{
    int failures = 0;
    struct conn_fixture fx;
    CHECK(fixture_open(&fx, 4096u, 2048u) == 0);
    if (fx.conn == nullptr)
    {
        return failures;
    }

    control_conn_close(fx.ctrl, fx.conn);
    CHECK(fx.conn->fd == -1);
    CHECK(!fx.conn->in_use);
    CHECK(fx.conn->read_buf == nullptr && fx.conn->write_buf == nullptr &&
          fx.conn->header_scratch == nullptr && fx.conn->resp.buf == nullptr);
    errno = 0;
    CHECK(fcntl(fx.sv[0], F_GETFL, 0) == -1 && errno == EBADF);

    control_conn_close(fx.ctrl, fx.conn);
    CHECK(fx.conn->fd == -1);
    CHECK(!fx.conn->in_use);
    CHECK(fx.conn->read_buf == nullptr && fx.conn->write_buf == nullptr);

    fixture_close(&fx);
    return failures;
}

// A close during an in-flight worker dispatch shuts the descriptor but
// leaves the buffers and slot for the worker; the posted flush task finds
// fd < 0 and performs the deferred free and slot recycle.
static int test_close_mid_dispatch_defers(void)
{
    int failures = 0;
    struct conn_fixture fx;
    CHECK(fixture_open(&fx, 4096u, 2048u) == 0);
    if (fx.conn == nullptr)
    {
        return failures;
    }
    struct kith_control_conn *conn = fx.conn;
    conn->state = KITH_CONTROL_CONN_DISPATCHING;

    control_conn_close(fx.ctrl, conn);
    CHECK(conn->fd == -1);
    CHECK(conn->in_use);
    CHECK(conn->read_buf != nullptr && conn->write_buf != nullptr &&
          conn->header_scratch != nullptr && conn->resp.buf != nullptr);
    CHECK(conn->state == KITH_CONTROL_CONN_DISPATCHING);

    control_conn_flush_task(conn);
    CHECK(conn->fd == -1);
    CHECK(!conn->in_use);
    CHECK(conn->read_buf == nullptr && conn->write_buf == nullptr &&
          conn->header_scratch == nullptr && conn->resp.buf == nullptr);
    CHECK(conn->state == KITH_CONTROL_CONN_READING);

    fixture_close(&fx);
    return failures;
}

// ERR and HUP outside a dispatch window are hard faults: the slot closes
// and frees immediately.
static int test_event_fault_closes_immediately(void)
{
    int failures = 0;
    for (unsigned int ev = 0u; ev < 2u; ev++)
    {
        struct conn_fixture fx;
        CHECK(fixture_open(&fx, 4096u, 2048u) == 0);
        if (fx.conn == nullptr)
        {
            return failures;
        }
        unsigned int events = (ev == 0u) ? KITH_REACTOR_ERR : KITH_REACTOR_HUP;
        control_conn_event_handler(fx.sv[0], events, fx.conn);
        CHECK(fx.conn->fd == -1);
        CHECK(!fx.conn->in_use);
        CHECK(fx.conn->read_buf == nullptr && fx.conn->write_buf == nullptr &&
              fx.conn->header_scratch == nullptr);
        fixture_close(&fx);
    }
    return failures;
}

// During a dispatch window HUP stays ignored (the response is still
// deliverable on the write side) and ERR defers the free like any
// mid-dispatch close; the flush task completes the recycle in both cases.
static int test_event_fault_during_dispatch(void)
{
    int failures = 0;
    for (unsigned int ev = 0u; ev < 2u; ev++)
    {
        struct conn_fixture fx;
        CHECK(fixture_open(&fx, 4096u, 2048u) == 0);
        if (fx.conn == nullptr)
        {
            return failures;
        }
        struct kith_control_conn *conn = fx.conn;
        conn->state = KITH_CONTROL_CONN_DISPATCHING;

        unsigned int events = (ev == 0u) ? KITH_REACTOR_HUP : KITH_REACTOR_ERR;
        control_conn_event_handler(fx.sv[0], events, conn);

        if (ev == 0u)
        {
            CHECK(conn->fd == fx.sv[0]);
            CHECK(conn->in_use);
            CHECK(conn->read_buf != nullptr && conn->state == KITH_CONTROL_CONN_DISPATCHING);
        }
        else
        {
            CHECK(conn->fd == -1);
            CHECK(conn->in_use);
            CHECK(conn->read_buf != nullptr && conn->header_scratch != nullptr);
        }

        control_conn_flush_task(conn);
        CHECK(conn->fd == -1);
        CHECK(!conn->in_use);
        CHECK(conn->read_buf == nullptr && conn->write_buf == nullptr &&
              conn->header_scratch == nullptr);
        fixture_close(&fx);
    }
    return failures;
}

// An IN event on a peer-closed socket reads EOF and closes the slot.
static int test_read_eof_closes(void)
{
    int failures = 0;
    struct conn_fixture fx;
    CHECK(fixture_open(&fx, 4096u, 2048u) == 0);
    if (fx.conn == nullptr)
    {
        return failures;
    }
    (void)close(fx.sv[1]);
    fx.sv[1] = -1;

    for (int spin = 0; spin < EVENT_DRIVE_ATTEMPTS && fx.conn->fd >= 0; spin++)
    {
        control_conn_event_handler(fx.sv[0], KITH_REACTOR_IN, fx.conn);
    }
    CHECK(fx.conn->fd == -1);
    CHECK(!fx.conn->in_use);
    CHECK(fx.conn->read_buf == nullptr && fx.conn->write_buf == nullptr &&
          fx.conn->header_scratch == nullptr);

    fixture_close(&fx);
    return failures;
}

// A full read buffer trips the overflow guard before any read syscall:
// the slot closes with zero bytes consumed.
static int test_read_overflow_guard_closes(void)
{
    int failures = 0;
    struct conn_fixture fx;
    CHECK(fixture_open(&fx, 4096u, 2048u) == 0);
    if (fx.conn == nullptr)
    {
        return failures;
    }
    fx.conn->read_pos = fx.conn->read_cap;

    control_conn_event_handler(fx.sv[0], KITH_REACTOR_IN, fx.conn);
    CHECK(fx.conn->fd == -1);
    CHECK(fx.conn->read_pos == fx.conn->read_cap);
    CHECK(!fx.conn->in_use);
    CHECK(fx.conn->read_buf == nullptr);

    fixture_close(&fx);
    return failures;
}

#define PARTIAL_PAYLOAD (1024u * 1024u)

/* Drive one partial-write attempt: a payload far larger than the socketpair
 * send buffer forces the drain loop to stop on EAGAIN partway through, with
 * the connection open in WRITING state and every delivered byte matching the
 * stream pattern. Returns assertion failures, or 0 with @p verified false
 * when a spurious host-level syscall fault closed the fixture before any
 * assertion binds (the caller rebuilds and retries). */
static int partial_write_attempt(bool *verified)
{
    *verified = false;
    struct conn_fixture fx;
    if (fixture_open(&fx, 4096u, PARTIAL_PAYLOAD) != 0)
    {
        return 1;
    }
    struct kith_control_conn *conn = fx.conn;
    fill_pattern(conn->write_buf, PARTIAL_PAYLOAD);
    conn->resp.buf = conn->write_buf;
    conn->resp.cap = PARTIAL_PAYLOAD;
    conn->resp.len = PARTIAL_PAYLOAD;
    conn->write_pos = 0u;
    conn->state = KITH_CONTROL_CONN_WRITING;

    for (int spin = 0;
         spin < EVENT_DRIVE_ATTEMPTS && conn->fd >= 0 && conn->write_pos < conn->resp.len;
         spin++)
    {
        control_conn_event_handler(fx.sv[0], KITH_REACTOR_OUT, conn);
    }

    if (conn->fd < 0)
    {
        fixture_close(&fx);
        return 0;
    }

    int failures = 0;
    CHECK(conn->write_pos > 0u);
    CHECK(conn->write_pos < conn->resp.len);
    CHECK(conn->state == KITH_CONTROL_CONN_WRITING);
    CHECK(conn->in_use);

    uint32_t delivered = 0u;
    CHECK(drain_and_verify(fx.sv[1], &delivered, conn->write_pos) == 0);
    CHECK(delivered == conn->write_pos);

    *verified = (failures == 0);
    fixture_close(&fx);
    return failures;
}

// A write larger than the socket buffer drains partially, keeps the
// connection open in WRITING state, and delivers exactly the acknowledged
// prefix intact.
static int test_write_partial_eagain(void)
{
    int failures = 0;
    bool verified = false;
    for (int attempt = 0; attempt < 3 && !verified; attempt++)
    {
        failures += partial_write_attempt(&verified);
    }
    CHECK(verified);
    return failures;
}

// Writing to a peer-closed socket raises EPIPE on the drain path and
// closes the slot. SIGPIPE is ignored in main, so the error surfaces as
// a write failure, not a signal.
static int test_write_epipe_closes(void)
{
    int failures = 0;
    struct conn_fixture fx;
    CHECK(fixture_open(&fx, 4096u, 2048u) == 0);
    if (fx.conn == nullptr)
    {
        return failures;
    }
    struct kith_control_conn *conn = fx.conn;
    (void)close(fx.sv[1]);
    fx.sv[1] = -1;
    fill_pattern(conn->write_buf, 64u);
    conn->resp.buf = conn->write_buf;
    conn->resp.cap = conn->write_cap;
    conn->resp.len = 64u;
    conn->write_pos = 0u;
    conn->state = KITH_CONTROL_CONN_WRITING;

    control_conn_event_handler(fx.sv[0], KITH_REACTOR_OUT, conn);
    CHECK(conn->fd == -1);
    CHECK(!conn->in_use);
    CHECK(conn->read_buf == nullptr && conn->write_buf == nullptr &&
          conn->header_scratch == nullptr);

    fixture_close(&fx);
    return failures;
}

// The fixed-size response writers respect the caller-configured write
// buffer cap: at a cap too small for the response, len stays inside the
// buffer (zero — nothing publishable), and at a sane cap the response
// lands whole. Any overflow is an out-of-bounds write under ASan.
static int test_response_writers_respect_cap(void)
{
    int failures = 0;

    // Cap 32: every fixed-size response is larger, so nothing is written.
    struct conn_fixture tiny;
    CHECK(fixture_open(&tiny, 4096u, 32u) == 0);
    if (tiny.conn != nullptr)
    {
        struct kith_control_conn *conn = tiny.conn;
        control_router_write_404(conn);
        CHECK(conn->resp.len == 0u);
        control_router_write_501(conn);
        CHECK(conn->resp.len == 0u);
        control_router_write_503(conn);
        CHECK(conn->resp.len == 0u);
        CHECK(control_handler_events_stream(nullptr, &conn->resp, tiny.ctrl) == 0);
        CHECK(conn->resp.len == 0u);
        CHECK(!conn->resp.sse);
        CHECK(control_handler_logs_stream(nullptr, &conn->resp, tiny.ctrl) == 0);
        CHECK(conn->resp.len == 0u);
        CHECK(!conn->resp.sse);
        fixture_close(&tiny);
    }

    // Sane cap: the responses land whole and the SSE subscription takes.
    struct conn_fixture sane;
    CHECK(fixture_open(&sane, 4096u, 4096u) == 0);
    if (sane.conn != nullptr)
    {
        struct kith_control_conn *conn = sane.conn;
        control_router_write_404(conn);
        CHECK(conn->resp.len > 0u && conn->resp.len < conn->resp.cap);
        CHECK(strstr((const char *)conn->resp.buf, "404 Not Found") != nullptr);
        control_router_write_503(conn);
        CHECK(conn->resp.len > 0u && conn->resp.len < conn->resp.cap);
        CHECK(strstr((const char *)conn->resp.buf, "503 Service Unavailable") != nullptr);
        CHECK(control_handler_events_stream(nullptr, &conn->resp, sane.ctrl) == 0);
        CHECK(conn->resp.len == strlen("HTTP/1.1 200 OK\r\n"
                                       "Content-Type: text/event-stream\r\n"
                                       "Cache-Control: no-cache\r\n"
                                       "Connection: keep-alive\r\n\r\n"));
        CHECK(conn->resp.sse);
        fixture_close(&sane);
    }
    return failures;
}

// An EINTR on the first drain attempt leaves the transfer unstarted: the
// slot stays open in WRITING state with every buffer held, and the next
// readiness drive completes the response byte-for-byte.
static int test_write_eintr_kept_open(void)
{
    int failures = 0;
    struct conn_fixture fx;
    CHECK(fixture_open(&fx, 4096u, 2048u) == 0);
    if (fx.conn == nullptr)
    {
        return failures;
    }
    struct kith_control_conn *conn = fx.conn;
    fill_pattern(conn->write_buf, 64u);
    conn->resp.buf = conn->write_buf;
    conn->resp.cap = conn->write_cap;
    conn->resp.len = 64u;
    conn->write_pos = 0u;
    conn->state = KITH_CONTROL_CONN_WRITING;

    atomic_store(&g_inject_eintr, 1);
    control_conn_event_handler(fx.sv[0], KITH_REACTOR_OUT, conn);
    CHECK(conn->fd >= 0);
    CHECK(conn->in_use);
    CHECK(conn->state == KITH_CONTROL_CONN_WRITING);
    CHECK(conn->write_pos == 0u);
    CHECK(conn->read_buf != nullptr && conn->write_buf != nullptr &&
          conn->header_scratch != nullptr);

    for (int spin = 0;
         spin < EVENT_DRIVE_ATTEMPTS && conn->fd >= 0 && conn->write_pos < conn->resp.len;
         spin++)
    {
        control_conn_event_handler(fx.sv[0], KITH_REACTOR_OUT, conn);
    }

    CHECK(conn->fd == -1);
    CHECK(!conn->in_use);
    uint32_t delivered = 0u;
    CHECK(drain_and_verify(fx.sv[1], &delivered, 64u) == 0);
    CHECK(delivered == 64u);

    fixture_close(&fx);
    return failures;
}

/* Publish one event into the scenario bus. */
static int publish_tick(struct kith_control *ctrl, const char *type, uint64_t ts)
{
    kith_control_event_t event = {.ts_mono_ns = ts, .type = type};
    return control_event_bus_publish(&ctrl->event_bus, &event);
}

/* One SSE delivery attempt against a subscribed slot: two published events
 * leave as two data lines on the first flush, the cursor advances past
 * them, the connection stays open in SSE state, and the second flush
 * delivers nothing. Returns assertion failures, or 0 with @p verified
 * false on a spurious host-level fault (caller retries). */
static int sse_delivery_attempt(bool *verified)
{
    *verified = false;
    int failures = 0;
    struct conn_fixture fx;
    if (fixture_open(&fx, 4096u, 4096u) != 0)
    {
        return 1;
    }
    fx.conn->resp.sse = true;
    fx.conn->sse_subscribed = true;
    fx.conn->sse_cursor = 0u;

    CHECK(publish_tick(fx.ctrl, "evt.one", 1111u) == 0);
    CHECK(publish_tick(fx.ctrl, "evt.two", 2222u) == 0);

    control_sse_flush(fx.ctrl);

    if (fx.conn->fd < 0)
    {
        fixture_close(&fx);
        return 0;
    }

    CHECK(fx.conn->fd == fx.sv[0]);
    CHECK(fx.conn->sse_cursor == 2u);
    CHECK(fx.conn->state == KITH_CONTROL_CONN_SSE);
    // The flush releases the drained records: the tail follows the only
    // subscriber's cursor.
    CHECK(atomic_load_explicit(&fx.ctrl->event_bus.tail, memory_order_acquire) == 2u);

    char data[2048] = {0};
    size_t have = 0u;
    while (have + 1u < sizeof(data))
    {
        ssize_t n = recv(fx.sv[1], data + have, sizeof(data) - 1u - have, MSG_DONTWAIT);
        if (n <= 0)
        {
            if (n < 0 && errno == EINTR)
            {
                continue;
            }
            break;
        }
        have += (size_t)n;
    }
    data[have] = '\0';
    CHECK(have > 0u);
    CHECK(count_occurrences(data, "data:") == 2);
    CHECK(strstr(data, "\"type\":\"evt.one\"") != nullptr &&
          strstr(data, "\"ts\":1111") != nullptr);
    CHECK(strstr(data, "\"type\":\"evt.two\"") != nullptr &&
          strstr(data, "\"ts\":2222") != nullptr);

    control_sse_flush(fx.ctrl);
    uint8_t probe[64];
    ssize_t n = recv(fx.sv[1], probe, sizeof(probe), MSG_DONTWAIT);
    CHECK(n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK));

    *verified = (failures == 0);
    fixture_close(&fx);
    return failures;
}

// The periodic SSE flush converts bus records into data lines, advances
// the subscriber cursor, reschedules itself, and delivers nothing new
// once the bus is drained to the cursor.
static int test_sse_flush_delivery(void)
{
    int failures = 0;
    bool verified = false;
    for (int attempt = 0; attempt < 3 && !verified; attempt++)
    {
        failures += sse_delivery_attempt(&verified);
    }
    CHECK(verified);
    return failures;
}

// A data line at least as large as the write buffer is skipped whole:
// no bytes reach the socket, the subscriber stays open, and the cursor
// still advances past the dropped record.
static int test_sse_flush_skips_oversized_line(void)
{
    int failures = 0;
    struct conn_fixture fx;
    CHECK(fixture_open(&fx, 4096u, 32u) == 0);
    if (fx.conn == nullptr)
    {
        return failures;
    }
    fx.conn->resp.sse = true;
    fx.conn->sse_subscribed = true;
    fx.conn->sse_cursor = 0u;

    char big_type[CONTROL_EVENT_MAX_TYPE + 16];
    memset(big_type, 'w', sizeof(big_type) - 1u);
    big_type[sizeof(big_type) - 1u] = '\0';
    CHECK(publish_tick(fx.ctrl, big_type, 3333u) == 0);

    control_sse_flush(fx.ctrl);

    CHECK(fx.conn->fd == fx.sv[0]);
    CHECK(fx.conn->in_use);
    CHECK(fx.conn->sse_cursor == 1u);
    CHECK(atomic_load_explicit(&fx.ctrl->event_bus.tail, memory_order_acquire) == 1u);
    uint8_t probe[64];
    ssize_t n = recv(fx.sv[1], probe, sizeof(probe), MSG_DONTWAIT);
    CHECK(n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK));

    fixture_close(&fx);
    return failures;
}

// An EWOULDBLOCK mid-line parks the in-flight SSE line on the write path
// instead of letting the next record overwrite the undrained tail: the
// buffered bytes survive, the subscription cursor rewinds past the parked
// line, subsequent flush ticks skip the parked connection, and OUT readiness
// completes the line before a resumed tick delivers the remaining record
// exactly once.
static int test_sse_flush_stall_parks_without_clobber(void)
{
    int failures = 0;
    struct conn_fixture fx;
    CHECK(fixture_open(&fx, 4096u, 4096u) == 0);
    if (fx.conn == nullptr)
    {
        return failures;
    }
    struct kith_control_conn *conn = fx.conn;
    conn->resp.sse = true;
    conn->sse_subscribed = true;
    conn->sse_cursor = 0u;

    CHECK(publish_tick(fx.ctrl, "evt.one", 1111u) == 0);
    CHECK(publish_tick(fx.ctrl, "evt.two", 2222u) == 0);

    atomic_store(&g_inject_eagain, 1);
    control_sse_flush(fx.ctrl);
    const char *line_one = "data: {\"type\":\"evt.one\",\"ts\":1111}\n\n";
    CHECK(conn->fd >= 0);
    CHECK(conn->in_use);
    CHECK(conn->state == KITH_CONTROL_CONN_WRITING);
    CHECK(conn->write_pos == 0u);
    CHECK(conn->resp.len == strlen(line_one));
    CHECK(memcmp(conn->write_buf, line_one, strlen(line_one)) == 0);
    CHECK(conn->sse_cursor == 1u);
    // The parked line's rewound cursor holds the ring: the tail releases
    // up to it and no further.
    CHECK(atomic_load_explicit(&fx.ctrl->event_bus.tail, memory_order_acquire) == 1u);

    // Subsequent flush ticks skip the parked connection entirely.
    control_sse_flush(fx.ctrl);
    CHECK(conn->state == KITH_CONTROL_CONN_WRITING);
    CHECK(conn->sse_cursor == 1u);
    CHECK(memcmp(conn->write_buf, line_one, strlen(line_one)) == 0);
    CHECK(atomic_load_explicit(&fx.ctrl->event_bus.tail, memory_order_acquire) == 1u);

    // OUT readiness drains the parked tail; completion restores SSE state.
    for (int spin = 0;
         spin < EVENT_DRIVE_ATTEMPTS && conn->fd >= 0 && conn->state == KITH_CONTROL_CONN_WRITING;
         spin++)
    {
        control_conn_event_handler(fx.sv[0], KITH_REACTOR_OUT, conn);
    }
    CHECK(conn->fd >= 0);
    CHECK(conn->state == KITH_CONTROL_CONN_SSE);

    // The resumed tick delivers the remaining record exactly once.
    control_sse_flush(fx.ctrl);
    CHECK(conn->state == KITH_CONTROL_CONN_SSE);
    CHECK(conn->sse_cursor == 2u);
    CHECK(atomic_load_explicit(&fx.ctrl->event_bus.tail, memory_order_acquire) == 2u);

    char data[2048] = {0};
    CHECK(recv_queued(fx.sv[1], data, sizeof(data)) > 0u);
    const char *one = strstr(data, "\"type\":\"evt.one\"");
    const char *two = strstr(data, "\"type\":\"evt.two\"");
    CHECK(one != nullptr && two != nullptr);
    CHECK(one < two);
    CHECK(count_occurrences(data, "evt.one") == 1);
    CHECK(count_occurrences(data, "evt.two") == 1);

    fixture_close(&fx);
    return failures;
}

// ---------------------------------------------------------------------------
// loopback listener
// ---------------------------------------------------------------------------

static int listener_open(uint16_t *out_port)
{
    int fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
    if (fd < 0)
    {
        return -1;
    }
    int opt = 1;
    (void)setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0u;
    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0 || listen(fd, 8) != 0)
    {
        (void)close(fd);
        return -1;
    }
    socklen_t addr_len = sizeof(addr);
    if (getsockname(fd, (struct sockaddr *)&addr, &addr_len) != 0)
    {
        (void)close(fd);
        return -1;
    }
    *out_port = ntohs(addr.sin_port);
    return fd;
}

static int connect_loopback(uint16_t port)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0)
    {
        return -1;
    }
    struct timeval tv = {.tv_sec = 2, .tv_usec = 0};
    (void)setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = htons(port);
    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0)
    {
        int err = errno;
        if (err == EINTR)
        {
            /* An interrupted connect keeps progressing asynchronously:
             * writability carries the outcome via SO_ERROR. */
            struct pollfd pfd = {.fd = fd, .events = POLLOUT, .revents = 0};
            int ready;
            do
            {
                ready = poll(&pfd, 1, 2000);
            } while (ready < 0 && errno == EINTR);
            err = ETIMEDOUT;
            if (ready > 0)
            {
                socklen_t err_len = sizeof(err);
                (void)getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &err_len);
            }
        }
        if (err != 0)
        {
            (void)close(fd);
            return -1;
        }
    }
    return fd;
}

/* Poll for readability, then observe the close: clean EOF or ECONNRESET
 * both mean the server shut the accepted descriptor. Unexpected payload
 * keeps the wait going within bounds. */
static int peer_observes_close(int fd)
{
    uint8_t sink[256];
    for (int round = 0; round < 16; round++)
    {
        struct pollfd pfd = {.fd = fd, .events = POLLIN, .revents = 0};
        int ready;
        do
        {
            ready = poll(&pfd, 1, 2000);
        } while (ready < 0 && errno == EINTR);
        if (ready <= 0)
        {
            return -1;
        }
        ssize_t n = recv(fd, sink, sizeof(sink), 0);
        if (n == 0)
        {
            return 0;
        }
        if (n < 0)
        {
            if (errno == EINTR)
            {
                continue;
            }
            if (errno == ECONNRESET)
            {
                return 0;
            }
            return -1;
        }
    }
    return -1;
}

// With every slot occupied, the accept drain closes each pending client
// immediately and leaves the occupied slots untouched.
static int test_accept_table_full(void)
{
    int failures = 0;
    struct kith_control *ctrl = ctrl_build(1u, 4096u, 2048u);
    CHECK(ctrl != nullptr);
    if (ctrl == nullptr)
    {
        return failures;
    }
    struct kith_control_conn *occupied = control_conn_alloc(ctrl);
    CHECK(occupied != nullptr);
    if (occupied == nullptr)
    {
        ctrl_teardown(ctrl);
        return failures;
    }
    CHECK(ctrl->conn_count == 1u);

    uint16_t port = 0u;
    int lfd = listener_open(&port);
    CHECK(lfd >= 0);
    if (lfd < 0)
    {
        ctrl_teardown(ctrl);
        return failures;
    }
    /* handle_accept drains ctrl->listener_fd, so the fixture stores the
     * descriptor where the started server holds it. */
    ctrl->listener_fd = lfd;

    int cli_a = connect_loopback(port);
    int cli_b = connect_loopback(port);
    CHECK(cli_a >= 0);
    CHECK(cli_b >= 0);

    control_listener_event_handler(lfd, 0u, ctrl);

    if (cli_a >= 0)
    {
        CHECK(peer_observes_close(cli_a) == 0);
        (void)close(cli_a);
    }
    if (cli_b >= 0)
    {
        CHECK(peer_observes_close(cli_b) == 0);
        (void)close(cli_b);
    }

    CHECK(occupied->in_use);
    CHECK(occupied->fd == -1);
    CHECK(occupied->read_buf != nullptr && occupied->write_buf != nullptr &&
          occupied->header_scratch != nullptr);
    CHECK(ctrl->conn_count == 1u);

    (void)close(lfd);
    ctrl_teardown(ctrl);
    return failures;
}

// The route worker task honors the interpreter exit gate: with the gate
// set, a python-bound route handler is never invoked and the connection
// stays in DISPATCHING (its buffers are freed directly by destroy); with
// the gate clear, the handler runs and fills the response buffer.
struct route_capture
{
    _Atomic uint32_t calls;
};

static int route_capture_handler(const struct kith_control_request *req,
                                 struct kith_control_response *resp,
                                 void *ctx)
{
    (void)req;
    struct route_capture *c = ctx;
    atomic_fetch_add_explicit(&c->calls, 1u, memory_order_relaxed);
    resp->len = (uint32_t)snprintf((char *)resp->buf, resp->cap, "ok");
    return 0;
}

static int test_route_worker_task_exit_gate(void)
{
    int failures = 0;
    struct conn_fixture fx;
    CHECK(fixture_open(&fx, 4096u, 2048u) == 0);
    if (fx.conn == nullptr)
    {
        return failures;
    }
    struct kith_control_conn *conn = fx.conn;
    struct kith_control_route_entry route = {0};
    (void)snprintf(route.method, sizeof(route.method), "GET");
    (void)snprintf(route.path, sizeof(route.path), "/gate");
    route.handler = route_capture_handler;
    route.ctx = nullptr;

    struct route_capture cap = {0};
    atomic_init(&cap.calls, 0u);
    route.ctx = &cap;

    conn->state = KITH_CONTROL_CONN_DISPATCHING;
    conn->work.ctrl = fx.ctrl;
    conn->work.conn = conn;
    conn->work.route = &route;

    // Gate set: the entry is refused, the handler never runs, the connection
    // stays in DISPATCHING with its buffers intact.
    kith_python_finalizing_set(1);
    control_route_worker_task(&conn->work);
    CHECK(atomic_load_explicit(&cap.calls, memory_order_acquire) == 0u);
    CHECK(conn->state == KITH_CONTROL_CONN_DISPATCHING);
    CHECK(conn->resp.len == 0u);
    CHECK(conn->in_use && conn->read_buf != nullptr && conn->write_buf != nullptr);
    kith_python_finalizing_set(0);

    // Gate clear: the handler runs and fills the response buffer.
    control_route_worker_task(&conn->work);
    CHECK(atomic_load_explicit(&cap.calls, memory_order_acquire) == 1u);
    CHECK(conn->resp.len > 0u);

    fixture_close(&fx);
    return failures;
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------
int main(void)
{
    (void)signal(SIGPIPE, SIG_IGN);

    /* One reactor serves every scenario; the events are synthesized
     * directly, so the loop never runs. Creation backs off on the
     * transient ring-memory reclaim window the host applies to
     * io_uring setup. */
    int rc = 0;
    for (unsigned int attempt = 0u; attempt < 60u && g_reactor == nullptr; attempt++)
    {
        if (kith_reactor_create(nullptr, nullptr, &g_reactor) == 0)
        {
            break;
        }
        struct timespec pause = {.tv_sec = 0, .tv_nsec = 25 * 1000000L};
        (void)nanosleep(&pause, nullptr);
    }
    if (g_reactor == nullptr)
    {
        (void)fprintf(stderr, "control conn: reactor create failed\n");
        return 1;
    }

    rc |= test_alloc_table_full();
    rc |= test_alloc_routes_through_handle_allocator();
    rc |= test_alloc_enomem_unwinds();
    rc |= test_double_close_noop();
    rc |= test_close_mid_dispatch_defers();
    rc |= test_event_fault_closes_immediately();
    rc |= test_event_fault_during_dispatch();
    rc |= test_read_eof_closes();
    rc |= test_read_overflow_guard_closes();
    rc |= test_write_partial_eagain();
    rc |= test_response_writers_respect_cap();
    rc |= test_write_eintr_kept_open();
    rc |= test_write_epipe_closes();
    rc |= test_sse_flush_delivery();
    rc |= test_sse_flush_skips_oversized_line();
    rc |= test_sse_flush_stall_parks_without_clobber();
    rc |= test_accept_table_full();
    rc |= test_route_worker_task_exit_gate();

    kith_reactor_destroy(g_reactor);
    g_reactor = nullptr;

    if (rc != 0)
    {
        (void)fprintf(stderr, "control conn tests FAILED\n");
    }
    return rc;
}
