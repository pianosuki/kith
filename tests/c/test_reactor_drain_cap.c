/* Unit tests for the reactor drain cap: a due timer must fire between drain
 * batches of a readiness flood wider than the cap, and every flooded
 * readiness must still dispatch exactly once. */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <fcntl.h>
#include <unistd.h>

#include "kith/reactor/reactor.h"
#include "kith/types.h"
#include "kith/version.h"

// ---------------------------------------------------------------------------
// test harness
// ---------------------------------------------------------------------------

static int check_cond(bool ok, int line)
{
    if (!ok)
    {
        (void)fprintf(stderr, "reactor drain cap: assertion at line %d failed\n", line);
        return 1;
    }
    return 0;
}

#define CHECK(cond) failures += check_cond((cond), __LINE__)

#define FLOOD_FDS 300u

// Mirrors the private URING_DRAIN_CQE_CAP in reactor_uring.c: the due-timer
// position assertion below is only meaningful against this width.
#define DRAIN_CAP_MIRROR 128u

struct flood_order
{
    char seq[FLOOD_FDS + 8u];
    size_t len;
    unsigned reads;
    unsigned timer_fires;
    bool stalled;
};

static void flood_read_cb(int fd, unsigned int events, void *ctx)
{
    struct flood_order *order = (struct flood_order *)ctx;
    (void)events;
    if (!order->stalled)
    {
        // Hold the first completion long enough for the timer wheel's
        // millisecond clock to move past a deadline registered for "now":
        // the drain must still hand control back to the loop within one
        // capped batch instead of finishing the whole flood first.
        order->stalled = true;
        struct timespec stall = {.tv_sec = 0, .tv_nsec = 2 * 1000L * 1000L};
        (void)nanosleep(&stall, nullptr);
    }
    char byte = 0;
    ssize_t n = read(fd, &byte, 1u);
    if (n == 1 && order->len < sizeof(order->seq))
    {
        order->seq[order->len++] = 'r';
        order->reads++;
    }
}

static void flood_timer_cb(void *ctx)
{
    struct flood_order *order = (struct flood_order *)ctx;
    if (order->len < sizeof(order->seq))
    {
        order->seq[order->len++] = 't';
    }
    order->timer_fires++;
}

static void flood_stop_cb(void *ctx)
{
    kith_reactor_stop((kith_reactor_t *)ctx);
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

// A readiness flood wider than the drain cap dispatches in batches with the
// due timer firing after the first capped batch; every readiness still
// dispatches exactly once and the loop exits cleanly on the stop timer.
static int test_drain_cap_lets_timer_interleave_flood(void)
{
    int failures = 0;
    kith_reactor_t *reactor = nullptr;
    int write_fds[FLOOD_FDS] = {0};
    int read_fds[FLOOD_FDS] = {0};

    kith_reactor_params_t params = {
        .size = sizeof(kith_reactor_params_t),
        .abi_version = KITH_ABI_VERSION,
        // 300 registrations ahead of the first drain need a submission ring
        // wide enough to hold them (two submission entries per registration).
        .max_fds = 2048u,
        .task_capacity = 64u,
    };
    CHECK(kith_reactor_create(&params, nullptr, &reactor) == 0);
    CHECK(reactor != nullptr);

    struct flood_order order = {0};

    for (unsigned int i = 0u; i < FLOOD_FDS; i++)
    {
        int fds[2] = {-1, -1};
        CHECK(pipe(fds) == 0);
        read_fds[i] = fds[0];
        write_fds[i] = fds[1];
        CHECK(kith_reactor_add(reactor, fds[0], KITH_REACTOR_IN, flood_read_cb, &order) == 0);
        char byte = 'x';
        CHECK(write(fds[1], &byte, 1u) == 1);
    }

    uint64_t now = kith_reactor_now_ms(reactor);
    CHECK(kith_reactor_schedule(reactor, now, flood_timer_cb, &order) == 0);
    CHECK(kith_reactor_schedule(reactor, now + 200u, flood_stop_cb, reactor) == 0);

    CHECK(kith_reactor_run(reactor) == 0);

    CHECK(order.reads == FLOOD_FDS);
    CHECK(order.timer_fires == 1);
    CHECK(order.len == FLOOD_FDS + 1u);

    size_t timer_index = order.len;
    for (size_t i = 0; i < order.len; i++)
    {
        if (order.seq[i] == 't')
        {
            timer_index = i;
            break;
        }
    }
    // Without the cap the whole flood drains inside the first wait call and
    // the timer fires only after read number 300; the cap hands control back
    // to the loop after the first batch, so the due timer lands within it.
    CHECK(timer_index <= DRAIN_CAP_MIRROR);

    for (unsigned int i = 0u; i < FLOOD_FDS; i++)
    {
        (void)kith_reactor_del(reactor, read_fds[i]);
        (void)close(read_fds[i]);
        (void)close(write_fds[i]);
    }
    kith_reactor_destroy(reactor);
    return failures;
}

int main(void)
{
    int failures = 0;
    failures += test_drain_cap_lets_timer_interleave_flood();
    if (failures == 0)
    {
        (void)printf("reactor drain cap: all tests passed\n");
    }
    return failures == 0 ? 0 : 1;
}
