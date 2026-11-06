/* Churn behavior of the reactor's fd registration map: deletions leave
 * tombstones that the insert path reuses, so add/del turnover far beyond the
 * map capacity neither exhausts the table nor degrades probe distances. The
 * sweep test reads the map directly through the internal handle to pin the
 * tombstone accounting and the per-fd probe distance. */

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <fcntl.h>
#include <unistd.h>

#include "kith/reactor/reactor.h"
#include "kith/types.h"
#include "kith/version.h"
#include "reactor/reactor_internal.h"

#include <sys/socket.h>

// ---------------------------------------------------------------------------
// test harness
// ---------------------------------------------------------------------------

static int check_cond(bool ok, int line)
{
    if (!ok)
    {
        (void)fprintf(stderr, "reactor fd churn: assertion at line %d failed\n", line);
        return 1;
    }
    return 0;
}

#define CHECK(cond) failures += check_cond((cond), __LINE__)

static void churn_noop_cb(int fd, unsigned int events, void *ctx)
{
    (void)fd;
    (void)events;
    (void)ctx;
}

// max_fds bounds the backend's per-fd array (every registered fd number must
// stay below it) and sizes the map at the next power of two above 2*max_fds,
// so 1024 leaves room for the ~252 descriptors the pool opens and keeps the
// io_uring ring small.
#define CHURN_MAX_FDS 1024u

static int reactor_create_churn(kith_reactor_t **out)
{
    kith_reactor_params_t params = {
        .size = sizeof(kith_reactor_params_t),
        .abi_version = KITH_ABI_VERSION,
        .max_fds = CHURN_MAX_FDS,
        .task_capacity = 64,
    };
    return kith_reactor_create(&params, nullptr, out);
}

// Open pair_count socketpairs; every descriptor number lands below
// CHURN_MAX_FDS, which the backend requires of registered fds.
static int open_pool(int pair_count, int *pool, int pool_cap)
{
    if (2 * pair_count > pool_cap)
    {
        return -1;
    }
    int used = 0;
    for (int i = 0; i < pair_count; i++)
    {
        int pair[2];
        if (socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, pair) != 0)
        {
            return -1;
        }
        pool[used++] = pair[0];
        pool[used++] = pair[1];
        if (pair[0] >= (int)CHURN_MAX_FDS || pair[1] >= (int)CHURN_MAX_FDS)
        {
            return -1;
        }
    }
    return used;
}

static void close_pool(const int *pool, int used)
{
    for (int i = 0; i < used; i++)
    {
        (void)close(pool[i]);
    }
}

static unsigned int tombstone_count(const kith_reactor_t *reactor)
{
    unsigned int count = 0u;
    for (unsigned int i = 0; i < reactor->fd_capacity; i++)
    {
        if (reactor->fd_entries[i].fd == -2)
        {
            count++;
        }
    }
    return count;
}

static unsigned int empty_count(const kith_reactor_t *reactor)
{
    unsigned int count = 0u;
    for (unsigned int i = 0; i < reactor->fd_capacity; i++)
    {
        if (reactor->fd_entries[i].fd == -1)
        {
            count++;
        }
    }
    return count;
}

// Walk the probe sequence exactly as dispatch lookup does and report how many
// slots it examines before terminating at the fd, an empty slot, or exhaustion.
static unsigned int probe_steps(const kith_reactor_t *reactor, int fd)
{
    unsigned int mask = reactor->fd_capacity - 1u;
    for (unsigned int i = 0; i < reactor->fd_capacity; i++)
    {
        unsigned int slot = ((unsigned int)fd + i) & mask;
        if (reactor->fd_entries[slot].fd == fd || reactor->fd_entries[slot].fd == -1)
        {
            return i + 1u;
        }
    }
    return reactor->fd_capacity;
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

// Register and deregister one descriptor repeatedly. Every cycle must reuse
// the vacated slot: the turnover here is several times the map capacity, so
// an insert path that burns one slot per deletion exhausts the table and
// reports kith_error_return(KITH_EBUSY) long before the loop ends.
static int test_del_readd_same_fd(void)
{
    int failures = 0;
    kith_reactor_t *reactor = nullptr;
    CHECK(reactor_create_churn(&reactor) == 0);

    int pool[2] = {-1, -1};
    CHECK(open_pool(1, pool, 2) == 2);
    const int fd = pool[0];
    if (fd < 0)
    {
        kith_reactor_destroy(reactor);
        return failures;
    }

    for (int cycle = 0; cycle < 4000; cycle++)
    {
        CHECK(kith_reactor_add(reactor, fd, KITH_REACTOR_IN, churn_noop_cb, nullptr) == 0);
        CHECK(kith_reactor_del(reactor, fd) == 0);
    }

    close_pool(pool, 2);
    kith_reactor_destroy(reactor);
    return failures;
}

// Keep half of a 252-descriptor pool registered while rotating which half,
// for far more steps than the map holds slots. Registrations must keep
// succeeding and lookups must keep resolving: the churn reclaims tombstones
// instead of accumulating them.
static int test_rotation_churn(void)
{
    int failures = 0;
    kith_reactor_t *reactor = nullptr;
    CHECK(reactor_create_churn(&reactor) == 0);

    int pool[252];
    int used = open_pool(126, pool, 252);
    CHECK(used == 252);
    if (used != 252)
    {
        kith_reactor_destroy(reactor);
        return failures;
    }

    const int window = used / 2;
    for (int k = 0; k < window; k++)
    {
        CHECK(kith_reactor_add(reactor, pool[k], KITH_REACTOR_IN, churn_noop_cb, nullptr) == 0);
    }

    // Each step drops the oldest descriptor in the window and registers the
    // one that fell out of the window `window` steps ago, so every add names
    // a descriptor that is not currently registered.
    for (int step = 0; step < 20000; step++)
    {
        int leaving = pool[step % used];
        int entering = pool[(step + window) % used];
        CHECK(kith_reactor_del(reactor, leaving) == 0);
        CHECK(kith_reactor_add(reactor, entering, KITH_REACTOR_IN, churn_noop_cb, nullptr) == 0);
    }

    // The map still resolves live and absent descriptors.
    CHECK(kith_reactor_mod(reactor, pool[20000 % used], KITH_REACTOR_IN) == 0);

    for (int k = 0; k < used; k++)
    {
        (void)kith_reactor_del(reactor, pool[k]);
    }

    close_pool(pool, used);
    kith_reactor_destroy(reactor);
    return failures;
}

// A full sweep leaves one tombstone per deleted registration; re-registering
// the same descriptors in the same order reclaims every one of them. The
// re-add lands in each descriptor's own home slot because all pool numbers
// are distinct modulo the map capacity and every slot is a tombstone at
// sweep end, so the reclaimed layout matches a fresh fill exactly: zero
// tombstones and a one-slot probe for every descriptor.
static int test_sweep_reclaim(void)
{
    int failures = 0;
    kith_reactor_t *reactor = nullptr;
    CHECK(reactor_create_churn(&reactor) == 0);

    int pool[252];
    int used = open_pool(126, pool, 252);
    CHECK(used == 252);
    if (used != 252)
    {
        kith_reactor_destroy(reactor);
        return failures;
    }

    for (int k = 0; k < used; k++)
    {
        CHECK(kith_reactor_add(reactor, pool[k], KITH_REACTOR_IN, churn_noop_cb, nullptr) == 0);
    }
    CHECK(reactor->fd_count == (unsigned int)used);
    CHECK(tombstone_count(reactor) == 0u);

    for (int k = 0; k < used; k++)
    {
        CHECK(kith_reactor_del(reactor, pool[k]) == 0);
    }
    CHECK(reactor->fd_count == 0u);
    CHECK(tombstone_count(reactor) == (unsigned int)used);
    CHECK(empty_count(reactor) == reactor->fd_capacity - (unsigned int)used);

    for (int k = 0; k < used; k++)
    {
        CHECK(kith_reactor_add(reactor, pool[k], KITH_REACTOR_IN, churn_noop_cb, nullptr) == 0);
    }
    CHECK(reactor->fd_count == (unsigned int)used);
    CHECK(tombstone_count(reactor) == 0u);
    CHECK(empty_count(reactor) == reactor->fd_capacity - (unsigned int)used);

    // Reclaimed layout: every descriptor sits in its own home slot, so the
    // dispatch probe for a live fd and the scan for an absent fd both end
    // after one slot.
    for (int k = 0; k < used; k++)
    {
        CHECK(probe_steps(reactor, pool[k]) == 1u);
    }
    int absent_pair[2];
    CHECK(socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, absent_pair) == 0);
    CHECK(absent_pair[0] < (int)CHURN_MAX_FDS);
    CHECK(probe_steps(reactor, absent_pair[0]) == 1u);
    CHECK(kith_reactor_mod(reactor, absent_pair[0], KITH_REACTOR_IN) ==
          kith_error_return(KITH_ENOENT));
    (void)close(absent_pair[0]);
    (void)close(absent_pair[1]);

    for (int k = 0; k < used; k++)
    {
        CHECK(kith_reactor_del(reactor, pool[k]) == 0);
    }

    close_pool(pool, used);
    kith_reactor_destroy(reactor);
    return failures;
}

#ifdef __linux__
struct retirement_probe
{
    kith_reactor_t *reactor;
    unsigned int ready;
    unsigned int tasks;
    bool timed_out;
    bool read_failed;
};

static struct io_uring_sqe *deny_sqe(struct io_uring *ring)
{
    (void)ring;
    return nullptr;
}

static int defer_submit(struct io_uring *ring)
{
    (void)ring;
    return -EIO;
}

static int defer_poll_submit(struct io_uring *ring)
{
    struct io_uring_sq *sq = &ring->sq;
    struct io_uring_sqe *sqe = &sq->sqes[(sq->sqe_tail - 1u) & sq->ring_mask];
    return sqe->opcode == IORING_OP_POLL_ADD ? -EIO : io_uring_submit(ring);
}

static void retirement_ready(int fd, unsigned int events, void *ctx)
{
    struct retirement_probe *probe = ctx;
    char byte;
    probe->ready++;
    probe->read_failed |= !(events & KITH_REACTOR_IN) || read(fd, &byte, 1) != 1;
    kith_reactor_stop(probe->reactor);
}

static void retirement_task(void *ctx)
{
    struct retirement_probe *probe = ctx;
    probe->tasks++;
    kith_reactor_stop(probe->reactor);
}

static void retirement_count_task(void *ctx)
{
    struct retirement_probe *probe = ctx;
    probe->tasks++;
}

static void retirement_timeout(void *ctx)
{
    struct retirement_probe *probe = ctx;
    probe->timed_out = true;
    kith_reactor_stop(probe->reactor);
}

static int run_retirement_probe(struct retirement_probe *probe)
{
    int failures = 0;
    struct timespec now;
    CHECK(clock_gettime(CLOCK_MONOTONIC, &now) == 0);
    uint64_t deadline = (uint64_t)now.tv_sec * 1000u + (uint64_t)now.tv_nsec / 1000000u + 200u;
    CHECK(kith_reactor_schedule(probe->reactor, deadline, retirement_timeout, probe) == 0);
    CHECK(kith_reactor_run(probe->reactor) == 0);
    CHECK(!probe->timed_out);
    CHECK(!probe->read_failed);
    return failures;
}

static struct kith_fd_entry *find_registration(kith_reactor_t *reactor, int fd)
{
    for (unsigned int i = 0; i < reactor->fd_capacity; i++)
    {
        if (reactor->fd_entries[i].fd == fd)
        {
            return &reactor->fd_entries[i];
        }
    }
    return nullptr;
}

static void count_backend_dispatch(int fd, unsigned int events, void *ctx)
{
    (void)fd;
    (void)events;
    (*(unsigned int *)ctx)++;
}

static int check_pending_cancel(struct uring_backend *backend, int fd, uint32_t gen)
{
    int failures = 0;
    CHECK(io_uring_sq_ready(&backend->ring) != 0u);
    struct io_uring_sq *sq = &backend->ring.sq;
    struct io_uring_sqe *sqe = &sq->sqes[(sq->sqe_tail - 1u) & sq->ring_mask];
    CHECK(sqe->opcode == IORING_OP_ASYNC_CANCEL);
    CHECK(sqe->addr == (((uint64_t)gen << 32) | (uint32_t)fd));
    CHECK(sqe->cancel_flags == 0u);
    return failures;
}

// Admission and submission errors end both callback ownership and backend
// activity. A pending cancellation names only the retired poll identity, and
// accepted tasks and a replacement registration continue to run.
static int test_retirement_error(bool submission_error)
{
    int failures = 0;
    kith_reactor_t *reactor = nullptr;
    CHECK(reactor_create_churn(&reactor) == 0);
    if (reactor == nullptr)
    {
        return failures;
    }
    int pool[2] = {-1, -1};
    CHECK(open_pool(1, pool, 2) == 2);
    CHECK(fcntl(pool[0], F_SETFL, O_NONBLOCK) == 0);
    struct retirement_probe old = {.reactor = reactor};
    struct retirement_probe replacement = {.reactor = reactor};
    CHECK(kith_reactor_add(reactor, pool[0], KITH_REACTOR_IN, retirement_ready, &old) == 0);
    if (submission_error)
    {
        CHECK(kith_reactor_submit(reactor, retirement_count_task, &old) == 0);
    }
    struct kith_fd_entry *entry = find_registration(reactor, pool[0]);
    struct uring_backend *backend = reactor->backend_data;
    uint32_t gen = backend->fd_entries[pool[0]].gen;
    auto get_sqe = backend->get_sqe;
    auto submit = backend->submit;
    backend->get_sqe = submission_error ? get_sqe : deny_sqe;
    backend->submit = submission_error ? defer_submit : submit;
    CHECK(kith_reactor_del(reactor, pool[0]) == kith_error_return(KITH_EIO));
    CHECK(entry != nullptr && entry->fd == -2 && entry->cb == nullptr && entry->ctx == nullptr);
    CHECK(reactor->fd_count == 0u && backend->fd_entries[pool[0]].fd == -1);
    CHECK(kith_reactor_del(reactor, pool[0]) == kith_error_return(KITH_ENOENT));
    backend->get_sqe = get_sqe;
    backend->submit = submit;
    if (submission_error)
    {
        failures += check_pending_cancel(backend, pool[0], gen);
    }
    else
    {
        CHECK(write(pool[1], "x", 1) == 1);
        unsigned int dispatched = 0u;
        unsigned int tail = backend->ring.sq.sqe_tail;
        CHECK(reactor->backend.wait(backend, 20, count_backend_dispatch, &dispatched) == 0);
        CHECK(dispatched == 0u && backend->ring.sq.sqe_tail == tail);
        CHECK(kith_reactor_submit(reactor, retirement_task, &old) == 0);
        failures += run_retirement_probe(&old);
        CHECK(old.tasks == 1u);
    }
    CHECK(kith_reactor_add(reactor, pool[0], KITH_REACTOR_IN, retirement_ready, &replacement) == 0);
    CHECK(backend->fd_entries[pool[0]].gen > gen);
    if (submission_error)
    {
        CHECK(write(pool[1], "x", 1) == 1);
    }
    failures += run_retirement_probe(&replacement);
    CHECK(old.ready == 0u && old.tasks == 1u && replacement.ready == 1u);
    CHECK(kith_reactor_del(reactor, pool[0]) == 0);
    close_pool(pool, 2);
    kith_reactor_destroy(reactor);
    return failures;
}

// A failed add still consumes its prepared poll identity. A failed mod's
// cancellation may submit after deletion/re-addition, but names the old poll.
static int test_pending_identities(bool defer_new_poll)
{
    int failures = 0;
    kith_reactor_t *reactor = nullptr;
    CHECK(reactor_create_churn(&reactor) == 0);
    if (reactor == nullptr)
    {
        return failures;
    }
    int pool[2] = {-1, -1};
    CHECK(open_pool(1, pool, 2) == 2);
    CHECK(fcntl(pool[0], F_SETFL, O_NONBLOCK) == 0);
    struct retirement_probe probe = {.reactor = reactor};
    struct uring_backend *backend = reactor->backend_data;
    auto submit = backend->submit;
    backend->submit = defer_submit;
    CHECK(kith_reactor_add(reactor, pool[0], KITH_REACTOR_IN, retirement_ready, &probe) ==
          kith_error_return(KITH_EIO));
    uint32_t failed_gen = backend->fd_entries[pool[0]].last_gen;
    CHECK(failed_gen != 0u && backend->fd_entries[pool[0]].fd == -1);
    CHECK(reactor->fd_count == 0u && io_uring_sq_ready(&backend->ring) != 0u);
    backend->submit = submit;
    CHECK(kith_reactor_add(reactor, pool[0], KITH_REACTOR_IN, retirement_ready, &probe) == 0);
    uint32_t active_gen = backend->fd_entries[pool[0]].gen;
    CHECK(active_gen > failed_gen);
    backend->submit = defer_new_poll ? defer_poll_submit : defer_submit;
    CHECK(kith_reactor_mod(reactor, pool[0], KITH_REACTOR_OUT) == kith_error_return(KITH_EIO));
    CHECK(backend->fd_entries[pool[0]].gen == active_gen);
    if (defer_new_poll)
    {
        CHECK(backend->fd_entries[pool[0]].last_gen > active_gen);
        CHECK(io_uring_sq_ready(&backend->ring) != 0u);
    }
    else
    {
        failures += check_pending_cancel(backend, pool[0], active_gen);
    }
    uint32_t last_gen = backend->fd_entries[pool[0]].last_gen;
    backend->submit = defer_submit;
    CHECK(kith_reactor_del(reactor, pool[0]) == kith_error_return(KITH_EIO));
    backend->submit = submit;
    CHECK(kith_reactor_add(reactor, pool[0], KITH_REACTOR_IN, retirement_ready, &probe) == 0);
    CHECK(backend->fd_entries[pool[0]].gen > last_gen);
    CHECK(write(pool[1], "x", 1) == 1);
    failures += run_retirement_probe(&probe);
    CHECK(probe.ready == 1u);
    CHECK(kith_reactor_del(reactor, pool[0]) == 0);
    close_pool(pool, 2);
    kith_reactor_destroy(reactor);
    return failures;
}

// Exhaustion rejects identity-changing operations without wrapping, but leaves
// the last admitted registration usable and unchanged-mask mods successful.
static int test_identity_exhaustion(void)
{
    int failures = 0;
    kith_reactor_t *reactor = nullptr;
    CHECK(reactor_create_churn(&reactor) == 0);
    if (reactor == nullptr)
    {
        return failures;
    }
    int pool[2] = {-1, -1};
    CHECK(open_pool(1, pool, 2) == 2);
    struct uring_backend *backend = reactor->backend_data;
    backend->fd_entries[pool[0]].last_gen = UINT32_MAX - 2u;
    CHECK(kith_reactor_add(reactor, pool[0], KITH_REACTOR_IN, churn_noop_cb, nullptr) == 0);
    CHECK(backend->fd_entries[pool[0]].gen == UINT32_MAX - 1u);
    CHECK(kith_reactor_mod(reactor, pool[0], KITH_REACTOR_IN) == 0);
    CHECK(kith_reactor_mod(reactor, pool[0], KITH_REACTOR_OUT) == kith_error_return(KITH_EBUSY));
    CHECK(backend->fd_entries[pool[0]].gen == UINT32_MAX - 1u);
    CHECK(kith_reactor_del(reactor, pool[0]) == 0);
    CHECK(kith_reactor_add(reactor, pool[0], KITH_REACTOR_IN, churn_noop_cb, nullptr) ==
          kith_error_return(KITH_EBUSY));
    CHECK(backend->fd_entries[pool[0]].last_gen == UINT32_MAX - 1u);
    CHECK(reactor->fd_count == 0u);
    close_pool(pool, 2);
    kith_reactor_destroy(reactor);
    return failures;
}
#endif

int main(void)
{
    int rc = 0;
    rc |= test_del_readd_same_fd();
    rc |= test_rotation_churn();
    rc |= test_sweep_reclaim();
#ifdef __linux__
    rc |= test_retirement_error(false);
    rc |= test_retirement_error(true);
    rc |= test_pending_identities(false);
    rc |= test_pending_identities(true);
    rc |= test_identity_exhaustion();
#endif

    if (rc != 0)
    {
        (void)fprintf(stderr, "reactor fd churn tests FAILED\n");
        return EXIT_FAILURE;
    }
    return EXIT_SUCCESS;
}
