/* Linux io_uring backend for the reactor: POLL_ADD-based fd readiness with an
 * eventfd for cross-thread wakeup, and a per-fd generation counter in the sqe
 * user_data to drop stale completions against reused fd numbers. Compiled only
 * under __linux__; the cross-platform frontend is reactor.c. */

#ifdef __linux__

#include <errno.h>

#include <liburing.h>
#include <poll.h>
#include <unistd.h>

#include "kith/types.h"
#include "reactor/reactor_internal.h"

#include <sys/eventfd.h>

// ---------------------------------------------------------------------------
// per-fd tracking for the uring backend
// ---------------------------------------------------------------------------
// A deregistered fd's pending POLL_ADD completions remain in the kernel
// completion queue until reaped. The OS reuses small integer fd numbers
// (close(9) then accept() -> 9 again), so a stale CQE for the old fd carries
// the same fd number as the new, reused registration. To keep a stale CQE
// from dispatching against the new registration, every POLL_ADD carries a
// per-fd generation counter in the high 32 bits of its user_data; a CQE whose
// generation does not match the fd's current generation is dropped.
struct uring_fd_entry
{
    int fd;
    unsigned int events; // events-of-interest (KITH_REACTOR_IN, etc.)
    uint32_t gen;        // bumped on each (re)registration; carried in sqe data
};

// ---------------------------------------------------------------------------
// backend data
// ---------------------------------------------------------------------------
struct uring_backend
{
    const kith_allocator_t *alloc;     // as given at init; owns every free
    struct io_uring ring;
    int wake_fd;                       // eventfd for waking io_uring_wait_cqe
    bool wake_fd_added;                // whether the wake fd is registered in the ring

    struct uring_fd_entry *fd_entries; // indexed by fd
    unsigned int fd_capacity;
};

// ---------------------------------------------------------------------------
// CQE user_data sentinels
// Normal fds use the fd value directly as data64; special completions
// use these sentinel values.
// ---------------------------------------------------------------------------
enum : uint64_t
{
    URING_DATA_WAKE = UINT64_MAX - 0,
    URING_DATA_CANCEL = UINT64_MAX - 1,
};

// Encode a (generation, fd) pair into the 64-bit sqe user_data. The sentinels
// above occupy the top of the u64 range, so any (gen, fd) with gen below
// URING_GEN_MAX encodes strictly below URING_DATA_CANCEL and never collides.
//
// fd is a non-negative int bounded by fd_capacity (it is an array index
// here), so it fits in the low 32 bits; the high 32 bits carry the generation.
#define URING_GEN_MAX (UINT32_MAX - 1u)

static uint64_t uring_encode_gen(uint32_t gen, int fd)
{
    return ((uint64_t)gen << 32) | (uint32_t)fd;
}

static int uring_decode_fd(uint64_t data)
{
    return (int)(data & 0xFFFFFFFFu);
}

static uint32_t uring_decode_gen(uint64_t data)
{
    return (uint32_t)(data >> 32);
}

// ---------------------------------------------------------------------------
// poll mask conversion
// ---------------------------------------------------------------------------

static unsigned int to_poll_mask(unsigned int events)
{
    unsigned int mask = 0;
    if (events & KITH_REACTOR_IN)
    {
        mask |= POLLIN;
    }
    if (events & KITH_REACTOR_OUT)
    {
        mask |= POLLOUT;
    }
    return mask;
}

static unsigned int from_poll_mask(short revents)
{
    unsigned int mask = 0;
    if (revents & POLLIN)
    {
        mask |= KITH_REACTOR_IN;
    }
    if (revents & POLLOUT)
    {
        mask |= KITH_REACTOR_OUT;
    }
    if (revents & (POLLHUP | POLLRDHUP))
    {
        mask |= KITH_REACTOR_HUP;
    }
    if (revents & POLLERR)
    {
        mask |= KITH_REACTOR_ERR;
    }
    return mask;
}

// ---------------------------------------------------------------------------
// CQE processing helpers (extracted from uring_wait)
// ---------------------------------------------------------------------------

// Re-arm a one-shot POLL_ADD for a registered fd. Called after the CQE fires.
static void uring_rearm(struct uring_backend *b, int fd)
{
    if (b->fd_entries[fd].fd != fd)
    {
        return;
    }
    struct io_uring_sqe *sqe = io_uring_get_sqe(&b->ring);
    if (sqe == nullptr)
    {
        return;
    }
    unsigned int poll_mask = to_poll_mask(b->fd_entries[fd].events);
    io_uring_prep_poll_add(sqe, fd, poll_mask);
    io_uring_sqe_set_data64(sqe, uring_encode_gen(b->fd_entries[fd].gen, fd));
}

// Re-arm the wake eventfd poll after it fires. POLL_ADD is one-shot, so the
// wake fd must be re-registered on every wake completion or subsequent
// cross-thread kith_reactor_submit wake() writes do not rouse a blocked
// uring_wait until the next timer deadline.
static void uring_rearm_wake(struct uring_backend *b)
{
    struct io_uring_sqe *sqe = io_uring_get_sqe(&b->ring);
    if (sqe == nullptr)
    {
        return;
    }
    io_uring_prep_poll_add(sqe, b->wake_fd, POLLIN);
    io_uring_sqe_set_data64(sqe, URING_DATA_WAKE);
    (void)io_uring_submit(&b->ring);
}

// Consume wakeup bytes from the eventfd.
static void uring_consume_wake(struct uring_backend *b)
{
    uint64_t val;
    (void)!read(b->wake_fd, &val, sizeof(val));
}

// Process one CQE. Returns whether a non-wake fd was dispatched.
static bool uring_process_cqe(struct uring_backend *b,
                              struct io_uring_cqe *cqe,
                              void (*dispatch)(int fd, unsigned int events, void *ctx),
                              void *dispatch_ctx)
{
    uint64_t data64 = io_uring_cqe_get_data64(cqe);

    if (data64 == URING_DATA_WAKE)
    {
        uring_consume_wake(b);
        // POLL_ADD is one-shot: re-arm the wake fd so the next cross-thread
        // submit wakes the reactor promptly.
        uring_rearm_wake(b);
        return false;
    }

    if (data64 == URING_DATA_CANCEL)
    {
        return false; // cancel completion — ignore
    }

    // Normal fd: the low 32 bits of data64 are the fd, the high 32 bits a
    // generation. A stale CQE (from a POLL_ADD submitted before the fd was
    // deregistered and the fd number reused by a new registration) has a
    // generation that does not match the current registration; drop the
    // completion instead of dispatching against it.
    int fd = uring_decode_fd(data64);
    if ((unsigned int)fd >= b->fd_capacity || b->fd_entries[fd].fd != fd ||
        b->fd_entries[fd].gen != uring_decode_gen(data64))
    {
        return false;
    }

    unsigned int revents = from_poll_mask(cqe->res >= 0 ? (short)cqe->res : 0);
    if (revents != 0)
    {
        // The one-shot poll is consumed for the dispatch. A handler that
        // re-registers the fd (mod) already submitted a fresh poll under a
        // bumped generation; rearming here too leaves two outstanding
        // same-generation polls, duplicating every subsequent dispatch.
        // Rearm only when the handler left this registration untouched.
        uint32_t gen_before = b->fd_entries[fd].gen;
        dispatch(fd, revents, dispatch_ctx);
        if (b->fd_entries[fd].fd == fd && b->fd_entries[fd].gen == gen_before)
        {
            uring_rearm(b, fd);
        }
        return true;
    }
    uring_rearm(b, fd);
    return false;
}

// Maximum completions processed per wait call. A mass disconnect delivers
// every HUP completion in one batch; an unbounded drain runs the whole batch
// inline before the loop reaches timer_wheel_advance, so a batch longer than
// the tick period delays every tick it spans. Truncating the drain leaves
// the remaining CQEs queued — partial cq_advance is the documented io_uring
// consumption pattern — and the next wait call returns immediately on the
// pending completions, so batches interleave with due timers instead of
// starving them. 128 completions bound one iteration's completion work to a
// few milliseconds at the worst per-completion cost observed in teardown
// sweeps while keeping large steady-state bursts at single-batch cost.
#define URING_DRAIN_CQE_CAP 128u

// Drain all available CQEs. Returns number of fds dispatched.
static int uring_drain_cqes(struct uring_backend *b,
                            void (*dispatch)(int fd, unsigned int events, void *ctx),
                            void *dispatch_ctx)
{
    unsigned head;
    unsigned count = 0;
    int nfds = 0;
    struct io_uring_cqe *cqe = nullptr;

    io_uring_for_each_cqe(&b->ring, head, cqe)
    {
        if (count >= URING_DRAIN_CQE_CAP)
        {
            break;
        }
        count++;
        if (uring_process_cqe(b, cqe, dispatch, dispatch_ctx))
        {
            nfds++;
        }
    }
    io_uring_cq_advance(&b->ring, count);
    io_uring_submit(&b->ring);
    return nfds;
}

// Register the wake eventfd as a poll source (done once, at init; the
// poll is one-shot, so every wake completion re-arms it).
static int uring_ensure_wake_registered(struct uring_backend *b)
{
    if (b->wake_fd_added)
    {
        return 0;
    }
    struct io_uring_sqe *sqe = io_uring_get_sqe(&b->ring);
    if (sqe == nullptr)
    {
        return kith_error_return(KITH_EIO);
    }
    io_uring_prep_poll_add(sqe, b->wake_fd, POLLIN);
    io_uring_sqe_set_data64(sqe, URING_DATA_WAKE);
    int rc = io_uring_submit(&b->ring);
    if (rc < 0)
    {
        return kith_error_return(KITH_EIO);
    }
    b->wake_fd_added = true;
    return 0;
}

// ---------------------------------------------------------------------------
// backend vtable implementation
// ---------------------------------------------------------------------------

// Create-time syscalls are retried when interrupted; on failure neither call
// installs a kernel resource, so retrying is side-effect-free. The cap bounds
// the loop so a pathologically interrupt-heavy host cannot spin here.
#define URING_INIT_RETRY_ATTEMPTS 8u

static int uring_init(const kith_allocator_t *alloc, void **out_data, unsigned int max_fds)
{
    struct uring_backend *b = kith_alloc_zero(alloc, 1, sizeof(*b));
    if (b == nullptr)
    {
        return kith_error_return(KITH_ENOMEM);
    }
    b->alloc = alloc;

    int rc;
    unsigned int attempts = 0u;
    do
    {
        struct io_uring_params p = {};
        p.flags |= IORING_SETUP_CLAMP;
        rc = io_uring_queue_init_params((unsigned int)(max_fds + 16), &b->ring, &p);
        attempts++;
    } while (rc == -EINTR && attempts < URING_INIT_RETRY_ATTEMPTS);
    if (rc < 0)
    {
        kith_free(alloc, b);
        switch (-rc)
        {
            case EINVAL:
                return kith_error_return(KITH_EINVAL);
            case EPERM:
                return kith_error_return(KITH_EPERM);
            case ENOMEM:
                return kith_error_return(KITH_ENOMEM);
            default:
                return kith_error_return(KITH_EIO);
        }
    }

    int wake_fd;
    attempts = 0u;
    do
    {
        wake_fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
        attempts++;
    } while (wake_fd < 0 && errno == EINTR && attempts < URING_INIT_RETRY_ATTEMPTS);
    if (wake_fd < 0)
    {
        io_uring_queue_exit(&b->ring);
        kith_free(alloc, b);
        return kith_error_return(KITH_EIO);
    }
    b->wake_fd = wake_fd;

    b->fd_capacity = (unsigned int)(max_fds + 16);
    b->fd_entries = kith_alloc_zero(alloc, b->fd_capacity, sizeof(struct uring_fd_entry));
    if (b->fd_entries == nullptr)
    {
        close(b->wake_fd);
        io_uring_queue_exit(&b->ring);
        kith_free(alloc, b);
        return kith_error_return(KITH_ENOMEM);
    }
    for (unsigned int i = 0; i < b->fd_capacity; i++)
    {
        b->fd_entries[i].fd = -1;
    }

    // Arm the wake poll at init. The eventfd produces no CQEs until a poll
    // watches it, so a reactor that never registers an fd otherwise never
    // sees a cross-thread wake: kith_reactor_run blocks in wait with
    // submitted tasks pending, and even kith_reactor_stop cannot rouse it.
    rc = uring_ensure_wake_registered(b);
    if (rc != 0)
    {
        close(b->wake_fd);
        io_uring_queue_exit(&b->ring);
        kith_free(alloc, b->fd_entries);
        kith_free(alloc, b);
        return rc;
    }

    *out_data = b;
    return 0;
}

static void uring_destroy(void *data)
{
    struct uring_backend *b = (struct uring_backend *)data;
    if (b == nullptr)
    {
        return;
    }
    close(b->wake_fd);
    io_uring_queue_exit(&b->ring);
    kith_free(b->alloc, b->fd_entries);
    kith_free(b->alloc, b);
}

static int uring_add(void *data, int fd, unsigned int events)
{
    struct uring_backend *b = (struct uring_backend *)data;
    if (b == nullptr || fd < 0 || (unsigned int)fd >= b->fd_capacity)
    {
        return kith_error_return(KITH_EINVAL);
    }

    int rc = uring_ensure_wake_registered(b);
    if (rc != 0)
    {
        return rc;
    }

    // Bump the generation on (re)registration so a stale CQE for a prior
    // registration of this fd number (deregistered, closed, and the number
    // reused by a new accept) carries an outdated generation and is dropped
    // by uring_process_cqe.
    uint32_t gen = b->fd_entries[fd].gen + 1u;
    if (gen >= URING_GEN_MAX)
    {
        gen = 0u;
    }

    struct io_uring_sqe *sqe = io_uring_get_sqe(&b->ring);
    if (sqe == nullptr)
    {
        return kith_error_return(KITH_EIO);
    }
    io_uring_prep_poll_add(sqe, fd, to_poll_mask(events));
    io_uring_sqe_set_data64(sqe, uring_encode_gen(gen, fd));

    rc = io_uring_submit(&b->ring);
    if (rc < 0)
    {
        return kith_error_return(KITH_EIO);
    }

    b->fd_entries[fd].fd = fd;
    b->fd_entries[fd].events = events;
    b->fd_entries[fd].gen = gen;
    return 0;
}

static int uring_mod(void *data, int fd, unsigned int events)
{
    struct uring_backend *b = (struct uring_backend *)data;
    if (b == nullptr || fd < 0 || (unsigned int)fd >= b->fd_capacity)
    {
        return kith_error_return(KITH_EINVAL);
    }
    if (b->fd_entries[fd].fd != fd)
    {
        return kith_error_return(KITH_ENOENT);
    }

    // No-op when the mask is unchanged: cancel + re-poll briefly leaves
    // two outstanding same-generation polls (the new one plus the backend's
    // post-dispatch rearm of a mod issued inside a readiness handler), and
    // each outstanding poll delivers its own CQE. Callers re-issue mods
    // every tick with mostly unchanged masks, so this early return also
    // removes the bulk of the redundant submissions.
    if (events == b->fd_entries[fd].events)
    {
        return 0;
    }

    // Cancel outstanding poll.
    struct io_uring_sqe *sqe = io_uring_get_sqe(&b->ring);
    if (sqe == nullptr)
    {
        return kith_error_return(KITH_EIO);
    }
    io_uring_prep_cancel_fd(sqe, fd, 0);
    io_uring_sqe_set_data64(sqe, URING_DATA_CANCEL);

    int rc = io_uring_submit(&b->ring);
    if (rc < 0)
    {
        return kith_error_return(KITH_EIO);
    }

    // Bump the generation so the canceled poll's CQE (carrying the prior
    // generation) is dropped by uring_process_cqe instead of dispatching with
    // the stale events. (A poll that completed normally just before the cancel
    // takes effect carries the prior generation too and is dropped here; the
    // new poll submitted below catches the next edge. This is the same edge-
    // triggered drop semantics the cancel already implied.)
    uint32_t gen = b->fd_entries[fd].gen + 1u;
    if (gen >= URING_GEN_MAX)
    {
        gen = 0u;
    }
    b->fd_entries[fd].gen = gen;

    // Submit new poll with updated events.
    sqe = io_uring_get_sqe(&b->ring);
    if (sqe == nullptr)
    {
        return kith_error_return(KITH_EIO);
    }
    io_uring_prep_poll_add(sqe, fd, to_poll_mask(events));
    io_uring_sqe_set_data64(sqe, uring_encode_gen(gen, fd));

    rc = io_uring_submit(&b->ring);
    if (rc < 0)
    {
        return kith_error_return(KITH_EIO);
    }

    b->fd_entries[fd].events = events;
    return 0;
}

static int uring_del_fn(void *data, int fd)
{
    struct uring_backend *b = (struct uring_backend *)data;
    if (b == nullptr || fd < 0 || (unsigned int)fd >= b->fd_capacity)
    {
        return kith_error_return(KITH_EINVAL);
    }

    struct io_uring_sqe *sqe = io_uring_get_sqe(&b->ring);
    if (sqe == nullptr)
    {
        return kith_error_return(KITH_EIO);
    }
    io_uring_prep_cancel_fd(sqe, fd, 0);
    io_uring_sqe_set_data64(sqe, URING_DATA_CANCEL);

    int rc = io_uring_submit(&b->ring);
    if (rc < 0)
    {
        return kith_error_return(KITH_EIO);
    }

    b->fd_entries[fd].fd = -1;
    return 0;
}

static int uring_wait(void *data,
                      int timeout_ms,
                      void (*dispatch)(int fd, unsigned int events, void *ctx),
                      void *dispatch_ctx)
{
    struct uring_backend *b = (struct uring_backend *)data;

    if (timeout_ms == 0)
    {
        return uring_drain_cqes(b, dispatch, dispatch_ctx);
    }

    struct __kernel_timespec ts;
    struct __kernel_timespec *tsp = nullptr;
    if (timeout_ms > 0)
    {
        ts.tv_sec = (long long)(timeout_ms / 1000);
        ts.tv_nsec = (long long)(timeout_ms % 1000) * 1000000L;
        tsp = &ts;
    }

    struct io_uring_cqe *cqe = nullptr;
    int rc = io_uring_wait_cqe_timeout(&b->ring, &cqe, tsp);

    if (rc == -ETIME || rc == -EINTR)
    {
        return uring_drain_cqes(b, dispatch, dispatch_ctx);
    }

    if (rc < 0)
    {
        return kith_error_return(KITH_EIO);
    }

    // CQE received — process it first, then drain the rest.
    int nfds = 0;
    if (uring_process_cqe(b, cqe, dispatch, dispatch_ctx))
    {
        nfds++;
    }
    io_uring_cqe_seen(&b->ring, cqe);

    nfds += uring_drain_cqes(b, dispatch, dispatch_ctx);
    return nfds;
}

static int uring_wake(void *data)
{
    struct uring_backend *b = (struct uring_backend *)data;
    if (b == nullptr)
    {
        return kith_error_return(KITH_EINVAL);
    }
    uint64_t val = 1;
    ssize_t rc = write(b->wake_fd, &val, sizeof(val));
    if (rc < 0 && errno != EAGAIN)
    {
        return kith_error_return(KITH_EIO);
    }
    return 0;
}

static int uring_ring_sizes(void *backend_data, unsigned int *out_sq, unsigned int *out_cq)
{
    struct uring_backend *b = (struct uring_backend *)backend_data;
    if (b == nullptr)
    {
        return kith_error_return(KITH_EINVAL);
    }
    *out_sq = b->ring.sq.ring_entries;
    *out_cq = b->ring.cq.ring_entries;
    return 0;
}

const struct kith_reactor_backend kith_reactor_backend_uring = {
    .init = uring_init,
    .destroy = uring_destroy,
    .add = uring_add,
    .mod = uring_mod,
    .del = uring_del_fn,
    .wait = uring_wait,
    .wake = uring_wake,
    .ring_sizes = uring_ring_sizes,
};

#endif // __linux__
