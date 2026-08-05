#include <stdio.h>
#include <stdlib.h>

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
        (void)fprintf(stderr, "reactor ring clamp: assertion at line %d failed\n", line);
        return 1;
    }
    return 0;
}

#define CHECK(cond) failures += check_cond((cond), __LINE__)

// ---------------------------------------------------------------------------
// IORING_SETUP_CLAMP clamping
//
// An oversized max_fds that pushes the requested SQ entry count above the
// kernel cap (32768) must succeed via clamping, not fail with EINVAL. The
// test also verifies the returned ring sizes match the clamped values
// (SQ = 32768, CQ = 2*SQ = 65536).
//
// Exercising the clamp needs ~3 MB of free ring budget: the clamped ring
// is ~2 MB of SQEs plus ~1 MB of CQEs. Ring pages are charged to the
// cgroup memory allocator (since Linux 5.12; RLIMIT_MEMLOCK covers only
// registered buffers), and the charge reclaims asynchronously after
// close, so a box that churns rings can transiently starve the cgroup
// memory budget; ENOMEM reports that starved budget, not a clamp
// failure. On a kernel older than 5.6 (where IORING_SETUP_CLAMP is not
// recognized) io_uring_setup rejects the flag with EINVAL, and EPERM
// reports io_uring disabled by policy. All three exit with the skip
// code, which ctest reports as not run.
// ---------------------------------------------------------------------------

#define KITH_TEST_SKIP 77

int main(void)
{
    int failures = 0;
    kith_reactor_t *reactor = nullptr;

    kith_reactor_params_t params = {
        .size = sizeof(kith_reactor_params_t),
        .abi_version = KITH_ABI_VERSION,
        .max_fds = 40000,
        .task_capacity = 64,
    };

    int rc = kith_reactor_create(&params, nullptr, &reactor);

    if (rc == kith_error_return(KITH_EINVAL) || rc == kith_error_return(KITH_EPERM) ||
        rc == kith_error_return(KITH_ENOMEM))
    {
        (void)fprintf(stderr, "reactor ring clamp: skipped (io_uring setup rc %d)\n", rc);
        return KITH_TEST_SKIP;
    }

    CHECK(rc == 0);
    CHECK(reactor != nullptr);

    if (rc == 0 && reactor != nullptr)
    {
        unsigned int sq = 0;
        unsigned int cq = 0;
        CHECK(kith_reactor_ring_sizes(reactor, &sq, &cq) == 0);
        CHECK(sq == 32768u);
        CHECK(cq == 2u * sq);

        CHECK(kith_reactor_ring_sizes(nullptr, &sq, &cq) == kith_error_return(KITH_EINVAL));
        CHECK(kith_reactor_ring_sizes(reactor, nullptr, &cq) == kith_error_return(KITH_EINVAL));
        CHECK(kith_reactor_ring_sizes(reactor, &sq, nullptr) == kith_error_return(KITH_EINVAL));

        kith_reactor_destroy(reactor);
    }

    if (failures != 0)
    {
        (void)fprintf(stderr, "reactor ring clamp tests FAILED\n");
        return EXIT_FAILURE;
    }
    return EXIT_SUCCESS;
}
