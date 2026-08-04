#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "kith/net/net.h"
#include "kith/proto/proto.h"
#include "kith/types.h"
#include "kith/version.h"

static int check_cond(bool ok, int line)
{
    if (!ok)
    {
        (void)fprintf(stderr, "net frame: assertion at line %d failed\n", line);
        return 1;
    }
    return 0;
}

#define CHECK(cond) failures += check_cond((cond), __LINE__)

static int test_create_and_data(void)
{
    int failures = 0;
    kith_proto_t *proto = nullptr;
    CHECK(kith_proto_create(nullptr, nullptr, &proto) == 0);

    kith_net_t *net = nullptr;
    CHECK(kith_net_create(nullptr, proto, nullptr, &net) == 0);

    /* A frame of 100 bytes lands in the 128-byte size class. */
    kith_net_frame_t *f = kith_net_frame_create(net, 100u);
    CHECK(f != nullptr);
    CHECK(kith_net_frame_len(f) == 100u);
    void *data = kith_net_frame_data(f);
    CHECK(data != nullptr);
    if (data != nullptr)
    {
        /* Write into the buffer and verify it persists. */
        memset(data, 0x5au, 100u);
        CHECK(((uint8_t *)data)[0] == 0x5au);
        CHECK(((uint8_t *)data)[99] == 0x5au);
    }

    /* set_len shrinks the used length. */
    kith_net_frame_set_len(f, 50u);
    CHECK(kith_net_frame_len(f) == 50u);

    kith_net_frame_release(f);
    kith_net_destroy(net);
    kith_proto_destroy(proto);
    return failures;
}

static int test_refcount(void)
{
    int failures = 0;
    kith_proto_t *proto = nullptr;
    CHECK(kith_proto_create(nullptr, nullptr, &proto) == 0);
    kith_net_t *net = nullptr;
    CHECK(kith_net_create(nullptr, proto, nullptr, &net) == 0);

    kith_net_frame_t *f = kith_net_frame_create(net, 64u);
    CHECK(f != nullptr);

    /* Acquire two extra references; release must not free until the third. */
    kith_net_frame_acquire(f);
    kith_net_frame_acquire(f);
    kith_net_frame_release(f);
    kith_net_frame_release(f);
    /* The buffer must still be valid (one reference remains). */
    void *data = kith_net_frame_data(f);
    CHECK(data != nullptr);
    if (data != nullptr)
    {
        memset(data, 0x11u, 64u);
        CHECK(((uint8_t *)data)[63] == 0x11u);
    }

    kith_net_frame_release(f);

    /* NULL is a no-op on all frame calls. */
    kith_net_frame_acquire(nullptr);
    kith_net_frame_release(nullptr);
    CHECK(kith_net_frame_data(nullptr) == nullptr);
    CHECK(kith_net_frame_len(nullptr) == 0u);
    kith_net_frame_set_len(nullptr, 0u);

    kith_net_destroy(net);
    kith_proto_destroy(proto);
    return failures;
}

static int test_size_classes(void)
{
    int failures = 0;
    kith_proto_t *proto = nullptr;
    CHECK(kith_proto_create(nullptr, nullptr, &proto) == 0);
    kith_net_t *net = nullptr;
    CHECK(kith_net_create(nullptr, proto, nullptr, &net) == 0);

    /* Frames across several size-class boundaries, including an oversize
     * allocation (above the 8192 largest class) served by a direct malloc. */
    uint32_t sizes[] = {1u, 64u, 65u, 8192u, 8193u, 100000u};
    for (size_t i = 0; i < sizeof(sizes) / sizeof(sizes[0]); ++i)
    {
        kith_net_frame_t *f = kith_net_frame_create(net, sizes[i]);
        CHECK(f != nullptr);
        CHECK(kith_net_frame_len(f) == sizes[i]);
        void *data = kith_net_frame_data(f);
        CHECK(data != nullptr);
        if (data != nullptr)
        {
            /* Touch the last byte to confirm the allocation is at least sizes[i]. */
            ((uint8_t *)data)[sizes[i] - 1u] = 0xffu;
            CHECK(((uint8_t *)data)[sizes[i] - 1u] == 0xffu);
        }
        kith_net_frame_release(f);
    }

    kith_net_destroy(net);
    kith_proto_destroy(proto);
    return failures;
}

static int test_pool_recycle(void)
{
    int failures = 0;
    kith_proto_t *proto = nullptr;
    CHECK(kith_proto_create(nullptr, nullptr, &proto) == 0);
    kith_net_t *net = nullptr;
    CHECK(kith_net_create(nullptr, proto, nullptr, &net) == 0);

    /* Allocate and release many frames of the same size class to exercise the
     * free-list push/pop path. The pool cap is 1024 per class, so the first
     * 1024 releases populate the cache and subsequent releases free directly. */
    enum
    {
        N = 1100
    };
    for (int i = 0; i < N; ++i)
    {
        kith_net_frame_t *f = kith_net_frame_create(net, 256u);
        CHECK(f != nullptr);
        kith_net_frame_release(f);
    }
    /* A subsequent create must succeed (served from the cache or fresh). */
    kith_net_frame_t *f = kith_net_frame_create(net, 256u);
    CHECK(f != nullptr);
    kith_net_frame_release(f);

    kith_net_destroy(net);
    kith_proto_destroy(proto);
    return failures;
}

static int test_arg_validation(void)
{
    int failures = 0;
    CHECK(kith_net_frame_create(nullptr, 64u) == nullptr);

    kith_proto_t *proto = nullptr;
    CHECK(kith_proto_create(nullptr, nullptr, &proto) == 0);
    kith_net_t *net = nullptr;
    CHECK(kith_net_create(nullptr, proto, nullptr, &net) == 0);

    /* A frame created from one transport is not tied to it across release. */
    kith_net_frame_t *f = kith_net_frame_create(net, 128u);
    CHECK(f != nullptr);
    kith_net_frame_release(f);

    kith_net_destroy(net);
    kith_proto_destroy(proto);
    return failures;
}

int main(void)
{
    int rc = 0;
    rc |= test_create_and_data();
    rc |= test_refcount();
    rc |= test_size_classes();
    rc |= test_pool_recycle();
    rc |= test_arg_validation();
    if (rc != 0)
    {
        (void)fprintf(stderr, "net frame tests FAILED\n");
    }
    return rc;
}
