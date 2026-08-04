/* Frame pool for the net module: size-classed free lists of refcounted frames,
 * create and release returning frames to their class bucket, and pool init
 * and destroy. Backs the connection path in conn.c; the private frame.h
 * declares the structures. */

#include "frame.h"

#include <stdatomic.h>

#ifdef __has_feature
#if __has_feature(address_sanitizer)
#define KITH_NET_FRAME_NO_POOL 1
#endif
#endif
#if defined(__SANITIZE_ADDRESS__) && !defined(KITH_NET_FRAME_NO_POOL)
#define KITH_NET_FRAME_NO_POOL 1
#endif

static const uint32_t frame_pool_caps[KITH_FRAME_CLASS_COUNT] = {
    64u,
    128u,
    256u,
    512u,
    1024u,
    2048u,
    4096u,
    8192u,
};

static int frame_class_for_len(uint32_t len)
{
    for (uint32_t i = 0u; i < KITH_FRAME_CLASS_COUNT; ++i)
    {
        if (len <= frame_pool_caps[i])
        {
            return (int)i;
        }
    }
    return -1;
}

static int frame_class_for_cap(uint32_t cap)
{
    for (uint32_t i = 0u; i < KITH_FRAME_CLASS_COUNT; ++i)
    {
        if (cap == frame_pool_caps[i])
        {
            return (int)i;
        }
    }
    return -1;
}

int kith_frame_pool_init(struct kith_frame_pool *pool, const kith_allocator_t *allocator)
{
    if (pool == nullptr)
    {
        return -1;
    }
    pool->allocator = allocator;
    if (pthread_mutex_init(&pool->lock, nullptr) != 0)
    {
        return -1;
    }
    for (uint32_t i = 0u; i < KITH_FRAME_CLASS_COUNT; ++i)
    {
        pool->heads[i] = nullptr;
        pool->counts[i] = 0u;
    }
    return 0;
}

void kith_frame_pool_destroy(struct kith_frame_pool *pool)
{
    if (pool == nullptr)
    {
        return;
    }
    for (uint32_t i = 0u; i < KITH_FRAME_CLASS_COUNT; ++i)
    {
        struct kith_net_frame *f = pool->heads[i];
        while (f != nullptr)
        {
            struct kith_net_frame *next = f->pool_next;
            kith_free(pool->allocator, f);
            f = next;
        }
        pool->heads[i] = nullptr;
        pool->counts[i] = 0u;
    }
    (void)pthread_mutex_destroy(&pool->lock);
}

static struct kith_net_frame *frame_alloc_raw(const kith_allocator_t *allocator, uint32_t cap)
{
    struct kith_net_frame *f = kith_alloc(allocator, sizeof(*f) + cap);
    if (f == nullptr)
    {
        return nullptr;
    }
    f->allocator = allocator;
    atomic_init(&f->refcount, 1);
    f->cap = cap;
    f->pool_next = nullptr;
    return f;
}

struct kith_net_frame *kith_frame_create(struct kith_frame_pool *pool, uint32_t len)
{
    if (pool == nullptr)
    {
        return nullptr;
    }

    const int idx = frame_class_for_len(len);
    const uint32_t cap = (idx >= 0) ? frame_pool_caps[idx] : len;
    struct kith_net_frame *f = nullptr;

#ifdef KITH_NET_FRAME_NO_POOL
    f = frame_alloc_raw(pool->allocator, cap);
    if (f == nullptr)
    {
        return nullptr;
    }
    f->pool = nullptr;
#else
    if (idx >= 0)
    {
        (void)pthread_mutex_lock(&pool->lock);
        f = pool->heads[idx];
        if (f != nullptr)
        {
            pool->heads[idx] = f->pool_next;
            if (pool->counts[idx] > 0u)
            {
                pool->counts[idx]--;
            }
        }
        (void)pthread_mutex_unlock(&pool->lock);
    }
    if (f == nullptr)
    {
        f = frame_alloc_raw(pool->allocator, cap);
        if (f == nullptr)
        {
            return nullptr;
        }
    }
    else
    {
        atomic_store(&f->refcount, 1);
        f->pool_next = nullptr;
    }
    f->pool = pool;
#endif

    f->len = len;
    return f;
}

void kith_frame_release(struct kith_net_frame *f)
{
    if (f == nullptr)
    {
        return;
    }
    if (atomic_fetch_sub_explicit(&f->refcount, 1, memory_order_acq_rel) == 1)
    {
#ifdef KITH_NET_FRAME_NO_POOL
        kith_free(f->allocator, f);
#else
        struct kith_frame_pool *pool = f->pool;
        if (pool == nullptr)
        {
            kith_free(f->allocator, f);
            return;
        }
        const int idx = frame_class_for_cap(f->cap);
        bool recycle = false;
        if (idx >= 0)
        {
            (void)pthread_mutex_lock(&pool->lock);
            if (pool->counts[idx] < KITH_FRAME_POOL_MAX_PER_CLASS)
            {
                f->pool_next = pool->heads[idx];
                pool->heads[idx] = f;
                pool->counts[idx]++;
                recycle = true;
            }
            (void)pthread_mutex_unlock(&pool->lock);
        }
        if (!recycle)
        {
            kith_free(f->allocator, f);
        }
#endif
    }
}
