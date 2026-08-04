/* Single translation unit of the proto module: a message-type registry (name
 * to id), frame encode with the wire header and optional 8-byte correlation
 * trailer, and frame decode. The public contract is include/kith/proto/proto.h. */

#include "kith/proto/proto.h"

#include <stdatomic.h>
#include <stdckdint.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include <pthread.h>

#include "kith/types.h"
#include "kith/version.h"

struct type_entry
{
    char *name;
    uint16_t id;
};

struct kith_proto
{
    const kith_allocator_t *allocator;
    struct type_entry *types;
    size_t count;
    size_t cap;
    uint32_t max_payload;
    pthread_mutex_t lock;
    /* Monotonic decode-path rejection tally: every -KITH_EPROTO the decode
     * entry point returns (malformed header, payload bound, unknown type,
     * correlation-trailer size), counted once at the rejection site. EAGAIN
     * (a frame still arriving), EINVAL, and ESTATE are not rejections. The
     * composition root records the per-tick delta as
     * kith_proto_rejections_total; with the transport's rb_max close
     * counter it reconciles the soak's malformed-input conservation
     * (malformed_injected == Σ proto + Σ net). */
    _Atomic uint64_t rejections;
};

/*---------------------------------------------------------------------------
 * big-endian byte assembly (portable, no <endian.h> dependency)
 *-------------------------------------------------------------------------*/

static uint16_t read_be_u16(const uint8_t *p)
{
    return (uint16_t)((uint16_t)p[0] << 8u) | (uint16_t)p[1];
}

static uint32_t read_be_u32(const uint8_t *p)
{
    return ((uint32_t)p[0] << 24u) | ((uint32_t)p[1] << 16u) | ((uint32_t)p[2] << 8u) |
           (uint32_t)p[3];
}

static uint64_t read_be_u64(const uint8_t *p)
{
    uint64_t v = 0;
    for (size_t i = 0; i < KITH_PROTO_CORR_TRAILER_SIZE; ++i)
    {
        v = (v << 8u) | (uint64_t)p[i];
    }
    return v;
}

static void write_be_u64(uint8_t *p, uint64_t v)
{
    for (size_t i = 0; i < KITH_PROTO_CORR_TRAILER_SIZE; ++i)
    {
        p[KITH_PROTO_CORR_TRAILER_SIZE - 1u - i] = (uint8_t)(v >> (8u * i));
    }
}

/*---------------------------------------------------------------------------
 * registry helpers (caller holds @p proto->lock)
 *-------------------------------------------------------------------------*/

static struct type_entry *find_by_id_locked(const kith_proto_t *proto, uint16_t id)
{
    for (size_t i = 0; i < proto->count; ++i)
    {
        if (proto->types[i].id == id)
        {
            return &proto->types[i];
        }
    }
    return nullptr;
}

static struct type_entry *find_by_name_locked(kith_proto_t *proto, const char *name)
{
    for (size_t i = 0; i < proto->count; ++i)
    {
        if (strcmp(proto->types[i].name, name) == 0)
        {
            return &proto->types[i];
        }
    }
    return nullptr;
}

static int grow_types_locked(kith_proto_t *proto)
{
    size_t next_cap = proto->cap == 0 ? 16 : proto->cap * 2;
    struct type_entry *next =
        kith_realloc(proto->allocator, proto->types, next_cap * sizeof(*next));
    if (next == nullptr)
    {
        return kith_error_return(KITH_ENOMEM);
    }
    proto->types = next;
    proto->cap = next_cap;
    return 0;
}

/*---------------------------------------------------------------------------
 * lifecycle
 *-------------------------------------------------------------------------*/

int kith_proto_create(const kith_proto_params_t *params,
                      const kith_allocator_t *alloc,
                      kith_proto_t **out_proto)
{
    if (out_proto == nullptr)
    {
        return kith_error_return(KITH_EINVAL);
    }
    *out_proto = nullptr;

    if (params != nullptr)
    {
        if (params->abi_version != KITH_ABI_VERSION)
        {
            return kith_error_return(KITH_EABIVER);
        }
        if (params->size < sizeof(*params))
        {
            return kith_error_return(KITH_ESIZE);
        }
    }
    if (alloc != nullptr)
    {
        const kith_error_t alloc_rc = kith_allocator_check(alloc);
        if (alloc_rc != 0)
        {
            return kith_error_return(alloc_rc);
        }
    }

    const kith_allocator_t *allocator = alloc != nullptr ? alloc : kith_allocator_default();
    kith_proto_t *p = kith_alloc(allocator, sizeof(*p));
    if (p == nullptr)
    {
        return kith_error_return(KITH_ENOMEM);
    }
    p->allocator = allocator;
    p->types = nullptr;
    p->count = 0;
    p->cap = 0;
    p->max_payload = (params == nullptr || params->max_payload == 0u)
                         ? KITH_PROTO_DEFAULT_MAX_PAYLOAD
                         : params->max_payload;
    atomic_init(&p->rejections, 0u);
    if (pthread_mutex_init(&p->lock, nullptr) != 0)
    {
        kith_free(p->allocator, p);
        return kith_error_return(KITH_ESTATE);
    }
    *out_proto = p;
    return 0;
}

void kith_proto_destroy(kith_proto_t *proto)
{
    if (proto == nullptr)
    {
        return;
    }
    for (size_t i = 0; i < proto->count; ++i)
    {
        kith_free(proto->allocator, proto->types[i].name);
    }
    kith_free(proto->allocator, proto->types);
    (void)pthread_mutex_destroy(&proto->lock);
    kith_free(proto->allocator, proto);
}

/*---------------------------------------------------------------------------
 * registry
 *-------------------------------------------------------------------------*/

int kith_proto_register_type_id(kith_proto_t *proto, const char *name, uint16_t type_id)
{
    if (proto == nullptr || name == nullptr || name[0] == '\0')
    {
        return kith_error_return(KITH_EINVAL);
    }
    if (pthread_mutex_lock(&proto->lock) != 0)
    {
        return kith_error_return(KITH_ESTATE);
    }

    struct type_entry *by_name = find_by_name_locked(proto, name);
    if (by_name != nullptr)
    {
        const int rc = (by_name->id == type_id) ? 0 : kith_error_return(KITH_EINVAL);
        (void)pthread_mutex_unlock(&proto->lock);
        return rc;
    }
    if (find_by_id_locked(proto, type_id) != nullptr)
    {
        (void)pthread_mutex_unlock(&proto->lock);
        return kith_error_return(KITH_EEXIST);
    }

    if (proto->count == proto->cap)
    {
        const int gr = grow_types_locked(proto);
        if (gr != 0)
        {
            (void)pthread_mutex_unlock(&proto->lock);
            return gr;
        }
    }
    char *copy = kith_strdup(proto->allocator, name);
    if (copy == nullptr)
    {
        (void)pthread_mutex_unlock(&proto->lock);
        return kith_error_return(KITH_ENOMEM);
    }
    proto->types[proto->count].name = copy;
    proto->types[proto->count].id = type_id;
    proto->count += 1;
    (void)pthread_mutex_unlock(&proto->lock);
    return 0;
}

int kith_proto_lookup_type(const kith_proto_t *proto, const char *name, uint16_t *out_type_id)
{
    if (proto == nullptr || name == nullptr || out_type_id == nullptr)
    {
        return kith_error_return(KITH_EINVAL);
    }
    kith_proto_t *m = (kith_proto_t *)proto;
    if (pthread_mutex_lock(&m->lock) != 0)
    {
        return kith_error_return(KITH_ESTATE);
    }
    struct type_entry *e = find_by_name_locked(m, name);
    const int rc = (e == nullptr) ? kith_error_return(KITH_ENOENT) : 0;
    if (e != nullptr)
    {
        *out_type_id = e->id;
    }
    (void)pthread_mutex_unlock(&m->lock);
    return rc;
}

const char *kith_proto_type_name(const kith_proto_t *proto, uint16_t type_id)
{
    if (proto == nullptr)
    {
        return nullptr;
    }
    kith_proto_t *m = (kith_proto_t *)proto;
    if (pthread_mutex_lock(&m->lock) != 0)
    {
        return nullptr;
    }
    struct type_entry *e = find_by_id_locked(m, type_id);
    const char *name = (e != nullptr) ? e->name : nullptr;
    (void)pthread_mutex_unlock(&m->lock);
    return name;
}

/*---------------------------------------------------------------------------
 * encode
 *-------------------------------------------------------------------------*/

size_t kith_proto_encode(const kith_proto_t *proto,
                         uint16_t type_id,
                         uint8_t flags,
                         uint64_t correlation_id,
                         const void *payload,
                         uint32_t payload_len,
                         void *buf,
                         size_t cap)
{
    if (proto == nullptr)
    {
        return 0;
    }
    if (payload_len != 0u && payload == nullptr)
    {
        return 0;
    }

    const uint32_t trailer_len =
        ((flags & KITH_PROTO_FLAG_CORRELATION) != 0u) ? KITH_PROTO_CORR_TRAILER_SIZE : 0u;

    uint32_t wire_len = 0;
    if (ckd_add(&wire_len, payload_len, trailer_len))
    {
        return 0;
    }
    if (wire_len > proto->max_payload)
    {
        return 0;
    }

    size_t total = 0;
    if (ckd_add(&total, (size_t)KITH_PROTO_HDR_SIZE, (size_t)wire_len))
    {
        return 0;
    }
    if (buf == nullptr || cap == 0u)
    {
        return total;
    }

    uint8_t hdr[KITH_PROTO_HDR_SIZE];
    hdr[0] = (uint8_t)KITH_PROTO_MAGIC0;
    hdr[1] = (uint8_t)KITH_PROTO_MAGIC1;
    hdr[2] = (uint8_t)KITH_PROTO_VERSION;
    hdr[3] = flags;
    hdr[4] = (uint8_t)(type_id >> 8u);
    hdr[5] = (uint8_t)type_id;
    hdr[6] = (uint8_t)(wire_len >> 24u);
    hdr[7] = (uint8_t)(wire_len >> 16u);
    hdr[8] = (uint8_t)(wire_len >> 8u);
    hdr[9] = (uint8_t)wire_len;

    uint8_t *out = (uint8_t *)buf;
    size_t n = (cap < total) ? cap : total;
    size_t off = 0;
    size_t hdr_copy = (n < (size_t)KITH_PROTO_HDR_SIZE) ? n : (size_t)KITH_PROTO_HDR_SIZE;
    memcpy(out, hdr, hdr_copy);
    off += hdr_copy;

    if (payload_len != 0u && off < n)
    {
        size_t copy = (off + payload_len <= n) ? payload_len : (n - off);
        memcpy(out + off, payload, copy);
        off += copy;
    }
    if (trailer_len != 0u && off < n)
    {
        uint8_t trailer[KITH_PROTO_CORR_TRAILER_SIZE];
        write_be_u64(trailer, correlation_id);
        size_t copy =
            (off + KITH_PROTO_CORR_TRAILER_SIZE <= n) ? KITH_PROTO_CORR_TRAILER_SIZE : (n - off);
        memcpy(out + off, trailer, copy);
    }
    return total;
}

/*---------------------------------------------------------------------------
 * decode
 *-------------------------------------------------------------------------*/

static int parse_header(
    const uint8_t *buf, size_t len, uint16_t *out_type, uint8_t *out_flags, uint32_t *out_wire_len)
{
    if (len < (size_t)KITH_PROTO_HDR_SIZE)
    {
        return kith_error_return(KITH_EAGAIN);
    }
    if (buf[0] != (uint8_t)KITH_PROTO_MAGIC0 || buf[1] != (uint8_t)KITH_PROTO_MAGIC1)
    {
        return kith_error_return(KITH_EPROTO);
    }
    if (buf[2] != (uint8_t)KITH_PROTO_VERSION)
    {
        return kith_error_return(KITH_EPROTO);
    }
    *out_flags = buf[3];
    *out_type = read_be_u16(buf + 4);
    *out_wire_len = read_be_u32(buf + 6);
    return 0;
}

/* Count one decode-path rejection on the handle's monotonic tally and
 * return the protocol error. Relaxed order: the counter is a tally read
 * through the accessor's own atomic load, never a synchronization edge. */
static int proto_reject(kith_proto_t *m)
{
    atomic_fetch_add_explicit(&m->rejections, 1u, memory_order_relaxed);
    return kith_error_return(KITH_EPROTO);
}

int kith_proto_decode(const kith_proto_t *proto,
                      const void *buf,
                      size_t len,
                      kith_proto_frame_t *out_frame,
                      size_t *out_consumed)
{
    if (proto == nullptr || buf == nullptr || out_frame == nullptr)
    {
        return kith_error_return(KITH_EINVAL);
    }
    if (out_consumed != nullptr)
    {
        *out_consumed = 0;
    }

    kith_proto_t *m = (kith_proto_t *)proto;
    uint16_t type_id = 0;
    uint8_t flags = 0;
    uint32_t wire_len = 0;
    const int pr = parse_header((const uint8_t *)buf, len, &type_id, &flags, &wire_len);
    if (pr != 0)
    {
        if (pr == kith_error_return(KITH_EPROTO))
        {
            return proto_reject(m);
        }
        return pr;
    }
    if (wire_len > proto->max_payload)
    {
        return proto_reject(m);
    }
    size_t frame_total = 0;
    if (ckd_add(&frame_total, (size_t)KITH_PROTO_HDR_SIZE, (size_t)wire_len))
    {
        return proto_reject(m);
    }
    if (len < frame_total)
    {
        return kith_error_return(KITH_EAGAIN);
    }

    if (pthread_mutex_lock(&m->lock) != 0)
    {
        return kith_error_return(KITH_ESTATE);
    }
    struct type_entry *e = find_by_id_locked(m, type_id);
    (void)pthread_mutex_unlock(&m->lock);
    if (e == nullptr)
    {
        return proto_reject(m);
    }

    const uint8_t *bytes = (const uint8_t *)buf;
    uint32_t payload_len = wire_len;
    uint64_t correlation_id = 0;
    bool has_corr = false;
    if ((flags & KITH_PROTO_FLAG_CORRELATION) != 0u)
    {
        if (wire_len < KITH_PROTO_CORR_TRAILER_SIZE)
        {
            return proto_reject(m);
        }
        correlation_id =
            read_be_u64(bytes + KITH_PROTO_HDR_SIZE + wire_len - KITH_PROTO_CORR_TRAILER_SIZE);
        payload_len = wire_len - KITH_PROTO_CORR_TRAILER_SIZE;
        has_corr = true;
    }

    out_frame->type_id = type_id;
    out_frame->flags = flags;
    out_frame->has_correlation = has_corr;
    out_frame->correlation_id = correlation_id;
    out_frame->payload_len = payload_len;
    out_frame->payload =
        (payload_len != 0u) ? (const void *)(bytes + KITH_PROTO_HDR_SIZE) : nullptr;
    if (out_consumed != nullptr)
    {
        *out_consumed = frame_total;
    }
    return 0;
}

int kith_proto_declared_total(const void *buf, size_t len, size_t *out_total)
{
    if (buf == nullptr || out_total == nullptr)
    {
        return kith_error_return(KITH_EINVAL);
    }

    uint16_t type_id = 0;
    uint8_t flags = 0;
    uint32_t wire_len = 0;
    const int pr = parse_header((const uint8_t *)buf, len, &type_id, &flags, &wire_len);
    if (pr != 0)
    {
        return pr;
    }

    size_t total = 0;
    if (ckd_add(&total, (size_t)KITH_PROTO_HDR_SIZE, (size_t)wire_len))
    {
        return kith_error_return(KITH_EPROTO);
    }
    *out_total = total;
    return 0;
}

/*---------------------------------------------------------------------------
 * rejection counter
 *-------------------------------------------------------------------------*/

int kith_proto_rejections(const kith_proto_t *proto, uint64_t *out_rejections)
{
    if (proto == nullptr || out_rejections == nullptr)
    {
        return kith_error_return(KITH_EINVAL);
    }
    *out_rejections = atomic_load_explicit(&proto->rejections, memory_order_relaxed);
    return 0;
}

/*---------------------------------------------------------------------------
 * correlation id hex formatting
 *-------------------------------------------------------------------------*/

size_t kith_proto_correlation_hex(uint64_t correlation_id, char buf[17])
{
    if (buf == nullptr)
    {
        return 0;
    }
    static const char hex_digits[16] = {
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
    for (int i = 0; i < 16; ++i)
    {
        unsigned int nibble =
            (unsigned int)(correlation_id >> (4u * (unsigned int)(15 - i))) & 0x0Fu;
        buf[i] = hex_digits[nibble];
    }
    buf[16] = '\0';
    return 16;
}
