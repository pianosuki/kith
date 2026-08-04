#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "kith/proto/proto.h"
#include "kith/types.h"
#include "kith/version.h"

static int check_cond(bool ok, int line)
{
    if (!ok)
    {
        (void)fprintf(stderr, "proto codec: assertion at line %d failed\n", line);
        return 1;
    }
    return 0;
}

#define CHECK(cond) failures += check_cond((cond), __LINE__)

static int test_create_validation(void)
{
    int failures = 0;
    kith_proto_t *p = nullptr;
    CHECK(kith_proto_create(nullptr, nullptr, nullptr) == kith_error_return(KITH_EINVAL));

    kith_proto_params_t bad_abi = {
        .size = sizeof(kith_proto_params_t),
        .abi_version = KITH_ABI_VERSION + 1u,
    };
    CHECK(kith_proto_create(&bad_abi, nullptr, &p) == kith_error_return(KITH_EABIVER));
    CHECK(p == nullptr);

    kith_proto_params_t small = {
        .size = 8u,
        .abi_version = KITH_ABI_VERSION,
    };
    CHECK(kith_proto_create(&small, nullptr, &p) == kith_error_return(KITH_ESIZE));
    CHECK(p == nullptr);

    kith_proto_destroy(p);
    return failures;
}

static int test_roundtrip_no_correlation(void)
{
    int failures = 0;
    kith_proto_t *p = nullptr;
    CHECK(kith_proto_create(nullptr, nullptr, &p) == 0);
    CHECK(kith_proto_register_type_id(p, "move", 1000u) == 0);

    const uint8_t payload[] = {0x01u, 0x02u, 0x03u, 0x04u, 0x05u};
    uint8_t buf[64];
    const size_t n = kith_proto_encode(p, 1000u, 0u, 0ull, payload, 5u, buf, sizeof(buf));
    CHECK(n == KITH_PROTO_HDR_SIZE + 5u);

    kith_proto_frame_t f;
    size_t consumed = 0;
    CHECK(kith_proto_decode(p, buf, n, &f, &consumed) == 0);
    CHECK(consumed == n);
    CHECK(f.type_id == 1000u);
    CHECK(f.flags == 0u);
    CHECK(!f.has_correlation);
    CHECK(f.correlation_id == 0ull);
    CHECK(f.payload_len == 5u);
    CHECK(f.payload != nullptr);
    if (f.payload != nullptr)
    {
        CHECK(memcmp(f.payload, payload, 5u) == 0);
    }

    kith_proto_destroy(p);
    return failures;
}

static int test_roundtrip_with_correlation(void)
{
    int failures = 0;
    kith_proto_t *p = nullptr;
    CHECK(kith_proto_create(nullptr, nullptr, &p) == 0);
    CHECK(kith_proto_register_type_id(p, "spawn", 1001u) == 0);

    const uint8_t payload[] = {0xaau, 0xbbu, 0xccu};
    const uint64_t corr = 0x0123456789abcdefull;
    uint8_t buf[64];
    const size_t n = kith_proto_encode(
        p, 1001u, KITH_PROTO_FLAG_CORRELATION, corr, payload, 3u, buf, sizeof(buf));
    /* wire payload_len = 3 (payload) + 8 (trailer) = 11; total = 10 + 11 = 21 */
    CHECK(n == KITH_PROTO_HDR_SIZE + 3u + KITH_PROTO_CORR_TRAILER_SIZE);

    /* The on-wire payload_len field (bytes 6..9) must include the trailer. */
    const uint8_t *bytes = buf;
    uint32_t wire_len = ((uint32_t)bytes[6] << 24u) | ((uint32_t)bytes[7] << 16u) |
                        ((uint32_t)bytes[8] << 8u) | (uint32_t)bytes[9];
    CHECK(wire_len == 3u + KITH_PROTO_CORR_TRAILER_SIZE);
    CHECK(bytes[3] == KITH_PROTO_FLAG_CORRELATION);

    kith_proto_frame_t f;
    size_t consumed = 0;
    CHECK(kith_proto_decode(p, buf, n, &f, &consumed) == 0);
    CHECK(consumed == n);
    CHECK(f.type_id == 1001u);
    CHECK((f.flags & KITH_PROTO_FLAG_CORRELATION) != 0u);
    CHECK(f.has_correlation);
    CHECK(f.correlation_id == corr);
    CHECK(f.payload_len == 3u);
    CHECK(f.payload != nullptr);
    if (f.payload != nullptr)
    {
        CHECK(memcmp(f.payload, payload, 3u) == 0);
    }

    kith_proto_destroy(p);
    return failures;
}

static int test_encode_flags_passthrough(void)
{
    int failures = 0;
    kith_proto_t *p = nullptr;
    CHECK(kith_proto_create(nullptr, nullptr, &p) == 0);
    CHECK(kith_proto_register_type_id(p, "chat", 1002u) == 0);

    /* Caller-supplied bits ride the header byte verbatim: the codec is
     * flags-agnostic, and bit 0x40 is undefined at v1 — its meaning is
     * the caller's policy, not the codec's. */
    const uint8_t flags = 0x40u | KITH_PROTO_FLAG_CORRELATION;
    const uint8_t payload[] = {0x11u};
    uint8_t buf[64];
    const size_t n = kith_proto_encode(p, 1002u, flags, 7ull, payload, 1u, buf, sizeof(buf));
    CHECK(n == KITH_PROTO_HDR_SIZE + 1u + KITH_PROTO_CORR_TRAILER_SIZE);
    CHECK(buf[3] == flags);

    kith_proto_frame_t f;
    size_t consumed = 0;
    CHECK(kith_proto_decode(p, buf, n, &f, &consumed) == 0);
    CHECK(consumed == n);
    CHECK(f.flags == flags);
    CHECK(f.has_correlation);
    CHECK(f.correlation_id == 7ull);

    /* The trailer is keyed on the correlation bit only: an unknown bit
     * alone writes no trailer and consumes no correlation id. */
    const size_t un = kith_proto_encode(p, 1002u, 0x40u, 9ull, payload, 1u, buf, sizeof(buf));
    CHECK(un == KITH_PROTO_HDR_SIZE + 1u);
    CHECK(buf[3] == 0x40u);

    CHECK(kith_proto_decode(p, buf, un, &f, &consumed) == 0);
    CHECK(consumed == un);
    CHECK(f.flags == 0x40u);
    CHECK(!f.has_correlation);

    kith_proto_destroy(p);
    return failures;
}

static int test_empty_payload_roundtrip(void)
{
    int failures = 0;
    kith_proto_t *p = nullptr;
    CHECK(kith_proto_create(nullptr, nullptr, &p) == 0);
    CHECK(kith_proto_register_type_id(p, "ping", 2000u) == 0);

    uint8_t buf[64];
    const size_t n = kith_proto_encode(p, 2000u, 0u, 0ull, nullptr, 0u, buf, sizeof(buf));
    CHECK(n == KITH_PROTO_HDR_SIZE);

    kith_proto_frame_t f;
    size_t consumed = 0;
    CHECK(kith_proto_decode(p, buf, n, &f, &consumed) == 0);
    CHECK(consumed == n);
    CHECK(f.type_id == 2000u);
    CHECK(f.payload_len == 0u);
    CHECK(f.payload == nullptr);

    kith_proto_destroy(p);
    return failures;
}

static int test_correlation_only_frame(void)
{
    int failures = 0;
    kith_proto_t *p = nullptr;
    CHECK(kith_proto_create(nullptr, nullptr, &p) == 0);
    CHECK(kith_proto_register_type_id(p, "trace", 2001u) == 0);

    /* A frame with the correlation flag and an empty message payload: the
     * trailer is the whole payload region. */
    const uint64_t corr = 0xfedcba9876543210ull;
    uint8_t buf[64];
    const size_t n = kith_proto_encode(
        p, 2001u, KITH_PROTO_FLAG_CORRELATION, corr, nullptr, 0u, buf, sizeof(buf));
    CHECK(n == KITH_PROTO_HDR_SIZE + KITH_PROTO_CORR_TRAILER_SIZE);

    kith_proto_frame_t f;
    size_t consumed = 0;
    CHECK(kith_proto_decode(p, buf, n, &f, &consumed) == 0);
    CHECK(f.has_correlation);
    CHECK(f.correlation_id == corr);
    CHECK(f.payload_len == 0u);
    CHECK(f.payload == nullptr);

    kith_proto_destroy(p);
    return failures;
}

static int test_decode_incomplete(void)
{
    int failures = 0;
    kith_proto_t *p = nullptr;
    CHECK(kith_proto_create(nullptr, nullptr, &p) == 0);
    CHECK(kith_proto_register_type_id(p, "move", 1000u) == 0);

    const uint8_t payload[] = {0x01u, 0x02u, 0x03u, 0x04u, 0x05u};
    uint8_t buf[64];
    const size_t n = kith_proto_encode(p, 1000u, 0u, 0ull, payload, 5u, buf, sizeof(buf));

    kith_proto_frame_t f;
    size_t consumed = 99u;
    /* Zero bytes: incomplete header. */
    CHECK(kith_proto_decode(p, buf, 0u, &f, &consumed) == kith_error_return(KITH_EAGAIN));
    CHECK(consumed == 0u);
    /* Partial header (4 of 10 bytes). */
    CHECK(kith_proto_decode(p, buf, 4u, &f, &consumed) == kith_error_return(KITH_EAGAIN));
    /* Full header but no payload. */
    CHECK(kith_proto_decode(p, buf, KITH_PROTO_HDR_SIZE, &f, &consumed) ==
          kith_error_return(KITH_EAGAIN));
    /* Header + 2 of 5 payload bytes. */
    CHECK(kith_proto_decode(p, buf, KITH_PROTO_HDR_SIZE + 2u, &f, &consumed) ==
          kith_error_return(KITH_EAGAIN));
    /* Full frame decodes. */
    CHECK(kith_proto_decode(p, buf, n, &f, &consumed) == 0);
    CHECK(consumed == n);

    kith_proto_destroy(p);
    return failures;
}

static int test_decode_malformed(void)
{
    int failures = 0;
    kith_proto_t *p = nullptr;
    CHECK(kith_proto_create(nullptr, nullptr, &p) == 0);
    CHECK(kith_proto_register_type_id(p, "move", 1000u) == 0);

    uint8_t buf[64];
    const size_t n = kith_proto_encode(p, 1000u, 0u, 0ull, nullptr, 0u, buf, sizeof(buf));
    CHECK(n == KITH_PROTO_HDR_SIZE);

    kith_proto_frame_t f;
    /* Bad magic. */
    buf[0] = 0x00u;
    CHECK(kith_proto_decode(p, buf, n, &f, nullptr) == kith_error_return(KITH_EPROTO));
    buf[0] = (uint8_t)KITH_PROTO_MAGIC0;
    /* Bad version. */
    buf[2] = 0xffu;
    CHECK(kith_proto_decode(p, buf, n, &f, nullptr) == kith_error_return(KITH_EPROTO));
    buf[2] = (uint8_t)KITH_PROTO_VERSION;

    /* Correlation flag set with a wire payload shorter than the trailer. */
    const uint8_t short_payload[4] = {0};
    const size_t sn = kith_proto_encode(p, 1000u, 0u, 0ull, short_payload, 4u, buf, sizeof(buf));
    CHECK(sn == KITH_PROTO_HDR_SIZE + 4u);
    buf[3] = (uint8_t)KITH_PROTO_FLAG_CORRELATION;
    CHECK(kith_proto_decode(p, buf, sn, &f, nullptr) == kith_error_return(KITH_EPROTO));

    kith_proto_destroy(p);

    /* Oversize: a proto with a tiny max payload rejects a large length. */
    kith_proto_params_t params = {
        .size = sizeof(kith_proto_params_t),
        .abi_version = KITH_ABI_VERSION,
        .max_payload = 4u,
    };
    kith_proto_t *small = nullptr;
    CHECK(kith_proto_create(&params, nullptr, &small) == 0);
    CHECK(kith_proto_register_type_id(small, "move", 1000u) == 0);
    uint8_t big[64];
    const size_t bn = kith_proto_encode(small, 1000u, 0u, 0ull, big, 5u, big, sizeof(big));
    CHECK(bn == 0u); /* 5 > 4 max */
    kith_proto_destroy(small);
    return failures;
}

static int test_decode_unknown_type(void)
{
    int failures = 0;
    kith_proto_t *p = nullptr;
    CHECK(kith_proto_create(nullptr, nullptr, &p) == 0);
    CHECK(kith_proto_register_type_id(p, "move", 1000u) == 0);

    uint8_t buf[64];
    /* Encode a type that IS registered, then mutate the type_id bytes to an
     * unregistered id and expect -KITH_EPROTO. */
    const size_t n = kith_proto_encode(p, 1000u, 0u, 0ull, nullptr, 0u, buf, sizeof(buf));
    CHECK(n == KITH_PROTO_HDR_SIZE);
    buf[4] = 0x27u; /* type_id = 9999 (unregistered) */
    buf[5] = 0x0fu;

    kith_proto_frame_t f;
    CHECK(kith_proto_decode(p, buf, n, &f, nullptr) == kith_error_return(KITH_EPROTO));
    kith_proto_destroy(p);
    return failures;
}

static int test_encode_measure_and_truncation(void)
{
    int failures = 0;
    kith_proto_t *p = nullptr;
    CHECK(kith_proto_create(nullptr, nullptr, &p) == 0);
    CHECK(kith_proto_register_type_id(p, "move", 1000u) == 0);

    const uint8_t payload[] = {0x01u, 0x02u, 0x03u, 0x04u, 0x05u};
    /* Measure-only: NULL buffer. */
    const size_t need = kith_proto_encode(p, 1000u, 0u, 0ull, payload, 5u, nullptr, 0u);
    CHECK(need == KITH_PROTO_HDR_SIZE + 5u);
    /* Measure-only: zero cap. */
    uint8_t buf[64];
    const size_t need2 = kith_proto_encode(p, 1000u, 0u, 0ull, payload, 5u, buf, 0u);
    CHECK(need2 == need);
    /* Truncation: cap < total returns the full length, writes the leading cap. */
    const size_t cap = KITH_PROTO_HDR_SIZE + 2u;
    const size_t got = kith_proto_encode(p, 1000u, 0u, 0ull, payload, 5u, buf, cap);
    CHECK(got == need);
    /* The leading cap bytes are the header + first 2 payload bytes. */
    CHECK(buf[0] == (uint8_t)KITH_PROTO_MAGIC0);
    CHECK(buf[1] == (uint8_t)KITH_PROTO_MAGIC1);
    CHECK(buf[2] == (uint8_t)KITH_PROTO_VERSION);
    CHECK(buf[9] == 5u); /* on-wire payload_len (last header byte) */
    CHECK(buf[KITH_PROTO_HDR_SIZE] == 0x01u);
    CHECK(buf[KITH_PROTO_HDR_SIZE + 1u] == 0x02u);

    /* NULL payload with nonzero length: encode rejects (returns 0). */
    CHECK(kith_proto_encode(p, 1000u, 0u, 0ull, nullptr, 5u, buf, sizeof(buf)) == 0u);

    kith_proto_destroy(p);
    return failures;
}

static int test_correlation_hex(void)
{
    int failures = 0;
    char hex[17];
    const size_t n = kith_proto_correlation_hex(0x0123456789abcdefull, hex);
    CHECK(n == 16u);
    CHECK(strcmp(hex, "0123456789abcdef") == 0);

    kith_proto_correlation_hex(0ull, hex);
    CHECK(strcmp(hex, "0000000000000000") == 0);

    kith_proto_correlation_hex(0xffffffffffffffffull, hex);
    CHECK(strcmp(hex, "ffffffffffffffff") == 0);

    CHECK(kith_proto_correlation_hex(1ull, nullptr) == 0u);
    return failures;
}

static int test_decode_arg_validation(void)
{
    int failures = 0;
    kith_proto_t *p = nullptr;
    CHECK(kith_proto_create(nullptr, nullptr, &p) == 0);
    CHECK(kith_proto_register_type_id(p, "move", 1000u) == 0);
    uint8_t buf[64];
    const size_t n = kith_proto_encode(p, 1000u, 0u, 0ull, nullptr, 0u, buf, sizeof(buf));

    kith_proto_frame_t f;
    CHECK(kith_proto_decode(nullptr, buf, n, &f, nullptr) == kith_error_return(KITH_EINVAL));
    CHECK(kith_proto_decode(p, nullptr, n, &f, nullptr) == kith_error_return(KITH_EINVAL));
    CHECK(kith_proto_decode(p, buf, n, nullptr, nullptr) == kith_error_return(KITH_EINVAL));
    kith_proto_destroy(p);
    return failures;
}

static int test_declared_total(void)
{
    int failures = 0;
    kith_proto_t *p = nullptr;
    CHECK(kith_proto_create(nullptr, nullptr, &p) == 0);
    CHECK(kith_proto_register_type_id(p, "move", 1000u) == 0);

    const uint8_t payload[] = {0x01u, 0x02u, 0x03u, 0x04u, 0x05u};
    uint8_t buf[64];
    const size_t n = kith_proto_encode(p, 1000u, 0u, 0ull, payload, 5u, buf, sizeof(buf));
    CHECK(n == KITH_PROTO_HDR_SIZE + 5u);

    /* A complete header yields the total even with no body bytes. */
    size_t total = 0u;
    CHECK(kith_proto_declared_total(buf, KITH_PROTO_HDR_SIZE, &total) == 0);
    CHECK(total == n);

    /* A partial buffer works too: the header alone decides. */
    CHECK(kith_proto_declared_total(buf, n, &total) == 0);
    CHECK(total == n);

    /* The correlation flag changes nothing: the header's length field
     * already includes the trailer. */
    const size_t cn = kith_proto_encode(
        p, 1000u, KITH_PROTO_FLAG_CORRELATION, 7ull, payload, 5u, buf, sizeof(buf));
    CHECK(cn == KITH_PROTO_HDR_SIZE + 5u + KITH_PROTO_CORR_TRAILER_SIZE);
    CHECK(kith_proto_declared_total(buf, KITH_PROTO_HDR_SIZE, &total) == 0);
    CHECK(total == cn);

    /* Incomplete header: no verdict yet. */
    CHECK(kith_proto_declared_total(buf, 0u, &total) == kith_error_return(KITH_EAGAIN));
    CHECK(kith_proto_declared_total(buf, 9u, &total) == kith_error_return(KITH_EAGAIN));

    /* Malformed header. */
    buf[0] = 0x00u;
    CHECK(kith_proto_declared_total(buf, KITH_PROTO_HDR_SIZE, &total) ==
          kith_error_return(KITH_EPROTO));
    buf[0] = (uint8_t)KITH_PROTO_MAGIC0;
    buf[2] = 0xffu;
    CHECK(kith_proto_declared_total(buf, KITH_PROTO_HDR_SIZE, &total) ==
          kith_error_return(KITH_EPROTO));
    buf[2] = (uint8_t)KITH_PROTO_VERSION;
    CHECK(kith_proto_declared_total(buf, KITH_PROTO_HDR_SIZE, &total) == 0);
    CHECK(total == cn);

    /* Argument validation. */
    CHECK(kith_proto_declared_total(nullptr, KITH_PROTO_HDR_SIZE, &total) ==
          kith_error_return(KITH_EINVAL));
    CHECK(kith_proto_declared_total(buf, KITH_PROTO_HDR_SIZE, nullptr) ==
          kith_error_return(KITH_EINVAL));

    kith_proto_destroy(p);
    return failures;
}

static int test_decode_rejection_counter(void)
{
    int failures = 0;
    kith_proto_t *p = nullptr;
    CHECK(kith_proto_create(nullptr, nullptr, &p) == 0);
    CHECK(kith_proto_register_type_id(p, "move", 1000u) == 0);

    uint64_t rejections = 0u;
    CHECK(kith_proto_rejections(p, &rejections) == 0);
    CHECK(rejections == 0u);

    uint8_t buf[64];
    const size_t n = kith_proto_encode(p, 1000u, 0u, 0ull, nullptr, 0u, buf, sizeof(buf));
    CHECK(n == KITH_PROTO_HDR_SIZE);
    kith_proto_frame_t f;

    /* Bad magic: counted. */
    buf[0] = 0x00u;
    CHECK(kith_proto_decode(p, buf, n, &f, nullptr) == kith_error_return(KITH_EPROTO));
    CHECK(kith_proto_rejections(p, &rejections) == 0);
    CHECK(rejections == 1u);
    buf[0] = (uint8_t)KITH_PROTO_MAGIC0;

    /* Bad version: counted. */
    buf[2] = 0xffu;
    CHECK(kith_proto_decode(p, buf, n, &f, nullptr) == kith_error_return(KITH_EPROTO));
    CHECK(kith_proto_rejections(p, &rejections) == 0);
    CHECK(rejections == 2u);
    buf[2] = (uint8_t)KITH_PROTO_VERSION;

    /* Unknown type id: counted. */
    buf[4] = 0x27u;
    buf[5] = 0x0fu;
    CHECK(kith_proto_decode(p, buf, n, &f, nullptr) == kith_error_return(KITH_EPROTO));
    CHECK(kith_proto_rejections(p, &rejections) == 0);
    CHECK(rejections == 3u);
    buf[4] = 0x00u;
    buf[5] = 0x00u;

    /* A frame still arriving is not a rejection: the count holds. */
    CHECK(kith_proto_decode(p, buf, KITH_PROTO_HDR_SIZE - 1u, &f, nullptr) ==
          kith_error_return(KITH_EAGAIN));
    CHECK(kith_proto_rejections(p, &rejections) == 0);
    CHECK(rejections == 3u);

    /* Argument validation is not a rejection: the count holds. */
    CHECK(kith_proto_decode(nullptr, buf, n, &f, nullptr) == kith_error_return(KITH_EINVAL));
    CHECK(kith_proto_rejections(p, &rejections) == 0);
    CHECK(rejections == 3u);

    /* Argument validation on the accessor itself. */
    CHECK(kith_proto_rejections(nullptr, &rejections) == kith_error_return(KITH_EINVAL));
    CHECK(kith_proto_rejections(p, nullptr) == kith_error_return(KITH_EINVAL));

    kith_proto_destroy(p);

    /* A wire length beyond the configured payload bound: counted on its
     * own handle. The header declares 9000 payload bytes; the codec's
     * bound is 4. */
    kith_proto_params_t tiny_params = {
        .size = sizeof(kith_proto_params_t),
        .abi_version = KITH_ABI_VERSION,
        .max_payload = 4u,
    };
    kith_proto_t *tiny = nullptr;
    CHECK(kith_proto_create(&tiny_params, nullptr, &tiny) == 0);
    CHECK(kith_proto_encode(tiny, 1000u, 0u, 0ull, nullptr, 0u, buf, sizeof(buf)) ==
          KITH_PROTO_HDR_SIZE);
    buf[6] = 0x00u;
    buf[7] = 0x00u;
    buf[8] = 0x23u;
    buf[9] = 0x28u;
    CHECK(kith_proto_decode(tiny, buf, KITH_PROTO_HDR_SIZE, &f, nullptr) ==
          kith_error_return(KITH_EPROTO));
    CHECK(kith_proto_rejections(tiny, &rejections) == 0);
    CHECK(rejections == 1u);
    kith_proto_destroy(tiny);
    return failures;
}

int main(void)
{
    int rc = 0;
    rc |= test_create_validation();
    rc |= test_roundtrip_no_correlation();
    rc |= test_roundtrip_with_correlation();
    rc |= test_encode_flags_passthrough();
    rc |= test_empty_payload_roundtrip();
    rc |= test_correlation_only_frame();
    rc |= test_decode_incomplete();
    rc |= test_decode_malformed();
    rc |= test_decode_unknown_type();
    rc |= test_decode_rejection_counter();
    rc |= test_encode_measure_and_truncation();
    rc |= test_correlation_hex();
    rc |= test_decode_arg_validation();
    rc |= test_declared_total();
    if (rc != 0)
    {
        (void)fprintf(stderr, "proto codec tests FAILED\n");
    }
    return rc;
}
