/* Corpus generator for the proto decoder fuzz target.
 *
 * Writes the deterministic seed corpus the fuzz_proto_decode ctest smoke
 * replays: valid round-trips through kith_proto_encode over the registry-
 * boundary type ids, plus targeted corruption (bad magic/version, oversized
 * declared payload length, unregistered type id, truncated header/payload)
 * that hands the fuzzer the decoder's reject paths without committing any
 * binary artifact to the repository.
 */

#include <errno.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "kith/proto/proto.h"

#include <sys/stat.h>

/* The harness registers these same four ids; decode accepts only registered
 * ids, so both tables must match for the boundary-id seeds to decode. */
static const struct
{
    const char *name;
    uint16_t id;
} fuzz_types[] = {
    {"zero", 0},
    {"plain", 42},
    {"user", 1000},
    {"max", 65535},
};

static const char payload_text[] = "kith corpus payload 0123456789abcdef";

struct seed_frame_spec
{
    uint16_t type_id;
    uint8_t flags;
    uint64_t correlation_id;
    const uint8_t *payload;
    uint32_t payload_len;
};

static int ensure_output_dir(const char *dir)
{
    /* mkdir(2) creates the leaf only, so each prefix of a nested path is
     * created in turn; an already-present component is not an error. */
    char scratch[512];
    const size_t len = strlen(dir);
    if (len == 0 || len >= sizeof(scratch))
    {
        return -1;
    }
    memcpy(scratch, dir, len + 1u);
    for (size_t i = 1u; i < len; ++i)
    {
        if (scratch[i] != '/')
        {
            continue;
        }
        scratch[i] = '\0';
        if (mkdir(scratch, 0755) != 0 && errno != EEXIST)
        {
            return -1;
        }
        scratch[i] = '/';
    }
    if (mkdir(scratch, 0755) != 0 && errno != EEXIST)
    {
        return -1;
    }
    return 0;
}

static int write_seed(const char *dir, const char *name, const uint8_t *bytes, size_t len)
{
    char path[512];
    const int n = snprintf(path, sizeof(path), "%s/%s", dir, name);
    if (n < 0 || (size_t)n >= sizeof(path))
    {
        return -1;
    }
    FILE *out = fopen(path, "wb");
    if (out == nullptr)
    {
        return -1;
    }
    const size_t written = fwrite(bytes, 1u, len, out);
    const int closed = fclose(out);
    return (written == len && closed == 0) ? 0 : -1;
}

static size_t
build_frame(const kith_proto_t *proto, const struct seed_frame_spec *spec, uint8_t *buf, size_t cap)
{
    return kith_proto_encode(proto,
                             spec->type_id,
                             spec->flags,
                             spec->correlation_id,
                             spec->payload,
                             spec->payload_len,
                             buf,
                             cap);
}

static int emit_simple_seeds(const kith_proto_t *proto, const char *dir)
{
    static const struct
    {
        const char *name;
        struct seed_frame_spec spec;
    } seeds[] = {
        {"valid_plain.bin", {42u, 0u, 0ull, (const uint8_t *)payload_text, 32u}},
        {"valid_empty.bin", {42u, 0u, 0ull, nullptr, 0u}},
        {"valid_corr_only.bin",
         {42u, (uint8_t)KITH_PROTO_FLAG_CORRELATION, 0x0102030405060708ull, nullptr, 0u}},
        {"valid_corr_payload.bin",
         {42u,
          (uint8_t)KITH_PROTO_FLAG_CORRELATION,
          0x0102030405060708ull,
          (const uint8_t *)payload_text,
          16u}},
        {"valid_type_zero.bin", {0u, 0u, 0ull, (const uint8_t *)payload_text, 8u}},
        {"valid_type_max.bin", {65535u, 0u, 0ull, (const uint8_t *)payload_text, 8u}},
    };

    int failures = 0;
    for (size_t i = 0; i < sizeof(seeds) / sizeof(seeds[0]); ++i)
    {
        uint8_t buf[128];
        const size_t n = build_frame(proto, &seeds[i].spec, buf, sizeof(buf));
        failures += write_seed(dir, seeds[i].name, buf, n);
    }
    return failures;
}

static int emit_special_seeds(const kith_proto_t *proto, const char *dir)
{
    const struct seed_frame_spec plain = {42u, 0u, 0ull, (const uint8_t *)payload_text, 32u};
    const struct seed_frame_spec corr = {42u,
                                         (uint8_t)KITH_PROTO_FLAG_CORRELATION,
                                         0x0102030405060708ull,
                                         (const uint8_t *)payload_text,
                                         16u};
    uint8_t buf[128];
    int failures = 0;

    /* Two frames back-to-back: exercises the consecutive-frame loop. */
    size_t n = build_frame(proto, &plain, buf, sizeof(buf));
    n += build_frame(proto, &corr, buf + n, sizeof(buf) - n);
    failures += write_seed(dir, "concat_pair.bin", buf, n);

    n = build_frame(proto, &plain, buf, sizeof(buf));
    buf[0] = (uint8_t)(buf[0] ^ 0xffu);
    failures += write_seed(dir, "bad_magic.bin", buf, n);

    n = build_frame(proto, &plain, buf, sizeof(buf));
    buf[2] = (uint8_t)(buf[2] + 1u);
    failures += write_seed(dir, "bad_version.bin", buf, n);

    /* Encode never consults the registry, so an unregistered id can be
     * built directly and decode must reject it. */
    const struct seed_frame_spec unknown = {20000u, 0u, 0ull, (const uint8_t *)payload_text, 12u};
    n = build_frame(proto, &unknown, buf, sizeof(buf));
    failures += write_seed(dir, "unknown_type.bin", buf, n);

    /* Declared wire length of 4 MiB against the 1 MiB default maximum: the
     * reject fires off the header alone, so a tiny input covers it. */
    n = build_frame(proto, &plain, buf, sizeof(buf));
    buf[6] = 0x00u;
    buf[7] = 0x40u;
    buf[8] = 0x00u;
    buf[9] = 0x00u;
    failures += write_seed(dir, "oversize_declared.bin", buf, n);

    n = build_frame(proto, &plain, buf, sizeof(buf));
    failures += write_seed(dir, "truncated_header.bin", buf, 7u);

    uint8_t long_payload[48];
    memset(long_payload, 0xa5u, sizeof(long_payload));
    const struct seed_frame_spec longspec = {42u, 0u, 0ull, long_payload, 48u};
    n = build_frame(proto, &longspec, buf, sizeof(buf));
    failures += write_seed(dir, "truncated_payload.bin", buf, KITH_PROTO_HDR_SIZE + 20u);

    return failures;
}

int main(int argc, char **argv)
{
    if (argc != 2)
    {
        fprintf(stderr, "usage: fuzz_proto_seeds <output-dir>\n");
        return EXIT_FAILURE;
    }

    kith_proto_t *proto = nullptr;
    if (kith_proto_create(nullptr, nullptr, &proto) != 0)
    {
        fprintf(stderr, "fuzz_proto_seeds: proto create failed\n");
        return EXIT_FAILURE;
    }
    for (size_t i = 0; i < sizeof(fuzz_types) / sizeof(fuzz_types[0]); ++i)
    {
        if (kith_proto_register_type_id(proto, fuzz_types[i].name, fuzz_types[i].id) != 0)
        {
            fprintf(stderr, "fuzz_proto_seeds: register %s failed\n", fuzz_types[i].name);
            kith_proto_destroy(proto);
            return EXIT_FAILURE;
        }
    }
    if (ensure_output_dir(argv[1]) != 0)
    {
        fprintf(stderr, "fuzz_proto_seeds: cannot create %s\n", argv[1]);
        kith_proto_destroy(proto);
        return EXIT_FAILURE;
    }

    const int failures = emit_simple_seeds(proto, argv[1]) + emit_special_seeds(proto, argv[1]);
    kith_proto_destroy(proto);
    if (failures != 0)
    {
        fprintf(stderr, "fuzz_proto_seeds: %d seed(s) failed to write\n", failures);
        return EXIT_FAILURE;
    }
    return EXIT_SUCCESS;
}
