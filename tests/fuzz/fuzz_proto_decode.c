/* libFuzzer harness for the proto wire decoder (kith_proto_decode).
 *
 * Drives consecutive-frame decoding over each input against one shared
 * handle created at startup: decode allocates nothing and keeps no parse
 * state between calls, so a persistent registry exercises the steady-state
 * path without per-iteration create/destroy churn drowning the decode
 * signal. Every decoded field — the header fields, the full payload range,
 * and when flagged the correlation-trailer bytes — folds into a volatile
 * sink so no load can be optimized out of the measurement.
 */

#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>

#include "kith/proto/proto.h"

/* Entry points the libFuzzer runtime resolves by symbol name. */
int LLVMFuzzerInitialize(int *argc, char ***argv);
int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size);

/* Inputs above this cap are dropped; invocations pass a matching -max_len. */
enum
{
    FUZZ_MAX_INPUT = 65'536
};

static volatile uint64_t fuzz_fold_sink;
static kith_proto_t *fuzz_proto;

static void fuzz_destroy_handle(void)
{
    kith_proto_destroy(fuzz_proto);
}

static void fuzz_register_or_die(const char *name, uint16_t type_id)
{
    if (kith_proto_register_type_id(fuzz_proto, name, type_id) != 0)
    {
        abort();
    }
}

int LLVMFuzzerInitialize(int *argc, char ***argv)
{
    /* Registered ids sit on the registry boundaries: the low edge, an
     * ordinary id, the user floor, and the high edge. The corpus generator
     * registers the same four ids; decode accepts only registered ids. */
    static const struct
    {
        const char *name;
        uint16_t id;
    } types[] = {
        {"zero", 0},
        {"plain", 42},
        {"user", 1000},
        {"max", 65535},
    };
    (void)argc;
    (void)argv;

    if (kith_proto_create(nullptr, nullptr, &fuzz_proto) != 0)
    {
        abort();
    }
    for (size_t i = 0; i < sizeof(types) / sizeof(types[0]); ++i)
    {
        fuzz_register_or_die(types[i].name, types[i].id);
    }
    if (atexit(fuzz_destroy_handle) != 0)
    {
        abort();
    }
    return 0;
}

static uint64_t fold_bytes(uint64_t fold, const uint8_t *bytes, size_t len)
{
    for (size_t i = 0; i < len; ++i)
    {
        fold = (fold << 5u) | (fold >> 59u);
        fold ^= bytes[i];
    }
    return fold;
}

/* Decode one frame at buf, fold every observable byte of it into the sink,
 * and return the consumed count; 0 means no further frames (error or empty).
 * A success whose consumed count violates the frame contract is a decoder
 * defect and aborts rather than advancing past a bogus boundary. */
static size_t fuzz_step(const uint8_t *buf, size_t len)
{
    kith_proto_frame_t frame;
    size_t consumed = 0;
    if (kith_proto_decode(fuzz_proto, buf, len, &frame, &consumed) != 0)
    {
        return 0;
    }

    /* Success consumes exactly the leading frame; when the correlation flag
     * is set the trailer fold reads the final 8 consumed bytes, so the count
     * must also cover header + trailer before any pointer math runs. */
    if (consumed == 0 || consumed > len ||
        (frame.has_correlation && consumed < KITH_PROTO_HDR_SIZE + KITH_PROTO_CORR_TRAILER_SIZE))
    {
        abort();
    }

    uint64_t fold = (uint64_t)frame.type_id << 48u;
    fold |= (uint64_t)frame.flags << 40u;
    fold += frame.correlation_id;
    fold += (uint64_t)frame.has_correlation;
    fold += frame.payload_len;

    /* payload borrows buf; it is NULL exactly when payload_len is 0. */
    if (frame.payload != nullptr && frame.payload_len > 0)
    {
        fold = fold_bytes(fold, frame.payload, frame.payload_len);
    }
    if (frame.has_correlation)
    {
        fold = fold_bytes(
            fold, buf + consumed - KITH_PROTO_CORR_TRAILER_SIZE, KITH_PROTO_CORR_TRAILER_SIZE);
    }

    fuzz_fold_sink ^= fold;
    return consumed;
}

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    if (data == nullptr || size > (size_t)FUZZ_MAX_INPUT)
    {
        return 0;
    }

    size_t offset = 0;
    while (offset < size)
    {
        const size_t consumed = fuzz_step(data + offset, size - offset);
        if (consumed == 0)
        {
            break;
        }
        offset += consumed;
    }
    return 0;
}
