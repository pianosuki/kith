#ifndef KITH_PROTO_PROTO_H
#define KITH_PROTO_PROTO_H

#include <stddef.h>
#include <stdint.h>

#include "kith/api.h"
#include "kith/types.h"

/**
 * Wire codec, message-type registry, and correlation-ID trailer.
 *
 * A proto handle owns a runtime registry of message types (a name to numeric
 * id map) and the codec configuration (the maximum payload size). The codec
 * frames opaque payload blobs into a length-prefixed binary frame and parses
 * them back; it does not know a message's field layout. Per-message payload
 * (de)serialization is the caller's concern.
 *
 * The frame format is a 10-byte header followed by a payload region:
 *
 *   offset  field         width  encoding
 *   ------  -----------   -----  --------
 *   0       magic         2      0x4B 0x54 ('K' 'T')
 *   2       version       1      KITH_PROTO_VERSION
 *   3       flags         1      bit mask (see kith_proto_flag)
 *   4       type_id       2      big-endian unsigned
 *   6       payload_len   4      big-endian unsigned
 *   10      payload       N      caller bytes
 *
 * When @c KITH_PROTO_FLAG_CORRELATION is set in the header flags, an 8-byte
 * big-endian correlation-ID trailer occupies the LAST 8 bytes of the payload
 * region and is INCLUDED in the @c payload_len field. The codec appends the
 * trailer on encode and strips it on decode, so a caller observes the message
 * payload without the trailer. A frame without the flag carries no trailer;
 * both forms are accepted on decode.
 *
 * Flag bits are meaningful only within a wire version. The version byte is
 * the dialect boundary: a version the decoder does not implement is
 * hard-rejected with -KITH_EPROTO. Within an implemented version, unknown
 * flag bits are ignored on decode and passed through to the frame view
 * unchanged: they never alter the meaning of a known bit, the header
 * layout, or the length semantics — a future feature needing different
 * framing is a new version byte, not a new bit. The codec is
 * flags-agnostic: encode writes the caller's flag bits verbatim, so a
 * sender sets only the bits defined for its version.
 *
 * The handle is owned by the composition root and passed by pointer; there is
 * no global accessor. Registry mutation (register) and read (lookup, encode,
 * decode) are serialized by a per-handle mutex, so concurrent calls on the
 * same handle do not interleave. Lifecycle calls (create/destroy) must not
 * race with each other or with an in-flight registry or codec call.
 */

/**
 * @defgroup kith_proto Protocol
 * @{
 */

/**
 * Fixed wire-format invariants. The underlying type is fixed so a constant
 * stored in an ABI surface stays a fixed width. These are protocol
 * invariants, not configuration: changing any of them breaks wire
 * compatibility. The correlation-trailer width and the
 * @c KITH_PROTO_FLAG_CORRELATION mechanism are fixed; the header layout is
 * the v1 wire contract.
 */
enum kith_proto_format : unsigned int
{
    /** Magic byte at frame offset 0 ('K'), a first-line reject for non-kith streams. */
    KITH_PROTO_MAGIC0 = 0x4Bu,
    /** Magic byte at frame offset 1 ('T'). */
    KITH_PROTO_MAGIC1 = 0x54u,
    /** Wire-format version carried in the header version byte. */
    KITH_PROTO_VERSION = 1u,
    /** Fixed header width in bytes. */
    KITH_PROTO_HDR_SIZE = 10u,
    /** Correlation-ID trailer width in bytes (big-endian uint64). */
    KITH_PROTO_CORR_TRAILER_SIZE = 8u,
    /**
     * Reserved id floor for game-specific message types (ids >= this value).
     * Framework message types use ids below it. The registry ships empty;
     * defining a type is additive (a new registered id plus a documented
     * contract).
     */
    KITH_PROTO_TYPE_USER_BASE = 1000u,
};

/**
 * Default maximum payload size (1 MiB) when @c kith_proto_params_t.max_payload
 * is 0. Frames whose @c payload_len exceeds the configured maximum are rejected
 * on decode with -KITH_EPROTO before any allocation, defending against
 * memory-exhaustion from a hostile length.
 */
#define KITH_PROTO_DEFAULT_MAX_PAYLOAD (1024u * 1024u)

/**
 * Frame header flag bits. The underlying type is fixed so a flags field stored
 * in an ABI surface stays a fixed width.
 */
enum kith_proto_flag : unsigned int
{
    /** Enables the 8-byte big-endian correlation-ID trailer. */
    KITH_PROTO_FLAG_CORRELATION = 0x02u,
};

/** Alias of enum kith_proto_flag. */
typedef enum kith_proto_flag kith_proto_flag_t;

/**
 * Opaque proto handle.
 *
 * @ownership callee — created by kith_proto_create, destroyed by
 *           kith_proto_destroy. The handle owns the message-type registry and
 *           the mutex that serializes access; all owned storage is released on
 *           destroy.
 */
typedef struct kith_proto kith_proto_t;

/**
 * Creation parameters. Size-versioned: callers set @p size to
 * sizeof(kith_proto_params_t) and @p abi_version to KITH_ABI_VERSION at their
 * compile time; the runtime rejects structs from an incompatible generation or
 * an undersized size. Future additive fields occupy the reserved slots so the
 * layout below stays stable across generations.
 */
struct kith_proto_params
{
    /** Must be sizeof(kith_proto_params_t). */
    uint32_t size;
    /** Must be KITH_ABI_VERSION (from kith/version.h). */
    uint32_t abi_version;

    /**
     * Maximum payload size accepted by decode and produced by encode, in bytes.
     * The value does NOT include the 10-byte header. 0 selects
     * @c KITH_PROTO_DEFAULT_MAX_PAYLOAD. A header-only frame (payload 0) is
     * always accepted regardless of the configured maximum.
     */
    uint32_t max_payload;

    /** Reserved for future additive fields. Must be zero-filled. */
    void *reserved[8];
};

/** Alias of struct kith_proto_params. */
typedef struct kith_proto_params kith_proto_params_t;

/**
 * A decoded frame view. This is an exposed-layout value type (like struct
 * iovec and kith_logger_field_t), not an opaque handle: the field set is part of
 * the public contract and stays stable. @p payload points into the buffer
 * handed to kith_proto_decode (no copy); the caller must not free that buffer
 * while using the view.
 */
struct kith_proto_frame
{
    /** Registered message-type id. */
    uint16_t type_id;
    /** Header flag bits (kith_proto_flag). */
    uint8_t flags;
    /** True when KITH_PROTO_FLAG_CORRELATION was set and a trailer was stripped. */
    bool has_correlation;
    /** The 64-bit correlation ID carried by the trailer, host byte order. */
    uint64_t correlation_id;
    /** Payload bytes, or NULL when @p payload_len is 0. Points into the input. */
    const void *payload;
    /** Payload length in bytes, NOT counting the correlation trailer. */
    uint32_t payload_len;
};

/** Alias of struct kith_proto_frame. */
typedef struct kith_proto_frame kith_proto_frame_t;

/**
 * Build a proto handle from @p params.
 *
 * @param params    Creation parameters; @c size and @c abi_version must match
 *                  the runtime generation. NULL selects the defaults (1 MiB
 *                  max payload, empty registry).
 * @param alloc     Allocator for the new handle and every registry entry it
 *                  records, used again when kith_proto_destroy frees them.
 *                  NULL selects the default allocator; a supplied allocator
 *                  is validated (see kith_allocator_t) and must outlive the
 *                  handle.
 * @param out_proto Receives the new handle on success.
 * @return          0 on success, negative kith_error on failure:
 *                  - -KITH_EINVAL if @p out_proto is NULL, or @p alloc is
 *                    missing an operation,
 *                  - -KITH_EABIVER if @p params or @p alloc has an
 *                    incompatible abi_version,
 *                  - -KITH_ESIZE if @p params or @p alloc has an undersized
 *                    size,
 *                  - -KITH_ENOMEM on allocation failure,
 *                  - -KITH_ESTATE if the handle's mutex cannot be
 *                    initialized.
 * @thread_safety unsafe — must not be called concurrently with another
 *                kith_proto_create on the same @p out_proto slot.
 * @ownership callee — the returned handle is freed by the caller with
 *           kith_proto_destroy.
 */
[[nodiscard]] KITH_API int kith_proto_create(const kith_proto_params_t *params,
                                             const kith_allocator_t *alloc,
                                             kith_proto_t **out_proto);

/**
 * Release a proto handle and the registry it owns. Passing NULL is a no-op.
 *
 * @param proto  Proto handle. NULL is a no-op.
 * @thread_safety unsafe — no register/lookup/encode/decode may be in flight on
 *                @p proto when this is called.
 * @ownership callee — @p proto is consumed and freed by the call.
 */
KITH_API void kith_proto_destroy(kith_proto_t *proto);

/**
 * Register a message type by name with an explicit numeric id. The id is the
 * wire identity of the type and MUST be stable across peers (both sides of a
 * connection must register the same name at the same id before they exchange
 * frames of that type). Game-specific types use ids >=
 * @c KITH_PROTO_TYPE_USER_BASE.
 *
 * @param proto    Proto handle. NULL is an error.
 * @param name     NUL-terminated type name, non-NULL, non-empty. Copied at
 *                 registration; the caller may free @p name after the call.
 * @param type_id  Caller-chosen id in [0, 65535].
 * @return         0 on success, negative kith_error on failure:
 *                 - -KITH_EINVAL if @p proto or @p name is NULL, @p name is
 *                   empty, or @p type_id is not the id the registry holds for
 *                   @p name when @p name is already registered,
 *                 - -KITH_EEXIST if @p type_id is already registered to a
 *                   different name,
 *                 - -KITH_ENOMEM on allocation failure.
 * @thread_safety safe — concurrent calls on the same handle are serialized.
 * @ownership caller — @p name is borrowed for the call only and copied on
 *           success.
 */
[[nodiscard]] KITH_API int
kith_proto_register_type_id(kith_proto_t *proto, const char *name, uint16_t type_id);

/**
 * Look up a message type id by name.
 *
 * @param proto        Proto handle. NULL is an error.
 * @param name         NUL-terminated type name.
 * @param out_type_id  Receives the id on success.
 * @return             0 on success, negative kith_error on failure:
 *                     - -KITH_EINVAL if @p proto or @p name is NULL,
 *                     - -KITH_ENOENT if @p name is not registered.
 * @thread_safety safe.
 * @ownership caller — @p proto and @p name are borrowed for the call only;
 *           @p out_type_id is the caller's output storage.
 */
[[nodiscard]] KITH_API int
kith_proto_lookup_type(const kith_proto_t *proto, const char *name, uint16_t *out_type_id);

/**
 * Report the registered name for @p type_id, or NULL when @p type_id is not
 * registered or @p proto is NULL. The returned string is owned by the handle
 * and lives until the type is removed (the registry has no removal path in
 * this generation, so it lives for the handle's lifetime).
 *
 * @param proto    Proto handle.
 * @param type_id  Message-type id to look up.
 * @return         The registered name, or NULL when @p type_id is not registered or @p proto is
 * NULL.
 * @thread_safety safe.
 * @ownership callee — the returned string is borrowed from the proto registry
 *           and is valid for the handle's lifetime; the caller must not free
 *           it.
 */
KITH_API const char *kith_proto_type_name(const kith_proto_t *proto, uint16_t type_id);

/**
 * Encode a frame into @p buf (snprintf-style).
 *
 * Writes the 10-byte header, the @p payload bytes, and (when @p flags has
 * @c KITH_PROTO_FLAG_CORRELATION) the 8-byte big-endian trailer. The trailer is
 * included in the header's @c payload_len field per the backward-compatible
 * convention. @p payload_len is the message payload length, NOT counting the
 * trailer. The codec does not consult the registry on encode; the caller
 * selects a type id it has agreed with its peer.
 *
 * @param proto           Proto handle (for the max_payload bound). NULL is an
 *                        error.
 * @param type_id         Message-type id to write into the header.
 * @param flags           Header flag bits (kith_proto_flag).
 * @param correlation_id  Correlation ID written as the trailer when
 *                        @c KITH_PROTO_FLAG_CORRELATION is set in @p flags;
 *                        ignored otherwise.
 * @param payload         Payload bytes, or NULL when @p payload_len is 0.
 * @param payload_len     Payload length in bytes, NOT counting the trailer.
 * @param buf             Output buffer, or NULL to only measure the needed size.
 * @param cap             Bytes available at @p buf. 0 measures without writing.
 * @return                The byte count the frame would occupy, or 0 when the
 *                        frame exceeds the configured max payload or @p proto
 *                        is NULL. When @p cap is smaller than the frame, @p buf
 *                        receives the leading @p cap bytes and the full length
 *                        is returned (snprintf-style).
 * @thread_safety safe — concurrent calls on the same handle are serialized.
 * @ownership caller — @p payload is borrowed for the call only; @p buf is the
 *           caller's output storage.
 */
KITH_API size_t kith_proto_encode(const kith_proto_t *proto,
                                  uint16_t type_id,
                                  uint8_t flags,
                                  uint64_t correlation_id,
                                  const void *payload,
                                  uint32_t payload_len,
                                  void *buf,
                                  size_t cap);

/**
 * Decode the leading frame from @p buf.
 *
 * Inspects the leading bytes of @p buf (length @p len) and either decodes one
 * complete frame or reports that more bytes are needed. The decoded frame's
 * @p payload field points into @p buf (no copy). The registry is enforced: a
 * frame whose @c type_id is not registered is rejected with -KITH_EPROTO.
 *
 * @param proto        Proto handle. NULL is an error.
 * @param buf          Bytes available for decode. Need not contain a complete
 *                     frame; when it does not, -KITH_EAGAIN is returned and
 *                     the caller reads more bytes and retries the same buffer.
 * @param len          Bytes available at @p buf. 0 returns -KITH_EAGAIN.
 * @param out_frame    Receives the decoded view on success.
 * @param out_consumed Receives the byte count the leading frame occupied on
 *                     success (the caller advances its buffer by this amount).
 *                     May be NULL when the caller does not need the count.
 * @return             0 on success, negative kith_error on failure:
 *                     - -KITH_EAGAIN if @p buf holds an incomplete frame;
 *                       the caller reads more bytes and retries. @p out_consumed
 *                       is set to 0,
 *                     - -KITH_EPROTO if the frame is malformed (bad magic,
 *                       unsupported version, payload_len exceeds the configured
 *                       maximum, or type_id is not registered),
 *                     - -KITH_EINVAL if @p proto or @p buf or @p out_frame is
 *                       NULL.
 * @thread_safety safe — concurrent calls on the same handle are serialized.
 * @ownership caller — @p buf is borrowed for the call only; the decoded view
 *           references it.
 */
[[nodiscard]] KITH_API int kith_proto_decode(const kith_proto_t *proto,
                                             const void *buf,
                                             size_t len,
                                             kith_proto_frame_t *out_frame,
                                             size_t *out_consumed);

/**
 * Report the total wire size declared by the frame header at @p buf.
 *
 * Takes no proto handle: the declared total is a property of the wire bytes
 * alone, independent of any registry or configured bound. A 0 return means
 * the header is well-formed (magic, version, and length arithmetic), NOT
 * that the frame is acceptable — the codec's max_payload bound is not
 * consulted here, and the declared total may exceed @p len when the frame
 * body has not arrived yet. Readers of a bounded buffer compare the total
 * against their own ceiling to reject frames that can never complete.
 *
 * @param buf       Byte buffer. NULL is an error.
 * @param len       Bytes available at @p buf.
 * @param out_total Receives the declared total (header size plus the
 *                  header's declared length) on success.
 * @return          0 with @p out_total set, negative kith_error on failure:
 *                  - -KITH_EAGAIN if @p len is smaller than the header (no
 *                    verdict yet),
 *                  - -KITH_EPROTO if the magic or version does not match,
 *                  - -KITH_EINVAL if @p buf or @p out_total is NULL.
 * @thread_safety safe — stateless; a pure function of the input bytes.
 * @ownership caller — @p buf is borrowed for the call only; @p out_total is
 *           the caller's output storage.
 */
[[nodiscard]] KITH_API int
kith_proto_declared_total(const void *buf, size_t len, size_t *out_total);

/**
 * Read the monotonic count of decode-path rejections. A rejection is a
 * @c kith_proto_decode call that returned -KITH_EPROTO: a malformed header
 * (magic or version), a payload beyond the configured bound, an unknown
 * type id, or a correlation-trailer size violation. Frames still arriving
 * (-KITH_EAGAIN) and invalid arguments (-KITH_EINVAL) are not rejections.
 * The counter is monotonic and never reset. The composition root records
 * its delta as @c kith_proto_rejections_total; with the transport's
 * rb_max-close counter it reconciles the total malformed input a run
 * injected against what the two planes rejected.
 *
 * @param proto          Proto handle. NULL is an error.
 * @param out_rejections Receives the monotonic rejection count on success.
 * @return               0 on success, negative kith_error on failure:
 *                       - -KITH_EINVAL if @p proto or @p out_rejections is
 *                         NULL.
 * @thread_safety safe — the counter is atomic; readable from any thread.
 *                Incremented on the decoding thread at rejection time.
 * @ownership caller — @p out_rejections is the caller's output storage.
 */
[[nodiscard]] KITH_API int kith_proto_rejections(const kith_proto_t *proto,
                                                 uint64_t *out_rejections);

/**
 * Format @p correlation_id as a 16-character lowercase hex string into @p buf,
 * NUL-terminated. @p buf must hold at least 17 bytes. Returns 16 (the hex
 * length, not counting the NUL). This is the preferred JSON form named by
 * docs/event_schema.md.
 *
 * @param correlation_id  Correlation ID to format.
 * @param buf             Output buffer. Must hold at least 17 bytes.
 * @return                16, the hex length not counting the NUL.
 * @thread_safety safe — stateless.
 * @ownership caller — @p buf is the caller's output storage.
 */
KITH_API size_t kith_proto_correlation_hex(uint64_t correlation_id, char buf[17]);

/** @} */

#endif /* KITH_PROTO_PROTO_H */
