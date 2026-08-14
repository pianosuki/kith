#ifndef KITH_CLIENT_CLIENT_H
#define KITH_CLIENT_CLIENT_H

#include <stddef.h>
#include <stdint.h>

#include "kith/api.h"
#include "kith/proto/proto.h"
#include "kith/types.h"

/**
 * Headless client engine: bootstrap FSM, interactive command injection,
 * and per-client event drain.
 *
 * A client handle owns a bootstrap state machine, an outbound frame queue,
 * an event ring buffer, and keepalive state. The engine is
 * transport-agnostic: it owns no socket and performs no I/O. The caller
 * feeds decoded @c kith_proto_frame_t views via @c kith_client_feed_frame
 * and pops encoded frames via @c kith_client_pop_outbound. The caller owns
 * the connection (socket, reactor registration, read/write).
 *
 * The bootstrap FSM brings the connection to a ready state through a
 * caller-supplied step table. Each step produces an outbound frame on
 * entry and awaits a reply; the reply callback decides the next step
 * (advance, retry, or fail). The engine drives the FSM state — the step
 * content is the caller's concern.
 *
 * Interactive command injection (@c kith_client_submit_interactive)
 * encodes a command as a proto frame and enqueues it for sending. The
 * per-client event drain (@c kith_client_drain_events) returns events
 * that the caller published via @c kith_client_publish_event (typically
 * from the frame handler callback for non-bootstrap frames).
 *
 * The engine is single-threaded: all functions are unsafe to call
 * concurrently. The harness drives the engine from one thread.
 */

/* Forward declarations — the full types live in their respective headers. */
typedef struct kith_logger kith_logger_t;

/**
 * @defgroup kith_client Client
 * @{
 */

/**
 * Opaque client engine handle.
 *
 * @ownership callee — created by kith_client_create, destroyed by
 *           kith_client_destroy. Owns the outbound queue, the event ring,
 *           and the bootstrap step table. The proto and logger handles
 *           are borrowed for the handle's lifetime.
 */
typedef struct kith_client kith_client_t;

/**
 * Default values used when the corresponding field in
 * @c kith_client_params_t is set to zero.
 */
enum kith_client_default : unsigned int
{
    /** Default outbound frame queue capacity in frames (params field = 0). */
    KITH_CLIENT_DEFAULT_OUTBOUND_CAP = 64u,
    /** Default event ring capacity in event records. */
    KITH_CLIENT_DEFAULT_EVENT_CAP = 256u,
    /** Default ping interval in milliseconds (0 = no ping). */
    KITH_CLIENT_DEFAULT_PING_INTERVAL_MS = 1000u,
    /** Default reconnect base delay in milliseconds. */
    KITH_CLIENT_DEFAULT_RECONNECT_BASE_MS = 250u,
    /** Default reconnect max delay in milliseconds. */
    KITH_CLIENT_DEFAULT_RECONNECT_MAX_MS = 4000u,
};

/**
 * Fixed format invariants. The underlying type is fixed so a constant
 * stored in an ABI surface stays a fixed width. These are format
 * invariants, not configuration: changing them breaks the event record
 * layout.
 */
enum kith_client_format : unsigned int
{
    /** Maximum event payload in bytes. Events whose payload exceeds this
     *  are rejected by kith_client_publish_event. */
    KITH_CLIENT_EVENT_PAYLOAD_MAX = 1024u,
};

/**
 * Bootstrap FSM state. The underlying type is fixed so a state stored in
 * an ABI surface stays a fixed width.
 */
enum kith_client_bootstrap_state : unsigned int
{
    /** No bootstrap configured or not yet started. */
    KITH_CLIENT_BOOTSTRAP_IDLE = 0u,
    /** Bootstrap in progress: a step is awaiting a reply. */
    KITH_CLIENT_BOOTSTRAP_RUNNING = 1u,
    /** Bootstrap complete: all steps finished. */
    KITH_CLIENT_BOOTSTRAP_READY = 2u,
    /** Bootstrap failed: a step returned failure. */
    KITH_CLIENT_BOOTSTRAP_FAILED = 3u,
};

/** Alias of enum kith_client_bootstrap_state. */
typedef enum kith_client_bootstrap_state kith_client_bootstrap_state_t;

/**
 * Step enter callback. Called when the FSM enters a step. Produces the
 * outbound frame to send for this step (the engine encodes it via
 * @c kith_proto_encode and enqueues it).
 *
 * @param client          Client handle. Non-NULL.
 * @param ctx             Per-step context pointer from the step record.
 * @param out_type_id     Receives the message-type id for the outbound
 *                        frame.
 * @param out_payload     Buffer to write the payload into. Capacity is
 *                        @p payload_cap bytes.
 * @param payload_cap     Payload buffer capacity.
 * @param out_payload_len Receives the payload length. 0 is valid
 *                        (header-only frame).
 * @return                0 on success, negative kith_error on failure
 *                        (the bootstrap is marked FAILED).
 */
typedef int (*kith_client_step_enter_fn)(kith_client_t *client,
                                         void *ctx,
                                         uint16_t *out_type_id,
                                         uint8_t *out_payload,
                                         uint32_t payload_cap,
                                         uint32_t *out_payload_len);

/**
 * Step reply callback. Called when a frame matching the step's
 * @c await_type_id arrives. Decides the next step.
 *
 * @param client        Client handle. Non-NULL.
 * @param ctx           Per-step context pointer from the step record.
 * @param frame         The decoded inbound frame. @p frame->payload points
 *                      into the caller's buffer and is valid only for this
 *                      call. May be NULL if the callback does not need it.
 * @param out_next_step Receives the next step index on success:
 *                      - a value < step_count → go to that step (the
 *                        engine calls its on_enter),
 *                      - a value == step_count → bootstrap READY,
 *                      - a value > step_count → bootstrap FAILED.
 *                      A value equal to the current step index retries
 *                      the current step (on_enter is called again).
 * @return              0 on success (@p out_next_step is set), negative
 *                      kith_error on failure (bootstrap marked FAILED).
 */
typedef int (*kith_client_step_reply_fn)(kith_client_t *client,
                                         void *ctx,
                                         const kith_proto_frame_t *frame,
                                         uint32_t *out_next_step);

/**
 * A bootstrap step. The caller passes an array of these to
 * @c kith_client_configure_bootstrap. This is an exposed-layout value
 * type (like @c kith_control_route_t).
 */
struct kith_client_bootstrap_step
{
    /**
     * The frame type id this step awaits. 0 means any frame type is
     * accepted (the step matches every inbound frame).
     */
    uint16_t await_type_id;
    /** Called when the step is entered. May be NULL (no outbound frame
     *  on enter — the step waits passively). */
    kith_client_step_enter_fn on_enter;
    /** Called when a matching frame arrives. May be NULL (any matching
     *  frame auto-advances to step + 1). */
    kith_client_step_reply_fn on_reply;
    /** Borrowed per-step context pointer. The caller owns it; the engine
     *  stores the pointer. May be NULL. */
    void *ctx;
};

/** Alias of struct kith_client_bootstrap_step. */
typedef struct kith_client_bootstrap_step kith_client_bootstrap_step_t;

/**
 * Runtime frame handler callback. Called by @c kith_client_feed_frame for
 * frames that do not match the current bootstrap step and are not pong.
 * The callback may call @c kith_client_publish_event to push events
 * derived from the frame.
 *
 * @param client Client handle. Non-NULL.
 * @param ctx    Context pointer from @c kith_client_set_frame_handler.
 * @param frame  The decoded inbound frame. @p frame->payload points into
 *               the caller's buffer and is valid only for this call.
 */
typedef void (*kith_client_frame_fn)(kith_client_t *client,
                                     void *ctx,
                                     const kith_proto_frame_t *frame);

/**
 * An interactive command, submitted via
 * @c kith_client_submit_interactive. The engine encodes it as a proto
 * frame and enqueues it for sending. This is an exposed-layout value
 * type.
 */
struct kith_client_command
{
    /** Message-type id for the outbound frame. */
    uint16_t type_id;
    /** Header flag bits (@c kith_proto_flag). */
    uint8_t flags;
    /** Correlation ID written as the trailer when
     *  @c KITH_PROTO_FLAG_CORRELATION is set in @p flags. */
    uint64_t correlation_id;
    /** Command payload, or NULL when @p payload_len is 0. Borrowed for
     *  the call. */
    const void *payload;
    /** Command payload length in bytes. */
    uint32_t payload_len;
};

/** Alias of struct kith_client_command. */
typedef struct kith_client_command kith_client_command_t;

/**
 * An event record, drained via @c kith_client_drain_events. The payload
 * is inline (not a pointer) so a drained event is self-contained — no
 * lifetime contract on the queue's storage. Size-versioned: a caller
 * publishing an event sets @p size and @p abi_version at its compile
 * time, and the runtime rejects an event whose struct generation is
 * incompatible before it enters the ring. Drained events are filled by
 * the runtime with @p size and @p abi_version set to the runtime's
 * generation, so a reader can probe either field to learn the record
 * shape.
 */
struct kith_client_event
{
    /** Must be sizeof(kith_client_event_t) when publishing. */
    uint32_t size;
    /** Must be KITH_ABI_VERSION (from kith/version.h) when publishing. */
    uint32_t abi_version;
    /** Monotonic timestamp in nanoseconds (CLOCK_MONOTONIC). */
    uint64_t ts_mono_ns;
    /** The frame type id that produced this event, or a caller-defined
     *  value. */
    uint16_t type_id;
    /** Payload length in bytes. 0 is valid (no payload). */
    uint32_t payload_len;
    /** Inline payload bytes. The first @p payload_len bytes are valid;
     *  the rest are zero. */
    uint8_t payload[KITH_CLIENT_EVENT_PAYLOAD_MAX];
    /** Reserved for future additive fields. Must be zero-filled. */
    void *reserved[8];
};

/** Alias of struct kith_client_event. */
typedef struct kith_client_event kith_client_event_t;

/**
 * Creation parameters. Size-versioned: callers set @p size to
 * sizeof(kith_client_params_t) and @p abi_version to KITH_ABI_VERSION at
 * their compile time; the runtime rejects structs from an incompatible
 * generation or an undersized size. A field set to 0 selects the
 * corresponding @c kith_client_default value.
 */
struct kith_client_params
{
    /** Must be sizeof(kith_client_params_t). */
    uint32_t size;
    /** Must be KITH_ABI_VERSION (from kith/version.h). */
    uint32_t abi_version;

    /** Outbound frame queue capacity in frames. 0 selects the default. */
    uint32_t outbound_cap;
    /** Event ring capacity in event records. 0 selects the default. */
    uint32_t event_cap;

    /**
     * Message-type id for ping frames. 0 disables keepalive pings.
     * The engine encodes a ping with an 8-byte big-endian timestamp
     * payload and this type id.
     */
    uint16_t ping_type_id;
    /**
     * Message-type id for pong frames. 0 disables pong handling. Frames
     * with this type id are handled as pong (RTT extracted) and not
     * delivered to the bootstrap or frame handler.
     */
    uint16_t pong_type_id;
    /** Ping interval in milliseconds. 0 selects the default. Ignored when
     *  @p ping_type_id is 0. */
    uint32_t ping_interval_ms;

    /** Reconnect base delay in milliseconds. 0 selects the default. */
    uint32_t reconnect_base_ms;
    /** Reconnect max delay in milliseconds. 0 selects the default. */
    uint32_t reconnect_max_ms;
    /** Max reconnect attempts. 0 means unlimited. */
    uint32_t reconnect_max_attempts;

    /** Reserved for future additive fields. Must be zero-filled. */
    void *reserved[8];
};

/** Alias of struct kith_client_params. */
typedef struct kith_client_params kith_client_params_t;

/**
 * Bootstrap status snapshot. Size-versioned.
 */
struct kith_client_bootstrap_status
{
    /** Must be sizeof(kith_client_bootstrap_status_t) on input. */
    uint32_t size;
    /** Must be KITH_ABI_VERSION on input. */
    uint32_t abi_version;
    /** Current bootstrap state (@c kith_client_bootstrap_state). */
    uint32_t state;
    /** Current step index (0-based). Valid when @p state is RUNNING. */
    uint32_t current_step;
    /** Total number of configured steps. */
    uint32_t step_count;
    /** Reserved for future additive fields. Must be zero-filled. */
    void *reserved[8];
};

/** Alias of struct kith_client_bootstrap_status. */
typedef struct kith_client_bootstrap_status kith_client_bootstrap_status_t;

/**
 * Runtime status snapshot (keepalive and connection state).
 * Size-versioned.
 */
struct kith_client_runtime_status
{
    /** Must be sizeof(kith_client_runtime_status_t) on input. */
    uint32_t size;
    /** Must be KITH_ABI_VERSION on input. */
    uint32_t abi_version;
    /** 1 if the connection is up, 0 otherwise. */
    uint8_t connected;
    /** 1 if a ping was sent and no pong has been received. */
    uint8_t awaiting_pong;
    /** 1 if the reconnect timer has elapsed (the harness should
     *  reconnect). */
    uint8_t reconnect_due;
    /** Reserved for future status fields. Must be zero-filled; the
     *  runtime leaves it untouched. */
    uint8_t reserved_u8;
    /** Number of reconnect attempts since the last successful connection. */
    uint32_t reconnect_attempts;
    /** Last round-trip time in milliseconds. */
    uint32_t rtt_last_ms;
    /** Minimum RTT observed, in milliseconds. */
    uint32_t rtt_min_ms;
    /** Maximum RTT observed, in milliseconds. */
    uint32_t rtt_max_ms;
    /** Sum of all RTT samples in milliseconds. */
    uint64_t rtt_sum_ms;
    /** Number of RTT samples collected. */
    uint32_t rtt_samples;
    /** Reserved for future status fields. Must be zero-filled; the
     *  runtime leaves it untouched. */
    uint32_t reserved_u32;
    /** Monotonic timestamp of the last ping sent, in milliseconds. */
    uint64_t last_ping_sent_ms;
    /** Monotonic deadline for the next ping, in milliseconds. */
    uint64_t next_ping_due_ms;
    /** Monotonic deadline for reconnect, in milliseconds. */
    uint64_t next_reconnect_due_ms;
    /** Reserved for future additive fields. Must be zero-filled. */
    void *reserved[8];
};

/** Alias of struct kith_client_runtime_status. */
typedef struct kith_client_runtime_status kith_client_runtime_status_t;

/*---------------------------------------------------------------------------
 * lifecycle
 *-------------------------------------------------------------------------*/

/**
 * Create a client engine handle.
 *
 * Allocates the handle, outbound queue, and event ring. Does not start
 * the bootstrap — call @c kith_client_on_connected to begin. The proto
 * handle is borrowed for the engine's lifetime.
 *
 * @param params     Creation parameters. Must be non-NULL with a valid
 *                   @p size and @p abi_version. NULL selects all defaults.
 * @param proto      Borrowed proto handle. Must outlive the client handle.
 *                   Non-NULL.
 * @param logger     Borrowed logger handle. May be NULL (logging is
 *                   silently dropped).
 * @param alloc      Allocator for the new handle, its outbound queue, and
 *                   its event ring, used again when kith_client_destroy
 *                   frees them and when outbound frames and the bootstrap
 *                   step table allocate and free over the handle's
 *                   lifetime. NULL selects the default allocator; a
 *                   supplied allocator is validated (see
 *                   kith_allocator_t) and must outlive the handle.
 * @param out_client Receives the new handle on success.
 * @return           0 on success, negative kith_error on failure:
 *                   - -KITH_EINVAL if @p proto or @p out_client is NULL,
 *                     or @p params has reconnect_max_ms < reconnect_base_ms,
 *                     or @p alloc is missing an operation,
 *                   - -KITH_EABIVER if @p params or @p alloc has an
 *                     incompatible abi_version,
 *                   - -KITH_ESIZE if @p params or @p alloc has an
 *                     undersized size,
 *                   - -KITH_ENOMEM on allocation failure.
 * @thread_safety unsafe — must not race with another kith_client_create
 *                on the same @p out_client slot.
 * @ownership callee — the returned handle is freed by the caller with
 *           kith_client_destroy.
 */
[[nodiscard]] KITH_API int kith_client_create(const kith_client_params_t *params,
                                              kith_proto_t *proto,
                                              kith_logger_t *logger,
                                              const kith_allocator_t *alloc,
                                              kith_client_t **out_client);

/**
 * Release all resources held by @p client. Passing NULL is a no-op.
 *
 * Frees the outbound queue (including queued frame buffers), the event
 * ring, and the bootstrap step table. The borrowed proto and logger
 * handles are not freed.
 *
 * @param client Client handle. NULL is a no-op.
 * @thread_safety unsafe — no feed/pop/submit/drain/tick may be in flight
 *                on @p client when this is called.
 * @ownership callee — @p client is consumed and freed by the call.
 */
KITH_API void kith_client_destroy(kith_client_t *client);

/*---------------------------------------------------------------------------
 * bootstrap configuration
 *-------------------------------------------------------------------------*/

/**
 * Configure the bootstrap step table. Copies the step records (the @p ctx
 * pointers are stored by value — the caller owns the pointed-to memory).
 * May be called at most once per handle. If @p count is 0, the bootstrap
 * immediately transitions to READY on @c kith_client_on_connected.
 *
 * @param client Client handle. Must be non-NULL.
 * @param steps  Step array. Must be non-NULL when @p count > 0. Each
 *               step's @p on_enter and @p on_reply may be NULL.
 * @param count  Number of steps at @p steps.
 * @return       0 on success, negative kith_error on failure:
 *               - -KITH_EINVAL if @p client is NULL, or @p steps is NULL
 *                 when @p count > 0,
 *               - -KITH_ESTATE if already configured,
 *               - -KITH_ENOMEM on allocation failure.
 * @thread_safety unsafe — must not race with feed_frame or on_connected.
 * @ownership caller — @p steps and each step's @p ctx are borrowed for
 *           the call and copied (the @p ctx pointer value is stored, not
 *           the pointed-to memory).
 */
[[nodiscard]] KITH_API int kith_client_configure_bootstrap(
    kith_client_t *client, const kith_client_bootstrap_step_t *steps, uint32_t count);

/**
 * Set the runtime frame handler. Called by @c kith_client_feed_frame for
 * frames that do not match the current bootstrap step and are not pong.
 * The callback may call @c kith_client_publish_event. May be called at
 * any time; passing NULL clears the handler.
 *
 * @param client  Client handle. Must be non-NULL.
 * @param on_frame Frame handler callback. NULL clears the handler (non-
 *                 bootstrap, non-pong frames are silently dropped).
 * @param ctx     Context pointer passed to @p on_frame. May be NULL.
 * @thread_safety unsafe — must not race with feed_frame.
 * @ownership caller — @p ctx is borrowed for the handler's lifetime (until the
 *           handler is replaced with a NULL @p on_frame or the client is
 *           destroyed).
 */
KITH_API void
kith_client_set_frame_handler(kith_client_t *client, kith_client_frame_fn on_frame, void *ctx);

/*---------------------------------------------------------------------------
 * connection lifecycle
 *-------------------------------------------------------------------------*/

/**
 * Signal that the connection is established. Resets the bootstrap FSM
 * and starts it (calls @c on_enter for step 0, if steps are configured).
 * If no steps are configured, the bootstrap immediately transitions to
 * READY. Resets keepalive state (clears awaiting_pong, schedules the
 * first ping, clears reconnect_attempts).
 *
 * @param client Client handle. Must be non-NULL.
 * @param now_ms Current monotonic time in milliseconds. The first ping
 *               deadline is computed from it.
 * @return       0 on success, negative kith_error on failure:
 *               - -KITH_EINVAL if @p client is NULL,
 *               - -KITH_ESTATE if a step's @c on_enter fails (bootstrap
 *                 marked FAILED).
 * @thread_safety unsafe — call from the harness thread.
 * @ownership caller — @p client is borrowed for the call only.
 */
[[nodiscard]] KITH_API int kith_client_on_connected(kith_client_t *client, uint64_t now_ms);

/**
 * Signal that the connection was lost. Clears the connected flag, stops
 * the keepalive, and schedules a reconnect timer (if reconnect is
 * configured). The harness checks @c kith_client_runtime_status to learn
 * when @c reconnect_due is set, then reconnects and calls
 * @c kith_client_on_connected.
 *
 * @param client Client handle. Must be non-NULL.
 * @param now_ms Current monotonic time in milliseconds.
 * @return       0 on success, negative kith_error on failure:
 *               - -KITH_EINVAL if @p client is NULL.
 * @thread_safety unsafe — call from the harness thread.
 * @ownership caller — @p client is borrowed for the call only.
 */
[[nodiscard]] KITH_API int kith_client_on_disconnected(kith_client_t *client, uint64_t now_ms);

/**
 * Signal that a connection attempt failed (the peer refused the
 * connection or was unreachable). Advances the reconnect schedule with
 * the same law as @c kith_client_on_disconnected — the attempt count,
 * the delay curve, and the next-due deadline — without touching
 * connection state: the bootstrap, the outbound queue, and the keepalive
 * belong to a connection, and no connection ever existed. The harness
 * calls this after each refused attempt so the backoff keeps growing
 * while the peer stays down.
 *
 * @param client Client handle. Must be non-NULL.
 * @param now_ms Current monotonic time in milliseconds.
 * @return       0 on success, negative kith_error on failure:
 *               - -KITH_EINVAL if @p client is NULL.
 * @thread_safety unsafe — call from the harness thread.
 * @ownership caller — @p client is borrowed for the call only.
 */
[[nodiscard]] KITH_API int kith_client_on_connect_failed(kith_client_t *client, uint64_t now_ms);

/*---------------------------------------------------------------------------
 * inbound / outbound
 *-------------------------------------------------------------------------*/

/**
 * Feed a decoded inbound frame to the engine. The engine processes the
 * frame in this order:
 * 1. If the frame is a pong (matches @c pong_type_id) and keepalive is
 *    active, extract RTT and clear @c awaiting_pong. Done.
 * 2. If the bootstrap is RUNNING and the frame's @c type_id matches the
 *    current step's @c await_type_id (or @c await_type_id is 0), call
 *    the step's @c on_reply. Done.
 * 3. Otherwise, call the configured frame handler (if any). Done.
 *
 * @param client Client handle. Must be non-NULL.
 * @param frame  Decoded frame view. Must be non-NULL. @p frame->payload
 *               points into the caller's buffer and is valid only for
 *               this call (the engine copies what it needs).
 * @param now_ms Current monotonic time in milliseconds. Used for RTT
 *               computation when a pong arrives.
 * @return       0 on success, negative kith_error on failure:
 *               - -KITH_EINVAL if @p client or @p frame is NULL,
 *               - -KITH_ESTATE if a step's @c on_reply fails (bootstrap
 *                 marked FAILED).
 * @thread_safety unsafe — call from the harness thread.
 * @ownership caller — @p frame and @p frame->payload are borrowed for the
 *           call only.
 */
[[nodiscard]] KITH_API int
kith_client_feed_frame(kith_client_t *client, const kith_proto_frame_t *frame, uint64_t now_ms);

/**
 * Pop the next outbound frame to send. The engine copies the frame into
 * @p out_buf and frees the queue slot.
 *
 * @param client      Client handle. Must be non-NULL.
 * @param out_buf     Output buffer. Must be non-NULL.
 * @param cap         Bytes available at @p out_buf.
 * @param out_len     Receives the frame length on success.
 * @return            0 on success, negative kith_error on failure:
 *                    - -KITH_EINVAL if @p client, @p out_buf, or
 *                      @p out_len is NULL,
 *                    - -KITH_EAGAIN if the queue is empty,
 *                    - -KITH_EOVERFLOW if @p cap is smaller than the
 *                      frame.
 * @thread_safety unsafe — call from the harness thread.
 * @ownership caller — @p out_buf and @p out_len are the caller's output
 *           storage.
 */
[[nodiscard]] KITH_API int
kith_client_pop_outbound(kith_client_t *client, uint8_t *out_buf, uint32_t cap, uint32_t *out_len);

/*---------------------------------------------------------------------------
 * interactive command injection
 *-------------------------------------------------------------------------*/

/**
 * Submit an interactive command. The engine encodes the command as a
 * proto frame (via @c kith_proto_encode), enqueues it on the outbound
 * queue, and assigns a command id.
 *
 * @param client    Client handle. Must be non-NULL.
 * @param command   Command to submit. Must be non-NULL.
 * @param out_cmd_id Receives the assigned command id. May be NULL if the
 *                   caller does not need it.
 * @return          0 on success, negative kith_error on failure:
 *                  - -KITH_EINVAL if @p client or @p command is NULL,
 *                  - -KITH_EOVERFLOW if the encoded frame exceeds the
 *                    proto max payload,
 *                  - -KITH_EBUSY if the outbound queue is full.
 * @thread_safety unsafe — call from the harness thread.
 * @ownership caller — @p command and @p command->payload are borrowed for
 *           the call only.
 */
[[nodiscard]] KITH_API int kith_client_submit_interactive(kith_client_t *client,
                                                          const kith_client_command_t *command,
                                                          uint64_t *out_cmd_id);

/*---------------------------------------------------------------------------
 * event queue
 *-------------------------------------------------------------------------*/

/**
 * Publish an event to the event ring. The event (including the inline
 * payload) is copied into the ring. If the ring is full the oldest event
 * is overwritten. Safe to call from within the frame handler callback
 * (single-threaded re-entrant).
 *
 * @param client Client handle. Must be non-NULL.
 * @param event  Event to publish. Must be non-NULL with @p size set to
 *               sizeof(kith_client_event_t) and @p abi_version set to
 *               KITH_ABI_VERSION. @p event->payload_len must not exceed
 *               @c KITH_CLIENT_EVENT_PAYLOAD_MAX.
 * @return       0 on success, negative kith_error on failure:
 *               - -KITH_EINVAL if @p client or @p event is NULL,
 *               - -KITH_ESIZE if @p event->size is undersized,
 *               - -KITH_EABIVER if @p event->abi_version is incompatible,
 *               - -KITH_EOVERFLOW if @p event->payload_len exceeds
 *                 @c KITH_CLIENT_EVENT_PAYLOAD_MAX.
 * @thread_safety unsafe — call from the harness thread (re-entrant from
 *                the frame handler callback).
 * @ownership caller — @p event is borrowed for the call and copied.
 */
[[nodiscard]] KITH_API int kith_client_publish_event(kith_client_t *client,
                                                     const kith_client_event_t *event);

/**
 * Drain events from the event ring. Copies up to @p out_cap events into
 * @p out_events and removes them from the ring. Each drained event is
 * self-contained (inline payload), with @p size and @p abi_version set
 * by the runtime to its own struct generation.
 *
 * @param client    Client handle. Must be non-NULL.
 * @param out_events Output event array. Must be non-NULL.
 * @param out_cap   Capacity of @p out_events in event records.
 * @param out_count Receives the number of events drained (0 if the ring
 *                  is empty). Must be non-NULL.
 * @return          0 on success, negative kith_error on failure:
 *                  - -KITH_EINVAL if @p client, @p out_events, or
 *                    @p out_count is NULL, or @p out_cap is 0.
 * @thread_safety unsafe — call from the harness thread.
 * @ownership caller — @p out_events is the caller's output storage.
 */
[[nodiscard]] KITH_API int kith_client_drain_events(kith_client_t *client,
                                                    kith_client_event_t *out_events,
                                                    size_t out_cap,
                                                    size_t *out_count);

/*---------------------------------------------------------------------------
 * tick / keepalive
 *-------------------------------------------------------------------------*/

/**
 * Drive the keepalive and reconnect timers. Called periodically by the
 * harness (e.g. from a reactor timer or a poll loop). When connected and
 * the ping interval has elapsed, enqueues a ping frame. When disconnected
 * and the reconnect timer has elapsed, sets @c reconnect_due.
 *
 * @param client Client handle. Must be non-NULL.
 * @param now_ms Current monotonic time in milliseconds.
 * @return       0 on success, negative kith_error on failure:
 *               - -KITH_EINVAL if @p client is NULL,
 *               - -KITH_ESTATE if the ping frame could not be encoded or
 *                 enqueued (an encode rejection, a full outbound queue, or
 *                 an allocation failure).
 * @thread_safety unsafe — call from the harness thread.
 * @ownership caller — @p client is borrowed for the call only.
 */
[[nodiscard]] KITH_API int kith_client_tick(kith_client_t *client, uint64_t now_ms);

/*---------------------------------------------------------------------------
 * status
 *-------------------------------------------------------------------------*/

/**
 * Query the bootstrap FSM status.
 *
 * @param client     Client handle. Must be non-NULL.
 * @param out_status Receives the status. Must be non-NULL with @p size
 *                   and @p abi_version set by the caller.
 * @return           0 on success, negative kith_error on failure:
 *                   - -KITH_EINVAL if @p client or @p out_status is NULL,
 *                   - -KITH_EABIVER if @p abi_version is incompatible,
 *                   - -KITH_ESIZE if @p size is undersized.
 * @thread_safety unsafe — call from the harness thread.
 * @ownership caller — @p out_status is the caller's output storage.
 */
[[nodiscard]] KITH_API int kith_client_bootstrap_status(kith_client_t *client,
                                                        kith_client_bootstrap_status_t *out_status);

/**
 * Query the runtime (keepalive and connection) status.
 *
 * @param client     Client handle. Must be non-NULL.
 * @param out_status Receives the status. Must be non-NULL with @p size
 *                   and @p abi_version set by the caller.
 * @return           0 on success, negative kith_error on failure:
 *                   - -KITH_EINVAL if @p client or @p out_status is NULL,
 *                   - -KITH_EABIVER if @p abi_version is incompatible,
 *                   - -KITH_ESIZE if @p size is undersized.
 * @thread_safety unsafe — call from the harness thread.
 * @ownership caller — @p out_status is the caller's output storage.
 */
[[nodiscard]] KITH_API int kith_client_runtime_status(kith_client_t *client,
                                                      kith_client_runtime_status_t *out_status);

/** @} */

#endif /* KITH_CLIENT_CLIENT_H */
