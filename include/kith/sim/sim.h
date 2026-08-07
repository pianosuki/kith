#ifndef KITH_SIM_SIM_H
#define KITH_SIM_SIM_H

#include <stddef.h>
#include <stdint.h>

#include "kith/api.h"
#include "kith/types.h"
#include "kith/version.h"

/**
 * Pluggable simulation models, actor authority, and per-cell artifacts.
 *
 * A sim handle owns a model registry (name to vtable map) and an artifact
 * store (per-cell immutable actor-state snapshots). The composition root
 * creates the handle, registers model implementations by name, instantiates
 * models, and steps them each tick. After stepping, the caller publishes
 * updated actor positions as cell artifacts. The fabric module borrows the
 * sim handle to query artifacts for subscription-indexed fanout.
 *
 * @section sim_determinism Deterministic physics
 *
 * Actor positions and velocities are stored as Q16.16 fixed-point values
 * (@c int64_t with 16 fractional bits). Integer arithmetic is bit-identical
 * across architectures, unlike floating-point which varies by platform and
 * compiler flags. The @c kith_sim_config_t fields accept integer speeds and
 * a float threshold; the model implementation converts these to fixed-point
 * at init. A record of applied inputs and a replay tool (@c tools/replay.py)
 * verify that a model produces identical output across runs.
 *
 * @section sim_cells Cell artifacts
 *
 * Artifacts are keyed by a 7-tuple: (zone, cell_x, cell_y, cell_z, lod,
 * authority_epoch, publish_seq). The cell coordinates are 3D-native (2D
 * models set cell_z = 0). The lod (level of detail) is a granularity tier.
 * The authority_epoch guards against stale messages after an authority
 * transition. The publish_seq is monotonic per cell; a newer artifact
 * supersedes an older one at the same key prefix.
 *
 * The artifact store uses a sharded hash (16 compartments keyed by zone) with
 * a dense actor array and O(1) hash lookup per shard. One mutex guards every
 * shard, so publish and snapshot calls are safe from any thread. The shard
 * index remains as a compartment hint.
 *
 * @section sim_models Model vtable
 *
 * A model implementation fills a @c kith_sim_model_vtable_t and registers it
 * by name. The vtable carries an @c abi_version field so a model compiled
 * against v1 loads under a v1.x runtime. The three operations are @c step
 * (advance all actors by dt_ms), @c apply_input (apply one input to one
 * actor), and @c load_behavior (load a model-specific behavior grid from a
 * file path). The built-in @c tile2d and @c free2d models are registered
 * automatically at handle creation.
 *
 * The handle is owned by the composition root and passed by pointer; there
 * is no global accessor. All state lives on the handle.
 */

/**
 * @defgroup kith_sim Simulation
 * @{
 */

/**
 * Format invariants. The underlying type is fixed so a constant stored in an
 * ABI surface stays a fixed width.
 */
enum kith_sim_format : unsigned int
{
    /** Number of artifact-store shards (compartment count). */
    KITH_SIM_ARTIFACT_SHARDS = 16u,
    /** Q16.16 fractional-bit count (positions carry 16 fractional bits). */
    KITH_SIM_FIX_SHIFT = 16u,
};

/**
 * Default configuration values. A @c kith_sim_params_t field set to 0 selects
 * the corresponding default at create time. The underlying type is fixed.
 */
enum kith_sim_default : unsigned int
{
    /** Default simulation tick rate in Hz (params.tick_hz = 0 selects this). */
    KITH_SIM_DEFAULT_TICK_HZ = 20u,
    /** Default artifact-store hash bucket count per shard. */
    KITH_SIM_DEFAULT_BUCKET_COUNT = 4096u,
};

/**
 * Opaque simulation handle.
 *
 * @ownership callee — created by kith_sim_create, destroyed by
 *           kith_sim_destroy. Owns the model registry and the artifact store.
 */
typedef struct kith_sim kith_sim_t;

/**
 * Opaque model instance handle.
 *
 * @ownership callee — created by kith_sim_create_model, destroyed by
 *           kith_sim_model_destroy. Owns model-specific state (behavior grid,
 *           internal buffers). The handle is independent of the sim handle
 *           after creation; stepping does not require the sim handle.
 */
typedef struct kith_sim_model kith_sim_model_t;

/**
 * Model creation parameters. Size-versioned: callers set @p size to
 * sizeof(kith_sim_config_t) and @p abi_version to KITH_ABI_VERSION at their
 * compile time; the runtime rejects structs from an incompatible generation
 * or an undersized size. The fields are model-specific physics constants in
 * the model's own coordinate units; the model implementation converts them to
 * fixed-point at init. Future additive fields occupy the reserved slots.
 * Borrowed for the init call only: implementations convert the fields and
 * retain no reference to the struct.
 */
struct kith_sim_config
{
    /** Must be sizeof(kith_sim_config_t). */
    uint32_t size;
    /** Must be KITH_ABI_VERSION (from kith/version.h). */
    uint32_t abi_version;

    /** Base movement speed in model coordinate units per tick. */
    uint32_t base_speed;
    /** Run movement speed (when the run flag is set in an input). */
    uint32_t run_speed;
    /** Acceleration rate in model units per tick per tick. */
    uint32_t accel;
    /** Deceleration rate in model units per tick per tick. */
    uint32_t decel;
    /**
     * Minimum movement threshold. Below this, the model does not produce a
     * publishable position change. The model converts this to fixed-point at
     * init.
     */
    float move_eps;
    /** Collision circle radius in model coordinate units. */
    uint32_t collision_radius;

    /** Reserved for future additive fields. Must be zero-filled. */
    void *reserved[8];
};

/** Alias of struct kith_sim_config. */
typedef struct kith_sim_config kith_sim_config_t;

/**
 * Sim handle creation parameters. Size-versioned.
 */
struct kith_sim_params
{
    /** Must be sizeof(kith_sim_params_t). */
    uint32_t size;
    /** Must be KITH_ABI_VERSION (from kith/version.h). */
    uint32_t abi_version;

    /** Simulation tick rate in Hz; 0 selects KITH_SIM_DEFAULT_TICK_HZ. */
    uint32_t tick_hz;
    /** Artifact-store hash bucket count per shard; 0 selects the default. */
    uint32_t artifact_bucket_count;

    /** Reserved for future additive fields. Must be zero-filled. */
    void *reserved[8];
};

/** Alias of struct kith_sim_params. */
typedef struct kith_sim_params kith_sim_params_t;

/**
 * One actor's simulation state. This is an exposed-layout value type (like
 * @c kith_proto_frame_t and @c struct iovec): the field set is part of the
 * public contract and stays stable. Positions and velocities are Q16.16
 * fixed-point (@c int64_t with @c KITH_SIM_FIX_SHIFT fractional bits). The
 * caller passes an array of these to @c kith_sim_model_step, which modifies
 * them in place.
 */
struct kith_sim_actor
{
    /** Opaque actor identifier (caller-supplied, treated as a key). */
    uint64_t id;
    /** Position X in Q16.16 fixed-point. */
    int64_t pos_x;
    /** Position Y in Q16.16 fixed-point. */
    int64_t pos_y;
    /** Position Z in Q16.16 fixed-point (0 for 2D models). */
    int64_t pos_z;
    /** Velocity X in Q16.16 fixed-point. */
    int64_t vel_x;
    /** Velocity Y in Q16.16 fixed-point. */
    int64_t vel_y;
    /** Velocity Z in Q16.16 fixed-point (0 for 2D models). */
    int64_t vel_z;
    /** Last input tick applied to this actor. */
    uint32_t input_tick;
    /** Movement flags (bit 0 = run). Model-specific bits start at bit 8. */
    uint32_t flags;
    /** Publisher-minted monotone movement-application counter for this
     *  actor. Models increment it once per applied movement input; 0 means
     *  the publisher never minted it. Copied verbatim into published
     *  artifacts, where the gateway's self-echo coverage certification
     *  consumes it. */
    uint32_t update_seq;
};

/** Alias of struct kith_sim_actor. */
typedef struct kith_sim_actor kith_sim_actor_t;

/**
 * One movement input. Exposed-layout value type. The move vector is
 * normalized to the range [-32767, 32767] per axis; the model scales it by
 * the configured speed at @c apply_input time.
 */
struct kith_sim_input
{
    /** Input tick number (monotonic per actor). */
    uint32_t input_tick;
    /** Normalized X move component [-32767, 32767]. */
    int16_t move_x;
    /** Normalized Y move component [-32767, 32767]. */
    int16_t move_y;
    /** Normalized Z move component [-32767, 32767] (0 for 2D models). */
    int16_t move_z;
    /** Input flags (bit 0 = run). */
    uint8_t flags;
};

/** Alias of struct kith_sim_input. */
typedef struct kith_sim_input kith_sim_input_t;

/**
 * Artifact key. Identifies one cell at one authority epoch and publish
 * sequence. Exposed-layout value type. The 7-tuple (zone, cell_x, cell_y,
 * cell_z, lod, authority_epoch, publish_seq) uniquely identifies an artifact
 * in the store.
 */
struct kith_sim_artifact_key
{
    /** Zone identifier (caller-supplied namespace). */
    uint32_t zone;
    /** Cell grid X coordinate. */
    int32_t cell_x;
    /** Cell grid Y coordinate. */
    int32_t cell_y;
    /** Cell grid Z coordinate (0 for 2D models). */
    int32_t cell_z;
    /** Level of detail / granularity tier. */
    uint8_t lod;
    /** Padding for alignment. */
    uint8_t pad[3];
    /** Authority epoch (stale-message guard; incremented on authority change). */
    uint32_t authority_epoch;
    /** Monotonic publish sequence (newer supersedes older at the same prefix). */
    uint64_t publish_seq;
};

/** Alias of struct kith_sim_artifact_key. */
typedef struct kith_sim_artifact_key kith_sim_artifact_key_t;

/**
 * One actor's published state in a cell. Exposed-layout value type. Returned
 * by @c kith_sim_snapshot_cell as a copy (no shared ownership).
 */
struct kith_sim_artifact
{
    /** The key identifying this artifact's cell, epoch, and sequence. */
    kith_sim_artifact_key_t key;
    /** The actor this artifact describes. */
    uint64_t actor_id;
    /** Position X in Q16.16 fixed-point. */
    int64_t pos_x;
    /** Position Y in Q16.16 fixed-point. */
    int64_t pos_y;
    /** Position Z in Q16.16 fixed-point. */
    int64_t pos_z;
    /** Velocity X in Q16.16 fixed-point. */
    int64_t vel_x;
    /** Velocity Y in Q16.16 fixed-point. */
    int64_t vel_y;
    /** Velocity Z in Q16.16 fixed-point. */
    int64_t vel_z;
    /** Last input tick applied when this artifact was published. */
    uint32_t input_tick;
    /** The actor's update_seq at publish time (0 when never minted). */
    uint32_t update_seq;
};

/** Alias of struct kith_sim_artifact. */
typedef struct kith_sim_artifact kith_sim_artifact_t;

/**
 * Per-cell product metadata. Exposed-layout value type. Describes the state
 * of one cell in the artifact store without copying the individual artifacts.
 */
struct kith_sim_cell_product
{
    /** Zone identifier. */
    uint32_t zone;
    /** Cell grid X coordinate. */
    int32_t cell_x;
    /** Cell grid Y coordinate. */
    int32_t cell_y;
    /** Cell grid Z coordinate. */
    int32_t cell_z;
    /** Level of detail / granularity tier. */
    uint8_t lod;
    /** Padding for alignment. */
    uint8_t pad[3];
    /** Number of actors currently published in this cell. */
    uint32_t actor_count;
    /** Latest publish sequence for this cell. */
    uint64_t latest_publish_seq;
    /** Authority epoch for this cell. */
    uint32_t authority_epoch;
};

/** Alias of struct kith_sim_cell_product. */
typedef struct kith_sim_cell_product kith_sim_cell_product_t;

/**
 * Model implementation vtable. A model implementation fills this struct and
 * registers it via @c kith_sim_register_model. The @c size and @c abi_version
 * fields allow the runtime to reject a model compiled against an incompatible
 * generation. The reserved slots accommodate future vtable extensions without
 * breaking the struct layout.
 */
struct kith_sim_model_vtable
{
    /**
     * Size of the caller's struct in bytes. Must be at least
     * @c sizeof(kith_sim_model_vtable_t) and is set by the model implementer
     * via @c sizeof(kith_sim_model_vtable_t) at compile time.
     */
    uint32_t size;
    /** Must be set to KITH_ABI_VERSION by the model implementer. */
    uint32_t abi_version;

    /**
     * Create a model instance from @p cfg. The implementation allocates and
     * initializes its internal state and the opaque handle itself through
     * @p alloc, and returns the handle. The runtime fills the handle's
     * @c vtable and @c allocator fields after @c init returns; the
     * implementation sets neither.
     *
     * @return 0 on success, negative kith_error on failure.
     */
    int (*init)(const kith_sim_config_t *cfg,
                const kith_allocator_t *alloc,
                kith_sim_model_t **out);

    /** Destroy a model instance and release all model-owned resources. */
    void (*destroy)(kith_sim_model_t *model);

    /**
     * Advance @p count actors by @p dt_ms milliseconds. The model modifies
     * the @p actors array in place (updates positions and velocities).
     *
     * @return 0 on success, negative kith_error on failure.
     */
    int (*step)(kith_sim_model_t *model, kith_sim_actor_t *actors, size_t count, uint32_t dt_ms);

    /**
     * Apply one input to one actor. The model updates the actor's pending
     * move vector, flags, and input tick.
     *
     * @return 0 on success, negative kith_error on failure.
     */
    int (*apply_input)(kith_sim_model_t *model,
                       kith_sim_actor_t *actor,
                       const kith_sim_input_t *input);

    /**
     * Load a behavior grid from @p path. The file format is model-specific
     * (the tile2d model accepts a text grid; the free2d model ignores the
     * call). Passing NULL clears any loaded behavior.
     *
     * @return 0 on success, negative kith_error on failure.
     */
    int (*load_behavior)(kith_sim_model_t *model, const char *path);

    /** Reserved for future vtable extensions. Set to NULL. */
    void *reserved[8];
};

/** Alias of struct kith_sim_model_vtable. */
typedef struct kith_sim_model_vtable kith_sim_model_vtable_t;

/**
 * Build a sim handle from @p params.
 *
 * @param params  Creation parameters; @c size and @c abi_version must match
 *                the runtime generation. NULL selects all defaults.
 * @param alloc   Allocator for the new handle, its model registry, and its
 *                artifact store (shard tables and every cell member array
 *                the store grows included), used again when kith_sim_destroy
 *                frees them. NULL selects the default allocator; a supplied
 *                allocator is validated (see kith_allocator_t) and must
 *                outlive the handle.
 * @param out_sim Receives the new handle on success.
 * @return        0 on success, negative kith_error on failure:
 *                - -KITH_EINVAL if @p out_sim is NULL, @p params has an
 *                  incompatible abi_version or an undersized size, or
 *                  @p alloc is missing an operation,
 *                - -KITH_EABIVER if @p params or @p alloc has an
 *                  incompatible abi_version,
 *                - -KITH_ESIZE if @p params or @p alloc has an undersized
 *                  size,
 *                - -KITH_ENOMEM on allocation failure.
 * @thread_safety unsafe — must not race with another kith_sim_create on the
 *                same @p out_sim slot.
 * @ownership callee — the returned handle is freed by the caller with
 *           kith_sim_destroy.
 */
[[nodiscard]] KITH_API int kith_sim_create(const kith_sim_params_t *params,
                                           const kith_allocator_t *alloc,
                                           kith_sim_t **out_sim);

/**
 * Release a sim handle, its model registry, and its artifact store. Passing
 * NULL is a no-op. Models created via @c kith_sim_create_model are not freed
 * here (the caller owns them and must call @c kith_sim_model_destroy).
 *
 * @param sim Sim handle. NULL is a no-op.
 * @thread_safety unsafe — no register/create/publish/snapshot may be in
 *                flight on @p sim when this is called.
 * @ownership callee — @p sim is consumed and freed by the call.
 */
KITH_API void kith_sim_destroy(kith_sim_t *sim);

/**
 * Register a model implementation by name.
 *
 * @param sim     Sim handle. NULL is an error.
 * @param name    NUL-terminated model name, non-NULL, non-empty. Copied at
 *                registration; the caller may free @p name after the call.
 * @param vtable  Model vtable. The @c size field must be at least
 *                @c sizeof(kith_sim_model_vtable_t) and the @c abi_version
 *                field must match KITH_ABI_VERSION. Borrowed for the call
 *                only and copied on success.
 * @return        0 on success, negative kith_error on failure:
 *                - -KITH_EINVAL if @p sim, @p name, or @p vtable is NULL,
 *                  @p name is empty, or @p vtable->abi_version is
 *                  incompatible,
 *                - -KITH_ESIZE if @p vtable->size is undersized,
 *                - -KITH_EABIVER if @p vtable->abi_version is incompatible
 *                  or a required callback is missing,
 *                - -KITH_EEXIST if @p name is already registered,
 *                - -KITH_ENOMEM on allocation failure.
 * @thread_safety unsafe — the model registry is not synchronized.
 * @ownership caller — @p name and @p vtable are borrowed for the call only.
 */
[[nodiscard]] KITH_API int
kith_sim_register_model(kith_sim_t *sim, const char *name, const kith_sim_model_vtable_t *vtable);

/**
 * Instantiate a model by name.
 *
 * Looks up the vtable registered as @p name and calls its @c init function
 * with @p cfg and the resolved allocator. The returned model is independent
 * of the sim handle after creation.
 *
 * @param sim       Sim handle. NULL is an error.
 * @param name      Registered model name.
 * @param cfg       Model creation parameters; @c size and @c abi_version must
 *                  match. NULL selects model defaults.
 * @param alloc     Allocator for the new model handle and the model's own
 *                  state, used again when the model's destroy frees them.
 *                  NULL selects the default allocator — not the sim handle's
 *                  allocator, since the model is independent of the sim
 *                  after creation. A supplied allocator is validated (see
 *                  kith_allocator_t) and must outlive the model.
 * @param out_model Receives the new model handle on success.
 * @return          0 on success, negative kith_error on failure:
 *                  - -KITH_EINVAL if @p sim, @p name, or @p out_model is
 *                    NULL, or @p alloc is missing an operation,
 *                  - -KITH_ENOENT if @p name is not registered,
 *                  - -KITH_EABIVER if @p cfg or @p alloc has an incompatible
 *                    abi_version, or -KITH_ESIZE if either has an undersized
 *                    size,
 *                  - -KITH_ENOMEM on allocation failure.
 * @thread_safety unsafe.
 * @ownership callee — the caller destroys the model with
 *           kith_sim_model_destroy.
 */
[[nodiscard]] KITH_API int kith_sim_create_model(kith_sim_t *sim,
                                                 const char *name,
                                                 const kith_sim_config_t *cfg,
                                                 const kith_allocator_t *alloc,
                                                 kith_sim_model_t **out_model);

/**
 * Destroy a model instance. Passing NULL is a no-op.
 *
 * @param model Model handle. NULL is a no-op.
 * @thread_safety unsafe — no step/apply_input/load_behavior may be in flight.
 * @ownership callee — @p model is consumed and freed by the call.
 */
KITH_API void kith_sim_model_destroy(kith_sim_model_t *model);

/**
 * Advance @p count actors by @p dt_ms. The model modifies @p actors in place.
 *
 * @param model  Model handle. NULL is an error.
 * @param actors Actor array. Modified in place.
 * @param count  Number of actors in @p actors.
 * @param dt_ms  Tick duration in milliseconds.
 * @return       0 on success, negative kith_error on failure:
 *               - -KITH_EINVAL if @p model is NULL, or @p actors is NULL when
 *                 @p count > 0.
 * @thread_safety unsafe — the model is not synchronized.
 * @ownership caller — @p actors is borrowed for the call and modified in place.
 */
[[nodiscard]] KITH_API int kith_sim_model_step(kith_sim_model_t *model,
                                               kith_sim_actor_t *actors,
                                               size_t count,
                                               uint32_t dt_ms);

/**
 * Apply one input to one actor.
 *
 * The built-in models coalesce the input into a fixed-size per-model
 * pending map (KITH_SIM_DEFAULT_BUCKET_COUNT nodes, one pending input per
 * actor). A repeat input for an indexed actor overwrites its entry; an
 * input for an actor with no free node is dropped silently — the map's
 * node count is a sizing bound on the distinct-actor population a model
 * absorbs, not a queue, and the drop raises no error.
 *
 * @param model  Model handle. NULL is an error.
 * @param actor  Actor to update. Modified in place.
 * @param input  Input to apply.
 * @return       0 on success, negative kith_error on failure:
 *               - -KITH_EINVAL if @p model, @p actor, or @p input is NULL.
 * @thread_safety unsafe.
 * @ownership caller — @p actor and @p input are borrowed for the call; @p actor
 *           is modified in place.
 */
[[nodiscard]] KITH_API int kith_sim_model_apply_input(kith_sim_model_t *model,
                                                      kith_sim_actor_t *actor,
                                                      const kith_sim_input_t *input);

/**
 * Load a behavior grid from @p path. The file format is model-specific.
 * Passing NULL for @p path clears any loaded behavior.
 *
 * @param model Model handle. NULL is an error.
 * @param path  NUL-terminated file path, or NULL to clear.
 * @return      0 on success, negative kith_error on failure:
 *              - -KITH_EINVAL if @p model is NULL,
 *              - -KITH_EIO on file read failure,
 *              - -KITH_EINVAL on parse failure.
 * @thread_safety unsafe.
 * @ownership caller — @p path is borrowed for the call only.
 */
[[nodiscard]] KITH_API int kith_sim_model_load_behavior(kith_sim_model_t *model, const char *path);

/**
 * Publish or update an actor's state as a cell artifact. The cell, epoch, and
 * lod are taken from @p key (the @c publish_seq field is ignored on input and
 * assigned by the store). If an artifact for @p actor_id already exists in
 * the same cell (zone, cell_x, cell_y, cell_z, lod), it is superseded: the
 * old artifact is removed and the new one is inserted with an incremented
 * publish_seq.
 *
 * @param sim    Sim handle. NULL is an error.
 * @param key    Cell locator (zone, cell_x, cell_y, cell_z, lod,
 *               authority_epoch). NULL is an error. @c publish_seq is
 *               assigned by the store.
 * @param actor  Actor state to publish. The position, velocity, input_tick,
 *               and update_seq are copied into the artifact.
 * @param out_seq Receives the assigned publish_seq. May be NULL.
 * @return        0 on success, negative kith_error on failure:
 *                - -KITH_EINVAL if @p sim, @p key, or @p actor is NULL,
 *                - -KITH_ENOMEM on allocation failure.
 * @thread_safety safe — the artifact store is internally synchronized.
 * @ownership caller — @p key and @p actor are borrowed for the call only.
 */
[[nodiscard]] KITH_API int kith_sim_publish_artifact(kith_sim_t *sim,
                                                     const kith_sim_artifact_key_t *key,
                                                     const kith_sim_actor_t *actor,
                                                     uint64_t *out_seq);

/**
 * Remove all artifacts for one actor across all cells.
 *
 * @param sim      Sim handle. NULL is an error.
 * @param actor_id Actor whose artifacts to remove.
 * @return         0 on success (even if no artifacts existed),
 *                 - -KITH_EINVAL if @p sim is NULL.
 * @thread_safety safe — the artifact store is internally synchronized.
 * @ownership caller — @p sim is borrowed for the call only.
 */
[[nodiscard]] KITH_API int kith_sim_remove_artifact(kith_sim_t *sim, uint64_t actor_id);

/**
 * Remove all artifacts for one zone.
 *
 * @param sim  Sim handle. NULL is an error.
 * @param zone Zone whose artifacts to remove.
 * @return     0 on success (even if no artifacts existed),
 *             - -KITH_EINVAL if @p sim is NULL.
 * @thread_safety safe — the artifact store is internally synchronized.
 * @ownership caller — @p sim is borrowed for the call only.
 */
[[nodiscard]] KITH_API int kith_sim_remove_zone(kith_sim_t *sim, uint32_t zone);

/**
 * Snapshot all artifacts in one cell. The cell is located by @p key (zone,
 * cell_x, cell_y, cell_z, lod); the @c authority_epoch and @c publish_seq
 * fields of @p key are ignored. The artifacts are copied into @p out; the
 * caller owns the copies. Artifact order is unspecified but deterministic
 * for a given sequence of store mutations; callers that require a specific
 * order sort their copy.
 *
 * @param sim       Sim handle. NULL is an error.
 * @param key       Cell locator. NULL is an error.
 * @param out       Output buffer. May be NULL when @p max is 0 (only
 *                  counts).
 * @param max       Maximum number of artifacts to copy.
 * @param out_count Receives the cell's artifact count at snapshot time: the
 *                  number of valid entries in @p out on success, or the
 *                  required buffer size when truncated. May be NULL.
 * @return          0 on success, negative kith_error on failure:
 *                  - -KITH_EINVAL if @p sim or @p key is NULL,
 *                  - -KITH_ERANGE if the cell holds more than @p max
 *                    artifacts: the first @p max entries of @p out receive
 *                    valid artifacts and @p out_count receives the required
 *                    count; retry with a buffer of at least @p out_count
 *                    entries.
 * @thread_safety safe — the artifact store is internally synchronized.
 * @ownership caller — @p out is the caller's output storage.
 */
[[nodiscard]] KITH_API int kith_sim_snapshot_cell(kith_sim_t *sim,
                                                  const kith_sim_artifact_key_t *key,
                                                  kith_sim_artifact_t *out,
                                                  size_t max,
                                                  size_t *out_count);

/**
 * Snapshot cell product headers for all cells in one zone at one lod. The
 * headers are copied into @p out; the caller owns the copies.
 *
 * @param sim       Sim handle. NULL is an error.
 * @param zone      Zone identifier.
 * @param lod       Level of detail tier.
 * @param out       Output buffer. May be NULL when @p max is 0 (only counts).
 * @param max       Maximum number of headers to copy.
 * @param out_count Receives the number of headers copied into @p out. May be
 *                  NULL.
 * @return          0 on success, negative kith_error on failure:
 *                  - -KITH_EINVAL if @p sim is NULL.
 * @thread_safety safe — the artifact store is internally synchronized.
 * @ownership caller — @p out is the caller's output storage.
 */
[[nodiscard]] KITH_API int kith_sim_snapshot_zone_cells(kith_sim_t *sim,
                                                        uint32_t zone,
                                                        uint8_t lod,
                                                        kith_sim_cell_product_t *out,
                                                        size_t max,
                                                        size_t *out_count);

/**
 * Snapshot one cell product header. The cell is located by @p key (zone,
 * cell_x, cell_y, cell_z, lod); the @c authority_epoch and @c publish_seq
 * fields of @p key are ignored.
 *
 * @param sim          Sim handle. NULL is an error.
 * @param key          Cell locator. NULL is an error.
 * @param out_product  Receives the product header on success.
 * @return             0 on success, negative kith_error on failure:
 *                     - -KITH_EINVAL if @p sim or @p key or @p out_product is
 *                       NULL,
 *                     - -KITH_ENOENT if the cell has no artifacts.
 * @thread_safety safe — the artifact store is internally synchronized.
 * @ownership caller — @p key is borrowed for the call only; @p out_product is
 *           the caller's output storage.
 */
[[nodiscard]] KITH_API int kith_sim_cell_product(kith_sim_t *sim,
                                                 const kith_sim_artifact_key_t *key,
                                                 kith_sim_cell_product_t *out_product);

/**
 * Return the total number of artifacts currently retained in the store.
 *
 * @param sim Sim handle.
 * @return    The number of artifacts currently retained.
 * @thread_safety safe — the artifact store is internally synchronized.
 * @ownership caller — @p sim is borrowed for the call only.
 */
KITH_API uint64_t kith_sim_artifact_count(const kith_sim_t *sim);

/** @} */

#endif /* KITH_SIM_SIM_H */
