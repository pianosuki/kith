/* Implementation of the spatial game library's native movement-apply core
 * (examples/spatial/native_core.h). Lock classes, mirroring the Python
 * handler set's discipline (control -> stripe -> bookkeeping, no inverse
 * order): a control lock guards table membership, 64 cache-line-aligned
 * stripe locks serialize each actor's apply sequence, a bookkeeping lock
 * guards the cross-actor dirty-cell set and authority epochs, and a record
 * lock guards the replay FIFO. The flush runs on the single per-tick
 * callback and never holds the bookkeeping lock across a fabric publish. */

#include "spatial/native_core.h"

#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>

#include <pthread.h>

#include "kith/types.h"
#include "kith/version.h"

/*---------------------------------------------------------------------------
 * constants
 *-------------------------------------------------------------------------*/

// Stripe count for the per-actor apply sequences; the same id-modulo
// topology the Python handler set uses.
#define SPATIAL_STRIPE_COUNT 64u

// Replay record ring capacity. At the gate point's demand (~8.4k inputs/s,
// 20 Hz flush) a full tick drains ~420 records; the ring holds over 150
// ticks of full-rate input without a drain, so the overflow counter only
// moves when the flush stalls far beyond any operating point.
#define SPATIAL_RECORD_CAP 65536u

// Cell-hash capacity floor; both hash tables size to the next power of two
// at or above four times the actor capacity.
#define SPATIAL_HASH_MIN_SLOTS 1024u

// Simulation step length of the wire path (a 20 Hz tick), matching the
// Python movement handler's dt.
#define SPATIAL_APPLY_DT_MS 50u

// Subscription-window radius bound. The game ships radius 1 (27 cells); the
// crossing diff builds two stack neighborhoods, so the core rejects a
// larger radius at create instead of growing unbounded stack frames.
#define SPATIAL_MAX_CELL_RADIUS 2u

// Length of the movement payload the wire handler decodes: 8-byte actor id,
// 4-byte input tick, three 2-byte move components, 1-byte flags.
#define SPATIAL_INPUT_WIRE_LEN 19u

/*---------------------------------------------------------------------------
 * small types
 *-------------------------------------------------------------------------*/

/** A cell locator in the game's floor-division geometry (lod always 0). */
struct spatial_cell
{
    int32_t x;
    int32_t y;
    int32_t z;
};

/** One dirty-set slot: the cell plus its generation stamp (0 = empty). */
struct spatial_dirty_slot
{
    struct spatial_cell cell;
    uint32_t stamp;
};

/** One authority-epoch table entry (epoch 0 = empty). */
struct spatial_epoch_slot
{
    struct spatial_cell cell;
    uint32_t epoch;
};

struct spatial_stripe
{
    alignas(64) pthread_mutex_t mutex;
};

struct spatial_native
{
    /* Creation parameters (const after create). */
    uint32_t zone_id;
    uint32_t cell_radius;
    uint64_t cell_size_q16;
    uint32_t max_actors;
    kith_sim_t *sim;
    kith_sim_model_t *model;
    kith_fabric_t *fabric;

    /* Actor table: slots indexed by actor_id - 1; an all-zero slot is empty
     * (ids are non-zero by construction). The cell map is parallel to the
     * table and is mutated only under the actor's stripe. */
    kith_sim_actor_t *actors;
    struct spatial_cell *cells;

    struct spatial_stripe stripes[SPATIAL_STRIPE_COUNT];

    /* Cross-actor bookkeeping: the per-tick dirty-cell set and the per-cell
     * authority epochs. Held only for brief map operations; the fabric
     * publish runs outside it. */
    pthread_mutex_t book_lock;
    struct spatial_dirty_slot *dirty_slots;
    uint32_t dirty_slot_count;
    uint32_t dirty_generation;
    struct spatial_epoch_slot *epoch_slots;
    uint32_t epoch_slot_count;

    /* Low-rate lifecycle state: occupancy transitions and the quiesced
     * snapshot. */
    pthread_mutex_t control_lock;

    /* Replay record FIFO: producers append under the record lock after the
     * stripe releases; the per-tick flush drains to a snapshot of the tail.
     * Fixed ring, so an append onto a full ring drops and counts rather
     * than blocking a pool worker. */
    pthread_mutex_t record_lock;
    struct spatial_move_record *records;
    uint64_t record_head; // next slot to drain
    uint64_t record_tail; // next slot to fill
    _Atomic uint64_t record_overflow_dropped;
    _Atomic uint64_t dirty_overflow_dropped;

    _Atomic uint64_t gate_drops;
};

/*---------------------------------------------------------------------------
 * helpers
 *-------------------------------------------------------------------------*/

static uint64_t spatial_mix64(uint64_t v)
{
    v += 0x9e3779b97f4a7c15ull;
    v = (v ^ (v >> 30u)) * 0xbf58476d1ce4e5b9ull;
    v = (v ^ (v >> 27u)) * 0x94d049bb133111ebu;
    return v ^ (v >> 31u);
}

static uint64_t spatial_cell_hash(const spatial_native_t *n, const struct spatial_cell *cell)
{
    uint64_t h = (uint64_t)n->zone_id;
    h = h * 0x100000001b3ull ^ (uint64_t)(uint32_t)cell->x;
    h = (h << 21u) - h + (uint64_t)(uint32_t)cell->y;
    h = (h << 21u) - h + (uint64_t)(uint32_t)cell->z;
    return spatial_mix64(h);
}

static bool spatial_cell_equal(const struct spatial_cell *a, const struct spatial_cell *b)
{
    return a->x == b->x && a->y == b->y && a->z == b->z;
}

// Floor division matching the game's Python geometry (pos // span per axis),
// which rounds toward negative infinity; C's / truncates toward zero, so a
// negative remainder adjusts the quotient down one.
static int64_t spatial_floor_div(int64_t v, uint64_t size)
{
    int64_t q = v / (int64_t)size;
    if ((v % (int64_t)size) != 0 && v < 0)
    {
        q -= 1;
    }
    return q;
}

static struct spatial_cell spatial_cell_of(const spatial_native_t *n, const kith_sim_actor_t *actor)
{
    const uint64_t size = n->cell_size_q16;
    struct spatial_cell cell = {
        .x = (int32_t)spatial_floor_div(actor->pos_x, size),
        .y = (int32_t)spatial_floor_div(actor->pos_y, size),
        .z = (int32_t)spatial_floor_div(actor->pos_z, size),
    };
    return cell;
}

static kith_fabric_cell_key_t spatial_cell_key(const spatial_native_t *n,
                                               const struct spatial_cell *cell)
{
    kith_fabric_cell_key_t key = {0};
    key.zone = n->zone_id;
    key.cell_x = cell->x;
    key.cell_y = cell->y;
    key.cell_z = cell->z;
    key.lod = 0u;
    return key;
}

/*---------------------------------------------------------------------------
 * dirty set and epoch table (open addressing under the bookkeeping lock)
 *-------------------------------------------------------------------------*/

// Insert-or-touch the cell in the generation-stamped dirty set: a cell
// dirtied N times between flushes occupies one slot and bumps once. The
// caller holds the bookkeeping lock. On a full table (unreachable: the
// capacity covers twice the actor population's worst-case mark fanout) the
// drop is counted in the overflow census rather than a cell silently
// missing its bump.
static void spatial_dirty_mark(spatial_native_t *n, const struct spatial_cell *cell)
{
    const uint64_t count = n->dirty_slot_count;
    const uint64_t mask = count - 1u;
    uint64_t i = spatial_cell_hash(n, cell) & mask;
    for (uint64_t probes = 0u; probes < count; ++probes)
    {
        if (n->dirty_slots[i].stamp == 0u)
        {
            n->dirty_slots[i].cell = *cell;
            n->dirty_slots[i].stamp = n->dirty_generation;
            return;
        }
        if (spatial_cell_equal(&n->dirty_slots[i].cell, cell))
        {
            return;
        }
        i = (i + 1u) & mask;
    }
    atomic_fetch_add_explicit(&n->dirty_overflow_dropped, 1u, memory_order_relaxed);
}

// Read-modify-write the cell's authority epoch, inserting at 1 on first
// sight. The caller holds the bookkeeping lock. Returns 0 when the table is
// full; every stored epoch is a bump of a previous value, so 0 is an
// unambiguous failure sentinel.
static uint32_t spatial_epoch_bump(spatial_native_t *n, const struct spatial_cell *cell)
{
    const uint64_t count = n->epoch_slot_count;
    const uint64_t mask = count - 1u;
    uint64_t i = spatial_cell_hash(n, cell) & mask;
    for (uint64_t probes = 0u; probes < count; ++probes)
    {
        if (n->epoch_slots[i].epoch == 0u)
        {
            n->epoch_slots[i].cell = *cell;
            n->epoch_slots[i].epoch = 1u;
            return 1u;
        }
        if (spatial_cell_equal(&n->epoch_slots[i].cell, cell))
        {
            n->epoch_slots[i].epoch += 1u;
            return n->epoch_slots[i].epoch;
        }
        i = (i + 1u) & mask;
    }
    return 0u;
}

/*---------------------------------------------------------------------------
 * apply core (the caller holds the actor's stripe)
 *-------------------------------------------------------------------------*/

// Publish the stepped actor into its position-derived cell, update the cell
// map, and dirty both the arrival cell and the departed cell. The stripe is
// held; the artifact lands at the last stored position even when two
// workers race the same actor, because same-actor applies serialize on the
// stripe. Epoch-0 artifact keys are the production shape: the Python facade
// zero-inits the key's authority epoch and publish_seq.
static int
spatial_publish_and_mark(spatial_native_t *n, uint64_t idx, const kith_sim_actor_t *stepped)
{
    const struct spatial_cell old_cell = n->cells[idx];
    const struct spatial_cell new_cell = spatial_cell_of(n, stepped);
    kith_sim_artifact_key_t key = {0};
    key.zone = n->zone_id;
    key.cell_x = new_cell.x;
    key.cell_y = new_cell.y;
    key.cell_z = new_cell.z;
    key.lod = 0u;
    int rc = kith_sim_publish_artifact(n->sim, &key, stepped, nullptr);
    if (rc != 0)
    {
        return rc;
    }
    n->cells[idx] = new_cell;
    pthread_mutex_lock(&n->book_lock);
    spatial_dirty_mark(n, &new_cell);
    if (!spatial_cell_equal(&old_cell, &new_cell))
    {
        spatial_dirty_mark(n, &old_cell);
    }
    pthread_mutex_unlock(&n->book_lock);
    return 0;
}

// The per-actor movement transaction, minus the stripe (the caller holds the
// actor's stripe): table copy, model apply_input via the vtable, one step,
// store, publish, dirty marks. A missing actor or any model/publish failure
// leaves the stored state untouched, mirroring the Python handler's
// exception path (no store, no publish, no record).
static int spatial_apply_core(spatial_native_t *n,
                              uint64_t idx,
                              const kith_sim_input_t *input,
                              uint32_t dt_ms,
                              kith_sim_actor_t *out_stepped)
{
    kith_sim_actor_t stepped = n->actors[idx];
    int rc = kith_sim_model_apply_input(n->model, &stepped, input);
    if (rc == 0)
    {
        rc = kith_sim_model_step(n->model, &stepped, 1u, dt_ms);
    }
    if (rc == 0)
    {
        // Store before publish, matching the Python transaction: a publish
        // failure leaves the stepped state stored but skips the cell-map
        // update, the dirty marks, and the record (the exception path).
        n->actors[idx] = stepped;
        rc = spatial_publish_and_mark(n, idx, &stepped);
    }
    if (rc == 0 && out_stepped != nullptr)
    {
        *out_stepped = stepped;
    }
    return rc;
}

/*---------------------------------------------------------------------------
 * identity gate and subscription-window diff (the caller holds the stripe)
 *-------------------------------------------------------------------------*/

// Build the Chebyshev neighborhood of one cell at the configured radius.
static uint32_t spatial_neighborhood(const spatial_native_t *n,
                                     const struct spatial_cell *center,
                                     kith_fabric_cell_key_t *out)
{
    const int32_t r = (int32_t)n->cell_radius;
    uint32_t count = 0u;
    for (int32_t dz = -r; dz <= r; ++dz)
    {
        for (int32_t dy = -r; dy <= r; ++dy)
        {
            for (int32_t dx = -r; dx <= r; ++dx)
            {
                const struct spatial_cell c = {
                    .x = center->x + dx,
                    .y = center->y + dy,
                    .z = center->z + dz,
                };
                out[count++] = spatial_cell_key(n, &c);
            }
        }
    }
    return count;
}

static bool spatial_key_in(const kith_fabric_cell_key_t *key,
                           const kith_fabric_cell_key_t *keys,
                           uint32_t count)
{
    for (uint32_t i = 0u; i < count; ++i)
    {
        const kith_fabric_cell_key_t *k = &keys[i];
        if (k->cell_x == key->cell_x && k->cell_y == key->cell_y && k->cell_z == key->cell_z &&
            k->zone == key->zone && k->lod == key->lod)
        {
            return true;
        }
    }
    return false;
}

// Diff the subscriber's window across a cell crossing: cells that left the
// neighborhood are removed first, cells that entered are added (the Python
// diff's order). A radius-r crossing changes at most 3 of 9 cells per axis
// pair, so the diff issues at most six operations instead of a clear plus a
// full re-add. A window operation failure aborts the diff with its error
// (the Python handler's exception path); the caller then skips the record.
static int spatial_diff_window(spatial_native_t *n,
                               kith_gateway_session_t *session,
                               const struct spatial_cell *before,
                               const struct spatial_cell *after)
{
    kith_fabric_cell_key_t old_keys[125u];
    kith_fabric_cell_key_t new_keys[125u];
    const uint32_t old_count = spatial_neighborhood(n, before, old_keys);
    const uint32_t new_count = spatial_neighborhood(n, after, new_keys);
    for (uint32_t i = 0u; i < old_count; ++i)
    {
        if (spatial_key_in(&old_keys[i], new_keys, new_count))
        {
            continue;
        }
        int rc = kith_gateway_session_window_remove(session, &old_keys[i]);
        if (rc != 0)
        {
            return rc;
        }
    }
    for (uint32_t i = 0u; i < new_count; ++i)
    {
        if (spatial_key_in(&new_keys[i], old_keys, old_count))
        {
            continue;
        }
        int rc = kith_gateway_session_window_add(session, &new_keys[i]);
        if (rc != 0)
        {
            return rc;
        }
    }
    return 0;
}

// The identity gate and the crossing window diff, in the wire handler's
// order (apply-first: the movement already committed above). A frame whose
// actor is not the session's bound one moves the shared actor, skips the
// window diff, and counts the drop — the exact order the Python gate keeps.
// A session-metadata failure aborts without a record, mirroring the Python
// handler's except path. Returns 0 on success (including a counted gate
// drop); nonzero aborts the handler before the record.
static int spatial_gate_and_diff(spatial_native_t *n,
                                 kith_gateway_session_t *session,
                                 uint64_t actor_id,
                                 const kith_sim_actor_t *before,
                                 const kith_sim_actor_t *stepped)
{
    kith_gateway_session_info_t info = {0};
    if (kith_gateway_session_info(session, &info) != 0)
    {
        return -1;
    }
    if (info.actor_id != actor_id)
    {
        atomic_fetch_add_explicit(&n->gate_drops, 1u, memory_order_relaxed);
        return 0;
    }
    const struct spatial_cell before_cell = spatial_cell_of(n, before);
    const struct spatial_cell after_cell = spatial_cell_of(n, stepped);
    if (spatial_cell_equal(&before_cell, &after_cell))
    {
        return 0;
    }
    return spatial_diff_window(n, session, &before_cell, &after_cell);
}

/*---------------------------------------------------------------------------
 * replay record FIFO
 *-------------------------------------------------------------------------*/

// Append one replay record after the stripe releases. A full ring drops and
// counts rather than blocking a pool worker; the snapshot-drain semantics
// keep each drained record exactly-once and bounded per tick.
static void spatial_record_append(spatial_native_t *n, const struct spatial_move_record *rec)
{
    pthread_mutex_lock(&n->record_lock);
    if (n->record_tail - n->record_head >= SPATIAL_RECORD_CAP)
    {
        atomic_fetch_add_explicit(&n->record_overflow_dropped, 1u, memory_order_relaxed);
    }
    else
    {
        n->records[n->record_tail % SPATIAL_RECORD_CAP] = *rec;
        n->record_tail += 1u;
    }
    pthread_mutex_unlock(&n->record_lock);
}

/*---------------------------------------------------------------------------
 * wire handler
 *-------------------------------------------------------------------------*/

// Decode the movement payload: little-endian, matching the game codec's
// ``struct.unpack("<QIhhhB", ...)`` (the codec is the game's concern; the
// gateway's own replication frames are big-endian, a separate surface).
// Fills the replay record; the handler derives the sim input from it.
static bool spatial_decode_input(const void *payload, uint32_t len, struct spatial_move_record *rec)
{
    const uint8_t *p = payload;
    if (p == nullptr || len < SPATIAL_INPUT_WIRE_LEN)
    {
        return false;
    }
    uint64_t actor_id = 0u;
    for (uint32_t i = 0u; i < 8u; ++i)
    {
        actor_id |= (uint64_t)p[i] << (8u * i);
    }
    uint32_t tick = 0u;
    for (uint32_t i = 0u; i < 4u; ++i)
    {
        tick |= (uint32_t)p[8u + i] << (8u * i);
    }
    rec->actor_id = actor_id;
    rec->input_tick = tick;
    rec->move_x = (int16_t)(uint16_t)((uint16_t)p[12] | ((uint16_t)p[13] << 8u));
    rec->move_y = (int16_t)(uint16_t)((uint16_t)p[14] | ((uint16_t)p[15] << 8u));
    rec->move_z = (int16_t)(uint16_t)((uint16_t)p[16] | ((uint16_t)p[17] << 8u));
    rec->flags = p[18];
    memset(rec->pad, 0, sizeof(rec->pad));
    return true;
}

// The pool-dispatched movement handler: the whole per-input
// sequence in C, in the Python wire handler's exact order — payload length
// check, decode, stripe, table-miss drop, apply/step/store/publish, dirty
// marks, identity gate, crossing window diff, then the record append after
// the stripe releases. A malformed payload, an unknown actor, or any
// model/publish/window failure skips the record, matching the Python
// handler's silent-drop and exception paths.
static void spatial_movement_handler(uint16_t msg_type,
                                     const void *payload,
                                     uint32_t payload_len,
                                     kith_gateway_session_t *session,
                                     void *user_data)
{
    (void)msg_type;
    spatial_native_t *n = user_data;
    struct spatial_move_record rec = {0};
    if (!spatial_decode_input(payload, payload_len, &rec))
    {
        return;
    }
    if (rec.actor_id == 0u || rec.actor_id > (uint64_t)n->max_actors)
    {
        return;
    }
    const uint64_t idx = rec.actor_id - 1u;
    struct spatial_stripe *stripe = &n->stripes[idx % SPATIAL_STRIPE_COUNT];
    kith_sim_input_t input = {0};
    input.input_tick = rec.input_tick;
    input.move_x = rec.move_x;
    input.move_y = rec.move_y;
    input.move_z = rec.move_z;
    input.flags = rec.flags;
    bool applied = false;
    pthread_mutex_lock(&stripe->mutex);
    do
    {
        if (n->actors[idx].id == 0u)
        {
            break; // unknown actor: dropped silently, no record
        }
        kith_sim_actor_t before = n->actors[idx];
        kith_sim_actor_t stepped = {0};
        if (spatial_apply_core(n, idx, &input, SPATIAL_APPLY_DT_MS, &stepped) != 0)
        {
            break;
        }
        applied = true;
        if (spatial_gate_and_diff(n, session, rec.actor_id, &before, &stepped) != 0)
        {
            applied = false; // window failure aborts the record (parity)
        }
    } while (false);
    pthread_mutex_unlock(&stripe->mutex);
    if (applied)
    {
        spatial_record_append(n, &rec);
    }
}

/*---------------------------------------------------------------------------
 * per-tick flush
 *-------------------------------------------------------------------------*/

// Bump every cell dirtied since the last flush: snapshot-and-clear under
// the bookkeeping lock, per-cell epoch read-modify-write under the same
// lock, the fabric publish outside it. A cell re-dirtied during the drain
// lands in the new generation and bumps next tick. A stale-epoch publish is
// suppressed and counted (the higher-epoch publish already marked the cell
// pending); any other publish error propagates.

// Snapshot-drain the record FIFO: the tail is captured under the record
// lock, at most @p cap records of the snapshot are copied out, and records
// appended after the snapshot wait for the next tick — the drain is
// bounded, and each record drains exactly once, in apply-completion order.
static void spatial_drain_records(spatial_native_t *n,
                                  struct spatial_move_record *out_records,
                                  uint32_t cap,
                                  uint32_t *out_count,
                                  uint32_t *out_more)
{
    pthread_mutex_lock(&n->record_lock);
    const uint64_t snap_tail = n->record_tail;
    uint32_t copied = 0u;
    while (n->record_head < snap_tail && copied < cap)
    {
        out_records[copied] = n->records[n->record_head % SPATIAL_RECORD_CAP];
        n->record_head += 1u;
        copied += 1u;
    }
    pthread_mutex_unlock(&n->record_lock);
    *out_count = copied;
    *out_more = (n->record_head < snap_tail) ? 1u : 0u;
}

/*---------------------------------------------------------------------------
 * lifecycle
 *-------------------------------------------------------------------------*/

static uint32_t spatial_pow2_at_least(uint32_t v)
{
    uint32_t p = SPATIAL_HASH_MIN_SLOTS;
    while (p < v)
    {
        p <<= 1u;
    }
    return p;
}

int spatial_native_create(const struct spatial_native_params *params, spatial_native_t **out_native)
{
    if (params == nullptr || out_native == nullptr || params->size != sizeof(*params) ||
        params->abi_version != KITH_ABI_VERSION || params->sim == nullptr ||
        params->model == nullptr || params->fabric == nullptr || params->cell_size_q16 == 0u ||
        params->cell_radius > SPATIAL_MAX_CELL_RADIUS || params->max_actors == 0u)
    {
        return -1;
    }
    for (uint32_t i = 0u; i < 4u; ++i)
    {
        if (params->reserved[i] != nullptr)
        {
            return -1;
        }
    }
    spatial_native_t *n = calloc(1u, sizeof(*n));
    if (n == nullptr)
    {
        return -2;
    }
    n->zone_id = params->zone_id;
    n->cell_radius = params->cell_radius;
    n->cell_size_q16 = params->cell_size_q16;
    n->max_actors = params->max_actors;
    n->sim = params->sim;
    n->model = params->model;
    n->fabric = params->fabric;
    n->actors = calloc(params->max_actors, sizeof(*n->actors));
    n->cells = calloc((size_t)params->max_actors, sizeof(*n->cells));
    n->records = calloc(SPATIAL_RECORD_CAP, sizeof(*n->records));
    n->dirty_slot_count = spatial_pow2_at_least(4u * params->max_actors);
    n->epoch_slot_count = n->dirty_slot_count;
    n->dirty_slots = calloc((size_t)n->dirty_slot_count, sizeof(*n->dirty_slots));
    n->epoch_slots = calloc((size_t)n->epoch_slot_count, sizeof(*n->epoch_slots));
    if (n->actors == nullptr || n->cells == nullptr || n->records == nullptr ||
        n->dirty_slots == nullptr || n->epoch_slots == nullptr)
    {
        spatial_native_destroy(n);
        return -2;
    }
    if (pthread_mutex_init(&n->book_lock, nullptr) != 0 ||
        pthread_mutex_init(&n->control_lock, nullptr) != 0 ||
        pthread_mutex_init(&n->record_lock, nullptr) != 0)
    {
        spatial_native_destroy(n);
        return -2;
    }
    for (uint32_t s = 0u; s < SPATIAL_STRIPE_COUNT; ++s)
    {
        if (pthread_mutex_init(&n->stripes[s].mutex, nullptr) != 0)
        {
            for (uint32_t d = 0u; d < s; ++d)
            {
                pthread_mutex_destroy(&n->stripes[d].mutex);
            }
            spatial_native_destroy(n);
            return -2;
        }
    }
    n->dirty_generation = 1u;
    *out_native = n;
    return 0;
}

void spatial_native_destroy(spatial_native_t *native)
{
    if (native == nullptr)
    {
        return;
    }
    pthread_mutex_destroy(&native->book_lock);
    pthread_mutex_destroy(&native->control_lock);
    pthread_mutex_destroy(&native->record_lock);
    for (uint32_t s = 0u; s < SPATIAL_STRIPE_COUNT; ++s)
    {
        pthread_mutex_destroy(&native->stripes[s].mutex);
    }
    free(native->actors);
    free(native->cells);
    free(native->records);
    free(native->dirty_slots);
    free(native->epoch_slots);
    free(native);
}

int spatial_native_register_movement_handler(spatial_native_t *native,
                                             kith_gateway_t *gateway,
                                             uint16_t msg_type)
{
    if (native == nullptr || gateway == nullptr)
    {
        return -1;
    }
    return kith_gateway_register_handler_flags(
        gateway, msg_type, spatial_movement_handler, native, KITH_GATEWAY_HANDLER_POOL);
}

/*---------------------------------------------------------------------------
 * control-plane accessors
 *-------------------------------------------------------------------------*/

int spatial_native_actor_insert(spatial_native_t *native, const kith_sim_actor_t *actor)
{
    if (native == nullptr || actor == nullptr || actor->id == 0u ||
        actor->id > (uint64_t)native->max_actors)
    {
        return -1;
    }
    const uint64_t idx = actor->id - 1u;
    int rc;
    pthread_mutex_lock(&native->control_lock);
    struct spatial_stripe *stripe = &native->stripes[idx % SPATIAL_STRIPE_COUNT];
    pthread_mutex_lock(&stripe->mutex);
    if (native->actors[idx].id != 0u)
    {
        rc = -1;
    }
    else
    {
        // The insert publishes the initial state under the fresh actor's
        // stripe, so a concurrent movement input on the fresh actor cannot
        // interleave a moved position between the store and the spawn
        // publish — the Python spawn path's exact hold order. The spawn
        // publish marks only the arrival cell: no prior cell exists for a
        // fresh actor.
        const struct spatial_cell cell = spatial_cell_of(native, actor);
        kith_sim_artifact_key_t key = {0};
        key.zone = native->zone_id;
        key.cell_x = cell.x;
        key.cell_y = cell.y;
        key.cell_z = cell.z;
        key.lod = 0u;
        rc = kith_sim_publish_artifact(native->sim, &key, actor, nullptr);
        if (rc == 0)
        {
            native->actors[idx] = *actor;
            native->cells[idx] = cell;
            pthread_mutex_lock(&native->book_lock);
            spatial_dirty_mark(native, &cell);
            pthread_mutex_unlock(&native->book_lock);
        }
    }
    pthread_mutex_unlock(&native->stripes[idx % SPATIAL_STRIPE_COUNT].mutex);
    pthread_mutex_unlock(&native->control_lock);
    return rc;
}

int spatial_native_actor_teleport(spatial_native_t *native,
                                  uint64_t actor_id,
                                  int64_t pos_x,
                                  int64_t pos_y,
                                  int64_t pos_z,
                                  kith_sim_actor_t *out_actor)
{
    if (native == nullptr || actor_id == 0u || actor_id > (uint64_t)native->max_actors)
    {
        return -1;
    }
    const uint64_t idx = actor_id - 1u;
    struct spatial_stripe *stripe = &native->stripes[idx % SPATIAL_STRIPE_COUNT];
    pthread_mutex_lock(&stripe->mutex);
    kith_sim_actor_t current = native->actors[idx];
    if (current.id == 0u)
    {
        pthread_mutex_unlock(&stripe->mutex);
        return -1;
    }
    kith_sim_actor_t updated = {0};
    updated.id = current.id;
    updated.pos_x = pos_x;
    updated.pos_y = pos_y;
    updated.pos_z = pos_z;
    updated.input_tick = current.input_tick;
    updated.flags = current.flags;
    // A teleport is not a movement apply: the update counter certifies
    // movement coverage, so it carries unchanged.
    updated.update_seq = current.update_seq;
    // Store before publish, matching the Python teleport transaction.
    native->actors[idx] = updated;
    int rc = spatial_publish_and_mark(native, idx, &updated);
    if (rc == 0 && out_actor != nullptr)
    {
        *out_actor = updated;
    }
    pthread_mutex_unlock(&stripe->mutex);
    return rc;
}

int spatial_native_actor_get(spatial_native_t *native,
                             uint64_t actor_id,
                             kith_sim_actor_t *out_actor)
{
    if (native == nullptr || out_actor == nullptr || actor_id == 0u ||
        actor_id > (uint64_t)native->max_actors)
    {
        return -1;
    }
    const uint64_t idx = actor_id - 1u;
    struct spatial_stripe *stripe = &native->stripes[idx % SPATIAL_STRIPE_COUNT];
    pthread_mutex_lock(&stripe->mutex);
    const kith_sim_actor_t current = native->actors[idx];
    pthread_mutex_unlock(&stripe->mutex);
    if (current.id == 0u)
    {
        return -1;
    }
    *out_actor = current;
    return 0;
}

int spatial_native_actor_snapshot(spatial_native_t *native,
                                  kith_sim_actor_t *out_actors,
                                  uint32_t cap,
                                  uint32_t *out_count)
{
    if (native == nullptr || out_actors == nullptr || out_count == nullptr)
    {
        return -1;
    }
    pthread_mutex_lock(&native->control_lock);
    for (uint32_t s = 0u; s < SPATIAL_STRIPE_COUNT; ++s)
    {
        pthread_mutex_lock(&native->stripes[s].mutex);
    }
    uint32_t count = 0u;
    for (uint64_t i = 0u; i < (uint64_t)native->max_actors; ++i)
    {
        if (native->actors[i].id != 0u)
        {
            if (count >= cap)
            {
                for (uint32_t s = SPATIAL_STRIPE_COUNT; s > 0u; --s)
                {
                    pthread_mutex_unlock(&native->stripes[s - 1u].mutex);
                }
                pthread_mutex_unlock(&native->control_lock);
                return -2;
            }
            out_actors[count] = native->actors[i];
            count += 1u;
        }
    }
    for (uint32_t s = SPATIAL_STRIPE_COUNT; s > 0u; --s)
    {
        pthread_mutex_unlock(&native->stripes[s - 1u].mutex);
    }
    pthread_mutex_unlock(&native->control_lock);
    *out_count = count;
    return 0;
}

int spatial_native_apply(spatial_native_t *native,
                         uint64_t actor_id,
                         const kith_sim_input_t *input,
                         uint32_t dt_ms,
                         kith_sim_actor_t *out_actor)
{
    if (native == nullptr || input == nullptr || actor_id == 0u ||
        actor_id > (uint64_t)native->max_actors)
    {
        return -1;
    }
    const uint64_t idx = actor_id - 1u;
    struct spatial_stripe *stripe = &native->stripes[idx % SPATIAL_STRIPE_COUNT];
    kith_sim_actor_t stepped = {0};
    pthread_mutex_lock(&stripe->mutex);
    if (native->actors[idx].id == 0u)
    {
        pthread_mutex_unlock(&stripe->mutex);
        return -1;
    }
    int rc = spatial_apply_core(native, idx, input, dt_ms, &stepped);
    pthread_mutex_unlock(&stripe->mutex);
    if (rc == 0)
    {
        if (out_actor != nullptr)
        {
            *out_actor = stepped;
        }
        struct spatial_move_record rec = {0};
        rec.actor_id = actor_id;
        rec.input_tick = input->input_tick;
        rec.move_x = input->move_x;
        rec.move_y = input->move_y;
        rec.move_z = input->move_z;
        rec.flags = input->flags;
        spatial_record_append(native, &rec);
    }
    return rc;
}

int spatial_native_mark_actor_cell_dirty(spatial_native_t *native, uint64_t actor_id)
{
    if (native == nullptr || actor_id == 0u || actor_id > (uint64_t)native->max_actors)
    {
        return -1;
    }
    const uint64_t idx = actor_id - 1u;
    struct spatial_stripe *stripe = &native->stripes[idx % SPATIAL_STRIPE_COUNT];
    pthread_mutex_lock(&stripe->mutex);
    const bool present = native->actors[idx].id != 0u;
    const struct spatial_cell cell = native->cells[idx];
    pthread_mutex_unlock(&stripe->mutex);
    if (!present)
    {
        return -1;
    }
    pthread_mutex_lock(&native->book_lock);
    spatial_dirty_mark(native, &cell);
    pthread_mutex_unlock(&native->book_lock);
    return 0;
}

uint64_t spatial_native_gate_drops(const spatial_native_t *native)
{
    if (native == nullptr)
    {
        return 0u;
    }
    return atomic_load_explicit(&native->gate_drops, memory_order_relaxed);
}

void spatial_native_overflows(const spatial_native_t *native,
                              uint64_t *out_record_drops,
                              uint64_t *out_dirty_drops)
{
    if (native == nullptr || out_record_drops == nullptr || out_dirty_drops == nullptr)
    {
        return;
    }
    *out_record_drops =
        atomic_load_explicit(&native->record_overflow_dropped, memory_order_relaxed);
    *out_dirty_drops = atomic_load_explicit(&native->dirty_overflow_dropped, memory_order_relaxed);
}

/*---------------------------------------------------------------------------
 * per-tick flush
 *-------------------------------------------------------------------------*/

int spatial_native_flush(spatial_native_t *native,
                         struct spatial_move_record *out_records,
                         uint32_t cap,
                         uint32_t *out_count,
                         uint32_t *out_more,
                         uint64_t *out_bumped,
                         uint64_t *out_eperm)
{
    if (native == nullptr || out_count == nullptr || out_more == nullptr || out_bumped == nullptr ||
        out_eperm == nullptr || (cap > 0u && out_records == nullptr))
    {
        return -1;
    }
    *out_count = 0u;
    *out_more = 0u;
    *out_bumped = 0u;
    *out_eperm = 0u;

    // Snapshot-and-clear the dirty set under the bookkeeping lock, then
    // bump each drained cell: handlers keep marking while the flush
    // publishes, and a cell re-dirtied mid-flush lands in the new
    // generation for the next tick.
    pthread_mutex_lock(&native->book_lock);
    const uint32_t gen = native->dirty_generation;
    native->dirty_generation = gen + 1u;
    if (native->dirty_generation == 0u)
    {
        for (uint32_t i = 0u; i < native->dirty_slot_count; ++i)
        {
            native->dirty_slots[i].stamp = 0u;
        }
        native->dirty_generation = 1u;
    }
    pthread_mutex_unlock(&native->book_lock);

    for (uint32_t i = 0u; i < native->dirty_slot_count; ++i)
    {
        struct spatial_cell cell = {0};
        bool claimed = false;
        pthread_mutex_lock(&native->book_lock);
        if (native->dirty_slots[i].stamp == gen)
        {
            cell = native->dirty_slots[i].cell;
            native->dirty_slots[i].stamp = 0u;
            claimed = true;
        }
        pthread_mutex_unlock(&native->book_lock);
        if (!claimed)
        {
            continue;
        }
        pthread_mutex_lock(&native->book_lock);
        const uint32_t epoch = spatial_epoch_bump(native, &cell);
        pthread_mutex_unlock(&native->book_lock);
        if (epoch == 0u)
        {
            return -3;
        }
        const kith_fabric_cell_key_t key = spatial_cell_key(native, &cell);
        uint64_t seq = 0u;
        const int rc = kith_fabric_publish(native->fabric, &key, epoch, &seq);
        if (rc == 0)
        {
            *out_bumped += 1u;
        }
        else if (rc == kith_error_return(KITH_EPERM))
        {
            // A stale-epoch publish loses to the higher-epoch publish that
            // already marked the cell pending; the next refresh still pulls
            // the latest sim state.
            *out_eperm += 1u;
        }
        else
        {
            return rc;
        }
    }

    spatial_drain_records(native, out_records, cap, out_count, out_more);
    return 0;
}
