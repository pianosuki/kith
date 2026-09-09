/*
 * C-native apply-transaction bench driver for the spatial handler sequence.
 *
 * Boots a sim handle (the built-in tile2d model, behavior grid passed in as
 * a text file) and a fabric handle borrowing it, then drives the shipped
 * per-input apply sequence - wire decode, bind-map resolve (the identity
 * gate), stripe lock, model apply and step, artifact-store publish,
 * dirty-cell marks, record-buffer append - from N always-backlogged threads
 * with no Python anywhere on the measured path. A dedicated flush thread
 * drains the dirty-cell set at the tick cadence (per-cell epoch bump + one
 * fabric publish + a subscription drain), mirroring the handler set's
 * per-tick flush shape.
 *
 * Headline metric: per-apply thread CPU from whole-window accumulators
 * (CLOCK_THREAD_CPUTIME_ID pairs), folded with the flush thread's CPU.
 * Per-apply CPU and wall-latency rings (overwrite-oldest) back the
 * percentile columns only. Output is a JSON report consumed by
 * tools/perf/apply_capacity_bench.py --c-native, which records the run
 * envelope.
 *
 * Known biases, all in the bench's favor and recorded for interpretation:
 * no worker-pool queue hop (off-CPU class), no gateway cache render or
 * compose (delivery-side fixed load), a dedicated flush thread (production
 * flush work rides a pool worker), no mid-run bind-mutation traffic, one
 * fabric subscription (multi-gateway fanout marking is unmeasured), encode
 * and decode both in-transaction (the wire path decodes only), per-crossing
 * subscription-window ops unmodeled (per-crossing, not per-input), no EBUSY
 * backpressure, and a bind map that models lookup cost only (the shipped
 * transaction holds the actor's stripe across the resolve, so a
 * stripe-sharded mirror reads it under the held lock).
 */

#include <errno.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <pthread.h>

#include "kith/fabric/fabric.h"
#include "kith/sim/sim.h"
#include "kith/types.h"
#include "kith/version.h"

// --------------------------------------------------------------------------
// named constants
// --------------------------------------------------------------------------

/** The spatial actor_input wire payload: 8+4+2+2+2+1 bytes, little-endian. */
#define BENCH_PAYLOAD_LEN 19u
/** Stripe-lock count; the handler set's STRIPE_COUNT topology. */
#define BENCH_STRIPE_COUNT 64u
/** Per-thread overwrite-oldest ring capacity for latency percentiles. */
#define BENCH_RING_CAP 65536u
/** Dirty-cell hash slots; a power of two above the 64-cell world. */
#define BENCH_DIRTY_CAP 256u
/** Per-cell authority-epoch hash slots; a power of two. */
#define BENCH_EPOCH_CAP 1024u
/** Subscription-drain scratch depth in product headers. */
#define BENCH_DRAIN_MAX 256u
/** Flush cadence in milliseconds: the embedded wiring's 20 Hz tick. */
#define BENCH_TICK_MS 50
/** One tile in Q16.16 fixed-point units; one cell spans one tile. */
#define BENCH_TILE_FIX (1 << 16)
/** The harness movement scenario's input deflection. */
#define BENCH_MOVE_MAGNITUDE 200
/** Upper bound on the --threads sweep (bounds worker arrays). */
#define BENCH_MAX_THREADS 64u
/** Window-count bound per config. */
#define BENCH_MAX_WINDOWS 64u

static_assert((BENCH_DIRTY_CAP & (BENCH_DIRTY_CAP - 1u)) == 0u, "dirty capacity is a power of two");
static_assert((BENCH_EPOCH_CAP & (BENCH_EPOCH_CAP - 1u)) == 0u, "epoch slots are a power of two");

// --------------------------------------------------------------------------
// state types
// --------------------------------------------------------------------------

/** One 3D cell locator (2D world: z stays 0). */
struct bench_cell
{
    int32_t x;
    int32_t y;
    int32_t z;
};

/** Per-actor patrol heading: advisory latest-wins bookkeeping read by the
 *  next frame build and written after the apply. Relaxed atomics — the pair
 *  is not updated atomically, and a mixed x/y read costs one frame of stale
 *  steering that the border steer corrects. */
struct bench_heading
{
    _Atomic int16_t x;
    _Atomic int16_t y;
};

/** One C-buffered movement record: a pure echo of the decoded input (the
 *  handler set's v1 MoveEvent: actor_id, input_tick, move vector, flags —
 *  no sim read), drained by the per-tick flush in the shipped design. */
struct bench_record
{
    uint64_t actor_id;
    uint32_t input_tick;
    int16_t move_x;
    int16_t move_y;
    int16_t move_z;
    uint8_t flags;
};

/** Per-worker overwrite-oldest latency rings; percentile columns only. */
struct bench_rings
{
    uint64_t *cpu;
    uint64_t *wall;
    uint64_t count;
};

/** Window-scoped accumulators; the headline CPU mean reads these, not rings
 *  (a saturated second can overrun the ring, so ring sums undercount). */
struct bench_tally
{
    uint64_t applies;
    uint64_t cpu_ns;
    uint64_t wall_ns;
};

struct bench_params
{
    uint64_t actors;
    const char *behavior_path;
    uint32_t zone;
    uint32_t tiles;
    uint32_t dt_ms;
    uint32_t windows;
    uint32_t tick;
    uint32_t seed;
    double window_s;
    double warmup_s;
};

/** One stripe lock, cache-line aligned so adjacent stripes do not share. */
struct bench_stripe
{
    alignas(64) pthread_mutex_t mutex;
};

/** Per-cell dirty marks: insert-or-touch per flush generation, so a cell
 *  dirtied N times between flushes occupies one slot and bumps once. */
struct bench_dirty
{
    struct bench_cell cell[BENCH_DIRTY_CAP];
    uint32_t stamp[BENCH_DIRTY_CAP];
    uint32_t generation;
};

/** Per-cell authority epochs, bumped once per flush under the book lock. */
struct bench_epochs
{
    struct bench_cell key[BENCH_EPOCH_CAP];
    uint32_t value[BENCH_EPOCH_CAP];
};

/** Session-to-actor binding mirror: lookup cost only (see file header). */
struct bench_bind_map
{
    uint64_t *session;
    uint64_t *actor;
    uint64_t mask;
};

struct bench_world
{
    kith_sim_t *sim;
    kith_sim_model_t *model;
    kith_fabric_t *fabric;
    kith_fabric_subscription_t *subscription;
    uint32_t zone;
    uint32_t tiles;
    uint64_t actors;
    kith_sim_actor_t *actor_table;
    struct bench_cell *actor_cells;
    struct bench_heading *headings;
    struct bench_stripe stripes[BENCH_STRIPE_COUNT];
    alignas(64) pthread_mutex_t book;
    struct bench_dirty dirty;
    struct bench_epochs epochs;
    struct bench_bind_map bind;
};

struct bench_worker
{
    struct bench_world *world;
    uint32_t dt_ms;
    uint64_t deadline_ns;
    uint64_t rng;
    uint32_t input_tick;
    struct bench_tally tally;
    struct bench_rings rings;
    struct bench_record *records;
    uint64_t record_count;
    int failed;
    const char *failed_what;
};

/** Flush-thread counters, snapshotted by value per config pass. */
struct bench_flush_stats
{
    uint64_t flush_count;
    uint64_t cpu_ns;
    uint64_t fabric_publishes;
    uint64_t eperm_suppressed;
    uint64_t cells_drained;
    uint64_t drain_products;
};

/** The flush thread plus its failure record (read by main after the join)
 *  and its counters, folded under stats_lock in one section per pass and
 *  snapshotted by value at config boundaries. */
struct bench_flusher
{
    struct bench_world *world;
    atomic_bool stop;
    pthread_t thread;
    int failed;
    const char *failed_what;
    pthread_mutex_t stats_lock;
    struct bench_flush_stats stats;
};

/** One timed window's headline numbers (per-config means from tallies). */
struct bench_row
{
    uint32_t threads;
    uint32_t tick;
    uint32_t window;
    uint64_t applies;
    double elapsed_s;
    double cpu_mean_us;
    double wall_mean_us;
};

struct bench_aggregate
{
    uint32_t threads;
    uint32_t tick_enabled;
    uint64_t applies_total;
    double elapsed_s;
    double applies_per_s;
    double cpu_mean_us;
    double cpu_p50_us;
    double cpu_p95_us;
    double cpu_p99_us;
    double wall_mean_us;
    double wall_p50_us;
    double wall_p95_us;
    double wall_p99_us;
    double total_cpu_us_per_apply;
    double flush_cpu_us_per_apply;
    double flush_total_cpu_s;
    uint64_t flush_count;
    uint64_t fabric_publishes;
    uint64_t eperm_suppressed;
    uint64_t dirty_cells_drained;
    uint64_t drain_products;
    uint64_t records_appended;
};

// --------------------------------------------------------------------------
// infrastructure
// --------------------------------------------------------------------------

[[noreturn]] static void bench_die(const char *what)
{
    (void)fprintf(stderr, "c_apply_bench: %s\n", what);
    exit(1);
}

static void bench_check(int rc, const char *what)
{
    if (rc != 0)
    {
        (void)fprintf(stderr, "c_apply_bench: %s failed: %d\n", what, rc);
        exit(1);
    }
}

static uint64_t bench_wall_ns(void)
{
    struct timespec ts;
    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0)
    {
        bench_die("clock_gettime(CLOCK_MONOTONIC)");
    }
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

static uint64_t bench_thread_cpu_ns(void)
{
    struct timespec ts;
    if (clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ts) != 0)
    {
        bench_die("clock_gettime(CLOCK_THREAD_CPUTIME_ID)");
    }
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

static uint64_t bench_splitmix64(uint64_t *state)
{
    *state += 0x9E3779B97F4A7C15ULL;
    uint64_t z = *state;
    z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
    z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
    return z ^ (z >> 31);
}

/** Floor division of a Q16.16 position into cell coordinates (the handler
 *  set's cell derivation: one cell per tile at the deployment cell size). */
static struct bench_cell bench_cell_of(const kith_sim_actor_t *actor)
{
    const struct bench_cell cell = {
        .x = (int32_t)(actor->pos_x >> KITH_SIM_FIX_SHIFT),
        .y = (int32_t)(actor->pos_y >> KITH_SIM_FIX_SHIFT),
        .z = (int32_t)(actor->pos_z >> KITH_SIM_FIX_SHIFT),
    };
    return cell;
}

static bool bench_cell_equal(const struct bench_cell *a, const struct bench_cell *b)
{
    return a->x == b->x && a->y == b->y && a->z == b->z;
}

static uint64_t bench_cell_hash(const struct bench_cell *cell)
{
    uint64_t h = (uint64_t)(uint32_t)cell->x;
    h = (h << 21u) ^ (uint64_t)(uint32_t)cell->y;
    h = (h << 21u) ^ (uint64_t)(uint32_t)cell->z;
    h *= 0x9E3779B97F4A7C15ULL;
    h ^= h >> 31;
    return h;
}

static int bench_cmp_u64(const void *lhs, const void *rhs)
{
    const uint64_t left = *(const uint64_t *)lhs;
    const uint64_t right = *(const uint64_t *)rhs;
    return (left > right) - (left < right);
}

static double bench_percentile_us(const uint64_t *sorted_ns, uint64_t count, double fraction)
{
    if (count == 0u)
    {
        return 0.0;
    }
    uint64_t index = (uint64_t)((double)(count - 1u) * fraction + 0.5);
    if (index >= count)
    {
        index = count - 1u;
    }
    return (double)sorted_ns[index] / 1000.0;
}

// --------------------------------------------------------------------------
// wire payload codec (the spatial actor_input layout, LE)
// --------------------------------------------------------------------------

/** Pack the 19-byte actor_input payload; the wire path receives frames, the
 *  bench builds one per apply so the decode step parses real bytes. */
static void bench_payload_encode(uint8_t out[BENCH_PAYLOAD_LEN],
                                 uint64_t actor_id,
                                 uint32_t input_tick,
                                 const struct bench_heading *heading)
{
    for (uint32_t i = 0u; i < 8u; ++i)
    {
        out[i] = (uint8_t)(actor_id >> (8u * i));
    }
    for (uint32_t i = 0u; i < 4u; ++i)
    {
        out[8u + i] = (uint8_t)((input_tick >> (8u * i)) & 0xFFu);
    }
    const uint16_t mx = (uint16_t)atomic_load_explicit(&heading->x, memory_order_relaxed);
    const uint16_t my = (uint16_t)atomic_load_explicit(&heading->y, memory_order_relaxed);
    out[12] = (uint8_t)(mx & 0xFFu);
    out[13] = (uint8_t)(mx >> 8);
    out[14] = (uint8_t)(my & 0xFFu);
    out[15] = (uint8_t)(my >> 8);
    out[16] = 0u;
    out[17] = 0u;
    out[18] = 0u;
}

static void
bench_payload_decode(const uint8_t *payload, uint64_t *out_actor_id, kith_sim_input_t *out_input)
{
    uint64_t actor_id = 0u;
    for (uint32_t i = 0u; i < 8u; ++i)
    {
        actor_id |= (uint64_t)payload[i] << (8u * i);
    }
    uint32_t tick = 0u;
    for (uint32_t i = 0u; i < 4u; ++i)
    {
        tick |= (uint32_t)payload[8u + i] << (8u * i);
    }
    const uint16_t mx = (uint16_t)(payload[12] | ((uint16_t)payload[13] << 8));
    const uint16_t my = (uint16_t)(payload[14] | ((uint16_t)payload[15] << 8));
    const uint16_t mz = (uint16_t)(payload[16] | ((uint16_t)payload[17] << 8));
    out_actor_id[0] = actor_id;
    out_input->input_tick = tick;
    out_input->move_x = (int16_t)mx;
    out_input->move_y = (int16_t)my;
    out_input->move_z = (int16_t)mz;
    out_input->flags = payload[18];
}

// --------------------------------------------------------------------------
// shared bench state
// --------------------------------------------------------------------------

static uint64_t bench_bind_lookup(const struct bench_bind_map *map, uint64_t session)
{
    uint64_t i = session & map->mask;
    while (map->session[i] != session)
    {
        i = (i + 1u) & map->mask;
    }
    return map->actor[i];
}

/** Insert-or-touch one dirty mark: a cell re-marked inside the current
 *  flush generation coalesces into its existing slot, matching the
 *  handler set's set[CellKey] semantics. */
static void bench_dirty_mark(struct bench_dirty *set, const struct bench_cell *cell)
{
    const size_t mask = BENCH_DIRTY_CAP - 1u;
    const uint32_t gen = set->generation;
    size_t i = (size_t)(bench_cell_hash(cell) & (uint64_t)mask);
    for (;;)
    {
        if (set->stamp[i] != gen)
        {
            set->cell[i] = *cell;
            set->stamp[i] = gen;
            return;
        }
        if (bench_cell_equal(&set->cell[i], cell))
        {
            return;
        }
        i = (i + 1u) & (BENCH_DIRTY_CAP - 1u);
    }
}

/** Read-modify-write one cell's authority epoch; the book lock is held. */
static uint32_t bench_epoch_bump(struct bench_epochs *epochs, const struct bench_cell *cell)
{
    const size_t mask = (size_t)BENCH_EPOCH_CAP - 1u;
    size_t i = (size_t)bench_cell_hash(cell) & mask;
    for (;;)
    {
        if (epochs->value[i] == 0u)
        {
            epochs->key[i] = *cell;
            epochs->value[i] = 1u;
            return 1u;
        }
        if (bench_cell_equal(&epochs->key[i], cell))
        {
            epochs->value[i] += 1u;
            return epochs->value[i];
        }
        i = (i + 1u) & mask;
    }
}

// --------------------------------------------------------------------------
// per-apply transaction
// --------------------------------------------------------------------------

/** Publish the stepped actor into its position-derived cell and mark dirty
 *  cells under the bookkeeping lock; the store relocates a moved actor
 *  atomically. Runs with the actor's stripe held (publish-under-lock: the
 *  artifact lands at the last stored position even under concurrent
 *  same-actor applies). */
static void bench_publish_and_mark(struct bench_world *world,
                                   uint64_t actor_index,
                                   const kith_sim_actor_t *stepped)
{
    const struct bench_cell new_cell = bench_cell_of(stepped);
    const struct bench_cell old_cell = world->actor_cells[actor_index];
    kith_sim_artifact_key_t key = {0};
    key.zone = world->zone;
    key.cell_x = new_cell.x;
    key.cell_y = new_cell.y;
    key.cell_z = new_cell.z;
    key.lod = 0u;
    if (kith_sim_publish_artifact(world->sim, &key, stepped, nullptr) != 0)
    {
        bench_die("kith_sim_publish_artifact");
    }
    world->actor_cells[actor_index] = new_cell;
    bench_check(pthread_mutex_lock(&world->book), "pthread_mutex_lock(book)");
    bench_dirty_mark(&world->dirty, &new_cell);
    if (!bench_cell_equal(&old_cell, &new_cell))
    {
        bench_dirty_mark(&world->dirty, &old_cell);
    }
    bench_check(pthread_mutex_unlock(&world->book), "pthread_mutex_unlock(book)");
}

/** Reflect a patrol heading per axis at half a tile from the world border;
 *  one apply at the scenario deflection travels ~0.2 tiles, so reflected
 *  actors stay inside the world and their cells stay in the grid. */
static void
bench_steer(struct bench_heading *heading, const kith_sim_actor_t *stepped, uint32_t tiles)
{
    const int64_t low = (int64_t)BENCH_TILE_FIX / 2;
    const int64_t high = (int64_t)tiles * BENCH_TILE_FIX - BENCH_TILE_FIX / 2;
    if (stepped->pos_x <= low)
    {
        atomic_store_explicit(&heading->x, BENCH_MOVE_MAGNITUDE, memory_order_relaxed);
    }
    else if (stepped->pos_x >= high)
    {
        atomic_store_explicit(&heading->x, -BENCH_MOVE_MAGNITUDE, memory_order_relaxed);
    }
    if (stepped->pos_y <= low)
    {
        atomic_store_explicit(&heading->y, BENCH_MOVE_MAGNITUDE, memory_order_relaxed);
    }
    else if (stepped->pos_y >= high)
    {
        atomic_store_explicit(&heading->y, -BENCH_MOVE_MAGNITUDE, memory_order_relaxed);
    }
}

/** One full apply transaction: the per-input work a pool-dispatched C
 *  handler performs, from decode to record append, with the CPU/wall pair
 *  bracketing exactly that span. The frame build and the patrol steering
 *  are bench bookkeeping outside the measured span. */
static void bench_apply_one(struct bench_worker *worker)
{
    struct bench_world *world = worker->world;
    const uint64_t session = (bench_splitmix64(&worker->rng) % world->actors) + 1u;
    const uint64_t bound = bench_bind_lookup(&world->bind, session);
    const uint32_t input_tick = worker->input_tick++;
    uint8_t payload[BENCH_PAYLOAD_LEN];
    bench_payload_encode(payload, bound, input_tick, &world->headings[bound - 1u]);

    const uint64_t cpu0 = bench_thread_cpu_ns();
    const uint64_t wall0 = bench_wall_ns();

    uint64_t actor_id = 0u;
    kith_sim_input_t input = {0};
    bench_payload_decode(payload, &actor_id, &input);
    // The identity gate: a frame naming an actor other than the session's
    // bound actor drops before the lock. Every bench session resolves to
    // its own actor, so the gate never engages; the check documents the
    // seam's per-input gate shape.
    if (actor_id != bound)
    {
        return;
    }
    const uint64_t actor_index = actor_id - 1u;
    struct bench_stripe *stripe = &world->stripes[actor_id % BENCH_STRIPE_COUNT];
    bench_check(pthread_mutex_lock(&stripe->mutex), "pthread_mutex_lock(stripe)");
    kith_sim_actor_t stepped = world->actor_table[actor_index];
    bench_check(kith_sim_model_apply_input(world->model, &stepped, &input), "model apply_input");
    bench_check(kith_sim_model_step(world->model, &stepped, 1u, worker->dt_ms), "model step");
    world->actor_table[actor_index] = stepped;
    bench_publish_and_mark(world, actor_index, &stepped);
    bench_check(pthread_mutex_unlock(&stripe->mutex), "pthread_mutex_unlock(stripe)");

    struct bench_record *rec = &worker->records[worker->record_count % BENCH_RING_CAP];
    rec->actor_id = actor_id;
    rec->input_tick = input_tick;
    rec->move_x = input.move_x;
    rec->move_y = input.move_y;
    rec->move_z = input.move_z;
    rec->flags = input.flags;
    worker->record_count++;

    const uint64_t cpu = bench_thread_cpu_ns() - cpu0;
    const uint64_t wall = bench_wall_ns() - wall0;
    const uint64_t slot = worker->rings.count % BENCH_RING_CAP;
    worker->rings.cpu[slot] = cpu;
    worker->rings.wall[slot] = wall;
    worker->rings.count++;
    worker->tally.applies += 1u;
    worker->tally.cpu_ns += cpu;
    worker->tally.wall_ns += wall;

    bench_steer(&world->headings[actor_index], &stepped, world->tiles);
}

static void *bench_worker_main(void *arg)
{
    struct bench_worker *worker = arg;
    while (bench_wall_ns() < worker->deadline_ns)
    {
        bench_apply_one(worker);
    }
    return nullptr;
}

// --------------------------------------------------------------------------
// flush thread
// --------------------------------------------------------------------------

/** One flush pass: snapshot and clear the dirty set under the bookkeeping
 *  lock, then per cell one book-locked epoch bump and one fabric publish
 *  outside it, then the subscription drain that keeps publish marks fresh
 *  (the gateway drains its subscription each tick). A stale-epoch publish
 *  (-KITH_EPERM) is counted and suppressed, matching the shipped flush's
 *  handling of a racing higher-epoch bump. The pass's counters fold under
 *  stats_lock in one section; a failed pass folds nothing and stops the
 *  thread (main reports the failure at the join). */
static void bench_flush_once(struct bench_flusher *flusher)
{
    struct bench_world *world = flusher->world;
    const uint64_t cpu0 = bench_thread_cpu_ns();
    struct bench_cell cells[BENCH_DIRTY_CAP];
    size_t count = 0u;
    uint64_t publishes = 0u;
    uint64_t eperm = 0u;

    bench_check(pthread_mutex_lock(&world->book), "pthread_mutex_lock(book)");
    const uint32_t gen = world->dirty.generation;
    for (size_t i = 0u; i < BENCH_DIRTY_CAP; ++i)
    {
        if (world->dirty.stamp[i] == gen)
        {
            cells[count++] = world->dirty.cell[i];
        }
    }
    world->dirty.generation = gen + 1u;
    bench_check(pthread_mutex_unlock(&world->book), "pthread_mutex_unlock(book)");

    for (size_t i = 0u; i < count; ++i)
    {
        bench_check(pthread_mutex_lock(&world->book), "pthread_mutex_lock(book)");
        const uint32_t epoch = bench_epoch_bump(&world->epochs, &cells[i]);
        bench_check(pthread_mutex_unlock(&world->book), "pthread_mutex_unlock(book)");

        const kith_fabric_cell_key_t key = {
            .zone = world->zone,
            .cell_x = cells[i].x,
            .cell_y = cells[i].y,
            .cell_z = cells[i].z,
            .lod = 0u,
        };
        uint64_t seq = 0u;
        const int rc = kith_fabric_publish(world->fabric, &key, epoch, &seq);
        if (rc == 0)
        {
            publishes += 1u;
        }
        else if (rc == kith_error_return(KITH_EPERM))
        {
            eperm += 1u;
        }
        else
        {
            flusher->failed = rc;
            flusher->failed_what = "kith_fabric_publish";
            return;
        }
    }

    kith_fabric_cell_product_t products[BENCH_DRAIN_MAX];
    size_t drained = 0u;
    const int drc =
        kith_fabric_drain(world->fabric, world->subscription, products, BENCH_DRAIN_MAX, &drained);
    if (drc != 0)
    {
        flusher->failed = drc;
        flusher->failed_what = "kith_fabric_drain";
        return;
    }
    const uint64_t cpu = bench_thread_cpu_ns() - cpu0;
    bench_check(pthread_mutex_lock(&flusher->stats_lock), "pthread_mutex_lock(stats)");
    flusher->stats.fabric_publishes += publishes;
    flusher->stats.eperm_suppressed += eperm;
    flusher->stats.drain_products += (uint64_t)drained;
    flusher->stats.cells_drained += (uint64_t)count;
    flusher->stats.flush_count += 1u;
    flusher->stats.cpu_ns += cpu;
    bench_check(pthread_mutex_unlock(&flusher->stats_lock), "pthread_mutex_unlock(stats)");
}

static void *bench_flush_main(void *arg)
{
    struct bench_flusher *flusher = arg;
    const struct timespec tick = {.tv_sec = 0, .tv_nsec = (long)BENCH_TICK_MS * 1000000L};
    while (!atomic_load_explicit(&flusher->stop, memory_order_relaxed))
    {
        // An interrupted sleep is a shortened wait; the cadence carries no
        // deadline contract.
        if (nanosleep(&tick, nullptr) != 0 && errno != EINTR)
        {
            flusher->failed = 1;
            flusher->failed_what = "nanosleep";
            return nullptr;
        }
        if (atomic_load_explicit(&flusher->stop, memory_order_relaxed))
        {
            return nullptr;
        }
        bench_flush_once(flusher);
        if (flusher->failed != 0)
        {
            return nullptr;
        }
    }
    return nullptr;
}

// --------------------------------------------------------------------------
// setup
// --------------------------------------------------------------------------

/** Build the stack: sim handle, tile2d model with the reference tuning and
 *  the loaded behavior grid, fabric borrowing the sim, one subscription
 *  covering every world cell (so publishes do their real subscription
 *  marking), the actor population spawned at the origin like the shipped
 *  spawn path, and the bind mirror (session s binds actor s, the
 *  one-session-per-actor deployment shape). */
/** Create the sim stack: sim handle, tile2d model with the reference
 *  tuning and the loaded behavior grid, fabric borrowing the sim, and one
 *  subscription covering every world cell (so publishes do their real
 *  subscription marking). */
static void bench_stack_create(struct bench_world *world, const struct bench_params *params)
{
    bench_check(kith_sim_create(nullptr, nullptr, &world->sim), "kith_sim_create");
    kith_sim_config_t cfg = {0};
    cfg.size = (uint32_t)sizeof(cfg);
    cfg.abi_version = KITH_ABI_VERSION;
    cfg.base_speed = 48u;
    cfg.run_speed = 96u;
    cfg.accel = 128u;
    cfg.decel = 160u;
    cfg.move_eps = 0.25f;
    cfg.collision_radius = 1u;
    bench_check(kith_sim_create_model(world->sim, "tile2d", &cfg, nullptr, &world->model),
                "create tile2d");
    bench_check(kith_sim_model_load_behavior(world->model, params->behavior_path), "load_behavior");

    kith_fabric_params_t fp = {0};
    fp.size = (uint32_t)sizeof(fp);
    fp.abi_version = KITH_ABI_VERSION;
    bench_check(kith_fabric_create(&fp, world->sim, nullptr, &world->fabric), "kith_fabric_create");
    bench_check(kith_fabric_create_subscription(world->fabric, &world->subscription),
                "kith_fabric_create_subscription");
    for (uint32_t cx = 0u; cx < params->tiles; ++cx)
    {
        for (uint32_t cy = 0u; cy < params->tiles; ++cy)
        {
            const kith_fabric_cell_key_t key = {
                .zone = params->zone,
                .cell_x = (int32_t)cx,
                .cell_y = (int32_t)cy,
                .cell_z = 0,
                .lod = 0u,
            };
            bench_check(kith_fabric_subscription_add(world->subscription, &key),
                        "subscription_add");
        }
    }
}

/** Allocate and initialize the population: the actor table at the origin,
 *  the per-actor cell and patrol-heading tables, and the bind mirror
 *  (session s binds actor s, the one-session-per-actor deployment shape). */
static void bench_population_create(struct bench_world *world, const struct bench_params *params)
{
    world->actor_table = calloc((size_t)params->actors, sizeof(*world->actor_table));
    world->actor_cells = calloc((size_t)params->actors, sizeof(*world->actor_cells));
    world->headings = calloc((size_t)params->actors, sizeof(*world->headings));
    if (world->actor_table == nullptr || world->actor_cells == nullptr ||
        world->headings == nullptr)
    {
        bench_die("population table allocation");
    }
    uint64_t bind_slots = 16u;
    while (bind_slots < params->actors * 2u)
    {
        bind_slots <<= 1;
    }
    world->bind.session = calloc((size_t)bind_slots, sizeof(*world->bind.session));
    world->bind.actor = calloc((size_t)bind_slots, sizeof(*world->bind.actor));
    world->bind.mask = bind_slots - 1u;
    if (world->bind.session == nullptr || world->bind.actor == nullptr)
    {
        bench_die("bind map allocation");
    }
    for (uint64_t s = 1u; s <= params->actors; ++s)
    {
        uint64_t i = s & world->bind.mask;
        while (world->bind.session[i] != 0u)
        {
            i = (i + 1u) & world->bind.mask;
        }
        world->bind.session[i] = s;
        world->bind.actor[i] = s;
    }
    for (uint64_t id = 1u; id <= params->actors; ++id)
    {
        kith_sim_actor_t *actor = &world->actor_table[id - 1u];
        actor->id = id;
        world->actor_cells[id - 1u] = (struct bench_cell){0, 0, 0};
        struct bench_heading *heading = &world->headings[id - 1u];
        atomic_init(&heading->x, (id % 2u != 0u) ? BENCH_MOVE_MAGNITUDE : -BENCH_MOVE_MAGNITUDE);
        atomic_init(&heading->y,
                    ((id >> 1) % 2u != 0u) ? BENCH_MOVE_MAGNITUDE : -BENCH_MOVE_MAGNITUDE);
    }
    for (size_t s = 0u; s < BENCH_STRIPE_COUNT; ++s)
    {
        bench_check(pthread_mutex_init(&world->stripes[s].mutex, nullptr), "mutex_init(stripe)");
    }
    bench_check(pthread_mutex_init(&world->book, nullptr), "mutex_init(book)");
    world->dirty.generation = 1u;
}

/** Spawn publishes: the shipped spawn path publishes the fresh actor's
 *  initial state into its position-derived cell (here, the origin cell). */
static void bench_population_publish(struct bench_world *world, const struct bench_params *params)
{
    for (uint64_t id = 1u; id <= params->actors; ++id)
    {
        const kith_sim_artifact_key_t key = {
            .zone = params->zone,
            .cell_x = 0,
            .cell_y = 0,
            .cell_z = 0,
            .lod = 0u,
        };
        bench_check(
            kith_sim_publish_artifact(world->sim, &key, &world->actor_table[id - 1u], nullptr),
            "spawn publish");
    }
}

static struct bench_world *bench_world_setup(const struct bench_params *params)
{
    struct bench_world *world = calloc(1u, sizeof(*world));
    if (world == nullptr)
    {
        bench_die("calloc(bench_world)");
    }
    world->zone = params->zone;
    world->tiles = params->tiles;
    world->actors = params->actors;
    bench_stack_create(world, params);
    bench_population_create(world, params);
    bench_population_publish(world, params);
    return world;
}

static void bench_world_teardown(struct bench_world *world)
{
    if (world == nullptr)
    {
        return;
    }
    kith_fabric_subscription_destroy(world->subscription);
    kith_fabric_destroy(world->fabric);
    kith_sim_model_destroy(world->model);
    kith_sim_destroy(world->sim);
    free(world->bind.session);
    free(world->bind.actor);
    free(world->actor_table);
    free(world->actor_cells);
    free(world->headings);
    for (size_t s = 0u; s < BENCH_STRIPE_COUNT; ++s)
    {
        (void)pthread_mutex_destroy(&world->stripes[s].mutex);
    }
    (void)pthread_mutex_destroy(&world->book);
    free(world);
}

// --------------------------------------------------------------------------
// window runner
// --------------------------------------------------------------------------

/** Spawn N always-backlogged applying threads for one window; the warmup
 *  call discards tallies, timed windows keep theirs. Rings keep their last
 *  BENCH_RING_CAP samples across the config pass for aggregate percentiles. */
static double bench_run_window(struct bench_worker *workers,
                               pthread_t *threads,
                               uint32_t n_threads,
                               uint64_t deadline_ns)
{
    struct timespec t0;
    struct timespec t1;
    bench_check(clock_gettime(CLOCK_MONOTONIC, &t0), "clock_gettime");
    for (uint32_t i = 0u; i < n_threads; ++i)
    {
        workers[i].deadline_ns = deadline_ns;
        workers[i].failed = 0;
        workers[i].failed_what = nullptr;
        bench_check(pthread_create(&threads[i], nullptr, bench_worker_main, &workers[i]),
                    "pthread_create(worker)");
    }
    for (uint32_t i = 0u; i < n_threads; ++i)
    {
        bench_check(pthread_join(threads[i], nullptr), "pthread_join(worker)");
        if (workers[i].failed != 0)
        {
            (void)fprintf(stderr, "c_apply_bench: worker %u: %s\n", i, workers[i].failed_what);
            exit(1);
        }
    }
    bench_check(clock_gettime(CLOCK_MONOTONIC, &t1), "clock_gettime");
    const double begin = (double)t0.tv_sec + (double)t0.tv_nsec * 1e-9;
    const double end = (double)t1.tv_sec + (double)t1.tv_nsec * 1e-9;
    return end - begin;
}

static void bench_reset_workers(struct bench_worker *workers,
                                uint32_t n_threads,
                                uint32_t dt_ms,
                                struct bench_world *world,
                                uint32_t seed)
{
    for (uint32_t i = 0u; i < n_threads; ++i)
    {
        workers[i].world = world;
        workers[i].dt_ms = dt_ms;
        workers[i].deadline_ns = 0u;
        workers[i].rng = (uint64_t)seed + ((uint64_t)i << 24);
        workers[i].input_tick = 1u;
        workers[i].tally.applies = 0u;
        workers[i].tally.cpu_ns = 0u;
        workers[i].tally.wall_ns = 0u;
        workers[i].failed = 0;
        workers[i].failed_what = nullptr;
    }
}

/** Reset window-scoped tallies between timed windows; the rings keep their
 *  overwrite-oldest tail across timed windows so the aggregate percentiles
 *  span the whole config. */
static void bench_reset_window_tallies(struct bench_worker *workers, uint32_t n_threads)
{
    for (uint32_t i = 0u; i < n_threads; ++i)
    {
        workers[i].tally.applies = 0u;
        workers[i].tally.cpu_ns = 0u;
        workers[i].tally.wall_ns = 0u;
        workers[i].input_tick = 1u;
    }
}

// --------------------------------------------------------------------------
// config pass
// --------------------------------------------------------------------------

/** Pool one worker ring (cpu when @p which is 0, wall when 1) into a scratch
 *  array, sort it, and return the nearest-rank percentile in microseconds.
 *  The ring holds each worker's last BENCH_RING_CAP samples; a full ring
 *  contributes its whole capacity (the same sample set percentiles need). */
static double bench_ring_percentile(
    struct bench_worker *workers, uint32_t n_threads, uint64_t *scratch, int which, double fraction)
{
    uint64_t len = 0u;
    for (uint32_t i = 0u; i < n_threads; ++i)
    {
        const uint64_t have = workers[i].rings.count < (uint64_t)BENCH_RING_CAP
                                  ? workers[i].rings.count
                                  : (uint64_t)BENCH_RING_CAP;
        const uint64_t *ring = (which == 0u) ? workers[i].rings.cpu : workers[i].rings.wall;
        memcpy(scratch + len, ring, (size_t)have * sizeof(uint64_t));
        len += have;
    }
    qsort(scratch, (size_t)len, sizeof(uint64_t), bench_cmp_u64);
    return bench_percentile_us(scratch, len, fraction);
}

/** One config's pooled apply-thread totals plus whether the flush thread
 *  ran in the pass (the flush deltas fold only when it did). */
struct bench_totals
{
    uint64_t cpu_ns;
    uint64_t wall_ns;
    bool flush_ran;
};

/** Fill one aggregate's pooled means, ring percentiles, and flush-thread
 *  deltas (the folded headline total; tick-OFF passes fold to the
 *  transaction cost alone). */
static void bench_fill_aggregate(struct bench_aggregate *agg,
                                 struct bench_worker *workers,
                                 uint32_t n_threads,
                                 uint64_t *scratch,
                                 const struct bench_flush_stats *before,
                                 const struct bench_flush_stats *after,
                                 const struct bench_totals *totals)
{
    const uint64_t total_cpu_ns = totals->cpu_ns;
    const uint64_t total_wall_ns = totals->wall_ns;
    agg->applies_per_s = agg->elapsed_s > 0.0 ? (double)agg->applies_total / agg->elapsed_s : 0.0;
    const uint64_t denom = agg->applies_total > 0u ? agg->applies_total : 1u;
    agg->cpu_mean_us = (double)total_cpu_ns / (double)denom / 1000.0;
    agg->cpu_p50_us = bench_ring_percentile(workers, n_threads, scratch, 0, 0.50);
    agg->cpu_p95_us = bench_ring_percentile(workers, n_threads, scratch, 0, 0.95);
    agg->cpu_p99_us = bench_ring_percentile(workers, n_threads, scratch, 0, 0.99);
    agg->wall_mean_us = (double)total_wall_ns / (double)denom / 1000.0;
    agg->wall_p50_us = bench_ring_percentile(workers, n_threads, scratch, 1, 0.50);
    agg->wall_p95_us = bench_ring_percentile(workers, n_threads, scratch, 1, 0.95);
    agg->wall_p99_us = bench_ring_percentile(workers, n_threads, scratch, 1, 0.99);
    if (!totals->flush_ran)
    {
        // No flush thread in this pass: the folded headline is the
        // transaction cost alone.
        agg->total_cpu_us_per_apply = agg->cpu_mean_us;
        return;
    }
    agg->flush_count = after->flush_count - before->flush_count;
    agg->fabric_publishes = after->fabric_publishes - before->fabric_publishes;
    agg->eperm_suppressed = after->eperm_suppressed - before->eperm_suppressed;
    agg->dirty_cells_drained = after->cells_drained - before->cells_drained;
    agg->drain_products = after->drain_products - before->drain_products;
    const double flush_cpu_ns = (double)(after->cpu_ns - before->cpu_ns);
    agg->flush_total_cpu_s = flush_cpu_ns * 1e-9;
    agg->flush_cpu_us_per_apply = flush_cpu_ns / 1000.0 / (double)denom;
    agg->total_cpu_us_per_apply = ((double)total_cpu_ns + flush_cpu_ns) / (double)denom / 1000.0;
}

/** One config pass (one thread count, one tick mode): warmup + timed
 *  windows, pooled tallies, aggregate percentiles from the rings' tails,
 *  and the flush-thread deltas when the tick loop is enabled. */
static struct bench_worker *bench_workers_alloc(uint32_t n_threads, pthread_t **out_threads)
{
    struct bench_worker *workers = calloc((size_t)n_threads, sizeof(*workers));
    pthread_t *threads = calloc((size_t)n_threads, sizeof(*threads));
    if (workers == nullptr || threads == nullptr)
    {
        bench_die("config worker allocation");
    }
    for (uint32_t i = 0u; i < n_threads; ++i)
    {
        workers[i].rings.cpu = malloc((size_t)BENCH_RING_CAP * sizeof(uint64_t));
        workers[i].rings.wall = malloc((size_t)BENCH_RING_CAP * sizeof(uint64_t));
        workers[i].records = calloc(BENCH_RING_CAP, sizeof(*workers[i].records));
        if (workers[i].rings.cpu == nullptr || workers[i].rings.wall == nullptr ||
            workers[i].records == nullptr)
        {
            bench_die("worker array allocation");
        }
    }
    out_threads[0] = threads;
    return workers;
}

static void bench_workers_free(struct bench_worker *workers, pthread_t *threads, uint32_t n_threads)
{
    for (uint32_t i = 0u; i < n_threads; ++i)
    {
        free(workers[i].rings.cpu);
        free(workers[i].rings.wall);
        free(workers[i].records);
    }
    free(workers);
    free(threads);
}

/** Copy the flush counters under their lock; main reads the deltas at
 *  config boundaries so the report's publish-per-flush bounds compare
 *  snapshot-consistent counter sets. */
static struct bench_flush_stats bench_flush_stats_snapshot(struct bench_flusher *flusher)
{
    bench_check(pthread_mutex_lock(&flusher->stats_lock), "pthread_mutex_lock(stats)");
    const struct bench_flush_stats stats = flusher->stats;
    bench_check(pthread_mutex_unlock(&flusher->stats_lock), "pthread_mutex_unlock(stats)");
    return stats;
}

/** One config pass (one thread count, one tick mode): warmup + timed
 *  windows, pooled tallies, aggregate percentiles from the rings' tails,
 *  and the flush-thread deltas when the tick loop is enabled. */
static struct bench_aggregate bench_run_config(struct bench_world *world,
                                               struct bench_flusher *flusher,
                                               const struct bench_params *params,
                                               uint32_t n_threads,
                                               struct bench_row *rows,
                                               uint32_t row_base,
                                               uint32_t row_capacity)
{
    pthread_t *threads = nullptr;
    struct bench_worker *workers = bench_workers_alloc(n_threads, &threads);
    uint64_t *scratch = malloc((size_t)n_threads * BENCH_RING_CAP * sizeof(uint64_t));
    if (scratch == nullptr)
    {
        bench_die("percentile scratch allocation");
    }

    struct bench_flush_stats before = {0};
    struct bench_flush_stats after = {0};
    if (flusher != nullptr)
    {
        before = bench_flush_stats_snapshot(flusher);
    }
    bench_reset_workers(workers, n_threads, params->dt_ms, world, params->seed);
    (void)bench_run_window(
        workers, threads, n_threads, bench_wall_ns() + (uint64_t)(params->warmup_s * 1e9));

    struct bench_aggregate agg = {0};
    agg.threads = n_threads;
    agg.tick_enabled = params->tick;
    uint64_t total_cpu_ns = 0u;
    uint64_t total_wall_ns = 0u;
    for (uint32_t w = 0u; w < params->windows && row_base + w < row_capacity; ++w)
    {
        bench_reset_window_tallies(workers, n_threads);
        const double elapsed = bench_run_window(
            workers, threads, n_threads, bench_wall_ns() + (uint64_t)(params->window_s * 1e9));
        struct bench_row *row = &rows[row_base + w];
        row->threads = n_threads;
        row->tick = params->tick;
        row->window = w + 1u;
        row->elapsed_s = elapsed;
        agg.elapsed_s += elapsed;
        for (uint32_t i = 0u; i < n_threads; ++i)
        {
            agg.applies_total += workers[i].tally.applies;
            total_cpu_ns += workers[i].tally.cpu_ns;
            total_wall_ns += workers[i].tally.wall_ns;
            row->applies += workers[i].tally.applies;
            row->cpu_mean_us += (double)workers[i].tally.cpu_ns / 1000.0;
            row->wall_mean_us += (double)workers[i].tally.wall_ns / 1000.0;
        }
        if (row->applies > 0u)
        {
            row->cpu_mean_us /= (double)row->applies;
            row->wall_mean_us /= (double)row->applies;
        }
    }
    for (uint32_t i = 0u; i < n_threads; ++i)
    {
        agg.records_appended += workers[i].record_count;
    }

    if (flusher != nullptr)
    {
        after = bench_flush_stats_snapshot(flusher);
    }
    const struct bench_totals totals = {
        .cpu_ns = total_cpu_ns,
        .wall_ns = total_wall_ns,
        .flush_ran = flusher != nullptr,
    };
    bench_fill_aggregate(&agg, workers, n_threads, scratch, &before, &after, &totals);
    free(scratch);
    bench_workers_free(workers, threads, n_threads);
    return agg;
}

// --------------------------------------------------------------------------
// stdout report
// --------------------------------------------------------------------------

/** Render the aggregate table and window rows to stdout as line records the
 *  Python wrapper parses into the JSON report (AGG/WIN/DONE markers). */
static void bench_print_report(const struct bench_aggregate *aggs,
                               uint32_t agg_count,
                               const struct bench_row *rows,
                               uint32_t row_count)
{
    (void)printf("c_apply_bench\n");
    for (uint32_t i = 0u; i < agg_count; ++i)
    {
        const struct bench_aggregate *agg = &aggs[i];
        (void)printf("AGG %u %u %llu %.4f %.2f %.2f %.2f %.2f %.2f %.2f %.2f %.2f %.2f %.2f "
                     "%.2f %.4f %llu %llu %llu %llu %llu %llu\n",
                     agg->threads,
                     agg->tick_enabled,
                     (unsigned long long)agg->applies_total,
                     agg->elapsed_s,
                     agg->applies_per_s,
                     agg->cpu_mean_us,
                     agg->cpu_p50_us,
                     agg->cpu_p95_us,
                     agg->cpu_p99_us,
                     agg->wall_mean_us,
                     agg->wall_p50_us,
                     agg->wall_p95_us,
                     agg->wall_p99_us,
                     agg->total_cpu_us_per_apply,
                     agg->flush_cpu_us_per_apply,
                     agg->flush_total_cpu_s,
                     (unsigned long long)agg->flush_count,
                     (unsigned long long)agg->fabric_publishes,
                     (unsigned long long)agg->eperm_suppressed,
                     (unsigned long long)agg->dirty_cells_drained,
                     (unsigned long long)agg->drain_products,
                     (unsigned long long)agg->records_appended);
    }
    for (uint32_t i = 0u; i < row_count; ++i)
    {
        const struct bench_row *row = &rows[i];
        (void)printf("WIN %u %u %u %llu %.4f %.2f %.2f\n",
                     row->threads,
                     row->tick,
                     row->window,
                     (unsigned long long)row->applies,
                     row->elapsed_s,
                     row->cpu_mean_us,
                     row->wall_mean_us);
    }
    (void)printf("DONE\n");
    (void)fflush(stdout);
}

// --------------------------------------------------------------------------
// arguments
// --------------------------------------------------------------------------

static uint64_t bench_arg_u64(const char *text, const char *flag)
{
    char *end = nullptr;
    const unsigned long long value = strtoull(text, &end, 10);
    if (text == nullptr || *text == '\0' || end == text || *end != '\0')
    {
        (void)fprintf(stderr, "c_apply_bench: invalid integer for %s: %s\n", flag, text);
        exit(1);
    }
    return (uint64_t)value;
}

static double bench_arg_f64(const char *text, const char *flag)
{
    char *end = nullptr;
    const double value = strtod(text, &end);
    if (text == nullptr || *text == '\0' || end == text || *end != '\0')
    {
        (void)fprintf(stderr, "c_apply_bench: invalid number for %s: %s\n", flag, text);
        exit(1);
    }
    return value;
}

/** Parse the comma-separated thread sweep into out_counts. */
static void bench_parse_threads(const char *cursor, uint32_t *out_counts, uint32_t *out_n)
{
    while (*cursor != '\0' && out_n[0] < 16u)
    {
        char *end = nullptr;
        const unsigned long value = strtoul(cursor, &end, 10);
        if (end == cursor || value == 0u || value > BENCH_MAX_THREADS)
        {
            bench_die("invalid --threads list");
        }
        out_counts[(*out_n)++] = (uint32_t)value;
        cursor = (*end == ',') ? end + 1 : end;
    }
}

/** Set one numeric parameter flag; returns false for an unrecognized flag. */
static bool bench_set_flag(struct bench_params *params, const char *flag, const char *value)
{
    if (strcmp(flag, "--actors") == 0)
    {
        params->actors = bench_arg_u64(value, flag);
    }
    else if (strcmp(flag, "--windows") == 0)
    {
        const uint64_t windows = bench_arg_u64(value, flag);
        if (windows == 0u || windows > BENCH_MAX_WINDOWS)
        {
            bench_die("--windows out of range");
        }
        params->windows = (uint32_t)windows;
    }
    else if (strcmp(flag, "--dt-ms") == 0)
    {
        params->dt_ms = (uint32_t)bench_arg_u64(value, flag);
    }
    else if (strcmp(flag, "--seed") == 0)
    {
        params->seed = (uint32_t)bench_arg_u64(value, flag);
    }
    else if (strcmp(flag, "--zone") == 0)
    {
        params->zone = (uint32_t)bench_arg_u64(value, flag);
    }
    else if (strcmp(flag, "--tiles") == 0)
    {
        params->tiles = (uint32_t)bench_arg_u64(value, flag);
    }
    else
    {
        return false;
    }
    return true;
}

/** Parse the driver's arguments into params and the thread sweep. */
static void bench_parse_args(int argc,
                             char **argv,
                             struct bench_params *params,
                             uint32_t *thread_counts,
                             uint32_t *n_thread_configs)
{
    params->actors = 1000u;
    params->zone = 1u;
    params->tiles = 8u;
    params->dt_ms = 50u;
    params->windows = 5u;
    params->seed = 12345u;
    params->window_s = 1.0;
    params->warmup_s = 1.0;
    params->tick = 1u;
    for (int i = 1; i < argc; ++i)
    {
        const char *flag = argv[i];
        const bool has_value = i + 1 < argc;
        if (strcmp(flag, "--behavior") == 0 && has_value)
        {
            params->behavior_path = argv[++i];
        }
        else if (strcmp(flag, "--threads") == 0 && has_value)
        {
            bench_parse_threads(argv[++i], thread_counts, n_thread_configs);
        }
        else if (strcmp(flag, "--window-s") == 0 && has_value)
        {
            params->window_s = bench_arg_f64(argv[++i], flag);
        }
        else if (strcmp(flag, "--warmup-s") == 0 && has_value)
        {
            params->warmup_s = bench_arg_f64(argv[++i], flag);
        }
        else if (has_value && bench_set_flag(params, flag, argv[i + 1]))
        {
            ++i;
        }
        else
        {
            (void)fprintf(stderr,
                          "usage: c_apply_bench --behavior PATH [--actors N] [--threads a,b,c] "
                          "[--windows N] [--window-s F] [--warmup-s F] [--dt-ms N] [--seed N] "
                          "[--zone N] [--tiles N]\n");
            exit(1);
        }
    }
    if (params->behavior_path == nullptr || n_thread_configs[0] == 0u || params->windows == 0u)
    {
        bench_die("missing --behavior or --threads");
    }
}

int main(int argc, char **argv)
{
    struct bench_params params = {0};
    uint32_t thread_counts[16] = {0};
    uint32_t n_thread_configs = 0u;
    bench_parse_args(argc, argv, &params, thread_counts, &n_thread_configs);

    struct bench_world *world = bench_world_setup(&params);
    struct bench_flusher flusher = {0};
    atomic_init(&flusher.stop, false);
    flusher.world = world;
    bench_check(pthread_mutex_init(&flusher.stats_lock, nullptr), "mutex_init(stats)");
    bench_check(pthread_create(&flusher.thread, nullptr, bench_flush_main, &flusher),
                "pthread_create(flush)");

    // Two passes: tick ON (the headline, flush thread live) then tick OFF
    // (transaction-only isolation). The driver prints AGG/WIN/DONE line
    // records on stdout; the Python wrapper parses them, records the run
    // envelope, and writes the JSON report.
    struct bench_aggregate aggregates[2u * 16u];
    uint32_t agg_count = 0u;
    uint32_t row_count = 0u;
    const uint32_t row_capacity = 2u * 16u * BENCH_MAX_WINDOWS;
    struct bench_row *rows = calloc((size_t)row_capacity, sizeof(*rows));
    if (rows == nullptr)
    {
        bench_die("row table allocation");
    }
    for (uint32_t mode = 0u; mode < 2u; ++mode)
    {
        if (mode == 1u)
        {
            atomic_store_explicit(&flusher.stop, true, memory_order_relaxed);
            bench_check(pthread_join(flusher.thread, nullptr), "pthread_join(flusher)");
            if (flusher.failed != 0)
            {
                (void)fprintf(stderr, "c_apply_bench: flush: %s\n", flusher.failed_what);
                exit(1);
            }
            (void)pthread_mutex_destroy(&flusher.stats_lock);
        }
        params.tick = (mode == 0u) ? 1u : 0u;
        for (uint32_t t = 0u; t < n_thread_configs; ++t)
        {
            aggregates[agg_count++] = bench_run_config(world,
                                                       params.tick != 0u ? &flusher : nullptr,
                                                       &params,
                                                       thread_counts[t],
                                                       rows,
                                                       row_count,
                                                       row_capacity);
            row_count += params.windows;
        }
    }
    bench_print_report(aggregates, agg_count, rows, row_count);
    bench_world_teardown(world);
    free(rows);
    return 0;
}
