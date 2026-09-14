# Writing C Sim Models

This guide adds a new simulation model in C: a struct that implements the
sim model vtable, registered by name on a `kith_sim_t` handle before it is
instantiated. The framework's pluggability boundary for the sim plane is the
vtable and the composition root that registers it, not dynamic library
loading. A model ships as a C source file compiled and linked into the
binary that owns the sim handle (the framework library itself, or a game
binary that links `libkith_sim`); the composition root calls
`kith_sim_register_model` and `kith_sim_create_model` directly.

The walkthrough mirrors the two built-in models, `tile2d` and `free2d`
(`src/sim/models/`), which are the canonical shape a custom model copies.
Both are registered automatically when a sim handle is created; a custom
model follows the same registration path from the composition root.

## What the vtable is

`kith_sim_model_vtable_t` (`include/kith/sim/sim.h`) is a size-versioned
struct that carries an `abi_version` field and five callbacks:

| Field | Called by | Contract |
|---|---|---|
| `size` | the runtime | `sizeof(kith_sim_model_vtable_t)` at the model's compile time; the runtime rejects an undersized struct with `-KITH_ESIZE`. |
| `abi_version` | the runtime | `KITH_ABI_VERSION` at the model's compile time; the runtime rejects a mismatched generation with `-KITH_EABIVER`. |
| `init` | `kith_sim_create_model` | Allocate and initialize the model state and the `kith_sim_model_t` handle itself through the allocator argument, from a `kith_sim_config_t`; return the handle through the out slot. Return `0` on success, a negative `kith_error` on failure. The runtime fills the handle's `vtable` and `allocator` fields after `init` returns; the implementation sets neither and reads `allocator` wherever it releases or grows model-owned memory. |
| `destroy` | `kith_sim_model_destroy` | Release all model-owned resources. NULL-safe. |
| `step` | `kith_sim_model_step` | Advance `count` actors by `dt_ms` milliseconds, modifying the array in place. |
| `apply_input` | `kith_sim_model_apply_input` | Apply one `kith_sim_input_t` to one actor (buffered until the next `step`). |
| `load_behavior` | `kith_sim_model_load_behavior` | Load a model-specific behavior grid from a file path. `NULL` clears any loaded behavior. A model with no behavior grid accepts the call and returns `0`. |
| `reserved[8]` | the runtime | Set to `NULL`. Future vtable extensions occupy these slots without breaking the struct layout. |

Every callback must be present; the runtime rejects a vtable with a missing
callback at registration with `-KITH_EABIVER`. The registry copies the
vtable and the name string on success, so the caller may free both after
`kith_sim_register_model` returns.

## The model: a 2D drift model

Add a model that applies a constant drift to every actor each tick, with no
collision grid and no behavior file. The shape copies `free2d` minus its
physics integration: a model is its own state struct, five callbacks, and a
storage vtable.

### 1. Define the model state

A model owns whatever state it needs. The opaque `kith_sim_model_t` the
runtime hands back is allocated by `init`; the model hangs its own state
off the `state` field of the internal struct. Keep the state plain-old-data
so `destroy` is a sequence of frees:

```c
#include "kith/sim/sim.h"
#include "kith/types.h"
#include "kith/version.h"

struct drift_state
{
    int64_t drift_x_fix;
    int64_t drift_y_fix;
};
```

Positions and velocities are Q16.16 fixed-point (`int64_t` with
`KITH_SIM_FIX_SHIFT` fractional bits). The model receives actors with
fixed-point positions and writes fixed-point positions back; integer
arithmetic is bit-identical across architectures, which is what makes a
model deterministic and replayable (`tools/replay.py`).

### 2. Implement `init`

`init` allocates the internal struct and the `kith_sim_model_t` the runtime
returns — both through the allocator argument — and converts the
caller-supplied `kith_sim_config_t` fields to fixed-point. A field left at
`0` selects the model's own default (the same per-field fallback the
built-in models use), so a caller passing a zeroed config gets a usable
model rather than a zero-speed one:

```c
static int drift_init(
    const kith_sim_config_t *cfg, const kith_allocator_t *alloc, kith_sim_model_t **out)
{
    *out = nullptr;
    struct drift_state *st = kith_alloc_zero(alloc, 1, sizeof(*st));
    if (!st)
    {
        return kith_error_return(KITH_ENOMEM);
    }
    /* One model unit per tick of drift; a NULL cfg reads as all-zero
       fields, so the default applies. */
    uint32_t drift_units = (cfg && cfg->base_speed) ? cfg->base_speed : 1u;
    st->drift_x_fix = (int64_t)drift_units << KITH_SIM_FIX_SHIFT;
    st->drift_y_fix = 0;

    kith_sim_model_t *model = kith_alloc_zero(alloc, 1, sizeof(*model));
    if (!model)
    {
        kith_free(alloc, st);
        return kith_error_return(KITH_ENOMEM);
    }
    model->state = st;
    *out = model;
    return 0;
}
```

The runtime fills the returned handle's `vtable` (from the registered
entry) and `allocator` (from the `kith_sim_create_model` argument) after
`init` returns; the implementation sets neither.

### 3. Implement `destroy`, `step`, `apply_input`, `load_behavior`

```c
static void drift_destroy(kith_sim_model_t *model)
{
    if (!model)
    {
        return;
    }
    kith_free(model->allocator, model->state);
    kith_free(model->allocator, model);
}

static int
drift_step(kith_sim_model_t *model, kith_sim_actor_t *actors, size_t count, uint32_t dt_ms)
{
    (void)dt_ms;
    if (!model)
    {
        return kith_error_return(KITH_EINVAL);
    }
    struct drift_state *st = model->state;
    for (size_t i = 0u; i < count; ++i)
    {
        actors[i].pos_x += st->drift_x_fix;
        actors[i].pos_y += st->drift_y_fix;
    }
    return 0;
}

static int drift_apply_input(kith_sim_model_t *model,
                             kith_sim_actor_t *actor,
                             const kith_sim_input_t *input)
{
    (void)model;
    if (!actor || !input)
    {
        return kith_error_return(KITH_EINVAL);
    }
    /* Drift ignores the move vector; record the tick and flags so the
       caller's input bookkeeping stays consistent. */
    actor->input_tick = input->input_tick;
    actor->flags = input->flags;
    return 0;
}

static int drift_load_behavior(kith_sim_model_t *model, const char *path)
{
    (void)model;
    (void)path;
    /* No behavior grid; accept the call and report success. Passing NULL
       clears any loaded behavior, which is a no-op here. */
    return 0;
}
```

A model that buffers pending inputs (the way `tile2d` and `free2d` do) holds
a pending-input map in its state and consumes it in `step`. The built-in
`sim_pending_map` helpers (`src/sim/sim_internal.h`) are available to models
compiled inside the framework library; a model compiled into a game binary
that only links the public headers implements its own buffering or reads
inputs inline. The helpers are internally synchronized for concurrent use
across different actor ids; same-actor ordering is the caller's (which input
a step consumes is a semantic choice, not a mechanical one), and `get`
returns a pointer into the map that stays valid only until that actor's next
`put` — the mutex does not extend to the pointer's later use.

### 4. Define the storage vtable

A single const instance of the vtable is the model's registration payload.
The `size` and `abi_version` fields are what the runtime checks first;

```c
static const kith_sim_model_vtable_t drift_vtable_storage = {
    .size = sizeof(kith_sim_model_vtable_t),
    .abi_version = KITH_ABI_VERSION,
    .init = drift_init,
    .destroy = drift_destroy,
    .step = drift_step,
    .apply_input = drift_apply_input,
    .load_behavior = drift_load_behavior,
    .reserved = {nullptr},
};
```

### 5. Register the model from the composition root

A custom model is registered by name on a `kith_sim_t` handle, then
instantiated by name. The handle's registry is not synchronized; registration
runs once at startup, before any `kith_sim_create_model` call:

```c
kith_sim_t *sim = nullptr;
int rc = kith_sim_create(nullptr, &sim);
/* ... check rc ... */

rc = kith_sim_register_model(sim, "drift2d", &drift_vtable_storage);
/* ... check rc: -KITH_EEXIST if the name is already registered ... */

kith_sim_model_t *model = nullptr;
rc = kith_sim_create_model(sim, "drift2d", nullptr, &model);
/* ... check rc ... */

/* Step the model each tick, then publish actors as cell artifacts. */
kith_sim_actor_t actors[1] = {0};
actors[0].id = 1;
kith_sim_model_step(model, actors, 1, 50);

kith_sim_artifact_key_t key = {
    .zone = 0, .cell_x = 0, .cell_y = 0, .cell_z = 0, .lod = 0,
};
kith_sim_publish_artifact(sim, &key, &actors[0], nullptr);

kith_sim_model_destroy(model);
kith_sim_destroy(sim);
```

The registry rejects a duplicate name with `-KITH_EEXIST`, an empty name or
NULL handle with `-KITH_EINVAL`, an undersized vtable with `-KITH_ESIZE`,
and an `abi_version` mismatch or a missing callback with `-KITH_EABIVER`
(`src/sim/model_registry.c`). `kith_sim_create_model` returns
`-KITH_ENOENT` for a name that was never registered.

## Building a model into the binary

A model's C source is compiled and linked into the binary that owns the sim
handle. There is no runtime `dlopen` of a third-party model shared library:
the pluggability point is the vtable plus the composition-root registration
call, not a dynamic load. Two landing sites:

- **Into the framework library.** Add the source to `KITH_SIM_SOURCES` in
  `cmake/lib_sim.cmake` alongside `src/sim/models/tile2d.c` and
  `src/sim/models/free2d.c`, and register it from
  `sim_registry_register_builtins` (`src/sim/model_registry.c`) so every
  sim handle auto-registers it. This is the path the built-in models take;
  it suits a model the framework ships.
- **Into a game binary.** Compile the model's source into the game's own
  target and call `kith_sim_register_model` from the game's startup before
  `kith_sim_create_model`. The model is reachable by name from any code that
  holds the sim handle, including the Python facade (below).

A model that lives in a separate shared library and is loaded with `dlopen`
is not a supported path: the framework's boundary is the vtable, and a
vtable carries C function pointers a dynamic loader cannot verify against
the runtime's `abi_version`. A model that needs to be loadable lands behind
an ADR that defines the loading contract (ADR-0032).

## Selecting a model from the Python facade

The Python facade exposes `Server.register_sim_model(name, config)`
(`python/kith/__init__.py`), which calls `kith_sim_create_model` to
instantiate a model the registry already knows. The built-in `tile2d` and
`free2d` are registered when the server's sim handle is created, so a
Python composition root selects one by name:

```python
from kith import Server, SimModelConfig

server = Server(...)
model = server.register_sim_model("tile2d", SimModelConfig())
```

A custom C model registered from C at startup is reachable from the Python
facade by the same name. The facade does not expose `kith_sim_register_model`
to Python: registering a vtable from Python is not meaningful, because a
vtable carries C function pointers, not Python-callable state. A model is
written in C and registered from C; the Python facade instantiates it by
name.

## Determinism and replay

Actor positions and velocities are Q16.16 fixed-point integers, so a model
that uses only integer arithmetic is bit-identical across runs and
architectures. `tools/replay.py` reads a recorded input stream, drives the
sim shared library through the same public ABI a composition root uses, and
prints a rolling FNV-1a hash of the live actor set. A second run on the same
record yields the same hash sequence; a hash that differs from a pinned
baseline flags a simulation-behavior regression without reasoning about the
internals.

A model that introduces floating-point state, threading, or any
non-deterministic ordering breaks replay. Keep the hot path integer-only.
Concurrency is layered: the artifact store and the built-in pending-input
map are internally synchronized, and the caller may drive a model's vtable
concurrently for different actors — but per-actor state (the actor struct)
and any model state beyond the built-in helpers are the caller's and the
model author's to serialize respectively: same-actor vtable calls must be
externally ordered, and a custom model that keeps shared mutable state
beyond the built-in helpers must synchronize it itself.

## Cell artifacts

After `step`, the caller publishes each actor's updated state as a cell
artifact keyed by `(zone, cell_x, cell_y, cell_z, lod, authority_epoch,
publish_seq)`. The cell coordinates are 3D-native (ADR-0016): a 2D model
sets `cell_z = 0`. The `lod` is a granularity tier the caller assigns. The
`authority_epoch` guards against stale messages after an authority
transition, and `publish_seq` is assigned by the store. The fabric module
borrows the sim handle to query artifacts for subscription-indexed fanout
(see `docs/architecture/planes.md`).

A model does not publish artifacts itself; it advances actor state, and the
composition root publishes. This keeps the sim plane's authority over actor
state and the fabric plane's authority over fanout separate.

## References

- `include/kith/sim/sim.h` — the public vtable, config, actor, and artifact
  contracts.
- `src/sim/models/tile2d.c`, `src/sim/models/free2d.c` — the built-in
  models, the canonical shape a custom model copies.
- `src/sim/model_registry.c` — the registry's validation paths
  (`-KITH_EEXIST`, `-KITH_ESIZE`, `-KITH_EABIVER`, `-KITH_ENOENT`).
- `tools/replay.py` — deterministic replay and hash regression.
- `docs/guides/writing_extensions.md` — the Python-side extension path (a
  new wire type and handler, no C).
- `docs/architecture/planes.md` — the sim and fabric plane contracts a
  model honors.
- ADR-0008 — opaque handles, size-versioned structs, version scripts, and
  the ABI-diff gate.
- ADR-0013 — pluggability is within planes, not across invariant-breaking
  topologies.
- ADR-0016 — 3D-native cell identity.
