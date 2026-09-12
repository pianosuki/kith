# Replay Record Format

The binary replay-record format is the container the deterministic-simulation
contract (ADR-0014) uses to capture a live session and replay it later to the
same outcome: a versioned header, per-tick records carrying the tick's input
events, pinned world-state hash expectations, and pseudo-random generator
checkpoints. Producers are the recording call sites wired into an example
composition root (`examples/_common/replay_recorder.py`, enabled by the
embedded server's `--record PATH` flag); consumers are the modes of
`tools/replay.py` (`play`, `verify`, `diff`, `transcode`). Artifacts use the
`.krpl` extension.

Design goals, in order: byte-exact regeneration (no timestamps, no ambient
state — two recordings of the same input set are logically identical);
forward compatibility within a version (unknown record types skipped whole);
loud refusal of anything misparseable (wrong version, truncated payloads,
ordering violations) instead of silent corruption.

## Conventions

All multi-byte fields are **big-endian** (network order), matching the
framework's existing serialization idiom — the golden-hash packing and the
wire codecs already serialize big-endian. This is a convention of this
format, not an appeal to ADR-0007: that decision governs delivery-payload
encoding, not this artifact container. Signed fields are two's complement.
Offsets below are relative to the start of the enclosing structure.

## Header (16 bytes)

| Offset | Size | Type     | Field       | Value                                    |
|-------:|-----:|----------|-------------|------------------------------------------|
| 0      | 4    | bytes[4] | magic       | `KRPL` (0x4b 0x52 0x50 0x4c)             |
| 4      | 2    | u16      | version     | 1                                        |
| 6      | 2    | u16      | header_size | 16                                       |
| 8      | 4    | u32      | tick_hz     | tick rate of the recorded session, >= 1  |
| 12     | 4    | u32      | flags       | 0 (readers reject unknown bits)          |

`tick_hz` is authoritative for playback cadence; the replay tool's
`--tick-hz` overrides it only when passed explicitly. `header_size`
decouples record parsing from the header layout: readers parse records
from `header_size` onward and reject versions above the one they
support loudly (rule R1).

## Record framing (8 bytes)

Every record after the header begins with:

| Offset | Size | Type | Field      | Value                                     |
|-------:|-----:|------|------------|-------------------------------------------|
| 0      | 1    | u8   | type       | record type code (table below)            |
| 1      | 1    | u8   | flags      | 0 for all v1 types                        |
| 2      | 2    | u16  | reserved   | 0                                         |
| 4      | 4    | u32  | payload_len| payload byte count                        |

A reader that meets an unknown `type` skips the whole record using
`payload_len` — this is the within-version evolution mechanism (rule R2).
Unknown flag bits on a known type are a parse error. Payload lengths are
exact: trailing or missing bytes in a known payload are corruption (rule R3).

## Record types

| Code | Type           | Payload                                              |
|-----:|----------------|------------------------------------------------------|
| 0x01 | TICK           | tick number + closed-set event list                  |
| 0x02 | META           | UTF-8 JSON object (informational)                    |
| 0x03 | EXPECTED_HASH  | pinned rolling world-state hash                      |
| 0x04 | RNG_CHECKPOINT | generator snapshot bound to the preceding tick       |

### TICK (0x01)

| Offset | Size        | Type | Field      | Value                             |
|-------:|-------------|------|------------|-----------------------------------|
| 0      | 8           | u64  | tick       | server tick index (1-based)       |
| 8      | 2           | u16  | event_count| number of events that follow      |
| 10     | variable    | —    | events     | exactly `event_count` events      |

Events are encoded back-to-back with no padding. Each event begins with a
one-byte code; the v1 event set is **closed** (see versioning rules):

### Event: SPAWN (0x01, 61 bytes on wire)

| Offset | Size | Type | Field    | Value                                   |
|-------:|-----:|------|----------|-----------------------------------------|
| 0      | 1    | u8   | code     | 0x01                                    |
| 1      | 8    | u64  | actor_id | caller-chosen actor identity            |
| 9      | 24   | i64×3| pos      | spawn position, Q16.16 fixed point      |
| 33     | 24   | i64×3| vel      | spawn velocity, Q16.16 fixed point      |
| 57     | 4    | u32  | flags    | movement flags                          |

(60-byte body after the code byte.)

### Event: MOVE (0x02, 20 bytes on wire)

| Offset | Size | Type | Field      | Value                                     |
|-------:|-----:|------|------------|-------------------------------------------|
| 0      | 1    | u8   | code       | 0x02                                      |
| 1      | 8    | u64  | actor_id   | moved actor                               |
| 9      | 4    | u32  | input_tick | monotonic input tick for the actor        |
| 13     | 6    | i16×3| move       | normalized move components [-32767, 32767]|
| 19     | 1    | u8   | flags      | input flags                               |

(19-byte body after the code byte.)

### Event: DESPAWN (0x03, 9 bytes on wire)

| Offset | Size | Type | Field    | Value        |
|-------:|-----:|------|----------|--------------|
| 0      | 1    | u8   | code     | 0x03         |
| 1      | 8    | u64  | actor_id | removed actor|

An unknown event code inside a TICK is a parse error, not a skip: event
bodies are fixed-width with no length prefix, so a reader cannot resynchronize
without guessing (this is why the event set is closed rather than open).

### META (0x02)

One UTF-8 JSON object. Informational only — readers never derive behavior
from it; writers use it for provenance such as the model name and root seed.
Legal exactly once, before the first TICK.

### EXPECTED_HASH (0x03)

| Offset | Size | Type | Value                                                          |
|-------:|-----:|------|----------------------------------------------------------------|
| 0      | 8    | u64  | tick the expectation binds to                                  |
| 8      | 8    | u64  | expected FNV-1a 64 rolling hash of the live actor set          |

The hash folds the actor count followed by each actor's `(id, pos_x, pos_y,
pos_z, vel_x, vel_y, vel_z, input_tick, flags)` in ascending actor-id order,
big-endian two's-complement encodings — the same function `tools/replay.py`
prints as `tick=N hash=HEX` during `play`. Expectations make an artifact
self-verifying: `verify` re-simulates the record and compares, running with
a positive `--hash-every` cadence whose emitted ticks cover every pinned
tick — an expectation whose tick the replay never emits fails the
verification rather than passing unchecked. Under the determinism contract
(ADR-0014) the replayed hash is a pure function of the recorded inputs and
initial state, so an artifact embedding expectations is self-verifying
against that guarantee on any host, compiler, or worker count, with no
external reference data. At most one expectation per tick.

### RNG_CHECKPOINT (0x04)

| Offset | Size | Type  | Field      | Value                                        |
|-------:|-----:|-------|------------|----------------------------------------------|
| 0      | 8    | u64   | tick       | equals the preceding TICK's tick             |
| 8      | 8    | u64   | stream_key | stable stream identity chosen by the writer  |
| 16     | 4    | u32   | state_len  | 40                                           |
| 20     | 40   | bytes | state      | `kith_rng_state_t`, big-endian               |

The state payload is the derivation seed (`u64`) followed by the four
algorithm words (`u64` × 4) — the complete snapshot
(`kith_rng_state_save`) of one generator stream. A checkpoint must
immediately follow its own TICK record, and stream keys must ascend
strictly within a tick.

## Ordering law

Records appear in this order, and readers enforce every clause:

1. META (if present) precedes the first TICK.
2. TICK records have strictly ascending, unique tick numbers.
3. Within a TICK, events appear in canonical order: spawns before moves
   before despawns; ties break by actor id, then by input tick. Canonical
   order makes a concurrently-fed recording a pure function of the logical
   input set rather than of worker-thread arrival order.
4. Each RNG_CHECKPOINT immediately follows its own TICK; within one tick,
   checkpoints ascend by stream key.

## Versioning rules

- **R1** — `version` changes only when the header or framing changes
  structurally. Readers refuse newer versions loudly instead of misparsing.
- **R2** — layouts of known types are frozen within a version. Additive
  evolution adds a new *record* type code, which old readers skip whole.
  The v1 *event* set is closed because events cannot be skipped: a new
  event kind requires a `version` bump.
- **R3** — payloads of known types must match their declared length
  exactly; only unknown record types may be skipped via `payload_len`.

## Recording semantics

What producers write, and what readers should expect:

- **Idle-tick elision** — a tick whose input buffer was empty produces no
  records at all; its checkpoint is omitted with it. Replay fills the gaps
  by stepping the simulation once per tick from zero to the highest
  recorded tick and applying events at their recorded tick, so elided
  ticks cost nothing, and generator continuity survives the gap because
  generator advancement is a pure function of the step count. Elision
  keeps an idle session's artifact bounded instead of growing one empty
  record set per tick.
- **Tail discard** — events buffered when the recording ends belong to a
  tick that never completed, so the producer drops them instead of writing
  a partial TICK. A recording ends on a tick boundary.
- **Durability** — the producer flushes the artifact stream at every
  drained boundary, so an unclean process exit preserves the recording up
  to the last completed tick.
- **Teleport divergence** — the v1 event set has no encoding for absolute
  repositioning. A recorded session that teleports an actor diverges
  permanently from the teleport tick onward when replayed (every later
  move applies to a wrong base position); the artifact carries no marker
  of this, so games that teleport should not rely on replay fidelity
  across the teleport until the format gains an encoding (a `version`
  bump per R2).
- **Wiring today** — recording call sites exist in the embedded-topology
  example composition root (`--record`, optional `--record-seed` for
  per-tick checkpoints of a root-seeded generator under stream key 0);
  other topologies have no recorder wiring yet.

## Legacy text encoding

Artifacts written before the binary format exist as one event per line:

```
<tick> <SPAWN|MOVE|DESPAWN> <actor_id> <fields...>
```

Blank lines and `#` comments are ignored. Positions and velocities are raw
Q16.16 decimal integers; MOVE takes `input_tick move_x move_y move_z flags`.
The tools distinguish encodings by sniffing the leading four bytes against
the `KRPL` magic; the legacy reader stays supported for old artifacts, and
`transcode` rewrites a text record as a binary one (optionally embedding
expectations harvested from a prior `play` run as a `tick=N hash=HEX`
sidecar).
