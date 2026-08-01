# kith

`kith` is a scalable server framework for real-time, stateful multiplayer
worlds.

[![license](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
![python](https://img.shields.io/badge/python-3.14%20%7C%20free--threaded-blue.svg)

A C23 core owns the performance-critical systems: transport, the reactor,
spatial indexing, the world stream fabric, and the simulation. Game logic
extends the core in Python. Simulation models that need more than Python
speed ship as C shared libraries against the same ABI. The core also stands
alone: it installs headers and CMake targets with no Python at all.

The world is divided into cells that publish state once; gateways
subscribe to cells and compose a per-player view. The fabric carries that
stream. The same game code runs as a single embedded process or as a
distributed cluster; the topology is a constructor argument.

kith fits real-time, stateful worlds under high concurrency: movement,
presence, spatial queries, world state streamed to every connected client.
A turn-based or request/response game gains little from it; state moves on
a simulation cadence.

The framework runs on Linux only; the reactor is io_uring. It ships
without accounts, authentication, matchmaking, or billing; those services
belong to the game.

## Capabilities

- Five-plane fabric: Sim, Fabric, Gateway, Coord, Control, with the
  data-flow contracts between planes enforced in the build.
- Pluggable components within planes: sim models, database queries, wire
  types, control routes, and delivery presets register behind their plane's
  contract.
- Switchable topologies: embedded single-process, distributed
  multi-instance, one build.
- Deterministic simulation with replay files and state-hash coverage.
- Free-threaded Python: handlers dispatch on a worker pool off the reactor;
  runs on standard and 3.14t interpreters.
- Additive C ABI: opaque types, size-versioned structs, an ABI-diff gate.
- Hardened builds by default: PIE, RELRO, stack canaries, CFI, NX.
- Agentic testing: headless clients drive setup, act, assert, diagnose.
- One observability contract: structured logging, metrics, tracing.

## Building from source

```sh
git clone https://github.com/pianosuki/kith kith && cd kith
uv sync
source .venv/bin/activate
cmake --preset release && cmake --build build/release
```

## Requirements

- Linux, x86-64.
- Python 3.14+, standard or free-threaded builds.
- Clang 22+ (or GCC 14+), clang-format-22 (the pinned formatter —
  `uv tool install clang-format==22.1.8`, then symlink `~/.local/bin/clang-format`
  as `clang-format-22`), CMake 4.4+, Ninja 1.13+, `uv`, `pre-commit`.

## Stability

Releases follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
from 1.0.0. The public C ABI evolves additively: public types stay opaque,
parameter structs carry size and ABI-version fields, and an ABI-diff gate
guards every header change
([ADR-0008](docs/architecture/adr/0008-opaque-types-stable-abi.md)). The
Python API is the ctypes binding of that ABI, generated from the same
headers, with no separate version line: a change that breaks the C ABI
breaks Python with it, and both ride the project version.

Within 1.x nothing is deprecated: evolution is additive, and breaking
changes wait for the next major version. The decision records under
`docs/architecture/adr/` are frozen at 1.0.0 and evolve only by supersession
or explicit amendment, not by external contribution.

## Contributing

[AGENTS.md](AGENTS.md) is the coding standard and review checklist.

## License

Apache License, Version 2.0. See [LICENSE](LICENSE) for the full text.
