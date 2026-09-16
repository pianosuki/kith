# kith

`kith` is a scalable server framework for real-time, stateful multiplayer
worlds.

[![license](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
![python](https://img.shields.io/badge/python-3.14%20%7C%20free--threaded-blue.svg)

A C23 core owns the performance-critical systems: transport, the reactor,
spatial indexing, the world stream fabric, and the simulation. Game logic
extends the core in Python. Simulation models that need more than Python
speed ship as C shared libraries against the same ABI. The core also stands
alone: it installs headers and CMake targets, and examples/minimal boots a
server with no Python at all.

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

## Quickstart: run the release

Tagged releases ship a binary wheel, the sdist, and SHA256 checksums on the
[releases page](https://github.com/pianosuki/kith/releases/latest). The
wheel bundles the compiled core; boot an embedded server:

```sh
python3 -m venv .venv && source .venv/bin/activate
pip install kith_fw-1.0.0-py3-none-manylinux_2_38_x86_64.whl
```

The PyPI distribution is `kith-fw`; the module you import is `kith`.

```python
from kith import Server

with Server(topology="embedded") as server:
    print(f"kith: gateway={server.listen_port}")
    server.serve()  # blocks; Ctrl-C stops it cleanly
```

The [getting started guide](docs/guides/getting_started.md) continues from
that boot: handlers, control routes, persistence, the two-instance cluster.

Prefer to see it move first? The released wheel ships an out-of-box visual
example — a living world with ambient actors, a graphical client, and one
command to play:

```sh
pip install "kith-fw[visual]"
kith-visual play crowd-in   # bots converge on your cell; watch the HUD
```

## Quickstart: build from source

```sh
git clone https://github.com/pianosuki/kith kith && cd kith
uv sync
source .venv/bin/activate
cmake --preset release && cmake --build build/release
python -m examples.free_movement.server  # prints: free_movement: gateway=… control=…
```

In a second terminal, drive the running server over its control plane:

```sh
curl -X POST http://127.0.0.1:<control>/spawn  # {"actor_id": 1}
curl http://127.0.0.1:<control>/query_state  # the actor's live state
```

`./scripts/verify.sh` is the same gate CI runs: lint, commits, build,
free-threaded. From a verified tree, [CONTRIBUTING.md](CONTRIBUTING.md)
is the contributor workflow.

## C consumers

From a built tree, install the headers and libraries:

```sh
cmake --install build/release --prefix /opt/kith
```

A downstream CMake project configures with the prefix on its search path
(`-DCMAKE_PREFIX_PATH=/opt/kith`) and links the imported targets —
`kith::server` is the composition root a game links against, and every plane
library is an imported target beside it:

```cmake
find_package(kith 1.0.0 REQUIRED)
target_link_libraries(my_game PRIVATE kith::server)
```

Hand-written Makefiles and autotools consume the pkg-config entry point:

```sh
PKG_CONFIG_PATH=/opt/kith/lib/pkgconfig pkg-config --cflags --libs kith
```

`tests/consumer/` is a minimal downstream project that exercises both
channels in CI.

## Requirements

- Linux, x86-64, glibc 2.38 or newer. `import kith` raises a load error on
  any other platform.
- Python 3.14+, standard or free-threaded builds.
- Wheel path: the bundled libraries link `liburing`, `libpq`, `libhiredis`
  at runtime; install them from the system package manager.
- Source path: Clang 22+ (or GCC 14+), clang-format-23 (the pinned formatter —
  `uv tool install clang-format==23.1.0`, then symlink `~/.local/bin/clang-format`
  as `clang-format-23`), CMake 4.4+, Ninja 1.13+, `uv`, `pre-commit`.

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

## Documentation

- API reference, by symbol: the
  [Doxygen reference](https://pianosuki.github.io/kith/) rendered from the
  public headers.
- Guides, by task: [getting started](docs/guides/getting_started.md),
  [Python extensions](docs/guides/writing_extensions.md),
  [C simulation models](docs/guides/writing_c_sim_models.md),
  [scaling gates](docs/guides/scaling_checklist.md),
  [agentic headless clients](docs/guides/agentic_headless_client.md),
  [the wire protocol](docs/guides/wire_protocol.md),
  [the publish choreography](docs/guides/publish_choreography.md),
  [operations](docs/guides/operations.md),
  [replay format](docs/guides/replay_format.md).
- Architecture, by contract: [planes](docs/architecture/planes.md),
  [layers](docs/architecture/layers.md),
  [topologies](docs/architecture/topologies.md),
  [tracing](docs/architecture/tracing.md),
  [performance budgets](docs/architecture/performance_budgets.md),
  [event schema](docs/event_schema.md), and the
  [decision records](docs/architecture/adr/).

## Contributing

[CONTRIBUTING.md](CONTRIBUTING.md) is the workflow; [AGENTS.md](AGENTS.md)
is the coding standard and review checklist. [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md)
governs participation; [SECURITY.md](SECURITY.md) takes private
vulnerability reports. General issues and questions are answered best-effort
by the single maintainer, with no response-time guarantee; vulnerability
reports carry their own channel and expectations in [SECURITY.md](SECURITY.md).

## License

Apache License, Version 2.0. See [LICENSE](LICENSE) for the full text.
