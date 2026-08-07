# libkith_sim — pluggable simulation shared library.
#
# Layer-2 Simulation-plane target. The sim module owns a model registry
# (name to vtable map with automatic tile2d/free2d registration), an
# artifact store (16-shard per-cell immutable actor-state snapshots with a
# dense actor array and O(1) open-addressed actor/cell hashes), and the
# two built-in Q16.16 fixed-point movement models. The composition root
# creates the handle, registers model implementations by name, instantiates
# models, and steps them each tick; after stepping, the caller publishes
# updated actor positions as cell artifacts. The reactor thread reads the
# store (the gateway cache refresh reaches it via the fabric snapshot
# path) while worker threads mutate it (publish / remove from Python
# handlers), so a single store-wide pthread mutex serializes every shard
# access and the grow/rehash path; the shard index remains as a compartment
# hint for future per-shard locking.
#
# The library depends on libc, the C23 standard library, and pthread
# (Threads::Threads). _POSIX_C_SOURCE exposes the pthread mutex prototypes;
# strict C23 mode does not define it implicitly.
#
# The version script (src/sim/kith_sim.map) is generated from the library's
# public header by tools/gen_symbols.py and committed alongside the header
# change that produced it.

set(KITH_SIM_SOURCES
    src/sim/sim.c
    src/sim/model_registry.c
    src/sim/artifact_store.c
    src/sim/models/tile2d.c
    src/sim/models/free2d.c
)

add_library(kith_sim SHARED ${KITH_SIM_SOURCES})

target_include_directories(kith_sim PUBLIC
    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)
target_include_directories(kith_sim PRIVATE
    ${PROJECT_SOURCE_DIR}/src
)

target_compile_definitions(kith_sim PRIVATE _POSIX_C_SOURCE=200809L)

find_package(Threads REQUIRED)
target_link_libraries(kith_sim PRIVATE Threads::Threads)

# SONAME + version script.
kith_apply_abi(kith_sim
    MAP_FILE ${PROJECT_SOURCE_DIR}/src/sim/kith_sim.map
)

# Install the library and its public header.
include(GNUInstallDirs)
install(TARGETS kith_sim
    EXPORT kithTargets
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})
install(FILES
    ${PROJECT_SOURCE_DIR}/include/kith/sim/sim.h
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/kith/sim)
