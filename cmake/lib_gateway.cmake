# libkith_gateway — client sockets, session lifecycle, shared cell cache,
# relevance composer, and bounded delivery shared library.
#
# Layer-2 Gateway-plane target. The gateway module owns the session table
# (connection to session map), the shared cell cache (refcounted per-cell
# snapshots sourced from the fabric), the per-subscriber relevance composer
# (bounded scored view sets), the handler registration table (message type
# to callback map), and the delivery path (encodes and enqueues the
# already-bounded send set on each session's connection). It borrows the
# net, fabric, and proto handles from the composition root; it holds no
# authoritative actor state and does not rescan raw zone state each tick.
#
# The handler registration table is serialized by a per-handle pthread
# mutex so concurrent register and dispatch calls do not interleave; the
# rest of the gateway is driven from one thread (the reactor thread) and
# is not synchronized. Threads::Threads provides pthread.
#
# The version script (src/gateway/kith_gateway.map) is generated from the
# library's public header by tools/gen_symbols.py and committed alongside
# the header change that produced it.

find_package(Threads REQUIRED)

set(KITH_GATEWAY_SOURCES
    src/gateway/gateway.c
    src/gateway/handler.c
    src/gateway/broadcast.c
    src/gateway/session/session.c
    src/gateway/cache/cache.c
    src/gateway/view/view.c
    src/gateway/delivery/delivery.c
    src/gateway/delivery/executor.c
    src/gateway/delivery/strategy.c
    src/gateway/delivery/strategy_full.c
    src/gateway/delivery/strategy_tiered.c
)

add_library(kith_gateway SHARED ${KITH_GATEWAY_SOURCES})

target_include_directories(kith_gateway PUBLIC
    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)
target_include_directories(kith_gateway PRIVATE
    ${PROJECT_SOURCE_DIR}/src
)

target_link_libraries(kith_gateway PRIVATE
    kith_fabric
    kith_net
    kith_proto
    kith_worker
    kith_util
    Threads::Threads
)

# The per-phase timing counters read CLOCK_MONOTONIC via clock_gettime, which
# strict C23 mode does not declare without a POSIX feature macro. _GNU_SOURCE
# matches the reactor and control targets (the other clock_gettime callers).
target_compile_definitions(kith_gateway PRIVATE _GNU_SOURCE)

# SONAME + version script.
kith_apply_abi(kith_gateway
    MAP_FILE ${PROJECT_SOURCE_DIR}/src/gateway/kith_gateway.map
)

# Install the library and its public header.
include(GNUInstallDirs)
install(TARGETS kith_gateway
    EXPORT kithTargets
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})
install(FILES
    ${PROJECT_SOURCE_DIR}/include/kith/gateway/gateway.h
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/kith/gateway)
