# libkith_server — composition root shared library.
#
# Layer-3 Composition target. The server module is the single artifact a
# game links against: it wires every plane handle (sim, fabric, gateway,
# coord, control, and the shared infrastructure they borrow) in dependency
# order, drives them through the reactor's event loop, and owns their
# lifecycles. It carries no game-specific concepts; game logic plugs into
# the plane contracts via the handler table, sim model registry, and query
# registry.
#
# The library links every layer-2 plane library it calls directly. Plane
# libraries carry their own transitive dependencies as PRIVATE links, so
# the server does not link util, state, db, aoi, or client directly: those
# are reached through the plane that owns them (sim owns aoi; fabric owns
# state and db; the planes share util/logger/metrics via their own links).
#
# The control plane is conditionally linked: when CONTROL_PLANE_ENABLED is
# ON the server links kith_control and compiles with
# KITH_CONTROL_PLANE_ENABLED=1, and the wiring registers control routes;
# when OFF the server carries zero control-plane code (the source guards
# control usage behind #ifdef KITH_CONTROL_PLANE_ENABLED).
#
# The version script (src/server/kith_server.map) is generated from the
# library's public header by tools/gen_symbols.py and committed alongside
# the header change that produced it.

set(KITH_SERVER_SOURCES
    src/server/server.c
    src/server/wire.c
    src/server/wiring.c
)

add_library(kith_server SHARED ${KITH_SERVER_SOURCES})

target_include_directories(kith_server PUBLIC
    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)
target_include_directories(kith_server PRIVATE
    ${PROJECT_SOURCE_DIR}/src
)

# strict C23 mode does not declare clock_gettime/CLOCK_MONOTONIC without a
# POSIX feature macro; the write-drain bracket in wire.c reads the clock the
# same way its sibling plane libraries do.
target_compile_definitions(kith_server PRIVATE _GNU_SOURCE)

target_link_libraries(kith_server PRIVATE
    kith_sim
    kith_fabric
    kith_gateway
    kith_coord
    kith_reactor
    kith_worker
    kith_net
    kith_proto
    kith_config
    kith_logger
    kith_metrics
    kith_util
)

# The control plane is an optional layer-2 library; link it and expose the
# guard macro only when its target exists (CONTROL_PLANE_ENABLED=ON).
if(TARGET kith_control)
    target_link_libraries(kith_server PRIVATE kith_control)
    target_compile_definitions(kith_server PRIVATE KITH_CONTROL_PLANE_ENABLED=1)
endif()

# SONAME + version script.
kith_apply_abi(kith_server
    MAP_FILE ${PROJECT_SOURCE_DIR}/src/server/kith_server.map
)

# Install the library and its public header.
include(GNUInstallDirs)
install(TARGETS kith_server
    EXPORT kithTargets
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})
install(FILES
    ${PROJECT_SOURCE_DIR}/include/kith/server/server.h
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/kith/server)
