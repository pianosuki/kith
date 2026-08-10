# libkith_fabric — append-only cell stream storage shared library.
#
# Layer-2 Fabric-plane target. The fabric module owns a per-zone
# append-only cell stream (16-shard open-addressed hash of cell products
# keyed by zone, cell_x, cell_y, cell_z, lod), subscription tables
# (per-gateway interest sets with pending-changed tracking), and tiered
# product rendering (full / reduced / crowd). It borrows a sim handle to
# query actor counts and artifacts during publish and snapshot, and fans
# out publishes to subscriptions whose interest set contains the changed
# cell. The composition root creates the handle, publishes cell products
# after each sim step, and gateways drain their subscriptions each tick.
#
# The library depends on the sim library (layer 2, same layer). The sim
# handle is borrowed (not owned). No pthread, no I/O, no global state.
#
# The version script (src/fabric/kith_fabric.map) is generated from the
# library's public header by tools/gen_symbols.py and committed alongside
# the header change that produced it.

set(KITH_FABRIC_SOURCES
    src/fabric/fabric.c
)

add_library(kith_fabric SHARED ${KITH_FABRIC_SOURCES})

target_include_directories(kith_fabric PUBLIC
    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)
target_include_directories(kith_fabric PRIVATE
    ${PROJECT_SOURCE_DIR}/src
)

target_link_libraries(kith_fabric PRIVATE
    kith_sim
)

# SONAME + version script.
kith_apply_abi(kith_fabric
    MAP_FILE ${PROJECT_SOURCE_DIR}/src/fabric/kith_fabric.map
)

# Install the library and its public header.
include(GNUInstallDirs)
install(TARGETS kith_fabric
    EXPORT kithTargets
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})
install(FILES
    ${PROJECT_SOURCE_DIR}/include/kith/fabric/fabric.h
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/kith/fabric)
