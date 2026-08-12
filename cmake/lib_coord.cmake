# libkith_coord — cell ownership, dual-authority split/merge, and the
# inter-node coordination bus shared library.
#
# Layer-2 Coord-plane target. The coord module owns the cell ownership
# table (a sharded open-addressed hash of authority overrides keyed by
# cell locator), the authority epoch counter (a stale-product guard
# bumped on each authority change), and the split/merge coordinator
# state (density entries and periodic threshold evaluation). The bus
# handle owns the loopback transport's inbound event queue, a refcounted
# zone subscription table, and a membership table with one member in the
# loopback transport (the local instance).
#
# The library depends on libc only (no pthread, no I/O, no sim or fabric
# link-time dependency). The coord borrows the bus at runtime within the
# same translation unit; the cell-key type is a header-only struct from
# the framework-level public headers, so no cross-library linkage is
# required. The composition root creates the coord and bus handles and
# drives the periodic evaluation from the reactor thread.
#
# The version script (src/coord/kith_coord.map) is generated from the
# library's public header by tools/gen_symbols.py and committed alongside
# the header change that produced it.

set(KITH_COORD_SOURCES
    src/coord/coord.c
)

add_library(kith_coord SHARED ${KITH_COORD_SOURCES})

target_include_directories(kith_coord PUBLIC
    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)
target_include_directories(kith_coord PRIVATE
    ${PROJECT_SOURCE_DIR}/src
)

# SONAME + version script.
kith_apply_abi(kith_coord
    MAP_FILE ${PROJECT_SOURCE_DIR}/src/coord/kith_coord.map
)

# Install the library and its public header.
include(GNUInstallDirs)
install(TARGETS kith_coord
    EXPORT kithTargets
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})
install(FILES
    ${PROJECT_SOURCE_DIR}/include/kith/coord/coord.h
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/kith/coord)
