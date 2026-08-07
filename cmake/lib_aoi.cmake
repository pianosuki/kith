# libkith_aoi — spatial query library shared library.
#
# Layer-2 shared-plane target. The aoi module owns a spatial hash index of
# generic 3D objects: a dual open-addressed hash keyed by object identifier
# (for lookup, update, and remove by id) and by cell coordinate (for spatial
# queries). Moving an object detaches it from its old cell and re-attaches
# it to the new one in O(1). Sphere and box queries scan only the cells
# intersecting the query region, expanded by the largest stored object
# radius so a center-bucketed object whose sphere reaches into the region
# is never missed.
#
# The library is a pure query tool: it returns the identifiers and positions
# of objects matching a spatial predicate; the caller composes delivery
# from the results. It depends on libc only (no pthread, no I/O, no sim or
# fabric handle). The composition root creates the handle, indexes objects
# as they enter the world, and queries near a location each tick.
#
# The version script (src/aoi/kith_aoi.map) is generated from the library's
# public header by tools/gen_symbols.py and committed alongside the
# header change that produced it.

set(KITH_AOI_SOURCES
    src/aoi/aoi.c
)

add_library(kith_aoi SHARED ${KITH_AOI_SOURCES})

target_include_directories(kith_aoi PUBLIC
    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)
target_include_directories(kith_aoi PRIVATE
    ${PROJECT_SOURCE_DIR}/src
)

# SONAME + version script.
kith_apply_abi(kith_aoi
    MAP_FILE ${PROJECT_SOURCE_DIR}/src/aoi/kith_aoi.map
)

# Install the library and its public header.
include(GNUInstallDirs)
install(TARGETS kith_aoi
    EXPORT kithTargets
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})
install(FILES
    ${PROJECT_SOURCE_DIR}/include/kith/aoi/aoi.h
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/kith/aoi)
