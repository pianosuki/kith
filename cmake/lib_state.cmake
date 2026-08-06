# libkith_state — Redis-backed asynchronous state store shared library.
#
# Layer-2 shared-plane target. The state store wraps a non-blocking Redis
# connection via hiredis async and integrates with the reactor for
# event-driven I/O. Operations (set, get, del, exists) are submitted
# asynchronously; results arrive via caller-supplied callbacks invoked
# from the reactor event loop.
#
# Keys and values are opaque byte buffers with explicit lengths, not
# null-terminated strings. An optional key prefix provides namespace
# isolation.
#
# The version script (src/state/kith_state.map) is generated from the
# library's public header by tools/gen_symbols.py and committed alongside
# the header change that produced it.

find_package(PkgConfig QUIET)
pkg_check_modules(hiredis REQUIRED IMPORTED_TARGET hiredis)

set(KITH_STATE_SOURCES
    src/state/state.c
)

add_library(kith_state SHARED ${KITH_STATE_SOURCES})

target_include_directories(kith_state PUBLIC
    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)
target_include_directories(kith_state PRIVATE
    ${PROJECT_SOURCE_DIR}/src
)

target_link_libraries(kith_state PRIVATE
    kith_reactor
    PkgConfig::hiredis
)

# SONAME + version script.
kith_apply_abi(kith_state
    MAP_FILE ${PROJECT_SOURCE_DIR}/src/state/kith_state.map
)

# Install the library and its public header.
include(GNUInstallDirs)
install(TARGETS kith_state
    EXPORT kithTargets
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})
install(FILES
    ${PROJECT_SOURCE_DIR}/include/kith/state/state.h
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/kith/state)
