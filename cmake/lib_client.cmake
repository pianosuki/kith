# libkith_client — headless client engine shared library.
#
# Layer-2 target. The client module owns a bootstrap FSM (pluggable step
# table), an outbound frame ring queue, an event ring with inline-payload
# records, and keepalive state (ping/pong/RTT/reconnect). It is
# transport-agnostic: the caller feeds decoded proto frame views and pops
# encoded frames. The engine is single-threaded (no socket, no I/O, no
# pthread, no reactor). It borrows a proto handle (for encode) and
# optionally a logger handle.
#
# The version script (src/client/kith_client.map) is generated from the
# library's public header and committed alongside the header change that
# produced it.

set(KITH_CLIENT_SOURCES
    src/client/client.c
    src/client/bootstrap.c
    src/client/queue.c
)

add_library(kith_client SHARED ${KITH_CLIENT_SOURCES})

target_include_directories(kith_client PUBLIC
    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)
target_include_directories(kith_client PRIVATE
    ${PROJECT_SOURCE_DIR}/src
)

target_link_libraries(kith_client PRIVATE
    kith_proto
    kith_logger
)

target_compile_definitions(kith_client PRIVATE _GNU_SOURCE)

# SONAME + version script.
kith_apply_abi(kith_client
    MAP_FILE ${PROJECT_SOURCE_DIR}/src/client/kith_client.map
)

# Install the library and its public header.
include(GNUInstallDirs)
install(TARGETS kith_client
    EXPORT kithTargets
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})
install(FILES
    ${PROJECT_SOURCE_DIR}/include/kith/client/client.h
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/kith/client)
