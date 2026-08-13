# libkith_control — embedded HTTP/1.1 + WebSocket control plane shared
# library.
#
# Layer-2 Control-plane target. The control module owns a non-blocking TCP
# listener, a connection table with per-connection HTTP/1.1 parsers, a
# WebSocket frame codec, a route table, and a single-producer/single-consumer
# event bus. It borrows a reactor for file-descriptor readiness (no threads,
# no epoll) and drives accept/read/write from reactor callbacks. The
# library ships built-in handlers for /health, /metrics (Prometheus scrape),
# /events/stream (SSE), and /logs/stream (SSE). The composition root
# registers additional routes via kith_control_register_route.
#
# The library is zero-cost when the build-time CMake option
# CONTROL_PLANE_ENABLED is OFF: this fragment is not included and the
# library is not built.
#
# The version script (src/control/kith_control.map) is generated from the
# library's public header and committed alongside the header change that
# produced it.

set(KITH_CONTROL_SOURCES
    src/control/control.c
    src/control/conn.c
    src/control/http_parser.c
    src/control/event_bus.c
    src/control/router.c
    src/control/handlers.c
)

add_library(kith_control SHARED ${KITH_CONTROL_SOURCES})

target_include_directories(kith_control PUBLIC
    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)
target_include_directories(kith_control PRIVATE
    ${PROJECT_SOURCE_DIR}/src
)

target_link_libraries(kith_control PRIVATE
    kith_reactor
    kith_worker
    kith_logger
    kith_metrics
    kith_util
)

target_compile_definitions(kith_control PRIVATE _GNU_SOURCE)

# SONAME + version script.
kith_apply_abi(kith_control
    MAP_FILE ${PROJECT_SOURCE_DIR}/src/control/kith_control.map
)

# Install the library and its public header.
include(GNUInstallDirs)
install(TARGETS kith_control
    EXPORT kithTargets
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})
install(FILES
    ${PROJECT_SOURCE_DIR}/include/kith/control/control.h
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/kith/control)
