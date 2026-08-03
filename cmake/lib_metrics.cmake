# libkith_metrics — generic named metric registry shared library.
#
# Layer-0 foundation target. The metrics library owns a registry of named
# metric series (counters, gauges, histograms) identified by a metric name
# and a sorted label set, registered implicitly by observing them. It
# serializes the registry into two text payloads a caller ships: a
# Prometheus text-exposition payload and an OpenTelemetry OTLP/JSON
# payload. The library performs no network I/O; the transport that pushes
# either payload belongs to the control plane. The handle is created by
# the composition root and passed by pointer; there is no global accessor.
# Registry ops and serialization reads are serialized by a per-handle
# mutex so concurrent calls do not interleave.
#
# The version script (src/metrics/kith_metrics.map) is generated from the
# library's public headers by tools/gen_symbols.py and committed alongside
# the header change that produced it; re-run the generator when a new
# KITH_API symbol is added to include/kith/metrics/.
#
# Private headers under src/metrics/ are reached via the src/ private
# include root (target_include_directories PRIVATE), so internal includes
# use the "metrics/..." path the layer checker maps back to this module.

find_package(Threads REQUIRED)

add_library(kith_metrics SHARED
    src/metrics/metrics.c
    src/metrics/render/prometheus.c
    src/metrics/render/otlp.c
)

target_include_directories(kith_metrics PUBLIC
    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)
target_include_directories(kith_metrics PRIVATE
    ${PROJECT_SOURCE_DIR}/src
)

# POSIX 2008 clock_gettime (the OTLP export timestamps every data point)
# and the pthread mutex are exposed under _POSIX_C_SOURCE; strict C23 mode
# (CMAKE_C_EXTENSIONS OFF) does not define it implicitly.
target_compile_definitions(kith_metrics PRIVATE _POSIX_C_SOURCE=200809L)

target_link_libraries(kith_metrics PRIVATE Threads::Threads)

# SONAME + version script. kith_apply_abi (cmake/abi.cmake) sets VERSION
# (and SOVERSION = major) from PROJECT_VERSION and links the version script
# that locks the exported
# surface to the KITH_API symbols listed in the .map.
kith_apply_abi(kith_metrics
    MAP_FILE ${PROJECT_SOURCE_DIR}/src/metrics/kith_metrics.map
)

# Install the library and the public header it exposes. Framework-level
# headers (api.h, version.h, types.h) are shared dependencies installed by
# lib_util.cmake; the metrics module header goes to include/kith/metrics/.
include(GNUInstallDirs)
install(TARGETS kith_metrics
    EXPORT kithTargets
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})
install(FILES
    ${PROJECT_SOURCE_DIR}/include/kith/metrics/metrics.h
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/kith/metrics)
