# libkith_logger — structured logger with a JSONL side-channel shared library.
#
# Layer-0 foundation target. The logger emits structured entries (a level, a
# short message, and caller-supplied key/value fields) to up to three sinks:
# a human-readable log file, an optional stdout mirror, and a JSONL
# side-channel file whose records follow the canonical NDJSON envelope
# (docs/event_schema.md) for control-plane streaming. The handle is created
# by the composition root and passed by pointer; there is no global accessor.
# Writes are serialized by a per-handle mutex so concurrent calls do not
# interleave.
#
# The version script (src/logger/kith_logger.map) is generated from the
# library's public headers by tools/gen_symbols.py and committed alongside
# the header change that produced it; re-run the generator when a new
# KITH_API symbol is added to include/kith/logger/.
#
# Private headers under src/logger/ are reached via the src/ private include
# root (target_include_directories PRIVATE), so internal includes use the
# "logger/..." path the layer checker maps back to this module.

find_package(Threads REQUIRED)

add_library(kith_logger SHARED
    src/logger/logger.c
    src/logger/structured/structured.c
)

target_include_directories(kith_logger PUBLIC
    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)
target_include_directories(kith_logger PRIVATE
    ${PROJECT_SOURCE_DIR}/src
)

# POSIX 2008 file I/O (open/write/close, O_CLOEXEC), clock_gettime/gmtime_r,
# and the pthread mutex are exposed under _POSIX_C_SOURCE; strict C23 mode
# (CMAKE_C_EXTENSIONS OFF) does not define it implicitly.
target_compile_definitions(kith_logger PRIVATE _POSIX_C_SOURCE=200809L)

target_link_libraries(kith_logger PRIVATE Threads::Threads)

# SONAME + version script. kith_apply_abi (cmake/abi.cmake) sets VERSION
# (and SOVERSION = major) from PROJECT_VERSION and links the version script
# that locks the exported
# surface to the KITH_API symbols listed in the .map.
kith_apply_abi(kith_logger
    MAP_FILE ${PROJECT_SOURCE_DIR}/src/logger/kith_logger.map
)

# Install the library and the public header it exposes. Framework-level
# headers (api.h, version.h, types.h) are shared dependencies installed by
# lib_util.cmake; the logger module header goes to include/kith/logger/.
include(GNUInstallDirs)
install(TARGETS kith_logger
    EXPORT kithTargets
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})
install(FILES
    ${PROJECT_SOURCE_DIR}/include/kith/logger/logger.h
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/kith/logger)
