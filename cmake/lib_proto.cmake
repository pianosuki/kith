# libkith_proto — wire codec + message-type registry shared library.
#
# Layer-1 infrastructure target. The proto library owns a runtime registry of
# message types (a name to numeric id map) and a length-prefixed binary frame
# codec that carries an optional 8-byte big-endian correlation-ID trailer
# (enabled by KITH_PROTO_FLAG_CORRELATION in the frame header flags). The codec
# frames opaque payload blobs into a caller-owned buffer on encode and parses
# the leading frame from a caller-owned buffer on decode; it does not own a
# read buffer or perform any I/O. The framing state machine (partial reads,
# ring buffers, backpressure) and the frame allocation pool are transport
# concerns owned by libkith_net.so. The handle is created by the composition
# root and passed by pointer; there is no global accessor. Registry mutation
# and read are serialized by a per-handle mutex so concurrent calls do not
# interleave; encode is lock-free (it consults only the immutable max-payload
# bound and does not touch the registry).
#
# The version script (src/proto/kith_proto.map) is generated from the
# library's public headers by tools/gen_symbols.py and committed alongside the
# header change that produced it; re-run the generator when a new KITH_API
# symbol is added to include/kith/proto/.
#
# Private helpers under src/proto/ are reached via the src/ private include
# root (target_include_directories PRIVATE), so internal includes use the
# "proto/..." path the layer checker maps back to this module.

find_package(Threads REQUIRED)

add_library(kith_proto SHARED
    src/proto/proto.c
)

target_include_directories(kith_proto PUBLIC
    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)
target_include_directories(kith_proto PRIVATE
    ${PROJECT_SOURCE_DIR}/src
)

# The pthread mutex serializing registry access is exposed under
# _POSIX_C_SOURCE; strict C23 mode (CMAKE_C_EXTENSIONS OFF) does not define
# it implicitly.
target_compile_definitions(kith_proto PRIVATE _POSIX_C_SOURCE=200809L)

target_link_libraries(kith_proto PRIVATE Threads::Threads)

# SONAME + version script. kith_apply_abi (cmake/abi.cmake) sets VERSION
# (and SOVERSION = major) from PROJECT_VERSION and links the version script
# that locks the exported
# surface to the KITH_API symbols listed in the .map.
kith_apply_abi(kith_proto
    MAP_FILE ${PROJECT_SOURCE_DIR}/src/proto/kith_proto.map
)

# Install the library and the public header it exposes. Framework-level
# headers (api.h, version.h, types.h) are shared dependencies installed by
# lib_util.cmake; the proto module header goes to include/kith/proto/.
include(GNUInstallDirs)
install(TARGETS kith_proto
    EXPORT kithTargets
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})
install(FILES
    ${PROJECT_SOURCE_DIR}/include/kith/proto/proto.h
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/kith/proto)
