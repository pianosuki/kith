# libkith_net — TCP transport + frame extraction shared library.
#
# Layer-1 infrastructure target (Gateway plane). The net library owns a TCP
# listener, a connection table, a per-transport frame pool, and the per-
# connection ring buffers + output queues that make up the transport layer.
# The read path drives libkith_proto's decoder against the ring buffer's
# contiguous region and delivers zero-copy proto frame views via a callback;
# the write path batches queued frames into writev. The transport does not
# own an event loop — it exposes the listener fd, each connection's fd, and a
# desired-events hint (kith_net_conn_events) so a reactor (libkith_reactor,
# a separate library driving epoll/io_uring/kqueue) can poll the fds and call
# the read/write/accept primitives on readiness. The proto handle is borrowed
# from the composition root for the transport's lifetime.
#
# The version script (src/net/kith_net.map) is generated from the library's
# public headers by tools/gen_symbols.py and committed alongside the header
# change that produced it; re-run the generator when a new KITH_API symbol is
# added to include/kith/net/.
#
# Private helpers under src/net/ are reached via the src/ private include
# root (target_include_directories PRIVATE), so internal includes use the
# "net/..." path the layer checker maps back to this module.

find_package(Threads REQUIRED)

add_library(kith_net SHARED
    src/net/net.c
    src/net/conn.c
    src/net/frame.c
    src/net/ringbuf.c
)

target_include_directories(kith_net PUBLIC
    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)
target_include_directories(kith_net PRIVATE
    ${PROJECT_SOURCE_DIR}/src
)

# GNU source: accept4, SOCK_NONBLOCK, SOCK_CLOEXEC, and IPV6_V6ONLY are Linux
# extensions not exposed under plain _POSIX_C_SOURCE; _GNU_SOURCE enables them
# and implies _POSIX_C_SOURCE/_DEFAULT_SOURCE. The clang-tidy system-include
# whitelist (ExtraArgs: -D_GNU_SOURCE) already matches, so the checker and the
# build agree on the feature surface.
target_compile_definitions(kith_net PRIVATE _GNU_SOURCE)

target_link_libraries(kith_net PRIVATE
    Threads::Threads
    kith_proto
)

# SONAME + version script. kith_apply_abi (cmake/abi.cmake) sets VERSION
# (and SOVERSION = major) from PROJECT_VERSION and links the version script
# that locks the exported
# surface to the KITH_API symbols listed in the .map.
kith_apply_abi(kith_net
    MAP_FILE ${PROJECT_SOURCE_DIR}/src/net/kith_net.map
)

# Install the library and the public header it exposes. Framework-level
# headers (api.h, version.h, types.h) are shared dependencies installed by
# lib_util.cmake; the proto header is installed by lib_proto.cmake. The net
# module header goes to include/kith/net/.
include(GNUInstallDirs)
install(TARGETS kith_net
    EXPORT kithTargets
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})
install(FILES
    ${PROJECT_SOURCE_DIR}/include/kith/net/net.h
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/kith/net)
