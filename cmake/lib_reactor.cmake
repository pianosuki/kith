# libkith_reactor — async event loop shared library.
#
# Layer-2 shared-plane target. The reactor polls file descriptors for
# readiness and dispatches callbacks; it also provides inter-thread task
# submission via a lock-free MPSC queue and a 256-slot timer wheel.
#
# Linux: io_uring backend via liburing (IORING_OP_POLL_ADD). Every other
# platform is unsupported: the backend file is not compiled and
# kith_reactor_create returns KITH_ENOSYS.
#
# The version script (src/reactor/kith_reactor.map) is generated from the
# library's public header by tools/gen_symbols.py and committed alongside
# the header change that produced it.

find_package(PkgConfig QUIET)
if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    pkg_check_modules(liburing IMPORTED_TARGET liburing>=2.7)
    if(NOT liburing_FOUND)
        message(FATAL_ERROR
            "liburing >= 2.7 is required for the io_uring reactor backend; "
            "found ${liburing_VERSION}. Install a current liburing "
            "(https://github.com/axboe/liburing/releases).")
    endif()
endif()

set(KITH_REACTOR_SOURCES
    src/reactor/reactor.c
)

if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    list(APPEND KITH_REACTOR_SOURCES src/reactor/reactor_uring.c)
endif()

add_library(kith_reactor SHARED ${KITH_REACTOR_SOURCES})

target_include_directories(kith_reactor PUBLIC
    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)
target_include_directories(kith_reactor PRIVATE
    ${PROJECT_SOURCE_DIR}/src
)

# Linux backend needs _GNU_SOURCE for eventfd.
if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    target_compile_definitions(kith_reactor PRIVATE _GNU_SOURCE)
    target_link_libraries(kith_reactor PRIVATE
        PkgConfig::liburing
    )
endif()

target_link_libraries(kith_reactor PRIVATE
    kith_util
)

# SONAME + version script.
kith_apply_abi(kith_reactor
    MAP_FILE ${PROJECT_SOURCE_DIR}/src/reactor/kith_reactor.map
)

# Install the library and its public header.
include(GNUInstallDirs)
install(TARGETS kith_reactor
    EXPORT kithTargets
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})
install(FILES
    ${PROJECT_SOURCE_DIR}/include/kith/reactor/reactor.h
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/kith/reactor)
