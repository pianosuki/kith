cmake_minimum_required(VERSION 4.4)

# The project version is the single source of truth that flows to every
# consumer channel: the library SONAMEs (kith_apply_abi reads
# PROJECT_VERSION), the pkg-config .pc file (Version:), the CMake package
# version file (write_basic_package_version), and the find_package(kith
# VERSION ...) check a downstream project issues. It tracks the release
# target the release infrastructure in this cluster produces.
project(kith VERSION 1.0.0 LANGUAGES C)

set(CMAKE_C_STANDARD 23)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_EXTENSIONS OFF)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(CMAKE_C_VISIBILITY_PRESET hidden)

# GNUInstallDirs provides CMAKE_INSTALL_LIBDIR / INCLUDEDIR / etc. Used by
# the per-library install(TARGETS EXPORT kithTargets) calls in
# cmake/lib_*.cmake and by the find_package / pkg-config consumer block at
# the bottom of this file; included here so the lib_*.cmake fragments pick
# up the variables without each re-including it (the local re-includes are
# idempotent and document each fragment's dependency).
include(GNUInstallDirs)

# Framework-wide conventions: warnings, hardening, visibility, sanitizers,
# abi helpers, linker/LTO/reproducible-build settings.
# framework.cmake is the single source of truth and includes the rest.
include(cmake/framework.cmake)

# Per-module shared-library targets. Each lib_<module>.cmake fragment
# defines one layer-2 SHARED library and its install rules; framework.cmake
# supplies the kith_apply_abi helper they all use.
include(cmake/lib_util.cmake)
include(cmake/lib_config.cmake)
include(cmake/lib_logger.cmake)
include(cmake/lib_metrics.cmake)
include(cmake/lib_proto.cmake)
include(cmake/lib_net.cmake)
include(cmake/lib_reactor.cmake)
include(cmake/lib_worker.cmake)
include(cmake/lib_state.cmake)
include(cmake/lib_db.cmake)
include(cmake/lib_client.cmake)
include(cmake/lib_sim.cmake)
include(cmake/lib_fabric.cmake)
include(cmake/lib_aoi.cmake)
include(cmake/lib_gateway.cmake)
include(cmake/lib_coord.cmake)
# The control plane is an optional layer-2 library. When
# CONTROL_PLANE_ENABLED is OFF the library is not built and contributes
# zero code to the build (no source-level #ifdef guards; the library is
# simply absent from the build).
option(CONTROL_PLANE_ENABLED "Build the control plane library (HTTP/WS introspection)" ON)
if(CONTROL_PLANE_ENABLED)
    include(cmake/lib_control.cmake)
endif()
# Composition root. The server is the single layer-3 library a game links
# against; it wires every plane library above and must be included after
# them (and after the optional control block) so their targets exist.
include(cmake/lib_server.cmake)

# Per-checker build targets (check-layout-consistency, check-clang-tidy, ...,
# check-all). Each runs the corresponding tools/*.py checker (or clang-tidy /
# clang-format) and fails the build on a violation. The format/verify split
# targets (`format`, the single fix entry point, plus the verify-only
# check-ruff-format / check-ruff / check-trivial-fixers targets that check-all
# depends on) come from the same dev toolchain: both fragments drive the
# tools/*.py checkers, the scripts/ helpers, and git ls-files, and both
# fail at use when one of their tools is missing. The full checkout ships
# tools/ and scripts/ together; a source distribution carries only the
# build inputs, so the fragments are included only when tools/ is present.
if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/tools")
    include(cmake/checks.cmake)
    include(cmake/format.cmake)
endif()

# C unit tests. enable_testing() must appear at the top level so ctest
# discovers tests registered by the tests/c subdirectory. The optional
# subtrees below exist only in a full checkout; a source distribution ships
# the build inputs alone, so each is composed only when its CMakeLists is
# present.
enable_testing()
if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/tests/c/CMakeLists.txt")
    add_subdirectory(tests/c)
endif()

# libFuzzer targets. Built only when KITH_ENABLE_FUZZ is set (the fuzz
# preset); the subdirectory hard-fails on non-Clang compilers because
# libFuzzer ships with Clang alone.
if(KITH_ENABLE_FUZZ)
    add_subdirectory(tests/fuzz)
endif()

# Examples. The C boots link against the composition root (kith_server) so
# the example executables exercise the public server surface end-to-end; the
# Python examples are plain modules run by pytest or `python -m`.
if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/examples/CMakeLists.txt")
    add_subdirectory(examples)
endif()

# --- Downstream consumer story: find_package(kith) + pkg-config -----------
# Every layer-2 (and the layer-3 composition root) library above installed
# with `EXPORT kithTargets`; this block assembles that export set into a
# CMake package a downstream finds with `find_package(kith 1.0.0 REQUIRED)`,
# and a pkg-config .pc file a hand-written Makefile or autotools consumer
# queries with `pkg-config --cflags --libs kith`. The version flowing into
# both channels is PROJECT_VERSION (set above), so the SONAME, the pkg-config
# Version, the find_package version check, and the kith_apply_abi SOVERSION
# all read from one source.
include(GNUInstallDirs)
include(CMakePackageConfigHelpers)

install(EXPORT kithTargets
    FILE kithTargets.cmake
    NAMESPACE kith::
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/kith)

configure_package_config_file(
    ${CMAKE_CURRENT_SOURCE_DIR}/cmake/kithConfig.cmake.in
    ${CMAKE_CURRENT_BINARY_DIR}/cmake/kithConfig.cmake
    INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/kith)

write_basic_package_version_file(
    ${CMAKE_CURRENT_BINARY_DIR}/cmake/kithConfigVersion.cmake
    VERSION ${PROJECT_VERSION}
    COMPATIBILITY SameMajorVersion)

install(FILES
    ${CMAKE_CURRENT_BINARY_DIR}/cmake/kithConfig.cmake
    ${CMAKE_CURRENT_BINARY_DIR}/cmake/kithConfigVersion.cmake
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/kith)

# pkg-config .pc file. The template is processed with @-only substitution
# (the .pc format treats ${} as its own variable expansion), then installed
# to <prefix>/lib/pkgconfig/kith.pc where `pkg-config --modversion kith`
# finds it.
configure_file(
    ${CMAKE_CURRENT_SOURCE_DIR}/cmake/kith.pc.in
    ${CMAKE_CURRENT_BINARY_DIR}/cmake/kith.pc
    @ONLY)
install(FILES
    ${CMAKE_CURRENT_BINARY_DIR}/cmake/kith.pc
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/pkgconfig)
