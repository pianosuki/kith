# Framework-wide build conventions.
#
# This module is the single source of truth for cross-cutting build settings
# and the include order of the other cmake/ modules. The top-level
# CMakeLists.txt includes this file for those settings and the per-module
# lib_*.cmake fragments separately; framework.cmake pulls in the modules
# below in the order that makes the preset logic correct:
#
#   warnings.cmake   - warning flags
#   visibility.cmake - hidden-visibility defaults
#   sanitizers.cmake - sanitizer/coverage/fuzz flags; sets KITH_ENABLE_FORTIFY
#                      OFF under asan/tsan (runs BEFORE hardening.cmake)
#   hardening.cmake  - security hardening; reads KITH_ENABLE_FORTIFY
#   fpenv.cmake      - floating-point environment for deterministic math
#   abi.cmake        - version scripts, SONAME, ABI baselines
#
# After the includes it configures the linker (lld default / mold opt-in),
# LTO (release only, via CMake IPO), reproducible-build prefix maps + rpath,
# and compressed debug info (release/relwithdebinfo).

# --- include order: sanitizers must precede hardening --------------------
include(${CMAKE_CURRENT_LIST_DIR}/warnings.cmake)
include(${CMAKE_CURRENT_LIST_DIR}/visibility.cmake)
include(${CMAKE_CURRENT_LIST_DIR}/sanitizers.cmake)
include(${CMAKE_CURRENT_LIST_DIR}/hardening.cmake)
include(${CMAKE_CURRENT_LIST_DIR}/fpenv.cmake)
include(${CMAKE_CURRENT_LIST_DIR}/abi.cmake)

if(NOT CMAKE_C_COMPILER_ID MATCHES "GNU|Clang")
    return()
endif()

# --- Compiler version floors ---------------------------------------------
# Clang 22+ and GCC 14+ are the minimum supported C compilers. Both ship a
# complete C23 implementation and the io_uring / FORTIFY / CFI support the
# framework builds on; an older toolchain fails configure here rather than
# surfacing as a cryptic compile or link error deeper in the build.
if(CMAKE_C_COMPILER_ID STREQUAL "Clang"
   AND CMAKE_C_COMPILER_VERSION VERSION_LESS 22)
    message(FATAL_ERROR
        "Clang 22+ is required; found Clang ${CMAKE_C_COMPILER_VERSION}.")
endif()
if(CMAKE_C_COMPILER_ID STREQUAL "GNU"
   AND CMAKE_C_COMPILER_VERSION VERSION_LESS 14)
    message(FATAL_ERROR
        "GCC 14+ is required; found GCC ${CMAKE_C_COMPILER_VERSION}.")
endif()

# --- Linker: lld default, mold opt-in -------------------------------------
find_program(LLD_EXECUTABLE NAMES lld.lld ld.lld)
if(LLD_EXECUTABLE)
    add_link_options(-fuse-ld=lld)
else()
    message(STATUS "lld not found; falling back to the system linker")
endif()
option(KITH_USE_MOLD "Use the mold linker (fastest; requires mold installed)" OFF)
if(KITH_USE_MOLD)
    add_link_options(-fuse-ld=mold)
endif()

# --- LTO: release only, via CMake's built-in IPO support -----------------
# The release preset sets CMAKE_INTERPROCEDURAL_OPTIMIZATION=ON; CMake then
# emits -flto=thin (Clang) / -flto (GCC) on every target. Verify the compiler
# supports IPO so an unsupported toolchain fails configure rather than link.
# The probe links with the linker selection above, not the compiler's
# default: check_ipo_supported forwards no linker flags to its try-compile,
# so it otherwise judges a default-linker toolchain the targets never use.
if(CMAKE_INTERPROCEDURAL_OPTIMIZATION)
    set(_kith_ipo_probe "${CMAKE_BINARY_DIR}/CMakeTmp/kith-ipo-probe")
    file(REMOVE_RECURSE "${_kith_ipo_probe}")
    file(MAKE_DIRECTORY "${_kith_ipo_probe}")
    file(WRITE "${_kith_ipo_probe}/main.c" "int main(void) { return 0; }\n")
    file(WRITE "${_kith_ipo_probe}/CMakeLists.txt"
        "cmake_minimum_required(VERSION ${CMAKE_VERSION})\n"
        "project(kith_ipo_probe LANGUAGES C)\n"
        "add_executable(ipo_probe main.c)\n")
    set(_kith_ipo_ld "")
    if(KITH_USE_MOLD)
        set(_kith_ipo_ld "-fuse-ld=mold")
    elseif(LLD_EXECUTABLE)
        set(_kith_ipo_ld "-fuse-ld=lld")
    endif()
    try_compile(_kith_ipo_supported
        PROJECT kith_ipo_probe
        SOURCE_DIR "${_kith_ipo_probe}"
        BINARY_DIR "${_kith_ipo_probe}/build"
        NO_CACHE
        CMAKE_FLAGS
            "-DCMAKE_INTERPROCEDURAL_OPTIMIZATION=ON"
            "-DCMAKE_EXE_LINKER_FLAGS=${CMAKE_EXE_LINKER_FLAGS} ${_kith_ipo_ld}"
        OUTPUT_VARIABLE _kith_ipo_output)
    if(NOT _kith_ipo_supported)
        string(REPLACE "\n" "\n  " _kith_ipo_output "${_kith_ipo_output}")
        message(FATAL_ERROR
            "CMAKE_INTERPROCEDURAL_OPTIMIZATION=ON but the LTO probe failed "
            "for ${CMAKE_C_COMPILER_ID} ${CMAKE_C_COMPILER_VERSION} "
            "linking with '${_kith_ipo_ld}'. Probe output:\n"
            "  ${_kith_ipo_output}")
    endif()
endif()

# --- Reproducible builds (required for SLSA L3) ---------------------------
# Strip absolute source/build paths from debug info and pin a relative RPATH
# so artifacts are portable and bit-reproducible given SOURCE_DATE_EPOCH.
add_compile_options(
    -ffile-prefix-map=${CMAKE_SOURCE_DIR}=.
    -fdebug-prefix-map=${CMAKE_SOURCE_DIR}=.
    -ffile-prefix-map=${CMAKE_BINARY_DIR}=.
    -fdebug-prefix-map=${CMAKE_BINARY_DIR}=.
)
set(CMAKE_BUILD_RPATH_USE_ORIGIN ON)
set(CMAKE_INSTALL_RPATH "$ORIGIN/../lib")

# --- Compressed debug info (release + relwithdebinfo) --------------------
if(CMAKE_BUILD_TYPE MATCHES "Release|RelWithDebInfo")
    add_compile_options(-gz=zstd)
endif()
