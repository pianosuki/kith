cmake_minimum_required(VERSION 4.4)

# The project version is the single source of truth: the wheel and sdist
# version tags derive from it (pyproject reads the project() line), and the
# consumer channels — the pkg-config Version, the CMake package version
# file, and the find_package(kith VERSION ...) check — use the same line.
project(kith VERSION 1.0.0 LANGUAGES C)

set(CMAKE_C_STANDARD 23)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_EXTENSIONS OFF)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(CMAKE_C_VISIBILITY_PRESET hidden)

# Framework-wide conventions: warnings, hardening, visibility, sanitizers,
# abi helpers, linker/LTO/reproducible-build settings.
# framework.cmake is the single source of truth and includes the rest.
include(cmake/framework.cmake)

# Per-tool build targets (check-clang-format, the ruff pair from
# format.cmake, and check-all). Each runs its tool over the tracked files
# and fails the build on a violation; both fragments drive the tools/
# helpers and git ls-files, and both fail at use when one of their tools is
# missing. A source distribution carries only the build inputs, so the
# fragments are included only when tools/ is present.
if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/tools")
    include(cmake/checks.cmake)
    include(cmake/format.cmake)
endif()
