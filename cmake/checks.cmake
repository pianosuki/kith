# CMake custom targets that wrap clang-format. Each target is runnable via
# `cmake --build --target check-<name>` and fails (non-zero) when the
# underlying check reports a violation. The check is the same one the
# pre-commit hook runs; this target exposes it at the build level so a
# single build command or CI step can drive it. `check-all` aggregates the
# individual targets.

find_program(KITH_PYTHON3_EXECUTABLE NAMES python3 REQUIRED)
# The dev-checker tools are required at use, not at configure: a full
# checkout configures with or without them, and the targets below fail with
# an install hint when their tool is missing.
# clang-format is pinned to the exact major the pre-commit hook enforces:
# two formatter majors in one tree reject each other's output, so this
# check and the hook must resolve the same version.
find_program(KITH_CLANG_FORMAT_EXECUTABLE NAMES clang-format-22)

# Tracked-file producer used by the format check. git ls-files keeps the
# list current without a reconfigure.
set(_kith_tracked_c_h
    "git ls-files \"*.c\" \"*.h\" | grep -v \"^third_party/\"")

add_custom_target(check-clang-format
    COMMAND bash -c
        "[ -x '${KITH_CLANG_FORMAT_EXECUTABLE}' ] || { echo 'error: clang-format-22 not found; install the pinned formatter' >&2; exit 2; }; ${_kith_tracked_c_h} | xargs -r '${KITH_CLANG_FORMAT_EXECUTABLE}' --dry-run --Werror"
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
    COMMENT "Checking clang-format compliance"
    VERBATIM
    USES_TERMINAL
)

# --- aggregate -----------------------------------------------------------
add_custom_target(check-all
    DEPENDS
        check-clang-format
        check-ruff-format
        check-ruff
    COMMENT "Running all check-* targets"
)
