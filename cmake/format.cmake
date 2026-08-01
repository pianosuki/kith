# CMake targets for the verify-only formatter checks.
#
# Each `check-*` target verifies a single invariant over the tracked files
# and exits non-zero on a violation, pointing the contributor at the fix.
# Gates never mutate the working tree; a contributor runs the formatter to
# fix whatever a check reports, then re-runs it.

# ruff, like the clang tools in checks.cmake, is required at use: the
# targets below fail with an install hint when it is missing.
find_program(KITH_RUFF_EXECUTABLE NAMES ruff)

# Tracked-file producer used by the ruff checks. git ls-files keeps the
# list current without a reconfigure; the exclude range mirrors
# .pre-commit-config.yaml.
set(_kith_format_tracked_py
    "git ls-files \"*.py\" | grep -v \"^tools/fixtures/\"")

add_custom_target(check-ruff-format
    COMMAND bash -c
        "[ -x '${KITH_RUFF_EXECUTABLE}' ] || { echo 'error: ruff not found; install the pinned version' >&2; exit 2; }; ${_kith_format_tracked_py} | xargs -r '${KITH_RUFF_EXECUTABLE}' format --check"
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
    COMMENT "Checking ruff format compliance"
    VERBATIM
    USES_TERMINAL
)

add_custom_target(check-ruff
    COMMAND bash -c
        "[ -x '${KITH_RUFF_EXECUTABLE}' ] || { echo 'error: ruff not found; install the pinned version' >&2; exit 2; }; ${_kith_format_tracked_py} | xargs -r '${KITH_RUFF_EXECUTABLE}' check"
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
    COMMENT "Checking ruff lint compliance"
    VERBATIM
    USES_TERMINAL
)
