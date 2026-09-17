# CMake targets for the format/verify split.
#
# The repository enforces two complementary commands:
#   - `cmake --build --target format`  applies every formatter in place (the
#     single fix entry point; never fails a build).
#   - `cmake --build --target check-*` verify a single invariant and fail the
#     build on violation. `check-all` (in checks.cmake) aggregates them.
#
# Gates never mutate the working tree; a contributor runs `format` to fix
# whatever a gate reports, then re-runs the gate. The same tools back both
# sides, so the verify targets and the fix target cannot disagree on what
# "compliant" means.

# ruff, like the clang tools in checks.cmake, is required at use: the
# targets below fail with an install hint when it is missing, and the gate
# invokes them, so a mis-provisioned host fails closed inside the gate.
find_program(KITH_RUFF_EXECUTABLE NAMES ruff)
find_program(KITH_PYTHON3_EXECUTABLE_FOR_FORMAT NAMES python3 REQUIRED)

# Tracked-file producers used by the format and check targets. git ls-files
# keeps the list current without a reconfigure; the exclude ranges mirror
# .pre-commit-config.yaml and checks.cmake.
set(_kith_format_tracked_c_h
    "git ls-files \"*.c\" \"*.h\" | grep -v \"^third_party/\"")
set(_kith_format_tracked_py
    "git ls-files \"*.py\" | grep -v \"^tools/fixtures/\"")
set(_kith_format_tracked_all
    "git ls-files \"*.c\" \"*.h\" \"*.py\" \
    | grep -v \"^third_party/\" | grep -v \"^tools/fixtures/\"")

# --- verify-only check targets ------------------------------------------
# These mirror the format target's scope but never write; each exits non-zero
# on a violation and points the contributor at the fix command. check-all
# (checks.cmake) depends on them so the build-level gate is complete.

add_custom_target(check-ruff-format
    COMMAND bash -c
        "[ -x '${KITH_RUFF_EXECUTABLE}' ] || { echo 'error: ruff not found; install the pinned version (scripts/setup.sh preflight names it)' >&2; exit 2; }; ${_kith_format_tracked_py} | xargs -r '${KITH_RUFF_EXECUTABLE}' format --check"
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
    COMMENT "Checking ruff format compliance"
    VERBATIM
    USES_TERMINAL
)

add_custom_target(check-ruff
    COMMAND bash -c
        "[ -x '${KITH_RUFF_EXECUTABLE}' ] || { echo 'error: ruff not found; install the pinned version (scripts/setup.sh preflight names it)' >&2; exit 2; }; ${_kith_format_tracked_py} | xargs -r '${KITH_RUFF_EXECUTABLE}' check"
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
    COMMENT "Checking ruff lint compliance"
    VERBATIM
    USES_TERMINAL
)

add_custom_target(check-trivial-fixers
    COMMAND bash -c
        "${_kith_format_tracked_all} \
        | xargs -r ${KITH_PYTHON3_EXECUTABLE_FOR_FORMAT} \
            ${CMAKE_SOURCE_DIR}/tools/check_trivial_fixers.py"
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
    COMMENT "Checking trivial whitespace/line-ending/BOM compliance"
    VERBATIM
    USES_TERMINAL
)

# --- the single fix target ----------------------------------------------
# Applies every formatter in place. Idempotent. Never fails a build: a
# well-formed tree is a no-op. The underlying script owns the implementation
# so the CMake target and `scripts/format.sh` cannot diverge.

add_custom_target(format
    COMMAND ${CMAKE_SOURCE_DIR}/scripts/format.sh
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
    COMMENT "Applying all formatters (clang-format, ruff, trivial fixers)"
    VERBATIM
    USES_TERMINAL
)
