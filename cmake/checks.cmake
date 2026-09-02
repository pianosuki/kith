# CMake custom targets that wrap the tools/*.py checkers, clang-format, and
# clang-tidy. Each target is runnable via `cmake --build --target check-<name>`
# and fails (non-zero) when the underlying checker reports a violation. The
# checkers are the same ones the pre-commit hooks run; these targets expose
# them at the build level so a single build command or CI step can drive any
# one of them. `check-all` aggregates the individual targets.

find_program(KITH_PYTHON3_EXECUTABLE NAMES python3 REQUIRED)
# The dev-checker tools are required at use, not at configure: a full
# checkout configures with or without them, and the targets below fail with
# an install hint when their tool is missing. The gate (scripts/verify.sh
# build -> check-all) invokes every target, so a mis-provisioned host
# still fails closed — inside the gate rather than before it.
# clang-format is pinned to the exact major the pre-commit hook enforces:
# two formatter majors in one gate reject each other's output, so this
# check, scripts/format.sh, and the hook must resolve the same version.
find_program(KITH_CLANG_FORMAT_EXECUTABLE NAMES clang-format-22)
find_program(KITH_CLANG_TIDY_EXECUTABLE NAMES clang-tidy)

# Tracked-file producers used by the file-list checkers. git ls-files keeps
# the list current without a reconfigure; tools/fixtures/ holds checker test
# inputs that the comment and forbidden-pattern checkers exclude, matching the
# .pre-commit-config.yaml exclude rules.
set(_kith_tracked_src_py
    "git ls-files \"*.c\" \"*.h\" \"*.py\" \"*.yml\" \"*.yaml\" \"*.cmake\" \"CMakeLists.txt\" | grep -v \"^tools/fixtures/\"")
set(_kith_tracked_src_py_md
    "git ls-files \"*.c\" \"*.h\" \"*.py\" \"*.md\" | grep -v \"^tools/fixtures/\"")
set(_kith_tracked_c_h
    "git ls-files \"*.c\" \"*.h\" | grep -v \"^third_party/\"")

# --- file-list checkers ---------------------------------------------------
add_custom_target(check-comments
    COMMAND bash -c
        "${_kith_tracked_src_py} | xargs -r ${KITH_PYTHON3_EXECUTABLE} tools/check_comments.py"
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
    COMMENT "Running comment-philosophy checker"
    VERBATIM
    USES_TERMINAL
)

add_custom_target(check-clang-format
    COMMAND bash -c
        "[ -x '${KITH_CLANG_FORMAT_EXECUTABLE}' ] || { echo 'error: clang-format-22 not found; install the pinned formatter' >&2; exit 2; }; ${_kith_tracked_c_h} | xargs -r '${KITH_CLANG_FORMAT_EXECUTABLE}' --dry-run --Werror"
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
    COMMENT "Checking clang-format compliance"
    VERBATIM
    USES_TERMINAL
)

# --- root-scanned checkers -----------------------------------------------
# Each takes one or more source roots (include/, src/) and scans the tree.
set(_kith_root_checkers
    "check_layout_consistency|check-layout-consistency|include src"
    "check_module_layers|check-module-layers|include src"
    "check_public_api_includes|check-public-api-includes|include src"
    "check_internal_includes|check-internal-includes|include src"
)
foreach(_entry IN LISTS _kith_root_checkers)
    string(REPLACE "|" ";" _parts "${_entry}")
    list(GET _parts 0 _script)
    list(GET _parts 1 _target)
    list(GET _parts 2 _args)
    separate_arguments(_args_sep NATIVE_COMMAND "${_args}")
    add_custom_target(${_target}
        COMMAND ${KITH_PYTHON3_EXECUTABLE}
                ${CMAKE_SOURCE_DIR}/tools/${_script}.py ${_args_sep}
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
        COMMENT "Running ${_script}"
        VERBATIM
        USES_TERMINAL
    )
endforeach()

add_custom_target(check-public-api
    COMMAND ${KITH_PYTHON3_EXECUTABLE}
            ${CMAKE_SOURCE_DIR}/tools/check_public_api.py
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
    COMMENT "Running public-API contract checker"
    VERBATIM
    USES_TERMINAL
)

# --- generated-bindings drift --------------------------------------------
add_custom_target(check-ctypes-drift
    COMMAND ${KITH_PYTHON3_EXECUTABLE}
            ${CMAKE_SOURCE_DIR}/tools/check_ctypes_drift.py
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
    COMMENT "Checking ctypes bindings for drift"
    VERBATIM
    USES_TERMINAL
)

# --- clang-tidy over the compilation database ----------------------------
# The file list is the tracked .c set minus the trees that compile outside
# the library targets' compilation database (tools/ probes and fixtures,
# third-party sources, the fuzz harnesses, and the downstream consumer
# probes): clang-tidy -p fails closed on a file the database has no entry
# for.
add_custom_target(check-clang-tidy
    COMMAND bash -c
        "[ -x '${KITH_CLANG_TIDY_EXECUTABLE}' ] || { echo 'error: clang-tidy not found; install the pinned clang tools' >&2; exit 2; }; git ls-files \"*.c\" | grep -v \"^tools/\" | grep -v \"^third_party/\" | grep -v \"^tests/fuzz/\" | grep -v \"^tests/consumer/\" | xargs -r '${KITH_CLANG_TIDY_EXECUTABLE}' -p ${CMAKE_BINARY_DIR} --quiet --warnings-as-errors='*'"
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
    COMMENT "Running clang-tidy over the compilation database"
    VERBATIM
    USES_TERMINAL
)

# --- aggregate -----------------------------------------------------------
add_custom_target(check-all
    DEPENDS
        check-layout-consistency
        check-module-layers
        check-public-api
        check-public-api-includes
        check-internal-includes
        check-comments
        check-ctypes-drift
        check-clang-tidy
        check-clang-format
        check-ruff-format
        check-ruff
        check-trivial-fixers
    COMMENT "Running all check-* targets"
)
