# Perf tools: the C-native apply-transaction bench driver. The Python
# orchestration (tools/perf/apply_capacity_bench.py --c-native) locates the
# built binary and wraps it with the run envelope; the C driver itself only
# prints its AGG/WIN/DONE line records on stdout.

add_executable(c_apply_bench perf/c_apply_bench.c)
target_link_libraries(c_apply_bench PRIVATE kith_sim kith_fabric Threads::Threads)
target_compile_definitions(c_apply_bench PRIVATE _POSIX_C_SOURCE=200809L)
set_target_properties(c_apply_bench PROPERTIES OUTPUT_NAME c_apply_bench)

# The sizeof probe: a header-only instrument printing sizeof() for every
# size-versioned public struct. The Python parity test
# (tests/python/test_generated_sizeof_parity.py) locates the built binary
# and asserts each ctypes binding's size against it — the C-compiler-layout
# half the bindings drift checker cannot read.
add_executable(sizeof_probe sizeof_probe.c)
target_include_directories(sizeof_probe PRIVATE ${PROJECT_SOURCE_DIR}/include)
