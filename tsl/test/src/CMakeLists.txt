set(SOURCES
    data_node.c
    deparse.c
    test_chunk_stats.c
    test_compression.c
    test_continuous_agg.c
    test_ddl_hook.c
    test_dist_util.c
    test_dist_commands.c)

include(${PROJECT_SOURCE_DIR}/tsl/src/build-defs.cmake)

add_library(${TSL_TESTS_LIB_NAME} OBJECT ${SOURCES})

# Since the test library will be linked into the loadable extension module, it
# needs to be compiled as position-independent code (e.g., the -fPIC compiler
# flag for GCC)
set_target_properties(${TSL_TESTS_LIB_NAME} PROPERTIES POSITION_INDEPENDENT_CODE
                                                       ON)
target_include_directories(${TSL_TESTS_LIB_NAME}
                           PRIVATE ${CMAKE_SOURCE_DIR}/test/src)
target_include_directories(${TSL_TESTS_LIB_NAME}
                           PRIVATE ${CMAKE_SOURCE_DIR}/tsl/src/remote)
target_include_directories(${TSL_TESTS_LIB_NAME} PRIVATE ${PG_INCLUDEDIR})
target_compile_definitions(${TSL_TESTS_LIB_NAME} PUBLIC TS_SUBMODULE)

add_subdirectory(remote)
