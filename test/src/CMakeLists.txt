set(SOURCES
    metadata.c
    adt_tests.c
    symbol_conflict.c
    test_time_to_internal.c
    test_with_clause_parser.c
    test_time_utils.c
    test_utils.c
    test_scanner.c)

include(${PROJECT_SOURCE_DIR}/src/build-defs.cmake)

add_library(${TESTS_LIB_NAME} OBJECT ${SOURCES})

# Since the test library will be linked into the loadable extension module, it
# needs to be compiled as position-independent code (e.g., the -fPIC compiler
# flag for GCC)
set_target_properties(${TESTS_LIB_NAME} PROPERTIES POSITION_INDEPENDENT_CODE ON)

# Set the MODULE_NAME for the symbol conflict test (see symbol_conflict.c)
target_compile_definitions(${TESTS_LIB_NAME} PUBLIC MODULE_NAME=timescaledb)
target_include_directories(${TESTS_LIB_NAME}
                           PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

add_subdirectory(bgw)
add_subdirectory(net)
if(USE_TELEMETRY)
  add_subdirectory(telemetry)
endif()

add_subdirectory(loader)
