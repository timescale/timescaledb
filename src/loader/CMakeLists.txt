set(HEADERS
)

set(SOURCES
  loader.c)

# Include code for tests in Debug build
if (CMAKE_BUILD_TYPE MATCHES Debug)
  set(TEST_SOURCES ../../test/src/symbol_conflict.c)
endif (CMAKE_BUILD_TYPE MATCHES Debug)

add_library(${PROJECT_NAME}-loader MODULE ${SOURCES} ${TEST_SOURCES} ${HEADERS})

if (CMAKE_BUILD_TYPE MATCHES Debug)
  add_subdirectory(../../test/loader-mock/ "${CMAKE_CURRENT_BINARY_DIR}/mock")

  # This define generates extension-specific code for symbol conflict testing
  target_compile_definitions(${PROJECT_NAME}-loader PUBLIC MODULE_NAME=loader)
endif (CMAKE_BUILD_TYPE MATCHES Debug)

set_target_properties(${PROJECT_NAME}-loader PROPERTIES
    OUTPUT_NAME ${PROJECT_NAME}
    PREFIX "")

install(
  TARGETS ${PROJECT_NAME}-loader
  DESTINATION ${PG_PKGLIBDIR})
