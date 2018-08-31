set(HEADERS
  bgw_message_queue.h
  bgw_counter.h
  bgw_launcher.h
  loader.h
  ../compat.h
  ../extension_constants.h
)

set(SOURCES
  loader.c
  bgw_message_queue.c
  bgw_counter.c
  bgw_launcher.c
  bgw_interface.c)

# Include code for tests in Debug build
if (CMAKE_BUILD_TYPE MATCHES Debug)
  set(TEST_SOURCES ../../test/src/symbol_conflict.c)
endif (CMAKE_BUILD_TYPE MATCHES Debug)

add_library(${PROJECT_NAME}-loader MODULE ${SOURCES} ${TEST_SOURCES} ${HEADERS})

# Do not compile mock extensions on Windows due to missing exported symbol
# in IsParallelWorker macro
if (CMAKE_BUILD_TYPE MATCHES Debug)
  if (NOT WIN32)
    add_subdirectory(../../test/loader-mock/ "${CMAKE_CURRENT_BINARY_DIR}/mock")
  endif (NOT WIN32)

  # This define generates extension-specific code for symbol conflict testing
  target_compile_definitions(${PROJECT_NAME}-loader PUBLIC MODULE_NAME=loader)
endif (CMAKE_BUILD_TYPE MATCHES Debug)

set_target_properties(${PROJECT_NAME}-loader PROPERTIES
    OUTPUT_NAME ${PROJECT_NAME}
    PREFIX "")

install(
  TARGETS ${PROJECT_NAME}-loader
  DESTINATION ${PG_PKGLIBDIR})
