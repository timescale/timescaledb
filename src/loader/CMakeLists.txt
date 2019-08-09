set(SOURCES
  loader.c
  bgw_message_queue.c
  bgw_counter.c
  bgw_launcher.c
  bgw_interface.c
  lwlocks.c
)

set(TEST_SOURCES
  ${PROJECT_SOURCE_DIR}/test/src/symbol_conflict.c
)

add_library(${PROJECT_NAME}-loader MODULE ${SOURCES})

if (CMAKE_BUILD_TYPE MATCHES Debug)
  # Include code for tests in Debug build
  target_sources(${PROJECT_NAME}-loader PRIVATE ${TEST_SOURCES})
  # This define generates extension-specific code for symbol conflict testing
  target_compile_definitions(${PROJECT_NAME}-loader PUBLIC MODULE_NAME=loader)
endif (CMAKE_BUILD_TYPE MATCHES Debug)

set_target_properties(${PROJECT_NAME}-loader PROPERTIES
    OUTPUT_NAME ${PROJECT_NAME}
    PREFIX "")

install(
  TARGETS ${PROJECT_NAME}-loader
  DESTINATION ${PG_PKGLIBDIR})
