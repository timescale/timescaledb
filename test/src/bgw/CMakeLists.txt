set(SOURCES
  "${CMAKE_CURRENT_SOURCE_DIR}/log.c"
  "${CMAKE_CURRENT_SOURCE_DIR}/timer_mock.c"
  "${CMAKE_CURRENT_SOURCE_DIR}/scheduler_mock.c"
  "${CMAKE_CURRENT_SOURCE_DIR}/params.c"
)
target_sources(${PROJECT_NAME} PRIVATE ${SOURCES})
