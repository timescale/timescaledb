set(SOURCES
  "${CMAKE_CURRENT_SOURCE_DIR}/test_conn.c"
  "${CMAKE_CURRENT_SOURCE_DIR}/test_http.c"
)
target_sources(${PROJECT_NAME} PRIVATE ${SOURCES})
