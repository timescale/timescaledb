set(SOURCES
  ${CMAKE_CURRENT_SOURCE_DIR}/test_conn.c
  ${CMAKE_CURRENT_SOURCE_DIR}/test_http.c
  ${CMAKE_CURRENT_SOURCE_DIR}/conn_mock.c
)

target_sources(${TESTS_LIB_NAME} PRIVATE ${SOURCES})
target_include_directories(${TESTS_LIB_NAME} PRIVATE ${PROJECT_SOURCE_DIR}/src/net)
