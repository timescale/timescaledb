set(SOURCES
  "${CMAKE_CURRENT_SOURCE_DIR}/test_conn.c"
  "${CMAKE_CURRENT_SOURCE_DIR}/test_http.c"
  )

if (CMAKE_BUILD_TYPE MATCHES Debug)
  list(APPEND SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/conn_mock.c")
endif (CMAKE_BUILD_TYPE MATCHES Debug)

target_sources(${PROJECT_NAME} PRIVATE ${SOURCES})
target_include_directories(${PROJECT_NAME} PRIVATE ${PROJECT_SOURCE_DIR}/src/net)
