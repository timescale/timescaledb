set(SOURCES
  ${CMAKE_CURRENT_SOURCE_DIR}/node_killer.c
  ${CMAKE_CURRENT_SOURCE_DIR}/connection.c
  ${CMAKE_CURRENT_SOURCE_DIR}/connection_cache.c
  ${CMAKE_CURRENT_SOURCE_DIR}/async.c
)

target_sources(${TSL_TESTS_LIB_NAME} PRIVATE ${SOURCES})
target_include_directories(${TSL_TESTS_LIB_NAME} PRIVATE ${PROJECT_SOURCE_DIR}/src/remote ${PG_INCLUDEDIR})
