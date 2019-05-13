set(SOURCES
  ${CMAKE_CURRENT_SOURCE_DIR}/node_killer.c
  ${CMAKE_CURRENT_SOURCE_DIR}/connection.c
  ${CMAKE_CURRENT_SOURCE_DIR}/connection_cache.c
  ${CMAKE_CURRENT_SOURCE_DIR}/async.c
  ${CMAKE_CURRENT_SOURCE_DIR}/remote_exec.c
  ${CMAKE_CURRENT_SOURCE_DIR}/test_utils.c
  ${CMAKE_CURRENT_SOURCE_DIR}/test_session.c
  ${CMAKE_CURRENT_SOURCE_DIR}/txn_id.c
  ${CMAKE_CURRENT_SOURCE_DIR}/dist_commands.c
  ${CMAKE_CURRENT_SOURCE_DIR}/txn_persistent_record.c
  ${CMAKE_CURRENT_SOURCE_DIR}/txn_resolve.c
  ${CMAKE_CURRENT_SOURCE_DIR}/stmt_params.c
)

target_sources(${TSL_TESTS_LIB_NAME} PRIVATE ${SOURCES})
target_include_directories(${TSL_TESTS_LIB_NAME} PRIVATE ${PROJECT_SOURCE_DIR}/src/remote ${PG_INCLUDEDIR})
