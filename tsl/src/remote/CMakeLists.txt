set(SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/async.c
    ${CMAKE_CURRENT_SOURCE_DIR}/connection.c
    ${CMAKE_CURRENT_SOURCE_DIR}/connection_cache.c
    ${CMAKE_CURRENT_SOURCE_DIR}/dist_txn.c
    ${CMAKE_CURRENT_SOURCE_DIR}/txn.c
    ${CMAKE_CURRENT_SOURCE_DIR}/txn_store.c
    ${CMAKE_CURRENT_SOURCE_DIR}/txn_id.c
  )
target_sources(${TSL_LIBRARY_NAME} PRIVATE ${SOURCES})
target_include_directories(${TSL_LIBRARY_NAME} PRIVATE ${PG_INCLUDEDIR})
