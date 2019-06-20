set(SOURCES
  ${CMAKE_CURRENT_SOURCE_DIR}/timescaledb_fdw.c
  ${CMAKE_CURRENT_SOURCE_DIR}/deparse.c
  ${CMAKE_CURRENT_SOURCE_DIR}/shippable.c
  ${CMAKE_CURRENT_SOURCE_DIR}/utils.c
  ${CMAKE_CURRENT_SOURCE_DIR}/option.c
  ${CMAKE_CURRENT_SOURCE_DIR}/server_chunk_assignment.c
  ${CMAKE_CURRENT_SOURCE_DIR}/server_scan_plan.c
  ${CMAKE_CURRENT_SOURCE_DIR}/server_scan_exec.c
)

target_sources(${TSL_LIBRARY_NAME} PRIVATE ${SOURCES})
