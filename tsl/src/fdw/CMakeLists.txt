set(SOURCES
  ${CMAKE_CURRENT_SOURCE_DIR}/analyze.c
  ${CMAKE_CURRENT_SOURCE_DIR}/relinfo.c
  ${CMAKE_CURRENT_SOURCE_DIR}/estimate.c
  ${CMAKE_CURRENT_SOURCE_DIR}/fdw.c
  ${CMAKE_CURRENT_SOURCE_DIR}/modify_plan.c
  ${CMAKE_CURRENT_SOURCE_DIR}/modify_exec.c
  ${CMAKE_CURRENT_SOURCE_DIR}/scan_plan.c
  ${CMAKE_CURRENT_SOURCE_DIR}/scan_exec.c
  ${CMAKE_CURRENT_SOURCE_DIR}/deparse.c
  ${CMAKE_CURRENT_SOURCE_DIR}/shippable.c
  ${CMAKE_CURRENT_SOURCE_DIR}/option.c
  ${CMAKE_CURRENT_SOURCE_DIR}/data_node_chunk_assignment.c
  ${CMAKE_CURRENT_SOURCE_DIR}/data_node_scan_plan.c
  ${CMAKE_CURRENT_SOURCE_DIR}/data_node_scan_exec.c
)

target_sources(${TSL_LIBRARY_NAME} PRIVATE ${SOURCES})
