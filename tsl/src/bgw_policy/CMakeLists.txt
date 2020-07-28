set(SOURCES
  ${CMAKE_CURRENT_SOURCE_DIR}/reorder_api.c
  ${CMAKE_CURRENT_SOURCE_DIR}/drop_chunks_api.c
  ${CMAKE_CURRENT_SOURCE_DIR}/compression_api.c
  ${CMAKE_CURRENT_SOURCE_DIR}/job.c
)
target_sources(${TSL_LIBRARY_NAME} PRIVATE ${SOURCES})
target_include_directories(${TSL_LIBRARY_NAME} PRIVATE ${CMAKE_SOURCE_DIR})
