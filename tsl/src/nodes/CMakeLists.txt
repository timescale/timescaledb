set(SOURCES
  ${CMAKE_CURRENT_SOURCE_DIR}/data_node_dispatch.c
)
target_sources(${TSL_LIBRARY_NAME} PRIVATE ${SOURCES})
add_subdirectory(compress_dml)
add_subdirectory(decompress_chunk)
add_subdirectory(gapfill)
add_subdirectory(skip_scan)
