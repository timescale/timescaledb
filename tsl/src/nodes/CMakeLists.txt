set(SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/async_append.c
    ${CMAKE_CURRENT_SOURCE_DIR}/data_node_dispatch.c
    ${CMAKE_CURRENT_SOURCE_DIR}/data_node_copy.c)
target_sources(${TSL_LIBRARY_NAME} PRIVATE ${SOURCES})
add_subdirectory(compress_dml)
add_subdirectory(decompress_chunk)
add_subdirectory(frozen_chunk_dml)
add_subdirectory(gapfill)
add_subdirectory(skip_scan)
