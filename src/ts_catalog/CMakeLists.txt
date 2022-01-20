set(SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/catalog.c
    ${CMAKE_CURRENT_SOURCE_DIR}/chunk_data_node.c
    ${CMAKE_CURRENT_SOURCE_DIR}/compression_chunk_size.c
    ${CMAKE_CURRENT_SOURCE_DIR}/continuous_agg.c
    ${CMAKE_CURRENT_SOURCE_DIR}/hypertable_compression.c
    ${CMAKE_CURRENT_SOURCE_DIR}/hypertable_data_node.c
    ${CMAKE_CURRENT_SOURCE_DIR}/metadata.c
    ${CMAKE_CURRENT_SOURCE_DIR}/tablespace.c)
target_sources(${PROJECT_NAME} PRIVATE ${SOURCES})
