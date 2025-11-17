set(SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/array_utils.c
    ${CMAKE_CURRENT_SOURCE_DIR}/catalog.c
    ${CMAKE_CURRENT_SOURCE_DIR}/chunk_column_stats.c
    ${CMAKE_CURRENT_SOURCE_DIR}/chunk_rewrite.c
    ${CMAKE_CURRENT_SOURCE_DIR}/compression_chunk_size.c
    ${CMAKE_CURRENT_SOURCE_DIR}/compression_settings.c
    ${CMAKE_CURRENT_SOURCE_DIR}/continuous_agg.c
    ${CMAKE_CURRENT_SOURCE_DIR}/continuous_aggs_watermark.c
    ${CMAKE_CURRENT_SOURCE_DIR}/metadata.c
    ${CMAKE_CURRENT_SOURCE_DIR}/tablespace.c)
target_sources(${PROJECT_NAME} PRIVATE ${SOURCES})
