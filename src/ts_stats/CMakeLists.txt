set(SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/ts_stats_record.c
    ${CMAKE_CURRENT_SOURCE_DIR}/ts_stats_segment.c
    ${CMAKE_CURRENT_SOURCE_DIR}/ts_stats_srf.c)
target_sources(${PROJECT_NAME} PRIVATE ${SOURCES})
