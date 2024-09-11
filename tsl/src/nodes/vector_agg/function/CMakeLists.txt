set(SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/functions.c
    ${CMAKE_CURRENT_SOURCE_DIR}/minmax_templates.c
    ${CMAKE_CURRENT_SOURCE_DIR}/int24_sum_templates.c
    ${CMAKE_CURRENT_SOURCE_DIR}/sum_float_templates.c
    ${CMAKE_CURRENT_SOURCE_DIR}/float48_accum_templates.c
    ${CMAKE_CURRENT_SOURCE_DIR}/int24_avg_accum_templates.c
    ${CMAKE_CURRENT_SOURCE_DIR}/int128_accum_templates.c)
target_sources(${TSL_LIBRARY_NAME} PRIVATE ${SOURCES})
