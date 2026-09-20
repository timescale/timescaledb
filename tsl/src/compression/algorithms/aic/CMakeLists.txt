set(SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/aic_analyze.c
    ${CMAKE_CURRENT_SOURCE_DIR}/aic_blocks.c
    ${CMAKE_CURRENT_SOURCE_DIR}/aic_bulk_decompress.c
    ${CMAKE_CURRENT_SOURCE_DIR}/aic_iter_decompress.c
    ${CMAKE_CURRENT_SOURCE_DIR}/aic_nulls.c
    ${CMAKE_CURRENT_SOURCE_DIR}/aic_send_recv.c)
target_sources(${TSL_LIBRARY_NAME} PRIVATE ${SOURCES})
