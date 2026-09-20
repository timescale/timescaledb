set(SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/rr_analyze.c
    ${CMAKE_CURRENT_SOURCE_DIR}/rr_blocks.c
    ${CMAKE_CURRENT_SOURCE_DIR}/rr_bulk_decompress.c
    ${CMAKE_CURRENT_SOURCE_DIR}/rr_iter_decompress.c
    ${CMAKE_CURRENT_SOURCE_DIR}/rr_nulls.c
    ${CMAKE_CURRENT_SOURCE_DIR}/rr_send_recv.c)
target_sources(${TSL_LIBRARY_NAME} PRIVATE ${SOURCES})
