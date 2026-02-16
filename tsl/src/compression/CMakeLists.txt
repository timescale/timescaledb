set(SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/api.c
    ${CMAKE_CURRENT_SOURCE_DIR}/batch_metadata_builder_bloom1.c
    ${CMAKE_CURRENT_SOURCE_DIR}/batch_metadata_builder_minmax.c
    ${CMAKE_CURRENT_SOURCE_DIR}/compression.c
    ${CMAKE_CURRENT_SOURCE_DIR}/compression_dml.c
    ${CMAKE_CURRENT_SOURCE_DIR}/compression_scankey.c
    ${CMAKE_CURRENT_SOURCE_DIR}/compression_storage.c
    ${CMAKE_CURRENT_SOURCE_DIR}/create.c
    ${CMAKE_CURRENT_SOURCE_DIR}/recompress.c)
target_sources(${TSL_LIBRARY_NAME} PRIVATE ${SOURCES})

add_subdirectory(algorithms)
