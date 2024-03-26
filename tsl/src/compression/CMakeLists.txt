set(SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/api.c
    ${CMAKE_CURRENT_SOURCE_DIR}/compression.c
    ${CMAKE_CURRENT_SOURCE_DIR}/compression_dml.c
    ${CMAKE_CURRENT_SOURCE_DIR}/compression_scankey.c
    ${CMAKE_CURRENT_SOURCE_DIR}/compression_storage.c
    ${CMAKE_CURRENT_SOURCE_DIR}/create.c
    ${CMAKE_CURRENT_SOURCE_DIR}/segment_meta.c)
target_sources(${TSL_LIBRARY_NAME} PRIVATE ${SOURCES})

add_subdirectory(algorithms)
