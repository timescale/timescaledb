# Add all *.c to sources in upperlevel directory
set(SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/chunk_dispatch.c
            ${CMAKE_CURRENT_SOURCE_DIR}/chunk_insert_state.c)
target_sources(${PROJECT_NAME} PRIVATE ${SOURCES})
