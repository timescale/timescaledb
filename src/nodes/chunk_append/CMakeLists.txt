# Add all *.c to sources in upperlevel directory
set(SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/chunk_append.c
    ${CMAKE_CURRENT_SOURCE_DIR}/exec.c ${CMAKE_CURRENT_SOURCE_DIR}/planner.c
    ${CMAKE_CURRENT_SOURCE_DIR}/transform.c)
target_sources(${PROJECT_NAME} PRIVATE ${SOURCES})
