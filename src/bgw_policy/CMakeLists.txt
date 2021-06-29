set(SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/policy.c
            ${CMAKE_CURRENT_SOURCE_DIR}/chunk_stats.c)
target_sources(${PROJECT_NAME} PRIVATE ${SOURCES})
