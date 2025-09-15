set(SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/modify_hypertable.c
            ${CMAKE_CURRENT_SOURCE_DIR}/modify_hypertable_exec.c)
target_sources(${PROJECT_NAME} PRIVATE ${SOURCES})
add_subdirectory(chunk_append)
add_subdirectory(constraint_aware_append)
