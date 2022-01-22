set(SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/chunk_dispatch.c
    ${CMAKE_CURRENT_SOURCE_DIR}/chunk_dispatch_plan.c
    ${CMAKE_CURRENT_SOURCE_DIR}/chunk_dispatch_state.c
    ${CMAKE_CURRENT_SOURCE_DIR}/chunk_insert_state.c
    ${CMAKE_CURRENT_SOURCE_DIR}/hypertable_modify.c)
target_sources(${PROJECT_NAME} PRIVATE ${SOURCES})
add_subdirectory(chunk_append)
add_subdirectory(constraint_aware_append)
