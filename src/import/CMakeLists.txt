set(SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/planner.c
    ${CMAKE_CURRENT_SOURCE_DIR}/allpaths.c
    ${CMAKE_CURRENT_SOURCE_DIR}/ht_hypertable_modify.c)

target_sources(${PROJECT_NAME} PRIVATE ${SOURCES})
