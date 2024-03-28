set(SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/allpaths.c
    ${CMAKE_CURRENT_SOURCE_DIR}/ht_hypertable_modify.c
    ${CMAKE_CURRENT_SOURCE_DIR}/list.c
    ${CMAKE_CURRENT_SOURCE_DIR}/planner.c
    ${CMAKE_CURRENT_SOURCE_DIR}/ts_explain.c)

target_sources(${PROJECT_NAME} PRIVATE ${SOURCES})
