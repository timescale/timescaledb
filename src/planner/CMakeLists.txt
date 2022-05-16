set(SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/planner.c
    ${CMAKE_CURRENT_SOURCE_DIR}/add_hashagg.c
    ${CMAKE_CURRENT_SOURCE_DIR}/agg_bookend.c
    ${CMAKE_CURRENT_SOURCE_DIR}/constify_now.c
    ${CMAKE_CURRENT_SOURCE_DIR}/expand_hypertable.c
    ${CMAKE_CURRENT_SOURCE_DIR}/partialize.c)
target_sources(${PROJECT_NAME} PRIVATE ${SOURCES})
