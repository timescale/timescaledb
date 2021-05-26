set(SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/planner.c
    ${CMAKE_CURRENT_SOURCE_DIR}/allpaths.c
)

target_sources(${PROJECT_NAME} PRIVATE ${SOURCES})
