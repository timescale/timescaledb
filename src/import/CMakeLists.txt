set(SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/allpaths.c
    ${CMAKE_CURRENT_SOURCE_DIR}/heapswap.c
    ${CMAKE_CURRENT_SOURCE_DIR}/list.c
    ${CMAKE_CURRENT_SOURCE_DIR}/planner.c
    ${CMAKE_CURRENT_SOURCE_DIR}/ts_explain.c)

# Don't run clang-tidy on the files we copied from Postgres. We don't want
# to introduce changes there unless absolutely necessary.
set_source_files_properties(${SOURCES} PROPERTIES SKIP_LINTING ON)

target_sources(${PROJECT_NAME} PRIVATE ${SOURCES})
