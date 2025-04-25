set(SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/alter_table_with_clause.c
    ${CMAKE_CURRENT_SOURCE_DIR}/create_table_with_clause.c
    ${CMAKE_CURRENT_SOURCE_DIR}/create_materialized_view_with_clause.c
    ${CMAKE_CURRENT_SOURCE_DIR}/with_clause_parser.c)

target_sources(${PROJECT_NAME} PRIVATE ${SOURCES})
