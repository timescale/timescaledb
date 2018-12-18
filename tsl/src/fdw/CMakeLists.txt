set(SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/timescaledb_fdw.c
    )

target_sources(${PROJECT_NAME} PRIVATE ${SOURCES})
