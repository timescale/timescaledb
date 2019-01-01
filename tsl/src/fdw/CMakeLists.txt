set(SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/timescaledb_fdw.c
    )

target_sources(${TSL_LIBRARY_NAME} PRIVATE ${SOURCES})
