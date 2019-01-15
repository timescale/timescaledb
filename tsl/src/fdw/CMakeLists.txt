set(SOURCES
  ${CMAKE_CURRENT_SOURCE_DIR}/timescaledb_fdw.c
  ${CMAKE_CURRENT_SOURCE_DIR}/deparse.c
  ${CMAKE_CURRENT_SOURCE_DIR}/shippable.c
  ${CMAKE_CURRENT_SOURCE_DIR}/utils.c
)

target_sources(${TSL_LIBRARY_NAME} PRIVATE ${SOURCES})
