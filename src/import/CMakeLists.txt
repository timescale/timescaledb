set(SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/planner.c
)

if (${PG_VERSION_MAJOR} GREATER_EQUAL "12")
  set(SOURCES
      ${SOURCES}
      ${CMAKE_CURRENT_SOURCE_DIR}/allpaths.c
    )
endif ()

target_sources(${PROJECT_NAME} PRIVATE ${SOURCES})
