if (${PG_VERSION_MAJOR} GREATER_EQUAL "12")
  set(SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/allpaths.c
    )
  target_sources(${PROJECT_NAME} PRIVATE ${SOURCES})
endif ()
