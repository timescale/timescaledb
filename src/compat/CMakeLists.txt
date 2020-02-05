if (${PG_VERSION_MAJOR} LESS "12")
  set(SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/tableam.c
    ${CMAKE_CURRENT_SOURCE_DIR}/tupconvert.c
    ${CMAKE_CURRENT_SOURCE_DIR}/tuptable.c
    )
  target_sources(${PROJECT_NAME} PRIVATE ${SOURCES})
endif ()
