set(SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/common.c
    ${CMAKE_CURRENT_SOURCE_DIR}/create.c
    ${CMAKE_CURRENT_SOURCE_DIR}/finalize.c
    ${CMAKE_CURRENT_SOURCE_DIR}/insert.c
    ${CMAKE_CURRENT_SOURCE_DIR}/invalidation_funcs.c
    ${CMAKE_CURRENT_SOURCE_DIR}/invalidation_threshold.c
    ${CMAKE_CURRENT_SOURCE_DIR}/invalidation.c
    ${CMAKE_CURRENT_SOURCE_DIR}/materialize.c
    ${CMAKE_CURRENT_SOURCE_DIR}/options.c
    ${CMAKE_CURRENT_SOURCE_DIR}/planner.c
    ${CMAKE_CURRENT_SOURCE_DIR}/refresh.c
    ${CMAKE_CURRENT_SOURCE_DIR}/repair.c
    ${CMAKE_CURRENT_SOURCE_DIR}/utils.c)
target_sources(${TSL_LIBRARY_NAME} PRIVATE ${SOURCES})

add_library(${PROJECT_NAME}-invalidations MODULE invalidation_plugin.c
                                                 invalidation_cache.c)
set_target_properties(
  ${PROJECT_NAME}-invalidations
  PROPERTIES OUTPUT_NAME ${PROJECT_NAME}-invalidations
             PREFIX ""
             POSITION_INDEPENDENT_CODE ON)
target_include_directories(${PROJECT_NAME}-invalidations
                           PRIVATE ${PG_INCLUDEDIR})
install(TARGETS ${PROJECT_NAME}-invalidations DESTINATION ${PG_PKGLIBDIR})
