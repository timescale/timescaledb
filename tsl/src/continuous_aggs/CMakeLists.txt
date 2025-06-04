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

add_library(${TSL_CAGG_INVALIDATION_PLUGIN_NAME} MODULE
            invalidation_plugin.c invalidation_plugin_cache.c)

# VERSION and SOVERSION are not used since they do not make sense for modules,
# and actually breaks the build on MacOS.
#
# See https://gitlab.kitware.com/cmake/cmake/-/issues/20782.
set_target_properties(
  ${TSL_CAGG_INVALIDATION_PLUGIN_NAME}
  PROPERTIES OUTPUT_NAME ${TSL_CAGG_INVALIDATION_PLUGIN_NAME}
             PREFIX ""
             POSITION_INDEPENDENT_CODE ON)
target_include_directories(${TSL_CAGG_INVALIDATION_PLUGIN_NAME}
                           PRIVATE ${PG_INCLUDEDIR})
install(TARGETS ${TSL_CAGG_INVALIDATION_PLUGIN_NAME}
        LIBRARY DESTINATION ${PG_PKGLIBDIR})
