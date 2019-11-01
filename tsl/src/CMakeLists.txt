set(SOURCES
  init.c
  license.c
  reorder.c
  telemetry.c
  partialize_finalize.c
  planner.c
)

# Add test source code in Debug builds
if (CMAKE_BUILD_TYPE MATCHES Debug)
  set(TS_DEBUG 1)
  set(DEBUG 1)
endif (CMAKE_BUILD_TYPE MATCHES Debug)

set(TSL_LIBRARY_NAME ${PROJECT_NAME}-tsl)

include(build-defs.cmake)

if (CMAKE_BUILD_TYPE MATCHES Debug)
add_library(${TSL_LIBRARY_NAME} MODULE ${SOURCES} $<TARGET_OBJECTS:${TSL_TESTS_LIB_NAME}>)
else ()
add_library(${TSL_LIBRARY_NAME} MODULE ${SOURCES})
endif ()

set_target_properties(${TSL_LIBRARY_NAME} PROPERTIES
  OUTPUT_NAME ${TSL_LIBRARY_NAME}-${PROJECT_VERSION_MOD}
  PREFIX "")

target_compile_definitions(${TSL_LIBRARY_NAME} PUBLIC TS_TSL)
target_compile_definitions(${TSL_LIBRARY_NAME} PUBLIC TS_SUBMODULE)

install(
  TARGETS ${TSL_LIBRARY_NAME}
  DESTINATION ${PG_PKGLIBDIR})

# if (WIN32)
#   target_link_libraries(${PROJECT_NAME} ${PROJECT_NAME}-${PROJECT_VERSION_MOD}.lib)
# endif(WIN32)

add_subdirectory(bgw_policy)
add_subdirectory(compression)
add_subdirectory(continuous_aggs)
add_subdirectory(nodes)
