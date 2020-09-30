set(SOURCES
  agg_bookend.c
  func_cache.c
  cache.c
  cache_invalidate.c
  catalog.c
  continuous_agg.c
  chunk.c
  chunk_adaptive.c
  chunk_constraint.c
  chunk_dispatch.c
  chunk_dispatch_plan.c
  chunk_dispatch_state.c
  chunk_index.c
  chunk_insert_state.c
  chunk_data_node.c
  constraint.c
  constraint_aware_append.c
  cross_module_fn.c
  copy.c
  compression_chunk_size.c
  compression_with_clause.c
  dimension.c
  dimension_slice.c
  dimension_vector.c
  estimate.c
  event_trigger.c
  extension.c
  gapfill.c
  guc.c
  histogram.c
  hypercube.c
  hypertable.c
  hypertable_cache.c
  hypertable_compression.c
  hypertable_insert.c
  hypertable_restrict_info.c
  hypertable_data_node.c
  indexing.c
  init.c
  metadata.c
  jsonb_utils.c
  license_guc.c
  partitioning.c
  planner.c
  plan_expand_hypertable.c
  plan_add_hashagg.c
  plan_agg_bookend.c
  plan_partialize.c
  process_utility.c
  scanner.c
  scan_iterator.c
  sort_transform.c
  subspace_store.c
  tablespace.c
  time_bucket.c
  time_utils.c
  custom_type_cache.c
  trigger.c
  utils.c
  version.c
  with_clause_parser.c
)

# Add test source code in Debug builds
if (CMAKE_BUILD_TYPE MATCHES Debug)
  set(TS_DEBUG 1)
  set(DEBUG 1)
  list(APPEND SOURCES
    debug_wait.c
    debug_guc.c)
endif (CMAKE_BUILD_TYPE MATCHES Debug)

include(build-defs.cmake)

if(GIT_FOUND)
  # We use "git describe" to generate the tag. It will find the latest
  # tag and also add some additional information if we are not on the
  # tag.
  execute_process(
    COMMAND ${GIT_EXECUTABLE} describe --dirty --always --tags
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
    OUTPUT_VARIABLE EXT_GIT_COMMIT_TAG
    RESULT_VARIABLE _describe_RESULT
    OUTPUT_STRIP_TRAILING_WHITESPACE)

  # Fetch the commit HASH of head (short version) using rev-parse
  execute_process(
    COMMAND ${GIT_EXECUTABLE} rev-parse --short HEAD
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
    OUTPUT_VARIABLE EXT_GIT_COMMIT_HASH
    RESULT_VARIABLE _revparse_RESULT
    OUTPUT_STRIP_TRAILING_WHITESPACE)

  # Fetch the date of the head commit
  execute_process(
    COMMAND ${GIT_EXECUTABLE} log -1 --format=%aI
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
    OUTPUT_VARIABLE EXT_GIT_COMMIT_TIME
    RESULT_VARIABLE _log_RESULT
    OUTPUT_STRIP_TRAILING_WHITESPACE)

  # Results are non-zero if there were an error
  if(_describe_RESULT OR _revparse_RESULT OR _log_RESULT)
    message(STATUS "Unable to get git commit information")
  endif()
endif()

configure_file(gitcommit.h.in gitcommit.h)

if (CMAKE_BUILD_TYPE MATCHES Debug)
  add_library(${PROJECT_NAME} MODULE ${SOURCES} ${GITCOMMIT_H} $<TARGET_OBJECTS:${TESTS_LIB_NAME}>)
else ()
  add_library(${PROJECT_NAME} MODULE ${SOURCES} ${GITCOMMIT_H})
endif ()

if (SEND_TELEMETRY_DEFAULT)
  set(TELEMETRY_DEFAULT TELEMETRY_BASIC)
else ()
  set(TELEMETRY_DEFAULT TELEMETRY_OFF)
endif ()

set_target_properties(${PROJECT_NAME} PROPERTIES
  OUTPUT_NAME ${PROJECT_NAME}-${PROJECT_VERSION_MOD}
  PREFIX "")

install(
  TARGETS ${PROJECT_NAME}
  DESTINATION ${PG_PKGLIBDIR})

if (USE_OPENSSL)
  set(TS_USE_OPENSSL ${USE_OPENSSL})
  target_include_directories(${PROJECT_NAME} SYSTEM PUBLIC ${OPENSSL_INCLUDE_DIR})
  target_link_libraries(${PROJECT_NAME} ${OPENSSL_LIBRARIES})
endif (USE_OPENSSL)

configure_file(config.h.in config.h)

include_directories(${CMAKE_CURRENT_SOURCE_DIR})
add_subdirectory(bgw)
add_subdirectory(chunk_append)
add_subdirectory(net)
add_subdirectory(telemetry)
add_subdirectory(loader)
add_subdirectory(bgw_policy)
add_subdirectory(compat)
add_subdirectory(import)
