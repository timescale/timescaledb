set(SOURCES
    uuid.c
    agg_bookend.c
    func_cache.c
    cache.c
    cache_invalidate.c
    chunk.c
    chunk_adaptive.c
    chunk_constraint.c
    chunk_index.c
    chunk_scan.c
    constraint.c
    cross_module_fn.c
    copy.c
    compression_with_clause.c
    dimension.c
    dimension_slice.c
    dimension_vector.c
    estimate.c
    event_trigger.c
    extension.c
    extension_constants.c
    gapfill.c
    guc.c
    histogram.c
    hypercube.c
    hypertable.c
    hypertable_cache.c
    hypertable_restrict_info.c
    indexing.c
    init.c
    jsonb_utils.c
    license_guc.c
    partitioning.c
    process_utility.c
    scanner.c
    scan_iterator.c
    sort_transform.c
    subspace_store.c
    timezones.c
    time_bucket.c
    time_utils.c
    custom_type_cache.c
    trigger.c
    utils.c
    version.c
    with_clause_parser.c)

# Add test source code in Debug builds
if(CMAKE_BUILD_TYPE MATCHES Debug)
  set(TS_DEBUG 1)
  set(DEBUG 1)
  list(APPEND SOURCES debug_point.c debug_guc.c)
endif(CMAKE_BUILD_TYPE MATCHES Debug)

include(build-defs.cmake)

set(GITREV_TMP ${CMAKE_CURRENT_BINARY_DIR}/tmp_gitcommit.h)
set(GITREV_FILE ${CMAKE_CURRENT_BINARY_DIR}/gitcommit.h)

# The commands for generating gitcommit.h need to be executed on every make run
# and not on cmake run to detect branch switches, commit changes or local
# modifications. That's why we add the commands in a custom target and run them
# on every make run. We do the generation part in a temporary file and only
# overwrite the actual file when the content is different to not trigger
# unnecessary recompilations.

add_custom_target(
  gitcheck
  COMMAND
    ${CMAKE_COMMAND} "-DGIT_FOUND=${GIT_FOUND}"
    "-DSOURCE_DIR=${CMAKE_CURRENT_SOURCE_DIR}"
    "-DGIT_EXECUTABLE=${GIT_EXECUTABLE}"
    "-DINPUT_FILE=${CMAKE_CURRENT_SOURCE_DIR}/gitcommit.h.in"
    "-DOUTPUT_FILE=${GITREV_TMP}" -P ${CMAKE_CURRENT_SOURCE_DIR}/gitcommit.cmake
  COMMAND ${CMAKE_COMMAND} -E copy_if_different ${GITREV_TMP} ${GITREV_FILE})

if(CMAKE_BUILD_TYPE MATCHES Debug)
  add_library(${PROJECT_NAME} MODULE ${SOURCES} ${GITCOMMIT_H}
                                     $<TARGET_OBJECTS:${TESTS_LIB_NAME}>)
else()
  add_library(${PROJECT_NAME} MODULE ${SOURCES} ${GITCOMMIT_H})
endif()

if(USE_TELEMETRY)
  if(SEND_TELEMETRY_DEFAULT)
    set(TELEMETRY_DEFAULT TELEMETRY_BASIC)
  else()
    set(TELEMETRY_DEFAULT TELEMETRY_OFF)
  endif()
endif()

set_target_properties(
  ${PROJECT_NAME} PROPERTIES OUTPUT_NAME ${PROJECT_NAME}-${PROJECT_VERSION_MOD}
                             PREFIX "")

install(TARGETS ${PROJECT_NAME} DESTINATION ${PG_PKGLIBDIR})

if(USE_OPENSSL)
  set(TS_USE_OPENSSL ${USE_OPENSSL})
  target_include_directories(${PROJECT_NAME} SYSTEM
                             PUBLIC ${OPENSSL_INCLUDE_DIR})
  target_link_libraries(${PROJECT_NAME} ${OPENSSL_LIBRARIES})
endif(USE_OPENSSL)

configure_file(config.h.in config.h)
add_dependencies(${PROJECT_NAME} gitcheck)
include_directories(${CMAKE_CURRENT_SOURCE_DIR})
add_subdirectory(bgw)
add_subdirectory(net)
if(USE_TELEMETRY)
  add_subdirectory(telemetry)
endif()
add_subdirectory(loader)
add_subdirectory(bgw_policy)
add_subdirectory(compat)
add_subdirectory(ts_catalog)
add_subdirectory(import)
add_subdirectory(nodes)
add_subdirectory(planner)
