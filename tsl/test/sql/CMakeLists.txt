set(TEST_FILES
  bgw_custom.sql
  bgw_policy.sql
  compression_bgw.sql
  compression_permissions.sql
  compression_qualpushdown.sql
  continuous_aggs_errors.sql
  continuous_aggs_invalidation.sql
  continuous_aggs_permissions.sql
  continuous_aggs_policy.sql
  continuous_aggs_refresh.sql
  continuous_aggs_watermark.sql
  dist_views.sql
  move.sql
  partialize_finalize.sql
  reorder.sql
  skip_scan.sql
)

set(TEST_FILES_DEBUG
  bgw_db_scheduler.sql
  bgw_reorder_drop_chunks.sql
  compress_bgw_reorder_drop_chunks.sql
  chunk_api.sql
  chunk_utils_compression.sql
  compression_algos.sql
  compression_ddl.sql
  compression_errors.sql
  compression_hypertable.sql
  compression_segment_meta.sql
  compression.sql
  compress_table.sql
  continuous_aggs_bgw_drop_chunks.sql
  continuous_aggs_bgw.sql
  continuous_aggs_ddl.sql
  continuous_aggs_drop_chunks.sql
  continuous_aggs_dump.sql
  continuous_aggs_multi.sql
  continuous_aggs.sql
  continuous_aggs_tableam.sql
  continuous_aggs_usage.sql
  continuous_aggs_policy_run.sql
  data_fetcher.sql
  data_node_bootstrap.sql
  data_node.sql
  ddl_hook.sql
  debug_notice.sql
  deparse.sql
  dist_api_calls.sql
  dist_commands.sql
  dist_compression.sql
  dist_ddl.sql
  dist_grant.sql
  dist_hypertable_pg12.sql
  dist_partial_agg.sql
  dist_policy.sql
  dist_query.sql
  dist_util.sql
  dist_triggers.sql
  dist_backup.sql
  read_only.sql
  remote_connection_cache.sql
  remote_connection.sql
  remote_stmt_params.sql
  remote_txn_id.sql
  remote_txn_resolve.sql
  remote_txn.sql
  telemetry_community.sql
  telemetry_distributed.sql
  transparent_decompression_queries.sql
  tsl_tables.sql
  license.sql
)

set(TEST_TEMPLATES
  compression_insert.sql.in
  continuous_aggs_union_view.sql.in
  plan_skip_scan.sql.in
  transparent_decompression.sql.in
  transparent_decompression_ordered_index.sql.in
)

if (CMAKE_BUILD_TYPE MATCHES Debug)
  list(APPEND TEST_TEMPLATES
    continuous_aggs_query.sql.in
    dist_hypertable.sql.in
    remote_copy.sql.in
  )
endif(CMAKE_BUILD_TYPE MATCHES Debug)

# Check if PostgreSQL was compiled with JIT support
set(PG_CONFIG_H "${PG_INCLUDEDIR}/pg_config.h")
if (EXISTS ${PG_CONFIG_H})
  file(STRINGS "${PG_CONFIG_H}" PG_USE_LLVM
       REGEX "^#[\t ]*define[\t ]+USE_LLVM[\t ]+1.*")
  if (PG_USE_LLVM)
    list(APPEND TEST_TEMPLATES jit.sql.in)
  endif()
endif()

# the following tests will run by itself before the parallel
# tests because they fail or are flaky when run in parallel
# dist_views.sql sets some global information, so do not run it
# in parallel
set(SOLO_TESTS
  bgw_db_scheduler
  bgw_reorder_drop_chunks
  compress_bgw_reorder_drop_chunks
  compression_ddl
  continuous_aggs_bgw
  continuous_aggs_ddl
  continuous_aggs_dump
  data_fetcher
  dist_util
  move
  remote_connection_cache
  remote_copy
  remote_txn
  remote_txn_resolve
  reorder
  telemetry_distributed
)

# Regression tests that vary with PostgreSQL version. Generated test
# files are put in the original source directory since all tests must
# be in the same directory. These files are updated when the template
# is edited, but not when the output file is deleted. If the output is
# deleted either recreate it manually, or rerun cmake on the root dir.
foreach(TEMPLATE_FILE ${TEST_TEMPLATES})
  string(LENGTH ${TEMPLATE_FILE} TEMPLATE_NAME_LEN)
  math(EXPR TEMPLATE_NAME_LEN ${TEMPLATE_NAME_LEN}-7)
  string(SUBSTRING ${TEMPLATE_FILE} 0 ${TEMPLATE_NAME_LEN} TEMPLATE)
  set(TEST_FILE ${TEMPLATE}-${TEST_VERSION_SUFFIX}.sql)
  configure_file(${TEMPLATE_FILE} ${CMAKE_CURRENT_SOURCE_DIR}/${TEST_FILE} COPYONLY)
  list(APPEND TEST_FILES ${TEST_FILE})
endforeach(TEMPLATE_FILE)

if (CMAKE_BUILD_TYPE MATCHES Debug)
  list(APPEND TEST_FILES ${TEST_FILES_DEBUG})
endif(CMAKE_BUILD_TYPE MATCHES Debug)

list(SORT TEST_FILES)
file(REMOVE ${TEST_SCHEDULE})

if(NOT TEST_GROUP_SIZE)
  set(PARALLEL_GROUP_SIZE 10)
else()
  set(PARALLEL_GROUP_SIZE ${TEST_GROUP_SIZE})
endif()

# append solo tests to schedule first
foreach(SOLO_TEST_NAME ${SOLO_TESTS})
  string(REGEX REPLACE "(.+)" "\\1.sql" TEST_FILE ${SOLO_TEST_NAME})
  if(TEST_FILE IN_LIST TEST_FILES)
    file(APPEND ${TEST_SCHEDULE} "test: ${SOLO_TEST_NAME}\n")
  endif ()
endforeach(SOLO_TEST_NAME)

set(GROUP_MEMBERS 0)
foreach(TEST_FILE ${TEST_FILES})
  string(REGEX REPLACE "(.+)\.sql" "\\1" TESTS_TO_RUN ${TEST_FILE})
  if(NOT (TESTS_TO_RUN IN_LIST SOLO_TESTS))
    if(GROUP_MEMBERS EQUAL 0)
      file(APPEND ${TEST_SCHEDULE} "\ntest: ")
    endif()
    file(APPEND ${TEST_SCHEDULE} "${TESTS_TO_RUN} ")
    MATH(EXPR GROUP_MEMBERS "(${GROUP_MEMBERS}+1)%${PARALLEL_GROUP_SIZE}")
  endif()
endforeach(TEST_FILE)
file(APPEND ${TEST_SCHEDULE} "\n")
