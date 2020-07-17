set(TEST_FILES
  compression_permissions.sql
  continuous_aggs_errors.sql
  continuous_aggs_permissions.sql
  continuous_aggs_watermark.sql
  edition.sql
  gapfill.sql
  partialize_finalize.sql
  plan_gapfill.sql
)

set(TEST_FILES_DEBUG
  bgw_policy.sql
  bgw_reorder_drop_chunks.sql
  chunk_api.sql
  compress_bgw_reorder_drop_chunks.sql
  compression_algos.sql
  compression_bgw.sql
  compression_ddl.sql
  compression_errors.sql
  compression_hypertable.sql
  compression_segment_meta.sql
  compression.sql
  compress_table.sql
  continuous_aggs_bgw_drop_chunks.sql
  continuous_aggs_bgw.sql
  continuous_aggs_ddl.sql
  continuous_aggs_dump.sql
  continuous_aggs_materialize.sql
  continuous_aggs_multi.sql
  continuous_aggs.sql
  continuous_aggs_usage.sql
  data_fetcher.sql
  data_node_bootstrap.sql
  data_node.sql
  ddl_hook.sql
  debug_notice.sql
  deparse_fail.sql
  deparse.sql
  dist_commands.sql
  dist_compression.sql
  dist_ddl.sql
  dist_grant.sql
  dist_partial_agg.sql
  dist_util.sql
  issues.sql
  remote_connection_cache.sql
  remote_connection.sql
  remote_copy.sql
  remote_stmt_params.sql
  remote_txn_id.sql
  remote_txn_resolve.sql
  remote_txn.sql
  telemetry_distributed.sql
  transparent_decompression_queries.sql
  tsl_tables.sql
  read_only.sql
)

set(TEST_TEMPLATES
  compression_qualpushdown.sql.in
  continuous_aggs_union_view.sql.in
  move.sql.in
  reorder.sql.in
  transparent_decompression.sql.in
  transparent_decompression_ordered_index.sql.in
)

if (CMAKE_BUILD_TYPE MATCHES Debug)
  list(APPEND TEST_TEMPLATES
    #current_timestamp_mock available only in debug mode
    continuous_aggs_query.sql.in
    dist_hypertable.sql.in
    dist_query.sql.in
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

# oid systemcolumn is PG11 only
if (${PG_VERSION_MAJOR} EQUAL "11")
  list(APPEND TEST_FILES_DEBUG
    compression_with_oids.sql
    dist_hypertable_with_oids.sql
  )
endif()

if (${PG_VERSION_MAJOR} GREATER_EQUAL "12")
  list(APPEND TEST_FILES_DEBUG
    dist_hypertable_am.sql
  )
endif()

# the following tests will run by itself before the parallel
# tests because they fail or are flaky when run in parallel
set(SOLO_TESTS
  bgw_reorder_drop_chunks
  chunk_api
  compress_bgw_reorder_drop_chunks
  continuous_aggs_bgw
  continuous_aggs_dump
  data_fetcher
  data_node
  debug_notice
  dist_api_calls.sql
  dist_commands
  dist_compression
  dist_ddl
  dist_hypertable-11
  dist_hypertable-12
  dist_hypertable_am
  dist_hypertable_with_oids
  dist_partial_agg
  issues
  read_only
  remote_connection_cache
  remote_copy
  remote_txn
  remote_txn_resolve
  reorder-11
  reorder-12
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
  set(PARALLEL_GROUP_SIZE 20)
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

