set(TEST_FILES
    continuous_aggs_dump.sql
    continuous_aggs_errors.sql
    continuous_aggs_usage.sql
    continuous_aggs_watermark.sql
    edition.sql
    gapfill.sql
    reorder.sql
    partialize_finalize.sql
)

set(TEST_FILES_DEBUG
    bgw_policy.sql
    bgw_reorder_drop_chunks.sql
    continuous_aggs.sql
    continuous_aggs_bgw.sql
    continuous_aggs_materialize.sql
    continuous_aggs_multi.sql
    ddl_hook.sql
    tsl_tables.sql
)

set(TEST_TEMPLATES
  # parallel plans are different between postgres versions
  plan_gapfill.sql.in
  continuous_aggs_ddl.sql.in
  continuous_aggs_permissions.sql.in
  )

# Regression tests that vary with PostgreSQL version. Generated test
# files are put in the original source directory since all tests must
# be in the same directory. These files are updated when the template
# is edited, but not when the output file is deleted. If the output is
# deleted either recreate it manually, or rerun cmake on the root dir.
if (${PG_VERSION_MAJOR} GREATER "9")
    set(TEST_VERSION_SUFFIX ${PG_VERSION_MAJOR})
else ()
    set(TEST_VERSION_SUFFIX ${PG_VERSION_MAJOR}.${PG_VERSION_MINOR})
endif ()

foreach(TEMPLATE_FILE ${TEST_TEMPLATES})
  string(LENGTH ${TEMPLATE_FILE} TEMPLATE_NAME_LEN)
  math(EXPR TEMPLATE_NAME_LEN ${TEMPLATE_NAME_LEN}-7)
  string(SUBSTRING ${TEMPLATE_FILE} 0 ${TEMPLATE_NAME_LEN} TEMPLATE)
  set(TEST_FILE ${TEMPLATE}-${TEST_VERSION_SUFFIX}.sql)
  configure_file(${TEMPLATE_FILE} ${CMAKE_CURRENT_SOURCE_DIR}/${TEST_FILE} COPYONLY)
  list(APPEND TEST_FILES ${TEST_FILE})
endforeach(TEMPLATE_FILE)

if (NOT ${PG_VERSION} VERSION_LESS "11")
  list(APPEND TEST_FILES
    chunk_api.sql
    timescaledb_fdw.sql
    dist_query.sql
    partitionwise_distributed.sql
    hypertable_distributed.sql
    ddl_distributed.sql
    dist_partial_agg.sql
  )
  list(APPEND TEST_FILES_DEBUG
    remote_connection.sql
    remote_connection_cache.sql
    remote_txn.sql
    remote_txn_id.sql
    data_node.sql
    data_node_bootstrap.sql
    dist_commands.sql
    deparse.sql
    deparse_fail.sql
    remote_txn_resolve.sql
    remote_stmt_params.sql
    remote_copy.sql
    dist_util.sql
  )
endif ()

if (CMAKE_BUILD_TYPE MATCHES Debug)
  list(APPEND TEST_FILES ${TEST_FILES_DEBUG})
endif(CMAKE_BUILD_TYPE MATCHES Debug)

list(SORT TEST_FILES)
file(REMOVE ${TEST_SCHEDULE})

foreach(TEST_FILE ${TEST_FILES})
  string(REGEX REPLACE "(.+)\.sql" "\\1" TESTS_TO_RUN ${TEST_FILE})
  file(APPEND ${TEST_SCHEDULE} "test: ${TESTS_TO_RUN}\n")
endforeach(TEST_FILE)
