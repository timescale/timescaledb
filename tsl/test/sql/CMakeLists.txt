include(GenerateTestSchedule)

# These are the configurations. There has to be a file whatever.conf.in for each
# configuration name in this list and the corresponding variables use the
# configuration name as a suffix.
set(TEST_CONFIGURATIONS postgresql max_bgw_8)

# These are the files for the 'postgresql' configuration. This is the default,
# so unless you have a good reason, add new test files here.
set(TEST_FILES_postgresql
    bgw_custom.sql
    bgw_policy.sql
    compression_bgw.sql
    compression_permissions.sql
    compression_qualpushdown.sql
    cagg_errors.sql
    cagg_invalidation.sql
    cagg_permissions.sql
    cagg_policy.sql
    cagg_refresh.sql
    cagg_watermark.sql
    dist_views.sql
    exp_cagg_next_gen.sql
    exp_cagg_monthly.sql
    exp_cagg_origin.sql
    exp_cagg_timezone.sql
    move.sql
    partialize_finalize.sql
    reorder.sql
    skip_scan.sql)

if(CMAKE_BUILD_TYPE MATCHES Debug)
  list(
    APPEND
    TEST_FILES_postgresql
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
    cagg_bgw_drop_chunks.sql
    cagg_bgw.sql
    cagg_bgw_dist_ht.sql
    cagg_ddl.sql
    cagg_ddl_dist_ht.sql
    cagg_drop_chunks.sql
    cagg_dump.sql
    cagg_invalidation_dist_ht.sql
    cagg_multi.sql
    continuous_aggs.sql
    cagg_tableam.sql
    cagg_usage.sql
    cagg_policy_run.sql
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
    transparent_decompression_queries.sql
    tsl_tables.sql
    license.sql)
  if(USE_TELEMETRY)
    list(APPEND TEST_FILES_postgresql telemetry_stats.sql)
  endif()
endif(CMAKE_BUILD_TYPE MATCHES Debug)

set(SOLO_TESTS_postgresql
    bgw_reorder_drop_chunks
    compress_bgw_reorder_drop_chunks
    compression_ddl
    cagg_bgw
    cagg_ddl
    cagg_ddl_dist_ht
    cagg_dump
    data_fetcher
    dist_util
    move
    remote_connection_cache
    remote_copy
    remote_txn
    remote_txn_resolve
    reorder
    telemetry_stats)

# Test files for the max_bgw_8 configuration that configures TimescaleDB with a
# max of 8 TimescaleDB background workers.
set(TEST_FILES_max_bgw_8)
set(SOLO_TESTS_max_bgw_8 bgw_db_scheduler)
if(CMAKE_BUILD_TYPE MATCHES Debug)
  list(APPEND TEST_FILES_max_bgw_8 bgw_db_scheduler.sql dist_move_chunk.sql)
endif()

set(TEST_TEMPLATES
    compression_insert.sql.in cagg_union_view.sql.in plan_skip_scan.sql.in
    transparent_decompression.sql.in
    transparent_decompression_ordered_index.sql.in)

if(CMAKE_BUILD_TYPE MATCHES Debug)
  list(APPEND TEST_TEMPLATES cagg_query.sql.in dist_hypertable.sql.in
       remote_copy.sql.in dist_grant.sql.in)
endif(CMAKE_BUILD_TYPE MATCHES Debug)

# Check if PostgreSQL was compiled with JIT support
set(PG_CONFIG_H "${PG_INCLUDEDIR}/pg_config.h")
if(EXISTS ${PG_CONFIG_H})
  file(STRINGS "${PG_CONFIG_H}" PG_USE_LLVM
       REGEX "^#[\t ]*define[\t ]+USE_LLVM[\t ]+1.*")
  if(PG_USE_LLVM)
    list(APPEND TEST_TEMPLATES jit.sql.in)
  endif()
endif()

# Regression tests that vary with PostgreSQL version. Generated test files are
# put in the original source directory since all tests must be in the same
# directory. These files are updated when the template is edited, but not when
# the output file is deleted. If the output is deleted either recreate it
# manually, or rerun cmake on the root dir.
foreach(TEMPLATE_FILE ${TEST_TEMPLATES})
  string(LENGTH ${TEMPLATE_FILE} TEMPLATE_NAME_LEN)
  math(EXPR TEMPLATE_NAME_LEN ${TEMPLATE_NAME_LEN}-7)
  string(SUBSTRING ${TEMPLATE_FILE} 0 ${TEMPLATE_NAME_LEN} TEMPLATE)
  set(TEST_FILE ${TEMPLATE}-${TEST_VERSION_SUFFIX}.sql)
  configure_file(${TEMPLATE_FILE} ${CMAKE_CURRENT_SOURCE_DIR}/${TEST_FILE}
                 COPYONLY)
  list(APPEND TEST_FILES_postgresql ${TEST_FILE})
endforeach(TEMPLATE_FILE)

if(NOT TEST_GROUP_SIZE)
  set(PARALLEL_GROUP_SIZE 10)
else()
  set(PARALLEL_GROUP_SIZE ${TEST_GROUP_SIZE})
endif()

# Generate a test schedule for each configuration.
foreach(_conf ${TEST_CONFIGURATIONS})
  generate_test_schedule(
    ${TEST_SCHEDULE}_${_conf}
    TEST_FILES
    ${TEST_FILES_${_conf}}
    SOLO
    ${SOLO_TESTS_${_conf}}
    GROUP_SIZE
    ${PARALLEL_GROUP_SIZE})
endforeach()
