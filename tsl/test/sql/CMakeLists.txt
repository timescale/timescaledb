include(GenerateTestSchedule)

# These are the files for the 'postgresql' configuration. This is the default,
# so unless you have a good reason, add new test files here.
set(TEST_FILES
    agg_partials_pushdown.sql
    bgw_custom.sql
    bgw_security.sql
    bgw_policy.sql
    cagg_errors.sql
    cagg_invalidation.sql
    cagg_permissions.sql
    cagg_policy.sql
    cagg_refresh.sql
    cagg_watermark.sql
    compressed_collation.sql
    compression_create_compressed_table.sql
    compression_bgw.sql
    compression_conflicts.sql
    compression_insert.sql
    compression_policy.sql
    compression_qualpushdown.sql
    exp_cagg_monthly.sql
    exp_cagg_next_gen.sql
    exp_cagg_origin.sql
    exp_cagg_timezone.sql
    move.sql
    partialize_finalize.sql
    reorder.sql
    skip_scan.sql
    size_utils_tsl.sql)

if(ENABLE_MULTINODE_TESTS)
  list(APPEND TEST_FILES dist_param.sql dist_views.sql)
endif()

if(USE_TELEMETRY)
  list(APPEND TEST_FILES bgw_telemetry.sql)
endif()

if(CMAKE_BUILD_TYPE MATCHES Debug)
  list(
    APPEND
    TEST_FILES
    bgw_db_scheduler.sql
    job_errors_permissions.sql
    troubleshooting_job_errors.sql
    bgw_db_scheduler_fixed.sql
    bgw_reorder_drop_chunks.sql
    scheduler_fixed.sql
    compress_bgw_reorder_drop_chunks.sql
    chunk_api.sql
    chunk_merge.sql
    chunk_utils_compression.sql
    compression_algos.sql
    compression_ddl.sql
    compression_errors.sql
    compression_hypertable.sql
    compression_merge.sql
    compression_indexscan.sql
    compression_segment_meta.sql
    compress_sorted_merge_filter.sql
    compress_table.sql
    cagg_bgw_drop_chunks.sql
    cagg_bgw.sql
    cagg_ddl.sql
    cagg_drop_chunks.sql
    cagg_dump.sql
    cagg_errors_deprecated.sql
    cagg_joins.sql
    cagg_migrate.sql
    cagg_multi.sql
    cagg_on_cagg.sql
    cagg_on_cagg_joins.sql
    cagg_tableam.sql
    cagg_usage.sql
    cagg_policy_run.sql
    cagg_repair.sql
    data_fetcher.sql
    data_node_bootstrap.sql
    data_node.sql
    ddl_hook.sql
    debug_notice.sql
    decompress_vector_qual.sql
    deparse.sql
    hypertable_generalization.sql
    insert_memory_usage.sql
    information_view_chunk_count.sql
    read_only.sql
    transparent_decompression_queries.sql
    tsl_tables.sql
    license_tsl.sql
    fixed_schedules.sql
    recompress_chunk_segmentwise.sql
    transparent_decompression_join_index.sql
    feature_flags.sql)

  if(ENABLE_MULTINODE_TESTS)
    list(
      APPEND
      TEST_FILES
      cagg_bgw_dist_ht.sql
      cagg_migrate_dist_ht.sql
      cagg_ddl_dist_ht.sql
      cagg_on_cagg_dist_ht.sql
      cagg_on_cagg_joins_dist_ht.sql
      dist_api_calls.sql
      dist_commands.sql
      dist_compression.sql
      dist_copy_available_dns.sql
      dist_copy_format_long.sql
      dist_copy_long.sql
      dist_ddl.sql
      dist_cagg.sql
      dist_move_chunk.sql
      dist_policy.sql
      dist_util.sql
      dist_triggers.sql
      dist_backup.sql
      remote_connection_cache.sql
      remote_connection.sql
      remote_copy.sql
      remote_stmt_params.sql
      remote_txn_id.sql
      remote_txn_resolve.sql
      remote_txn.sql)
  endif()

endif(CMAKE_BUILD_TYPE MATCHES Debug)

if((${PG_VERSION_MAJOR} GREATER_EQUAL "14"))
  if(CMAKE_BUILD_TYPE MATCHES Debug)
    list(APPEND TEST_FILES chunk_utils_internal.sql)
  endif()
  list(APPEND TEST_FILES compression.sql compression_update_delete.sql
       compression_permissions.sql)
endif()

if((${PG_VERSION_MAJOR} GREATER_EQUAL "15"))
  list(APPEND TEST_FILES merge_compress.sql)
endif()

set(SOLO_TESTS
    # dist_hypertable needs a lot of memory when the Sanitizer is active
    dist_hypertable-${PG_VERSION_MAJOR}
    bgw_db_scheduler
    job_errors_permissions
    troubleshooting_job_errors
    bgw_db_scheduler_fixed
    bgw_reorder_drop_chunks
    scheduler_fixed
    compress_bgw_reorder_drop_chunks
    compression_ddl
    cagg_bgw
    cagg_bgw_dist_ht
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
    telemetry_stats-${PG_VERSION_MAJOR})
# In PG versions 15.0 to 15.2, dist_move_chunk can cause a deadlock when run in
# parallel with other tests as mentioned in #4972.
if(${PG_VERSION_MAJOR} EQUAL "15" AND ${PG_VERSION_MINOR} LESS "3")
  list(APPEND SOLO_TESTS dist_move_chunk)
endif()

set(TEST_TEMPLATES
    compression_sorted_merge.sql.in
    cagg_union_view.sql.in
    plan_skip_scan.sql.in
    transparent_decompression.sql.in
    transparent_decompression_ordered_index.sql.in
    merge_append_partially_compressed.sql.in)

# This test runs only with PG version >= 14
if((${PG_VERSION_MAJOR} GREATER_EQUAL "14"))
  set(TEST_FILES_ON_VERSION_GE_14 modify_exclusion.sql.in)
endif()

if(CMAKE_BUILD_TYPE MATCHES Debug)
  list(APPEND TEST_TEMPLATES cagg_query.sql.in continuous_aggs.sql.in
       continuous_aggs_deprecated.sql.in)
  if(ENABLE_MULTINODE_TESTS)
    list(
      APPEND
      TEST_TEMPLATES
      cagg_invalidation_dist_ht.sql.in
      dist_hypertable.sql.in
      dist_grant.sql.in
      dist_ref_table_join.sql.in
      dist_remote_error.sql.in
      dist_partial_agg.sql.in
      dist_query.sql.in)
  endif()
  if(USE_TELEMETRY)
    list(APPEND TEST_TEMPLATES telemetry_stats.sql.in)
  endif()
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
  list(APPEND TEST_FILES ${TEST_FILE})
endforeach(TEMPLATE_FILE)

foreach(TEST_FILES_GE_14 ${TEST_FILES_ON_VERSION_GE_14})
  string(LENGTH ${TEST_FILES_GE_14} TEST_FILES_GE_14_NAME_LEN)
  math(EXPR TEST_FILES_GE_14_NAME_LEN ${TEST_FILES_GE_14_NAME_LEN}-7)
  string(SUBSTRING ${TEST_FILES_GE_14} 0 ${TEST_FILES_GE_14_NAME_LEN} TEMPLATE)
  set(TEST_FILE ${TEMPLATE}-${TEST_VERSION_SUFFIX}.sql)
  configure_file(${TEST_FILES_GE_14} ${CMAKE_CURRENT_SOURCE_DIR}/${TEST_FILE}
                 COPYONLY)
  list(APPEND TEST_FILES ${TEST_FILE})
endforeach(TEST_FILES_GE_14)

if(NOT TEST_GROUP_SIZE)
  set(PARALLEL_GROUP_SIZE 10)
else()
  set(PARALLEL_GROUP_SIZE ${TEST_GROUP_SIZE})
endif()

# Generate a test schedule
generate_test_schedule(
  ${TEST_SCHEDULE}
  TEST_FILES
  ${TEST_FILES}
  SOLO
  ${SOLO_TESTS}
  GROUP_SIZE
  ${PARALLEL_GROUP_SIZE})
