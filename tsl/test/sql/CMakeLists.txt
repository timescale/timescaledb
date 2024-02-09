include(GenerateTestSchedule)

# These are the files for the 'postgresql' configuration. This is the default,
# so unless you have a good reason, add new test files here.
set(TEST_FILES
    agg_partials_pushdown.sql
    bgw_security.sql
    bgw_policy.sql
    bgw_job_ddl.sql
    cagg_deprecated_bucket_ng.sql
    cagg_errors.sql
    cagg_invalidation.sql
    cagg_policy.sql
    cagg_query.sql
    cagg_refresh.sql
    cagg_utils.sql
    cagg_watermark.sql
    compress_auto_sparse_index.sql
    compress_default.sql
    compress_dml_copy.sql
    compress_float8_corrupt.sql
    compressed_detoaster.sql
    compressed_collation.sql
    compression.sql
    compression_create_compressed_table.sql
    compression_conflicts.sql
    compression_defaults.sql
    compression_fks.sql
    compression_insert.sql
    compression_policy.sql
    compression_qualpushdown.sql
    compression_settings.sql
    compression_sequence_num_removal.sql
    compression_sorted_merge_distinct.sql
    compression_sorted_merge_columns.sql
    decompress_index.sql
    foreign_keys.sql
    move.sql
    partialize_finalize.sql
    policy_generalization.sql
    reorder.sql
    size_utils_tsl.sql
    skip_scan.sql
    transparent_decompression_join_index.sql
    vector_agg_functions.sql
    vector_agg_param.sql
    vectorized_aggregation.sql)

if(USE_TELEMETRY)
  list(APPEND TEST_FILES bgw_telemetry.sql)
endif()

if(CMAKE_BUILD_TYPE MATCHES Debug)
  list(
    APPEND
    TEST_FILES
    bgw_custom.sql
    bgw_db_scheduler.sql
    bgw_job_stat_history.sql
    bgw_job_stat_history_errors.sql
    bgw_job_stat_history_errors_permissions.sql
    bgw_db_scheduler_fixed.sql
    bgw_reorder_drop_chunks.sql
    scheduler_fixed.sql
    compress_bgw_reorder_drop_chunks.sql
    chunk_api.sql
    chunk_merge.sql
    chunk_utils_compression.sql
    chunk_utils_internal.sql
    compression_algos.sql
    compression_bgw.sql
    compression_ddl.sql
    compression_hypertable.sql
    compression_merge.sql
    compression_indexscan.sql
    compression_segment_meta.sql
    compression_sorted_merge_filter.sql
    cagg_bgw_drop_chunks.sql
    cagg_drop_chunks.sql
    cagg_dump.sql
    cagg_joins.sql
    cagg_migrate.sql
    cagg_multi.sql
    cagg_on_cagg.sql
    cagg_on_cagg_joins.sql
    cagg_tableam.sql
    cagg_policy_run.sql
    decompress_memory.sql
    decompress_vector_qual.sql
    exp_cagg_monthly.sql
    exp_cagg_next_gen.sql
    exp_cagg_origin.sql
    exp_cagg_timezone.sql
    hypertable_generalization.sql
    insert_memory_usage.sql
    information_view_chunk_count.sql
    read_only.sql
    transparent_decompression_queries.sql
    tsl_tables.sql
    license_tsl.sql
    fixed_schedules.sql
    recompress_chunk_segmentwise.sql
    feature_flags.sql
    vector_agg_default.sql
    vector_agg_segmentby.sql)
endif(CMAKE_BUILD_TYPE MATCHES Debug)

if((${PG_VERSION_MAJOR} GREATER_EQUAL "15"))
  if(CMAKE_BUILD_TYPE MATCHES Debug)
    list(APPEND TEST_FILES bgw_scheduler_control.sql)
  endif()
  list(
    APPEND
    TEST_FILES
    merge_compress.sql
    cagg_query_using_merge.sql
    cagg_refresh_using_merge.sql
    hyperstore.sql
    hyperstore_scans.sql)
endif()

if((${PG_VERSION_MAJOR} GREATER_EQUAL "17"))
  list(APPEND TEST_FILES privilege_maintain.sql)
endif()

set(SOLO_TESTS
    # This interferes with other tests since it reloads the config to increase
    # log level.
    bgw_custom
    bgw_scheduler_control
    bgw_db_scheduler
    bgw_job_stat_history_errors_permissions
    bgw_job_stat_history_errors
    bgw_job_stat_history
    bgw_db_scheduler_fixed
    bgw_reorder_drop_chunks
    scheduler_fixed
    compress_bgw_reorder_drop_chunks
    compression_ddl
    cagg_bgw
    cagg_ddl-${PG_VERSION_MAJOR}
    cagg_dump
    cagg_invalidation
    move
    reorder
    telemetry_stats)

set(TEST_TEMPLATES
    cagg_union_view.sql.in
    cagg_permissions.sql.in
    chunk_column_stats.sql.in
    compression_permissions.sql.in
    compression_sorted_merge.sql.in
    merge_append_partially_compressed.sql.in
    modify_exclusion.sql.in
    plan_skip_scan.sql.in
    transparent_decompression.sql.in
    transparent_decompression_ordered_index.sql.in)

if(CMAKE_BUILD_TYPE MATCHES Debug)
  list(
    APPEND
    TEST_TEMPLATES
    cagg_bgw.sql.in
    cagg_ddl.sql.in
    cagg_migrate_function.sql.in
    cagg_repair.sql.in
    cagg_usage.sql.in
    compression_errors.sql.in
    compression_update_delete.sql.in
    continuous_aggs.sql.in)
  if(USE_TELEMETRY)
    list(APPEND TEST_FILES telemetry_stats.sql)
  endif()
endif(CMAKE_BUILD_TYPE MATCHES Debug)

# Check if PostgreSQL was compiled with JIT support
set(PG_CONFIG_H "${PG_INCLUDEDIR}/pg_config.h")
if(EXISTS ${PG_CONFIG_H})
  file(STRINGS "${PG_CONFIG_H}" PG_USE_LLVM
       REGEX "^#[\t ]*define[\t ]+USE_LLVM[\t ]+1.*")
  if(PG_USE_LLVM)
    list(APPEND TEST_FILES jit.sql)
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
