include(GenerateTestSchedule)

# These are the files for the 'postgresql' configuration. This is the default,
# so unless you have a good reason, add new test files here.
set(TEST_FILES
    agg_partials_pushdown.sql
    bgw_job_ddl.sql
    bgw_policy.sql
    bgw_security.sql
    cagg_api.sql
    cagg_config.sql
    cagg_deprecated_bucket_ng.sql
    cagg_errors.sql
    cagg_invalidation.sql
    cagg_invalidation_multi.sql
    cagg_policy.sql
    cagg_policy_move.sql
    cagg_policy_concurrent.sql
    cagg_plugin.sql
    cagg_refresh_using_trigger.sql
    cagg_refresh_using_wal.sql
    cagg_refresh_using_merge.sql
    cagg_utils.sql
    cagg_watermark.sql
    chunk_column_stats.sql
    columnar_scan_cost.sql
    columnstore_aliases.sql
    compress_auto_sparse_index.sql
    compress_bitmap_scan.sql
    compress_sparse_config.sql
    compress_default.sql
    compress_dml_copy.sql
    compress_float8_corrupt.sql
    compress_qualpushdown_saop.sql
    compress_sort_transform.sql
    compressed_collation.sql
    compressed_detoaster.sql
    direct_compress_insert.sql
    compression.sql
    compression_allocation.sql
    compression_conflicts.sql
    compression_constraints.sql
    compression_create_compressed_table.sql
    compression_defaults.sql
    compression_fks.sql
    compression_indexcreate.sql
    compression_insert.sql
    compression_null_dump_restore.sql
    compression_nulls_and_defaults.sql
    compression_policy.sql
    compression_qualpushdown.sql
    compression_sequence_num_removal.sql
    compression_settings.sql
    compression_sorted_merge.sql
    compression_sorted_merge_distinct.sql
    compression_uuid.sql
    compression_trigger.sql
    create_table_with.sql
    custom_hashagg.sql
    decompress_index.sql
    merge_compress.sql
    move.sql
    plan_skip_scan_notnull.sql
    policy_generalization.sql
    recompression_integrity_tests.sql
    reorder.sql
    size_utils_tsl.sql
    skip_scan.sql
    split_chunk.sql
    transparent_decompression_join_index.sql
    unlogged.sql
    vector_agg_functions.sql
    vector_agg_groupagg.sql
    vector_agg_param.sql
    vectorized_aggregation.sql)

set(TEST_TEMPLATES
    cagg_query.sql.in
    cagg_query_using_merge.sql.in
    cagg_union_view.sql.in
    cagg_permissions.sql.in
    compress_bloom_sparse.sql.in
    compression_permissions.sql.in
    foreign_keys_test.sql.in
    modify_exclusion.sql.in
    ordered_append.sql.in
    plan_skip_scan.sql.in
    transparent_decompression.sql.in
    transparent_decompression_ordered_index.sql.in)

if(USE_TELEMETRY)
  list(APPEND TEST_FILES bgw_telemetry.sql)
endif()

if(NOT APPLE)
  list(APPEND TEST_FILES direct_compress_copy.sql)
endif()

if(CMAKE_BUILD_TYPE MATCHES Debug)
  list(
    APPEND
    TEST_FILES
    attach_chunk.sql
    bgw_custom.sql
    bgw_db_scheduler.sql
    bgw_job_stat_history.sql
    bgw_job_stat_history_errors.sql
    bgw_job_stat_history_errors_permissions.sql
    bgw_db_scheduler_fixed.sql
    bgw_scheduler_control.sql
    bgw_scheduler_restart.sql
    bgw_reorder_drop_chunks.sql
    scheduler_fixed.sql
    cagg_policy_incremental.sql
    compress_bgw_reorder_drop_chunks.sql
    chunk_api.sql
    chunk_merge.sql
    chunk_utils_compression.sql
    chunk_utils_internal.sql
    compress_bloom_sparse_debug.sql
    compression_algos.sql
    compression_bgw.sql
    compression_bools.sql
    compression_bool_vectorized.sql
    compression_ddl.sql
    compression_errors.sql
    compression_hypertable.sql
    compression_merge.sql
    compression_indexscan.sql
    compression_segment_meta.sql
    compression_sorted_merge_columns.sql
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
    detach_chunk.sql
    exp_cagg_monthly.sql
    exp_cagg_next_gen.sql
    exp_cagg_origin.sql
    exp_cagg_timezone.sql
    hypertable_generalization.sql
    insert_memory_usage.sql
    information_view_chunk_count.sql
    read_only.sql
    tsl_tables.sql
    license_tsl.sql
    merge_chunks.sql
    fixed_schedules.sql
    partialize_finalize.sql
    recompress_chunk_segmentwise.sql
    feature_flags.sql
    vector_agg_byte.sql
    vector_agg_default.sql
    vector_agg_filter.sql
    vector_agg_grouping.sql
    vector_agg_text.sql
    vector_agg_memory.sql
    vector_agg_segmentby.sql
    vector_agg_uuid.sql)

  list(
    APPEND
    TEST_TEMPLATES
    cagg_bgw.sql.in
    cagg_ddl.sql.in
    cagg_migrate_function.sql.in
    cagg_repair.sql.in
    cagg_usage.sql.in
    compression_update_delete.sql.in
    continuous_aggs.sql.in
    transparent_decompression_queries.sql.in)

  if(USE_TELEMETRY)
    list(APPEND TEST_FILES telemetry_stats.sql)
  endif()
endif(CMAKE_BUILD_TYPE MATCHES Debug)

if((${PG_VERSION_MAJOR} GREATER_EQUAL "16"))
  list(APPEND TEST_FILES cagg_planning.sql skip_scan_dagg.sql)
  list(APPEND TEST_TEMPLATES plan_skip_scan_dagg.sql.in)
endif()

if((${PG_VERSION_MAJOR} GREATER_EQUAL "17"))
  list(APPEND TEST_FILES privilege_maintain.sql
       merge_append_partially_compressed.sql)
endif()

set(SOLO_TESTS
    # This interferes with other tests since it reloads the config to increase
    # log level.
    bgw_custom
    bgw_scheduler_control
    bgw_scheduler_restart
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
    cagg_policy_incremental
    move
    reorder
    split_chunk
    telemetry_stats
    # These are solo tests because of a bug in PostgreSQL 17.5. A fix for it
    # seems to be scheduled for release with 17.6.
    #
    # See
    # https://github.com/postgres/postgres/commit/d87d07b7ad3b782cb74566cd771ecdb2823adf6a
    cagg_invalidation
    cagg_invalidation_multi
    cagg_plugin)

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
