# these tests need MODULE_NAME and cannot be added as is to TEST_FILES
set(TEST_TEMPLATES_MODULE reorder_deadlock.spec.in
                          reorder_vs_insert_other_chunk.spec.in)

set(TEST_TEMPLATES_MODULE_DEBUG
    reorder_vs_insert.spec.in reorder_vs_select.spec.in
    decompression_chunk_and_parallel_query.in)

# This one must go first because it is very dependent on chunk IDs in EXPLAIN.
if(CMAKE_BUILD_TYPE MATCHES Debug)
  set(TEST_FILES cagg_watermark_concurrent_update.spec)
endif()

list(
  APPEND
  TEST_FILES
  compression_ddl_iso.spec
  compression_conflicts_iso.spec
  cagg_insert.spec
  cagg_multi_iso.spec
  deadlock_drop_chunks_compress.spec
  deadlock_drop_index_vacuum.spec
  parallel_compression.spec
  osm_range_updates_iso.spec
  concurrent_decompress_update.spec
  bgw_job_stat_history_retention_isolation.spec)

if(CMAKE_BUILD_TYPE MATCHES Debug)
  list(APPEND TEST_TEMPLATES_MODULE ${TEST_TEMPLATES_MODULE_DEBUG})
  list(
    APPEND
    TEST_FILES
    bgw_job_duplicate_race.spec
    cagg_concurrent_move.spec
    cagg_concurrent_invalidation.spec
    cagg_concurrent_refresh.spec
    compression_chunk_race.spec
    direct_compress_copy.spec
    compression_freeze.spec
    compression_merge_race.spec
    compression_recompress.spec
    decompression_chunk_and_parallel_query_wo_idx.spec
    merge_chunks_concurrent.spec
    split_chunk_concurrent.spec
    freeze_chunk.spec
    compression_dml_iso.spec
    # These tests are using markers for the isolation tests (to avoid race
    # conditions causing differing output), which were added after 13.3 and in
    # all later major versions.
    deadlock_recompress_chunk.spec
    detach_chunk_isolation.spec
    attach_chunk_isolation.spec)

endif(CMAKE_BUILD_TYPE MATCHES Debug)

# need to generate MODULE name for the .spec files
foreach(TEMP_FILE ${TEST_TEMPLATES_MODULE})
  get_filename_component(TEMPLATE ${TEMP_FILE} NAME_WE)
  set(TEST_FILE ${TEMPLATE}.spec)
  configure_file(${TEMP_FILE} ${TEST_FILE})
  list(APPEND TEST_FILES "${TEST_FILE}")
endforeach(TEMP_FILE)

# generate PG version specific .spec file
foreach(TEMPLATE_FILE ${TEST_TEMPLATES})
  get_filename_component(TEMPLATE ${TEMPLATE_FILE} NAME_WE)
  set(TEST_FILE ${TEMPLATE}-${TEST_VERSION_SUFFIX}.spec)
  configure_file(${TEMPLATE_FILE} ${TEST_FILE})
  list(APPEND TEST_FILES "${TEST_FILE}")
endforeach(TEMPLATE_FILE)

file(REMOVE ${ISOLATION_TEST_SCHEDULE})

foreach(TEST_FILE ${TEST_FILES})
  string(REGEX REPLACE "(.+)\.spec" "\\1" TESTS_TO_RUN ${TEST_FILE})
  file(APPEND ${ISOLATION_TEST_SCHEDULE} "test: ${TESTS_TO_RUN}\n")
endforeach(TEST_FILE)
