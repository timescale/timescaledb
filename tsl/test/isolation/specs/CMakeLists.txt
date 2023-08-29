# these tests need MODULE_NAME and cannot be added as is to TEST_FILES
set(TEST_TEMPLATES_MODULE reorder_deadlock.spec.in
                          reorder_vs_insert_other_chunk.spec.in)

set(TEST_TEMPLATES_MODULE_DEBUG
    reorder_vs_insert.spec.in reorder_vs_select.spec.in
    dist_su_copy_chunk.spec.in dist_cmd_exec.spec.in
    decompression_chunk_and_parallel_query.in)

list(
  APPEND
  TEST_FILES
  compression_ddl_iso.spec
  compression_conflicts_iso.spec
  cagg_insert.spec
  cagg_multi_iso.spec
  cagg_concurrent_refresh.spec
  cagg_concurrent_refresh_dist_ht.spec
  deadlock_drop_chunks_compress.spec)

if(PG_VERSION VERSION_GREATER_EQUAL "14.0")
  list(APPEND TEST_FILES concurrent_decompress_update.spec)
endif()

if(CMAKE_BUILD_TYPE MATCHES Debug)
  list(APPEND TEST_TEMPLATES_MODULE ${TEST_TEMPLATES_MODULE_DEBUG})
  list(
    APPEND
    TEST_FILES
    cagg_drop_chunks_iso.spec
    cagg_multi_dist_ht.spec
    compression_chunk_race.spec
    compression_merge_race.spec
    decompression_chunk_and_parallel_query_wo_idx.spec
    dist_ha_chunk_drop.spec
    dist_ha_chunk_drop.spec
    dist_restore_point.spec
    remote_create_chunk.spec)
  if(PG_VERSION VERSION_GREATER_EQUAL "14.0")
    list(APPEND TEST_FILES freeze_chunk.spec compression_dml_iso.spec)
  endif()
  # These tests are using markers for the isolation tests (to avoid race
  # conditions causing differing output), which were added after 12.7, 13.3, and
  # 14.0.
  if(PG_VERSION VERSION_GREATER "12.7"
     OR PG_VERSION VERSION_GREATER "13.3"
     OR PG_VERSION VERSION_GREATER_EQUAL "14.0")
    list(APPEND TEST_FILES deadlock_recompress_chunk.spec)
  endif()

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

list(SORT TEST_FILES)

foreach(TEST_FILE ${TEST_FILES})
  string(REGEX REPLACE "(.+)\.spec" "\\1" TESTS_TO_RUN ${TEST_FILE})
  file(APPEND ${ISOLATION_TEST_SCHEDULE} "test: ${TESTS_TO_RUN}\n")
endforeach(TEST_FILE)
