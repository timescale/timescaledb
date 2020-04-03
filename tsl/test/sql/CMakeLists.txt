set(TEST_FILES
  continuous_aggs_errors.sql
  continuous_aggs_watermark.sql
  edition.sql
  gapfill.sql
  partialize_finalize.sql
)

set(TEST_FILES_DEBUG
  bgw_policy.sql
  bgw_reorder_drop_chunks.sql
  continuous_aggs.sql
  continuous_aggs_bgw.sql
  continuous_aggs_bgw_drop_chunks.sql
  continuous_aggs_ddl.sql
  continuous_aggs_dump.sql
  continuous_aggs_materialize.sql
  continuous_aggs_multi.sql
  continuous_aggs_usage.sql
  ddl_hook.sql
  tsl_tables.sql
)

set(TEST_TEMPLATES
  continuous_aggs_permissions.sql.in
  continuous_aggs_union_view.sql.in
  move.sql.in
  plan_gapfill.sql.in
  reorder.sql.in
)

# Check if PostgreSQL was compiled with JIT support
set(PG_CONFIG_H "${PG_INCLUDEDIR}/pg_config.h")
if (EXISTS ${PG_CONFIG_H})
  file(STRINGS "${PG_CONFIG_H}" PG_USE_LLVM
       REGEX "^#[\t ]*define[\t ]+USE_LLVM[\t ]+1.*")
  if (PG_USE_LLVM AND ((${PG_VERSION_MAJOR} EQUAL "11") OR (${PG_VERSION_MAJOR} GREATER "11")))
    list(APPEND TEST_TEMPLATES jit.sql.in)
  endif()
endif()

if (CMAKE_BUILD_TYPE MATCHES Debug)
  list(APPEND TEST_TEMPLATES
    #current_timestamp_mock available only in debug mode
    continuous_aggs_query.sql.in
    )
endif(CMAKE_BUILD_TYPE MATCHES Debug)

#compression only for PG > 9
if (${PG_VERSION_MAJOR} GREATER "9")

  list(APPEND TEST_FILES_DEBUG
    compress_table.sql
    compression.sql
    compression_algos.sql
    compression_ddl.sql
    compression_errors.sql
    compression_hypertable.sql
    compression_segment_meta.sql
    compression_bgw.sql
    compress_bgw_drop_chunks.sql
    transparent_decompression_queries.sql
  )

  list(APPEND TEST_TEMPLATES
    compression_permissions.sql.in
  )
  if (CMAKE_BUILD_TYPE MATCHES Debug)
    list(APPEND TEST_TEMPLATES
      transparent_decompression.sql.in
      )
  endif(CMAKE_BUILD_TYPE MATCHES Debug)

  if (${PG_VERSION_MAJOR} LESS "12")
    list(APPEND TEST_FILES_DEBUG
      compression_with_oids.sql
    )
  endif()

endif()

if (CMAKE_BUILD_TYPE MATCHES Debug)
  list(APPEND TEST_FILES ${TEST_FILES_DEBUG})
endif(CMAKE_BUILD_TYPE MATCHES Debug)

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

list(SORT TEST_FILES)
file(REMOVE ${TEST_SCHEDULE})

foreach(TEST_FILE ${TEST_FILES})
  string(REGEX REPLACE "(.+)\.sql" "\\1" TESTS_TO_RUN ${TEST_FILE})
  file(APPEND ${TEST_SCHEDULE} "test: ${TESTS_TO_RUN}\n")
endforeach(TEST_FILE)
