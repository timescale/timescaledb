set(TEST_FILES
  agg_bookends.sql
  alternate_users.sql
  alter.sql
  append.sql
  append_unoptimized.sql
  append_x_diff.sql
  chunks.sql
  cluster.sql
  constraint.sql
  copy.sql
  create_chunks.sql
  create_hypertable.sql
  create_table.sql
  ddl_alter_column.sql
  ddl_errors.sql
  ddl_single.sql
  ddl.sql
  delete.sql
  drop_schema.sql
  drop_owned.sql
  drop_chunks.sql
  drop_extension.sql
  drop_hypertable.sql
  drop_rename_hypertable.sql
  dump_meta.sql
  extension.sql
  hash.sql
  histogram_test.sql
  index.sql
  insert_single.sql
  insert.sql
  partitioning.sql
  pg_dump.sql
  plain.sql
  plan_expand_hypertable_optimized.sql
  plan_expand_hypertable_results_diff.sql
  reindex.sql
  relocate_extension.sql
  reloptions.sql
  size_utils.sql
  sql_query_results_optimized.sql
  sql_query_results_unoptimized.sql
  sql_query_results_x_diff.sql
  sql_query.sql
  tablespace.sql
  timestamp.sql
  triggers.sql
  truncate.sql
  update.sql
  upsert.sql
  util.sql
  vacuum.sql
  version.sql)

IF(CMAKE_BUILD_TYPE MATCHES Debug)
  list(APPEND TEST_FILES
    loader.sql)
ENDIF(CMAKE_BUILD_TYPE MATCHES Debug)

set(TEST_TEMPLATES
  parallel.sql.in
  partitioning.sql.in)

# Regression tests that vary with PostgreSQL version. Generated test
# files are put in the original source directory since all tests must
# be in the same directory.
if (${PG_VERSION_MAJOR} EQUAL "10")
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

list(SORT TEST_FILES)
file(REMOVE ${TEST_SCHEDULE})

foreach(TEST_FILE ${TEST_FILES})
  string(REGEX REPLACE "(.+)\.sql" "\\1" TESTS_TO_RUN ${TEST_FILE})
  file(APPEND ${TEST_SCHEDULE} "test: ${TESTS_TO_RUN}\n")
endforeach(TEST_FILE)
