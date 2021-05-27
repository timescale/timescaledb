set(TEST_FILES
  alter.sql
  alternate_users.sql
  broken_tables.sql
  chunks.sql
  chunk_adaptive.sql
  chunk_utils.sql
  create_chunks.sql
  create_hypertable.sql
  create_table.sql
  cursor.sql
  ddl_errors.sql
  drop_extension.sql
  drop_hypertable.sql
  drop_owned.sql
  drop_rename_hypertable.sql
  drop_schema.sql
  dump_meta.sql
  extension.sql
  generated_as_identity.sql
  grant_hypertable.sql
  hash.sql
  histogram_test.sql
  index.sql
  insert_many.sql
  insert_single.sql
  join.sql
  lateral.sql
  misc.sql
  parallel.sql
  partition.sql
  partitioning.sql
  pg_dump.sql
  pg_dump_unprivileged.sql
  plain.sql
  plan_hashagg.sql
  plan_ordered_append.sql
  query.sql
  relocate_extension.sql
  reloptions.sql
  size_utils.sql
  sort_optimization.sql
  sql_query.sql
  tableam.sql
  tablespace.sql
  timestamp.sql
  triggers.sql
  truncate.sql
  upsert.sql
  util.sql
  vacuum.sql
  version.sql
  views.sql
  license.sql
)

set(TEST_TEMPLATES
  agg_bookends.sql.in
  append.sql.in
  insert.sql.in
  cluster.sql.in
  constraint.sql.in
  copy.sql.in
  ddl.sql.in
  delete.sql.in
  partitionwise.sql.in
  plan_expand_hypertable.sql.in
  plan_hypertable_inline.sql.in
  update.sql.in
)

# tests that fail or are unreliable when run in parallel
# bgw tests need to run first otherwise they are flaky
set(SOLO_TESTS
  alter
  alternate_users
  bgw_launcher
  chunk_utils
  index
  loader
  net
  pg_dump_unprivileged
  tablespace
  telemetry
)

if (CMAKE_BUILD_TYPE MATCHES Debug)
  list(APPEND TEST_FILES
    bgw_launcher.sql
    c_unit_tests.sql
    guc_options.sql
    loader.sql
    metadata.sql
    multi_transaction_index.sql
    net.sql
    symbol_conflict.sql
    telemetry.sql
    test_utils.sql
    timestamp_limits.sql
    with_clause_parser.sql
  )
endif (CMAKE_BUILD_TYPE MATCHES Debug)

if(${PG_VERSION_MAJOR} EQUAL "12")
  list(APPEND TEST_FILES
    rowsecurity-12.sql
  )
endif()

if(${PG_VERSION_MAJOR} EQUAL "13")
  list(APPEND TEST_FILES
    rowsecurity-13.sql
  )
endif()

if ((${PG_VERSION_MAJOR} GREATER_EQUAL "13"))
  list(APPEND TEST_FILES
    trusted_extension.sql
    vacuum_parallel.sql
  )
endif()

# only test custom type if we are in 64-bit architecture
if("${CMAKE_SIZEOF_VOID_P}" STREQUAL "8")
  list(APPEND TEST_FILES
    custom_type.sql)
endif()

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
  configure_file(${TEMPLATE_FILE} ${CMAKE_CURRENT_SOURCE_DIR}/${TEST_FILE} @ONLY)
  list(APPEND TEST_FILES ${TEST_FILE})
endforeach(TEMPLATE_FILE)

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

add_subdirectory(loader)
