set(TEST_FILES
  alter.sql
  chunk_utils.sql
  chunks.sql
  cluster.sql
  constraint.sql
  copy.sql
  create_chunks.sql
  create_hypertable.sql
  create_table.sql
  cursor.sql
  ddl_alter_column.sql
  ddl_errors.sql
  delete.sql
  drop_schema.sql
  drop_owned.sql
  drop_extension.sql
  drop_hypertable.sql
  drop_rename_hypertable.sql
  dump_meta.sql
  extension.sql
  gapfill.sql
  hash.sql
  histogram_test.sql
  index.sql
  insert_single.sql
  lateral.sql
  pg_dump.sql
  pg_dump_unprivileged.sql
  plain.sql
  query.sql
  reindex.sql
  relocate_extension.sql
  reloptions.sql
  size_utils.sql
  tablespace.sql
  timestamp.sql
  triggers.sql
  truncate.sql
  update.sql
  upsert.sql
  util.sql
  vacuum.sql
  version.sql
  views.sql
)

# edition tests need community edition
if(NOT APACHE_ONLY)
  list(APPEND TEST_FILES
    edition.sql
    telemetry_community.sql
    )
#compression only for PG > 9
if (${PG_VERSION_MAJOR} GREATER "9")
  list(APPEND TEST_FILES
    telemetry_compression.sql
  )
endif ()
endif()

# tests that fail or are unreliable when run in parallel
# bgw tests need to run first otherwise they are flaky
set(SOLO_TESTS
  bgw_db_scheduler
  bgw_launcher
  alternate_users-9.6
  alternate_users-10
  alternate_users-11
  alternate_users-12
  net
  pg_dump_unprivileged
  telemetry
)

if (CMAKE_BUILD_TYPE MATCHES Debug)
  list(APPEND TEST_FILES
    bgw_launcher.sql
    bgw_db_scheduler.sql
    c_unit_tests.sql
    loader.sql
    metadata.sql
    net.sql
    symbol_conflict.sql
    telemetry.sql)
  if (USE_OPENSSL)
    list(APPEND TEST_FILES
      privacy.sql)
  endif (USE_OPENSSL)
endif (CMAKE_BUILD_TYPE MATCHES Debug)

if (${PG_VERSION_MAJOR} EQUAL "9" )
  list(APPEND TEST_FILES
    rowsecurity-9.6.sql
  )
else() #everything else gets generated_as_identity
  list(APPEND TEST_FILES
    generated_as_identity.sql
  )
endif ()
if(${PG_VERSION_MAJOR} EQUAL "10")
  list(APPEND TEST_FILES
    rowsecurity-10.sql
  )
endif()
if(${PG_VERSION_MAJOR} EQUAL "11")
  list(APPEND TEST_FILES
    rowsecurity-11.sql
    vacuum_multi.sql
  )
endif()

if (${PG_VERSION_MAJOR} GREATER_EQUAL "11")
  list(APPEND TEST_FILES
    join.sql
    partitionwise.sql
  )
endif()

if (${PG_VERSION_MAJOR} GREATER_EQUAL "12")
  list(APPEND TEST_FILES
    generated_columns.sql
    misc.sql
  )
endif()

set(TEST_TEMPLATES
  agg_bookends.sql.in
  append.sql.in
  alternate_users.sql.in
  ddl.sql.in
  ddl_single.sql.in
  insert.sql.in
  chunk_adaptive.sql.in
  insert_many.sql.in
  parallel.sql.in
  partition.sql.in
  partitioning.sql.in
  plan_expand_hypertable.sql.in
  #hashagg is different in 9.6 and 10 because of hashagg parallelism
  plan_hashagg.sql.in
  # lateral plans are different across versions, explains run without analyze on 9.6
  plan_ordered_append.sql.in
  # Query tests differ in how PostgreSQL deals with projections on partitioned tables.
  sort_optimization.sql.in
  sql_query.sql.in
  )

if (CMAKE_BUILD_TYPE MATCHES Debug)
  list(APPEND TEST_TEMPLATES
    multi_transaction_index.sql.in
  )
endif()

# only test custom type if we are in 64-bit architecture
if("${CMAKE_SIZEOF_VOID_P}" STREQUAL "8")
  list(APPEND TEST_TEMPLATES
    custom_type.sql.in)
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
