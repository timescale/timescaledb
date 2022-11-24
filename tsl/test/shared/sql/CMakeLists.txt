set(TEST_FILES_SHARED
    cagg_compression.sql
    classify_relation.sql
    constify_timestamptz_op_interval.sql
    constraint_exclusion_prepared.sql
    decompress_placeholdervar.sql
    dist_chunk.sql
    dist_distinct_pushdown.sql
    dist_gapfill.sql
    dist_insert.sql
    dist_queries.sql
    extension.sql
    subtract_integer_from_now.sql)

set(TEST_TEMPLATES_SHARED
    decompress_join.sql.in
    dist_fetcher_type.sql.in
    gapfill.sql.in
    generated_columns.sql.in
    ordered_append.sql.in
    ordered_append_join.sql.in
    transparent_decompress_chunk.sql.in
    dist_distinct.sql.in)

if((${PG_VERSION_MAJOR} GREATER_EQUAL "14"))
  list(APPEND TEST_FILES_SHARED compression_dml.sql memoize.sql)
endif()

if(CMAKE_BUILD_TYPE MATCHES Debug)
  list(APPEND TEST_FILES_SHARED dist_parallel_agg.sql timestamp_limits.sql
       with_clause_parser.sql)
  list(APPEND TEST_TEMPLATES_SHARED constify_now.sql.in space_constraint.sql.in
       dist_remote_error.sql.in)
endif(CMAKE_BUILD_TYPE MATCHES Debug)

# Regression tests that vary with PostgreSQL version. Generated test files are
# put in the original source directory since all tests must be in the same
# directory. These files are updated when the template is edited, but not when
# the output file is deleted. If the output is deleted either recreate it
# manually, or rerun cmake on the root dir.
foreach(TEMPLATE_FILE ${TEST_TEMPLATES_SHARED})
  string(LENGTH ${TEMPLATE_FILE} TEMPLATE_NAME_LEN)
  math(EXPR TEMPLATE_NAME_LEN ${TEMPLATE_NAME_LEN}-7)
  string(SUBSTRING ${TEMPLATE_FILE} 0 ${TEMPLATE_NAME_LEN} TEMPLATE)
  set(TEST_FILE ${TEMPLATE}-${TEST_VERSION_SUFFIX}.sql)
  configure_file(${TEMPLATE_FILE} ${CMAKE_CURRENT_SOURCE_DIR}/${TEST_FILE}
                 COPYONLY)
  list(APPEND TEST_FILES_SHARED ${TEST_FILE})
endforeach(TEMPLATE_FILE)

if(DEFINED GIT_FOUND)
  list(APPEND TEST_FILES_SHARED build_info.sql)
endif()

if(TEST_FILES_SHARED)
  list(SORT TEST_FILES_SHARED)
endif()
file(REMOVE ${TEST_SCHEDULE_SHARED})

if(NOT TEST_GROUP_SIZE)
  set(PARALLEL_GROUP_SIZE 20)
else()
  set(PARALLEL_GROUP_SIZE ${TEST_GROUP_SIZE})
endif()

set(GROUP_MEMBERS 0)
foreach(TEST_FILE ${TEST_FILES_SHARED})
  string(REGEX REPLACE "(.+)\.sql" "\\1" TESTS_TO_RUN ${TEST_FILE})
  if(GROUP_MEMBERS EQUAL 0)
    file(APPEND ${TEST_SCHEDULE_SHARED} "\ntest: ")
  endif()
  file(APPEND ${TEST_SCHEDULE_SHARED} "${TESTS_TO_RUN} ")
  math(EXPR GROUP_MEMBERS "(${GROUP_MEMBERS}+1)%${PARALLEL_GROUP_SIZE}")
endforeach(TEST_FILE)
file(APPEND ${TEST_SCHEDULE_SHARED} "\n")
