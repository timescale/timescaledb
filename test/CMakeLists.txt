# Testing support
find_program(PG_REGRESS pg_regress
  PATHS
  "${PG_PKGLIBDIR}/pgxs/src/test/regress/"
  REQUIRED)

message(STATUS "Using pg_regress ${PG_REGRESS}")

set(TEST_ROLE_SUPERUSER super_user)
set(TEST_ROLE_DEFAULT_PERM_USER default_perm_user)
set(TEST_ROLE_DEFAULT_PERM_USER_2 default_perm_user_2)

# Basic connection info for test instance
set(TEST_PGPORT_LOCAL 5432)
set(TEST_PGPORT_TEMP_INSTANCE 1432)
set(TEST_PGHOST localhost)
set(TEST_PGUSER ${TEST_ROLE_DEFAULT_PERM_USER})
set(TEST_DBNAME single)
set(TEST_INPUT_DIR ${CMAKE_CURRENT_SOURCE_DIR})
set(TEST_OUTPUT_DIR ${CMAKE_CURRENT_BINARY_DIR})
set(TEST_CLUSTER ${TEST_OUTPUT_DIR}/testcluster)
set(TEST_SCHEDULE ${CMAKE_CURRENT_BINARY_DIR}/test_schedule)

add_subdirectory(sql)

set(PG_REGRESS_OPTS_COMMON
  --create-role=${TEST_ROLE_SUPERUSER},${TEST_ROLE_DEFAULT_PERM_USER},${TEST_ROLE_DEFAULT_PERM_USER_2}
  --dbname=${TEST_DBNAME}
  --outputdir=${TEST_OUTPUT_DIR}
  --launcher=${TEST_INPUT_DIR}/runner.sh
  --host=${TEST_PGHOST}
  --load-language=plpgsql
  --dlpath=${PROJECT_BINARY_DIR}/src
  --inputdir=${TEST_INPUT_DIR})
SET(PG_REGRESS_ENV
  TEST_PGUSER=${TEST_PGUSER}
  TEST_ROLE_SUPERUSER=${TEST_ROLE_SUPERUSER}
  TEST_ROLE_DEFAULT_PERM_USER=${TEST_ROLE_DEFAULT_PERM_USER}
  TEST_ROLE_DEFAULT_PERM_USER_2=${TEST_ROLE_DEFAULT_PERM_USER_2}
  TEST_DBNAME=${TEST_DBNAME}
  TEST_INPUT_DIR=${TEST_INPUT_DIR}
  TEST_OUTPUT_DIR=${TEST_OUTPUT_DIR}
  TEST_SCHEDULE=${TEST_SCHEDULE}
  PG_REGRESS=${PG_REGRESS})

set(PG_REGRESS_OPTS_TEMP_INSTANCE
  --user=${TEST_PGUSER}
  --port=${TEST_PGPORT_TEMP_INSTANCE}
  --temp-instance=${TEST_CLUSTER}
  --temp-config=${TEST_OUTPUT_DIR}/postgresql.conf
  ${PG_REGRESS_OPTS_COMMON})

set(PG_REGRESS_OPTS_LOCAL
  --user=postgres
  --port=${TEST_PGPORT_LOCAL}
  ${PG_REGRESS_OPTS_COMMON})

file(MAKE_DIRECTORY ${TEST_OUTPUT_DIR})
file(WRITE ${TEST_OUTPUT_DIR}/postgresql.conf "shared_preload_libraries=timescaledb")

# installcheck starts up new temporary instances for testing code
add_custom_target(installcheck
  COMMAND ${CMAKE_COMMAND} -E env ${PG_REGRESS_ENV}
  ${CMAKE_CURRENT_SOURCE_DIR}/pg_regress.sh ${PG_REGRESS_OPTS_TEMP_INSTANCE}
  USES_TERMINAL)

# installchecklocal tests against an existing postgres instance
add_custom_target(installchecklocal
  COMMAND ${CMAKE_COMMAND} -E env ${PG_REGRESS_ENV}
  ${CMAKE_CURRENT_SOURCE_DIR}/pg_regress.sh ${PG_REGRESS_OPTS_LOCAL}
  USES_TERMINAL)

set(PG_REGRESS_DIR "" CACHE PATH "Path to postgresql src regress directory.")

if (NOT ${PG_REGRESS_DIR} STREQUAL "")

set(BASE_TEST_INPUT_DIR ${CMAKE_CURRENT_SOURCE_DIR}/base_test)
set(BASE_TEST_OUTPUT_DIR ${CMAKE_CURRENT_BINARY_DIR}/base_test)
set(BASE_TEST_CLUSTER ${BASE_TEST_OUTPUT_DIR}/testcluster)

set(PG_REGRESS_BASE_COMMON_ENV
  PG_REGRESS_DIR=${PG_REGRESS_DIR}
  PG_REGRESS=${PG_REGRESS}
  SQL_BASE_TEST_WORKING_DIR=${BASE_TEST_OUTPUT_DIR}
  SQL_BASE_DIFF_FILES_DIR=${CMAKE_CURRENT_SOURCE_DIR}/base_test_diff_files/
)

set(PG_REGRESS_OPTS_BASE_COMMON
  --temp-config=${TEST_OUTPUT_DIR}/postgresql.conf
  --load-language=plpgsql
  --inputdir=${BASE_TEST_INPUT_DIR}
  --outputdir=${BASE_TEST_OUTPUT_DIR}
  --load-extension=timescaledb
  --dlpath=${PROJECT_BINARY_DIR}/src)

set(PG_REGRESS_OPTS_BASE_LOCAL
  --host=${TEST_PGHOST}
  --port=${TEST_PGPORT_LOCAL}
)

set(PG_REGRESS_OPTS_BASE_TEMP_INSTANCE
  --temp-instance=${TEST_CLUSTER}
  --host=${TEST_PGHOST}
  --port=${TEST_PGPORT_TEMP_INSTANCE}
)
file(MAKE_DIRECTORY ${BASE_TEST_OUTPUT_DIR})

configure_file(${CMAKE_CURRENT_SOURCE_DIR}/base_test_excludes.txt ${CMAKE_CURRENT_BINARY_DIR}/base_test_excludes.txt COPYONLY)

add_custom_target(baseinstallcheck
  COMMAND ${CMAKE_COMMAND} -E env ${PG_REGRESS_BASE_COMMON_ENV}
  ${CMAKE_CURRENT_SOURCE_DIR}/base_pg_regress.sh ${PG_REGRESS_OPTS_BASE_COMMON} ${PG_REGRESS_OPTS_BASE_TEMP_INSTANCE}
  USES_TERMINAL)

add_custom_target(baseinstallchecklocal
  COMMAND ${CMAKE_COMMAND} -E env ${PG_REGRESS_BASE_COMMON_ENV}
  ${CMAKE_CURRENT_SOURCE_DIR}/base_pg_regress.sh ${PG_REGRESS_OPTS_BASE_COMMON} ${PG_REGRESS_OPTS_BASE_LOCAL}
  USES_TERMINAL)

endif (NOT ${PG_REGRESS_DIR} STREQUAL "")
