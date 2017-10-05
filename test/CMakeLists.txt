# Testing support
find_program(PG_REGRESS pg_regress
  PATHS
  "${PG_PKGLIBDIR}/pgxs/src/test/regress/"
  ${POSTGRES_PATHS}
  REQUIRED)

message(STATUS "Using pg_regress ${PG_REGRESS}")

set(TEST_ROLE_SUPERUSER super_user)
set(TEST_ROLE_DEFAULT_PERM_USER default_perm_user)
set(TEST_ROLE_DEFAULT_PERM_USER_2 default_perm_user_2)

# Basic connection info for test instance
set(TEST_PGPORT 1432)
set(TEST_PGHOST localhost)
set(TEST_PGUSER ${TEST_ROLE_DEFAULT_PERM_USER})
set(TEST_DBNAME single)
set(TEST_INPUT_DIR ${CMAKE_CURRENT_SOURCE_DIR})
set(TEST_OUTPUT_DIR ${CMAKE_CURRENT_BINARY_DIR})
set(TEST_CLUSTER ${TEST_OUTPUT_DIR}/testcluster)
set(PG_REGRESS_OPTS
  --temp-instance=${TEST_CLUSTER}
  --temp-config=${TEST_OUTPUT_DIR}/postgresql.conf
  --create-role=${TEST_ROLE_SUPERUSER},${TEST_ROLE_DEFAULT_PERM_USER},${TEST_ROLE_DEFAULT_PERM_USER_2}
  --dbname=${TEST_DBNAME}
  --outputdir=${TEST_OUTPUT_DIR}
  --launcher=${TEST_INPUT_DIR}/runner.sh
  --host=${TEST_PGHOST}
  --port=${TEST_PGPORT}
  --user=${TEST_PGUSER}
  --load-language=plpgsql
  --dlpath=${PROJECT_BINARY_DIR}/src
  --inputdir=${TEST_INPUT_DIR})

# Configuration setting needed to LOAD module for tests instead of
# preloading
file(MAKE_DIRECTORY ${TEST_OUTPUT_DIR})
file(WRITE ${TEST_OUTPUT_DIR}/postgresql.conf "shared_preload_libraries=timescaledb")

add_custom_target(installcheck
  COMMAND ${CMAKE_COMMAND} -E env
  TEST_PGUSER=${TEST_PGUSER}
  TEST_ROLE_SUPERUSER=${TEST_ROLE_SUPERUSER}
  TEST_ROLE_DEFAULT_PERM_USER=${TEST_ROLE_DEFAULT_PERM_USER}
  TEST_ROLE_DEFAULT_PERM_USER_2=${TEST_ROLE_DEFAULT_PERM_USER_2}
  TEST_DBNAME=${TEST_DBNAME}
  TEST_INPUT_DIR=${TEST_INPUT_DIR}
  TEST_OUTPUT_DIR=${TEST_OUTPUT_DIR}
  PG_REGRESS=${PG_REGRESS}
  ${CMAKE_CURRENT_SOURCE_DIR}/pg_regress.sh ${PG_REGRESS_OPTS}
  USES_TERMINAL)
