set(TEST_ROLE_SUPERUSER super_user)
set(TEST_ROLE_CLUSTER_SUPERUSER cluster_super_user)
set(TEST_ROLE_DEFAULT_PERM_USER default_perm_user)
set(TEST_ROLE_DEFAULT_PERM_USER_2 default_perm_user_2)

set(TEST_ROLE_1 test_role_1)
set(TEST_ROLE_2 test_role_2)
set(TEST_ROLE_3 test_role_3)
set(TEST_ROLE_READ_ONLY test_role_read_only)

# TEST_ROLE_2 has password in passfile
set(TEST_ROLE_2_PASS pass)
# TEST_ROLE_3 does not have password in passfile
set(TEST_ROLE_3_PASS pass)

set(TEST_INPUT_DIR ${CMAKE_CURRENT_SOURCE_DIR})
set(TEST_OUTPUT_DIR ${CMAKE_CURRENT_BINARY_DIR})
set(TEST_CLUSTER ${TEST_OUTPUT_DIR}/testcluster)

# Basic connection info for test instance
set(TEST_PGPORT_LOCAL
    5432
    CACHE STRING "The port of a running PostgreSQL instance")
set(TEST_PGHOST
    localhost
    CACHE STRING "The hostname of a running PostgreSQL instance")
set(TEST_PGUSER
    ${TEST_ROLE_DEFAULT_PERM_USER}
    CACHE STRING "The PostgreSQL test user")
set(TEST_DBNAME
    single
    CACHE STRING "The database name to use for tests")
set(TEST_PGPORT_TEMP_INSTANCE
    55432
    CACHE STRING "The port to run a temporary test PostgreSQL instance on")
set(TEST_SCHEDULE ${CMAKE_CURRENT_BINARY_DIR}/test_schedule)
set(TEST_SCHEDULE_SHARED
    ${CMAKE_CURRENT_BINARY_DIR}/shared/test_schedule_shared)
set(ISOLATION_TEST_SCHEDULE ${CMAKE_CURRENT_BINARY_DIR}/isolation_test_schedule)
set(TEST_PASSFILE ${TEST_OUTPUT_DIR}/pgpass.conf)

configure_file(${PRIMARY_TEST_DIR}/pg_hba.conf.in pg_hba.conf)
set(TEST_PG_HBA_FILE ${TEST_OUTPUT_DIR}/pg_hba.conf)

if(USE_TELEMETRY)
  set(TELEMETRY_DEFAULT_SETTING "timescaledb.telemetry_level=off")
else()
  set(TELEMETRY_DEFAULT_SETTING)
endif()
configure_file(postgresql.conf.in postgresql.conf)
configure_file(max_bgw_8.conf.in max_bgw_8.conf)
configure_file(${PRIMARY_TEST_DIR}/pgtest.conf.in pgtest.conf)

# pgpass file requires chmod 0600
configure_file(${PRIMARY_TEST_DIR}/pgpass.conf.in
               ${TEST_OUTPUT_DIR}${CMAKE_FILES_DIRECTORY}/pgpass.conf)
file(
  COPY ${TEST_OUTPUT_DIR}${CMAKE_FILES_DIRECTORY}/pgpass.conf
  DESTINATION ${TEST_OUTPUT_DIR}
  NO_SOURCE_PERMISSIONS
  FILE_PERMISSIONS OWNER_READ OWNER_WRITE)

set(PG_REGRESS_OPTS_BASE --host=${TEST_PGHOST}
                         --dlpath=${PROJECT_BINARY_DIR}/src)

set(PG_REGRESS_OPTS_EXTRA
    --create-role=${TEST_ROLE_SUPERUSER},${TEST_ROLE_DEFAULT_PERM_USER},${TEST_ROLE_DEFAULT_PERM_USER_2},${TEST_ROLE_CLUSTER_SUPERUSER},${TEST_ROLE_1},${TEST_ROLE_2},${TEST_ROLE_3},${TEST_ROLE_READ_ONLY}
    --dbname=${TEST_DBNAME}
    --launcher=${PRIMARY_TEST_DIR}/runner.sh)

set(PG_REGRESS_SHARED_OPTS_EXTRA
    --create-role=${TEST_ROLE_SUPERUSER},${TEST_ROLE_DEFAULT_PERM_USER},${TEST_ROLE_DEFAULT_PERM_USER_2}
    --dbname=${TEST_DBNAME}
    --launcher=${PRIMARY_TEST_DIR}/runner_shared.sh)

set(PG_ISOLATION_REGRESS_OPTS_EXTRA
    --create-role=${TEST_ROLE_SUPERUSER},${TEST_ROLE_DEFAULT_PERM_USER},${TEST_ROLE_DEFAULT_PERM_USER_2},${TEST_ROLE_CLUSTER_SUPERUSER},${TEST_ROLE_1},${TEST_ROLE_2},${TEST_ROLE_3},${TEST_ROLE_READ_ONLY}
    --dbname=${TEST_DBNAME})

set(PG_REGRESS_OPTS_INOUT --inputdir=${TEST_INPUT_DIR}
                          --outputdir=${TEST_OUTPUT_DIR})

set(PG_REGRESS_SHARED_OPTS_INOUT
    --inputdir=${TEST_INPUT_DIR}/shared --outputdir=${TEST_OUTPUT_DIR}/shared
    --load-extension=timescaledb)

set(PG_ISOLATION_REGRESS_OPTS_INOUT
    --inputdir=${TEST_INPUT_DIR}/isolation
    --outputdir=${TEST_OUTPUT_DIR}/isolation --load-extension=timescaledb)

set(PG_REGRESS_OPTS_TEMP_INSTANCE
    --port=${TEST_PGPORT_TEMP_INSTANCE} --temp-instance=${TEST_CLUSTER}
    --temp-config=${TEST_OUTPUT_DIR}/postgresql.conf)

set(PG_REGRESS_OPTS_TEMP_INSTANCE_PGTEST
    --port=${TEST_PGPORT_TEMP_INSTANCE} --temp-instance=${TEST_CLUSTER}
    --temp-config=${TEST_OUTPUT_DIR}/pgtest.conf)

set(PG_REGRESS_OPTS_LOCAL_INSTANCE --port=${TEST_PGPORT_LOCAL})

if(PG_REGRESS)
  set(PG_REGRESS_ENV
      TEST_PGUSER=${TEST_PGUSER}
      TEST_PGHOST=${TEST_PGHOST}
      TEST_ROLE_SUPERUSER=${TEST_ROLE_SUPERUSER}
      TEST_ROLE_DEFAULT_PERM_USER=${TEST_ROLE_DEFAULT_PERM_USER}
      TEST_ROLE_DEFAULT_PERM_USER_2=${TEST_ROLE_DEFAULT_PERM_USER_2}
      TEST_ROLE_CLUSTER_SUPERUSER=${TEST_ROLE_CLUSTER_SUPERUSER}
      TEST_ROLE_1=${TEST_ROLE_1}
      TEST_ROLE_2=${TEST_ROLE_2}
      TEST_ROLE_3=${TEST_ROLE_3}
      TEST_ROLE_READ_ONLY=${TEST_ROLE_READ_ONLY}
      TEST_ROLE_2_PASS=${TEST_ROLE_2_PASS}
      TEST_ROLE_3_PASS=${TEST_ROLE_3_PASS}
      TEST_DBNAME=${TEST_DBNAME}
      TEST_INPUT_DIR=${TEST_INPUT_DIR}
      TEST_OUTPUT_DIR=${TEST_OUTPUT_DIR}
      TEST_SCHEDULE=${TEST_SCHEDULE}
      PG_BINDIR=${PG_BINDIR}
      PG_REGRESS=${PG_REGRESS})
endif()

if(PG_ISOLATION_REGRESS)
  set(PG_ISOLATION_REGRESS_ENV
      TEST_PGUSER=${TEST_PGUSER}
      TEST_ROLE_SUPERUSER=${TEST_ROLE_SUPERUSER}
      TEST_ROLE_DEFAULT_PERM_USER=${TEST_ROLE_DEFAULT_PERM_USER}
      TEST_ROLE_DEFAULT_PERM_USER_2=${TEST_ROLE_DEFAULT_PERM_USER_2}
      TEST_ROLE_CLUSTER_SUPERUSER=${TEST_ROLE_CLUSTER_SUPERUSER}
      TEST_ROLE_1=${TEST_ROLE_1}
      TEST_ROLE_2=${TEST_ROLE_2}
      TEST_ROLE_3=${TEST_ROLE_3}
      TEST_ROLE_2_PASS=${TEST_2_PASS}
      TEST_ROLE_3_PASS=${TEST_3_PASS}
      TEST_ROLE_READ_ONLY=${TEST_ROLE_READ_ONLY}
      TEST_DBNAME=${TEST_DBNAME}
      TEST_INPUT_DIR=${TEST_INPUT_DIR}
      TEST_OUTPUT_DIR=${TEST_OUTPUT_DIR}
      TEST_SCHEDULE=${ISOLATION_TEST_SCHEDULE}
      PG_REGRESS=${PG_ISOLATION_REGRESS})
endif()

set(TEST_VERSION_SUFFIX ${PG_VERSION_MAJOR})
