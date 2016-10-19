#!/bin/bash
set -u
set -e 

echo "DROP DATABASE IF EXISTS system; CREATE DATABASE system;"| psql -U postgres -h postgres -v ON_ERROR_STOP=1
echo "DROP DATABASE IF EXISTS test; CREATE DATABASE test;"| psql -U postgres -h postgres -v ON_ERROR_STOP=1
echo "CREATE EXTENSION IF NOT EXISTS hashlib"| psql -U postgres -h postgres -d test -v ON_ERROR_STOP=1
psql -U postgres -h postgres -v ON_ERROR_STOP=1 -d test -f  ../../plpgunit/install/1.install-unit-test.sql 
psql -U postgres -h postgres -v ON_ERROR_STOP=1  -d system -f  ../../../vendor/bitbucket.org/440-labs/backend-common-go/namespace/schema.sql 
psql -U postgres -h postgres -v ON_ERROR_STOP=1 -d system -f  test_mock_system_data.sql 
psql -U postgres -h postgres -v ON_ERROR_STOP=1 -d system -f  test_utils.sql 
psql -U postgres -h postgres -v ON_ERROR_STOP=1 -d test -f  ../tables.sql 
psql -U postgres -h postgres -v ON_ERROR_STOP=1 -d test -f  ../partitioning.sql 
psql -U postgres -h postgres -v ON_ERROR_STOP=1 -d test -f  ../insert.sql 
psql -U postgres -h postgres -v ON_ERROR_STOP=1 -d test -f  test_create_system_foreign_schema.sql
psql -U postgres -h postgres -v ON_ERROR_STOP=1 -d test -f  ../ioql.sql 
psql -U postgres -h postgres -v ON_ERROR_STOP=1 -d test -f  ../ioql_unoptimized.sql 
psql -U postgres -h postgres -v ON_ERROR_STOP=1 -d test -f  ../ioql_optimized.sql 
psql -U postgres -h postgres -v ON_ERROR_STOP=1 -d test -f  ../ioql_optimized_nonagg.sql 
psql -U postgres -h postgres -v ON_ERROR_STOP=1 -d test -f  ../ioql_optimized_agg.sql 
psql -U postgres -h postgres -v ON_ERROR_STOP=1 -d test -f  test_mock_project_fields.sql 
echo "******* start test **********"
psql -U postgres -h postgres -v ON_ERROR_STOP=1 -d test -f  test.sql 
psql -U postgres -h postgres -v ON_ERROR_STOP=1 -d test -f  test_ioql_data.sql 
psql -U postgres -h postgres -v ON_ERROR_STOP=1 -d test -f  test_ioql_unoptimized.sql 
psql -U postgres -h postgres -v ON_ERROR_STOP=1 -d test -f  test_ioql_optimized.sql 
psql -U postgres -h postgres -v ON_ERROR_STOP=1 -d test -f  execute_tests.sql 
