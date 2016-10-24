#!/bin/bash
#
# Run without args to run all *_test.sql files in current directory. Run with list of test files to only run them.
#
# To avoid pw writing, add localhost:5432:*:postgres:test to ~/.pgpass
set -u
set -e

POSTGRES_HOST=${POSTGRES_HOST:-localhost}
POSTGRES_USER=${POSTGRES_USER:-postgres}
RESET_POSTGRES_DB=${RESET_POSTGRES_DB:-true}

echo "Connecting to $POSTGRES_HOST as user $POSTGRES_USER"

if [ $RESET_POSTGRES_DB == "true" ]; then
    echo "Cleaning up DB"

    psql -U $POSTGRES_USER -h $POSTGRES_HOST -v ON_ERROR_STOP=1 -f ../tests/idempotent_include.sql
    psql -U $POSTGRES_USER -h $POSTGRES_HOST -v ON_ERROR_STOP=1 -d meta -f ../plpgunit/install/1.install-unit-test.sql
fi

if [ "$#" -ne 0 ]; then
    tests="$@"
else
    tests=`ls -1 *_test.sql`
fi

for test in $tests; do
    echo 'Setting up:' $test
    psql -U $POSTGRES_USER -h $POSTGRES_HOST -v ON_ERROR_STOP=1 -f $test
done

psql -U $POSTGRES_USER -h $POSTGRES_HOST -v ON_ERROR_STOP=1 -f ./start_tests.sql
