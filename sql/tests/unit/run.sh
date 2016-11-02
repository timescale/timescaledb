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
INSTALL_DB=${INSTALL_DB:-Test1}
echo "Connecting to $POSTGRES_HOST as user $POSTGRES_USER"

NODES="Test1 test2"
if [ $RESET_POSTGRES_DB == "true" ]; then
    echo "Cleaning up DB"

    ../../setup/setup_meta.sh
    for node in $NODES; do
        ../../setup/setup_node.sh $node
        ../../setup/setup_kafka.sh $node
    done

    psql -U $POSTGRES_USER -h $POSTGRES_HOST -v ON_ERROR_STOP=1 -d $INSTALL_DB -f ../../plpgunit/install/1.install-unit-test.sql
fi

if [ "$#" -ne 0 ]; then
    tests="$@"
else
    tests=`ls -1 *_test.sql`
fi

for test in $tests; do
    echo 'Setting up:' $test
    psql -U $POSTGRES_USER -h $POSTGRES_HOST -d $INSTALL_DB -v ON_ERROR_STOP=1 -f $test
done

psql -U $POSTGRES_USER -h $POSTGRES_HOST -d $INSTALL_DB -v ON_ERROR_STOP=1 -f ./start_tests.sql
