#!/bin/bash

# Run smoke tests to test that updating between versions work.
#
# This is based on our update tests but doing some tweaks to ensure we
# can run it on Forge (or any other PostgreSQL server that only permit
# a single user and single database).
#
# In particular, we cannot create new roles and we cannot create new
# databases.
#
# For this version, we focus on single-node update and smoke testing
# cannot (necessarily) be done with multinode setups. Feel free to try
# though.
#
# The following environment variables can be set: 
# - UPDATE_FROM_TAG is the version to update from (optional).
#
# - UPDATE_TO_TAG is the version to update to (optional).
#
# - PGHOST is host to use for the connection (required).
#
# - PGPORT is the port to use for the connection (required).
#
# - PGDATABASE is the database to use for the connection (required).
#
# - PGUSER is the username to use for the connection (required).
#
# - PGPASSWORD is the password to use for the connection
#   (optional). If not set, password from .pgpass will be used (if
#   available).

SCRIPT_DIR=$(dirname $0)
BASE_DIR=${PWD}/${SCRIPT_DIR}/..
SCRATCHDIR=`mktemp -d -t 'smoketest-XXXX'`
DUMPFILE="$SCRATCHDIR/smoke.dump"
UPGRADE_OUT="$SCRATCHDIR/upgrade.out"
CLEAN_OUT="$SCRATCHDIR/clean.out"
RESTORE_OUT="$SCRATCHDIR/restore.out"
TEST_VERSION=${TEST_VERSION:-v6}

while getopts "d" opt;
do
    case $opt in
        d)
            cleanup=1
            ;;
    esac
done

shift $((OPTIND-1))

UPDATE_FROM_TAG=${1:-${UPDATE_FROM_TAG:-1.7.4}}
UPDATE_TO_TAG=${2:-${UPDATE_TO_TAG:-2.0.1}}

# We do not have superuser privileges when running smoke tests.
WITH_SUPERUSER=false

# Extra options to pass to psql
PGOPTS="-v TEST_VERSION=${TEST_VERSION} -v WITH_SUPERUSER=${WITH_SUPERUSER}"
PSQL="psql -qX $PGOPTS"

cleanup() {
    rm -rf $SCRATCHDIR
}

if [[ "x$cleanup" != "x" ]]; then
    echo "**** Debug mode: $SCRATCHDIR will not be removed"
    trap cleanup EXIT
fi

missing_versions() {
        $PSQL -t <<EOF
SELECT * FROM (VALUES ('$1'), ('$2')) AS foo
EXCEPT
SELECT version FROM pg_available_extension_versions WHERE name = 'timescaledb' AND version IN ('$1', '$2');
EOF
}

set -e

echo "**** Scratch directory: ${SCRATCHDIR}"
echo "**** Update files in directory ${BASE_DIR}/test/sql/updates"
cd ${BASE_DIR}/test/sql/updates

$PSQL -c '\conninfo'

missing=($(missing_versions $UPDATE_FROM_TAG $UPDATE_TO_TAG))
if [[ ${#missing[@]} -gt 0 ]]; then
    echo "ERROR: Missing version(s) ${missing[@]} of 'timescaledb'"
    echo "Available versions: " $($PSQL -tc "SELECT version FROM pg_available_extension_versions WHERE name = 'timescaledb'")
    exit 1
fi

set -x

# For the comments below, we assume the upgrade is from 1.7.5 to 2.0.2
# (this is just an example, the real value is given by variables
# above).

# Create a 1.7.5 version Upgrade
: ---- Connecting to ${FORGE_CONNINFO} and running setup ----
$PSQL -c "DROP EXTENSION IF EXISTS timescaledb CASCADE"
$PSQL -f pre.cleanup.sql
$PSQL -c "CREATE EXTENSION timescaledb VERSION '${UPDATE_FROM_TAG}'"
$PSQL -c "\dx"

# Run setup on Upgrade
$PSQL -f pre.smoke.sql
$PSQL -f setup.${TEST_VERSION}.sql

# Run update on Upgrade. You now have a 2.0.2 version in Upgrade.
$PSQL -c "ALTER EXTENSION timescaledb UPDATE TO '${UPDATE_TO_TAG}'"

# Dump the contents of Upgrade
pg_dump -Fc -f $DUMPFILE

# Run the post scripts on Upgrade to get UpgradeOut to compare
# with.  We can now discard Upgrade database.
$PSQL -f post.${TEST_VERSION}.sql >$UPGRADE_OUT

: ---- Create a 2.0.2 version Clean ----
$PSQL -c "DROP EXTENSION IF EXISTS timescaledb CASCADE"
$PSQL -f pre.cleanup.sql
$PSQL -c "CREATE EXTENSION timescaledb VERSION '${UPDATE_TO_TAG}'"

: ---- Run the setup scripts on Clean, with post-update actions ----
$PSQL -f pre.smoke.sql
$PSQL -f setup.${TEST_VERSION}.sql
#$PSQL -f post.repair.sql

: ---- Run the post scripts on Clean to get output CleanOut ----
$PSQL -f post.${TEST_VERSION}.sql >$CLEAN_OUT

: ---- Create a 2.0.2 version Restore ----
$PSQL -c "DROP EXTENSION IF EXISTS timescaledb CASCADE"
$PSQL -f pre.cleanup.sql
$PSQL -c "CREATE EXTENSION timescaledb VERSION '${UPDATE_TO_TAG}'"

: ---- Restore the UpgradeDump into Restore ----
$PSQL -c "SELECT timescaledb_pre_restore()"
pg_restore -d $PGDATABASE $DUMPFILE || true
$PSQL -c "SELECT timescaledb_post_restore()"

: ---- Run the post scripts on Restore to get a RestoreOut ----
$PSQL -f post.${TEST_VERSION}.sql >$RESTORE_OUT

: ---- Compare UpgradeOut with CleanOut and make sure they are identical ----
diff -u $UPGRADE_OUT $CLEAN_OUT && echo "No difference between $UPGRADE_OUT and $CLEAN_OUT" | tee $SCRATCHDIR/upgrade-clean.diff

: ---- Compare RestoreOut with CleanOut and make sure they are identical ----
diff -u $RESTORE_OUT $CLEAN_OUT && echo "No difference between $RESTORE_OUT and $CLEAN_OUT" | tee $SCRATCHDIR/restore-clean.diff

