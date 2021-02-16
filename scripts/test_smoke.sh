# Run smoke tests on forge and test that updating between versions
# work. This is based on our update tests but doing some tweaks to
# ensure we can run it on forge. In particular, we cannot create new
# roles and we cannot create new databases.
#
# For this version, we focus on single-node update.
#
# UPDATE_FROM_TAG is the version to update from.
# UPDATE_TO_TAG is the version to update to.
#
# For connection information, we use the normal environment variables
# supported by psql:
# - PGHOST is host to use for the connection.
# - PGPORT is the port to use for the connection.
# - PGDATABASE is the database to use for the connection.
# - PGUSER is the username to use for the connection.
# - PGPASSWORD is the password to use for the connection

: ${PGUSER:?Please provide the user in PGUSER}
: ${PGHOST:?Please provide the host in PGHOST}
: ${PGPORT:?Please provide the port in PGPORT}
: ${PGDATABASE:?Please provide the database in PGDATABASE}
: ${PGPASSWORD:?Please provide the password in PGPASSWORD}
: ${TEST_VERSION:?Please provide a test version in TEST_VERSION}
SCRIPT_DIR=$(dirname $0)
BASE_DIR=${PWD}/${SCRIPT_DIR}/..
SCRATCHDIR=`mktemp -d -t 'smoketest-XXXX'`
DUMPFILE="$SCRATCHDIR/smoke.dump"
UPGRADE_OUT="$SCRATCHDIR/upgrade.out"
CLEAN_OUT="$SCRATCHDIR/clean.out"
RESTORE_OUT="$SCRATCHDIR/restore.out"
UPDATE_FROM_TAG=${UPDATE_FROM_TAG:-1.7.4}
UPDATE_TO_TAG=${UPDATE_TO_TAG:-2.0.1}

# trap cleanup EXIT

# cleanup() {
#     rm -rf $SCRATCHDIR
# }

# Extra options to pass to psql
PGOPTS="-v TEST_VERSION=${TEST_VERSION} -v TEST_REPAIR=false"
PSQL="psql -X $PGOPTS"

set -o pipefail

echo "**** Output and dumps will be saved in ${SCRATCHDIR} ****"
echo "**** Moving to directory ${BASE_DIR}/test/sql/updates ****"
cd ${BASE_DIR}/test/sql/updates

echo "**** Connecting as '$PGUSER' to database $PGDATABASE on $PGHOST:$PGPORT ****"

set -x

# For the comments below, we assume the upgrade is from 1.7.5 to 2.0.2

$PSQL -c "SELECT * FROM pg_available_extensions WHERE name LIKE 'timescaledb%'"

# Create a 1.7.5 version Upgrade
echo "**** Connecting to ${FORGE_CONNINFO} and running setup ****"
$PSQL -c "DROP EXTENSION IF EXISTS timescaledb CASCADE"
$PSQL -f pre.cleanup.sql
$PSQL -c "\d"
$PSQL -c "\df"
$PSQL -c "CREATE EXTENSION timescaledb VERSION '${UPDATE_FROM_TAG}'"
$PSQL -c "\dx"

# Run setup on Upgrade
$PSQL -f pre.smoke.sql
$PSQL -f setup.${TEST_VERSION}.sql

# Run update on Upgrade. You now have a 2.0.2 version in Upgrade.
$PSQL -c "ALTER EXTENSION timescaledb UPDATE TO '${UPDATE_TO_TAG}'"

# Apply post-update actions
#$PSQL -f post.repair.sql

# Dump the contents of Upgrade
pg_dump -Fc -f $DUMPFILE

# Run the post scripts on Upgrade to get UpgradeOut to compare
# with. We can now discard Upgrade database.
$PSQL -f post.${TEST_VERSION}.sql >$UPGRADE_OUT

: Create a 2.0.2 version Clean
$PSQL -c "DROP EXTENSION IF EXISTS timescaledb CASCADE"
$PSQL -f pre.cleanup.sql
$PSQL -c "CREATE EXTENSION timescaledb VERSION '${UPDATE_TO_TAG}'"

: Run the setup scripts on Clean, with post-update actions.
$PSQL -f pre.smoke.sql
$PSQL -f setup.${TEST_VERSION}.sql
#$PSQL -f post.repair.sql

: Run the post scripts on Clean to get output CleanOut
$PSQL -f post.${TEST_VERSION}.sql >$CLEAN_OUT

: Create a 2.0.2 version Restore
$PSQL -c "DROP EXTENSION IF EXISTS timescaledb CASCADE"
$PSQL -f pre.cleanup.sql
$PSQL -c "CREATE EXTENSION timescaledb VERSION '${UPDATE_TO_TAG}'"

: Restore the UpgradeDump into Restore
$PSQL -c "SELECT timescaledb_pre_restore()"
pg_restore -d $PGDATABASE $DUMPFILE || true
$PSQL -c "SELECT timescaledb_post_restore()"

: Run the post scripts on Restore to get a RestoreOut
$PSQL -f post.${TEST_VERSION}.sql >$RESTORE_OUT

# Compare UpgradeOut with CleanOut and make sure they are identical
diff -u $UPGRADE_OUT $CLEAN_OUT && echo "No difference between $UPGRADE_OUT and $CLEAN_OUT"

# Compare RestoreOut with CleanOut and make sure they are identical
diff -u $RESTORE_OUT $CLEAN_OUT && echo "No difference between $RESTORE_OUT and $CLEAN_OUT"

