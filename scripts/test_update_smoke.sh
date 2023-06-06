#!/bin/bash
# shellcheck disable=SC2129,SC2230
# SC2129: Consider using { cmd1; cmd2; } >> file instead of individual redirects.
# SC2230: which is non-standard. Use builtin 'command -v' instead.

# Run smoke tests to test that updating between versions work.
#
# Usage:
#      bash test_update_smoke.sh postgres://...
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
SCRATCHDIR=$(mktemp -d -t 'smoketest-XXXX')
LOGFILE="$SCRATCHDIR/update-test.log"
DUMPFILE="$SCRATCHDIR/smoke.dump"
UPGRADE_OUT="$SCRATCHDIR/upgrade.out"
CLEAN_OUT="$SCRATCHDIR/clean.out"
RESTORE_OUT="$SCRATCHDIR/restore.out"
TEST_VERSION=${TEST_VERSION:-v7}
UPDATE_FROM_TAG=${UPDATE_FROM_TAG:-1.7.4}
UPDATE_TO_TAG=${UPDATE_TO_TAG:-2.0.1}
# We do not have superuser privileges when running smoke tests.
WITH_SUPERUSER=false
WITH_ROLES=false

while getopts "s:t:" opt;
do
    case $opt in
        s)
            UPDATE_FROM_TAG=$OPTARG
            ;;
        t)
            UPDATE_TO_TAG=$OPTARG
            ;;
        *)
            ;;
    esac
done

shift $((OPTIND-1))

echo "**** pg_dump at   " "$(which pg_dump)"
echo "**** pg_restore at" "$(which pg_restore)"

# Extra options to pass to psql
PGOPTS="-v TEST_VERSION=${TEST_VERSION} -v WITH_SUPERUSER=${WITH_SUPERUSER} -v WITH_ROLES=${WITH_ROLES} -v WITH_CHUNK=false"
PSQL="psql -a -qX $PGOPTS"

# If we are providing a URI for the connection, we parse it here and
# set the PG??? variables since that is the only reliable way to
# provide connection information to psql, pg_dump, and pg_restore.
#
# To work with Forge, we need to only set PGPASSWORD when the password
# is available and leave it unset otherwise. If the user has either
# set PGPASSWORD or has the password in a .pgpass file, it will be
# picked up and used for the connection.
if [[ $# -gt 0 ]]; then
    # shellcheck disable=SC2207 # Prefer mapfile or read -a to split command output (or quote to avoid splitting).
    parts=($(echo $1 | perl -mURI::Split=uri_split -ne '@F = uri_split($_); print join(" ", split(qr/[:@]/, $F[1]), substr($F[2], 1))'))
    export PGUSER=${parts[0]}
    if [[ ${#parts[@]} -eq 5 ]]; then
    # Cloud has 5 fields
	export PGPASSWORD=${parts[1]}
	export PGHOST=${parts[2]}
	export PGPORT=${parts[3]}
	export PGDATABASE=${parts[4]}
    elif [[ ${#parts[@]} -eq 4 ]]; then
    # Forge has 4 fields
	export PGHOST=${parts[1]}
	export PGPORT=${parts[2]}
	export PGDATABASE=${parts[3]}
    else
	echo "Malformed URL '$1'" 1>&2
	exit 2
    fi
fi

err_trap() {
    exit 3
}

exit_trap() {
    exit_code=$?
    if [ "$exit_code" != "0" ]; then
        echo "!!!! FAILED !!!!"
    else
        echo "**** passed ****"
    fi
    echo "**** logs can be found in $SCRATCHDIR"
}

set -e
set -o pipefail
trap exit_trap EXIT
trap err_trap ERR

missing_versions() {
        $PSQL -v ECHO=none -t <<-EOF
	SELECT * FROM (VALUES ('$1'), ('$2')) AS foo
	EXCEPT
	SELECT version FROM pg_available_extension_versions
	WHERE name = 'timescaledb' AND version IN ('$1', '$2');
	EOF
}


echo "**** Scratch directory: ${SCRATCHDIR}"
echo "**** Update files in directory ${BASE_DIR}/test/sql/updates"
cd ${BASE_DIR}/test/sql/updates

$PSQL -c '\conninfo'

# shellcheck disable=SC2207 # Prefer mapfile or read -a to split command output (or quote to avoid splitting).
missing=($(missing_versions $UPDATE_FROM_TAG $UPDATE_TO_TAG))
if [[ ${#missing[@]} -gt 0 ]]; then
    echo "ERROR: Missing version(s) ${missing[*]} of 'timescaledb'"
    echo "Available versions: " "$($PSQL -tc "SELECT version FROM pg_available_extension_versions WHERE name = 'timescaledb'")"
    exit 1
fi

# For the comments below, we assume the upgrade is from 1.7.5 to 2.0.2
# (this is just an example, the real value is given by variables
# above).

# Create a 1.7.5 version Upgrade
echo "---- Connecting to ${FORGE_CONNINFO} and running setup ----"
$PSQL -f cleanup.${TEST_VERSION}.sql >>$LOGFILE 2>&1
$PSQL -c "DROP EXTENSION IF EXISTS timescaledb CASCADE" >>$LOGFILE 2>&1
$PSQL -f pre.cleanup.sql >>$LOGFILE 2>&1
$PSQL -c "CREATE EXTENSION timescaledb VERSION '${UPDATE_FROM_TAG}'" >>$LOGFILE 2>&1
$PSQL -c "\dx"

# Run setup on Upgrade
$PSQL -f pre.smoke.sql >>$LOGFILE 2>&1
$PSQL -f setup.${TEST_VERSION}.sql >>$LOGFILE 2>&1
# Run update on Upgrade. You now have a 2.0.2 version in Upgrade.
$PSQL -c "ALTER EXTENSION timescaledb UPDATE TO '${UPDATE_TO_TAG}'" >>$LOGFILE 2>&1
$PSQL -f <(echo '\c dn1';echo "ALTER EXTENSION timescaledb UPDATE TO '${UPDATE_TO_TAG}'") >>$LOGFILE 2>&1

echo -n "Dumping the contents of Upgrade..."
pg_dump -Fc -f $DUMPFILE >>$LOGFILE 2>&1
echo "done"

# Run the post scripts on Upgrade to get UpgradeOut to compare
# with.  We can now discard Upgrade database.
echo -n "Collecting post-update status..."
$PSQL -f post.${TEST_VERSION}.sql >$UPGRADE_OUT
echo "done"

$PSQL -f cleanup.${TEST_VERSION}.sql >>$LOGFILE 2>&1

echo "---- Create a ${UPDATE_TO_TAG} version Clean ----"
$PSQL -c "DROP EXTENSION IF EXISTS timescaledb CASCADE" >>$LOGFILE 2>&1
$PSQL -f pre.cleanup.sql >>$LOGFILE 2>&1
$PSQL -c "CREATE EXTENSION timescaledb VERSION '${UPDATE_TO_TAG}'" >>$LOGFILE 2>&1
$PSQL -c "\dx"

echo "---- Run the setup scripts on Clean, with post-update actions ----"
$PSQL -f pre.smoke.sql >>$LOGFILE 2>&1
$PSQL -f setup.${TEST_VERSION}.sql >>$LOGFILE 2>&1

echo "---- Run the post scripts on Clean to get output CleanOut ----"
$PSQL -f post.${TEST_VERSION}.sql >$CLEAN_OUT

$PSQL -f cleanup.${TEST_VERSION}.sql >>$LOGFILE 2>&1

echo "---- Create a ${UPDATE_TO_TAG} version Restore ----"
$PSQL -c "DROP EXTENSION IF EXISTS timescaledb CASCADE" >>$LOGFILE 2>&1
$PSQL -f pre.cleanup.sql >>$LOGFILE 2>&1
$PSQL -c "CREATE EXTENSION timescaledb VERSION '${UPDATE_TO_TAG}'" >>$LOGFILE 2>&1
$PSQL -c "\dx"

echo "---- Restore the UpgradeDump into Restore ----"
echo -n "Restoring dump..."
$PSQL -c "SELECT timescaledb_pre_restore()" >>$LOGFILE 2>&1
pg_restore -d $PGDATABASE $DUMPFILE >>$LOGFILE 2>&1 || true
$PSQL -c "SELECT timescaledb_post_restore()" >>$LOGFILE 2>&1
echo "done"

echo "---- Run the post scripts on Restore to get a RestoreOut ----"
$PSQL -f post.${TEST_VERSION}.sql >$RESTORE_OUT

echo "---- Compare UpgradeOut with CleanOut and make sure they are identical ----"
diff -u $UPGRADE_OUT $CLEAN_OUT | tee $SCRATCHDIR/upgrade-clean.diff

echo "---- Compare RestoreOut with CleanOut and make sure they are identical ----"
diff -u $RESTORE_OUT $CLEAN_OUT | tee $SCRATCHDIR/restore-clean.diff

$PSQL -f cleanup.${TEST_VERSION}.sql >>$LOGFILE 2>&1
