#!/bin/bash
set -eu

#
# Run regression check
#
# Param: <next_version>
# Param: <connection_string>
#

if [ "$#" -ne 2 ]; then
    echo "${0} <next_version> <connection_string>"
    exit 2
fi

#NEXT_VERSION=$1
#CONNECTION_STRING=$2

#psql "$CONNECTION_STRING" -c "ALTER EXTENSION timescaledb UPDATE TO '${NEXT_VERSION}'"

#export TEST_PGPORT_REMOTE="$PORT"
#export TEST_PGHOST="$HOSTNAME"
#export TEST_PGUSER="$SERVICE_USER"
#export TEST_DBNAME="$DBNAME"

#export PGUSER="$SERVICE_USER"
#export PGPASSWORD="$PASSWORD"
#export PGPORT="$PORT"
#export USER="tsdbadmin" #$SERVICE_USER"

#Run the test
#make regresscheck-shared

#exit 2

# timescaledb Version/Tag related parameters, more changeable:
#NEW_DEV_VERSION="2.17.0-dev"
#COUNT="0"

# PG Tag related parameters , usually less changeable:
#PG_MAJOR_VERSION=16
#PG_MINOR_VERSION=4


#create required users on the service.

#This is blocked , for now.
#psql "$SERVICE_URL" -c "CREATE USER default_perm_user"
#psql "$SERVICE_URL" -c "GRANT default_perm_user TO tsdbadmin"
#psql "$SERVICE_URL" -c "CREATE USER default_perm_user_2"
#psql "$SERVICE_URL" -c "GRANT default_perm_user_2 TO tsdbadmin"


#Modify some GUCs, as needed on the service.

#psql tsdb -c "ALTER database tsdb set timescaledb_experimental.enable_distributed_ddl to on;"
#psql tsdb -c "ALTER database tsdb set timezone to 'US/Pacific';"
#psql tsdb -c "ALTER database tsdb set timescaledb.telemetry_level to off;" 
#psql tsdb -c "ALTER database tsdb set extra_float_digits to 0;"
#psql tsdb -c "ALTER database tsdb set random_page_cost to 1.0;"
#psql tsdb -c "ALTER database tsdb set datestyle to 'Postgres, MDY';"
#psql tsdb -c "ALTER system set autovacuum to false;"
#psql tsdb -c "ALTER system set wal_level to 'logical';"
#psql tsdb -c "SELECT _timescaledb_functions.stop_background_workers();"
#psql tsdb -c "ALTER database tsdb set max_parallel_workers to 8;"
#psql tsdb -c "ALTER database tsdb set max_parallel_workers_per_gather to 2;"
# This does not switch off Query Identifier , right now. Need to check further.
#psql tsdb -c "SET compute_query_id = off;"



#sleeps are to allow the server to restart after these GUC parameters' change.
#sc service parameters -E "$ENVIRONMENT" -R "$REGION" -S "$SERVICE_ID" set -p max_connections -n 200
#sleep 120
#sc service parameters -E "$ENVIRONMENT" -R "$REGION" -S "$SERVICE_ID" set -p max_worker_processes -n 24
#sleep 120


#Set the parameters

#export TEST_PGPORT_REMOTE="$PORT"
#export TEST_PGHOST="$HOSTNAME"
#export TEST_PGUSER="$SERVICE_USER"
#export TEST_DBNAME="$DBNAME"

#export PGUSER="$SERVICE_USER"
#export PGPASSWORD="$PASSWORD"
#export PGPORT="$PORT"
#export USER="$SERVICE_USER"

#Run the test
#make regresscheck-shared
