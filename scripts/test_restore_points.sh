#!/usr/bin/env bash

#
# Test PostgreSQL restore point recovery for single node (1 instance, regular hypertable)
# and multi node cluster (4 instances, distributed hypertable).
#
# Make sure that single and multi node results match after recovery.
#

set -e
set -o pipefail
set -x

SN_NAME=${SN_NAME:-sn}
AN_NAME=${AN_NAME:-an}
DN1_NAME=${DN1_NAME:-dn1}
DN2_NAME=${DN2_NAME:-dn2}
DN3_NAME=${DN3_NAME:-dn3}

HOST=${HOST:-'localhost'}
SN_PORT=${SN_PORT:-5442}
AN_PORT=${AN_PORT:-5442}
DN1_PORT=${DN1_PORT:-5443}
DN2_PORT=${DN2_PORT:-5444}
DN3_PORT=${DN3_PORT:-5445}

TEST_DIR=${TEST_DIR:-$(pwd)}
WAL_DIR=${TEST_DIR}/wal
BASEBACKUP_DIR=${TEST_DIR}/basebackup
STORAGE_DIR=${TEST_DIR}/storage
RESULT_DIR=${TEST_DIR}/result

TEST_RESTORE_POINT_1=${TEST_RESTORE_POINT_1:-rp1}
TEST_RESTORE_POINT_2=${TEST_RESTORE_POINT_2:-rp2}
TEST_RESTORE_POINT_3=${TEST_RESTORE_POINT_3:-rp3}

PG_BINDIR=${PG_BINDIR:-$(pg_config --bindir)}
PG_VERSION_MAJOR=$(${PG_BINDIR}/postgres -V | awk '{print $3}' | sed 's/\./ /g' | awk '{print $1}')
PG_USER=${PG_USER:-postgres}

function instance_configure()
{
	NAME=$1
	PORT=$2
	RESTORE_POINT_NAME=$3
	${PG_BINDIR}/pg_ctl init -U ${PG_USER} -D "${STORAGE_DIR}/${NAME}"
	cat > "${STORAGE_DIR}/${NAME}/postgresql.conf" <<EOF
port=${PORT}
shared_preload_libraries = 'timescaledb'
autovacuum=false
timescaledb.telemetry_level=off
timescaledb_telemetry.cloud='ci'
max_prepared_transactions = 100
wal_level = 'archive'
archive_mode = on
archive_command = 'test ! -f ${WAL_DIR}/${NAME}/%f && cp %p ${WAL_DIR}/${NAME}/%f'
EOF
}

function instance_start()
{
	NAME=$1
	HOST=$2
	PORT=$3
	${PG_BINDIR}/pg_ctl start -U ${PG_USER} -D "${STORAGE_DIR}/${NAME}"
	for _ in {1..20}; do
		sleep 0.5
		if ${PG_BINDIR}/pg_isready -q -U ${PG_USER} -h ${HOST} -p ${PORT} -d postgres
		then
			sleep 0.2
			return 0
		fi
	done
	exit 1
}

function instance_create()
{
	NAME=$1
	HOST=$2
	PORT=$3
	RESTORE_POINT_NAME=$4
	mkdir -p "${WAL_DIR}/${NAME}"
	mkdir -p "${STORAGE_DIR}/${NAME}"
	mkdir -p "${BASEBACKUP_DIR}/${NAME}"
	instance_configure ${NAME} ${PORT} ${RESTORE_POINT_NAME}
	instance_start ${NAME} ${HOST} ${PORT}
}

function instance_stop()
{
	NAME=$1
	${PG_BINDIR}/pg_ctl stop -U ${PG_USER} -D "${STORAGE_DIR}/${NAME}"
}

function instance_recover()
{
	NAME=$1
	HOST=$2
	PORT=$3
	cp -R "${BASEBACKUP_DIR}/${NAME}" "${STORAGE_DIR}/${NAME}"
	chmod 700 "${STORAGE_DIR}/${NAME}"
	touch "${STORAGE_DIR}/${NAME}/recovery.signal"
	RECOVERY_FILE="${STORAGE_DIR}/${NAME}/postgresql.conf"
	cat >> "${RECOVERY_FILE}" <<EOF
restore_command = 'cp ${WAL_DIR}/${NAME}/%f %p'
recovery_target_name = '${RESTORE_POINT_NAME}'
recovery_target_action = 'promote'
EOF
	instance_start ${NAME} ${HOST} ${PORT}
}

function instance_basebackup()
{
	NAME=$1
	HOST=$2
	PORT=$3
	echo "CHECKPOINT" |${PG_BINDIR}/psql -U ${PG_USER} -p ${PORT} -h ${HOST} ${NAME}
	${PG_BINDIR}/pg_basebackup -U ${PG_USER} --progress -p ${PORT} -h ${HOST} -D "${BASEBACKUP_DIR}/${NAME}"
}

function instance_cleanup_storage()
{
	NAME=$1
	rm -rf "${STORAGE_DIR:?}/${NAME}"
}

function instance_cleanup()
{
	NAME=$1
	rm -rf "${STORAGE_DIR:?}/${NAME}"
	rm -rf "${WAL_DIR:?}/${NAME}"
	rm -rf "${BASEBACKUP_DIR:?}/${NAME}"
}

function instance_cleanup_dirs()
{
	rm -rf ${STORAGE_DIR}
	rm -rf ${WAL_DIR}
	rm -rf ${BASEBACKUP_DIR}
	rm -rf ${RESULT_DIR}
}

function test_single_node()
{
	RESTORE_POINT_NAME=$1
	RESULT_FILE_NAME=${RESULT_DIR}/single_node_${RESTORE_POINT_NAME}

	# create and start single node instance
	instance_create ${SN_NAME} ${HOST} ${SN_PORT} ${RESTORE_POINT_NAME}

	# setup database
	${PG_BINDIR}/createdb -U ${PG_USER} -p ${SN_PORT} -h ${HOST} ${SN_NAME}

	${PG_BINDIR}/psql -U ${PG_USER} -p ${SN_PORT} -h ${HOST} ${SN_NAME} <<EOF
CREATE EXTENSION timescaledb;

CREATE TABLE test(time timestamp NOT NULL,  device int,  temp float);
SELECT create_hypertable('test', 'time', 'device', 3);

INSERT INTO test VALUES('2017-01-01 06:01', 1, 1.1), ('2017-01-01 09:11', 3, 2.1);
EOF
	# do initial basebackup
	instance_basebackup ${SN_NAME} ${HOST} ${SN_PORT}

	# insert more data and create the restore points
	${PG_BINDIR}/psql -U ${PG_USER} -p ${SN_PORT} -h ${HOST} ${SN_NAME} <<EOF
SELECT pg_create_restore_point('${TEST_RESTORE_POINT_1}');

INSERT INTO test VALUES('2017-01-01 08:01', 1, 1.2), ('2017-01-02 08:01', 2, 1.3);

SELECT pg_create_restore_point('${TEST_RESTORE_POINT_2}');

INSERT INTO test VALUES
('2018-07-02 08:01', 87, 1.6),
('2018-07-01 06:01', 13, 1.4),
('2018-07-01 09:11', 90, 2.7),
('2018-07-01 08:01', 29, 1.5);

SELECT pg_create_restore_point('${TEST_RESTORE_POINT_3}');
EOF

	# stop instance and cleanup data
	instance_stop ${SN_NAME}

	# delete original data
	instance_cleanup_storage ${SN_NAME}

	# recover database up to the restore point
	instance_recover ${SN_NAME} ${HOST} ${SN_PORT}

	# ensure database has expected set of row
	${PG_BINDIR}/psql -U ${PG_USER} -p ${SN_PORT} -h ${HOST} ${SN_NAME} > ${RESULT_FILE_NAME} <<EOF
SELECT * FROM test ORDER BY time;
EOF

	# stop instance and clean the repository
	instance_stop ${SN_NAME}
	instance_cleanup ${SN_NAME}
}

function test_multi_node()
{
	RESTORE_POINT_NAME=$1
	RESULT_FILE_NAME=${RESULT_DIR}/multi_node_${RESTORE_POINT_NAME}

	# setup instances
	instance_create ${AN_NAME}  ${HOST} ${AN_PORT}  ${RESTORE_POINT_NAME}
	instance_create ${DN1_NAME} ${HOST} ${DN1_PORT} ${RESTORE_POINT_NAME}
	instance_create ${DN2_NAME} ${HOST} ${DN2_PORT} ${RESTORE_POINT_NAME}
	instance_create ${DN3_NAME} ${HOST} ${DN3_PORT} ${RESTORE_POINT_NAME}

	# setup multinode cluster
	${PG_BINDIR}/createdb -U ${PG_USER} -p ${AN_PORT} -h ${HOST} ${AN_NAME}

	${PG_BINDIR}/psql -U ${PG_USER} -p ${AN_PORT} -h ${HOST} ${AN_NAME} <<EOF
CREATE EXTENSION timescaledb;

SELECT add_data_node('${DN1_NAME}', database => '${DN1_NAME}', host => '${HOST}', port => ${DN1_PORT});
SELECT add_data_node('${DN2_NAME}', database => '${DN2_NAME}', host => '${HOST}', port => ${DN2_PORT});
SELECT add_data_node('${DN3_NAME}', database => '${DN3_NAME}', host => '${HOST}', port => ${DN3_PORT});

CREATE TABLE test(time timestamp NOT NULL,  device int,  temp float);
SELECT create_distributed_hypertable('test', 'time', 'device', 3);

INSERT INTO test VALUES('2017-01-01 06:01', 1, 1.1), ('2017-01-01 09:11', 3, 2.1);
EOF

	# do initial basebackup
	instance_basebackup ${AN_NAME}  ${HOST} ${AN_PORT}
	instance_basebackup ${DN1_NAME} ${HOST} ${DN1_PORT}
	instance_basebackup ${DN2_NAME} ${HOST} ${DN2_PORT}
	instance_basebackup ${DN3_NAME} ${HOST} ${DN3_PORT}

	# insert more data and create the restore points
	${PG_BINDIR}/psql -U ${PG_USER} -p ${AN_PORT} -h ${HOST} ${AN_NAME} <<EOF
SELECT create_distributed_restore_point('${TEST_RESTORE_POINT_1}');

INSERT INTO test VALUES('2017-01-01 08:01', 1, 1.2), ('2017-01-02 08:01', 2, 1.3);

SELECT create_distributed_restore_point('${TEST_RESTORE_POINT_2}');

INSERT INTO test VALUES
('2018-07-02 08:01', 87, 1.6),
('2018-07-01 06:01', 13, 1.4),
('2018-07-01 09:11', 90, 2.7),
('2018-07-01 08:01', 29, 1.5);

SELECT create_distributed_restore_point('${TEST_RESTORE_POINT_3}');

INSERT INTO test VALUES ('2021-01-01 00:00', 95, 1.3);
EOF

	# stop instances and cleanup data
	instance_stop ${AN_NAME}
	instance_stop ${DN1_NAME}
	instance_stop ${DN2_NAME}
	instance_stop ${DN3_NAME}

	# delete original data
	instance_cleanup_storage ${AN_NAME}
	instance_cleanup_storage ${DN1_NAME}
	instance_cleanup_storage ${DN2_NAME}
	instance_cleanup_storage ${DN3_NAME}

	# recover multinode cluster up to the restore point
	instance_recover ${AN_NAME}  ${HOST} ${AN_PORT}
	instance_recover ${DN1_NAME} ${HOST} ${DN1_PORT}
	instance_recover ${DN2_NAME} ${HOST} ${DN2_PORT}
	instance_recover ${DN3_NAME} ${HOST} ${DN3_PORT}

	# ensure cluster has expected set of row
	${PG_BINDIR}/psql -U ${PG_USER} -p ${AN_PORT} -h ${HOST} ${AN_NAME} > ${RESULT_FILE_NAME} <<EOF
SELECT * FROM test ORDER BY time;
EOF

	# stop instances and clean repositories
	instance_stop ${AN_NAME}
	instance_stop ${DN1_NAME}
	instance_stop ${DN2_NAME}
	instance_stop ${DN3_NAME}

	instance_cleanup ${AN_NAME}
	instance_cleanup ${DN1_NAME}
	instance_cleanup ${DN2_NAME}
	instance_cleanup ${DN3_NAME}
}

function test_diff()
{
	RESTORE_POINT_NAME=$1
	diff ${RESULT_DIR}/single_node_${RESTORE_POINT_NAME} ${RESULT_DIR}/multi_node_${RESTORE_POINT_NAME}
}

if [ ${PG_VERSION_MAJOR} -lt 12 ]; then
  echo "Current PostgreSQL version is not supported (expected version >= PG12)"
  exit 1
fi

mkdir -p ${RESULT_DIR}

echo "Running single node tests"
test_single_node ${TEST_RESTORE_POINT_1}
test_single_node ${TEST_RESTORE_POINT_2}
test_single_node ${TEST_RESTORE_POINT_3}

echo "Running multi node tests"
test_multi_node ${TEST_RESTORE_POINT_1}
test_multi_node ${TEST_RESTORE_POINT_2}
test_multi_node ${TEST_RESTORE_POINT_3}

echo "Compare results"
test_diff ${TEST_RESTORE_POINT_1}
test_diff ${TEST_RESTORE_POINT_2}
test_diff ${TEST_RESTORE_POINT_3}

instance_cleanup_dirs
