DB=$1
COMMAND=$2
${PG_BINDIR}/psql -h ${PGHOST} -U ${TEST_ROLE_SUPERUSER} -d ${DB} --command="${COMMAND}"