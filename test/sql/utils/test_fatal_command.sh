DB=$1
COMMAND=$2
${PG_BINDIR}/psql -X -h ${PGHOST} -U ${TEST_ROLE_SUPERUSER} -d ${DB} --command="${COMMAND}"
