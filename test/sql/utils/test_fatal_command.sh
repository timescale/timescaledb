DB=$1
COMMAND=$2
${PG_BINDIR}/psql -X -h ${PGHOST} -U ${TEST_ROLE_SUPERUSER} -d ${DB} --command="${COMMAND}" 2>&1 | grep -v CONTEXT | grep -v "extension script file"
