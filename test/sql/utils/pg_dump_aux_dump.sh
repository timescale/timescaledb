DUMPFILE=${DUMPFILE:-$1}
# Override PGOPTIONS to remove verbose output
PGOPTIONS='--client-min-messages=warning'

export PGOPTIONS

${PG_BINDIR}/pg_dump -h ${PGHOST} -U ${TEST_ROLE_SUPERUSER} -Fc ${TEST_DBNAME} > /dev/null 2>&1 -f ${DUMPFILE}
${PG_BINDIR}/dropdb -h ${PGHOST} -U ${TEST_ROLE_SUPERUSER} ${TEST_DBNAME}
${PG_BINDIR}/createdb -h ${PGHOST} -U ${TEST_ROLE_SUPERUSER} ${TEST_DBNAME}
