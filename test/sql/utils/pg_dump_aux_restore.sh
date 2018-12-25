DUMPFILE=$1

# Redirect output to /dev/null to suppress NOTICE
${PG_BINDIR}/pg_restore -h ${PGHOST} -U ${TEST_ROLE_SUPERUSER} -d ${TEST_DBNAME} ${DUMPFILE} > /dev/null 2>&1
