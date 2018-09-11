# Redirect output to /dev/null to suppress NOTICE
${PG_BINDIR}/pg_restore -h ${PGHOST} -U ${TEST_ROLE_SUPERUSER} -d single dump/single.sql > /dev/null 2>&1
