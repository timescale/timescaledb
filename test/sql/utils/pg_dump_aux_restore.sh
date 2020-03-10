DUMPFILE=${DUMPFILE:-$1}
# Override PGOPTIONS to remove verbose output
PGOPTIONS='--client-min-messages=warning'

export PGOPTIONS

# Redirect output to /dev/null to suppress NOTICE
${PG_BINDIR}/pg_restore -h ${PGHOST} -U ${TEST_ROLE_SUPERUSER} -d ${TEST_DBNAME} ${DUMPFILE} > /dev/null 2>&1
