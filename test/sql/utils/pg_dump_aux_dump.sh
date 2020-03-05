export PGOPTIONS

DUMPFILE=$1
PGOPTIONS='--client-min-messages=warning'

# PG12 changed client logging so NOTICE messages are now warning.
# We adopt this also for older PG versions to make tests compatible.
${PG_BINDIR}/pg_dump -h ${PGHOST} -U ${TEST_ROLE_SUPERUSER} -Fc ${TEST_DBNAME} 2>&1 > ${DUMPFILE} | sed 's/NOTICE/warning/g'
${PG_BINDIR}/dropdb -h ${PGHOST} -U ${TEST_ROLE_SUPERUSER} ${TEST_DBNAME}
${PG_BINDIR}/createdb -h ${PGHOST} -U ${TEST_ROLE_SUPERUSER} ${TEST_DBNAME}
