${PG_BINDIR}/pg_dump -h ${PGHOST} -U ${TEST_ROLE_SUPERUSER} -Fc single > dump/single.sql
${PG_BINDIR}/dropdb -h ${PGHOST} -U ${TEST_ROLE_SUPERUSER} single
${PG_BINDIR}/createdb -h ${PGHOST} -U ${TEST_ROLE_SUPERUSER} single
