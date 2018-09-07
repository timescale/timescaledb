pg_dump -h ${PGHOST} -U ${TEST_ROLE_SUPERUSER} -Fc single > dump/single.sql
dropdb -h ${PGHOST} -U ${TEST_ROLE_SUPERUSER} single
createdb -h ${PGHOST} -U ${TEST_ROLE_SUPERUSER} single
