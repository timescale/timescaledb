export PGOPTIONS
PGOPTIONS='--client-min-messages=warning'

${PG_BINDIR}/pg_dump -h ${PGHOST} -U dump_unprivileged dump_unprivileged > /dev/null

if [ $? -eq 0 ]; then
  echo "Database dumped successfully"
fi

