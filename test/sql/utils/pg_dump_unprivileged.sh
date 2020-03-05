export PGOPTIONS
PGOPTIONS='--client-min-messages=warning'

# PG12 changed client logging so NOTICE messages are now warning.
# We adopt this also for older PG versions to make tests compatible.
${PG_BINDIR}/pg_dump -h ${PGHOST} -U dump_unprivileged dump_unprivileged 2>&1 > /dev/null | sed 's/NOTICE/warning/g'

if [ $? -eq 0 ]; then
  echo "Database dumped successfully"
fi
