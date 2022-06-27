DUMPFILE=${DUMPFILE:-$1}
# Override PGOPTIONS to remove verbose output
PGOPTIONS='--client-min-messages=warning'

export PGOPTIONS

# PG12 changed client logging so NOTICE messages are now warning.
# We adopt this also for older PG versions to make tests compatible.

${PG_BINDIR}/pg_dump -h ${PGHOST} -U dump_owner -Fc dump_owner 2>&1 > ${DUMPFILE} | sed 's/NOTICE/warning/g'

if [ $? -eq 0 ]; then
  echo "Database dumped successfully"
fi

# to check the contents, will remove
echo "First few lines of dump: "
cat ${DUMPFILE} 
echo ""
