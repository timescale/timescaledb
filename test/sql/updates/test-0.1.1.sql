\d+ _timescaledb_catalog.*;
\df+ _timescaledb_internal.*;
\dy

SELECT count(*)
  FROM pg_depend
 WHERE refclassid = 'pg_extension'::regclass
     AND refobjid = (SELECT oid FROM pg_extension WHERE extname = 'timescaledb');

-- The list of tables configured to be dumped.
SELECT unnest(extconfig)::regclass::text c from pg_extension where extname='timescaledb' ORDER BY c;
