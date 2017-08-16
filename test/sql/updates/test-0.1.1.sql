\d+ _timescaledb_catalog.*;
\df+ _timescaledb_internal.*;
\dy
\d+ PUBLIC.*

SELECT count(*)
  FROM pg_depend
 WHERE refclassid = 'pg_extension'::regclass
     AND refobjid = (SELECT oid FROM pg_extension WHERE extname = 'timescaledb');

-- The list of tables configured to be dumped.
SELECT unnest(extconfig)::regclass::text c from pg_extension where extname='timescaledb' ORDER BY c;

SELECT * FROM _timescaledb_catalog.chunk_constraint ORDER BY chunk_id, dimension_slice_id, constraint_name;
SELECT * FROM _timescaledb_catalog.hypertable_index ORDER BY hypertable_id, main_index_name;
SELECT schema_name, table_name, main_schema_name, main_index_name FROM _timescaledb_catalog.chunk_index ORDER BY schema_name, table_name, main_schema_name, main_index_name;

SELECT * FROM public."two_Partitions";
