-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

\d+ _timescaledb_catalog.hypertable
\d+ _timescaledb_catalog.chunk
\d+ _timescaledb_catalog.dimension
\d+ _timescaledb_catalog.dimension_slice
\d+ _timescaledb_catalog.chunk_constraint
\d+ _timescaledb_catalog.chunk_index
\d+ _timescaledb_catalog.tablespace

SELECT nspname AS Schema,
       relname AS Name,
       unnest(relacl)::text as ACL
FROM pg_class JOIN pg_namespace ns ON relnamespace = ns.oid
WHERE nspname IN ('_timescaledb_catalog', '_timescaledb_config')
ORDER BY Schema, Name, ACL;

SELECT nspname AS schema,
       relname AS name,
       unnest(initprivs)::text AS initpriv
FROM pg_class cl JOIN pg_namespace ns ON ns.oid = relnamespace
            LEFT JOIN pg_init_privs ON objoid = cl.oid AND objsubid = 0
WHERE classoid = 'pg_class'::regclass
  AND nspname IN ('_timescaledb_catalog', '_timescaledb_config')
ORDER BY schema, name, initpriv;

\di _timescaledb_catalog.*
\ds+ _timescaledb_catalog.*;
\df _timescaledb_internal.*;
\df+ _timescaledb_internal.*;
\df public.*;
\df+ public.*;

\dy
\d+ public.*

\dx+ timescaledb
SELECT count(*)
  FROM pg_depend
 WHERE refclassid = 'pg_extension'::regclass
     AND refobjid = (SELECT oid FROM pg_extension WHERE extname = 'timescaledb');

-- The list of tables configured to be dumped.
SELECT obj::regclass::text
FROM (SELECT unnest(extconfig) AS obj FROM pg_extension WHERE extname='timescaledb') AS objects
ORDER BY obj::regclass::text;

-- Show dropped chunks
SELECT *
FROM  _timescaledb_catalog.chunk c
WHERE c.dropped
ORDER BY c.id, c.hypertable_id;

-- Show chunks that are not dropped and include owner in the output
SELECT c.*, cl.relowner::regrole
FROM  _timescaledb_catalog.chunk c
INNER JOIN pg_class cl ON (cl.oid=format('%I.%I', schema_name, table_name)::regclass)
WHERE NOT c.dropped
ORDER BY c.id, c.hypertable_id;

SELECT * FROM _timescaledb_catalog.chunk_constraint ORDER BY chunk_id, dimension_slice_id, constraint_name;
SELECT index_name FROM _timescaledb_catalog.chunk_index ORDER BY index_name;

-- Indices can have different column names between an upgrade and a
-- restore of a dump, so we only list the tables. This will include
-- the indexes defined on the tables, but not the exact definition of
-- the indexes and in particular not the column name in the index
-- which will be different in a restored database and an updated
-- database for columns that were renamed before the update.
