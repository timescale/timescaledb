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
       -- PG17 introduced MAINTAIN acl (m) so removed it to keep output backward compatible
       replace(unnest(relacl)::text, 'm', '') as ACL
FROM pg_class JOIN pg_namespace ns ON relnamespace = ns.oid
WHERE nspname IN ('_timescaledb_catalog', '_timescaledb_config')
ORDER BY Schema, Name, ACL;

SELECT nspname AS schema,
       relname AS name,
       -- PG17 introduced MAINTAIN acl (m) so removed it to keep output backward compatible
       replace(unnest(initprivs)::text, 'm', '') AS initpriv
FROM pg_class cl JOIN pg_namespace ns ON ns.oid = relnamespace
            LEFT JOIN pg_init_privs ON objoid = cl.oid AND objsubid = 0
WHERE classoid = 'pg_class'::regclass
  AND nspname IN ('_timescaledb_catalog', '_timescaledb_config')
ORDER BY schema, name, initpriv;

\di _timescaledb_catalog.*
\ds+ _timescaledb_catalog.*
\df _timescaledb_internal.*
\df+ _timescaledb_internal.*
\df public.*;
\df+ public.*;

\dy
\d public.*

-- Keep the output backward compatible
\if :PG_UPGRADE_TEST
  SELECT oid AS extoid FROM pg_catalog.pg_extension WHERE extname = 'timescaledb' \gset

  WITH ext AS (
    SELECT pg_catalog.pg_describe_object(classid, objid, 0) AS objdesc
    FROM pg_catalog.pg_depend
    WHERE refclassid = 'pg_catalog.pg_extension'::pg_catalog.regclass AND refobjid = :'extoid' AND deptype = 'e'
    ORDER BY 1
  )
  SELECT objdesc AS "Object description" FROM ext
  WHERE objdesc !~ '^type' OR objdesc ~ '^type _timescaledb_internal.(compressed_data|dimension_info)$'
  ORDER BY 1;

  WITH ext AS (
    SELECT pg_catalog.pg_describe_object(classid, objid, 0) AS objdesc
    FROM pg_catalog.pg_depend
    WHERE refclassid = 'pg_catalog.pg_extension'::pg_catalog.regclass AND refobjid = :'extoid' AND deptype = 'e'
    ORDER BY 1
  )
  SELECT count(*) FROM ext
  WHERE objdesc !~ '^type' OR objdesc ~ '^type _timescaledb_internal.(compressed_data|dimension_info)$';
\else
  \dx+ timescaledb
  SELECT count(*)
    FROM pg_depend
   WHERE refclassid = 'pg_extension'::regclass
       AND refobjid = (SELECT oid FROM pg_extension WHERE extname = 'timescaledb');
\endif

-- The list of tables configured to be dumped.
SELECT unnest(extconfig)::regclass::text, unnest(extcondition) FROM pg_extension WHERE extname = 'timescaledb' ORDER BY 1;

-- Show chunks that are not dropped and include owner in the output
SELECT c.id, c.hypertable_id, c.schema_name, c.table_name, c.dropped, cl.relowner::regrole
FROM  _timescaledb_catalog.chunk c
INNER JOIN pg_class cl ON (cl.oid=format('%I.%I', schema_name, table_name)::regclass)
WHERE NOT c.dropped
ORDER BY c.id, c.hypertable_id;

SELECT chunk_constraint.* FROM _timescaledb_catalog.chunk_constraint
JOIN _timescaledb_catalog.chunk ON chunk.id = chunk_constraint.chunk_id
WHERE NOT chunk.dropped
ORDER BY chunk_constraint.chunk_id, chunk_constraint.dimension_slice_id, chunk_constraint.constraint_name;

SELECT index_name FROM _timescaledb_catalog.chunk_index ORDER BY index_name;

-- Show attnum of all regclass objects belonging to our extension
-- if those are not the same between fresh install/update our update scripts are broken
SELECT
  att.attrelid::regclass,
  att.attnum,
  att.attname
FROM pg_depend dep
  INNER JOIN pg_extension ext ON (dep.refobjid=ext.oid AND ext.extname = 'timescaledb')
  INNER JOIN pg_attribute att ON (att.attrelid=dep.objid AND att.attnum > 0)
WHERE classid='pg_class'::regclass
ORDER BY attrelid::regclass::text,att.attnum;

-- Show constraints
SELECT conrelid::regclass::text, conname, pg_get_constraintdef(oid)
FROM pg_constraint
WHERE conrelid::regclass::text ~ '^_timescaledb_'
ORDER BY 1, 2, 3;
