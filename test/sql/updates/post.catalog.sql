-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

SELECT NOT (extversion >= '2.19.0' AND extversion <= '2.20.3') AS has_fixed_compression_algorithms
  FROM pg_extension
 WHERE extname = 'timescaledb' \gset

\d+ _timescaledb_catalog.hypertable
\d+ _timescaledb_catalog.chunk
\d+ _timescaledb_catalog.dimension
\d+ _timescaledb_catalog.dimension_slice
\d+ _timescaledb_catalog.chunk_constraint
\d+ _timescaledb_catalog.tablespace

-- since we forgot to add bool and null compression with 2.19.0 to the preinstall
-- script fresh installations of 2.19+ won't have these compression algorithms
\if :has_fixed_compression_algorithms
SELECT * from _timescaledb_catalog.compression_algorithm algo ORDER BY algo;
\endif

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

-- Functions in schemas:
--   * _timescaledb_internal
--   * _timescaledb_functions
--   * public
SELECT n.nspname as "Schema",
  p.proname as "Name",
  pg_catalog.pg_get_function_result(p.oid) as "Result data type",
  pg_catalog.pg_get_function_arguments(p.oid) as "Argument data types",
 CASE p.prokind
  WHEN 'a' THEN 'agg'
  WHEN 'w' THEN 'window'
  WHEN 'p' THEN 'proc'
  ELSE 'func'
 END as "Type",
 CASE
  WHEN p.provolatile = 'i' THEN 'immutable'
  WHEN p.provolatile = 's' THEN 'stable'
  WHEN p.provolatile = 'v' THEN 'volatile'
 END as "Volatility",
 CASE
  WHEN p.proparallel = 'r' THEN 'restricted'
  WHEN p.proparallel = 's' THEN 'safe'
  WHEN p.proparallel = 'u' THEN 'unsafe'
 END as "Parallel",
 pg_catalog.pg_get_userbyid(p.proowner) as "Owner",
 CASE WHEN prosecdef THEN 'definer' ELSE 'invoker' END AS "Security",
 CASE WHEN pg_catalog.array_length(p.proacl, 1) = 0 THEN '(none)' ELSE pg_catalog.array_to_string(p.proacl, E'\n') END AS "Access privileges",
 l.lanname as "Language",
 p.prosrc as "Source code",
 CASE WHEN l.lanname IN ('internal', 'c') THEN p.prosrc END as "Internal name",
 pg_catalog.obj_description(p.oid, 'pg_proc') as "Description"
FROM pg_catalog.pg_proc p
     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
     LEFT JOIN pg_catalog.pg_language l ON l.oid = p.prolang
WHERE n.nspname OPERATOR(pg_catalog.~) '^(_timescaledb_internal|_timescaledb_functions|public)$' COLLATE pg_catalog.default
ORDER BY 1, 2, 4;

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

SELECT * FROM _timescaledb_catalog.compression_settings ORDER BY relid::regclass;
