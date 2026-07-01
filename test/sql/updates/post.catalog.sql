-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- Chunk relation names are not deterministic between a fresh install and a post-upgrade catalog.
CREATE OR REPLACE FUNCTION pg_temp.normalize_chunk(t text) RETURNS text
  LANGUAGE sql IMMUTABLE AS $$
  SELECT regexp_replace(
           regexp_replace(t,
             'compress_hyper_[0-9]+_[0-9]+_chunk|_hyper_[0-9]+_[0-9]+_chunk_compressed',
             'compressed_chunk', 'g'),
           '(_hyper_[0-9]+)_[0-9]+_chunk', '\1_X_chunk', 'g')
$$;

SELECT NOT (extversion >= '2.19.0' AND extversion <= '2.20.3') AS has_fixed_compression_algorithms
  FROM pg_extension
 WHERE extname = 'timescaledb' \gset

SELECT (extversion >= '2.28.0') AS has_chunk_owned_slices
  FROM pg_extension WHERE extname = 'timescaledb' \gset

\if :PG_UPGRADE_TEST
\else
\d+ _timescaledb_catalog.hypertable
\d+ _timescaledb_catalog.chunk
\d+ _timescaledb_catalog.dimension
\d+ _timescaledb_catalog.dimension_slice
\d+ _timescaledb_catalog.tablespace
\endif

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

-- indexes in _timescaledb_catalog schema
SELECT n.nspname as "Schema",
  c.relname as "Name",
  CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' WHEN 'm' THEN 'materialized view' WHEN 'i' THEN 'index' WHEN 'S' THEN 'sequence' WHEN 't' THEN 'TOAST table' WHEN 'f' THEN 'foreign table' WHEN 'p' THEN 'partitioned table' WHEN 'I' THEN 'partitioned index' END as "Type",
  pg_catalog.pg_get_userbyid(c.relowner) as "Owner",
  c2.relname as "Table"
FROM pg_catalog.pg_class c
     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
     LEFT JOIN pg_catalog.pg_am am ON am.oid = c.relam
     LEFT JOIN pg_catalog.pg_index i ON i.indexrelid = c.oid
     LEFT JOIN pg_catalog.pg_class c2 ON i.indrelid = c2.oid
WHERE c.relkind IN ('i','I','')
      AND n.nspname = '_timescaledb_catalog'
  AND pg_catalog.pg_table_is_visible(c.oid)
ORDER BY 1,2;

-- sequences in _timescaledb_catalog schema
SELECT n.nspname as "Schema",
  c.relname as "Name",
  CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' WHEN 'm' THEN 'materialized view' WHEN 'i' THEN 'index' WHEN 'S' THEN 'sequence' WHEN 't' THEN 'TOAST table' WHEN 'f' THEN 'foreign table' WHEN 'p' THEN 'partitioned table' WHEN 'I' THEN 'partitioned index' END as "Type",
  pg_catalog.pg_get_userbyid(c.relowner) as "Owner",
  CASE c.relpersistence WHEN 'p' THEN 'permanent' WHEN 't' THEN 'temporary' WHEN 'u' THEN 'unlogged' END as "Persistence",
  pg_catalog.pg_size_pretty(pg_catalog.pg_table_size(c.oid)) as "Size",
  pg_catalog.obj_description(c.oid, 'pg_class') as "Description"
FROM pg_catalog.pg_class c
     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('S','')
      AND n.nspname = '_timescaledb_catalog'
  AND pg_catalog.pg_table_is_visible(c.oid)
ORDER BY 1,2;

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

-- Relations in the public schema and their columns
SELECT n.nspname || '.' || c.relname AS relation,
       CASE c.relkind
         WHEN 'r' THEN 'table'
         WHEN 'p' THEN 'partitioned table'
         WHEN 'v' THEN 'view'
         WHEN 'm' THEN 'materialized view'
         WHEN 'c' THEN 'composite type'
         WHEN 'f' THEN 'foreign table'
       END AS kind,
       a.attname,
       pg_catalog.format_type(a.atttypid, a.atttypmod) AS type,
       a.attnotnull AS notnull,
       pg_catalog.pg_get_expr(d.adbin, d.adrelid) AS "default"
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid
LEFT JOIN pg_catalog.pg_attrdef d ON d.adrelid = c.oid AND d.adnum = a.attnum
WHERE n.nspname = 'public'
  AND c.relkind IN ('r', 'p', 'v', 'm', 'c', 'f')
  AND a.attnum > 0
  AND NOT a.attisdropped
ORDER BY relation, a.attnum;

-- indexes on public tables
SELECT c.relname AS table_name,
       i.relname AS index_name,
       pg_catalog.pg_get_indexdef(idx.indexrelid) AS definition
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
JOIN pg_catalog.pg_index idx ON idx.indrelid = c.oid
JOIN pg_catalog.pg_class i ON i.oid = idx.indexrelid
WHERE n.nspname = 'public'
ORDER BY 1, 2;

-- constraints on public tables
SELECT
    conname AS constraint_name,
		conrelid::regclass AS conrelid,
		confrelid::regclass AS confrelid,
    contype,
    pg_get_constraintdef(con.oid) AS def
FROM pg_catalog.pg_constraint con
JOIN pg_catalog.pg_class c ON c.oid = conrelid AND c.relnamespace = 'public'::regnamespace
WHERE con.contype <> 'n'
ORDER BY conrelid::regclass::text, contype, conname, confrelid::regclass::text;

-- child tables
SELECT parent.relname AS table_name,
    pg_temp.normalize_chunk(i.inhrelid::regclass::text) AS child_table
FROM pg_catalog.pg_inherits i
JOIN pg_catalog.pg_class parent ON parent.oid = i.inhparent AND parent.relnamespace = 'public'::regnamespace
ORDER BY parent.relname, pg_temp.normalize_chunk(i.inhrelid::regclass::text);

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

-- Show chunks that include owner in the output. The chunk id is not
-- deterministic post-upgrade, so drop it and normalize the chunk relation name.
SELECT c.hypertable_id, c.schema_name, pg_temp.normalize_chunk(c.table_name) AS table_name, cl.relowner::regrole
FROM  _timescaledb_catalog.chunk c
INNER JOIN pg_class cl ON (cl.oid=format('%I.%I', schema_name, table_name)::regclass)
ORDER BY c.hypertable_id, pg_temp.normalize_chunk(c.table_name);

-- Per-chunk dimensional ranges. Slice ids are assigned by SERIAL and chunk ids
-- are renumbered, so both differ between a fresh install and a post-upgrade
-- catalog. Dump and order by the dimensional range only.
\if :has_chunk_owned_slices
SELECT ds.dimension_id, ds.range_start, ds.range_end
FROM _timescaledb_catalog.dimension_slice ds
ORDER BY ds.dimension_id, ds.range_start, ds.range_end;
\else
SELECT ds.dimension_id, ds.range_start, ds.range_end
FROM _timescaledb_catalog.chunk_constraint cc
JOIN _timescaledb_catalog.dimension_slice ds ON ds.id = cc.dimension_slice_id
ORDER BY ds.dimension_id, ds.range_start, ds.range_end;
\endif

-- Show attributes of all regclass objects belonging to our extension
-- if those are not the same between fresh install/update our update scripts are broken
-- attstattarget default is -1 in PG16 and NULL in later versions
SELECT
  a.attrelid::regclass, a.attnum, a.attname, a.atttypid::regtype, a.attlen, a.atttypmod,
  a.attndims, a.attbyval, a.attalign, a.attstorage, a.attcompression, a.attnotnull,
  a.atthasdef, a.atthasmissing, a.attidentity, a.attgenerated, a.attisdropped, a.attislocal,
  a.attinhcount, a.attcollation::regcollation,
  NULLIF(a.attstattarget, -1) AS attstattarget,
  a.attacl, a.attoptions, a.attfdwoptions, a.attmissingval
FROM pg_depend dep
  INNER JOIN pg_extension ext ON (dep.refobjid=ext.oid AND ext.extname = 'timescaledb')
  INNER JOIN pg_attribute a ON (a.attrelid=dep.objid AND a.attnum > 0)
WHERE classid='pg_class'::regclass
ORDER BY attrelid::regclass::text COLLATE "C", a.attnum;

-- Show constraints, stripping numeric prefixes so the legacy
-- "<chunk_id>_<seq>_<parent>" form (2.27.1) and the new "<chunk_id>_<parent>"
-- form (2.28.0) compare equal. The dimensional CHECKs are named
-- "constraint_<slice_id>" and slice ids are not deterministic between a
-- fresh install and a post-upgrade catalog, so collapse the suffix too.
SELECT pg_temp.normalize_chunk(conrelid::regclass::text) AS conrelid,
       regexp_replace(
           regexp_replace(conname, '^([0-9]+_){1,2}', ''),
           '^constraint_[0-9]+$', 'constraint_dim') AS conname,
       pg_get_constraintdef(oid)
FROM pg_constraint
WHERE conrelid::regclass::text ~ '^_timescaledb_'
\if :PG_UPGRADE_TEST
AND pg_get_constraintdef(oid) NOT LIKE 'NOT NULL %'
\endif
ORDER BY 1, 2, 3;

-- The index column depends on the version that compressed each chunk (the
-- orderby sparse index type changed across releases and existing chunks are
-- not rewritten on upgrade), so it differs between a fresh install and a
-- post-upgrade catalog. Skip it and compare the remaining columns.
SELECT pg_temp.normalize_chunk(relid::regclass::text) AS relid,
       pg_temp.normalize_chunk(compress_relid::regclass::text) AS compress_relid,
       segmentby, orderby, orderby_desc, orderby_nullsfirst
FROM _timescaledb_catalog.compression_settings
ORDER BY pg_temp.normalize_chunk(relid::regclass::text), segmentby, orderby;
