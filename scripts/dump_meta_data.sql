--
-- This file is licensed under the Apache License, see LICENSE-APACHE
-- at the top level directory of the TimescaleDB distribution.

-- This script will dump relevant meta data from internal TimescaleDB tables
-- that can help our engineers trouble shoot.
--
-- usage:
-- psql [your connect flags] -d your_timescale_db < dump_meta_data.sql > dumpfile.txt

\echo 'TimescaleDB meta data dump'
\echo '<exclude_from_test>'
\echo 'Date, git commit, and extension version can change without it being an error.'
\echo 'Adding this tag allows us to run regression tests on this script file.'
select now();
\echo 'Postgres version'
select version();

\echo 'Build tag'
\set ON_ERROR_STOP 0
SELECT * FROM _timescaledb_internal.get_git_commit();
\set ON_ERROR_STOP 1
\dx

\echo '</exclude_from_test>'

\echo 'List of tables'
\dt

\echo 'List of hypertables'
SELECT * FROM _timescaledb_catalog.hypertable;

\echo 'List of chunk indexes'
SELECT ch.id AS chunk_id, ci.indexrelid::regclass::text AS index_name, h.id AS hypertable_id
FROM _timescaledb_catalog.hypertable h
JOIN _timescaledb_catalog.chunk ch ON ch.hypertable_id = h.id
JOIN pg_index ci ON ci.indrelid = format('%I.%I', ch.schema_name, ch.table_name)::regclass
ORDER BY h.id, ch.id, ci.indrelid::regclass::text;

\echo 'Size of hypertables'
SELECT hypertable,
       table_bytes,
       index_bytes,
       toast_bytes,
       total_bytes
       FROM (
       SELECT *, total_bytes-index_bytes-COALESCE(toast_bytes,0) AS table_bytes FROM (
              SELECT
              pgc.oid::regclass::text as hypertable,
              sum(pg_total_relation_size('"' || c.schema_name || '"."' || c.table_name || '"'))::bigint as total_bytes,
              sum(pg_indexes_size('"' || c.schema_name || '"."' || c.table_name || '"'))::bigint AS index_bytes,
              sum(pg_total_relation_size(reltoastrelid))::bigint AS toast_bytes
              FROM
              _timescaledb_catalog.hypertable h,
              _timescaledb_catalog.chunk c,
              pg_class pgc,
              pg_namespace pns
              WHERE c.hypertable_id = h.id
              AND pgc.relname = h.table_name
              AND pns.oid = pgc.relnamespace
              AND pns.nspname = h.schema_name
              AND relkind = 'r'
              AND c.dropped is false
              GROUP BY pgc.oid
              ) sub1
       ) sub2;

\echo 'Chunk sizes:'
SELECT chunk_id,
chunk_table,
partitioning_columns,
partitioning_column_types,
partitioning_hash_functions,
ranges,
table_bytes,
index_bytes,
toast_bytes,
total_bytes
FROM (
SELECT *,
      total_bytes-index_bytes-COALESCE(toast_bytes,0) AS table_bytes
      FROM (
       SELECT c.id as chunk_id,
       '"' || c.schema_name || '"."' || c.table_name || '"' as chunk_table,
       pg_total_relation_size('"' || c.schema_name || '"."' || c.table_name || '"') AS total_bytes,
       pg_indexes_size('"' || c.schema_name || '"."' || c.table_name || '"') AS index_bytes,
       pg_total_relation_size(reltoastrelid) AS toast_bytes,
       array_agg(d.column_name ORDER BY d.interval_length, d.column_name ASC) as partitioning_columns,
       array_agg(d.column_type ORDER BY d.interval_length, d.column_name ASC) as partitioning_column_types,
       array_agg(d.partitioning_func_schema || '.' || d.partitioning_func ORDER BY d.interval_length, d.column_name ASC) as partitioning_hash_functions,
       array_agg('[' || _timescaledb_functions.range_value_to_pretty(range_start, column_type) ||
                 ',' ||
                 _timescaledb_functions.range_value_to_pretty(range_end, column_type) || ')' ORDER BY d.interval_length, d.column_name ASC) as ranges
       FROM
       _timescaledb_catalog.hypertable h,
       _timescaledb_catalog.chunk c,
       _timescaledb_catalog.chunk_constraint cc,
       _timescaledb_catalog.dimension d,
       _timescaledb_catalog.dimension_slice ds,
       pg_class pgc,
       pg_namespace pns
       WHERE pgc.relname = h.table_name
             AND pns.oid = pgc.relnamespace
             AND pns.nspname = h.schema_name
             AND relkind = 'r'
             AND c.hypertable_id = h.id
             AND c.id = cc.chunk_id
             AND cc.dimension_slice_id = ds.id
             AND ds.dimension_id = d.id
             AND c.dropped is false
       GROUP BY c.id, pgc.reltoastrelid, pgc.oid ORDER BY c.id
       ) sub1
) sub2;

\echo 'Hypertable index sizes'
SET search_path TO pg_catalog, pg_temp;
SELECT
  i.indrelid::regclass AS hypertable,
  i.indexrelid::regclass AS index_name,
  public.hypertable_index_size(i.indexrelid::regclass) AS index_bytes
FROM _timescaledb_catalog.hypertable h
JOIN pg_index i ON i.indrelid = format('%I.%I',h.schema_name,h.table_name)::regclass
ORDER BY i.indrelid::regclass::text, i.indexrelid::regclass::text;
RESET search_path;

