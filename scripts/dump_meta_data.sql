-- This script will dump relevant meta data from internal TimescaleDB tables
-- that can help our engineers trouble shoot.
--
-- usage:
-- psql [your connect flags] -d your_timescale_db < dump_meta_data.sql > dumpfile.txt

\echo 'TimescaleDB meta data dump'
\echo '<exclude_from_test>'
\echo 'Date, git commit, and extension version can change without it being an error.'
\echo 'Adding this tag allows us to run regression tests on this script file.'
\echo `date`
\echo 'Build tag'
SELECT * FROM _timescaledb_internal.get_git_commit();

\dx

\echo '</exclude_from_test>'

\echo 'List of tables'
\dt

\echo 'List of hypertables'
SELECT * FROM _timescaledb_catalog.hypertable;

\echo 'List of hypertable indexes'
SELECT * FROM _timescaledb_catalog.hypertable_index;

\echo 'List of chunk indexes'
SELECT * FROM _timescaledb_catalog.chunk_index;

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
       array_agg('[' || _timescaledb_internal.range_value_to_pretty(range_start, column_type) ||
                 ',' ||
                 _timescaledb_internal.range_value_to_pretty(range_end, column_type) || ')' ORDER BY d.interval_length, d.column_name ASC) as ranges
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
       GROUP BY c.id, pgc.reltoastrelid, pgc.oid ORDER BY c.id
       ) sub1
) sub2;

\echo 'Hypertable index sizes'
SELECT h.schema_name || '.' || h.table_name as hypertable, hi.main_schema_name || '.' || hi.main_index_name as index_name,
       sum(pg_relation_size('"' || ci.schema_name || '"."' || ci.index_name || '"'))::bigint as index_bytes
FROM
_timescaledb_catalog.hypertable h,
_timescaledb_catalog.hypertable_index hi,
_timescaledb_catalog.chunk_index ci
WHERE h.id = hi.hypertable_id
      AND ci.main_index_name = hi.main_index_name
      AND ci.main_schema_name = hi.main_schema_name
GROUP BY h.schema_name || '.' || h.table_name, hi.main_schema_name || '.' || hi.main_index_name;

