-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

SELECT * FROM compress ORDER BY time DESC, small_cardinality, large_cardinality, some_double, some_int, some_custom, some_bool;

INSERT INTO compress
SELECT g, 'QW', g::text, 2, 0, (100,4)::custom_type_for_compression, false
FROM generate_series('2019-11-01 00:00'::timestamp, '2019-12-15 00:00'::timestamp, '1 day') g;

SELECT
  count(compress_chunk(chunks.chunk_schema || '.' || chunks.chunk_name)) AS count_compressed
FROM
  timescaledb_information.chunks
WHERE
  hypertable_name = 'compress'
  AND NOT is_compressed;

SELECT * FROM compress ORDER BY time DESC, small_cardinality, large_cardinality, some_double, some_int, some_custom, some_bool;

\x on
WITH hypertables AS (
        SELECT ht.id hypertable_id,
	       ht.schema_name,
	       ht.table_name,
	       ht.compressed_hypertable_id
          FROM pg_class cl JOIN pg_namespace ns ON ns.oid = relnamespace
	  JOIN _timescaledb_catalog.hypertable ht ON relname = ht.table_name AND nspname = ht.schema_name
    ),
    table_summary AS (
	SELECT format('%I.%I', ht1.schema_name, ht1.table_name) AS hypertable_name,
	       format('%I.%I', ht2.schema_name, ht2.table_name) AS compressed_hypertable_name,
	       format('%I.%I', ch2.schema_name, ch2.table_name) AS compressed_chunk_name
	FROM hypertables ht1
	JOIN hypertables ht2 ON ht1.compressed_hypertable_id = ht2.hypertable_id
        JOIN _timescaledb_catalog.chunk ch2 ON ch2.hypertable_id = ht2.hypertable_id
    )
SELECT hypertable_name,
       (SELECT relacl FROM pg_class WHERE oid = hypertable_name::regclass) AS hypertable_acl,
       compressed_hypertable_name,
       (SELECT relacl FROM pg_class WHERE oid = compressed_hypertable_name::regclass) AS compressed_hypertable_acl,
       compressed_chunk_name,
       (SELECT relacl FROM pg_class WHERE oid = compressed_chunk_name::regclass) AS compressed_chunk_acl
  FROM table_summary
  ORDER BY hypertable_name, compressed_hypertable_name, compressed_chunk_name;
\x off

