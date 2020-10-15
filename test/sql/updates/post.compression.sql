-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

SELECT * FROM compress ORDER BY time DESC, small_cardinality;

INSERT INTO compress
SELECT g, 'QW', g::text, 2, 0, (100,4)::custom_type_for_compression, false
FROM generate_series('2019-11-01 00:00'::timestamp, '2019-12-15 00:00'::timestamp, '1 day') g;

SELECT
  count(compress_chunk(chunk.schema_name || '.' || chunk.table_name)) AS count_compressed
FROM
  _timescaledb_catalog.chunk chunk
  INNER JOIN _timescaledb_catalog.hypertable hypertable ON (chunk.hypertable_id = hypertable.id)
WHERE
  hypertable.table_name = 'compress'
  AND chunk.compressed_chunk_id IS NULL;

SELECT * FROM compress ORDER BY time DESC, small_cardinality;

-- check count and approximate_row_count are the same after analyze
ANALYZE compress;
SELECT
  count,
  approximate,
  CASE WHEN count != approximate THEN
    'counts not matching' || random()::TEXT
  ELSE
    'match'
  END AS MATCH
FROM (
  SELECT
    count(*)
  FROM
    compress) AS count,
  approximate_row_count('compress') AS approximate;

SELECT
  hypertable_schema,
  hypertable_name,
  approximate_row_count(format('%I.%I', hypertable_schema, hypertable_name)::REGCLASS)
FROM
  timescaledb_information.hypertables
WHERE
  compression_enabled = true
ORDER BY
  1,
  2;
