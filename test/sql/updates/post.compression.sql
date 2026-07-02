-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

SELECT * FROM compress ORDER BY time DESC, small_cardinality, large_cardinality, some_double, some_int, some_custom, some_bool;

-- This recompression is necessary only for downgrades from 2.17 to 2.16.1
-- due to downgrade migration requiring to add sequence number metadata
-- column and causing compressed chunks to be unordered.
-- Recompressing the chunks fully fixes the difference.
SELECT count(decompress_chunk(ch, true)) FROM show_chunks('compress') ch;
SELECT count(compress_chunk(ch, true)) FROM show_chunks('compress') ch;

-- Running this query again to confirm data is consistent even after above recompression
SELECT * FROM compress ORDER BY time DESC, small_cardinality, large_cardinality, some_double, some_int, some_custom, some_bool;

INSERT INTO compress(time, small_cardinality, large_cardinality, some_double, some_int, some_custom, some_bool)
SELECT g, 'QW', g::text, 2, 0, (100,4)::custom_type_for_compression, false
FROM generate_series('2019-11-01 00:00'::timestamp, '2019-12-15 00:00'::timestamp, '1 day') g;

SELECT count(compress_chunk(ch, true)) FROM show_chunks('compress') ch;

SELECT * FROM compress ORDER BY time DESC, small_cardinality, large_cardinality, some_double, some_int, some_custom, some_bool;

\x on
WITH chunks AS (
  SELECT
    to_regclass(format('%I.%I', ht.schema_name, ht.table_name)) AS hypertable,
    cs.relid AS uncompressed_relation,
    cs.compress_relid AS compressed_relation
	  FROM _timescaledb_catalog.hypertable ht
    JOIN _timescaledb_catalog.chunk ch ON ch.hypertable_id = ht.id
    JOIN _timescaledb_catalog.compression_settings cs ON cs.relid = to_regclass(format('%I.%I', ch.schema_name, ch.table_name))
)
SELECT hypertable,
       (SELECT relacl FROM pg_class WHERE oid = hypertable) AS hypertable_acl,
       pg_temp.normalize_chunk(uncompressed_relation::text) AS chunk_name,
       (SELECT relacl FROM pg_class WHERE oid = uncompressed_relation) AS uncompressed_chunk_acl,
       pg_temp.normalize_chunk(compressed_relation::text) AS compressed_chunk_name,
       (SELECT relacl FROM pg_class WHERE oid = compressed_relation) AS compressed_chunk_acl
  FROM chunks
  ORDER BY hypertable::text COLLATE "C", pg_temp.normalize_chunk(uncompressed_relation::text) COLLATE "C";
\x off

