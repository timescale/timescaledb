-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

CREATE TABLE t1 (time timestamptz, device text, value float) WITH (tsdb.hypertable);
CREATE TABLE t2 (time timestamptz, device int, value float) WITH (tsdb.hypertable);

INSERT INTO t1 SELECT '2025-01-01'::timestamptz + format('%s ms', i)::interval, format('Device %s',i%1000), 1 + 1/i FROM generate_series(1, 100000) i;
INSERT INTO t2 SELECT '2025-01-01'::timestamptz + format('%s ms', i)::interval, i%1000, 1 + 1/i FROM generate_series(1, 100000) i;

VACUUM FULL t1;
VACUUM FULL t2;

-- VACUUM FULL each chunk before compression to eliminate bloat and stabilize uncompressed sizes
DO $$
DECLARE
    chunk_name text;
BEGIN
    FOR chunk_name IN SELECT show_chunks('t1')
    LOOP
        EXECUTE format('VACUUM FULL %s', chunk_name);
    END LOOP;
    FOR chunk_name IN SELECT show_chunks('t2')
    LOOP
        EXECUTE format('VACUUM FULL %s', chunk_name);
    END LOOP;
END $$;

SELECT compress_chunk(chunk) FROM show_chunks('t1') AS chunk;
SELECT compress_chunk(chunk) FROM show_chunks('t2') AS chunk;


SELECT ccs.compressed_chunk_id,round(ccs.uncompressed_heap_size, -5) uncompressed_heap_size,ccs.uncompressed_toast_size,round(ccs.uncompressed_index_size,-5) uncompressed_index_size, ccs.uncompressed_heap_size + ccs.uncompressed_toast_size + ccs.uncompressed_index_size AS uncompressed_total_size, l.relation_size, l.index_size, l.total_size FROM _timescaledb_catalog.compression_chunk_size ccs JOIN _timescaledb_catalog.chunk ch ON ch.id=ccs.compressed_chunk_id JOIN LATERAL (SELECT * FROM _timescaledb_functions.estimate_uncompressed_size(format('%I.%I',ch.schema_name,ch.table_name))) l ON true;

