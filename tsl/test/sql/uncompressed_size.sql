-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

CREATE TABLE t1 (time timestamptz, device text, value float) WITH (tsdb.hypertable);
CREATE TABLE t2 (time timestamptz, device int, value float) WITH (tsdb.hypertable);

INSERT INTO t1 SELECT '2025-01-01'::timestamptz + format('%s ms', i)::interval, format('Device %s',i%1000), 1 + 1/i FROM generate_series(1, 100000) i;
INSERT INTO t2 SELECT '2025-01-01'::timestamptz + format('%s ms', i)::interval, i%1000, 1 + 1/i FROM generate_series(1, 100000) i;

VACUUM FULL t1;
VACUUM FULL t2;

SELECT compress_chunk(chunk) FROM show_chunks('t1') AS chunk;
SELECT compress_chunk(chunk) FROM show_chunks('t2') AS chunk;


SELECT cs.compress_relid, l.relation_size, l.index_size, l.total_size FROM _timescaledb_catalog.compression_chunk_size ccs JOIN _timescaledb_catalog.chunk uc ON uc.id=ccs.chunk_id JOIN _timescaledb_catalog.compression_settings cs ON cs.relid=format('%I.%I',uc.schema_name,uc.table_name)::regclass JOIN LATERAL (SELECT * FROM _timescaledb_functions.estimate_uncompressed_size(cs.compress_relid)) l ON true;

-- test NULL compression does not error and returns NULL
INSERT INTO t1 SELECT '2026-01-01'::timestamptz + format('%s ms', i)::interval, NULL, NULL FROM generate_series(1, 3000) i;
SELECT compress_chunk(chunk) FROM show_chunks('t1') AS chunk;
SELECT _timescaledb_functions.compressed_data_info(time), _timescaledb_functions.compressed_data_info(device), _timescaledb_functions.compressed_data_info(value) FROM _timescaledb_internal.compress_hyper_2_6_chunk;
SELECT * FROM _timescaledb_functions.estimate_uncompressed_size('_timescaledb_internal.compress_hyper_2_6_chunk');
