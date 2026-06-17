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

-- samplerate of 100 reads the whole chunk and matches the default
SELECT
  d.tuples = s.tuples AS tuples_match,
  d.relation_size = s.relation_size AS relation_size_match,
  d.index_size = s.index_size AS index_size_match,
  d.total_size = s.total_size AS total_size_match
FROM _timescaledb_functions.estimate_uncompressed_size('_timescaledb_internal.compress_hyper_2_3_chunk') d,
     _timescaledb_functions.estimate_uncompressed_size('_timescaledb_internal.compress_hyper_2_3_chunk', 100) s;

-- samplerate scales the sampled variable length size back up to the full chunk
SELECT
  s.tuples = d.tuples AS tuples_match,
  s.relation_size BETWEEN d.relation_size * 0.8 AND d.relation_size * 1.2 AS relation_size_in_range,
  s.index_size BETWEEN d.index_size * 0.8 AND d.index_size * 1.2 AS index_size_in_range,
  s.total_size BETWEEN d.total_size * 0.8 AND d.total_size * 1.2 AS total_size_in_range
FROM _timescaledb_functions.estimate_uncompressed_size('_timescaledb_internal.compress_hyper_2_3_chunk') d,
     _timescaledb_functions.estimate_uncompressed_size('_timescaledb_internal.compress_hyper_2_3_chunk', 50) s;

-- samplerate outside the (0, 100] range is rejected
\set ON_ERROR_STOP 0
SELECT * FROM _timescaledb_functions.estimate_uncompressed_size('_timescaledb_internal.compress_hyper_2_3_chunk', 0);
SELECT * FROM _timescaledb_functions.estimate_uncompressed_size('_timescaledb_internal.compress_hyper_2_3_chunk', -5);
\set ON_ERROR_STOP 1

-- test NULL compression does not error and returns NULL
INSERT INTO t1 SELECT '2026-01-01'::timestamptz + format('%s ms', i)::interval, NULL, NULL FROM generate_series(1, 3000) i;
SELECT compress_chunk(chunk) FROM show_chunks('t1') AS chunk;
SELECT _timescaledb_functions.compressed_data_info(time), _timescaledb_functions.compressed_data_info(device), _timescaledb_functions.compressed_data_info(value) FROM _timescaledb_internal.compress_hyper_2_6_chunk;
SELECT * FROM _timescaledb_functions.estimate_uncompressed_size('_timescaledb_internal.compress_hyper_2_6_chunk');

-- test varlen segmentby column keeps
CREATE TABLE t3 (time timestamptz, device text, value float) WITH (tsdb.hypertable, tsdb.segmentby = 'device');
INSERT INTO t3 SELECT '2025-01-01'::timestamptz + format('%s ms', i)::interval, format('Device %s', i%10), 1 + 1/i FROM generate_series(1, 100000) i;
CREATE INDEX t3_device_idx ON t3 (device);
VACUUM FULL t3;
SELECT compress_chunk(chunk) FROM show_chunks('t3') AS chunk;

SELECT l.relation_size > 0 AS relation_positive,
       l.index_size > 0 AS index_positive,
       l.total_size = l.relation_size + l.index_size AS total_consistent
FROM show_chunks('t3') AS chunk
JOIN _timescaledb_catalog.compression_settings cs ON cs.relid = chunk
JOIN LATERAL _timescaledb_functions.estimate_uncompressed_size(cs.compress_relid) l ON true;
