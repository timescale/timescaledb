-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

CREATE TABLE metrics_compressed( time timestamptz NOT NULL, device_id int, v0 int, v1 int, v2 float, v3 float);
CREATE INDEX ON metrics_compressed(time);
CREATE INDEX ON metrics_compressed(device_id,time);
SELECT create_hypertable('metrics_compressed','time',create_default_indexes:=false);

INSERT INTO metrics_compressed(time,device_id,v0,v1,v2,v3) SELECT time, device_id, device_id+1,  device_id + 2, device_id + 0.5, NULL FROM generate_series('2000-01-01 0:00:00+0'::timestamptz,'2000-01-05 23:55:00+0','2m') gtime(time), generate_series(1,5,1) gdevice(device_id);
INSERT INTO metrics_compressed(time,device_id,v0,v1,v2,v3) SELECT time, device_id, device_id-1, device_id + 2, device_id + 0.5, NULL FROM generate_series('2000-01-06 0:00:00+0'::timestamptz,'2000-01-12 23:55:00+0','2m') gtime(time), generate_series(1,5,1) gdevice(device_id);
INSERT INTO metrics_compressed(time,device_id,v0,v1,v2,v3) SELECT time, device_id, device_id, device_id + 2, device_id + 0.5, NULL FROM generate_series('2000-01-13 0:00:00+0'::timestamptz,'2000-01-19 23:55:00+0','2m') gtime(time), generate_series(1,5,1) gdevice(device_id);
ANALYZE metrics_compressed;

-- compress chunks
ALTER TABLE metrics_compressed SET (timescaledb.compress, timescaledb.compress_orderby='time DESC', timescaledb.compress_segmentby='device_id');
SELECT compress_chunk(c.schema_name|| '.' || c.table_name)
FROM _timescaledb_catalog.chunk c, _timescaledb_catalog.hypertable ht WHERE c.hypertable_id = ht.id and ht.table_name = 'metrics_compressed' and c.compressed_chunk_id IS NULL
ORDER BY c.table_name DESC;

-- case1: no: of new segment added is > 1000, which calls resize_affectedsegmentscxt()
-- Number of segments decompressed is 10
BEGIN;
-- affects segment with sequence_num = 20,30
INSERT INTO metrics_compressed(time,device_id,v0,v1,v2,v3) SELECT time, device_id, V+1.5,  V + 2.5, V + 0.5, NULL FROM generate_series('2000-01-02 01:26:00+05:30'::timestamptz,'2000-01-04 20:06:00+05:30','22m') gtime(time), generate_series(1,1020,1) gdevice(device_id), generate_series(1,1,1) gv(V);
-- check compression status
SELECT * FROM _timescaledb_catalog.chunk WHERE table_name = '_hyper_1_1_chunk' ORDER BY id;
SELECT count(distinct device_id) FROM _timescaledb_internal.compress_hyper_2_6_chunk;
CALL recompress_chunk('_timescaledb_internal._hyper_1_1_chunk');
SELECT count(distinct device_id) FROM _timescaledb_internal.compress_hyper_2_6_chunk;
-- check compression status
SELECT * FROM _timescaledb_catalog.chunk WHERE table_name = '_hyper_1_1_chunk' ORDER BY id;
ROLLBACK;

-- case2: no: of affected existing segment is > 1000, which calls resize_affectedsegmentscxt()
INSERT INTO metrics_compressed(time,device_id,v0,v1,v2,v3) SELECT time, device_id, V+1.5,  V + 2.5, V + 0.5, NULL FROM generate_series('2000-01-02 01:26:00+05:30'::timestamptz,'2000-01-04 20:06:00+05:30','22m') gtime(time), generate_series(1,1020,1) gdevice(device_id), generate_series(1,1,1) gv(V);
CALL recompress_chunk('_timescaledb_internal._hyper_1_1_chunk');
BEGIN;
-- affects segment with sequence_num = 20,30
INSERT INTO metrics_compressed(time,device_id,v0,v1,v2,v3) SELECT time, device_id, V+1.5,  V + 2.5, V + 0.5, NULL FROM generate_series('2000-01-02 01:26:00+05:30'::timestamptz,'2000-01-04 20:06:00+05:30','22m') gtime(time), generate_series(1,1020,1) gdevice(device_id), generate_series(1,1,1) gv(V);
-- check compression status
SELECT * FROM _timescaledb_catalog.chunk WHERE table_name = '_hyper_1_1_chunk' ORDER BY id;
SELECT count(distinct device_id) FROM _timescaledb_internal.compress_hyper_2_6_chunk;
CALL recompress_chunk('_timescaledb_internal._hyper_1_1_chunk');
SELECT count(distinct device_id) FROM _timescaledb_internal.compress_hyper_2_6_chunk;
-- check compression status
SELECT * FROM _timescaledb_catalog.chunk WHERE table_name = '_hyper_1_1_chunk' ORDER BY id;
ROLLBACK;

-- case3: gaps between sequence no: are filled up, thus call legacy recompression
BEGIN;
-- affects segment with sequence_num = 20,30
INSERT INTO metrics_compressed(time,device_id,v0,v1,v2,v3) SELECT time, device_id, V+1.5,  V + 2.5, V + 0.5, NULL FROM generate_series('2000-01-02 01:26:00+05:30'::timestamptz,'2000-01-04 20:06:00+05:30','22m') gtime(time), generate_series(3,3,1) gdevice(device_id), generate_series(1,100,1) gv(V);
INSERT INTO metrics_compressed(time,device_id,v0,v1,v2,v3) SELECT time, device_id, V+1.5,  V + 2.5, V + 0.5, NULL FROM generate_series('2000-01-02 01:26:00+05:30'::timestamptz,'2000-01-04 20:06:00+05:30','22m') gtime(time), generate_series(3,3,1) gdevice(device_id), generate_series(1,100,1) gv(V);
-- check compression status
SELECT * FROM _timescaledb_catalog.chunk WHERE table_name = '_hyper_1_1_chunk' ORDER BY id;
SELECT device_id, _ts_meta_sequence_num, _ts_meta_count FROM _timescaledb_internal.compress_hyper_2_6_chunk WHERE device_id = 3 ORDER BY 1, 2;
set client_min_messages TO LOG;
CALL recompress_chunk('_timescaledb_internal._hyper_1_1_chunk');
SELECT device_id, _ts_meta_sequence_num, _ts_meta_count FROM _timescaledb_internal.compress_hyper_2_6_chunk WHERE device_id = 3 ORDER BY 1, 2;
COMMIT;
BEGIN;
INSERT INTO metrics_compressed(time,device_id,v0,v1,v2,v3) SELECT time, device_id, V+1.5,  V + 2.5, V + 0.5, NULL FROM generate_series('2000-01-02 01:26:00+05:30'::timestamptz,'2000-01-04 20:06:00+05:30','22m') gtime(time), generate_series(3,3,1) gdevice(device_id), generate_series(1,100,1) gv(V);
-- should invoke legacy recompression
CALL recompress_chunk('_timescaledb_internal._hyper_1_1_chunk');
SELECT device_id, _ts_meta_sequence_num, _ts_meta_count FROM _timescaledb_internal.compress_hyper_2_6_chunk WHERE device_id = 3 ORDER BY 1, 2;
COMMIT;
BEGIN;
INSERT INTO metrics_compressed(time,device_id,v0,v1,v2,v3) SELECT time, device_id, V+1.5,  V + 2.5, V + 0.5, NULL FROM generate_series('2000-01-02 01:26:00+05:30'::timestamptz,'2000-01-04 20:06:00+05:30','22m') gtime(time), generate_series(3,3,1) gdevice(device_id), generate_series(1,100,1) gv(V);
-- should invoke optimized recompression as there are enough gaps in sequence numbers in compressed chunk
CALL recompress_chunk('_timescaledb_internal._hyper_1_1_chunk');
SELECT device_id, _ts_meta_sequence_num, _ts_meta_count FROM _timescaledb_internal.compress_hyper_2_6_chunk WHERE device_id = 3 ORDER BY 1, 2;
COMMIT;
-- check compression status
SELECT * FROM _timescaledb_catalog.chunk WHERE table_name = '_hyper_1_1_chunk' ORDER BY id;
RESET client_min_messages;

-- check for heapscan
DROP INDEX _timescaledb_internal.compress_hyper_2_6_chunk__compressed_hypertable_2_device_id__ts;
-- INSERT new segments with device_id = 4
INSERT INTO metrics_compressed VALUES ('1999-12-31 05:30:00+05:30'::timestamp with time zone, 4, 2, 3, 2.2, 3.3);
INSERT INTO metrics_compressed VALUES ('1999-12-30 15:30:00+05:30'::timestamp with time zone, 4, 2, 3, 22.2, 33.3);
-- does not affects any segments, append to existing segments
INSERT INTO metrics_compressed VALUES ('1999-12-31 05:30:00+05:30'::timestamp with time zone, 1, 2, 3, 2.2, 3.3);
INSERT INTO metrics_compressed VALUES ('1999-12-30 15:30:00+05:30'::timestamp with time zone, 1, 2, 3, 22.2, 33.3);
INSERT INTO metrics_compressed VALUES ('1999-12-31 05:30:00+05:30'::timestamp with time zone, 2, 2, 3, 222.2, 333.3);
INSERT INTO metrics_compressed VALUES ('1999-12-31 05:30:00+05:30'::timestamp with time zone, 3, 2, 3, 2222.2, 3333.3);
INSERT INTO metrics_compressed VALUES ('1999-12-31 05:30:00+05:30'::timestamp with time zone, 4, 2, 3, 22222.2, 33333.3);
INSERT INTO metrics_compressed VALUES ('1999-12-30 15:30:00+05:30'::timestamp with time zone, 4, 2, 3, 22222.2, 33333.3);
-- affects segment with sequence_num = 30
INSERT INTO metrics_compressed(time,device_id,v0,v1,v2,v3) SELECT time, device_id, device_id+1,  device_id + 2, device_id + 0.5, NULL FROM generate_series('2000-01-02 01:26:00+05:30'::timestamptz,'2000-01-03 10:44:00+05:30','12s') gtime(time), generate_series(4,4,1) gdevice(device_id);
-- create new segment
INSERT INTO metrics_compressed VALUES ('2000-01-03 15:46:00+05:30'::timestamp with time zone, 11, 2, 3, 22.2, 33.3);
INSERT INTO metrics_compressed VALUES ('2000-01-03 15:56:00+05:30'::timestamp with time zone, 11, 2, 3, 222.2, 333.3);
INSERT INTO metrics_compressed VALUES ('2000-01-03 15:46:00+05:30'::timestamp with time zone, 11, 2, 3, 22.2, 33.3);
INSERT INTO metrics_compressed VALUES ('2000-01-01 15:30:00+05:30'::timestamp with time zone, 13, 2, 3, 22.2, 33.3);
INSERT INTO metrics_compressed VALUES ('2000-01-01 15:30:00+05:30'::timestamp with time zone, 14, 2, 3, 22.2, 33.3);
INSERT INTO metrics_compressed VALUES ('2000-01-01 15:30:00+05:30'::timestamp with time zone, 15, 2, 3, 22.2, 33.3);
INSERT INTO metrics_compressed VALUES ('2000-01-01 15:30:00+05:30'::timestamp with time zone, 12, 2, 3, 22.2, 33.3);
-- check compression status
SELECT * FROM _timescaledb_catalog.chunk WHERE table_name = '_hyper_1_1_chunk' ORDER BY id;
SELECT device_id, _ts_meta_sequence_num, _ts_meta_count, _ts_meta_min_1, _ts_meta_max_1 FROM _timescaledb_internal.compress_hyper_2_6_chunk WHERE device_id = 4 ORDER BY 1, 2;
set client_min_messages TO LOG;
CALL recompress_chunk('_timescaledb_internal._hyper_1_1_chunk');
RESET client_min_messages;
SELECT device_id, _ts_meta_sequence_num, _ts_meta_count, _ts_meta_min_1, _ts_meta_max_1 FROM _timescaledb_internal.compress_hyper_2_7_chunk WHERE device_id = 4 ORDER BY 1, 2;
-- check compression status
SELECT * FROM _timescaledb_catalog.chunk WHERE table_name = '_hyper_1_1_chunk' ORDER BY id;

DROP TABLE metrics_compressed;
