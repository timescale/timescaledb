
SET client_min_messages TO ERROR;

-- create normal hypertable with dropped columns, each chunk will have different attribute numbers
CREATE TABLE metrics(filler_1 int, filler_2 int, filler_3 int, time timestamptz NOT NULL, device_id int, v0 int, v1 int, v2 float, v3 float);
CREATE INDEX ON metrics(time DESC);
CREATE INDEX ON metrics(device_id,time DESC);
SELECT create_hypertable('metrics','time',create_default_indexes:=false);

ALTER TABLE metrics DROP COLUMN filler_1;
INSERT INTO metrics(time,device_id,v0,v1,v2,v3) SELECT time, device_id, device_id+1,  device_id + 2, device_id + 0.5, NULL FROM generate_series('2000-01-01 0:00:00+0'::timestamptz,'2000-01-05 23:55:00+0','2m') gtime(time), generate_series(1,5,1) gdevice(device_id);
ALTER TABLE metrics DROP COLUMN filler_2;
INSERT INTO metrics(time,device_id,v0,v1,v2,v3) SELECT time, device_id, device_id-1, device_id + 2, device_id + 0.5, NULL FROM generate_series('2000-01-06 0:00:00+0'::timestamptz,'2000-01-12 23:55:00+0','2m') gtime(time), generate_series(1,5,1) gdevice(device_id);
ALTER TABLE metrics DROP COLUMN filler_3;
INSERT INTO metrics(time,device_id,v0,v1,v2,v3) SELECT time, device_id, device_id, device_id + 2, device_id + 0.5, NULL FROM generate_series('2000-01-13 0:00:00+0'::timestamptz,'2000-01-19 23:55:00+0','2m') gtime(time), generate_series(1,5,1) gdevice(device_id);
ANALYZE metrics;

-- create identical hypertable with space partitioning
CREATE TABLE metrics_space(filler_1 int, filler_2 int, filler_3 int, time timestamptz NOT NULL, device_id int, v0 int, v1 float, v2 float, v3 float);
CREATE INDEX ON metrics_space(time);
CREATE INDEX ON metrics_space(device_id,time);
SELECT create_hypertable('metrics_space','time','device_id',3,create_default_indexes:=false);

ALTER TABLE metrics_space DROP COLUMN filler_1;
INSERT INTO metrics_space(time,device_id,v0,v1,v2,v3) SELECT time, device_id, device_id+1, device_id + 2, device_id + 0.5, NULL FROM generate_series('2000-01-01 0:00:00+0'::timestamptz,'2000-01-05 23:55:00+0','2m') gtime(time), generate_series(1,5,1) gdevice(device_id);
ALTER TABLE metrics_space DROP COLUMN filler_2;
INSERT INTO metrics_space(time,device_id,v0,v1,v2,v3) SELECT time, device_id, device_id+1, device_id + 2, device_id + 0.5, NULL FROM generate_series('2000-01-06 0:00:00+0'::timestamptz,'2000-01-12 23:55:00+0','2m') gtime(time), generate_series(1,5,1) gdevice(device_id);
ALTER TABLE metrics_space DROP COLUMN filler_3;
INSERT INTO metrics_space(time,device_id,v0,v1,v2,v3) SELECT time, device_id, device_id+1, device_id + 2, device_id + 0.5, NULL FROM generate_series('2000-01-13 0:00:00+0'::timestamptz,'2000-01-19 23:55:00+0','2m') gtime(time), generate_series(1,5,1) gdevice(device_id);
ANALYZE metrics_space;


-- create hypertable with compression
CREATE TABLE metrics_compressed(filler_1 int, filler_2 int, filler_3 int, time timestamptz NOT NULL, device_id int, v0 int, v1 int, v2 float, v3 float);
CREATE INDEX ON metrics_compressed(time);
CREATE INDEX ON metrics_compressed(device_id,time);
SELECT create_hypertable('metrics_compressed','time',create_default_indexes:=false);

ALTER TABLE metrics_compressed DROP COLUMN filler_1;
INSERT INTO metrics_compressed(time,device_id,v0,v1,v2,v3) SELECT time, device_id, device_id+1,  device_id + 2, device_id + 0.5, NULL FROM generate_series('2000-01-01 0:00:00+0'::timestamptz,'2000-01-05 23:55:00+0','2m') gtime(time), generate_series(1,5,1) gdevice(device_id);
ALTER TABLE metrics_compressed DROP COLUMN filler_2;
INSERT INTO metrics_compressed(time,device_id,v0,v1,v2,v3) SELECT time, device_id, device_id-1, device_id + 2, device_id + 0.5, NULL FROM generate_series('2000-01-06 0:00:00+0'::timestamptz,'2000-01-12 23:55:00+0','2m') gtime(time), generate_series(1,5,1) gdevice(device_id);
ALTER TABLE metrics_compressed DROP COLUMN filler_3;
INSERT INTO metrics_compressed(time,device_id,v0,v1,v2,v3) SELECT time, device_id, device_id, device_id + 2, device_id + 0.5, NULL FROM generate_series('2000-01-13 0:00:00+0'::timestamptz,'2000-01-19 23:55:00+0','2m') gtime(time), generate_series(1,5,1) gdevice(device_id);
ANALYZE metrics_compressed;

-- compress chunks
ALTER TABLE metrics_compressed SET (timescaledb.compress, timescaledb.compress_orderby='time DESC', timescaledb.compress_segmentby='device_id');
SELECT compress_chunk(c.schema_name|| '.' || c.table_name)
FROM _timescaledb_catalog.chunk c, _timescaledb_catalog.hypertable ht where c.hypertable_id = ht.id and ht.table_name = 'metrics_compressed' and c.compressed_chunk_id IS NULL
ORDER BY c.table_name DESC;

-- create hypertable with space partitioning and compression
CREATE TABLE metrics_space_compressed(filler_1 int, filler_2 int, filler_3 int, time timestamptz NOT NULL, device_id int, v0 int, v1 float, v2 float, v3 float);
CREATE INDEX ON metrics_space_compressed(time);
CREATE INDEX ON metrics_space_compressed(device_id,time);
SELECT create_hypertable('metrics_space_compressed','time','device_id',3,create_default_indexes:=false);

ALTER TABLE metrics_space_compressed DROP COLUMN filler_1;
INSERT INTO metrics_space_compressed(time,device_id,v0,v1,v2,v3) SELECT time, device_id, device_id+1, device_id + 2, device_id + 0.5, NULL FROM generate_series('2000-01-01 0:00:00+0'::timestamptz,'2000-01-05 23:55:00+0','2m') gtime(time), generate_series(1,5,1) gdevice(device_id);
ALTER TABLE metrics_space_compressed DROP COLUMN filler_2;
INSERT INTO metrics_space_compressed(time,device_id,v0,v1,v2,v3) SELECT time, device_id, device_id+1, device_id + 2, device_id + 0.5, NULL FROM generate_series('2000-01-06 0:00:00+0'::timestamptz,'2000-01-12 23:55:00+0','2m') gtime(time), generate_series(1,5,1) gdevice(device_id);
ALTER TABLE metrics_space_compressed DROP COLUMN filler_3;
INSERT INTO metrics_space_compressed(time,device_id,v0,v1,v2,v3) SELECT time, device_id, device_id+1, device_id + 2, device_id + 0.5, NULL FROM generate_series('2000-01-13 0:00:00+0'::timestamptz,'2000-01-19 23:55:00+0','2m') gtime(time), generate_series(1,5,1) gdevice(device_id);
ANALYZE metrics_space_compressed;

-- compress chunks
ALTER TABLE metrics_space_compressed SET (timescaledb.compress, timescaledb.compress_orderby='time DESC', timescaledb.compress_segmentby='device_id');
SELECT compress_chunk(c.schema_name|| '.' || c.table_name)
FROM _timescaledb_catalog.chunk c, _timescaledb_catalog.hypertable ht where c.hypertable_id = ht.id and ht.table_name = 'metrics_space_compressed' and c.compressed_chunk_id IS NULL
ORDER BY c.table_name DESC;
