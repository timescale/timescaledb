-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

SET work_mem TO '50MB';

-- for ordered append tests on compressed chunks we need a hypertable with time as compress_orderby column
CREATE TABLE metrics_ordered(time timestamptz NOT NULL, device_id int, device_id_peer int, v0 int, v1 int, v2 float, v3 float);
SELECT create_hypertable('metrics_ordered','time');
ALTER TABLE metrics_ordered SET (timescaledb.compress, timescaledb.compress_orderby='time DESC',timescaledb.compress_segmentby='device_id,device_id_peer');

INSERT INTO metrics_ordered SELECT * FROM metrics;
CREATE INDEX ON metrics_ordered(device_id,device_id_peer,time);
CREATE INDEX ON metrics_ordered(device_id,time);
CREATE INDEX ON metrics_ordered(device_id_peer,time);

SELECT compress_chunk('_timescaledb_internal._hyper_5_20_chunk');
SELECT compress_chunk('_timescaledb_internal._hyper_5_21_chunk');
SELECT compress_chunk('_timescaledb_internal._hyper_5_22_chunk');

-- should not have ordered DecompressChunk path because segmentby columns are not part of pathkeys
:PREFIX SELECT * FROM metrics_ordered ORDER BY time DESC LIMIT 10;

-- should have ordered DecompressChunk path because segmentby columns have equality constraints
:PREFIX SELECT * FROM metrics_ordered WHERE device_id = 1 AND device_id_peer = 3 ORDER BY time DESC LIMIT 10;

:PREFIX SELECT DISTINCT ON (d.device_id) * FROM metrics_ordered d INNER JOIN LATERAL (SELECT * FROM metrics_ordered m WHERE m.device_id=d.device_id AND m.device_id_peer = 3 ORDER BY time DESC LIMIT 1 ) m ON true;
