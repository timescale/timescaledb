-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- tests for explain plan only --
---check index backward scans instead of seq scans ------------

CREATE TABLE metrics_ordered_idx2(time timestamptz NOT NULL, device_id int, device_id_peer int, v0 int, v1 int);
SELECT create_hypertable('metrics_ordered_idx2','time', chunk_time_interval=>'2days'::interval);
ALTER TABLE metrics_ordered_idx2 SET (timescaledb.compress, timescaledb.compress_orderby='time ASC, v0 desc',timescaledb.compress_segmentby='device_id,device_id_peer');
INSERT INTO metrics_ordered_idx2(time,device_id,device_id_peer,v0, v1) SELECT generate_series('2000-01-20 0:00:00+0'::timestamptz,'2000-01-20 11:55:00+0','10s') , 3, 3, generate_series(1,5,1) , generate_series(555,559,1);

SELECT
  compress_chunk(c.schema_name || '.' || c.table_name)
FROM _timescaledb_catalog.chunk c
  INNER JOIN _timescaledb_catalog.hypertable ht ON c.hypertable_id=ht.id
WHERE ht.table_name = 'metrics_ordered_idx2'
ORDER BY c.id;

--all queries have only prefix of compress_orderby in ORDER BY clause

-- should have ordered DecompressChunk path because segmentby columns have equality constraints
:PREFIX SELECT * FROM metrics_ordered_idx2 WHERE device_id = 3 AND device_id_peer = 3 ORDER BY time DESC LIMIT 10;
:PREFIX SELECT * FROM metrics_ordered_idx2 WHERE device_id = 3 AND device_id_peer = 3 ORDER BY time DESC , v0 asc LIMIT 10;
:PREFIX SELECT * FROM metrics_ordered_idx2 WHERE device_id = 3 AND device_id_peer = 3 ORDER BY time DESC , v0 desc LIMIT 10;

:PREFIX SELECT d.device_id, m.time,  m.time
 FROM metrics_ordered_idx2 d INNER JOIN LATERAL (SELECT * FROM metrics_ordered_idx2 m WHERE m.device_id=d.device_id AND m.device_id_peer = 3 ORDER BY time DESC LIMIT 1 ) m ON m.device_id_peer = d.device_id_peer;
