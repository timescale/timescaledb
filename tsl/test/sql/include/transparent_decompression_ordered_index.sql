-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

SET work_mem TO '50MB';

---Lets test for index backward scans instead of seq scans ------------
-- for ordered append tests on compressed chunks we need a hypertable with time as compress_orderby column

-- should not have ordered DecompressChunk path because segmentby columns are not part of pathkeys
:PREFIX select * from ( SELECT * FROM metrics_ordered_idx ORDER BY time DESC LIMIT 10 ) as q  order by 1,2,3,4;

-- should have ordered DecompressChunk path because segmentby columns have equality constraints
:PREFIX select * from (SELECT * FROM metrics_ordered_idx WHERE device_id = 3 AND device_id_peer = 3 ORDER BY time DESC LIMIT 10) as q order by 1, 2, 3, 4;

:PREFIX SELECT DISTINCT ON (d.device_id) * FROM metrics_ordered_idx d INNER JOIN LATERAL (SELECT * FROM metrics_ordered_idx m WHERE m.device_id=d.device_id AND m.device_id_peer = 3 ORDER BY time DESC LIMIT 1 ) m ON m.device_id_peer = d.device_id_peer;
:PREFIX SELECT d.device_id, m.time,  m.time
FROM metrics_ordered_idx d INNER JOIN LATERAL (SELECT * FROM metrics_ordered_idx m WHERE m.device_id=d.device_id AND m.device_id_peer = 3 ORDER BY time DESC LIMIT 1 ) m ON m.device_id_peer = d.device_id_peer;

