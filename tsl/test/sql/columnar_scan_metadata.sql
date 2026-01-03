-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\set PREFIX 'EXPLAIN (costs off)'

CREATE TABLE test_metadata_scan(
    time timestamptz NOT NULL,
    device text,
    sensor text,
    value float
) WITH (tsdb.hypertable,tsdb.orderby='time desc',tsdb.segmentby='device,sensor',tsdb.index='minmax(value)');

INSERT INTO test_metadata_scan VALUES
('2025-01-01 00:00:00 PST', 'd1', 'A', 10.0),
('2025-01-01 01:00:00 PST', 'd1', 'A', 20.0),
('2025-01-01 02:00:00 PST', 'd1', 'A', 15.0),
('2025-01-01 00:30:00 PST', 'd1', 'B', 5.0),
('2025-01-01 01:30:00 PST', 'd1', 'B', 25.0),
('2025-01-01 02:30:00 PST', 'd1', 'B', 30.0),
('2025-01-01 00:00:00 PST', 'd2', 'A', 10.0),
('2025-01-01 01:00:00 PST', 'd2', 'A', 20.0),
('2025-01-01 02:00:00 PST', 'd2', 'A', 15.0),
('2025-01-01 00:30:00 PST', 'd2', 'C', 5.0),
('2025-01-01 01:30:00 PST', 'd2', 'C', 25.0),
('2025-01-01 02:30:00 PST', 'd2', 'C', 30.0);

-- Compress all chunks
SELECT compress_chunk(c) FROM show_chunks('test_metadata_scan') c;

SET max_parallel_workers_per_gather = 0;
SET timescaledb.enable_columnarindexscan = on;

:PREFIX SELECT device, max(time) FROM test_metadata_scan GROUP BY device;
SELECT device, max(time) FROM test_metadata_scan GROUP BY device;

:PREFIX SELECT sensor, min(time) FROM test_metadata_scan GROUP BY sensor;
SELECT sensor, min(time) FROM test_metadata_scan GROUP BY sensor;

-- test multiple group by columns
:PREFIX SELECT device, sensor, max(time) FROM test_metadata_scan GROUP BY device,sensor;
SELECT device, sensor, max(time) FROM test_metadata_scan GROUP BY device,sensor;

-- order by does not prevent optimization
:PREFIX SELECT device, max(time) FROM test_metadata_scan GROUP BY device ORDER BY device;
SELECT device, max(time) FROM test_metadata_scan GROUP BY device ORDER BY device;

-- filter on segmentby allows optimization
:PREFIX SELECT device, min(time) FROM test_metadata_scan WHERE device = 'd1' GROUP BY device;
:PREFIX SELECT device, min(time) FROM test_metadata_scan WHERE device = 'd1' AND sensor='B' GROUP BY device;

-- filter on non-segmentby prevents optimization
:PREFIX SELECT device, max(time) FROM test_metadata_scan WHERE time <> '2025-01-01'  GROUP BY device;
SELECT device, max(time) FROM test_metadata_scan WHERE time <> '2025-01-01'  GROUP BY device;

-- tableoid doesnt prevent optimization
:PREFIX SELECT tableoid, device, max(time) FROM test_metadata_scan GROUP BY device, tableoid;

-- group by on non-segmentby prevents optimization
:PREFIX SELECT max(time) FROM test_metadata_scan GROUP BY value;
SELECT max(time) FROM test_metadata_scan GROUP BY value;
:PREFIX SELECT device, max(time) FROM test_metadata_scan GROUP BY device,value;
SELECT device, max(time) FROM test_metadata_scan GROUP BY device,value;

-- no group by prevents optimization
:PREFIX SELECT max(time) FROM test_metadata_scan;
SELECT max(time) FROM test_metadata_scan;

-- multiple min/max prevent optimization
:PREFIX SELECT device, min(time), max(time) FROM test_metadata_scan GROUP BY device;

-- aggregate on segmentby does not use metadata optimization
:PREFIX SELECT device, min(sensor) FROM test_metadata_scan GROUP BY device;

