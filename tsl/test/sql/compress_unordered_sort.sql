-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

SET timezone TO 'UTC';

CREATE TABLE metrics(
    time timestamptz NOT NULL,
    device text NOT NULL,
    sensor text NOT NULL,
    value float
) WITH (tsdb.hypertable,tsdb.orderby='time desc',tsdb.segmentby='device,sensor');

-- create unordered chunks
SET timescaledb.enable_direct_compress_insert = true;
INSERT INTO metrics VALUES
('2025-01-01 00:00:00', 'd1', 'A', 10.0),
('2025-01-01 01:00:00', 'd1', 'A', 20.0),
('2025-01-01 02:00:00', 'd1', 'A', 15.0),
('2025-01-01 00:30:00', 'd1', 'B', 5.0),
('2025-01-01 01:30:00', 'd1', 'B', 25.0),
('2025-01-01 02:30:00', 'd1', 'B', 30.0),
('2025-01-01 00:00:00', 'd2', 'A', 10.0),
('2025-01-01 01:00:00', 'd2', 'A', 20.0),
('2025-01-01 02:00:00', 'd2', 'A', 15.0),
('2025-01-01 00:30:00', 'd2', 'C', 5.0),
('2025-01-01 01:30:00', 'd2', 'C', 25.0),
('2025-01-01 02:30:00', 'd2', 'C', 30.0);

INSERT INTO metrics VALUES
('2025-01-01 03:00:00', 'd1', 'A', 11.0),
('2025-01-01 01:00:00', 'd1', 'A', 21.0),
('2025-01-01 02:00:00', 'd1', 'A', 15.1),
('2025-01-01 03:30:00', 'd1', 'B', 5.1),
('2025-01-01 01:30:00', 'd1', 'B', 25.1),
('2025-01-01 02:30:00', 'd1', 'B', 31.0),
('2025-01-01 03:00:00', 'd2', 'A', 11.0),
('2025-01-01 01:00:00', 'd2', 'A', 21.0),
('2025-01-01 02:00:00', 'd2', 'A', 15.2),
('2025-01-01 03:30:00', 'd2', 'C', 5.3),
('2025-01-01 01:30:00', 'd2', 'C', 25.3),
('2025-01-01 02:30:00', 'd2', 'C', 32.0);

SELECT _timescaledb_functions.chunk_status_text(ch.status)
FROM _timescaledb_catalog.hypertable h, _timescaledb_catalog.chunk ch
WHERE h.id = ch.hypertable_id AND h.table_name = 'metrics';

\set PREFIX 'EXPLAIN (buffers off, costs off, timing off, summary off)'

SET max_parallel_workers_per_gather = 0;
SET enable_bitmapscan=0;
SET enable_seqscan=0;

-- Can use SkipScan on unordered chunks if only segmentby columns are distinct
:PREFIX select distinct device, sensor from metrics order by 1,2;
select distinct device, sensor from metrics order by 1,2;

:PREFIX select distinct on(device) device from metrics order by device;
select distinct on(device) device from metrics order by device;

:PREFIX select distinct on(device,sensor) device, sensor from metrics order by 1,2;
select distinct on(device,sensor) device, sensor from metrics order by 1,2;

:PREFIX select distinct on(device) device from metrics where sensor='A' order by device;
select distinct on(device) device from metrics where sensor='A' order by device;

:PREFIX select distinct on(device, sensor) device, sensor from metrics where device = 'd2' order by device, sensor;
select distinct on(device, sensor) device, sensor from metrics where device = 'd2' order by device, sensor;

:PREFIX select distinct on(device, sensor) device, sensor from metrics where value > 6 order by 1,2;
select distinct on(device, sensor) device, sensor from metrics where value > 6 order by 1,2;

:PREFIX select distinct device, sensor from metrics where time > '2025-01-01 01:00:00' order by 1,2;
select distinct device, sensor from metrics where time > '2025-01-01 01:00:00' order by 1,2;

-- Can use compressed sort on unordered chunks when we need aggregation results ordered by segmentby columns
:PREFIX select device, sensor from metrics group by device, sensor order by 1,2;
select device, sensor from metrics group by device, sensor order by 1,2;

:PREFIX select device, sensor, count(*), max(sensor) from metrics group by device, sensor order by 1,2;
select device, sensor, count(*), max(sensor) from metrics group by device, sensor order by 1,2;

:PREFIX select device, sensor, count(*) from metrics where value > length(sensor) group by device, sensor order by 1,2;
select device, sensor, count(*) from metrics where value > length(sensor) group by device, sensor order by 1,2;

:PREFIX select device, sensor, avg(value) from metrics where time > '2025-01-01 01:00:00' group by device, sensor order by 1,2;
select device, sensor, avg(value) from metrics where time > '2025-01-01 01:00:00' group by device, sensor order by 1,2;

-- Can use expressions on aggregates as it won't affect grouping/sorting
:PREFIX select device, sensor, max(sensor||'1'), min(time), max(time) + interval '1 day' from metrics group by device, sensor order by 1,2;
select device, sensor, max(sensor||'1'), min(time), max(time) + interval '1 day' from metrics group by device, sensor order by 1,2;

:PREFIX select device, sensor, avg(value+1), max(time) + make_interval(days => length(sensor)) from metrics group by device, sensor order by 1,2;
select device, sensor, avg(value+1), max(time) + make_interval(days => length(sensor)) from metrics group by device, sensor order by 1,2;

-- Can use Having as it filters on aggregate outputs without changing sort
:PREFIX select device, sensor, count(*) from metrics group by device, sensor having count(*) > length(sensor) order by 1,2;
select device, sensor, count(*) from metrics group by device, sensor having count(*) > length(sensor) order by 1,2;

-- Cannot use compressed sort on non-var keys
:PREFIX select device, sensor||'1', min(time), max(time) from metrics group by device, sensor||'1' order by 1,2;
select device, sensor||'1', min(time), max(time) from metrics group by device, sensor||'1' order by 1,2;

-- Columnar Index Scan produces unsorted output currently,
-- so Columnar Index Scan eligible queries will not benefit from unordered sort, yet
:PREFIX select device, sensor, min(time) from metrics group by device, sensor order by 1,2;
select device, sensor, min(time) from metrics group by device, sensor order by 1,2;

:PREFIX select device, sensor, first(time,time) from metrics group by device, sensor order by 1,2;
select device, sensor, first(time,time) from metrics group by device, sensor order by 1,2;

-- Cannot use compressed sort on unordered chunk when sort keys contain non-segmentby keys
:PREFIX select device, sensor, time from metrics group by device, sensor, time order by 1,2, time DESC;
select device, sensor, time from metrics group by device, sensor, time order by 1,2, time DESC;

:PREFIX select distinct on (device) device, time from metrics order by device, time DESC;
select distinct on (device) device, time from metrics order by device, time DESC;

:PREFIX select time, avg(value) from metrics where device = 'd1' and sensor='A' group by time order by time DESC;
select time, avg(value) from metrics  where device = 'd1' and sensor='A' group by time order by time DESC;

-- Cannot use compressed sort on unordered chunk when columnstore doesn't have segmentby keys
CREATE TABLE metrics2(
    time timestamptz NOT NULL,
    device text NOT NULL,
    sensor text NOT NULL,
    value float
) WITH (tsdb.hypertable,tsdb.orderby='time desc');

-- create unordered chunks
INSERT INTO metrics2 VALUES
('2025-01-01 00:00:00', 'd1', 'A', 10.0),
('2025-01-01 01:00:00', 'd1', 'A', 20.0),
('2025-01-01 02:00:00', 'd1', 'A', 15.0),
('2025-01-01 00:30:00', 'd1', 'B', 5.0),
('2025-01-01 01:30:00', 'd1', 'B', 25.0),
('2025-01-01 02:30:00', 'd1', 'B', 30.0),
('2025-01-01 00:00:00', 'd2', 'A', 10.0),
('2025-01-01 01:00:00', 'd2', 'A', 20.0),
('2025-01-01 02:00:00', 'd2', 'A', 15.0),
('2025-01-01 00:30:00', 'd2', 'C', 5.0),
('2025-01-01 01:30:00', 'd2', 'C', 25.0),
('2025-01-01 02:30:00', 'd2', 'C', 30.0);

SELECT _timescaledb_functions.chunk_status_text(ch.status)
FROM _timescaledb_catalog.hypertable h, _timescaledb_catalog.chunk ch
WHERE h.id = ch.hypertable_id AND h.table_name = 'metrics2';

-- Cannot use compressed sort on unordered chunks for this columnstore as it's not segmented
:PREFIX select time, avg(value) from metrics2 where time > '2025-01-01 01:00:00' group by time order by time DESC;
select time, avg(value) from metrics2  where time > '2025-01-01 01:00:00' group by time order by time DESC;

drop table metrics cascade;
drop table metrics2 cascade;

RESET timescaledb.enable_direct_compress_insert;

RESET max_parallel_workers_per_gather;
RESET enable_bitmapscan;
RESET enable_seqscan;
