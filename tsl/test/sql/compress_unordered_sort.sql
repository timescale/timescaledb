-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

CREATE TABLE metrics(
    time timestamptz NOT NULL,
    device text NOT NULL,
    sensor text NOT NULL,
    value float,
    value2 float
) WITH (tsdb.hypertable,tsdb.orderby='time desc',tsdb.segmentby='device,sensor');

-- create unordered chunks
SET timescaledb.enable_direct_compress_insert = true;
INSERT INTO metrics VALUES
('2025-01-01 00:00:00 PST', 'd1', 'A', 10.0, 10.0),
('2025-01-01 01:00:00 PST', 'd1', 'A', 20.0, 20.0),
('2025-01-01 02:00:00 PST', 'd1', 'A', 15.0, 15.0),
('2025-01-01 00:30:00 PST', 'd1', 'B', 5.0, 5.0),
('2025-01-01 01:30:00 PST', 'd1', 'B', 25.0, 25.0),
('2025-01-01 02:30:00 PST', 'd1', 'B', 30.0, 30.0),
('2025-01-01 00:00:00 PST', 'd2', 'A', 10.0, 10.0),
('2025-01-01 01:00:00 PST', 'd2', 'A', 20.0, 20.0),
('2025-01-01 02:00:00 PST', 'd2', 'A', 15.0, 15.0),
('2025-01-01 00:30:00 PST', 'd2', 'C', 5.0, 5.0),
('2025-01-01 01:30:00 PST', 'd2', 'C', 25.0, 25.0),
('2025-01-01 02:30:00 PST', 'd2', 'C', 30.0, 30.0);

INSERT INTO metrics VALUES
('2025-01-01 03:00:00 PST', 'd1', 'A', 11.0, 11.0),
('2025-01-01 01:00:00 PST', 'd1', 'A', 21.0, 21.0),
('2025-01-01 02:00:00 PST', 'd1', 'A', 15.1, 15.1),
('2025-01-01 03:30:00 PST', 'd1', 'B', 5.1, 5.1),
('2025-01-01 01:30:00 PST', 'd1', 'B', 25.1, 25.0),
('2025-01-01 02:30:00 PST', 'd1', 'B', 31.0, 31.0),
('2025-01-01 03:00:00 PST', 'd2', 'A', 11.0, 11.0),
('2025-01-01 01:00:00 PST', 'd2', 'A', 21.0, 21.0),
('2025-01-01 02:00:00 PST', 'd2', 'A', 15.2, 15.2),
('2025-01-01 03:30:00 PST', 'd2', 'C', 5.3, 5.3),
('2025-01-01 01:30:00 PST', 'd2', 'C', 25.3, 25.3),
('2025-01-01 02:30:00 PST', 'd2', 'C', 32.0, 32.0);

SELECT _timescaledb_functions.chunk_status_text(ch.status)
FROM _timescaledb_catalog.hypertable h, _timescaledb_catalog.chunk ch
WHERE h.id = ch.hypertable_id AND h.table_name = 'metrics';

\set PREFIX 'EXPLAIN (buffers off, costs off, timing off, summary off)'

SET timescaledb.enable_compressed_unordered_sort = 1;

SET max_parallel_workers_per_gather = 0;
SET enable_bitmapscan=0;
SET enable_seqscan=0;

SET timezone TO PST8PDT;

-- Can use SkipScan on unordered chunks if only segmentby columns are involved
:PREFIX select distinct device, sensor from metrics order by 1,2;
select distinct device, sensor from metrics order by 1,2;

:PREFIX select distinct on(device) device from metrics order by device;
select distinct on(device) device from metrics order by device;

:PREFIX select distinct on(device,sensor) device, sensor from metrics order by 1,2;
select distinct on(device,sensor) device, sensor from metrics order by 1,2;

:PREFIX select distinct on(device, sensor) device, sensor from metrics where sensor='A' order by 1,2;
select distinct on(device, sensor) device, sensor from metrics where sensor='A' order by 1,2;

:PREFIX select distinct on(device) device from metrics where sensor='A' order by device;
select distinct on(device) device from metrics where sensor='A' order by device;

:PREFIX select distinct on(device, sensor) device, sensor from metrics where sensor>'A' order by 1,2;
select distinct on(device, sensor) device, sensor from metrics where sensor>'A' order by 1,2;

:PREFIX select distinct on(device, sensor) device, sensor from metrics where device = 'd2' order by device, sensor;
select distinct on(device, sensor) device, sensor from metrics where device = 'd2' order by device, sensor;

-- If nonsegmentby columns are involved we cannot use compressed index as is and therefore cannot use SkipScan
:PREFIX select distinct device, sensor from metrics where value > 5 order by 1,2;
select distinct device, sensor from metrics where value > 5 order by 1,2;

:PREFIX select distinct device, sensor from metrics where time > '2025-01-01 01:00:00 PST' order by 1,2;
select distinct device, sensor from metrics where time > '2025-01-01 01:00:00 PST' order by 1,2;

-- Can use compressed index when we need ordered aggregation results
:PREFIX select device, sensor from metrics group by device, sensor order by 1,2;
select device, sensor from metrics group by device, sensor order by 1,2;

:PREFIX select device, sensor, count(*), max(sensor) from metrics group by device, sensor order by 1,2;
select device, sensor, count(*), max(sensor) from metrics group by device, sensor order by 1,2;

:PREFIX select device, sensor, first(time,time), last(time,time) from metrics group by device, sensor order by 1,2;
select device, sensor, first(time,time), last(time,time) from metrics group by device, sensor order by 1,2;

-- Can use expressions on aggregates as it won't affect grouping/sorting
:PREFIX select device, sensor, max(sensor||'1'), min(time), max(time) + interval '1 day' from metrics group by device, sensor order by 1,2;
select device, sensor, max(sensor||'1'), min(time), max(time) + interval '1 day' from metrics group by device, sensor order by 1,2;

:PREFIX select device, sensor, min(time), max(time) + make_interval(days => length(sensor)) from metrics group by device, sensor order by 1,2;
select device, sensor, min(time), max(time) + make_interval(days => length(sensor)) from metrics group by device, sensor order by 1,2;

-- Can aggregate on expression over segmentby
:PREFIX select device, sensor, max(sensor||'1'), avg(length(device)) from metrics group by device, sensor order by 1,2;
select device, sensor, max(sensor||'1'), avg(length(device)) from metrics group by device, sensor order by 1,2;

-- Can use Having as it filters on aggregate outputs without changing sort
:PREFIX select device, sensor, count(*) from metrics group by device, sensor having count(*) > length(sensor) order by 1,2;
select device, sensor, count(*) from metrics group by device, sensor having count(*) > length(sensor) order by 1,2;

-- Cannot use expressions on order by keys as sort might be broken
:PREFIX select device, sensor||'1', min(time), max(time) from metrics group by device, sensor order by 1,2;
select device, sensor||'1', min(time), max(time) from metrics group by device, sensor order by 1,2;

-- Cannot pushdown sort on non-var keys
:PREFIX select device, sensor||'1', min(time), max(time) from metrics group by device, sensor||'1' order by 1,2;
select device, sensor||'1', min(time), max(time) from metrics group by device, sensor||'1' order by 1,2;

-- Cannot use non-segmentby columns outside of eligible aggregates
:PREFIX select device, sensor from metrics where value > length(sensor) group by device, sensor order by 1,2;
select device, sensor from metrics where value > length(sensor) group by device, sensor order by 1,2;

:PREFIX select device, sensor from metrics where time > '2025-01-01 01:00:00 PST' group by device, sensor order by 1,2;
select device, sensor from metrics where time > '2025-01-01 01:00:00 PST' group by device, sensor order by 1,2;

:PREFIX select device, sensor, avg(EXTRACT(epoch FROM time)::integer) from metrics group by device, sensor order by 1,2;
select device, sensor, avg(EXTRACT(epoch FROM time)::integer) from metrics group by device, sensor order by 1,2;

:PREFIX select device, sensor, max(value) from metrics group by device, sensor order by 1,2;
select device, sensor, count(*), max(value) from metrics group by device, sensor order by 1,2;

-- Min/max over expression on orderby column is also not permissible
:PREFIX select device, sensor, max(EXTRACT(epoch FROM time)::integer) from metrics group by device, sensor order by 1,2;
select device, sensor, max(EXTRACT(epoch FROM time)::integer) from metrics group by device, sensor order by 1,2;

-- Columnar Index Scan produces unsorted output currently,
-- so Columnar Index Scan eligible queries will not benefit from unordered sort, yet
:PREFIX select device, sensor, min(time) from metrics group by device, sensor order by 1,2;
select device, sensor, min(time) from metrics group by device, sensor order by 1,2;

:PREFIX select device, sensor, first(time,time) from metrics group by device, sensor order by 1,2;
select device, sensor, first(time,time) from metrics group by device, sensor order by 1,2;

drop table metrics cascade;

RESET timescaledb.enable_direct_compress_insert;
RESET timescaledb.enable_compressed_unordered_sort;

RESET max_parallel_workers_per_gather;
RESET enable_bitmapscan;
RESET enable_seqscan;
