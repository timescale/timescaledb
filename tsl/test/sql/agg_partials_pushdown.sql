-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\set PREFIX 'EXPLAIN (analyze, verbose, buffers off, costs off, timing off, summary off)'

-- Make parallel plans predictable
SET max_parallel_workers_per_gather = 1;
SET parallel_leader_participation = off;

CREATE TABLE testtable(filter_1 int, filler_2 int, filler_3 int, time timestamptz NOT NULL, device_id int, v0 int, v1 int, v2 float, v3 float);
SELECT create_hypertable('testtable', 'time');
ALTER TABLE testtable SET (timescaledb.compress, timescaledb.compress_orderby='time DESC', timescaledb.compress_segmentby='device_id');

INSERT INTO testtable(time,device_id,v0,v1,v2,v3)
SELECT time, device_id, device_id+1,  device_id + 2, device_id + 0.5, NULL
FROM generate_series('2000-01-01 0:00:00+0'::timestamptz,'2000-01-10 23:55:00+0','1day') gtime(time), generate_series(1,5,1) gdevice(device_id);

SELECT compress_chunk(c) FROM show_chunks('testtable') c;

ANALYZE testtable;

-- Pushdown aggregation to the chunk level
SELECT count(*), sum(v0), sum(v1), sum(v2), sum(v3) FROM testtable WHERE time >= '2000-01-01 00:00:00+0' AND time <= '2000-02-01 00:00:00+0';

:PREFIX
SELECT count(*), sum(v0), sum(v1), sum(v2), sum(v3) FROM testtable WHERE time >= '2000-01-01 00:00:00+0' AND time <= '2000-02-01 00:00:00+0';

-- Create partially compressed chunk
INSERT INTO testtable(time,device_id,v0,v1,v2,v3)
SELECT time, device_id, device_id+1,  device_id + 2, device_id + 0.5, NULL
FROM generate_series('2000-01-01 0:00:00+0'::timestamptz,'2000-01-10 23:55:00+0','1day') gtime(time), generate_series(1,5,1) gdevice(device_id);

ANALYZE testtable;

-- Pushdown aggregation to the chunk level
SELECT count(*), sum(v0), sum(v1), sum(v2), sum(v3) FROM testtable WHERE time >= '2000-01-01 00:00:00+0' AND time <= '2000-02-01 00:00:00+0';

:PREFIX
SELECT count(*), sum(v0), sum(v1), sum(v2), sum(v3) FROM testtable WHERE time >= '2000-01-01 00:00:00+0' AND time <= '2000-02-01 00:00:00+0';


-- Same query using chunk append
SELECT count(*), sum(v0), sum(v1), sum(v2), sum(v3) FROM testtable WHERE time >= '2000-01-01 00:00:00+0'::text::timestamptz AND time <= '2000-02-01 00:00:00+0';

:PREFIX
SELECT count(*), sum(v0), sum(v1), sum(v2), sum(v3) FROM testtable WHERE time >= '2000-01-01 00:00:00+0'::text::timestamptz AND time <= '2000-02-01 00:00:00+0';

-- Perform chunk append startup chunk exclusion - issue 6282
:PREFIX
SELECT count(*), sum(v0), sum(v1), sum(v2), sum(v3) FROM testtable WHERE time >= '2000-01-09 00:00:00+0'::text::timestamptz AND time <= '2000-02-01 00:00:00+0'::text::timestamptz;

-- Force plain / sorted aggregation
SET enable_hashagg = OFF;

SELECT count(*), sum(v0), sum(v1), sum(v2), sum(v3) FROM testtable WHERE time >= '2000-01-01 00:00:00+0'::text::timestamptz AND time <= '2000-02-01 00:00:00+0';

:PREFIX
SELECT count(*), sum(v0), sum(v1), sum(v2), sum(v3) FROM testtable WHERE time >= '2000-01-01 00:00:00+0'::text::timestamptz AND time <= '2000-02-01 00:00:00+0';

RESET enable_hashagg;

-- Check chunk exclusion for index scans
SET enable_seqscan = OFF;

SELECT count(*), sum(v0), sum(v1), sum(v2), sum(v3) FROM testtable WHERE time >= '2000-01-09 00:00:00+0'::text::timestamptz AND time <= '2000-02-01 00:00:00+0'::text::timestamptz;

:PREFIX
SELECT count(*), sum(v0), sum(v1), sum(v2), sum(v3) FROM testtable WHERE time >= '2000-01-09 00:00:00+0'::text::timestamptz AND time <= '2000-02-01 00:00:00+0'::text::timestamptz;

RESET enable_seqscan;

-- Check Append Node under ChunkAppend
RESET enable_hashagg;
RESET timescaledb.enable_chunkwise_aggregation;

CREATE TABLE testtable2 (
  timecustom BIGINT NOT NULL,
  device_id TEXT NOT NULL,
  series_0 DOUBLE PRECISION NULL,
  series_1 DOUBLE PRECISION NULL,
  series_2 DOUBLE PRECISION NULL,
  series_bool BOOLEAN NULL
);

CREATE INDEX ON testtable2 (timeCustom DESC NULLS LAST, device_id);

SELECT * FROM create_hypertable('testtable2', 'timecustom', 'device_id', number_partitions => 2, chunk_time_interval=>_timescaledb_functions.interval_to_usec('1 month'));

INSERT INTO testtable2 VALUES
(1257894000000000000, 'dev1', 1.5, 1, 2, true),
(1257894000000000000, 'dev1', 1.5, 2, NULL, NULL),
(1257894000000001000, 'dev1', 2.5, 3, NULL, NULL),
(1257894001000000000, 'dev1', 3.5, 4, NULL, NULL),
(1257897600000000000, 'dev1', 4.5, 5, NULL, false),
(1257894002000000000, 'dev1', 5.5, 6, NULL, true),
(1257894002000000000, 'dev1', 5.5, 7, NULL, false);

INSERT INTO testtable2(timeCustom, device_id, series_0, series_1) VALUES
(1257987600000000000, 'dev1', 1.5, 1),
(1257987600000000000, 'dev1', 1.5, 2),
(1257894000000000000, 'dev2', 1.5, 1),
(1257894002000000000, 'dev1', 2.5, 3);

SELECT timeCustom t, min(series_0) FROM PUBLIC.testtable2 GROUP BY t ORDER BY t DESC NULLS LAST limit 2;

:PREFIX
SELECT timeCustom t, min(series_0) FROM PUBLIC.testtable2 GROUP BY t ORDER BY t DESC NULLS LAST limit 2;

-- Force parallel query
SELECT set_config(CASE WHEN current_setting('server_version_num')::int < 160000 THEN 'force_parallel_mode' ELSE 'debug_parallel_query' END,'on', false);
SET parallel_setup_cost = 0;
SET parallel_tuple_cost = 0;

SELECT timeCustom t, min(series_0) FROM PUBLIC.testtable2 GROUP BY t ORDER BY t DESC NULLS LAST limit 2;

:PREFIX
SELECT timeCustom t, min(series_0) FROM PUBLIC.testtable2 GROUP BY t ORDER BY t DESC NULLS LAST limit 2;

-- Test that we don't process groupingSets
:PREFIX
SELECT timeCustom t, min(series_0) FROM PUBLIC.testtable2 GROUP BY ROLLUP(t);

-- Check parallel fallback into a non-partial aggregation
SET timescaledb.enable_chunkwise_aggregation = OFF;
SET enable_hashagg = OFF;

SELECT timeCustom t, min(series_0) FROM PUBLIC.testtable2 GROUP BY t ORDER BY t DESC NULLS LAST limit 2;

:PREFIX
SELECT timeCustom t, min(series_0) FROM PUBLIC.testtable2 GROUP BY t ORDER BY t DESC NULLS LAST limit 2;

RESET timescaledb.enable_chunkwise_aggregation;
RESET enable_hashagg;

-- Test aggregation pushdown with MergeAppend node
CREATE TABLE merge_append_test (start_time timestamptz, sensor_id int, cluster varchar (253), cost_recommendation_memory numeric);
SELECT * FROM create_hypertable('merge_append_test', 'start_time');
CREATE INDEX merge_append_test_sensorid ON merge_append_test USING btree (start_time, sensor_id);

INSERT INTO merge_append_test
SELECT
    date_series,
    1,
    'production-1',
   random() * 100
   FROM generate_series('2023-10-01 00:00:00', '2023-12-01 00:00:00', INTERVAL '1 hour') AS date_series
;

INSERT INTO merge_append_test
SELECT
    date_series,
    sensor_id,
    'production-2',
   random() * 100
   FROM generate_series('2023-10-01 00:00:00', '2023-12-01 00:00:00', INTERVAL '1 hour') AS date_series,
generate_series(1, 100, 1) AS sensor_id
;

ANALYZE merge_append_test;

SET enable_seqscan = off;
SET random_page_cost = 0;
SET cpu_operator_cost = 0;
SET enable_hashagg = off;
RESET parallel_setup_cost;
RESET parallel_tuple_cost;
SELECT set_config(CASE WHEN current_setting('server_version_num')::int < 160000 THEN 'force_parallel_mode' ELSE 'debug_parallel_query' END, 'off', false);

:PREFIX
SELECT
    start_time, sensor_id,
    SUM(cost_recommendation_memory)
FROM
    merge_append_test
WHERE
    start_time >= '2023-11-27 00:00:00Z'
    AND start_time <= '2023-12-01 00:00:00Z'
    AND sensor_id < 10
    AND CLUSTER = 'production-2'
GROUP BY
    1, 2;

RESET enable_seqscan;
RESET random_page_cost;
RESET cpu_operator_cost;
RESET enable_hashagg;
