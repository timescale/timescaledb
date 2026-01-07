-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

--
--
-- Test caggs with "time" partitioning on UUIDv7
--
--
CREATE TABLE uuid_events(id uuid primary key, device int, temp float);

\set ON_ERROR_STOP 0
-- Test invalid interval type
SELECT create_hypertable('uuid_events', 'id', chunk_time_interval => true);
\set ON_ERROR_STOP 1

SELECT create_hypertable('uuid_events', 'id', chunk_time_interval => interval '1 day');

INSERT INTO uuid_events VALUES
       ('0194214e-cd00-7000-a9a7-63f1416dab45', 2, 2.0),
       ('01942117-de80-7000-8121-f12b2b69dd96', 1, 1.0),
       ('0194263e-3a80-7000-8f40-82c987b1bc1f', 3, 3.0),
       ('01942675-2900-7000-8db1-a98694b18785', 4, 4.0),
       ('01942bd2-7380-7000-9bc4-5f97443907b8', 5, 5.0),
       ('01942d52-f900-7000-866e-07d6404d53c1', 6, 6.0);


SELECT * FROM show_chunks('uuid_events');

CREATE TABLE ts_events(ts timestamptz primary key, device int, temp float);
SELECT create_hypertable('ts_events', 'ts', chunk_time_interval => interval '1 day');
INSERT INTO ts_events SELECT uuid_timestamp(id), device, temp FROM uuid_events;


SELECT
    _timescaledb_functions.to_timestamp(range_start) AS chunk_range_start,
    _timescaledb_functions.to_timestamp(range_end) AS chunk_range_end
FROM _timescaledb_catalog.dimension_slice ds
JOIN _timescaledb_catalog.dimension d ON (ds.dimension_id = d.id)
JOIN _timescaledb_catalog.hypertable h ON (d.hypertable_id = h.id)
WHERE h.table_name = 'uuid_events'
LIMIT 1 OFFSET 1 \gset

CREATE MATERIALIZED VIEW daily_uuid_events WITH (timescaledb.continuous) AS
SELECT time_bucket('1 day', id) AS day, round(avg(temp)::numeric, 3) AS temp
FROM uuid_events WHERE device < 6
GROUP BY 1;

CREATE MATERIALIZED VIEW daily_ts_events WITH (timescaledb.continuous) AS
SELECT time_bucket('1 day', ts) AS day, round(avg(temp)::numeric, 3) AS temp
FROM ts_events WHERE device < 6
GROUP BY 1;

-- The uuid and timestmap caggs should look the same
SELECT * FROM daily_uuid_events ORDER BY day;
SELECT * FROM daily_ts_events ORDER BY day;

-- Update both caggs with new data with same timestamps
INSERT INTO uuid_events VALUES
       ('0194254e-cd00-7000-a9a7-63f1416dab45', 2, 2.0),
       ('019552dd-7500-7000-8a80-103c4d6ee60e', 3, 7.0);

INSERT INTO ts_events VALUES
       (uuid_timestamp('0194254e-cd00-7000-a9a7-63f1416dab45'), 2, 2.0),
       (uuid_timestamp('019552dd-7500-7000-8a80-103c4d6ee60e'), 3, 7.0);

-- Refresh the caggs
CALL refresh_continuous_aggregate('daily_uuid_events', NULL, NULL);
CALL refresh_continuous_aggregate('daily_ts_events', NULL, NULL);

-- The caggs should be updated in the same way
SELECT * FROM daily_uuid_events ORDER BY day;
SELECT * FROM daily_ts_events ORDER BY day;

-- Test merge refresh
SET timescaledb.enable_merge_on_cagg_refresh = true;
INSERT INTO uuid_events VALUES
       ('0194256e-cd00-7000-a9a7-63f1416dab45', 4, 8.0),
       ('019552fd-7500-7000-8a80-103c4d6ee60e', 5, 9.0);

INSERT INTO ts_events VALUES
       (uuid_timestamp('0194256e-cd00-7000-a9a7-63f1416dab45'), 4, 8.0),
       (uuid_timestamp('019552fd-7500-7000-8a80-103c4d6ee60e'), 5, 9.0);

CALL refresh_continuous_aggregate('daily_uuid_events', NULL, NULL);
CALL refresh_continuous_aggregate('daily_ts_events', NULL, NULL);

SELECT * FROM daily_uuid_events ORDER BY day;
SELECT * FROM daily_ts_events ORDER BY day;

ALTER MATERIALIZED VIEW daily_uuid_events SET (tsdb.enable_columnstore = true);
SELECT ch AS chunk FROM show_chunks('daily_uuid_events') ch ORDER BY ch LIMIT 1 \gset
CALL convert_to_columnstore(:'chunk');

SELECT * FROM daily_uuid_events ORDER BY day;

SET timescaledb.enable_merge_on_cagg_refresh = true;
-- Test insert with direct compress enabled
SET timescaledb.enable_direct_compress_insert = true;
-- Enable compression also on the raw hypertable
ALTER TABLE uuid_events SET (tsdb.enable_columnstore = true);

SElECT setseed(0.9);

-- Generate 2000 rows with UUIDv7 from timestamps
CREATE TEMP TABLE tmpdata (ts, device, temp) AS
SELECT
    ts,
    (row_number() OVER () % 10) + 1,
    random() * 100
FROM generate_series(
  '2025-03-01 11:00',
  '2025-03-04 11:00',
    interval '1 minute'
) AS ts;

INSERT INTO uuid_events
SELECT to_uuidv7(ts), device, temp
FROM tmpdata;

INSERT INTO ts_events
SELECT ts, device, temp
FROM tmpdata;

select count(*) from uuid_events;
select count(*) from ts_events;
CALL refresh_continuous_aggregate('daily_uuid_events', NULL, NULL);
CALL refresh_continuous_aggregate('daily_ts_events', NULL, NULL);

SELECT * FROM daily_uuid_events ORDER BY day;
SELECT * FROM daily_ts_events ORDER BY day;

-- Test hierarchical cagg
CREATE MATERIALIZED VIEW weekly_uuid_events WITH (timescaledb.continuous) AS
SELECT time_bucket('1 week', day) AS week, max(temp) AS max_temp
FROM daily_uuid_events
GROUP BY 1;

CREATE MATERIALIZED VIEW weekly_ts_events WITH (timescaledb.continuous) AS
SELECT time_bucket('1 week', day) AS week, max(temp) AS max_temp
FROM daily_ts_events
GROUP BY 1;

SELECT * FROM weekly_uuid_events ORDER BY week;
SELECT * FROM weekly_ts_events ORDER BY week;

INSERT INTO uuid_events
VALUES
  ('01958280-4800-7000-bc29-713158a4e8b6', 1, 11.0),
  ('019587a6-a400-7000-ac1e-577d59f409af', 2, 12.0);

INSERT INTO ts_events
VALUES
  (uuid_timestamp('01958280-4800-7000-bc29-713158a4e8b6'), 1, 11.0),
  (uuid_timestamp('019587a6-a400-7000-ac1e-577d59f409af'), 2, 12.0);

-- Test refresh via policy
SELECT add_continuous_aggregate_policy('daily_uuid_events', start_offset => NULL, end_offset => NULL, schedule_interval => '1 minute') AS job_id \gset

CALL run_job(:job_id);
CALL refresh_continuous_aggregate('weekly_uuid_events', NULL, NULL);
CALL refresh_continuous_aggregate('daily_ts_events', NULL, NULL);
CALL refresh_continuous_aggregate('weekly_ts_events', NULL, NULL);

SELECT * FROM daily_uuid_events ORDER BY day;
SELECT * FROM weekly_uuid_events ORDER BY week;

SELECT * FROM daily_ts_events ORDER BY day;
SELECT * FROM weekly_ts_events ORDER BY week;
